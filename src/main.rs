mod config;
mod metadata;
mod s3_handler;
mod storage;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use chdb_rust::connection::Connection;
use clap::Parser;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use tokio::net::TcpListener;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use crate::config::{Config, DirectoryConfig};
use crate::metadata::init_schema;
use crate::s3_handler::StorageHandler;
use crate::storage::JbodStorage;

#[derive(Parser, Debug)]
#[command(name = "s3-server", version, about = "S3-compatible object storage server", long_about = None)]
struct Args {
    #[arg(short, long, value_name = "FILE", help = "Path to configuration file")]
    config: Option<PathBuf>,

    #[arg(short = 'H', long, help = "Override server host")]
    host: Option<String>,

    #[arg(short, long, help = "Override server port")]
    port: Option<u16>,

    #[arg(long, value_name = "PATH", help = "Override metadata storage path")]
    metadata_path: Option<String>,

    #[arg(
        short = 'D',
        long,
        value_name = "DIR",
        help = "Add storage directory (can be used multiple times)"
    )]
    directory: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let mut cfg = match args.config {
        Some(path) => Config::from_file(&path.to_string_lossy())
            .with_context(|| format!("failed to load config from {}", path.display()))?,
        None => {
            warn!("no config file provided; using built-in dev defaults");
            Config::default_dev()
        }
    };

    if let Some(host) = args.host {
        cfg.server.host = host;
    }
    if let Some(port) = args.port {
        cfg.server.port = port;
    }
    if let Some(path) = args.metadata_path {
        cfg.metadata.path = path;
    }
    if !args.directory.is_empty() {
        cfg.directories = args
            .directory
            .into_iter()
            .map(|path| DirectoryConfig { path })
            .collect();
    }

    info!(
        host = %cfg.server.host,
        port = cfg.server.port,
        meta_path = %cfg.metadata.path,
        n_dirs = cfg.directories.len(),
        "starting s3-server"
    );

    std::fs::create_dir_all(&cfg.metadata.path)
        .with_context(|| format!("create metadata dir {}", cfg.metadata.path))?;

    let conn = Connection::open_with_path(&cfg.metadata.path)
        .with_context(|| format!("open chDB at {}", cfg.metadata.path))?;
    let db = Arc::new(Mutex::new(conn));
    init_schema(&db).context("init chDB schema")?;
    info!("chDB metadata store ready");

    let dirs: Vec<PathBuf> = cfg
        .directories
        .iter()
        .map(|d| PathBuf::from(&d.path))
        .collect();
    let storage = Arc::new(JbodStorage::new(dirs));
    storage.init().await.context("init storage directories")?;
    info!("JBOD storage engine ready ({} dirs)", cfg.directories.len());

    let handler = StorageHandler {
        db: Arc::clone(&db),
        storage: Arc::clone(&storage),
    };

    let mut auth = SimpleAuth::new();
    for cred in &cfg.auth.credentials {
        auth.register(cred.access_key.clone(), cred.secret_key.clone().into());
    }
    info!(n_credentials = cfg.auth.credentials.len(), "loaded auth credentials");
    
    let mut svc_builder = S3ServiceBuilder::new(handler);
    svc_builder.set_auth(auth);
    let svc = svc_builder.build();

    let addr: SocketAddr = format!("{}:{}", cfg.server.host, cfg.server.port)
        .parse()
        .context("parse listen address")?;

    let listener = TcpListener::bind(addr).await.context("bind TCP listener")?;
    info!(addr = %addr, "listening");

    loop {
        let (stream, peer) = listener.accept().await.context("accept TCP connection")?;
        let svc = svc.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            if let Err(e) = ConnBuilder::new(TokioExecutor::new())
                .serve_connection(io, svc)
                .await
            {
                warn!(peer = %peer, error = %e, "connection error");
            }
        });
    }
}
