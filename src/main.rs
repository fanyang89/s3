mod config;
mod metadata;
mod s3_handler;
mod storage;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use chdb_rust::connection::Connection;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use tokio::net::TcpListener;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use crate::config::Config;
use crate::metadata::init_schema;
use crate::s3_handler::StorageHandler;
use crate::storage::JbodStorage;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let cfg = match std::env::args().nth(1) {
        Some(path) => Config::from_file(&path)
            .with_context(|| format!("failed to load config from {path}"))?,
        None => {
            warn!("no config file provided; using built-in dev defaults");
            Config::default_dev()
        }
    };

    info!(
        host = %cfg.server.host,
        port = cfg.server.port,
        meta_path = %cfg.metadata.path,
        n_dirs = cfg.directories.len(),
        "starting s3-server"
    );

    // ---------------------------------------------------------------------------
    // Metadata (chDB)
    // ---------------------------------------------------------------------------
    std::fs::create_dir_all(&cfg.metadata.path)
        .with_context(|| format!("create metadata dir {}", cfg.metadata.path))?;

    let conn = Connection::open_with_path(&cfg.metadata.path)
        .with_context(|| format!("open chDB at {}", cfg.metadata.path))?;
    let db = Arc::new(Mutex::new(conn));
    init_schema(&db).context("init chDB schema")?;
    info!("chDB metadata store ready");

    // ---------------------------------------------------------------------------
    // JBOD Storage
    // ---------------------------------------------------------------------------
    let dirs: Vec<PathBuf> = cfg
        .directories
        .iter()
        .map(|d| PathBuf::from(&d.path))
        .collect();

    let storage = Arc::new(JbodStorage::new(dirs));
    storage.init().await.context("init storage directories")?;
    info!("JBOD storage engine ready ({} dirs)", cfg.directories.len());

    // ---------------------------------------------------------------------------
    // S3 Service
    // ---------------------------------------------------------------------------
    let handler = StorageHandler {
        db: Arc::clone(&db),
        storage: Arc::clone(&storage),
    };

    let auth = SimpleAuth::from_single("test", "testtest");
    let mut svc_builder = S3ServiceBuilder::new(handler);
    svc_builder.set_auth(auth);
    let svc = svc_builder.build();

    // ---------------------------------------------------------------------------
    // TCP listener
    // ---------------------------------------------------------------------------
    let addr: SocketAddr = format!("{}:{}", cfg.server.host, cfg.server.port)
        .parse()
        .context("parse listen address")?;

    let listener = TcpListener::bind(addr).await.context("bind TCP listener")?;
    info!(addr = %addr, "listening");

    // ---------------------------------------------------------------------------
    // Accept loop
    // ---------------------------------------------------------------------------
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
