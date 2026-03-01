//! Server configuration.
//!
//! Example config.toml:
//!
//! ```toml
//! [server]
//! host = "0.0.0.0"
//! port = 9000
//!
//! [metadata]
//! path = "/var/lib/s3/meta"
//!
//! [[directories]]
//! path = "/data/disk1"
//!
//! [[directories]]
//! path = "/data/disk2"
//! ```

use anyhow::{bail, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}
fn default_port() -> u16 {
    9000
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct MetadataConfig {
    /// Path where chDB stores its data files.
    pub path: String,
}

impl Default for MetadataConfig {
    fn default() -> Self {
        Self {
            path: "/var/lib/s3/meta".to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct DirectoryConfig {
    pub path: String,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub metadata: MetadataConfig,
    #[serde(rename = "directories")]
    pub directories: Vec<DirectoryConfig>,
}

impl Config {
    pub fn from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let cfg: Config = toml::from_str(&content)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Build a default in-memory config (useful for development / tests).
    pub fn default_dev() -> Self {
        Self {
            server: ServerConfig::default(),
            metadata: MetadataConfig {
                path: "/tmp/s3-meta".to_string(),
            },
            directories: vec![DirectoryConfig {
                path: "/tmp/s3-data".to_string(),
            }],
        }
    }

    fn validate(&self) -> Result<()> {
        if self.directories.is_empty() {
            bail!("At least one [[directories]] entry is required");
        }
        Ok(())
    }
}
