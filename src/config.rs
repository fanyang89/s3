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
//!
//! [[auth.credentials]]
//! access_key = "AKIAIOSFODNN7EXAMPLE"
//! secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
//!
//! [[auth.credentials]]
//! access_key = "test"
//! secret_key = "testtest"
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

#[derive(Debug, Deserialize, Clone)]
pub struct Credential {
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Debug, Deserialize)]
pub struct AuthConfig {
    #[serde(rename = "credentials")]
    pub credentials: Vec<Credential>,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub metadata: MetadataConfig,
    #[serde(rename = "directories")]
    pub directories: Vec<DirectoryConfig>,
    pub auth: AuthConfig,
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
            auth: AuthConfig {
                credentials: vec![Credential {
                    access_key: "test".to_string(),
                    secret_key: "testtest".to_string(),
                }],
            },
        }
    }

    fn validate(&self) -> Result<()> {
        if self.directories.is_empty() {
            bail!("At least one [[directories]] entry is required");
        }
        if self.auth.credentials.is_empty() {
            bail!("At least one [[auth.credentials]] entry is required");
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_dev_is_valid() {
        let cfg = Config::default_dev();
        assert_eq!(cfg.server.host, "0.0.0.0");
        assert_eq!(cfg.server.port, 9000);
        assert!(!cfg.directories.is_empty());
    }

    #[test]
    fn parse_minimal_toml() {
        let toml = r#"
[[directories]]
path = "/data/disk0"

[[auth.credentials]]
access_key = "test"
secret_key = "testtest"
"#;
        let cfg: Config = toml::from_str(toml).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.server.host, "0.0.0.0");
        assert_eq!(cfg.server.port, 9000);
        assert_eq!(cfg.directories.len(), 1);
        assert_eq!(cfg.directories[0].path, "/data/disk0");
    }

    #[test]
    fn parse_full_toml() {
        let toml = r#"
[server]
host = "127.0.0.1"
port = 8080

[metadata]
path = "/var/meta"

[[directories]]
path = "/data/disk0"

[[directories]]
path = "/data/disk1"

[[auth.credentials]]
access_key = "admin"
secret_key = "password123"

[[auth.credentials]]
access_key = "backup"
secret_key = "backup-secret"
"#;
        let cfg: Config = toml::from_str(toml).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.server.host, "127.0.0.1");
        assert_eq!(cfg.server.port, 8080);
        assert_eq!(cfg.metadata.path, "/var/meta");
        assert_eq!(cfg.directories.len(), 2);
        assert_eq!(cfg.auth.credentials.len(), 2);
    }

    #[test]
    fn validate_rejects_empty_directories() {
        let cfg = Config {
            server: ServerConfig::default(),
            metadata: MetadataConfig::default(),
            directories: vec![],
            auth: AuthConfig {
                credentials: vec![Credential {
                    access_key: "test".to_string(),
                    secret_key: "test".to_string(),
                }],
            },
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn from_file_nonexistent_returns_error() {
        let result = Config::from_file("/nonexistent/path/config.toml");
        assert!(result.is_err());
    }
}
