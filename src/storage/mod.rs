//! JBOD storage engine.
//!
//! Each object is stored on exactly one disk, selected by:
//!   `disk_index = fnv1a(bucket + "/" + key) % n_disks`
//!
//! On-disk layout:
//!   `{directory}/{bucket}/{key}`
//!
//! The key may contain '/' which naturally becomes nested directories.

use std::path::PathBuf;

use anyhow::{Context, Result};

// ---------------------------------------------------------------------------
// JBOD engine
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct JbodStorage {
    dirs: Vec<PathBuf>,
}

impl JbodStorage {
    pub fn new(dirs: Vec<PathBuf>) -> Self {
        assert!(!dirs.is_empty(), "JbodStorage requires at least one directory");
        Self { dirs }
    }

    /// Ensure all root directories exist.
    pub async fn init(&self) -> Result<()> {
        for dir in &self.dirs {
            tokio::fs::create_dir_all(dir)
                .await
                .with_context(|| format!("create storage dir {:?}", dir))?;
        }
        Ok(())
    }

    /// Choose the disk index for a given (bucket, key) pair deterministically.
    pub fn disk_index(&self, bucket: &str, key: &str) -> u8 {
        let n = self.dirs.len() as u64;
        let hash = fnv1a(bucket, key);
        (hash % n) as u8
    }

    /// Resolve the full filesystem path for an object.
    pub fn object_path(&self, disk_index: u8, bucket: &str, key: &str) -> PathBuf {
        self.dirs[disk_index as usize].join(bucket).join(key)
    }

    /// Write object data to disk, creating parent directories as needed.
    /// Returns (disk_index, etag).
    pub async fn put(&self, bucket: &str, key: &str, data: &[u8]) -> Result<(u8, String)> {
        let idx = self.disk_index(bucket, key);
        let path = self.object_path(idx, bucket, key);

        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create parent dir {:?}", parent))?;
        }

        tokio::fs::write(&path, data)
            .await
            .with_context(|| format!("write object {:?}", path))?;

        let etag = compute_etag(data);
        Ok((idx, etag))
    }

    /// Read object data from disk.
    pub async fn get(&self, disk_index: u8, bucket: &str, key: &str) -> Result<Vec<u8>> {
        let path = self.object_path(disk_index, bucket, key);
        tokio::fs::read(&path)
            .await
            .with_context(|| format!("read object {:?}", path))
    }

    /// Stream object data using a tokio::fs::File for large objects.
    pub async fn get_file(
        &self,
        disk_index: u8,
        bucket: &str,
        key: &str,
    ) -> Result<tokio::fs::File> {
        let path = self.object_path(disk_index, bucket, key);
        tokio::fs::File::open(&path)
            .await
            .with_context(|| format!("open object {:?}", path))
    }

    /// Delete object data from disk.
    pub async fn delete(&self, disk_index: u8, bucket: &str, key: &str) -> Result<()> {
        let path = self.object_path(disk_index, bucket, key);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).with_context(|| format!("delete object {:?}", path)),
        }
    }

    /// Create the bucket directory on all disks.
    pub async fn create_bucket(&self, bucket: &str) -> Result<()> {
        for dir in &self.dirs {
            let bucket_dir = dir.join(bucket);
            tokio::fs::create_dir_all(&bucket_dir)
                .await
                .with_context(|| format!("create bucket dir {:?}", bucket_dir))?;
        }
        Ok(())
    }

    /// Remove the bucket directory from all disks (must be empty per metadata check).
    pub async fn delete_bucket(&self, bucket: &str) -> Result<()> {
        for dir in &self.dirs {
            let bucket_dir = dir.join(bucket);
            match tokio::fs::remove_dir_all(&bucket_dir).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(e)
                        .with_context(|| format!("remove bucket dir {:?}", bucket_dir));
                }
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// FNV-1a 64-bit hash of `bucket/key`.
fn fnv1a(bucket: &str, key: &str) -> u64 {
    const OFFSET: u64 = 14695981039346656037;
    const PRIME: u64 = 1099511628211;

    let mut hash = OFFSET;
    for byte in bucket.bytes().chain(b"/".iter().copied()).chain(key.bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// Compute S3-compatible ETag: hex(MD5(data)), no quotes.
pub fn compute_etag(data: &[u8]) -> String {
    use md5::Digest;
    let digest = md5::Md5::digest(data);
    hex::encode(digest)
}
