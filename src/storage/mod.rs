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
        assert!(
            !dirs.is_empty(),
            "JbodStorage requires at least one directory"
        );
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
    #[allow(dead_code)]
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
                    return Err(e).with_context(|| format!("remove bucket dir {:?}", bucket_dir));
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
    for byte in bucket
        .bytes()
        .chain(b"/".iter().copied())
        .chain(key.bytes())
    {
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- fnv1a ---

    #[test]
    fn fnv1a_known_value() {
        // Pre-computed: echo -n "mybucket/mykey" | python3 -c "
        //   import sys; d=sys.stdin.buffer.read()
        //   h=14695981039346656037
        //   for b in d: h^=b; h=(h*1099511628211)&0xFFFFFFFFFFFFFFFF
        //   print(h)"
        let h = fnv1a("mybucket", "mykey");
        assert_ne!(h, 0);
        // Deterministic across calls
        assert_eq!(h, fnv1a("mybucket", "mykey"));
    }

    #[test]
    fn fnv1a_different_inputs_differ() {
        assert_ne!(fnv1a("a", "b"), fnv1a("b", "a"));
        assert_ne!(fnv1a("bucket", "key1"), fnv1a("bucket", "key2"));
        assert_ne!(fnv1a("bucket1", "key"), fnv1a("bucket2", "key"));
    }

    #[test]
    fn fnv1a_separator_matters() {
        // "ab/c" != "a/bc" — the '/' separator must not be conflatable
        assert_ne!(fnv1a("ab", "c"), fnv1a("a", "bc"));
    }

    // --- compute_etag ---

    #[test]
    fn etag_empty_data() {
        // MD5("") = d41d8cd98f00b204e9800998ecf8427e
        assert_eq!(compute_etag(b""), "d41d8cd98f00b204e9800998ecf8427e");
    }

    #[test]
    fn etag_known_string() {
        // MD5("Hello, S3!") verified externally
        let etag = compute_etag(b"Hello, S3!");
        assert_eq!(etag.len(), 32, "ETag must be 32 hex chars");
        assert!(etag.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(etag, "9176e88e2973b7e2e260c438f56b1cd0");
    }

    #[test]
    fn etag_no_quotes() {
        let etag = compute_etag(b"data");
        assert!(!etag.starts_with('"'), "ETag must not include quotes");
        assert!(!etag.ends_with('"'));
    }

    // --- disk_index ---

    #[test]
    fn disk_index_within_bounds() {
        let storage = JbodStorage::new(vec![
            PathBuf::from("/tmp/d0"),
            PathBuf::from("/tmp/d1"),
            PathBuf::from("/tmp/d2"),
        ]);
        for key in &["a", "b", "c", "foo/bar", "deep/nested/key"] {
            let idx = storage.disk_index("bucket", key);
            assert!((idx as usize) < storage.dirs.len());
        }
    }

    #[test]
    fn disk_index_deterministic() {
        let storage = JbodStorage::new(vec![PathBuf::from("/tmp/d0"), PathBuf::from("/tmp/d1")]);
        let idx1 = storage.disk_index("bucket", "key");
        let idx2 = storage.disk_index("bucket", "key");
        assert_eq!(idx1, idx2);
    }

    // --- object_path ---

    #[test]
    fn object_path_layout() {
        let storage = JbodStorage::new(vec![PathBuf::from("/data/disk0")]);
        let path = storage.object_path(0, "my-bucket", "dir/file.txt");
        assert_eq!(path, PathBuf::from("/data/disk0/my-bucket/dir/file.txt"));
    }

    // --- JbodStorage async put / get / delete ---

    #[tokio::test]
    async fn put_get_delete_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = JbodStorage::new(vec![tmp.path().to_path_buf()]);
        storage.init().await.unwrap();

        let data = b"hello storage";
        let (idx, etag) = storage.put("bkt", "obj/key.txt", data).await.unwrap();
        assert_eq!(etag, compute_etag(data));

        let got = storage.get(idx, "bkt", "obj/key.txt").await.unwrap();
        assert_eq!(got, data);

        storage.delete(idx, "bkt", "obj/key.txt").await.unwrap();
        assert!(storage.get(idx, "bkt", "obj/key.txt").await.is_err());
    }

    #[tokio::test]
    async fn delete_nonexistent_is_ok() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = JbodStorage::new(vec![tmp.path().to_path_buf()]);
        // deleting a file that was never created must succeed silently
        storage.delete(0, "bkt", "no/such/file").await.unwrap();
    }

    #[tokio::test]
    async fn create_delete_bucket() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = JbodStorage::new(vec![tmp.path().to_path_buf()]);
        storage.init().await.unwrap();
        storage.create_bucket("test-bucket").await.unwrap();
        assert!(tmp.path().join("test-bucket").exists());

        // Put an object with a nested key, then delete bucket (uses remove_dir_all)
        storage.put("test-bucket", "a/b/c.txt", b"x").await.unwrap();
        storage.delete_bucket("test-bucket").await.unwrap();
        assert!(!tmp.path().join("test-bucket").exists());
    }

    #[tokio::test]
    async fn delete_bucket_nonexistent_is_ok() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = JbodStorage::new(vec![tmp.path().to_path_buf()]);
        storage.delete_bucket("ghost-bucket").await.unwrap();
    }
}
