//! chDB metadata layer.
//!
//! Wraps chdb-rust abstractions and provides CRUD operations
//! for `buckets` and `objects` tables.
//!
//! # Usage
//!
//! There are two ways to use this module:
//!
//! ## 1. Using MetaStore (Recommended)
//!
//! ```rust,ignore
//! use std::sync::{Arc, Mutex};
//! use chdb_rust::connection::Connection;
//! use crate::metadata::MetaStore;
//!
//! // Create a connection
//! let conn = Connection::open_with_path("/path/to/db")?;
//! let conn = Arc::new(Mutex::new(conn));
//!
//! // Create MetaStore
//! let store = MetaStore::new(conn);
//!
//! // Initialize schema
//! store.init_schema()?;
//!
//! // Use the store
//! store.bucket_create("my-bucket")?;
//! let exists = store.bucket_exists("my-bucket")?;
//! ```.
//!
//! ## 2. Using legacy functions (Backward compatible)
//!
//! ```rust,ignore
//! use std::sync::{Arc, Mutex};
//! use chdb_rust::connection::Connection;
//! use crate::metadata::{init_schema, bucket_create, bucket_exists};
//!
//! let conn = Connection::open_with_path("/path/to/db")?;
//! let db = Arc::new(Mutex::new(conn));
//!
//! init_schema(&db)?;
//! bucket_create(&db, "my-bucket")?;
//! let exists = bucket_exists(&db, "my-bucket")?;
//! ```

use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};
use chdb_rust::{connection::Connection, format::OutputFormat};
use chrono::{DateTime, Utc};
use tracing::debug;

// ---------------------------------------------------------------------------
// MetaStore - Main metadata store wrapper
// ---------------------------------------------------------------------------

/// Metadata store wrapper for chDB connection.
///
/// Provides a clean object-oriented interface for metadata operations.
/// Wraps an `Arc<Mutex<Connection>>` and provides methods for bucket
/// and object management.
pub struct MetaStore {
    conn: Arc<Mutex<Connection>>,
}

impl MetaStore {
    /// Create a new MetaStore instance.
    ///
    /// # Arguments
    ///
    /// * `conn` - An Arc-wrapped Mutex containing the chDB Connection
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self { conn }
    }

    /// Initialize the database schema.
    ///
    /// Creates the necessary tables (buckets, objects, deleted_objects)
    /// if they don't already exist.
    pub fn init_schema(&self) -> Result<()> {
        self.execute(CREATE_DB)?;
        self.execute(CREATE_BUCKETS)?;
        self.execute(CREATE_OBJECTS)?;
        self.execute(CREATE_DELETED_OBJECTS)?;
        Ok(())
    }

    fn exec(&self, sql: &str, format: &str) -> Result<Vec<u8>> {
        let guard = self.conn.lock().unwrap();
        let output_fmt = output_format_from_str(format)?;
        let result = guard.query(sql, output_fmt).context("chdb query failed")?;
        let data = result.data_ref().to_vec();

        debug!(sql = %sql, result_bytes = data.len(), "chdb exec");
        Ok(data)
    }

    fn execute(&self, sql: &str) -> Result<()> {
        self.exec(sql, "TabSeparated")?;
        Ok(())
    }

    fn query_str(&self, sql: &str, format: &str) -> Result<String> {
        let bytes = self.exec(sql, format)?;
        String::from_utf8(bytes).context("chDB result is not valid UTF-8")
    }

    /// Create a new bucket.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the bucket to create
    pub fn bucket_create(&self, name: &str) -> Result<()> {
        let sql = format!(
            "INSERT INTO meta.buckets (name, created_at) VALUES ('{}', now64(3))",
            esc(name)
        );
        self.execute(&sql)
    }

    /// Check if a bucket exists.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the bucket to check
    ///
    /// # Returns
    ///
    /// `true` if the bucket exists, `false` otherwise
    pub fn bucket_exists(&self, name: &str) -> Result<bool> {
        let sql = format!(
            "SELECT count() FROM meta.buckets WHERE name = '{}'",
            esc(name)
        );
        let result = self.query_str(&sql, "TabSeparated")?;
        let count: u64 = result.trim().parse().context("parse bucket count")?;
        Ok(count > 0)
    }

    /// List all buckets.
    ///
    /// # Returns
    ///
    /// A vector of `BucketMeta` containing bucket metadata
    pub fn bucket_list(&self) -> Result<Vec<BucketMeta>> {
        let sql = "SELECT name, created_at FROM meta.buckets ORDER BY name";
        let result = self.query_str(sql, "JSONEachRow")?;
        let mut buckets = Vec::new();
        for line in result.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let v: serde_json::Value = serde_json::from_str(line)?;
            let name = v["name"].as_str().unwrap_or("").to_string();
            let ts_str = v["created_at"].as_str().unwrap_or("").to_string();
            let created_at = parse_datetime64(&ts_str);
            buckets.push(BucketMeta { name, created_at });
        }
        Ok(buckets)
    }

    /// Delete a bucket.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the bucket to delete
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket is not empty
    pub fn bucket_delete(&self, name: &str) -> Result<()> {
        let count_sql = format!(
            "SELECT count() FROM meta.objects FINAL WHERE bucket = '{}' \
             AND (bucket, key) NOT IN (SELECT bucket, key FROM meta.deleted_objects WHERE bucket = '{}')",
            esc(name),
            esc(name)
        );
        let result = self.query_str(&count_sql, "TabSeparated")?;
        let count: u64 = result.trim().parse().context("parse object count")?;
        if count > 0 {
            bail!("BucketNotEmpty");
        }

        let sql = format!(
            "ALTER TABLE meta.buckets DELETE WHERE name = '{}'",
            esc(name)
        );
        self.execute(&sql)
    }

    /// Put an object into storage.
    ///
    /// # Arguments
    ///
    /// * `meta` - The object metadata to store
    pub fn object_put(&self, meta: &ObjectMeta) -> Result<()> {
        let del_sql = format!(
            "ALTER TABLE meta.deleted_objects DELETE WHERE bucket = '{}' AND key = '{}'",
            esc(&meta.bucket),
            esc(&meta.key)
        );
        self.execute(&del_sql)?;

        let sql = format!(
            "INSERT INTO meta.objects \
             (bucket, key, disk_index, size, etag, content_type, last_modified) \
             VALUES ('{}', '{}', {}, {}, '{}', '{}', now64(3))",
            esc(&meta.bucket),
            esc(&meta.key),
            meta.disk_index,
            meta.size,
            esc(&meta.etag),
            esc(&meta.content_type),
        );
        self.execute(&sql)
    }

    /// Get object metadata.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket name
    /// * `key` - The object key
    ///
    /// # Returns
    ///
    /// `Some(ObjectMeta)` if the object exists, `None` otherwise
    pub fn object_get(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        let sql = format!(
            "SELECT bucket, key, disk_index, size, etag, content_type, last_modified \
             FROM meta.objects FINAL \
             WHERE bucket = '{}' AND key = '{}' \
             AND (bucket, key) NOT IN \
               (SELECT bucket, key FROM meta.deleted_objects WHERE bucket = '{}' AND key = '{}') \
             LIMIT 1",
            esc(bucket),
            esc(key),
            esc(bucket),
            esc(key)
        );
        let result = self.query_str(&sql, "JSONEachRow")?;
        let line = result.lines().find(|l| !l.trim().is_empty());
        match line {
            None => Ok(None),
            Some(l) => Ok(Some(parse_object_meta(l)?)),
        }
    }

    /// Delete an object.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket name
    /// * `key` - The object key
    pub fn object_delete(&self, bucket: &str, key: &str) -> Result<()> {
        let sql = format!(
            "INSERT INTO meta.deleted_objects (bucket, key, deleted_at) \
             VALUES ('{}', '{}', now64(3))",
            esc(bucket),
            esc(key)
        );
        self.execute(&sql)
    }

    /// List objects in a bucket.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket name
    /// * `prefix` - Optional prefix filter
    /// * `max_keys` - Maximum number of keys to return
    /// * `continuation` - Optional continuation token for pagination
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - A vector of `ObjectMeta` for the objects
    /// - A boolean indicating if there are more results (is_truncated)
    pub fn object_list(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        max_keys: u32,
        continuation: Option<&str>,
    ) -> Result<(Vec<ObjectMeta>, bool)> {
        let prefix_clause = match prefix {
            Some(p) if !p.is_empty() => format!("AND startsWith(key, '{}')", esc(p)),
            _ => String::new(),
        };
        let continuation_clause = match continuation {
            Some(c) if !c.is_empty() => format!("AND key > '{}'", esc(c)),
            _ => String::new(),
        };

        let sql = format!(
            "SELECT bucket, key, disk_index, size, etag, content_type, last_modified \
             FROM meta.objects FINAL \
             WHERE bucket = '{}' \
             AND (bucket, key) NOT IN \
               (SELECT bucket, key FROM meta.deleted_objects WHERE bucket = '{}') \
             {} {} \
             ORDER BY key \
             LIMIT {}",
            esc(bucket),
            esc(bucket),
            prefix_clause,
            continuation_clause,
            max_keys + 1
        );

        let result = self.query_str(&sql, "JSONEachRow")?;
        let mut objects = Vec::new();
        for line in result.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            objects.push(parse_object_meta(line)?);
        }

        let is_truncated = objects.len() > max_keys as usize;
        if is_truncated {
            objects.truncate(max_keys as usize);
        }

        Ok((objects, is_truncated))
    }
}

// ---------------------------------------------------------------------------
// Schema constants
// ---------------------------------------------------------------------------

const CREATE_DB: &str = "CREATE DATABASE IF NOT EXISTS meta";

const CREATE_BUCKETS: &str = "
CREATE TABLE IF NOT EXISTS meta.buckets (
    name        String,
    created_at  DateTime64(3, 'UTC')
) ENGINE = MergeTree()
ORDER BY name";

const CREATE_OBJECTS: &str = "
CREATE TABLE IF NOT EXISTS meta.objects (
    bucket        String,
    key           String,
    disk_index    UInt8,
    size          UInt64,
    etag          String,
    content_type  String,
    last_modified DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_modified)
ORDER BY (bucket, key)";

const CREATE_DELETED_OBJECTS: &str = "
CREATE TABLE IF NOT EXISTS meta.deleted_objects (
    bucket      String,
    key         String,
    deleted_at  DateTime64(3, 'UTC')
) ENGINE = MergeTree()
ORDER BY (bucket, key)";

// ---------------------------------------------------------------------------
// Metadata types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct BucketMeta {
    pub name: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ObjectMeta {
    pub bucket: String,
    pub key: String,
    pub disk_index: u8,
    pub size: u64,
    pub etag: String,
    pub content_type: String,
    pub last_modified: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Legacy compatibility functions (for backward compatibility)
// ---------------------------------------------------------------------------

fn conn_exec(conn: &Mutex<Connection>, sql: &str, format: &str) -> Result<Vec<u8>> {
    let guard = conn.lock().unwrap();
    let output_fmt = output_format_from_str(format)?;
    let result = guard.query(sql, output_fmt).context("chdb query failed")?;
    let data = result.data_ref().to_vec();

    debug!(sql = %sql, result_bytes = data.len(), "chdb exec");
    Ok(data)
}

fn conn_execute(conn: &Mutex<Connection>, sql: &str) -> Result<()> {
    conn_exec(conn, sql, "TabSeparated")?;
    Ok(())
}

fn conn_query_str(conn: &Mutex<Connection>, sql: &str, format: &str) -> Result<String> {
    let bytes = conn_exec(conn, sql, format)?;
    String::from_utf8(bytes).context("chDB result is not valid UTF-8")
}

pub fn init_schema(conn: &Mutex<Connection>) -> Result<()> {
    conn_execute(conn, CREATE_DB)?;
    conn_execute(conn, CREATE_BUCKETS)?;
    conn_execute(conn, CREATE_OBJECTS)?;
    conn_execute(conn, CREATE_DELETED_OBJECTS)?;
    Ok(())
}

pub fn bucket_create(conn: &Mutex<Connection>, name: &str) -> Result<()> {
    let sql = format!(
        "INSERT INTO meta.buckets (name, created_at) VALUES ('{}', now64(3))",
        esc(name)
    );
    conn_execute(conn, &sql)
}

pub fn bucket_exists(conn: &Mutex<Connection>, name: &str) -> Result<bool> {
    let sql = format!(
        "SELECT count() FROM meta.buckets WHERE name = '{}'",
        esc(name)
    );
    let result = conn_query_str(conn, &sql, "TabSeparated")?;
    let count: u64 = result.trim().parse().context("parse bucket count")?;
    Ok(count > 0)
}

pub fn bucket_list(conn: &Mutex<Connection>) -> Result<Vec<BucketMeta>> {
    let sql = "SELECT name, created_at FROM meta.buckets ORDER BY name";
    let result = conn_query_str(conn, sql, "JSONEachRow")?;
    let mut buckets = Vec::new();
    for line in result.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(line)?;
        let name = v["name"].as_str().unwrap_or("").to_string();
        let ts_str = v["created_at"].as_str().unwrap_or("").to_string();
        let created_at = parse_datetime64(&ts_str);
        buckets.push(BucketMeta { name, created_at });
    }
    Ok(buckets)
}

pub fn bucket_delete(conn: &Mutex<Connection>, name: &str) -> Result<()> {
    let count_sql = format!(
        "SELECT count() FROM meta.objects FINAL WHERE bucket = '{}' \
         AND (bucket, key) NOT IN (SELECT bucket, key FROM meta.deleted_objects WHERE bucket = '{}')",
        esc(name),
        esc(name)
    );
    let result = conn_query_str(conn, &count_sql, "TabSeparated")?;
    let count: u64 = result.trim().parse().context("parse object count")?;
    if count > 0 {
        bail!("BucketNotEmpty");
    }

    let sql = format!(
        "ALTER TABLE meta.buckets DELETE WHERE name = '{}'",
        esc(name)
    );
    conn_execute(conn, &sql)
}

pub fn object_put(conn: &Mutex<Connection>, meta: &ObjectMeta) -> Result<()> {
    let del_sql = format!(
        "ALTER TABLE meta.deleted_objects DELETE WHERE bucket = '{}' AND key = '{}'",
        esc(&meta.bucket),
        esc(&meta.key)
    );
    conn_execute(conn, &del_sql)?;

    let sql = format!(
        "INSERT INTO meta.objects \
         (bucket, key, disk_index, size, etag, content_type, last_modified) \
         VALUES ('{}', '{}', {}, {}, '{}', '{}', now64(3))",
        esc(&meta.bucket),
        esc(&meta.key),
        meta.disk_index,
        meta.size,
        esc(&meta.etag),
        esc(&meta.content_type),
    );
    conn_execute(conn, &sql)
}

pub fn object_get(conn: &Mutex<Connection>, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
    let sql = format!(
        "SELECT bucket, key, disk_index, size, etag, content_type, last_modified \
         FROM meta.objects FINAL \
         WHERE bucket = '{}' AND key = '{}' \
         AND (bucket, key) NOT IN \
           (SELECT bucket, key FROM meta.deleted_objects WHERE bucket = '{}' AND key = '{}') \
         LIMIT 1",
        esc(bucket),
        esc(key),
        esc(bucket),
        esc(key)
    );
    let result = conn_query_str(conn, &sql, "JSONEachRow")?;
    let line = result.lines().find(|l| !l.trim().is_empty());
    match line {
        None => Ok(None),
        Some(l) => Ok(Some(parse_object_meta(l)?)),
    }
}

pub fn object_delete(conn: &Mutex<Connection>, bucket: &str, key: &str) -> Result<()> {
    let sql = format!(
        "INSERT INTO meta.deleted_objects (bucket, key, deleted_at) \
         VALUES ('{}', '{}', now64(3))",
        esc(bucket),
        esc(key)
    );
    conn_execute(conn, &sql)
}

pub fn object_list(
    conn: &Mutex<Connection>,
    bucket: &str,
    prefix: Option<&str>,
    max_keys: u32,
    continuation: Option<&str>,
) -> Result<(Vec<ObjectMeta>, bool)> {
    let prefix_clause = match prefix {
        Some(p) if !p.is_empty() => format!("AND startsWith(key, '{}')", esc(p)),
        _ => String::new(),
    };
    let continuation_clause = match continuation {
        Some(c) if !c.is_empty() => format!("AND key > '{}'", esc(c)),
        _ => String::new(),
    };

    let sql = format!(
        "SELECT bucket, key, disk_index, size, etag, content_type, last_modified \
         FROM meta.objects FINAL \
         WHERE bucket = '{}' \
         AND (bucket, key) NOT IN \
           (SELECT bucket, key FROM meta.deleted_objects WHERE bucket = '{}') \
         {} {} \
         ORDER BY key \
         LIMIT {}",
        esc(bucket),
        esc(bucket),
        prefix_clause,
        continuation_clause,
        max_keys + 1
    );

    let result = conn_query_str(conn, &sql, "JSONEachRow")?;
    let mut objects = Vec::new();
    for line in result.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        objects.push(parse_object_meta(line)?);
    }

    let is_truncated = objects.len() > max_keys as usize;
    if is_truncated {
        objects.truncate(max_keys as usize);
    }

    Ok((objects, is_truncated))
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

fn esc(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

fn parse_object_meta(json_line: &str) -> Result<ObjectMeta> {
    let v: serde_json::Value = serde_json::from_str(json_line)?;
    Ok(ObjectMeta {
        bucket: v["bucket"].as_str().unwrap_or("").to_string(),
        key: v["key"].as_str().unwrap_or("").to_string(),
        disk_index: v["disk_index"].as_u64().unwrap_or(0) as u8,
        size: v["size"].as_u64().unwrap_or(0),
        etag: v["etag"].as_str().unwrap_or("").to_string(),
        content_type: v["content_type"]
            .as_str()
            .unwrap_or("application/octet-stream")
            .to_string(),
        last_modified: parse_datetime64(v["last_modified"].as_str().unwrap_or("")),
    })
}

fn parse_datetime64(s: &str) -> DateTime<Utc> {
    let formats = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
    ];
    for fmt in &formats {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
            return DateTime::from_naive_utc_and_offset(dt, Utc);
        }
    }
    Utc::now()
}

fn output_format_from_str(format: &str) -> Result<OutputFormat> {
    match format {
        "JSONEachRow" => Ok(OutputFormat::JSONEachRow),
        "TabSeparated" => Ok(OutputFormat::TabSeparated),
        _ => bail!("unsupported output format: {format}"),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn esc_plain_string() {
        assert_eq!(esc("hello"), "hello");
    }

    #[test]
    fn esc_single_quote() {
        assert_eq!(esc("it's"), "it\\'s");
    }

    #[test]
    fn esc_backslash() {
        assert_eq!(esc("a\\b"), "a\\\\b");
    }

    #[test]
    fn esc_combined() {
        assert_eq!(esc("a\\'b"), "a\\\\\\'b");
    }

    #[test]
    fn parse_datetime64_space_format() {
        let dt = parse_datetime64("2024-01-15 12:34:56.789");
        assert_eq!(
            dt.format("%Y-%m-%d %H:%M:%S").to_string(),
            "2024-01-15 12:34:56"
        );
    }

    #[test]
    fn parse_datetime64_no_millis() {
        let dt = parse_datetime64("2024-06-01 00:00:00");
        assert_eq!(dt.format("%Y-%m-%d").to_string(), "2024-06-01");
    }

    #[test]
    fn parse_datetime64_invalid_falls_back_to_now() {
        let before = Utc::now();
        let dt = parse_datetime64("not-a-date");
        let after = Utc::now();
        assert!(dt >= before && dt <= after);
    }

    #[test]
    fn output_format_json_each_row() {
        assert!(matches!(
            output_format_from_str("JSONEachRow").unwrap(),
            OutputFormat::JSONEachRow
        ));
    }

    #[test]
    fn output_format_tab_separated() {
        assert!(matches!(
            output_format_from_str("TabSeparated").unwrap(),
            OutputFormat::TabSeparated
        ));
    }

    #[test]
    fn output_format_unknown_rejected() {
        assert!(output_format_from_str("UnknownFormat").is_err());
    }

    #[test]
    fn parse_object_meta_valid() {
        let json = r#"{"bucket":"b","key":"k","disk_index":1,"size":42,"etag":"abc","content_type":"text/plain","last_modified":"2024-01-01 00:00:00"}"#;
        let meta = parse_object_meta(json).unwrap();
        assert_eq!(meta.bucket, "b");
        assert_eq!(meta.key, "k");
        assert_eq!(meta.disk_index, 1);
        assert_eq!(meta.size, 42);
        assert_eq!(meta.etag, "abc");
        assert_eq!(meta.content_type, "text/plain");
    }

    #[test]
    fn parse_object_meta_missing_content_type_defaults() {
        let json = r#"{"bucket":"b","key":"k","disk_index":0,"size":0,"etag":"","last_modified":"2024-01-01 00:00:00"}"#;
        let meta = parse_object_meta(json).unwrap();
        assert_eq!(meta.content_type, "application/octet-stream");
    }
}
