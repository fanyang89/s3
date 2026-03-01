//! chDB metadata layer.
//!
//! Wraps the raw FFI with safe Rust abstractions and provides CRUD operations
//! for `buckets` and `objects` tables.

pub mod chdb_sys;

use std::ffi::{CStr, CString};
use std::sync::Mutex;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use tracing::debug;

use chdb_sys::{
    chdb_close_conn, chdb_connect, chdb_destroy_query_result, chdb_query_n, chdb_result_buffer,
    chdb_result_error, chdb_result_length, ChdbConnection,
};

// ---------------------------------------------------------------------------
// Safe connection wrapper
// ---------------------------------------------------------------------------

/// A thread-safe chDB connection.
///
/// chDB allows a single connection per process; we protect it with a `Mutex`
/// so that concurrent tokio tasks serialise through it.
pub struct ChdbConn {
    inner: Mutex<*mut ChdbConnection>,
}

// SAFETY: ChdbConnection is internally thread-safe per the chDB docs.
unsafe impl Send for ChdbConn {}
unsafe impl Sync for ChdbConn {}

impl ChdbConn {
    /// Open a persistent chDB database at `path`.
    pub fn open(path: &str) -> Result<Self> {
        let path_arg = format!("--path={}", path);
        let prog = CString::new("chdb").unwrap();
        let path_cstr = CString::new(path_arg.as_str()).context("path contains null byte")?;

        let argv: Vec<*const std::ffi::c_char> = vec![prog.as_ptr(), path_cstr.as_ptr()];

        let conn_ptr = unsafe { chdb_connect(argv.len() as i32, argv.as_ptr()) };

        if conn_ptr.is_null() {
            bail!("chdb_connect returned NULL for path={}", path);
        }

        Ok(Self {
            inner: Mutex::new(conn_ptr),
        })
    }

    /// Execute a SQL statement; returns raw UTF-8 result bytes.
    pub fn exec(&self, sql: &str, format: &str) -> Result<Vec<u8>> {
        let guard = self.inner.lock().unwrap();
        let conn: ChdbConnection = unsafe { **guard };

        let sql_bytes = sql.as_bytes();
        let fmt_bytes = format.as_bytes();

        let result = unsafe {
            chdb_query_n(
                conn,
                sql_bytes.as_ptr() as *const std::ffi::c_char,
                sql_bytes.len(),
                fmt_bytes.as_ptr() as *const std::ffi::c_char,
                fmt_bytes.len(),
            )
        };

        if result.is_null() {
            bail!("chdb_query_n returned NULL");
        }

        // Check for error
        let err_ptr = unsafe { chdb_result_error(result) };
        if !err_ptr.is_null() {
            let err_msg = unsafe { CStr::from_ptr(err_ptr) }
                .to_string_lossy()
                .into_owned();
            unsafe { chdb_destroy_query_result(result) };
            if !err_msg.is_empty() {
                bail!("chDB query error: {}", err_msg);
            }
        }

        let len = unsafe { chdb_result_length(result) };
        let buf_ptr = unsafe { chdb_result_buffer(result) };

        let data = if len > 0 && !buf_ptr.is_null() {
            unsafe { std::slice::from_raw_parts(buf_ptr as *const u8, len) }.to_vec()
        } else {
            Vec::new()
        };

        unsafe { chdb_destroy_query_result(result) };

        debug!(sql = %sql, result_bytes = data.len(), "chdb exec");
        Ok(data)
    }

    /// Convenience: execute DDL / DML that returns no meaningful rows.
    pub fn execute(&self, sql: &str) -> Result<()> {
        self.exec(sql, "TabSeparated")?;
        Ok(())
    }

    /// Convenience: execute a SELECT and return UTF-8 text result.
    pub fn query_str(&self, sql: &str, format: &str) -> Result<String> {
        let bytes = self.exec(sql, format)?;
        String::from_utf8(bytes).context("chDB result is not valid UTF-8")
    }
}

impl Drop for ChdbConn {
    fn drop(&mut self) {
        let guard = self.inner.lock().unwrap();
        unsafe { chdb_close_conn(*guard) };
    }
}

// ---------------------------------------------------------------------------
// Schema initialisation
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

// Soft-delete log – used so that ListObjects can reflect deletes promptly
// before the ReplacingMergeTree background merge runs.
const CREATE_DELETED_OBJECTS: &str = "
CREATE TABLE IF NOT EXISTS meta.deleted_objects (
    bucket      String,
    key         String,
    deleted_at  DateTime64(3, 'UTC')
) ENGINE = MergeTree()
ORDER BY (bucket, key)";

pub fn init_schema(conn: &ChdbConn) -> Result<()> {
    conn.execute(CREATE_DB)?;
    conn.execute(CREATE_BUCKETS)?;
    conn.execute(CREATE_OBJECTS)?;
    conn.execute(CREATE_DELETED_OBJECTS)?;
    Ok(())
}

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
// Bucket operations
// ---------------------------------------------------------------------------

/// Escape a string for inclusion inside a ClickHouse single-quoted literal.
fn esc(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

pub fn bucket_create(conn: &ChdbConn, name: &str) -> Result<()> {
    let sql = format!(
        "INSERT INTO meta.buckets (name, created_at) VALUES ('{}', now64(3))",
        esc(name)
    );
    conn.execute(&sql)
}

pub fn bucket_exists(conn: &ChdbConn, name: &str) -> Result<bool> {
    let sql = format!(
        "SELECT count() FROM meta.buckets WHERE name = '{}'",
        esc(name)
    );
    let result = conn.query_str(&sql, "TabSeparated")?;
    let count: u64 = result.trim().parse().context("parse bucket count")?;
    Ok(count > 0)
}

pub fn bucket_list(conn: &ChdbConn) -> Result<Vec<BucketMeta>> {
    let sql = "SELECT name, created_at FROM meta.buckets ORDER BY name";
    let result = conn.query_str(sql, "JSONEachRow")?;
    let mut buckets = Vec::new();
    for line in result.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(line)?;
        let name = v["name"].as_str().unwrap_or("").to_string();
        let ts_str = v["created_at"].as_str().unwrap_or("").to_string();
        // chDB returns DateTime64 as "YYYY-MM-DD HH:MM:SS.mmm"
        let created_at = parse_datetime64(&ts_str);
        buckets.push(BucketMeta { name, created_at });
    }
    Ok(buckets)
}

pub fn bucket_delete(conn: &ChdbConn, name: &str) -> Result<()> {
    // First check if bucket is empty
    let count_sql = format!(
        "SELECT count() FROM meta.objects FINAL WHERE bucket = '{}' \
         AND (bucket, key) NOT IN (SELECT bucket, key FROM meta.deleted_objects WHERE bucket = '{}')",
        esc(name), esc(name)
    );
    let result = conn.query_str(&count_sql, "TabSeparated")?;
    let count: u64 = result.trim().parse().context("parse object count")?;
    if count > 0 {
        bail!("BucketNotEmpty");
    }

    let sql = format!(
        "ALTER TABLE meta.buckets DELETE WHERE name = '{}'",
        esc(name)
    );
    conn.execute(&sql)
}

// ---------------------------------------------------------------------------
// Object operations
// ---------------------------------------------------------------------------

pub fn object_put(conn: &ChdbConn, meta: &ObjectMeta) -> Result<()> {
    // Remove any pending delete entry first
    let del_sql = format!(
        "ALTER TABLE meta.deleted_objects DELETE WHERE bucket = '{}' AND key = '{}'",
        esc(&meta.bucket),
        esc(&meta.key)
    );
    conn.execute(&del_sql)?;

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
    conn.execute(&sql)
}

pub fn object_get(conn: &ChdbConn, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
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
    let result = conn.query_str(&sql, "JSONEachRow")?;
    let line = result.lines().find(|l| !l.trim().is_empty());
    match line {
        None => Ok(None),
        Some(l) => Ok(Some(parse_object_meta(l)?)),
    }
}

pub fn object_delete(conn: &ChdbConn, bucket: &str, key: &str) -> Result<()> {
    let sql = format!(
        "INSERT INTO meta.deleted_objects (bucket, key, deleted_at) \
         VALUES ('{}', '{}', now64(3))",
        esc(bucket),
        esc(key)
    );
    conn.execute(&sql)
}

pub fn object_list(
    conn: &ChdbConn,
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

    let result = conn.query_str(&sql, "JSONEachRow")?;
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
// Helpers
// ---------------------------------------------------------------------------

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
    // chDB returns DateTime64 in format "YYYY-MM-DD HH:MM:SS.mmm"
    // Try a few formats.
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
