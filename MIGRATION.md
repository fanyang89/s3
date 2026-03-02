# MetaStore Migration Guide

This document describes the new `MetaStore` struct and how to migrate from the legacy function-based API.

## Overview

The `MetaStore` struct provides an object-oriented wrapper around the chDB connection for metadata operations. It offers better encapsulation and cleaner API design while maintaining backward compatibility.

## What's New

### MetaStore Struct

A new struct that wraps `Arc<Mutex<Connection>>` and provides methods for all metadata operations:

```rust
pub struct MetaStore {
    conn: Arc<Mutex<Connection>>,
}
```

### Available Methods

- `new(conn: Arc<Mutex<Connection>>) -> Self` - Create a new MetaStore
- `init_schema(&self) -> Result<()>` - Initialize database schema
- `bucket_create(&self, name: &str) -> Result<()>` - Create a bucket
- `bucket_exists(&self, name: &str) -> Result<bool>` - Check if bucket exists
- `bucket_list(&self) -> Result<Vec<BucketMeta>>` - List all buckets
- `bucket_delete(&self, name: &str) -> Result<()>` - Delete a bucket
- `object_put(&self, meta: &ObjectMeta) -> Result<()>` - Put object metadata
- `object_get(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>>` - Get object metadata
- `object_delete(&self, bucket: &str, key: &str) -> Result<()>` - Delete object
- `object_list(&self, bucket: &str, prefix: Option<&str>, max_keys: u32, continuation: Option<&str>) -> Result<(Vec<ObjectMeta>, bool)>` - List objects

## Migration Examples

### Before (Legacy API)

```rust
use std::sync::{Arc, Mutex};
use chdb_rust::connection::Connection;
use crate::metadata::{init_schema, bucket_create, bucket_exists};

fn setup() -> Result<()> {
    let conn = Connection::open_with_path("/path/to/db")?;
    let db = Arc::new(Mutex::new(conn));
    
    init_schema(&db)?;
    bucket_create(&db, "my-bucket")?;
    let exists = bucket_exists(&db, "my-bucket")?;
    
    Ok(())
}
```

### After (New MetaStore API)

```rust
use std::sync::{Arc, Mutex};
use chdb_rust::connection::Connection;
use crate::metadata::MetaStore;

fn setup() -> Result<()> {
    let conn = Connection::open_with_path("/path/to/db")?;
    let conn = Arc::new(Mutex::new(conn));
    let store = MetaStore::new(conn);
    
    store.init_schema()?;
    store.bucket_create("my-bucket")?;
    let exists = store.bucket_exists("my-bucket")?;
    
    Ok(())
}
```

### Migrating StorageHandler

#### Before

```rust
pub struct StorageHandler {
    pub db: Arc<Mutex<Connection>>,
    pub storage: Arc<JbodStorage>,
}

impl StorageHandler {
    fn create_bucket(&self, name: &str) -> Result<()> {
        bucket_create(&self.db, name)
    }
}
```

#### After

```rust
pub struct StorageHandler {
    pub meta_store: Arc<MetaStore>,
    pub storage: Arc<JbodStorage>,
}

impl StorageHandler {
    fn create_bucket(&self, name: &str) -> Result<()> {
        self.meta_store.bucket_create(name)
    }
}
```

## Benefits

1. **Better Encapsulation**: All metadata operations are grouped in one struct
2. **Cleaner API**: No need to pass `&Mutex<Connection>` to every function
3. **Easier Testing**: Can mock the MetaStore for unit tests
4. **Type Safety**: The MetaStore owns the connection, preventing misuse
5. **Backward Compatible**: Legacy functions still work for existing code

## Backward Compatibility

The legacy functions are still available and work exactly as before. You can gradually migrate your code to use `MetaStore` without breaking existing functionality.

All legacy functions are preserved:
- `init_schema(conn: &Mutex<Connection>)`
- `bucket_create(conn: &Mutex<Connection>, name: &str)`
- `bucket_exists(conn: &Mutex<Connection>, name: &str)`
- `bucket_list(conn: &Mutex<Connection>)`
- `bucket_delete(conn: &Mutex<Connection>, name: &str)`
- `object_put(conn: &Mutex<Connection>, meta: &ObjectMeta)`
- `object_get(conn: &Mutex<Connection>, bucket: &str, key: &str)`
- `object_delete(conn: &Mutex<Connection>, bucket: &str, key: &str)`
- `object_list(conn: &Mutex<Connection>, bucket: &str, prefix: Option<&str>, max_keys: u32, continuation: Option<&str>)`

## Running the Example

To see a complete example of MetaStore usage:

```bash
cargo run --example metastore_usage
```

## Future Improvements

Potential enhancements for the MetaStore:

1. Add connection pooling support
2. Implement caching for frequently accessed metadata
3. Add batch operations for better performance
4. Support for transactions
5. Add metrics and monitoring hooks

## Questions?

If you have questions about migrating to MetaStore, please open an issue on GitHub.
