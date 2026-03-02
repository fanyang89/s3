//! Example demonstrating MetaStore usage.
//!
//! This example shows how to use the new MetaStore struct for metadata operations.

use anyhow::Result;
use chdb_rust::connection::Connection;
use std::sync::{Arc, Mutex};

// Note: In actual usage, you would import from your crate:
// use your_crate::metadata::{MetaStore, ObjectMeta};

fn main() -> Result<()> {
    // Example 1: Creating a MetaStore instance
    println!("=== MetaStore Usage Example ===\n");

    // In a real application, you would:
    // let conn = Connection::open_with_path("/path/to/metadata")?;
    // let conn = Arc::new(Mutex::new(conn));
    // let store = MetaStore::new(conn);

    println!("1. Initialize MetaStore:");
    println!("   let conn = Connection::open_with_path(\"/path/to/db\")?;");
    println!("   let conn = Arc::new(Mutex::new(conn));");
    println!("   let store = MetaStore::new(conn);\n");

    // Example 2: Initialize schema
    println!("2. Initialize database schema:");
    println!("   store.init_schema()?;\n");

    // Example 3: Bucket operations
    println!("3. Bucket operations:");
    println!("   // Create a bucket");
    println!("   store.bucket_create(\"my-bucket\")?;");
    println!("");
    println!("   // Check if bucket exists");
    println!("   let exists = store.bucket_exists(\"my-bucket\")?;");
    println!("   println!(\"Bucket exists: {{}}\", exists);");
    println!("");
    println!("   // List all buckets");
    println!("   let buckets = store.bucket_list()?;");
    println!("   for bucket in buckets {{");
    println!("       println!(\"Bucket: {{}}, Created: {{:?}}\", bucket.name, bucket.created_at);");
    println!("   }}");
    println!("");
    println!("   // Delete a bucket (must be empty)");
    println!("   store.bucket_delete(\"my-bucket\")?;\n");

    // Example 4: Object operations
    println!("4. Object operations:");
    println!("   // Create object metadata");
    println!("   let meta = ObjectMeta {{");
    println!("       bucket: \"my-bucket\".to_string(),");
    println!("       key: \"path/to/object.txt\".to_string(),");
    println!("       disk_index: 0,");
    println!("       size: 1024,");
    println!("       etag: \"abc123\".to_string(),");
    println!("       content_type: \"text/plain\".to_string(),");
    println!("       last_modified: chrono::Utc::now(),");
    println!("   }};");
    println!("");
    println!("   // Put object metadata");
    println!("   store.object_put(&meta)?;");
    println!("");
    println!("   // Get object metadata");
    println!("   if let Some(obj) = store.object_get(\"my-bucket\", \"path/to/object.txt\")? {{");
    println!("       println!(\"Object size: {{}}\", obj.size);");
    println!("   }}");
    println!("");
    println!("   // List objects with pagination");
    println!("   let (objects, is_truncated) = store.object_list(");
    println!("       \"my-bucket\",");
    println!("       Some(\"path/\"),  // prefix filter");
    println!("       100,            // max keys");
    println!("       None,           // continuation token");
    println!("   )?;");
    println!("   for obj in objects {{");
    println!("       println!(\"Object: {{}}\", obj.key);");
    println!("   }}");
    println!("");
    println!("   // Delete object");
    println!("   store.object_delete(\"my-bucket\", \"path/to/object.txt\")?;\n");

    // Example 5: Using with StorageHandler
    println!("5. Integration with StorageHandler:");
    println!("   // In s3_handler.rs, you can replace:");
    println!("   // pub db: Arc<Mutex<Connection>>");
    println!("   // with:");
    println!("   // pub meta_store: Arc<MetaStore>");
    println!("   //");
    println!("   // Then use it like:");
    println!("   // self.meta_store.bucket_create(bucket)?;");
    println!("   // let exists = self.meta_store.bucket_exists(bucket)?;\n");

    println!("=== Benefits of MetaStore ===");
    println!("- Better encapsulation and organization");
    println!("- Object-oriented interface");
    println!("- Easier to test and mock");
    println!("- Cleaner method signatures (no need to pass &Mutex<Connection>)");
    println!("- Backward compatible with existing code\n");

    Ok(())
}
