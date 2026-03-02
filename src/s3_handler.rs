//! S3 operation handler.
//!
//! Implements the `s3s::S3` trait by delegating to our JBOD storage engine
//! (for data I/O) and chDB metadata layer (for bucket/object metadata).

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use chdb_rust::connection::Connection;
use futures::TryStreamExt;
use s3s::dto::*;
use s3s::{S3Request, S3Response, S3Result, s3_error};
use tracing::{error, info};

use crate::metadata::{
    BucketMeta, ObjectMeta, bucket_create, bucket_delete, bucket_exists, bucket_list,
    object_delete, object_get, object_list, object_put,
};
use crate::storage::JbodStorage;

// ---------------------------------------------------------------------------
// Handler state
// ---------------------------------------------------------------------------

pub struct StorageHandler {
    pub db: Arc<Mutex<Connection>>,
    pub storage: Arc<JbodStorage>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Collect a `StreamingBlob` into a `Bytes` buffer.
async fn collect_blob(blob: Option<StreamingBlob>) -> S3Result<Bytes> {
    let Some(blob) = blob else {
        return Ok(Bytes::new());
    };
    let chunks: Vec<Bytes> = blob
        .into_stream()
        .try_collect()
        .await
        .map_err(|e| s3_error!(InternalError, "failed to read request body: {e}"))?;

    let total = chunks.iter().map(|b| b.len()).sum();
    let mut buf = bytes::BytesMut::with_capacity(total);
    for chunk in chunks {
        buf.extend_from_slice(&chunk);
    }
    Ok(buf.freeze())
}

/// Convert our `ObjectMeta` last-modified timestamp to `Timestamp`.
fn to_timestamp(dt: chrono::DateTime<chrono::Utc>) -> Timestamp {
    Timestamp::from(
        std::time::SystemTime::UNIX_EPOCH
            + std::time::Duration::from_millis(dt.timestamp_millis() as u64),
    )
}

fn wrap_etag(raw: &str) -> ETag {
    // Our stored ETags are raw hex (no quotes); wrap as strong.
    ETag::Strong(raw.to_owned())
}

// ---------------------------------------------------------------------------
// S3 trait implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl s3s::S3 for StorageHandler {
    // ------------------------------------------------------------------
    // Bucket operations
    // ------------------------------------------------------------------

    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let bucket = req.input.bucket.as_str();
        info!(bucket, "CreateBucket");

        // Idempotency: ignore if already exists
        if bucket_exists(&self.db, bucket)
            .map_err(|e| s3_error!(InternalError, "metadata error: {e}"))?
        {
            // AWS returns BucketAlreadyOwnedByYou if the same account already owns it;
            // for simplicity we just accept the request as idempotent.
        } else {
            bucket_create(&self.db, bucket)
                .map_err(|e| s3_error!(InternalError, "metadata error: {e}"))?;
            self.storage
                .create_bucket(bucket)
                .await
                .map_err(|e| s3_error!(InternalError, "storage error: {e}"))?;
        }

        let output = CreateBucketOutput {
            location: Some(format!("/{}", bucket)),
        };
        Ok(S3Response::new(output))
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        let bucket = req.input.bucket.as_str();
        info!(bucket, "DeleteBucket");

        if !bucket_exists(&self.db, bucket)
            .map_err(|e| s3_error!(InternalError, "metadata error: {e}"))?
        {
            return Err(s3_error!(NoSuchBucket, "bucket {bucket} does not exist"));
        }

        bucket_delete(&self.db, bucket).map_err(|e| {
            if e.to_string().contains("BucketNotEmpty") {
                s3_error!(BucketNotEmpty, "bucket {bucket} is not empty")
            } else {
                s3_error!(InternalError, "metadata error: {e}")
            }
        })?;

        self.storage
            .delete_bucket(bucket)
            .await
            .map_err(|e| s3_error!(InternalError, "storage error: {e}"))?;

        Ok(S3Response::new(DeleteBucketOutput {}))
    }

    async fn list_buckets(
        &self,
        _req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        info!("ListBuckets");

        let metas: Vec<BucketMeta> =
            bucket_list(&self.db).map_err(|e| s3_error!(InternalError, "metadata error: {e}"))?;

        let buckets: Vec<Bucket> = metas
            .into_iter()
            .map(|m| Bucket {
                name: Some(BucketName::from(m.name.as_str())),
                creation_date: Some(to_timestamp(m.created_at)),
                bucket_region: None,
            })
            .collect();

        let output = ListBucketsOutput {
            buckets: Some(buckets),
            owner: None,
            continuation_token: None,
            prefix: None,
        };
        Ok(S3Response::new(output))
    }

    // ------------------------------------------------------------------
    // Object operations
    // ------------------------------------------------------------------

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let bucket = req.input.bucket.as_str();
        let key = req.input.key.as_str();
        let content_type = req
            .input
            .content_type
            .as_deref()
            .unwrap_or("application/octet-stream")
            .to_owned();
        info!(bucket, key, "PutObject");

        if !bucket_exists(&self.db, bucket)
            .map_err(|e| s3_error!(InternalError, "metadata error: {e}"))?
        {
            return Err(s3_error!(NoSuchBucket, "bucket {bucket} does not exist"));
        }

        let data = collect_blob(req.input.body).await?;

        let (disk_index, etag) = self
            .storage
            .put(bucket, key, &data)
            .await
            .map_err(|e| s3_error!(InternalError, "storage error: {e}"))?;

        let meta = ObjectMeta {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            disk_index,
            size: data.len() as u64,
            etag: etag.clone(),
            content_type,
            last_modified: chrono::Utc::now(),
        };
        object_put(&self.db, &meta).map_err(|e| s3_error!(InternalError, "metadata error: {e}"))?;

        let output = PutObjectOutput {
            e_tag: Some(wrap_etag(&etag)),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let bucket = req.input.bucket.as_str();
        let key = req.input.key.as_str();
        info!(bucket, key, "GetObject");

        let meta = object_get(&self.db, bucket, key)
            .map_err(|e| s3_error!(InternalError, "metadata error: {e}"))?
            .ok_or_else(|| s3_error!(NoSuchKey, "object {key} not found in bucket {bucket}"))?;

        let data = self
            .storage
            .get(meta.disk_index, bucket, key)
            .await
            .map_err(|e| s3_error!(NoSuchKey, "storage error: {e}"))?;

        let size = data.len() as i64;
        let bytes = Bytes::from(data);
        let stream = futures::stream::once(async move { Ok::<_, std::io::Error>(bytes) });
        let body = StreamingBlob::wrap(stream);

        let output = GetObjectOutput {
            body: Some(body),
            content_length: Some(size),
            content_type: Some(meta.content_type.clone()),
            e_tag: Some(wrap_etag(&meta.etag)),
            last_modified: Some(to_timestamp(meta.last_modified)),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let bucket = req.input.bucket.as_str();
        let key = req.input.key.as_str();
        info!(bucket, key, "DeleteObject");

        // Look up disk_index before deleting metadata
        let meta_opt = object_get(&self.db, bucket, key)
            .map_err(|e| s3_error!(InternalError, "metadata error: {e}"))?;

        if let Some(meta) = meta_opt {
            object_delete(&self.db, bucket, key)
                .map_err(|e| s3_error!(InternalError, "metadata error: {e}"))?;
            // Best-effort file delete; ignore not-found
            if let Err(e) = self.storage.delete(meta.disk_index, bucket, key).await {
                error!(bucket, key, error = %e, "failed to delete object file (metadata already removed)");
            }
        }
        // S3 spec: delete is idempotent; no error if key didn't exist.
        Ok(S3Response::new(DeleteObjectOutput {
            delete_marker: None,
            request_charged: None,
            version_id: None,
        }))
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let bucket = req.input.bucket.as_str();
        let key = req.input.key.as_str();
        info!(bucket, key, "HeadObject");

        let meta = object_get(&self.db, bucket, key)
            .map_err(|e| s3_error!(InternalError, "metadata error: {e}"))?
            .ok_or_else(|| s3_error!(NoSuchKey, "object {key} not found in bucket {bucket}"))?;

        let output = HeadObjectOutput {
            content_length: Some(meta.size as i64),
            content_type: Some(meta.content_type.clone()),
            e_tag: Some(wrap_etag(&meta.etag)),
            last_modified: Some(to_timestamp(meta.last_modified)),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        let bucket = req.input.bucket.as_str();
        let prefix = req.input.prefix.as_deref();
        let max_keys = req.input.max_keys.unwrap_or(1000).max(0) as u32;
        let marker = req.input.marker.as_deref();
        info!(bucket, ?prefix, "ListObjects");

        if !bucket_exists(&self.db, bucket)
            .map_err(|e| s3_error!(InternalError, "metadata error: {e}"))?
        {
            return Err(s3_error!(NoSuchBucket, "bucket {bucket} does not exist"));
        }

        let (objects, is_truncated) = object_list(&self.db, bucket, prefix, max_keys, marker)
            .map_err(|e| s3_error!(InternalError, "metadata error: {e}"))?;

        let last_key = objects.last().map(|o| o.key.clone());

        let contents: Vec<Object> = objects
            .iter()
            .map(|o| Object {
                key: Some(ObjectKey::from(o.key.as_str())),
                e_tag: Some(wrap_etag(&o.etag)),
                size: Some(o.size as i64),
                last_modified: Some(to_timestamp(o.last_modified)),
                owner: None,
                storage_class: None,
                checksum_algorithm: None,
                checksum_type: None,
                restore_status: None,
            })
            .collect();

        let output = ListObjectsOutput {
            name: Some(BucketName::from(bucket)),
            prefix: req.input.prefix,
            marker: req.input.marker,
            max_keys: Some(max_keys as i32),
            is_truncated: Some(is_truncated),
            contents: if contents.is_empty() {
                None
            } else {
                Some(contents)
            },
            next_marker: if is_truncated { last_key } else { None },
            delimiter: req.input.delimiter,
            common_prefixes: None,
            encoding_type: None,
            request_charged: None,
        };
        Ok(S3Response::new(output))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let bucket = req.input.bucket.as_str();
        let prefix = req.input.prefix.as_deref();
        let max_keys = req.input.max_keys.unwrap_or(1000).max(0) as u32;
        let continuation = req.input.continuation_token.as_deref();
        info!(bucket, ?prefix, "ListObjectsV2");

        if !bucket_exists(&self.db, bucket)
            .map_err(|e| s3_error!(InternalError, "metadata error: {e}"))?
        {
            return Err(s3_error!(NoSuchBucket, "bucket {bucket} does not exist"));
        }

        let (objects, is_truncated) = object_list(&self.db, bucket, prefix, max_keys, continuation)
            .map_err(|e| s3_error!(InternalError, "metadata error: {e}"))?;

        let last_key = objects.last().map(|o| o.key.clone());
        let key_count = objects.len() as i32;

        let contents: Vec<Object> = objects
            .iter()
            .map(|o| Object {
                key: Some(ObjectKey::from(o.key.as_str())),
                e_tag: Some(wrap_etag(&o.etag)),
                size: Some(o.size as i64),
                last_modified: Some(to_timestamp(o.last_modified)),
                owner: None,
                storage_class: None,
                checksum_algorithm: None,
                checksum_type: None,
                restore_status: None,
            })
            .collect();

        let output = ListObjectsV2Output {
            name: Some(BucketName::from(bucket)),
            prefix: req.input.prefix,
            max_keys: Some(max_keys as i32),
            key_count: Some(key_count),
            is_truncated: Some(is_truncated),
            contents: if contents.is_empty() {
                None
            } else {
                Some(contents)
            },
            next_continuation_token: if is_truncated { last_key } else { None },
            continuation_token: req.input.continuation_token,
            delimiter: req.input.delimiter,
            common_prefixes: None,
            encoding_type: None,
            start_after: req.input.start_after,
            request_charged: None,
        };
        Ok(S3Response::new(output))
    }
}
