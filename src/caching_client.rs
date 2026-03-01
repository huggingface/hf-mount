use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use cas_client::adaptive_concurrency::ConnectionPermit;
use cas_client::{Client, ProgressCallback, URLProvider};
use cas_object::SerializedCasObject;
use cas_types::{BatchQueryReconstructionResponse, FileRange, QueryReconstructionResponse};
use mdb_shard::file_structs::MDBFileInfo;
use merklehash::MerkleHash;
use tracing::debug;

type Result<T> = std::result::Result<T, cas_client::CasClientError>;

const CACHE_TTL: Duration = Duration::from_secs(120);
const MAX_CACHE_ENTRIES: usize = 1024;

type CacheKey = (MerkleHash, Option<FileRange>);
type CacheEntry = (Instant, QueryReconstructionResponse);

pub struct CachingClient {
    inner: Arc<dyn Client>,
    cache: Mutex<HashMap<CacheKey, CacheEntry>>,
}

impl CachingClient {
    pub fn new(inner: Arc<dyn Client>) -> Self {
        Self {
            inner,
            cache: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait::async_trait]
impl Client for CachingClient {
    async fn get_reconstruction(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponse>> {
        let key = (*file_id, bytes_range);

        // Check cache
        {
            let cache = self.cache.lock().unwrap();
            if let Some((inserted_at, response)) = cache.get(&key)
                && inserted_at.elapsed() < CACHE_TTL
            {
                debug!("reconstruction cache hit for {file_id}");
                return Ok(Some(response.clone()));
            }
        }

        // Cache miss or expired — call inner
        let result = self.inner.get_reconstruction(file_id, bytes_range).await?;

        if let Some(ref response) = result {
            let mut cache = self.cache.lock().unwrap();
            // Evict expired entries if at capacity
            if cache.len() >= MAX_CACHE_ENTRIES {
                cache.retain(|_, (inserted_at, _)| inserted_at.elapsed() < CACHE_TTL);
            }
            // If still at capacity after eviction, drop oldest entry
            if cache.len() >= MAX_CACHE_ENTRIES
                && let Some(oldest_key) = cache.iter().min_by_key(|(_, (ts, _))| *ts).map(|(k, _)| *k)
            {
                cache.remove(&oldest_key);
            }
            cache.insert(key, (Instant::now(), response.clone()));
        }

        Ok(result)
    }

    async fn get_file_reconstruction_info(
        &self,
        file_hash: &MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
        self.inner.get_file_reconstruction_info(file_hash).await
    }

    async fn batch_get_reconstruction(&self, file_ids: &[MerkleHash]) -> Result<BatchQueryReconstructionResponse> {
        self.inner.batch_get_reconstruction(file_ids).await
    }

    async fn acquire_download_permit(&self) -> Result<ConnectionPermit> {
        self.inner.acquire_download_permit().await
    }

    async fn get_file_term_data(
        &self,
        url_info: Box<dyn URLProvider>,
        download_permit: ConnectionPermit,
        progress_callback: Option<ProgressCallback>,
        uncompressed_size_if_known: Option<usize>,
    ) -> Result<(Bytes, Vec<u32>)> {
        self.inner
            .get_file_term_data(url_info, download_permit, progress_callback, uncompressed_size_if_known)
            .await
    }

    async fn query_for_global_dedup_shard(&self, prefix: &str, chunk_hash: &MerkleHash) -> Result<Option<Bytes>> {
        self.inner.query_for_global_dedup_shard(prefix, chunk_hash).await
    }

    async fn acquire_upload_permit(&self) -> Result<ConnectionPermit> {
        self.inner.acquire_upload_permit().await
    }

    async fn upload_shard(&self, shard_data: Bytes, upload_permit: ConnectionPermit) -> Result<bool> {
        self.inner.upload_shard(shard_data, upload_permit).await
    }

    async fn upload_xorb(
        &self,
        prefix: &str,
        serialized_cas_object: SerializedCasObject,
        progress_callback: Option<ProgressCallback>,
        upload_permit: ConnectionPermit,
    ) -> Result<u64> {
        self.inner
            .upload_xorb(prefix, serialized_cas_object, progress_callback, upload_permit)
            .await
    }
}
