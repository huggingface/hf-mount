use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use cas_client::adaptive_concurrency::ConnectionPermit;
use cas_client::{Client, ProgressCallback, URLProvider};
use cas_object::SerializedCasObject;
use cas_types::{BatchQueryReconstructionResponse, FileRange, QueryReconstructionResponse};
use mdb_shard::file_structs::MDBFileInfo;
use merklehash::MerkleHash;
use tracing::debug;

type Result<T> = std::result::Result<T, cas_client::CasClientError>;

const MAX_CACHE_ENTRIES: usize = 1024;

type CacheKey = (MerkleHash, Option<FileRange>);

// TODO: move this into xet-core (cas_client or file_reconstruction) so all consumers benefit.
pub struct CachedXetClient {
    inner: Arc<dyn Client>,
    cache: Mutex<HashMap<CacheKey, QueryReconstructionResponse>>,
}

impl CachedXetClient {
    pub fn new(inner: Arc<dyn Client>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            cache: Mutex::new(HashMap::new()),
        })
    }
}

#[async_trait::async_trait]
impl Client for CachedXetClient {
    async fn get_reconstruction(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponse>> {
        let key = (*file_id, bytes_range);

        {
            let cache = self.cache.lock().expect("cache poisoned");
            if let Some(response) = cache.get(&key) {
                debug!("reconstruction cache hit for {file_id}");
                return Ok(Some(response.clone()));
            }
        }

        let result = self.inner.get_reconstruction(file_id, bytes_range).await?;

        if let Some(ref response) = result {
            let mut cache = self.cache.lock().expect("cache poisoned");
            if cache.len() >= MAX_CACHE_ENTRIES {
                cache.clear();
            }
            cache.insert(key, response.clone());
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use cas_client::CasClientError;
    use cas_types::FileRange;
    use merklehash::compute_data_hash;
    use tokio::task::JoinSet;

    #[derive(Clone, Copy)]
    enum MockMode {
        ReturnSome,
        ReturnNone,
        ReturnErr,
    }

    struct MockClient {
        mode: MockMode,
        calls: Mutex<HashMap<CacheKey, usize>>,
        total_calls: AtomicUsize,
        barrier: Option<Arc<tokio::sync::Barrier>>,
    }

    impl MockClient {
        fn new(mode: MockMode) -> Self {
            Self {
                mode,
                calls: Mutex::new(HashMap::new()),
                total_calls: AtomicUsize::new(0),
                barrier: None,
            }
        }

        fn with_barrier(mode: MockMode, parties: usize) -> Self {
            Self {
                mode,
                calls: Mutex::new(HashMap::new()),
                total_calls: AtomicUsize::new(0),
                barrier: Some(Arc::new(tokio::sync::Barrier::new(parties))),
            }
        }

        fn call_count(&self, key: CacheKey) -> usize {
            self.calls
                .lock()
                .expect("calls lock poisoned")
                .get(&key)
                .copied()
                .unwrap_or(0)
        }

        fn total_calls(&self) -> usize {
            self.total_calls.load(Ordering::Relaxed)
        }
    }

    #[async_trait::async_trait]
    impl Client for MockClient {
        async fn get_reconstruction(
            &self,
            file_id: &MerkleHash,
            bytes_range: Option<FileRange>,
        ) -> Result<Option<QueryReconstructionResponse>> {
            let key = (*file_id, bytes_range);
            {
                let mut calls = self.calls.lock().expect("calls lock poisoned");
                *calls.entry(key).or_insert(0) += 1;
            }
            self.total_calls.fetch_add(1, Ordering::Relaxed);

            if let Some(barrier) = &self.barrier {
                barrier.wait().await;
            }

            match self.mode {
                MockMode::ReturnSome => Ok(Some(QueryReconstructionResponse {
                    offset_into_first_range: bytes_range.map_or(0, |r| r.start),
                    terms: Vec::new(),
                    fetch_info: HashMap::new(),
                })),
                MockMode::ReturnNone => Ok(None),
                MockMode::ReturnErr => Err(CasClientError::Other("boom".to_string())),
            }
        }

        async fn get_file_reconstruction_info(
            &self,
            _file_hash: &MerkleHash,
        ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
            unimplemented!("not needed in these tests")
        }

        async fn batch_get_reconstruction(&self, _file_ids: &[MerkleHash]) -> Result<BatchQueryReconstructionResponse> {
            Ok(BatchQueryReconstructionResponse {
                files: HashMap::new(),
                fetch_info: HashMap::new(),
            })
        }

        async fn acquire_download_permit(&self) -> Result<ConnectionPermit> {
            unimplemented!("not needed in these tests")
        }

        async fn get_file_term_data(
            &self,
            _url_info: Box<dyn URLProvider>,
            _download_permit: ConnectionPermit,
            _progress_callback: Option<ProgressCallback>,
            _uncompressed_size_if_known: Option<usize>,
        ) -> Result<(Bytes, Vec<u32>)> {
            unimplemented!("not needed in these tests")
        }

        async fn query_for_global_dedup_shard(&self, _prefix: &str, _chunk_hash: &MerkleHash) -> Result<Option<Bytes>> {
            unimplemented!("not needed in these tests")
        }

        async fn acquire_upload_permit(&self) -> Result<ConnectionPermit> {
            unimplemented!("not needed in these tests")
        }

        async fn upload_shard(&self, _shard_data: Bytes, _upload_permit: ConnectionPermit) -> Result<bool> {
            unimplemented!("not needed in these tests")
        }

        async fn upload_xorb(
            &self,
            _prefix: &str,
            _serialized_cas_object: SerializedCasObject,
            _progress_callback: Option<ProgressCallback>,
            _upload_permit: ConnectionPermit,
        ) -> Result<u64> {
            unimplemented!("not needed in these tests")
        }
    }

    fn hash_for(i: usize) -> MerkleHash {
        compute_data_hash(&i.to_le_bytes())
    }

    #[tokio::test]
    async fn caches_successful_response_for_same_key() {
        let inner_impl = Arc::new(MockClient::new(MockMode::ReturnSome));
        let inner: Arc<dyn Client> = inner_impl.clone();
        let client = CachedXetClient::new(inner);
        let key = hash_for(1);

        let r1 = client.get_reconstruction(&key, None).await.unwrap().unwrap();
        let r2 = client.get_reconstruction(&key, None).await.unwrap().unwrap();

        assert_eq!(r1.offset_into_first_range, 0);
        assert_eq!(r2.offset_into_first_range, 0);
        assert_eq!(inner_impl.call_count((key, None)), 1);
        assert_eq!(inner_impl.total_calls(), 1);
    }

    #[tokio::test]
    async fn none_responses_are_not_cached() {
        let inner_impl = Arc::new(MockClient::new(MockMode::ReturnNone));
        let inner: Arc<dyn Client> = inner_impl.clone();
        let client = CachedXetClient::new(inner);
        let key = hash_for(2);

        assert!(client.get_reconstruction(&key, None).await.unwrap().is_none());
        assert!(client.get_reconstruction(&key, None).await.unwrap().is_none());
        assert_eq!(inner_impl.call_count((key, None)), 2);
    }

    #[tokio::test]
    async fn errors_are_not_cached() {
        let inner_impl = Arc::new(MockClient::new(MockMode::ReturnErr));
        let inner: Arc<dyn Client> = inner_impl.clone();
        let client = CachedXetClient::new(inner);
        let key = hash_for(3);

        assert!(client.get_reconstruction(&key, None).await.is_err());
        assert!(client.get_reconstruction(&key, None).await.is_err());
        assert_eq!(inner_impl.call_count((key, None)), 2);
    }

    #[tokio::test]
    async fn cache_key_includes_bytes_range() {
        let inner_impl = Arc::new(MockClient::new(MockMode::ReturnSome));
        let inner: Arc<dyn Client> = inner_impl.clone();
        let client = CachedXetClient::new(inner);
        let key = hash_for(4);
        let r = Some(FileRange::new(10, 20));

        client.get_reconstruction(&key, None).await.unwrap();
        client.get_reconstruction(&key, None).await.unwrap();
        client.get_reconstruction(&key, r).await.unwrap();
        client.get_reconstruction(&key, r).await.unwrap();

        assert_eq!(inner_impl.call_count((key, None)), 1);
        assert_eq!(inner_impl.call_count((key, r)), 1);
        assert_eq!(inner_impl.total_calls(), 2);
    }

    #[tokio::test]
    async fn cache_clears_when_capacity_is_reached() {
        let inner_impl = Arc::new(MockClient::new(MockMode::ReturnSome));
        let inner: Arc<dyn Client> = inner_impl.clone();
        let client = CachedXetClient::new(inner);

        for i in 0..MAX_CACHE_ENTRIES {
            let key = hash_for(i);
            client.get_reconstruction(&key, None).await.unwrap();
        }

        let overflow = hash_for(MAX_CACHE_ENTRIES);
        client.get_reconstruction(&overflow, None).await.unwrap();

        let first = hash_for(0);
        client.get_reconstruction(&first, None).await.unwrap();

        assert_eq!(inner_impl.call_count((first, None)), 2);
        assert_eq!(inner_impl.call_count((overflow, None)), 1);
    }

    #[tokio::test]
    async fn concurrent_same_key_requests_do_not_panic_under_race() {
        let contenders = 8usize;
        let inner_impl = Arc::new(MockClient::with_barrier(MockMode::ReturnSome, contenders));
        let inner: Arc<dyn Client> = inner_impl.clone();
        let client = CachedXetClient::new(inner);
        let key = hash_for(99);

        let mut set = JoinSet::new();
        for _ in 0..contenders {
            let c = client.clone();
            set.spawn(async move { c.get_reconstruction(&key, None).await });
        }

        while let Some(result) = set.join_next().await {
            let resp = result.expect("task panicked").expect("request failed");
            assert!(resp.is_some());
        }

        // This test asserts race-safety (no panics/deadlocks), not single-flight behavior.
        assert_eq!(inner_impl.call_count((key, None)), contenders);
    }
}
