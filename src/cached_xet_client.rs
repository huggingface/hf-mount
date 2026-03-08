use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use cas_client::adaptive_concurrency::ConnectionPermit;
use cas_client::{Client, ProgressCallback, URLProvider};
use xorb_object::SerializedXorbObject;
use cas_types::{
    BatchQueryReconstructionResponse, FileRange, HexMerkleHash, QueryReconstructionResponse, XorbReconstructionTerm,
};
use mdb_shard::file_structs::MDBFileInfo;
use merklehash::MerkleHash;

type Result<T> = std::result::Result<T, cas_client::CasClientError>;

const MAX_CACHE_ENTRIES: usize = 1024;
/// Presigned URLs returned by the CAS server expire after 1 hour.
/// Evict cache entries just before expiry to avoid serving stale URLs.
const CACHE_TTL: Duration = Duration::from_secs(59 * 60);

struct CacheEntry {
    response: QueryReconstructionResponse,
    inserted_at: Instant,
}

impl CacheEntry {
    fn new(response: QueryReconstructionResponse) -> Self {
        Self {
            response,
            inserted_at: Instant::now(),
        }
    }

    fn is_valid(&self, ttl: Duration) -> bool {
        self.inserted_at.elapsed() < ttl
    }
}

// TODO: move this into xet-core (cas_client or file_reconstruction) so all consumers benefit.
pub struct CachedXetClient {
    inner: Arc<dyn Client>,
    /// Only full-file reconstruction plans are cached (key = file hash, no range).
    /// Range responses are derived on the fly via `derive_range_response`.
    cache: Mutex<HashMap<MerkleHash, CacheEntry>>,
    ttl: Duration,
}

impl CachedXetClient {
    pub fn new(inner: Arc<dyn Client>) -> Arc<Self> {
        Self::new_with_ttl(inner, CACHE_TTL)
    }

    fn new_with_ttl(inner: Arc<dyn Client>, ttl: Duration) -> Arc<Self> {
        Arc::new(Self {
            inner,
            cache: Mutex::new(HashMap::new()),
            ttl,
        })
    }
}

/// Derive a range-scoped `QueryReconstructionResponse` from a cached full-file response.
///
/// The full-file response lists all terms in file order with their unpacked byte lengths.
/// We walk the terms, track cumulative byte offsets, and keep only terms that overlap
/// `[range.start, range.end)`. The `offset_into_first_range` is the byte offset within
/// the first overlapping term at which the requested range starts.
///
/// # Limitation: over-fetch vs CAS server trimming (P2)
///
/// When the CAS server handles a range query directly, it trims each term's `ChunkRange`
/// at chunk granularity: it advances `range.start` past whole chunks whose data falls
/// entirely before the requested offset, and cuts `range.end` after the last needed chunk.
/// This produces tight presigned S3 URLs covering only the necessary compressed bytes.
///
/// This function cannot do the same trimming because `XorbReconstructionTerm` only carries
/// `unpacked_length` for the whole term, not per-chunk sizes. As a result, derived responses
/// keep the full `ChunkRange` of each overlapping term — the caller downloads and decompresses
/// up to one extra chunk at the start and one at the end per term.
///
/// In practice the over-fetch is small: at most ~2 × chunk_size (~128 KB) per term per
/// range query. For typical safetensors workloads this is negligible (<1% of total data).
///
/// TODO: fix P2 — add `chunk_uncompressed_sizes: Vec<u32>` (and `chunk_compressed_sizes`)
/// to `XorbReconstructionTerm` in xet-core/xetcas so that chunk-level trimming can be
/// replicated client-side, reducing over-fetch to zero.
fn derive_range_response(full: &QueryReconstructionResponse, range: FileRange) -> QueryReconstructionResponse {
    let mut cur_offset: u64 = 0;
    let mut result_terms: Vec<XorbReconstructionTerm> = Vec::new();
    let mut offset_into_first: u64 = 0;
    let mut needed_hashes: std::collections::HashSet<HexMerkleHash> = std::collections::HashSet::new();

    for term in &full.terms {
        let term_start = cur_offset;
        let term_end = cur_offset + term.unpacked_length as u64;

        if term_end <= range.start {
            cur_offset = term_end;
            continue;
        }
        if term_start >= range.end {
            break;
        }

        if result_terms.is_empty() {
            offset_into_first = range.start.saturating_sub(term_start);
        }
        needed_hashes.insert(term.hash);
        result_terms.push(term.clone());
        cur_offset = term_end;
    }

    let fetch_info = full
        .fetch_info
        .iter()
        .filter(|(k, _)| needed_hashes.contains(*k))
        .map(|(k, v)| (*k, v.clone()))
        .collect();

    QueryReconstructionResponse {
        offset_into_first_range: offset_into_first,
        terms: result_terms,
        fetch_info,
    }
}

#[async_trait::async_trait]
impl Client for CachedXetClient {
    async fn get_reconstruction(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponse>> {
        // Clone the full-file plan under a brief lock, then derive outside the lock so
        // concurrent pread() calls from N threads don't serialize on this mutex.
        let cached_full = {
            let mut cache = self.cache.lock().expect("cache poisoned");
            match cache.get(file_id) {
                Some(entry) if entry.is_valid(self.ttl) => Some(entry.response.clone()),
                Some(_) => {
                    // Expired — evict so the next caller fetches fresh URLs from CAS.
                    cache.remove(file_id);
                    None
                }
                None => None,
            }
        }; // ← lock released here

        if let Some(full) = cached_full {
            if let Some(range) = bytes_range {
                let resp = derive_range_response(&full, range);
                // Mirror the CAS server's EOF contract:
                // - Non-empty file, range past EOF → None
                // - Empty file, range.start == 0 → Some(empty_terms)  (only valid range on empty file)
                // - Empty file, range.start > 0  → None               (past EOF on empty file)
                if resp.terms.is_empty() && (!full.terms.is_empty() || range.start > 0) {
                    return Ok(None);
                }
                tracing::debug!(
                    "recon: DERV file={:.8} range={:?} terms={}",
                    file_id,
                    bytes_range,
                    resp.terms.len()
                );
                return Ok(Some(resp));
            } else {
                tracing::debug!("recon: HIT  file={:.8} range=None terms={}", file_id, full.terms.len());
                return Ok(Some(full));
            }
        }

        tracing::debug!("recon: CAS  file={:.8} range={:?}", file_id, bytes_range);
        let result = self.inner.get_reconstruction(file_id, bytes_range).await?;

        // Only cache full-file plans. Range responses are never stored: they are always
        // derived from the full-file plan, and the full-file plan is warmed at open() time.
        if bytes_range.is_none()
            && let Some(response) = &result
        {
            let mut cache = self.cache.lock().expect("cache poisoned");
            if cache.len() >= MAX_CACHE_ENTRIES {
                cache.clear();
            }
            cache.insert(*file_id, CacheEntry::new(response.clone()));
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
        serialized_cas_object: SerializedXorbObject,
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
    #[allow(clippy::enum_variant_names)]
    enum MockMode {
        ReturnSome,
        ReturnNone,
        ReturnErr,
    }

    struct MockClient {
        mode: MockMode,
        calls: Mutex<HashMap<(MerkleHash, Option<FileRange>), usize>>,
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

        fn call_count(&self, key: (MerkleHash, Option<FileRange>)) -> usize {
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
            _serialized_cas_object: SerializedXorbObject,
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
    async fn range_derived_from_full_file_plan() {
        let inner_impl = Arc::new(MockClient::new(MockMode::ReturnSome));
        let inner: Arc<dyn Client> = inner_impl.clone();
        let client = CachedXetClient::new(inner);
        let key = hash_for(4);
        let r = Some(FileRange::new(10, 20));

        // Fetch full-file plan first (caches it).
        client.get_reconstruction(&key, None).await.unwrap();
        client.get_reconstruction(&key, None).await.unwrap();

        // Range queries are derived from the cached full-file plan — never hit backend.
        client.get_reconstruction(&key, r).await.unwrap();
        client
            .get_reconstruction(&key, Some(FileRange::new(20, 30)))
            .await
            .unwrap();

        assert_eq!(inner_impl.call_count((key, None)), 1);
        assert_eq!(inner_impl.call_count((key, r)), 0); // derived, never hit backend
        assert_eq!(inner_impl.total_calls(), 1);
    }

    #[tokio::test]
    async fn range_query_hits_backend_when_no_full_file_cached() {
        let inner_impl = Arc::new(MockClient::new(MockMode::ReturnSome));
        let inner: Arc<dyn Client> = inner_impl.clone();
        let client = CachedXetClient::new(inner);
        let key = hash_for(40);
        let r = Some(FileRange::new(10, 20));

        // Range query with no full-file plan cached → hits backend.
        // The response is NOT cached (range responses are never stored).
        client.get_reconstruction(&key, r).await.unwrap();
        client.get_reconstruction(&key, r).await.unwrap();

        assert_eq!(inner_impl.call_count((key, r)), 2); // no cache, each hits backend
        assert_eq!(inner_impl.total_calls(), 2);
    }

    #[tokio::test]
    async fn expired_entries_are_evicted_and_refetched() {
        let inner_impl = Arc::new(MockClient::new(MockMode::ReturnSome));
        let inner: Arc<dyn Client> = inner_impl.clone();
        let client = CachedXetClient::new_with_ttl(inner, Duration::ZERO);
        let key = hash_for(50);

        // With TTL=0 every entry is immediately expired — each call hits the backend.
        client.get_reconstruction(&key, None).await.unwrap();
        client.get_reconstruction(&key, None).await.unwrap();

        assert_eq!(inner_impl.call_count((key, None)), 2);
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
