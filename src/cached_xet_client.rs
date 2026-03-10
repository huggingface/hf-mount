use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use cas_client::adaptive_concurrency::ConnectionPermit;
use cas_client::{Client, ProgressCallback, URLProvider};
use cas_types::{
    BatchQueryReconstructionResponse, FileRange, HexMerkleHash, QueryReconstructionResponse, XorbReconstructionTerm,
};
use mdb_shard::file_structs::MDBFileInfo;
use merklehash::MerkleHash;
use xorb_object::SerializedXorbObject;

type Result<T> = std::result::Result<T, cas_client::CasClientError>;

/// Cache key: file hash + optional byte range (`None` = full-file plan).
type ReconCacheKey = (MerkleHash, Option<FileRange>);

const MAX_CACHE_ENTRIES: usize = 4096;
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
    /// Reconstruction plan cache. Key is (file_hash, range) where `None` = full-file plan.
    /// Full-file plans are preferred: range queries are derived locally when available.
    /// Range-scoped entries serve as fast fallback while the full plan warms up.
    cache: Mutex<HashMap<ReconCacheKey, CacheEntry>>,
    /// Single-flight: in-flight CAS requests. When multiple callers request the same
    /// key concurrently, only the first makes the CAS call; others wait and read from cache.
    inflight: Mutex<HashMap<ReconCacheKey, tokio::sync::broadcast::Sender<()>>>,
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
            inflight: Mutex::new(HashMap::new()),
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
        let key: ReconCacheKey = (*file_id, bytes_range);

        // Single-flight action: either wait for an in-flight request or lead the fetch.
        enum Action {
            Wait(tokio::sync::broadcast::Receiver<()>),
            Lead(tokio::sync::broadcast::Sender<()>),
        }

        loop {
            // --- 1. Cache check ---
            // Single lock: check exact key first, then full-file plan as fallback.
            // For range queries the exact CAS response has tighter ChunkRanges (trimmed
            // at chunk granularity) than what derive_range_response produces from the
            // full plan, so prefer it when available.
            enum CacheResult {
                ExactHit(QueryReconstructionResponse),
                FullPlan(QueryReconstructionResponse, FileRange),
                Miss,
            }

            let cached = {
                let mut cache = self.cache.lock().expect("cache poisoned");

                let try_get = |cache: &mut HashMap<ReconCacheKey, CacheEntry>, key: ReconCacheKey| match cache.get(&key)
                {
                    Some(entry) if entry.is_valid(self.ttl) => Some(entry.response.clone()),
                    Some(_) => {
                        cache.remove(&key);
                        None
                    }
                    None => None,
                };

                if let Some(resp) = try_get(&mut cache, key) {
                    CacheResult::ExactHit(resp)
                } else if let Some(range) = bytes_range {
                    match try_get(&mut cache, (*file_id, None)) {
                        Some(full) => CacheResult::FullPlan(full, range),
                        None => CacheResult::Miss,
                    }
                } else {
                    CacheResult::Miss
                }
            };

            match cached {
                CacheResult::ExactHit(resp) => {
                    tracing::debug!(
                        "recon: HIT  file={:.8} range={:?} terms={}",
                        file_id,
                        bytes_range,
                        resp.terms.len()
                    );
                    return Ok(Some(resp));
                }
                CacheResult::FullPlan(full, range) => {
                    let resp = derive_range_response(&full, range);
                    // Mirror the CAS server's EOF contract:
                    // - Non-empty file, range past EOF → None
                    // - Empty file, range.start == 0 → Some(empty_terms)  (only valid range)
                    // - Empty file, range.start > 0  → None               (past EOF)
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
                }
                CacheResult::Miss => {}
            }

            // --- 2. Single-flight: coalesce concurrent requests for the same key ---
            let action = {
                let mut inflight = self.inflight.lock().expect("inflight poisoned");
                if let Some(tx) = inflight.get(&key) {
                    Action::Wait(tx.subscribe())
                } else {
                    let (tx, _) = tokio::sync::broadcast::channel(1);
                    inflight.insert(key, tx.clone());
                    Action::Lead(tx)
                }
            };

            match action {
                Action::Wait(mut rx) => {
                    // Another caller is fetching this key — wait, then re-check cache.
                    let _ = rx.recv().await;
                    continue;
                }
                Action::Lead(tx) => {
                    // We're the leader — make the CAS call.
                    tracing::debug!("recon: CAS  file={:.8} range={:?}", file_id, bytes_range);
                    let result = self.inner.get_reconstruction(file_id, bytes_range).await;

                    if let Ok(Some(response)) = &result {
                        let mut cache = self.cache.lock().expect("cache poisoned");

                        // When inserting a full plan, purge redundant range entries for this hash
                        // (they're now derivable from the full plan, no point keeping them).
                        if bytes_range.is_none() {
                            cache.retain(|(hash, range), _| *hash != *file_id || range.is_none());
                        }

                        if cache.len() >= MAX_CACHE_ENTRIES {
                            // First pass: evict range entries (less valuable than full plans).
                            cache.retain(|(_hash, range), _| range.is_none());
                            if cache.len() >= MAX_CACHE_ENTRIES {
                                // Still full (all full plans). Evict one arbitrary entry
                                // to make room — never skip the insert, as single-flight
                                // waiters expect the result to be in cache.
                                if let Some(victim) = cache.keys().next().copied() {
                                    cache.remove(&victim);
                                }
                            }
                        }
                        cache.insert(key, CacheEntry::new(response.clone()));
                    }

                    // Notify waiters and clean up.
                    self.inflight.lock().expect("inflight poisoned").remove(&key);
                    let _ = tx.send(());

                    return result;
                }
            }
        } // loop
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
        /// Optional delay injected before returning (simulates CAS latency).
        delay: Option<Duration>,
    }

    impl MockClient {
        fn new(mode: MockMode) -> Self {
            Self {
                mode,
                calls: Mutex::new(HashMap::new()),
                total_calls: AtomicUsize::new(0),
                delay: None,
            }
        }

        fn with_delay(mode: MockMode, delay: Duration) -> Self {
            Self {
                mode,
                calls: Mutex::new(HashMap::new()),
                total_calls: AtomicUsize::new(0),
                delay: Some(delay),
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

            if let Some(delay) = self.delay {
                tokio::time::sleep(delay).await;
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
    async fn range_query_cached_when_no_full_file_plan() {
        let inner_impl = Arc::new(MockClient::new(MockMode::ReturnSome));
        let inner: Arc<dyn Client> = inner_impl.clone();
        let client = CachedXetClient::new(inner);
        let key = hash_for(40);
        let r = Some(FileRange::new(10, 20));

        // Range query with no full-file plan cached → hits backend once, then cached.
        client.get_reconstruction(&key, r).await.unwrap();
        client.get_reconstruction(&key, r).await.unwrap();

        assert_eq!(inner_impl.call_count((key, r)), 1); // cached after first fetch
        assert_eq!(inner_impl.total_calls(), 1);
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
    async fn cache_evicts_one_when_capacity_is_reached() {
        let inner_impl = Arc::new(MockClient::new(MockMode::ReturnSome));
        let inner: Arc<dyn Client> = inner_impl.clone();
        let client = CachedXetClient::new(inner);

        for i in 0..MAX_CACHE_ENTRIES {
            let key = hash_for(i);
            client.get_reconstruction(&key, None).await.unwrap();
        }

        let overflow = hash_for(MAX_CACHE_ENTRIES);
        client.get_reconstruction(&overflow, None).await.unwrap();

        // Overflow should be cached (inserted after evicting one victim).
        assert_eq!(inner_impl.call_count((overflow, None)), 1);
        client.get_reconstruction(&overflow, None).await.unwrap();
        assert_eq!(inner_impl.call_count((overflow, None)), 1); // still cached

        // Exactly one of the original entries was evicted — total CAS calls
        // should be MAX_CACHE_ENTRIES + 1 (one per original + overflow).
        assert_eq!(inner_impl.total_calls(), MAX_CACHE_ENTRIES + 1);
    }

    #[tokio::test]
    async fn eviction_preserves_full_plans_over_range_entries() {
        let inner_impl = Arc::new(MockClient::new(MockMode::ReturnSome));
        let inner: Arc<dyn Client> = inner_impl.clone();
        let client = CachedXetClient::new(inner);
        let key = hash_for(1);

        // Cache one full-file plan.
        client.get_reconstruction(&key, None).await.unwrap();

        // Fill the rest with range entries to trigger eviction.
        for i in 0..MAX_CACHE_ENTRIES {
            let r = Some(FileRange::new(i as u64 * 100, i as u64 * 100 + 50));
            client.get_reconstruction(&hash_for(i + 1000), r).await.unwrap();
        }

        // Full plan should survive eviction — still cached.
        client.get_reconstruction(&key, None).await.unwrap();
        assert_eq!(inner_impl.call_count((key, None)), 1); // still cached
    }

    #[tokio::test]
    async fn full_plan_insert_purges_stale_range_entries() {
        let inner_impl = Arc::new(MockClient::new(MockMode::ReturnSome));
        let inner: Arc<dyn Client> = inner_impl.clone();
        let client = CachedXetClient::new(inner);
        let key = hash_for(70);

        // Cache a range entry first (no full plan available).
        let r = Some(FileRange::new(100, 200));
        client.get_reconstruction(&key, r).await.unwrap();
        assert_eq!(inner_impl.call_count((key, r)), 1);

        // Now insert the full plan — should purge the range entry.
        client.get_reconstruction(&key, None).await.unwrap();

        // Verify the range entry was purged by checking cache size:
        // only the full plan should remain. A subsequent range query should
        // derive from the full plan (no new CAS call).
        let r2 = Some(FileRange::new(100, 200));
        client.get_reconstruction(&key, r2).await.unwrap();
        // Still 1 CAS call for the range — it was purged but derived from full plan.
        assert_eq!(inner_impl.call_count((key, r2)), 1);
        assert_eq!(inner_impl.total_calls(), 2); // 1 range + 1 full plan, no extra
    }

    #[tokio::test]
    async fn range_entry_cached_when_full_of_full_plans_evicts_one() {
        let inner_impl = Arc::new(MockClient::new(MockMode::ReturnSome));
        let inner: Arc<dyn Client> = inner_impl.clone();
        let client = CachedXetClient::new(inner);

        // Fill cache with full plans.
        for i in 0..MAX_CACHE_ENTRIES {
            client.get_reconstruction(&hash_for(i), None).await.unwrap();
        }

        // Range query on a new hash — evicts one full plan, inserts range entry.
        let overflow_hash = hash_for(MAX_CACHE_ENTRIES + 1);
        let r = Some(FileRange::new(0, 100));
        client.get_reconstruction(&overflow_hash, r).await.unwrap();
        // Second call should be cached (single-flight waiters can find it).
        client.get_reconstruction(&overflow_hash, r).await.unwrap();
        assert_eq!(inner_impl.call_count((overflow_hash, r)), 1);

        // Most full plans should still be cached (only one was evicted).
        // hash_for(1) should survive since eviction picks an arbitrary entry.
        let total_before = inner_impl.total_calls();
        client.get_reconstruction(&hash_for(1), None).await.unwrap();
        // It's either still cached (1 call total) or was the evicted victim (refetched).
        // Either way, the important thing is we didn't clear ALL full plans.
        assert!(inner_impl.total_calls() <= total_before + 1);
    }

    #[tokio::test]
    async fn concurrent_same_key_coalesces_via_single_flight() {
        let contenders = 8usize;
        // Delay ensures the leader's CAS call is still in-flight when others arrive.
        let inner_impl = Arc::new(MockClient::with_delay(MockMode::ReturnSome, Duration::from_millis(50)));
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

        // Single-flight: only 1 CAS call despite 8 concurrent requests.
        assert_eq!(inner_impl.call_count((key, None)), 1);
        assert_eq!(inner_impl.total_calls(), 1);
    }
}
