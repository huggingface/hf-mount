//! Whole-file content cache, addressed by xet hash.
//!
//! Orthogonal to xet-core's chunk_cache: when a `FileCache` is attached, the
//! VFS read path serves opens from the local copy as soon as it is fully
//! populated, sidestepping xorb-range fragmentation entirely. The chunk_cache
//! is therefore disabled in `setup` whenever a `FileCache` is built.
//!
//! Layout (rooted at `<cache_dir>/files/`):
//!   `aa/aabbcc...`   final file, named by full xet hash, sharded by 2-hex prefix
//!   `.tmp/<rand>`    in-flight downloads; renamed atomically into place on success
//!
//! Single-flight: concurrent populates for the same hash collapse to one
//! download via a per-hash broadcast channel. Eviction is LRU by last-access,
//! tracked via a per-item monotonic counter so opens only need a read lock.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{RwLock, broadcast};
use tracing::{debug, info, warn};

use crate::error::{Error, Result};

const FILES_DIR: &str = "files";
const TMP_DIR: &str = ".tmp";

struct CacheItem {
    size: u64,
    /// Logical clock value at last access. Higher = more recent.
    last_access: AtomicU64,
}

struct State {
    items: HashMap<String, CacheItem>,
    total_bytes: u64,
}

pub struct FileCache {
    root: PathBuf,
    /// Per-process subdirectory under `<root>/.tmp/<pid>` for in-flight downloads.
    /// Isolated so concurrent mounts sharing `cache_dir` don't trample each other.
    tmp_dir: PathBuf,
    capacity: u64,
    state: RwLock<State>,
    inflight: Mutex<HashMap<String, broadcast::Sender<()>>>,
    /// Monotonic counter issued on every `try_open` / insert to order LRU
    /// without taking a write lock on the hot path.
    clock: AtomicU64,
}

impl FileCache {
    pub fn new(cache_dir: &Path, capacity: u64) -> Result<Arc<Self>> {
        let root = cache_dir.join(FILES_DIR);
        fs::create_dir_all(&root).map_err(|e| io_err(format!("mkdir {root:?}: {e}")))?;
        // Per-process tmp dir so concurrent mounts sharing the same cache_dir
        // don't clobber each other's in-flight downloads when one (re)starts.
        let tmp_dir = root.join(TMP_DIR).join(std::process::id().to_string());
        fs::create_dir_all(&tmp_dir).map_err(|e| io_err(format!("mkdir {tmp_dir:?}: {e}")))?;
        // Reap our own leftovers from a prior crashed run with the same pid (rare
        // but harmless). Other pids' tmp dirs are left alone.
        if let Ok(rd) = fs::read_dir(&tmp_dir) {
            for entry in rd.flatten() {
                let _ = fs::remove_file(entry.path());
            }
        }
        let state = Self::scan_existing(&root);
        info!(
            "file_cache: dir={:?} capacity={} discovered_items={} discovered_bytes={}",
            root,
            capacity,
            state.items.len(),
            state.total_bytes,
        );
        Ok(Arc::new(Self {
            root,
            tmp_dir,
            capacity,
            state: RwLock::new(state),
            inflight: Mutex::new(HashMap::new()),
            clock: AtomicU64::new(0),
        }))
    }

    fn scan_existing(root: &Path) -> State {
        let mut items = HashMap::new();
        let mut total_bytes = 0u64;
        let Ok(rd1) = fs::read_dir(root) else {
            return State { items, total_bytes };
        };
        for shard in rd1.flatten() {
            if shard.file_name() == TMP_DIR {
                continue;
            }
            let shard_path = shard.path();
            if !shard_path.is_dir() {
                continue;
            }
            let Ok(rd2) = fs::read_dir(&shard_path) else {
                continue;
            };
            for entry in rd2.flatten() {
                let name = entry.file_name();
                let Some(hash) = name.to_str() else { continue };
                let Ok(meta) = entry.metadata() else { continue };
                if !meta.is_file() {
                    continue;
                }
                let size = meta.len();
                items.insert(
                    hash.to_string(),
                    CacheItem {
                        size,
                        last_access: AtomicU64::new(0),
                    },
                );
                total_bytes += size;
            }
        }
        State { items, total_bytes }
    }

    fn item_path(&self, hash: &str) -> PathBuf {
        let prefix = if hash.len() >= 2 { &hash[..2] } else { "_" };
        self.root.join(prefix).join(hash)
    }

    fn next_tick(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) async fn contains(&self, hash: &str) -> bool {
        if hash.is_empty() {
            return false;
        }
        self.state.read().await.items.contains_key(hash)
    }

    /// Open the cached file if present, bumping LRU. Returns `None` on miss.
    /// On a stale entry (file vanished underneath us), the entry is evicted.
    /// Hits take only a read lock on `state`.
    pub(crate) async fn try_open(&self, hash: &str) -> Option<Arc<std::fs::File>> {
        if hash.is_empty() {
            return None;
        }
        {
            let state = self.state.read().await;
            let item = state.items.get(hash)?;
            item.last_access.store(self.next_tick(), Ordering::Relaxed);
        }
        match std::fs::File::open(self.item_path(hash)) {
            Ok(f) => Some(Arc::new(f)),
            Err(e) => {
                warn!("file_cache: open {hash} failed ({e}); forgetting entry");
                self.forget(hash).await;
                None
            }
        }
    }

    /// Drop in-memory entry and on-disk file (best-effort).
    pub(crate) async fn forget(&self, hash: &str) {
        let path = self.item_path(hash);
        let mut state = self.state.write().await;
        if let Some(item) = state.items.remove(hash) {
            state.total_bytes = state.total_bytes.saturating_sub(item.size);
        }
        drop(state);
        let _ = fs::remove_file(path);
    }

    /// Populate `hash` by running `fetch(dest)` to download into a tmp file
    /// inside this cache's `.tmp` directory. On success the tmp file is
    /// renamed into the canonical location and the entry is published.
    /// Concurrent populates for the same hash share a single download.
    /// `expected_size = None` skips the size check (use when the caller
    /// can't know the final size up front).
    pub(crate) async fn populate<F, Fut>(
        self: &Arc<Self>,
        hash: &str,
        expected_size: Option<u64>,
        fetch: F,
    ) -> Result<()>
    where
        F: FnOnce(PathBuf) -> Fut + Send,
        Fut: std::future::Future<Output = Result<()>> + Send,
    {
        if hash.is_empty() {
            return Ok(());
        }
        // Files that don't fit in the cache would be downloaded only to be
        // immediately evicted (taking warm entries down with them), so skip
        // them entirely. The post-download size check below is the safety net
        // when `expected_size` was unknown.
        if expected_size.is_some_and(|s| s > self.capacity) {
            return Ok(());
        }

        let (is_leader, mut rx) = {
            let mut inflight = self.inflight.lock().expect("file_cache.inflight poisoned");
            // Re-check under the inflight lock so a concurrent populate that
            // just finished doesn't get retried.
            if self.state.try_read().is_ok_and(|s| s.items.contains_key(hash)) {
                return Ok(());
            }
            if let Some(tx) = inflight.get(hash) {
                (false, tx.subscribe())
            } else {
                let (tx, rx) = broadcast::channel::<()>(1);
                inflight.insert(hash.to_string(), tx);
                (true, rx)
            }
        };

        if !is_leader {
            let _ = rx.recv().await;
            return if self.contains(hash).await {
                Ok(())
            } else {
                Err(Error::Xet(format!("file_cache: populate failed for {hash}")))
            };
        }

        let result = self.populate_inner(hash, expected_size, fetch).await;
        if let Err(ref e) = result {
            warn!("file_cache: populate {hash} failed: {e}");
        }
        let mut inflight = self.inflight.lock().expect("file_cache.inflight poisoned");
        if let Some(tx) = inflight.remove(hash) {
            let _ = tx.send(());
        }
        result
    }

    async fn populate_inner<F, Fut>(self: &Arc<Self>, hash: &str, expected_size: Option<u64>, fetch: F) -> Result<()>
    where
        F: FnOnce(PathBuf) -> Fut + Send,
        Fut: std::future::Future<Output = Result<()>> + Send,
    {
        let final_path = self.item_path(hash);
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).map_err(|e| io_err(format!("mkdir {parent:?}: {e}")))?;
        }

        let tmp_path = self.tmp_dir.join(format!("{hash}.{}", ulid::Ulid::new()));

        if let Err(e) = fetch(tmp_path.clone()).await {
            let _ = fs::remove_file(&tmp_path);
            return Err(e);
        }

        let actual_size = fs::metadata(&tmp_path)
            .map_err(|e| io_err(format!("stat {tmp_path:?}: {e}")))?
            .len();
        if let Some(expected) = expected_size
            && actual_size != expected
        {
            let _ = fs::remove_file(&tmp_path);
            return Err(Error::Xet(format!(
                "file_cache: size mismatch for {hash}: got {actual_size}, expected {expected}",
            )));
        }
        // Defense in depth: if the size wasn't known up front, drop oversized
        // files now rather than letting eviction nuke the warm cache.
        if actual_size > self.capacity {
            let _ = fs::remove_file(&tmp_path);
            warn!(
                "file_cache: skipping {hash}: size {actual_size} exceeds capacity {}",
                self.capacity
            );
            return Ok(());
        }

        fs::rename(&tmp_path, &final_path).map_err(|e| io_err(format!("rename {tmp_path:?} → {final_path:?}: {e}")))?;

        let to_remove = {
            let mut state = self.state.write().await;
            let tick = self.next_tick();
            state.items.insert(
                hash.to_string(),
                CacheItem {
                    size: actual_size,
                    last_access: AtomicU64::new(tick),
                },
            );
            state.total_bytes += actual_size;
            self.evict_locked(&mut state)
        };
        for path in to_remove {
            let _ = fs::remove_file(&path);
        }
        debug!("file_cache: populated {hash} ({actual_size} bytes)");
        Ok(())
    }

    fn evict_locked(&self, state: &mut State) -> Vec<PathBuf> {
        if state.total_bytes <= self.capacity {
            return Vec::new();
        }
        let mut entries: Vec<(&str, u64, u64)> = state
            .items
            .iter()
            .map(|(k, v)| (k.as_str(), v.last_access.load(Ordering::Relaxed), v.size))
            .collect();
        entries.sort_by_key(|(_, t, _)| *t);

        let mut victims = Vec::new();
        let mut freed = 0u64;
        for (hash, _, size) in entries {
            if state.total_bytes - freed <= self.capacity {
                break;
            }
            victims.push(hash.to_string());
            freed += size;
        }
        let mut to_remove = Vec::with_capacity(victims.len());
        for hash in victims {
            if let Some(item) = state.items.remove(&hash) {
                state.total_bytes = state.total_bytes.saturating_sub(item.size);
            }
            to_remove.push(self.item_path(&hash));
        }
        to_remove
    }
}

fn io_err(msg: String) -> Error {
    Error::Io(std::io::Error::other(format!("file_cache: {msg}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_file(path: &Path, bytes: &[u8]) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        let mut f = fs::File::create(path).unwrap();
        f.write_all(bytes).unwrap();
    }

    async fn populate_with(cache: &Arc<FileCache>, hash: &str, payload: Vec<u8>) {
        let len = payload.len() as u64;
        cache
            .populate(hash, Some(len), move |dest| async move {
                write_file(&dest, &payload);
                Ok(())
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn populate_and_hit() {
        let dir = tempfile::tempdir().unwrap();
        let cache = FileCache::new(dir.path(), 1 << 30).unwrap();
        let hash = "abcdef0123456789";
        let payload = b"hello world".repeat(100);
        populate_with(&cache, hash, payload.clone()).await;
        let f = cache.try_open(hash).await.unwrap();
        let mut buf = Vec::new();
        std::io::Read::read_to_end(&mut (&*f), &mut buf).unwrap();
        assert_eq!(buf, payload);
    }

    #[tokio::test]
    async fn miss_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let cache = FileCache::new(dir.path(), 1 << 30).unwrap();
        assert!(cache.try_open("dead").await.is_none());
    }

    #[tokio::test]
    async fn size_mismatch_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let cache = FileCache::new(dir.path(), 1 << 30).unwrap();
        let hash = "deadbeef00";
        let err = cache
            .populate(hash, Some(100), |dest| async move {
                write_file(&dest, b"only-five");
                Ok(())
            })
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Xet(_)));
        assert!(!cache.contains(hash).await);
    }

    #[tokio::test]
    async fn lru_evicts_oldest_first() {
        let dir = tempfile::tempdir().unwrap();
        let cache = FileCache::new(dir.path(), 30).unwrap();
        for i in 0..3 {
            populate_with(&cache, &format!("hash{i:08x}"), vec![b'x'; 20]).await;
        }
        // Capacity 30 < 3*20 = 60. Oldest two entries should be gone.
        let mut surviving = 0;
        for i in 0..3 {
            if cache.contains(&format!("hash{i:08x}")).await {
                surviving += 1;
            }
        }
        assert_eq!(surviving, 1, "expected only the most recent entry to survive");
        assert!(cache.contains("hash00000002").await);
    }

    #[tokio::test]
    async fn try_open_bumps_lru() {
        let dir = tempfile::tempdir().unwrap();
        let cache = FileCache::new(dir.path(), 40).unwrap();
        // Populate A, B both 20 bytes — fits in 40.
        populate_with(&cache, "aa00", vec![b'a'; 20]).await;
        populate_with(&cache, "bb00", vec![b'b'; 20]).await;
        // Touch A so it becomes the most-recently-used.
        cache.try_open("aa00").await.unwrap();
        // Add C (20 bytes) — total 60 > 40, must evict.
        populate_with(&cache, "cc00", vec![b'c'; 20]).await;
        assert!(cache.contains("aa00").await, "A was just touched, should survive");
        assert!(!cache.contains("bb00").await, "B is the oldest, should be evicted");
        assert!(cache.contains("cc00").await);
    }

    #[tokio::test]
    async fn rediscovers_existing_files() {
        let dir = tempfile::tempdir().unwrap();
        let cache = FileCache::new(dir.path(), 1 << 30).unwrap();
        populate_with(&cache, "ab1234567890", b"data".to_vec()).await;
        drop(cache);
        let cache2 = FileCache::new(dir.path(), 1 << 30).unwrap();
        assert!(cache2.contains("ab1234567890").await);
    }

    #[tokio::test]
    async fn forget_drops_entry_and_file() {
        let dir = tempfile::tempdir().unwrap();
        let cache = FileCache::new(dir.path(), 1 << 30).unwrap();
        populate_with(&cache, "fade123456", b"data".to_vec()).await;
        cache.forget("fade123456").await;
        assert!(!cache.contains("fade123456").await);
        assert!(cache.try_open("fade123456").await.is_none());
    }

    #[tokio::test]
    async fn oversized_file_does_not_evict_warm_cache() {
        // Capacity 30: warm A (20 bytes) fits. B with size 100 must be rejected
        // without touching A.
        let dir = tempfile::tempdir().unwrap();
        let cache = FileCache::new(dir.path(), 30).unwrap();
        populate_with(&cache, "aa00", vec![b'a'; 20]).await;
        cache
            .populate("bb00", Some(100), |dest| async move {
                write_file(&dest, &[b'b'; 100]);
                Ok(())
            })
            .await
            .unwrap();
        assert!(cache.contains("aa00").await, "A must survive oversized B");
        assert!(!cache.contains("bb00").await, "B is too large to cache");
    }

    #[tokio::test]
    async fn concurrent_populate_collapses_to_single_download() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let dir = tempfile::tempdir().unwrap();
        let cache = FileCache::new(dir.path(), 1 << 30).unwrap();
        let calls = Arc::new(AtomicU32::new(0));
        let hash = "cafebabe";
        let mut joins = Vec::new();
        for _ in 0..8 {
            let cache = cache.clone();
            let calls = calls.clone();
            joins.push(tokio::spawn(async move {
                cache
                    .populate(hash, Some(11), move |dest| async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                        write_file(&dest, b"hello world");
                        Ok(())
                    })
                    .await
                    .unwrap();
            }));
        }
        for j in joins {
            j.await.unwrap();
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1, "fetch should run exactly once");
        assert!(cache.contains(hash).await);
    }
}
