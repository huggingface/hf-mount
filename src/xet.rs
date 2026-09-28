use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use xet_client::cas_client::Client;
use xet_client::cas_types::FileRange;
use xet_client::chunk_cache::ChunkCache;
use xet_core_structures::merklehash::MerkleHash;
use xet_core_structures::xorb_object::constants::MAX_XORB_BYTES;
use xet_data::file_reconstruction::{DownloadStream, FileReconstructor};
use xet_data::processing::configurations::TranslatorConfig;
use xet_data::processing::{FileDownloadSession, FileUploadSession, Sha256Policy, SingleFileCleaner, XetFileInfo};
use xet_runtime::core::XetContext;
use xet_runtime::utils::adjustable_semaphore::AdjustableSemaphore;

use crate::error::{Error, Result};

// ── Traits ───────────────────────────────────────────────────────────

/// Trait abstracting CAS operations used by VirtualFs and FlushManager.
#[async_trait::async_trait]
pub trait XetOps: Send + Sync {
    async fn create_streaming_writer(&self) -> Result<Box<dyn StreamingWriterOps>>;
    async fn download_to_file(&self, xet_hash: &str, file_size: u64, dest: &Path) -> Result<()>;
    async fn upload_files(&self, paths: &[&Path]) -> Result<Vec<XetFileInfo>>;
    fn download_stream_boxed(
        &self,
        file_info: &XetFileInfo,
        offset: u64,
        end: Option<u64>,
    ) -> Result<Box<dyn DownloadStreamOps>>;
    /// Pre-warm the reconstruction cache for a file by fetching its full plan.
    /// Errors are silently ignored — this is best-effort.
    async fn warm_reconstruction_cache(&self, xet_hash: &str);
}

/// Append-only streaming writer trait (abstracts StreamingWriter for testing).
#[async_trait::async_trait]
pub trait StreamingWriterOps: Send {
    async fn write(&mut self, data: &[u8]) -> Result<()>;
    async fn finish_boxed(self: Box<Self>) -> Result<XetFileInfo>;
    fn len(&self) -> u64;
    fn is_empty(&self) -> bool;
}

/// Streaming download trait (abstracts DownloadStream for testing).
#[async_trait::async_trait]
pub trait DownloadStreamOps: Send {
    async fn next(&mut self) -> Result<Option<Bytes>>;
}

// ── Per-stream download buffers ───────────────────────────────────────

/// Ceiling of a single stream's buffer: pipelining depth of a lone reader.
const STREAM_BUFFER_MAX: u64 = 256 * 1_048_576;

/// Floor of a single stream's buffer: one full xorb, the largest possible
/// term. xet-core clamps a term acquire against the semaphore total only
/// when it is issued (`AdjustableSemaphore::to_physical_acquire`) and does
/// not re-clamp pending acquires on shrink, so shrinking below the largest
/// term would leave an in-flight acquire that can never be satisfied. The
/// floor can become memory-driven once that is fixed upstream.
fn stream_buffer_min() -> u64 {
    *MAX_XORB_BYTES as u64
}

/// Gives every read stream a private download buffer.
///
/// Streams are consumed by FUSE `read()` calls that each pin a worker thread.
/// With xet-core's single global buffer (FIFO), a stream that already holds
/// buffer cannot release it until its consumer gets a worker thread, which
/// may itself be blocked waiting for buffer on another stream. Under many
/// concurrent cold readers this starves new streams at their first byte and
/// wedges the mount (#234). Private buffers mean streams never wait on each
/// other. Each is sized `budget / active_streams`, clamped to
/// `[stream_buffer_min(), STREAM_BUFFER_MAX]`, so memory in flight is bounded
/// by `max(budget, streams * stream_buffer_min())`.
struct StreamBufferPool {
    budget: u64,
    inner: Mutex<StreamBuffers>,
}

#[derive(Default)]
struct StreamBuffers {
    active: Vec<Arc<AdjustableSemaphore>>,
    /// Share currently applied to every active buffer.
    share: u64,
}

impl StreamBufferPool {
    fn new(budget: u64) -> Arc<Self> {
        Arc::new(Self {
            budget,
            inner: Mutex::new(StreamBuffers::default()),
        })
    }

    /// Create a buffer for a new stream, already sized to the new fair
    /// share, and resize the other active buffers to match. The guard
    /// unregisters the buffer on drop.
    fn register(self: &Arc<Self>) -> StreamBufferGuard {
        let mut inner = self.inner.lock().expect("stream buffers poisoned");
        let share = self.share_for(inner.active.len() + 1);
        let buffer = AdjustableSemaphore::new(share, (stream_buffer_min(), STREAM_BUFFER_MAX));
        inner.active.push(buffer.clone());
        Self::apply_share(&mut inner, share);
        StreamBufferGuard {
            pool: self.clone(),
            buffer,
        }
    }

    fn unregister(&self, buffer: &Arc<AdjustableSemaphore>) {
        let mut inner = self.inner.lock().expect("stream buffers poisoned");
        if let Some(index) = inner.active.iter().position(|other| Arc::ptr_eq(other, buffer)) {
            inner.active.swap_remove(index);
        }
        let share = self.share_for(inner.active.len());
        Self::apply_share(&mut inner, share);
    }

    /// Resize every active buffer to `share`; a no-op when it is unchanged.
    /// Shrinks apply lazily: permits already held are reclaimed as they return.
    fn apply_share(inner: &mut StreamBuffers, share: u64) {
        if inner.share == share {
            return;
        }
        inner.share = share;
        for buffer in &inner.active {
            // Each call is a no-op when the target is on the other side of
            // the current total; the increment's virtual permit releases the
            // added capacity on drop.
            drop(buffer.increment_permits_to_target(share));
            buffer.decrement_permits_to_target(share);
        }
    }

    fn share_for(&self, active_streams: usize) -> u64 {
        (self.budget / active_streams.max(1) as u64).clamp(stream_buffer_min(), STREAM_BUFFER_MAX)
    }
}

/// Keeps a stream's buffer registered in its pool; dropping it (with the
/// stream) hands the freed share back to the remaining streams.
struct StreamBufferGuard {
    pool: Arc<StreamBufferPool>,
    buffer: Arc<AdjustableSemaphore>,
}

impl Drop for StreamBufferGuard {
    fn drop(&mut self) {
        self.pool.unregister(&self.buffer);
    }
}

// ── XetSessions ───────────────────────────────────────────────────────

/// Core xet-core sessions for CAS downloads and uploads.
/// Used by all write modes (simple streaming + advanced staging).
pub struct XetSessions {
    ctx: XetContext,
    session: Arc<FileDownloadSession>,
    upload_config: Option<Arc<TranslatorConfig>>,
    /// Kept separately from `session` for bounded range downloads via `FileReconstructor`.
    cas_client: Arc<dyn Client>,
    /// Chunk cache attached to unbounded streams; bounded range downloads skip it
    /// to avoid pulling whole xorbs for small range requests.
    chunk_cache: Option<Arc<dyn ChunkCache>>,
    stream_buffers: Arc<StreamBufferPool>,
}

impl XetSessions {
    pub fn new(
        ctx: XetContext,
        session: Arc<FileDownloadSession>,
        upload_config: Option<Arc<TranslatorConfig>>,
        cas_client: Arc<dyn Client>,
        chunk_cache: Option<Arc<dyn ChunkCache>>,
    ) -> Arc<Self> {
        // The same knob xet-core uses for its global buffer, so the existing
        // HF_XET_RECONSTRUCTION_DOWNLOAD_BUFFER_LIMIT override keeps working.
        let stream_buffers = StreamBufferPool::new(ctx.config.reconstruction.download_buffer_limit.as_u64());
        Arc::new(Self {
            ctx,
            session,
            upload_config,
            cas_client,
            chunk_cache,
            stream_buffers,
        })
    }

    /// Start a streaming download for a byte range.
    /// When `end` is `Some`, only bytes `[offset, end)` are fetched (bounded range).
    /// When `end` is `None`, fetches from `offset` to end of file (unbounded stream).
    fn download_stream(&self, file_info: &XetFileInfo, offset: u64, end: Option<u64>) -> Result<DownloadStreamWrapper> {
        let hash = file_info
            .merkle_hash()
            .map_err(|e| Error::Xet(format!("invalid hash: {e}")))?;
        let is_unbounded = end.is_none();
        let file_size = file_info.file_size().unwrap_or(u64::MAX);
        let end = end.unwrap_or(file_size);
        let buffer = self.stream_buffers.register();
        let mut reconstructor = FileReconstructor::new(&self.ctx, &self.cas_client, hash)
            .with_byte_range(FileRange::new(offset, end))
            .with_buffer_semaphore(buffer.buffer.clone());
        // Attach chunk cache only to the unbounded stream path: the xorb disk
        // cache pulls full xorbs (~64MB) even for small range requests, which
        // is wasteful for random reads. Sequential reads (unbounded) benefit.
        if is_unbounded && let Some(cache) = self.chunk_cache.as_ref() {
            reconstructor = reconstructor.with_chunk_cache(cache.clone());
        }
        Ok(DownloadStreamWrapper {
            stream: reconstructor.reconstruct_to_stream(),
            _buffer: buffer,
        })
    }
}

#[async_trait::async_trait]
impl XetOps for XetSessions {
    async fn create_streaming_writer(&self) -> Result<Box<dyn StreamingWriterOps>> {
        let config = self
            .upload_config
            .as_ref()
            .ok_or_else(|| Error::hub("no upload config (read-only mode)"))?;
        let session = FileUploadSession::new(config.clone()).await?;
        let (_id, cleaner) = session.start_clean(None, None, Sha256Policy::Skip)?;
        Ok(Box::new(StreamingWriter {
            cleaner,
            session,
            bytes_written: 0,
        }))
    }

    async fn download_to_file(&self, xet_hash: &str, file_size: u64, dest: &Path) -> Result<()> {
        let file_info = XetFileInfo::new(xet_hash.to_string(), file_size);
        self.session.download_file(&file_info, dest).await?;
        Ok(())
    }

    async fn upload_files(&self, paths: &[&Path]) -> Result<Vec<XetFileInfo>> {
        let config = self
            .upload_config
            .as_ref()
            .ok_or_else(|| Error::hub("no upload config (read-only mode)"))?;

        let upload_session = FileUploadSession::new(config.clone()).await?;

        let files: Vec<(PathBuf, Sha256Policy)> = paths.iter().map(|p| (p.to_path_buf(), Sha256Policy::Skip)).collect();

        let results = upload_session.upload_files(files).await?;
        upload_session.finalize().await?;

        Ok(results)
    }

    fn download_stream_boxed(
        &self,
        file_info: &XetFileInfo,
        offset: u64,
        end: Option<u64>,
    ) -> Result<Box<dyn DownloadStreamOps>> {
        Ok(Box::new(self.download_stream(file_info, offset, end)?))
    }

    async fn warm_reconstruction_cache(&self, xet_hash: &str) {
        if let Ok(hash) = MerkleHash::from_hex(xet_hash) {
            let _ = self.cas_client.get_reconstruction(&hash, None).await;
        }
    }
}

// ── DownloadStreamWrapper ─────────────────────────────────────────────

struct DownloadStreamWrapper {
    stream: DownloadStream,
    /// Declared after `stream` so the stream (and its reconstruction task) is
    /// cancelled before the buffer share is handed back.
    _buffer: StreamBufferGuard,
}

#[async_trait::async_trait]
impl DownloadStreamOps for DownloadStreamWrapper {
    async fn next(&mut self) -> Result<Option<Bytes>> {
        Ok(self.stream.next().await?)
    }
}

// ── StagingDir ────────────────────────────────────────────────────────

/// On-disk staging area for advanced writes (random seek, read-modify-write).
/// Not used in simple (append-only) mode.
///
/// Each mount gets its own random subdirectory under `cache_dir` so mounts
/// never observe each other's staging files — no session key suffix on file
/// names, no seeding of `bytes_used` from foreign entries, and a clean rm
/// when the last clone is dropped.
///
/// Tracks disk usage via `bytes_used`. When `max_bytes > 0` and usage exceeds
/// the limit, the flush loop garbage-collects flushed staging files. When
/// under the limit (or unlimited), staging files persist as a read-after-write
/// cache within the mount lifetime.
#[derive(Clone)]
pub struct StagingDir {
    /// Shared root so the directory is only deleted when the last clone drops.
    root: Arc<StagingRoot>,
    /// Approximate bytes used by staging files on disk.
    bytes_used: Arc<AtomicU64>,
    /// Maximum staging bytes before GC kicks in. 0 = unlimited.
    max_bytes: u64,
}

/// Owns the on-disk staging directory and removes it when dropped.
struct StagingRoot {
    dir: PathBuf,
}

impl Drop for StagingRoot {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_dir_all(&self.dir) {
            tracing::warn!("staging: failed to remove {}: {}", self.dir.display(), e);
        }
    }
}

impl StagingDir {
    pub fn new(cache_dir: &Path, max_bytes: u64) -> Self {
        // Random per-mount subdir so two mounts sharing cache_dir, or a mount
        // started after a crashed previous one, never see each other's files.
        let dir = cache_dir.join(format!("staging-{:016x}", rand_u64()));
        std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("Failed to create staging dir {:?}: {e}", dir));

        Self {
            root: Arc::new(StagingRoot { dir }),
            bytes_used: Arc::new(AtomicU64::new(0)),
            max_bytes,
        }
    }

    /// Root directory of the staging area.
    pub fn root(&self) -> &Path {
        &self.root.dir
    }

    /// Get the staging path for a given inode.
    pub fn path(&self, inode: u64) -> PathBuf {
        self.root.dir.join(format!("ino_{:x}", inode))
    }

    /// Size of the on-disk staging file for `inode`, or 0 if it doesn't exist.
    pub fn file_size(&self, inode: u64) -> u64 {
        std::fs::metadata(self.path(inode)).map(|m| m.len()).unwrap_or(0)
    }

    /// Remove the staging file for `inode`, ignoring NotFound.
    /// Returns `true` if the file was actually removed.
    pub fn try_remove(&self, inode: u64) -> bool {
        let path = self.path(inode);
        let size = self.file_size(inode);
        match std::fs::remove_file(&path) {
            Ok(()) => {
                self.resize_bytes(size, 0);
                true
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
            Err(e) => {
                tracing::warn!("staging GC: failed to remove ino={}: {}", inode, e);
                false
            }
        }
    }

    /// Whether staging usage exceeds the configured limit.
    pub fn is_over_limit(&self) -> bool {
        self.max_bytes > 0 && self.bytes_used.load(Ordering::Relaxed) > self.max_bytes
    }

    /// Whether a non-zero disk budget was configured (i.e. GC is armed).
    pub fn has_budget(&self) -> bool {
        self.max_bytes > 0
    }

    #[cfg(test)]
    pub fn bytes_used(&self) -> u64 {
        self.bytes_used.load(Ordering::Relaxed)
    }

    /// Apply the net change when a staging file goes from `old` to `new` bytes.
    /// Saturates at zero on shrink to tolerate accounting drift. Covers
    /// plain add (old=0), plain remove (new=0), and in-place resize.
    pub fn resize_bytes(&self, old: u64, new: u64) {
        self.bytes_used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(old).saturating_add(new))
            })
            .ok();
    }

    pub fn local_exists(&self, inode: u64) -> std::io::Result<bool> {
        Ok(self.path(inode).exists())
    }

    pub fn open_local_file(
        &self,
        inode: u64,
        read: bool,
        write: bool,
        create: bool,
        truncate: bool,
    ) -> std::io::Result<std::fs::File> {
        let path = self.path(inode);
        let mut options = std::fs::OpenOptions::new();
        options.read(read).write(write);
        if create {
            options.create(true);
        }
        if truncate {
            options.truncate(true);
        }
        options.open(path)
    }

    pub fn remove_local_file(&self, inode: u64) -> std::io::Result<()> {
        std::fs::remove_file(self.path(inode))
    }
}

fn rand_u64() -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::time::Instant::now().hash(&mut hasher);
    std::process::id().hash(&mut hasher);
    hasher.finish()
}

// ── StreamingWriter ────────────────────────────────────────────────────

/// Append-only writer that streams data directly to CAS via SingleFileCleaner.
pub struct StreamingWriter {
    cleaner: SingleFileCleaner,
    session: Arc<FileUploadSession>,
    bytes_written: u64,
}

#[async_trait::async_trait]
impl StreamingWriterOps for StreamingWriter {
    async fn write(&mut self, data: &[u8]) -> Result<()> {
        self.cleaner.add_data(data).await?;
        self.bytes_written += data.len() as u64;
        Ok(())
    }

    async fn finish_boxed(self: Box<Self>) -> Result<XetFileInfo> {
        let (info, _metrics) = self.cleaner.finish().await?;
        self.session.finalize().await?;
        Ok(info)
    }

    fn len(&self) -> u64 {
        self.bytes_written
    }

    fn is_empty(&self) -> bool {
        self.bytes_written == 0
    }
}

#[cfg(test)]
mod stream_buffer_tests {
    use super::*;

    const MIB: u64 = 1_048_576;
    const BUDGET: u64 = 1024 * MIB;

    fn total(guard: &StreamBufferGuard) -> u64 {
        guard.buffer.total_permits()
    }

    #[test]
    fn lone_stream_gets_the_ceiling() {
        let pool = StreamBufferPool::new(BUDGET);
        let stream = pool.register();
        assert_eq!(total(&stream), 256 * MIB);
    }

    #[test]
    fn shares_shrink_to_the_floor_and_grow_back() {
        let pool = StreamBufferPool::new(BUDGET);
        let mut streams: Vec<_> = (0..4).map(|_| pool.register()).collect();
        // 1 GiB / 4 = 256 MiB: still at the ceiling.
        assert!(streams.iter().all(|stream| total(stream) == 256 * MIB));

        streams.extend((0..4).map(|_| pool.register()));
        // 1 GiB / 8 = 128 MiB for everyone, including the early streams.
        assert!(streams.iter().all(|stream| total(stream) == 128 * MIB));

        streams.extend((0..56).map(|_| pool.register()));
        // 1 GiB / 64 = 16 MiB, clamped up to the floor.
        assert!(streams.iter().all(|stream| total(stream) == 64 * MIB));

        streams.truncate(2);
        // 1 GiB / 2 = 512 MiB, clamped down to the ceiling.
        assert!(streams.iter().all(|stream| total(stream) == 256 * MIB));
        assert_eq!(pool.inner.lock().unwrap().active.len(), 2);
    }

    #[tokio::test]
    async fn shrink_applies_once_held_permits_return() {
        let pool = StreamBufferPool::new(BUDGET);
        let first = pool.register();
        let held = first.buffer.acquire_many(200 * MIB).await.unwrap();

        let _others: Vec<_> = (0..7).map(|_| pool.register()).collect();
        // Target is 128 MiB but 200 MiB are out: the total is updated now,
        // the part that cannot be reclaimed yet stays pending.
        assert_eq!(total(&first), 128 * MIB);
        assert!(first.buffer.available_permits() <= 56 * MIB);
        drop(held);
        assert_eq!(first.buffer.available_permits(), 128 * MIB);
    }
}
