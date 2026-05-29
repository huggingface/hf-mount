use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use tokio::io::AsyncRead;
use tracing::info;
use xet_client::cas_client::Client;
use xet_client::cas_types::FileRange;
use xet_client::chunk_cache::ChunkCache;
use xet_core_structures::merklehash::MerkleHash;
use xet_data::file_reconstruction::{DownloadStream, FileReconstructor};
use xet_data::processing::configurations::TranslatorConfig;
use xet_data::processing::{
    DirtyInput, FileDownloadSession, FileUploadSession, Sha256Policy, SingleFileCleaner, XetFileInfo,
};
use xet_runtime::core::XetContext;

use crate::error::{Error, Result};

// ── RangeSnapshot ────────────────────────────────────────────────────

/// A self-contained snapshot of one dirty range, ready to feed `range_upload`.
///
/// The caller (typically `flush_batch`) reads the bytes from the staging file
/// while holding the per-inode I/O lock, then releases the lock and hands the
/// snapshot to `range_upload`. `range_upload` itself never touches disk and
/// never holds the I/O lock, so concurrent writes to staging can proceed
/// during the upload without producing torn reads (codex review finding: the
/// previous per-chunk lock acquisition could let a concurrent pwrite interleave
/// between chunks, hashing chimeric content).
pub struct RangeSnapshot {
    /// Offset in the new file where these bytes live.
    pub offset: u64,
    /// Pre-read bytes from staging at this offset.
    pub data: Bytes,
}

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

    /// Compose a new CAS file from an existing reconstruction (`original_hash`)
    /// plus a set of pre-snapshotted dirty ranges. Used for sparse-write flushes
    /// so unmodified bytes never round-trip the network.
    ///
    /// `new_file_size` is the size of the resulting file. When it is less than
    /// `original_size`, a synthetic delete is appended past the last dirty range
    /// so the original tail is dropped.
    async fn range_upload(
        &self,
        original_hash: &str,
        original_size: u64,
        new_file_size: u64,
        dirty_snapshots: Vec<RangeSnapshot>,
    ) -> Result<XetFileInfo>;
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
}

impl XetSessions {
    pub fn new(
        ctx: XetContext,
        session: Arc<FileDownloadSession>,
        upload_config: Option<Arc<TranslatorConfig>>,
        cas_client: Arc<dyn Client>,
        chunk_cache: Option<Arc<dyn ChunkCache>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            ctx,
            session,
            upload_config,
            cas_client,
            chunk_cache,
        })
    }

    /// Start a streaming download for a byte range.
    /// When `end` is `Some`, only bytes `[offset, end)` are fetched (bounded range).
    /// When `end` is `None`, fetches from `offset` to end of file (unbounded stream).
    pub fn download_stream(&self, file_info: &XetFileInfo, offset: u64, end: Option<u64>) -> Result<DownloadStream> {
        let hash = file_info
            .merkle_hash()
            .map_err(|e| Error::Xet(format!("invalid hash: {e}")))?;
        let is_unbounded = end.is_none();
        let file_size = file_info.file_size().unwrap_or(u64::MAX);
        let end = end.unwrap_or(file_size);
        let mut reconstructor =
            FileReconstructor::new(&self.ctx, &self.cas_client, hash).with_byte_range(FileRange::new(offset, end));
        // Attach chunk cache only to the unbounded stream path: the xorb disk
        // cache pulls full xorbs (~64MB) even for small range requests, which
        // is wasteful for random reads. Sequential reads (unbounded) benefit.
        if is_unbounded && let Some(cache) = self.chunk_cache.as_ref() {
            reconstructor = reconstructor.with_chunk_cache(cache.clone());
        }
        Ok(reconstructor.reconstruct_to_stream())
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
        let stream = self.download_stream(file_info, offset, end)?;
        Ok(Box::new(DownloadStreamWrapper(stream)))
    }

    async fn warm_reconstruction_cache(&self, xet_hash: &str) {
        if let Ok(hash) = MerkleHash::from_hex(xet_hash) {
            let _ = self.cas_client.get_reconstruction(&hash, None).await;
        }
    }

    async fn range_upload(
        &self,
        original_hash: &str,
        original_size: u64,
        new_file_size: u64,
        dirty_snapshots: Vec<RangeSnapshot>,
    ) -> Result<XetFileInfo> {
        let config = self
            .upload_config
            .as_ref()
            .ok_or_else(|| Error::hub("no upload config (read-only mode)"))?;

        let original_merkle =
            MerkleHash::from_hex(original_hash).map_err(|e| Error::Xet(format!("invalid original hash: {e}")))?;

        // No-op: no dirty bytes and file size unchanged means the new content
        // is bit-for-bit the original. Skip the upload round-trip.
        if dirty_snapshots.is_empty() && new_file_size == original_size {
            return Ok(XetFileInfo::new(original_hash.to_string(), original_size));
        }

        // Plan the `original_range` each dirty input REPLACES (empty for inserts
        // past EOF, partial for writes spanning EOF, full for overwrites within
        // the original), plus any synthetic truncate-tail delete. Pure and
        // unit-tested in `plan_original_ranges` so the composition mapping has
        // coverage independent of a live CAS (the mock XetOps bypasses it).
        let dirty_count = dirty_snapshots.len();
        let meta: Vec<(u64, u64)> = dirty_snapshots
            .iter()
            .map(|s| (s.offset, s.data.len() as u64))
            .collect();
        let plan = plan_original_ranges(&meta, original_size, new_file_size);

        // Pair plan entries with their readers by index: the first `meta.len()`
        // entries are data-bearing (one per snapshot, same order); a trailing
        // entry (if any) is the synthetic truncate tail with an empty reader.
        let mut snaps = dirty_snapshots.into_iter();
        let mut dirty_inputs: Vec<DirtyInput> = Vec::with_capacity(plan.len());
        for (idx, (original_range, new_length)) in plan.into_iter().enumerate() {
            let reader: Pin<Box<dyn AsyncRead + Send>> = if idx < meta.len() {
                let snap = snaps.next().expect("one snapshot per data plan entry");
                Box::pin(std::io::Cursor::new(snap.data))
            } else {
                Box::pin(tokio::io::empty())
            };
            dirty_inputs.push(DirtyInput {
                original_range,
                reader,
                new_length,
            });
        }

        let result = xet_data::processing::upload_ranges(
            config.clone(),
            self.cas_client.clone(),
            original_merkle,
            original_size,
            dirty_inputs,
        )
        .await
        .map_err(|e| Error::Xet(e.to_string()))?;

        info!(
            "range_upload: hash={} size={:?} (original_size={}, {} dirty ranges)",
            result.hash(),
            result.file_size(),
            original_size,
            dirty_count
        );

        Ok(result)
    }
}

// ── range_upload composition planning ────────────────────────────────

/// For each dirty snapshot `(offset, new_length)`, compute the `original_range`
/// of the ORIGINAL file that the snapshot REPLACES, plus an optional synthetic
/// truncate-tail delete when the new file is shorter than the original.
///
/// Returns one entry per snapshot (same order) followed by at most one extra
/// `(range, 0)` truncate entry, so callers can pair the first `snapshots.len()`
/// entries with their readers by index. Mapping rules per snapshot spanning
/// `[start, start+new_length)`:
/// - fully within the original (`end <= original_size`): replaces `start..end`;
/// - fully past EOF (`start >= original_size`): inserts (replaces an empty
///   `original_size..original_size`);
/// - spanning EOF: replaces `start..original_size` and extends.
///
/// Pure function: the composition mapping is unit-tested here, independent of a
/// live CAS (the mock `XetOps::range_upload` bypasses this logic entirely).
fn plan_original_ranges(
    snapshots: &[(u64, u64)],
    original_size: u64,
    new_file_size: u64,
) -> Vec<(std::ops::Range<u64>, u64)> {
    let mut plan: Vec<(std::ops::Range<u64>, u64)> = Vec::with_capacity(snapshots.len() + 1);
    for &(start, new_length) in snapshots {
        let end = start + new_length;
        let original_range = if end <= original_size {
            start..end
        } else if start >= original_size {
            original_size..original_size
        } else {
            start..original_size
        };
        plan.push((original_range, new_length));
    }

    // Truncate-past-end: if the new file is shorter than the original AND the
    // truncation point isn't already covered by a dirty input, append a
    // synthetic empty input over the cut tail so upload_ranges drops those
    // original bytes.
    if new_file_size < original_size {
        let last_covered = plan.last().map(|(r, _)| r.end).unwrap_or(0);
        let truncate_start = new_file_size.max(last_covered);
        if truncate_start < original_size {
            plan.push((truncate_start..original_size, 0));
        }
    }
    plan
}

#[cfg(test)]
mod range_upload_tests {
    use super::plan_original_ranges;

    #[test]
    fn in_place_overwrite_maps_to_same_range() {
        // Overwrite [10,20) of a 100-byte file: replaces exactly [10,20).
        assert_eq!(plan_original_ranges(&[(10, 10)], 100, 100), vec![(10..20, 10)]);
    }

    #[test]
    fn write_past_eof_is_an_insert() {
        // Write 5 bytes at offset 30 of a 20-byte file: inserts past EOF.
        assert_eq!(plan_original_ranges(&[(30, 5)], 20, 35), vec![(20..20, 5)]);
    }

    #[test]
    fn write_spanning_eof_replaces_tail_and_extends() {
        // Write [15,25) of a 20-byte file: replaces [15,20), extends to 25.
        assert_eq!(plan_original_ranges(&[(15, 10)], 20, 25), vec![(15..20, 10)]);
    }

    #[test]
    fn pure_shrink_appends_truncate_tail() {
        // No dirty bytes, shrink 50 -> 20: one synthetic delete of [20,50).
        assert_eq!(plan_original_ranges(&[], 50, 20), vec![(20..50, 0)]);
    }

    #[test]
    fn shrink_with_dirty_prefix_truncates_after_last_dirty() {
        // Overwrite [0,10) and shrink 50 -> 20: keep the overwrite, drop [20,50).
        assert_eq!(plan_original_ranges(&[(0, 10)], 50, 20), vec![(0..10, 10), (20..50, 0)]);
    }

    #[test]
    fn shrink_below_a_dirty_range_does_not_double_count_tail() {
        // Dirty [0,30) but file shrinks to 25: the dirty input already covers
        // past new_file_size, so truncate_start = max(25, 30) = 30 == nothing
        // extra to drop beyond the dirty range's original_range end.
        assert_eq!(plan_original_ranges(&[(0, 30)], 50, 25), vec![(0..30, 30), (30..50, 0)]);
    }

    #[test]
    fn no_change_yields_empty_plan() {
        assert!(plan_original_ranges(&[], 100, 100).is_empty());
    }
}

// ── DownloadStreamWrapper ─────────────────────────────────────────────

struct DownloadStreamWrapper(DownloadStream);

#[async_trait::async_trait]
impl DownloadStreamOps for DownloadStreamWrapper {
    async fn next(&mut self) -> Result<Option<Bytes>> {
        Ok(self.0.next().await?)
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

    /// Actual on-disk usage of the staging file for `inode` (allocated blocks ×
    /// 512), or 0 if it doesn't exist. Unlike `file_size` (logical length),
    /// this reflects what a sparse file truly occupies: a freshly punched hole
    /// (`set_len` with no data written) reports ~0 here, so sparse opens charge
    /// ~0 against the GC budget instead of the full logical size. Bytes are
    /// then accounted as they materialize (CAS read-fill, writes).
    pub fn disk_usage(&self, inode: u64) -> u64 {
        std::fs::metadata(self.path(inode))
            .map(|m| m.blocks() * 512)
            .unwrap_or(0)
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
