use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use xet_client::cas_client::Client;
use xet_client::cas_types::FileRange;
use xet_client::chunk_cache::ChunkCache;
use xet_core_structures::merklehash::MerkleHash;
use xet_data::file_reconstruction::{DownloadStream, FileReconstructor};
use xet_data::processing::configurations::TranslatorConfig;
use xet_data::processing::{FileDownloadSession, FileUploadSession, Sha256Policy, SingleFileCleaner, XetFileInfo};
use xet_runtime::core::XetContext;

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

    /// Download a whole CAS object to `dest`, streaming it through
    /// `download_stream_boxed` and bounding each chunk read with `timeout`.
    ///
    /// Unlike `download_to_file` (which calls xet-core's `download_file` and
    /// awaits it with no ceiling), every chunk read here is bounded: a stalled
    /// CAS/CDN connection on a write-path download (open-for-write of a remote
    /// file, flush, or a background file-cache populate) would otherwise park
    /// the caller — a FUSE worker thread for the synchronous paths, a tokio
    /// worker for the detached populate task — indefinitely, the same wedge that
    /// affects reads. `Duration::ZERO` disables the bound. The reconstruction
    /// still runs ahead concurrently on the stream's own spawned task, so
    /// streaming the bytes here does not serialize the network.
    async fn download_to_file_bounded(
        &self,
        xet_hash: &str,
        file_size: u64,
        dest: &Path,
        timeout: std::time::Duration,
    ) -> Result<()> {
        use tokio::io::AsyncWriteExt;

        // The stream is bounded to `[0, file_size)`, so a CAS object *shorter*
        // than `file_size` is caught by the size check below. An object *longer*
        // cannot occur here: `xet_hash` and `file_size` come from the same inode
        // snapshot and the hash is content-addressed, so the declared size is
        // exactly the reconstructed size.
        let file_info = XetFileInfo::new(xet_hash.to_string(), file_size);

        // Stream into a temporary sibling and rename on success: a timeout or
        // error must never leave a partial file at `dest`. A leftover partial
        // would later be mistaken for a complete local copy (e.g. setattr's
        // `local_exists` short-circuit) and flushed as corrupted content.
        let mut tmp = dest.as_os_str().to_owned();
        tmp.push(".part");
        let tmp = PathBuf::from(tmp);

        let result = async {
            let mut stream = self.download_stream_boxed(&file_info, 0, None)?;
            let mut file = tokio::fs::File::create(&tmp)
                .await
                .map_err(|e| Error::Xet(format!("download_to_file_bounded: create {tmp:?}: {e}")))?;

            let mut written: u64 = 0;
            loop {
                let next = if timeout.is_zero() {
                    stream.next().await
                } else {
                    match tokio::time::timeout(timeout, stream.next()).await {
                        Ok(result) => result,
                        Err(_elapsed) => {
                            return Err(Error::Xet(format!(
                                "download_to_file_bounded stalled after {timeout:?} at {written}/{file_size} bytes \
                                 (hash={xet_hash}) — aborting to avoid parking the worker indefinitely"
                            )));
                        }
                    }
                };
                match next? {
                    Some(chunk) => {
                        file.write_all(&chunk)
                            .await
                            .map_err(|e| Error::Xet(format!("download_to_file_bounded: write {tmp:?}: {e}")))?;
                        written += chunk.len() as u64;
                    }
                    None => break,
                }
            }
            file.flush()
                .await
                .map_err(|e| Error::Xet(format!("download_to_file_bounded: flush {tmp:?}: {e}")))?;

            if written != file_size {
                return Err(Error::Xet(format!(
                    "download_to_file_bounded size mismatch: got {written}, expected {file_size} (hash={xet_hash})"
                )));
            }
            Ok(())
        }
        .await;

        match result {
            Ok(()) => tokio::fs::rename(&tmp, dest)
                .await
                .map_err(|e| Error::Xet(format!("download_to_file_bounded: rename {tmp:?} -> {dest:?}: {e}"))),
            Err(e) => {
                // Best-effort cleanup so no partial file is left behind.
                let _ = tokio::fs::remove_file(&tmp).await;
                Err(e)
            }
        }
    }
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
