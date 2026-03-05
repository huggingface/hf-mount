use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use data::configurations::TranslatorConfig;
use data::{FileDownloadSession, FileUploadSession, SingleFileCleaner, XetFileInfo};

use crate::error::{Error, Result};

// ── Types ────────────────────────────────────────────────────────────

/// A file to upload via the batch upload path (advanced writes / FlushManager).
/// Carries the staging path and, for modified files, the CAS hash of the
/// previous version so we can preload its shard data for deduplication.
pub struct UploadFile<'a> {
    pub path: &'a Path,
    /// CAS hash of the previous file version (before this write).
    /// When set, the upload session can preload the old file's shard data
    /// so unchanged chunks are recognized as duplicates.
    pub old_xet_hash: Option<&'a str>,
}

// ── Traits ───────────────────────────────────────────────────────────

/// Trait abstracting CAS operations used by VirtualFs and FlushManager.
#[async_trait::async_trait]
pub trait XetOps: Send + Sync {
    async fn create_streaming_writer(&self) -> Result<Box<dyn StreamingWriterOps>>;
    async fn download_to_file(&self, xet_hash: &str, file_size: u64, dest: &Path) -> Result<()>;
    async fn upload_files(&self, files: &[UploadFile<'_>]) -> Result<Vec<XetFileInfo>>;
    fn download_stream_boxed(&self, file_info: &XetFileInfo, offset: u64) -> Result<Box<dyn DownloadStreamOps>>;

    /// Delta upload: upload only the changed regions of a file.
    /// Returns `Ok(Some(info))` on success, `Ok(None)` if delta upload is not supported
    /// or not applicable (caller should fall back to full upload).
    async fn upload_file_delta(
        &self,
        old_xet_hash: &str,
        new_file_size: u64,
        dirty_ranges: &[(u64, u64)],
        staging_path: &Path,
    ) -> Result<Option<XetFileInfo>>;
}

/// Append-only streaming writer trait (abstracts StreamingWriter for testing).
#[async_trait::async_trait]
pub trait StreamingWriterOps: Send {
    async fn write(&mut self, data: &[u8]) -> Result<()>;
    async fn finish_boxed(self: Box<Self>) -> Result<XetFileInfo>;
    fn len(&self) -> u64;
    fn is_empty(&self) -> bool;
}

/// Streaming download trait (abstracts data::DownloadStream for testing).
#[async_trait::async_trait]
pub trait DownloadStreamOps: Send {
    async fn next(&mut self) -> Result<Option<Bytes>>;
}

// ── XetSessions ───────────────────────────────────────────────────────

/// Core xet-core sessions for CAS downloads and uploads.
/// Used by all write modes (simple streaming + advanced staging).
pub struct XetSessions {
    session: Arc<FileDownloadSession>,
    upload_config: Option<Arc<TranslatorConfig>>,
}

impl XetSessions {
    pub fn new(session: Arc<FileDownloadSession>, upload_config: Option<Arc<TranslatorConfig>>) -> Arc<Self> {
        Arc::new(Self { session, upload_config })
    }

    /// Start a streaming download from a byte offset (sync, returns an iterator-like stream).
    pub fn download_stream(&self, file_info: &XetFileInfo, offset: u64) -> Result<data::DownloadStream> {
        self.session
            .download_stream_from_offset(file_info, offset, None)
            .map_err(|e| Error::Xet(e.to_string()))
    }
}

#[async_trait::async_trait]
impl XetOps for XetSessions {
    async fn create_streaming_writer(&self) -> Result<Box<dyn StreamingWriterOps>> {
        let config = self
            .upload_config
            .as_ref()
            .ok_or_else(|| Error::Hub("no upload config (read-only mode)".into()))?;
        let session = FileUploadSession::new(config.clone(), None)
            .await
            .map_err(|e| Error::Xet(e.to_string()))?;
        let cleaner = session
            .start_clean(None, None, Some(mdb_shard::Sha256::default()))
            .await;
        Ok(Box::new(StreamingWriter {
            cleaner,
            session,
            bytes_written: 0,
        }))
    }

    async fn download_to_file(&self, xet_hash: &str, file_size: u64, dest: &Path) -> Result<()> {
        let file_info = XetFileInfo::new(xet_hash.to_string(), file_size);
        self.session
            .download_file(&file_info, dest, None)
            .await
            .map_err(|e| Error::Xet(e.to_string()))?;
        Ok(())
    }

    async fn upload_files(&self, files: &[UploadFile<'_>]) -> Result<Vec<XetFileInfo>> {
        let config = self
            .upload_config
            .as_ref()
            .ok_or_else(|| Error::Hub("no upload config (read-only mode)".into()))?;

        let upload_session = FileUploadSession::new(config.clone(), None)
            .await
            .map_err(|e| Error::Xet(e.to_string()))?;

        // TODO(delta-upload): When xet-core supports shard preloading, call
        // upload_session.preload_file_chunks(old_hash) here for files with
        // old_xet_hash set. This will import the old file's chunk hashes into
        // the dedup table so unchanged chunks are recognized during re-upload.
        for file in files {
            if let Some(old_hash) = file.old_xet_hash {
                tracing::debug!(
                    path = %file.path.display(),
                    old_xet_hash = old_hash,
                    "file has previous CAS version, delta dedup may apply"
                );
            }
        }

        let upload_files: Vec<_> = files
            .iter()
            .map(|f| (f.path.to_path_buf(), Some(mdb_shard::Sha256::default())))
            .collect();

        let results = upload_session
            .upload_files(upload_files)
            .await
            .map_err(|e| Error::Xet(e.to_string()))?;

        upload_session.finalize().await.map_err(|e| Error::Xet(e.to_string()))?;

        Ok(results)
    }

    fn download_stream_boxed(&self, file_info: &XetFileInfo, offset: u64) -> Result<Box<dyn DownloadStreamOps>> {
        let stream = self.download_stream(file_info, offset)?;
        Ok(Box::new(DownloadStreamWrapper(stream)))
    }

    async fn upload_file_delta(
        &self,
        _old_xet_hash: &str,
        _new_file_size: u64,
        _dirty_ranges: &[(u64, u64)],
        _staging_path: &Path,
    ) -> Result<Option<XetFileInfo>> {
        // TODO(delta-upload): Implement delta upload via xet-core's FileUploadSession.
        // For now, return None to signal the caller should fall back to full upload.
        Ok(None)
    }
}

// ── DownloadStreamWrapper ─────────────────────────────────────────────

struct DownloadStreamWrapper(data::DownloadStream);

#[async_trait::async_trait]
impl DownloadStreamOps for DownloadStreamWrapper {
    async fn next(&mut self) -> Result<Option<Bytes>> {
        self.0.next().await.map_err(|e| Error::Xet(e.to_string()))
    }
}

// ── StagingDir ────────────────────────────────────────────────────────

/// On-disk staging area for advanced writes (random seek, read-modify-write).
/// Not used in simple (append-only) mode.
#[derive(Clone)]
pub struct StagingDir {
    dir: PathBuf,
    /// Per-session random key to make staging paths unpredictable.
    session_key: u64,
}

impl StagingDir {
    pub fn new(cache_dir: &Path) -> Self {
        let dir = cache_dir.join("staging");
        std::fs::create_dir_all(&dir).ok();
        Self {
            dir,
            session_key: rand_u64(),
        }
    }

    /// Root directory of the staging area.
    pub fn root(&self) -> &Path {
        &self.dir
    }

    /// Get the staging path for a given inode.
    /// Deterministic within a session but unpredictable from outside.
    pub fn path(&self, inode: u64) -> PathBuf {
        self.dir.join(format!("ino_{:x}_{:016x}", inode, self.session_key))
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
        self.cleaner
            .add_data(data)
            .await
            .map_err(|e| Error::Xet(e.to_string()))?;
        self.bytes_written += data.len() as u64;
        Ok(())
    }

    async fn finish_boxed(self: Box<Self>) -> Result<XetFileInfo> {
        let (info, _metrics) = self.cleaner.finish().await.map_err(|e| Error::Xet(e.to_string()))?;
        self.session.finalize().await.map_err(|e| Error::Xet(e.to_string()))?;
        Ok(info)
    }

    fn len(&self) -> u64 {
        self.bytes_written
    }

    fn is_empty(&self) -> bool {
        self.bytes_written == 0
    }
}
