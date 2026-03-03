use std::path::{Path, PathBuf};
use std::sync::Arc;

use data::configurations::TranslatorConfig;
use data::{FileDownloadSession, FileUploadSession, SingleFileCleaner, XetFileInfo};

use crate::error::{Error, Result};

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

    /// Start a streaming download (sync, returns an iterator-like stream).
    pub fn download_stream(&self, file_info: &XetFileInfo) -> Result<data::DownloadStream> {
        self.session
            .download_stream(file_info, None)
            .map_err(|e| Error::Xet(e.to_string()))
    }

    /// Download a byte range into a writer.
    pub async fn download_to_writer<W: std::io::Write + Send + 'static>(
        &self,
        file_info: &XetFileInfo,
        range: std::ops::Range<u64>,
        writer: W,
    ) -> Result<u64> {
        self.session
            .download_to_writer(file_info, range, writer, None)
            .await
            .map_err(|e| Error::Xet(e.to_string()))
    }

    /// Download a file directly to a destination path (for writable opens).
    pub async fn download_to_file(&self, xet_hash: &str, file_size: u64, dest: &Path) -> Result<()> {
        let file_info = XetFileInfo::new(xet_hash.to_string(), file_size);
        self.session
            .download_file(&file_info, dest, None)
            .await
            .map_err(|e| Error::Xet(e.to_string()))?;
        Ok(())
    }

    /// Upload multiple files in a single session.
    /// Returns one XetFileInfo per file, in the same order.
    pub async fn upload_files(&self, paths: &[&Path]) -> Result<Vec<XetFileInfo>> {
        let config = self
            .upload_config
            .as_ref()
            .ok_or_else(|| Error::Hub("no upload config (read-only mode)".into()))?;

        let upload_session = FileUploadSession::new(config.clone(), None)
            .await
            .map_err(|e| Error::Xet(e.to_string()))?;

        // Pass a dummy (all-zeros) SHA-256: bucket uploads don't need a real SHA-256
        // in shard metadata (sha_index GSI is only used for LFS pointer resolution).
        // Using ProvidedValue skips the SHA-256 computation in the upload pipeline.
        // Wait for https://github.com/huggingface/xet-core/pull/679 to avoid the need for this hack.
        let files: Vec<_> = paths
            .iter()
            .map(|p| (p.to_path_buf(), Some(mdb_shard::Sha256::default())))
            .collect();

        let results = upload_session
            .upload_files(files)
            .await
            .map_err(|e| Error::Xet(e.to_string()))?;

        upload_session.finalize().await.map_err(|e| Error::Xet(e.to_string()))?;

        Ok(results)
    }

    /// Create a new StreamingWriter that streams data directly to CAS.
    pub async fn create_streaming_writer(&self) -> Result<StreamingWriter> {
        let config = self
            .upload_config
            .as_ref()
            .ok_or_else(|| Error::Hub("no upload config (read-only mode)".into()))?;
        let session = FileUploadSession::new(config.clone(), None)
            .await
            .map_err(|e| Error::Xet(e.to_string()))?;
        // Pass a dummy (all-zeros) SHA-256: bucket uploads don't need a real SHA-256
        // in shard metadata (sha_index GSI is only used for LFS pointer resolution).
        // Using ProvidedValue skips the SHA-256 computation in the upload pipeline.
        // Wait for https://github.com/huggingface/xet-core/pull/679 to avoid the need for this hack.
        let cleaner = session.start_clean(None, 0, Some(mdb_shard::Sha256::default())).await;
        Ok(StreamingWriter {
            cleaner,
            session,
            bytes_written: 0,
        })
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
/// Each write() call feeds data incrementally (no full-file buffering).
/// finish() finalizes the upload session and returns the file info.
pub struct StreamingWriter {
    cleaner: SingleFileCleaner,
    session: Arc<FileUploadSession>,
    bytes_written: u64,
}

impl StreamingWriter {
    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        self.cleaner
            .add_data(data)
            .await
            .map_err(|e| Error::Xet(e.to_string()))?;
        self.bytes_written += data.len() as u64;
        Ok(())
    }

    pub fn len(&self) -> u64 {
        self.bytes_written
    }

    pub fn is_empty(&self) -> bool {
        self.bytes_written == 0
    }

    /// Finish the upload: finalize the cleaner, commit xorbs, return file info.
    pub async fn finish(self) -> Result<XetFileInfo> {
        let (info, _metrics) = self.cleaner.finish().await.map_err(|e| Error::Xet(e.to_string()))?;
        self.session.finalize().await.map_err(|e| Error::Xet(e.to_string()))?;
        Ok(info)
    }
}
