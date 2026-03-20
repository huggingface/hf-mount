use std::io::SeekFrom;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use tokio::fs::File as TokioFile;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt};
use tracing::info;
use xet_client::cas_client::Client;
use xet_core_structures::merklehash::MerkleHash;
use xet_data::file_reconstruction::DownloadStream;
use xet_data::processing::configurations::TranslatorConfig;
use xet_data::processing::{
    DirtyInput, FileDownloadSession, FileUploadSession, Sha256Policy, SingleFileCleaner, XetFileInfo,
};

use crate::error::{Error, Result};
use crate::virtual_fs::inode::SparseWriteState;

// ── Traits ───────────────────────────────────────────────────────────

/// Trait abstracting CAS operations used by VirtualFs and FlushManager.
#[async_trait::async_trait]
pub trait XetOps: Send + Sync {
    async fn create_streaming_writer(&self) -> Result<Box<dyn StreamingWriterOps>>;
    async fn download_to_file(&self, xet_hash: &str, file_size: u64, dest: &Path) -> Result<()>;
    async fn upload_files(&self, paths: &[&Path]) -> Result<Vec<XetFileInfo>>;
    async fn download_stream_boxed(
        &self,
        file_info: &XetFileInfo,
        offset: u64,
        end: Option<u64>,
    ) -> Result<Box<dyn DownloadStreamOps>>;
    /// Pre-warm the reconstruction cache for a file by fetching its full plan.
    /// Errors are silently ignored — this is best-effort.
    async fn warm_reconstruction_cache(&self, xet_hash: &str);

    /// Upload only the modified portion of a sparse file, composing the CAS reconstruction
    /// plan from existing segments (prefix/suffix) + newly uploaded segments (dirty range).
    /// Returns the combined file info (hash, total_size).
    async fn range_upload(
        &self,
        sparse_state: &SparseWriteState,
        staging_path: &Path,
        file_size: u64,
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
    session: Arc<FileDownloadSession>,
    /// Session without chunk cache for bounded range downloads (random reads, sparse hole fills).
    /// The chunk cache downloads full xorbs (~64MB) even for small range requests, which is
    /// wasteful for seeks and random access patterns.
    nocache_session: Arc<FileDownloadSession>,
    upload_config: Option<Arc<TranslatorConfig>>,
    /// Kept separately from `session` for bounded range downloads via `FileReconstructor`.
    cas_client: Arc<dyn Client>,
}

impl XetSessions {
    pub fn new(
        session: Arc<FileDownloadSession>,
        nocache_session: Arc<FileDownloadSession>,
        upload_config: Option<Arc<TranslatorConfig>>,
        cas_client: Arc<dyn Client>,
    ) -> Arc<Self> {
        Arc::new(Self {
            session,
            nocache_session,
            upload_config,
            cas_client,
        })
    }

    /// Start a streaming download for a byte range.
    /// When `end` is `Some`, only bytes `[offset, end)` are fetched (bounded range).
    /// When `end` is `None`, fetches from `offset` to end of file (unbounded stream).
    pub async fn download_stream(
        &self,
        file_info: &XetFileInfo,
        offset: u64,
        end: Option<u64>,
    ) -> Result<DownloadStream> {
        let range: Option<Range<u64>> = if offset == 0 && end.is_none() {
            None
        } else {
            let range_end = end.unwrap_or(file_info.file_size());
            Some(offset..range_end)
        };

        // Use the no-cache session for bounded reads (random access, sparse hole fills).
        // The chunk cache fetches full xorbs (~64MB) even for small ranges, which is
        // wasteful for seeks. Unbounded/full-file reads benefit from the cache.
        let session = if end.is_some() {
            &self.nocache_session
        } else {
            &self.session
        };
        let (_id, stream) = session
            .download_stream(file_info, range)
            .await
            .map_err(|e| Error::Xet(e.to_string()))?;
        Ok(stream)
    }
}

#[async_trait::async_trait]
impl XetOps for XetSessions {
    async fn create_streaming_writer(&self) -> Result<Box<dyn StreamingWriterOps>> {
        let config = self
            .upload_config
            .as_ref()
            .ok_or_else(|| Error::Hub("no upload config (read-only mode)".into()))?;
        let session = FileUploadSession::new(config.clone())
            .await
            .map_err(|e| Error::Xet(e.to_string()))?;
        let (_id, cleaner) = session
            .start_clean(None, None, Sha256Policy::Skip)
            .map_err(|e| Error::Xet(e.to_string()))?;
        Ok(Box::new(StreamingWriter {
            cleaner,
            session,
            bytes_written: 0,
        }))
    }

    async fn download_to_file(&self, xet_hash: &str, file_size: u64, dest: &Path) -> Result<()> {
        let file_info = XetFileInfo::new(xet_hash.to_string(), file_size);
        self.session
            .download_file(&file_info, dest)
            .await
            .map_err(|e| Error::Xet(e.to_string()))?;
        Ok(())
    }

    async fn upload_files(&self, paths: &[&Path]) -> Result<Vec<XetFileInfo>> {
        let config = self
            .upload_config
            .as_ref()
            .ok_or_else(|| Error::Hub("no upload config (read-only mode)".into()))?;

        let upload_session = FileUploadSession::new(config.clone())
            .await
            .map_err(|e| Error::Xet(e.to_string()))?;

        let files: Vec<_> = paths.iter().map(|p| (p.to_path_buf(), Sha256Policy::Skip)).collect();

        let results = upload_session
            .upload_files(files)
            .await
            .map_err(|e| Error::Xet(e.to_string()))?;

        upload_session.finalize().await.map_err(|e| Error::Xet(e.to_string()))?;

        Ok(results)
    }

    async fn download_stream_boxed(
        &self,
        file_info: &XetFileInfo,
        offset: u64,
        end: Option<u64>,
    ) -> Result<Box<dyn DownloadStreamOps>> {
        let stream = self.download_stream(file_info, offset, end).await?;
        Ok(Box::new(DownloadStreamWrapper(stream)))
    }

    async fn warm_reconstruction_cache(&self, xet_hash: &str) {
        if let Ok(hash) = MerkleHash::from_hex(xet_hash) {
            let _ = self.cas_client.get_reconstruction(&hash, None).await;
        }
    }

    async fn range_upload(
        &self,
        sparse_state: &SparseWriteState,
        staging_path: &Path,
        file_size: u64,
    ) -> Result<XetFileInfo> {
        let config = self
            .upload_config
            .as_ref()
            .ok_or_else(|| Error::Hub("no upload config (read-only mode)".into()))?;

        let original_hash = MerkleHash::from_hex(&sparse_state.original_hash)
            .map_err(|e| Error::Xet(format!("invalid original hash: {e}")))?;

        if sparse_state.dirty_ranges.is_empty() && file_size == sparse_state.original_size {
            return Ok(XetFileInfo::new(
                sparse_state.original_hash.clone(),
                sparse_state.original_size,
            ));
        }

        // Build DirtyInput for each dirty range, reading from the staging file.
        let mut dirty_inputs = Vec::with_capacity(sparse_state.dirty_ranges.len());
        for &(start, end) in &sparse_state.dirty_ranges {
            let mut file = TokioFile::open(staging_path).await.map_err(Error::Io)?;
            file.seek(SeekFrom::Start(start)).await.map_err(Error::Io)?;
            let reader: Pin<Box<dyn AsyncRead + Send>> = Box::pin(file.take(end - start));
            dirty_inputs.push(DirtyInput {
                range: start..end,
                reader,
            });
        }

        let result = xet_data::processing::upload_ranges(
            config.clone(),
            self.cas_client.clone(),
            original_hash,
            sparse_state.original_size,
            dirty_inputs,
            file_size,
        )
        .await
        .map_err(|e| Error::Xet(e.to_string()))?;

        info!(
            "range_upload: hash={} size={} (original={}, {} dirty ranges)",
            result.hash(),
            result.file_size(),
            sparse_state.original_size,
            sparse_state.dirty_ranges.len()
        );

        Ok(result)
    }
}

// ── DownloadStreamWrapper ─────────────────────────────────────────────

struct DownloadStreamWrapper(DownloadStream);

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
        let (info, _, _metrics) = self.cleaner.finish().await.map_err(|e| Error::Xet(e.to_string()))?;
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
