use std::path::{Path, PathBuf};
use std::sync::Arc;

use data::configurations::TranslatorConfig;
use data::{FileDownloadSession, FileUploadSession, XetFileInfo};

use crate::error::{Error, Result};

fn rand_u64() -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::time::Instant::now().hash(&mut hasher);
    std::process::id().hash(&mut hasher);
    hasher.finish()
}

pub struct FileCache {
    staging_dir: PathBuf,
    session: Arc<FileDownloadSession>,
    upload_config: Option<Arc<TranslatorConfig>>,
    /// Per-session random key to make staging paths unpredictable.
    session_key: u64,
}

impl FileCache {
    pub fn new(
        cache_dir: PathBuf,
        session: Arc<FileDownloadSession>,
        upload_config: Option<Arc<TranslatorConfig>>,
    ) -> Self {
        std::fs::create_dir_all(&cache_dir).ok();
        let staging_dir = cache_dir.join("staging");
        std::fs::create_dir_all(&staging_dir).ok();
        Self {
            staging_dir,
            session,
            upload_config,
            session_key: rand_u64(),
        }
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

    /// Get the staging path for a given inode (for files being written).
    /// Deterministic within a session but unpredictable from outside.
    pub fn staging_path(&self, inode: u64) -> PathBuf {
        self.staging_dir
            .join(format!("ino_{:x}_{:016x}", inode, self.session_key))
    }

    /// Upload multiple staged files in a single session.
    /// Returns one XetFileInfo per file, in the same order.
    pub async fn upload_files(&self, staged_paths: &[&Path]) -> Result<Vec<XetFileInfo>> {
        let config = self
            .upload_config
            .as_ref()
            .ok_or_else(|| Error::Hub("no upload config (read-only mode)".into()))?;

        let upload_session = FileUploadSession::new(config.clone(), None)
            .await
            .map_err(|e| Error::Xet(e.to_string()))?;

        let files: Vec<_> = staged_paths
            .iter()
            .map(|p| (p.to_path_buf(), None::<mdb_shard::Sha256>))
            .collect();

        let results = upload_session
            .upload_files(files)
            .await
            .map_err(|e| Error::Xet(e.to_string()))?;

        upload_session.finalize().await.map_err(|e| Error::Xet(e.to_string()))?;

        Ok(results)
    }

    pub fn download_session(&self) -> &Arc<FileDownloadSession> {
        &self.session
    }
}
