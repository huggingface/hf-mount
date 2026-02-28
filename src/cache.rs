use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use data::configurations::TranslatorConfig;
use data::{FileDownloadSession, FileUploadSession, XetFileInfo};
use tokio::sync::RwLock;

use crate::error::{Error, Result};

pub struct FileCache {
    cache_dir: PathBuf,
    staging_dir: PathBuf,
    session: Arc<FileDownloadSession>,
    upload_config: Option<Arc<TranslatorConfig>>,
    cached: RwLock<HashMap<String, PathBuf>>,
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
            cache_dir,
            staging_dir,
            session,
            upload_config,
            cached: RwLock::new(HashMap::new()),
        }
    }

    /// Ensure a file is downloaded to the local cache. Returns path to cached file.
    pub async fn ensure_cached(&self, xet_hash: &str, file_size: u64) -> Result<PathBuf> {
        // Check if already cached
        {
            let cached = self.cached.read().await;
            if let Some(path) = cached.get(xet_hash)
                && path.exists()
            {
                return Ok(path.clone());
            }
        }

        // Download to cache
        let file_info = XetFileInfo::new(xet_hash.to_string(), file_size);
        let cache_path = self.cache_dir.join(xet_hash);

        self.session
            .download_file(&file_info, &cache_path, None)
            .await
            .map_err(|e| Error::Xet(e.to_string()))?;

        let mut cached = self.cached.write().await;
        cached.insert(xet_hash.to_string(), cache_path.clone());

        Ok(cache_path)
    }

    /// Get the staging path for a given inode (for files being written).
    pub fn staging_path(&self, inode: u64) -> PathBuf {
        self.staging_dir.join(format!("inode_{}", inode))
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

        upload_session
            .finalize()
            .await
            .map_err(|e| Error::Xet(e.to_string()))?;

        Ok(results)
    }

    pub fn download_session(&self) -> &Arc<FileDownloadSession> {
        &self.session
    }
}
