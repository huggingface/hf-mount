use std::ffi::{CStr, CString};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use ulid::Ulid;
use xet_client::cas_client::Client;
use xet_client::cas_types::FileRange;
use xet_core_structures::merklehash::MerkleHash;
use xet_data::file_reconstruction::{DownloadStream, FileReconstructor};
use xet_data::processing::configurations::TranslatorConfig;
use xet_data::processing::file_cleaner::Sha256Policy;
use xet_data::processing::{FileDownloadSession, FileUploadSession, SingleFileCleaner, XetFileInfo};

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

// ── XetSessions ───────────────────────────────────────────────────────

/// Core xet-core sessions for CAS downloads and uploads.
/// Used by all write modes (simple streaming + advanced staging).
pub struct XetSessions {
    session: Arc<FileDownloadSession>,
    upload_config: Option<Arc<TranslatorConfig>>,
    /// Kept separately from `session` for bounded range downloads via `FileReconstructor`.
    cas_client: Arc<dyn Client>,
}

impl XetSessions {
    pub fn new(
        session: Arc<FileDownloadSession>,
        upload_config: Option<Arc<TranslatorConfig>>,
        cas_client: Arc<dyn Client>,
    ) -> Arc<Self> {
        Arc::new(Self {
            session,
            upload_config,
            cas_client,
        })
    }

    /// Start a streaming download for a byte range.
    /// When `end` is `Some`, only bytes `[offset, end)` are fetched (bounded range).
    /// When `end` is `None`, fetches from `offset` to end of file (unbounded stream).
    pub fn download_stream(&self, file_info: &XetFileInfo, offset: u64, end: Option<u64>) -> Result<DownloadStream> {
        match end {
            None => self
                .session
                .download_stream_from_offset(file_info, offset, Ulid::new())
                .map_err(|e| Error::Xet(e.to_string())),
            Some(end) => {
                let hash = file_info
                    .merkle_hash()
                    .map_err(|e| Error::Xet(format!("invalid hash: {e}")))?;
                // No chunk cache for bounded range downloads: the xorb disk cache
                // downloads the full xorb (~64MB) even for a 256K range request,
                // which is wasteful for random reads. Sequential reads use the
                // unbounded stream path above which benefits from the cache.
                let reconstructor = FileReconstructor::new(&self.cas_client, hash)
                    .with_file_size(file_info.file_size())
                    .with_byte_range(FileRange::new(offset, end));
                Ok(reconstructor.reconstruct_to_stream())
            }
        }
    }
}

#[async_trait::async_trait]
impl XetOps for XetSessions {
    async fn create_streaming_writer(&self) -> Result<Box<dyn StreamingWriterOps>> {
        let config = self
            .upload_config
            .as_ref()
            .ok_or_else(|| Error::hub("no upload config (read-only mode)"))?;
        let session = FileUploadSession::new(config.clone(), None)
            .await
            .map_err(|e| Error::Xet(e.to_string()))?;
        let cleaner = session.start_clean(None, None, Sha256Policy::Skip, Ulid::new()).await;
        Ok(Box::new(StreamingWriter {
            cleaner,
            session,
            bytes_written: 0,
        }))
    }

    async fn download_to_file(&self, xet_hash: &str, file_size: u64, dest: &Path) -> Result<()> {
        let file_info = XetFileInfo::new(xet_hash.to_string(), file_size);
        self.session
            .download_file(&file_info, dest, Ulid::new())
            .await
            .map_err(|e| Error::Xet(e.to_string()))?;
        Ok(())
    }

    async fn upload_files(&self, paths: &[&Path]) -> Result<Vec<XetFileInfo>> {
        let config = self
            .upload_config
            .as_ref()
            .ok_or_else(|| Error::hub("no upload config (read-only mode)"))?;

        let upload_session = FileUploadSession::new(config.clone(), None)
            .await
            .map_err(|e| Error::Xet(e.to_string()))?;

        let files: Vec<_> = paths.iter().map(|p| (p.to_path_buf(), None, Ulid::new())).collect();

        let results = upload_session
            .upload_files(files)
            .await
            .map_err(|e| Error::Xet(e.to_string()))?;

        upload_session.finalize().await.map_err(|e| Error::Xet(e.to_string()))?;

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
        self.0.next().await.map_err(|e| Error::Xet(e.to_string()))
    }
}

// ── StagingDir ────────────────────────────────────────────────────────

/// On-disk staging area for advanced writes (random seek, read-modify-write).
///
/// Shared via Arc between VFS and FlushManager instead.
pub struct StagingDir {
    dir: PathBuf,
    /// Per-session random key to make staging paths unpredictable.
    session_key: u64,
}

#[derive(Debug, Clone)]
pub struct OverlayDirEntry {
    pub name: String,
    pub is_dir: bool,
    pub is_symlink: bool,
    pub size: u64,
    pub mtime: SystemTime,
    pub mode: u16,
}

impl StagingDir {
    pub fn new(cache_dir: &Path) -> Self {
        let dir = cache_dir.join("staging");
        std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("Failed to create staging dir {:?}: {e}", dir));
        Self {
            dir,
            session_key: rand_u64(),
        }
    }

    /// Root directory of the staging area.
    pub fn root(&self) -> &Path {
        &self.dir
    }

    /// Inode-based staging path (used by FlushManager).
    pub fn path(&self, inode: u64) -> PathBuf {
        self.dir.join(format!("ino_{:x}_{:016x}", inode, self.session_key))
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

/// Overlay-local writable backing rooted at the pre-mount directory fd.
pub struct OverlayBacking {
    /// Kept alive so the pre-mount directory remains reachable and, on macOS,
    /// provides the dirfd used by fd-relative helper calls.
    dirfd: std::fs::File,
}

impl OverlayBacking {
    pub fn new(fd: std::fs::File) -> Self {
        Self { dirfd: fd }
    }

    pub fn exists(&self, full_path: &str) -> std::io::Result<bool> {
        let rel = Self::validate_rel_path(full_path)?;
        if rel.as_os_str().is_empty() {
            return Ok(true);
        }
        let path = path_to_cstring(rel)?;
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        let rc = unsafe {
            libc::fstatat(
                self.dirfd(),
                path.as_ptr(),
                stat.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if rc == 0 {
            return Ok(true);
        }
        let err = std::io::Error::last_os_error();
        if err.kind() == std::io::ErrorKind::NotFound {
            Ok(false)
        } else {
            Err(err)
        }
    }

    pub fn open_file(
        &self,
        full_path: &str,
        read: bool,
        write: bool,
        create: bool,
        truncate: bool,
    ) -> std::io::Result<std::fs::File> {
        let rel = Self::validate_rel_path(full_path)?;
        let path = path_to_cstring(rel)?;
        let access_mode = match (read, write) {
            (true, true) => libc::O_RDWR,
            (true, false) => libc::O_RDONLY,
            (false, true) => libc::O_WRONLY,
            (false, false) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "overlay open requires read or write access",
                ));
            }
        };
        let mut flags = access_mode;
        if create {
            flags |= libc::O_CREAT;
        }
        if truncate {
            flags |= libc::O_TRUNC;
        }
        let fd = unsafe { libc::openat(self.dirfd(), path.as_ptr(), flags, 0o666) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(unsafe { std::fs::File::from_raw_fd(fd) })
    }

    pub fn create_parent_dirs(&self, full_path: &str) -> std::io::Result<()> {
        let rel = Self::validate_rel_path(full_path)?;
        let Some(parent) = rel.parent().filter(|p| !p.as_os_str().is_empty()) else {
            return Ok(());
        };

        let mut current = PathBuf::new();
        for component in parent.components() {
            if let std::path::Component::Normal(part) = component {
                current.push(part);
                let path = path_to_cstring(&current)?;
                let rc = unsafe { libc::mkdirat(self.dirfd(), path.as_ptr(), 0o755) };
                if rc != 0 {
                    let err = std::io::Error::last_os_error();
                    if err.kind() != std::io::ErrorKind::AlreadyExists {
                        return Err(err);
                    }
                }
            }
        }
        Ok(())
    }

    pub fn create_dir(&self, full_path: &str, mode: u16) -> std::io::Result<()> {
        let rel = Self::validate_rel_path(full_path)?;
        let path = path_to_cstring(rel)?;
        let rc = unsafe { libc::mkdirat(self.dirfd(), path.as_ptr(), mode as libc::mode_t) };
        if rc == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    pub fn remove_file(&self, full_path: &str) -> std::io::Result<()> {
        let rel = Self::validate_rel_path(full_path)?;
        let path = path_to_cstring(rel)?;
        let rc = unsafe { libc::unlinkat(self.dirfd(), path.as_ptr(), 0) };
        if rc == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    pub fn remove_dir(&self, full_path: &str) -> std::io::Result<()> {
        let rel = Self::validate_rel_path(full_path)?;
        let path = path_to_cstring(rel)?;
        let rc = unsafe { libc::unlinkat(self.dirfd(), path.as_ptr(), libc::AT_REMOVEDIR) };
        if rc == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    pub fn rename(&self, old_path: &str, new_path: &str) -> std::io::Result<()> {
        let old_rel = Self::validate_rel_path(old_path)?;
        let new_rel = Self::validate_rel_path(new_path)?;
        let old_cstr = path_to_cstring(old_rel)?;
        let new_cstr = path_to_cstring(new_rel)?;
        let rc = unsafe { libc::renameat(self.dirfd(), old_cstr.as_ptr(), self.dirfd(), new_cstr.as_ptr()) };
        if rc == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    pub fn read_dir(&self, full_path: &str) -> std::io::Result<Vec<OverlayDirEntry>> {
        let rel = Self::validate_rel_path(full_path)?;
        let fd = if rel.as_os_str().is_empty() {
            let dup_fd = unsafe { libc::fcntl(self.dirfd(), libc::F_DUPFD_CLOEXEC, 0) };
            if dup_fd < 0 {
                return Err(std::io::Error::last_os_error());
            }
            dup_fd
        } else {
            let path = path_to_cstring(rel)?;
            let fd = unsafe { libc::openat(self.dirfd(), path.as_ptr(), libc::O_RDONLY | libc::O_DIRECTORY) };
            if fd < 0 {
                return Err(std::io::Error::last_os_error());
            }
            fd
        };

        let dir = unsafe { libc::fdopendir(fd) };
        if dir.is_null() {
            let err = std::io::Error::last_os_error();
            unsafe {
                libc::close(fd);
            }
            return Err(err);
        }

        struct DirGuard(*mut libc::DIR);
        impl Drop for DirGuard {
            fn drop(&mut self) {
                unsafe {
                    libc::closedir(self.0);
                }
            }
        }

        let guard = DirGuard(dir);
        let dir_fd = unsafe { libc::dirfd(guard.0) };
        let mut entries = Vec::new();

        loop {
            clear_errno();
            let entry = unsafe { libc::readdir(guard.0) };
            if entry.is_null() {
                if current_errno() == 0 {
                    break;
                }
                return Err(std::io::Error::last_os_error());
            }

            let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }
                .to_string_lossy()
                .into_owned();
            if name == "." || name == ".." {
                continue;
            }

            let name_cstr = CString::new(name.as_bytes())
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "overlay entry contains NUL"))?;
            let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
            let rc = unsafe { libc::fstatat(dir_fd, name_cstr.as_ptr(), stat.as_mut_ptr(), libc::AT_SYMLINK_NOFOLLOW) };
            if rc != 0 {
                continue;
            }
            let stat = unsafe { stat.assume_init() };
            let kind = stat.st_mode & libc::S_IFMT;
            entries.push(OverlayDirEntry {
                name,
                is_dir: kind == libc::S_IFDIR,
                is_symlink: kind == libc::S_IFLNK,
                size: stat.st_size as u64,
                mtime: stat_mtime(&stat),
                mode: (stat.st_mode & 0o777) as u16,
            });
        }

        Ok(entries)
    }

    fn validate_rel_path(full_path: &str) -> std::io::Result<&Path> {
        let path = Path::new(full_path);
        if path.has_root()
            || path.components().any(|component| {
                matches!(
                    component,
                    std::path::Component::ParentDir | std::path::Component::Prefix(_)
                )
            })
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("overlay path must be a safe relative path, got: {full_path:?}"),
            ));
        }
        Ok(path)
    }

    fn dirfd(&self) -> libc::c_int {
        self.dirfd.as_raw_fd()
    }
}

fn path_to_cstring(path: &Path) -> std::io::Result<CString> {
    CString::new(path.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "overlay path contains NUL"))
}

fn stat_mtime(stat: &libc::stat) -> SystemTime {
    if stat.st_mtime < 0 {
        UNIX_EPOCH
    } else {
        UNIX_EPOCH + Duration::new(stat.st_mtime as u64, stat.st_mtime_nsec as u32)
    }
}

#[cfg(target_os = "linux")]
fn errno_location() -> *mut libc::c_int {
    unsafe { libc::__errno_location() }
}

#[cfg(target_os = "macos")]
fn errno_location() -> *mut libc::c_int {
    unsafe { libc::__error() }
}

fn clear_errno() {
    unsafe {
        *errno_location() = 0;
    }
}

fn current_errno() -> libc::c_int {
    unsafe { *errno_location() }
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
