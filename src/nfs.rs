use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use nfsserve::nfs::{
    fattr3, fileid3, filename3, ftype3, nfspath3, nfsstat3, nfsstring, nfstime3, sattr3, set_atime, set_gid3,
    set_mode3, set_mtime, set_size3, set_uid3, specdata3,
};
use nfsserve::tcp::{NFSTcp, NFSTcpListener};
use nfsserve::vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities};
use tracing::info;

use crate::virtual_fs::inode::InodeKind;
use crate::virtual_fs::{VirtualFs, VirtualFsAttr};

// ── NFS Adapter ────────────────────────────────────────────────────────

pub struct NFSAdapter {
    virtual_fs: Arc<VirtualFs>,
    handle_pool: Arc<Mutex<HandlePool>>,
    read_only: bool,
}

impl NFSAdapter {
    pub fn new(virtual_fs: Arc<VirtualFs>, read_only: bool) -> Self {
        Self {
            virtual_fs,
            handle_pool: Arc::new(Mutex::new(HandlePool::new())),
            read_only,
        }
    }

    /// Evict a handle: flush dirty data, then release.
    async fn evict_handle(&self, ino: u64, file_handle: u64) {
        // Flush commits any buffered writes to CAS+Hub before releasing.
        let _ = self.virtual_fs.flush(ino, file_handle, None).await;
        if let Err(e) = self.virtual_fs.release(file_handle).await {
            tracing::error!("NFS evict_handle: release failed for ino={}: errno={}", ino, e);
        }
    }

    /// Get or open a file handle from the pool.
    async fn get_or_open_handle(&self, ino: u64) -> Result<u64, nfsstat3> {
        // Check pool first (quick lock)
        if let Some(file_handle) = self.handle_pool.lock().expect("handle_pool poisoned").get(ino) {
            return Ok(file_handle);
        }
        // Pool miss: open file (may await download)
        let file_handle = self
            .virtual_fs
            .open(ino, false, false, None)
            .await
            .map_err(errno_to_nfs)?;
        // Insert into pool (evict LRU if full)
        let (evicted, dup_handle) = {
            let mut pool = self.handle_pool.lock().expect("handle_pool poisoned");
            // Double-check: another task may have opened it concurrently
            if let Some(existing) = pool.get(ino) {
                (None, Some((existing, file_handle)))
            } else {
                (pool.insert(ino, file_handle), None)
            }
        };
        // Release duplicate handle outside the lock (release is async).
        // This is a freshly-opened read-only handle, so errors are non-critical.
        if let Some((existing, dup)) = dup_handle {
            let _ = self.virtual_fs.release(dup).await;
            return Ok(existing);
        }
        if let Some((evicted_ino, evicted_handle)) = evicted {
            self.evict_handle(evicted_ino, evicted_handle).await;
        }
        Ok(file_handle)
    }

    /// Create a file and insert the handle into the pool.
    async fn create_file(
        &self,
        dirid: fileid3,
        filename: &filename3,
        mode: u16,
        uid: u32,
        gid: u32,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let name = nfs_name(filename)?;
        let (attr, file_handle) = self
            .virtual_fs
            .create(dirid, name, mode, uid, gid, None)
            .await
            .map_err(errno_to_nfs)?;
        let ino = attr.ino;
        let fattr = vfs_attr_to_nfs(&attr);
        self.insert_handle(ino, file_handle).await;
        Ok((ino, fattr))
    }

    /// Insert a writable handle into the pool (used after create).
    async fn insert_handle(&self, ino: u64, file_handle: u64) {
        let evicted = {
            let mut pool = self.handle_pool.lock().expect("handle_pool poisoned");
            pool.insert(ino, file_handle)
        };
        if let Some((evicted_ino, evicted_handle)) = evicted {
            self.evict_handle(evicted_ino, evicted_handle).await;
        }
    }
}

#[async_trait]
impl NFSFileSystem for NFSAdapter {
    fn root_dir(&self) -> fileid3 {
        1
    }

    fn capabilities(&self) -> VFSCapabilities {
        if self.read_only {
            VFSCapabilities::ReadOnly
        } else {
            VFSCapabilities::ReadWrite
        }
    }

    async fn lookup(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        let name = nfs_name(filename).map_err(|_| nfsstat3::NFS3ERR_NOENT)?;
        self.virtual_fs
            .lookup(dirid, name)
            .await
            .map(|a| a.ino)
            .map_err(errno_to_nfs)
    }

    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        self.virtual_fs
            .getattr(id)
            .map(|a| vfs_attr_to_nfs(&a))
            .map_err(errno_to_nfs)
    }

    async fn read(&self, id: fileid3, offset: u64, count: u32) -> Result<(Vec<u8>, bool), nfsstat3> {
        let file_handle = self.get_or_open_handle(id).await?;
        self.virtual_fs
            .read(file_handle, offset, count)
            .await
            .map(|(b, eof)| (b.to_vec(), eof))
            .map_err(errno_to_nfs)
    }

    async fn readdir(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        let entries = self.virtual_fs.readdir(dirid).await.map_err(errno_to_nfs)?;
        let skip = if start_after > 0 {
            entries
                .iter()
                .position(|e| e.ino == start_after)
                .map(|i| i + 1)
                .unwrap_or(0)
        } else {
            0
        };
        let page: Vec<DirEntry> = entries[skip..]
            .iter()
            .take(max_entries)
            .map(|e| {
                let attr = self
                    .virtual_fs
                    .getattr(e.ino)
                    .map(|a| vfs_attr_to_nfs(&a))
                    .unwrap_or_default();
                DirEntry {
                    fileid: e.ino,
                    name: e.name.clone().into_bytes().into(),
                    attr,
                }
            })
            .collect();
        let end = skip + page.len() >= entries.len();
        Ok(ReadDirResult { entries: page, end })
    }

    // ── Write operations ────────────────────────────────────────────────

    async fn setattr(&self, id: fileid3, setattr: sattr3) -> Result<fattr3, nfsstat3> {
        let size = match setattr.size {
            set_size3::size(s) => Some(s),
            set_size3::Void => None,
        };
        let mode = match setattr.mode {
            set_mode3::mode(m) => Some((m & 0o7777) as u16),
            set_mode3::Void => None,
        };
        let uid = match setattr.uid {
            set_uid3::uid(u) => Some(u),
            set_uid3::Void => None,
        };
        let gid = match setattr.gid {
            set_gid3::gid(g) => Some(g),
            set_gid3::Void => None,
        };
        let atime = match setattr.atime {
            set_atime::SET_TO_CLIENT_TIME(t) => Some(nfstime_to_system_time(t)),
            set_atime::SET_TO_SERVER_TIME => Some(SystemTime::now()),
            set_atime::DONT_CHANGE => None,
        };
        let mtime = match setattr.mtime {
            set_mtime::SET_TO_CLIENT_TIME(t) => Some(nfstime_to_system_time(t)),
            set_mtime::SET_TO_SERVER_TIME => Some(SystemTime::now()),
            set_mtime::DONT_CHANGE => None,
        };
        self.virtual_fs
            .setattr(id, size, mode, uid, gid, atime, mtime)
            .await
            .map(|a| vfs_attr_to_nfs(&a))
            .map_err(errno_to_nfs)
    }

    async fn write(&self, id: fileid3, offset: u64, data: &[u8]) -> Result<fattr3, nfsstat3> {
        // Write requires a handle already in the pool (from create).
        let file_handle = self
            .handle_pool
            .lock()
            .expect("handle_pool poisoned")
            .get(id)
            .ok_or(nfsstat3::NFS3ERR_STALE)?;
        // NFS always uses advanced_writes (staging files), so write() is a
        // synchronous pwrite() — safe to call from async context.
        self.virtual_fs
            .write(id, file_handle, offset, data)
            .map_err(errno_to_nfs)?;
        self.virtual_fs
            .getattr(id)
            .map(|a| vfs_attr_to_nfs(&a))
            .map_err(errno_to_nfs)
    }

    async fn create(&self, dirid: fileid3, filename: &filename3, attr: sattr3) -> Result<(fileid3, fattr3), nfsstat3> {
        let mode = match attr.mode {
            set_mode3::mode(m) => (m & 0o7777) as u16,
            set_mode3::Void => 0o644,
        };
        let uid = match attr.uid {
            set_uid3::uid(u) => u,
            set_uid3::Void => self.virtual_fs.default_uid(),
        };
        let gid = match attr.gid {
            set_gid3::gid(g) => g,
            set_gid3::Void => self.virtual_fs.default_gid(),
        };
        let (ino, fattr) = self.create_file(dirid, filename, mode, uid, gid).await?;
        Ok((ino, fattr))
    }

    async fn create_exclusive(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        let (ino, _) = self
            .create_file(
                dirid,
                filename,
                0o644,
                self.virtual_fs.default_uid(),
                self.virtual_fs.default_gid(),
            )
            .await?;
        Ok(ino)
    }

    async fn mkdir(&self, dirid: fileid3, dirname: &filename3) -> Result<(fileid3, fattr3), nfsstat3> {
        let name = nfs_name(dirname)?;
        let attr = self
            .virtual_fs
            .mkdir(
                dirid,
                name,
                0o755,
                self.virtual_fs.default_uid(),
                self.virtual_fs.default_gid(),
            )
            .await
            .map_err(errno_to_nfs)?;
        Ok((attr.ino, vfs_attr_to_nfs(&attr)))
    }

    async fn remove(&self, dirid: fileid3, filename: &filename3) -> Result<(), nfsstat3> {
        let name = nfs_name(filename)?;
        // Check whether this is a file or directory.
        let child_ino = self.virtual_fs.lookup(dirid, name).await.map_err(errno_to_nfs)?.ino;
        let attr = self.virtual_fs.getattr(child_ino).map_err(errno_to_nfs)?;
        match attr.kind {
            InodeKind::Directory => self.virtual_fs.rmdir(dirid, name).await.map_err(errno_to_nfs),
            _ => self.virtual_fs.unlink(dirid, name).await.map_err(errno_to_nfs),
        }
    }

    async fn rename(
        &self,
        from_dirid: fileid3,
        from_filename: &filename3,
        to_dirid: fileid3,
        to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        let from_name = nfs_name(from_filename)?;
        let to_name = nfs_name(to_filename)?;
        self.virtual_fs
            .rename(from_dirid, from_name, to_dirid, to_name, false)
            .await
            .map_err(errno_to_nfs)
    }

    async fn symlink(
        &self,
        dirid: fileid3,
        linkname: &filename3,
        symlink: &nfspath3,
        attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let name = nfs_name(linkname)?;
        let target = std::str::from_utf8(&symlink.0).map_err(|_| nfsstat3::NFS3ERR_INVAL)?;
        let mode = match attr.mode {
            set_mode3::mode(m) => (m & 0o7777) as u16,
            set_mode3::Void => 0o777,
        };
        let uid = match attr.uid {
            set_uid3::uid(u) => u,
            set_uid3::Void => self.virtual_fs.default_uid(),
        };
        let gid = match attr.gid {
            set_gid3::gid(g) => g,
            set_gid3::Void => self.virtual_fs.default_gid(),
        };
        let vfs_attr = self
            .virtual_fs
            .symlink(dirid, name, target, mode, uid, gid)
            .await
            .map_err(errno_to_nfs)?;
        Ok((vfs_attr.ino, vfs_attr_to_nfs(&vfs_attr)))
    }

    async fn readlink(&self, id: fileid3) -> Result<nfspath3, nfsstat3> {
        let target = self.virtual_fs.readlink(id).map_err(errno_to_nfs)?;
        Ok(nfsstring(target.into_bytes()))
    }
}

// ── Mount orchestration ────────────────────────────────────────────────

pub async fn mount_nfs(
    virtual_fs: Arc<VirtualFs>,
    mount_point: &Path,
    metadata_ttl_ms: u64,
    read_only: bool,
) -> std::io::Result<()> {
    let vfs_for_shutdown = virtual_fs.clone();
    let adapter = NFSAdapter::new(virtual_fs, read_only);
    let mut listener = NFSTcpListener::bind("127.0.0.1:0", adapter).await?;
    let port = listener.get_listen_port();
    info!("NFS server listening on 127.0.0.1:{}", port);

    // Register mount/unmount listener: nfsserve sends `false` on UMNT.
    let (mount_tx, mut mount_rx) = tokio::sync::mpsc::channel::<bool>(1);
    listener.set_mount_listener(mount_tx);

    let mount_point_str = mount_point
        .to_str()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid mount path"))?;

    // Start serving NFS requests *before* the mount command, otherwise
    // mount.nfs has nobody to talk to and fails with EIO.
    let server_handle = tokio::spawn(async move {
        if let Err(e) = listener.handle_forever().await {
            tracing::error!("NFS server error: {}", e);
        }
    });

    // Convert ms to seconds (rounding up so 100ms → 1s, not 0s which disables caching entirely)
    let actimeo = metadata_ttl_ms.div_ceil(1000);

    // Platform-specific mount command
    #[cfg(target_os = "macos")]
    {
        let mut opts = format!("nolocks,vers=3,tcp,rsize=1048576,actimeo={actimeo},port={port},mountport={port}");
        if read_only {
            opts = format!("rdonly,{opts}");
        } else {
            opts = format!("{opts},wsize=1048576");
        }
        let status = std::process::Command::new("mount_nfs")
            .args(["-o", &opts, "127.0.0.1:/", mount_point_str])
            .status()?;
        if !status.success() {
            server_handle.abort();
            return Err(std::io::Error::other(format!("mount command failed with {status}")));
        }
    }

    #[cfg(target_os = "linux")]
    {
        let mut mount_opts = format!("nolock,vers=3,tcp,rsize=1048576,actimeo={actimeo},port={port},mountport={port}");
        if !read_only {
            mount_opts = format!("{mount_opts},wsize=1048576");
        }
        let output = if unsafe { libc::getuid() } == 0 {
            std::process::Command::new("mount.nfs")
                .args(["-o", &mount_opts, "127.0.0.1:/", mount_point_str])
                .output()?
        } else {
            std::process::Command::new("sudo")
                .args(["-n", "mount.nfs", "-o", &mount_opts, "127.0.0.1:/", mount_point_str])
                .output()?
        };
        if !output.status.success() {
            server_handle.abort();
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(std::io::Error::other(format!(
                "mount.nfs failed with {}: stdout={stdout} stderr={stderr}",
                output.status
            )));
        }
    }

    info!("NFS mount active at {}", mount_point_str);

    // Wait for unmount signal, server exit, or Ctrl+C.
    // nfsserve sends `true` on MNT and `false` on UMNT — ignore mount events.
    // handle_forever() is an infinite accept() loop that never returns on its own.
    // On Linux, `umount` doesn't always send the UMNT RPC, so we also poll
    // /proc/mounts as a fallback to detect when the mount disappears.
    let mut sigterm =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("Failed to register SIGTERM");
    tokio::pin!(server_handle);
    loop {
        tokio::select! {
            msg = mount_rx.recv() => {
                match msg {
                    Some(true) => continue,  // mount event, keep waiting
                    _ => {
                        info!("NFS unmount detected via UMNT, shutting down");
                        break;
                    }
                }
            }
            _ = &mut server_handle => {
                info!("NFS server exited");
                break;
            }
            _ = tokio::signal::ctrl_c() => {
                info!("Received Ctrl+C, unmounting...");
                unmount_nfs(mount_point_str);
                break;
            }
            _ = sigterm.recv() => {
                info!("Received SIGTERM, unmounting...");
                unmount_nfs(mount_point_str);
                break;
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {
                if !is_mounted(mount_point_str) {
                    info!("NFS mount disappeared, shutting down");
                    break;
                }
            }
        }
    }

    vfs_for_shutdown.shutdown();
    Ok(())
}

// ── Handle Pool ────────────────────────────────────────────────────────
//
// NFS v3 is stateless — there is no open/close. Every read arrives with
// just a fileid. Our VFS, however, is stateful: open() allocates a file
// handle that tracks prefetch buffers, staging files, etc.
//
// The handle pool bridges the gap: it caches VFS file handles keyed by
// inode, evicting the least-recently-used entry when full. Eviction
// calls flush() then release() — flush commits dirty write data to
// CAS+Hub, release frees the prefetch buffer. A subsequent read on an
// evicted file simply re-opens it (cold open — prefetch restarts).
//
// Each open handle may hold a prefetch buffer (~8 MB worst case), so the
// pool size caps memory usage at roughly capacity × 8 MB.

const HANDLE_POOL_CAPACITY: usize = 64;

struct HandlePool {
    handles: HashMap<u64, u64>, // ino -> file_handle
    order: VecDeque<u64>,       // ino access order (front = oldest)
}

impl HandlePool {
    fn new() -> Self {
        Self {
            handles: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn get(&mut self, ino: u64) -> Option<u64> {
        if let Some(&file_handle) = self.handles.get(&ino) {
            self.order.retain(|&i| i != ino);
            self.order.push_back(ino);
            Some(file_handle)
        } else {
            None
        }
    }

    /// Insert a handle, returning the evicted (ino, file_handle) if pool was full.
    fn insert(&mut self, ino: u64, file_handle: u64) -> Option<(u64, u64)> {
        let evicted = if self.handles.len() >= HANDLE_POOL_CAPACITY {
            self.order
                .pop_front()
                .and_then(|old_ino| self.handles.remove(&old_ino).map(|old_fh| (old_ino, old_fh)))
        } else {
            None
        };
        self.handles.insert(ino, file_handle);
        self.order.push_back(ino);
        evicted
    }
}

// ── Conversions ────────────────────────────────────────────────────────

fn nfs_name(filename: &filename3) -> Result<&str, nfsstat3> {
    std::str::from_utf8(&filename.0).map_err(|_| nfsstat3::NFS3ERR_INVAL)
}

fn errno_to_nfs(e: i32) -> nfsstat3 {
    match e {
        libc::ENOENT => nfsstat3::NFS3ERR_NOENT,
        libc::EIO => nfsstat3::NFS3ERR_IO,
        libc::EACCES => nfsstat3::NFS3ERR_ACCES,
        libc::EEXIST => nfsstat3::NFS3ERR_EXIST,
        libc::ENOTDIR => nfsstat3::NFS3ERR_NOTDIR,
        libc::EISDIR => nfsstat3::NFS3ERR_ISDIR,
        libc::EINVAL => nfsstat3::NFS3ERR_INVAL,
        libc::EROFS => nfsstat3::NFS3ERR_ROFS,
        libc::ENOTEMPTY => nfsstat3::NFS3ERR_NOTEMPTY,
        libc::EBADF => nfsstat3::NFS3ERR_STALE,
        libc::ENOSPC => nfsstat3::NFS3ERR_NOSPC,
        _ => nfsstat3::NFS3ERR_IO,
    }
}

fn system_time_to_nfstime(t: SystemTime) -> nfstime3 {
    let d = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    nfstime3 {
        seconds: d.as_secs() as u32,
        nseconds: d.subsec_nanos(),
    }
}

/// Check if a path is still an active mount point.
fn unmount_nfs(mount_point: &str) {
    use std::ffi::CString;

    // Try libc unmount first (no external process dependency).
    if let Ok(c_path) = CString::new(mount_point) {
        #[cfg(target_os = "linux")]
        {
            if unsafe { libc::umount2(c_path.as_ptr(), libc::MNT_DETACH) } == 0 {
                return;
            }
        }
        #[cfg(target_os = "macos")]
        {
            if unsafe { libc::unmount(c_path.as_ptr(), libc::MNT_FORCE) } == 0 {
                return;
            }
        }
    }

    // Fallback: external command.
    #[cfg(target_os = "macos")]
    let _ = std::process::Command::new("umount").arg(mount_point).status();
    #[cfg(target_os = "linux")]
    {
        let _ = if unsafe { libc::getuid() } == 0 {
            std::process::Command::new("umount").arg(mount_point).status()
        } else {
            std::process::Command::new("sudo")
                .args(["-n", "umount", mount_point])
                .status()
        };
    }
}

fn is_mounted(path: &str) -> bool {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/mounts")
            .map(|s| s.lines().any(|line| line.split_whitespace().nth(1) == Some(path)))
            .unwrap_or(false)
    }
    #[cfg(target_os = "macos")]
    {
        // On macOS, check via statfs: a mounted NFS will have f_fstypename = "nfs"
        use std::ffi::CString;
        use std::mem::MaybeUninit;
        let c_path = match CString::new(path) {
            Ok(p) => p,
            Err(_) => return false,
        };
        unsafe {
            let mut buf = MaybeUninit::<libc::statfs>::uninit();
            if libc::statfs(c_path.as_ptr(), buf.as_mut_ptr()) == 0 {
                let buf = buf.assume_init();
                let fstype = std::ffi::CStr::from_ptr(buf.f_fstypename.as_ptr());
                fstype.to_bytes() == b"nfs"
            } else {
                false
            }
        }
    }
}

fn vfs_attr_to_nfs(attr: &VirtualFsAttr) -> fattr3 {
    let ftype = match attr.kind {
        InodeKind::File => ftype3::NF3REG,
        InodeKind::Directory => ftype3::NF3DIR,
        InodeKind::Symlink => ftype3::NF3LNK,
    };
    fattr3 {
        ftype,
        mode: attr.perm as u32,
        nlink: attr.nlink,
        uid: attr.uid,
        gid: attr.gid,
        size: attr.size,
        used: attr.blocks * 512,
        rdev: specdata3 {
            specdata1: 0,
            specdata2: 0,
        },
        fsid: 0,
        fileid: attr.ino,
        atime: system_time_to_nfstime(attr.atime),
        mtime: system_time_to_nfstime(attr.mtime),
        ctime: system_time_to_nfstime(attr.ctime),
    }
}

fn nfstime_to_system_time(t: nfstime3) -> SystemTime {
    let nsec = t.nseconds.min(999_999_999);
    UNIX_EPOCH + std::time::Duration::new(t.seconds as u64, nsec)
}
