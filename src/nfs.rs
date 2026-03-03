use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use nfsserve::nfs::{fattr3, fileid3, filename3, ftype3, nfspath3, nfsstat3, nfstime3, sattr3, specdata3};
use nfsserve::tcp::{NFSTcp, NFSTcpListener};
use nfsserve::vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities};
use tracing::info;

use crate::inode::InodeKind;
use crate::virtual_fs::{VirtualFs, VirtualFsAttr};

// ── NFS Adapter ────────────────────────────────────────────────────────

pub struct NFSAdapter {
    virtual_fs: Arc<VirtualFs>,
    handle_pool: Arc<Mutex<HandlePool>>,
}

impl NFSAdapter {
    pub fn new(virtual_fs: Arc<VirtualFs>) -> Self {
        Self {
            virtual_fs,
            handle_pool: Arc::new(Mutex::new(HandlePool::new())),
        }
    }

    /// Get or open a file handle from the pool.
    async fn get_or_open_handle(&self, ino: u64) -> Result<u64, nfsstat3> {
        // Check pool first (quick lock)
        if let Some(file_handle) = self.handle_pool.lock().expect("handle_pool poisoned").get(ino) {
            return Ok(file_handle);
        }
        // Pool miss: open file (may await download)
        let file_handle = self.virtual_fs.open(ino, false, false).await.map_err(errno_to_nfs)?;
        // Insert into pool
        let mut pool = self.handle_pool.lock().expect("handle_pool poisoned");
        // Double-check: another task may have opened it concurrently
        if let Some(existing) = pool.get(ino) {
            self.virtual_fs.release(file_handle);
            return Ok(existing);
        }
        if let Some((_evicted_ino, evicted_handle)) = pool.insert(ino, file_handle) {
            self.virtual_fs.release(evicted_handle);
        }
        Ok(file_handle)
    }
}

#[async_trait]
impl NFSFileSystem for NFSAdapter {
    fn root_dir(&self) -> fileid3 {
        1
    }

    fn capabilities(&self) -> VFSCapabilities {
        VFSCapabilities::ReadOnly
    }

    async fn lookup(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        let name = std::str::from_utf8(&filename.0).map_err(|_| nfsstat3::NFS3ERR_NOENT)?;
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

    // ── Read-only stubs ────────────────────────────────────────────────

    async fn setattr(&self, _id: fileid3, _setattr: sattr3) -> Result<fattr3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn write(&self, _id: fileid3, _offset: u64, _data: &[u8]) -> Result<fattr3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn create(
        &self,
        _dirid: fileid3,
        _filename: &filename3,
        _attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn create_exclusive(&self, _dirid: fileid3, _filename: &filename3) -> Result<fileid3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn mkdir(&self, _dirid: fileid3, _dirname: &filename3) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn remove(&self, _dirid: fileid3, _filename: &filename3) -> Result<(), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn rename(
        &self,
        _from_dirid: fileid3,
        _from_filename: &filename3,
        _to_dirid: fileid3,
        _to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn symlink(
        &self,
        _dirid: fileid3,
        _linkname: &filename3,
        _symlink: &nfspath3,
        _attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    async fn readlink(&self, _id: fileid3) -> Result<nfspath3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }
}

// ── Mount orchestration ────────────────────────────────────────────────

pub async fn mount_nfs(virtual_fs: Arc<VirtualFs>, mount_point: &Path, metadata_ttl_ms: u64) -> std::io::Result<()> {
    let vfs_for_shutdown = virtual_fs.clone();
    let adapter = NFSAdapter::new(virtual_fs);
    let listener = NFSTcpListener::bind("127.0.0.1:0", adapter).await?;
    let port = listener.get_listen_port();
    info!("NFS server listening on 127.0.0.1:{}", port);

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
        let status = std::process::Command::new("mount_nfs")
            .args([
                "-o",
                &format!("rdonly,nolocks,vers=3,tcp,rsize=1048576,actimeo={actimeo},port={port},mountport={port}"),
                "127.0.0.1:/",
                mount_point_str,
            ])
            .status()?;
        if !status.success() {
            return Err(std::io::Error::other(format!("mount command failed with {status}")));
        }
    }

    #[cfg(target_os = "linux")]
    {
        let mount_opts = format!("nolock,vers=3,tcp,rsize=1048576,actimeo={actimeo},port={port},mountport={port}");
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
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(std::io::Error::other(format!(
                "mount.nfs failed with {}: stdout={stdout} stderr={stderr}",
                output.status
            )));
        }
    }

    info!("NFS mount active at {}", mount_point_str);

    // Block until the NFS server exits (e.g. on unmount)
    let _ = server_handle.await;
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
// calls virtual_fs.release(), which flushes dirty data and frees the prefetch
// buffer. A subsequent read on an evicted file simply re-opens it (cold
// open — slightly slower, prefetch restarts from scratch).
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

fn vfs_attr_to_nfs(attr: &VirtualFsAttr) -> fattr3 {
    let ftype = match attr.kind {
        InodeKind::File => ftype3::NF3REG,
        InodeKind::Directory => ftype3::NF3DIR,
    };
    let mtime = system_time_to_nfstime(attr.mtime);
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
        atime: mtime,
        mtime,
        ctime: mtime,
    }
}
