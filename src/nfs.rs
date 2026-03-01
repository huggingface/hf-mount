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
use crate::vfs::{HfVfsCore, VfsAttr};

// ── Handle Pool ────────────────────────────────────────────────────────

const HANDLE_POOL_CAPACITY: usize = 64;

struct HandlePool {
    handles: HashMap<u64, u64>, // ino -> fh
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
        if let Some(&fh) = self.handles.get(&ino) {
            self.order.retain(|&i| i != ino);
            self.order.push_back(ino);
            Some(fh)
        } else {
            None
        }
    }

    /// Insert a handle, returning the evicted (ino, fh) if pool was full.
    fn insert(&mut self, ino: u64, fh: u64) -> Option<(u64, u64)> {
        let evicted = if self.handles.len() >= HANDLE_POOL_CAPACITY {
            self.order
                .pop_front()
                .and_then(|old_ino| self.handles.remove(&old_ino).map(|fh| (old_ino, fh)))
        } else {
            None
        };
        self.handles.insert(ino, fh);
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

fn vfs_attr_to_nfs(attr: &VfsAttr) -> fattr3 {
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

/// Get or open a file handle from the pool. Called from blocking context.
fn get_or_open_handle(vfs: &HfVfsCore, handle_pool: &Mutex<HandlePool>, ino: u64) -> Result<u64, nfsstat3> {
    // Check pool first (quick lock)
    if let Some(fh) = handle_pool.lock().unwrap().get(ino) {
        return Ok(fh);
    }
    // Pool miss: open file (may block on download)
    let (fh, _direct_io) = vfs.open(ino, false, false).map_err(errno_to_nfs)?;
    // Insert into pool
    let mut pool = handle_pool.lock().unwrap();
    // Double-check: another thread may have opened it concurrently
    if let Some(existing_fh) = pool.get(ino) {
        vfs.release(ino, fh);
        return Ok(existing_fh);
    }
    if let Some((evicted_ino, evicted_fh)) = pool.insert(ino, fh) {
        vfs.release(evicted_ino, evicted_fh);
    }
    Ok(fh)
}

// ── NFS Adapter ────────────────────────────────────────────────────────

pub struct NfsAdapter {
    vfs: Arc<HfVfsCore>,
    handle_pool: Arc<Mutex<HandlePool>>,
}

impl NfsAdapter {
    pub fn new(vfs: Arc<HfVfsCore>) -> Self {
        Self {
            vfs,
            handle_pool: Arc::new(Mutex::new(HandlePool::new())),
        }
    }
}

#[async_trait]
impl NFSFileSystem for NfsAdapter {
    fn root_dir(&self) -> fileid3 {
        1
    }

    fn capabilities(&self) -> VFSCapabilities {
        VFSCapabilities::ReadOnly
    }

    async fn lookup(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        let name = std::str::from_utf8(&filename.0)
            .map_err(|_| nfsstat3::NFS3ERR_NOENT)?
            .to_string();
        let vfs = self.vfs.clone();
        tokio::task::spawn_blocking(move || vfs.lookup(dirid, &name).map(|a| a.ino).map_err(errno_to_nfs))
            .await
            .map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?
    }

    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        let vfs = self.vfs.clone();
        tokio::task::spawn_blocking(move || vfs.getattr(id).map(|a| vfs_attr_to_nfs(&a)).map_err(errno_to_nfs))
            .await
            .map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?
    }

    async fn read(&self, id: fileid3, offset: u64, count: u32) -> Result<(Vec<u8>, bool), nfsstat3> {
        let vfs = self.vfs.clone();
        let pool = self.handle_pool.clone();
        tokio::task::spawn_blocking(move || {
            let fh = get_or_open_handle(&vfs, &pool, id)?;
            let data = vfs.read(fh, offset, count).map_err(errno_to_nfs)?;
            let file_size = vfs.getattr(id).map(|a| a.size).unwrap_or(0);
            let eof = offset + data.len() as u64 >= file_size;
            Ok((data, eof))
        })
        .await
        .map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?
    }

    async fn readdir(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        let vfs = self.vfs.clone();
        tokio::task::spawn_blocking(move || {
            let entries = vfs.readdir(dirid).map_err(errno_to_nfs)?;
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
                    let attr = vfs.getattr(e.ino).map(|a| vfs_attr_to_nfs(&a)).unwrap_or_default();
                    DirEntry {
                        fileid: e.ino,
                        name: e.name.clone().into_bytes().into(),
                        attr,
                    }
                })
                .collect();
            let end = skip + page.len() >= entries.len();
            Ok(ReadDirResult { entries: page, end })
        })
        .await
        .map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?
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

pub async fn mount_nfs(vfs: Arc<HfVfsCore>, mount_point: &Path) -> std::io::Result<()> {
    let adapter = NfsAdapter::new(vfs);
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

    // Platform-specific mount command
    #[cfg(target_os = "macos")]
    {
        let status = std::process::Command::new("/sbin/mount")
            .args([
                "-t",
                "nfs",
                "-o",
                &format!("rdonly,nolocks,vers=3,tcp,rsize=1048576,actimeo=60,port={port},mountport={port}"),
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
        let output = std::process::Command::new("mount.nfs")
            .args([
                "-o",
                &format!("nolock,vers=3,tcp,rsize=1048576,actimeo=60,port={port},mountport={port}"),
                "127.0.0.1:/",
                mount_point_str,
            ])
            .output()?;
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
    Ok(())
}
