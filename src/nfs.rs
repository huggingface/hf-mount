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

use crate::daemon::DaemonGuard;
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

    /// Get or open a pooled read handle with a shared pin. Cold-read races
    /// converge on a single pooled handle so the per-handle prefetch buffer
    /// absorbs concurrent NFS readahead RPCs instead of spawning duplicate
    /// Xet streams.
    async fn get_or_open_handle(&self, ino: u64) -> Result<u64, nfsstat3> {
        if let Some(handle) = self.acquire_shared(ino) {
            return Ok(handle);
        }
        let file_handle = match self.virtual_fs.open(ino, false, false, None).await {
            Ok(handle) => handle,
            Err(err) => return self.acquire_shared(ino).ok_or_else(|| errno_to_nfs(err)),
        };

        enum Outcome {
            ShareExisting(u64),
            Inserted(InsertResult),
        }
        let outcome = {
            let mut pool = self.handle_pool.lock().expect("handle_pool poisoned");
            if let Some(existing) = pool.acquire_shared(ino) {
                Outcome::ShareExisting(existing)
            } else {
                let result = pool.insert(ino, file_handle);
                pool.acquire_shared(ino);
                Outcome::Inserted(result)
            }
        };
        match outcome {
            Outcome::ShareExisting(existing) => {
                let _ = self.virtual_fs.release(file_handle).await;
                Ok(existing)
            }
            Outcome::Inserted(result) => {
                self.process_insert_result(result).await;
                Ok(file_handle)
            }
        }
    }

    fn acquire_shared(&self, ino: u64) -> Option<u64> {
        self.handle_pool
            .lock()
            .expect("handle_pool poisoned")
            .acquire_shared(ino)
    }

    async fn process_insert_result(&self, result: InsertResult) {
        for (evicted_ino, evicted_handle) in result.evicted {
            self.evict_handle(evicted_ino, evicted_handle).await;
        }
        if let Some(replaced_handle) = result.replaced {
            let _ = self.virtual_fs.release(replaced_handle).await;
        }
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
        let result = {
            let mut pool = self.handle_pool.lock().expect("handle_pool poisoned");
            pool.insert(ino, file_handle)
        };
        for (evicted_ino, evicted_handle) in result.evicted {
            self.evict_handle(evicted_ino, evicted_handle).await;
        }
        if let Some(replaced_handle) = result.replaced {
            self.evict_handle(ino, replaced_handle).await;
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
        // Share the pooled handle across concurrent readers — its prefetch
        // buffer absorbs NFS readahead RPCs efficiently. The shared pin only
        // blocks LRU eviction; concurrent reads still serialize on the
        // per-handle prefetch mutex inside virtual_fs.
        let file_handle = match self.acquire_shared(id) {
            Some(handle) => handle,
            None => self.get_or_open_handle(id).await?,
        };

        let result = self.virtual_fs.read(file_handle, offset, count).await;
        self.handle_pool.lock().expect("handle_pool poisoned").unpin(id);

        match result {
            Ok((bytes, eof)) => Ok((bytes.to_vec(), eof)),
            Err(libc::EBADF) => {
                // unlink/rename can remove() a pinned entry while a read is
                // in flight. Drop the stale entry and retry once with a
                // freshly-opened handle outside the pool.
                {
                    let mut pool = self.handle_pool.lock().expect("handle_pool poisoned");
                    if pool.peek(id) == Some(file_handle) {
                        pool.remove(id);
                    }
                }
                let handle = self
                    .virtual_fs
                    .open(id, false, false, None)
                    .await
                    .map_err(errno_to_nfs)?;
                let result = self.virtual_fs.read(handle, offset, count).await;
                let _ = self.virtual_fs.release(handle).await;
                result.map(|(b, eof)| (b.to_vec(), eof)).map_err(errno_to_nfs)
            }
            Err(err) => Err(errno_to_nfs(err)),
        }
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
        // Fast path: try the existing pool handle. `virtual_fs.write` is a
        // synchronous `pwrite` with no yield point, so peeking without
        // pinning is safe — the pool can't evict mid-call.
        //
        // The pool may hold a *read-only* handle for this inode from a prior
        // READ RPC. macOS NFS readily issues READs during stat / `ls` to
        // populate its attribute cache, well before any write. Writing to a
        // Lazy/read-only handle returns EBADF, which `errno_to_nfs` maps to
        // `NFS3ERR_STALE`. macOS treats STALE on WRITE as a hard failure and
        // silently drops the pending writes from its buffer — `dd` reports
        // success but the bytes never reach the server. Symptom seen in the
        // wild: `fsync(2)` on the file returns ESTALE.
        //
        // Fix: on EBADF, evict the read-only handle and reopen writable
        // (which materializes the staging file), then retry the write.
        let existing = self.handle_pool.lock().expect("handle_pool poisoned").peek(id);
        if let Some(fh) = existing {
            match self.virtual_fs.write(id, fh, offset, data) {
                Ok(_) => {
                    self.virtual_fs.schedule_flush(id);
                    return self
                        .virtual_fs
                        .getattr(id)
                        .map(|a| vfs_attr_to_nfs(&a))
                        .map_err(errno_to_nfs);
                }
                Err(libc::EBADF) => {
                    // Handle was opened read-only. Evict + remove from pool so
                    // a concurrent caller doesn't peek a freshly-released fh.
                    // Mirrors the analogous EBADF retry in `read()` above.
                    // Guard against removing a different fh: a successful
                    // concurrent upgrader may have already replaced our entry.
                    {
                        let mut pool = self.handle_pool.lock().expect("handle_pool poisoned");
                        if pool.peek(id) == Some(fh) {
                            pool.remove(id);
                        }
                    }
                    self.evict_handle(id, fh).await;
                }
                Err(e) => return Err(errno_to_nfs(e)),
            }
        }

        // Slow path: open a writable handle, run the pwrite, THEN publish the
        // handle to the pool. Two concurrent writers can both reach this
        // branch and open distinct writable handles (open is serialized by
        // VirtualFs's per-inode staging lock, so the calls don't overlap, but
        // they DO produce two distinct fh). If we inserted before the pwrite,
        // the second writer's `insert_handle` would release the first
        // writer's freshly-opened fh as `replaced` — and the first writer's
        // subsequent pwrite would hit EBADF on a closed fh, mapping back to
        // NFS3ERR_STALE (silent data loss, the very symptom this code path
        // exists to avoid). By writing before publishing, the fh stays
        // private to this task until its pwrite completes; nothing else can
        // see it, nothing else can release it.
        let fh = self
            .virtual_fs
            .open(id, true, false, None)
            .await
            .map_err(errno_to_nfs)?;
        if let Err(e) = self.virtual_fs.write(id, fh, offset, data) {
            // Write failed — release the fh we opened so we don't leak it.
            let _ = self.virtual_fs.release(fh).await;
            return Err(errno_to_nfs(e));
        }
        // Publish only on success. Any concurrent writer that reaches this
        // point will release its own fh via the same Err path or replace
        // ours via insert_handle's eviction, but by then our pwrite has
        // committed to the staging file.
        self.insert_handle(id, fh).await;
        // NFS has no close/flush RPC, so schedule a debounced flush after
        // each write to ensure data eventually gets committed to the Hub.
        self.virtual_fs.schedule_flush(id);
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
        // Schedule a flush so empty files (e.g. `touch`) get committed to remote.
        self.virtual_fs.schedule_flush(ino);
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
        self.virtual_fs.schedule_flush(ino);
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
        let attr = self.virtual_fs.lookup(dirid, name).await.map_err(errno_to_nfs)?;
        let ino = attr.ino;
        match attr.kind {
            InodeKind::Directory => self.virtual_fs.rmdir(dirid, name).await.map_err(errno_to_nfs)?,
            _ => self.virtual_fs.unlink(dirid, name).await.map_err(errno_to_nfs)?,
        }
        // Evict from handle pool so the FD is released immediately.
        // Without this, deleted files' handles linger until LRU-evicted.
        let evicted = self.handle_pool.lock().expect("handle_pool poisoned").remove(ino);
        if let Some(file_handle) = evicted {
            self.evict_handle(ino, file_handle).await;
        }
        Ok(())
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
        // If destination exists and is a different inode from source, it will be
        // unlinked by rename. Evict its stale handle (same reason as remove()).
        // Only resolve source ino when destination exists (the rare overwrite case).
        let dest_ino = self.virtual_fs.lookup(to_dirid, to_name).await.ok().map(|a| a.ino);
        let src_ino = if dest_ino.is_some() {
            self.virtual_fs.lookup(from_dirid, from_name).await.ok().map(|a| a.ino)
        } else {
            None
        };
        self.virtual_fs
            .rename(from_dirid, from_name, to_dirid, to_name, false)
            .await
            .map_err(errno_to_nfs)?;
        // Skip same-inode renames (no-op) to avoid evicting the live handle.
        if let Some(ino) = dest_ino
            && dest_ino != src_ino
        {
            let evicted = self.handle_pool.lock().expect("handle_pool poisoned").remove(ino);
            if let Some(file_handle) = evicted {
                self.evict_handle(ino, file_handle).await;
            }
        }
        Ok(())
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
    daemon_guard: Option<&mut DaemonGuard>,
) -> std::io::Result<()> {
    let vfs_for_shutdown = virtual_fs.clone();
    let adapter = NFSAdapter::new(virtual_fs, read_only);
    let pool_for_shutdown = adapter.handle_pool.clone();

    // When the poll loop detects a remote change on `ino`, drop the pooled
    // file handle. Pooled handles are bound to the xet_hash captured at
    // open() time (see `open_lazy`); without eviction, subsequent NFS reads
    // keep streaming the pre-update content even though `stat()` already
    // reports the new size — see issue #160.
    let pool = pool_for_shutdown.clone();
    let vfs = vfs_for_shutdown.clone();
    let rt = tokio::runtime::Handle::current();
    // NFS has no kernel page cache to invalidate, so the `InvalKind` page-vs-attr
    // distinction (a FUSE-only deadlock guard, #195) doesn't apply here: we always
    // drop the pooled handle so the next read re-opens at the new xet_hash.
    vfs_for_shutdown.set_invalidator(Box::new(move |ino, _kind| {
        let Some(fh) = pool.lock().expect("handle_pool poisoned").remove(ino) else {
            return;
        };
        let vfs = vfs.clone();
        rt.spawn(async move {
            if let Err(e) = vfs.release(fh).await {
                tracing::debug!("NFS invalidator: release ino={} failed: errno={}", ino, e);
            }
        });
    }));

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
        // `locallocks` (not `nolocks`): macOS mount_nfs treats `nolocks` as
        // "advisory locking unsupported" and returns ENOTSUP on flock/fcntl,
        // which breaks Python `filelock`, `huggingface_hub`, `datasets`, …
        // `locallocks` keeps lock handling inside the client kernel (no NLM
        // round-trip to the server). `nfsserve` does not implement NLM, so
        // local locking is the only viable option anyway.
        let mut opts = format!("locallocks,vers=3,tcp,rsize=1048576,actimeo={actimeo},port={port},mountport={port}");
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
        // mount.nfs (nfs-common on Debian/Ubuntu, nfs-utils on RHEL/Fedora/AL2023)
        // is required. It conventionally lives in /sbin or /usr/sbin, which aren't
        // always on PATH for non-login processes, so resolve it explicitly. When it
        // is missing the bare spawn fails with a cryptic "No such file or directory
        // (os error 2)" — see issue #101.
        let Some(mount_nfs) = find_mount_nfs() else {
            server_handle.abort();
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "mount.nfs not found. Install nfs-common (Debian/Ubuntu) or nfs-utils (RHEL/Fedora/AL2023).",
            ));
        };

        let mut mount_opts = format!("nolock,vers=3,tcp,rsize=1048576,actimeo={actimeo},port={port},mountport={port}");
        if !read_only {
            mount_opts = format!("{mount_opts},wsize=1048576");
        }
        let output = if unsafe { libc::getuid() } == 0 {
            std::process::Command::new(&mount_nfs)
                .args(["-o", &mount_opts, "127.0.0.1:/", mount_point_str])
                .output()?
        } else {
            std::process::Command::new("sudo")
                .arg("-n")
                .arg(&mount_nfs)
                .args(["-o", &mount_opts, "127.0.0.1:/", mount_point_str])
                .output()?
        };
        if !output.status.success() {
            server_handle.abort();
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            // mount.nfs exits 32 with EPERM when the kernel mount() syscall is
            // denied — typically a container without CAP_SYS_ADMIN. See issue #101.
            let hint = if stderr.contains("Operation not permitted") {
                "\nhint: the mount syscall was denied. NFS mounts require CAP_SYS_ADMIN; \
                 run the container privileged (e.g. `docker run --privileged` or `--cap-add SYS_ADMIN`) \
                 and ensure seccomp/AppArmor profiles allow the mount syscall."
            } else {
                ""
            };
            return Err(std::io::Error::other(format!(
                "mount.nfs failed with {}: stdout={stdout} stderr={stderr}{hint}",
                output.status
            )));
        }
    }

    info!("NFS mount active at {}", mount_point_str);

    // Signal the parent process that the mount is live (daemon mode).
    if let Some(guard) = daemon_guard {
        guard.notify_ready();
    }

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

    // Drain handle pool: flush and release all cached handles before VFS shutdown.
    let entries = pool_for_shutdown.lock().expect("handle_pool poisoned").drain();
    for (ino, file_handle) in entries {
        let _ = vfs_for_shutdown.flush(ino, file_handle, None).await;
        let _ = vfs_for_shutdown.release(file_handle).await;
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

struct InsertResult {
    /// LRU evictions of unpinned entries to bring the pool back to capacity.
    evicted: Vec<(u64, u64)>,
    /// Replaced handle for the same ino (e.g. read -> write upgrade).
    replaced: Option<u64>,
}

struct HandleEntry {
    file_handle: u64,
    /// Number of in-flight operations using this handle. Pinned entries
    /// (pin_count > 0) are skipped during LRU eviction so that concurrent
    /// reads never encounter a released handle.
    pin_count: u32,
}

struct HandlePool {
    handles: HashMap<u64, HandleEntry>, // ino -> entry
    order: VecDeque<u64>,               // ino access order (front = oldest)
}

impl HandlePool {
    fn new() -> Self {
        Self {
            handles: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    /// Increment pin_count and return the handle. Does not touch LRU order.
    fn pin(&mut self, ino: u64) -> Option<u64> {
        let entry = self.handles.get_mut(&ino)?;
        entry.pin_count += 1;
        Some(entry.file_handle)
    }

    fn unpin(&mut self, ino: u64) {
        if let Some(entry) = self.handles.get_mut(&ino) {
            entry.pin_count = entry.pin_count.saturating_sub(1);
        }
    }

    /// Pin and promote to MRU. Concurrent readers stack pins on the same
    /// entry — eviction is blocked until every pin is released.
    fn acquire_shared(&mut self, ino: u64) -> Option<u64> {
        let handle = self.pin(ino)?;
        if self.order.back() != Some(&ino) {
            self.order_remove(ino);
            self.order.push_back(ino);
        }
        Some(handle)
    }

    /// Look up a handle without pinning. Safe only when the caller does not
    /// yield before using the handle.
    fn peek(&self, ino: u64) -> Option<u64> {
        self.handles.get(&ino).map(|e| e.file_handle)
    }

    /// Remove an entry from the pool, returning the file handle if present.
    fn remove(&mut self, ino: u64) -> Option<u64> {
        let entry = self.handles.remove(&ino)?;
        self.order_remove(ino);
        Some(entry.file_handle)
    }

    /// Drain all entries, returning (ino, file_handle) pairs.
    fn drain(&mut self) -> Vec<(u64, u64)> {
        self.order.clear();
        self.handles
            .drain()
            .map(|(ino, entry)| (ino, entry.file_handle))
            .collect()
    }

    fn order_remove(&mut self, ino: u64) {
        if let Some(pos) = self.order.iter().position(|&i| i == ino) {
            self.order.remove(pos);
        }
    }

    /// Insert a handle. Returns evicted entries that the caller must release:
    /// - `evicted`: LRU evictions to bring the pool back to capacity
    /// - `replaced`: old handle for the same ino (e.g. replacing read with write)
    ///
    /// Pinned entries (in-flight reads) are skipped during eviction. If all
    /// entries are pinned the pool grows beyond capacity temporarily but
    /// shrinks back on the next insert after pins are released.
    fn insert(&mut self, ino: u64, file_handle: u64) -> InsertResult {
        let replaced = if let Some(old) = self.handles.remove(&ino) {
            self.order_remove(ino);
            Some(old.file_handle)
        } else {
            None
        };
        // Evict the strict LRU (front of order). If it is pinned, grow the
        // pool rather than evicting a newer unpinned entry — otherwise a
        // freshly-inserted create handle could be released before the client
        // sends its first WRITE.
        let mut evicted = Vec::new();
        while self.handles.len() >= HANDLE_POOL_CAPACITY {
            let lru_ino = match self.order.front() {
                Some(&ino) => ino,
                None => break,
            };
            let pinned = self.handles.get(&lru_ino).is_some_and(|entry| entry.pin_count != 0);
            if pinned {
                break;
            }
            self.order.pop_front();
            if let Some(entry) = self.handles.remove(&lru_ino) {
                evicted.push((lru_ino, entry.file_handle));
            }
        }
        self.handles.insert(
            ino,
            HandleEntry {
                file_handle,
                pin_count: 0,
            },
        );
        self.order.push_back(ino);
        InsertResult { evicted, replaced }
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
        seconds: d.as_secs().min(u32::MAX as u64) as u32,
        nseconds: d.subsec_nanos(),
    }
}

/// Resolve the `mount.nfs` binary, searching `$PATH` plus the conventional
/// sbin directories (which are often absent from a non-login process's PATH).
/// Returns `None` when `nfs-common`/`nfs-utils` is not installed.
#[cfg(target_os = "linux")]
fn find_mount_nfs() -> Option<std::path::PathBuf> {
    let mut dirs: Vec<std::path::PathBuf> = std::env::var_os("PATH")
        .map(|path| std::env::split_paths(&path).collect())
        .unwrap_or_default();
    for fallback in ["/sbin", "/usr/sbin", "/bin", "/usr/bin"] {
        let dir = std::path::PathBuf::from(fallback);
        if !dirs.contains(&dir) {
            dirs.push(dir);
        }
    }
    dirs.into_iter()
        .map(|dir| dir.join("mount.nfs"))
        .find(|candidate| candidate.is_file())
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
    if let Err(e) = std::process::Command::new("umount").arg(mount_point).status() {
        tracing::warn!("NFS unmount fallback failed for {}: {}", mount_point, e);
    }
    #[cfg(target_os = "linux")]
    {
        let result = if unsafe { libc::getuid() } == 0 {
            std::process::Command::new("umount").arg(mount_point).status()
        } else {
            std::process::Command::new("sudo")
                .args(["-n", "umount", mount_point])
                .status()
        };
        if let Err(e) = result {
            tracing::warn!("NFS unmount fallback failed for {}: {}", mount_point, e);
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_mocks::{MockHub, MockXet, TestOpts, make_test_vfs};

    /// Regression: a WRITE that arrives after a prior READ has populated the
    /// pool with a Lazy/read-only handle must NOT surface as `NFS3ERR_STALE`.
    /// Pre-fix, `nfs.rs::write()` peeked the read-only handle, called
    /// `virtual_fs.write()` which returned EBADF, and `errno_to_nfs` mapped
    /// that to STALE — at which point macOS NFS silently discards the write.
    ///
    /// The fix: on EBADF, evict the read-only handle and re-open writable,
    /// then retry. This test exercises that exact sequence.
    #[test]
    fn write_after_read_upgrades_handle_instead_of_returning_stale() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let hub = MockHub::new();
        hub.add_file("file.txt", 11, Some("orig_hash"), None);
        let xet = MockXet::new();
        xet.add_file("orig_hash", b"hello world");

        let vfs = make_test_vfs(
            hub.clone(),
            xet.clone(),
            TestOpts {
                advanced_writes: true,
                ..Default::default()
            },
            &rt,
        );

        // NFS adapter under test (read-write, like a real bucket mount).
        let adapter = NFSAdapter::new(vfs.clone(), false);

        rt.block_on(async {
            // Resolve the ino so we don't hard-code it.
            let name = nfsstring(b"file.txt".to_vec());
            let ino = adapter.lookup(1, &name).await.expect("lookup");

            // Step 1: a READ populates the pool with a read-only (Lazy) handle.
            let (_buf, _eof) = adapter.read(ino, 0, 11).await.expect("read");
            let pooled_fh_after_read = adapter
                .handle_pool
                .lock()
                .expect("poisoned")
                .peek(ino)
                .expect("pool entry");

            // Step 2: the critical operation — WRITE on the same inode.
            // Pre-fix: this returned Err(NFS3ERR_STALE). Post-fix: it must
            // upgrade the handle and succeed.
            let attr = adapter
                .write(ino, 6, b"RUST!")
                .await
                .expect("write must not return STALE");

            // The new attributes reflect the write.
            assert_eq!(attr.size, 11, "file size should be unchanged (in-place edit)");

            // The pool's handle must have been swapped for a writable one
            // (the slow path inserts a fresh handle after the upgrade).
            let pooled_fh_after_write = adapter
                .handle_pool
                .lock()
                .expect("poisoned")
                .peek(ino)
                .expect("pool entry");
            assert_ne!(
                pooled_fh_after_read, pooled_fh_after_write,
                "pool handle must be different after the upgrade (old Lazy → new writable)"
            );
        });
    }

    /// A second WRITE on the same ino reuses the now-writable pool handle
    /// (fast path: no further open/release dance).
    #[test]
    fn second_write_reuses_writable_handle() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let hub = MockHub::new();
        hub.add_file("file.txt", 11, Some("orig_hash"), None);
        let xet = MockXet::new();
        xet.add_file("orig_hash", b"hello world");

        let vfs = make_test_vfs(
            hub.clone(),
            xet.clone(),
            TestOpts {
                advanced_writes: true,
                ..Default::default()
            },
            &rt,
        );
        let adapter = NFSAdapter::new(vfs.clone(), false);

        rt.block_on(async {
            let name = nfsstring(b"file.txt".to_vec());
            let ino = adapter.lookup(1, &name).await.expect("lookup");

            // First read + write triggers the upgrade.
            adapter.read(ino, 0, 11).await.expect("read");
            adapter.write(ino, 0, b"A").await.expect("first write");
            let fh_after_first_write = adapter
                .handle_pool
                .lock()
                .expect("poisoned")
                .peek(ino)
                .expect("pool entry");

            // Second write should take the fast path (no new open).
            adapter.write(ino, 1, b"B").await.expect("second write");
            let fh_after_second_write = adapter
                .handle_pool
                .lock()
                .expect("poisoned")
                .peek(ino)
                .expect("pool entry");

            assert_eq!(
                fh_after_first_write, fh_after_second_write,
                "fast path must reuse the writable handle"
            );
        });
    }

    /// A WRITE on a file that was never opened goes through the slow path
    /// directly (no fast-path EBADF). Verifies the slow path stands alone.
    #[test]
    fn write_without_prior_read_opens_writable_directly() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let hub = MockHub::new();
        hub.add_file("file.txt", 11, Some("orig_hash"), None);
        let xet = MockXet::new();
        xet.add_file("orig_hash", b"hello world");

        let vfs = make_test_vfs(
            hub.clone(),
            xet.clone(),
            TestOpts {
                advanced_writes: true,
                ..Default::default()
            },
            &rt,
        );
        let adapter = NFSAdapter::new(vfs.clone(), false);

        rt.block_on(async {
            let name = nfsstring(b"file.txt".to_vec());
            let ino = adapter.lookup(1, &name).await.expect("lookup");

            // Pool is empty for this ino; write goes straight to slow path.
            assert!(adapter.handle_pool.lock().unwrap().peek(ino).is_none());
            adapter.write(ino, 0, b"X").await.expect("write");
            assert!(
                adapter.handle_pool.lock().unwrap().peek(ino).is_some(),
                "slow path must have inserted a writable handle"
            );
        });
    }

    #[test]
    fn handle_pool_basic() {
        let mut pool = HandlePool::new();
        assert!(pool.peek(1).is_none());

        let result = pool.insert(1, 100);
        assert!(result.evicted.is_empty());
        assert!(result.replaced.is_none());
        assert_eq!(pool.peek(1), Some(100));
    }

    #[test]
    fn handle_pool_lru_eviction() {
        let mut pool = HandlePool::new();
        for i in 0..HANDLE_POOL_CAPACITY as u64 {
            pool.insert(i, i + 1000);
        }
        // Promote ino=0 to MRU so the LRU is now ino=1.
        pool.acquire_shared(0);
        pool.unpin(0);

        let result = pool.insert(999, 9999);
        assert_eq!(result.evicted.len(), 1);
        assert_eq!(result.evicted[0], (1, 1001));
        assert!(result.replaced.is_none());

        assert!(pool.peek(1).is_none());
        assert_eq!(pool.peek(999), Some(9999));
    }

    #[test]
    fn handle_pool_no_duplicate_on_reinsert() {
        let mut pool = HandlePool::new();
        pool.insert(1, 100);
        pool.insert(2, 200);
        // Re-insert ino=1 with new handle (e.g. replacing read with write)
        let result = pool.insert(1, 101);
        assert_eq!(result.replaced, Some(100), "old handle should be returned for release");
        assert!(result.evicted.is_empty());

        assert_eq!(pool.peek(1), Some(101));
        // order should have exactly 2 entries, not 3
        assert_eq!(pool.order.len(), 2);
    }

    #[test]
    fn handle_pool_remove() {
        let mut pool = HandlePool::new();
        pool.insert(1, 100);
        pool.insert(2, 200);
        pool.insert(3, 300);

        assert_eq!(pool.remove(2), Some(200));
        assert!(pool.peek(2).is_none());
        assert_eq!(pool.order.len(), 2);
        assert_eq!(pool.handles.len(), 2);
        // Remaining entries still work
        assert_eq!(pool.peek(1), Some(100));
        assert_eq!(pool.peek(3), Some(300));
    }

    #[test]
    fn handle_pool_drain() {
        let mut pool = HandlePool::new();
        pool.insert(1, 100);
        pool.insert(2, 200);
        pool.insert(3, 300);

        let entries = pool.drain();
        assert_eq!(entries.len(), 3);
        assert!(pool.handles.is_empty());
        assert!(pool.order.is_empty());
        // Entries contain all inserted pairs
        assert!(entries.contains(&(1, 100)));
        assert!(entries.contains(&(2, 200)));
        assert!(entries.contains(&(3, 300)));
    }

    #[test]
    fn handle_pool_remove_nonexistent() {
        let mut pool = HandlePool::new();
        pool.insert(1, 100);
        assert_eq!(pool.remove(999), None); // no-op
        assert_eq!(pool.peek(1), Some(100));
        assert_eq!(pool.order.len(), 1);
    }

    #[test]
    fn handle_pool_pinned_lru_grows_pool() {
        let mut pool = HandlePool::new();
        // Fill pool to capacity. ino=0 is the LRU entry.
        for i in 0..HANDLE_POOL_CAPACITY as u64 {
            pool.insert(i, i + 1000);
        }
        // Pin ino=0 in place so the LRU is unevictable.
        pool.pin(0);
        assert_eq!(pool.order.front(), Some(&0));

        // Insert one more — the pool grows rather than evicting a newer
        // unpinned entry (which could be a freshly-created file's handle).
        let result = pool.insert(999, 9999);
        assert!(result.evicted.is_empty(), "pinned LRU should not trigger eviction");
        assert_eq!(pool.handles.len(), HANDLE_POOL_CAPACITY + 1);

        // All originals still present.
        for i in 0..HANDLE_POOL_CAPACITY as u64 {
            assert!(pool.handles.contains_key(&i));
        }
        pool.unpin(0);
    }

    #[test]
    fn handle_pool_unpin_allows_eviction() {
        let mut pool = HandlePool::new();
        for i in 0..HANDLE_POOL_CAPACITY as u64 {
            pool.insert(i, i + 1000);
        }
        // Pin in place (no promotion), then unpin so ino=0 is evictable LRU.
        pool.pin(0);
        pool.unpin(0);
        assert_eq!(pool.order.front(), Some(&0));

        // ino=0 is unpinned and at the front of the order, so it evicts.
        let result = pool.insert(999, 9999);
        assert_eq!(result.evicted.len(), 1);
        assert_eq!(result.evicted[0].0, 0, "unpinned LRU should evict normally");
    }

    #[test]
    fn handle_pool_all_pinned_grows_beyond_capacity() {
        let mut pool = HandlePool::new();
        for i in 0..HANDLE_POOL_CAPACITY as u64 {
            pool.insert(i, i + 1000);
            pool.pin(i);
        }
        // All entries pinned — insert should succeed with no eviction
        let result = pool.insert(999, 9999);
        assert!(result.evicted.is_empty(), "no eviction when all entries are pinned");
        assert_eq!(pool.handles.len(), HANDLE_POOL_CAPACITY + 1);

        // Unpin all
        for i in 0..HANDLE_POOL_CAPACITY as u64 {
            pool.unpin(i);
        }

        // Next insert should reclaim overflow entries
        let result = pool.insert(998, 9998);
        // Should evict enough to bring pool back to capacity (evict 2: the
        // overflow entry 999 is MRU, so oldest unpinned entries are evicted)
        assert!(result.evicted.len() >= 2, "pool should shrink after overflow");
        assert!(pool.handles.len() <= HANDLE_POOL_CAPACITY + 1);
    }

    #[test]
    fn handle_pool_acquire_shared_stacks_pins() {
        let mut pool = HandlePool::new();
        assert!(pool.acquire_shared(1).is_none());

        pool.insert(1, 100);
        assert_eq!(pool.acquire_shared(1), Some(100));
        assert_eq!(pool.acquire_shared(1), Some(100));
        assert_eq!(pool.handles[&1].pin_count, 2);

        pool.unpin(1);
        pool.unpin(1);
        assert_eq!(pool.handles[&1].pin_count, 0);
    }
}
