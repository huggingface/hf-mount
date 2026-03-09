use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use fuser::{
    Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags, Generation, INodeNo, InitFlags, KernelConfig,
    OpenFlags, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite,
    Request, TimeOrNow,
};
use tracing::{error, info};

use crate::virtual_fs::inode::InodeKind;
use crate::virtual_fs::{VirtualFs, VirtualFsAttr};

/// Always 0: we never recycle inode numbers, so generation is unnecessary.
const GENERATION: Generation = Generation(0);

pub struct FuseAdapter {
    runtime: tokio::runtime::Handle,
    virtual_fs: Arc<VirtualFs>,
    /// Kernel metadata cache TTL. Short enough to detect remote changes quickly,
    /// long enough to avoid cascade re-lookups in the kernel.
    metadata_ttl: Duration,
    read_only: bool,
    advanced_writes: bool,
    /// FOPEN_DIRECT_IO flag on open/create, bypassing kernel page cache.
    direct_io: bool,
}

impl FuseAdapter {
    pub fn new(
        runtime: tokio::runtime::Handle,
        virtual_fs: Arc<VirtualFs>,
        metadata_ttl: Duration,
        read_only: bool,
        advanced_writes: bool,
        direct_io: bool,
    ) -> Self {
        Self {
            runtime,
            virtual_fs,
            metadata_ttl,
            read_only,
            advanced_writes,
            direct_io,
        }
    }

    fn open_flags(&self) -> FopenFlags {
        if self.direct_io {
            FopenFlags::FOPEN_DIRECT_IO
        } else {
            FopenFlags::empty()
        }
    }
}

fn vfs_attr_to_fuse(attr: &VirtualFsAttr) -> FileAttr {
    let kind = match attr.kind {
        InodeKind::File => FileType::RegularFile,
        InodeKind::Directory => FileType::Directory,
        InodeKind::Symlink => FileType::Symlink,
    };
    FileAttr {
        ino: INodeNo(attr.ino),
        size: attr.size,
        blocks: attr.blocks,
        atime: attr.atime,
        mtime: attr.mtime,
        ctime: attr.ctime,
        crtime: attr.mtime,
        kind,
        perm: attr.perm,
        nlink: attr.nlink,
        uid: attr.uid,
        gid: attr.gid,
        rdev: 0,
        blksize: 512,
        flags: 0,
    }
}

/// Convert an OsStr to &str, or reply with EINVAL and return early.
macro_rules! os_to_str {
    ($name:expr, $reply:expr) => {
        match $name.to_str() {
            Some(n) => n,
            None => {
                $reply.error(Errno::EINVAL);
                return;
            }
        }
    };
}

impl Filesystem for FuseAdapter {
    /// Called once when the filesystem is mounted. Configures kernel FUSE parameters.
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> std::io::Result<()> {
        // Max concurrent background kernel requests (readahead, writeback…).
        // Kernel default (12) is too low for network-backed I/O.
        let _ = config.set_max_background(64);
        // Readahead benefits local/cached files; remote lazy files use DIRECT_IO
        // and rely on our userspace PrefetchState instead.
        let _ = config.set_max_readahead(16 * 1_048_576); // 16 MiB
        let _ = config.set_max_write(16 * 1_048_576); // 16 MiB — fewer round-trips for large sequential writes

        // Allow mmap on DIRECT_IO files (Linux 6.6+). Without this, mmap()
        // returns EINVAL when FOPEN_DIRECT_IO is set, breaking safetensors and
        // other memory-mapped readers.
        if self.direct_io && config.add_capabilities(InitFlags::FUSE_DIRECT_IO_ALLOW_MMAP).is_err() {
            info!(
                "--direct-io: kernel does not support FUSE_DIRECT_IO_ALLOW_MMAP; \
                 mmap-based readers (e.g. safetensors) may fail with EINVAL"
            );
        }

        // Receive O_TRUNC in open() flags instead of a separate setattr(size=0) call.
        if !self.read_only {
            let trunc_ok = config.add_capabilities(InitFlags::FUSE_ATOMIC_O_TRUNC).is_ok();
            if !trunc_ok && !self.advanced_writes {
                // Simple streaming mode requires atomic O_TRUNC — setattr truncation
                // is rejected, so without this capability overwrites would silently fail.
                tracing::error!(
                    "Kernel does not support FUSE_ATOMIC_O_TRUNC; \
                     simple streaming write mode cannot function. \
                     Use --advanced-writes or upgrade your kernel."
                );
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "FUSE_ATOMIC_O_TRUNC not supported by kernel",
                ));
            }
        }
        Ok(())
    }

    /// Resolve a child name inside a directory → returns inode attributes.
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let name = os_to_str!(name, reply);
        match self.runtime.block_on(self.virtual_fs.lookup(parent.0, name)) {
            Ok(attr) => reply.entry(&self.metadata_ttl, &vfs_attr_to_fuse(&attr), GENERATION),
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    /// Get file/directory attributes (stat).
    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        match self.virtual_fs.getattr(ino.0) {
            Ok(attr) => reply.attr(&self.metadata_ttl, &vfs_attr_to_fuse(&attr)),
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    /// List directory entries. `offset` is the index of the last entry already returned;
    /// entries before it are skipped so the kernel can paginate large directories.
    fn readdir(&self, _req: &Request, ino: INodeNo, _fh: FileHandle, offset: u64, mut reply: ReplyDirectory) {
        match self.runtime.block_on(self.virtual_fs.readdir(ino.0)) {
            Ok(entries) => {
                for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
                    let file_type = match entry.kind {
                        InodeKind::File => FileType::RegularFile,
                        InodeKind::Directory => FileType::Directory,
                        InodeKind::Symlink => FileType::Symlink,
                    };
                    // (i + 1) is the offset cookie the kernel will pass back on the next call.
                    if reply.add(INodeNo(entry.ino), (i + 1) as u64, file_type, entry.name) {
                        break; // reply buffer full
                    }
                }
                reply.ok();
            }
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    /// Open a file. Returns a file handle and FOPEN flags.
    fn open(&self, req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let accmode = flags.0 & libc::O_ACCMODE;
        let writable = accmode == libc::O_WRONLY || accmode == libc::O_RDWR;
        let truncate = (flags.0 & libc::O_TRUNC) != 0;

        match self
            .runtime
            .block_on(self.virtual_fs.open(ino.0, writable, truncate, Some(req.pid())))
        {
            Ok(file_handle) => {
                // Skip DIRECT_IO only for O_RDWR in simple streaming mode:
                // streaming handles don't support read(), so DIRECT_IO would
                // surface EBADF on any read attempt. O_WRONLY and read-only
                // opens are fine.
                let rdwr = accmode == libc::O_RDWR;
                let flags = if rdwr && !self.advanced_writes {
                    FopenFlags::empty()
                } else {
                    self.open_flags()
                };
                reply.opened(FileHandle(file_handle), flags);
            }
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    /// Read data from an open file at the given offset.
    fn read(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyData,
    ) {
        match self.runtime.block_on(self.virtual_fs.read(fh.0, offset, size)) {
            Ok((data, _eof)) => reply.data(&data),
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    /// Write data to an open file at the given offset.
    fn write(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: fuser::WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyWrite,
    ) {
        match self.virtual_fs.write(ino.0, fh.0, offset, data) {
            Ok(written) => reply.written(written),
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    /// Called on close(2). For streaming writes, synchronously uploads and commits.
    fn flush(&self, req: &Request, ino: INodeNo, fh: FileHandle, _lock_owner: fuser::LockOwner, reply: ReplyEmpty) {
        match self
            .runtime
            .block_on(self.virtual_fs.flush(ino.0, fh.0, Some(req.pid())))
        {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    /// Called when all references to a file handle are closed. Triggers async flush to Hub.
    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        match self.runtime.block_on(self.virtual_fs.release(fh.0)) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    /// Create and open a new file in one call (O_CREAT).
    fn create(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let name = os_to_str!(name, reply);
        let effective_mode = (mode & !umask & 0o7777) as u16;
        match self.runtime.block_on(self.virtual_fs.create(
            parent.0,
            name,
            effective_mode,
            req.uid(),
            req.gid(),
            Some(req.pid()),
        )) {
            Ok((attr, file_handle)) => {
                // Same guard as open(): skip DIRECT_IO for O_RDWR in simple
                // streaming mode (handle is write-only, reads would EBADF).
                let rdwr = (flags & libc::O_ACCMODE) == libc::O_RDWR;
                let oflags = if rdwr && !self.advanced_writes {
                    FopenFlags::empty()
                } else {
                    self.open_flags()
                };
                reply.created(
                    &self.metadata_ttl,
                    &vfs_attr_to_fuse(&attr),
                    GENERATION,
                    FileHandle(file_handle),
                    oflags,
                );
            }
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    /// Create a new directory.
    fn mkdir(&self, req: &Request, parent: INodeNo, name: &OsStr, mode: u32, umask: u32, reply: ReplyEntry) {
        let name = os_to_str!(name, reply);
        let effective_mode = (mode & !umask & 0o7777) as u16;
        match self.runtime.block_on(
            self.virtual_fs
                .mkdir(parent.0, name, effective_mode, req.uid(), req.gid()),
        ) {
            Ok(attr) => reply.entry(&self.metadata_ttl, &vfs_attr_to_fuse(&attr), GENERATION),
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    /// Remove a file.
    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let name = os_to_str!(name, reply);
        match self.runtime.block_on(self.virtual_fs.unlink(parent.0, name)) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    /// Create a symbolic link.
    fn symlink(&self, req: &Request, parent: INodeNo, link_name: &OsStr, target: &std::path::Path, reply: ReplyEntry) {
        let link_name = os_to_str!(link_name, reply);
        let target = match target.to_str() {
            Some(t) => t,
            None => {
                reply.error(Errno::EINVAL);
                return;
            }
        };
        match self.runtime.block_on(
            self.virtual_fs
                .symlink(parent.0, link_name, target, 0o777, req.uid(), req.gid()),
        ) {
            Ok(attr) => reply.entry(&self.metadata_ttl, &vfs_attr_to_fuse(&attr), GENERATION),
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    /// Read the target of a symbolic link.
    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        match self.virtual_fs.readlink(ino.0) {
            Ok(target) => reply.data(target.as_bytes()),
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    fn link(&self, _req: &Request, _ino: INodeNo, _newparent: INodeNo, _newname: &OsStr, reply: ReplyEntry) {
        reply.error(Errno::from_i32(libc::ENOTSUP));
    }

    /// Remove an empty directory.
    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let name = os_to_str!(name, reply);
        match self.runtime.block_on(self.virtual_fs.rmdir(parent.0, name)) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    /// Rename/move a file or directory.
    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: fuser::RenameFlags,
        reply: ReplyEmpty,
    ) {
        let _ = &flags; // used conditionally on linux

        // Reject unsupported flags
        #[cfg(target_os = "linux")]
        if flags.intersects(fuser::RenameFlags::RENAME_EXCHANGE | fuser::RenameFlags::RENAME_WHITEOUT) {
            reply.error(Errno::EINVAL);
            return;
        }

        let name = os_to_str!(name, reply);
        let newname = os_to_str!(newname, reply);

        #[cfg(target_os = "linux")]
        let no_replace = flags.contains(fuser::RenameFlags::RENAME_NOREPLACE);
        #[cfg(not(target_os = "linux"))]
        let no_replace = false;

        match self
            .runtime
            .block_on(self.virtual_fs.rename(parent.0, name, newparent.0, newname, no_replace))
        {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    /// Set file attributes (size, mode, uid, gid, timestamps).
    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<fuser::BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let resolve_time = |t: TimeOrNow| match t {
            TimeOrNow::SpecificTime(st) => st,
            TimeOrNow::Now => SystemTime::now(),
        };
        match self.runtime.block_on(self.virtual_fs.setattr(
            ino.0,
            size,
            mode.map(|m| (m & 0o7777) as u16),
            uid,
            gid,
            atime.map(resolve_time),
            mtime.map(resolve_time),
        )) {
            Ok(attr) => reply.attr(&self.metadata_ttl, &vfs_attr_to_fuse(&attr)),
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    /// Open a directory (allocates a handle for readdir).
    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        match self.virtual_fs.getattr(ino.0) {
            Ok(attr) if attr.kind == InodeKind::Directory => {
                reply.opened(FileHandle(self.virtual_fs.alloc_file_handle()), FopenFlags::empty());
            }
            Ok(_) => reply.error(Errno::ENOTDIR),
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    /// Release a directory handle (no-op).
    fn releasedir(&self, _req: &Request, _ino: INodeNo, _fh: FileHandle, _flags: OpenFlags, reply: ReplyEmpty) {
        reply.ok();
    }

    /// Filesystem statistics (df). Reports 42 PB for fun
    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        const BLOCK_SIZE: u32 = 512;
        const BLOCKS: u64 = 42 * 1024 * 1024 * 1024 * 1024 * 1024 / BLOCK_SIZE as u64; // 42 PB
        //           blocks, bfree,  bavail, files, ffree, bsize,      namelen, frsize
        reply.statfs(BLOCKS, BLOCKS, BLOCKS, 0, 0, BLOCK_SIZE, 255, 0);
    }

    /// Called on unmount. Flushes pending writes and stops background tasks.
    fn destroy(&mut self) {
        self.virtual_fs.shutdown();
    }
}

/// Mount the VFS as a FUSE filesystem and block until unmount.
#[allow(clippy::too_many_arguments)]
pub fn mount_fuse(
    virtual_fs: Arc<VirtualFs>,
    mount_point: &Path,
    metadata_ttl: Duration,
    read_only: bool,
    advanced_writes: bool,
    direct_io: bool,
    max_threads: usize,
    runtime: &tokio::runtime::Runtime,
) {
    let adapter = FuseAdapter::new(
        runtime.handle().clone(),
        virtual_fs.clone(),
        metadata_ttl,
        read_only,
        advanced_writes,
        direct_io,
    );

    let mut config = fuser::Config::default();
    config.mount_options = vec![
        fuser::MountOption::FSName("hf-mount".to_string()),
        fuser::MountOption::DefaultPermissions,
    ];
    if read_only {
        config.mount_options.push(fuser::MountOption::RO);
    }
    config.acl = fuser::SessionACL::All;
    config.clone_fd = true;
    config.n_threads = Some(max_threads);

    let session = match fuser::Session::new(adapter, mount_point, &config) {
        Ok(s) => s,
        Err(e) => {
            if e.kind() == std::io::ErrorKind::PermissionDenied {
                error!(
                    "Permission denied: mounting a FUSE filesystem requires root privileges. \
                     Try running with: sudo {}",
                    std::env::args().collect::<Vec<_>>().join(" ")
                );
            } else {
                error!("FUSE session failed: {}", e);
            }
            std::process::exit(1);
        }
    };
    let notifier = session.notifier();
    virtual_fs.set_invalidator(Box::new(move |ino| {
        if let Err(e) = notifier.inval_inode(fuser::INodeNo(ino), 0, -1) {
            tracing::debug!("inval_inode({}) failed: {}", ino, e);
        }
    }));
    let bg = match session.spawn() {
        Ok(bg) => bg,
        Err(e) => {
            error!("FUSE spawn failed: {}", e);
            std::process::exit(1);
        }
    };

    // Catch SIGINT/SIGTERM and trigger a clean unmount. On success, fuser
    // calls destroy() which runs shutdown(). On failure, we flush here before
    // exiting so dirty data is not silently lost.
    // We intentionally do NOT call shutdown() before unmount: FUSE is still
    // live, so other processes could write to staging files during the flush,
    // causing CAS to upload a mid-write snapshot.
    let vfs_for_signal = virtual_fs.clone();
    let mp = mount_point.to_path_buf();
    runtime.spawn(async move {
        wait_for_signal().await;
        info!("Received signal, unmounting...");
        if !unmount_fuse(&mp) {
            // Unmount failed -- flush dirty data before force-exiting so we
            // don't silently lose writes.
            error!("Unmount failed, flushing dirty files before exit...");
            vfs_for_signal.shutdown();
            std::process::exit(1);
        }
    });

    let _ = bg.join();
    // Safety net: flush after FUSE session ends. Covers external unmount
    // (e.g. `fusermount -u`) where destroy() may not fire. Idempotent
    // because shutdown() takes handles from Mutex<Option<...>>.
    virtual_fs.shutdown();
}

/// Wait for SIGINT, SIGTERM, or SIGHUP.
async fn wait_for_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sigterm = signal(SignalKind::terminate()).expect("Failed to register SIGTERM handler");
    let mut sighup = signal(SignalKind::hangup()).expect("Failed to register SIGHUP handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sigterm.recv() => {}
        _ = sighup.recv() => {}
    }
}

/// Trigger FUSE unmount. Returns `true` on success. Uses libc as primary
/// method (no external process dependency), then falls back to fusermount/umount.
fn unmount_fuse(mount_point: &Path) -> bool {
    use std::ffi::CString;

    let c_path = CString::new(mount_point.to_string_lossy().as_bytes()).ok();

    // Try libc unmount first.
    if let Some(ref c_path) = c_path {
        #[cfg(target_os = "linux")]
        {
            // MNT_DETACH: lazy unmount, detaches immediately.
            if unsafe { libc::umount2(c_path.as_ptr(), libc::MNT_DETACH) } == 0 {
                return true;
            }
        }
        #[cfg(target_os = "macos")]
        {
            // MNT_FORCE: force unmount even with open files.
            if unsafe { libc::unmount(c_path.as_ptr(), libc::MNT_FORCE) } == 0 {
                return true;
            }
        }
    }

    // Fallback: external command. Try fusermount3 first (FUSE3), then fusermount.
    #[cfg(target_os = "linux")]
    let cmd_ok = std::process::Command::new("fusermount3")
        .args(["-u", "-z", &mount_point.to_string_lossy()])
        .status()
        .is_ok_and(|s| s.success())
        || std::process::Command::new("fusermount")
            .args(["-u", "-z", &mount_point.to_string_lossy()])
            .status()
            .is_ok_and(|s| s.success());
    #[cfg(target_os = "macos")]
    let cmd_ok = std::process::Command::new("umount")
        .arg(mount_point)
        .status()
        .is_ok_and(|s| s.success());

    if !cmd_ok {
        error!("Failed to unmount {:?}", mount_point);
        return false;
    }
    true
}
