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

use crate::inode::InodeKind;
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
}

impl FuseAdapter {
    pub fn new(
        runtime: tokio::runtime::Handle,
        virtual_fs: Arc<VirtualFs>,
        metadata_ttl: Duration,
        read_only: bool,
        advanced_writes: bool,
    ) -> Self {
        Self {
            runtime,
            virtual_fs,
            metadata_ttl,
            read_only,
            advanced_writes,
        }
    }
}

fn vfs_attr_to_fuse(attr: &VirtualFsAttr) -> FileAttr {
    let kind = match attr.kind {
        InodeKind::File => FileType::RegularFile,
        InodeKind::Directory => FileType::Directory,
    };
    FileAttr {
        ino: INodeNo(attr.ino),
        size: attr.size,
        blocks: attr.blocks,
        atime: attr.mtime,
        mtime: attr.mtime,
        ctime: attr.mtime,
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
                // KEEP_CACHE preserves the kernel page cache across re-opens.
                // The poll loop calls notify_inval_inode on remote changes;
                // the short metadata TTL handles local change visibility.
                reply.opened(FileHandle(file_handle), FopenFlags::FOPEN_KEEP_CACHE);
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
        self.runtime.block_on(self.virtual_fs.release(fh.0));
        reply.ok();
    }

    /// Create and open a new file in one call (O_CREAT).
    fn create(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let name = os_to_str!(name, reply);
        match self
            .runtime
            .block_on(self.virtual_fs.create(parent.0, name, Some(req.pid())))
        {
            Ok((attr, file_handle)) => {
                reply.created(
                    &self.metadata_ttl,
                    &vfs_attr_to_fuse(&attr),
                    GENERATION,
                    FileHandle(file_handle),
                    FopenFlags::empty(),
                );
            }
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    /// Create a new directory.
    fn mkdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, _mode: u32, _umask: u32, reply: ReplyEntry) {
        let name = os_to_str!(name, reply);
        match self.runtime.block_on(self.virtual_fs.mkdir(parent.0, name)) {
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

    /// Set file attributes. Only size (truncate) is supported.
    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<fuser::BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        match self.runtime.block_on(self.virtual_fs.setattr(ino.0, size)) {
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
pub fn mount_fuse(
    virtual_fs: Arc<VirtualFs>,
    mount_point: &Path,
    metadata_ttl: Duration,
    read_only: bool,
    advanced_writes: bool,
    max_threads: usize,
    runtime: &tokio::runtime::Runtime,
) {
    let adapter = FuseAdapter::new(
        runtime.handle().clone(),
        virtual_fs.clone(),
        metadata_ttl,
        read_only,
        advanced_writes,
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

    // Catch SIGINT/SIGTERM and trigger a clean unmount instead of letting
    // the default handler kill the process (which leaves a stale mount).
    let mp = mount_point.to_path_buf();
    runtime.spawn(async move {
        wait_for_signal().await;
        info!("Received signal, unmounting...");
        unmount_fuse(&mp);
    });

    let _ = bg.join();
}

/// Wait for SIGINT or SIGTERM.
async fn wait_for_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sigterm = signal(SignalKind::terminate()).expect("Failed to register SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sigterm.recv() => {}
    }
}

/// Trigger FUSE unmount. Uses libc as primary method (no external process
/// dependency), then falls back to fusermount/umount. If all fail, force-exits
/// to avoid hanging (tokio's signal handler replaces the default one, so
/// bg.join() would block forever if unmount fails).
fn unmount_fuse(mount_point: &Path) {
    use std::ffi::CString;

    let c_path = CString::new(mount_point.to_string_lossy().as_bytes()).ok();

    // Try libc unmount first (like mountpoint-s3).
    if let Some(ref c_path) = c_path {
        #[cfg(target_os = "linux")]
        {
            // MNT_DETACH: lazy unmount, detaches immediately.
            if unsafe { libc::umount2(c_path.as_ptr(), libc::MNT_DETACH) } == 0 {
                return;
            }
        }
        #[cfg(target_os = "macos")]
        {
            // MNT_FORCE: force unmount even with open files.
            if unsafe { libc::unmount(c_path.as_ptr(), libc::MNT_FORCE) } == 0 {
                return;
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
        error!("Failed to unmount {:?}, forcing exit", mount_point);
        std::process::exit(1);
    }
}
