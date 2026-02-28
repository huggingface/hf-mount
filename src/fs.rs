use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags, Generation, INodeNo,
    OpenFlags, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen,
    ReplyStatfs, ReplyWrite, Request, TimeOrNow,
};
use tokio::sync::mpsc;
use tracing::{debug, error, info};

use crate::cache::FileCache;
use crate::hub_api::{BatchOp, HubApiClient};
use crate::inode::{InodeEntry, InodeKind, InodeTable};

const TTL: Duration = Duration::from_secs(60);
const BLOCK_SIZE: u32 = 512;
const DEBOUNCE_DURATION: Duration = Duration::from_secs(2);
const MAX_BATCH_WINDOW: Duration = Duration::from_secs(30);

/// A request to flush a dirty file to CAS + bucket.
/// Only carries the inode number; full_path and staging_path are resolved
/// at flush time from the inode table to handle renames/unlinks correctly.
struct FlushRequest {
    ino: u64,
}

pub struct HfFs {
    rt: tokio::runtime::Handle,
    hub_client: Arc<HubApiClient>,
    bucket_id: String,
    cache: Arc<FileCache>,
    read_only: bool,
    inodes: Arc<Mutex<InodeTable>>,
    /// Maps fh → (inode, open File handle, writable)
    open_files: Mutex<HashMap<u64, (u64, File, bool)>>,
    next_fh: Mutex<u64>,
    uid: u32,
    gid: u32,
    /// Channel to send dirty files for debounced batch flush.
    flush_tx: Option<mpsc::UnboundedSender<FlushRequest>>,
    /// Handle to the background flush task, used for graceful shutdown.
    flush_handle: Option<tokio::task::JoinHandle<()>>,
}

impl HfFs {
    pub fn new(
        rt: tokio::runtime::Handle,
        hub_client: Arc<HubApiClient>,
        bucket_id: String,
        cache: Arc<FileCache>,
        read_only: bool,
        uid: u32,
        gid: u32,
    ) -> Self {
        let inodes = Arc::new(Mutex::new(InodeTable::new()));

        let (flush_tx, flush_handle) = if !read_only {
            let (tx, rx) = mpsc::unbounded_channel::<FlushRequest>();
            let bg_cache = cache.clone();
            let bg_hub = hub_client.clone();
            let bg_bucket = bucket_id.clone();
            let bg_inodes = inodes.clone();

            let handle = rt.spawn(Self::flush_loop(rx, bg_cache, bg_hub, bg_bucket, bg_inodes));

            (Some(tx), Some(handle))
        } else {
            (None, None)
        };

        Self {
            rt,
            hub_client,
            bucket_id,
            cache,
            read_only,
            inodes,
            open_files: Mutex::new(HashMap::new()),
            next_fh: Mutex::new(1),
            uid,
            gid,
            flush_tx,
            flush_handle,
        }
    }

    /// Background task: accumulates flush requests with debounce,
    /// then uploads all files in a single session + single batch call.
    async fn flush_loop(
        mut rx: mpsc::UnboundedReceiver<FlushRequest>,
        cache: Arc<FileCache>,
        hub_client: Arc<HubApiClient>,
        bucket_id: String,
        inodes: Arc<Mutex<InodeTable>>,
    ) {
        loop {
            // Wait for the first request
            let first = match rx.recv().await {
                Some(req) => req,
                None => return, // channel closed, exit
            };

            let mut pending = vec![first];

            // Debounce: keep collecting for DEBOUNCE_DURATION after each new item,
            // but cap total wait at MAX_BATCH_WINDOW to avoid unbounded delay.
            let window_deadline = tokio::time::Instant::now() + MAX_BATCH_WINDOW;
            loop {
                let remaining = window_deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let timeout = DEBOUNCE_DURATION.min(remaining);
                match tokio::time::timeout(timeout, rx.recv()).await {
                    Ok(Some(req)) => pending.push(req),
                    _ => break, // timeout (debounce expired) or channel closed
                }
            }

            let count = pending.len();
            info!("Flushing batch of {} dirty file(s)", count);

            Self::flush_batch(pending, &cache, &hub_client, &bucket_id, &inodes).await;
        }
    }

    /// Upload a batch of files in a single session, then commit via a single /batch call.
    /// Resolves paths from the inode table at flush time (handles renames/unlinks).
    async fn flush_batch(
        pending: Vec<FlushRequest>,
        cache: &FileCache,
        hub_client: &HubApiClient,
        bucket_id: &str,
        inodes: &Mutex<InodeTable>,
    ) {
        // Dedup by inode (keep last request per ino)
        let mut seen = std::collections::HashSet::new();
        let deduped: Vec<u64> = pending
            .into_iter()
            .rev()
            .filter(|r| seen.insert(r.ino))
            .map(|r| r.ino)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();

        // Resolve paths from inode table, skip deleted/non-dirty inodes
        let to_flush: Vec<(u64, String, PathBuf)> = {
            let inode_table = inodes.lock().unwrap();
            deduped
                .into_iter()
                .filter_map(|ino| {
                    let entry = inode_table.get(ino)?;
                    if !entry.dirty {
                        return None;
                    }
                    let staging_path = cache.staging_path(ino);
                    if !staging_path.exists() {
                        error!("Staging file missing for ino={}, skipping", ino);
                        return None;
                    }
                    Some((ino, entry.full_path.clone(), staging_path))
                })
                .collect()
        };

        if to_flush.is_empty() {
            return;
        }

        // Upload all files through a single upload session
        let staging_paths: Vec<&std::path::Path> =
            to_flush.iter().map(|(_, _, p)| p.as_path()).collect();
        let upload_results = match cache.upload_files(&staging_paths).await {
            Ok(results) => results,
            Err(e) => {
                error!("Batch upload failed: {}", e);
                return;
            }
        };

        let mtime_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Build batch operations
        let mut ops = Vec::with_capacity(to_flush.len());
        let mut successes: Vec<(u64, String, u64)> = Vec::new();

        for ((ino, full_path, _), file_info) in to_flush.iter().zip(upload_results.iter()) {
            info!(
                "Uploaded file ino={} path={} xet_hash={} size={}",
                ino,
                full_path,
                file_info.hash(),
                file_info.file_size()
            );

            ops.push(BatchOp::AddFile {
                path: full_path.clone(),
                xet_hash: file_info.hash().to_string(),
                mtime: mtime_ms,
                content_type: None,
            });

            successes.push((*ino, file_info.hash().to_string(), file_info.file_size()));
        }

        // Single batch commit
        if let Err(e) = hub_client.batch_operations(bucket_id, &ops).await {
            error!("Batch commit failed: {}", e);
            return;
        }

        // Update inodes
        let mut inode_table = inodes.lock().unwrap();
        let now = SystemTime::now();
        for (ino, xet_hash, size) in successes {
            if let Some(entry) = inode_table.get_mut(ino) {
                entry.xet_hash = Some(xet_hash);
                entry.size = size;
                entry.dirty = false;
                entry.mtime = now;
            }
        }

        info!("Batch flush completed: {} file(s) committed", to_flush.len());
    }

    fn inode_to_attr(&self, entry: &InodeEntry) -> FileAttr {
        let kind = match entry.kind {
            InodeKind::File => FileType::RegularFile,
            InodeKind::Directory => FileType::Directory,
        };
        let perm = if self.read_only {
            match entry.kind {
                InodeKind::File => 0o444,
                InodeKind::Directory => 0o555,
            }
        } else {
            match entry.kind {
                InodeKind::File => 0o644,
                InodeKind::Directory => 0o755,
            }
        };
        let nlink = match entry.kind {
            InodeKind::File => 1,
            InodeKind::Directory => 2,
        };

        FileAttr {
            ino: INodeNo(entry.inode),
            size: entry.size,
            blocks: (entry.size + BLOCK_SIZE as u64 - 1) / BLOCK_SIZE as u64,
            atime: entry.mtime,
            mtime: entry.mtime,
            ctime: entry.mtime,
            crtime: entry.mtime,
            kind,
            perm,
            nlink,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            blksize: BLOCK_SIZE,
            flags: 0,
        }
    }

    /// Ensure children of a directory inode are loaded from the Hub API.
    fn ensure_children_loaded(&self, parent_ino: u64) {
        let needs_load = {
            let inodes = self.inodes.lock().unwrap();
            inodes
                .get(parent_ino)
                .map(|e| !e.children_loaded && e.kind == InodeKind::Directory)
                .unwrap_or(false)
        };

        if !needs_load {
            return;
        }

        let prefix = {
            let inodes = self.inodes.lock().unwrap();
            match inodes.get(parent_ino) {
                Some(e) => e.full_path.clone(),
                None => return,
            }
        };

        let hub = self.hub_client.clone();
        let bucket_id = self.bucket_id.clone();

        let entries = match self.rt.block_on(hub.list_tree(&bucket_id, &prefix)) {
            Ok(entries) => entries,
            Err(e) => {
                error!("Failed to list tree for prefix '{}': {}", prefix, e);
                return;
            }
        };

        let mut inodes = self.inodes.lock().unwrap();

        let mut seen_dirs: std::collections::HashSet<String> = std::collections::HashSet::new();

        for entry in entries {
            let rel_path = if prefix.is_empty() {
                entry.path.as_str()
            } else {
                entry
                    .path
                    .strip_prefix(&prefix)
                    .and_then(|p| p.strip_prefix('/'))
                    .unwrap_or(&entry.path)
            };

            if let Some(slash_pos) = rel_path.find('/') {
                let dir_name = &rel_path[..slash_pos];
                if seen_dirs.insert(dir_name.to_string()) {
                    let dir_full_path = if prefix.is_empty() {
                        dir_name.to_string()
                    } else {
                        format!("{}/{}", prefix, dir_name)
                    };
                    inodes.insert(
                        parent_ino,
                        dir_name.to_string(),
                        dir_full_path,
                        InodeKind::Directory,
                        0,
                        UNIX_EPOCH,
                        None,
                    );
                }
            } else {
                let kind = if entry.entry_type == "directory" {
                    InodeKind::Directory
                } else {
                    InodeKind::File
                };
                let size = entry.size.unwrap_or(0);
                let mtime = entry
                    .mtime
                    .as_deref()
                    .map(HubApiClient::mtime_from_str)
                    .unwrap_or(UNIX_EPOCH);

                inodes.insert(
                    parent_ino,
                    rel_path.to_string(),
                    entry.path,
                    kind,
                    size,
                    mtime,
                    entry.xet_hash,
                );
            }
        }

        if let Some(parent) = inodes.get_mut(parent_ino) {
            parent.children_loaded = true;
        }
    }

    fn alloc_fh(&self) -> FileHandle {
        let mut fh = self.next_fh.lock().unwrap();
        let val = *fh;
        *fh += 1;
        FileHandle(val)
    }

    /// Enqueue a dirty file for debounced batch flush.
    fn enqueue_flush(&self, ino: u64) {
        if let Some(tx) = &self.flush_tx {
            if tx.send(FlushRequest { ino }).is_err() {
                error!("Flush channel closed, cannot enqueue ino={}", ino);
            }
        }
    }
}

impl Filesystem for HfFs {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let parent = parent.0;
        let name = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(Errno::ENOENT);
                return;
            }
        };

        debug!("lookup: parent={}, name={}", parent, name);

        self.ensure_children_loaded(parent);

        let inodes = self.inodes.lock().unwrap();
        match inodes.lookup_child(parent, name) {
            Some(entry) => {
                let attr = self.inode_to_attr(entry);
                reply.entry(&TTL, &attr, Generation(0));
            }
            None => {
                reply.error(Errno::ENOENT);
            }
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let ino = ino.0;
        debug!("getattr: ino={}", ino);

        let inodes = self.inodes.lock().unwrap();
        match inodes.get(ino) {
            Some(entry) => {
                let attr = self.inode_to_attr(entry);
                reply.attr(&TTL, &attr);
            }
            None => {
                reply.error(Errno::ENOENT);
            }
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let ino = ino.0;
        debug!("readdir: ino={}, offset={}", ino, offset);

        self.ensure_children_loaded(ino);

        let inodes = self.inodes.lock().unwrap();
        let entry = match inodes.get(ino) {
            Some(e) => e,
            None => {
                reply.error(Errno::ENOENT);
                return;
            }
        };

        let mut entries: Vec<(u64, FileType, String)> = Vec::new();
        entries.push((ino, FileType::Directory, ".".to_string()));
        entries.push((entry.parent, FileType::Directory, "..".to_string()));

        for &child_ino in &entry.children {
            if let Some(child) = inodes.get(child_ino) {
                let ft = match child.kind {
                    InodeKind::File => FileType::RegularFile,
                    InodeKind::Directory => FileType::Directory,
                };
                entries.push((child.inode, ft, child.name.clone()));
            }
        }

        for (i, (ino, ft, name)) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(INodeNo(ino), (i + 1) as u64, ft, name) {
                break;
            }
        }

        reply.ok();
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let ino = ino.0;
        debug!("open: ino={}, flags={:?}", ino, flags);

        let accmode = flags.0 & libc::O_ACCMODE;
        let writable = accmode == libc::O_WRONLY || accmode == libc::O_RDWR;
        let truncate = (flags.0 & libc::O_TRUNC) != 0;

        if writable && self.read_only {
            reply.error(Errno::EROFS);
            return;
        }

        let (full_path, xet_hash, size) = {
            let inodes = self.inodes.lock().unwrap();
            let entry = match inodes.get(ino) {
                Some(e) if e.kind == InodeKind::File => e,
                _ => {
                    reply.error(Errno::ENOENT);
                    return;
                }
            };

            (
                entry.full_path.clone(),
                entry.xet_hash.clone().unwrap_or_default(),
                entry.size,
            )
        };

        if writable {
            let staging_path = self.cache.staging_path(ino);

            if !truncate && !xet_hash.is_empty() && size > 0 {
                let cache = self.cache.clone();
                match self.rt.block_on(cache.ensure_cached(&xet_hash, size)) {
                    Ok(cached_path) => {
                        if let Err(e) = std::fs::copy(&cached_path, &staging_path) {
                            error!("Failed to copy to staging: {}", e);
                            reply.error(Errno::EIO);
                            return;
                        }
                    }
                    Err(e) => {
                        error!("Failed to cache file for write: {}", e);
                        reply.error(Errno::EIO);
                        return;
                    }
                }
            } else {
                if let Err(e) = File::create(&staging_path) {
                    error!("Failed to create staging file: {}", e);
                    reply.error(Errno::EIO);
                    return;
                }
            }

            match OpenOptions::new()
                .read(true)
                .write(true)
                .open(&staging_path)
            {
                Ok(file) => {
                    let fh = self.alloc_fh();
                    {
                        let mut inodes = self.inodes.lock().unwrap();
                        if let Some(entry) = inodes.get_mut(ino) {
                            entry.dirty = true;
                            if truncate {
                                entry.size = 0;
                            }
                        }
                    }
                    self.open_files
                        .lock()
                        .unwrap()
                        .insert(fh.0, (ino, file, true));
                    reply.opened(fh, FopenFlags::empty());
                }
                Err(e) => {
                    error!("Failed to open staging file: {}", e);
                    reply.error(Errno::EIO);
                }
            }
        } else {
            // Read-only open: check dirty staging first, then fall back to CAS cache
            let staging_path = self.cache.staging_path(ino);
            let is_dirty = {
                let inodes = self.inodes.lock().unwrap();
                inodes.get(ino).map(|e| e.dirty).unwrap_or(false)
            };

            let file_to_open = if is_dirty && staging_path.exists() {
                // Dirty file: read from staging area (handles files not yet flushed)
                staging_path
            } else if xet_hash.is_empty() {
                if size == 0 {
                    // Empty file with no hash: create a temp empty file to open
                    let empty_path = self.cache.staging_path(ino);
                    if !empty_path.exists() {
                        File::create(&empty_path).ok();
                    }
                    empty_path
                } else {
                    error!("No xet hash for non-empty, non-dirty file {}", full_path);
                    reply.error(Errno::EIO);
                    return;
                }
            } else {
                let cache = self.cache.clone();
                match self.rt.block_on(cache.ensure_cached(&xet_hash, size)) {
                    Ok(p) => p,
                    Err(e) => {
                        error!("Failed to cache file {}: {}", full_path, e);
                        reply.error(Errno::EIO);
                        return;
                    }
                }
            };

            match File::open(&file_to_open) {
                Ok(file) => {
                    let fh = self.alloc_fh();
                    self.open_files
                        .lock()
                        .unwrap()
                        .insert(fh.0, (ino, file, false));
                    reply.opened(fh, FopenFlags::empty());
                }
                Err(e) => {
                    error!("Failed to open cached file {:?}: {}", file_to_open, e);
                    reply.error(Errno::EIO);
                }
            }
        }
    }

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
        debug!("read: fh={}, offset={}, size={}", fh.0, offset, size);

        let files = self.open_files.lock().unwrap();
        match files.get(&fh.0) {
            Some((_ino, file, _writable)) => {
                let fd = file.as_raw_fd();
                let mut buf = vec![0u8; size as usize];

                let n = unsafe {
                    libc::pread(
                        fd,
                        buf.as_mut_ptr() as *mut libc::c_void,
                        size as usize,
                        offset as i64,
                    )
                };

                if n < 0 {
                    reply.error(Errno::EIO);
                } else {
                    buf.truncate(n as usize);
                    reply.data(&buf);
                }
            }
            None => {
                reply.data(&[]);
            }
        }
    }

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
        debug!(
            "write: ino={}, fh={}, offset={}, len={}",
            ino.0,
            fh.0,
            offset,
            data.len()
        );

        if self.read_only {
            reply.error(Errno::EROFS);
            return;
        }

        // Extract fd from open_files, then release the lock before acquiring inodes
        // to maintain consistent lock ordering (inodes before open_files elsewhere).
        let fd = {
            let files = self.open_files.lock().unwrap();
            match files.get(&fh.0) {
                Some((_, file, true)) => file.as_raw_fd(),
                Some((_, _, false)) => {
                    reply.error(Errno::EBADF);
                    return;
                }
                None => {
                    reply.error(Errno::EBADF);
                    return;
                }
            }
        };

        let n = unsafe {
            libc::pwrite(
                fd,
                data.as_ptr() as *const libc::c_void,
                data.len(),
                offset as i64,
            )
        };

        if n < 0 {
            reply.error(Errno::EIO);
        } else {
            let written = n as u32;
            let new_end = offset + written as u64;
            let mut inodes = self.inodes.lock().unwrap();
            if let Some(entry) = inodes.get_mut(ino.0) {
                if new_end > entry.size {
                    entry.size = new_end;
                }
            }
            reply.written(written);
        }
    }

    fn flush(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _lock_owner: fuser::LockOwner,
        reply: ReplyEmpty,
    ) {
        debug!("flush: ino={}", ino.0);
        // Actual upload happens via debounced batch in release()
        reply.ok();
    }

    fn release(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        debug!("release: ino={}, fh={}", ino.0, fh.0);

        let was_writable = {
            let files = self.open_files.lock().unwrap();
            files.get(&fh.0).map(|(_, _, w)| *w).unwrap_or(false)
        };

        if was_writable && !self.read_only {
            self.enqueue_flush(ino.0);
        }

        self.open_files.lock().unwrap().remove(&fh.0);
        reply.ok();
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        if self.read_only {
            reply.error(Errno::EROFS);
            return;
        }

        let name = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(Errno::EINVAL);
                return;
            }
        };

        debug!("create: parent={}, name={}", parent.0, name);

        let parent_path = {
            let inodes = self.inodes.lock().unwrap();
            match inodes.get(parent.0) {
                Some(e) if e.kind == InodeKind::Directory => e.full_path.clone(),
                _ => {
                    reply.error(Errno::ENOENT);
                    return;
                }
            }
        };

        let full_path = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent_path, name)
        };

        let now = SystemTime::now();
        let ino = {
            let mut inodes = self.inodes.lock().unwrap();
            let ino = inodes.insert(
                parent.0,
                name.to_string(),
                full_path,
                InodeKind::File,
                0,
                now,
                None,
            );
            if let Some(entry) = inodes.get_mut(ino) {
                entry.dirty = true;
            }
            ino
        };

        let staging_path = self.cache.staging_path(ino);
        match OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&staging_path)
        {
            Ok(file) => {
                let fh = self.alloc_fh();
                self.open_files
                    .lock()
                    .unwrap()
                    .insert(fh.0, (ino, file, true));

                let inodes = self.inodes.lock().unwrap();
                let attr = self.inode_to_attr(inodes.get(ino).unwrap());
                reply.created(&TTL, &attr, Generation(0), fh, FopenFlags::empty());
            }
            Err(e) => {
                error!("Failed to create staging file: {}", e);
                reply.error(Errno::EIO);
            }
        }
    }

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        if self.read_only {
            reply.error(Errno::EROFS);
            return;
        }

        let name = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(Errno::EINVAL);
                return;
            }
        };

        debug!("mkdir: parent={}, name={}", parent.0, name);

        self.ensure_children_loaded(parent.0);

        let parent_path = {
            let inodes = self.inodes.lock().unwrap();
            match inodes.get(parent.0) {
                Some(e) if e.kind == InodeKind::Directory => e.full_path.clone(),
                _ => {
                    reply.error(Errno::ENOENT);
                    return;
                }
            }
        };

        let full_path = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent_path, name)
        };

        let mut inodes = self.inodes.lock().unwrap();

        if inodes.lookup_child(parent.0, name).is_some() {
            reply.error(Errno::EEXIST);
            return;
        }

        let now = SystemTime::now();
        let ino = inodes.insert(
            parent.0,
            name.to_string(),
            full_path,
            InodeKind::Directory,
            0,
            now,
            None,
        );

        if let Some(entry) = inodes.get_mut(ino) {
            entry.children_loaded = true;
        }

        let attr = self.inode_to_attr(inodes.get(ino).unwrap());
        reply.entry(&TTL, &attr, Generation(0));
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        if self.read_only {
            reply.error(Errno::EROFS);
            return;
        }

        let name = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(Errno::EINVAL);
                return;
            }
        };

        debug!("unlink: parent={}, name={}", parent.0, name);

        self.ensure_children_loaded(parent.0);

        let (ino, full_path, was_dirty) = {
            let inodes = self.inodes.lock().unwrap();
            match inodes.lookup_child(parent.0, name) {
                Some(entry) if entry.kind == InodeKind::File => {
                    (entry.inode, entry.full_path.clone(), entry.dirty)
                }
                Some(_) => {
                    reply.error(Errno::EISDIR);
                    return;
                }
                None => {
                    reply.error(Errno::ENOENT);
                    return;
                }
            }
        };

        let needs_remote_delete = !was_dirty || {
            let inodes = self.inodes.lock().unwrap();
            inodes
                .get(ino)
                .and_then(|e| e.xet_hash.as_ref())
                .is_some()
        };

        if needs_remote_delete {
            let hub = self.hub_client.clone();
            let bucket_id = self.bucket_id.clone();
            let path = full_path.clone();

            if let Err(e) = self.rt.block_on(async {
                hub.batch_operations(&bucket_id, &[BatchOp::DeleteFile { path }])
                    .await
            }) {
                error!("Failed to delete file {}: {}", full_path, e);
                reply.error(Errno::EIO);
                return;
            }
        }

        let staging_path = self.cache.staging_path(ino);
        std::fs::remove_file(&staging_path).ok();

        self.inodes.lock().unwrap().remove(ino);

        info!("Deleted file: {}", full_path);
        reply.ok();
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        if self.read_only {
            reply.error(Errno::EROFS);
            return;
        }

        let name = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(Errno::EINVAL);
                return;
            }
        };

        debug!("rmdir: parent={}, name={}", parent.0, name);

        self.ensure_children_loaded(parent.0);

        let ino = {
            let inodes = self.inodes.lock().unwrap();
            match inodes.lookup_child(parent.0, name) {
                Some(entry) if entry.kind == InodeKind::Directory => {
                    if !entry.children.is_empty() {
                        reply.error(Errno::ENOTEMPTY);
                        return;
                    }
                    entry.inode
                }
                Some(_) => {
                    reply.error(Errno::ENOTDIR);
                    return;
                }
                None => {
                    reply.error(Errno::ENOENT);
                    return;
                }
            }
        };

        self.ensure_children_loaded(ino);

        {
            let inodes = self.inodes.lock().unwrap();
            if let Some(entry) = inodes.get(ino) {
                if !entry.children.is_empty() {
                    reply.error(Errno::ENOTEMPTY);
                    return;
                }
            }
        }

        self.inodes.lock().unwrap().remove(ino);
        reply.ok();
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        _flags: fuser::RenameFlags,
        reply: ReplyEmpty,
    ) {
        if self.read_only {
            reply.error(Errno::EROFS);
            return;
        }

        let name = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(Errno::EINVAL);
                return;
            }
        };
        let newname = match newname.to_str() {
            Some(n) => n,
            None => {
                reply.error(Errno::EINVAL);
                return;
            }
        };

        debug!(
            "rename: parent={}, name={}, newparent={}, newname={}",
            parent.0, name, newparent.0, newname
        );

        self.ensure_children_loaded(parent.0);
        if parent.0 != newparent.0 {
            self.ensure_children_loaded(newparent.0);
        }

        let (ino, old_path, kind, xet_hash, is_dirty) = {
            let inodes = self.inodes.lock().unwrap();
            match inodes.lookup_child(parent.0, name) {
                Some(entry) => (
                    entry.inode,
                    entry.full_path.clone(),
                    entry.kind,
                    entry.xet_hash.clone(),
                    entry.dirty,
                ),
                None => {
                    reply.error(Errno::ENOENT);
                    return;
                }
            }
        };

        let new_parent_path = {
            let inodes = self.inodes.lock().unwrap();
            match inodes.get(newparent.0) {
                Some(e) => e.full_path.clone(),
                None => {
                    reply.error(Errno::ENOENT);
                    return;
                }
            }
        };

        let new_full_path = if new_parent_path.is_empty() {
            newname.to_string()
        } else {
            format!("{}/{}", new_parent_path, newname)
        };

        if kind == InodeKind::File && xet_hash.is_some() && !is_dirty {
            let hub = self.hub_client.clone();
            let bucket_id = self.bucket_id.clone();
            let hash = xet_hash.unwrap();

            let mtime_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            let ops = vec![
                BatchOp::AddFile {
                    path: new_full_path.clone(),
                    xet_hash: hash,
                    mtime: mtime_ms,
                    content_type: None,
                },
                BatchOp::DeleteFile {
                    path: old_path.clone(),
                },
            ];

            if let Err(e) = self.rt.block_on(hub.batch_operations(&bucket_id, &ops)) {
                error!("Failed to rename {} -> {}: {}", old_path, new_full_path, e);
                reply.error(Errno::EIO);
                return;
            }
        }

        {
            let mut inodes = self.inodes.lock().unwrap();

            if let Some(old_parent) = inodes.get_mut(parent.0) {
                old_parent.children.retain(|&c| c != ino);
            }

            inodes.get(ino).map(|e| e.full_path.clone()).map(|old| {
                inodes.remove_path(&old);
            });

            if let Some(entry) = inodes.get_mut(ino) {
                entry.name = newname.to_string();
                entry.full_path = new_full_path.clone();
                entry.parent = newparent.0;
            }

            inodes.insert_path(new_full_path, ino);

            if let Some(new_parent) = inodes.get_mut(newparent.0) {
                new_parent.children.push(ino);
            }
        }

        reply.ok();
    }

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
        debug!("setattr: ino={}, size={:?}", ino.0, size);

        if self.read_only {
            reply.error(Errno::EROFS);
            return;
        }

        if let Some(new_size) = size {
            let staging_path = self.cache.staging_path(ino.0);

            if new_size == 0 {
                if let Err(e) = File::create(&staging_path) {
                    error!("Failed to truncate staging file: {}", e);
                    reply.error(Errno::EIO);
                    return;
                }
            } else if staging_path.exists() {
                if let Err(e) = OpenOptions::new()
                    .write(true)
                    .open(&staging_path)
                    .and_then(|f| f.set_len(new_size))
                {
                    error!("Failed to set staging file length: {}", e);
                    reply.error(Errno::EIO);
                    return;
                }
            }

            let mut inodes = self.inodes.lock().unwrap();
            if let Some(entry) = inodes.get_mut(ino.0) {
                entry.size = new_size;
                entry.dirty = true;
            }
        }

        let inodes = self.inodes.lock().unwrap();
        match inodes.get(ino.0) {
            Some(entry) => {
                let attr = self.inode_to_attr(entry);
                reply.attr(&TTL, &attr);
            }
            None => {
                reply.error(Errno::ENOENT);
            }
        }
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        debug!("opendir: ino={}", ino.0);
        let inodes = self.inodes.lock().unwrap();
        match inodes.get(ino.0) {
            Some(e) if e.kind == InodeKind::Directory => {
                let fh = self.alloc_fh();
                reply.opened(fh, FopenFlags::empty());
            }
            _ => reply.error(Errno::ENOENT),
        }
    }

    fn releasedir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        reply.statfs(0, 0, 0, 0, 0, 512, 255, 0);
    }

    fn destroy(&mut self) {
        info!("Destroying filesystem, flushing pending writes...");
        // Drop the sender to signal the flush loop to drain and exit.
        self.flush_tx.take();
        // Wait for the flush task to complete (processes remaining items).
        if let Some(handle) = self.flush_handle.take() {
            if let Err(e) = self.rt.block_on(handle) {
                error!("Flush task panicked: {}", e);
            }
        }
        info!("Flush loop finished, filesystem destroyed.");
    }
}
