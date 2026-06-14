use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use tracing::{debug, error, info, warn};
use xet_data::processing::XetFileInfo;

use crate::file_cache::FileCache;
use crate::hub_api::{BatchOp, HubOps};
use crate::overlay::OverlayBacking;

mod flush;
pub mod inode;
mod poll;
mod prefetch;
mod staging;
use crate::xet::{StagingDir, StreamingWriterOps, XetOps};
use inode::{InodeEntry, InodeKind, InodeTable};
use prefetch::{FetchPlan, PrefetchState};
use staging::StagingCoordinator;

// ── Constants ──────────────────────────────────────────────────────────

/// Block size reported in stat(2) for `st_blocks` calculation.
const BLOCK_SIZE: u32 = 512;
/// Maximum entries in the negative-lookup cache (parent_path/name → Instant).
/// Prevents repeated Hub API calls for paths known to not exist. Each entry
/// holds an owned `String` key, so this cache is the dominant cost of the
/// negative-cache pool. 1k is enough to absorb bursts; older entries roll
/// over and a real lookup absorbs the cost on miss.
const NEG_CACHE_CAPACITY: usize = 1_000;
/// How long a negative-cache entry stays valid before being re-checked.
const NEG_CACHE_TTL: Duration = Duration::from_secs(30);
/// `notify_inval_entry` is a blocking syscall that takes the parent dir's
/// `i_rwsem` in the kernel and walks the dcache. Issuing thousands per sweep
/// starves concurrent FUSE ops (lookup/readdir wait on the same lock) and
/// can leave a worker in `D` state for seconds. Cap the batch and pause
/// between batches so the kernel can drain.
const INVAL_BATCH_SIZE: usize = 64;
const INVAL_BATCH_PAUSE: Duration = Duration::from_millis(10);

type InvalidatorFn = Box<dyn Fn(u64) + Send + Sync>;
/// `(parent, name) -> Ok/Err` maps to `fuse_notify_inval_entry`. Returns
/// false when the FUSE notify channel is saturated (EAGAIN/ENOMEM) so the
/// sweep can back off instead of burning CPU on a full queue. Separate
/// from `InvalidatorFn` because cgroup-bound memory pressure doesn't
/// propagate to the host's dentry shrinker, so the LRU sweep has to push
/// dentry drops instead of waiting for the kernel to pull them.
type EntryInvalidatorFn = Box<dyn Fn(u64, &str) -> bool + Send + Sync>;
type Invalidator = Arc<OnceLock<InvalidatorFn>>;
type EntryInvalidator = Arc<OnceLock<EntryInvalidatorFn>>;

type CommitHookTx = tokio::sync::watch::Sender<Option<Result<(), i32>>>;
type CommitHookRx = tokio::sync::watch::Receiver<Option<Result<(), i32>>>;

/// Returns `true` for OS-generated junk files that should not be synced to remote storage.
fn is_os_junk(name: &str) -> bool {
    matches!(
        name,
        ".DS_Store" | ".Spotlight-V100" | ".Trashes" | ".fseventsd" | "__MACOSX" | "Thumbs.db" | "desktop.ini"
    ) || name.starts_with("._")
}

// ── VirtualFs ──────────────────────────────────────────────────────────

/// Configuration for [`VirtualFs`], grouping all tunable options.
pub struct VfsConfig {
    pub read_only: bool,
    pub advanced_writes: bool,
    pub uid: u32,
    pub gid: u32,
    pub poll_interval_secs: u64,
    /// Maximum concurrent tree-listing requests per poll round.
    /// Must be >= 1.
    pub poll_listing_concurrency: usize,
    pub metadata_ttl: Duration,
    pub serve_lookup_from_cache: bool,
    pub filter_os_files: bool,
    pub direct_io: bool,
    pub flush_debounce: Duration,
    pub flush_max_batch_window: Duration,
    /// 0 disables the LRU evictor.
    pub inode_soft_limit: usize,
    pub lru_sweep_interval: Duration,
}

/// Lock ordering (acquire in this order to prevent deadlocks):
///
///   dir_loading_locks[ino]      (tokio::sync::Mutex, per-directory)
///     → inode_table             (RwLock, read or write)
///
///   staging.lock(ino)           (tokio::sync::Mutex, per-inode)
///     → inode_table             (RwLock, read or write)
///         → open_files          (RwLock, read only — via has_open_handles)
///         → negative_cache      (RwLock, write — in poll_remote_changes)
///
///   StreamingChannel.commit_hook (Mutex)
///     → pending_commits          (Mutex)
///
/// General discipline: locks are held briefly and never across await points
/// (except the per-inode tokio::sync::Mutex from StagingCoordinator). Most paths acquire a lock,
/// extract data, drop the lock, perform async I/O, then re-acquire to apply.
///
/// Exception: setattr(truncate) holds inode_table.write() across File::create
/// / set_len syscalls (microseconds) to prevent write() from updating
/// inode.size between the file truncation and the metadata update.
pub struct VirtualFs {
    runtime: tokio::runtime::Handle,
    hub_client: Arc<dyn HubOps>,
    xet_sessions: Arc<dyn XetOps>,
    /// Staging area + per-inode locks. The per-inode locks are always
    /// present (used even in simple mode to serialize streaming writer
    /// creation); the staging directory is `None` outside advanced writes.
    /// Shared via `Arc` so flush-path subsystems can take the same per-inode
    /// lock as `open_advanced_write`.
    staging: Arc<StagingCoordinator>,
    /// Overlay backing fd. When `Some`, writes stay local under the pre-mount
    /// directory accessed via this fd, and remote mutations are skipped.
    overlay_backing: Option<Arc<OverlayBacking>>,
    read_only: bool,
    advanced_writes: bool,
    inode_table: Arc<RwLock<InodeTable>>,
    /// Maps file_handle → OpenFile (local fd or lazy remote reference).
    open_files: Arc<RwLock<HashMap<u64, OpenFile>>>,
    next_file_handle: AtomicU64,
    uid: u32,
    gid: u32,
    /// Negative lookup cache: paths known to not exist (TTL-based).
    negative_cache: Arc<RwLock<HashMap<String, Instant>>>,
    /// Per-directory loading locks: serializes concurrent ensure_children_loaded() calls
    /// for the same directory so only one HTTP request is made (prevents thundering herd
    /// when Finder/Spotlight send many lookups on mount).
    ///
    /// Stored as `Weak` so entries self-clean once no loader holds the Arc — without this,
    /// long-lived mounts over churning directory trees (e.g. CI doc buckets with PR dirs)
    /// would accumulate one entry per inode ever loaded.
    dir_loading_locks: Mutex<HashMap<u64, Weak<tokio::sync::Mutex<()>>>>,
    /// Per-inode pending commit receivers. release() publishes the commit result here;
    /// open() awaits it instead of blindly blocking on staging_lock.
    pending_commits: Mutex<HashMap<u64, CommitHookRx>>,
    /// Batched flush pipeline: dirty file writes + remote delete queue.
    /// Only present in advanced_writes mode.
    flush_manager: Option<flush::FlushManager>,
    /// Background poll task handle, aborted in shutdown().
    poll_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Kernel cache invalidation callback. Set via `set_invalidator()` after mount.
    /// The poll loop calls this to actively invalidate stale inodes when remote changes
    /// are detected, allowing the kernel page cache to be used instead of DIRECT_IO.
    invalidator: Invalidator,
    entry_invalidator: EntryInvalidator,
    /// Background LRU evictor handle, aborted in `shutdown()`.
    lru_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// How long a file's metadata is trusted before re-checking via HEAD.
    /// Matches the kernel metadata TTL so HEAD is called at most once per TTL window.
    metadata_ttl: Duration,
    /// When false (minimal mode), every lookup triggers a HEAD request regardless
    /// of `last_revalidated`.
    /// When true, lookups within the metadata TTL window skip HEAD.
    serve_lookup_from_cache: bool,
    /// When true, reject creation of OS junk files (.DS_Store, Thumbs.db, etc.).
    filter_os_files: bool,
    /// When true, prefetch buffers drain after serving (forward-only, no re-read cache).
    direct_io: bool,
    /// Optional whole-file cache. When `Some`, opens hit the local copy if the
    /// xet hash is already populated; misses kick a background populate so the
    /// next open is fast. Mutually exclusive with xet-core's chunk cache.
    file_cache: Option<Arc<FileCache>>,
}

mod core;
mod handle;
mod lookup;
mod attr;
mod open;
mod io;
mod dir;
mod rename;


// ── VFS types ──────────────────────────────────────────────────────────
pub type VirtualFsError = i32;
pub type VirtualFsResult<T> = std::result::Result<T, VirtualFsError>;

#[derive(Debug)]
pub struct VirtualFsAttr {
    pub ino: u64,
    pub size: u64,
    pub blocks: u64,
    pub mtime: SystemTime,
    pub atime: SystemTime,
    pub ctime: SystemTime,
    pub kind: InodeKind,
    pub perm: u16,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
}

#[derive(Debug)]
pub struct VirtualFsDirEntry {
    pub ino: u64,
    pub kind: InodeKind,
    pub name: String,
}

/// Snapshot of rename state collected under read lock (phase 1),
/// consumed by remote sync (phase 2) and local apply (phase 3).
struct RenameInfo {
    ino: u64,
    old_path: String,
    kind: InodeKind,
    xet_hash: Option<String>,
    is_dirty: bool,
    new_full_path: String,
    /// Descendant clean files to rename on the remote (dir renames only).
    descendant_files: Vec<(String, String)>,
}

// ── Internal types ─────────────────────────────────────────────────────

/// Snapshot of file inode fields captured under a short-lived read lock.
struct FileEntry {
    xet_hash: String,
    size: u64,
    is_dirty: bool,
    full_path: String,
}

/// State machine for streaming writes.
/// Message sent from the write() caller to the background streaming worker.
enum WriteMsg {
    Data(Vec<u8>),
    Finish(tokio::sync::oneshot::Sender<crate::error::Result<XetFileInfo>>),
}

/// Snapshot of inode state captured when a streaming writer is opened.
/// Used to revert the inode on commit failure (data loss recovery).
struct InodeSnapshot {
    xet_hash: Option<String>,
    size: u64,
    mtime: SystemTime,
    pending_deletes: Vec<String>,
    /// false for create() (new file), true for open(O_TRUNC) (overwrite).
    existed_before: bool,
}

/// Lifecycle state of a streaming write channel's commit.
enum CommitState {
    /// Streaming writes in progress. Worker is running.
    Writing,
    /// flush() deferred commit (dup'd fd or zero writes). release() will handle it.
    Deferred,
    /// Commit completed successfully.
    Committed,
    /// Unrecoverable error — inode has been reverted.
    Failed(String),
}

/// Channel-based streaming handle. Decouples the sync write() caller from the
/// async add_data() pipeline: writes enqueue data into a bounded channel
/// (avoids deadlock when tokio worker threads are saturated by FUSE block_on calls),
/// a background tokio task drains it and feeds the CAS cleaner.
struct StreamingChannel {
    tx: tokio::sync::mpsc::Sender<WriteMsg>,
    bytes_written: AtomicU64,
    /// Set by the background worker if add_data() fails. Shared with worker via Arc.
    error: Arc<std::sync::Mutex<Option<String>>>,
    /// Commit lifecycle state machine.
    state: std::sync::Mutex<CommitState>,
    /// CAS upload succeeded but Hub commit failed — stored for retry.
    pending_info: std::sync::Mutex<Option<XetFileInfo>>,
    /// PID of the process that opened this file (for dup'd fd detection).
    open_pid: Option<u32>,
    /// Pre-write inode snapshot for revert on commit failure.
    snapshot: InodeSnapshot,
    /// Dirty generation at open time, used by streaming_commit to avoid
    /// clobbering concurrent writers via clear_dirty_if. AtomicU64 so it
    /// can be updated after Arc construction (set after inode mutation).
    dirty_generation_at_open: AtomicU64,
    /// Watch sender for the pending commit hook. Created in flush() on deferral,
    /// fulfilled in release() when the commit completes (or fails).
    commit_hook: std::sync::Mutex<Option<CommitHookTx>>,
}

/// An open file handle — either a local fd, lazy remote reference, or streaming writer.
enum OpenFile {
    /// Local file (staging for writes, or dirty reads).
    /// Wrapped in a Mutex so concurrent read/write/seek is safe on Windows
    /// (where File is not Sync) and so seek+read/write replaces pread/pwrite.
    Local {
        ino: u64,
        file: Arc<Mutex<File>>,
        writable: bool,
    },
    /// Lazy remote — data fetched on-demand with adaptive prefetch buffer.
    Lazy {
        ino: u64,
        prefetch: Arc<tokio::sync::Mutex<PrefetchState>>,
    },
    /// Streaming append-only writer (default write mode).
    Streaming { ino: u64, channel: Arc<StreamingChannel> },
}

/// Check whether two PIDs belong to the same process.
/// On Linux, compares thread-group IDs via /proc to handle PID namespaces.
/// On macOS (no /proc), falls back to direct PID comparison.
fn same_process(pid_a: u32, pid_b: u32) -> bool {
    if pid_a == pid_b {
        return true;
    }

    #[cfg(target_os = "linux")]
    {
        fn read_tgid(pid: u32) -> Option<u32> {
            let path = format!("/proc/{}/status", pid);
            let status = std::fs::read_to_string(path).ok()?;
            for line in status.lines() {
                if let Some(val) = line.strip_prefix("Tgid:\t") {
                    return val.trim().parse().ok();
                }
            }
            None
        }
        match (read_tgid(pid_a), read_tgid(pid_b)) {
            (Some(a), Some(b)) => a == b,
            _ => false,
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

/// Background task: drains the write channel and feeds data into the CAS streaming writer.
/// Runs until a Finish message is received or the channel is dropped.
async fn streaming_worker(
    mut writer: Box<dyn StreamingWriterOps>,
    mut rx: tokio::sync::mpsc::Receiver<WriteMsg>,
    error: Arc<std::sync::Mutex<Option<String>>>,
) {
    let mut failed = false;
    while let Some(msg) = rx.recv().await {
        match msg {
            WriteMsg::Data(data) => {
                if failed {
                    continue; // drain remaining messages
                }
                if let Err(e) = writer.write(&data).await {
                    *error.lock().unwrap() = Some(e.to_string());
                    failed = true;
                }
            }
            WriteMsg::Finish(reply) => {
                let result = if failed {
                    Err(crate::error::Error::hub("streaming write failed"))
                } else {
                    writer.finish_boxed().await
                };
                let _ = reply.send(result);
                return;
            }
        }
    }
    // Channel closed without Finish → data is lost (same as close-without-flush)
}

/// What to do in read() after releasing the open_files lock.
enum ReadTarget {
    /// Hold an Arc<Mutex<File>> so the FD stays alive even if release() runs concurrently.
    LocalFd(Arc<Mutex<File>>),
    Remote {
        prefetch: Arc<tokio::sync::Mutex<PrefetchState>>,
    },
}

#[cfg(test)]
mod tests;
