use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use tracing::debug;

use crate::xet::StagingDir;

use super::inode::InodeTable;

/// Bundles the on-disk staging area with per-inode locks so subsystems
/// outside `VirtualFs` (e.g. the flush-path GC) can take the same lock as
/// `open_advanced_write` / `setattr(truncate)` to serialize staging I/O.
///
/// Two lock maps are tracked per inode:
/// - `locks` (tokio async): held across awaits (download, unlink). Coarse
///   serialization between `open_advanced_write`, `setattr(truncate)`, and
///   the flush-path GC.
/// - `io_locks` (std sync): held briefly around the per-syscall I/O critical
///   sections (`pread` + sparse coverage snapshot, `pwrite` + `track_write`,
///   range_upload's per-chunk reads). Sync because callers include both
///   async tasks (read, flush) and sync code paths (write, called from
///   FUSE/NFS handlers without spawn_blocking). The critical sections hold
///   no `.await`, so a sync mutex is safe everywhere.
pub(crate) struct StagingCoordinator {
    dir: Option<StagingDir>,
    locks: Mutex<HashMap<u64, Arc<tokio::sync::Mutex<()>>>>,
    io_locks: Mutex<HashMap<u64, Arc<std::sync::Mutex<()>>>>,
}

impl StagingCoordinator {
    pub(crate) fn new(dir: Option<StagingDir>) -> Self {
        Self {
            dir,
            locks: Mutex::new(HashMap::new()),
            io_locks: Mutex::new(HashMap::new()),
        }
    }

    /// Sync per-inode lock for serializing pread / pwrite / range_upload's
    /// per-chunk reads. Held only across non-await operations — never block
    /// an async runtime worker.
    #[allow(dead_code)]
    pub(crate) fn io_lock(&self, ino: u64) -> Arc<std::sync::Mutex<()>> {
        self.io_locks
            .lock()
            .expect("staging io_locks poisoned")
            .entry(ino)
            .or_insert_with(|| Arc::new(std::sync::Mutex::new(())))
            .clone()
    }

    pub(crate) fn dir(&self) -> Option<&StagingDir> {
        self.dir.as_ref()
    }

    pub(crate) fn path(&self, ino: u64) -> Option<PathBuf> {
        self.dir.as_ref().map(|sd| sd.path(ino))
    }

    /// Get or create the per-inode async lock. Held across awaits (download,
    /// unlink) so concurrent opens and flush-path GC can't interleave.
    pub(crate) fn lock(&self, ino: u64) -> Arc<tokio::sync::Mutex<()>> {
        self.locks
            .lock()
            .expect("staging locks poisoned")
            .entry(ino)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Unconditionally remove an inode's staging file under the per-inode
    /// lock. Used by paths that have already decided the inode is gone
    /// (unlink, rename-replaced target). Holding the lock serializes with
    /// in-flight `flush_batch` uploads so xet-core never sees the file
    /// vanish mid-read.
    pub(crate) async fn drop_locked(&self, ino: u64) {
        let Some(dir) = self.dir() else { return };
        let lock = self.lock(ino);
        let _guard = lock.lock().await;
        dir.try_remove(ino);
    }

    /// Reclaim a single clean inode's staging file when usage exceeds the
    /// disk budget. Takes the per-inode lock so in-flight opens can't race
    /// the unlink, then re-checks `is_dirty` under the inode read lock to
    /// catch a concurrent writer that set dirty while we waited. Returns
    /// true when the file was actually removed.
    pub(crate) async fn gc_one(&self, ino: u64, inodes: &RwLock<InodeTable>) -> bool {
        let Some(dir) = self.dir() else { return false };
        if !dir.is_over_limit() {
            return false;
        }
        let lock = self.lock(ino);
        let _guard = lock.lock().await;
        let still_clean = inodes
            .read()
            .expect("inodes poisoned")
            .get(ino)
            .is_some_and(|entry| !entry.is_dirty());
        still_clean && dir.try_remove(ino)
    }

    /// Reclaim staging files by least-recently-touched order until the disk
    /// budget is satisfied. Ranks eligible inodes (clean, no open handles,
    /// no pending renames) by `last_touched` ascending so actively-accessed
    /// files stay cached. No-op when under the budget.
    pub(crate) async fn gc(&self, inodes: &RwLock<InodeTable>) -> usize {
        let Some(dir) = self.dir() else { return 0 };
        if !dir.is_over_limit() {
            return 0;
        }
        let candidates = inodes.read().expect("inodes poisoned").staging_gc_candidates();
        let mut count = 0;
        for ino in candidates {
            if !dir.is_over_limit() {
                break;
            }
            // Only pay the try_remove cost if this inode actually has a staging file.
            if dir.file_size(ino) == 0 {
                continue;
            }
            if self.gc_one(ino, inodes).await {
                count += 1;
                debug!("staging GC: removed ino={}", ino);
            }
        }
        count
    }
}
