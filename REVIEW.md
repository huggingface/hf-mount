# Code Review: Correctness, Invariant, and Coverage Concerns

## Bugs and Concerns

### 1. [HIGH] Poll thread can delete locally-dirty inodes — data loss

**Files:** `src/virtual_fs/poll.rs:109-118`

`file_snapshot()` (line 39) captures inodes as non-dirty. The **update** path
correctly re-checks dirty status inside the write lock (`inode.rs:310`). But
the **deletion** path does not re-check: if a local write marks an inode dirty
between the snapshot and the Phase 2 write lock, `inode_table.remove(ino)`
destroys the dirty inode, losing uncommitted data. The subsequent flush fails
because the inode is gone from the table.

**Scenario:**
1. File exists locally and remotely, clean.
2. `file_snapshot()` captures it as non-dirty.
3. Between snapshot and Phase 2 write-lock, a local write makes the inode dirty.
4. Poll thread takes write lock and calls `remove(ino)`, destroying the dirty
   inode along with its `full_path` mapping.
5. The in-flight write's eventual flush fails because the inode is gone.

**Fix:** Re-check `is_dirty()` inside the write lock before calling `remove()`:
```rust
for ino in &deletions {
    if let Some(entry) = inode_table.get(*ino) {
        if entry.is_dirty() {
            continue; // local writes take precedence
        }
        // ... proceed with removal
    }
}
```

---

### 2. [MEDIUM] Poll thread can delete inodes with open file handles

**Files:** `src/virtual_fs/poll.rs:109-118`

Same deletion path doesn't check whether the inode has open file handles. If a
file is opened for reading and is then remotely deleted, the inode is removed
while handles still reference it. Subsequent `read()`, `getattr()`, and
`release()` calls on those handles encounter a missing inode, causing spurious
`ENOENT` errors.

---

### 3. [MEDIUM] `apply_commit` clobbers `size` and `xet_hash` unconditionally

**Files:** `src/virtual_fs/inode.rs:88-97`

`self.xet_hash` and `self.size` are set **before** checking
`clear_dirty_if(dirty_generation)`. If a concurrent writer advanced the
generation, `clear_dirty_if` correctly returns false (inode stays dirty), but
the size has already been overwritten with the flushed (stale) value.

**Scenario:**
1. Writer A flushes 10 MB (gen 5).
2. While upload is in flight, Writer B extends to 15 MB (gen 6).
3. Flush completes, `apply_commit` sets `size = 10 MB`.
4. `clear_dirty_if(5)` returns false (gen is now 6), dirty stays.
5. Applications see `size = 10 MB` until next flush, even though staging file
   is 15 MB.

**Fix:** Move `self.size` and `self.xet_hash` inside the `clear_dirty_if` success branch:
```rust
pub fn apply_commit(&mut self, hash: &str, size: u64, dirty_generation: u64) {
    if self.clear_dirty_if(dirty_generation) {
        self.xet_hash = Some(hash.to_string());
        self.size = size;
        self.pending_deletes.clear();
    }
    let now = SystemTime::now();
    self.mtime = now;
    self.ctime = now;
}
```

---

### 4. [MEDIUM] `setattr` truncate races with concurrent `write` in advanced mode

**Files:** `src/virtual_fs/mod.rs:2556-2619` and `src/virtual_fs/mod.rs:1461-1468`

`setattr` acquires the staging lock and truncates the staging file. But
`write()` does `pwrite()` on a cloned `Arc<File>` fd **without** holding the
staging lock. A concurrent `pwrite` at offset N after `ftruncate(0)` creates a
sparse file, while the inode table says `size = 0`. The staging file content is
correct (has the write), but `inode.size` is wrong until next flush.

---

### 5. [MEDIUM] Rename phase 2/3 non-atomicity

**Files:** `src/virtual_fs/mod.rs:2257-2268`

Between `rename_remote()` (Phase 2) and `rename_apply_local()` (Phase 3),
concurrent operations can make Phase 3 fail with ENOENT/EEXIST while the remote
already has the rename applied. This leaves remote and local state divergent.
The comment says "next poll cycle or flush corrects it," but if the old path was
deleted remotely, a flush from the still-local inode targeting the old path may
fail.

---

### 6. [LOW-MEDIUM] NFS handle pool TOCTOU

**Files:** `src/nfs.rs:149-167`

Between `get_or_open_handle` returning a handle and `read()` using it, a
concurrent request can LRU-evict that handle (calling `release()`). The single
EBADF retry mitigates most cases, but under heavy load (>64 concurrent open
files), eviction churn can cause transient IO errors. A second concurrent
eviction during the retry is not handled.

---

### 7. [LOW] NFS serializes all reads on same inode through one prefetch lock

**Files:** `src/virtual_fs/mod.rs:1341`

The tokio Mutex on `PrefetchState` is held for the entire read including network
I/O. In FUSE each `open()` gets its own handle, but NFS shares one handle per
inode. Two NFS clients reading different parts of the same file serialize, and
the second reader's seek pattern disrupts the first's prefetch window.

---

## Missing Test Coverage

### `PrefetchState::prepare_fetch` — no direct unit tests

The core adaptive window logic (`src/virtual_fs/prefetch.rs:106-174`) has zero
direct unit tests. It is only exercised indirectly through VFS-level integration
tests.

**Missing test cases:**
- `StartStream`: first read at offset 0 with empty buffer
- `ContinueStream`: sequential read where offset == buf_end, window doubles
- Window doubling progression from `INITIAL_WINDOW` to `MAX_WINDOW`
- `RangeDownload` on backward seek beyond `SEEK_WINDOW`
- `RangeDownload` on far forward jump past `FORWARD_SKIP`
- Forward skip within `FORWARD_SKIP` stays sequential
- `fetch_size` clamped near EOF

### `FlushManager` — no unit tests

`src/virtual_fs/flush.rs` has no unit tests at all. The entire flush pipeline is
only tested through VFS-level tests.

**Missing test cases:**
- Debounce coalescing (N rapid enqueues → 1 batch)
- Same-inode dedup in `flush_batch`
- `flush_pending_deletes` retry and re-queue on failure
- `cancel_delete` / `cancel_delete_prefix`
- Shutdown draining all dirty inodes
- Concurrent `flush_one` + write (dirty_generation race)

### `poll_remote_changes` — no unit tests

`src/virtual_fs/poll.rs` has no unit tests.

**Missing test cases:**
- New remote file → parent invalidated, negative cache cleared
- Remote file updated (hash change) → inode updated
- Remote file deleted → inode removed
- Dirty file skipped during poll update AND deletion
- Hub API failure → poll continues on next interval

### `InodeTable` — missing edge cases

- `remove_orphan()`: no direct test (only tested transitively via `unlink_one`)
- `apply_commit` with generation mismatch should NOT update `size`/`xet_hash`
  (the existing test `apply_commit_keeps_dirty_on_generation_mismatch` asserts
  that size IS updated, which reflects the current buggy behavior)

### `symlink` / `readlink` — no tests

No unit or integration tests for symlink creation or reading.

### `setattr` for non-size fields — untested

Setting uid, gid, mode, atime, mtime via setattr is never tested.
