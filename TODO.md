# Code Review Findings

## HIGH

- [x] 1. **FD race in read/write**: `read`/`write` capture `as_raw_fd()` then release lock; concurrent `release` can close the FD → wrong-file read/write. Fix: hold `Arc<File>` instead of raw fd.
- [x] 2. **setattr truncate-to-nonzero on clean remote file**: if staging file doesn't exist and `new_size > 0`, nothing happens but inode is marked dirty. Fix: download the file first, then truncate.
- [x] 3. **Rename skips remote delete for dirty files**: only does Add+Delete when `!is_dirty`, leaving stale old remote file. Fix: always delete old path (or record it for flush-time cleanup).
- [x] 4. **Directory rename doesn't update descendants' paths**: only the renamed inode's `full_path` is updated; children's `full_path` and `path_to_inode` become stale. Fix: recursive path update.
- [x] 5. **Poll misses new files in new directories**: only invalidates parent dirs that already exist as inodes. Fix: also create intermediate directory inodes for new remote paths.

## MEDIUM-HIGH

- [x] 6. **Write durability errors swallowed**: `flush`/`release` always return ok; upload errors only logged. Fix: track flush errors per-inode and report on next fsync/close.

## MEDIUM

- [x] 7. **create leaves phantom dirty inode on staging open failure**: inode inserted+marked dirty before staging file open; on failure, inode not rolled back. Fix: rollback on error.
- [x] 8. **CachingClient unbounded cache growth**: no size cap, no expired entry cleanup. Fix: add max entries + periodic eviction of expired entries.
- [x] 9. **Predictable staging paths + symlink following**: `inode_<n>` under `/tmp` + `File::create` follows symlinks. Fix: use `O_NOFOLLOW` or randomize staging names.
- [x] 10. **Panic paths in non-test hot path**: `expect` on `Arc::try_unwrap` and poisoned mutex in read path. Fix: return EIO instead of panicking.

## LOW

- [x] 11. **rename ignores kernel RenameFlags**: `RENAME_NOREPLACE` etc. not enforced. Fix: check flags and return EEXIST/EINVAL as appropriate.
