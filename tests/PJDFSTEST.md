# pjdfstest POSIX Compliance Results

hf-mount is tested against [pjdfstest](https://github.com/pjd/pjdfstest), a POSIX filesystem
test suite with 237 test files and 8,789 individual subtests.

**Current score: 178/237 files pass, 5,174/8,789 subtests pass (58.9%)**

The non-regression test (`tests/pjdfstest.rs`) runs in CI and asserts minimum pass thresholds.

## Summary by Category

| Category | Files | Pass | Fail | Status | Primary Failure Cause |
|---|---|---|---|---|---|
| chflags | 14 | 14 | 0 | OK | N/A (BSD-only, all skipped on Linux) |
| chmod | 17 | 9 | 8 | FAIL | mkfifo/mknod cascade |
| chown | 15 | 7 | 8 | FAIL | mkfifo/mknod cascade, EPERM enforcement |
| ftruncate | 16 | 14 | 2 | FAIL | ENAMETOOLONG |
| granular | 7 | 7 | 0 | OK | N/A |
| link | 22 | 14 | 8 | FAIL | mkfifo/mknod cascade |
| mkdir | 16 | 10 | 6 | FAIL | mkfifo/mknod cascade |
| mkfifo | 20 | 6 | 14 | FAIL | mkfifo ENOSYS (intentional) |
| mknod | 20 | 4 | 16 | FAIL | mknod ENOSYS (intentional) |
| open | 33 | 19 | 14 | FAIL | mkfifo/mknod cascade, O_TRUNC ctime |
| posix_fallocate | 1 | 1 | 0 | OK | N/A |
| rename | 35 | 15 | 20 | FAIL | mkfifo/mknod cascade, sticky bit |
| rmdir | 19 | 13 | 6 | FAIL | mkfifo/mknod cascade |
| symlink | 15 | 11 | 4 | FAIL | mkfifo/mknod cascade |
| truncate | 16 | 14 | 2 | FAIL | ENAMETOOLONG |
| unlink | 19 | 11 | 8 | FAIL | mkfifo/mknod cascade, sticky bit |
| utimensat | 11 | 9 | 2 | FAIL | mkfifo/mknod cascade |

## Root Causes

### 1. mkfifo/mknod ENOSYS (intentional, ~3,500 subtests)

hf-mount does not implement `mknod()`. Creating FIFOs, block/character devices, and Unix
sockets returns `ENOSYS`. This is intentional: hf-mount is a cloud storage filesystem where
special device files have no meaning.

This causes massive cascading failures: many tests create a FIFO or device, then test
operations (chmod, chown, open, unlink, rename, etc.) on it. When creation fails, all
subsequent operations fail with ENOENT.

**Affected categories**: chmod, chown, link, mkdir, mkfifo, mknod, open, rename, rmdir,
symlink, unlink, utimensat

**Will not fix**: intentionally unsupported.

### 2. Sticky bit enforcement missing (~2,000 subtests)

When a directory has the sticky bit (`S_ISVTX`) set, only the file owner, directory owner,
or root should be able to delete/rename files within it. hf-mount does not enforce this.

**Affected categories**: rename (09.t, 10.t), unlink (11.t)

**Could fix**: add sticky bit checks in VFS rename/unlink paths.

### 3. chown EPERM enforcement (~800 subtests)

POSIX requires that non-root users can only `chown` to change the group (not uid), and only
to a group they belong to. With FUSE `default_permissions`, the kernel handles basic
read/write/execute checks, but does not enforce ownership transfer rules.

**Affected categories**: chown (00.t, 07.t)

**Could fix**: complex, requires tracking supplementary groups per caller.

### 4. ENAMETOOLONG not enforced (~15 subtests)

hf-mount does not validate that filenames stay within `NAME_MAX` (255 bytes). Standard Linux
filesystems reject names exceeding this limit with `ENAMETOOLONG`.

**Affected categories**: chmod, chown, ftruncate, link, mkdir, open, rename, rmdir, symlink,
truncate, unlink (02.t in each)

**Easy fix**: add name length check in VFS lookup/create/mkdir/symlink/link/rename.

### 5. O_TRUNC not updating mtime/ctime (2 subtests)

When opening a file with `O_TRUNC`, the file size is set to 0 but mtime and ctime are not
updated. POSIX requires both timestamps to be updated on truncation.

**Affected categories**: open (00.t tests 43-44)

**Easy fix**: update mtime/ctime in `open_advanced_write()` when truncating.

### 6. nlink tracking for deleted-but-open files (~3 subtests)

When a file is unlinked while still open, `fstat()` should show `nlink == 0`. hf-mount may
not fully track this edge case.

**Affected categories**: unlink (14.t), rename (24.t)

**Easy fix**: set nlink to 0 on inode when last link is removed.

## Fix Priority

| Fix | Effort | Subtests Fixed | Categories |
|---|---|---|---|
| ENAMETOOLONG validation | Low | ~15 | 11 categories |
| O_TRUNC mtime/ctime | Trivial | 2 | open |
| nlink for unlinked-open files | Low | ~3 | unlink, rename |
| Sticky bit enforcement | Medium | ~2,000 | rename, unlink |
| chown EPERM enforcement | High | ~800 | chown |
