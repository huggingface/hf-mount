# Changelog

All notable changes to this project will be documented in this file.

## [0.2.0] - 2026-03-26

### Fixed
- Tolerate rename Phase 3 failure after remote mutation (#77)
- Serialize setattr truncate with write to prevent inode size race (#76)
- Prevent apply_commit from clobbering size/hash on generation mismatch (#75)
- Prevent poll from deleting inodes with open file handles (#74)
- Prevent file descriptor exhaustion on bulk NFS deletes (#68)
- Schedule flush after NFS exclusive create with empty files (#71)
- Re-check dirty status before poll deletes inode to fix TOCTOU race (#67)

### Performance
- Chunk upload_files to bound FD usage during flush (#72)
- Skip redundant HEAD revalidation after flush commit (#73)

## [0.1.0] - Initial release
