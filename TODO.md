# hf-mount - TODO

## P0 - Critical (data loss)

- [x] **Dirty flag race in flush_batch and streaming_commit** - `dirty_generation` counter
- [x] **StagingDir creation failure swallowed** - `unwrap_or_else` panic
- [x] **rename TOCTOU with remote** - Documented, self-healing via re-validation + poll
- [x] **Streaming write deadlock** - Unbounded channel

## P1 - Important

- [x] **Panic on HEAD without x-linked-size** - Graceful return with warning
- [x] **set_var unsafe guard** - OnceLock prevents double init
- [x] **Sanitize macOS volname** - Skip volname for paths with commas
- [x] **NFS handles not released on shutdown** - Drain pool before VFS shutdown
- [ ] **NFS handle eviction race** (`nfs.rs:142-148`) - Handle can be evicted between get_or_open and read
- [ ] **fsync not implemented** (`fuse.rs`) - rsync/git get ENOSYS in advanced-writes mode
- [ ] **inode_table write lock on every write()** (`virtual_fs/mod.rs:1580`) - Perf: serialize all writers
- [ ] **NFS HandlePool.get() is O(n)** (`nfs.rs:517`) - Replace with LinkedHashMap if pool grows

## P2 - Minor

- [ ] **NFS time truncation** (`nfs.rs:564`)
- [ ] **Predictable rand_u64** (`xet.rs:210-216`)
- [ ] **DaemonGuard libc::write unchecked** (`daemon.rs:134-148`)
- [ ] **Negative cache eviction O(n)** (`virtual_fs/mod.rs:883-893`)
- [ ] **HandlePool duplicate entries** (`nfs.rs:534-535`)
- [ ] **remove_orphan skips path_to_inode** (`inode.rs:436-440`)
- [ ] **NFS unmount fallback silent** (`nfs.rs:593`)

## Architecture improvements

- [ ] **Split virtual_fs/mod.rs** (2949 lines)
- [ ] **Structured Hub errors** - `Error::Hub(String)` erases error type
- [ ] **NFS handle pool unit tests**

## Open source

- [x] LICENSE (Apache-2.0)
- [x] Cargo.toml metadata
- [x] macOS FUSE support (remove macos-no-mount, fix clone_fd/n_threads)
- [x] Finder sidebar visibility (volname + local mount options)
- [x] Release workflow (Linux x86_64/aarch64 + macOS arm64/x86_64)
- [x] README rewrite for public audience
- [x] Replace internal terminology (CAS, xorb) in user-facing text
- [x] Feature flags: fuse/nfs behind default features
- [ ] Squash history and push to github.com/huggingface/hf-mount
