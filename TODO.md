# Performance optimization TODO

Findings from audit on 2026-05-20 (commit `65db489`). Grouped by impact, with
file:line pointers. Check items off as PRs land.

Severity tags: 🔴 high · 🟡 medium · 🟢 quick-win

---

## Hub API / polling

- [ ] 🔴 **Tune `reqwest::Client`** — `hub_api.rs:363-375`. Add `pool_max_idle_per_host(128)`, `pool_idle_timeout(90s)`, `connect_timeout(10s)`, per-request `timeout`, `tcp_keepalive`. Today a hung Hub freezes the whole poll loop indefinitely.
- [ ] 🔴 **Cheap-probe before tree fan-out** — `poll.rs` + `hub_api.rs`. Repos expose `sha` (commit head) on `/api/{type}/{id}`; buckets expose `lastModified`. Cache last-seen value per mount; skip the whole tree listing when unchanged. Replaces the unworkable "ETag on /tree" idea.
- [ ] 🔴 **Per-prefix backoff on 5xx** — `poll.rs:91-103`. A prefix that 5xx'd recently shouldn't be re-listed on the next round at full concurrency. `HashMap<prefix, not_before: Instant>`, exponential per consecutive failure.
- [ ] 🟡 **Separate download client from control-plane** — `hub_api.rs:861`. Long-lived blob downloads currently share the pool with list/head, starving the latter under load.
- [ ] 🟡 **Lock-free `auth()` cache** — `hub_api.rs:477-504`. Std `Mutex` acquired up to 3× per request. Switch to `ArcSwap` or `parking_lot::RwLock`.
- [ ] 🟡 **Jitter + global semaphore on retries** — `hub_api.rs:329-361`. Retry-clumping on partial Hub outage (16 concurrent → 48 retries in 1.5s). Add jitter, bound in-flight.
- [ ] 🟡 **Honor `Retry-After` header** — `hub_api.rs:273`. Currently only `RateLimit-*` is parsed.
- [ ] 🟢 **`tokio::fs` for etag sidecar** — `hub_api.rs:884-889`. Currently `std::fs::read_to_string` on async thread.
- [ ] 🟢 **Typed structs for repo/bucket info** — `hub_api.rs:402,439`. `serde_json::Value` allocates everything.
- [ ] 🟢 **Bound error-body read** — `hub_api.rs:350`. `resp.text()` on every non-success buffers full body even on 401/404.
- [ ] 🟢 **Don't hold `inodes.read()` across await in poll loop** — `poll.rs:45`. Snapshot prefix strings, release lock before await.

## CAS / reconstruction cache

- [ ] 🔴 **Wrap cached response in `Arc`** — `cached_xet_client.rs:169,263`. Every cache hit currently clones full `terms: Vec` + `xorbs: HashMap`. Arc'ing makes the hit O(1).
- [ ] 🟡 **Proper LRU eviction** — `cached_xet_client.rs:248-262`. Today: `cache.retain(|...| range.is_none())` then `keys().next()` as victim (random). Use `lru::LruCache` or track `inserted_at`.
- [ ] 🟡 **Precompute term offsets** — `cached_xet_client.rs:101`. `derive_range_response` rebuilds `HashSet<HexMerkleHash>` + filters `xorbs` HashMap per range query. Cache cumulative offsets → binary search + slice.
- [ ] 🟢 **`Notify` instead of `broadcast::channel(1)`** for singleflight — `cached_xet_client.rs:53`.
- [ ] 🟢 **Rate-limit warm-up** — `xet.rs:153 warm_reconstruction_cache`. Fire-and-forget without throttle bypasses CAS adaptive concurrency.

## Virtual FS — allocations and locks

- [ ] 🔴 **Stop `.full_path.to_string()` on `Arc<str>`** — ~20 call sites across `virtual_fs/mod.rs` (lookup, getattr, revalidate, streaming_commit, etc.). Each is a heap alloc + memcpy of the path on the hottest path. Pass `Arc<str>` instead.
- [ ] 🔴 **Stop copying write buffers** — `virtual_fs/mod.rs:2245`. `channel.tx.blocking_send(WriteMsg::Data(data.to_vec()))` memcpys every FUSE write (128 KB). Use `Bytes` end-to-end.
- [ ] 🔴 **HashMap index for big dirs** — `inode.rs:517 lookup_child`. O(N) linear scan over `children: Vec<DirChild>`. Add `HashMap<Arc<str>, u64>` when `children.len() > 32`.
- [ ] 🔴 **Iterative `update_subtree_paths`** — `inode.rs:770-780`. Recursive, clones full `children` Vec at each level; also re-creates `Arc::from(...)` instead of reusing the parent's.
- [ ] 🟡 **Cache `mode`/perm in `InodeEntry`** — `mod.rs:545 make_vfs_attr`. Recomputed per getattr (the #1 most-called op).
- [ ] 🟡 **Return attr from `revalidate_file` directly** — `mod.rs:1232-1234`. Currently re-acquires `inode_table.read()` to read the value we just wrote.
- [ ] 🟡 **`Vec`/`String` clones in `file_snapshot` / `staging_gc_candidates` / `dirty_inos`** — `inode.rs:411,639,649`. Full-table scans + clones on every poll cycle. Return `Arc<str>` and/or use bounded heap.
- [ ] 🟡 **`negative_cache_insert` double scan + write-lock duration** — `mod.rs:1112`. `Vec<String>` clone of up to 128 keys, then 128 cache.remove() rehashes.
- [ ] 🟢 **`Arc<str>` for `VirtualFsDirEntry.name`** — `mod.rs:1567 readdir`. Currently `.to_string()` per child.
- [ ] 🟢 **Drop `seek_data.make_contiguous()`** — `prefetch.rs:225 try_serve_seek`. Mutates VecDeque just to slice; replace with `BytesMut`/`Bytes::slice()` zero-copy.

## FUSE adapter

- [ ] 🔴 **Document or tune `n_threads`** — `fuse.rs:191+`. Every op does `runtime.block_on(...)` on a FUSE worker thread; concurrency is capped by thread count. Either bump default or migrate hot ops (`read`, `getattr`) to non-blocking reply dispatch.
- [ ] 🟡 **Cache readdir result on `opendir`** — `fuse.rs:213-231`. Kernel re-calls `readdir` with growing `offset`; we rebuild the entries Vec each time.
- [ ] 🟢 **Fire-and-forget `release`** — `fuse.rs:329`. Currently `block_on`; kernel ignores errors after release.
- [ ] 🟢 **Spawn `destroy()`** — `fuse.rs:537-539`. Blocks last FUSE thread on shutdown flush; risk of systemd timeout.

## NFS adapter

- [ ] 🔴 **Shardable prefetch state per inode** — `nfs.rs:166-202`. PR #80 avoided duplicate Xet streams, but concurrent reads on the same inode still serialize on the per-handle prefetch mutex. Open distinct handles or shard by offset region.
- [ ] 🟡 **Make `HANDLE_POOL_CAPACITY` configurable** — `nfs.rs:615`. Hard 64. Past that, every open evicts an active prefetch buffer.
- [ ] 🟡 **Shard the handle-pool `Mutex`** — `nfs.rs:62-71,124-126`. Single global lock taken twice per read.
- [ ] 🟡 **Batch readdir attrs** — `nfs.rs:204-238`. N getattrs per readdir page; should be one inode-table read lock pass.
- [ ] 🟢 **Switch `order` to `IndexSet`** — `nfs.rs:692`. O(N) linear scan; fine at cap=64 but blocks raising cap.
- [ ] 🟢 **Avoid `bytes.to_vec()` on reply** — `nfs.rs:180` (and `fuse.rs:273`). Check if nfsserve/fuser accept `Bytes` directly.

## File cache / overlay

- [ ] 🟡 **`tokio::fs` instead of `std::fs` in hot paths** — `file_cache.rs:177-184,321`. `open()` / `rename()` block the runtime.
- [ ] 🟡 **Lazy LRU eviction** — `file_cache.rs:351-379`. Full scan+sort under write lock on every populate. Use BTreeMap secondary index or min-heap.
- [ ] 🟡 **`forget()` write lock on miss** — `file_cache.rs:189-195`. Bursts of `try_open` on a stale hash serialize behind write lock. Re-check under upgrade.
- [ ] 🟢 **Skip metadata in `overlay::read_dir` when not needed** — `overlay.rs:144`. 1k extra `fstatat` per hot dir.

---

## Suggested PR ordering

1. Hub client tuning (#1 above) + cheap-probe (#2) — biggest user-facing Hub-load win, ~1h work, ships well with the 401 investigation.
2. `Arc<str>` cleanup across VFS — mechanical, broad alloc reduction.
3. `Arc<QueryReconstructionResponseV2>` + LRU on reconstruction cache — measurable on read-heavy workloads.
4. NFS prefetch sharding — unblocks the known mmap bottleneck.

After each PR, run `tests/bench.rs` + `tests/fio_bench.rs` to quantify.

---

# Code review findings — sparse-writes PR (2026-05-27)

Max-recall review of branch `feat/append-write` (PR #41). 15 findings survived
verification. Each finding has a planned regression test in
`src/virtual_fs/tests.rs` (or `tests/` for integration); test names are below.

Severity tags: 🔴 data-loss / persistent failure · 🟠 silent corruption /
staleness · 🟡 incorrect mtime / errno · 🟢 defensive / theoretical

---

## 🔴 Data-loss / persistent failure

- [x] **C3** — setattr Clean-file branch after `O_TRUNC + write` built `SparseWriteState::new(pre_truncate_hash, K)` with mismatched `original_size`. **Fix**: capture `was_dirty` before `set_dirty`; gate Clean-file branch on `!was_dirty`. **Test**: `c3_o_trunc_then_write_then_setattr_extend_loses_user_bytes`
- [x] **B5** — lazy `sparse_write` install gate `entry.size > 0` failed after `ftruncate(0)+flush`. **Fix**: same as C3 (`was_dirty` guard in Clean-file branch). **Test**: `b5_setattr_zero_then_write_then_setattr_extend_loses_bytes`
- [x] **B1** — `setattr(shrink)` on non-Xet (`xet_hash=None`) skipped the HTTP download and uploaded zeros. **Fix**: download via `hub_client.download_file_http` before `set_len` when `xet_hash.is_none() && new_size > 0`. **Test**: `b1_setattr_shrink_on_non_xet_file_loses_original_content`

## 🟠 Silent corruption / staleness races

- [x] **D1/A6 + E1** — `read()`/`write()` didn't take the per-inode staging lock; race with `range_upload`'s `PreadReader` and with each other. **Fix**: hold `self.staging.lock(ino)` around pread+sparse_write snapshot (async `lock_owned().await`) and around pwrite+track_write (`blocking_lock_owned()` since `write()` is sync). **Test**: `d1_e1_read_and_write_serialize_via_staging_lock`
- [x] **C1** — setattr mutated `sparse_write` without checking it was current. **Fix**: drop stale `sparse_write` (`sw.original_hash != entry.xet_hash`) before the setattr branches. **Test**: `c1_setattr_on_stale_sparse_write_rolls_back_remote_revision`
- [x] **C2** — read used stale `sparse_write` after `update_remote_file` preserved it across a hash change. **Fix**: `update_remote_file` now clears `sparse_write` (guard already proved no handles open); read defensively skips `fill_sparse_holes` when `sw.original_hash != entry.xet_hash`. **Test**: `c2_update_remote_file_leaves_sparse_write_stale_vs_xet_hash`
- [x] **C4** — poll Phase 2 pushed `update.ino` to `inos_to_invalidate` even when `update_remote_file` returned false. **Fix**: bind the bool return; only push if applied. **Test**: `c4_poll_phase2_invalidates_even_when_update_was_rejected`
- [x] **E3** — in-flight reads kept a stale `sparse_write` Arc clone after `apply_commit` swapped it. **Fix**: covered by D1/E1 — the staging lock now serializes apply_commit (via flush_batch's lock) against in-flight reads. **Test**: `e3_read_releases_lock_before_awaiting_fill_sparse_holes`
- [x] **B6/E7** — `update_remote_file` froze on any open handle, including clean read-only ones. **Fix**: track `open_write_handles` separately; gate `update_remote_file` on `is_dirty() || has_open_write_handles()`. **Test**: `b6_open_readonly_handle_freezes_update_remote_file`

## 🟡 Incorrect mtime / errno

- [x] **B8** — `setattr(size=N)` where `N == prev_size` on a clean file bumped local mtime but no Hub commit fired. **Fix**: treat same-size setattr on clean file as a full no-op (no local mtime bump, no flush), consistent with `chmod`/`utime`. **Test**: `b8_setattr_same_size_is_consistent_noop`
- [x] **C5/E2** — `apply_noop_commit` / `apply_commit` bumped `mtime/ctime/last_revalidated` even when `clear_dirty_if` returned false. **Fix**: move the timestamp bumps inside the `clear_dirty_if` success branch. **Test**: `c5_apply_noop_commit_bumps_mtime_even_on_generation_mismatch`
- [x] **B4** — `nfs.rs::errno_to_nfs` had no EAGAIN arm → transient drift surfaced as EIO. **Fix**: add `libc::EAGAIN => NFS3ERR_JUKEBOX`. **Test**: `b4_errno_to_nfs_lacks_eagain_arm`

## 🟢 Defensive / theoretical

- [x] **E4** — lazy `sparse_write` install used `debug_assert!` (no-op in release). **Fix**: replace with runtime check that skips the install and logs an error. **Test**: `e4_lazy_sparse_install_does_not_rely_on_debug_assert`
- [x] **E5** — `set_dirty` used `saturating_add(1)` → pinned at `u64::MAX`. **Fix**: `wrapping_add` with skip-0 (preserves the "0 means clean" sentinel). **Test**: `e5_saturated_dirty_generation_lets_stale_flush_clobber_concurrent_writer`


