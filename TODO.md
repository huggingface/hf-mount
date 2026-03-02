# TODO

## Code Review (cleanup branch)

- [x] FUSE thread count: added `--max-threads` CLI arg, default 16 (like mountpoint-s3)
- [ ] Continue code review: `fuse.rs`, `virtual_fs.rs`, `nfs.rs`, `cache.rs`, `hub_api.rs`, `inode.rs`, `caching_client.rs`
- [ ] Commit all cleanup changes

## Flush semantics

- [ ] Add `--flush-on-close` mode (safe mode): `close()` waits for the next batch flush to include the file before returning, guaranteeing durability. Default remains async (performant mode). Enables write-then-verify test patterns and matches S3 mount semantics (mountpoint-for-amazon-s3, s3fs).

## Optimizations

- [ ] Investigate `notify_inval_entry`/`notify_inval_inode` to actively invalidate kernel cache when poll detects changes (instead of relying on TTL expiry). Requires passing `Session` ref to VFS poll loop.

## Cleanup

- [x] `CachedXetClient`: `CacheKey` uses `Option<FileRange>` because the `Client` trait signature from xet-core takes `bytes_range: Option<FileRange>`. `None` = full file, `Some(range)` = partial range. This design is intentional and matches the upstream trait.

## TO CHECK

- [ ] en tout cas la prochaine fois je viens prendre un whisky dans ton bureau
      [21 h 41]demande à ton stagiaire de regarder dans le prefetch s'il peut éviter de byte copy
      [21 h 41]ca te fait masse de copy je pense
      [21 h 44]y'a mm ptet moyen d'utilise Bytes et d'avoir du zero copy
      [21 h 44]enfin tu devras en faire une pour avoir des bytes contigus à un moment
      [21 h 45]mais comme dans xet ils utilisent Bytes tu dois pouvoir optim pas mal
