# TODO

## Flush semantics

- [ ] Add `--flush-on-close` mode (safe mode): `close()` waits for the next batch flush to include the file before returning, guaranteeing durability. Default remains async (performant mode). Enables write-then-verify test patterns and matches S3 mount semantics (mountpoint-for-amazon-s3, s3fs).

## Streaming writes (no staging file)

- [ ] Add `--streaming-writes` flag: append-only writes streamed directly to xet-core via `start_clean()` + `add_data()`, no staging file on disk. Rejects seek/random writes (like mountpoint-s3). Bounded memory buffer, flushed incrementally. Falls back to current staging-file mode when flag is off.

## Known races

- [ ] Add concurrent unlink/rename-during-open test to verify expected behavior (ENOENT vs valid handle).
