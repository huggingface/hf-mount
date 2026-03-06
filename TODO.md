# Random Read Performance - SafeTensor GPU Loading

## Context
Mounting safetensors directly to GPU is slow. SafeTensor pattern: header read (few KB at offset 0), then ordered non-contiguous reads for each tensor weight.

## Bottlenecks (by severity)

### 1. [HIGH] Per-file Mutex serializes all reads — DONE
- **Location**: `src/virtual_fs/mod.rs` — `read()` method
- **Problem**: All reads on same file handle serialize on PrefetchState async Mutex. Lock held during network I/O.
- **Fix**: `try_lock()` with independent fallback. On contention, reads open their own CAS stream.
- **Bench** (m6id.2xlarge, 256 MiB file, shared fd, 1 MiB random reads):
  - Before: 1t=21 MB/s, 2t=7.5 MB/s (0.36x), 4t=7.9 MB/s (0.37x), 8t=9.2 MB/s (0.44x)
  - After: 1t=21 MB/s, 2t=41.6 MB/s (1.96x), 4t=48 MB/s (2.25x), 8t=88.3 MB/s (4.15x)
- **Status**: DONE

### 2. [HIGH] New CAS stream per random read
- **Location**: `src/prefetch.rs:125-127`
- **Problem**: Jumps >16 MiB drop the persistent stream and open a new one. Each tensor = new stream = CAS connection + reconstruction query. 10 tensors = 10 stream opens.
- **Fix**: Detect "ordered random" pattern (increasing offsets with gaps). Keep stream and skip forward instead of recreating.
- **Status**: TODO

### 3. [HIGH] Window resets to 8 MiB on every seek
- **Location**: `src/prefetch.rs:110-114`
- **Problem**: Any jump >16 MiB resets prefetch window from 128 MiB to 8 MiB. SafeTensor ordered reads treated as pure random.
- **Fix**: Don't reset window when offsets are increasing (ordered random).
- **Status**: TODO

### 4. [MEDIUM] Seek window too small (1 MiB)
- **Location**: `src/prefetch.rs:18`
- **Problem**: Only last 1 MiB kept for backward seeks. Header re-reads beyond 1 MiB = cache miss.
- **Fix**: Increase seek window or add a pinned header cache.
- **Status**: TODO

### 5. [MEDIUM] No CAS query batching
- **Problem**: Each read triggers individual CAS reconstruction query. No batch API used.
- **Fix**: Batch reconstruction queries when multiple ranges requested concurrently.
- **Status**: TODO

### 6. [MEDIUM] CAS cache eviction is brutal
- **Location**: `src/cached_xet_client.rs:15`
- **Problem**: 1024 entries max, full clear on overflow. Large models thrash cache.
- **Fix**: LRU eviction instead of full clear.
- **Status**: TODO

## Benchmark plan
- EC2 m6id (NVMe local storage) for consistent I/O baseline
- Each fix gets a before/after benchmark
- Focus on FUSE mount
- Test file: safetensor-like random read pattern (header + ordered offset reads)
