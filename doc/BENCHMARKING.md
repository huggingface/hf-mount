## Benchmarking

hf-mount is a high-throughput FUSE/NFS filesystem for HuggingFace datasets and model repos.
To track performance and enable comparison with [mountpoint-s3](https://github.com/awslabs/mountpoint-s3),
we run fio benchmarks using the **same workloads and methodology** as mountpoint-s3.

### Workloads

The fio job files under `scripts/fio/` are copied directly from mountpoint-s3 and run unchanged.

**Read throughput** (`scripts/fio/read/`) — 30-second timed runs, 256 KiB block size:

| Job | Description |
|-----|-------------|
| `seq_read` | Sequential read, 1 thread, 100 GiB file |
| `seq_read_4t` | Sequential read, 4 threads |
| `seq_read_small` | Sequential read, 5 MiB file |
| `rand_read` | Random read, 1 thread, 100 GiB file |
| `rand_read_4t` | Random read, 4 threads |
| `rand_read_small` | Random read, 5 MiB file |
| `*_direct` | Variants with `O_DIRECT` (bypasses kernel page cache) |
| `seq_read_skip_17m` | Sequential read with 17 MiB skip (safetensors-like pattern) |

**Read latency** (`scripts/fio/read_latency/`) — time to first byte (TTFB):

| Job | Description |
|-----|-------------|
| `ttfb` | Read 1 byte from a 1 GiB file, 10 loops |
| `ttfb_small` | Read 1 byte from a 5 MiB file, 10 loops |

**Write throughput** (`scripts/fio/write/`) — 30-second timed runs:

| Job | Description |
|-----|-------------|
| `seq_write` | Sequential write, 100 GiB |
| `seq_write_direct` | Sequential write with `O_DIRECT` |

### Comparison with mountpoint-s3

mountpoint-s3 publishes benchmark results at:
- [Throughput chart](https://awslabs.github.io/mountpoint-s3/dev/bench/)
- [Latency chart](https://awslabs.github.io/mountpoint-s3/dev/latency_bench/)

hf-mount publishes results at:
- [Throughput chart](https://huggingface-internal.github.io/hf-mount/dev/bench/)
- [Latency chart](https://huggingface-internal.github.io/hf-mount/dev/latency_bench/)

For a fair comparison, both should be run on the same class of instance with comparable network
bandwidth. mountpoint-s3 uses `m5dn.24xlarge` (100 Gbps, local NVMe SSD).

### Running the benchmark

1. Build the release binary:

       cargo build --release

2. Run throughput benchmarks (creates a temporary bucket, uploads files, runs fio, cleans up):

       ./scripts/fs_bench.sh

3. Run latency (TTFB) benchmarks:

       ./scripts/fs_latency_bench.sh

4. Find results in `results/output.json`.

**Environment variables:**

| Variable | Description |
|----------|-------------|
| `HF_TOKEN` | HuggingFace API token (required) |
| `HF_ENDPOINT` | Hub endpoint (default: `https://huggingface.co`) |
| `HF_MOUNT_BIN` | Path to `hf-mount-fuse` binary |
| `HF_BENCH_BUCKET` | Reuse a pre-existing bucket (skips file upload) |
| `HF_JOB_NAME_FILTER` | Only run jobs matching this substring (e.g. `small`) |
| `HF_NO_DISK_CACHE` | Set to `1` to disable the on-disk xorb chunk cache |
| `iterations` | fio iterations per job (default: 10) |

**Cache behavior:**

By default, hf-mount caches xorb chunks on disk between reads (comparable to mountpoint-s3
with `--cache`). Set `HF_NO_DISK_CACHE=1` to disable this: reads fetch chunks from the CAS
network on each FUSE cache miss (OS page cache still applies), which is comparable to
mountpoint-s3 without `--cache`.

**Tip:** To skip re-uploading 100 GiB files on repeated runs, set `HF_BENCH_BUCKET` to a
previously created benchmark bucket.

### Regression testing

The CI runs benchmarks automatically:
- **On every push to `main`**: full run, results published to GitHub Pages.
- **On PRs labeled `performance`**: results posted as a PR comment.

The CI uses `HF_JOB_NAME_FILTER=small` and `iterations=3` to keep runtime reasonable.
For full 100 GiB comparison runs, remove the filter and set `iterations=10`.

### Key differences vs mountpoint-s3

| | hf-mount | mountpoint-s3 |
|-|----------|---------------|
| Storage backend | HuggingFace CAS (xorb-chunked) | Amazon S3 |
| File creation | Write-through FUSE → CAS upload | Direct S3 PUT |
| Cache | On-disk xorb chunk cache | Optional local disk cache |
| Write support | Yes (append + staging modes) | Yes |
| Read-only repos | Yes (git-backed model repos) | N/A |
