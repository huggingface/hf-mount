# hf-mount

Use any Hugging Face model or dataset as if it were a local directory. No download, no copy, no waiting.

```bash
hf-mount-fuse repo gpt2 /mnt/gpt2
```

```python
from transformers import AutoModelForCausalLM
model = AutoModelForCausalLM.from_pretrained("/mnt/gpt2")  # reads on demand, no download step
```

hf-mount exposes [Hugging Face Hub](https://huggingface.co) repos and [Buckets](https://huggingface.co/docs/hub/buckets) as a local filesystem via FUSE or NFS. Files are fetched lazily on first read, so only the bytes your code actually touches ever leave the network.

## Install

### Pre-built binaries

Download the latest release for your platform from [GitHub Releases](https://github.com/huggingface/hf-mount/releases):

| Platform | Binary |
| --- | --- |
| Linux x86_64 | `hf-mount-fuse-x86_64-linux` |
| Linux aarch64 | `hf-mount-fuse-aarch64-linux` |
| macOS Apple Silicon | `hf-mount-fuse-arm64-apple-darwin` |
| macOS Intel | `hf-mount-fuse-x86_64-apple-darwin` |

```bash
# Example: Linux x86_64
curl -L -o hf-mount-fuse https://github.com/huggingface/hf-mount/releases/latest/download/hf-mount-fuse-x86_64-linux
chmod +x hf-mount-fuse
sudo mv hf-mount-fuse /usr/local/bin/
```

### System dependencies

**Linux**: `sudo apt-get install -y fuse3`

**macOS**: install [macFUSE](https://osxfuse.github.io/) (`brew install macfuse`, requires reboot on first install)

### Build from source

Requires Rust 1.85+.

```bash
# Linux
sudo apt-get install -y fuse3 libfuse3-dev
cargo build --release

# macOS (macFUSE must be installed first)
cargo build --release
```

Binaries: `target/release/hf-mount-fuse`, `target/release/hf-mount-nfs`, `target/release/hf-mount-daemon`

## Quick start

```bash
# Mount a public model (no token needed)
mkdir /tmp/gpt2
hf-mount-fuse repo gpt2 /tmp/gpt2
ls /tmp/gpt2

# Use it from Python
python -c "
from transformers import AutoTokenizer, AutoModelForCausalLM
tok = AutoTokenizer.from_pretrained('/tmp/gpt2')
model = AutoModelForCausalLM.from_pretrained('/tmp/gpt2')
print(tok.decode(model.generate(**tok('Hello', return_tensors='pt'), max_new_tokens=20)[0]))
"

# Unmount
fusermount -u /tmp/gpt2   # Linux
umount /tmp/gpt2           # macOS
```

For private repos or [Buckets](https://huggingface.co/docs/hub/buckets), pass `--hf-token` or set the `HF_TOKEN` env var.

## Best for / Not for

**Best for:**
- Loading models and datasets without downloading the full repo
- Browsing repo contents (`ls`, `cat`, `find`) without cloning
- Read-heavy ML workloads (training, inference, evaluation)
- Environments where disk space is limited

**Not for:**
- General-purpose networked filesystem (no multi-writer support, no file locking)
- Latency-sensitive random I/O (first reads require network round-trips)
- Workloads that need strong consistency (files can be stale for up to 10 s)
- Heavy concurrent writes from multiple mounts (last writer wins, no conflict detection)

See [Consistency model](#consistency-model) for details.

## Usage

### Mount a repo (read-only)

```bash
# Public model (no token needed)
hf-mount-fuse repo gpt2 /mnt/gpt2

# Private model
hf-mount-fuse --hf-token $HF_TOKEN repo myorg/my-private-model /mnt/model

# Dataset
hf-mount-fuse repo datasets/squad /mnt/squad

# Specific revision
hf-mount-fuse repo openai-community/gpt2 /mnt/gpt2 --revision v1.0

# Subfolder only
hf-mount-fuse repo openai-community/gpt2/onnx /mnt/onnx
```

### Mount a Bucket (read-write)

[Buckets](https://huggingface.co/docs/hub/buckets) are HF Hub's object storage for arbitrary data (checkpoints, logs, artifacts).

```bash
hf-mount-fuse --hf-token $HF_TOKEN bucket myuser/my-bucket /mnt/data

# Read-only
hf-mount-fuse --hf-token $HF_TOKEN --read-only bucket myuser/my-bucket /mnt/data

# Subfolder only
hf-mount-fuse --hf-token $HF_TOKEN bucket myuser/my-bucket/checkpoints /mnt/ckpts
```

### NFS backend

Use `hf-mount-nfs` when `/dev/fuse` is unavailable (e.g., unprivileged Kubernetes containers). Requires `nfs-common` on Linux (`sudo apt-get install -y nfs-common`).

```bash
hf-mount-nfs --hf-token $HF_TOKEN bucket myuser/my-bucket /mnt/data
```

### Background daemon

`hf-mount-daemon` runs the mount in the background, with automatic PID tracking and log management:

```bash
# Start a daemon (NFS by default, --fuse for FUSE)
hf-mount-daemon start --hf-token $HF_TOKEN bucket myuser/my-bucket /mnt/data
hf-mount-daemon start --fuse repo gpt2 /mnt/gpt2

# List running daemons
hf-mount-daemon status

# Stop a daemon (unmounts and waits for flush)
hf-mount-daemon stop /mnt/data
```

Logs are written to `~/.hf-mount/logs/`. PID files are stored in `~/.hf-mount/pids/`.

### Unmount

```bash
fusermount -u /mnt/data   # FUSE (Linux)
umount /mnt/data           # FUSE (macOS) or NFS
hf-mount-daemon stop /mnt  # daemon mounts
```

### Options

| Flag | Default | Description |
| --- | --- | --- |
| `--hf-token` | `$HF_TOKEN` | HF API token (required for private repos/buckets) |
| `--hub-endpoint` | `https://huggingface.co` | Hub API endpoint |
| `--cache-dir` | `/tmp/hf-mount-cache` | Local cache directory |
| `--cache-size` | `10000000000` (~10 GB) | Max on-disk chunk cache size in bytes |
| `--read-only` | `false` | Mount read-only (always on for repos) |
| `--advanced-writes` | `false` | Enable staging files + async flush (random writes, seek, overwrite) |
| `--poll-interval-secs` | `30` | Remote change polling interval (0 to disable) |
| `--max-threads` | `16` | Maximum FUSE worker threads (Linux only) |
| `--metadata-ttl-ms` | `10000` | How long file metadata is cached before re-checking (ms) |
| `--metadata-ttl-minimal` | `false` | Re-check on every access (maximum freshness, lower throughput) |
| `--flush-debounce-ms` | `2000` | Advanced writes: flush debounce delay (ms) |
| `--flush-max-batch-window-ms` | `30000` | Advanced writes: max flush batch window (ms) |
| `--no-disk-cache` | `false` | Disable local chunk cache (every read fetches from HF) |
| `--no-filter-os-files` | `false` | Stop filtering OS junk files (.DS_Store, Thumbs.db, etc.) |
| `--uid` / `--gid` | current user | Override UID/GID for mounted files |
| `--token-file` | | Path to a token file (re-read on each request for credential rotation) |

### Logging

```bash
RUST_LOG=hf_mount=debug hf-mount-fuse repo gpt2 /mnt/gpt2
```

## Features

- **FUSE & NFS backends** -- FUSE for standard Linux/macOS, NFS for environments without `/dev/fuse`
- **Lazy loading** -- files are fetched on demand, not eagerly downloaded
- **Subfolder mounting** -- mount only a subdirectory (e.g. `user/model/ckpt/v2`)
- **Simple writes** (default) -- append-only, in-memory, synchronous upload on close
- **Advanced writes** (`--advanced-writes`) -- staging files on disk, random writes + seek, async debounced flush
- **Remote sync** -- background polling detects remote changes and updates the local view
- **POSIX metadata** -- chmod, chown, timestamps, symlinks (in-memory only, lost on unmount)

## Consistency model

hf-mount provides **eventual consistency** with remote changes. There is no push notification from the Hub; all freshness relies on client-side polling.

### Reads

Files can be stale for up to `--metadata-ttl-ms` (default 10 s) after a remote update. Two mechanisms detect changes:

1. **Metadata revalidation** (FUSE only) -- when the per-file TTL expires, the next access checks the Hub. If the file changed, cached data is invalidated.
2. **Background polling** (default every 30 s) -- lists the full tree and detects additions, modifications, and deletions.

### Writes

| | Streaming (default) | Advanced (`--advanced-writes`) |
| --- | --- | --- |
| Write pattern | Append-only (sequential) | Random writes, seek, overwrite |
| Storage | In-memory buffer | Local staging file on disk |
| Modify existing files | Overwrite only (O_TRUNC) | Yes (downloads file first) |
| Durability | On close | Async, debounced (2 s / 30 s max) |
| Disk space needed | None | Full file size per open file |

**Streaming mode** buffers writes in memory and uploads on `close()`. A crash before close means data loss.

**Advanced mode** downloads the full file to local disk before allowing edits. After `close()`, dirty files are flushed asynchronously. A crash before flush completes means data loss.

### FUSE vs NFS

| | FUSE | NFS |
| --- | --- | --- |
| Metadata revalidation | Per-file, within TTL | No (NFS uses file handles) |
| Page cache invalidation | Supported | Not supported by NFS protocol |
| Staleness window | ~10 s | Up to poll interval (30 s) |
| Write mode | Streaming by default | Advanced always |

## How it works

hf-mount sits between your application and the Hugging Face Hub. It presents a standard filesystem interface (FUSE or NFS) and translates file operations into Hub API calls and storage fetches.

Reads go through an adaptive prefetch buffer that starts small and grows with sequential access. Writes are uploaded to HF storage and committed via the Hub API. A background poll loop keeps the local view in sync with remote changes.

Built on [xet-core](https://github.com/huggingface/xet-core) for content-addressed storage and efficient file transfers, and [fuser](https://github.com/cberner/fuser) for the FUSE implementation.

## Testing

```bash
# Unit tests (no network, no token)
cargo test --lib

# Integration tests (require HF_TOKEN and FUSE)
HF_TOKEN=... cargo test --release --test fuse_ops -- --test-threads=1 --nocapture
HF_TOKEN=... cargo test --release --test nfs_ops -- --test-threads=1 --nocapture

# Repo mount test (public repo, no token needed)
cargo test --release --test repo_ops -- --test-threads=1 --nocapture

# Benchmarks
HF_TOKEN=... cargo test --release --test bench -- --nocapture
HF_TOKEN=... cargo test --release --test fio_bench -- --nocapture
```

## License

Apache-2.0
