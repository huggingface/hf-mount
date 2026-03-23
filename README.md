# hf-mount

Mount [Hugging Face Buckets](https://huggingface.co/docs/hub/storage-buckets) and repos as local filesystems. No download, no copy, no waiting.

```bash
hf-mount start bucket myuser/my-bucket /tmp/data
```

Also works with any model or dataset repo (read-only):

```bash
hf-mount start repo openai/gpt-oss-20b /tmp/gpt-oss-20b
```

Commands will pick up your HF_TOKEN from env if present, or you can pass it explicitly:

```bash
hf-mount --hf-token $HF_TOKEN 
```

Then use your local folders as usual
```python
from transformers import AutoModelForCausalLM
model = AutoModelForCausalLM.from_pretrained("/tmp/gpt-oss-20b")  # reads on demand, no download step
```

hf-mount exposes [Hugging Face Buckets](https://huggingface.co/docs/hub/storage-buckets) and [Hub repos](https://huggingface.co) as a local filesystem via FUSE or NFS. Files are fetched lazily on first read, so only the bytes your code actually touches ever hit the network.

Two backends are available:
- **NFS** (recommended) -- works everywhere, no root, no kernel extension
- **FUSE** -- tighter kernel integration, requires root or [macFUSE](https://osxfuse.github.io/) on macOS

![hf-mount thumbnail](https://huggingface.co/datasets/huggingface/documentation-images/resolve/main/hf-mount/hf-mount.png)

## Install

### Quick install

```bash
curl -fsSL https://raw.githubusercontent.com/huggingface/hf-mount/main/install.sh | sh
```

Installs `hf-mount` to `~/.local/bin/`. Set `INSTALL_DIR` to change the location. Supports Linux (x86_64, aarch64) and macOS (Apple Silicon, Intel).

### Manual download

Binaries are available on [GitHub Releases](https://github.com/huggingface/hf-mount/releases):

| Platform | NFS | FUSE |
| --- | --- | --- |
| Linux x86_64 | `hf-mount-nfs-x86_64-linux` | `hf-mount-fuse-x86_64-linux` |
| Linux aarch64 | `hf-mount-nfs-aarch64-linux` | `hf-mount-fuse-aarch64-linux` |
| macOS Apple Silicon | `hf-mount-nfs-arm64-apple-darwin` | `hf-mount-fuse-arm64-apple-darwin` |

### System dependencies (FUSE only)

The NFS backend has no system dependencies. For FUSE:

**Linux**: `sudo apt-get install -y fuse3`

**macOS**: install [macFUSE](https://osxfuse.github.io/) (`brew install macfuse`, requires reboot on first install)

### Build from source

Requires Rust 1.85+.

```bash
# NFS only (no system deps)
cargo build --release --no-default-features --features nfs

# All backends (requires FUSE libs)
# Linux: sudo apt-get install -y fuse3 libfuse3-dev
# macOS: brew install macfuse (requires reboot on first install)
cargo build --release
```

Binaries: `target/release/hf-mount`, `target/release/hf-mount-nfs`, `target/release/hf-mount-fuse`

## Quick start

```bash
# Mount a public model as a background daemon (no token needed)
hf-mount start repo openai/gpt-oss-20b /tmp/gpt-oss-20b
ls /tmp/gpt-oss-20b

# Use it from Python
python -c "
from transformers import AutoTokenizer, AutoModelForCausalLM
tok = AutoTokenizer.from_pretrained('/tmp/gpt-oss-20b')
model = AutoModelForCausalLM.from_pretrained('/tmp/gpt-oss-20b')
print(tok.decode(model.generate(**tok('Hello', return_tensors='pt'), max_new_tokens=20)[0]))
"

# Mount a dataset
hf-mount start repo datasets/open-index/hacker-news /tmp/hn
du -sh /tmp/hn/*.parquet

# List running mounts
hf-mount status

# Stop
hf-mount stop /tmp/gpt-oss-20b
hf-mount stop /tmp/hn
```

For private repos or [Buckets](https://huggingface.co/docs/huggingface_hub/guides/buckets), pass `--hf-token` or set the `HF_TOKEN` env var.

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

All examples use `hf-mount-nfs`. Replace with `hf-mount-fuse` if you prefer the FUSE backend.

```bash
# Public model (no token needed)
hf-mount-nfs repo openai/gpt-oss-20b /tmp/gpt-oss-20b

# Private model
hf-mount-nfs --hf-token $HF_TOKEN repo myorg/my-private-model /tmp/model

# Dataset
hf-mount-nfs repo datasets/open-index/hacker-news /tmp/hn

# Specific revision
hf-mount-nfs repo openai-community/gpt2 /tmp/gpt2 --revision v1.0

# Subfolder only
hf-mount-nfs repo openai-community/gpt2/onnx /tmp/onnx
```

### Mount a Bucket (read-write)

[Buckets](https://huggingface.co/docs/huggingface_hub/guides/buckets) are S3-like object storage on the Hub, designed for large-scale mutable data (training checkpoints, logs, artifacts) without git version control.

```bash
hf-mount-nfs --hf-token $HF_TOKEN bucket myuser/my-bucket /tmp/data

# Read-only
hf-mount-nfs --hf-token $HF_TOKEN --read-only bucket myuser/my-bucket /tmp/data

# Subfolder only
hf-mount-nfs --hf-token $HF_TOKEN bucket myuser/my-bucket/checkpoints /tmp/ckpts
```

### FUSE backend

Use `hf-mount-fuse` for tighter kernel integration (page cache invalidation, per-file metadata revalidation). Requires `fuse3` on Linux or [macFUSE](https://osxfuse.github.io/) on macOS.

```bash
hf-mount-fuse --hf-token $HF_TOKEN bucket myuser/my-bucket /mnt/data
```

### Background daemon

`hf-mount` is the main entry point. It runs the mount in the background with automatic PID tracking and log management:

```bash
# Start (NFS by default, --fuse for FUSE)
hf-mount start --hf-token $HF_TOKEN bucket myuser/my-bucket /mnt/data
hf-mount start --fuse repo gpt2 /mnt/gpt2

# List running mounts
hf-mount status

# Stop (unmounts and waits for flush)
hf-mount stop /mnt/data
```

Logs are written to `~/.hf-mount/logs/`. PID files are stored in `~/.hf-mount/pids/`.

#### macOS: launch as a daemon with launchd

To have `hf-mount` start automatically on login, create a LaunchAgent:

```bash
label=co.huggingface.hf-mount

mkdir -p ~/Library/LaunchAgents

cat > ~/Library/LaunchAgents/$label.plist <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>$label</string>
    <key>ProgramArguments</key>
    <array>
        <string>$HOME/.local/bin/hf-mount-nfs</string>
        <string>repo</string>
        <string>openai/gpt-oss-20b</string>
        <string>/tmp/gpt-oss-20b</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/tmp/hf-mount.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/hf-mount.log</string>
</dict>
</plist>
EOF

launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/$label.plist
```

To stop: `launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/$label.plist`

### Unmount

```bash
umount /tmp/data                 # NFS or FUSE (macOS)
fusermount -u /tmp/data          # FUSE (Linux)
hf-mount stop /tmp/data          # daemon mounts
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
