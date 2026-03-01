# hf-mount

Mount [Hugging Face Buckets](https://huggingface.co/docs/hub/buckets) as a local filesystem using FUSE or NFS.

## Features

- **FUSE & NFS backends** — FUSE for standard Linux/macOS, NFS for environments without `/dev/fuse` (e.g., Kubernetes CSI)
- **Adaptive prefetch** — 8 MB initial window, grows up to 128 MB for sequential reads
- **Lazy loading** — files are fetched on demand from CAS, not eagerly downloaded
- **Read-write support** — create, write, rename, delete files; changes are batched and uploaded asynchronously
- **Debounced flush** — writes are coalesced (2s debounce, 30s max window) into a single CAS upload + Hub API call
- **Remote sync** — background polling detects remote changes and updates the local view
- **Read-only mode** — `--read-only` flag for safe, read-only mounts

## Architecture

```mermaid
graph TD
    subgraph Clients
        APP[Application]
    end

    subgraph "hf-mount"
        FUSE["fs.rs<br/>FUSE Adapter"]
        NFS["nfs.rs<br/>NFS Adapter"]
        VFS["vfs.rs<br/>HfVfsCore"]
        INODE["inode.rs<br/>Inode Table"]
        CACHE["cache.rs<br/>File Cache & Staging"]
        HUB["hub_api.rs<br/>Hub API Client"]
        AUTH["auth.rs<br/>Token Refresh"]
        CC["caching_client.rs<br/>CAS Client + Cache"]
    end

    subgraph "Hugging Face"
        HUB_API["Hub API"]
        CAS["CAS Storage<br/>(xet-core)"]
    end

    APP -->|"mount"| FUSE
    APP -->|"mount"| NFS
    FUSE --> VFS
    NFS --> VFS
    VFS --> INODE
    VFS --> CACHE
    VFS --> CC
    VFS --> HUB
    HUB --> HUB_API
    CACHE --> CAS
    CC --> CAS
    AUTH --> HUB_API
    VFS -->|"poll_remote_changes"| HUB
    VFS -->|"flush_loop"| CACHE
    VFS -->|"flush_loop"| HUB
```

### Data flow

```mermaid
sequenceDiagram
    participant App
    participant VFS as HfVfsCore
    participant Prefetch as PrefetchState
    participant CAS as CAS Storage
    participant Hub as Hub API

    Note over App,Hub: Read path
    App->>VFS: read(fh, offset, size)
    VFS->>Prefetch: check buffer
    alt Cache hit
        Prefetch-->>VFS: buffered data
    else Cache miss
        Prefetch->>CAS: download range
        CAS-->>Prefetch: data
        Prefetch-->>VFS: data
    end
    VFS-->>App: bytes

    Note over App,Hub: Write path
    App->>VFS: write(ino, fh, offset, data)
    VFS->>VFS: write to staging file
    App->>VFS: flush(ino)
    VFS->>VFS: mark dirty, enqueue flush
    Note over VFS: debounce 2s / max 30s
    VFS->>CAS: upload_files (batch)
    VFS->>Hub: batch_operations (metadata)
```

## Installation

### Prerequisites

- Rust 1.80+
- FUSE: `libfuse-dev` (Linux) or `macFUSE` (macOS)
- NFS: no extra dependency

### Build

```bash
# FUSE backend only (default)
cargo build --release

# FUSE + NFS backends
cargo build --release --features nfs
```

The binary is at `target/release/hf-mount`.

## Usage

```bash
hf-mount \
  --bucket-id <USER/BUCKET> \
  --mount-point <PATH> \
  --hf-token <TOKEN>
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--bucket-id` | *required* | Hugging Face bucket ID (e.g. `myuser/mybucket`) |
| `--mount-point` | *required* | Local directory to mount on |
| `--hf-token` | *required* | HF API token (or `HF_TOKEN` env var) |
| `--hub-endpoint` | `https://huggingface.co` | Hub API endpoint |
| `--cache-dir` | `/tmp/hf-mount-cache` | Local cache directory |
| `--backend` | `fuse` | `fuse` or `nfs` |
| `--read-only` | `false` | Mount read-only |
| `--poll-interval-secs` | `30` | Remote change polling interval (0 to disable) |
| `--uid` | current user | UID for mounted files |
| `--gid` | current group | GID for mounted files |

### Examples

```bash
# Read-write FUSE mount
hf-mount --bucket-id myuser/data \
         --mount-point /mnt/data \
         --hf-token $HF_TOKEN

# Read-only NFS mount (e.g. in a container without /dev/fuse)
hf-mount --bucket-id myuser/models \
         --mount-point /mnt/models \
         --hf-token $HF_TOKEN \
         --backend nfs \
         --read-only

# Custom cache and polling
hf-mount --bucket-id myuser/data \
         --mount-point /mnt/data \
         --hf-token $HF_TOKEN \
         --cache-dir /var/cache/hf-mount \
         --poll-interval-secs 60
```

### Logging

```bash
RUST_LOG=hf_mount=debug hf-mount ...
```

## Benchmarks

Cold read performance on a 100 MB file (m5.xlarge, us-east-1):

| Backend | Throughput | vs mountpoint-s3 |
|---------|-----------|-------------------|
| **hf-mount FUSE** | **333 MB/s** | 1.9x faster |
| **hf-mount NFS** | **175 MB/s** | 1.0x |
| mountpoint-s3 | 171 MB/s | baseline |

Multi-file sequential reads (5 x 10 MB):

| Backend | Throughput |
|---------|-----------|
| **hf-mount FUSE** | **243 MB/s** |
| **hf-mount NFS** | **130 MB/s** |

*Benchmarks averaged over 3 runs with cache cleared between each run.*

## Testing

```bash
# Unit tests
cargo test

# Integration tests (require HF_TOKEN and network)
HF_TOKEN=... cargo test --test smoke
HF_TOKEN=... cargo test --test fuse_ops
HF_TOKEN=... cargo test --test write_smoke
HF_TOKEN=... cargo test --test nfs_smoke --features nfs
```

## License

Apache-2.0
