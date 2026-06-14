# feat: Windows Support, NFS Desktop, and Kubernetes CSI Driver

## Summary

This PR adds comprehensive cross-platform support for `hf-mount`, enabling the project to compile and run on **Windows** via the NFS backend, and introduces a full **Kubernetes CSI driver** (`hf-mount-csi`) for dynamic volume provisioning of Hugging Face repos and buckets.

### Three-Phase Implementation

#### Phase 1: Cross-Platform Core

- Gated all Unix-specific APIs (`libc`, `std::os::unix`, `nix`) behind `#[cfg(unix)]`
- Replaced `pread`/`pwrite` with cross-platform `Mutex<File>` + `seek`+`read`/`write` for thread-safe file I/O
- Restructured `hf-mount.rs` binary: Unix uses daemon fork/exec, Windows spawns backend directly
- Verified: `cargo check --target x86_64-pc-windows-gnu --features nfs` passes clean

#### Phase 2: Windows Desktop via NFS

- Added `--nfs-bind` CLI option with platform-specific defaults (`127.0.0.1:0` on Unix, `0.0.0.0:2049` on Windows)
- `hf-mount-nfs.exe` binds to `0.0.0.0:2049` by default so the Windows built-in NFS client can connect
- Added Windows usage docs to README with PowerShell examples

#### Phase 3: Kubernetes CSI Driver

- Added `tonic` + `prost` dependencies with `build.rs` for protobuf code generation
- Created `proto/csi.proto` with Identity, Controller, and Node services
- Implemented `src/csi.rs` with `HfCsiIdentity`, `HfCsiController`, and `HfCsiNode`
- Created `src/bin/hf-mount-csi.rs` — TCP-based CSI driver with gRPC health checks and graceful shutdown
- Platform-specific `NodePublishVolume`: Unix spawns `hf-mount-nfs`, Windows spawns `hf-mount-nfs.exe` + auto-mounts via `mount -o anon`
- Controller validates sources against HF Hub API during `CreateVolume`
- Full Kubernetes manifests in `deploy/csi/`: CSIDriver, StorageClass, RBAC, Controller Deployment, Node DaemonSet (Linux + Windows hostProcess), Secret template, Kustomization base

## New Binaries

| Binary             | Platform | Description                                                      |
| ------------------ | -------- | ---------------------------------------------------------------- |
| `hf-mount-csi`     | All      | Kubernetes CSI gRPC driver (Identity, Controller, Node services) |
| `hf-mount-nfs.exe` | Windows  | NFS server serving on `0.0.0.0:2049`                             |

## New Tests

- `tests/csi_bidirectional.rs` — 9 tests verifying protobuf round-tripping of `repeated` fields and `oneof` types (PluginCapability, ControllerServiceCapability, NodeServiceCapability, VolumeCapability, etc.)
- `tests/csi_conformance.rs` — 13 integration tests for CSI service conformance (Identity, Controller, Node)
- `tests/csi_unit.rs` — 9 unit tests using mocked `HubValidator` and `ProcessSpawner` traits for `VolumeRegistry`, `HfCsiController`, and `HfCsiNode`
- `tests/csi_health.rs` — 9 integration tests for `tonic-health` gRPC health checking (liveness, readiness, graceful shutdown)

## Verified Builds

```bash
# macOS host
cargo check --all-targets --features nfs                                        # OK
cargo clippy --all-targets --features nfs                                       # clean
cargo test --lib --features nfs                                                 # 342 passed
cargo test --test csi_bidirectional --test csi_conformance --test csi_unit --test csi_health --features nfs  # 56 passed

# Windows cross-compile
cargo check --lib --bins --target x86_64-pc-windows-gnu --features nfs          # OK
```

## Usage

### Windows Desktop

```powershell
# Start the NFS server (binds to 0.0.0.0:2049 by default)
hf-mount-nfs.exe repo openai-community/gpt2 C:\mnt\gpt2

# Mount from Windows (requires NFS client feature)
Enable-WindowsOptionalFeature -Online -FeatureName ServicesForNFS-ClientOnly
mount -o anon \\127.0.0.1\\ C:
```

### Kubernetes

```bash
# Install the CSI driver
kubectl apply -k deploy/csi

# Create a PVC using the StorageClass
kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: hf-gpt2
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: hf-mount
  resources:
    requests:
      storage: 1Gi
EOF
```

## Files Changed

- `Cargo.toml` — Added `tonic`, `prost`, `tonic-health`, `tokio/process` feature
- `build.rs` — Protobuf compilation
- `proto/csi.proto` — CSI spec
- `src/csi.rs` — CSI service implementations with `HubValidator` and `ProcessSpawner` trait injection for testability
- `src/bin/hf-mount-csi.rs` — CSI driver binary
- `src/lib.rs` — Registered `csi` module
- `src/virtual_fs/mod.rs` — Refactored: split ~3,800-line monolithic impl into topical submodules (`core`, `handle`, `lookup`, `attr`, `open`, `io`, `dir`, `rename`)
- `src/virtual_fs/{core,handle,lookup,attr,open,io,dir,rename}.rs` — New submodule files extracted from `mod.rs`
- `deploy/csi/*` — Kubernetes manifests
- `.github/workflows/ci.yml` — Added CSI test suite and Windows cross-compile check to CI
- `README.md` — Windows usage, Kubernetes CSI driver info, and testing instructions
- `tests/csi_bidirectional.rs` — Protobuf round-trip tests
- `tests/csi_conformance.rs` — CSI conformance integration tests
- `tests/csi_unit.rs` — Unit tests with mocked network and process spawners
- `tests/csi_health.rs` — `tonic-health` integration tests
