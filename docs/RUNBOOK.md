# hf-mount Operational Runbook

Quick reference for operating, troubleshooting, and recovering hf-mount in production environments.

## Table of Contents

1. [Installation Verification](#installation-verification)
2. [Common Operations](#common-operations)
3. [Troubleshooting](#troubleshooting)
4. [CSI Driver Operations](#csi-driver-operations)
5. [Performance Tuning](#performance-tuning)
6. [Security Best Practices](#security-best-practices)
7. [Incident Response](#incident-response)

---

## Installation Verification

### Check Binary Version
```bash
hf-mount --version
hf-mount-fuse --version
hf-mount-nfs --version
```

### Verify Backends Available
```bash
# List available binaries
which hf-mount hf-mount-fuse hf-mount-nfs

# Check FUSE availability (Linux)
ls -la /dev/fuse

# Check NFS client (macOS/Linux)
showmount -e localhost
```

### Test Mount (Public Repo)
```bash
# Quick test with a small public repo
mkdir -p /tmp/hf-test
hf-mount-nfs repo openai-community/gpt2 /tmp/hf-test
ls /tmp/hf-test
umount /tmp/hf-test  # or fusermount -u /tmp/hf-test on Linux
```

---

## Common Operations

### Mount a Bucket
```bash
# NFS backend (recommended)
hf-mount start bucket myuser/my-bucket /mnt/data

# FUSE backend (tighter integration)
hf-mount start --fuse bucket myuser/my-bucket /mnt/data

# With custom cache directory
hf-mount start bucket myuser/my-bucket /mnt/data --cache-dir /var/cache/hf-mount
```

### Mount a Repo (Read-Only)
```bash
# Public repo (no token needed)
hf-mount start repo openai/gpt-oss-20b /mnt/model

# Private repo
hf-mount start --hf-token $HF_TOKEN repo myorg/private-model /mnt/model

# Specific revision
hf-mount start repo openai-community/gpt2 /mnt/gpt2 --revision v1.0

# Subfolder only
hf-mount start repo openai-community/gpt2/onnx /mnt/onnx
```

### Check Mount Status
```bash
hf-mount status
```

### Stop a Mount
```bash
# Using hf-mount (daemon mode)
hf-mount stop /mnt/data

# Direct unmount (if daemon not available)
umount /mnt/data           # macOS / NFS
fusermount -u /mnt/data    # Linux FUSE
```

### View Logs
```bash
# Daemon logs
tail -f ~/.hf-mount/logs/*.log

# Live debug logging
RUST_LOG=hf_mount=debug hf-mount-nfs bucket myuser/my-bucket /mnt/data

# JSON structured logs
RUST_LOG=hf_mount=info hf-mount-nfs bucket myuser/my-bucket /mnt/data 2>&1 | jq -R 'fromjson?'
```

---

## Troubleshooting

### Mount Fails to Start

**Symptom:** `hf-mount start` returns immediately but mount point empty

**Diagnosis:**
```bash
# Check if mount process exists
hf-mount status

# Check logs
tail ~/.hf-mount/logs/*.log

# Try foreground mode for visibility
hf-mount-nfs bucket myuser/my-bucket /mnt/data
```

**Common Causes:**
- Invalid token (401/403 errors in logs)
- Bucket/repo doesn't exist (404 errors)
- Network connectivity issues
- Mount point not empty or permission denied

### Permission Denied (macOS)

**Symptom:** `Operation not permitted` when accessing files

**Solution:**
1. System Settings > Privacy & Security > Full Disk Access
2. Add your terminal application (Terminal.app, iTerm2, VSCode)
3. Restart the application

### Stale File Handle

**Symptom:** `ESTALE` or "Stale file handle" errors

**Cause:** Remote file changed since last poll (default 30s)

**Solution:**
```bash
# Remount with shorter poll interval
hf-mount start bucket myuser/my-bucket /mnt/data --poll-interval-secs 5

# Or remount with minimal metadata TTL (FUSE only)
hf-mount start --fuse --metadata-ttl-minimal bucket myuser/my-bucket /mnt/data
```

### High Memory Usage

**Symptom:** RSS grows unbounded during large tree operations

**Solution:**
```bash
# Set inode soft limit to bound memory
hf-mount start bucket myuser/my-bucket /mnt/data --inode-soft-limit 10000

# Faster LRU sweep
hf-mount start bucket myuser/my-bucket /mnt/data --inode-soft-limit 10000 --lru-sweep-interval-ms 2000
```

### Write Failures

**Symptom:** Cannot modify files, `EPERM` errors

**Common Causes:**
- Trying to write to read-only repo mount
- Streaming mode doesn't support text editors (use `--advanced-writes`)
- Trying to modify remote files in overlay mode

**Solution:**
```bash
# For text editors, use advanced writes
hf-mount start --advanced-writes bucket myuser/my-bucket /mnt/data

# For overlay mode, only local layer files are writable
# Copy remote file to new local name first
cp /mnt/data/remote-file.txt /mnt/data/local-copy.txt
# Now edit local-copy.txt
```

### Network Timeouts

**Symptom:** Reads hang, eventually timeout

**Diagnosis:**
```bash
# Test Hub connectivity
curl -H "Authorization: Bearer $HF_TOKEN" https://huggingface.co/api/whoami-v2

# Check if specific repo is accessible
curl https://huggingface.co/api/models/openai-community/gpt2
```

**Solutions:**
- Check firewall/proxy settings
- Verify HF_TOKEN is valid
- Increase timeouts (if supported by backend)

---

## CSI Driver Operations

### Prerequisites
- Kubernetes 1.24+ cluster
- kubectl configured
- Docker images built: `ghcr.io/huggingface/hf-mount-csi:latest`

### Install CSI Driver (Recommended)
```bash
# Deploy with helper script (creates namespace, secrets, and components)
./scripts/deploy-csi.sh hf-csi $HF_TOKEN

# Or deploy manually
kubectl create namespace hf-csi
kubectl apply -f deploy/csi/rbac.yaml -n hf-csi
kubectl apply -f deploy/csi/csi-driver.yaml
kubectl apply -f deploy/csi/storageclass.yaml
kubectl apply -f deploy/csi/controller.yaml -n hf-csi
kubectl apply -f deploy/csi/node.yaml -n hf-csi
```

### Verify Installation
```bash
# Wait for rollout to complete
./scripts/wait-for-csi.sh hf-csi

# Check driver pods
kubectl get pods -n hf-csi -l app=hf-csi-node
kubectl get pods -n hf-csi -l app=hf-csi-controller

# Check CSIDriver resource
kubectl get csidriver csi.huggingface.co

# Check StorageClass
kubectl get storageclass hf-csi
```

### Run Smoke Tests
```bash
# Run end-to-end tests (creates PVC, mounts it, validates)
./scripts/test-csi.sh hf-csi
```

### Create a PVC
```bash
kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: hf-model
  namespace: hf-csi
spec:
  accessModes: [ReadOnlyMany]
  storageClassName: hf-csi
  resources:
    requests:
      storage: 1Gi
EOF
```

### Debug Volume Issues
```bash
# Check PVC events
kubectl describe pvc hf-model -n hf-csi

# Check PV
kubectl get pv

# Check driver logs
kubectl logs -n hf-csi -l app=hf-csi-controller -c csi-driver
kubectl logs -n hf-csi -l app=hf-csi-node -c csi-driver

# Check node driver registrar logs
kubectl logs -n hf-csi -l app=hf-csi-node -c csi-node-driver-registrar

# Check CSI health endpoint
kubectl exec -n hf-csi deploy/hf-csi-controller -- wget -qO- http://localhost:50051/healthz
```

### Uninstall CSI Driver
```bash
# Safe removal (checks for existing PVCs)
./scripts/undeploy-csi.sh hf-csi

# Force removal (deletes everything including namespace)
./scripts/undeploy-csi.sh hf-csi --force
```

### Force Delete Stuck Volume
```bash
# Remove finalizer if volume stuck in Terminating
kubectl patch pv <pv-name> -p '{"metadata":{"finalizers":[]}}' --type=merge
```

### Battle/Chaos Testing with kube-rs/envtest

```bash
# Run integration tests that simulate cluster failures
cargo test --features nfs csi_integration -- --nocapture

# Test scenarios include:
# - Controller pod restart during volume provisioning
# - Node drain while volume is mounted
# - Network partition simulation
# - Concurrent PVC operations
```

---

## Performance Tuning

### Cache Optimization
```bash
# Larger cache for repeated reads
hf-mount start bucket myuser/my-bucket /mnt/data --cache-size 50000000000  # 50 GB

# Disable cache for pure streaming (benchmarking)
hf-mount start bucket myuser/my-bucket /mnt/data --no-disk-cache

# Use file-level cache for warm reloads
hf-mount start bucket myuser/my-bucket /mnt/data --cache-mode file
```

### Concurrency Tuning
```bash
# Reduce poll concurrency in shared environments
hf-mount start bucket myuser/my-bucket /mnt/data --poll-listing-concurrency 1

# Disable polling entirely (manual refresh only)
hf-mount start bucket myuser/my-bucket /mnt/data --poll-interval-secs 0
```

### Read Optimization
```bash
# Direct I/O for large sequential reads (bypass page cache)
hf-mount start --fuse --direct-io bucket myuser/my-bucket /mnt/data
```

---

## Security Best Practices

### Token Handling
```bash
# ❌ Bad: Token visible in process list
hf-mount start --hf-token $HF_TOKEN bucket myuser/my-bucket /mnt/data

# ✅ Good: Token from environment (less visible)
HF_TOKEN=$(cat ~/.hf-token) hf-mount start bucket myuser/my-bucket /mnt/data

# ✅ Better: Token from file (re-reads for rotation)
echo "my-token" > ~/.hf-token
chmod 600 ~/.hf-token
hf-mount start --token-file ~/.hf-token bucket myuser/my-bucket /mnt/data

# ✅ Best: Force file-only mode (prevents accidental CLI token)
hf-mount start --token-file-only --token-file ~/.hf-token bucket myuser/my-bucket /mnt/data
```

### CSI Security
```bash
# Check CSI pods are running as non-root
kubectl get pods -n kube-system -l app=hf-csi-node -o jsonpath='{.items[0].spec.containers[0].securityContext}'

# Verify seccomp profile
kubectl get pods -n kube-system -l app=hf-csi-node -o jsonpath='{.items[0].spec.securityContext.seccompProfile}'
```

### Network Security
```bash
# Bind NFS to localhost only (default on Unix)
hf-mount-nfs --nfs-bind 127.0.0.1:2049 bucket myuser/my-bucket /mnt/data

# On Windows, restrict to specific interface
hf-mount-nfs.exe --nfs-bind 192.168.1.100:2049 bucket myuser/my-bucket C:\mnt\data
```

---

## Incident Response

### P1: Complete Mount Failure

**Checklist:**
1. Check daemon status: `hf-mount status`
2. Check logs: `tail -n 100 ~/.hf-mount/logs/*.log`
3. Test Hub connectivity: `curl https://huggingface.co/api/models/openai-community/gpt2`
4. Verify token: `curl -H "Authorization: Bearer $HF_TOKEN" https://huggingface.co/api/whoami-v2`
5. Try foreground mode: `hf-mount-nfs bucket myuser/my-bucket /mnt/data`
6. If urgent, remount with `--no-disk-cache` to rule out cache corruption

### P2: Data Loss on Write

**Checklist:**
1. Check if crash occurred before `close()` (streaming mode) or flush (advanced mode)
2. Check logs for upload errors
3. Verify Hub state: `curl -H "Authorization: Bearer $HF_TOKEN" https://huggingface.co/api/buckets/myuser/my-bucket`
4. Document incident: note file size, mount options, timing
5. Switch to `--advanced-writes` for critical data with frequent checkpoints

### P3: Performance Degradation

**Checklist:**
1. Check cache usage: `du -sh /tmp/hf-mount-cache`
2. Check memory: `ps aux | grep hf-mount`
3. Review mount options: `--inode-soft-limit`, `--cache-size`
4. Check network latency: `ping huggingface.co`
5. Consider `--poll-interval-secs 0` if polling is causing load

### P4: CSI Driver Issues

**Checklist:**
1. Check driver pod status: `kubectl get pods -n kube-system -l app=hf-csi-node`
2. Check gRPC health: `grpc_health_probe -addr=localhost:50051` (from within pod)
3. Check driver logs: `kubectl logs -n kube-system deployment/hf-csi-controller`
4. Verify RBAC: `kubectl auth can-i create volumesnapshot -n kube-system --as=system:serviceaccount:kube-system:hf-csi-driver`
5. Restart driver: `kubectl rollout restart daemonset/hf-csi-node -n kube-system`

---

## Quick Reference

### Environment Variables
| Variable      | Purpose            | Example                  |
| ------------- | ------------------ | ------------------------ |
| `HF_TOKEN`    | API authentication | `hf_xxxxx`               |
| `HF_ENDPOINT` | Hub endpoint       | `https://huggingface.co` |
| `RUST_LOG`    | Logging level      | `hf_mount=debug`         |

### Common Mount Options
| Option                 | Purpose           | Example                     |
| ---------------------- | ----------------- | --------------------------- |
| `--read-only`          | Prevent writes    | `--read-only`               |
| `--advanced-writes`    | Staging files     | `--advanced-writes`         |
| `--cache-dir`          | Custom cache path | `--cache-dir /var/cache/hf` |
| `--cache-size`         | Max cache bytes   | `--cache-size 50000000000`  |
| `--inode-soft-limit`   | Memory cap        | `--inode-soft-limit 10000`  |
| `--poll-interval-secs` | Poll frequency    | `--poll-interval-secs 60`   |

### Log Locations
| Platform    | Path                            |
| ----------- | ------------------------------- |
| Linux/macOS | `~/.hf-mount/logs/`             |
| Windows     | `%USERPROFILE%\.hf-mount\logs\` |

### Exit Codes
| Code | Meaning              |
| ---- | -------------------- |
| 0    | Success              |
| 1    | General error        |
| 2    | Invalid arguments    |
| 130  | Interrupted (Ctrl+C) |

---

## Getting Help

- **Issues:** https://github.com/huggingface/hf-mount/issues
- **Discussions:** https://github.com/huggingface/hf-mount/discussions
- **Documentation:** https://huggingface.co/docs/hub/storage-buckets

## Changelog

See [CHANGELOG.md](../CHANGELOG.md) or GitHub Releases for version history.
