# HF CSI Driver Deployment

Kubernetes CSI driver for mounting Hugging Face Hub buckets and repos as persistent volumes.

## Prerequisites

- Kubernetes 1.24+ cluster
- `kubectl` configured to access your cluster
- NFS client support on nodes (for NFS backend)
- HF_TOKEN secret for private repos (optional for public access)

## Quick Start

```bash
# Deploy with the helper script
./scripts/deploy-csi.sh hf-csi $HF_TOKEN

# Or manually apply manifests
kubectl create namespace hf-csi
kubectl apply -f deploy/csi/rbac.yaml -n hf-csi
kubectl apply -f deploy/csi/csi-driver.yaml
kubectl apply -f deploy/csi/storageclass.yaml
kubectl apply -f deploy/csi/controller.yaml -n hf-csi
kubectl apply -f deploy/csi/node.yaml -n hf-csi
```

## Configuration

### Required Secrets

```bash
# Create token secret for private repos
kubectl create secret generic hf-token \
  -n hf-csi \
  --from-literal=token=$HF_TOKEN
```

### Storage Classes

The driver provides a `hf-csi` storage class by default. Configure parameters:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: hf-csi-cache
provisioner: csi.huggingface.co
parameters:
  cache-size: "20Gi"           # Cache size per node
  advanced-writes: "false"     # Enable for writable mounts
  read-only: "true"           # Default: read-only
```

## Usage Examples

### Mount a Public Dataset

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: my-dataset
spec:
  accessModes:
    - ReadOnlyMany
  storageClassName: hf-csi
  resources:
    requests:
      storage: 1Gi
  volumeMode: Filesystem
  dataSource:
    kind: VolumeSnapshot
    name: hf-source
    apiGroup: snapshot.storage.k8s.io
---
apiVersion: v1
kind: Pod
metadata:
  name: dataset-consumer
spec:
  containers:
  - name: app
    image: python:3.11
    volumeMounts:
    - name: data
      mountPath: /data
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: my-dataset
```

### Private Repository with Token

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: my-private-repo
  annotations:
    hf.repo.type: "model"      # or "dataset"
    hf.repo.id: "username/repo-name"
spec:
  accessModes:
    - ReadOnlyMany
  storageClassName: hf-csi
  resources:
    requests:
      storage: 1Gi
```

## Troubleshooting

### Check driver status
```bash
./scripts/wait-for-csi.sh hf-csi
```

### View controller logs
```bash
kubectl logs -n hf-csi -l app=hf-csi-controller
```

### View node logs
```bash
kubectl logs -n hf-csi -l app=hf-csi-node
```

### Check CSI driver registration
```bash
kubectl get csidrivers csi.huggingface.co -o yaml
```

### Common Issues

**Pod stuck in Pending**
- Check PVC status: `kubectl get pvc`
- Verify driver is registered: `kubectl get csidrivers`
- Check controller logs for provisioning errors

**Mount errors**
- Ensure NFS client is installed on nodes
- Check node pod logs for mount-specific errors
- Verify cache directory permissions

## Uninstallation

```bash
# Safe removal (preserves PVCs)
./scripts/undeploy-csi.sh hf-csi

# Force removal (deletes everything)
./scripts/undeploy-csi.sh hf-csi --force
```

## Architecture

The CSI driver consists of:

- **Controller**: Handles volume provisioning/deletion
- **Node**: Runs on every node, handles mount/unmount
- **Sidecars**: csi-provisioner, csi-attacher, node-driver-registrar, livenessprobe

## Security

- Controller runs as non-root user
- Node DaemonSet requires privileged access for mounting
- Secrets mounted as environment variables
- No persistent tokens in container images

## Testing

```bash
# Run smoke tests
./scripts/test-csi.sh hf-csi

# Manual test with busybox
kubectl run test --rm -it --image=busybox \
  --overrides='{"spec":{"volumes":[{"name":"data","persistentVolumeClaim":{"claimName":"my-dataset"}}],"containers":[{"name":"test","image":"busybox","command":["sh"],"volumeMounts":[{"name":"data","mountPath":"/data"}]}]}}'
```

## References

- [Kubernetes CSI Documentation](https://kubernetes-csi.github.io/docs/)
- [Hugging Face Hub API](https://huggingface.co/docs/hub/api)
- [Runbook - CSI Operations](/docs/RUNBOOK.md#csi-driver-operations)
