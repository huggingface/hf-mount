# HF CSI Helm Chart

Helm chart for deploying the Hugging Face CSI driver on Kubernetes.

## Prerequisites

- Kubernetes 1.24+
- Helm 3.8+
- Hugging Face token (for private repositories)

## Installation

### Add the Helm repository

```bash
helm repo add hf-csi https://huggingface.github.io/hf-mount/charts
helm repo update
```

### Install the chart

```bash
# Create namespace
kubectl create namespace hf-csi

# Install with token
helm install hf-csi hf-csi/hf-csi \
  --namespace hf-csi \
  --set token.value=YOUR_HF_TOKEN

# Or install with existing secret
helm install hf-csi hf-csi/hf-csi \
  --namespace hf-csi \
  --set token.existingSecret=my-hf-token-secret
```

## Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `controller.replicaCount` | Number of controller replicas | `2` |
| `controller.image.repository` | Controller image repository | `ghcr.io/huggingface/hf-mount-csi` |
| `controller.image.tag` | Controller image tag | `latest` |
| `token.value` | Hugging Face token value | `""` |
| `token.existingSecret` | Existing secret name for HF token | `""` |
| `storageClass.create` | Create a default StorageClass | `true` |
| `storageClass.isDefault` | Set as default StorageClass | `false` |
| `monitoring.enabled` | Enable Prometheus monitoring | `true` |
| `monitoring.serviceMonitor.enabled` | Create ServiceMonitor CRD | `true` |

## Usage Example

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: hf-dataset-pvc
spec:
  accessModes:
    - ReadOnlyMany
  storageClassName: hf-csi
  resources:
    requests:
      storage: 1Gi  # Virtual size for CSI
---
apiVersion: v1
kind: Pod
metadata:
  name: hf-training
spec:
  containers:
    - name: training
      image: huggingface/transformers-pytorch-gpu:latest
      volumeMounts:
        - name: dataset
          mountPath: /data
          readOnly: true
  volumes:
    - name: dataset
      csi:
        driver: csi.huggingface.co
        volumeAttributes:
          repo: "huggingface/datasets/imdb"
          revision: "main"
```

## Uninstallation

```bash
helm uninstall hf-csi --namespace hf-csi
```

## Troubleshooting

See the [CSI Driver Operations](../../docs/RUNBOOK.md) section in the runbook for detailed troubleshooting steps.
