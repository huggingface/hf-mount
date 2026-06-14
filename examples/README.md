# HF CSI Driver Examples

This directory contains example Kubernetes workloads demonstrating various use cases for the Hugging Face CSI driver.

## Available Examples

### 1. PyTorch ImageNet Training (`pytorch-imagenet.yaml`)

GPU-accelerated ImageNet training using PyTorch with datasets mounted via CSI.

```bash
kubectl apply -f examples/pytorch-imagenet.yaml
```

**Features:**
- GPU resource allocation (NVIDIA)
- Data validation init container
- Multi-GPU ready

### 2. Jupyter Notebook (`jupyter-notebook.yaml`)

Interactive Jupyter environment with HF datasets pre-mounted.

```bash
kubectl apply -f examples/jupyter-notebook.yaml
kubectl port-forward svc/jupyter-hf 8888:8888
```

**Access:** http://localhost:8888 (token: `hf-demo`)

### 3. LLM Fine-tuning (`huggingface-transformers.yaml`)

Fine-tuning large language models with Transformers.

```bash
kubectl apply -f examples/huggingface-transformers.yaml
```

**Features:**
- Llama-2 model loading
- Multi-GPU training
- Automatic model caching

## Prerequisites

1. CSI driver installed: `helm install hf-csi hf-csi/hf-csi`
2. GPU nodes available (for GPU examples)
3. Sufficient PVC quota

## Common Patterns

### PVC with CSI

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: my-dataset
spec:
  accessModes: [ReadOnlyMany]
  storageClassName: hf-csi
  resources:
    requests:
      storage: 1Gi  # Virtual size
```

### Using the PVC in a Pod

```yaml
volumes:
  - name: dataset
    persistentVolumeClaim:
      claimName: my-dataset
```

## Cleanup

```bash
kubectl delete -f examples/
```
