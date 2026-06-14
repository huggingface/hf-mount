#!/bin/bash
# Deploy HF CSI driver to Kubernetes cluster
# Usage: ./deploy-csi.sh [namespace] [hf-token]

set -euo pipefail

NAMESPACE="${1:-hf-csi}"
HF_TOKEN="${2:-${HF_TOKEN:-}}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl not found. Please install kubectl."
        exit 1
    fi
    
    if ! kubectl cluster-info &> /dev/null; then
        log_error "Cannot connect to Kubernetes cluster."
        exit 1
    fi
    
    log_info "Connected to cluster: $(kubectl config current-context)"
}

# Create namespace
create_namespace() {
    log_info "Creating namespace: ${NAMESPACE}"
    kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
}

# Create or update HF_TOKEN secret
create_secret() {
    if [[ -z "${HF_TOKEN}" ]]; then
        log_warn "HF_TOKEN not provided. CSI driver will only work with public repos/buckets."
        log_warn "To add token later: kubectl create secret generic hf-token -n ${NAMESPACE} --from-literal=token=YOUR_TOKEN"
        # Create empty secret for optional token mounting
        kubectl create secret generic hf-token \
            -n "${NAMESPACE}" \
            --from-literal=token="" \
            --dry-run=client -o yaml | kubectl apply -f -
    else
        log_info "Creating HF_TOKEN secret..."
        kubectl create secret generic hf-token \
            -n "${NAMESPACE}" \
            --from-literal=token="${HF_TOKEN}" \
            --dry-run=client -o yaml | kubectl apply -f -
        log_info "Secret created. Token length: ${#HF_TOKEN}"
    fi
}

# Deploy CSI components
deploy_components() {
    log_info "Deploying CSI driver components..."
    
    local deploy_dir="${REPO_ROOT}/deploy/csi"
    
    # Apply RBAC first
    log_info "Applying RBAC..."
    kubectl apply -f "${deploy_dir}/rbac.yaml" -n "${NAMESPACE}"
    
    # Apply CSI Driver CRD
    log_info "Applying CSI Driver..."
    kubectl apply -f "${deploy_dir}/csi-driver.yaml"
    
    # Apply StorageClass
    log_info "Applying StorageClass..."
    kubectl apply -f "${deploy_dir}/storageclass.yaml"
    
    # Apply Controller
    log_info "Applying Controller..."
    kubectl apply -f "${deploy_dir}/controller.yaml" -n "${NAMESPACE}"
    
    # Apply Node DaemonSet
    log_info "Applying Node DaemonSet..."
    kubectl apply -f "${deploy_dir}/node.yaml" -n "${NAMESPACE}"
}

# Wait for rollout
wait_for_rollout() {
    log_info "Waiting for CSI Controller rollout..."
    kubectl rollout status deployment/hf-csi-controller -n "${NAMESPACE}" --timeout=120s
    
    log_info "Waiting for CSI Node DaemonSet rollout..."
    kubectl rollout status daemonset/hf-csi-node -n "${NAMESPACE}" --timeout=120s
}

# Verify deployment
verify_deployment() {
    log_info "Verifying deployment..."
    
    # Check controller pod
    local controller_pod
    controller_pod=$(kubectl get pods -n "${NAMESPACE}" -l app=hf-csi-controller -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    if [[ -z "${controller_pod}" ]]; then
        log_error "Controller pod not found"
        return 1
    fi
    log_info "Controller pod: ${controller_pod}"
    
    # Check node pods
    local node_pods
    node_pods=$(kubectl get pods -n "${NAMESPACE}" -l app=hf-csi-node -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)
    if [[ -z "${node_pods}" ]]; then
        log_error "Node pods not found"
        return 1
    fi
    log_info "Node pods: ${node_pods}"
    
    # Check CSI driver registration
    log_info "Checking CSI driver registration..."
    if kubectl get csidrivers csi.huggingface.co &> /dev/null; then
        log_info "CSI driver registered successfully"
    else
        log_warn "CSI driver not yet registered in API"
    fi
    
    return 0
}

# Main deployment
main() {
    log_info "Deploying HF CSI driver to namespace: ${NAMESPACE}"
    
    check_prerequisites
    create_namespace
    create_secret
    deploy_components
    wait_for_rollout
    
    if verify_deployment; then
        log_info "✅ CSI driver deployed successfully!"
        log_info ""
        log_info "Next steps:"
        log_info "  - Create a PVC: kubectl apply -f examples/pvc.yaml"
        log_info "  - Test mounting: ./scripts/test-csi.sh ${NAMESPACE}"
        log_info "  - View logs: kubectl logs -n ${NAMESPACE} -l app=hf-csi-controller"
    else
        log_error "❌ Deployment verification failed"
        log_info "Debug commands:"
        log_info "  kubectl get pods -n ${NAMESPACE}"
        log_info "  kubectl logs -n ${NAMESPACE} -l app=hf-csi-controller"
        log_info "  kubectl describe daemonset hf-csi-node -n ${NAMESPACE}"
        exit 1
    fi
}

main "$@"
