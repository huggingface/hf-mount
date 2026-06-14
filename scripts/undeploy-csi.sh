#!/bin/bash
# Cleanly remove HF CSI driver from Kubernetes cluster
# Usage: ./undeploy-csi.sh [namespace] [--force]

set -euo pipefail

NAMESPACE="${1:-hf-csi}"
FORCE=false
if [[ "${2:-}" == "--force" ]]; then
    FORCE=true
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check for existing PVCs
check_pvcs() {
    log_info "Checking for existing PVCs using hf-csi storage class..."
    
    local pvcs
    pvcs=$(kubectl get pvc --all-namespaces -o json | jq -r '.items[] | select(.spec.storageClassName=="hf-csi") | "\(.metadata.namespace)/\(.metadata.name)"' 2>/dev/null || true)
    
    if [[ -n "${pvcs}" ]]; then
        log_warn "Found PVCs still using hf-csi:"
        echo "${pvcs}"
        
        if [[ "${FORCE}" == "false" ]]; then
            log_error "Refusing to undeploy while PVCs exist. Use --force to override."
            log_info "To delete PVCs: kubectl delete pvc <name> -n <namespace>"
            exit 1
        else
            log_warn "Force mode enabled. PVCs will become unusable."
        fi
    else
        log_info "No PVCs found using hf-csi storage class"
    fi
}

# Drain volumes (best effort)
drain_volumes() {
    log_info "Attempting to drain volumes..."
    
    # Get all PVs provisioned by hf-csi
    local pvs
    pvs=$(kubectl get pv -o json | jq -r '.items[] | select(.spec.csi.driver=="csi.huggingface.co") | .metadata.name' 2>/dev/null || true)
    
    if [[ -n "${pvs}" ]]; then
        log_warn "Found PVs that will be orphaned:"
        echo "${pvs}"
        
        for pv in ${pvs}; do
            log_info "Attempting to clean up PV: ${pv}"
            kubectl patch pv "${pv}" -p '{"metadata":{"finalizers":[]}}' --type=merge 2>/dev/null || true
        done
    fi
}

# Delete resources in reverse order of deployment
undeploy_components() {
    local deploy_dir="${REPO_ROOT}/deploy/csi"
    
    log_info "Deleting CSI Node DaemonSet..."
    kubectl delete -f "${deploy_dir}/node.yaml" -n "${NAMESPACE}" --ignore-not-found=true
    
    log_info "Deleting CSI Controller..."
    kubectl delete -f "${deploy_dir}/controller.yaml" -n "${NAMESPACE}" --ignore-not-found=true
    
    log_info "Deleting StorageClass..."
    kubectl delete -f "${deploy_dir}/storageclass.yaml" --ignore-not-found=true
    
    log_info "Deleting CSI Driver..."
    kubectl delete -f "${deploy_dir}/csi-driver.yaml" --ignore-not-found=true
    
    log_info "Deleting RBAC..."
    kubectl delete -f "${deploy_dir}/rbac.yaml" -n "${NAMESPACE}" --ignore-not-found=true
}

# Delete secrets
delete_secrets() {
    log_info "Deleting secrets..."
    kubectl delete secret hf-token -n "${NAMESPACE}" --ignore-not-found=true
}

# Wait for pod termination
wait_for_termination() {
    log_info "Waiting for pods to terminate..."
    
    local retries=30
    while [[ ${retries} -gt 0 ]]; do
        local pods
        pods=$(kubectl get pods -n "${NAMESPACE}" -l 'app in (hf-csi-controller, hf-csi-node)' -o name 2>/dev/null || true)
        
        if [[ -z "${pods}" ]]; then
            log_info "All CSI pods terminated"
            return 0
        fi
        
        log_info "Waiting... (${retries} retries left)"
        sleep 2
        ((retries--)) || true
    done
    
    log_warn "Some pods may still be terminating"
    return 1
}

# Delete namespace (optional)
delete_namespace() {
    if kubectl get namespace "${NAMESPACE}" &> /dev/null; then
        log_info "Deleting namespace ${NAMESPACE}..."
        kubectl delete namespace "${NAMESPACE}" --wait=false
        log_info "Namespace deletion initiated (may take time)"
    fi
}

# Main
main() {
    log_info "Undeploying HF CSI driver from namespace: ${NAMESPACE}"
    
    check_pvcs
    drain_volumes
    undeploy_components
    delete_secrets
    wait_for_termination
    
    if [[ "${FORCE}" == "true" ]]; then
        delete_namespace
    fi
    
    log_info "✅ CSI driver undeployed"
    log_info ""
    log_info "Note: PVs may remain in Terminating state if pods were still using them."
    log_info "To force delete: kubectl patch pv <name> -p '{\"metadata\":{\"finalizers\":[]}}' --type=merge"
}

main "$@"
