#!/bin/bash
# Wait for CSI driver to be ready and verify health
# Usage: ./wait-for-csi.sh [namespace] [timeout]

set -euo pipefail

NAMESPACE="${1:-hf-csi}"
TIMEOUT="${2:-120}"

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

# Check controller health
check_controller() {
    local pod
    pod=$(kubectl get pods -n "${NAMESPACE}" -l app=hf-csi-controller -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    
    if [[ -z "${pod}" ]]; then
        return 1
    fi
    
    # Check if container is ready
    local ready
    ready=$(kubectl get pod "${pod}" -n "${NAMESPACE}" -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null || true)
    
    if [[ "${ready}" != "true" ]]; then
        return 1
    fi
    
    # Check health endpoint
    if kubectl exec "${pod}" -n "${NAMESPACE}" -- wget -q -O- http://localhost:50051/healthz &> /dev/null || \
       kubectl exec "${pod}" -n "${NAMESPACE}" -- grpc-health-probe -addr=:50051 &> /dev/null 2>&1; then
        return 0
    fi
    
    # Just check if it's running
    local phase
    phase=$(kubectl get pod "${pod}" -n "${NAMESPACE}" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    if [[ "${phase}" == "Running" ]]; then
        return 0
    fi
    
    return 1
}

# Check node daemonset health
check_nodes() {
    local nodes
    nodes=$(kubectl get daemonset hf-csi-node -n "${NAMESPACE}" -o jsonpath='{.status.numberReady}' 2>/dev/null || echo "0")
    local desired
    desired=$(kubectl get daemonset hf-csi-node -n "${NAMESPACE}" -o jsonpath='{.status.desiredNumberScheduled}' 2>/dev/null || echo "0")
    
    if [[ "${nodes}" -eq "${desired}" && "${nodes}" -gt 0 ]]; then
        return 0
    fi
    
    log_info "Node pods ready: ${nodes}/${desired}"
    return 1
}

# Check CSI driver registration
check_csi_driver() {
    if kubectl get csidrivers csi.huggingface.co &> /dev/null; then
        return 0
    fi
    return 1
}

# Wait loop
wait_for_ready() {
    local end_time
    end_time=$((SECONDS + TIMEOUT))
    
    log_info "Waiting up to ${TIMEOUT}s for CSI driver to be ready..."
    
    while [[ $SECONDS -lt ${end_time} ]]; do
        local controller_ok=false
        local nodes_ok=false
        local driver_ok=false
        
        if check_controller; then
            controller_ok=true
        fi
        
        if check_nodes; then
            nodes_ok=true
        fi
        
        if check_csi_driver; then
            driver_ok=true
        fi
        
        if [[ "${controller_ok}" == "true" && "${nodes_ok}" == "true" && "${driver_ok}" == "true" ]]; then
            log_info "✅ CSI driver is ready!"
            return 0
        fi
        
        local status=""
        [[ "${controller_ok}" == "true" ]] && status="${status}C" || status="${status}c"
        [[ "${nodes_ok}" == "true" ]] && status="${status}N" || status="${status}n"
        [[ "${driver_ok}" == "true" ]] && status="${status}D" || status="${status}d"
        
        printf "\rWaiting... (C=controller, N=nodes, D=driver): %s (timeout in %ds) " "${status}" $((end_time - SECONDS))
        sleep 2
    done
    
    echo ""  # New line after progress
    log_error "❌ Timeout waiting for CSI driver"
    return 1
}

# Print status
print_status() {
    echo ""
    log_info "CSI Driver Status:"
    echo ""
    
    echo "Namespace: ${NAMESPACE}"
    kubectl get namespace "${NAMESPACE}" 2>/dev/null || echo "  Namespace not found"
    echo ""
    
    echo "Pods:"
    kubectl get pods -n "${NAMESPACE}" -l 'app in (hf-csi-controller, hf-csi-node)' 2>/dev/null || echo "  No pods found"
    echo ""
    
    echo "CSI Driver Registration:"
    kubectl get csidrivers csi.huggingface.co 2>/dev/null || echo "  Not registered"
    echo ""
    
    echo "StorageClass:"
    kubectl get storageclass hf-csi 2>/dev/null || echo "  Not found"
    echo ""
    
    echo "Recent Events:"
    kubectl get events -n "${NAMESPACE}" --field-selector reason!=Scheduled --sort-by='.lastTimestamp' | tail -5 2>/dev/null || echo "  No events"
}

# Main
main() {
    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl not found"
        exit 1
    fi
    
    if wait_for_ready; then
        print_status
        exit 0
    else
        print_status
        exit 1
    fi
}

main "$@"
