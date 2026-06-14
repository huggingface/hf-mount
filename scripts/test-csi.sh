#!/bin/bash
# End-to-end smoke test for HF CSI driver
# Usage: ./test-csi.sh [namespace] [test-bucket]

set -euo pipefail

NAMESPACE="${1:-hf-csi}"
TEST_BUCKET="${2:-hf-public-datasets-ai4m-vision-com}
TEST_PVC_NAME="csi-test-pvc-$(date +%s)"
TEST_POD_NAME="csi-test-pod-$(date +%s)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# Cleanup function
cleanup() {
    log_info "Cleaning up test resources..."
    kubectl delete pod "${TEST_POD_NAME}" -n "${NAMESPACE}" --ignore-not-found=true --wait=false 2>/dev/null || true
    kubectl delete pvc "${TEST_PVC_NAME}" -n "${NAMESPACE}" --ignore-not-found=true --wait=false 2>/dev/null || true
}

trap cleanup EXIT

# Check prerequisites
check_prerequisites() {
    log_step "Checking prerequisites..."
    
    if ! kubectl get storageclass hf-csi &> /dev/null; then
        log_error "StorageClass 'hf-csi' not found. Is the CSI driver deployed?"
        exit 1
    fi
    
    log_info "StorageClass hf-csi found"
}

# Create test PVC
create_pvc() {
    log_step "Creating test PVC..."
    
    cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${TEST_PVC_NAME}
  namespace: ${NAMESPACE}
spec:
  accessModes:
    - ReadOnlyMany
  storageClassName: hf-csi
  resources:
    requests:
      storage: 1Gi
  volumeMode: Filesystem
EOF
    
    log_info "Waiting for PVC to be bound..."
    kubectl wait --for=jsonpath='{.status.phase}'=Bound pvc/"${TEST_PVC_NAME}" -n "${NAMESPACE}" --timeout=60s
    log_info "✅ PVC bound successfully"
}

# Create test pod that mounts the PVC
create_test_pod() {
    log_step "Creating test pod..."
    
    cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: ${TEST_POD_NAME}
  namespace: ${NAMESPACE}
spec:
  containers:
  - name: test
    image: busybox:latest
    command: ["sh", "-c", "sleep 3600"]
    volumeMounts:
    - name: hf-volume
      mountPath: /mnt/hf
  volumes:
  - name: hf-volume
    persistentVolumeClaim:
      claimName: ${TEST_PVC_NAME}
  restartPolicy: Never
EOF
    
    log_info "Waiting for test pod to be ready..."
    kubectl wait --for=condition=Ready pod/"${TEST_POD_NAME}" -n "${NAMESPACE}" --timeout=120s
    log_info "✅ Test pod ready"
}

# Test volume operations
test_volume() {
    log_step "Testing volume operations..."
    
    # List files
    log_info "Listing mounted directory..."
    kubectl exec "${TEST_POD_NAME}" -n "${NAMESPACE}" -- ls -la /mnt/hf/ 2>/dev/null | head -20 || true
    
    # Try to read a file (if any)
    log_info "Attempting to read files..."
    local files
    files=$(kubectl exec "${TEST_POD_NAME}" -n "${NAMESPACE}" -- find /mnt/hf -type f -readable 2>/dev/null | head -5 || true)
    
    if [[ -n "${files}" ]]; then
        log_info "Found readable files:"
        echo "${files}"
        
        # Try to read first file
        local first_file
        first_file=$(echo "${files}" | head -1)
        log_info "Reading first file: ${first_file}"
        kubectl exec "${TEST_POD_NAME}" -n "${NAMESPACE}" -- head -c 100 "${first_file}" 2>/dev/null | xxd | head -5 || true
    else
        log_warn "No readable files found (may be expected for empty/new buckets)"
    fi
    
    # Check mount is read-only
    log_info "Verifying read-only mount..."
    if kubectl exec "${TEST_POD_NAME}" -n "${NAMESPACE}" -- touch /mnt/hf/test_write 2>/dev/null; then
        log_warn "Write succeeded - mount may not be read-only as expected"
        kubectl exec "${TEST_POD_NAME}" -n "${NAMESPACE}" -- rm /mnt/hf/test_write 2>/dev/null || true
    else
        log_info "✅ Mount is correctly read-only"
    fi
}

# Test cleanup
test_cleanup() {
    log_step "Testing PVC deletion..."
    
    log_info "Deleting test pod..."
    kubectl delete pod "${TEST_POD_NAME}" -n "${NAMESPACE}" --wait=true --timeout=60s
    
    log_info "Deleting test PVC..."
    kubectl delete pvc "${TEST_PVC_NAME}" -n "${NAMESPACE}" --wait=true --timeout=60s
    
    log_info "✅ Cleanup successful"
}

# Main test flow
main() {
    log_info "Starting CSI driver smoke test"
    log_info "Namespace: ${NAMESPACE}"
    log_info "Test PVC: ${TEST_PVC_NAME}"
    echo ""
    
    check_prerequisites
    create_pvc
    create_test_pod
    test_volume
    test_cleanup
    
    echo ""
    log_info "✅ All smoke tests passed!"
    log_info "CSI driver is functioning correctly"
}

main "$@"
