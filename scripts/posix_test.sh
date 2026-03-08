#!/usr/bin/env bash
# Run pjdfstest POSIX conformance tests against an hf-mount FUSE mount.
#
# Required env vars:
#   HF_TOKEN                — HuggingFace API token
#
# Optional env vars:
#   HF_ENDPOINT             — defaults to https://huggingface.co
#   HF_MOUNT_BIN            — path to hf-mount-fuse (default: ./target/release/hf-mount-fuse)
#   PJDFSTEST_DIR           — pjdfstest install dir (default: /tmp/pjdfstest)
#   PJDFSTEST_REV           — pinned commit (default below)
#
# Prerequisites: autoconf, automake, gcc, perl (prove), fuse
#
# Usage:
#   export HF_TOKEN=hf_xxx
#   ./scripts/posix_test.sh

set -euo pipefail

# ── Config ──────────────────────────────────────────────────────────────
HF_ENDPOINT="${HF_ENDPOINT:-https://huggingface.co}"
HF_MOUNT_BIN="${HF_MOUNT_BIN:-./target/release/hf-mount-fuse}"
PJDFSTEST_DIR="${PJDFSTEST_DIR:-/tmp/pjdfstest}"
PJDFSTEST_REV="${PJDFSTEST_REV:-03eb25706d8dbf3611c3f820b45b7a5e09a36c06}"

# Expected baselines (update when adding new POSIX features)
EXPECTED_FILES_PASS=130
EXPECTED_TESTS_PASS=832

# Categories excluded from testing (unsupported special file types / ops)
EXCLUDED_CATEGORIES="mkfifo|mknod|link"

# Patterns in .t file content that trigger exclusion:
#   mkfifo, mknod, "for type in": special file types (ENOSYS, cascade-fail)
#   ENAMETOOLONG, NAME_MAX: name length validation (not enforced)
#   S_ISVTX, sticky: sticky bit enforcement (not implemented)
#   socket: Unix domain sockets (unsupported)
#   "expect 0 link": hard link syscall (ENOTSUP)
EXCLUDED_PATTERNS="mkfifo|mknod|for type in|ENAMETOOLONG|NAME_MAX|S_ISVTX|sticky|socket|expect 0 link"

PID=$$
MOUNT_POINT="/tmp/hf-pjdfs-${PID}"
CACHE_DIR="/tmp/hf-pjdfs-cache-${PID}"

# ── Helpers ─────────────────────────────────────────────────────────────
die() { echo "FATAL: $*" >&2; exit 1; }

cleanup() {
    echo "Cleaning up..."
    fusermount -u "$MOUNT_POINT" 2>/dev/null || true
    sleep 1
    # Kill any leftover mount process
    if [[ -n "${MOUNT_PID:-}" ]]; then
        kill "$MOUNT_PID" 2>/dev/null || true
        wait "$MOUNT_PID" 2>/dev/null || true
    fi
    # Delete the bucket
    if [[ -n "${BUCKET_ID:-}" ]]; then
        curl -sf -X DELETE \
            -H "Authorization: Bearer ${HF_TOKEN}" \
            "${HF_ENDPOINT}/api/buckets/${BUCKET_ID}" || true
        echo "Deleted bucket: ${BUCKET_ID}"
    fi
    rm -rf "$MOUNT_POINT" "$CACHE_DIR"
}
trap cleanup EXIT

# ── Preflight ───────────────────────────────────────────────────────────
[[ -z "${HF_TOKEN:-}" ]] && die "HF_TOKEN not set"
[[ -x "$HF_MOUNT_BIN" ]] || die "Binary not found: $HF_MOUNT_BIN"
command -v prove >/dev/null 2>&1 || die "prove (perl TAP harness) not found"

# ── Build pjdfstest ─────────────────────────────────────────────────────
if [[ -x "${PJDFSTEST_DIR}/pjdfstest" ]]; then
    # Verify correct revision
    CACHED_REV=$(git -C "$PJDFSTEST_DIR" rev-parse HEAD 2>/dev/null || echo "")
    if [[ "$CACHED_REV" != "$PJDFSTEST_REV" ]]; then
        echo "Cached pjdfstest has wrong revision, rebuilding..."
        rm -rf "$PJDFSTEST_DIR"
    else
        echo "Using cached pjdfstest at $PJDFSTEST_DIR"
    fi
fi

if [[ ! -x "${PJDFSTEST_DIR}/pjdfstest" ]]; then
    echo "Building pjdfstest..."
    rm -rf "$PJDFSTEST_DIR"
    git clone https://github.com/pjd/pjdfstest.git "$PJDFSTEST_DIR"
    (cd "$PJDFSTEST_DIR" && git checkout "$PJDFSTEST_REV" && autoreconf -ifs && ./configure && make pjdfstest)
fi

# ── Create bucket ───────────────────────────────────────────────────────
USERNAME=$(curl -sf -H "Authorization: Bearer ${HF_TOKEN}" \
    "${HF_ENDPOINT}/api/whoami-v2" | python3 -c "import sys,json; print(json.load(sys.stdin)['name'])")
BUCKET_ID="${USERNAME}/hf-mount-pjdfs-${PID}"

curl -sf -X POST \
    -H "Authorization: Bearer ${HF_TOKEN}" \
    -H "Content-Type: application/json" \
    -d '{}' \
    "${HF_ENDPOINT}/api/buckets/${BUCKET_ID}" >/dev/null || true
echo "Created bucket: ${BUCKET_ID}"

# ── Mount ───────────────────────────────────────────────────────────────
mkdir -p "$MOUNT_POINT" "$CACHE_DIR"

RUST_LOG=hf_mount=info "$HF_MOUNT_BIN" \
    --hf-token "$HF_TOKEN" \
    --hub-endpoint "$HF_ENDPOINT" \
    --cache-dir "$CACHE_DIR" \
    --poll-interval-secs 0 \
    --advanced-writes \
    bucket "$BUCKET_ID" "$MOUNT_POINT" \
    > /tmp/hf-pjdfs-mount.log 2>&1 &
MOUNT_PID=$!

# Wait for mount to be ready
echo -n "Waiting for mount..."
for i in $(seq 1 30); do
    if grep -q "$MOUNT_POINT" /proc/mounts 2>/dev/null; then
        echo " ready (${i}s)"
        break
    fi
    sleep 1
done
grep -q "$MOUNT_POINT" /proc/mounts 2>/dev/null || die "Mount not ready after 30s"

# ── Collect test files ──────────────────────────────────────────────────
TESTS_DIR="${PJDFSTEST_DIR}/tests"
TEST_FILES=()
SKIPPED=0

for cat_dir in "$TESTS_DIR"/*/; do
    cat_name=$(basename "$cat_dir")
    if echo "$cat_name" | grep -qE "^(${EXCLUDED_CATEGORIES})$"; then
        echo "  Skipping category: $cat_name (unsupported)"
        continue
    fi
    for t_file in "$cat_dir"*.t; do
        [[ -f "$t_file" ]] || continue
        if grep -qE "$EXCLUDED_PATTERNS" "$t_file"; then
            SKIPPED=$((SKIPPED + 1))
            continue
        fi
        TEST_FILES+=("$t_file")
    done
done

echo "Running ${#TEST_FILES[@]} test files ($SKIPPED skipped for excluded patterns)"

# ── Run prove ───────────────────────────────────────────────────────────
cd "$MOUNT_POINT"
PROVE_OUTPUT=$(sudo prove "${TEST_FILES[@]}" 2>&1) || true
cd /

echo "$PROVE_OUTPUT"

# ── Parse results ───────────────────────────────────────────────────────
TOTAL_FILES=$(echo "$PROVE_OUTPUT" | grep -oP 'Files=\K\d+' || echo 0)
TOTAL_TESTS=$(echo "$PROVE_OUTPUT" | grep -oP 'Tests=\K\d+' || echo 0)
PASSED_FILES=$(echo "$PROVE_OUTPUT" | grep -c ' ok$' || echo 0)
RESULT=$(echo "$PROVE_OUTPUT" | grep '^Result:' | awk '{print $2}')
RESULT="${RESULT:-UNKNOWN}"

# Count failed subtests
FAILED_TESTS=$(echo "$PROVE_OUTPUT" | grep 'Failed.*subtests' | \
    sed 's/.*Failed \([0-9]*\)\/.*/\1/' | \
    awk '{s+=$1} END {print s+0}')

PASSED_TESTS=$((TOTAL_TESTS - FAILED_TESTS))

echo ""
echo "============================================================"
echo "  pjdfstest POSIX Compliance Results"
echo "------------------------------------------------------------"
printf "  Files: %d/%d passed    Tests: %d total (%d subtests failed)\n" \
    "$PASSED_FILES" "$TOTAL_FILES" "$TOTAL_TESTS" "$FAILED_TESTS"
echo "  Result: $RESULT"
echo "============================================================"

# ── Assertions ──────────────────────────────────────────────────────────
EXIT_CODE=0

if [[ "$PASSED_FILES" -lt "$EXPECTED_FILES_PASS" ]]; then
    echo "REGRESSION: ${PASSED_FILES}/${TOTAL_FILES} files passed (expected >= ${EXPECTED_FILES_PASS})"
    EXIT_CODE=1
fi

if [[ "$PASSED_TESTS" -lt "$EXPECTED_TESTS_PASS" ]]; then
    echo "REGRESSION: ${PASSED_TESTS}/${TOTAL_TESTS} tests passed (expected >= ${EXPECTED_TESTS_PASS})"
    EXIT_CODE=1
fi

if [[ "$EXIT_CODE" -eq 0 ]]; then
    echo "PASS: ${PASSED_FILES} files, ${PASSED_TESTS} tests (baselines: ${EXPECTED_FILES_PASS} files, ${EXPECTED_TESTS_PASS} tests)"
fi

exit $EXIT_CODE
