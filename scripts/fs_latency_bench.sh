#!/usr/bin/env bash
# Benchmark hf-mount read latency (TTFB) — mirrors mountpoint-s3/scripts/fs_latency_bench.sh.
#
# Required env vars:
#   HF_TOKEN                — HuggingFace API token
#
# Optional env vars:
#   HF_ENDPOINT             — defaults to https://huggingface.co
#   HF_MOUNT_BIN            — path to hf-mount-fuse (default: ./target/release/hf-mount-fuse)
#   HF_BENCH_BUCKET         — reuse a pre-existing bucket (skips create/upload/delete)
#   HF_NO_DISK_CACHE        — set to 1 to disable the on-disk xorb chunk cache.
#                             Recommended for TTFB: measures real CAS download latency,
#                             comparable to mountpoint-s3 without --cache.
#
# Usage:
#   cargo build --release
#   ./scripts/fs_latency_bench.sh
set -euo pipefail

if ! command -v fio &>/dev/null; then
  echo "fio must be installed: sudo apt-get install -y fio" >&2; exit 1
fi
if ! command -v jq &>/dev/null; then
  echo "jq must be installed: sudo apt-get install -y jq" >&2; exit 1
fi
if [[ -z "${HF_TOKEN:-}" ]]; then
  echo "Set HF_TOKEN to run this benchmark" >&2; exit 1
fi

HF_ENDPOINT="${HF_ENDPOINT:-https://huggingface.co}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
HF_MOUNT_BIN="${HF_MOUNT_BIN:-${PROJECT_DIR}/target/release/hf-mount-fuse}"

results_dir="${PROJECT_DIR}/results"
rm -rf "${results_dir}"
mkdir -p "${results_dir}"

if [[ ! -x "${HF_MOUNT_BIN}" ]]; then
  echo "hf-mount-fuse not found at ${HF_MOUNT_BIN}. Run: cargo build --release" >&2; exit 1
fi

# ── Bucket lifecycle ──────────────────────────────────────────────────────────

OWNS_BUCKET=false
if [[ -z "${HF_BENCH_BUCKET:-}" ]]; then
  USERNAME=$(curl -sf -H "Authorization: Bearer ${HF_TOKEN}" \
    "${HF_ENDPOINT}/api/whoami-v2" | jq -r '.name')
  HF_BENCH_BUCKET="${USERNAME}/hf-latency-bench-$$"
  curl -sf -X POST "${HF_ENDPOINT}/api/buckets/${HF_BENCH_BUCKET}" \
    -H "Authorization: Bearer ${HF_TOKEN}" -H "Content-Type: application/json" \
    -d '{}' >/dev/null
  echo "Created bucket: ${HF_BENCH_BUCKET}" >&2
  OWNS_BUCKET=true
fi

_CHILD_PID=""
_MOUNT_DIR=""

cleanup() {
  if [[ -n "${_MOUNT_DIR}" ]] && mountpoint -q "${_MOUNT_DIR}" 2>/dev/null; then
    fusermount -u "${_MOUNT_DIR}" 2>/dev/null || true
  fi
  if [[ -n "${_CHILD_PID}" ]]; then
    kill "${_CHILD_PID}" 2>/dev/null || true
    wait "${_CHILD_PID}" 2>/dev/null || true
  fi
  if [[ "${OWNS_BUCKET}" == true ]]; then
    curl -sf -X DELETE "${HF_ENDPOINT}/api/buckets/${HF_BENCH_BUCKET}" \
      -H "Authorization: Bearer ${HF_TOKEN}" >/dev/null 2>&1 || true
    echo "Deleted bucket: ${HF_BENCH_BUCKET}" >&2
  fi
}
trap cleanup EXIT

# ── Mount helpers ─────────────────────────────────────────────────────────────

do_mount() {
  local cache_dir="$1"
  shift
  _MOUNT_DIR="$(mktemp -d /tmp/hf-bench-XXXXXXXXXX)"
  mkdir -p "${cache_dir}"
  "${HF_MOUNT_BIN}" \
    --hf-token "${HF_TOKEN}" \
    --hub-endpoint "${HF_ENDPOINT}" \
    --cache-dir "${cache_dir}" \
    --poll-interval-secs 0 \
    "$@" \
    bucket "${HF_BENCH_BUCKET}" "${_MOUNT_DIR}" \
    &>/dev/null &
  _CHILD_PID=$!
  for i in $(seq 1 30); do
    grep -q "${_MOUNT_DIR}" /proc/mounts 2>/dev/null && \
      { echo "Mount ready after $((i*500))ms" >&2; return 0; }
    sleep 0.5
  done
  echo "Warning: mount not ready after 15s" >&2
}

do_unmount() {
  fusermount -u "${_MOUNT_DIR}" 2>/dev/null || true
  for _ in $(seq 1 30); do
    kill -0 "${_CHILD_PID}" 2>/dev/null || { _CHILD_PID=""; _MOUNT_DIR=""; return 0; }
    sleep 1
  done
  kill "${_CHILD_PID}" 2>/dev/null || true
  wait "${_CHILD_PID}" 2>/dev/null || true
  _CHILD_PID=""; _MOUNT_DIR=""
}

# ── File setup ────────────────────────────────────────────────────────────────

if [[ "${OWNS_BUCKET}" == true ]]; then
  echo "Laying out bench files for TTFB..." >&2
  do_mount "/tmp/hf-latency-setup-cache-$$"
  for job_file in "${SCRIPT_DIR}"/fio/read_latency/*.fio; do
    echo "  Creating files for $(basename "${job_file}")" >&2
    fio --thread --directory="${_MOUNT_DIR}" --create_only=1 --eta=never "${job_file}" \
      &>/dev/null || true
  done
  echo "Unmounting to flush uploads to CAS..." >&2
  do_unmount
  echo "Files uploaded." >&2
fi

# ── TTFB benchmark ────────────────────────────────────────────────────────────

run_ttfb_jobs() {
  local jobs_dir="${SCRIPT_DIR}/fio/read_latency"

  for job_file in "${jobs_dir}"/*.fio; do
    local job_name cache_dir
    job_name="$(basename "${job_file}" .fio)"
    cache_dir="/tmp/hf-latency-cache-$$/${job_name}"

    local extra_args=(--read-only)
    [[ "${HF_NO_DISK_CACHE:-0}" == "1" ]] && extra_args+=(--no-disk-cache)
    do_mount "${cache_dir}" "${extra_args[@]}"
    echo "Running ${job_name}" >&2

    set +e
    timeout 300s fio \
      --thread \
      --output="${results_dir}/${job_name}.json" \
      --output-format=json \
      --directory="${_MOUNT_DIR}" \
      "${job_file}"
    local status=$?
    set -e

    do_unmount
    rm -rf "${_MOUNT_DIR}" 2>/dev/null || true

    if [[ ${status} -ne 0 ]]; then
      echo "Job ${job_name} failed with exit code ${status}" >&2; exit 1
    fi

    # Same jq as mountpoint-s3/scripts/fs_latency_bench.sh
    jq -n 'inputs.jobs[] |
      if .["job options"].rw == "read" then
        {name: .jobname, value: (.read.lat_ns.mean / 1000000), unit: "milliseconds"}
      elif .["job options"].rw == "randread" then
        {name: .jobname, value: (.read.lat_ns.mean / 1000000), unit: "milliseconds"}
      elif .["job options"].rw == "randwrite" then
        {name: .jobname, value: (.write.lat_ns.mean / 1000000), unit: "milliseconds"}
      else
        {name: .jobname, value: (.write.lat_ns.mean / 1000000), unit: "milliseconds"}
      end
    ' "${results_dir}/${job_name}.json" | tee "${results_dir}/${job_name}_parsed.json"
  done
}

# ── Main ──────────────────────────────────────────────────────────────────────

run_ttfb_jobs

echo "Latency results:" >&2
jq -n '[inputs] | flatten' "${results_dir}"/*.json | tee "${results_dir}/output.json"
