#!/usr/bin/env bash
# Benchmark hf-mount throughput with fio — mirrors mountpoint-s3/scripts/fs_bench.sh.
#
# Required env vars:
#   HF_TOKEN                — HuggingFace API token
#
# Optional env vars:
#   HF_ENDPOINT             — defaults to https://huggingface.co
#   HF_MOUNT_BIN            — path to hf-mount-fuse (default: ./target/release/hf-mount-fuse)
#   HF_BENCH_BUCKET         — reuse a pre-existing bucket (skips create/upload/delete).
#                             Use this on repeated runs to avoid re-uploading large files.
#   HF_JOB_NAME_FILTER      — only run jobs whose filename matches this substring
#                             (e.g. "small" to skip 100G jobs in CI)
#   HF_NO_DISK_CACHE        — set to 1 to disable the on-disk xorb chunk cache.
#                             Comparable to mountpoint-s3 without --cache (reads fetch
#                             from CAS on each FUSE miss, OS page cache still applies).
#   iterations              — fio iterations per job (default: 10)
#
# Comparable to mountpoint-s3 when run on a high-network instance (they use m5dn.24xlarge,
# 100 Gbps). Set HF_JOB_NAME_FILTER=small for quick CI runs on smaller instances.
#
# Usage:
#   cargo build --release
#   ./scripts/fs_bench.sh
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
: "${iterations:=10}"

results_dir="${PROJECT_DIR}/results"
rm -rf "${results_dir}"
mkdir -p "${results_dir}"

if [[ ! -x "${HF_MOUNT_BIN}" ]]; then
  echo "hf-mount-fuse not found at ${HF_MOUNT_BIN}. Run: cargo build --release" >&2; exit 1
fi

[[ -n "${HF_JOB_NAME_FILTER:-}" ]] && echo "Job filter: ${HF_JOB_NAME_FILTER}" >&2

# ── Bucket lifecycle ──────────────────────────────────────────────────────────

OWNS_BUCKET=false
if [[ -z "${HF_BENCH_BUCKET:-}" ]]; then
  USERNAME=$(curl -sf -H "Authorization: Bearer ${HF_TOKEN}" \
    "${HF_ENDPOINT}/api/whoami-v2" | jq -r '.name')
  HF_BENCH_BUCKET="${USERNAME}/hf-bench-$$"
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

# Mount hf-mount-fuse; sets _CHILD_PID and _MOUNT_DIR.
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

# ── File setup (one-time) ─────────────────────────────────────────────────────
# Write all benchmark files through hf-mount's write path, flush to CAS, then unmount.
# This mirrors mountpoint-s3's "create_only" step, but batched for efficiency.

if [[ "${OWNS_BUCKET}" == true ]]; then
  echo "Laying out bench files (write-through to CAS)..." >&2
  do_mount "/tmp/hf-bench-setup-cache-$$"
  for category in read write; do
    jobs_dir="${SCRIPT_DIR}/fio/${category}"
    [[ -d "${jobs_dir}" ]] || continue
    for job_file in "${jobs_dir}"/*.fio; do
      if ! { [[ -z "${HF_JOB_NAME_FILTER:-}" ]] || [[ "${job_file}" == *"${HF_JOB_NAME_FILTER}"* ]]; }; then
        continue
      fi
      echo "  Creating files for $(basename "${job_file}")" >&2
      fio --thread --directory="${_MOUNT_DIR}" --create_only=1 --eta=never "${job_file}" \
        &>/dev/null || true
    done
  done
  echo "Unmounting to flush uploads to CAS (may take a while for large files)..." >&2
  do_unmount
  echo "Files uploaded." >&2
fi

# ── fio benchmark runner ──────────────────────────────────────────────────────

# Mirrors mountpoint-s3's run_fio_job() and its jq aggregation exactly.
run_fio_job() {
  local job_file="$1" mount_dir="$2"
  local job_name
  job_name="$(basename "${job_file}" .fio)"

  echo -n "Running job ${job_name} for ${iterations} iterations... " >&2
  for i in $(seq 1 "${iterations}"); do
    echo -n "${i};" >&2
    set +e
    timeout 300s fio \
      --thread \
      --output="${results_dir}/${job_name}_iter${i}.json" \
      --output-format=json \
      --directory="${mount_dir}" \
      --eta=never \
      "${job_file}"
    local job_status=$?
    set -e
    if [[ ${job_status} -ne 0 ]]; then
      echo "Job ${job_name} failed with exit code ${job_status}" >&2; exit 1
    fi
  done
  echo "done" >&2

  # Average across iterations — same jq logic as mountpoint-s3/scripts/fs_bench.sh
  jq -s '[
    [.[].jobs] | add | group_by(.jobname)[] |
    {
      name: .[0].jobname,
      value: ((map(
        if .["job options"].rw | test("^(rand)?read")
        then .read.bw
        else .write.bw
        end
      ) | add) / (length * 1024)),
      unit: "MiB/s"
    }
  ] | {
    name: map(.name) | unique | join(","),
    value: map(.value) | add,
    unit: "MiB/s"
  }' "${results_dir}/${job_name}_iter"*.json | tee "${results_dir}/${job_name}_parsed.json"
}

run_benchmarks() {
  local category="$1"
  local jobs_dir="${SCRIPT_DIR}/fio/${category}"
  [[ -d "${jobs_dir}" ]] || return 0

  for job_file in "${jobs_dir}"/*.fio; do
    if ! { [[ -z "${HF_JOB_NAME_FILTER:-}" ]] || [[ "${job_file}" == *"${HF_JOB_NAME_FILTER}"* ]]; }; then
      echo "Skipping $(basename "${job_file}")" >&2
      continue
    fi

    local job_name cache_dir
    job_name="$(basename "${job_file}" .fio)"
    cache_dir="/tmp/hf-bench-cache-$$/${job_name}"

    # Mount read-write so fio can create and benchmark on the same mount.
    local extra_args=()
    [[ "${HF_NO_DISK_CACHE:-0}" == "1" ]] && extra_args+=(--no-disk-cache)
    do_mount "${cache_dir}" "${extra_args[@]}"

    # Lay out files (create if not present), then immediately run benchmark on same mount.
    echo "Laying out files for ${job_file}" >&2
    fio --thread --directory="${_MOUNT_DIR}" --create_only=1 --eta=never "${job_file}" &>/dev/null

    echo "Running ${job_file}" >&2
    run_fio_job "${job_file}" "${_MOUNT_DIR}"
    do_unmount
    rm -rf "${_MOUNT_DIR}" 2>/dev/null || true
  done
}

# ── Main ──────────────────────────────────────────────────────────────────────

run_benchmarks read
run_benchmarks write
run_benchmarks mix

echo "Throughput:" >&2
jq -n '[inputs]' "${results_dir}"/*_parsed.json | tee "${results_dir}/output.json"
