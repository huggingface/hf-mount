#!/usr/bin/env bash
# Benchmark hf-mount throughput with fio — mirrors mountpoint-s3/scripts/fs_bench.sh.
#
# Required env vars:
#   HF_TOKEN                — HuggingFace API token
#
# Optional env vars:
#   HF_ENDPOINT             — defaults to https://huggingface.co
#   HF_MOUNT_BIN            — path to hf-mount-fuse (default: ./target/release/hf-mount-fuse)
#   HF_MOUNT_BACKEND        — "fuse" (default) or "nfs"
#   HF_ADVANCED_WRITES      — set to 1 to enable --advanced-writes
#   HF_NO_DISK_CACHE        — set to 1 to disable the on-disk xorb chunk cache
#   HF_BENCH_BUCKET         — reuse a pre-existing bucket (skips create/delete)
#   HF_JOB_NAME_FILTER      — only run jobs whose filename matches this substring
#   HF_CATEGORIES           — comma-separated list of categories to run
#                             (default: read,write,mix,read_latency,write_latency,create)
#   iterations              — fio iterations per job (default: 10)
#
# Usage:
#   cargo build --release
#   # FUSE with cache (default)
#   ./scripts/fs_bench.sh
#   # FUSE without cache
#   HF_NO_DISK_CACHE=1 ./scripts/fs_bench.sh
#   # FUSE with advanced writes
#   HF_ADVANCED_WRITES=1 ./scripts/fs_bench.sh
#   # NFS
#   HF_MOUNT_BACKEND=nfs ./scripts/fs_bench.sh
#   # Quick run (small files only)
#   HF_JOB_NAME_FILTER=small ./scripts/fs_bench.sh
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
BACKEND="${HF_MOUNT_BACKEND:-fuse}"
: "${iterations:=10}"

# Build the mode label for results directory
MODE="${BACKEND}"
[[ "${HF_NO_DISK_CACHE:-0}" == "1" ]] && MODE="${MODE}_nocache"
[[ "${HF_ADVANCED_WRITES:-0}" == "1" ]] && MODE="${MODE}_advwr"

# Resolve binary
if [[ "${BACKEND}" == "nfs" ]]; then
  HF_MOUNT_BIN="${HF_MOUNT_BIN:-${PROJECT_DIR}/target/release/hf-mount-nfs}"
else
  HF_MOUNT_BIN="${HF_MOUNT_BIN:-${PROJECT_DIR}/target/release/hf-mount-fuse}"
fi

if [[ ! -x "${HF_MOUNT_BIN}" ]]; then
  echo "Binary not found at ${HF_MOUNT_BIN}. Run: cargo build --release" >&2; exit 1
fi

# Categories to run
IFS=',' read -ra CATEGORIES <<< "${HF_CATEGORIES:-read,write,mix,read_latency,write_latency,create}"

results_dir="${PROJECT_DIR}/results/${MODE}"
rm -rf "${results_dir}"
mkdir -p "${results_dir}"

echo "=== Benchmark mode: ${MODE} ===" >&2
echo "Binary: ${HF_MOUNT_BIN}" >&2
echo "Categories: ${CATEGORIES[*]}" >&2
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
    if [[ "${BACKEND}" == "nfs" ]]; then
      sudo umount "${_MOUNT_DIR}" 2>/dev/null || true
    else
      fusermount -u "${_MOUNT_DIR}" 2>/dev/null || true
    fi
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

  local mount_args=(
    --hf-token "${HF_TOKEN}"
    --hub-endpoint "${HF_ENDPOINT}"
    --cache-dir "${cache_dir}"
    --poll-interval-secs 0
  )
  [[ "${HF_NO_DISK_CACHE:-0}" == "1" ]] && mount_args+=(--no-disk-cache)
  [[ "${HF_ADVANCED_WRITES:-0}" == "1" ]] && mount_args+=(--advanced-writes)
  mount_args+=("$@")
  mount_args+=(bucket "${HF_BENCH_BUCKET}" "${_MOUNT_DIR}")

  "${HF_MOUNT_BIN}" "${mount_args[@]}" &>/dev/null &
  _CHILD_PID=$!
  for i in $(seq 1 30); do
    grep -q "${_MOUNT_DIR}" /proc/mounts 2>/dev/null && \
      { echo "Mount ready after $((i*500))ms" >&2; return 0; }
    sleep 0.5
  done
  echo "Warning: mount not ready after 15s" >&2
}

do_unmount() {
  if [[ "${BACKEND}" == "nfs" ]]; then
    sudo umount "${_MOUNT_DIR}" 2>/dev/null || true
  else
    fusermount -u "${_MOUNT_DIR}" 2>/dev/null || true
  fi
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

# Throughput aggregation (MiB/s) — same jq as mountpoint-s3, with null-safe rw check.
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

  jq -s '[
    [.[].jobs] | add | group_by(.jobname)[] |
    {
      name: .[0].jobname,
      value: ((map(
        if (.["job options"].rw // "write") | test("^(rand)?read")
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

# Latency aggregation (milliseconds).
run_fio_latency_job() {
  local job_file="$1" mount_dir="$2"
  local job_name
  job_name="$(basename "${job_file}" .fio)"

  echo -n "Running latency job ${job_name}... " >&2
  set +e
  timeout 300s fio \
    --thread \
    --output="${results_dir}/${job_name}_iter1.json" \
    --output-format=json \
    --directory="${mount_dir}" \
    --eta=never \
    "${job_file}"
  local job_status=$?
  set -e
  if [[ ${job_status} -ne 0 ]]; then
    echo "Job ${job_name} failed with exit code ${job_status}" >&2; exit 1
  fi
  echo "done" >&2

  jq -n 'inputs.jobs[] |
    if (.["job options"].rw // "write") | test("^(rand)?read")
    then {name: .jobname, value: (.read.lat_ns.mean / 1000000), unit: "milliseconds"}
    else {name: .jobname, value: (.write.lat_ns.mean / 1000000), unit: "milliseconds"}
    end
  ' "${results_dir}/${job_name}_iter1.json" | tee "${results_dir}/${job_name}_parsed.json"
}

# Create benchmark aggregation (files/s via IOPS).
run_fio_create_job() {
  local job_file="$1" mount_dir="$2"
  local job_name
  job_name="$(basename "${job_file}" .fio)"

  echo -n "Running create job ${job_name} for ${iterations} iterations... " >&2
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

  # Sum IOPS across all threads, average across iterations
  jq -s '{
    name: .[0].jobs[0].jobname,
    value: ([.[].jobs | map(.write.iops) | add] | add / length),
    unit: "files/s"
  }' "${results_dir}/${job_name}_iter"*.json | tee "${results_dir}/${job_name}_parsed.json"
}

# ── Category runners ──────────────────────────────────────────────────────────

# Generate a temporary fio write job from a read job (for populating test files).
# Strips time_based/runtime so fio writes the full file, changes rw to write,
# and removes direct=1 (not useful for layout writes).
make_write_job() {
  local read_job="$1"
  local write_job
  write_job=$(mktemp /tmp/fio-write-XXXX.fio)
  sed -e 's/rw=\(rand\)\?read/rw=write/' \
      -e '/^time_based/d' \
      -e '/^runtime=/d' \
      -e '/^direct=/d' \
      "${read_job}" > "${write_job}"
  echo "${write_job}"
}

# Read category: write real data first, then benchmark reads on a fresh mount.
# This ensures reads hit CAS instead of reading sparse zeros.
run_read_category() {
  local jobs_dir="${SCRIPT_DIR}/fio/read"
  [[ -d "${jobs_dir}" ]] || return 0

  # Collect jobs to run
  local job_files=()
  for job_file in "${jobs_dir}"/*.fio; do
    should_run_job "${job_file}" || continue
    job_files+=("${job_file}")
  done
  [[ ${#job_files[@]} -gt 0 ]] || return 0

  # Phase 1: populate all test files on a single writable mount
  local populate_cache="/tmp/hf-bench-populate-$$"
  do_mount "${populate_cache}"
  for job_file in "${job_files[@]}"; do
    echo "Populating data for $(basename "${job_file}")" >&2
    local write_job
    write_job=$(make_write_job "${job_file}")
    fio --thread --directory="${_MOUNT_DIR}" --eta=never "${write_job}" &>/dev/null
    rm -f "${write_job}"
  done
  echo "Unmounting to commit writes..." >&2
  do_unmount
  rm -rf "${_MOUNT_DIR}" 2>/dev/null || true

  # Phase 2: benchmark each job on a fresh cold-cache mount
  for job_file in "${job_files[@]}"; do
    local job_name cache_dir
    job_name="$(basename "${job_file}" .fio)"
    cache_dir="/tmp/hf-bench-read-$$/${job_name}"

    do_mount "${cache_dir}"
    run_fio_job "${job_file}" "${_MOUNT_DIR}"
    do_unmount
    rm -rf "${_MOUNT_DIR}" 2>/dev/null || true
  done
}

# Standard throughput categories (write, mix).
run_throughput_category() {
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

    echo "Laying out files for ${job_file}" >&2
    fio --thread --directory="${_MOUNT_DIR}" --create_only=1 --eta=never "${job_file}" &>/dev/null

    run_fio_job "${job_file}" "${_MOUNT_DIR}"
    do_unmount
    rm -rf "${_MOUNT_DIR}" 2>/dev/null || true
  done
}

# Read latency: write real data, unmount, remount read-only, measure TTFB.
run_read_latency() {
  local jobs_dir="${SCRIPT_DIR}/fio/read_latency"
  [[ -d "${jobs_dir}" ]] || return 0

  local setup_cache="/tmp/hf-bench-latency-setup-$$"

  # Populate all latency test files with real data on one mount
  echo "Populating read_latency files..." >&2
  do_mount "${setup_cache}"
  for job_file in "${jobs_dir}"/*.fio; do
    should_run_job "${job_file}" || continue
    local write_job
    write_job=$(make_write_job "${job_file}")
    fio --thread --directory="${_MOUNT_DIR}" --eta=never "${write_job}" &>/dev/null || true
    rm -f "${write_job}"
  done
  echo "Unmounting to commit writes..." >&2
  do_unmount

  # Benchmark each job on a fresh read-only mount
  for job_file in "${jobs_dir}"/*.fio; do
    if ! should_run_job "${job_file}"; then
      echo "Skipping $(basename "${job_file}")" >&2
      continue
    fi

    local job_name cache_dir
    job_name="$(basename "${job_file}" .fio)"
    cache_dir="/tmp/hf-bench-latency-$$/${job_name}"

    do_mount "${cache_dir}" --read-only

    run_fio_latency_job "${job_file}" "${_MOUNT_DIR}"

    do_unmount
    rm -rf "${_MOUNT_DIR}" 2>/dev/null || true
  done
}

# Write latency.
run_write_latency() {
  local jobs_dir="${SCRIPT_DIR}/fio/write_latency"
  [[ -d "${jobs_dir}" ]] || return 0

  for job_file in "${jobs_dir}"/*.fio; do
    if ! should_run_job "${job_file}"; then
      echo "Skipping $(basename "${job_file}")" >&2
      continue
    fi

    local job_name cache_dir
    job_name="$(basename "${job_file}" .fio)"
    cache_dir="/tmp/hf-bench-cache-$$/${job_name}"

    do_mount "${cache_dir}"

    run_fio_latency_job "${job_file}" "${_MOUNT_DIR}"

    do_unmount
    rm -rf "${_MOUNT_DIR}" 2>/dev/null || true
  done
}

# Create benchmark (files/s).
run_create() {
  local jobs_dir="${SCRIPT_DIR}/fio/create"
  [[ -d "${jobs_dir}" ]] || return 0

  for job_file in "${jobs_dir}"/*.fio; do
    if ! should_run_job "${job_file}"; then
      echo "Skipping $(basename "${job_file}")" >&2
      continue
    fi

    local job_name cache_dir
    job_name="$(basename "${job_file}" .fio)"
    cache_dir="/tmp/hf-bench-cache-$$/${job_name}"

    do_mount "${cache_dir}"

    run_fio_create_job "${job_file}" "${_MOUNT_DIR}"

    do_unmount
    rm -rf "${_MOUNT_DIR}" 2>/dev/null || true
  done
}

# ── Main ──────────────────────────────────────────────────────────────────────

for cat in "${CATEGORIES[@]}"; do
  echo "" >&2
  echo "=== Category: ${cat} ===" >&2
  case "${cat}" in
    read)
      run_read_category ;;
    write|mix)
      run_throughput_category "${cat}" ;;
    read_latency)
      run_read_latency ;;
    write_latency)
      run_write_latency ;;
    create)
      run_create ;;
    *)
      echo "Unknown category: ${cat}" >&2 ;;
  esac
done

echo "" >&2
echo "=== Results (${MODE}) ===" >&2
jq -n '[inputs]' "${results_dir}"/*_parsed.json | tee "${results_dir}/output.json"
