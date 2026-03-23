//! fsx (File System eXerciser) integration tests using the real xfstests fsx binary.
//!
//! Builds xfstests/ltp/fsx from source, then runs it against an hf-mount FUSE mount.
//! mmap is disabled (-R -W) because FUSE MAPWRITE goes through the kernel page cache
//! without notifying the FUSE handler (known FUSE limitation).
//!
//! Two modes:
//! - Normal: 50K random ops (read/write/truncate/copy/fallocate)
//! - Paranoid: 100 ops with close+reopen after each op (-c 1), forces flush to remote
//!
//! Requires HF_TOKEN and a real mount. Run with:
//!   cargo test --release --test fsx -- --nocapture

mod common;

use std::process::Command;

const XFSTESTS_DIR: &str = "/tmp/xfstests";
const FSX_BIN: &str = "/tmp/xfstests/ltp/fsx";

/// Build xfstests fsx binary if not present. Returns false if build fails.
fn ensure_fsx() -> bool {
    if std::path::Path::new(FSX_BIN).exists() {
        return true;
    }
    eprintln!("Building xfstests fsx...");
    let _ = Command::new("bash")
        .args([
            "-c",
            &format!(
                "cd /tmp && \
                 git clone --depth 1 https://git.kernel.org/pub/scm/fs/xfs/xfstests-dev.git {} 2>&1 && \
                 cd {} && make -j$(nproc) 2>&1 | tail -5",
                XFSTESTS_DIR, XFSTESTS_DIR
            ),
        ])
        .status();
    std::path::Path::new(FSX_BIN).exists()
}

/// Run fsx with the given args on a mounted bucket. Returns the fsx exit status.
async fn run_fsx(test_name: &str, fsx_args: &[&str]) -> bool {
    if !ensure_fsx() {
        eprintln!("Skipping: failed to build xfstests fsx");
        return true; // skip, not fail
    }

    let (token, bucket_id, _hub) = match common::setup_bucket(test_name).await {
        Some(cfg) => cfg,
        None => return true, // skip
    };

    let pid = std::process::id();
    let mount_point = format!("/tmp/hf-fsx-{}-{}", test_name, pid);
    let cache_dir = format!("/tmp/hf-fsx-{}-cache-{}", test_name, pid);
    let test_file = format!("{}/fsx_{}", mount_point, pid);

    let child = common::mount_bucket(&bucket_id, &mount_point, &cache_dir, &["--advanced-writes"]);

    eprintln!("fsx-{}: file={}, args={:?}", test_name, test_file, fsx_args);

    let output = Command::new("sudo")
        .arg(FSX_BIN)
        .args(fsx_args)
        .arg(&test_file)
        .output()
        .expect("Failed to run fsx");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !stdout.is_empty() {
        eprintln!("{}", stdout);
    }
    if !stderr.is_empty() {
        eprintln!("{}", stderr);
    }

    let success = output.status.success();

    std::fs::remove_file(&test_file).ok();
    common::unmount(&mount_point, child, 10);
    common::delete_bucket(&common::endpoint(), &token, &bucket_id).await;
    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    success
}

/// 50K random ops: read, write, truncate, copy, fallocate (no mmap).
#[tokio::test]
async fn test_fsx_data_integrity() {
    let num_ops = std::env::var("FSX_OPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50_000u64);

    assert!(
        run_fsx(
            "integrity",
            &[
                "-N",
                &num_ops.to_string(),
                "-l",
                "1048576",
                "-S",
                "42",
                "-R", // skip mmap reads
                "-W", // skip mmap writes
            ],
        )
        .await,
        "fsx data integrity failed"
    );
}

/// 100 ops with close+reopen after every op (-c 1).
/// Forces flush to remote between each operation, verifying CAS round-trip integrity.
#[tokio::test]
async fn test_fsx_paranoid() {
    let num_ops = std::env::var("FSX_PARANOID_OPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100u64);

    assert!(
        run_fsx(
            "paranoid",
            &[
                "-N",
                &num_ops.to_string(),
                "-l",
                "1048576",
                "-S",
                "42",
                "-R", // skip mmap reads
                "-W", // skip mmap writes
                "-c",
                "1", // close+reopen after every op
            ],
        )
        .await,
        "fsx paranoid (close+reopen) failed"
    );
}
