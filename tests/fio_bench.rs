mod common;

use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn run_fio_suite(mount_point: &str) {
    run_fio(
        mount_point,
        "seq-read-100M",
        &[
            "--filename",
            "large_0.bin",
            "--rw",
            "read",
            "--bs",
            "128k",
            "--ioengine",
            "sync",
            "--output-format",
            "normal",
        ],
    );

    run_fio(
        mount_point,
        "seq-reread-100M",
        &[
            "--filename",
            "large_0.bin",
            "--rw",
            "read",
            "--bs",
            "128k",
            "--ioengine",
            "sync",
            "--output-format",
            "normal",
        ],
    );

    run_fio(
        mount_point,
        "rand-read-4k",
        &[
            "--filename",
            "large_0.bin",
            "--rw",
            "randread",
            "--bs",
            "4k",
            "--ioengine",
            "sync",
            "--runtime",
            "10",
            "--time_based",
            "--output-format",
            "normal",
        ],
    );

    run_fio(
        mount_point,
        "seq-read-5x10M",
        &[
            "--filename",
            "medium_0.bin:medium_1.bin:medium_2.bin:medium_3.bin:medium_4.bin",
            "--rw",
            "read",
            "--bs",
            "128k",
            "--ioengine",
            "sync",
            "--output-format",
            "normal",
        ],
    );

    run_fio(
        mount_point,
        "rand-read-small",
        &[
            "--filename",
            "small_0.bin:small_1.bin:small_2.bin:small_3.bin:small_4.bin:small_5.bin:small_6.bin:small_7.bin:small_8.bin:small_9.bin",
            "--rw",
            "randread",
            "--bs",
            "4k",
            "--ioengine",
            "sync",
            "--runtime",
            "10",
            "--time_based",
            "--output-format",
            "normal",
        ],
    );
}

#[tokio::test]
async fn test_fio_compare() {
    // Check fio is installed
    if Command::new("fio").arg("--version").output().is_err() {
        eprintln!("Skipping: fio not installed");
        return;
    }

    let (token, bucket_id, hub) = match common::setup_bucket("fio-cmp").await {
        Some(cfg) => cfg,
        None => return,
    };

    let write_config = common::build_write_config(&hub).await;

    // Upload files: 10x 1MB, 5x 10MB, 1x 100MB
    let pid = std::process::id();
    let tmp_dir = std::env::temp_dir().join(format!("hf-fio-cmp-setup-{}", pid));
    std::fs::create_dir_all(&tmp_dir).ok();

    let mut batch_ops = Vec::new();
    let mtime_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let file_specs: Vec<(String, usize)> = (0..10)
        .map(|i| (format!("small_{}.bin", i), 1024 * 1024))
        .chain((0..5).map(|i| (format!("medium_{}.bin", i), 10 * 1024 * 1024)))
        .chain(std::iter::once(("large_0.bin".to_string(), 100 * 1024 * 1024)))
        .collect();

    let total_mb: usize = file_specs.iter().map(|(_, s)| s).sum::<usize>() / (1024 * 1024);
    eprintln!("Uploading {} files ({} MB total)...", file_specs.len(), total_mb);

    for (filename, size) in &file_specs {
        let data = common::generate_pattern(*size);
        let staging_path = tmp_dir.join(filename);
        std::fs::write(&staging_path, &data).expect("write staging file");

        let file_info = common::upload_file(write_config.clone(), &staging_path).await;
        let xet_hash = file_info.hash().to_string();
        eprintln!(
            "  Uploaded {} ({} MB) hash={}",
            filename,
            size / (1024 * 1024),
            &xet_hash[..16]
        );

        batch_ops.push(hf_mount::hub_api::BatchOp::AddFile {
            path: filename.clone(),
            xet_hash,
            mtime: mtime_ms,
            content_type: None,
        });
    }

    hub.batch_operations(&batch_ops).await.expect("batch add failed");
    eprintln!("All files committed to bucket");

    std::fs::remove_dir_all(&tmp_dir).ok();

    // --- FUSE fio ---
    let fuse_mount = format!("/tmp/hf-fio-cmp-fuse-{}", pid);
    let fuse_cache = format!("/tmp/hf-fio-cmp-fuse-cache-{}", pid);

    eprintln!("\n============================================================");
    eprintln!("  FUSE — fio benchmarks");
    eprintln!("============================================================");

    let fuse_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket(&bucket_id, &fuse_mount, &fuse_cache, &["--read-only"]);
        run_fio_suite(&fuse_mount);
        common::unmount(&fuse_mount, child, 5);
    }));

    std::fs::remove_dir_all(&fuse_mount).ok();
    std::fs::remove_dir_all(&fuse_cache).ok();

    // --- NFS fio ---
    let nfs_mount = format!("/tmp/hf-fio-cmp-nfs-{}", pid);
    let nfs_cache = format!("/tmp/hf-fio-cmp-nfs-cache-{}", pid);

    eprintln!("\n============================================================");
    eprintln!("  NFS — fio benchmarks");
    eprintln!("============================================================");

    let nfs_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket(&bucket_id, &nfs_mount, &nfs_cache, &["--backend=nfs", "--read-only"]);
        run_fio_suite(&nfs_mount);
        common::unmount_nfs(&nfs_mount, child, 5);
    }));

    std::fs::remove_dir_all(&nfs_mount).ok();
    std::fs::remove_dir_all(&nfs_cache).ok();

    // Cleanup
    common::delete_bucket(common::ENDPOINT, &token, &bucket_id).await;

    if let Err(e) = fuse_result {
        std::panic::resume_unwind(e);
    }
    if let Err(e) = nfs_result {
        std::panic::resume_unwind(e);
    }

    eprintln!("\n============================================================");
    eprintln!("  fio comparison complete");
    eprintln!("============================================================");
}

fn run_fio(mount_point: &str, job_name: &str, extra_args: &[&str]) {
    eprintln!("\n--- fio: {} ---", job_name);
    let output = Command::new("fio")
        .args(["--name", job_name, "--directory", mount_point, "--readonly"])
        .args(extra_args)
        .output()
        .expect("Failed to run fio");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !stderr.is_empty() {
        eprintln!("{}", stderr);
    }
    if !stdout.is_empty() {
        eprintln!("{}", stdout);
    }
}
