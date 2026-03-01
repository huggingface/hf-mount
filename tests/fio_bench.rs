mod common;

use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

const ENDPOINT: &str = "https://huggingface.co";

fn run_fio(mount_point: &str, job_name: &str, extra_args: &[&str]) {
    eprintln!("\n--- fio: {} ---", job_name);
    let output = Command::new("fio")
        .args([
            "--name",
            job_name,
            "--directory",
            mount_point,
            "--readonly",
            "--minimal",
        ])
        .args(extra_args)
        .output()
        .expect("Failed to run fio");

    // Print human-readable output
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !stderr.is_empty() {
        eprintln!("{}", stderr);
    }
    if !stdout.is_empty() {
        eprintln!("{}", stdout);
    }
}

#[tokio::test]
async fn test_fio_bench() {
    let token = match std::env::var("HF_TOKEN") {
        Ok(t) => t,
        Err(_) => {
            eprintln!("Skipping: HF_TOKEN not set");
            return;
        }
    };

    let pid = std::process::id();
    let username = common::whoami(ENDPOINT, &token).await;
    let bucket_id = format!("{}/hf-mount-fio-{}", username, pid);

    common::create_bucket(ENDPOINT, &token, &bucket_id).await;
    eprintln!("Created bucket: {}", bucket_id);

    let hub = std::sync::Arc::new(hf_mount::hub_api::HubApiClient::new(ENDPOINT, &token));
    let write_config = common::build_write_config(&hub, &bucket_id).await;

    // Upload files of various sizes: 10x 1MB, 5x 10MB, 1x 100MB
    let tmp_dir = std::env::temp_dir().join(format!("hf-fio-setup-{}", pid));
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

    hub.batch_operations(&bucket_id, &batch_ops)
        .await
        .expect("batch add failed");
    eprintln!("All files committed to bucket");

    std::fs::remove_dir_all(&tmp_dir).ok();

    let mount_point = format!("/tmp/hf-fio-mount-{}", pid);
    let cache_dir = format!("/tmp/hf-fio-cache-{}", pid);

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket(&bucket_id, &mount_point, &cache_dir, &["--read-only"]);

        // 1. Sequential read of the large file
        run_fio(
            &mount_point,
            "seq-read-large",
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

        // 2. Sequential re-read (xorb cache hot)
        run_fio(
            &mount_point,
            "seq-reread-large",
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

        // 3. Random read 4K on large file
        run_fio(
            &mount_point,
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

        // 4. Sequential read of multiple medium files
        run_fio(
            &mount_point,
            "seq-read-medium",
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

        // 5. Random read across small files
        run_fio(
            &mount_point,
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

        common::unmount(&mount_point, child, 5);
    }));

    common::delete_bucket(ENDPOINT, &token, &bucket_id).await;
    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}
