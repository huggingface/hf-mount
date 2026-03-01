mod common;

use std::io::{Read, Seek, SeekFrom};
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const ENDPOINT: &str = "https://huggingface.co";

async fn setup_bucket_with_file() -> Option<(String, String, Arc<hf_mount::hub_api::HubApiClient>, String, String)> {
    let token = match std::env::var("HF_TOKEN") {
        Ok(t) => t,
        Err(_) => {
            eprintln!("Skipping: HF_TOKEN not set");
            return None;
        }
    };

    let username = common::whoami(ENDPOINT, &token).await;
    let bucket_id = format!("{}/hf-mount-nfs-{}", username, std::process::id());

    common::create_bucket(ENDPOINT, &token, &bucket_id).await;
    eprintln!("Created bucket: {}", bucket_id);

    let hub = Arc::new(hf_mount::hub_api::HubApiClient::new(ENDPOINT, &token));

    let write_config = common::build_write_config(&hub, &bucket_id).await;

    let test_content = "AAAA_HEADER_AAAA|BBBB_MIDDLE_BBBB|CCCC_FOOTER_CCCC".to_string();
    let test_filename = format!("nfs_test_{}.txt", std::process::id());

    let tmp_dir = std::env::temp_dir().join("hf-mount-nfs-setup");
    std::fs::create_dir_all(&tmp_dir).ok();
    let staging_path = tmp_dir.join(&test_filename);
    std::fs::write(&staging_path, &test_content).expect("write staging file");

    let file_info = common::upload_file(write_config, &staging_path).await;

    let xet_hash = file_info.hash().to_string();
    let file_size = file_info.file_size();
    eprintln!("Uploaded: xet_hash={}, size={}", xet_hash, file_size);

    let mtime_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    hub.batch_operations(
        &bucket_id,
        &[hf_mount::hub_api::BatchOp::AddFile {
            path: test_filename.clone(),
            xet_hash,
            mtime: mtime_ms,
            content_type: None,
        }],
    )
    .await
    .expect("batch add failed");

    eprintln!("Committed {} to bucket", test_filename);
    std::fs::remove_dir_all(&tmp_dir).ok();

    Some((token, bucket_id, hub, test_filename, test_content))
}

/// Mount with --backend=nfs --read-only. Returns the child process.
fn mount_nfs_bucket(bucket_id: &str, mount_point: &str, cache_dir: &str) -> Child {
    common::mount_bucket(bucket_id, mount_point, cache_dir, &["--backend=nfs", "--read-only"])
}

/// Unmount NFS (uses umount instead of fusermount).
fn unmount_nfs(mount_point: &str, mut child: Child, graceful_secs: u64) {
    let _ = Command::new("umount").arg(mount_point).status();

    for _ in 0..graceful_secs {
        if let Ok(Some(status)) = child.try_wait() {
            eprintln!("hf-mount exited: {}", status);
            return;
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    child.kill().ok();
    match child.wait() {
        Ok(status) => eprintln!("hf-mount killed: {}", status),
        Err(e) => eprintln!("wait error: {}", e),
    }
}

#[tokio::test]
async fn test_nfs_read_only() {
    let (token, bucket_id, _hub, test_filename, test_content) = match setup_bucket_with_file().await {
        Some(cfg) => cfg,
        None => return,
    };

    let mount_point = format!("/tmp/hf-mount-nfs-test-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-nfs-cache-{}", std::process::id());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = mount_nfs_bucket(&bucket_id, &mount_point, &cache_dir);
        let test_result = run_nfs_tests(&mount_point, &test_filename, &test_content);
        unmount_nfs(&mount_point, child, 5);
        test_result
    }));

    common::delete_bucket(ENDPOINT, &token, &bucket_id).await;
    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    match result {
        Ok(Ok(())) => eprintln!("All NFS tests passed!"),
        Ok(Err(e)) => panic!("NFS test failed: {}", e),
        Err(e) => std::panic::resume_unwind(e),
    }
}

fn run_nfs_tests(
    mount_point: &str,
    test_filename: &str,
    test_content: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let file_path = format!("{}/{}", mount_point, test_filename);

    eprintln!("=== Test 1: readdir ===");
    let entries: Vec<String> = std::fs::read_dir(mount_point)?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    eprintln!("  Directory entries: {:?}", entries);
    assert!(
        entries.contains(&test_filename.to_string()),
        "file should appear in readdir"
    );

    eprintln!("=== Test 2: full read ===");
    let content = std::fs::read_to_string(&file_path)?;
    assert_eq!(content, test_content, "full read content should match");
    eprintln!("  Full read OK: {} bytes", content.len());

    eprintln!("=== Test 3: range read ===");
    {
        let mut f = std::fs::File::open(&file_path)?;
        f.seek(SeekFrom::Start(17))?;
        let mut buf = vec![0u8; 16];
        f.read_exact(&mut buf)?;
        let middle = String::from_utf8(buf)?;
        eprintln!("  Range read at offset 17, len 16: {:?}", middle);
        assert_eq!(middle, "BBBB_MIDDLE_BBBB", "range read should return middle portion");
    }

    eprintln!("=== Test 4: tail range read ===");
    {
        let mut f = std::fs::File::open(&file_path)?;
        f.seek(SeekFrom::Start(34))?;
        let mut buf = String::new();
        f.read_to_string(&mut buf)?;
        eprintln!("  Tail read from offset 34: {:?}", buf);
        assert_eq!(buf, "CCCC_FOOTER_CCCC", "tail range should return footer");
    }

    eprintln!("=== Test 5: stat size ===");
    {
        let meta = std::fs::metadata(&file_path)?;
        assert_eq!(meta.len(), test_content.len() as u64, "stat should report correct size");
        eprintln!("  Stat OK: size={}", meta.len());
    }

    eprintln!("=== Test 6: read-only enforcement ===");
    {
        let result = std::fs::write(format!("{}/should_fail.txt", mount_point), "nope");
        assert!(result.is_err(), "write should fail on read-only NFS mount");
        eprintln!("  Write correctly rejected: {:?}", result.unwrap_err().kind());
    }

    eprintln!("=== All NFS tests passed ===");
    Ok(())
}
