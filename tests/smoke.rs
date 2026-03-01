mod common;

use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const ENDPOINT: &str = "https://huggingface.co";

/// Setup: create a bucket, upload a test file to it via CAS.
/// Returns (token, bucket_id, hub, test_filename, test_content).
async fn setup_bucket_with_file() -> Option<(String, String, Arc<hf_mount::hub_api::HubApiClient>, String, String)> {
    let token = match std::env::var("HF_TOKEN") {
        Ok(t) => t,
        Err(_) => {
            eprintln!("Skipping: HF_TOKEN not set");
            return None;
        }
    };

    let username = common::whoami(ENDPOINT, &token).await;
    let bucket_id = format!("{}/hf-mount-fuse-{}", username, std::process::id());

    common::create_bucket(ENDPOINT, &token, &bucket_id).await;
    eprintln!("Created bucket: {}", bucket_id);

    let hub = Arc::new(hf_mount::hub_api::HubApiClient::new(ENDPOINT, &token));

    // Upload a test file via CAS
    let write_config = common::build_write_config(&hub, &bucket_id).await;

    // Create a file with recognizable content for range read testing
    let test_content = "AAAA_HEADER_AAAA|BBBB_MIDDLE_BBBB|CCCC_FOOTER_CCCC".to_string();
    let test_filename = format!("range_test_{}.txt", std::process::id());

    let tmp_dir = std::env::temp_dir().join("hf-mount-fuse-setup");
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

/// FUSE smoke test: mounts a bucket, reads files through the filesystem (range reads),
/// writes a new file, reads it back, then cleans up.
#[tokio::test]
async fn test_fuse_range_read_and_write() {
    let (token, bucket_id, _hub, test_filename, test_content) = match setup_bucket_with_file().await {
        Some(cfg) => cfg,
        None => return,
    };

    let mount_point = format!("/tmp/hf-mount-test-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-cache-{}", std::process::id());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket(&bucket_id, &mount_point, &cache_dir, &[]);
        let test_result = run_fuse_tests(&mount_point, &test_filename, &test_content);
        common::unmount(&mount_point, child, 5);
        test_result
    }));

    // Always clean up bucket
    common::delete_bucket(ENDPOINT, &token, &bucket_id).await;
    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    match result {
        Ok(Ok(())) => eprintln!("All FUSE tests passed!"),
        Ok(Err(e)) => panic!("FUSE test failed: {}", e),
        Err(e) => std::panic::resume_unwind(e),
    }
}

fn run_fuse_tests(
    mount_point: &str,
    test_filename: &str,
    test_content: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let file_path = format!("{}/{}", mount_point, test_filename);

    // --- Test 1: readdir lists the file ---
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

    // --- Test 2: full read matches expected content ---
    eprintln!("=== Test 2: full read ===");
    let content = std::fs::read_to_string(&file_path)?;
    assert_eq!(content, test_content, "full read content should match");
    eprintln!("  Full read OK: {} bytes", content.len());

    // --- Test 3: range read (seek + read a portion) ---
    eprintln!("=== Test 3: range read ===");
    {
        let mut f = std::fs::File::open(&file_path)?;

        // Read just the "BBBB_MIDDLE_BBBB" part (offset 17, len 16)
        // "AAAA_HEADER_AAAA|BBBB_MIDDLE_BBBB|CCCC_FOOTER_CCCC"
        //  0123456789012345678901234567890123
        f.seek(SeekFrom::Start(17))?;
        let mut buf = vec![0u8; 16];
        f.read_exact(&mut buf)?;
        let middle = String::from_utf8(buf)?;
        eprintln!("  Range read at offset 17, len 16: {:?}", middle);
        assert_eq!(middle, "BBBB_MIDDLE_BBBB", "range read should return middle portion");
    }

    // --- Test 4: read just the last part ---
    eprintln!("=== Test 4: tail range read ===");
    {
        let mut f = std::fs::File::open(&file_path)?;
        f.seek(SeekFrom::Start(34))?;
        let mut buf = String::new();
        f.read_to_string(&mut buf)?;
        eprintln!("  Tail read from offset 34: {:?}", buf);
        assert_eq!(buf, "CCCC_FOOTER_CCCC", "tail range should return footer");
    }

    // --- Test 5: write a new file and read it back ---
    eprintln!("=== Test 5: write + read back ===");
    {
        let new_file = format!("{}/written_by_test.txt", mount_point);
        let write_content = "hello from FUSE test!\n";
        std::fs::write(&new_file, write_content)?;

        // Read it back through the mount
        let read_back = std::fs::read_to_string(&new_file)?;
        assert_eq!(read_back, write_content, "written content should match when read back");
        eprintln!("  Write + read back OK");
    }

    // --- Test 6: stat reports correct size ---
    eprintln!("=== Test 6: stat size ===");
    {
        let meta = std::fs::metadata(&file_path)?;
        assert_eq!(
            meta.len(),
            test_content.len() as u64,
            "stat should report correct file size"
        );
        eprintln!("  Stat OK: size={}", meta.len());
    }

    eprintln!("=== All FUSE tests passed ===");
    Ok(())
}
