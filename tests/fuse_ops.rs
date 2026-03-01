mod common;

use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const ENDPOINT: &str = "https://huggingface.co";

/// Setup: create a bucket, upload a remote test file.
/// Returns (token, bucket_id, hub, remote_filename, remote_content).
async fn setup_bucket_with_file() -> Option<(String, String, Arc<hf_mount::hub_api::HubApiClient>, String, String)> {
    let token = match std::env::var("HF_TOKEN") {
        Ok(t) => t,
        Err(_) => {
            eprintln!("Skipping: HF_TOKEN not set");
            return None;
        }
    };

    let username = common::whoami(ENDPOINT, &token).await;
    let bucket_id = format!("{}/hf-mount-ops-{}", username, std::process::id());

    common::create_bucket(ENDPOINT, &token, &bucket_id).await;
    eprintln!("Created bucket: {}", bucket_id);

    let hub = Arc::new(hf_mount::hub_api::HubApiClient::new(ENDPOINT, &token));
    let write_config = common::build_write_config(&hub, &bucket_id).await;

    let test_content = "AAAA_HEADER_AAAA|BBBB_MIDDLE_BBBB|CCCC_FOOTER_CCCC".to_string();
    let test_filename = format!("remote_{}.txt", std::process::id());

    let tmp_dir = std::env::temp_dir().join("hf-mount-ops-setup");
    std::fs::create_dir_all(&tmp_dir).ok();
    let staging_path = tmp_dir.join(&test_filename);
    std::fs::write(&staging_path, &test_content).expect("write staging file");

    let file_info = common::upload_file(write_config, &staging_path).await;

    let xet_hash = file_info.hash().to_string();
    eprintln!("Uploaded: xet_hash={}, size={}", xet_hash, file_info.file_size());

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

    std::fs::remove_dir_all(&tmp_dir).ok();

    Some((token, bucket_id, hub, test_filename, test_content))
}

#[tokio::test]
async fn test_fuse_file_operations() {
    let (token, bucket_id, hub, remote_file, remote_content) = match setup_bucket_with_file().await {
        Some(cfg) => cfg,
        None => return,
    };

    let mount_point = format!("/tmp/hf-mount-ops-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-ops-cache-{}", std::process::id());
    let remote_file_clone = remote_file.clone();

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket(&bucket_id, &mount_point, &cache_dir, &[]);
        let test_result = run_op_tests(&mount_point, &remote_file_clone, &remote_content);
        common::unmount(&mount_point, child, 30);
        test_result
    }));

    // Verify Hub state after unmount (flush should have committed files)
    let hub_check = verify_hub_state(&hub, &bucket_id, &remote_file).await;

    common::delete_bucket(ENDPOINT, &token, &bucket_id).await;
    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("FUSE op test failed: {}", e),
        Err(e) => std::panic::resume_unwind(e),
    }
    hub_check.unwrap();
}

fn run_op_tests(
    mp: &str,
    remote_file: &str,
    remote_content: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // === Test 1: Create empty file + stat ===
    eprintln!("=== Test 1: empty file ===");
    {
        let path = format!("{}/empty.txt", mp);
        std::fs::write(&path, "")?;
        let meta = std::fs::metadata(&path)?;
        assert_eq!(meta.len(), 0, "empty file should have size 0");
        let content = std::fs::read_to_string(&path)?;
        assert!(content.is_empty(), "empty file should read as empty");
        eprintln!("  OK");
    }

    // === Test 2: Write + read back ===
    eprintln!("=== Test 2: write + read back ===");
    {
        let path = format!("{}/data.txt", mp);
        std::fs::write(&path, "hello world")?;
        let content = std::fs::read_to_string(&path)?;
        assert_eq!(content, "hello world");
        eprintln!("  OK");
    }

    // === Test 3: Truncate dirty file to 0 ===
    eprintln!("=== Test 3: truncate to 0 ===");
    {
        let path = format!("{}/data.txt", mp);
        let f = std::fs::OpenOptions::new().write(true).open(&path)?;
        f.set_len(0)?;
        drop(f);
        let meta = std::fs::metadata(&path)?;
        assert_eq!(meta.len(), 0, "truncated file should have size 0");
        let content = std::fs::read_to_string(&path)?;
        assert!(content.is_empty(), "truncated file should read as empty");
        eprintln!("  OK");
    }

    // === Test 4: Truncate remote file to non-zero (finding #2) ===
    // Without the fix, truncating a remote file to non-zero would not
    // download the content first, resulting in an empty or missing staging file.
    eprintln!("=== Test 4: truncate remote to non-zero (finding #2) ===");
    {
        let path = format!("{}/{}", mp, remote_file);
        // remote_content = "AAAA_HEADER_AAAA|BBBB_MIDDLE_BBBB|CCCC_FOOTER_CCCC" (50 bytes)
        let f = std::fs::OpenOptions::new().write(true).open(&path)?;
        f.set_len(17)?;
        drop(f);

        let meta = std::fs::metadata(&path)?;
        assert_eq!(meta.len(), 17, "truncated remote should have size 17");

        let content = std::fs::read_to_string(&path)?;
        assert_eq!(
            content,
            &remote_content[..17],
            "truncated remote should match first 17 bytes"
        );
        eprintln!("  OK: truncated to {:?}", content);
    }

    // === Test 5: Read past EOF ===
    eprintln!("=== Test 5: read past EOF ===");
    {
        let path = format!("{}/small.txt", mp);
        std::fs::write(&path, "abc")?;

        let mut f = std::fs::File::open(&path)?;
        f.seek(SeekFrom::Start(100))?;
        let mut buf = vec![0u8; 10];
        let n = f.read(&mut buf)?;
        assert_eq!(n, 0, "read past EOF should return 0 bytes");
        eprintln!("  OK");
    }

    // === Test 6: Rename remote dirty file (finding #3) ===
    // The remote file was made dirty by truncation in test 4.
    // Renaming a dirty file with remote presence should record
    // pending_deletes so flush deletes the old Hub path.
    eprintln!("=== Test 6: rename dirty remote file (finding #3) ===");
    {
        let old_path = format!("{}/{}", mp, remote_file);
        let new_path = format!("{}/moved_remote.txt", mp);
        std::fs::rename(&old_path, &new_path)?;

        let content = std::fs::read_to_string(&new_path)?;
        assert_eq!(
            content,
            &remote_content[..17],
            "renamed file should still have truncated content"
        );

        assert!(
            std::fs::metadata(&old_path).is_err(),
            "old path should not exist after rename"
        );
        eprintln!("  OK");
    }

    // === Test 7: Mkdir + nested file ===
    eprintln!("=== Test 7: mkdir + nested file ===");
    {
        let dir = format!("{}/mydir", mp);
        std::fs::create_dir(&dir)?;

        let nested = format!("{}/mydir/child.txt", mp);
        std::fs::write(&nested, "nested content")?;

        let content = std::fs::read_to_string(&nested)?;
        assert_eq!(content, "nested content");

        let entries: Vec<String> = std::fs::read_dir(&dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert!(
            entries.contains(&"child.txt".to_string()),
            "readdir should list child.txt"
        );
        eprintln!("  OK: readdir={:?}", entries);
    }

    // === Test 8: Directory rename with children (finding #4) ===
    // Without the fix, update_subtree_paths wouldn't update children's
    // full_path, so flush would commit the file at the old parent path.
    eprintln!("=== Test 8: directory rename (finding #4) ===");
    {
        std::fs::rename(format!("{}/mydir", mp), format!("{}/renamed_dir", mp))?;

        let content = std::fs::read_to_string(format!("{}/renamed_dir/child.txt", mp))?;
        assert_eq!(content, "nested content", "child should be readable at new parent path");

        assert!(
            std::fs::metadata(format!("{}/mydir", mp)).is_err(),
            "old directory should not exist"
        );
        eprintln!("  OK");
    }

    // === Test 9: Unlink + rmdir ===
    eprintln!("=== Test 9: unlink + rmdir ===");
    {
        let dir = format!("{}/rmtest", mp);
        std::fs::create_dir(&dir)?;
        let file = format!("{}/rmtest/victim.txt", mp);
        std::fs::write(&file, "delete me")?;

        std::fs::remove_file(&file)?;
        assert!(std::fs::metadata(&file).is_err(), "unlinked file should not exist");

        std::fs::remove_dir(&dir)?;
        assert!(std::fs::metadata(&dir).is_err(), "removed dir should not exist");
        eprintln!("  OK");
    }

    // === Test 10: rmdir non-empty fails ===
    eprintln!("=== Test 10: rmdir non-empty ===");
    {
        let dir = format!("{}/nonempty", mp);
        std::fs::create_dir(&dir)?;
        std::fs::write(format!("{}/nonempty/file.txt", mp), "x")?;

        let result = std::fs::remove_dir(&dir);
        assert!(result.is_err(), "rmdir on non-empty dir should fail");
        eprintln!("  OK: rmdir failed as expected");

        // Clean up for the next tests
        std::fs::remove_file(format!("{}/nonempty/file.txt", mp))?;
        std::fs::remove_dir(&dir)?;
    }

    // === Test 11: Overwrite existing file ===
    eprintln!("=== Test 11: overwrite file ===");
    {
        let path = format!("{}/overwrite.txt", mp);
        std::fs::write(&path, "version 1")?;
        assert_eq!(std::fs::read_to_string(&path)?, "version 1");

        std::fs::write(&path, "version 2 is longer")?;
        assert_eq!(std::fs::read_to_string(&path)?, "version 2 is longer");

        let meta = std::fs::metadata(&path)?;
        assert_eq!(meta.len(), 19);
        eprintln!("  OK");
    }

    eprintln!("=== All file operation tests passed ===");
    Ok(())
}

async fn verify_hub_state(
    hub: &hf_mount::hub_api::HubApiClient,
    bucket_id: &str,
    remote_file: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("=== Verifying Hub state after flush ===");

    let entries = hub.list_tree(bucket_id, "").await?;
    let paths: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();
    eprintln!("  Hub files: {:?}", paths);

    // Finding #3: renamed dirty remote file should be at new path, not old
    assert!(
        paths.contains(&"moved_remote.txt"),
        "Hub should contain 'moved_remote.txt' (renamed dirty file)"
    );
    assert!(
        !paths.contains(&remote_file),
        "Hub should NOT contain '{}' (old path after rename)",
        remote_file
    );

    // Finding #4: dir-renamed child should be at new parent path
    assert!(
        paths.contains(&"renamed_dir/child.txt"),
        "Hub should contain 'renamed_dir/child.txt' (dir rename updated child path)"
    );
    assert!(
        !paths.iter().any(|p| p.starts_with("mydir/")),
        "Hub should NOT contain any 'mydir/*' paths (old parent)"
    );

    eprintln!("  OK: Hub state verified");
    Ok(())
}
