mod common;

/// Test default (simple/streaming) write mode: append-only, synchronous close.
#[tokio::test]
async fn test_fuse_simple_writes() {
    let test_content = common::test_content();
    let remote_file = format!("test_{}.txt", std::process::id());
    let guard = match common::setup_bucket_with_file("fuse-simple", &remote_file, test_content.as_bytes()).await {
        Some(g) => g,
        None => return,
    };

    let mount_point = format!("/tmp/hf-mount-fuse-simple-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-fuse-simple-cache-{}", std::process::id());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket(&guard.bucket_id, &mount_point, &cache_dir, &[]);
        let r = common::fs_tests::run_read_tests(&mount_point, &remote_file, &test_content)
            .and_then(|_| common::fs_tests::run_simple_write_tests(&mount_point, &remote_file));
        common::unmount(&mount_point, child, 30);
        r
    }));

    let hub_check =
        common::fs_tests::verify_simple_hub_state(&guard.hub, &remote_file, test_content.len() as u64).await;

    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("FUSE simple write test failed: {}", e),
        Err(e) => std::panic::resume_unwind(e),
    }
    if let Err(e) = hub_check {
        panic!("Hub state check failed: {}", e);
    }
}

/// Test --advanced-writes mode: staging files, async flush, truncate, overwrite.
#[tokio::test]
async fn test_fuse_advanced_writes() {
    let test_content = common::test_content();
    let remote_file = format!("test_{}.txt", std::process::id());
    let guard = match common::setup_bucket_with_file("fuse-adv", &remote_file, test_content.as_bytes()).await {
        Some(g) => g,
        None => return,
    };

    let mount_point = format!("/tmp/hf-mount-fuse-adv-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-fuse-adv-cache-{}", std::process::id());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket(&guard.bucket_id, &mount_point, &cache_dir, &["--advanced-writes"]);
        let r = common::fs_tests::run_read_tests(&mount_point, &remote_file, &test_content)
            .and_then(|_| common::fs_tests::run_write_tests(&mount_point, &remote_file, &test_content));
        common::unmount(&mount_point, child, 30);
        r
    }));

    let trunc_size = test_content.find("BBBB_MIDDLE_BBBB").unwrap() as u64;
    let hub_check = common::fs_tests::verify_hub_state(&guard.hub, &remote_file, trunc_size).await;

    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("FUSE advanced write test failed: {}", e),
        Err(e) => std::panic::resume_unwind(e),
    }
    if let Err(e) = hub_check {
        panic!("Hub state check failed: {}", e);
    }
}

/// Read a file under a large subdir without any prior `readdir`: the VFS
/// slow path should resolve the file via HEAD alone, without materializing
/// the sibling inodes. We can't observe the inode table from a subprocess,
/// but a successful content match proves the HEAD path wires up correctly
/// end-to-end.
#[tokio::test]
async fn test_fuse_point_lookup_skips_list_tree() {
    let guard = match common::setup_bucket("fuse-point-lookup").await {
        Some(g) => g,
        None => return,
    };

    // Upload 20 siblings into a subdir — these must not be required to
    // resolve the one file we actually open.
    let write_config = common::build_write_config(&guard.hub).await;
    let tmp_dir = std::env::temp_dir().join(format!("hf-mount-pl-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).ok();
    let mut ops = Vec::with_capacity(21);
    for i in 0..20 {
        let path = tmp_dir.join(format!("sib_{i:02}.txt"));
        std::fs::write(&path, format!("sib_{i:02}")).unwrap();
        let info = common::upload_file(write_config.clone(), &path).await;
        ops.push(hf_mount::hub_api::BatchOp::AddFile {
            path: format!("big_dir/sib_{i:02}.txt"),
            xet_hash: info.hash().to_string(),
            mtime: 0,
            content_type: None,
        });
    }
    let target_path = tmp_dir.join("target.txt");
    let target_content = b"hello from the target";
    std::fs::write(&target_path, target_content).unwrap();
    let target_info = common::upload_file(write_config.clone(), &target_path).await;
    ops.push(hf_mount::hub_api::BatchOp::AddFile {
        path: "big_dir/target.txt".to_string(),
        xet_hash: target_info.hash().to_string(),
        mtime: 0,
        content_type: None,
    });
    guard.hub.batch_operations(&ops).await.expect("batch add failed");
    std::fs::remove_dir_all(&tmp_dir).ok();

    let mount_point = format!("/tmp/hf-mount-pl-mnt-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-pl-cache-{}", std::process::id());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<(), String> {
        let child = common::mount_bucket(&guard.bucket_id, &mount_point, &cache_dir, &["--read-only"]);
        // Read the target file directly — no `ls`, no readdir on big_dir.
        let content = std::fs::read(format!("{}/big_dir/target.txt", mount_point))
            .map_err(|e| format!("read target: {e}"))?;
        if content != target_content {
            return Err(format!("content mismatch: got {} bytes", content.len()));
        }
        common::unmount(&mount_point, child, 30);
        Ok(())
    }));

    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("point lookup test failed: {}", e),
        Err(e) => std::panic::resume_unwind(e),
    }
}

/// Read a file via a cold deep path — every intermediate directory must
/// resolve correctly through the HEAD-then-list-tree fallback chain.
#[tokio::test]
async fn test_fuse_deep_cold_read() {
    let guard = match common::setup_bucket("fuse-deep-read").await {
        Some(g) => g,
        None => return,
    };
    let write_config = common::build_write_config(&guard.hub).await;
    let tmp_dir = std::env::temp_dir().join(format!("hf-mount-dr-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).ok();
    let staging = tmp_dir.join("payload.txt");
    let payload = b"deep payload";
    std::fs::write(&staging, payload).unwrap();
    let info = common::upload_file(write_config, &staging).await;
    guard
        .hub
        .batch_operations(&[hf_mount::hub_api::BatchOp::AddFile {
            path: "a/b/c/d/payload.txt".to_string(),
            xet_hash: info.hash().to_string(),
            mtime: 0,
            content_type: None,
        }])
        .await
        .expect("batch add failed");
    std::fs::remove_dir_all(&tmp_dir).ok();

    let mount_point = format!("/tmp/hf-mount-dr-mnt-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-dr-cache-{}", std::process::id());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<(), String> {
        let child = common::mount_bucket(&guard.bucket_id, &mount_point, &cache_dir, &["--read-only"]);
        let content = std::fs::read(format!("{}/a/b/c/d/payload.txt", mount_point))
            .map_err(|e| format!("read deep: {e}"))?;
        if content != payload {
            return Err("deep read content mismatch".to_string());
        }
        // readdir on an intermediate should still work.
        let entries: Vec<_> = std::fs::read_dir(format!("{}/a/b", mount_point))
            .map_err(|e| format!("readdir a/b: {e}"))?
            .filter_map(|e| e.ok().and_then(|e| e.file_name().into_string().ok()))
            .collect();
        if !entries.contains(&"c".to_string()) {
            return Err(format!("readdir a/b should list 'c', got {:?}", entries));
        }
        common::unmount(&mount_point, child, 30);
        Ok(())
    }));

    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("deep cold read test failed: {}", e),
        Err(e) => std::panic::resume_unwind(e),
    }
}

/// Test HEAD revalidation: remote file changes are detected via lookup() HEAD
/// and the kernel page cache is invalidated so re-reads return fresh content.
#[tokio::test]
async fn test_fuse_revalidation() {
    let test_content = common::test_content();
    let remote_file = format!("test_{}.txt", std::process::id());
    let guard = match common::setup_bucket_with_file("fuse-reval", &remote_file, test_content.as_bytes()).await {
        Some(g) => g,
        None => return,
    };

    let mount_point = format!("/tmp/hf-mount-fuse-reval-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-fuse-reval-cache-{}", std::process::id());

    let child = common::mount_bucket(
        &guard.bucket_id,
        &mount_point,
        &cache_dir,
        &["--metadata-ttl-ms", "100"],
    );

    let result = common::fs_tests::run_revalidation_test(
        &mount_point,
        &remote_file,
        &test_content,
        &guard.hub,
        100, // metadata_ttl_ms
    )
    .await;

    common::unmount(&mount_point, child, 30);
    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    if let Err(e) = result {
        panic!("FUSE revalidation test failed: {}", e);
    }
}
