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
/// the sibling inodes. A successful content match proves the HEAD path
/// wires up correctly end-to-end.
#[tokio::test]
async fn test_fuse_point_lookup_skips_list_tree() {
    let guard = match common::setup_bucket("fuse-point-lookup").await {
        Some(g) => g,
        None => return,
    };
    let (target_rel, target_content) = common::seed_big_dir_with_target(&guard.hub, "fuse-pl").await;

    let mount_point = format!("/tmp/hf-mount-pl-mnt-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-pl-cache-{}", std::process::id());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket(&guard.bucket_id, &mount_point, &cache_dir, &["--read-only"]);
        let op = common::fs_tests::assert_point_read(&mount_point, &target_rel, target_content);
        common::unmount(&mount_point, child, 30);
        op
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
    let (deep_rel, payload) = common::seed_deep_tree(&guard.hub, "fuse-dr").await;

    let mount_point = format!("/tmp/hf-mount-dr-mnt-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-dr-cache-{}", std::process::id());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket(&guard.bucket_id, &mount_point, &cache_dir, &["--read-only"]);
        let op = common::fs_tests::assert_deep_read_and_intermediate_readdir(&mount_point, &deep_rel, payload);
        common::unmount(&mount_point, child, 30);
        op
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
