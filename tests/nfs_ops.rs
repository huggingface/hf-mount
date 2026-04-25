mod common;

#[tokio::test]
async fn test_nfs_read_only() {
    let test_content = common::test_content();
    let remote_file = format!("test_{}.txt", std::process::id());
    let guard = match common::setup_bucket_with_file("nfs", &remote_file, test_content.as_bytes()).await {
        Some(g) => g,
        None => return,
    };

    let mount_point = format!("/tmp/hf-mount-nfs-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-nfs-cache-{}", std::process::id());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket_nfs(&guard.bucket_id, &mount_point, &cache_dir, &["--read-only"]);
        let r = common::fs_tests::run_read_tests(&mount_point, &remote_file, &test_content).map(|_| {
            // Read-only enforcement: writes must fail
            eprintln!("  [nfs] read-only enforcement");
            let result = std::fs::write(format!("{}/should_fail.txt", mount_point), "nope");
            assert!(result.is_err(), "write should fail on read-only NFS mount");
            eprintln!("  [nfs] write correctly rejected: {:?}", result.unwrap_err().kind());
        });
        common::unmount_nfs(&mount_point, child, 5);
        r
    }));

    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    match result {
        Ok(Ok(())) => eprintln!("All NFS read-only tests passed!"),
        Ok(Err(e)) => panic!("NFS test failed: {}", e),
        Err(e) => std::panic::resume_unwind(e),
    }
}

/// Read a file under a large subdir without any prior `readdir`. The VFS
/// slow path should resolve the file via HEAD alone, without materializing
/// its siblings.
#[tokio::test]
async fn test_nfs_point_lookup_in_large_dir() {
    let guard = match common::setup_bucket("nfs-point-lookup").await {
        Some(g) => g,
        None => return,
    };
    let (target_rel, target_content) = common::seed_big_dir_with_target(&guard.hub, "nfs-pl").await;

    let mount_point = format!("/tmp/hf-mount-nfs-pl-mnt-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-nfs-pl-cache-{}", std::process::id());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket_nfs(&guard.bucket_id, &mount_point, &cache_dir, &["--read-only"]);
        let op = common::fs_tests::assert_point_read(&mount_point, &target_rel, target_content);
        common::unmount_nfs(&mount_point, child, 10);
        op
    }));

    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("NFS point-lookup test failed: {}", e),
        Err(e) => std::panic::resume_unwind(e),
    }
}

/// Read a file via a cold deep path — every intermediate directory must
/// resolve correctly through the HEAD → list_tree fallback chain.
#[tokio::test]
async fn test_nfs_deep_cold_read() {
    let guard = match common::setup_bucket("nfs-deep-read").await {
        Some(g) => g,
        None => return,
    };
    let (deep_rel, payload) = common::seed_deep_tree(&guard.hub, "nfs-dr").await;

    let mount_point = format!("/tmp/hf-mount-nfs-dr-mnt-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-nfs-dr-cache-{}", std::process::id());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket_nfs(&guard.bucket_id, &mount_point, &cache_dir, &["--read-only"]);
        let op = common::fs_tests::assert_deep_read_and_intermediate_readdir(&mount_point, &deep_rel, payload);
        common::unmount_nfs(&mount_point, child, 10);
        op
    }));

    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("NFS deep cold read failed: {}", e),
        Err(e) => std::panic::resume_unwind(e),
    }
}

/// NFS writable mount — uses advanced_writes (staging files) automatically.
#[tokio::test]
async fn test_nfs_writes() {
    let test_content = common::test_content();
    let remote_file = format!("test_{}.txt", std::process::id());
    let guard = match common::setup_bucket_with_file("nfs-w", &remote_file, test_content.as_bytes()).await {
        Some(g) => g,
        None => return,
    };

    let mount_point = format!("/tmp/hf-mount-nfs-w-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-nfs-w-cache-{}", std::process::id());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket_nfs(&guard.bucket_id, &mount_point, &cache_dir, &[]);
        let r = common::fs_tests::run_read_tests(&mount_point, &remote_file, &test_content)
            .and_then(|_| common::fs_tests::run_write_tests(&mount_point, &remote_file, &test_content));
        common::unmount_nfs(&mount_point, child, 30);
        r
    }));

    let trunc_size = test_content.find("BBBB_MIDDLE_BBBB").unwrap() as u64;
    let hub_check = common::fs_tests::verify_hub_state(&guard.hub, &remote_file, trunc_size).await;

    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    match result {
        Ok(Ok(())) => eprintln!("All NFS write tests passed!"),
        Ok(Err(e)) => panic!("NFS write test failed: {}", e),
        Err(e) => std::panic::resume_unwind(e),
    }
    if let Err(e) = hub_check {
        panic!("NFS write Hub verification failed: {}", e);
    }
}
