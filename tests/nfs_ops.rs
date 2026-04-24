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
/// its siblings. End-to-end smoke test for the HEAD-based point lookup.
#[tokio::test]
async fn test_nfs_point_lookup_in_large_dir() {
    let guard = match common::setup_bucket("nfs-point-lookup").await {
        Some(g) => g,
        None => return,
    };

    let write_config = common::build_write_config(&guard.hub).await;
    let tmp_dir = std::env::temp_dir().join(format!("hf-mount-nfs-pl-{}", std::process::id()));
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

    let mount_point = format!("/tmp/hf-mount-nfs-pl-mnt-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-nfs-pl-cache-{}", std::process::id());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<(), String> {
        let child = common::mount_bucket_nfs(&guard.bucket_id, &mount_point, &cache_dir, &["--read-only"]);
        // Read the target directly — no `ls`, no readdir on big_dir.
        let content = std::fs::read(format!("{}/big_dir/target.txt", mount_point))
            .map_err(|e| format!("read target: {e}"))?;
        if content != target_content {
            return Err(format!("content mismatch: got {} bytes", content.len()));
        }
        common::unmount_nfs(&mount_point, child, 10);
        Ok(())
    }));

    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    match result {
        Ok(Ok(())) => eprintln!("NFS point-lookup test passed"),
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
    let write_config = common::build_write_config(&guard.hub).await;
    let tmp_dir = std::env::temp_dir().join(format!("hf-mount-nfs-dr-{}", std::process::id()));
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

    let mount_point = format!("/tmp/hf-mount-nfs-dr-mnt-{}", std::process::id());
    let cache_dir = format!("/tmp/hf-mount-nfs-dr-cache-{}", std::process::id());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<(), String> {
        let child = common::mount_bucket_nfs(&guard.bucket_id, &mount_point, &cache_dir, &["--read-only"]);
        let content = std::fs::read(format!("{}/a/b/c/d/payload.txt", mount_point))
            .map_err(|e| format!("read deep: {e}"))?;
        if content != payload {
            return Err("deep read content mismatch".to_string());
        }
        // readdir on an intermediate should still work via the list_tree fallback.
        let entries: Vec<_> = std::fs::read_dir(format!("{}/a/b", mount_point))
            .map_err(|e| format!("readdir a/b: {e}"))?
            .filter_map(|e| e.ok().and_then(|e| e.file_name().into_string().ok()))
            .collect();
        if !entries.contains(&"c".to_string()) {
            return Err(format!("readdir a/b should list 'c', got {:?}", entries));
        }
        common::unmount_nfs(&mount_point, child, 10);
        Ok(())
    }));

    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    match result {
        Ok(Ok(())) => eprintln!("NFS deep cold read test passed"),
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
