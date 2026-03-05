mod common;

/// Test mounting a real HuggingFace model repo (openai-community/gpt2) and reading
/// both xet-backed and plain git/LFS files.
#[tokio::test]
async fn test_repo_fuse_read() {
    let pid = std::process::id();
    let mount_point = format!("/tmp/hf-mount-repo-fuse-{}", pid);
    let cache_dir = format!("/tmp/hf-mount-repo-fuse-cache-{}", pid);

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_repo("openai-community/gpt2", &mount_point, &cache_dir, &[]);

        let r = run_repo_read_tests(&mount_point);

        common::unmount(&mount_point, child, 10);
        r
    }));

    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    match result {
        Ok(Ok(())) => eprintln!("All repo read tests passed!"),
        Ok(Err(e)) => panic!("Repo read test failed: {}", e),
        Err(e) => std::panic::resume_unwind(e),
    }
}

fn run_repo_read_tests(mp: &str) -> Result<(), Box<dyn std::error::Error>> {
    // 1. readdir — root should contain known gpt2 files
    eprintln!("  [repo] readdir");
    let entries: Vec<String> = std::fs::read_dir(mp)?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    eprintln!("  [repo] root entries: {:?}", entries);
    assert!(
        entries.iter().any(|e| e == "config.json"),
        "config.json should exist in gpt2"
    );
    assert!(
        entries.iter().any(|e| e == "README.md"),
        "README.md should exist in gpt2"
    );

    // 2. Read a small non-xet file (config.json — plain git blob)
    eprintln!("  [repo] read config.json (non-xet)");
    let config = std::fs::read_to_string(format!("{}/config.json", mp))?;
    assert!(
        config.contains("\"model_type\""),
        "config.json should contain model_type"
    );
    assert!(config.contains("gpt2"), "config.json should reference gpt2");
    eprintln!("  [repo] config.json: {} bytes", config.len());

    // 3. Read README.md (non-xet)
    eprintln!("  [repo] read README.md (non-xet)");
    let readme = std::fs::read_to_string(format!("{}/README.md", mp))?;
    assert!(!readme.is_empty(), "README.md should not be empty");
    eprintln!("  [repo] README.md: {} bytes", readme.len());

    // 4. Stat a large file (model.safetensors — likely xet-backed)
    eprintln!("  [repo] stat model.safetensors");
    let meta = std::fs::metadata(format!("{}/model.safetensors", mp))?;
    eprintln!("  [repo] model.safetensors: {} bytes", meta.len());
    assert!(meta.len() > 100_000_000, "model.safetensors should be >100MB");

    // 5. Range read from model.safetensors (first 4KB)
    eprintln!("  [repo] range read model.safetensors (first 4KB)");
    {
        use std::io::Read;
        let mut f = std::fs::File::open(format!("{}/model.safetensors", mp))?;
        let mut buf = vec![0u8; 4096];
        let n = f.read(&mut buf)?;
        assert_eq!(n, 4096, "should read 4096 bytes");
        // safetensors files start with a header length (little-endian u64)
        assert!(buf[..8] != [0u8; 8], "first 8 bytes should be non-zero (header length)");
        eprintln!("  [repo] range read OK");
    }

    // 6. Verify read-only: writes should fail
    eprintln!("  [repo] verify read-only");
    let write_result = std::fs::write(format!("{}/should_fail.txt", mp), "nope");
    assert!(write_result.is_err(), "writes should fail on repo mount");
    eprintln!("  [repo] write correctly rejected");

    eprintln!("  [repo] all passed");
    Ok(())
}
