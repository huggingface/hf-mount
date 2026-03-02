use std::io::{Read, Seek, SeekFrom};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// Read-only tests: readdir, full read, range read, tail read, stat.
/// Works on any backend (FUSE or NFS) — only requires a mounted filesystem
/// with a known remote file.
pub fn run_read_tests(mp: &str, remote_file: &str, remote_content: &str) -> TestResult {
    let path = format!("{}/{}", mp, remote_file);

    // readdir
    eprintln!("  [read] readdir");
    let entries: Vec<String> = std::fs::read_dir(mp)?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    assert!(
        entries.contains(&remote_file.to_string()),
        "readdir should list remote file"
    );

    // full read
    eprintln!("  [read] full read");
    let content = std::fs::read_to_string(&path)?;
    assert_eq!(content, remote_content, "full read should match uploaded content");

    // range read: extract "BBBB_MIDDLE_BBBB" by finding its offset in the content
    eprintln!("  [read] range read");
    {
        let middle_offset = remote_content.find("BBBB_MIDDLE_BBBB").expect("test content missing middle marker") as u64;
        let mut f = std::fs::File::open(&path)?;
        f.seek(SeekFrom::Start(middle_offset))?;
        let mut buf = vec![0u8; 16];
        f.read_exact(&mut buf)?;
        assert_eq!(
            String::from_utf8(buf)?,
            "BBBB_MIDDLE_BBBB",
            "range read should return middle portion"
        );
    }

    // tail read: seek to "CCCC_FOOTER_CCCC" offset, read to EOF
    eprintln!("  [read] tail read");
    {
        let footer_offset = remote_content.find("CCCC_FOOTER_CCCC").expect("test content missing footer marker") as u64;
        let mut f = std::fs::File::open(&path)?;
        f.seek(SeekFrom::Start(footer_offset))?;
        let mut tail = String::new();
        f.read_to_string(&mut tail)?;
        assert!(tail.starts_with("CCCC_FOOTER_CCCC"), "tail read should start with footer");
        assert_eq!(tail.len(), remote_content.len() - footer_offset as usize, "tail read length mismatch");
    }

    // stat
    eprintln!("  [read] stat");
    let meta = std::fs::metadata(&path)?;
    assert_eq!(
        meta.len(),
        remote_content.len() as u64,
        "stat should report correct size"
    );

    eprintln!("  [read] all passed");
    Ok(())
}

/// Write tests: create, write, truncate, rename, mkdir, unlink, rmdir, overwrite.
/// Requires a writable mount with a known remote file for truncate/rename tests.
pub fn run_write_tests(mp: &str, remote_file: &str, remote_content: &str) -> TestResult {
    // 1. Create empty file + stat
    eprintln!("  [write] empty file");
    {
        let path = format!("{}/empty.txt", mp);
        std::fs::write(&path, "")?;
        let meta = std::fs::metadata(&path)?;
        assert_eq!(meta.len(), 0, "empty file should have size 0");
        let content = std::fs::read_to_string(&path)?;
        assert!(content.is_empty(), "empty file should read as empty");
    }

    // 2. Write + read back
    eprintln!("  [write] write + read back");
    {
        let path = format!("{}/data.txt", mp);
        std::fs::write(&path, "hello world")?;
        let content = std::fs::read_to_string(&path)?;
        assert_eq!(content, "hello world");
    }

    // 3. Truncate dirty file to 0
    eprintln!("  [write] truncate to 0");
    {
        let path = format!("{}/data.txt", mp);
        let f = std::fs::OpenOptions::new().write(true).open(&path)?;
        f.set_len(0)?;
        drop(f);
        let meta = std::fs::metadata(&path)?;
        assert_eq!(meta.len(), 0, "truncated file should have size 0");
        let content = std::fs::read_to_string(&path)?;
        assert!(content.is_empty(), "truncated file should read as empty");
    }

    // 4. Truncate remote file to non-zero
    // Truncate to just past "AAAA_HEADER_AAAA|" so we keep a recognizable prefix.
    eprintln!("  [write] truncate remote to non-zero");
    let trunc_len = remote_content.find("BBBB_MIDDLE_BBBB").expect("test content missing middle marker") as u64;
    {
        let path = format!("{}/{}", mp, remote_file);
        let f = std::fs::OpenOptions::new().write(true).open(&path)?;
        f.set_len(trunc_len)?;
        drop(f);
        let meta = std::fs::metadata(&path)?;
        assert_eq!(meta.len(), trunc_len, "truncated remote should have expected size");
        let content = std::fs::read_to_string(&path)?;
        assert_eq!(content, &remote_content[..trunc_len as usize], "truncated remote content mismatch");
    }

    // 5. Read past EOF
    eprintln!("  [write] read past EOF");
    {
        let path = format!("{}/small.txt", mp);
        std::fs::write(&path, "abc")?;
        let mut f = std::fs::File::open(&path)?;
        f.seek(SeekFrom::Start(100))?;
        let mut buf = vec![0u8; 10];
        let n = f.read(&mut buf)?;
        assert_eq!(n, 0, "read past EOF should return 0 bytes");
    }

    // 6. Rename dirty remote file
    // The remote file was made dirty by truncation in step 4.
    // Renaming should record pending_deletes so flush deletes the old Hub path.
    eprintln!("  [write] rename dirty remote file");
    {
        let old_path = format!("{}/{}", mp, remote_file);
        let new_path = format!("{}/moved_remote.txt", mp);
        std::fs::rename(&old_path, &new_path)?;
        let content = std::fs::read_to_string(&new_path)?;
        assert_eq!(content, &remote_content[..trunc_len as usize], "renamed file should still have truncated content");
        assert!(std::fs::metadata(&old_path).is_err(), "old path should not exist after rename");
    }

    // 7. Mkdir + nested file
    eprintln!("  [write] mkdir + nested file");
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
        assert!(entries.contains(&"child.txt".to_string()), "readdir should list child.txt");
    }

    // 8. Directory rename with children
    // update_subtree_paths must update children's full_path so flush
    // commits files at the new parent path.
    eprintln!("  [write] directory rename");
    {
        std::fs::rename(format!("{}/mydir", mp), format!("{}/renamed_dir", mp))?;
        let content = std::fs::read_to_string(format!("{}/renamed_dir/child.txt", mp))?;
        assert_eq!(content, "nested content", "child should be readable at new parent path");
        assert!(
            std::fs::metadata(format!("{}/mydir", mp)).is_err(),
            "old directory should not exist"
        );
    }

    // 9. Unlink + rmdir
    eprintln!("  [write] unlink + rmdir");
    {
        let dir = format!("{}/rmtest", mp);
        std::fs::create_dir(&dir)?;
        let file = format!("{}/rmtest/victim.txt", mp);
        std::fs::write(&file, "delete me")?;
        std::fs::remove_file(&file)?;
        assert!(std::fs::metadata(&file).is_err(), "unlinked file should not exist");
        std::fs::remove_dir(&dir)?;
        assert!(std::fs::metadata(&dir).is_err(), "removed dir should not exist");
    }

    // 10. rmdir non-empty fails
    eprintln!("  [write] rmdir non-empty");
    {
        let dir = format!("{}/nonempty", mp);
        std::fs::create_dir(&dir)?;
        std::fs::write(format!("{}/nonempty/file.txt", mp), "x")?;
        let result = std::fs::remove_dir(&dir);
        assert!(result.is_err(), "rmdir on non-empty dir should fail");
        std::fs::remove_file(format!("{}/nonempty/file.txt", mp))?;
        std::fs::remove_dir(&dir)?;
    }

    // 11. Overwrite existing file
    eprintln!("  [write] overwrite file");
    {
        let path = format!("{}/overwrite.txt", mp);
        std::fs::write(&path, "version 1")?;
        assert_eq!(std::fs::read_to_string(&path)?, "version 1");
        std::fs::write(&path, "version 2 is longer")?;
        assert_eq!(std::fs::read_to_string(&path)?, "version 2 is longer");
        let meta = std::fs::metadata(&path)?;
        assert_eq!(meta.len(), 19);
    }

    eprintln!("  [write] all passed");
    Ok(())
}

/// Verify Hub state after unmount: checks that flush committed files correctly.
/// Validates paths, sizes, and that deleted/renamed files are gone.
/// `trunc_size` must match the truncation length used in `run_write_tests`.
pub async fn verify_hub_state(
    hub: &hf_mount::hub_api::HubApiClient,
    remote_file: &str,
    trunc_size: u64,
) -> Result<(), String> {
    eprintln!("=== Verifying Hub state after flush ===");

    let entries = hub.list_tree("").await.map_err(|e| format!("list_tree: {e}"))?;
    let paths: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();
    eprintln!("  Hub files: {:?}", paths);

    let mut errors = Vec::new();

    // Expected files with sizes (from write test steps)
    let expected: &[(&str, u64)] = &[
        ("moved_remote.txt", trunc_size), // step 4: truncated, step 6: renamed
        ("renamed_dir/child.txt", 14), // step 7: "nested content", step 8: dir renamed
        ("empty.txt", 0),         // step 1
        ("data.txt", 0),          // step 2: wrote "hello world", step 3: truncated to 0
        ("small.txt", 3),         // step 5: "abc"
        ("overwrite.txt", 19),    // step 11: "version 2 is longer"
    ];

    for &(path, expected_size) in expected {
        match entries.iter().find(|e| e.path == path) {
            None => errors.push(format!("missing '{path}'")),
            Some(entry) => {
                if let Some(size) = entry.size {
                    if size != expected_size {
                        errors.push(format!("'{path}': expected size {expected_size}, got {size}"));
                    }
                }
            }
        }
    }

    // Files that should NOT exist
    let absent = [remote_file, "rmtest/victim.txt", "nonempty/file.txt"];
    for path in absent {
        if paths.contains(&path) {
            errors.push(format!("'{path}' should not exist"));
        }
    }
    // Old directory prefixes should be gone
    for prefix in ["mydir/", "rmtest/", "nonempty/"] {
        if paths.iter().any(|p| p.starts_with(prefix)) {
            errors.push(format!("still contains '{prefix}*' paths"));
        }
    }

    if errors.is_empty() {
        eprintln!("  Hub state verified");
        Ok(())
    } else {
        Err(format!("Hub state errors:\n  - {}", errors.join("\n  - ")))
    }
}
