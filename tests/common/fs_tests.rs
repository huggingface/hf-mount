use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

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
        let middle_offset = remote_content
            .find("BBBB_MIDDLE_BBBB")
            .expect("test content missing middle marker") as u64;
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
        let footer_offset = remote_content
            .find("CCCC_FOOTER_CCCC")
            .expect("test content missing footer marker") as u64;
        let mut f = std::fs::File::open(&path)?;
        f.seek(SeekFrom::Start(footer_offset))?;
        let mut tail = String::new();
        f.read_to_string(&mut tail)?;
        assert!(
            tail.starts_with("CCCC_FOOTER_CCCC"),
            "tail read should start with footer"
        );
        assert_eq!(
            tail.len(),
            remote_content.len() - footer_offset as usize,
            "tail read length mismatch"
        );
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

/// Write tests for --advanced-writes mode (staging files + async flush).
/// Supports truncate, overwrite, random writes.
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
    let trunc_len = remote_content
        .find("BBBB_MIDDLE_BBBB")
        .expect("test content missing middle marker") as u64;
    {
        let path = format!("{}/{}", mp, remote_file);
        let f = std::fs::OpenOptions::new().write(true).open(&path)?;
        f.set_len(trunc_len)?;
        drop(f);
        let meta = std::fs::metadata(&path)?;
        assert_eq!(meta.len(), trunc_len, "truncated remote should have expected size");
        let content = std::fs::read_to_string(&path)?;
        assert_eq!(
            content,
            &remote_content[..trunc_len as usize],
            "truncated remote content mismatch"
        );
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
        assert_eq!(
            content,
            &remote_content[..trunc_len as usize],
            "renamed file should still have truncated content"
        );
        assert!(
            std::fs::metadata(&old_path).is_err(),
            "old path should not exist after rename"
        );
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
        assert!(
            entries.contains(&"child.txt".to_string()),
            "readdir should list child.txt"
        );
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

    // 12. Flush idempotency: dup fd then close both
    eprintln!("  [write] flush idempotency (dup fd)");
    {
        use std::io::{Seek, SeekFrom, Write};
        use std::os::unix::io::AsRawFd;
        let path = format!("{}/duptest.txt", mp);
        let f = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)?;
        let fd2 = unsafe { libc::dup(f.as_raw_fd()) };
        assert!(fd2 >= 0, "dup() should succeed");
        let mut f = f;
        f.write_all(b"dup content")?;
        f.seek(SeekFrom::Start(0))?;
        let mut buf = String::new();
        f.read_to_string(&mut buf)?;
        assert_eq!(buf, "dup content");
        drop(f);
        // Second flush via dup'd fd — must not panic or return EIO
        unsafe { libc::close(fd2) };
        let content = std::fs::read_to_string(&path)?;
        assert_eq!(content, "dup content", "file should be readable after double flush");
    }

    // ── Rename edge cases ──

    // 13. Rename to same location (noop)
    eprintln!("  [write] rename noop (same location)");
    {
        let path = format!("{}/overwrite.txt", mp);
        std::fs::rename(&path, &path)?;
        assert_eq!(std::fs::read_to_string(&path)?, "version 2 is longer");
    }

    // 14. Rename non-existent file → ENOENT
    eprintln!("  [write] rename non-existent → ENOENT");
    {
        let result = std::fs::rename(format!("{}/does_not_exist.txt", mp), format!("{}/ghost.txt", mp));
        assert!(result.is_err(), "renaming non-existent file should fail");
    }

    // 15. Rename file over existing file (replace)
    eprintln!("  [write] rename file replaces existing");
    {
        let a = format!("{}/replace_src.txt", mp);
        let b = format!("{}/replace_dst.txt", mp);
        std::fs::write(&a, "source")?;
        std::fs::write(&b, "destination")?;
        std::fs::rename(&a, &b)?;
        assert_eq!(
            std::fs::read_to_string(&b)?,
            "source",
            "destination should have source content"
        );
        assert!(std::fs::metadata(&a).is_err(), "source should be gone");
    }

    // 16. Rename dir over non-empty dir → ENOTEMPTY
    eprintln!("  [write] rename dir over non-empty dir → ENOTEMPTY");
    {
        let d1 = format!("{}/ren_src_dir", mp);
        let d2 = format!("{}/ren_dst_dir", mp);
        std::fs::create_dir(&d1)?;
        std::fs::create_dir(&d2)?;
        std::fs::write(format!("{}/ren_dst_dir/blocker.txt", mp), "x")?;
        let result = std::fs::rename(&d1, &d2);
        assert!(result.is_err(), "rename dir over non-empty dir should fail");
        // cleanup
        std::fs::remove_file(format!("{}/ren_dst_dir/blocker.txt", mp))?;
        std::fs::remove_dir(&d2)?;
        std::fs::remove_dir(&d1)?;
    }

    // 17. Rename dir over empty dir (replace)
    eprintln!("  [write] rename dir over empty dir");
    {
        let d1 = format!("{}/ren_src2", mp);
        let d2 = format!("{}/ren_dst2", mp);
        std::fs::create_dir(&d1)?;
        std::fs::write(format!("{}/ren_src2/payload.txt", mp), "moved")?;
        std::fs::create_dir(&d2)?;
        std::fs::rename(&d1, &d2)?;
        assert_eq!(
            std::fs::read_to_string(format!("{}/ren_dst2/payload.txt", mp))?,
            "moved",
            "child should be accessible at new path"
        );
        assert!(std::fs::metadata(&d1).is_err(), "old dir should be gone");
        // cleanup
        std::fs::remove_file(format!("{}/ren_dst2/payload.txt", mp))?;
        std::fs::remove_dir(&d2)?;
    }

    // 18. Rename file into different directory
    eprintln!("  [write] rename file across directories");
    {
        let src_dir = format!("{}/xdir_src", mp);
        let dst_dir = format!("{}/xdir_dst", mp);
        std::fs::create_dir(&src_dir)?;
        std::fs::create_dir(&dst_dir)?;
        std::fs::write(format!("{}/xdir_src/file.txt", mp), "cross")?;
        std::fs::rename(format!("{}/xdir_src/file.txt", mp), format!("{}/xdir_dst/file.txt", mp))?;
        assert_eq!(std::fs::read_to_string(format!("{}/xdir_dst/file.txt", mp))?, "cross");
        assert!(
            std::fs::metadata(format!("{}/xdir_src/file.txt", mp)).is_err(),
            "old path gone"
        );
        // cleanup
        std::fs::remove_file(format!("{}/xdir_dst/file.txt", mp))?;
        std::fs::remove_dir(&dst_dir)?;
        std::fs::remove_dir(&src_dir)?;
    }

    eprintln!("  [write] all passed");
    Ok(())
}

/// Verify Hub state after unmount for --advanced-writes mode.
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
        ("renamed_dir/child.txt", 14),    // step 7: "nested content", step 8: dir renamed
        ("empty.txt", 0),                 // step 1
        ("data.txt", 0),                  // step 2: wrote "hello world", step 3: truncated to 0
        ("small.txt", 3),                 // step 5: "abc"
        ("overwrite.txt", 19),            // step 11: "version 2 is longer"
    ];

    for &(path, expected_size) in expected {
        match entries.iter().find(|e| e.path == path) {
            None => errors.push(format!("missing '{path}'")),
            Some(entry) => {
                if let Some(size) = entry.size
                    && size != expected_size
                {
                    errors.push(format!("'{path}': expected size {expected_size}, got {size}"));
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

/// Write tests for default (simple/streaming) mode.
/// Append-only, no truncate, no overwrite of existing files.
pub fn run_simple_write_tests(mp: &str, remote_file: &str) -> TestResult {
    // 1. Create empty file + stat
    eprintln!("  [simple-write] empty file");
    {
        let path = format!("{}/empty.txt", mp);
        std::fs::write(&path, "")?;
        let meta = std::fs::metadata(&path)?;
        assert_eq!(meta.len(), 0, "empty file should have size 0");
    }

    // 2. Write + read back (close triggers synchronous upload)
    eprintln!("  [simple-write] write + read back");
    {
        let path = format!("{}/data.txt", mp);
        std::fs::write(&path, "hello world")?;
        let content = std::fs::read_to_string(&path)?;
        assert_eq!(content, "hello world");
    }

    // 3. Mkdir + nested file
    eprintln!("  [simple-write] mkdir + nested file");
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
        assert!(entries.contains(&"child.txt".to_string()));
    }

    // 4. Directory rename with clean children
    // mydir/child.txt was flushed to Hub on close (step 3), so it's clean.
    // Renaming the parent must re-commit descendants at the new path.
    eprintln!("  [simple-write] directory rename");
    {
        std::fs::rename(format!("{}/mydir", mp), format!("{}/renamed_dir", mp))?;
        let content = std::fs::read_to_string(format!("{}/renamed_dir/child.txt", mp))?;
        assert_eq!(content, "nested content", "child should be readable at new parent path");
        assert!(
            std::fs::metadata(format!("{}/mydir", mp)).is_err(),
            "old directory should not exist"
        );
    }

    // 5. Rename clean remote file
    eprintln!("  [simple-write] rename remote file");
    {
        let old_path = format!("{}/{}", mp, remote_file);
        let new_path = format!("{}/moved_remote.txt", mp);
        std::fs::rename(&old_path, &new_path)?;
        assert!(std::fs::metadata(&new_path).is_ok(), "renamed file should exist");
        assert!(std::fs::metadata(&old_path).is_err(), "old path should not exist");
    }

    // 6. Unlink + rmdir
    eprintln!("  [simple-write] unlink + rmdir");
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

    // 7. rmdir non-empty fails
    eprintln!("  [simple-write] rmdir non-empty");
    {
        let dir = format!("{}/nonempty", mp);
        std::fs::create_dir(&dir)?;
        std::fs::write(format!("{}/nonempty/file.txt", mp), "x")?;
        let result = std::fs::remove_dir(&dir);
        assert!(result.is_err(), "rmdir on non-empty dir should fail");
        std::fs::remove_file(format!("{}/nonempty/file.txt", mp))?;
        std::fs::remove_dir(&dir)?;
    }

    // 8. Open existing for write should fail (EPERM)
    eprintln!("  [simple-write] open existing for write → EPERM");
    {
        let path = format!("{}/data.txt", mp);
        let result = std::fs::OpenOptions::new().write(true).open(&path);
        assert!(
            result.is_err(),
            "opening existing file for write should fail in simple mode"
        );
    }

    // 9. Truncate via ftruncate should fail (EPERM)
    eprintln!("  [simple-write] truncate → EPERM");
    {
        let path = format!("{}/data.txt", mp);
        // truncate(2) goes through setattr, which returns EPERM in simple mode
        let result = std::fs::OpenOptions::new().write(true).truncate(true).open(&path);
        assert!(result.is_err(), "truncate should fail in simple mode");
    }

    // 10. Non-sequential write → EINVAL (append-only enforcement)
    eprintln!("  [simple-write] non-sequential write → EINVAL");
    {
        use std::io::{Seek, SeekFrom, Write};
        let path = format!("{}/seektest.txt", mp);
        // Create via std::fs::File::create (FUSE create path)
        let mut f = std::fs::File::create(&path)?;
        f.write_all(b"hello")?;
        // Seek backwards and try to write → should fail
        f.seek(SeekFrom::Start(0))?;
        let result = f.write(b"X");
        assert!(result.is_err(), "non-sequential write should fail in simple mode");
        drop(f);
        // Clean up: file may be partially written, just remove it
        std::fs::remove_file(&path).ok();
    }

    // 11. Rename a just-written file (flush makes it clean, then rename)
    eprintln!("  [simple-write] rename just-written file");
    {
        let path = format!("{}/to_rename.txt", mp);
        std::fs::write(&path, "rename me")?;
        let new_path = format!("{}/renamed.txt", mp);
        std::fs::rename(&path, &new_path)?;
        let content = std::fs::read_to_string(&new_path)?;
        assert_eq!(content, "rename me");
        assert!(std::fs::metadata(&path).is_err(), "old path should not exist");
    }

    // 12. Unlink a file never opened for write (no staging file to clean up)
    eprintln!("  [simple-write] unlink clean remote file");
    {
        let path = format!("{}/moved_remote.txt", mp);
        assert!(std::fs::metadata(&path).is_ok(), "moved_remote.txt should exist");
        std::fs::remove_file(&path)?;
        assert!(std::fs::metadata(&path).is_err(), "file should be gone after unlink");
    }

    // 13. Read an empty file (size=0, no xet hash — served via lazy prefetch, not staging)
    eprintln!("  [simple-write] read empty file");
    {
        let path = format!("{}/empty.txt", mp);
        let content = std::fs::read(&path)?;
        assert!(content.is_empty(), "empty file should read as empty bytes");
    }

    // 14. Flush idempotency: dup fd then close both (FUSE calls flush per fd)
    eprintln!("  [simple-write] flush idempotency (dup fd)");
    {
        use std::io::Write;
        use std::os::unix::io::AsRawFd;
        let path = format!("{}/duptest.txt", mp);
        let f = std::fs::File::create(&path)?;
        // dup() the fd — kernel will call flush() on each close
        let fd2 = unsafe { libc::dup(f.as_raw_fd()) };
        assert!(fd2 >= 0, "dup() should succeed");
        // Write through original fd
        let mut f = f;
        f.write_all(b"dup content")?;
        // Close original fd (first flush)
        drop(f);
        // Close dup'd fd (second flush — should not panic or error)
        unsafe { libc::close(fd2) };
        // File should still be readable
        let content = std::fs::read_to_string(&path)?;
        assert_eq!(content, "dup content", "file should be readable after double flush");
    }

    // ── Rename edge cases ──

    // 15. Rename noop (same location)
    eprintln!("  [simple-write] rename noop");
    {
        let path = format!("{}/data.txt", mp);
        std::fs::rename(&path, &path)?;
        assert_eq!(std::fs::read_to_string(&path)?, "hello world");
    }

    // 16. Rename non-existent → ENOENT
    eprintln!("  [simple-write] rename non-existent → ENOENT");
    {
        let result = std::fs::rename(format!("{}/does_not_exist.txt", mp), format!("{}/ghost.txt", mp));
        assert!(result.is_err(), "renaming non-existent file should fail");
    }

    // 17. Rename file over existing file (replace)
    eprintln!("  [simple-write] rename file replaces existing");
    {
        let a = format!("{}/replace_src.txt", mp);
        let b = format!("{}/replace_dst.txt", mp);
        std::fs::write(&a, "source")?;
        std::fs::write(&b, "destination")?;
        std::fs::rename(&a, &b)?;
        assert_eq!(std::fs::read_to_string(&b)?, "source");
        assert!(std::fs::metadata(&a).is_err(), "source should be gone");
        // cleanup
        std::fs::remove_file(&b)?;
    }

    // 18. Rename dir over non-empty dir → ENOTEMPTY
    eprintln!("  [simple-write] rename dir over non-empty dir → ENOTEMPTY");
    {
        let d1 = format!("{}/ren_src_dir", mp);
        let d2 = format!("{}/ren_dst_dir", mp);
        std::fs::create_dir(&d1)?;
        std::fs::create_dir(&d2)?;
        std::fs::write(format!("{}/ren_dst_dir/blocker.txt", mp), "x")?;
        let result = std::fs::rename(&d1, &d2);
        assert!(result.is_err(), "rename dir over non-empty dir should fail");
        // cleanup
        std::fs::remove_file(format!("{}/ren_dst_dir/blocker.txt", mp))?;
        std::fs::remove_dir(&d2)?;
        std::fs::remove_dir(&d1)?;
    }

    // 19. Rename file across directories
    eprintln!("  [simple-write] rename file across directories");
    {
        let src_dir = format!("{}/xdir_src", mp);
        let dst_dir = format!("{}/xdir_dst", mp);
        std::fs::create_dir(&src_dir)?;
        std::fs::create_dir(&dst_dir)?;
        std::fs::write(format!("{}/xdir_src/file.txt", mp), "cross")?;
        std::fs::rename(format!("{}/xdir_src/file.txt", mp), format!("{}/xdir_dst/file.txt", mp))?;
        assert_eq!(std::fs::read_to_string(format!("{}/xdir_dst/file.txt", mp))?, "cross");
        assert!(std::fs::metadata(format!("{}/xdir_src/file.txt", mp)).is_err());
        // cleanup
        std::fs::remove_file(format!("{}/xdir_dst/file.txt", mp))?;
        std::fs::remove_dir(&dst_dir)?;
        std::fs::remove_dir(&src_dir)?;
    }

    eprintln!("  [simple-write] all passed");
    Ok(())
}

/// Verify Hub state after unmount for default (simple) mode.
pub async fn verify_simple_hub_state(
    hub: &hf_mount::hub_api::HubApiClient,
    remote_file: &str,
    _remote_content_len: u64,
) -> Result<(), String> {
    eprintln!("=== Verifying Hub state (simple mode) ===");

    let entries = hub.list_tree("").await.map_err(|e| format!("list_tree: {e}"))?;
    let paths: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();
    eprintln!("  Hub files: {:?}", paths);

    let mut errors = Vec::new();

    let expected: &[(&str, u64)] = &[
        ("empty.txt", 0),              // step 1
        ("data.txt", 11),              // step 2: "hello world"
        ("renamed_dir/child.txt", 14), // step 3: created, step 4: dir renamed
        ("renamed.txt", 9),            // step 11: "rename me"
        ("duptest.txt", 11),           // step 14: "dup content"
    ];

    for &(path, expected_size) in expected {
        match entries.iter().find(|e| e.path == path) {
            None => errors.push(format!("missing '{path}'")),
            Some(entry) => {
                if let Some(size) = entry.size
                    && size != expected_size
                {
                    errors.push(format!("'{path}': expected size {expected_size}, got {size}"));
                }
            }
        }
    }

    // Files that should NOT exist
    let absent = [
        remote_file,
        "moved_remote.txt", // step 12: unlinked
        "to_rename.txt",
        "rmtest/victim.txt",
        "nonempty/file.txt",
    ];
    for path in absent {
        if paths.contains(&path) {
            errors.push(format!("'{path}' should not exist"));
        }
    }
    // Old directory prefix should be gone (renamed mydir → renamed_dir)
    if paths.iter().any(|p| p.starts_with("mydir/")) {
        errors.push("still contains 'mydir/*' paths".to_string());
    }

    if errors.is_empty() {
        eprintln!("  Hub state verified");
        Ok(())
    } else {
        Err(format!("Hub state errors:\n  - {}", errors.join("\n  - ")))
    }
}

/// HEAD revalidation test: verifies that lookup() detects remote file changes
/// via HEAD and invalidates the kernel page cache so re-reads return fresh content.
///
/// Flow:
/// 1. Read the file (kernel caches pages via FOPEN_KEEP_CACHE)
/// 2. Upload new content remotely (bypassing the mount)
/// 3. Wait for metadata TTL to expire
/// 4. Re-read the file → should see new content (HEAD detects hash change)
pub async fn run_revalidation_test(
    mp: &str,
    remote_file: &str,
    original_content: &str,
    hub: &Arc<hf_mount::hub_api::HubApiClient>,
    metadata_ttl_ms: u64,
) -> TestResult {
    let path = format!("{}/{}", mp, remote_file);

    // 1. Read original content (populates kernel page cache)
    eprintln!("  [revalidation] initial read");
    let content = std::fs::read_to_string(&path)?;
    assert_eq!(content, original_content, "initial read should match uploaded content");

    // 2. Upload new content remotely (bypass the mount)
    eprintln!("  [revalidation] uploading new content remotely");
    let new_content = "REVALIDATION_TEST_NEW_CONTENT_12345";
    let write_config = super::build_write_config(hub).await;

    let tmp_dir = std::env::temp_dir().join("hf-mount-reval-test");
    std::fs::create_dir_all(&tmp_dir)?;
    let staging_path = tmp_dir.join(remote_file);
    std::fs::write(&staging_path, new_content)?;

    let file_info = super::upload_file(write_config, &staging_path).await;
    let xet_hash = file_info.hash().to_string();
    eprintln!(
        "  [revalidation] new xet_hash={}, size={}",
        xet_hash,
        file_info.file_size()
    );

    let mtime_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    hub.batch_operations(&[hf_mount::hub_api::BatchOp::AddFile {
        path: remote_file.to_string(),
        xet_hash,
        mtime: mtime_ms,
        content_type: None,
    }])
    .await?;

    std::fs::remove_dir_all(&tmp_dir).ok();

    // 3. Wait for metadata TTL to expire so next lookup triggers HEAD
    eprintln!("  [revalidation] waiting {}ms for TTL expiry", metadata_ttl_ms + 100);
    std::thread::sleep(std::time::Duration::from_millis(metadata_ttl_ms + 100));

    // 4. Re-read — lookup() should HEAD, detect hash change, invalidate page cache
    eprintln!("  [revalidation] re-reading file (expecting new content)");
    let content = std::fs::read_to_string(&path)?;
    assert_eq!(
        content, new_content,
        "re-read after remote update should return new content (HEAD revalidation)"
    );

    eprintln!("  [revalidation] passed");
    Ok(())
}
