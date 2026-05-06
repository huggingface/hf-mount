use std::time::Duration;

use super::inode::ROOT_INODE;
use super::*;
use crate::hub_api::HeadFileInfo;
use crate::test_mocks::{MockHub, MockXet, TestOpts, make_overlay_test_vfs_with_root, make_test_vfs};

/// Create a fresh overlay temp dir, removing any stale contents from previous runs.
fn fresh_overlay_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("hf_overlay_{}_{}", name, std::process::id()));
    if dir.exists() {
        std::fs::remove_dir_all(&dir).expect("failed to clean stale overlay dir");
    }
    std::fs::create_dir_all(&dir).expect("failed to create overlay dir");
    dir
}

fn new_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// Call VirtualFs::write from a blocking thread (required because write() uses
/// blocking_send() which panics if called from within the tokio runtime).
async fn write_blocking(
    vfs: &std::sync::Arc<VirtualFs>,
    ino: u64,
    fh: u64,
    offset: u64,
    data: &[u8],
) -> VirtualFsResult<u32> {
    let vfs = vfs.clone();
    let data = data.to_vec();
    tokio::task::spawn_blocking(move || vfs.write(ino, fh, offset, &data))
        .await
        .unwrap()
}

/// Build a VFS with default settings (simple write mode, bucket source).
fn vfs_simple(
    hub: &std::sync::Arc<MockHub>,
    xet: &std::sync::Arc<MockXet>,
) -> (tokio::runtime::Runtime, std::sync::Arc<VirtualFs>) {
    let rt = new_runtime();
    let vfs = make_test_vfs(hub.clone(), xet.clone(), TestOpts::default(), &rt);
    (rt, vfs)
}

/// Build a VFS with advanced writes enabled.
fn vfs_advanced(
    hub: &std::sync::Arc<MockHub>,
    xet: &std::sync::Arc<MockXet>,
) -> (tokio::runtime::Runtime, std::sync::Arc<VirtualFs>) {
    let rt = new_runtime();
    let vfs = make_test_vfs(
        hub.clone(),
        xet.clone(),
        TestOpts {
            advanced_writes: true,
            ..Default::default()
        },
        &rt,
    );
    (rt, vfs)
}

/// Build a read-only VFS.
fn vfs_readonly(
    hub: &std::sync::Arc<MockHub>,
    xet: &std::sync::Arc<MockXet>,
) -> (tokio::runtime::Runtime, std::sync::Arc<VirtualFs>) {
    let rt = new_runtime();
    let vfs = make_test_vfs(
        hub.clone(),
        xet.clone(),
        TestOpts {
            read_only: true,
            ..Default::default()
        },
        &rt,
    );
    (rt, vfs)
}

/// Build a VFS with the LRU evictor enabled at `soft_limit`.
fn vfs_with_lru(
    hub: &std::sync::Arc<MockHub>,
    xet: &std::sync::Arc<MockXet>,
    soft_limit: usize,
) -> (tokio::runtime::Runtime, std::sync::Arc<VirtualFs>) {
    let rt = new_runtime();
    let vfs = make_test_vfs(
        hub.clone(),
        xet.clone(),
        TestOpts {
            inode_soft_limit: soft_limit,
            ..Default::default()
        },
        &rt,
    );
    (rt, vfs)
}

// ── Commit lifecycle ────────────────────────────────────────────────

/// Open(O_TRUNC) -> write -> flush -> release commits to Hub and clears dirty flag.
#[test]
fn streaming_write_happy_path() {
    let hub = MockHub::new();
    hub.add_file("hello.txt", 100, Some("old_hash"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "hello.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, true, Some(42)).await.unwrap();

        let written = write_blocking(&vfs, ino, fh, 0, b"hello world").await.unwrap();
        assert_eq!(written, 11);

        vfs.flush(ino, fh, Some(42)).await.unwrap();
        vfs.release(fh).await.unwrap();

        let inodes = vfs.inode_table.read().unwrap();
        let entry = inodes.get(ino).unwrap();
        assert!(!entry.is_dirty());
        assert!(entry.xet_hash.is_some());
    });

    let logs = hub.take_batch_log();
    assert_eq!(logs.len(), 1);
}

/// In streaming mode, unlink is blocked while the file has open handles.
/// This prevents editors (vim) from deleting files they can't rewrite.
#[test]
fn unlink_blocked_with_open_handles_streaming() {
    let hub = MockHub::new();
    hub.add_file("guarded.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "guarded.txt").await.unwrap();
        let ino = attr.ino;

        // Open read-only (like vim does before trying to save)
        let fh = vfs.open(ino, false, false, Some(42)).await.unwrap();

        // Unlink should be blocked (has open handles in streaming mode)
        let err = vfs.unlink(ROOT_INODE, "guarded.txt").await.unwrap_err();
        assert_eq!(err, libc::EPERM, "unlink must be blocked with open handles");

        // Close the handle
        vfs.release(fh).await.unwrap();

        // Now unlink should succeed
        vfs.unlink(ROOT_INODE, "guarded.txt").await.unwrap();
    });
}

/// flush() from a different PID (dup'd fd) defers commit; release() commits.
#[test]
fn flush_deferred_pid_mismatch() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, true, Some(100)).await.unwrap();
        write_blocking(&vfs, ino, fh, 0, b"data").await.unwrap();

        // Flush from different PID -> deferred
        vfs.flush(ino, fh, Some(200)).await.unwrap();
        assert!(hub.take_batch_log().is_empty());

        // Release commits
        vfs.release(fh).await.unwrap();
        let logs = hub.take_batch_log();
        assert_eq!(logs.len(), 1);
    });
}

/// flush() with zero bytes written defers commit; release() commits the empty file.
#[test]
fn flush_deferred_zero_bytes() {
    let hub = MockHub::new();
    hub.add_file("empty.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "empty.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, true, Some(42)).await.unwrap();

        vfs.flush(ino, fh, Some(42)).await.unwrap();
        assert!(hub.take_batch_log().is_empty());

        vfs.release(fh).await.unwrap();
        let logs = hub.take_batch_log();
        assert_eq!(logs.len(), 1);
    });
}

/// CAS streaming writer failure after N bytes surfaces as EIO on next write().
#[test]
fn worker_error_propagates_eio() {
    let hub = MockHub::new();
    hub.add_file("fail.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    xet.fail_writer_after(5);
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "fail.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, true, Some(42)).await.unwrap();

        write_blocking(&vfs, ino, fh, 0, b"hi").await.unwrap();
        write_blocking(&vfs, ino, fh, 2, b"this is too long").await.unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;

        let result = write_blocking(&vfs, ino, fh, 18, b"more").await;
        assert_eq!(result.unwrap_err(), libc::EIO);

        // release returns EIO because the streaming worker died
        assert_eq!(vfs.release(fh).await.unwrap_err(), libc::EIO);
    });
}

/// Hub batch_operations fails once -> flush returns EIO, release retries successfully.
#[test]
fn hub_commit_fail_retry_in_release() {
    let hub = MockHub::new();
    hub.add_file("retry.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "retry.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, true, Some(42)).await.unwrap();
        write_blocking(&vfs, ino, fh, 0, b"data").await.unwrap();

        hub.fail_next_batch(1);
        let result = vfs.flush(ino, fh, Some(42)).await;
        assert_eq!(result.unwrap_err(), libc::EIO);

        // release() retries and succeeds
        vfs.release(fh).await.unwrap();

        let logs = hub.take_batch_log();
        assert_eq!(logs.len(), 1);

        let inodes = vfs.inode_table.read().unwrap();
        let entry = inodes.get(ino).unwrap();
        assert!(!entry.is_dirty());
        assert!(entry.xet_hash.is_some());
    });
}

/// Permanent commit failure on a newly created file removes the inode entirely.
#[test]
fn revert_inode_new_file_removed() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let (attr, fh) = vfs
            .create(ROOT_INODE, "new_file.txt", 0o644, 1000, 1000, Some(42))
            .await
            .unwrap();
        let ino = attr.ino;
        write_blocking(&vfs, ino, fh, 0, b"data").await.unwrap();

        // flush(1) + release attempt(1) + release retry(1) = 3 failures
        hub.fail_next_batch(3);
        let _ = vfs.flush(ino, fh, Some(42)).await;
        // release returns EIO because all commit attempts failed
        assert_eq!(vfs.release(fh).await.unwrap_err(), libc::EIO);

        let inodes = vfs.inode_table.read().unwrap();
        assert!(inodes.get(ino).is_none());
    });
}

/// Permanent commit failure on an overwritten file restores original hash/size.
#[test]
fn revert_inode_overwrite_restored() {
    let hub = MockHub::new();
    hub.add_file("exist.txt", 100, Some("original_hash"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "exist.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, true, Some(42)).await.unwrap();
        write_blocking(&vfs, ino, fh, 0, b"overwrite").await.unwrap();

        hub.fail_next_batch(3);
        let _ = vfs.flush(ino, fh, Some(42)).await;
        // release returns EIO because all commit attempts failed
        assert_eq!(vfs.release(fh).await.unwrap_err(), libc::EIO);

        let inodes = vfs.inode_table.read().unwrap();
        let entry = inodes.get(ino).unwrap();
        assert!(!entry.is_dirty());
        assert_eq!(entry.xet_hash.as_deref(), Some("original_hash"));
        assert_eq!(entry.size, 100);
    });
}

/// open(O_TRUNC) on a file with a deferred commit awaits the commit via hook.
#[test]
fn commit_hook_notifies_waiting_open() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        let fh1 = vfs.open(ino, true, true, Some(100)).await.unwrap();
        write_blocking(&vfs, ino, fh1, 0, b"data").await.unwrap();
        vfs.flush(ino, fh1, Some(200)).await.unwrap(); // deferred (pid mismatch)

        let vfs2 = vfs.clone();
        let open_task = tokio::spawn(async move { vfs2.open(ino, true, true, Some(300)).await });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!open_task.is_finished(), "open should be blocked waiting for commit");

        // Release fulfills the commit hook, unblocking the second open
        vfs.release(fh1).await.unwrap();

        let fh2 = open_task.await.unwrap().unwrap();
        vfs.release(fh2).await.unwrap();
    });
}

/// Second flush() after a successful commit is a no-op (idempotent).
#[test]
fn flush_idempotent_on_committed() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, true, Some(42)).await.unwrap();
        write_blocking(&vfs, ino, fh, 0, b"data").await.unwrap();

        vfs.flush(ino, fh, Some(42)).await.unwrap();
        vfs.flush(ino, fh, Some(42)).await.unwrap(); // idempotent

        let logs = hub.take_batch_log();
        assert_eq!(logs.len(), 1); // only one batch

        vfs.release(fh).await.unwrap();
    });
}

/// release() without prior flush() still commits the streaming write.
#[test]
fn release_commits_without_flush() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, true, Some(42)).await.unwrap();
        write_blocking(&vfs, ino, fh, 0, b"data").await.unwrap();

        vfs.release(fh).await.unwrap();

        let logs = hub.take_batch_log();
        assert_eq!(logs.len(), 1);

        let inodes = vfs.inode_table.read().unwrap();
        let entry = inodes.get(ino).unwrap();
        assert!(!entry.is_dirty());
    });
}

// ── Rename / Unlink / Rmdir ─────────────────────────────────────────

/// Rename to the same path is a no-op (no remote batch ops).
#[test]
fn rename_noop_same_path() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let _ = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        vfs.rename(ROOT_INODE, "file.txt", ROOT_INODE, "file.txt", false)
            .await
            .unwrap();
        assert!(hub.take_batch_log().is_empty());
    });
}

/// RENAME_NOREPLACE with an existing destination returns EEXIST.
#[test]
fn rename_noreplace_eexist() {
    let hub = MockHub::new();
    hub.add_file("a.txt", 10, Some("h1"), None);
    hub.add_file("b.txt", 20, Some("h2"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let _ = vfs.lookup(ROOT_INODE, "a.txt").await.unwrap();
        let _ = vfs.lookup(ROOT_INODE, "b.txt").await.unwrap();
        let result = vfs.rename(ROOT_INODE, "a.txt", ROOT_INODE, "b.txt", true).await;
        assert_eq!(result.unwrap_err(), libc::EEXIST);
    });
}

/// Moving a directory into its own subtree returns EINVAL (cycle detection).
#[test]
fn rename_cycle_detection() {
    let hub = MockHub::new();
    hub.add_dir("parent");
    hub.add_dir("parent/child");
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let parent_attr = vfs.lookup(ROOT_INODE, "parent").await.unwrap();
        let child_attr = vfs.lookup(parent_attr.ino, "child").await.unwrap();

        let result = vfs.rename(ROOT_INODE, "parent", child_attr.ino, "parent", false).await;
        assert_eq!(result.unwrap_err(), libc::EINVAL);
    });
}

/// Renaming a dirty file (advanced mode) records old path in pending_deletes
/// instead of sending immediate remote batch ops.
#[test]
fn rename_dirty_file_pending_deletes() {
    let hub = MockHub::new();
    hub.add_file("old.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    xet.add_file("hash1", b"original content for download");
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "old.txt").await.unwrap();
        let ino = attr.ino;

        let fh = vfs.open(ino, true, false, Some(42)).await.unwrap();
        write_blocking(&vfs, ino, fh, 0, b"modified").await.unwrap();

        vfs.rename(ROOT_INODE, "old.txt", ROOT_INODE, "new.txt", false)
            .await
            .unwrap();

        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            assert!(entry.pending_deletes.contains(&"old.txt".to_string()));
            assert_eq!(entry.full_path.as_ref(), "new.txt");
        }

        assert!(hub.take_batch_log().is_empty());

        vfs.release(fh).await.unwrap();
    });
}

/// Renaming a directory sends batch add+delete for each clean descendant file.
#[test]
fn rename_dir_clean_descendants_batch() {
    let hub = MockHub::new();
    hub.add_dir("src");
    hub.add_file("src/a.txt", 10, Some("ha"), None);
    hub.add_file("src/b.txt", 20, Some("hb"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let _ = vfs.lookup(ROOT_INODE, "src").await.unwrap();
        let src_attr = vfs.lookup(ROOT_INODE, "src").await.unwrap();
        let _ = vfs.lookup(src_attr.ino, "a.txt").await.unwrap();
        let _ = vfs.lookup(src_attr.ino, "b.txt").await.unwrap();

        vfs.rename(ROOT_INODE, "src", ROOT_INODE, "dst", false).await.unwrap();

        let logs = hub.take_batch_log();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].len(), 4); // 2 adds + 2 deletes

        let inodes = vfs.inode_table.read().unwrap();
        assert!(inodes.get_by_path("dst/a.txt").is_some());
        assert!(inodes.get_by_path("dst/b.txt").is_some());
        assert!(inodes.get_by_path("src/a.txt").is_none());
    });
}

/// Rename of a clean file sends add+delete batch ops and updates inode path.
#[test]
fn rename_clean_file() {
    let hub = MockHub::new();
    hub.add_file("src.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "src.txt").await.unwrap();
        let ino = attr.ino;

        vfs.rename(ROOT_INODE, "src.txt", ROOT_INODE, "dst.txt", false)
            .await
            .unwrap();

        let logs = hub.take_batch_log();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].len(), 2);

        let inodes = vfs.inode_table.read().unwrap();
        let entry = inodes.get(ino).unwrap();
        assert_eq!(entry.full_path.as_ref(), "dst.txt");
        assert_eq!(&*entry.name, "dst.txt");
    });
}

/// Rename without NOREPLACE replaces the destination file, removing its inode.
#[test]
fn rename_replaces_destination() {
    let hub = MockHub::new();
    hub.add_file("src.txt", 10, Some("h_src"), None);
    hub.add_file("dst.txt", 20, Some("h_dst"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let src = vfs.lookup(ROOT_INODE, "src.txt").await.unwrap();
        let dst = vfs.lookup(ROOT_INODE, "dst.txt").await.unwrap();
        let src_ino = src.ino;
        let dst_ino = dst.ino;

        vfs.rename(ROOT_INODE, "src.txt", ROOT_INODE, "dst.txt", false)
            .await
            .unwrap();

        // Inode-level check
        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(src_ino).unwrap();
            assert_eq!(entry.full_path.as_ref(), "dst.txt");
            assert!(inodes.get(dst_ino).is_none());
        }

        // Lookup-level check: src gone, dst resolves to src_ino
        assert_eq!(vfs.lookup(ROOT_INODE, "src.txt").await.unwrap_err(), libc::ENOENT);
        let dst_attr = vfs.lookup(ROOT_INODE, "dst.txt").await.unwrap();
        assert_eq!(dst_attr.ino, src_ino);
    });
}

/// Hub batch failure on unlink returns EIO but leaves the local inode intact.
#[test]
fn unlink_remote_fail_eio_local_untouched() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        hub.fail_next_batch(1);
        let result = vfs.unlink(ROOT_INODE, "file.txt").await;
        assert_eq!(result.unwrap_err(), libc::EIO);

        let inodes = vfs.inode_table.read().unwrap();
        assert!(inodes.get(ino).is_some());
    });
}

/// Unlink sends remote delete synchronously in simple mode.
#[test]
fn unlink_sends_remote_delete() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        vfs.unlink(ROOT_INODE, "file.txt").await.unwrap();

        // Inode is gone locally.
        let inodes = vfs.inode_table.read().unwrap();
        assert!(inodes.get(ino).is_none());
        drop(inodes);

        let logs = hub.take_batch_log();
        assert!(!logs.is_empty(), "delete should have been sent to remote");
    });
}

/// Unlink a locally-created file (committed after release) removes inode + sends delete.
#[test]
fn unlink_locally_created_file() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let (attr, fh) = vfs
            .create(ROOT_INODE, "local.txt", 0o644, 1000, 1000, Some(42))
            .await
            .unwrap();
        let ino = attr.ino;
        write_blocking(&vfs, ino, fh, 0, b"data").await.unwrap();
        vfs.release(fh).await.unwrap();

        hub.take_batch_log(); // clear commit log

        vfs.unlink(ROOT_INODE, "local.txt").await.unwrap();

        let inodes = vfs.inode_table.read().unwrap();
        assert!(inodes.get(ino).is_none());
        drop(inodes);

        // Verify a delete batch op was sent to Hub
        let logs = hub.take_batch_log();
        assert!(!logs.is_empty(), "unlink should send batch delete");
        let has_delete = logs
            .iter()
            .flatten()
            .any(|op| matches!(op, BatchOp::DeleteFile { path } if path == "local.txt"));
        assert!(has_delete, "expected DeleteFile for local.txt");
    });
}

/// rmdir on a directory with unloaded remote children loads them and returns ENOTEMPTY.
#[test]
fn rmdir_enotempty_lazy_children() {
    let hub = MockHub::new();
    hub.add_dir("mydir");
    hub.add_file("mydir/file.txt", 10, Some("h"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let _ = vfs.lookup(ROOT_INODE, "mydir").await.unwrap();
        let result = vfs.rmdir(ROOT_INODE, "mydir").await;
        assert_eq!(result.unwrap_err(), libc::ENOTEMPTY);
    });
}

/// rmdir on a locally-created empty directory succeeds and removes the inode.
#[test]
fn rmdir_empty_succeeds() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.mkdir(ROOT_INODE, "empty_dir", 0o755, 1000, 1000).await.unwrap();
        let ino = attr.ino;
        vfs.rmdir(ROOT_INODE, "empty_dir").await.unwrap();

        let inodes = vfs.inode_table.read().unwrap();
        assert!(inodes.get(ino).is_none());
    });
}

/// create() rolls back the inode if the streaming writer fails to initialize.
#[test]
fn create_rollback_streaming_writer_fail() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    xet.fail_next_writer_create();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let result = vfs.create(ROOT_INODE, "fail.txt", 0o644, 1000, 1000, Some(42)).await;
        assert_eq!(result.unwrap_err(), libc::EIO);

        let inodes = vfs.inode_table.read().unwrap();
        assert!(inodes.lookup_child(ROOT_INODE, "fail.txt").is_none());
    });
}

/// create() on an existing file returns EEXIST.
#[test]
fn create_eexist_duplicate() {
    let hub = MockHub::new();
    hub.add_file("exist.txt", 10, Some("h"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let _ = vfs.lookup(ROOT_INODE, "exist.txt").await.unwrap();
        let result = vfs.create(ROOT_INODE, "exist.txt", 0o644, 1000, 1000, Some(42)).await;
        assert_eq!(result.unwrap_err(), libc::EEXIST);
    });
}

/// Two concurrent create() calls for the same name: first succeeds, second gets EEXIST.
#[test]
fn concurrent_create_eexist() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let (_, fh1) = vfs
            .create(ROOT_INODE, "race.txt", 0o644, 1000, 1000, Some(42))
            .await
            .unwrap();
        let result = vfs.create(ROOT_INODE, "race.txt", 0o644, 1000, 1000, Some(43)).await;
        assert_eq!(result.unwrap_err(), libc::EEXIST);
        vfs.release(fh1).await.unwrap();
    });
}

// ── OS junk file filtering ──────────────────────────────────────────

/// create/mkdir/rename reject OS junk file names with EACCES.
#[test]
fn os_junk_files_rejected() {
    let hub = MockHub::new();
    hub.add_file("legit.txt", 10, Some("h"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        for name in [".DS_Store", "._metadata", "Thumbs.db", "desktop.ini", "__MACOSX"] {
            assert_eq!(
                vfs.create(ROOT_INODE, name, 0o644, 1000, 1000, Some(1))
                    .await
                    .unwrap_err(),
                libc::EACCES,
                "create({name})"
            );
            assert_eq!(
                vfs.mkdir(ROOT_INODE, name, 0o755, 1000, 1000).await.unwrap_err(),
                libc::EACCES,
                "mkdir({name})"
            );
        }
        // rename to a junk destination name is also blocked
        let _ = vfs.lookup(ROOT_INODE, "legit.txt").await.unwrap();
        assert_eq!(
            vfs.rename(ROOT_INODE, "legit.txt", ROOT_INODE, ".DS_Store", false)
                .await
                .unwrap_err(),
            libc::EACCES
        );
    });
}

// ── Lookup / revalidation / poll ────────────────────────────────────

/// HEAD failure is silently ignored (graceful degradation, cached data served).
#[test]
fn revalidate_head_fails_graceful() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        hub.fail_next_head();
        {
            let mut inodes = vfs.inode_table.write().unwrap();
            if let Some(e) = inodes.get_mut(ino) {
                e.last_revalidated = None;
            }
        }

        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        assert_eq!(attr.size, 100);
    });
}

/// With serve_lookup_from_cache=true, HEAD is skipped within the metadata TTL window.
#[test]
fn lookup_ttl_skips_head_within_window() {
    let hub = MockHub::new();
    hub.add_file("cached.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let rt = new_runtime();
    let vfs = make_test_vfs(
        hub.clone(),
        xet.clone(),
        TestOpts {
            serve_lookup_from_cache: true,
            metadata_ttl: Duration::from_secs(60),
            ..Default::default()
        },
        &rt,
    );

    rt.block_on(async {
        let _ = vfs.lookup(ROOT_INODE, "cached.txt").await.unwrap();

        hub.set_head(
            "cached.txt",
            Some(HeadFileInfo {
                xet_hash: Some("new_hash".to_string()),
                etag: None,
                size: Some(200),
                last_modified: None,
            }),
        );

        // Within TTL: HEAD skipped, stale size returned
        let attr = vfs.lookup(ROOT_INODE, "cached.txt").await.unwrap();
        assert_eq!(attr.size, 100);
    });
}

/// A `lookup(parent, name)` under an unloaded directory resolves the single
/// file via HEAD without listing its siblings — the doc-site workload never
/// calls `readdir`, so paying for a list_tree on every cold lookup would
/// bloat the table with sibling inodes the caller never asked for.
#[test]
fn lookup_uses_head_not_list_tree() {
    let hub = MockHub::new();
    for i in 0..50 {
        hub.add_file(&format!("subdir/file_{i:02}.txt"), 10, Some(&format!("h{i}")), None);
    }
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        // Root is materialized at startup; `subdir` is a lazy directory child.
        let subdir = vfs.lookup(ROOT_INODE, "subdir").await.unwrap();
        assert_eq!(subdir.kind, InodeKind::Directory);

        let pre_list = hub.list_tree_call_count();
        let pre_head = hub.head_file_call_count();
        let pre_len = vfs.inode_table.read().unwrap().len();

        let attr = vfs.lookup(subdir.ino, "file_07.txt").await.unwrap();
        assert_eq!(attr.size, 10);

        assert_eq!(
            hub.list_tree_call_count(),
            pre_list,
            "lookup under an unloaded dir must not trigger list_tree"
        );
        assert_eq!(
            hub.head_file_call_count(),
            pre_head + 1,
            "one HEAD for the point lookup"
        );
        assert_eq!(
            vfs.inode_table.read().unwrap().len(),
            pre_len + 1,
            "only the requested file should be materialized"
        );
    });
}

/// Missing name under an unloaded parent: HEAD returns 404, list_tree
/// confirms the name isn't there, negative cache is populated, and the
/// follow-up lookup short-circuits without re-hitting the Hub.
#[test]
fn lookup_missing_under_unloaded_parent_populates_negative_cache() {
    let hub = MockHub::new();
    hub.add_file("subdir/exists.txt", 1, Some("h"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let subdir = vfs.lookup(ROOT_INODE, "subdir").await.unwrap();

        let pre_head = hub.head_file_call_count();
        let pre_list = hub.list_tree_call_count();

        let err = vfs.lookup(subdir.ino, "ghost.txt").await.unwrap_err();
        assert_eq!(err, libc::ENOENT);
        // Slow path: HEAD probe + list_tree fallback.
        assert_eq!(hub.head_file_call_count(), pre_head + 1);
        assert_eq!(hub.list_tree_call_count(), pre_list + 1);
        assert!(vfs.negative_cache_check("subdir/ghost.txt"));

        // Second lookup should hit the negative cache: no extra Hub calls.
        let err2 = vfs.lookup(subdir.ino, "ghost.txt").await.unwrap_err();
        assert_eq!(err2, libc::ENOENT);
        assert_eq!(hub.head_file_call_count(), pre_head + 1, "neg-cache should skip HEAD");
        assert_eq!(
            hub.list_tree_call_count(),
            pre_list + 1,
            "neg-cache should skip list_tree"
        );
    });
}

/// A file appears remotely after the parent's listing was cached. A lookup
/// hitting `FastResult::Miss` must re-validate via HEAD and surface the new
/// entry instead of trusting the stale cache.
#[test]
fn lookup_miss_finds_remotely_added_file() {
    let hub = MockHub::new();
    hub.add_file("subdir/known.txt", 5, Some("h_k"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let subdir = vfs.lookup(ROOT_INODE, "subdir").await.unwrap();
        // Warm parent.children_loaded by listing it.
        let _ = vfs.readdir(subdir.ino).await.unwrap();
        assert!(vfs.inode_table.read().unwrap().is_children_loaded(subdir.ino));

        // Simulate a remote concurrent upload after we listed.
        hub.add_file("subdir/freshly_added.txt", 9, Some("h_fresh"), None);
        hub.set_head(
            "subdir/freshly_added.txt",
            Some(HeadFileInfo {
                xet_hash: Some("h_fresh".to_string()),
                etag: None,
                size: Some(9),
                last_modified: None,
            }),
        );

        // Miss arm should HEAD-probe and discover the entry.
        let attr = vfs.lookup(subdir.ino, "freshly_added.txt").await.unwrap();
        assert_eq!(attr.size, 9);
        assert_eq!(attr.kind, InodeKind::File);
    });
}

/// A directory appears remotely after the parent's listing was cached. A
/// lookup hitting `FastResult::Miss` must fall back to the targeted list_tree
/// probe (resolve endpoint returns 404 for dirs) and surface the new entry.
#[test]
fn lookup_miss_finds_remotely_added_dir() {
    let hub = MockHub::new();
    hub.add_file("repo/v1.0.0/file.txt", 3, Some("h_v1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let repo = vfs.lookup(ROOT_INODE, "repo").await.unwrap();
        // Warm parent.children_loaded; the only known sub-dir is v1.0.0.
        let _ = vfs.readdir(repo.ino).await.unwrap();

        // Simulate a remote upload of a new version directory.
        hub.add_file("repo/v2.0.0/file.txt", 4, Some("h_v2"), None);

        // Miss arm: HEAD on `repo/v2.0.0` returns 404 (it's a dir), list_tree
        // probe finds entries under it, and we insert the dir non-destructively.
        let attr = vfs.lookup(repo.ino, "v2.0.0").await.unwrap();
        assert_eq!(attr.kind, InodeKind::Directory);

        // Sibling listing must still be intact: v1.0.0 should not have been evicted.
        let inodes = vfs.inode_table.read().unwrap();
        assert!(inodes.lookup_child(repo.ino, "v1.0.0").is_some());
    });
}

/// A name absent from a cached parent listing AND from the remote: ENOENT
/// is served and the negative cache short-circuits subsequent lookups.
#[test]
fn lookup_miss_truly_absent_populates_neg_cache() {
    let hub = MockHub::new();
    hub.add_file("subdir/known.txt", 1, Some("h"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let subdir = vfs.lookup(ROOT_INODE, "subdir").await.unwrap();
        let _ = vfs.readdir(subdir.ino).await.unwrap();

        let pre_head = hub.head_file_call_count();
        let pre_list = hub.list_tree_call_count();

        let err = vfs.lookup(subdir.ino, "ghost.txt").await.unwrap_err();
        assert_eq!(err, libc::ENOENT);
        assert_eq!(hub.head_file_call_count(), pre_head + 1, "HEAD probed once");
        assert_eq!(hub.list_tree_call_count(), pre_list + 1, "list_tree probed once");
        assert!(vfs.negative_cache_check("subdir/ghost.txt"));

        // Second lookup should hit neg cache, no extra Hub calls.
        let err2 = vfs.lookup(subdir.ino, "ghost.txt").await.unwrap_err();
        assert_eq!(err2, libc::ENOENT);
        assert_eq!(hub.head_file_call_count(), pre_head + 1);
        assert_eq!(hub.list_tree_call_count(), pre_list + 1);
    });
}

/// unlink seeds the negative cache so a lookup right after — before the
/// remote delete is flushed — doesn't resurrect the file via the new
/// HEAD-on-miss probe.
#[test]
fn unlink_populates_neg_cache() {
    let hub = MockHub::new();
    hub.add_file("doomed.txt", 5, Some("h"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let _ = vfs.lookup(ROOT_INODE, "doomed.txt").await.unwrap();

        vfs.unlink(ROOT_INODE, "doomed.txt").await.unwrap();
        assert!(vfs.negative_cache_check("doomed.txt"));

        // Even though MockHub still has the file (remote delete is async),
        // the lookup must return ENOENT via the negative cache.
        let pre_head = hub.head_file_call_count();
        let err = vfs.lookup(ROOT_INODE, "doomed.txt").await.unwrap_err();
        assert_eq!(err, libc::ENOENT);
        assert_eq!(hub.head_file_call_count(), pre_head, "neg cache should skip HEAD");
    });
}

/// rmdir seeds the negative cache for the removed directory path.
#[test]
fn rmdir_populates_neg_cache() {
    let hub = MockHub::new();
    hub.add_dir("emptydir");
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let _ = vfs.lookup(ROOT_INODE, "emptydir").await.unwrap();
        vfs.rmdir(ROOT_INODE, "emptydir").await.unwrap();
        assert!(vfs.negative_cache_check("emptydir"));
    });
}

/// On a writable mount the children list also holds locally-created entries
/// that have not yet propagated to the remote tree listing. A Miss-arm
/// lookup of a different name must NOT evict those local entries — it should
/// probe the new name in isolation.
#[test]
fn lookup_miss_does_not_evict_local_uploads_on_writable_mount() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        // Locally create + flush a file. After flush it is "clean" but not
        // yet visible in the remote tree listing (Xet propagation gap).
        let (attr, fh) = vfs
            .create(ROOT_INODE, "local_upload.txt", 0o644, 1000, 1000, None)
            .await
            .unwrap();
        write_blocking(&vfs, attr.ino, fh, 0, b"data").await.unwrap();
        vfs.flush(attr.ino, fh, None).await.unwrap();
        vfs.release(fh).await.unwrap();

        // Trigger a Miss-arm lookup for an unrelated name. Old behavior
        // (invalidate + relist) would have dropped local_upload.txt because
        // the mock tree doesn't know about it yet.
        let _ = vfs.lookup(ROOT_INODE, "ghost.txt").await.unwrap_err();

        // The local upload must still resolve.
        let still_there = vfs.lookup(ROOT_INODE, "local_upload.txt").await.unwrap();
        assert_eq!(still_there.ino, attr.ino);
    });
}

/// rename seeds the negative cache for the source path so the HEAD-on-miss
/// probe doesn't resurrect it from a not-yet-flushed remote delete.
#[test]
fn rename_populates_neg_cache_for_old_path() {
    let hub = MockHub::new();
    hub.add_file("src.txt", 3, Some("h"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let _ = vfs.lookup(ROOT_INODE, "src.txt").await.unwrap();
        vfs.rename(ROOT_INODE, "src.txt", ROOT_INODE, "dst.txt", false)
            .await
            .unwrap();
        assert!(vfs.negative_cache_check("src.txt"));

        // Lookup of old path must not resurrect via HEAD.
        let pre_head = hub.head_file_call_count();
        let err = vfs.lookup(ROOT_INODE, "src.txt").await.unwrap_err();
        assert_eq!(err, libc::ENOENT);
        assert_eq!(hub.head_file_call_count(), pre_head, "neg cache should skip HEAD");
    });
}

/// Sequence: point-lookup (HEAD slow path) → readdir on the same parent
/// (list_tree populates `children_loaded`) → re-lookup the file. The HEAD-
/// inserted inode must survive the listing reconciliation and the re-lookup
/// must return the same ino without re-fetching.
#[test]
fn lookup_then_readdir_then_lookup_preserves_inode() {
    let hub = MockHub::new();
    hub.add_file("subdir/keep.txt", 7, Some("h_keep"), None);
    hub.add_file("subdir/sib.txt", 3, Some("h_sib"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let subdir = vfs.lookup(ROOT_INODE, "subdir").await.unwrap();

        // 1. HEAD-based point lookup of one file (parent not listed yet).
        let first = vfs.lookup(subdir.ino, "keep.txt").await.unwrap();
        let inserted_ino = first.ino;
        assert_eq!(first.size, 7);
        assert!(
            !vfs.inode_table.read().unwrap().is_children_loaded(subdir.ino),
            "HEAD path must not flip children_loaded"
        );

        // 2. readdir runs list_tree → reconciles the full listing.
        let entries = vfs.readdir(subdir.ino).await.unwrap();
        let names: std::collections::HashSet<_> = entries.into_iter().map(|e| e.name).collect();
        assert!(names.contains("keep.txt"));
        assert!(names.contains("sib.txt"));
        assert!(vfs.inode_table.read().unwrap().is_children_loaded(subdir.ino));

        // 3. Re-lookup the original file → fast path, same ino, no extra HEAD.
        let pre_head = hub.head_file_call_count();
        let again = vfs.lookup(subdir.ino, "keep.txt").await.unwrap();
        assert_eq!(again.ino, inserted_ino, "inode must be stable across readdir");
        assert_eq!(again.size, 7);
        // Fast path may revalidate via HEAD (TTL-gated); at most one extra call.
        assert!(hub.head_file_call_count() <= pre_head + 1);
    });
}

/// HEAD responses without size can't be trusted for the inode (open_readonly
/// would take the empty-file shortcut), so fall back to list_tree which has
/// the authoritative size from the tree index.
#[test]
fn lookup_falls_back_to_list_when_head_has_no_size() {
    let hub = MockHub::new();
    hub.add_file("sub/sized.txt", 42, Some("h"), None);
    hub.set_head(
        "sub/sized.txt",
        Some(HeadFileInfo {
            xet_hash: Some("h".to_string()),
            etag: None,
            size: None,
            last_modified: None,
        }),
    );
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let sub = vfs.lookup(ROOT_INODE, "sub").await.unwrap();
        let pre_list = hub.list_tree_call_count();
        let attr = vfs.lookup(sub.ino, "sized.txt").await.unwrap();
        assert_eq!(attr.size, 42, "size must come from the listing, not the HEAD");
        assert_eq!(
            hub.list_tree_call_count(),
            pre_list + 1,
            "HEAD without size should trigger a list_tree"
        );
    });
}

/// A cached directory entry under an unloaded parent must be re-resolved
/// rather than served from memory — without a fresh listing we can't tell
/// whether the dir was removed or replaced remotely.
#[test]
fn lookup_rechecks_cached_directory_when_parent_unloaded() {
    let hub = MockHub::new();
    hub.add_file("area/nested/file.txt", 10, Some("h"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let area = vfs.lookup(ROOT_INODE, "area").await.unwrap();
        // Prime `area`'s children so `nested` is in the table.
        let _ = vfs.readdir(area.ino).await.unwrap();
        assert!(vfs.inode_table.read().unwrap().is_children_loaded(area.ino));

        // Invalidate the parent's listing (mirrors what LRU eviction does).
        if let Some(e) = vfs.inode_table.write().unwrap().get_mut(area.ino) {
            e.children_loaded_at = None;
        }

        let pre_list = hub.list_tree_call_count();
        let pre_head = hub.head_file_call_count();

        let nested = vfs.lookup(area.ino, "nested").await.unwrap();
        assert_eq!(nested.kind, InodeKind::Directory);

        // Must re-resolve via the slow path: one HEAD probe (404, it's a dir)
        // and one list_tree to confirm.
        assert_eq!(hub.head_file_call_count(), pre_head + 1);
        assert_eq!(hub.list_tree_call_count(), pre_list + 1);
    });
}

/// If a previously-cached directory has been replaced by a file on the
/// remote, the HEAD slow path must evict the stale dir and expose the new
/// file instead of returning the old cached entry.
#[test]
fn lookup_replaces_stale_directory_with_file_via_head() {
    let hub = MockHub::new();
    hub.add_file("swap/child.txt", 1, Some("h_inner"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        // Cache `swap` as a directory + one child.
        let swap_dir = vfs.lookup(ROOT_INODE, "swap").await.unwrap();
        assert_eq!(swap_dir.kind, InodeKind::Directory);
        let _ = vfs.readdir(swap_dir.ino).await.unwrap();

        // Remote replaces the directory with a file at the same path.
        hub.remove_file("swap/child.txt");
        hub.add_file("swap", 99, Some("h_file"), None);

        // Invalidate root's listing so lookup takes the slow path.
        if let Some(e) = vfs.inode_table.write().unwrap().get_mut(ROOT_INODE) {
            e.children_loaded_at = None;
        }

        let attr = vfs.lookup(ROOT_INODE, "swap").await.unwrap();
        assert_eq!(
            attr.kind,
            InodeKind::File,
            "stale directory must be replaced by the file"
        );
        assert_eq!(attr.size, 99);
    });
}

/// If the stale cached directory has an open file handle on one of its
/// descendants, the HEAD path must NOT evict it — doing so would drop
/// local state still referenced by FUSE. The lookup returns the stale
/// directory entry in place; correctness yields to data safety in this
/// narrow race.
#[test]
fn lookup_preserves_stale_directory_with_open_descendant() {
    let hub = MockHub::new();
    hub.add_file("swap/child.txt", 5, Some("h_inner"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let swap_dir = vfs.lookup(ROOT_INODE, "swap").await.unwrap();
        let _ = vfs.readdir(swap_dir.ino).await.unwrap();

        // Keep a file handle on a descendant of the cached dir.
        let child = vfs.lookup(swap_dir.ino, "child.txt").await.unwrap();
        let fh = vfs.open(child.ino, false, false, None).await.unwrap();

        // Remote swap: dir → file.
        hub.remove_file("swap/child.txt");
        hub.add_file("swap", 99, Some("h_file"), None);

        // Invalidate root's listing so lookup hits the slow path.
        if let Some(e) = vfs.inode_table.write().unwrap().get_mut(ROOT_INODE) {
            e.children_loaded_at = None;
        }

        let attr = vfs.lookup(ROOT_INODE, "swap").await.unwrap();
        // Fallback to list_tree, which keeps the stale dir alive because
        // descendants are still open — safer than losing the open handle.
        assert_eq!(attr.kind, InodeKind::Directory);

        vfs.release(fh).await.unwrap();
    });
}

/// When the HEAD probe returns 404 the name could be a directory — fall
/// back to a full listing so dir traversal still works.
#[test]
fn lookup_falls_back_to_list_for_directory() {
    let hub = MockHub::new();
    hub.add_file("outer/inner/file.txt", 10, Some("h"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let outer = vfs.lookup(ROOT_INODE, "outer").await.unwrap();
        let pre_list = hub.list_tree_call_count();
        let pre_head = hub.head_file_call_count();

        let inner = vfs.lookup(outer.ino, "inner").await.unwrap();
        assert_eq!(inner.kind, InodeKind::Directory);

        assert_eq!(hub.head_file_call_count(), pre_head + 1, "HEAD probed first");
        assert_eq!(
            hub.list_tree_call_count(),
            pre_list + 1,
            "404 on HEAD falls back to a single list_tree"
        );
    });
}

/// Lookup of nonexistent path returns ENOENT and populates the negative cache.
#[test]
fn negative_cache_insert() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let result = vfs.lookup(ROOT_INODE, "nope.txt").await;
        assert_eq!(result.unwrap_err(), libc::ENOENT);
        assert!(vfs.negative_cache_check("nope.txt"));
    });
}

/// update_remote_file() skips dirty inodes (local writes not overwritten by poll).
#[test]
fn poll_dirty_files_skipped() {
    let hub = MockHub::new();
    hub.add_file("dirty.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "dirty.txt").await.unwrap();
        let ino = attr.ino;

        {
            let mut inodes = vfs.inode_table.write().unwrap();
            inodes.get_mut(ino).unwrap().set_dirty();
        }

        let mut inodes = vfs.inode_table.write().unwrap();
        let updated = inodes.update_remote_file(ino, Some("new_hash".to_string()), None, 999, SystemTime::now());
        assert!(!updated);
        assert_eq!(inodes.get(ino).unwrap().xet_hash.as_deref(), Some("hash1"));
    });
}

// ── Read/write handle management ────────────────────────────────────

/// Streaming write at a non-sequential offset returns EINVAL.
#[test]
fn write_streaming_nonsequential_einval() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let fh = vfs.open(attr.ino, true, true, Some(42)).await.unwrap();

        let result = write_blocking(&vfs, attr.ino, fh, 10, b"data").await;
        assert_eq!(result.unwrap_err(), libc::EINVAL);

        vfs.release(fh).await.unwrap();
    });
}

/// write() to a read-only file handle returns EBADF.
#[test]
fn write_readonly_handle_ebadf() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    xet.add_file("hash1", b"hello world test data padding for read");
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let fh = vfs.open(attr.ino, false, false, None).await.unwrap();

        let result = write_blocking(&vfs, attr.ino, fh, 0, b"data").await;
        assert_eq!(result.unwrap_err(), libc::EBADF);

        vfs.release(fh).await.unwrap();
    });
}

/// read() from a streaming (write-only) handle returns EBADF.
#[test]
fn read_streaming_handle_ebadf() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let fh = vfs.open(attr.ino, true, true, Some(42)).await.unwrap();

        let result = vfs.read(fh, 0, 10).await;
        assert_eq!(result.unwrap_err(), libc::EBADF);

        vfs.release(fh).await.unwrap();
    });
}

/// read/write with a bogus file handle returns EBADF.
#[test]
fn read_write_invalid_handle_ebadf() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let result = vfs.read(999, 0, 10).await;
        assert_eq!(result.unwrap_err(), libc::EBADF);

        let result = write_blocking(&vfs, 1, 999, 0, b"data").await;
        assert_eq!(result.unwrap_err(), libc::EBADF);
    });
}

/// Opening a remote xet-backed file for read returns correct data via CAS range reads.
#[test]
fn read_remote_file_range() {
    let hub = MockHub::new();
    let content = b"hello world from CAS storage";
    hub.add_file("remote.txt", content.len() as u64, Some("xet_hash_1"), None);
    let xet = MockXet::new();
    xet.add_file("xet_hash_1", content);
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "remote.txt").await.unwrap();
        let fh = vfs.open(attr.ino, false, false, None).await.unwrap();

        let (data, eof) = vfs.read(fh, 0, 100).await.unwrap();
        assert_eq!(&data[..], content);
        assert!(eof);

        vfs.release(fh).await.unwrap();
    });
}

// ── Mode matrix + edge cases ────────────────────────────────────────

/// open(writable) on a read-only VFS returns EROFS.
#[test]
fn open_readonly_fs_erofs() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_readonly(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let result = vfs.open(attr.ino, true, true, Some(42)).await;
        assert_eq!(result.unwrap_err(), libc::EROFS);
    });
}

/// open(writable, !truncate) in simple mode returns EPERM (append-only semantics).
#[test]
fn open_simple_no_truncate_eperm() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let result = vfs.open(attr.ino, true, false, Some(42)).await;
        assert_eq!(result.unwrap_err(), libc::EPERM);
    });
}

/// setattr(size) in simple mode is a silent noop (size unchanged).
#[test]
fn setattr_simple_mode_noop() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let result = vfs.setattr(attr.ino, Some(50), None, None, None, None, None).await;
        assert!(result.is_ok(), "ftruncate should succeed (noop)");
        let inodes = vfs.inode_table.read().unwrap();
        let entry = inodes.get(attr.ino).unwrap();
        assert_eq!(entry.size, 100, "size should be unchanged (noop)");
    });
}

/// setattr(size) on an empty file in simple mode is a silent noop.
#[test]
fn setattr_simple_mode_empty_file_noop() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let (attr, _fh) = vfs
            .create(ROOT_INODE, "new.txt", 0o644, 1000, 1000, Some(1))
            .await
            .unwrap();
        // ftruncate(fd, N) in simple mode succeeds but is a noop
        let result = vfs.setattr(attr.ino, Some(100), None, None, None, None, None).await;
        assert!(result.is_ok(), "ftruncate should succeed (noop): {:?}", result);
        let inodes = vfs.inode_table.read().unwrap();
        let entry = inodes.get(attr.ino).unwrap();
        assert_eq!(entry.size, 0, "size should be unchanged (noop)");
    });
}

/// setattr(size) on a directory returns EISDIR.
#[test]
fn setattr_directory_eisdir() {
    let hub = MockHub::new();
    hub.add_dir("mydir");
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "mydir").await.unwrap();
        let result = vfs.setattr(attr.ino, Some(0), None, None, None, None, None).await;
        assert_eq!(result.unwrap_err(), libc::EISDIR);
    });
}

/// setattr truncate to 0 in advanced mode creates an empty staging file, marks dirty.
#[test]
fn setattr_advanced_truncate_zero() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        let result = vfs.setattr(ino, Some(0), None, None, None, None, None).await.unwrap();
        assert_eq!(result.size, 0);

        let inodes = vfs.inode_table.read().unwrap();
        let entry = inodes.get(ino).unwrap();
        assert!(entry.is_dirty());
        assert!(entry.xet_hash.is_none());
    });
}

/// Regression: truncating an unstaged remote file to 0 must not debit the
/// staging budget for bytes that were never added (it was never staged).
#[test]
fn setattr_truncate_unstaged_does_not_debit_budget() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let sd = vfs.staging.dir().unwrap();
        // Simulate other live staging files consuming budget.
        sd.resize_bytes(0, 500);
        assert_eq!(sd.bytes_used(), 500);

        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        // Truncate to 0 without ever staging the remote file.
        vfs.setattr(attr.ino, Some(0), None, None, None, None, None)
            .await
            .unwrap();

        // The 500 bytes belonging to other files must remain accounted; this
        // inode's 100 bytes were never added so they must not be subtracted.
        assert_eq!(sd.bytes_used(), 500);
    });
}

/// setattr on a read-only VFS returns EROFS.
#[test]
fn setattr_readonly_erofs() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_readonly(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let result = vfs.setattr(attr.ino, Some(0), None, None, None, None, None).await;
        assert_eq!(result.unwrap_err(), libc::EROFS);
    });
}

/// FlushManager surfaces upload errors via check_error() after debounce.
#[test]
fn flush_manager_check_error_surfaces() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        let fh = vfs.open(ino, true, true, Some(42)).await.unwrap();
        write_blocking(&vfs, ino, fh, 0, b"data").await.unwrap();

        xet.fail_upload();
        vfs.release(fh).await.unwrap();

        // Wait for flush debounce (100ms in test config) + processing
        tokio::time::sleep(Duration::from_secs(3)).await;

        let err = vfs.flush_manager.as_ref().unwrap().check_error(ino);
        assert!(err.is_some(), "flush manager should have recorded an error");
    });
}

/// Concurrent shutdown calls must not deadlock. Reproduces the scenario where
/// destroy() and the signal handler both call shutdown() simultaneously.
#[test]
fn concurrent_shutdown_no_deadlock() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let fh = vfs.open(attr.ino, true, true, Some(42)).await.unwrap();
        write_blocking(&vfs, attr.ino, fh, 0, b"dirty data").await.unwrap();
        vfs.release(fh).await.unwrap();
    });

    // Spawn two threads calling shutdown concurrently (simulates destroy + signal handler).
    let vfs1 = vfs.clone();
    let vfs2 = vfs.clone();

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let done_tx2 = done_tx.clone();

    std::thread::spawn(move || {
        vfs1.shutdown();
        let _ = done_tx.send(());
    });
    std::thread::spawn(move || {
        // Small delay to overlap with the first shutdown.
        std::thread::sleep(Duration::from_millis(10));
        vfs2.shutdown();
        let _ = done_tx2.send(());
    });

    // Both must complete within 10s, otherwise we have a deadlock.
    for _ in 0..2 {
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("shutdown deadlocked: thread did not complete within 10s");
    }
}

/// getattr on a nonexistent inode returns ENOENT.
#[test]
fn getattr_nonexistent_enoent() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (_, vfs) = vfs_simple(&hub, &xet);

    let result = vfs.getattr(9999);
    assert_eq!(result.unwrap_err(), libc::ENOENT);
}

/// getattr on root inode returns a valid directory attr.
#[test]
fn getattr_root_inode() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (_, vfs) = vfs_simple(&hub, &xet);

    let attr = vfs.getattr(ROOT_INODE).unwrap();
    assert_eq!(attr.ino, ROOT_INODE);
    assert_eq!(attr.kind, InodeKind::Directory);
}

/// readdir on a file inode returns ENOTDIR.
#[test]
fn readdir_on_file_enotdir() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let result = vfs.readdir(attr.ino).await;
        assert_eq!(result.unwrap_err(), libc::ENOTDIR);
    });
}

/// readdir includes . and .. entries plus child files.
#[test]
fn readdir_returns_dot_entries() {
    let hub = MockHub::new();
    hub.add_file("a.txt", 10, Some("h1"), None);
    hub.add_file("b.txt", 20, Some("h2"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let entries = vfs.readdir(ROOT_INODE).await.unwrap();
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"."));
        assert!(names.contains(&".."));
        assert!(names.contains(&"a.txt"));
        assert!(names.contains(&"b.txt"));
        assert_eq!(entries.len(), 4);
    });
}

/// mkdir on an existing name returns EEXIST; mkdir on a read-only VFS returns EROFS.
#[test]
fn mkdir_eexist_and_erofs() {
    let hub = MockHub::new();
    hub.add_dir("existing");
    let xet = MockXet::new();

    {
        let (rt, vfs) = vfs_simple(&hub, &xet);
        rt.block_on(async {
            let _ = vfs.lookup(ROOT_INODE, "existing").await.unwrap();
            let result = vfs.mkdir(ROOT_INODE, "existing", 0o755, 1000, 1000).await;
            assert_eq!(result.unwrap_err(), libc::EEXIST);
        });
    }

    {
        let (rt, vfs) = vfs_readonly(&hub, &xet);
        rt.block_on(async {
            let result = vfs.mkdir(ROOT_INODE, "newdir", 0o755, 1000, 1000).await;
            assert_eq!(result.unwrap_err(), libc::EROFS);
        });
    }
}

/// Batch ops are ordered: AddFile first, then DeleteFile (Hub API requirement).
#[test]
fn flush_batch_ordering_adds_before_deletes() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        let fh = vfs.open(ino, true, true, Some(42)).await.unwrap();
        write_blocking(&vfs, ino, fh, 0, b"data").await.unwrap();

        // Inject pending_deletes to test ordering
        {
            let mut inodes = vfs.inode_table.write().unwrap();
            if let Some(entry) = inodes.get_mut(ino) {
                entry.pending_deletes.push("old_path.txt".to_string());
            }
        }

        vfs.flush(ino, fh, Some(42)).await.unwrap();

        let logs = hub.take_batch_log();
        assert_eq!(logs.len(), 1);
        let ops = &logs[0];
        assert!(ops.len() >= 2);
        assert!(matches!(ops.first().unwrap(), BatchOp::AddFile { .. }));
        assert!(matches!(ops.last().unwrap(), BatchOp::DeleteFile { .. }));

        vfs.release(fh).await.unwrap();
    });
}

/// create() + write in advanced mode writes to a staging file (supports random writes).
#[test]
fn create_and_write_advanced_mode() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let (attr, fh) = vfs
            .create(ROOT_INODE, "new.txt", 0o644, 1000, 1000, Some(42))
            .await
            .unwrap();
        let ino = attr.ino;
        assert_eq!(attr.size, 0);

        let written = write_blocking(&vfs, ino, fh, 0, b"hello staging").await.unwrap();
        assert_eq!(written, 13);

        let (data, _eof) = vfs.read(fh, 0, 100).await.unwrap();
        assert_eq!(&data[..], b"hello staging");

        vfs.release(fh).await.unwrap();

        let inodes = vfs.inode_table.read().unwrap();
        let entry = inodes.get(ino).unwrap();
        assert!(entry.is_dirty());
    });
}

/// shutdown() completes without panic even with no dirty files or poll task.
#[test]
fn shutdown_aborts_poll() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (_, vfs) = vfs_simple(&hub, &xet);
    vfs.shutdown();
}

// ── Read-only mode guards ───────────────────────────────────────────

/// unlink() on a directory returns EISDIR.
#[test]
fn unlink_directory_eisdir() {
    let hub = MockHub::new();
    hub.add_dir("mydir");
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let _ = vfs.lookup(ROOT_INODE, "mydir").await.unwrap();
        let result = vfs.unlink(ROOT_INODE, "mydir").await;
        assert_eq!(result.unwrap_err(), libc::EISDIR);
    });
}

/// Read-only VFS reports 0o444 for files and 0o555 for directories.
#[test]
fn readonly_permissions() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    hub.add_dir("mydir");
    let xet = MockXet::new();
    let (rt, vfs) = vfs_readonly(&hub, &xet);

    rt.block_on(async {
        let file_attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        assert_eq!(file_attr.perm, 0o444);

        let dir_attr = vfs.lookup(ROOT_INODE, "mydir").await.unwrap();
        assert_eq!(dir_attr.perm, 0o555);
    });
}

/// Writable VFS reports 0o644 for files and 0o755 for directories.
#[test]
fn writable_permissions() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    hub.add_dir("mydir");
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let file_attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        assert_eq!(file_attr.perm, 0o644);

        let dir_attr = vfs.lookup(ROOT_INODE, "mydir").await.unwrap();
        assert_eq!(dir_attr.perm, 0o755);
    });
}

/// unlink/rename/rmdir on a read-only VFS all return EROFS.
#[test]
fn mutation_ops_readonly_erofs() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    hub.add_dir("mydir");
    let xet = MockXet::new();
    let (rt, vfs) = vfs_readonly(&hub, &xet);

    rt.block_on(async {
        let _ = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let _ = vfs.lookup(ROOT_INODE, "mydir").await.unwrap();

        assert_eq!(vfs.unlink(ROOT_INODE, "file.txt").await.unwrap_err(), libc::EROFS);
        assert_eq!(
            vfs.rename(ROOT_INODE, "file.txt", ROOT_INODE, "new.txt", false)
                .await
                .unwrap_err(),
            libc::EROFS
        );
        assert_eq!(vfs.rmdir(ROOT_INODE, "mydir").await.unwrap_err(), libc::EROFS);
    });
}

// ── same_process ────────────────────────────────────────────────────

/// same_process: equal PIDs → true, different process → false, nonexistent PID → false.
#[test]
fn same_process_cases() {
    let pid = std::process::id();
    assert!(super::same_process(pid, pid));
    assert!(!super::same_process(pid, 1)); // PID 1 = init/launchd
    assert!(!super::same_process(pid, 4_000_000_000));
}

// ── repo lazy tree loading ─────────────────────────────────────────

/// Build a VFS backed by a repo MockHub (lazy-loads directories on access).
fn vfs_repo(
    hub: &std::sync::Arc<MockHub>,
    xet: &std::sync::Arc<MockXet>,
) -> (tokio::runtime::Runtime, std::sync::Arc<VirtualFs>) {
    let rt = new_runtime();
    let vfs = make_test_vfs(
        hub.clone(),
        xet.clone(),
        TestOpts {
            read_only: true,
            ..Default::default()
        },
        &rt,
    );
    (rt, vfs)
}

/// Repo mode lazy-loads nested dirs: lookup triggers ensure_children_loaded.
#[test]
fn repo_lazy_load_nested_dirs() {
    let hub = MockHub::new_repo();
    hub.add_file("a/b/c/file.txt", 42, Some("h1"), None);
    hub.add_file("a/b/other.txt", 10, Some("h2"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_repo(&hub, &xet);

    rt.block_on(async {
        // Intermediate dirs "a", "a/b", "a/b/c" should exist.
        let a = vfs.lookup(ROOT_INODE, "a").await.unwrap();
        assert_eq!(a.kind, InodeKind::Directory);

        let b = vfs.lookup(a.ino, "b").await.unwrap();
        assert_eq!(b.kind, InodeKind::Directory);

        let c = vfs.lookup(b.ino, "c").await.unwrap();
        assert_eq!(c.kind, InodeKind::Directory);

        let file = vfs.lookup(c.ino, "file.txt").await.unwrap();
        assert_eq!(file.size, 42);

        let other = vfs.lookup(b.ino, "other.txt").await.unwrap();
        assert_eq!(other.size, 10);
    });
}

/// Repo mode loads root files on startup.
#[test]
fn repo_root_files_loaded() {
    let hub = MockHub::new_repo();
    hub.add_file("README.md", 100, Some("h1"), None);
    hub.add_file("config.json", 50, Some("h2"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_repo(&hub, &xet);

    rt.block_on(async {
        let readme = vfs.lookup(ROOT_INODE, "README.md").await.unwrap();
        assert_eq!(readme.size, 100);

        let config = vfs.lookup(ROOT_INODE, "config.json").await.unwrap();
        assert_eq!(config.size, 50);
    });
}

/// Repo mode lazy-loads subdirectory children on readdir.
#[test]
fn repo_lazy_load_subdirs() {
    let hub = MockHub::new_repo();
    hub.add_file("models/bert/config.json", 10, Some("h1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_repo(&hub, &xet);

    rt.block_on(async {
        let models = vfs.lookup(ROOT_INODE, "models").await.unwrap();
        // readdir should work without any additional API calls
        let entries = vfs.readdir(models.ino).await.unwrap();
        // ".", "..", "bert"
        assert_eq!(entries.len(), 3);
    });
}

/// Repo mode preserves oid as etag on file inodes (loaded via lookup).
#[test]
fn repo_preserves_oid() {
    let hub = MockHub::new_repo();
    hub.add_file("file.txt", 100, Some("xet_h"), Some("oid_123"));
    let xet = MockXet::new();
    let (rt, vfs) = vfs_repo(&hub, &xet);

    rt.block_on(async {
        let _ = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let inodes = vfs.inode_table.read().unwrap();
        let entry = inodes.lookup_child(ROOT_INODE, "file.txt").unwrap();
        assert_eq!(entry.etag.as_deref(), Some("oid_123"));
    });
}

// ── ensure_children_loaded ──────────────────────────────────────────

/// ensure_children_loaded creates subdirectory entries from the Hub tree listing.
#[test]
fn ensure_children_loaded_with_subdirs() {
    let hub = MockHub::new();
    hub.add_dir("subdir");
    hub.add_file("subdir/file.txt", 50, Some("h1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let subdir = vfs.lookup(ROOT_INODE, "subdir").await.unwrap();
        assert_eq!(subdir.kind, InodeKind::Directory);

        // Load children of subdir
        let file = vfs.lookup(subdir.ino, "file.txt").await.unwrap();
        assert_eq!(file.size, 50);
    });
}

/// ensure_children_loaded is idempotent: calling twice doesn't duplicate children.
#[test]
fn ensure_children_loaded_idempotent() {
    let hub = MockHub::new();
    hub.add_file("a.txt", 10, Some("h1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        // First call loads children (happens implicitly in lookup)
        let _ = vfs.lookup(ROOT_INODE, "a.txt").await.unwrap();
        // Second call is a no-op (children already loaded)
        let entries = vfs.readdir(ROOT_INODE).await.unwrap();
        let file_count = entries.iter().filter(|e| e.name == "a.txt").count();
        assert_eq!(file_count, 1);
    });
}

// ── open_readonly dispatch ──────────────────────────────────────────

/// Opening an empty file (size=0, no hash) read-only succeeds.
#[test]
fn open_readonly_empty_file() {
    let hub = MockHub::new();
    hub.add_file("empty.txt", 0, None, None);
    // Need to set head response for revalidation
    hub.set_head(
        "empty.txt",
        Some(HeadFileInfo {
            xet_hash: None,
            etag: None,
            size: Some(0),
            last_modified: None,
        }),
    );
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "empty.txt").await.unwrap();
        let fh = vfs.open(attr.ino, false, false, None).await.unwrap();
        let (data, eof) = vfs.read(fh, 0, 100).await.unwrap();
        assert!(data.is_empty());
        assert!(eof);
        vfs.release(fh).await.unwrap();
    });
}

/// Opening a dirty file with staging file reads from local staging (advanced mode).
#[test]
fn open_readonly_dirty_staging() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("old_hash"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        // Open for write + truncate to create staging file
        let wfh = vfs.open(ino, true, true, None).await.unwrap();
        write_blocking(&vfs, ino, wfh, 0, b"local content").await.unwrap();
        vfs.release(wfh).await.unwrap();

        // Now open read-only — should read from staging file
        let rfh = vfs.open(ino, false, false, None).await.unwrap();
        let (data, _) = vfs.read(rfh, 0, 100).await.unwrap();
        assert_eq!(&data[..], b"local content");
        vfs.release(rfh).await.unwrap();
    });
}

/// Opening a non-Xet file (no xet_hash) uses HTTP download.
#[test]
fn open_readonly_http_download() {
    let hub = MockHub::new_repo();
    // File with no xet_hash but size > 0 — triggers HTTP download path
    hub.add_file("lfs_file.bin", 500, None, Some("oid_abc"));
    hub.set_head(
        "lfs_file.bin",
        Some(HeadFileInfo {
            xet_hash: None,
            etag: Some("oid_abc".to_string()),
            size: Some(500),
            last_modified: None,
        }),
    );
    let xet = MockXet::new();
    let (rt, vfs) = vfs_repo(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "lfs_file.bin").await.unwrap();
        // The HTTP download mock writes nothing, so open will succeed
        // (download_file_http is a no-op in MockHub), but the file will be empty.
        // The point is that the HTTP download path is exercised without error.
        let fh = vfs.open(attr.ino, false, false, None).await.unwrap();
        vfs.release(fh).await.unwrap();
    });
}

/// HTTP download failure returns EIO.
#[test]
fn open_readonly_http_download_fail() {
    let hub = MockHub::new_repo();
    hub.add_file("fail.bin", 500, None, Some("oid_abc"));
    hub.set_head(
        "fail.bin",
        Some(HeadFileInfo {
            xet_hash: None,
            etag: Some("oid_abc".to_string()),
            size: Some(500),
            last_modified: None,
        }),
    );
    hub.fail_next_download();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_repo(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "fail.bin").await.unwrap();
        let result = vfs.open(attr.ino, false, false, None).await;
        assert_eq!(result.unwrap_err(), libc::EIO);
    });
}

// ── open_advanced_write ─────────────────────────────────────────────

/// Advanced write with truncate creates empty staging file.
#[test]
fn open_advanced_write_truncate() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("old_hash"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        let fh = vfs.open(ino, true, true, None).await.unwrap();
        // File should be truncated to 0
        {
            let inodes = vfs.inode_table.read().unwrap();
            assert_eq!(inodes.get(ino).unwrap().size, 0);
            assert!(inodes.get(ino).unwrap().is_dirty());
        }
        vfs.release(fh).await.unwrap();
    });
}

/// Reusing a dirty staging file without truncate skips download.
#[test]
fn open_advanced_write_reuse_dirty_staging() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("old_hash"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        // First open: truncate + write
        let fh1 = vfs.open(ino, true, true, None).await.unwrap();
        write_blocking(&vfs, ino, fh1, 0, b"modified").await.unwrap();
        vfs.release(fh1).await.unwrap();

        // Second open without truncate: should reuse the existing staging file
        let fh2 = vfs.open(ino, true, false, None).await.unwrap();
        let (data, _) = vfs.read(fh2, 0, 100).await.unwrap();
        assert_eq!(&data[..], b"modified");
        vfs.release(fh2).await.unwrap();
    });
}

// ── setattr truncate ────────────────────────────────────────────────

/// setattr truncate to non-zero downloads remote then truncates staging.
#[test]
fn setattr_truncate_nonzero() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    xet.add_file("hash1", &[b'X'; 100]);
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        let new_attr = vfs.setattr(ino, Some(50), None, None, None, None, None).await.unwrap();
        assert_eq!(new_attr.size, 50);

        let inodes = vfs.inode_table.read().unwrap();
        let entry = inodes.get(ino).unwrap();
        assert!(entry.is_dirty());
    });
}

/// setattr on a nonexistent inode returns ENOENT (advanced mode required to reach the check).
#[test]
fn setattr_nonexistent_enoent() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let result = vfs.setattr(99999, Some(0), None, None, None, None, None).await;
        assert_eq!(result.unwrap_err(), libc::ENOENT);
    });
}

// ── rename_validate edge cases ──────────────────────────────────────

/// Renaming a file over a directory returns EISDIR.
#[test]
fn rename_file_over_dir_eisdir() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("h1"), None);
    hub.add_dir("target");
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let _ = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let _ = vfs.lookup(ROOT_INODE, "target").await.unwrap();
        let result = vfs.rename(ROOT_INODE, "file.txt", ROOT_INODE, "target", false).await;
        assert_eq!(result.unwrap_err(), libc::EISDIR);
    });
}

/// Renaming a directory over a file returns ENOTDIR.
#[test]
fn rename_dir_over_file_enotdir() {
    let hub = MockHub::new();
    hub.add_dir("mydir");
    hub.add_file("file.txt", 100, Some("h1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let _ = vfs.lookup(ROOT_INODE, "mydir").await.unwrap();
        let _ = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let result = vfs.rename(ROOT_INODE, "mydir", ROOT_INODE, "file.txt", false).await;
        assert_eq!(result.unwrap_err(), libc::ENOTDIR);
    });
}

/// Renaming a directory over a non-empty directory returns ENOTEMPTY.
#[test]
fn rename_dir_over_nonempty_dir() {
    let hub = MockHub::new();
    hub.add_dir("src");
    hub.add_dir("dst");
    hub.add_file("dst/child.txt", 10, Some("h1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let _ = vfs.lookup(ROOT_INODE, "src").await.unwrap();
        let dst = vfs.lookup(ROOT_INODE, "dst").await.unwrap();
        // Load dst's children so the emptiness check sees the child
        let _ = vfs.readdir(dst.ino).await.unwrap();
        let result = vfs.rename(ROOT_INODE, "src", ROOT_INODE, "dst", false).await;
        assert_eq!(result.unwrap_err(), libc::ENOTEMPTY);
    });
}

/// Renaming a source that doesn't exist returns ENOENT.
#[test]
fn rename_source_enoent() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let result = vfs.rename(ROOT_INODE, "nope.txt", ROOT_INODE, "dest.txt", false).await;
        assert_eq!(result.unwrap_err(), libc::ENOENT);
    });
}

// ── read range / prefetch ───────────────────────────────────────────

/// Sequential reads from a xet file trigger streaming prefetch then range reads.
#[test]
fn read_sequential_xet_file() {
    let hub = MockHub::new();
    let content = vec![b'A'; 8192];
    hub.add_file("big.bin", content.len() as u64, Some("big_hash"), None);
    let xet = MockXet::new();
    xet.add_file("big_hash", &content);
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "big.bin").await.unwrap();
        let fh = vfs.open(attr.ino, false, false, None).await.unwrap();

        // Read in two chunks
        let (data1, _) = vfs.read(fh, 0, 4096).await.unwrap();
        assert_eq!(data1.len(), 4096);
        assert!(data1.iter().all(|&b| b == b'A'));

        let (data2, _) = vfs.read(fh, 4096, 4096).await.unwrap();
        assert_eq!(data2.len(), 4096);

        vfs.release(fh).await.unwrap();
    });
}

/// Reading past EOF returns empty data.
#[test]
fn read_past_eof() {
    let hub = MockHub::new();
    hub.add_file("small.txt", 5, Some("small_hash"), None);
    let xet = MockXet::new();
    xet.add_file("small_hash", b"hello");
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "small.txt").await.unwrap();
        let fh = vfs.open(attr.ino, false, false, None).await.unwrap();

        let (data, eof) = vfs.read(fh, 100, 100).await.unwrap();
        assert!(data.is_empty());
        assert!(eof);

        vfs.release(fh).await.unwrap();
    });
}

/// Range read (seek backward) falls back to a temporary stream.
#[test]
fn read_seek_backward() {
    let hub = MockHub::new();
    let content: Vec<u8> = (0..128).collect();
    hub.add_file("data.bin", content.len() as u64, Some("data_hash"), None);
    let xet = MockXet::new();
    xet.add_file("data_hash", &content);
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "data.bin").await.unwrap();
        let fh = vfs.open(attr.ino, false, false, None).await.unwrap();

        // Read from offset 64 first
        let (data1, _) = vfs.read(fh, 64, 32).await.unwrap();
        assert_eq!(data1[0], 64);

        // Then seek backward to offset 0
        let (data2, _) = vfs.read(fh, 0, 32).await.unwrap();
        assert_eq!(data2[0], 0);
        assert_eq!(data2.len(), 32);

        vfs.release(fh).await.unwrap();
    });
}

/// Range download that fails once then succeeds on retry.
#[test]
fn read_range_retry_on_error() {
    let hub = MockHub::new();
    let content: Vec<u8> = (0..=255).collect();
    hub.add_file("data.bin", content.len() as u64, Some("h1"), None);
    let xet = MockXet::new();
    xet.add_file("h1", &content);
    // First range download fails, second succeeds
    xet.fail_range_downloads(1);
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "data.bin").await.unwrap();
        let fh = vfs.open(attr.ino, false, false, None).await.unwrap();

        // Read at non-zero offset to force range download (not streaming)
        let (data, _) = vfs.read(fh, 64, 32).await.unwrap();
        assert_eq!(data[0], 64);
        assert_eq!(data.len(), 32);

        vfs.release(fh).await.unwrap();
    });
}

/// Range download that returns empty once then succeeds on retry.
#[test]
fn read_range_retry_on_empty() {
    let hub = MockHub::new();
    let content: Vec<u8> = (0..=255).collect();
    hub.add_file("data.bin", content.len() as u64, Some("h2"), None);
    let xet = MockXet::new();
    xet.add_file("h2", &content);
    // First range download returns empty, second succeeds
    xet.empty_range_downloads(1);
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "data.bin").await.unwrap();
        let fh = vfs.open(attr.ino, false, false, None).await.unwrap();

        let (data, _) = vfs.read(fh, 64, 32).await.unwrap();
        assert_eq!(data[0], 64);
        assert_eq!(data.len(), 32);

        vfs.release(fh).await.unwrap();
    });
}

/// Range download that fails all 3 attempts returns EIO.
#[test]
fn read_range_all_retries_exhausted() {
    let hub = MockHub::new();
    let content: Vec<u8> = (0..=255).collect();
    hub.add_file("data.bin", content.len() as u64, Some("h3"), None);
    let xet = MockXet::new();
    xet.add_file("h3", &content);
    // All 3 attempts fail
    xet.fail_range_downloads(3);
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "data.bin").await.unwrap();
        let fh = vfs.open(attr.ino, false, false, None).await.unwrap();

        let result = vfs.read(fh, 64, 32).await;
        assert_eq!(result.unwrap_err(), libc::EIO);

        vfs.release(fh).await.unwrap();
    });
}

// ── advanced write local read/write ─────────────────────────────────

/// Advanced mode write at arbitrary offset (random write).
#[test]
fn advanced_write_random_offset() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("hash1"), None);
    let xet = MockXet::new();
    xet.add_file("hash1", b"0123456789");
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        // Open without truncate — downloads remote content
        let fh = vfs.open(ino, true, false, None).await.unwrap();

        // Overwrite bytes 5..8
        write_blocking(&vfs, ino, fh, 5, b"ABC").await.unwrap();

        // Read back full content
        let (data, _) = vfs.read(fh, 0, 20).await.unwrap();
        assert_eq!(&data[..], b"01234ABC89");

        vfs.release(fh).await.unwrap();
    });
}

// ── unlink cleans staging file ──────────────────────────────────────

/// unlink() in advanced mode removes the staging file.
#[test]
fn unlink_cleans_staging() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 0, None, None);
    hub.set_head(
        "file.txt",
        Some(HeadFileInfo {
            xet_hash: None,
            etag: None,
            size: Some(0),
            last_modified: None,
        }),
    );
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        // Create a staging file by opening for write
        let fh = vfs.open(ino, true, true, None).await.unwrap();
        write_blocking(&vfs, ino, fh, 0, b"data").await.unwrap();
        vfs.release(fh).await.unwrap();

        // Unlink — should also clean up staging
        vfs.unlink(ROOT_INODE, "file.txt").await.unwrap();

        // Verify inode is gone
        let result = vfs.lookup(ROOT_INODE, "file.txt").await;
        assert_eq!(result.unwrap_err(), libc::ENOENT);
    });
}

// ── poll_remote_changes ─────────────────────────────────────────────

/// poll_remote_changes calls probe_revision once per round and skips the
/// tree-listing fan-out when the value is unchanged. When the revision
/// advances, list_tree fires again. When the probe returns a non-401 error,
/// the loop falls back to a full fan-out.
#[test]
fn poll_skips_list_tree_when_revision_unchanged() {
    let hub = MockHub::new_repo();
    hub.add_file("file.txt", 10, Some("h1"), None);
    hub.set_revision("rev-a");
    let xet = MockXet::new();
    let (rt, vfs) = vfs_repo(&hub, &xet);

    rt.block_on(async {
        // Mark root as loaded so loaded_dir_prefixes() returns at least one prefix.
        let _ = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        // The lookup above triggered ensure_children_loaded -> 1 list_tree call.
        let baseline_list_tree = hub.list_tree_call_count();

        let stop = Arc::new(tokio::sync::Notify::new());
        let stop_clone = stop.clone();
        let hub_dyn: Arc<dyn crate::hub_api::HubOps> = hub.clone();
        let inodes = vfs.inode_table.clone();
        let neg = vfs.negative_cache.clone();
        let inv = vfs.invalidator.clone();
        let handle = tokio::spawn(async move {
            tokio::select! {
                _ = VirtualFs::poll_remote_changes(
                    hub_dyn, inodes, neg, inv,
                    Duration::from_millis(10), 4,
                ) => {}
                _ = stop_clone.notified() => {}
            }
        });

        // Let 5 rounds run with identical revision -> probe fires, list_tree does not.
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(hub.probe_revision_call_count() >= 3, "probe should run each round");
        assert_eq!(
            hub.list_tree_call_count(),
            baseline_list_tree,
            "list_tree must not fire when revision is unchanged"
        );

        // Advance the revision -> next round fans out.
        let probes_before = hub.probe_revision_call_count();
        hub.set_revision("rev-b");
        // Wait until at least one more probe + one fan-out.
        for _ in 0..50 {
            if hub.probe_revision_call_count() > probes_before && hub.list_tree_call_count() > baseline_list_tree {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            hub.list_tree_call_count() > baseline_list_tree,
            "list_tree must fire after revision change"
        );

        // Probe error (non-401) -> fall back to full fan-out.
        let lists_before = hub.list_tree_call_count();
        hub.fail_revision(None, "simulated probe failure");
        for _ in 0..50 {
            if hub.list_tree_call_count() > lists_before {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            hub.list_tree_call_count() > lists_before,
            "list_tree must fire when probe fails (fallback)"
        );

        stop.notify_one();
        let _ = handle.await;
    });
}

/// Revalidation detects remote file content change (hash changed via HEAD).
#[test]
fn revalidation_detects_hash_change() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash_v1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        // Load the file into inode table
        let _ = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();

        // Simulate remote change: update HEAD response
        hub.set_head(
            "file.txt",
            Some(HeadFileInfo {
                xet_hash: Some("hash_v2".to_string()),
                etag: None,
                size: Some(200),
                last_modified: None,
            }),
        );

        // Lookup triggers revalidation → detects hash change
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let inodes = vfs.inode_table.read().unwrap();
        let entry = inodes.get(attr.ino).unwrap();
        assert_eq!(entry.xet_hash.as_deref(), Some("hash_v2"));
        assert_eq!(entry.size, 200);
    });
}

/// Revalidation detects remote deletion (HEAD returns None).
#[test]
fn poll_detects_remote_deletion_via_revalidation() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let _ = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();

        // Simulate remote deletion
        hub.remove_file("file.txt");
        hub.set_head("file.txt", None);

        // Lookup again triggers revalidation → detects deletion
        let result = vfs.lookup(ROOT_INODE, "file.txt").await;
        assert_eq!(result.unwrap_err(), libc::ENOENT);
    });
}

/// Dirty files are not overwritten by revalidation (uses advanced mode to avoid streaming hangs).
#[test]
fn revalidation_skips_dirty_files() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        // Make the file dirty via advanced write (truncate)
        let fh = vfs.open(ino, true, true, None).await.unwrap();
        write_blocking(&vfs, ino, fh, 0, b"local").await.unwrap();
        vfs.release(fh).await.unwrap();

        // Change remote HEAD response
        hub.set_head(
            "file.txt",
            Some(HeadFileInfo {
                xet_hash: Some("hash_v2".to_string()),
                etag: None,
                size: Some(500),
                last_modified: None,
            }),
        );

        // Lookup triggers revalidation — but dirty files skip it
        let attr2 = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let inodes = vfs.inode_table.read().unwrap();
        let entry = inodes.get(attr2.ino).unwrap();
        assert!(entry.is_dirty());
        // Hash and size must remain unchanged (not overwritten by remote)
        assert_eq!(entry.xet_hash.as_deref(), Some("hash1"));
        assert_ne!(entry.size, 500, "dirty file size should not be updated from remote");
    });
}

/// Poll skips unloaded directories: files under unexplored dirs don't trigger invalidation.
#[test]
fn poll_skips_unloaded_directories() {
    let hub = MockHub::new_repo();
    hub.add_file("root.txt", 10, Some("h1"), None);
    hub.add_file("a/deep/file.txt", 20, Some("h2"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_repo(&hub, &xet);

    rt.block_on(async {
        // Only explore root — dir "a" is known but "a/deep" is NOT loaded.
        let _ = vfs.lookup(ROOT_INODE, "root.txt").await.unwrap();
        let a = vfs.lookup(ROOT_INODE, "a").await.unwrap();

        // Confirm "a" children are NOT loaded (never did readdir/lookup inside "a").
        {
            let inodes = vfs.inode_table.read().unwrap();
            assert!(!inodes.is_children_loaded(a.ino), "dir 'a' should not be loaded yet");
            assert!(inodes.is_children_loaded(ROOT_INODE), "root should be loaded");
        }

        // Simulate a poll cycle (same as poll_remote_changes: list each loaded prefix).
        let prefixes = vfs.inode_table.read().unwrap().loaded_dir_prefixes();
        let mut remote = Vec::new();
        for prefix in &prefixes {
            remote.extend(hub.list_tree(prefix).await.unwrap());
        }
        let polled: std::collections::HashSet<String> = prefixes.into_iter().collect();
        VirtualFs::apply_poll_diff(remote, &polled, &vfs.inode_table, &vfs.negative_cache, &vfs.invalidator);

        // Dir "a" should still NOT have children_loaded set (unchanged).
        // Root should still be loaded (not invalidated).
        {
            let inodes = vfs.inode_table.read().unwrap();
            assert!(
                !inodes.is_children_loaded(a.ino),
                "dir 'a' should still be unloaded after poll"
            );
            assert!(
                inodes.is_children_loaded(ROOT_INODE),
                "root should still be loaded (no new root-level files)"
            );
        }
    });
}

/// Poll detects new files in loaded directories and invalidates them.
#[test]
fn poll_invalidates_loaded_dir_with_new_file() {
    let hub = MockHub::new_repo();
    hub.add_file("file.txt", 10, Some("h1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_repo(&hub, &xet);

    rt.block_on(async {
        // Load root fully.
        let _ = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        assert!(vfs.inode_table.read().unwrap().is_children_loaded(ROOT_INODE));

        // Add a new file remotely.
        hub.add_file("new.txt", 50, Some("h2"), None);

        let prefixes = vfs.inode_table.read().unwrap().loaded_dir_prefixes();
        let mut remote = Vec::new();
        for prefix in &prefixes {
            remote.extend(hub.list_tree(prefix).await.unwrap());
        }
        let polled: std::collections::HashSet<String> = prefixes.into_iter().collect();
        VirtualFs::apply_poll_diff(remote, &polled, &vfs.inode_table, &vfs.negative_cache, &vfs.invalidator);

        // Root should be invalidated because it's loaded and has a new file.
        assert!(
            !vfs.inode_table.read().unwrap().is_children_loaded(ROOT_INODE),
            "root should be invalidated after new file detected"
        );
    });
}

/// Poll detects a remote file update (hash change) in a loaded directory.
#[test]
fn poll_detects_file_update_in_loaded_dir() {
    let hub = MockHub::new_repo();
    hub.add_file("data.txt", 10, Some("old_hash"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_repo(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "data.txt").await.unwrap();
        let ino = attr.ino;
        assert_eq!(attr.size, 10);

        // Simulate remote update: same path, new hash and size.
        hub.add_file("data.txt", 99, Some("new_hash"), None);

        let prefixes = vfs.inode_table.read().unwrap().loaded_dir_prefixes();
        let mut remote = Vec::new();
        for prefix in &prefixes {
            remote.extend(hub.list_tree(prefix).await.unwrap());
        }
        let polled: std::collections::HashSet<String> = prefixes.into_iter().collect();
        VirtualFs::apply_poll_diff(remote, &polled, &vfs.inode_table, &vfs.negative_cache, &vfs.invalidator);

        let inodes = vfs.inode_table.read().unwrap();
        let entry = inodes.get(ino).unwrap();
        assert_eq!(entry.size, 99, "poll should update size");
        assert_eq!(entry.xet_hash.as_deref(), Some("new_hash"), "poll should update hash");
    });
}

/// Regression for #160: poll calls the invalidator with the changed inode.
///
/// The NFS adapter relies on this callback to evict its pooled file handle —
/// pooled handles bind to the xet_hash captured at open() time, so without
/// eviction subsequent reads serve pre-update content while `stat` already
/// reports the new size.
#[test]
fn poll_calls_invalidator_for_updated_file() {
    let hub = MockHub::new_repo();
    hub.add_file("data.txt", 10, Some("old_hash"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_repo(&hub, &xet);

    let invalidated: Arc<std::sync::Mutex<Vec<u64>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    {
        let sink = invalidated.clone();
        vfs.set_invalidator(Box::new(move |ino| {
            sink.lock().unwrap().push(ino);
        }));
    }

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "data.txt").await.unwrap();
        let ino = attr.ino;

        // Remote update: same path, new hash and size.
        hub.add_file("data.txt", 99, Some("new_hash"), None);

        let prefixes = vfs.inode_table.read().unwrap().loaded_dir_prefixes();
        let mut remote = Vec::new();
        for prefix in &prefixes {
            remote.extend(hub.list_tree(prefix).await.unwrap());
        }
        let polled: std::collections::HashSet<String> = prefixes.into_iter().collect();
        VirtualFs::apply_poll_diff(remote, &polled, &vfs.inode_table, &vfs.negative_cache, &vfs.invalidator);

        let calls = invalidated.lock().unwrap();
        assert!(
            calls.contains(&ino),
            "invalidator must be called for the updated inode (got {:?})",
            *calls,
        );
    });
}

/// Poll detects a remote file deletion in a loaded directory.
#[test]
fn poll_detects_file_deletion_in_loaded_dir() {
    let hub = MockHub::new_repo();
    hub.add_file("ephemeral.txt", 10, Some("h1"), None);
    hub.add_file("keeper.txt", 20, Some("h2"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_repo(&hub, &xet);

    rt.block_on(async {
        let _ = vfs.lookup(ROOT_INODE, "ephemeral.txt").await.unwrap();
        let _ = vfs.lookup(ROOT_INODE, "keeper.txt").await.unwrap();

        // Remove the file remotely.
        hub.remove_file("ephemeral.txt");

        let prefixes = vfs.inode_table.read().unwrap().loaded_dir_prefixes();
        let mut remote = Vec::new();
        for prefix in &prefixes {
            remote.extend(hub.list_tree(prefix).await.unwrap());
        }
        let polled: std::collections::HashSet<String> = prefixes.into_iter().collect();
        VirtualFs::apply_poll_diff(remote, &polled, &vfs.inode_table, &vfs.negative_cache, &vfs.invalidator);

        // ephemeral.txt should be gone, keeper.txt should remain.
        assert_eq!(vfs.lookup(ROOT_INODE, "ephemeral.txt").await.unwrap_err(), libc::ENOENT);
        vfs.lookup(ROOT_INODE, "keeper.txt").await.unwrap();
    });
}

/// Poll skips deletion of inodes with open file handles.
#[test]
fn poll_skips_deletion_with_open_handles() {
    let hub = MockHub::new_repo();
    hub.add_file("open.txt", 10, Some("h1"), None);
    hub.add_file("closed.txt", 20, Some("h2"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_repo(&hub, &xet);

    rt.block_on(async {
        let open_attr = vfs.lookup(ROOT_INODE, "open.txt").await.unwrap();
        let _ = vfs.lookup(ROOT_INODE, "closed.txt").await.unwrap();

        // Open a handle on open.txt (lazy read handle)
        let fh = vfs.open(open_attr.ino, false, false, None).await.unwrap();

        // Remove both files remotely
        hub.remove_file("open.txt");
        hub.remove_file("closed.txt");

        let prefixes = vfs.inode_table.read().unwrap().loaded_dir_prefixes();
        let mut remote = Vec::new();
        for prefix in &prefixes {
            remote.extend(hub.list_tree(prefix).await.unwrap());
        }
        let polled: std::collections::HashSet<String> = prefixes.into_iter().collect();
        VirtualFs::apply_poll_diff(remote, &polled, &vfs.inode_table, &vfs.negative_cache, &vfs.invalidator);

        // closed.txt should be deleted (no open handles)
        assert_eq!(vfs.lookup(ROOT_INODE, "closed.txt").await.unwrap_err(), libc::ENOENT);

        // open.txt: inode survives as orphan (nlink=0), but path is unlinked
        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(open_attr.ino).expect("orphan inode should survive");
            assert_eq!(entry.nlink, 0, "inode should be orphaned (nlink=0)");
            assert!(
                inodes.lookup_child(ROOT_INODE, "open.txt").is_none(),
                "open.txt should not be visible by name"
            );
        }

        // Release the handle: release() cleans up the orphan
        vfs.release(fh).await.unwrap();

        let inodes = vfs.inode_table.read().unwrap();
        assert!(
            inodes.get(open_attr.ino).is_none(),
            "orphan should be removed after release"
        );
    });
}

/// Poll with multiple loaded directories fetches each independently.
#[test]
fn poll_multiple_loaded_dirs() {
    let hub = MockHub::new_repo();
    hub.add_file("root.txt", 10, Some("h1"), None);
    hub.add_file("sub/nested.txt", 20, Some("h2"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_repo(&hub, &xet);

    rt.block_on(async {
        // Load both root and sub directory via readdir — lookup alone no
        // longer populates the full children listing (it uses HEAD for point
        // resolution), but the poll loop only watches dirs that have been
        // readdir'd.
        let _ = vfs.readdir(ROOT_INODE).await.unwrap();
        let sub = vfs.lookup(ROOT_INODE, "sub").await.unwrap();
        let _ = vfs.readdir(sub.ino).await.unwrap();

        // Both dirs should be loaded.
        {
            let inodes = vfs.inode_table.read().unwrap();
            assert!(inodes.is_children_loaded(ROOT_INODE));
            assert!(inodes.is_children_loaded(sub.ino));
        }

        // Update file in sub, add file in root.
        hub.add_file("sub/nested.txt", 99, Some("h2_new"), None);
        hub.add_file("new_root.txt", 5, Some("h3"), None);

        let prefixes = vfs.inode_table.read().unwrap().loaded_dir_prefixes();
        assert!(prefixes.len() >= 2, "should poll at least 2 dirs");

        let mut remote = Vec::new();
        for prefix in &prefixes {
            remote.extend(hub.list_tree(prefix).await.unwrap());
        }
        let polled: std::collections::HashSet<String> = prefixes.into_iter().collect();
        VirtualFs::apply_poll_diff(remote, &polled, &vfs.inode_table, &vfs.negative_cache, &vfs.invalidator);

        // sub/nested.txt should be updated.
        let nested_ino = vfs.lookup(sub.ino, "nested.txt").await.unwrap().ino;
        let inodes = vfs.inode_table.read().unwrap();
        let entry = inodes.get(nested_ino).unwrap();
        assert_eq!(entry.size, 99);
        assert_eq!(entry.xet_hash.as_deref(), Some("h2_new"));
    });
}

/// Poll with a failed prefix fetch does not spuriously delete files in that dir.
#[test]
fn poll_failed_prefix_no_spurious_deletion() {
    let hub = MockHub::new_repo();
    hub.add_file("root.txt", 10, Some("h1"), None);
    hub.add_file("sub/file.txt", 20, Some("h2"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_repo(&hub, &xet);

    rt.block_on(async {
        // Load both dirs
        let _ = vfs.lookup(ROOT_INODE, "root.txt").await.unwrap();
        let sub = vfs.lookup(ROOT_INODE, "sub").await.unwrap();
        let _ = vfs.lookup(sub.ino, "file.txt").await.unwrap();

        // Simulate poll where "sub" prefix failed (only root succeeded)
        let root_entries = hub.list_tree("").await.unwrap();
        let polled: std::collections::HashSet<String> = ["".to_string()].into_iter().collect();
        VirtualFs::apply_poll_diff(
            root_entries,
            &polled,
            &vfs.inode_table,
            &vfs.negative_cache,
            &vfs.invalidator,
        );

        // sub/file.txt must NOT be deleted (its prefix wasn't polled)
        vfs.lookup(sub.ino, "file.txt").await.unwrap();
    });
}

/// Poll after invalidation does not delete files from invalidated dirs.
#[test]
fn poll_after_invalidation_no_spurious_deletion() {
    let hub = MockHub::new_repo();
    hub.add_file("a.txt", 10, Some("h1"), None);
    hub.add_file("b.txt", 20, Some("h2"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_repo(&hub, &xet);

    rt.block_on(async {
        let _ = vfs.lookup(ROOT_INODE, "a.txt").await.unwrap();
        let _ = vfs.lookup(ROOT_INODE, "b.txt").await.unwrap();

        // First poll: add a new file → root gets invalidated
        hub.add_file("new.txt", 5, Some("h3"), None);
        let prefixes = vfs.inode_table.read().unwrap().loaded_dir_prefixes();
        let mut remote = Vec::new();
        for prefix in &prefixes {
            remote.extend(hub.list_tree(prefix).await.unwrap());
        }
        let polled: std::collections::HashSet<String> = prefixes.into_iter().collect();
        VirtualFs::apply_poll_diff(remote, &polled, &vfs.inode_table, &vfs.negative_cache, &vfs.invalidator);

        // Root is now invalidated (children_loaded=false).
        // Second poll: root is NOT in loaded_dir_prefixes anymore.
        let prefixes2 = vfs.inode_table.read().unwrap().loaded_dir_prefixes();
        let mut remote2 = Vec::new();
        for prefix in &prefixes2 {
            remote2.extend(hub.list_tree(prefix).await.unwrap());
        }
        let polled2: std::collections::HashSet<String> = prefixes2.into_iter().collect();
        VirtualFs::apply_poll_diff(
            remote2,
            &polled2,
            &vfs.inode_table,
            &vfs.negative_cache,
            &vfs.invalidator,
        );

        // a.txt and b.txt must still exist (not spuriously deleted)
        let inodes = vfs.inode_table.read().unwrap();
        assert!(inodes.get_by_path("a.txt").is_some(), "a.txt must survive invalidation");
        assert!(inodes.get_by_path("b.txt").is_some(), "b.txt must survive invalidation");
    });
}

/// When a subdirectory is deleted remotely, its files are cleaned up.
/// The parent listing no longer shows the dir, so its prefix is marked
/// as polled (deleted dir) and its files are removed.
#[test]
fn poll_detects_deleted_subdirectory() {
    let hub = MockHub::new_repo();
    hub.add_file("root.txt", 10, Some("h1"), None);
    hub.add_file("sub/child.txt", 20, Some("h2"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_repo(&hub, &xet);

    rt.block_on(async {
        // Load both dirs
        let _ = vfs.lookup(ROOT_INODE, "root.txt").await.unwrap();
        let sub = vfs.lookup(ROOT_INODE, "sub").await.unwrap();
        let _ = vfs.lookup(sub.ino, "child.txt").await.unwrap();

        // Delete the subdir remotely
        hub.remove_file("sub/child.txt");

        // Simulate poll: root succeeds, "sub" fails (404 → removed from Hub).
        // The parent listing (root) no longer shows "sub" as a directory.
        let root_entries = hub.list_tree("").await.unwrap();
        // "sub" is not in root_entries anymore (no files under "sub/" means no dir entry).
        // Mark "sub" as polled since its parent confirmed it's gone.
        let polled: std::collections::HashSet<String> = ["".to_string(), "sub".to_string()].into_iter().collect();

        // Also include root entries
        VirtualFs::apply_poll_diff(
            root_entries,
            &polled,
            &vfs.inode_table,
            &vfs.negative_cache,
            &vfs.invalidator,
        );

        // sub/child.txt should be deleted
        let inodes = vfs.inode_table.read().unwrap();
        assert!(
            inodes.get_by_path("sub/child.txt").is_none(),
            "deleted dir's files must be removed"
        );
    });
}

// ── negative cache ──────────────────────────────────────────────────

/// Negative cache prevents repeated ENOENT lookups from hitting the Hub.
#[test]
fn negative_cache_prevents_repeated_lookups() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        // First lookup: ENOENT, populates negative cache
        let r1 = vfs.lookup(ROOT_INODE, "missing.txt").await;
        assert_eq!(r1.unwrap_err(), libc::ENOENT);
        assert!(
            vfs.negative_cache_check("missing.txt"),
            "path should be in negative cache"
        );

        // Second lookup: should hit negative cache (no Hub call)
        let r2 = vfs.lookup(ROOT_INODE, "missing.txt").await;
        assert_eq!(r2.unwrap_err(), libc::ENOENT);
    });
}

/// create() clears negative cache entry for the created path.
#[test]
fn negative_cache_cleared_on_create() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        // Populate negative cache
        let _ = vfs.lookup(ROOT_INODE, "new.txt").await;

        // Create the file
        let (attr, fh) = vfs
            .create(ROOT_INODE, "new.txt", 0o644, 1000, 1000, Some(1))
            .await
            .unwrap();

        // Lookup should now succeed (negative cache was cleared)
        let found = vfs.lookup(ROOT_INODE, "new.txt").await.unwrap();
        assert_eq!(found.ino, attr.ino);

        vfs.release(fh).await.unwrap();
    });
}

/// write() on a read-only VFS returns EROFS.
#[test]
fn write_readonly_vfs_erofs() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("h1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_readonly(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        // Can't even open writable on RO vfs, so use a fake fh
        let result = write_blocking(&vfs, attr.ino, 99999, 0, b"x").await;
        assert_eq!(result.unwrap_err(), libc::EROFS);
    });
}

// ── getattr ─────────────────────────────────────────────────────────

/// getattr on a file returns correct attributes.
#[test]
fn getattr_file() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 42, Some("h1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let lookup = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let attr = vfs.getattr(lookup.ino).unwrap();
        assert_eq!(attr.size, 42);
        assert_eq!(attr.kind, InodeKind::File);
        assert_eq!(attr.perm, 0o644);
    });
}

// ── mkdir ───────────────────────────────────────────────────────────

/// mkdir creates a new directory with children_loaded=true.
#[test]
fn mkdir_children_loaded() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.mkdir(ROOT_INODE, "newdir", 0o755, 1000, 1000).await.unwrap();
        assert_eq!(attr.kind, InodeKind::Directory);

        // Should be able to readdir immediately (children_loaded = true)
        let entries = vfs.readdir(attr.ino).await.unwrap();
        // Only . and ..
        assert_eq!(entries.len(), 2);
    });
}

/// mkdir on a nonexistent parent returns ENOENT.
#[test]
fn mkdir_parent_enoent() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let result = vfs.mkdir(99999, "newdir", 0o755, 1000, 1000).await;
        assert_eq!(result.unwrap_err(), libc::ENOENT);
    });
}

// ── ensure_subtree_loaded ───────────────────────────────────────────

/// Renaming a directory with nested children moves all descendants.
/// Uses serve_lookup_from_cache to avoid revalidation interference.
#[test]
fn rename_dir_with_nested_children() {
    let hub = MockHub::new();
    hub.add_dir("a");
    hub.add_dir("a/b");
    hub.add_file("a/b/deep.txt", 10, Some("h1"), None);
    let xet = MockXet::new();
    let rt = new_runtime();
    let vfs = make_test_vfs(
        hub.clone(),
        xet.clone(),
        TestOpts {
            serve_lookup_from_cache: true,
            metadata_ttl: Duration::from_secs(600),
            ..Default::default()
        },
        &rt,
    );

    rt.block_on(async {
        // Pre-load the full tree
        let a = vfs.lookup(ROOT_INODE, "a").await.unwrap();
        let b = vfs.lookup(a.ino, "b").await.unwrap();
        let deep = vfs.lookup(b.ino, "deep.txt").await.unwrap();
        assert_eq!(deep.size, 10);

        // Rename "a" -> "x"
        vfs.rename(ROOT_INODE, "a", ROOT_INODE, "x", false).await.unwrap();

        // Descendants should still be accessible under new name
        let x = vfs.lookup(ROOT_INODE, "x").await.unwrap();
        assert_eq!(x.ino, a.ino);

        let b2 = vfs.lookup(x.ino, "b").await.unwrap();
        let deep2 = vfs.lookup(b2.ino, "deep.txt").await.unwrap();
        assert_eq!(deep2.size, 10);
    });
}

// ── short-read protection ────────────────────────────────────────────

/// Reads at a prefetch buffer boundary must NOT return a short read (fewer
/// bytes than the kernel requested) unless the read reaches real EOF.
/// The Linux FUSE kernel module shrinks i_size on short reads
/// (fuse_read_update_size), which truncates the file from the app's view.
#[test]
fn read_no_short_read_at_buffer_boundary() {
    let hub = MockHub::new();
    // File larger than one prefetch window (8 MiB). We use 10 MiB so the
    // first window fills ~8 MiB and the second read at the boundary must
    // fetch more rather than returning a short read.
    let file_size: usize = 10 * 1024 * 1024;
    let content: Vec<u8> = (0..file_size).map(|i| (i % 251) as u8).collect();
    hub.add_file("big.bin", file_size as u64, Some("big_hash"), None);
    let xet = MockXet::new();
    xet.add_file("big_hash", &content);
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "big.bin").await.unwrap();
        let fh = vfs.open(attr.ino, false, false, None).await.unwrap();

        // Simulate sequential reads of 128 KiB (typical FUSE read size).
        // This will exhaust the first 8 MiB prefetch window. The read that
        // crosses the boundary must return a full 128 KiB, not a short read.
        let chunk_size: u32 = 128 * 1024;
        let mut offset: u64 = 0;
        while offset < file_size as u64 {
            let (data, eof) = vfs.read(fh, offset, chunk_size).await.unwrap();
            let remaining = file_size as u64 - offset;
            if remaining >= chunk_size as u64 {
                // Must return full chunk, never a short read
                assert_eq!(
                    data.len(),
                    chunk_size as usize,
                    "short read at offset {}: got {} bytes, expected {} (file_size={})",
                    offset,
                    data.len(),
                    chunk_size,
                    file_size,
                );
                // eof may be true when this read reaches exactly file_size
                if remaining > chunk_size as u64 {
                    assert!(!eof, "unexpected eof at offset {} (remaining={})", offset, remaining);
                }
            } else {
                // Final read at EOF — short is fine
                assert_eq!(data.len(), remaining as usize);
                assert!(eof);
            }
            // Verify content correctness
            for (i, &b) in data.iter().enumerate() {
                let expected = ((offset as usize + i) % 251) as u8;
                assert_eq!(b, expected, "wrong byte at offset {}", offset as usize + i);
            }
            offset += data.len() as u64;
        }
        assert_eq!(offset, file_size as u64);

        vfs.release(fh).await.unwrap();
    });
}

// ── shutdown ────────────────────────────────────────────────────────

/// shutdown() with dirty files flushes them before completing.
#[test]
fn shutdown_flushes_dirty() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let (attr, fh) = vfs
            .create(ROOT_INODE, "dirty.txt", 0o644, 1000, 1000, None)
            .await
            .unwrap();
        write_blocking(&vfs, attr.ino, fh, 0, b"unflushed data").await.unwrap();
        vfs.release(fh).await.unwrap();
    });

    // File should be dirty
    {
        let inodes = vfs.inode_table.read().unwrap();
        let dirty_count = inodes.dirty_inos().len();
        assert!(dirty_count > 0);
    }

    // Shutdown should flush dirty files
    vfs.shutdown();

    // After shutdown, batch log should show the flush
    let logs = hub.take_batch_log();
    assert!(!logs.is_empty());
}

/// Helper: create VFS with advanced writes + staging GC enabled (low limit).
fn vfs_advanced_with_gc(
    hub: &std::sync::Arc<MockHub>,
    xet: &std::sync::Arc<MockXet>,
) -> (tokio::runtime::Runtime, std::sync::Arc<VirtualFs>) {
    let rt = new_runtime();
    let vfs = make_test_vfs(
        hub.clone(),
        xet.clone(),
        TestOpts {
            advanced_writes: true,
            max_staging_size: 1, // 1 byte = always over limit, GC always runs
            ..Default::default()
        },
        &rt,
    );
    (rt, vfs)
}

/// After a successful flush, the staging file for a clean inode should be
/// garbage-collected (deleted from disk).
#[test]
fn staging_gc_after_flush() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced_with_gc(&hub, &xet);

    rt.block_on(async {
        let (attr, fh) = vfs
            .create(ROOT_INODE, "gc_test.txt", 0o644, 1000, 1000, None)
            .await
            .unwrap();
        let ino = attr.ino;

        write_blocking(&vfs, ino, fh, 0, b"some data").await.unwrap();

        // Staging file should exist before release
        let staging_path = vfs.staging.dir().unwrap().path(ino);
        assert!(staging_path.exists(), "staging file should exist after write");

        vfs.release(fh).await.unwrap();

        // Wait for flush debounce (100ms) + processing
        tokio::time::sleep(Duration::from_secs(3)).await;

        // After flush, inode should be clean
        let is_clean = vfs.inode_table.read().unwrap().get(ino).is_some_and(|e| !e.is_dirty());
        assert!(is_clean, "inode should be clean after flush");

        // Staging file should have been GC'd
        assert!(
            !staging_path.exists(),
            "staging file should be deleted after successful flush"
        );
    });
}

/// After fsync schedules a commit and the handle is released, the staging file
/// should be GC'd once the background flush completes (inode becomes clean).
#[test]
fn staging_gc_after_fsync_then_release() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced_with_gc(&hub, &xet);

    rt.block_on(async {
        let (attr, fh) = vfs
            .create(ROOT_INODE, "fsync_gc.txt", 0o644, 1000, 1000, None)
            .await
            .unwrap();
        let ino = attr.ino;

        write_blocking(&vfs, ino, fh, 0, b"fsync data").await.unwrap();

        let staging_path = vfs.staging.dir().unwrap().path(ino);
        assert!(staging_path.exists(), "staging file should exist after write");

        // fsync is async (schedules a flush); staging must survive while the handle is open.
        vfs.fsync(ino, fh, None).await.unwrap();
        assert!(
            staging_path.exists(),
            "staging file should survive fsync (handle still open)"
        );

        // Release the handle and wait for the background flush + GC to complete.
        vfs.release(fh).await.unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await;

        let is_clean = vfs.inode_table.read().unwrap().get(ino).is_some_and(|e| !e.is_dirty());
        assert!(is_clean, "inode should be clean after flush");
        assert!(
            !staging_path.exists(),
            "staging file should be GC'd after flush completes"
        );
    });
}

/// Partial reclaim: GC stops as soon as usage drops back under the limit
/// instead of evicting every candidate. Validates the "while over_limit"
/// loop, not a blanket sweep.
#[test]
fn staging_gc_stops_once_under_limit() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let rt = new_runtime();
    // 10-byte budget; each file below is 8 bytes — room for exactly one.
    let vfs = make_test_vfs(
        hub.clone(),
        xet.clone(),
        TestOpts {
            advanced_writes: true,
            max_staging_size: 10,
            ..Default::default()
        },
        &rt,
    );

    rt.block_on(async {
        for i in 0..3 {
            let name = format!("f{i}.txt");
            let (attr, fh) = vfs.create(ROOT_INODE, &name, 0o644, 1000, 1000, None).await.unwrap();
            write_blocking(&vfs, attr.ino, fh, 0, b"xxxxxxxx").await.unwrap();
            vfs.release(fh).await.unwrap();
        }
        tokio::time::sleep(Duration::from_secs(3)).await;

        let sd = vfs.staging.dir().unwrap();
        assert!(
            sd.bytes_used() <= 10,
            "usage should be at or below budget, got {}",
            sd.bytes_used()
        );
        assert!(
            sd.bytes_used() > 0,
            "GC should have stopped once under budget, not evicted everything"
        );
    });
}

/// LRU order: oldest-touched files are reclaimed first. Creates three files
/// sequentially (each successive `create` bumps the touch counter), flushes,
/// and verifies that only the most-recently-created staging file survives
/// under a one-file budget.
#[test]
fn staging_gc_evicts_least_recently_touched_first() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let rt = new_runtime();
    let vfs = make_test_vfs(
        hub.clone(),
        xet.clone(),
        TestOpts {
            advanced_writes: true,
            max_staging_size: 10,
            ..Default::default()
        },
        &rt,
    );

    rt.block_on(async {
        let mut inos = Vec::new();
        for i in 0..3 {
            let name = format!("f{i}.txt");
            let (attr, fh) = vfs.create(ROOT_INODE, &name, 0o644, 1000, 1000, None).await.unwrap();
            write_blocking(&vfs, attr.ino, fh, 0, b"xxxxxxxx").await.unwrap();
            vfs.release(fh).await.unwrap();
            inos.push(attr.ino);
        }
        tokio::time::sleep(Duration::from_secs(3)).await;

        let sd = vfs.staging.dir().unwrap();
        assert!(!sd.path(inos[0]).exists(), "oldest (f0) should be reclaimed");
        assert!(!sd.path(inos[1]).exists(), "middle (f1) should be reclaimed");
        assert!(sd.path(inos[2]).exists(), "newest (f2) should survive");
    });
}

/// Sequential reads should pass `end=None` (unbounded stream), while seek reads
/// should pass `end=Some(offset+size)` (bounded range). Validates that the mock
/// correctly records and respects the `end` parameter.
#[test]
fn stream_calls_record_bounded_end_for_seek_reads() {
    // File must be larger than INITIAL_WINDOW (8 MiB) + FORWARD_SKIP (16 MiB) so a
    // far seek triggers RangeDownload instead of a forward skip restart.
    const FILE_SIZE: u64 = 30 * 1_048_576; // 30 MiB

    let hub = MockHub::new();
    hub.add_file("file.bin", FILE_SIZE, Some("hash_xyz"), None);
    let xet = MockXet::new();
    // Use a small repeated pattern — only allocate enough to verify content.
    let data: Vec<u8> = (0..FILE_SIZE as usize).map(|i| (i % 251) as u8).collect();
    xet.add_file("hash_xyz", &data);

    let (rt, vfs) = vfs_readonly(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.bin").await.unwrap();
        let fh = vfs.open(attr.ino, false, false, None).await.unwrap();
        tokio::task::yield_now().await;

        // Sequential read at offset 0 — should use unbounded stream (end=None).
        let (chunk, _) = vfs.read(fh, 0, 4096).await.unwrap();
        assert_eq!(chunk.len(), 4096);

        // Far seek read (past INITIAL_WINDOW + FORWARD_SKIP) — should use bounded range.
        let far_offset = 25 * 1_048_576u64; // 25 MiB
        let (chunk2, _) = vfs.read(fh, far_offset, 4096).await.unwrap();
        assert_eq!(chunk2.len(), 4096);
        assert_eq!(chunk2[0], (far_offset as usize % 251) as u8);

        let calls = xet.stream_calls.lock().unwrap().clone();
        // First call: sequential (end=None).
        assert_eq!(calls[0].0, 0, "first call should start at offset 0");
        assert_eq!(calls[0].1, None, "sequential read should have end=None");
        // Second call: seek (end=Some).
        assert_eq!(calls[1].0, far_offset, "seek call should start at far_offset");
        assert!(
            calls[1].1.is_some(),
            "seek read should have end=Some (bounded range), got {:?}",
            calls[1].1
        );

        vfs.release(fh).await.unwrap();
    });
}

/// fsync on a streaming handle is a no-op (doesn't tear down the writer).
/// Writes after fsync must still succeed.
#[test]
fn fsync_streaming_noop() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let (_, fh) = vfs.create(ROOT_INODE, "fsync_test", 0o644, 0, 0, None).await.unwrap();
        let ino = ROOT_INODE + 1;

        // Write some data
        write_blocking(&vfs, ino, fh, 0, b"hello").await.unwrap();

        // fsync should succeed (no-op for streaming)
        vfs.fsync(ino, fh, None).await.unwrap();

        // Write more data after fsync -- must not fail
        write_blocking(&vfs, ino, fh, 5, b" world").await.unwrap();

        // flush + release commit the full data
        vfs.flush(ino, fh, None).await.unwrap();
        vfs.release(fh).await.unwrap();

        let entry = vfs.inode_table.read().unwrap().get(ino).unwrap().clone();
        assert!(!entry.is_dirty());
        assert!(entry.xet_hash.is_some());
    });
}

/// fsync on an advanced-writes handle enqueues for background flush (async).
/// The inode stays dirty until the background loop processes it.
#[test]
fn fsync_advanced_writes_enqueues() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let (_, fh) = vfs.create(ROOT_INODE, "fsync_adv", 0o644, 0, 0, None).await.unwrap();
        let ino = ROOT_INODE + 1;

        // Write data (goes to staging file)
        write_blocking(&vfs, ino, fh, 0, b"advanced data").await.unwrap();

        // Before fsync: inode is dirty
        assert!(vfs.inode_table.read().unwrap().get(ino).unwrap().is_dirty());

        // fsync enqueues for background flush but returns immediately
        vfs.fsync(ino, fh, None).await.unwrap();

        // After fsync: inode is still dirty (flush is async)
        assert!(vfs.inode_table.read().unwrap().get(ino).unwrap().is_dirty());

        vfs.release(fh).await.unwrap();
    });
}

/// fsync on an empty file enqueues for background flush like any other file.
#[test]
fn fsync_empty_file_enqueues() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let (_, fh) = vfs.create(ROOT_INODE, "empty", 0o644, 0, 0, None).await.unwrap();
        let ino = ROOT_INODE + 1;

        // No write — staging file stays empty (0 bytes)
        assert!(vfs.inode_table.read().unwrap().get(ino).unwrap().is_dirty());

        // fsync enqueues but doesn't flush synchronously
        vfs.fsync(ino, fh, None).await.unwrap();

        // Inode is still dirty (background flush hasn't run yet)
        assert!(vfs.inode_table.read().unwrap().get(ino).unwrap().is_dirty());

        vfs.release(fh).await.unwrap();
    });
}

/// Async fsync doesn't commit synchronously. Writes before and after
/// fsync all leave the inode dirty for the background flush loop.
#[test]
fn fsync_between_writes_stays_dirty() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let (_, fh) = vfs.create(ROOT_INODE, "redirty", 0o644, 0, 0, None).await.unwrap();
        let ino = ROOT_INODE + 1;

        write_blocking(&vfs, ino, fh, 0, b"initial").await.unwrap();
        assert!(vfs.inode_table.read().unwrap().get(ino).unwrap().is_dirty());

        vfs.fsync(ino, fh, None).await.unwrap();
        assert!(vfs.inode_table.read().unwrap().get(ino).unwrap().is_dirty());

        let result = vfs.write(ino, fh, 0, b"more");
        assert!(result.is_ok(), "write after fsync should succeed");
        assert!(vfs.inode_table.read().unwrap().get(ino).unwrap().is_dirty());

        vfs.release(fh).await.unwrap();
    });
}

/// When a file is flushed and then re-dirtied without content change,
/// the background flush should skip the Hub commit (same xet_hash).
#[test]
fn flush_skips_hub_commit_when_hash_unchanged() {
    use std::sync::atomic::Ordering;

    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let (_, fh) = vfs.create(ROOT_INODE, "f.txt", 0o644, 0, 0, None).await.unwrap();
        let ino = ROOT_INODE + 1;

        write_blocking(&vfs, ino, fh, 0, b"hello").await.unwrap();
        vfs.release(fh).await.unwrap();

        // Wait for background flush (debounce = 100ms).
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        // First flush committed to Hub.
        let logs = hub.take_batch_log();
        assert_eq!(logs.len(), 1, "first flush should commit");

        let entry = vfs.inode_table.read().unwrap().get(ino).unwrap().clone();
        assert!(!entry.is_dirty());
        let hash = entry.xet_hash.unwrap();

        // Reset MockXet's counter so next upload returns the same hash.
        // e.g. hash = "mock_hash_1" → store 1 so next_hash_string() returns "mock_hash_1".
        let counter: u64 = hash.strip_prefix("mock_hash_").unwrap().parse().unwrap();
        xet.next_hash.store(counter, Ordering::SeqCst);

        // Re-dirty the inode (simulates a write that didn't change content).
        vfs.inode_table.write().unwrap().get_mut(ino).unwrap().set_dirty();
        vfs.schedule_flush(ino);

        // Wait for second flush.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        // Second flush should NOT commit (same hash → skipped).
        let logs2 = hub.take_batch_log();
        assert!(logs2.is_empty(), "unchanged file should skip Hub commit");

        // Inode should be clean again.
        assert!(!vfs.inode_table.read().unwrap().get(ino).unwrap().is_dirty());
    });

    vfs.shutdown();
}

/// Regression test for the flush ↔ unlink race that produced
/// "No such file or directory" / "failed to fill whole buffer" errors in
/// xfstests CI.
///
/// flush_batch reads each staging file by path; without the per-inode
/// staging lock, a concurrent unlink can drop the file mid-read. The test
/// gates `MockXet::upload_files` to hold the upload at the exact point
/// where the file is being read, then issues an unlink. The unlink must
/// wait on the staging lock until the upload completes, and the upload
/// must succeed (no errors recorded in the FlushManager).
#[test]
fn flush_unlink_race_serializes_via_staging_lock() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        // Install the gate before triggering the flush.
        let gate = xet.install_upload_gate();

        let (_, fh) = vfs.create(ROOT_INODE, "racy.txt", 0o644, 0, 0, None).await.unwrap();
        let ino = ROOT_INODE + 1;
        write_blocking(&vfs, ino, fh, 0, b"payload").await.unwrap();
        vfs.release(fh).await.unwrap();

        // Wait for flush_batch to enter `upload_files` (gate.entered).
        // At this point flush holds the per-inode staging lock.
        gate.entered.notified().await;
        assert_eq!(xet.uploads_inflight.load(std::sync::atomic::Ordering::SeqCst), 1);

        // Issue a concurrent unlink. With the fix it blocks on
        // `staging.drop_locked` (same per-inode lock). Without the fix it
        // would unlink the staging file immediately and the upload below
        // would fail with ENOENT or short read.
        let vfs_clone = vfs.clone();
        let unlink_task = tokio::spawn(async move { vfs_clone.unlink(ROOT_INODE, "racy.txt").await });

        // Give the unlink a chance to reach `drop_locked`. If the lock
        // weren't held it would have completed by now.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!unlink_task.is_finished(), "unlink must wait for in-flight upload");

        // Release the upload. flush completes, drops the lock, unlink
        // proceeds to remove the staging file.
        gate.release.notify_one();
        unlink_task.await.unwrap().unwrap();

        // Wait for the flush_batch to finish (post-upload commit + GC).
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // The upload must have succeeded — no flush error recorded for ino.
        assert!(
            vfs.flush_manager.as_ref().unwrap().check_error(ino).is_none(),
            "upload should have succeeded (the lock prevented the race)"
        );

        // Hub commit (AddFile) ran before the unlink's pending delete drained.
        let logs = hub.take_batch_log();
        assert!(
            logs.iter()
                .flatten()
                .any(|op| matches!(op, crate::hub_api::BatchOp::AddFile { path, .. } if path == "racy.txt")),
            "AddFile should appear in the Hub log: {logs:?}"
        );
    });

    vfs.shutdown();
}

/// In a batch with mixed changed/unchanged files, only changed files
/// appear in the Hub commit. Unchanged files still get cleared dirty.
#[test]
fn flush_mixed_changed_and_unchanged() {
    use std::sync::atomic::Ordering;

    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        // Create two files and flush them.
        let (_, fh1) = vfs.create(ROOT_INODE, "file1.txt", 0o644, 0, 0, None).await.unwrap();
        let ino1 = ROOT_INODE + 1;
        write_blocking(&vfs, ino1, fh1, 0, b"content1").await.unwrap();

        let (_, fh2) = vfs.create(ROOT_INODE, "file2.txt", 0o644, 0, 0, None).await.unwrap();
        let ino2 = ROOT_INODE + 2;
        write_blocking(&vfs, ino2, fh2, 0, b"content2").await.unwrap();

        vfs.release(fh1).await.unwrap();
        vfs.release(fh2).await.unwrap();

        // Wait for background flush.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let logs = hub.take_batch_log();
        assert_eq!(logs.len(), 1, "both files in one batch");
        assert_eq!(logs[0].len(), 2, "both files committed");

        // Grab hash for file1 (will be "unchanged").
        let hash1 = vfs
            .inode_table
            .read()
            .unwrap()
            .get(ino1)
            .unwrap()
            .xet_hash
            .clone()
            .unwrap();
        let counter1: u64 = hash1.strip_prefix("mock_hash_").unwrap().parse().unwrap();

        // Reset counter so file1 gets the same hash (unchanged).
        // File2's prev hash is changed to force a mismatch (simulates changed content).
        xet.next_hash.store(counter1, Ordering::SeqCst);

        {
            let mut inodes = vfs.inode_table.write().unwrap();
            inodes.get_mut(ino1).unwrap().set_dirty();
            inodes.get_mut(ino2).unwrap().xet_hash = Some("stale_hash".to_string());
            inodes.get_mut(ino2).unwrap().set_dirty();
        }
        vfs.schedule_flush(ino1);
        vfs.schedule_flush(ino2);

        // Wait for second flush.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let logs2 = hub.take_batch_log();
        assert_eq!(logs2.len(), 1, "should still make one Hub commit");
        // Only file2 (changed) should be in the commit.
        assert_eq!(logs2[0].len(), 1, "only changed file should be committed");

        // Both inodes should be clean.
        assert!(!vfs.inode_table.read().unwrap().get(ino1).unwrap().is_dirty());
        assert!(!vfs.inode_table.read().unwrap().get(ino2).unwrap().is_dirty());
    });

    vfs.shutdown();
}

/// If streaming writer creation fails on open(O_TRUNC), the inode must NOT
/// be truncated. Before the fix, the inode was mutated before writer setup,
/// leaving it stuck at size=0 with no handle to recover.
#[test]
fn failed_writer_create_preserves_inode() {
    let hub = MockHub::new();
    hub.add_file("existing.txt", 1000, Some("original_hash"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "existing.txt").await.unwrap();
        let ino = attr.ino;
        assert_eq!(attr.size, 1000);

        // Make the next streaming writer creation fail
        xet.fail_next_writer_create();

        // Open with O_TRUNC should fail (writer creation error -> EIO)
        let result = vfs.open(ino, true, true, None).await;
        assert_eq!(result.unwrap_err(), libc::EIO);

        // Inode must be preserved: original hash, original size, NOT dirty
        let entry = vfs.inode_table.read().unwrap().get(ino).unwrap().clone();
        assert_eq!(entry.xet_hash.as_deref(), Some("original_hash"));
        assert_eq!(entry.size, 1000);
        assert!(!entry.is_dirty(), "inode should not be dirty after failed open");
    });
}

// ── FD leak: open handles cleaned up on unlink/release ────────────────

/// Unlink a file with an open handle (advanced writes) releases the handle on close.
/// Verifies that open_files is empty after release (no FD leak).
#[test]
fn unlink_with_open_handle_cleans_open_files() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 0, None, None);
    hub.set_head(
        "file.txt",
        Some(HeadFileInfo {
            xet_hash: None,
            etag: None,
            size: Some(0),
            last_modified: None,
        }),
    );
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        // Open for write → creates entry in open_files
        let fh = vfs.open(ino, true, true, None).await.unwrap();
        assert!(vfs.has_open_handles(ino), "should have open handle after open");

        // Unlink while handle is open → inode stays as orphan
        vfs.unlink(ROOT_INODE, "file.txt").await.unwrap();
        assert!(vfs.has_open_handles(ino), "handle should still be open after unlink");

        // Release → handle removed, orphan cleaned up, no FD leak
        vfs.release(fh).await.unwrap();
        assert!(!vfs.has_open_handles(ino), "handle should be gone after release");

        // open_files should be empty
        let count = vfs.open_files.read().unwrap().len();
        assert_eq!(count, 0, "open_files should be empty after unlink+release");
    });
}

/// Creating and deleting many files (advanced writes) leaves no orphan open_files entries.
#[test]
fn bulk_create_delete_no_open_files_leak() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        // Create 100 files, release handles, then delete
        let mut inos = Vec::new();
        for i in 0..100 {
            let name = format!("file_{i}.txt");
            let (attr, fh) = vfs.create(ROOT_INODE, &name, 0o644, 1000, 1000, None).await.unwrap();
            inos.push(attr.ino);
            vfs.release(fh).await.unwrap();
        }

        for i in 0..100 {
            let name = format!("file_{i}.txt");
            vfs.unlink(ROOT_INODE, &name).await.unwrap();
        }

        // All handles released, all inodes unlinked → open_files should be empty
        let count = vfs.open_files.read().unwrap().len();
        assert_eq!(count, 0, "open_files should be empty after bulk create+delete");

        // All inodes should be gone
        let inodes = vfs.inode_table.read().unwrap();
        for ino in &inos {
            assert!(inodes.get(*ino).is_none(), "ino={ino} should be removed after unlink");
        }
    });
}

/// When an inode is LRU-evicted via forget(), its staging file must be removed
/// too. Otherwise a long-lived mount accumulates orphaned staging files on disk.
#[test]
fn staging_file_removed_on_inode_eviction() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let (attr, fh) = vfs
            .create(ROOT_INODE, "evict_me.txt", 0o644, 1000, 1000, None)
            .await
            .unwrap();
        let ino = attr.ino;
        write_blocking(&vfs, ino, fh, 0, b"staging data").await.unwrap();

        let staging_path = vfs.staging.dir().unwrap().path(ino);
        assert!(staging_path.exists(), "staging file should exist after write");

        vfs.release(fh).await.unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await;

        let is_clean = vfs.inode_table.read().unwrap().get(ino).is_some_and(|e| !e.is_dirty());
        assert!(is_clean, "inode should be clean after flush");

        // Simulate the FUSE kernel dropping the dentry, triggering LRU eviction.
        vfs.bump_nlookup(ino);
        vfs.forget(ino, 1);

        assert!(
            vfs.inode_table.read().unwrap().get(ino).is_none(),
            "inode should be evicted after forget"
        );
        assert!(
            !staging_path.exists(),
            "staging file should be removed when the inode is evicted"
        );
    });
}

/// LRU sweep variant: a clean staging file must be removed when the sweep
/// itself evicts the inode (the kernel never sends `forget()` for inodes
/// that came from a readdir, so the sweep's drop_staging is the only path).
#[test]
fn lru_sweep_drops_clean_staging_file() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let (attr, fh) = vfs
            .create(ROOT_INODE, "swept.txt", 0o644, 1000, 1000, None)
            .await
            .unwrap();
        let ino = attr.ino;
        write_blocking(&vfs, ino, fh, 0, b"data").await.unwrap();
        let staging_path = vfs.staging.dir().unwrap().path(ino);
        assert!(staging_path.exists());

        vfs.release(fh).await.unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(
            vfs.inode_table.read().unwrap().get(ino).is_some_and(|e| !e.is_dirty()),
            "inode should be clean after flush"
        );
        assert!(
            staging_path.exists(),
            "clean staging file must still be on disk pre-sweep"
        );

        // Wire a no-op invalidator so lru_evict_sweep can run.
        vfs.set_entry_invalidator(Box::new(|_, _| true));

        // Force the sweep to consider every file as overflow.
        let evicted = vfs.lru_evict_sweep(0).await;
        assert!(evicted >= 1, "sweep should evict the clean file");
        assert!(vfs.inode_table.read().unwrap().get(ino).is_none());
        assert!(!staging_path.exists(), "sweep must drop the staging file");
    });
}

/// Truncating a hashed remote file produces an empty staging file, which does
/// not match the remote. `staging_is_current` must stay false — otherwise the
/// next open would reuse empty staging as if it held the remote content.
#[test]
fn truncate_open_does_not_flag_staging_current() {
    let hub = MockHub::new();
    hub.add_file("dst.txt", 11, Some("remote_hash"), None);
    let xet = MockXet::new();
    xet.add_file("remote_hash", b"hello world");
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "dst.txt").await.unwrap();
        let ino = attr.ino;

        let fh = vfs.open(ino, true, true, None).await.unwrap();
        vfs.release(fh).await.unwrap();

        let flagged = vfs
            .inode_table
            .read()
            .unwrap()
            .get(ino)
            .is_some_and(|e| e.staging_is_current);
        assert!(
            !flagged,
            "truncate of a hashed file must not flag empty staging as current"
        );
    });
}

/// After a flush, the staging file is kept on disk and flagged as current.
/// Reopening the file for write must reuse that staging cache without
/// issuing a new download.
#[test]
fn open_advanced_write_reuses_staging_cache_after_flush() {
    use std::sync::atomic::Ordering;

    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let (attr, fh1) = vfs
            .create(ROOT_INODE, "cache_me.txt", 0o644, 1000, 1000, None)
            .await
            .unwrap();
        let ino = attr.ino;
        write_blocking(&vfs, ino, fh1, 0, b"hello world").await.unwrap();
        vfs.release(fh1).await.unwrap();

        tokio::time::sleep(Duration::from_secs(3)).await;
        let is_clean = vfs.inode_table.read().unwrap().get(ino).is_some_and(|e| !e.is_dirty());
        assert!(is_clean, "inode should be clean after flush");

        let downloads_before = xet.download_to_file_calls.load(Ordering::SeqCst);
        let fh2 = vfs.open(ino, true, false, None).await.unwrap();
        let downloads_after = xet.download_to_file_calls.load(Ordering::SeqCst);
        assert_eq!(
            downloads_before, downloads_after,
            "re-opening for write should reuse staging cache, not re-download"
        );
        vfs.release(fh2).await.unwrap();
    });
}

/// Rename overwriting a destination cleans up the destination inode.
#[test]
fn rename_overwrite_cleans_destination() {
    let hub = MockHub::new();
    hub.add_file("src.txt", 100, Some("hash_src"), None);
    hub.add_file("dst.txt", 200, Some("hash_dst"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let src_attr = vfs.lookup(ROOT_INODE, "src.txt").await.unwrap();
        let dst_attr = vfs.lookup(ROOT_INODE, "dst.txt").await.unwrap();
        let dst_ino = dst_attr.ino;

        // Rename src -> dst (overwrites dst)
        vfs.rename(ROOT_INODE, "src.txt", ROOT_INODE, "dst.txt", false)
            .await
            .unwrap();

        // dst_ino should be gone (overwritten)
        let inodes = vfs.inode_table.read().unwrap();
        assert!(inodes.get(dst_ino).is_none(), "overwritten dst inode should be removed");

        // src_ino should now be at dst.txt
        let entry = inodes.lookup_child(ROOT_INODE, "dst.txt").unwrap();
        assert_eq!(entry.inode, src_attr.ino, "dst.txt should now be src's inode");
    });
}

// ── setattr truncate + write serialization ────────────────────────────

/// After setattr(truncate=0) followed by a write, inode.size must reflect the
/// write. This exercises the fix for REVIEW.md #4: setattr now holds
/// inode_table.write() during file truncation, so a concurrent write's size
/// update is serialized rather than clobbered.
#[test]
fn setattr_truncate_then_write_size_consistent() {
    let hub = MockHub::new();
    hub.add_file("data.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    xet.add_file("hash1", &[0u8; 100]);
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "data.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, true, None).await.unwrap();

        // Truncate to 0
        vfs.setattr(ino, Some(0), None, None, None, None, None).await.unwrap();
        let attr = vfs.getattr(ino).unwrap();
        assert_eq!(attr.size, 0, "size should be 0 after truncate");

        // Write 10 bytes at offset 0
        let data = b"helloworld";
        write_blocking(&vfs, ino, fh, 0, data).await.unwrap();

        // inode.size must reflect the write, not the prior truncation
        let attr = vfs.getattr(ino).unwrap();
        assert_eq!(attr.size, 10, "size should match write length after truncate+write");

        let _ = vfs.release(fh).await;
    });
}

// ── rename phase 2/3 divergence ───────────────────────────────────────

/// Rename of a clean file sends remote batch ops (Phase 2) and applies locally
/// (Phase 3). Verifies the happy path still works after the Phase 2/3 error
/// handling refactor.
#[test]
fn rename_clean_file_remote_and_local() {
    let hub = MockHub::new();
    hub.add_file("src.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let src_attr = vfs.lookup(ROOT_INODE, "src.txt").await.unwrap();
        let src_ino = src_attr.ino;

        vfs.rename(ROOT_INODE, "src.txt", ROOT_INODE, "dst.txt", false)
            .await
            .unwrap();

        // Phase 2 should have sent batch ops
        let logs = hub.take_batch_log();
        assert_eq!(logs.len(), 1, "Phase 2 should have sent batch ops");

        // Phase 3 should have applied locally
        let inodes = vfs.inode_table.read().unwrap();
        let entry = inodes.get(src_ino).unwrap();
        assert_eq!(entry.full_path.as_ref(), "dst.txt");
    });
}

// ── LRU eviction: end-to-end memory bound ───────────────────────────────

/// End-to-end test of the forget-driven eviction path on a hierarchical
/// tree — the realistic shape for a Hub repo crawl. Walks
/// `SUBDIRS × FILES_PER_DIR` files through the full FUSE-style protocol
/// (lookup → bump_nlookup → forget) under a soft limit of 100.
///
/// Exercises the production reclamation chain: async LRU sweep →
/// `inval_entry` callback → kernel drops the dentry → `forget()` →
/// `evict_if_safe` removes the inode. The `inval_entry` callback is
/// simulated here (no real FUSE), but it drives `forget` synchronously,
/// which is the only part of the kernel's behavior the table depends on.
///
/// We drive one sweep per dir to keep the test deterministic — in prod
/// the sweep runs on an interval and the table overshoots between ticks.
#[test]
fn lru_keeps_inode_table_bounded_under_lookup_churn() {
    const SUBDIRS: usize = 20;
    const FILES_PER_DIR: usize = 40;
    const SOFT_LIMIT: usize = 100;
    // Peak = steady state after a sweep (~SOFT_LIMIT) + one bulk dir
    // load + headroom for the trailing dir entries the sweep can't
    // evict (non-leaf dirs are protected).
    const MAX_EXPECTED: usize = SOFT_LIMIT + FILES_PER_DIR + SUBDIRS + 32;

    let hub = MockHub::new();
    for d in 0..SUBDIRS {
        for f in 0..FILES_PER_DIR {
            hub.add_file(&format!("d{d:02}/f{f:02}.txt"), 0, Some("hash"), None);
        }
    }
    let xet = MockXet::new();
    let (rt, vfs) = vfs_with_lru(&hub, &xet, SOFT_LIMIT);

    // Simulate the FUSE kernel's response to `inval_entry`: drop all
    // outstanding nlookup refs on the child. `forget` then runs
    // `evict_if_safe` and removes the inode if nothing else holds it.
    let vfs_cb = vfs.clone();
    vfs.set_entry_invalidator(Box::new(move |parent, name| {
        use std::sync::atomic::Ordering;
        let ino_and_lookup = {
            let inodes = vfs_cb.inode_table.read().expect("inodes poisoned");
            inodes
                .lookup_child(parent, name)
                .map(|e| (e.inode, e.eviction.nlookup.load(Ordering::Relaxed)))
        };
        if let Some((ino, nlookup)) = ino_and_lookup
            && nlookup > 0
        {
            vfs_cb.forget(ino, nlookup);
        }
        true
    }));

    rt.block_on(async {
        let mut peak = 0;
        for d in 0..SUBDIRS {
            let dir_name = format!("d{d:02}");
            let dir_attr = vfs
                .lookup(ROOT_INODE, &dir_name)
                .await
                .unwrap_or_else(|e| panic!("lookup({dir_name}) failed: errno={e}"));
            vfs.bump_nlookup(dir_attr.ino);

            for f in 0..FILES_PER_DIR {
                let file_name = format!("f{f:02}.txt");
                let attr = vfs
                    .lookup(dir_attr.ino, &file_name)
                    .await
                    .unwrap_or_else(|e| panic!("lookup({dir_name}/{file_name}) failed: errno={e}"));
                vfs.bump_nlookup(attr.ino);
            }

            // Drive one sweep per completed directory, matching the prod
            // cadence (sweep runs on an interval, not per insert).
            vfs.lru_evict_sweep(SOFT_LIMIT).await;

            let len = vfs.inode_table.read().unwrap().len();
            peak = peak.max(len);
            assert!(
                len < MAX_EXPECTED,
                "table overshoot after {dir_name}: len={len} > {MAX_EXPECTED}"
            );
        }

        // After every file has been looked up and the sweep has run,
        // the table sits near the soft limit — well below the total
        // workload.
        let final_len = vfs.inode_table.read().unwrap().len();
        let total_lookups = SUBDIRS * FILES_PER_DIR;
        assert!(
            final_len < MAX_EXPECTED,
            "table not bounded: final_len={final_len}, peak={peak}, total_lookups={total_lookups}"
        );
    });
}

// ── Flush pipeline ─────────────────────────────────────────────────

/// Multiple enqueues for the same inode collapse to a single upload.
#[test]
fn flush_dedup_same_inode() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        let fh = vfs.open(ino, true, true, Some(42)).await.unwrap();
        write_blocking(&vfs, ino, fh, 0, b"hello").await.unwrap();

        let fm = vfs.flush_manager.as_ref().unwrap();
        fm.enqueue(ino);
        fm.enqueue(ino);
        fm.enqueue(ino);

        tokio::time::sleep(Duration::from_millis(1500)).await;

        let logs = hub.take_batch_log();
        let add_count = logs
            .iter()
            .flatten()
            .filter(|op| matches!(op, BatchOp::AddFile { path, .. } if path == "file.txt"))
            .count();
        assert_eq!(add_count, 1, "dedup should yield exactly 1 hub commit");

        vfs.release(fh).await.unwrap();
    });
}

#[test]
fn flush_cancel_delete() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let fm = vfs.flush_manager.as_ref().unwrap();
        fm.enqueue_delete("old/path.txt".to_string());
        fm.cancel_delete("old/path.txt");

        tokio::time::sleep(Duration::from_millis(1500)).await;

        let logs = hub.take_batch_log();
        let has_delete = logs
            .iter()
            .flatten()
            .any(|op| matches!(op, BatchOp::DeleteFile { path } if path == "old/path.txt"));
        assert!(!has_delete, "cancelled delete should not appear in batch log");
    });
}

#[test]
fn flush_cancel_delete_prefix() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let fm = vfs.flush_manager.as_ref().unwrap();
        fm.enqueue_delete("dir/a.txt".to_string());
        fm.enqueue_delete("dir/b.txt".to_string());
        fm.enqueue_delete("other.txt".to_string());
        fm.cancel_delete_prefix("dir/");

        tokio::time::sleep(Duration::from_millis(1500)).await;

        let logs = hub.take_batch_log();
        let all_ops: Vec<&BatchOp> = logs.iter().flatten().collect();

        let has_dir_delete = all_ops
            .iter()
            .any(|op| matches!(op, BatchOp::DeleteFile { path } if path.starts_with("dir/")));
        assert!(!has_dir_delete, "dir/ deletes should have been cancelled");

        let has_other_delete = all_ops
            .iter()
            .any(|op| matches!(op, BatchOp::DeleteFile { path } if path == "other.txt"));
        assert!(has_other_delete, "other.txt delete should still be sent");
    });
}

/// A failed delete batch is re-queued and retried on the next flush cycle.
#[test]
fn flush_pending_deletes_requeue_on_failure() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let fm = vfs.flush_manager.as_ref().unwrap();
        hub.fail_next_batch(1);
        fm.enqueue_delete("retry/file.txt".to_string());

        tokio::time::sleep(Duration::from_millis(1500)).await;
        // First attempt errored — batch_log records nothing.
        let logs = hub.take_batch_log();
        assert!(logs.is_empty(), "first attempt should have failed, no batch log");

        // Re-queued path is sitting in pending_deletes but no WakeDeletes was
        // sent; trigger a second cycle by enqueuing a dummy path.
        fm.enqueue_delete("dummy_wake.txt".to_string());
        tokio::time::sleep(Duration::from_millis(1500)).await;

        let logs = hub.take_batch_log();
        let has_retry = logs
            .iter()
            .flatten()
            .any(|op| matches!(op, BatchOp::DeleteFile { path } if path == "retry/file.txt"));
        assert!(has_retry, "delete should succeed on retry after initial failure");
    });
}

// ── Symlinks ────────────────────────────────────────────────────────

#[test]
fn symlink_create_and_readlink() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs
            .symlink(ROOT_INODE, "link", "target.txt", 0o777, 1000, 1000)
            .await
            .unwrap();
        let ino = attr.ino;

        let looked_up = vfs.lookup(ROOT_INODE, "link").await.unwrap();
        assert_eq!(looked_up.ino, ino);

        let target = vfs.readlink(ino).unwrap();
        assert_eq!(target, "target.txt");
        assert_eq!(attr.kind, super::inode::InodeKind::Symlink);
    });
}

#[test]
fn readlink_regular_file_returns_error() {
    let hub = MockHub::new();
    hub.add_file("regular.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_simple(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "regular.txt").await.unwrap();
        let result = vfs.readlink(attr.ino);
        assert_eq!(result.unwrap_err(), libc::EINVAL);
    });
}

// ── Overlay mode tests ─────────────────────────────────────────────

/// Overlay mode forces advanced_writes inside the VFS constructor.
#[test]
fn overlay_constructor_forces_advanced_writes() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let rt = new_runtime();
    let vfs = make_test_vfs(
        hub,
        xet,
        TestOpts {
            overlay: true,
            advanced_writes: false,
            ..Default::default()
        },
        &rt,
    );

    assert!(vfs.overlay());
    assert!(vfs.advanced_writes);
    assert!(vfs.flush_manager.is_none());
}

/// Readdir merges remote bucket entries with local overlay files.
#[test]
fn overlay_readdir_merges_local_and_remote() {
    let hub = MockHub::new();
    hub.add_file("remote.txt", 5, Some("rhash"), None);
    let xet = MockXet::new();

    // Pre-populate overlay root BEFORE VFS creation
    let overlay_root = fresh_overlay_dir("readdir");
    std::fs::write(overlay_root.join("local.txt"), b"local data").unwrap();

    let t = make_overlay_test_vfs_with_root(hub, xet, overlay_root);

    t.runtime.block_on(async {
        let entries = t.vfs.readdir(ROOT_INODE).await.unwrap();
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"remote.txt"), "missing remote.txt: {names:?}");
        assert!(names.contains(&"local.txt"), "missing local.txt: {names:?}");
    });
}

/// Read a file that only exists locally in the overlay dir.
#[test]
fn overlay_read_local_file() {
    let hub = MockHub::new();
    let xet = MockXet::new();

    let overlay_root = fresh_overlay_dir("readlocal");
    std::fs::write(overlay_root.join("data.txt"), b"hello overlay").unwrap();

    let t = make_overlay_test_vfs_with_root(hub, xet, overlay_root);

    t.runtime.block_on(async {
        let entries = t.vfs.readdir(ROOT_INODE).await.unwrap();
        let ino = entries
            .iter()
            .find(|e| e.name == "data.txt")
            .expect("data.txt not in readdir")
            .ino;

        let fh = t.vfs.open(ino, false, false, None).await.unwrap();
        let (data, _eof) = t.vfs.read(fh, 0, 1024).await.unwrap();
        assert_eq!(data.as_ref(), b"hello overlay");
    });
}

/// Local file takes precedence over remote file with the same name.
#[test]
fn overlay_local_overrides_remote() {
    let hub = MockHub::new();
    hub.add_file("conflict.txt", 6, Some("remote_hash"), None);
    let xet = MockXet::new();
    xet.add_file("remote_hash", b"remote");

    let overlay_root = fresh_overlay_dir("conflict");
    std::fs::write(overlay_root.join("conflict.txt"), b"local wins").unwrap();

    let t = make_overlay_test_vfs_with_root(hub, xet, overlay_root);

    t.runtime.block_on(async {
        let entries = t.vfs.readdir(ROOT_INODE).await.unwrap();
        let ino = entries.iter().find(|e| e.name == "conflict.txt").unwrap().ino;

        // Size should reflect local file
        let attr = t.vfs.getattr(ino).unwrap();
        assert_eq!(attr.size, 10); // "local wins" = 10 bytes

        let fh = t.vfs.open(ino, false, false, None).await.unwrap();
        let (data, _eof) = t.vfs.read(fh, 0, 1024).await.unwrap();
        assert_eq!(data.as_ref(), b"local wins");
    });
}

/// Writes via VFS land at the original path in the overlay root dir.
#[test]
fn overlay_write_lands_at_original_path() {
    let hub = MockHub::new();
    let xet = MockXet::new();

    let overlay_root = fresh_overlay_dir("write");
    let t = make_overlay_test_vfs_with_root(hub, xet, overlay_root);

    t.runtime.block_on(async {
        let (attr, fh) = t
            .vfs
            .create(ROOT_INODE, "new.txt", 0o644, 1000, 1000, None)
            .await
            .unwrap();
        write_blocking(&t.vfs, attr.ino, fh, 0, b"written via vfs")
            .await
            .unwrap();
        t.vfs.release(fh).await.unwrap();
    });

    let on_disk = std::fs::read_to_string(t.overlay_root.join("new.txt")).unwrap();
    assert_eq!(on_disk, "written via vfs");
}

/// mkdir via VFS creates a real directory in the overlay root.
#[test]
fn overlay_mkdir_creates_local_dir() {
    let hub = MockHub::new();
    let xet = MockXet::new();

    let overlay_root = fresh_overlay_dir("mkdir");
    let t = make_overlay_test_vfs_with_root(hub, xet, overlay_root);

    t.runtime.block_on(async {
        t.vfs.mkdir(ROOT_INODE, "subdir", 0o755, 1000, 1000).await.unwrap();
    });

    assert!(t.overlay_root.join("subdir").is_dir());
}

/// In overlay mode, no batch ops are sent to the remote hub.
#[test]
fn overlay_no_flush_manager() {
    let hub = MockHub::new();
    let xet = MockXet::new();

    let overlay_root = fresh_overlay_dir("noflush");
    let t = make_overlay_test_vfs_with_root(hub.clone(), xet, overlay_root);

    t.runtime.block_on(async {
        let (attr, fh) = t
            .vfs
            .create(ROOT_INODE, "nopush.txt", 0o644, 1000, 1000, None)
            .await
            .unwrap();
        write_blocking(&t.vfs, attr.ino, fh, 0, b"data").await.unwrap();
        t.vfs.release(fh).await.unwrap();
    });

    let log = hub.take_batch_log();
    assert!(log.is_empty(), "expected no remote ops, got: {log:?}");
}

/// fsync in overlay mode does not upload to remote.
#[test]
fn overlay_fsync_no_upload() {
    let hub = MockHub::new();
    let xet = MockXet::new();

    let overlay_root = fresh_overlay_dir("fsync");
    let t = make_overlay_test_vfs_with_root(hub.clone(), xet, overlay_root);

    t.runtime.block_on(async {
        let (attr, fh) = t
            .vfs
            .create(ROOT_INODE, "synced.txt", 0o644, 1000, 1000, None)
            .await
            .unwrap();
        write_blocking(&t.vfs, attr.ino, fh, 0, b"data").await.unwrap();
        t.vfs.fsync(attr.ino, fh, None).await.unwrap();
        t.vfs.release(fh).await.unwrap();
    });

    let log = hub.take_batch_log();
    assert!(log.is_empty(), "expected no remote ops after fsync, got: {log:?}");
}

/// Nested local directories are discovered through readdir.
#[test]
fn overlay_nested_readdir() {
    let hub = MockHub::new();
    let xet = MockXet::new();

    let overlay_root = fresh_overlay_dir("nested");
    std::fs::create_dir_all(overlay_root.join("dir1")).unwrap();
    std::fs::write(overlay_root.join("dir1/file1.txt"), b"nested content").unwrap();

    let t = make_overlay_test_vfs_with_root(hub, xet, overlay_root);

    t.runtime.block_on(async {
        // Root should list dir1
        let root_entries = t.vfs.readdir(ROOT_INODE).await.unwrap();
        let dir1_entry = root_entries
            .iter()
            .find(|e| e.name == "dir1")
            .expect("dir1 not in root readdir");

        // dir1 should list file1.txt
        let dir1_entries = t.vfs.readdir(dir1_entry.ino).await.unwrap();
        let names: Vec<&str> = dir1_entries.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"file1.txt"), "missing file1.txt in dir1: {names:?}");
    });
}

/// Remote files are still readable when no local override exists.
#[test]
fn overlay_read_remote_file() {
    let hub = MockHub::new();
    hub.add_file("remote_only.txt", 11, Some("xhash"), None);
    let xet = MockXet::new();
    xet.add_file("xhash", b"remote data");

    let overlay_root = fresh_overlay_dir("remote");
    let t = make_overlay_test_vfs_with_root(hub, xet, overlay_root);

    t.runtime.block_on(async {
        let entries = t.vfs.readdir(ROOT_INODE).await.unwrap();
        let ino = entries.iter().find(|e| e.name == "remote_only.txt").unwrap().ino;

        let fh = t.vfs.open(ino, false, false, None).await.unwrap();
        let (data, _eof) = t.vfs.read(fh, 0, 1024).await.unwrap();
        assert_eq!(data.as_ref(), b"remote data");
    });

    // File should NOT appear in overlay root
    assert!(!t.overlay_root.join("remote_only.txt").exists());
}

/// Remote-only files cannot be opened writable in overlay mode.
#[test]
fn overlay_open_remote_write_eperm() {
    let hub = MockHub::new();
    hub.add_file("remote.txt", 11, Some("xhash"), None);
    let xet = MockXet::new();
    xet.add_file("xhash", b"remote data");

    let overlay_root = fresh_overlay_dir("openremote");
    let t = make_overlay_test_vfs_with_root(hub, xet, overlay_root);

    t.runtime.block_on(async {
        let entries = t.vfs.readdir(ROOT_INODE).await.unwrap();
        let ino = entries.iter().find(|e| e.name == "remote.txt").unwrap().ino;

        let err = t.vfs.open(ino, true, false, None).await.unwrap_err();
        assert_eq!(err, libc::EPERM);
    });

    assert!(!t.overlay_root.join("remote.txt").exists());
}

/// O_TRUNC on a remote-only file is rejected in overlay mode.
#[test]
fn overlay_open_remote_truncate_eperm() {
    let hub = MockHub::new();
    hub.add_file("remote.txt", 11, Some("xhash"), None);
    let xet = MockXet::new();
    xet.add_file("xhash", b"remote data");

    let overlay_root = fresh_overlay_dir("truncremote");
    let t = make_overlay_test_vfs_with_root(hub, xet, overlay_root);

    t.runtime.block_on(async {
        let entries = t.vfs.readdir(ROOT_INODE).await.unwrap();
        let ino = entries.iter().find(|e| e.name == "remote.txt").unwrap().ino;

        let err = t.vfs.open(ino, true, true, None).await.unwrap_err();
        assert_eq!(err, libc::EPERM);
    });

    assert!(!t.overlay_root.join("remote.txt").exists());
}

/// Data written via VFS is readable back in the same session.
#[test]
fn overlay_write_persists_after_reread() {
    let hub = MockHub::new();
    let xet = MockXet::new();

    let overlay_root = fresh_overlay_dir("reread");
    let t = make_overlay_test_vfs_with_root(hub, xet, overlay_root);

    t.runtime.block_on(async {
        let (attr, fh) = t
            .vfs
            .create(ROOT_INODE, "reread.txt", 0o644, 1000, 1000, None)
            .await
            .unwrap();
        write_blocking(&t.vfs, attr.ino, fh, 0, b"hello").await.unwrap();
        t.vfs.release(fh).await.unwrap();

        // Re-open read-only
        let fh2 = t.vfs.open(attr.ino, false, false, None).await.unwrap();
        let (data, _eof) = t.vfs.read(fh2, 0, 1024).await.unwrap();
        assert_eq!(data.as_ref(), b"hello");
    });
}

/// Reloading a directory should not keep bumping dirty_generation for the same
/// overlay-local entry when nothing changed on disk.
#[test]
fn overlay_reread_does_not_bump_dirty_generation() {
    let hub = MockHub::new();
    let xet = MockXet::new();

    let overlay_root = fresh_overlay_dir("dirtyreload");
    std::fs::write(overlay_root.join("local.txt"), b"stable").unwrap();

    let t = make_overlay_test_vfs_with_root(hub, xet, overlay_root);

    t.runtime.block_on(async {
        let entries = t.vfs.readdir(ROOT_INODE).await.unwrap();
        let ino = entries
            .iter()
            .find(|e| e.name == "local.txt")
            .expect("local.txt not in readdir")
            .ino;

        let first_generation = {
            let inodes = t.vfs.inode_table.read().unwrap();
            inodes.get(ino).unwrap().dirty_generation
        };
        assert!(first_generation > 0, "overlay-local entries should be marked dirty");

        {
            let mut inodes = t.vfs.inode_table.write().unwrap();
            inodes.get_mut(ROOT_INODE).unwrap().children_loaded_at = None;
        }

        let entries = t.vfs.readdir(ROOT_INODE).await.unwrap();
        let ino_after_reload = entries
            .iter()
            .find(|e| e.name == "local.txt")
            .expect("local.txt missing after reload")
            .ino;
        let second_generation = {
            let inodes = t.vfs.inode_table.read().unwrap();
            inodes.get(ino_after_reload).unwrap().dirty_generation
        };

        assert_eq!(ino_after_reload, ino);
        assert_eq!(second_generation, first_generation);
    });
}

/// Unlink of a remote-only file returns EPERM in overlay mode.
#[test]
fn overlay_unlink_remote_only_eperm() {
    let hub = MockHub::new();
    hub.add_file("remote.txt", 5, Some("rhash"), None);
    let xet = MockXet::new();

    let overlay_root = fresh_overlay_dir("unlinkremote");
    let t = make_overlay_test_vfs_with_root(hub, xet, overlay_root);

    t.runtime.block_on(async {
        t.vfs.readdir(ROOT_INODE).await.unwrap();
        let err = t.vfs.unlink(ROOT_INODE, "remote.txt").await.unwrap_err();
        assert_eq!(err, libc::EPERM);
    });
}

/// Unlink of a locally-created file succeeds in overlay mode.
#[test]
fn overlay_unlink_local_file_ok() {
    let hub = MockHub::new();
    let xet = MockXet::new();

    let overlay_root = fresh_overlay_dir("unlinklocal");
    let t = make_overlay_test_vfs_with_root(hub.clone(), xet, overlay_root);

    t.runtime.block_on(async {
        let (attr, fh) = t
            .vfs
            .create(ROOT_INODE, "local.txt", 0o644, 1000, 1000, None)
            .await
            .unwrap();
        write_blocking(&t.vfs, attr.ino, fh, 0, b"data").await.unwrap();
        t.vfs.release(fh).await.unwrap();

        t.vfs.unlink(ROOT_INODE, "local.txt").await.unwrap();
    });

    let log = hub.take_batch_log();
    assert!(log.is_empty(), "unlink should not send remote ops in overlay: {log:?}");
}

/// Rename of a remote-only file returns EPERM in overlay mode.
#[test]
fn overlay_rename_remote_only_eperm() {
    let hub = MockHub::new();
    hub.add_file("remote.txt", 5, Some("rhash"), None);
    let xet = MockXet::new();

    let overlay_root = fresh_overlay_dir("renameremote");
    let t = make_overlay_test_vfs_with_root(hub, xet, overlay_root);

    t.runtime.block_on(async {
        t.vfs.readdir(ROOT_INODE).await.unwrap();
        let err = t
            .vfs
            .rename(ROOT_INODE, "remote.txt", ROOT_INODE, "moved.txt", false)
            .await
            .unwrap_err();
        assert_eq!(err, libc::EPERM);
    });
}

/// Rename of a locally-created file moves the on-disk overlay file.
#[test]
fn overlay_rename_local_moves_on_disk() {
    let hub = MockHub::new();
    let xet = MockXet::new();

    let overlay_root = fresh_overlay_dir("renamedisk");
    let t = make_overlay_test_vfs_with_root(hub.clone(), xet, overlay_root);

    t.runtime.block_on(async {
        let (attr, fh) = t
            .vfs
            .create(ROOT_INODE, "src.txt", 0o644, 1000, 1000, None)
            .await
            .unwrap();
        write_blocking(&t.vfs, attr.ino, fh, 0, b"rename me").await.unwrap();
        t.vfs.release(fh).await.unwrap();

        t.vfs
            .rename(ROOT_INODE, "src.txt", ROOT_INODE, "dst.txt", false)
            .await
            .unwrap();
    });

    assert!(!t.overlay_root.join("src.txt").exists(), "old path should be gone");
    assert_eq!(
        std::fs::read_to_string(t.overlay_root.join("dst.txt")).unwrap(),
        "rename me"
    );

    let log = hub.take_batch_log();
    assert!(log.is_empty(), "rename should not send remote ops in overlay: {log:?}");
}

/// setattr truncation in overlay mode writes to the correct overlay path.
#[test]
fn overlay_setattr_truncate_uses_overlay_path() {
    let hub = MockHub::new();
    let xet = MockXet::new();

    let overlay_root = fresh_overlay_dir("setattr");
    let t = make_overlay_test_vfs_with_root(hub, xet, overlay_root);

    t.runtime.block_on(async {
        let (attr, fh) = t
            .vfs
            .create(ROOT_INODE, "trunc.txt", 0o644, 1000, 1000, None)
            .await
            .unwrap();
        write_blocking(&t.vfs, attr.ino, fh, 0, b"hello world").await.unwrap();
        t.vfs.release(fh).await.unwrap();

        // Truncate via setattr
        t.vfs
            .setattr(attr.ino, Some(5), None, None, None, None, None)
            .await
            .unwrap();

        // Re-read: should be 5 bytes
        let fh2 = t.vfs.open(attr.ino, false, false, None).await.unwrap();
        let (data, _eof) = t.vfs.read(fh2, 0, 1024).await.unwrap();
        assert_eq!(data.len(), 5);
    });

    // The truncated file should exist at the overlay path, not an inode-based path
    assert!(t.overlay_root.join("trunc.txt").exists());
}

/// Overlay create/chmod mode changes persist to disk and survive a directory reload.
#[test]
fn overlay_mode_changes_persist_on_disk() {
    use std::os::unix::fs::PermissionsExt;

    let hub = MockHub::new();
    let xet = MockXet::new();

    let overlay_root = fresh_overlay_dir("modepersist");
    let t = make_overlay_test_vfs_with_root(hub, xet, overlay_root);

    t.runtime.block_on(async {
        let (attr, fh) = t
            .vfs
            .create(ROOT_INODE, "mode.txt", 0o600, 1000, 1000, None)
            .await
            .unwrap();
        t.vfs.release(fh).await.unwrap();

        let created_mode = std::fs::metadata(t.overlay_root.join("mode.txt"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(created_mode, 0o600);

        t.vfs
            .setattr(attr.ino, None, Some(0o700), None, None, None, None)
            .await
            .unwrap();

        let updated_mode = std::fs::metadata(t.overlay_root.join("mode.txt"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(updated_mode, 0o700);

        {
            let mut inodes = t.vfs.inode_table.write().unwrap();
            inodes.get_mut(ROOT_INODE).unwrap().children_loaded_at = None;
        }
        t.vfs.readdir(ROOT_INODE).await.unwrap();
        assert_eq!(t.vfs.getattr(attr.ino).unwrap().perm, 0o700);
    });
}

/// setattr truncation of a clean remote file is rejected in overlay mode.
#[test]
fn overlay_setattr_remote_truncate_eperm() {
    let hub = MockHub::new();
    hub.add_file("remote.txt", 11, Some("xhash"), None);
    let xet = MockXet::new();

    let overlay_root = fresh_overlay_dir("setattrremote");
    let t = make_overlay_test_vfs_with_root(hub, xet, overlay_root);

    t.runtime.block_on(async {
        let entries = t.vfs.readdir(ROOT_INODE).await.unwrap();
        let ino = entries.iter().find(|e| e.name == "remote.txt").unwrap().ino;

        let err = t
            .vfs
            .setattr(ino, Some(0), None, None, None, None, None)
            .await
            .unwrap_err();
        assert_eq!(err, libc::EPERM);

        let err = t
            .vfs
            .setattr(ino, None, Some(0o600), None, None, None, None)
            .await
            .unwrap_err();
        assert_eq!(err, libc::EPERM);
    });

    assert!(!t.overlay_root.join("remote.txt").exists());
}

/// When a local file shadows a remote file, xet_hash is cleared so the
/// shadowed entry is treated as local (not remote-only). This means
/// unlink succeeds instead of returning EPERM.
#[test]
fn overlay_local_shadow_clears_xet_hash() {
    let hub = MockHub::new();
    hub.add_file("config.txt", 6, Some("remote_hash"), None);
    let xet = MockXet::new();
    xet.add_file("remote_hash", b"remote");

    let overlay_root = fresh_overlay_dir("shadow");
    std::fs::write(overlay_root.join("config.txt"), b"local override").unwrap();

    let t = make_overlay_test_vfs_with_root(hub.clone(), xet, overlay_root);

    t.runtime.block_on(async {
        t.vfs.readdir(ROOT_INODE).await.unwrap();
        // Unlink should succeed — the local shadow cleared xet_hash,
        // so this is treated as a local (dirty) file, not remote-only.
        t.vfs.unlink(ROOT_INODE, "config.txt").await.unwrap();
    });

    let log = hub.take_batch_log();
    assert!(log.is_empty(), "no remote ops for shadowed file: {log:?}");
}

/// OS junk files in the overlay directory are filtered out of readdir.
#[test]
fn overlay_filters_os_junk() {
    let hub = MockHub::new();
    let xet = MockXet::new();

    let overlay_root = fresh_overlay_dir("junk");
    std::fs::write(overlay_root.join(".DS_Store"), b"junk").unwrap();
    std::fs::write(overlay_root.join("real.txt"), b"keep").unwrap();

    let t = make_overlay_test_vfs_with_root(hub, xet, overlay_root);

    t.runtime.block_on(async {
        let entries = t.vfs.readdir(ROOT_INODE).await.unwrap();
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"real.txt"), "real.txt should be visible");
        assert!(!names.contains(&".DS_Store"), ".DS_Store should be filtered");
    });
}

/// Symlinks in the overlay directory are skipped during readdir merge.
#[test]
fn overlay_skips_symlinks() {
    let hub = MockHub::new();
    let xet = MockXet::new();

    let overlay_root = fresh_overlay_dir("symlink");
    std::fs::write(overlay_root.join("real.txt"), b"content").unwrap();
    // Create a symlink — should be ignored by overlay merge
    if let Err(e) = std::os::unix::fs::symlink(overlay_root.join("real.txt"), overlay_root.join("link.txt")) {
        match e.kind() {
            std::io::ErrorKind::Unsupported | std::io::ErrorKind::PermissionDenied => {
                eprintln!("skipping overlay_skips_symlinks: unable to create symlink: {e}");
                return;
            }
            _ => panic!("failed to create test symlink: {e}"),
        }
    }

    let t = make_overlay_test_vfs_with_root(hub, xet, overlay_root);

    t.runtime.block_on(async {
        let entries = t.vfs.readdir(ROOT_INODE).await.unwrap();
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"real.txt"), "real.txt should be visible");
        assert!(!names.contains(&"link.txt"), "symlinks should be skipped");
    });
}

/// Rename of a clean remote directory returns EPERM in overlay mode.
#[test]
fn overlay_rename_remote_dir_eperm() {
    let hub = MockHub::new();
    hub.add_file("subdir/file.txt", 5, Some("rhash"), None);
    let xet = MockXet::new();

    let overlay_root = fresh_overlay_dir("renamedir");
    let t = make_overlay_test_vfs_with_root(hub, xet, overlay_root);

    t.runtime.block_on(async {
        // Load root to discover subdir
        t.vfs.readdir(ROOT_INODE).await.unwrap();
        let err = t
            .vfs
            .rename(ROOT_INODE, "subdir", ROOT_INODE, "moved", false)
            .await
            .unwrap_err();
        assert_eq!(err, libc::EPERM);
    });
}

/// mkdir in overlay mode marks the directory as dirty so it survives
/// stale-child pruning on reload.
#[test]
fn overlay_mkdir_marks_dirty() {
    let hub = MockHub::new();
    let xet = MockXet::new();

    let overlay_root = fresh_overlay_dir("mkdirdirty");
    let t = make_overlay_test_vfs_with_root(hub, xet, overlay_root);

    t.runtime.block_on(async {
        let attr = t.vfs.mkdir(ROOT_INODE, "newdir", 0o755, 1000, 1000).await.unwrap();
        let inodes = t.vfs.inode_table.read().expect("inodes poisoned");
        let entry = inodes.get(attr.ino).expect("directory inode should exist");
        assert!(entry.is_dirty(), "overlay mkdir should mark directory as dirty");
    });
}

/// rmdir in overlay mode removes the on-disk directory.
#[test]
fn overlay_rmdir_removes_on_disk() {
    let hub = MockHub::new();
    let xet = MockXet::new();

    let overlay_root = fresh_overlay_dir("rmdir");
    let t = make_overlay_test_vfs_with_root(hub, xet, overlay_root);

    t.runtime.block_on(async {
        t.vfs.mkdir(ROOT_INODE, "mydir", 0o755, 1000, 1000).await.unwrap();
        assert!(t.overlay_root.join("mydir").is_dir());

        t.vfs.rmdir(ROOT_INODE, "mydir").await.unwrap();
    });

    assert!(
        !t.overlay_root.join("mydir").exists(),
        "rmdir should remove on-disk overlay directory"
    );
}

/// Overlay merge handles type conflicts: local dir shadows remote file.
#[test]
fn overlay_type_conflict_local_dir_wins() {
    let hub = MockHub::new();
    hub.add_file("conflict", 5, Some("rhash"), None);
    let xet = MockXet::new();

    let overlay_root = fresh_overlay_dir("typeconflict");
    // Local has a directory where remote has a file
    std::fs::create_dir_all(overlay_root.join("conflict")).unwrap();
    std::fs::write(overlay_root.join("conflict/inner.txt"), b"data").unwrap();

    let t = make_overlay_test_vfs_with_root(hub, xet, overlay_root);

    t.runtime.block_on(async {
        let entries = t.vfs.readdir(ROOT_INODE).await.unwrap();
        let entry = entries
            .iter()
            .find(|e| e.name == "conflict")
            .expect("conflict should exist");
        // Local directory should win over remote file
        assert_eq!(entry.kind, InodeKind::Directory);
    });
}

/// rmdir of a clean remote directory returns EPERM in overlay mode.
#[test]
fn overlay_rmdir_remote_dir_eperm() {
    let hub = MockHub::new();
    hub.add_file("subdir/file.txt", 5, Some("rhash"), None);
    let xet = MockXet::new();

    let overlay_root = fresh_overlay_dir("rmdirremote");
    let t = make_overlay_test_vfs_with_root(hub, xet, overlay_root);

    t.runtime.block_on(async {
        // Load root to discover subdir, then load subdir children
        let entries = t.vfs.readdir(ROOT_INODE).await.unwrap();
        let subdir = entries.iter().find(|e| e.name == "subdir").unwrap();
        t.vfs.readdir(subdir.ino).await.unwrap();

        // Unlink the child first so dir is empty
        t.vfs.unlink(subdir.ino, "file.txt").await.unwrap_err(); // EPERM: clean remote file

        // rmdir should also fail: clean remote directory
        let err = t.vfs.rmdir(ROOT_INODE, "subdir").await.unwrap_err();
        assert_eq!(err, libc::EPERM);
    });
}

// ── Sparse-write integration tests ──────────────────────────────────

/// Open an existing file for write: staging is sparse (no download), inode tracks
/// SparseWriteState pointing at the original CAS hash.
#[test]
fn sparse_open_for_write_no_download() {
    let hub = MockHub::new();
    hub.add_file("sparse.txt", 26, Some("hash_orig"), None);
    let xet = MockXet::new();
    xet.add_file("hash_orig", b"ABCDEFGHIJKLMNOPQRSTUVWXYZ");

    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "sparse.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, false, None).await.unwrap();

        // sparse_write set, no dirty ranges, no CAS download
        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            assert!(entry.is_dirty(), "open-for-write marks dirty");
            let sw = entry.sparse_write.as_ref().expect("sparse_write set");
            assert_eq!(sw.original_hash, "hash_orig");
            assert_eq!(sw.original_size, 26);
            assert!(sw.dirty_ranges.is_empty());
        }

        // Staging file is the right size but contains zeros (no download happened)
        let staging_path = vfs.staging.path(ino).expect("staging path");
        let on_disk = std::fs::read(&staging_path).unwrap();
        assert_eq!(on_disk.len(), 26);
        assert_eq!(&on_disk[..], &[0u8; 26]);

        vfs.release(fh).await.unwrap();
    });
}

/// Read of a sparse staging file returns original CAS data via `fill_sparse_holes`.
#[test]
fn sparse_read_unwritten_returns_cas() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("orig_hash"), None);
    let xet = MockXet::new();
    xet.add_file("orig_hash", b"0123456789");
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, false, None).await.unwrap();

        let (data, _) = vfs.read(fh, 0, 10).await.unwrap();
        assert_eq!(&data[..], b"0123456789");

        vfs.release(fh).await.unwrap();
    });
}

/// Mid-file write + read: dirty bytes from staging, holes filled from CAS.
#[test]
fn sparse_read_dirty_and_hole() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("orig_hash"), None);
    let xet = MockXet::new();
    xet.add_file("orig_hash", b"0123456789");
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, false, None).await.unwrap();

        write_blocking(&vfs, ino, fh, 5, b"ABC").await.unwrap();

        let (data, _) = vfs.read(fh, 0, 10).await.unwrap();
        assert_eq!(&data[..], b"01234ABC89");

        vfs.release(fh).await.unwrap();
    });
}

/// Write past EOF: dirty range starts at original_size, file grows.
#[test]
fn sparse_append_then_read() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("orig_hash"), None);
    let xet = MockXet::new();
    xet.add_file("orig_hash", b"0123456789");
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, false, None).await.unwrap();

        write_blocking(&vfs, ino, fh, 10, b"XYZ").await.unwrap();

        let (data, _) = vfs.read(fh, 0, 20).await.unwrap();
        assert_eq!(&data[..], b"0123456789XYZ");

        vfs.release(fh).await.unwrap();
    });
}

/// Write at offset 0: prefix becomes dirty, suffix still comes from CAS.
#[test]
fn sparse_write_at_zero_then_read() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("orig_hash"), None);
    let xet = MockXet::new();
    xet.add_file("orig_hash", b"0123456789");
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, false, None).await.unwrap();

        write_blocking(&vfs, ino, fh, 0, b"abc").await.unwrap();

        let (data, _) = vfs.read(fh, 0, 10).await.unwrap();
        assert_eq!(&data[..], b"abc3456789");

        vfs.release(fh).await.unwrap();
    });
}

/// Multiple non-adjacent writes: each tracked separately.
#[test]
fn sparse_multiple_writes_then_read() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("orig_hash"), None);
    let xet = MockXet::new();
    xet.add_file("orig_hash", b"0123456789");
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, false, None).await.unwrap();

        write_blocking(&vfs, ino, fh, 2, b"XX").await.unwrap();
        write_blocking(&vfs, ino, fh, 7, b"YY").await.unwrap();

        let (data, _) = vfs.read(fh, 0, 10).await.unwrap();
        assert_eq!(&data[..], b"01XX456YY9");

        // Two non-adjacent dirty ranges
        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            let sw = entry.sparse_write.as_ref().expect("sparse_write set");
            assert_eq!(sw.dirty_ranges, vec![(2, 4), (7, 9)]);
        }
        vfs.release(fh).await.unwrap();
    });
}

/// Open with truncate: sparse_write cleared, no CAS download.
#[test]
fn sparse_open_with_truncate_clears_state() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("orig_hash"), None);
    let xet = MockXet::new();
    xet.add_file("orig_hash", b"0123456789");
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, true, None).await.unwrap();

        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            assert!(entry.is_dirty());
            assert_eq!(entry.size, 0);
            assert!(entry.sparse_write.is_none(), "truncate clears sparse state");
        }

        vfs.release(fh).await.unwrap();
    });
}

/// setattr(truncate to N < original_size) trims dirty ranges and clips effective_original_size
/// (but leaves original_size — the immutable CAS object size — intact).
#[test]
fn sparse_setattr_shrink_clips_state() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("orig_hash"), None);
    let xet = MockXet::new();
    xet.add_file("orig_hash", b"0123456789");
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, false, None).await.unwrap();

        // Write past original size, then truncate below it
        write_blocking(&vfs, ino, fh, 8, b"XXX").await.unwrap();
        vfs.setattr(ino, Some(5), None, None, None, None, None).await.unwrap();

        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            let sw = entry.sparse_write.as_ref().expect("sparse_write preserved on shrink");
            assert_eq!(sw.original_size, 10, "original_size is the immutable CAS size");
            assert_eq!(
                sw.effective_original_size, 5,
                "effective_original_size clipped to truncate target"
            );
            assert!(sw.dirty_ranges.is_empty(), "dirty ranges past 5 are trimmed");
        }

        vfs.release(fh).await.unwrap();
    });
}
