use std::time::Duration;

use super::inode::ROOT_INODE;
use super::*;
use crate::hub_api::HeadFileInfo;
use crate::test_mocks::{MockHub, MockXet, TestOpts, make_test_vfs};

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
            assert_eq!(entry.full_path, "new.txt");
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
        assert_eq!(entry.full_path, "dst.txt");
        assert_eq!(entry.name, "dst.txt");
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
            assert_eq!(entry.full_path, "dst.txt");
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

/// Opening a plain file without xet_hash (LFS/git) uses HTTP download.
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

/// Poll with multiple loaded directories fetches each independently.
#[test]
fn poll_multiple_loaded_dirs() {
    let hub = MockHub::new_repo();
    hub.add_file("root.txt", 10, Some("h1"), None);
    hub.add_file("sub/nested.txt", 20, Some("h2"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_repo(&hub, &xet);

    rt.block_on(async {
        // Load both root and sub directory.
        let _ = vfs.lookup(ROOT_INODE, "root.txt").await.unwrap();
        let sub = vfs.lookup(ROOT_INODE, "sub").await.unwrap();
        let _ = vfs.lookup(sub.ino, "nested.txt").await.unwrap();

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

/// fsync on an advanced-writes handle triggers a synchronous flush to Hub.
#[test]
fn fsync_advanced_writes_commits() {
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

        // fsync triggers synchronous flush
        vfs.fsync(ino, fh, None).await.unwrap();

        // After fsync: inode is clean, hash is set
        let entry = vfs.inode_table.read().unwrap().get(ino).unwrap().clone();
        assert!(!entry.is_dirty());
        assert!(entry.xet_hash.is_some());

        vfs.release(fh).await.unwrap();
    });
}

/// fsync on an empty file (0 bytes) goes through CAS upload + Hub commit
/// like any other file. The inode should be clean after fsync.
#[test]
fn fsync_empty_file_commits() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let (_, fh) = vfs.create(ROOT_INODE, "empty", 0o644, 0, 0, None).await.unwrap();
        let ino = ROOT_INODE + 1;

        // No write — staging file stays empty (0 bytes)
        assert!(vfs.inode_table.read().unwrap().get(ino).unwrap().is_dirty());

        vfs.fsync(ino, fh, None).await.unwrap();

        // After fsync: inode is clean, hash is set (CAS returns a hash even for empty files)
        let entry = vfs.inode_table.read().unwrap().get(ino).unwrap().clone();
        assert!(!entry.is_dirty(), "empty file should be clean after fsync");
        assert!(entry.xet_hash.is_some(), "empty file should have xet_hash from CAS");
        assert_eq!(entry.size, 0);

        // Hub should have received a batch commit
        let logs = hub.take_batch_log();
        assert!(!logs.is_empty(), "empty file should be committed to Hub");

        vfs.release(fh).await.unwrap();
    });
}

/// Race test: a concurrent writer advances dirty_generation while fsync is
/// mid-flight (between upload and Hub commit). The inode must stay dirty
/// because the concurrent writer's data hasn't been flushed yet.
#[test]
fn dirty_generation_race_with_concurrent_writer() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let (_, fh) = vfs.create(ROOT_INODE, "race", 0o644, 0, 0, None).await.unwrap();
        let ino = ROOT_INODE + 1;
        write_blocking(&vfs, ino, fh, 0, b"writer A").await.unwrap();

        // Set a barrier on batch_operations: fsync will pause after upload,
        // before commit. We inject a concurrent write at that point.
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        hub.set_batch_barrier(barrier.clone());

        // Launch fsync in background — it will block at the barrier inside batch_operations
        let vfs2 = vfs.clone();
        let fsync_handle = tokio::spawn(async move { vfs2.fsync(ino, fh, None).await });

        // Wait for fsync to reach the barrier (upload done, about to commit)
        barrier.wait().await;

        // Concurrent writer B advances the generation while fsync is mid-commit
        vfs.inode_table.write().unwrap().get_mut(ino).unwrap().set_dirty();

        // Clear barrier so future batch_operations don't block
        hub.set_batch_barrier(std::sync::Arc::new(tokio::sync::Barrier::new(1)));

        // fsync completes (batch_operations proceeds after barrier)
        fsync_handle.await.unwrap().unwrap();

        // Inode must STILL be dirty: writer B advanced the generation,
        // so clear_dirty_if(snapshot) returned false.
        let entry = vfs.inode_table.read().unwrap().get(ino).unwrap().clone();
        assert!(
            entry.is_dirty(),
            "inode must stay dirty when concurrent writer advanced generation during flush"
        );
        // But the hash WAS updated (the upload+commit succeeded)
        assert!(entry.xet_hash.is_some());

        vfs.release(fh).await.unwrap();
    });
}

/// After fsync commits, a subsequent write re-dirties the inode.
#[test]
fn fsync_then_write_redirties() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let (_, fh) = vfs.create(ROOT_INODE, "redirty", 0o644, 0, 0, None).await.unwrap();
        let ino = ROOT_INODE + 1;

        // Write + fsync -> clean
        write_blocking(&vfs, ino, fh, 0, b"initial").await.unwrap();
        assert!(vfs.inode_table.read().unwrap().get(ino).unwrap().is_dirty());

        vfs.fsync(ino, fh, None).await.unwrap();
        assert!(
            !vfs.inode_table.read().unwrap().get(ino).unwrap().is_dirty(),
            "should be clean after fsync"
        );

        // Write again -> must re-dirty
        let result = vfs.write(ino, fh, 0, b"more");
        assert!(result.is_ok(), "write after fsync should succeed");
        assert!(
            vfs.inode_table.read().unwrap().get(ino).unwrap().is_dirty(),
            "should be dirty again after write"
        );

        vfs.release(fh).await.unwrap();
    });
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
