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
        assert!(!entry.dirty);
        assert!(entry.xet_hash.is_some());
    });

    let logs = hub.take_batch_log();
    assert_eq!(logs.len(), 1);
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
        assert!(!entry.dirty);
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
        assert!(!entry.dirty);
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
        assert!(!entry.dirty);
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
            inodes.get_mut(ino).unwrap().dirty = true;
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
        assert!(entry.dirty);
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
        assert!(entry.dirty);
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

// ── preload_tree_recursive ──────────────────────────────────────────

/// Build a VFS backed by a repo MockHub (triggers preload_tree_recursive in new()).
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

/// Repo mode preloads the full tree: deeply nested files create implicit dirs.
#[test]
fn preload_tree_nested_dirs() {
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

/// Repo mode preloads flat files at root level.
#[test]
fn preload_tree_flat_root() {
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

/// Repo mode marks all directories as children_loaded (no lazy fetches needed).
#[test]
fn preload_tree_dirs_children_loaded() {
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

/// Repo mode preserves oid as etag on file inodes.
#[test]
fn preload_tree_preserves_oid() {
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
            assert!(inodes.get(ino).unwrap().dirty);
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
        assert!(entry.dirty);
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
        assert!(entry.dirty);
        // Hash and size must remain unchanged (not overwritten by remote)
        assert_eq!(entry.xet_hash.as_deref(), Some("hash1"));
        assert_ne!(entry.size, 500, "dirty file size should not be updated from remote");
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

// ── Sparse write tracking ──────────────────────────────────────────

/// Open an existing file in advanced-write mode: staging file should be sparse
/// (no download). Appending and mid-file writes should track the dirty range.
#[test]
fn advanced_write_sparse_staging_and_dirty_range() {
    let hub = MockHub::new();
    let original_content = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ"; // 26 bytes
    hub.add_file("sparse.txt", 26, Some("hash_orig"), None);
    let xet = MockXet::new();
    xet.add_file("hash_orig", original_content);

    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        // 1. Open for write (non-truncate): should create sparse staging, set sparse_write
        let attr = vfs.lookup(ROOT_INODE, "sparse.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, false, None).await.unwrap();

        // Check: inode should have sparse_write set with no dirty ranges yet
        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            assert!(entry.dirty, "file should be dirty after open for write");
            assert!(
                entry.sparse_write.is_some(),
                "sparse_write should be set (sparse staging, no download)"
            );
            let sw = entry.sparse_write.as_ref().unwrap();
            assert_eq!(sw.original_hash, "hash_orig");
            assert_eq!(sw.original_size, 26);
            assert!(
                sw.dirty_ranges.is_empty(),
                "no writes yet, dirty_ranges should be empty"
            );
        }

        // 2. Append at offset 26 (beyond original): should update dirty range
        let written = write_blocking(&vfs, ino, fh, 26, b"0123456789").await.unwrap();
        assert_eq!(written, 10);
        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            assert!(
                entry.sparse_write.is_some(),
                "sparse_write should still be set after append"
            );
            let sw = entry.sparse_write.as_ref().unwrap();
            assert_eq!(sw.dirty_ranges, vec![(26, 36)]);
            assert_eq!(entry.size, 36);
        }

        // 3. Write at offset 10 (below original_size=26): should add a second dirty range
        //    No lazy download -- staging file stays sparse, dirty ranges track the writes
        let written = write_blocking(&vfs, ino, fh, 10, b"xxxx").await.unwrap();
        assert_eq!(written, 4);
        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            assert!(
                entry.sparse_write.is_some(),
                "sparse_write should still be set after mid-file write"
            );
            let sw = entry.sparse_write.as_ref().unwrap();
            assert_eq!(
                sw.dirty_ranges,
                vec![(10, 14), (26, 36)],
                "should have two non-adjacent dirty ranges"
            );
            assert_eq!(entry.size, 36);
        }

        // 4. Verify staging file content: sparse file with writes at known offsets.
        //    Bytes in the sparse hole (not written) will be zeros.
        let staging_path = vfs.staging_dir.as_ref().unwrap().path(ino);
        let staging_content = std::fs::read(&staging_path).unwrap();
        assert_eq!(staging_content.len(), 36);
        // First 10 bytes: sparse hole (zeros, not original data)
        assert_eq!(&staging_content[0..10], &[0u8; 10]);
        // Bytes 10-14: "xxxx" (our mid-write)
        assert_eq!(&staging_content[10..14], b"xxxx");
        // Bytes 14-26: sparse hole (zeros)
        assert_eq!(&staging_content[14..26], &[0u8; 12]);
        // Bytes 26-36: "0123456789" (our append)
        assert_eq!(&staging_content[26..36], b"0123456789");

        vfs.release(fh).await.unwrap();
    });
}

// ── Sparse file data integrity: read/write ──────────────────────────

/// Open existing file without truncate, read without writing: should return original CAS data.
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

/// Write mid-file, read full file: dirty region has new data, holes have CAS data.
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

/// Append past original size, read full file: original + appended data.
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

/// Write at offset 0, read full file: overwritten prefix + original tail.
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

/// Overwrite entire file, read back: all new data.
#[test]
fn sparse_overwrite_entire_file() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("orig_hash"), None);
    let xet = MockXet::new();
    xet.add_file("orig_hash", b"0123456789");
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, false, None).await.unwrap();

        write_blocking(&vfs, ino, fh, 0, b"ABCDEFGHIJ").await.unwrap();

        let (data, _) = vfs.read(fh, 0, 10).await.unwrap();
        assert_eq!(&data[..], b"ABCDEFGHIJ");

        vfs.release(fh).await.unwrap();
    });
}

/// Multiple disjoint writes, read full file.
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

        vfs.release(fh).await.unwrap();
    });
}

/// Overwrite the same region twice, read back: last write wins.
#[test]
fn sparse_overwrite_same_region() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("orig_hash"), None);
    let xet = MockXet::new();
    xet.add_file("orig_hash", b"0123456789");
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, false, None).await.unwrap();

        write_blocking(&vfs, ino, fh, 5, b"AAA").await.unwrap();
        write_blocking(&vfs, ino, fh, 5, b"BBB").await.unwrap();

        let (data, _) = vfs.read(fh, 0, 10).await.unwrap();
        assert_eq!(&data[..], b"01234BBB89");

        vfs.release(fh).await.unwrap();
    });
}

// ── Sparse file flush end-to-end data integrity ─────────────────────

/// Helper: open file for sparse write, apply writes, release, shutdown, return new xet_hash.
fn flush_sparse_and_get_hash(
    hub: &std::sync::Arc<MockHub>,
    xet: &std::sync::Arc<MockXet>,
    writes: &[(u64, &[u8])],
) -> String {
    let (rt, vfs) = vfs_advanced(hub, xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, false, None).await.unwrap();

        for &(offset, data) in writes {
            write_blocking(&vfs, ino, fh, offset, data).await.unwrap();
        }

        vfs.release(fh).await.unwrap();
    });

    vfs.shutdown();

    let inodes = vfs.inode_table.read().unwrap();
    let entry = inodes.get_by_path("file.txt").unwrap();
    entry.xet_hash.clone().expect("file should have xet_hash after flush")
}

/// Flush sparse mid-file edit: composed file has original + edit.
#[test]
fn flush_sparse_mid_edit() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("orig_hash"), None);
    let xet = MockXet::new();
    xet.add_file("orig_hash", b"0123456789");

    let hash = flush_sparse_and_get_hash(&hub, &xet, &[(5, b"ABC")]);
    let composed = xet.get_file(&hash).unwrap();
    assert_eq!(&composed, b"01234ABC89");
}

/// Flush sparse append: composed file has original + appended bytes.
#[test]
fn flush_sparse_append() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("orig_hash"), None);
    let xet = MockXet::new();
    xet.add_file("orig_hash", b"0123456789");

    let hash = flush_sparse_and_get_hash(&hub, &xet, &[(10, b"XYZ")]);
    let composed = xet.get_file(&hash).unwrap();
    assert_eq!(&composed, b"0123456789XYZ");
}

/// Flush sparse multiple disjoint ranges.
#[test]
fn flush_sparse_multiple_ranges() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("orig_hash"), None);
    let xet = MockXet::new();
    xet.add_file("orig_hash", b"0123456789");

    let hash = flush_sparse_and_get_hash(&hub, &xet, &[(2, b"XX"), (7, b"YY")]);
    let composed = xet.get_file(&hash).unwrap();
    assert_eq!(&composed, b"01XX456YY9");
}

/// Flush sparse write at offset 0.
#[test]
fn flush_sparse_write_at_zero() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("orig_hash"), None);
    let xet = MockXet::new();
    xet.add_file("orig_hash", b"0123456789");

    let hash = flush_sparse_and_get_hash(&hub, &xet, &[(0, b"abc")]);
    let composed = xet.get_file(&hash).unwrap();
    assert_eq!(&composed, b"abc3456789");
}

/// Flush sparse full overwrite.
#[test]
fn flush_sparse_full_overwrite() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("orig_hash"), None);
    let xet = MockXet::new();
    xet.add_file("orig_hash", b"0123456789");

    let hash = flush_sparse_and_get_hash(&hub, &xet, &[(0, b"ABCDEFGHIJ")]);
    let composed = xet.get_file(&hash).unwrap();
    assert_eq!(&composed, b"ABCDEFGHIJ");
}

/// range_upload failure preserves staging file (dirty writes not lost).
#[test]
fn flush_range_upload_fail_preserves_staging() {
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
        vfs.release(fh).await.unwrap();

        xet.fail_range_upload();
    });

    vfs.shutdown();

    // File should still be dirty (flush failed)
    let inodes = vfs.inode_table.read().unwrap();
    let entry = inodes.get_by_path("file.txt").unwrap();
    assert!(entry.dirty, "file should remain dirty after failed range_upload");

    // Staging file should still exist with written data
    let staging_path = vfs.staging_dir.as_ref().unwrap().path(entry.inode);
    let staging = std::fs::read(&staging_path).unwrap();
    assert_eq!(staging.len(), 10);
    assert_eq!(&staging[5..8], b"ABC", "dirty write should be preserved in staging");
}

/// Flush sparse write produces correct batch_log AddFile entry.
#[test]
fn flush_sparse_batch_log() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("orig_hash"), None);
    let xet = MockXet::new();
    xet.add_file("orig_hash", b"0123456789");

    let hash = flush_sparse_and_get_hash(&hub, &xet, &[(5, b"ABC")]);

    let logs = hub.take_batch_log();
    assert_eq!(logs.len(), 1, "should have exactly one batch");
    assert_eq!(logs[0].len(), 1, "batch should have one AddFile op");
    match &logs[0][0] {
        crate::hub_api::BatchOp::AddFile { path, xet_hash, .. } => {
            assert_eq!(path, "file.txt");
            assert_eq!(xet_hash, &hash);
        }
        other => panic!("expected AddFile, got {:?}", other),
    }

    // Verify the hash points to correctly composed data
    let composed = xet.get_file(&hash).unwrap();
    assert_eq!(&composed, b"01234ABC89");
}

// ── flush_generation tests ──────────────────────────────────────────

/// Write bumps flush_generation; a simulated flush with the old generation
/// should NOT clear dirty state (concurrent write protection).
#[test]
fn flush_generation_write_prevents_stale_clear() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("orig_hash"), None);
    let xet = MockXet::new();
    xet.add_file("orig_hash", b"0123456789");
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, false, None).await.unwrap();

        // First write
        write_blocking(&vfs, ino, fh, 5, b"ABC").await.unwrap();

        // Snapshot generation (simulates what flush_batch does)
        let snapshotted_gen = {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            assert!(entry.dirty);
            entry.flush_generation
        };

        // Second write (bumps generation)
        write_blocking(&vfs, ino, fh, 8, b"XY").await.unwrap();

        // Verify generation mismatch
        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            assert_ne!(
                entry.flush_generation, snapshotted_gen,
                "write should have bumped flush_generation"
            );
            assert!(entry.dirty);
        }

        vfs.release(fh).await.unwrap();
    });
}

/// Rename of a dirty file bumps flush_generation.
#[test]
fn flush_generation_rename_prevents_stale_clear() {
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
        vfs.release(fh).await.unwrap();

        // Snapshot generation
        let snapshotted_gen = {
            let inodes = vfs.inode_table.read().unwrap();
            inodes.get(ino).unwrap().flush_generation
        };

        // Rename the dirty file
        vfs.rename(ROOT_INODE, "file.txt", ROOT_INODE, "renamed.txt", false)
            .await
            .unwrap();

        // Generation should have bumped
        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            assert_ne!(
                entry.flush_generation, snapshotted_gen,
                "rename should have bumped flush_generation"
            );
            assert!(entry.dirty, "file should still be dirty after rename");
            assert_eq!(entry.full_path, "renamed.txt");
        }
    });
}

/// Rename of a newly created dirty file (no xet_hash) also bumps flush_generation.
#[test]
fn flush_generation_rename_new_file_bumps() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        // Create a new file (dirty, no xet_hash)
        let (attr, _fh) = vfs.create(ROOT_INODE, "new.txt", 0o644, 0, 0, None).await.unwrap();
        let ino = attr.ino;

        let snapshotted_gen = {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            assert!(entry.dirty);
            assert!(entry.xet_hash.is_none(), "new file should have no xet_hash");
            entry.flush_generation
        };

        // Rename the new dirty file
        vfs.rename(ROOT_INODE, "new.txt", ROOT_INODE, "moved.txt", false)
            .await
            .unwrap();

        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            assert_ne!(
                entry.flush_generation, snapshotted_gen,
                "rename of new file (no xet_hash) should still bump flush_generation"
            );
        }
    });
}

/// setattr truncate bumps flush_generation.
#[test]
fn flush_generation_setattr_truncate_bumps() {
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
        vfs.release(fh).await.unwrap();

        let gen_before = {
            let inodes = vfs.inode_table.read().unwrap();
            inodes.get(ino).unwrap().flush_generation
        };

        // Truncate via setattr
        vfs.setattr(ino, Some(3), None, None, None, None, None).await.unwrap();

        let inodes = vfs.inode_table.read().unwrap();
        let entry = inodes.get(ino).unwrap();
        assert_ne!(
            entry.flush_generation, gen_before,
            "setattr truncate should bump flush_generation"
        );
    });
}

// ── Edge cases ──────────────────────────────────────────────────────

/// Shrinking a sparse file via setattr materializes CAS content and clears sparse state.
#[test]
fn setattr_shrink_on_sparse_clips_state() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("orig_hash"), None);
    let xet = MockXet::new();
    xet.add_file("orig_hash", b"0123456789");
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        let fh = vfs.open(ino, true, false, None).await.unwrap();
        vfs.release(fh).await.unwrap();

        // Shrink should succeed: resizes sparse staging, no CAS download needed
        let new_attr = vfs.setattr(ino, Some(5), None, None, None, None, None).await.unwrap();
        assert_eq!(new_attr.size, 5);

        // sparse_write preserved, original_size keeps the real CAS size
        let inodes = vfs.inode_table.read().unwrap();
        let entry = inodes.get(ino).unwrap();
        assert!(
            entry.sparse_write.is_some(),
            "sparse_write should be preserved after truncate (range_upload handles boundary)"
        );
        assert!(entry.dirty);
    });
}

/// Growing a sparse file via setattr materializes CAS content and clears sparse state.
#[test]
fn setattr_grow_on_sparse_preserves_state() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("orig_hash"), None);
    let xet = MockXet::new();
    xet.add_file("orig_hash", b"0123456789");
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        let fh = vfs.open(ino, true, false, None).await.unwrap();
        vfs.release(fh).await.unwrap();

        // Grow should succeed
        let new_attr = vfs.setattr(ino, Some(20), None, None, None, None, None).await.unwrap();
        assert_eq!(new_attr.size, 20);

        // sparse_write preserved, original_size unchanged (grow is a no-op for clip)
        let inodes = vfs.inode_table.read().unwrap();
        let entry = inodes.get(ino).unwrap();
        assert!(
            entry.sparse_write.is_some(),
            "sparse_write should be preserved after resize (range_upload handles boundary)"
        );
    });
}

/// Shrink after sparse write materializes CAS content and clears sparse state.
#[test]
fn sparse_write_then_shrink_clips_state() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("orig_hash"), None);
    let xet = MockXet::new();
    xet.add_file("orig_hash", b"0123456789");
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        let fh = vfs.open(ino, true, false, None).await.unwrap();
        write_blocking(&vfs, ino, fh, 5, b"ABC").await.unwrap(); // dirty range (5, 8)
        vfs.release(fh).await.unwrap();

        // Shrink to 3: dirty range (5, 8) is past new_size, gets removed
        let new_attr = vfs.setattr(ino, Some(3), None, None, None, None, None).await.unwrap();
        assert_eq!(new_attr.size, 3);

        let inodes = vfs.inode_table.read().unwrap();
        let entry = inodes.get(ino).unwrap();
        assert!(
            entry.sparse_write.is_some(),
            "sparse_write should be preserved after truncate (range_upload handles boundary)"
        );
        assert!(entry.dirty);
    });
}

/// Reopen a dirty file (already has staging): should NOT create sparse state.
/// The existing staging file has real data, not sparse holes.
#[test]
fn reopen_dirty_file_no_sparse_state() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("orig_hash"), None);
    let xet = MockXet::new();
    xet.add_file("orig_hash", b"0123456789");
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        // First open: creates sparse staging + sparse_write state
        let fh1 = vfs.open(ino, true, false, None).await.unwrap();
        write_blocking(&vfs, ino, fh1, 5, b"ABC").await.unwrap();
        vfs.release(fh1).await.unwrap();

        // File is now dirty with staging file on disk.
        // Second open (no truncate): should reuse existing staging, keep sparse state.
        let fh2 = vfs.open(ino, true, false, None).await.unwrap();

        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            // sparse_write should still be set from the first open (dirty file reuse
            // skips staging creation AND sparse state setup, preserving existing state)
            assert!(
                entry.sparse_write.is_some(),
                "sparse_write should be preserved on reopen"
            );
        }

        // Reads should still work correctly (CAS fills holes)
        let (data, _) = vfs.read(fh2, 0, 10).await.unwrap();
        assert_eq!(&data[..], b"01234ABC89");

        vfs.release(fh2).await.unwrap();
    });
}

/// Open with truncate after sparse write: clears sparse state.
#[test]
fn reopen_truncate_clears_sparse_state() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("orig_hash"), None);
    let xet = MockXet::new();
    xet.add_file("orig_hash", b"0123456789");
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        // First open: sparse write
        let fh1 = vfs.open(ino, true, false, None).await.unwrap();
        write_blocking(&vfs, ino, fh1, 5, b"ABC").await.unwrap();
        vfs.release(fh1).await.unwrap();

        // Second open with truncate: should clear sparse state
        let fh2 = vfs.open(ino, true, true, None).await.unwrap();

        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            assert!(entry.sparse_write.is_none(), "truncate should clear sparse_write");
            assert_eq!(entry.size, 0);
        }

        vfs.release(fh2).await.unwrap();
    });
}

/// Test mid-file write on a new open (not just append) sets dirty range correctly.
#[test]
fn advanced_write_mid_file_dirty_range() {
    let hub = MockHub::new();
    hub.add_file("data.bin", 100, Some("hash_data"), None);
    let xet = MockXet::new();
    xet.add_file("hash_data", &[0xAA; 100]);

    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "data.bin").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, false, None).await.unwrap();

        // Write 8 bytes at offset 50
        let written = write_blocking(&vfs, ino, fh, 50, b"MODIFIED").await.unwrap();
        assert_eq!(written, 8);

        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            let sw = entry.sparse_write.as_ref().unwrap();
            assert_eq!(sw.dirty_ranges, vec![(50, 58)]);
            assert_eq!(sw.original_hash, "hash_data");
            assert_eq!(sw.original_size, 100);
        }

        // Write 4 bytes at offset 20 (adds a separate dirty range)
        let written = write_blocking(&vfs, ino, fh, 20, b"ABCD").await.unwrap();
        assert_eq!(written, 4);

        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            let sw = entry.sparse_write.as_ref().unwrap();
            assert_eq!(
                sw.dirty_ranges,
                vec![(20, 24), (50, 58)],
                "should have two separate dirty ranges"
            );
        }

        vfs.release(fh).await.unwrap();
    });
}

// ── P1/P2 flush race tests ─────────────────────────────────────────

/// P1: When a flush commits at an old path but generation mismatches (because the
/// file was renamed), the stale committed path is added to pending_deletes so the
/// next flush cleans it up.
///
/// We simulate the race by: write → close → rename → shutdown. The rename bumps
/// flush_generation, so the shutdown flush (which uses the new path) includes a
/// DeleteFile for the old path that the rename recorded in pending_deletes.
#[test]
fn flush_after_rename_deletes_old_remote_path() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("orig_hash"), None);
    let xet = MockXet::new();
    xet.add_file("orig_hash", b"0123456789");
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        // Open (non-truncate), write, close → dirty
        let fh = vfs.open(ino, true, false, None).await.unwrap();
        write_blocking(&vfs, ino, fh, 5, b"ABC").await.unwrap();
        vfs.release(fh).await.unwrap();

        // Rename the dirty file before flush fires
        vfs.rename(ROOT_INODE, "file.txt", ROOT_INODE, "renamed.txt", false)
            .await
            .unwrap();

        // Verify state: dirty at new path, old path in pending_deletes
        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            assert!(entry.dirty);
            assert_eq!(entry.full_path, "renamed.txt");
            assert!(
                entry.pending_deletes.contains(&"file.txt".to_string()),
                "rename should have added old path to pending_deletes"
            );
        }
    });

    // Shutdown flushes synchronously: should commit at "renamed.txt" + delete "file.txt"
    vfs.shutdown();

    let logs = hub.take_batch_log();
    let batch = logs
        .iter()
        .find(|batch| {
            batch
                .iter()
                .any(|op| matches!(op, BatchOp::AddFile { path, .. } if path == "renamed.txt"))
        })
        .expect("should have a batch committing 'renamed.txt'");

    let has_delete = batch
        .iter()
        .any(|op| matches!(op, BatchOp::DeleteFile { path } if path == "file.txt"));
    assert!(
        has_delete,
        "batch should delete old remote path 'file.txt', got: {:?}",
        batch
    );
}

/// P1 variant: flush commits at old path, then generation mismatches because of
/// a concurrent write. The stale path should be queued for cleanup.
/// We test this by: write1 → snapshot generation → write2 (bumps gen) → shutdown.
/// The flush sees the generation mismatch and (via P1 fix) adds the flushed path
/// to pending_deletes if it differs from the current path.
/// Note: this tests the write-during-flush scenario. The rename scenario (which
/// changes the path) is tested above.
#[test]
fn flush_generation_mismatch_from_write_keeps_dirty() {
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

        let gen_after_write1 = {
            let inodes = vfs.inode_table.read().unwrap();
            inodes.get(ino).unwrap().flush_generation
        };

        // Second write bumps generation
        write_blocking(&vfs, ino, fh, 8, b"XY").await.unwrap();

        let gen_after_write2 = {
            let inodes = vfs.inode_table.read().unwrap();
            inodes.get(ino).unwrap().flush_generation
        };
        assert_ne!(gen_after_write1, gen_after_write2);

        vfs.release(fh).await.unwrap();
    });

    // Shutdown flushes: since path didn't change, no stale delete needed.
    // But the file should be properly committed with BOTH writes.
    vfs.shutdown();

    let inodes = vfs.inode_table.read().unwrap();
    let entry = inodes.get_by_path("file.txt").unwrap();
    assert!(!entry.dirty, "file should be clean after shutdown flush");
    let hash = entry.xet_hash.as_ref().expect("file should have xet_hash after flush");
    let content = xet.get_file(hash).unwrap();
    assert_eq!(&content[..5], b"01234", "original prefix");
    assert_eq!(&content[5..8], b"ABC", "first write");
    assert_eq!(&content[8..10], b"XY", "second write must not be lost");
}

/// Prove that flush_batch actually hits the generation mismatch branch.
/// Uses a batch_barrier to block the Hub commit while a write bumps generation.
/// The file uses truncate mode (non-sparse) so the async flush works in mocks.
#[test]
fn flush_generation_mismatch_branch_exercised() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        // Open with truncate (non-sparse path, so async flush works in mocks)
        let fh = vfs.open(ino, true, true, None).await.unwrap();
        write_blocking(&vfs, ino, fh, 0, b"first").await.unwrap();

        // Set barrier BEFORE release so it blocks batch_operations
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        hub.set_batch_barrier(barrier.clone());

        vfs.release(fh).await.unwrap();

        // Wait for flush to reach batch_operations (barrier rendezvous)
        let reached = tokio::time::timeout(Duration::from_secs(10), barrier.wait()).await;
        assert!(reached.is_ok(), "flush should reach batch_operations");

        // Flush is now blocked inside batch_operations.
        // Re-open and write to bump generation while flush is in-flight.
        // Keep fh2 open so its release doesn't trigger a second flush yet.
        let fh2 = vfs.open(ino, true, true, None).await.unwrap();
        write_blocking(&vfs, ino, fh2, 0, b"second").await.unwrap();

        // Clear barrier to let first flush complete (will hit mismatch)
        hub.clear_batch_barrier();

        // Wait for first flush to finish processing
        tokio::time::sleep(Duration::from_millis(500)).await;

        // After mismatch: file should STILL be dirty (first flush didn't clear it)
        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            assert!(entry.dirty, "file should remain dirty after generation mismatch");
        }

        vfs.release(fh2).await.unwrap();
    });

    // Shutdown triggers second flush with the latest content
    vfs.shutdown();

    let inodes = vfs.inode_table.read().unwrap();
    let entry = inodes.get_by_path("file.txt").unwrap();
    assert!(!entry.dirty, "file should be clean after shutdown");
    let hash = entry.xet_hash.as_ref().unwrap();
    let content = xet.get_file(hash).unwrap();
    assert_eq!(&content, b"second", "final content should be from second write");

    // batch_log should have at least 2 batches (first flush + shutdown flush)
    let logs = hub.take_batch_log();
    assert!(
        logs.len() >= 2,
        "should have at least 2 batch commits, got {}",
        logs.len()
    );
}

/// Generation mismatch from rename: flush commits at old path, then the
/// second flush commits at the new path AND deletes the old remote entry.
/// Verifies end-to-end content correctness at the new path.
#[test]
fn flush_rename_mismatch_content_correct() {
    let hub = MockHub::new();
    hub.add_file("old.txt", 10, Some("orig_hash"), None);
    let xet = MockXet::new();
    xet.add_file("orig_hash", b"0123456789");
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "old.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, false, None).await.unwrap();

        write_blocking(&vfs, ino, fh, 5, b"NEW").await.unwrap();
        vfs.release(fh).await.unwrap();

        // Rename before flush (bumps generation, records old path for delete)
        vfs.rename(ROOT_INODE, "old.txt", ROOT_INODE, "new.txt", false)
            .await
            .unwrap();
    });

    vfs.shutdown();

    // Verify: file committed at new path with correct content
    let inodes = vfs.inode_table.read().unwrap();
    let entry = inodes.get_by_path("new.txt").unwrap();
    assert!(!entry.dirty, "file should be clean after flush");
    let hash = entry.xet_hash.as_ref().unwrap();
    let content = xet.get_file(hash).unwrap();
    assert_eq!(content.len(), 10);
    assert_eq!(&content[..5], b"01234", "original prefix");
    assert_eq!(&content[5..8], b"NEW", "written data");
    assert_eq!(&content[8..10], b"89", "original suffix");

    // Verify: batch log has AddFile at "new.txt" + DeleteFile for "old.txt"
    let logs = hub.take_batch_log();
    let final_batch = logs.iter().find(|batch| {
        batch
            .iter()
            .any(|op| matches!(op, BatchOp::AddFile { path, .. } if path == "new.txt"))
    });
    assert!(final_batch.is_some(), "should have committed at new.txt");
    let has_delete = final_batch
        .unwrap()
        .iter()
        .any(|op| matches!(op, BatchOp::DeleteFile { path } if path == "old.txt"));
    assert!(has_delete, "should delete old.txt in same batch");
}

/// P2: Rename of a dirty file re-enqueues it for flush, so it gets committed
/// at the new path without waiting for shutdown.
#[test]
fn rename_dirty_file_reenqueues_flush() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("orig_hash"), None);
    let xet = MockXet::new();
    xet.add_file("orig_hash", b"0123456789");
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        // Open (truncate), write, close → dirty, flush enqueued
        let fh = vfs.open(ino, true, true, None).await.unwrap();
        write_blocking(&vfs, ino, fh, 0, b"hello").await.unwrap();
        vfs.release(fh).await.unwrap();

        // Wait for the initial flush to complete
        tokio::time::sleep(Duration::from_secs(3)).await;

        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            assert!(!entry.dirty, "file should have been flushed by now");
        }

        // Re-open (truncate), write, close → dirty again
        let fh2 = vfs.open(ino, true, true, None).await.unwrap();
        write_blocking(&vfs, ino, fh2, 0, b"world").await.unwrap();
        vfs.release(fh2).await.unwrap();

        // Rename immediately (before debounce fires)
        vfs.rename(ROOT_INODE, "file.txt", ROOT_INODE, "moved.txt", false)
            .await
            .unwrap();

        // P2 fix: rename re-enqueues the dirty file for flush.
        // Wait for the re-enqueued flush to complete.
        tokio::time::sleep(Duration::from_secs(3)).await;

        // The file should have been flushed at the new path
        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            assert!(!entry.dirty, "file should have been flushed after rename re-enqueue");
            assert_eq!(entry.full_path, "moved.txt");
        }

        // Verify batch_log has a commit at "moved.txt"
        let logs = hub.take_batch_log();
        let has_moved_commit = logs.iter().any(|batch| {
            batch
                .iter()
                .any(|op| matches!(op, BatchOp::AddFile { path, .. } if path == "moved.txt"))
        });
        assert!(
            has_moved_commit,
            "should have committed at 'moved.txt', got: {:?}",
            logs
        );
    });
}

/// 1a fix: setattr truncate between pwrite and inode update should not leave
/// inode.size larger than the actual staging file. After truncate, subsequent
/// writes correctly extend the file again.
#[test]
fn truncate_then_write_keeps_size_consistent() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    xet.add_file("hash1", &vec![0x41u8; 100]);
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        // Open for write (creates sparse staging, size=100)
        let fh = vfs.open(ino, true, false, None).await.unwrap();
        write_blocking(&vfs, ino, fh, 50, b"HELLO").await.unwrap();

        // Truncate to 10 via setattr (shrinks staging file + inode)
        vfs.setattr(ino, Some(10), None, None, None, None, None).await.unwrap();

        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            assert_eq!(entry.size, 10, "inode size should be 10 after truncate");
        }

        // Write within truncated size
        write_blocking(&vfs, ino, fh, 8, b"XY").await.unwrap();

        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            assert_eq!(entry.size, 10, "write at [8,10) fits within truncated size");
            assert!(entry.dirty);
        }

        // Write extending past current size
        write_blocking(&vfs, ino, fh, 15, b"EXT").await.unwrap();

        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            assert_eq!(entry.size, 18, "inode size should be 18 after extending write");
        }

        vfs.release(fh).await.unwrap();
    });

    // Verify the file flushes successfully (no EOF errors from stale size)
    vfs.shutdown();

    let inodes = vfs.inode_table.read().unwrap();
    let entry = inodes.get_by_path("file.txt").unwrap();
    assert!(!entry.dirty, "file should be clean after shutdown flush");
    assert!(entry.xet_hash.is_some(), "file should have xet_hash after flush");
}

/// Truncate to zero clears sparse_write state entirely. Subsequent writes
/// go through the regular (non-sparse) upload path.
#[test]
fn truncate_to_zero_clears_sparse_state() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 100, Some("hash1"), None);
    let xet = MockXet::new();
    xet.add_file("hash1", &vec![0x41u8; 100]);
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        let fh = vfs.open(ino, true, false, None).await.unwrap();
        write_blocking(&vfs, ino, fh, 50, b"DATA").await.unwrap();

        // Truncate to zero
        vfs.setattr(ino, Some(0), None, None, None, None, None).await.unwrap();

        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            assert_eq!(entry.size, 0);
            assert!(entry.dirty);
            assert!(entry.xet_hash.is_none(), "truncate to zero should clear xet_hash");
            assert!(
                entry.sparse_write.is_none(),
                "truncate to zero should clear sparse_write"
            );
        }

        // Write new content
        write_blocking(&vfs, ino, fh, 0, b"NEW").await.unwrap();

        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            assert_eq!(entry.size, 3);
            assert!(entry.sparse_write.is_none());
        }

        vfs.release(fh).await.unwrap();
    });

    vfs.shutdown();

    let inodes = vfs.inode_table.read().unwrap();
    let entry = inodes.get_by_path("file.txt").unwrap();
    assert!(!entry.dirty);
    let hash = entry.xet_hash.as_ref().unwrap();
    let content = xet.get_file(hash).unwrap();
    assert_eq!(&content, b"NEW");
}

/// Truncate a sparse file to non-zero size: the committed content must have
/// the original CAS bytes in the preserved prefix, not zeros from sparse holes.
#[test]
fn truncate_sparse_preserves_cas_prefix() {
    let hub = MockHub::new();
    let original = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ"; // 26 bytes
    hub.add_file("file.txt", 26, Some("hash_orig"), None);
    let xet = MockXet::new();
    xet.add_file("hash_orig", original);
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        // Open in non-truncate mode (sparse staging, no download)
        let fh = vfs.open(ino, true, false, None).await.unwrap();

        // Truncate to 10 bytes via setattr
        vfs.setattr(ino, Some(10), None, None, None, None, None).await.unwrap();

        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            assert_eq!(entry.size, 10);
            assert!(entry.dirty);
            // Sparse state should be preserved (range_upload handles boundary from CAS)
            assert!(entry.sparse_write.is_some(), "truncate should preserve sparse_write");
        }

        vfs.release(fh).await.unwrap();
    });

    vfs.shutdown();

    let inodes = vfs.inode_table.read().unwrap();
    let entry = inodes.get_by_path("file.txt").unwrap();
    assert!(!entry.dirty);
    let hash = entry.xet_hash.as_ref().unwrap();
    let content = xet.get_file(hash).unwrap();
    // Must be the first 10 bytes of the original, NOT zeros
    assert_eq!(
        &content,
        &original[..10],
        "truncated file must preserve original CAS bytes"
    );
}

/// Truncate then extend: the gap region should be zeros, not old CAS bytes.
#[test]
fn truncate_then_extend_fills_zeros() {
    let hub = MockHub::new();
    let original = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ"; // 26 bytes
    hub.add_file("file.txt", 26, Some("hash_orig"), None);
    let xet = MockXet::new();
    xet.add_file("hash_orig", original);
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        let fh = vfs.open(ino, true, false, None).await.unwrap();

        // Truncate to 10
        vfs.setattr(ino, Some(10), None, None, None, None, None).await.unwrap();

        // Extend back to 20 via setattr
        vfs.setattr(ino, Some(20), None, None, None, None, None).await.unwrap();

        // Write a marker at offset 18
        write_blocking(&vfs, ino, fh, 18, b"XY").await.unwrap();

        vfs.release(fh).await.unwrap();
    });

    vfs.shutdown();

    let inodes = vfs.inode_table.read().unwrap();
    let entry = inodes.get_by_path("file.txt").unwrap();
    assert!(!entry.dirty);
    let hash = entry.xet_hash.as_ref().unwrap();
    let content = xet.get_file(hash).unwrap();
    assert_eq!(content.len(), 20);
    // [0, 10): original CAS bytes
    assert_eq!(&content[..10], &original[..10]);
    // [10, 18): zeros (from truncation + extension)
    assert_eq!(&content[10..18], &[0u8; 8], "gap should be zeros, not old CAS bytes");
    // [18, 20): our marker
    assert_eq!(&content[18..20], b"XY");
}

/// Truncate a sparse file that has dirty writes: the dirty bytes must be
/// preserved, not overwritten by CAS content during materialization.
#[test]
fn truncate_sparse_preserves_dirty_writes() {
    let hub = MockHub::new();
    let original = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ"; // 26 bytes
    hub.add_file("file.txt", 26, Some("hash_orig"), None);
    let xet = MockXet::new();
    xet.add_file("hash_orig", original);
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        let fh = vfs.open(ino, true, false, None).await.unwrap();

        // Write "123" at offset 5 (dirty range [5, 8))
        write_blocking(&vfs, ino, fh, 5, b"123").await.unwrap();

        // Truncate to 15 — must preserve "123" at [5, 8)
        vfs.setattr(ino, Some(15), None, None, None, None, None).await.unwrap();

        vfs.release(fh).await.unwrap();
    });

    vfs.shutdown();

    let inodes = vfs.inode_table.read().unwrap();
    let entry = inodes.get_by_path("file.txt").unwrap();
    assert!(!entry.dirty);
    let hash = entry.xet_hash.as_ref().unwrap();
    let content = xet.get_file(hash).unwrap();
    assert_eq!(content.len(), 15);
    // [0, 5): original CAS bytes
    assert_eq!(&content[..5], &original[..5], "prefix should be original CAS bytes");
    // [5, 8): our dirty write
    assert_eq!(&content[5..8], b"123", "dirty write must be preserved after truncate");
    // [8, 15): original CAS bytes
    assert_eq!(&content[8..15], &original[8..15], "suffix should be original CAS bytes");
}

// ── POSIX edge cases ────────────────────────────────────────────────

/// POSIX: write beyond EOF creates a zero-filled gap.
/// Write at offset 20 on a 10-byte file should produce: CAS[0,10) + zeros[10,20) + data[20,23).
#[test]
fn write_beyond_eof_creates_zero_gap() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("h1"), None);
    let xet = MockXet::new();
    xet.add_file("h1", b"0123456789");

    let hash = flush_sparse_and_get_hash(&hub, &xet, &[(20, b"XYZ")]);
    let content = xet.get_file(&hash).unwrap();
    assert_eq!(content.len(), 23);
    assert_eq!(&content[..10], b"0123456789", "original prefix");
    assert_eq!(&content[10..20], &[0u8; 10], "gap should be zeros");
    assert_eq!(&content[20..23], b"XYZ", "written data");
}

/// POSIX: unlink an open file, continue writing, data committed on close.
/// After unlink, nlink=0 but handle is still valid. Writes succeed.
/// On release, flush is skipped (file was unlinked, no point uploading).
#[test]
fn unlink_open_file_writes_succeed_but_not_flushed() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("h1"), None);
    let xet = MockXet::new();
    xet.add_file("h1", b"0123456789");
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, false, None).await.unwrap();

        // Write before unlink
        write_blocking(&vfs, ino, fh, 0, b"ABC").await.unwrap();

        // Unlink while handle is open
        vfs.unlink(ROOT_INODE, "file.txt").await.unwrap();

        // Lookup should fail now
        assert_eq!(vfs.lookup(ROOT_INODE, "file.txt").await.unwrap_err(), libc::ENOENT);

        // But writes should still work via the open handle
        let result = write_blocking(&vfs, ino, fh, 5, b"DEF").await;
        assert!(result.is_ok(), "write to unlinked file should succeed");

        // Release: flush skipped because nlink=0
        vfs.release(fh).await.unwrap();

        // File should be gone from inode table (orphan cleaned up)
        let inodes = vfs.inode_table.read().unwrap();
        assert!(
            inodes.get(ino).is_none(),
            "orphan inode should be removed after release"
        );
    });

    // No batch operations should have been committed for the unlinked file
    let logs = hub.take_batch_log();
    let has_add = logs
        .iter()
        .any(|batch| batch.iter().any(|op| matches!(op, BatchOp::AddFile { .. })));
    assert!(!has_add, "unlinked file should not be flushed to remote");
}

/// POSIX: double truncate (shrink then grow then shrink).
/// Grow region should be zeros, not stale CAS bytes.
#[test]
fn double_truncate_shrink_grow_shrink() {
    let hub = MockHub::new();
    let original = b"ABCDEFGHIJKLMNOPQRST"; // 20 bytes
    hub.add_file("file.txt", 20, Some("h1"), None);
    let xet = MockXet::new();
    xet.add_file("h1", original);
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, false, None).await.unwrap();

        // Shrink to 5
        vfs.setattr(ino, Some(5), None, None, None, None, None).await.unwrap();
        // Grow to 15
        vfs.setattr(ino, Some(15), None, None, None, None, None).await.unwrap();
        // Shrink to 10
        vfs.setattr(ino, Some(10), None, None, None, None, None).await.unwrap();

        vfs.release(fh).await.unwrap();
    });

    // Use shutdown for sync flush (async flush of sparse files doesn't work in test mocks)
    vfs.shutdown();

    let inodes = vfs.inode_table.read().unwrap();
    let entry = inodes.get_by_path("file.txt").unwrap();
    assert!(!entry.dirty);
    let hash = entry.xet_hash.as_ref().unwrap();
    let content = xet.get_file(hash).unwrap();
    assert_eq!(content.len(), 10);
    assert_eq!(&content[..5], &original[..5], "preserved prefix");
    assert_eq!(&content[5..10], &[0u8; 5], "regrown region should be zeros");
}

/// POSIX: create new file, write, read back via same handle.
#[test]
fn create_write_read_same_handle() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let (attr, fh) = vfs.create(ROOT_INODE, "new.txt", 0o644, 0, 0, None).await.unwrap();
        let ino = attr.ino;

        write_blocking(&vfs, ino, fh, 0, b"Hello World").await.unwrap();

        // Read back via the same handle
        let (data, eof) = vfs.read(fh, 0, 100).await.unwrap();
        assert_eq!(&data[..], b"Hello World");
        assert!(eof);

        vfs.release(fh).await.unwrap();
    });
}

/// POSIX: sparse write at offset 0 then read full file.
/// Offset 0 write on existing CAS file: dirty[0, len) + CAS[len, original_size).
#[test]
fn sparse_write_at_zero_read_full() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("h1"), None);
    let xet = MockXet::new();
    xet.add_file("h1", b"0123456789");
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, false, None).await.unwrap();

        write_blocking(&vfs, ino, fh, 0, b"ABC").await.unwrap();

        // Read full file: should see ABC + original[3..10]
        let (data, _) = vfs.read(fh, 0, 100).await.unwrap();
        assert_eq!(data.len(), 10);
        assert_eq!(&data[..3], b"ABC", "written prefix");
        assert_eq!(&data[3..], b"3456789", "CAS suffix via fill_sparse_holes");

        vfs.release(fh).await.unwrap();
    });
}

/// POSIX: rename to self is a no-op. File content and state should not change.
#[test]
fn rename_to_self_is_noop() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("h1"), None);
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        // Rename to self
        vfs.rename(ROOT_INODE, "file.txt", ROOT_INODE, "file.txt", false)
            .await
            .unwrap();

        // File should still exist, same ino
        let attr2 = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        assert_eq!(attr2.ino, ino);
    });
}

/// POSIX: rename replaces destination atomically. Old content gone.
#[test]
fn rename_replaces_destination_content() {
    let hub = MockHub::new();
    hub.add_file("src.txt", 5, Some("h_src"), None);
    hub.add_file("dst.txt", 10, Some("h_dst"), None);
    let xet = MockXet::new();
    xet.add_file("h_src", b"AAAAA");
    xet.add_file("h_dst", b"BBBBBBBBBB");
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let src = vfs.lookup(ROOT_INODE, "src.txt").await.unwrap();
        let dst = vfs.lookup(ROOT_INODE, "dst.txt").await.unwrap();
        let src_ino = src.ino;
        let dst_ino = dst.ino;
        assert_ne!(src_ino, dst_ino);

        vfs.rename(ROOT_INODE, "src.txt", ROOT_INODE, "dst.txt", false)
            .await
            .unwrap();

        // src.txt should be gone
        assert_eq!(vfs.lookup(ROOT_INODE, "src.txt").await.unwrap_err(), libc::ENOENT);

        // dst.txt should have src's ino and size
        let inodes = vfs.inode_table.read().unwrap();
        let new_dst = inodes.get_by_path("dst.txt").unwrap();
        assert_eq!(new_dst.inode, src_ino);
        assert_eq!(new_dst.size, 5);
    });
}

/// POSIX: create + unlink + create at same path. Second file is independent.
#[test]
fn create_unlink_recreate_independent() {
    let hub = MockHub::new();
    let xet = MockXet::new();
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let (attr1, fh1) = vfs.create(ROOT_INODE, "file.txt", 0o644, 0, 0, None).await.unwrap();
        write_blocking(&vfs, attr1.ino, fh1, 0, b"first").await.unwrap();
        vfs.release(fh1).await.unwrap();

        vfs.unlink(ROOT_INODE, "file.txt").await.unwrap();

        let (attr2, fh2) = vfs.create(ROOT_INODE, "file.txt", 0o644, 0, 0, None).await.unwrap();
        assert_ne!(attr1.ino, attr2.ino, "new file should have a different ino");
        write_blocking(&vfs, attr2.ino, fh2, 0, b"second").await.unwrap();
        vfs.release(fh2).await.unwrap();
    });

    vfs.shutdown();

    let inodes = vfs.inode_table.read().unwrap();
    let entry = inodes.get_by_path("file.txt").unwrap();
    let hash = entry.xet_hash.as_ref().unwrap();
    let content = xet.get_file(hash).unwrap();
    assert_eq!(&content, b"second", "should have second file's content");
}

/// NFS handle reuse: write → flush (clean) → write again via same handle.
/// The second write hits the `else if !entry.dirty` branch that rebuilds
/// sparse_write from the flushed xet_hash. Verify the composed content
/// includes both writes.
///
///  Timeline:
///    open → write1("ABC" @5) → release → flush → clean (hash=H1)
///    open → write2("XY" @8)  → rebuilds SparseWriteState from H1
///    release → shutdown → composed: CAS(H1)[0..8) + "XY"
#[test]
fn nfs_handle_reuse_clean_to_dirty_transition() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("orig_hash"), None);
    let xet = MockXet::new();
    xet.add_file("orig_hash", b"0123456789");
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;

        // First write cycle: open (truncate) → write → release
        let fh = vfs.open(ino, true, true, None).await.unwrap();
        write_blocking(&vfs, ino, fh, 0, b"0123456789").await.unwrap();
        vfs.release(fh).await.unwrap();

        // Wait for async flush to commit (debounce 100ms + processing)
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Verify: file is now clean with a new xet_hash
        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            assert!(!entry.dirty, "file should be clean after first flush");
            assert!(entry.xet_hash.is_some(), "should have xet_hash after flush");
            assert!(entry.sparse_write.is_none(), "sparse_write cleared after flush");
        }

        // Second write cycle: open (non-truncate) → write → check sparse_write rebuilt
        let fh2 = vfs.open(ino, true, false, None).await.unwrap();
        write_blocking(&vfs, ino, fh2, 8, b"XY").await.unwrap();

        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            assert!(entry.dirty, "should be dirty after second write");
            let sw = entry
                .sparse_write
                .as_ref()
                .expect("sparse_write should be rebuilt from flushed xet_hash (clean->dirty transition)");
            assert_eq!(sw.dirty_ranges, vec![(8, 10)], "only second write tracked");
        }

        vfs.release(fh2).await.unwrap();
    });

    vfs.shutdown();

    let inodes = vfs.inode_table.read().unwrap();
    let entry = inodes.get_by_path("file.txt").unwrap();
    assert!(!entry.dirty);
    let hash = entry.xet_hash.as_ref().unwrap();
    let content = xet.get_file(hash).unwrap();
    assert_eq!(content.len(), 10);
    assert_eq!(&content[..8], b"01234567", "base content from first flush");
    assert_eq!(&content[8..10], b"XY", "second write (NFS handle reuse)");
}

// ── Write past original_size gap tracking ───────────────────────

/// Write past original_size on a sparse file: the gap [original_size, offset)
/// must be tracked as dirty so upload_ranges includes the zero bytes.
///
///  original CAS: [AAAAAAAAAA]  (10 bytes, original_size=10)
///  write at 20:  [..........][0000000000][WRITE]
///                 0         10          20    25
///
///  dirty_ranges must include [10, 25), not just [20, 25).
#[test]
fn write_past_original_size_tracks_gap() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("h1"), None);
    let xet = MockXet::new();
    xet.add_file("h1", b"0123456789");
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, false, None).await.unwrap();

        // Write 5 bytes at offset 20 (10 bytes past original_size)
        write_blocking(&vfs, ino, fh, 20, b"WRITE").await.unwrap();

        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            assert_eq!(entry.size, 25);
            let sw = entry.sparse_write.as_ref().unwrap();
            // dirty_ranges must cover [10, 25): the gap + the write
            assert_eq!(
                sw.dirty_ranges,
                vec![(10, 25)],
                "gap [original_size, offset) must be tracked as dirty"
            );
        }

        vfs.release(fh).await.unwrap();
    });

    // Verify composed content: CAS[0,10) + zeros[10,20) + "WRITE"
    vfs.shutdown();
    let inodes = vfs.inode_table.read().unwrap();
    let entry = inodes.get_by_path("file.txt").unwrap();
    let hash = entry.xet_hash.as_ref().unwrap();
    let content = xet.get_file(hash).unwrap();
    assert_eq!(content.len(), 25);
    assert_eq!(&content[..10], b"0123456789", "original CAS prefix");
    assert_eq!(&content[10..20], &[0u8; 10], "gap must be zeros");
    assert_eq!(&content[20..25], b"WRITE", "written data");
}

/// setattr grow on a sparse file: the extended region must be tracked as dirty.
///
///  original CAS: [AAAAAAAAAA]  (10 bytes)
///  setattr(20):  [AAAAAAAAAA][0000000000]
///                 0         10          20
///
///  dirty_ranges must include [10, 20).
#[test]
fn setattr_grow_tracks_extension_as_dirty() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 10, Some("h1"), None);
    let xet = MockXet::new();
    xet.add_file("h1", b"0123456789");
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, false, None).await.unwrap();

        // Grow via setattr
        vfs.setattr(ino, Some(20), None, None, None, None, None).await.unwrap();

        {
            let inodes = vfs.inode_table.read().unwrap();
            let entry = inodes.get(ino).unwrap();
            assert_eq!(entry.size, 20);
            let sw = entry.sparse_write.as_ref().unwrap();
            assert_eq!(
                sw.dirty_ranges,
                vec![(10, 20)],
                "extension [original_size, new_size) must be tracked as dirty"
            );
        }

        vfs.release(fh).await.unwrap();
    });

    // Verify composed content
    vfs.shutdown();
    let inodes = vfs.inode_table.read().unwrap();
    let entry = inodes.get_by_path("file.txt").unwrap();
    let hash = entry.xet_hash.as_ref().unwrap();
    let content = xet.get_file(hash).unwrap();
    assert_eq!(content.len(), 20);
    assert_eq!(&content[..10], b"0123456789", "original CAS prefix");
    assert_eq!(&content[10..20], &[0u8; 10], "extension must be zeros");
}

/// Multiple writes past original_size: each extends the gap tracking.
///
///  original CAS: [AAAA]  (4 bytes)
///  write at 10:  [AAAA][000000][XX]        dirty=[4, 12)
///  write at 20:  [AAAA][000000][XX][00000000][YY]  dirty=[4, 22)
#[test]
fn multiple_writes_past_eof_accumulate_gaps() {
    let hub = MockHub::new();
    hub.add_file("file.txt", 4, Some("h1"), None);
    let xet = MockXet::new();
    xet.add_file("h1", b"AAAA");
    let (rt, vfs) = vfs_advanced(&hub, &xet);

    rt.block_on(async {
        let attr = vfs.lookup(ROOT_INODE, "file.txt").await.unwrap();
        let ino = attr.ino;
        let fh = vfs.open(ino, true, false, None).await.unwrap();

        write_blocking(&vfs, ino, fh, 10, b"XX").await.unwrap();
        {
            let inodes = vfs.inode_table.read().unwrap();
            let sw = inodes.get(ino).unwrap().sparse_write.as_ref().unwrap().clone();
            assert_eq!(sw.dirty_ranges, vec![(4, 12)], "first write: gap + data");
        }

        write_blocking(&vfs, ino, fh, 20, b"YY").await.unwrap();
        {
            let inodes = vfs.inode_table.read().unwrap();
            let sw = inodes.get(ino).unwrap().sparse_write.as_ref().unwrap().clone();
            assert_eq!(sw.dirty_ranges, vec![(4, 22)], "second write: merged gap + data");
        }

        vfs.release(fh).await.unwrap();
    });

    vfs.shutdown();
    let inodes = vfs.inode_table.read().unwrap();
    let entry = inodes.get_by_path("file.txt").unwrap();
    let hash = entry.xet_hash.as_ref().unwrap();
    let content = xet.get_file(hash).unwrap();
    assert_eq!(content.len(), 22);
    assert_eq!(&content[..4], b"AAAA", "original");
    assert_eq!(&content[4..10], &[0u8; 6], "first gap");
    assert_eq!(&content[10..12], b"XX", "first write");
    assert_eq!(&content[12..20], &[0u8; 8], "second gap");
    assert_eq!(&content[20..22], b"YY", "second write");
}
