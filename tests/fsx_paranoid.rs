//! Paranoid fsx variant: every mutation does a full CAS round-trip.
//!
//! After each write/truncate, the file is closed, we wait for the async flush to
//! commit to CAS, then re-open and read back. This catches composition bugs in
//! `range_upload` that the canonical fsx (in `fsx.rs`) misses since it reads from
//! the local staging file, not from CAS.
//!
//! Slow (~1.5s per op for flush debounce + CAS propagation). Use `FSX_PARANOID_OPS`
//! to control iteration count (default: 100).
//!
//! Requires HF_TOKEN. Run with:
//!   cargo test --release --test fsx_paranoid -- --nocapture

mod common;

use std::io::{Seek, SeekFrom};

const MAX_SIZE: usize = 1 << 20; // 1 MB

#[tokio::test]
async fn test_fsx_paranoid_cas_roundtrip() {
    let guard = match common::setup_bucket("fsx-paranoid").await {
        Some(g) => g,
        None => return,
    };
    let bucket_id = guard.bucket_id.clone();

    let pid = std::process::id();
    let mount_point = format!("/tmp/hf-fsx-paranoid-{}", pid);
    let cache_dir = format!("/tmp/hf-fsx-paranoid-cache-{}", pid);

    // Force sparse engagement for this 1 MiB workload. Without overriding the
    // 256 MiB production default, the `--sparse-writes` CLI flag below would
    // be a no-op: every file in this test stays well under the threshold and
    // would silently fall back to the legacy download-then-upload path,
    // defeating the test's stated purpose of catching range_upload
    // composition regressions. Safe to mutate here: each `tests/*.rs` file
    // compiles as its own test binary, and this binary has a single test.
    //
    // SAFETY: single-threaded mutation in test setup before any reader
    // observes the variable. The mount child inherits this env at spawn.
    unsafe {
        std::env::set_var("HF_MOUNT_SPARSE_MIN_BYTES", "0");
    }

    let child = common::mount_bucket(
        &bucket_id,
        &mount_point,
        &cache_dir,
        // --sparse-writes is required: the test's stated purpose is to catch
        // range_upload composition bugs, and range_upload is only exercised
        // on the sparse path. Without this flag the test runs the legacy
        // download-then-upload path and never invokes range_upload.
        //
        // --direct-io is required for the round-trip to actually validate CAS:
        // without it the kernel page cache (FOPEN_KEEP_CACHE) can serve the
        // read-back from the just-written pages, so a range_upload that
        // corrupted the committed CAS object would still match `reference`.
        // direct-io forces every read through the FUSE handler, which on the
        // now-clean inode reads from the committed CAS revision.
        &[
            "--advanced-writes",
            "--sparse-writes",
            "--direct-io",
            "--flush-debounce-ms",
            "100",
        ],
    );

    let num_ops = std::env::var("FSX_PARANOID_OPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);

    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
        | 1; // ensure non-zero for xorshift

    eprintln!("fsx-paranoid: {} ops, seed={}, mount={}", num_ops, seed, mount_point);

    let test_file = format!("{}/fsx_paranoid_{}", mount_point, pid);
    // flush_debounce=100ms + upload + CAS propagation
    let flush_wait = std::time::Duration::from_millis(1500);

    let mut reference = vec![0u8; MAX_SIZE];
    let mut file_size: usize = 0;
    let mut rng_state = seed;

    let mut xorshift = || -> u64 {
        rng_state ^= rng_state << 13;
        rng_state ^= rng_state >> 7;
        rng_state ^= rng_state << 17;
        rng_state
    };

    for op in 1..=num_ops {
        match xorshift() % 3 {
            0 => {
                // Write random bytes at random offset
                let offset = (xorshift() as usize) % (MAX_SIZE / 4);
                let mut len = 1 + (xorshift() as usize) % 4096;
                if offset + len > MAX_SIZE {
                    len = MAX_SIZE - offset;
                }
                let mut wbuf = vec![0u8; len];
                for byte in &mut wbuf {
                    *byte = xorshift() as u8;
                }

                {
                    use std::io::Write;
                    let mut f = if file_size == 0 {
                        std::fs::File::create(&test_file).expect("create")
                    } else {
                        std::fs::OpenOptions::new()
                            .write(true)
                            .open(&test_file)
                            .expect("open for write")
                    };
                    f.seek(SeekFrom::Start(offset as u64)).expect("seek");
                    f.write_all(&wbuf).expect("write");
                }
                reference[offset..offset + len].copy_from_slice(&wbuf);
                if offset + len > file_size {
                    file_size = offset + len;
                }
                eprintln!("  op {}/{}: write {} bytes at offset {}", op, num_ops, len, offset);
            }
            1 => {
                if file_size < 100 {
                    continue;
                }
                let new_size = (xorshift() as usize) % file_size;
                {
                    let f = std::fs::OpenOptions::new()
                        .write(true)
                        .open(&test_file)
                        .expect("open for truncate");
                    f.set_len(new_size as u64).expect("truncate");
                }
                for byte in &mut reference[new_size..file_size] {
                    *byte = 0;
                }
                file_size = new_size;
                eprintln!("  op {}/{}: truncate to {}", op, num_ops, new_size);
            }
            2 => {
                let new_size = file_size + 1 + (xorshift() as usize) % 2048;
                let new_size = new_size.min(MAX_SIZE);
                if new_size <= file_size {
                    continue;
                }
                {
                    let f = if file_size == 0 {
                        std::fs::File::create(&test_file).expect("create")
                    } else {
                        std::fs::OpenOptions::new()
                            .write(true)
                            .open(&test_file)
                            .expect("open for grow")
                    };
                    f.set_len(new_size as u64).expect("grow");
                }
                file_size = new_size;
                eprintln!("  op {}/{}: grow to {}", op, num_ops, new_size);
            }
            _ => unreachable!(),
        }

        if file_size == 0 {
            continue;
        }

        // Wait for async flush to commit to CAS, with retry. A flush can fail
        // transiently if the previous mutation's upload is still in-flight when
        // we modify the staging file (early EOF). The generation counter keeps
        // the file dirty and the next flush retries.
        let mut verified = false;
        for attempt in 0..3 {
            std::thread::sleep(flush_wait);
            match std::fs::read(&test_file) {
                Ok(content) if content.len() == file_size && content == reference[..file_size] => {
                    verified = true;
                    break;
                }
                Ok(content) if attempt < 2 => {
                    eprintln!(
                        "  op {}/{}: verify attempt {} failed (size {}/{}), retrying...",
                        op,
                        num_ops,
                        attempt + 1,
                        content.len(),
                        file_size
                    );
                }
                Ok(content) => {
                    if content.len() != file_size {
                        panic!(
                            "op {}: size mismatch after 3 CAS attempts: got {}, expected {}",
                            op,
                            content.len(),
                            file_size
                        );
                    }
                    for i in 0..file_size {
                        if content[i] != reference[i] {
                            panic!(
                                "op {}: CAS MISMATCH at byte {}: got 0x{:02x} expected 0x{:02x} (file_size={})",
                                op, i, content[i], reference[i], file_size
                            );
                        }
                    }
                }
                Err(e) if attempt < 2 => {
                    eprintln!("  op {}/{}: read failed ({}), retrying...", op, num_ops, e);
                }
                Err(e) => panic!("op {}: read failed after 3 attempts: {}", op, e),
            }
        }
        assert!(verified, "op {}: CAS verify failed after 3 attempts", op);
        eprintln!("  op {}/{}: CAS verify OK (size={})", op, num_ops, file_size);
    }

    eprintln!("fsx-paranoid: PASSED {} ops (final size={})", num_ops, file_size);
    std::fs::remove_file(&test_file).ok();

    common::unmount(&mount_point, child, 10);
    drop(guard);
    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();
}
