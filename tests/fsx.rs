//! fsx (File System eXerciser): random read/write/truncate with in-memory verification.
//!
//! This is the gold standard test for filesystem data integrity. It performs random
//! operations on a file and verifies every read against an in-memory reference copy.
//! Any single-byte mismatch means data corruption.
//!
//! Requires HF_TOKEN and a real mount. Run with:
//!   cargo test --release --test fsx -- --nocapture

mod common;

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::io::AsRawFd;

const MAX_SIZE: usize = 1 << 20; // 1 MB max file size
const DEFAULT_OPS: usize = 50_000;

struct FsxState {
    fd: File,
    path: String,
    reference: Vec<u8>,
    file_size: usize,
    ops: usize,
    seed: u64,
}

impl FsxState {
    fn new(path: &str, seed: u64) -> Self {
        let fd = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .expect("Failed to create test file");
        Self {
            fd,
            path: path.to_string(),
            reference: vec![0u8; MAX_SIZE],
            file_size: 0,
            ops: 0,
            seed,
        }
    }

    fn rand(&mut self) -> u64 {
        // xorshift64
        self.seed ^= self.seed << 13;
        self.seed ^= self.seed >> 7;
        self.seed ^= self.seed << 17;
        self.seed
    }

    fn do_write(&mut self) {
        let offset = (self.rand() as usize) % (MAX_SIZE / 2);
        let mut len = 1 + (self.rand() as usize) % 8192;
        if offset + len > MAX_SIZE {
            len = MAX_SIZE - offset;
        }

        let mut wbuf = vec![0u8; len];
        for byte in &mut wbuf {
            *byte = self.rand() as u8;
        }

        let n = unsafe {
            libc::pwrite(
                self.fd.as_raw_fd(),
                wbuf.as_ptr() as *const libc::c_void,
                len,
                offset as i64,
            )
        };
        assert_eq!(n as usize, len, "op {}: pwrite short", self.ops);

        self.reference[offset..offset + len].copy_from_slice(&wbuf);
        if offset + len > self.file_size {
            self.file_size = offset + len;
        }
    }

    fn do_read_verify(&mut self) {
        if self.file_size == 0 {
            return;
        }
        let offset = (self.rand() as usize) % self.file_size;
        let mut len = 1 + (self.rand() as usize) % 8192;
        if offset + len > self.file_size {
            len = self.file_size - offset;
        }

        let mut rbuf = vec![0u8; len];
        let n = unsafe {
            libc::pread(
                self.fd.as_raw_fd(),
                rbuf.as_mut_ptr() as *mut libc::c_void,
                len,
                offset as i64,
            )
        };
        assert!(
            n >= 0,
            "op {}: pread failed: {}",
            self.ops,
            std::io::Error::last_os_error()
        );
        assert_eq!(
            n as usize, len,
            "op {}: pread short: got {} expected {} at offset {}",
            self.ops, n, len, offset
        );

        if rbuf != self.reference[offset..offset + len] {
            for (i, &byte) in rbuf.iter().enumerate() {
                if byte != self.reference[offset + i] {
                    panic!(
                        "op {}: DATA MISMATCH at byte {}: got 0x{:02x} expected 0x{:02x} (offset={}, len={})",
                        self.ops,
                        offset + i,
                        byte,
                        self.reference[offset + i],
                        offset,
                        len
                    );
                }
            }
        }
    }

    fn do_truncate_shrink(&mut self) {
        if self.file_size < 100 {
            return;
        }
        let new_size = (self.rand() as usize) % self.file_size;
        self.fd.set_len(new_size as u64).expect("ftruncate shrink");
        // POSIX: truncated region becomes zero on re-extension
        for byte in &mut self.reference[new_size..self.file_size] {
            *byte = 0;
        }
        self.file_size = new_size;
    }

    fn do_truncate_grow(&mut self) {
        let new_size = self.file_size + 1 + (self.rand() as usize) % 4096;
        let new_size = new_size.min(MAX_SIZE);
        if new_size <= self.file_size {
            return;
        }
        self.fd.set_len(new_size as u64).expect("ftruncate grow");
        // POSIX: extended region is zero-filled (reference already has zeros)
        self.file_size = new_size;
    }

    fn final_verify(&mut self) {
        self.fd.seek(SeekFrom::Start(0)).expect("seek");
        let mut buf = vec![0u8; self.file_size];
        self.fd.read_exact(&mut buf).expect("final read");

        if buf != self.reference[..self.file_size] {
            for (i, &byte) in buf.iter().enumerate().take(self.file_size) {
                if byte != self.reference[i] {
                    panic!(
                        "FINAL VERIFY FAILED at byte {}: got 0x{:02x} expected 0x{:02x} (file_size={})",
                        i, byte, self.reference[i], self.file_size
                    );
                }
            }
        }
    }

    fn run(&mut self, num_ops: usize) {
        for op_num in 1..=num_ops {
            self.ops = op_num;
            match self.rand() % 4 {
                0 => self.do_write(),
                1 => self.do_read_verify(),
                2 => self.do_truncate_shrink(),
                3 => self.do_truncate_grow(),
                _ => unreachable!(),
            }
            if op_num % 10000 == 0 {
                eprintln!("  {}/{} ops OK (size={})", op_num, num_ops, self.file_size);
            }
        }
        self.final_verify();
    }
}

impl Drop for FsxState {
    fn drop(&mut self) {
        std::fs::remove_file(&self.path).ok();
    }
}

#[tokio::test]
async fn test_fsx_data_integrity() {
    let (token, bucket_id, _hub) = match common::setup_bucket("fsx").await {
        Some(cfg) => cfg,
        None => return,
    };

    let pid = std::process::id();
    let mount_point = format!("/tmp/hf-fsx-{}", pid);
    let cache_dir = format!("/tmp/hf-fsx-cache-{}", pid);

    let child = common::mount_bucket(&bucket_id, &mount_point, &cache_dir, &["--advanced-writes"]);

    let num_ops = std::env::var("FSX_OPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_OPS);

    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
        | 1; // ensure non-zero for xorshift

    eprintln!("fsx: {} ops, seed={}, mount={}", num_ops, seed, mount_point);

    let test_file = format!("{}/fsx_test_{}", mount_point, pid);
    let mut fsx = FsxState::new(&test_file, seed);
    fsx.run(num_ops);

    eprintln!("fsx: PASSED {} ops (final size={})", num_ops, fsx.file_size);
    drop(fsx);

    common::unmount(&mount_point, child, 10);
    common::delete_bucket(&common::endpoint(), &token, &bucket_id).await;
    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();
}

/// Paranoid fsx: every write does a full CAS round-trip.
///
/// After each mutation (write/truncate), the file is closed, we wait for the
/// async flush to commit to CAS, then re-open and read back from CAS to verify.
/// This catches composition bugs in range_upload that the fast fsx misses
/// (since fast fsx reads from the local staging file, not CAS).
///
/// Very slow (~3s per mutation for flush debounce). Use FSX_PARANOID_OPS to control
/// iteration count (default: 20).
#[tokio::test]
async fn test_fsx_paranoid_cas_roundtrip() {
    let (token, bucket_id, _hub) = match common::setup_bucket("fsx-paranoid").await {
        Some(cfg) => cfg,
        None => return,
    };

    let pid = std::process::id();
    let mount_point = format!("/tmp/hf-fsx-paranoid-{}", pid);
    let cache_dir = format!("/tmp/hf-fsx-paranoid-cache-{}", pid);

    let child = common::mount_bucket(
        &bucket_id,
        &mount_point,
        &cache_dir,
        &["--advanced-writes", "--flush-debounce-ms", "100"],
    );

    let num_ops = std::env::var("FSX_PARANOID_OPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);

    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
        | 1;

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
        let mutation = xorshift() % 3;
        match mutation {
            0 => {
                // Write random data at random offset
                let offset = (xorshift() as usize) % (MAX_SIZE / 4);
                let mut len = 1 + (xorshift() as usize) % 4096;
                if offset + len > MAX_SIZE {
                    len = MAX_SIZE - offset;
                }
                let mut wbuf = vec![0u8; len];
                for byte in &mut wbuf {
                    *byte = xorshift() as u8;
                }

                // Write via open+seek+write+close
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
                // Truncate shrink
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
                // Truncate grow
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
                // reference already has zeros in the extended region
                file_size = new_size;
                eprintln!("  op {}/{}: grow to {}", op, num_ops, new_size);
            }
            _ => unreachable!(),
        }

        if file_size == 0 {
            continue;
        }

        // Wait for async flush to commit to CAS, with retry.
        // The flush can fail transiently if a previous mutation's upload is still
        // in-flight when we modify the staging file (early eof). The generation
        // counter keeps the file dirty and the next flush retries.
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
                    // Final attempt: report the exact mismatch
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
    common::delete_bucket(&common::endpoint(), &token, &bucket_id).await;
    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();
}
