//! xfstests generic/quick integration test.
//!
//! Runs the Linux kernel filesystem test suite (generic tests only) against
//! hf-mount with --advanced-writes. Patches xfstests to work with FUSE:
//! - Replaces mount/unmount with sync + drop_caches
//! - Adds scratch filesystem support via a second mount
//!
//! Requires HF_TOKEN. Run with:
//!   cargo test --release --test xfstests -- --nocapture

mod common;

use std::process::Command;

const XFSTESTS_DIR: &str = "/tmp/xfstests";
const XFSTESTS_REV: &str = "v2025.03.30";

/// Expected minimum pass count (established from initial run).
/// Update when adding new POSIX features.
const EXPECTED_PASS: usize = 160;

fn ensure_xfstests() -> bool {
    let check_script = format!("{}/check", XFSTESTS_DIR);
    if std::path::Path::new(&check_script).exists() {
        // Check if already patched
        let patched = std::fs::read_to_string(format!("{}/common/rc", XFSTESTS_DIR))
            .map(|s| s.contains("FUSE: skip"))
            .unwrap_or(false);
        if patched {
            return true;
        }
        eprintln!("xfstests found but not patched, rebuilding...");
        std::fs::remove_dir_all(XFSTESTS_DIR).ok();
    }

    eprintln!("Building xfstests...");
    let deps = Command::new("sh")
        .args([
            "-c",
            "sudo apt-get install -y -qq libtool autoconf automake libaio-dev \
             libacl1-dev libuuid1 uuid-dev xfsprogs xfslibs-dev attr acl bc 2>/dev/null \
             || sudo dnf install -y libtool autoconf automake libaio-devel \
             libacl-devel libuuid-devel xfsprogs-devel attr acl bc 2>/dev/null",
        ])
        .status();
    if deps.map(|s| !s.success()).unwrap_or(true) {
        eprintln!("Warning: some xfstests deps may be missing");
    }

    let ok = Command::new("sh")
        .args([
            "-c",
            &format!(
                "git clone --depth 1 --branch {rev} https://github.com/kdave/xfstests.git {dir} && \
                 cd {dir} && make",
                dir = XFSTESTS_DIR,
                rev = XFSTESTS_REV
            ),
        ])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);

    if !ok {
        eprintln!("Failed to build xfstests");
        return false;
    }

    // Apply FUSE patches to common/rc
    apply_fuse_patches();
    true
}

/// Patch xfstests common/rc to work with FUSE filesystems:
/// - _check_mounted_on: skip mount validation
/// - _test_mount/_test_unmount: sync + drop_caches instead of remount
/// - _scratch_*: handle scratch without real block device
/// - _require_scratch: always available
fn apply_fuse_patches() {
    let rc_path = format!("{}/common/rc", XFSTESTS_DIR);
    let rc = std::fs::read_to_string(&rc_path).expect("read common/rc");

    let patches = [
        (
            "_check_mounted_on()\n{",
            "_check_mounted_on()\n{\n\t# FUSE: skip mount validation\n\tif [ \"$FSTYP\" = \"fuse\" ]; then return 0; fi",
        ),
        (
            "_test_mount()\n{",
            "_test_mount()\n{\n\t# FUSE: sync + drop caches instead of remount\n\tif [ \"$FSTYP\" = \"fuse\" ]; then\n\t\tsync\n\t\techo 3 > /proc/sys/vm/drop_caches 2>/dev/null\n\t\treturn 0\n\tfi",
        ),
        (
            "_test_unmount()\n{",
            "_test_unmount()\n{\n\t# FUSE: sync instead of unmount\n\tif [ \"$FSTYP\" = \"fuse\" ]; then\n\t\tsync\n\t\techo 3 > /proc/sys/vm/drop_caches 2>/dev/null\n\t\treturn 0\n\tfi",
        ),
        (
            "_try_scratch_mount()\n{",
            "_try_scratch_mount()\n{\n\t# FUSE: already mounted\n\tif [ \"$FSTYP\" = \"fuse\" ]; then return 0; fi",
        ),
        (
            "_scratch_mount()\n{",
            "_scratch_mount()\n{\n\t# FUSE: sync + drop caches\n\tif [ \"$FSTYP\" = \"fuse\" ]; then\n\t\tsync\n\t\techo 3 > /proc/sys/vm/drop_caches 2>/dev/null\n\t\treturn 0\n\tfi",
        ),
        (
            "_scratch_unmount()\n{",
            "_scratch_unmount()\n{\n\t# FUSE: sync instead of unmount\n\tif [ \"$FSTYP\" = \"fuse\" ]; then\n\t\tsync\n\t\techo 3 > /proc/sys/vm/drop_caches 2>/dev/null\n\t\treturn 0\n\tfi",
        ),
        (
            "_scratch_mkfs()\n{",
            "_scratch_mkfs()\n{\n\t# FUSE: clean scratch dir instead of mkfs\n\tif [ \"$FSTYP\" = \"fuse\" ]; then\n\t\trm -rf \"$SCRATCH_MNT\"/* 2>/dev/null\n\t\treturn 0\n\tfi",
        ),
        (
            "_require_scratch()\n{",
            "_require_scratch()\n{\n\t# FUSE: scratch always available\n\tif [ \"$FSTYP\" = \"fuse\" ]; then return 0; fi",
        ),
    ];

    let mut patched = rc;
    for (find, replace) in &patches {
        patched = patched.replace(find, replace);
    }

    std::fs::write(&rc_path, patched).expect("write patched common/rc");
    eprintln!("Applied {} FUSE patches to common/rc", patches.len());
}

fn create_mount_wrapper(token: &str, binary: &std::path::Path) {
    let wrapper = format!(
        "#!/bin/bash\nMOUNTPOINT=\"$1\"\nmkdir -p \"$MOUNTPOINT\"\n\
         exec {} --hf-token {} --hub-endpoint {} \
         --poll-interval-secs 0 --advanced-writes \
         --cache-dir /tmp/xfstests-cache \
         bucket \"$HF_XFSTESTS_BUCKET\" \"$MOUNTPOINT\"",
        binary.display(),
        token,
        common::endpoint()
    );
    std::fs::write("/usr/local/bin/hf-mount", &wrapper).ok();
    // Try with sudo if direct write fails
    if !std::path::Path::new("/usr/local/bin/hf-mount").exists() {
        Command::new("sudo")
            .args(["tee", "/usr/local/bin/hf-mount"])
            .stdin(std::process::Stdio::piped())
            .spawn()
            .and_then(|mut child| {
                use std::io::Write;
                child.stdin.as_mut().unwrap().write_all(wrapper.as_bytes())?;
                child.wait()
            })
            .ok();
    }
    Command::new("sudo")
        .args(["chmod", "+x", "/usr/local/bin/hf-mount"])
        .status()
        .ok();
}

fn write_config(test_dir: &str, scratch_dir: &str) {
    let config = format!(
        "export FSTYP=fuse\n\
         export TEST_DEV=hf-mount\n\
         export TEST_DIR={}\n\
         export SCRATCH_DEV=hf-mount\n\
         export SCRATCH_MNT={}\n",
        test_dir, scratch_dir
    );
    std::fs::write(format!("{}/local.config", XFSTESTS_DIR), config).expect("write local.config");
}

#[tokio::test]
async fn test_xfstests_generic() {
    let is_ci = std::env::var("CI").is_ok();
    if !ensure_xfstests() {
        if is_ci {
            panic!("xfstests build failed in CI");
        }
        eprintln!("Skipping: xfstests not available");
        return;
    }

    let (token, bucket_id, _hub) = match common::setup_bucket("xfstests").await {
        Some(cfg) => cfg,
        None => return,
    };

    let pid = std::process::id();
    let test_dir = format!("/tmp/hf-xfstests-{}", pid);
    let scratch_dir = format!("/tmp/hf-xfstests-scratch-{}", pid);
    let cache_dir = format!("/tmp/hf-xfstests-cache-{}", pid);

    // Mount test + scratch
    let child_test = common::mount_bucket(&bucket_id, &test_dir, &cache_dir, &["--advanced-writes"]);
    let child_scratch = common::mount_bucket(
        &bucket_id,
        &scratch_dir,
        &format!("{}-scratch", cache_dir),
        &["--advanced-writes"],
    );

    // Create mount wrapper for xfstests remount
    let binary = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("hf-mount-fuse");
    // SAFETY: single-threaded test, no concurrent env access
    unsafe { std::env::set_var("HF_XFSTESTS_BUCKET", &bucket_id) };
    create_mount_wrapper(&token, &binary);

    // Write xfstests config
    write_config(&test_dir, &scratch_dir);

    // Run generic/quick, excluding tests that are too slow for remote-backed FUSE:
    // - generic/308: writes at 16 TB offset (sparse file), staging file allocation too slow
    // TODO: re-enable generic/308 when sparse upload is implemented
    eprintln!("Running xfstests generic/quick...");
    let output = Command::new("sudo")
        // Exclude tests too slow for remote-backed FUSE:
        // generic/113: aio-stress 20 threads x 20 files (~10 min on 4 vCPU)
        // generic/308: writes at 16TB offset (sparse file staging too slow)
        // TODO: re-enable when sparse upload + faster I/O path are implemented
        .args(["./check", "-g", "generic/quick", "-e", "generic/113 generic/308"])
        .current_dir(XFSTESTS_DIR)
        .output()
        .expect("Failed to run xfstests");

    let combined = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    // Parse results
    let passed_count = combined
        .lines()
        .filter(|l| {
            let trimmed = l.trim();
            trimmed.starts_with("generic/")
                && trimmed.ends_with('s')
                && trimmed.contains("    ")
                && !trimmed.contains("[")
        })
        .count();

    let failed_line = combined
        .lines()
        .find(|l| l.starts_with("Failed"))
        .unwrap_or("Failed 0 of 0 tests");
    let failures_line = combined.lines().find(|l| l.starts_with("Failures:")).unwrap_or("");

    eprintln!("\n============================================================");
    eprintln!("  xfstests generic/quick Results");
    eprintln!("------------------------------------------------------------");
    eprintln!("  {}", failed_line);
    if !failures_line.is_empty() {
        eprintln!("  {}", failures_line);
    }
    eprintln!("============================================================");

    // Print full output for CI
    eprintln!("{}", combined);

    // Cleanup
    common::unmount(&test_dir, child_test, 5);
    common::unmount(&scratch_dir, child_scratch, 5);
    common::delete_bucket(&common::endpoint(), &token, &bucket_id).await;
    std::fs::remove_dir_all(&test_dir).ok();
    std::fs::remove_dir_all(&scratch_dir).ok();
    std::fs::remove_dir_all(&cache_dir).ok();
    std::fs::remove_dir_all(format!("{}-scratch", cache_dir)).ok();

    // Assert regression
    assert!(
        passed_count >= EXPECTED_PASS,
        "xfstests regression: {} tests passed (expected at least {}). {}",
        passed_count,
        EXPECTED_PASS,
        failed_line
    );
}
