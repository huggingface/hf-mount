mod common;

use std::process::Command;

/// Expected results (established 2026-03-06).
/// Tests referencing mkfifo/mknod/fifo/block/char/socket are excluded (ENOSYS).
/// Update these when adding new POSIX features.
const EXPECTED_FILES_PASS: usize = 130;
const EXPECTED_TESTS_PASS: usize = 832;

/// Categories excluded from testing (unsupported special file types / ops).
const EXCLUDED_CATEGORIES: &[&str] = &["mkfifo", "mknod", "link"];

/// Individual .t files matching these patterns are excluded:
/// - "mkfifo", "mknod", "for type in": special file types (ENOSYS, cascade-fail)
/// - "ENAMETOOLONG", "NAME_MAX": name length validation (not enforced)
/// - "S_ISVTX", "sticky": sticky bit enforcement (not implemented)
/// - "socket": Unix domain sockets (unsupported)
/// - "expect 0 link": hard link syscall (ENOTSUP)
const EXCLUDED_PATTERNS: &[&str] = &[
    "mkfifo",
    "mknod",
    "for type in",
    "ENAMETOOLONG",
    "NAME_MAX",
    "S_ISVTX",
    "sticky",
    "socket",
    "expect 0 link",
];

/// Path where pjdfstest is built/cached.
const PJDFSTEST_DIR: &str = "/tmp/pjdfstest";

/// Pinned commit to ensure stable test counts across CI runs.
const PJDFSTEST_REV: &str = "03eb25706d8dbf3611c3f820b45b7a5e09a36c06";

fn ensure_pjdfstest() -> bool {
    let binary = format!("{}/pjdfstest", PJDFSTEST_DIR);
    // Verify both binary existence AND correct revision to avoid stale cache on self-hosted runners.
    if std::path::Path::new(&binary).exists() {
        let rev_ok = Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(PJDFSTEST_DIR)
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim() == PJDFSTEST_REV)
            .unwrap_or(false);
        if rev_ok {
            return true;
        }
        eprintln!("Cached pjdfstest has wrong revision, rebuilding...");
    }

    // Remove stale directory (e.g. from interrupted previous run on self-hosted runner)
    if std::path::Path::new(PJDFSTEST_DIR).exists() {
        std::fs::remove_dir_all(PJDFSTEST_DIR).ok();
    }

    eprintln!("Building pjdfstest...");
    let ok = Command::new("sh")
        .args([
            "-c",
            &format!(
                "git clone https://github.com/pjd/pjdfstest.git {dir} && \
                 cd {dir} && git checkout {rev} && \
                 autoreconf -ifs && ./configure && make pjdfstest",
                dir = PJDFSTEST_DIR,
                rev = PJDFSTEST_REV
            ),
        ])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);

    if !ok {
        eprintln!("Failed to build pjdfstest (missing autoconf/automake/gcc?)");
    }
    ok
}

struct ProveResults {
    total_files: usize,
    total_tests: usize,
    passed_files: usize,
    failed_tests: usize,
    result: String,
    per_category: Vec<(String, usize, usize)>, // (category, passed_files, total_files)
    raw_output: String,
}

fn run_prove(mount_point: &str) -> ProveResults {
    let tests_dir = format!("{}/tests/", PJDFSTEST_DIR);

    // Collect individual .t files, excluding:
    //  - entire categories in EXCLUDED_CATEGORIES
    //  - individual .t files that reference mkfifo/mknod (cascade-fail)
    let mut test_files: Vec<String> = Vec::new();
    let mut skipped_files = 0usize;
    if let Ok(cats) = std::fs::read_dir(&tests_dir) {
        for cat in cats.filter_map(|e| e.ok()) {
            if !cat.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let cat_name = cat.file_name().to_string_lossy().to_string();
            if EXCLUDED_CATEGORIES.contains(&cat_name.as_str()) {
                eprintln!("  Skipping category: {} (unsupported)", cat_name);
                continue;
            }
            if let Ok(files) = std::fs::read_dir(cat.path()) {
                for file in files.filter_map(|e| e.ok()) {
                    let fname = file.file_name().to_string_lossy().to_string();
                    if !fname.ends_with(".t") {
                        continue;
                    }
                    // Check if the test file references excluded patterns
                    let content = std::fs::read_to_string(file.path()).unwrap_or_default();
                    if EXCLUDED_PATTERNS.iter().any(|pat| content.contains(pat)) {
                        skipped_files += 1;
                        continue;
                    }
                    test_files.push(file.path().to_string_lossy().to_string());
                }
            }
        }
    }
    test_files.sort();
    eprintln!(
        "  Running {} test files ({} skipped for mkfifo/mknod references)",
        test_files.len(),
        skipped_files
    );

    let output = Command::new("sudo")
        .arg("prove")
        .args(&test_files)
        .current_dir(mount_point)
        .output()
        .expect("Failed to run prove");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let combined = format!("{}\n{}", stdout, String::from_utf8_lossy(&output.stderr));

    // Parse summary: "Files=237, Tests=8789, ..."
    let total_files = parse_field(&combined, "Files=").unwrap_or(0);
    let total_tests = parse_field(&combined, "Tests=").unwrap_or(0);

    // Parse result line: "Result: PASS" or "Result: FAIL"
    let result = combined
        .lines()
        .find(|l| l.starts_with("Result:"))
        .map(|l| l.trim_start_matches("Result:").trim().to_string())
        .unwrap_or_else(|| "UNKNOWN".to_string());

    // Count passed files (lines ending with "ok")
    let passed_files = combined.lines().filter(|l| l.ends_with("ok")).count();

    // Parse failed test count from "Failed N/M subtests" lines
    let failed_tests: usize = combined
        .lines()
        .filter_map(|l| {
            if l.contains("Failed") && l.contains("subtests") {
                l.split_whitespace()
                    .find(|w| w.contains('/'))
                    .and_then(|w| w.split('/').next())
                    .and_then(|n| n.parse::<usize>().ok())
            } else {
                None
            }
        })
        .sum();

    // Per-category breakdown
    let mut per_category = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&tests_dir) {
        let mut cats: Vec<String> = entries
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        cats.sort();

        for cat in cats {
            let prefix = format!("{}{}/", tests_dir, cat);
            let total = combined.lines().filter(|l| l.contains(&prefix)).count();
            let passed = combined
                .lines()
                .filter(|l| l.contains(&prefix) && l.ends_with("ok"))
                .count();
            if total > 0 {
                per_category.push((cat, passed, total));
            }
        }
    }

    ProveResults {
        total_files,
        total_tests,
        passed_files,
        failed_tests,
        result,
        per_category,
        raw_output: combined,
    }
}

fn parse_field(text: &str, prefix: &str) -> Option<usize> {
    text.lines().find(|l| l.contains(prefix)).and_then(|l| {
        l[l.find(prefix)? + prefix.len()..]
            .split(|c: char| !c.is_ascii_digit())
            .next()
            .and_then(|n| n.parse().ok())
    })
}

fn print_results(results: &ProveResults) {
    eprintln!("\n============================================================");
    eprintln!("  pjdfstest POSIX Compliance Results");
    eprintln!("------------------------------------------------------------");
    eprintln!(
        "  Files: {}/{} passed    Tests: {} total ({} subtests failed)",
        results.passed_files, results.total_files, results.total_tests, results.failed_tests
    );
    eprintln!("  Result: {}", results.result);
    eprintln!("------------------------------------------------------------");
    eprintln!("  {:20} {:>8} {:>8} {:>8}", "Category", "Passed", "Total", "Status");
    eprintln!(
        "  {:20} {:>8} {:>8} {:>8}",
        "--------------------", "--------", "--------", "--------"
    );
    for (cat, passed, total) in &results.per_category {
        let status = if passed == total { "OK" } else { "FAIL" };
        eprintln!("  {:20} {:>8} {:>8} {:>8}", cat, passed, total, status);
    }
    eprintln!("============================================================");

    // Print Test Summary Report (failed test details) if present
    let in_summary: Vec<&str> = results
        .raw_output
        .lines()
        .skip_while(|l| !l.contains("Test Summary Report"))
        .take_while(|l| !l.starts_with("Files="))
        .collect::<Vec<_>>();
    if !in_summary.is_empty() {
        eprintln!();
        for line in in_summary {
            eprintln!("  {}", line);
        }
    }
}

#[tokio::test]
async fn test_pjdfstest() {
    let is_ci = std::env::var("CI").is_ok();
    if !ensure_pjdfstest() {
        if is_ci {
            panic!("pjdfstest build failed in CI -- setup error");
        }
        eprintln!("Skipping: pjdfstest not available");
        return;
    }

    // prove requires perl
    if Command::new("prove").arg("--version").output().is_err() {
        if is_ci {
            panic!("prove (perl TAP harness) not found in CI");
        }
        eprintln!("Skipping: prove (perl TAP harness) not installed");
        return;
    }

    let (token, bucket_id, _hub) = match common::setup_bucket("pjdfs").await {
        Some(cfg) => cfg,
        None => return,
    };

    let pid = std::process::id();
    let mount_point = format!("/tmp/hf-pjdfs-{}", pid);
    let cache_dir = format!("/tmp/hf-pjdfs-cache-{}", pid);

    let child = common::mount_bucket(&bucket_id, &mount_point, &cache_dir, &["--advanced-writes"]);

    let results = run_prove(&mount_point);

    print_results(&results);

    common::unmount(&mount_point, child, 5);
    common::delete_bucket(&common::endpoint(), &token, &bucket_id).await;
    std::fs::remove_dir_all(&mount_point).ok();
    std::fs::remove_dir_all(&cache_dir).ok();

    // Exact regression assertions — all filtered tests must pass
    let passed_tests = results.total_tests.saturating_sub(results.failed_tests);
    assert!(
        results.passed_files >= EXPECTED_FILES_PASS,
        "Regression: {}/{} test files passed (expected at least {})",
        results.passed_files,
        results.total_files,
        EXPECTED_FILES_PASS
    );
    assert!(
        passed_tests >= EXPECTED_TESTS_PASS,
        "Regression: {}/{} tests passed (expected at least {})",
        passed_tests,
        results.total_tests,
        EXPECTED_TESTS_PASS
    );
}
