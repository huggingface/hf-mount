mod common;

use std::process::Command;

/// Non-regression baseline (established 2026-03-06).
/// pjdfstest has 237 test files and 8789 individual tests.
/// We don't support mknod/mkfifo (ENOSYS), which causes cascading failures
/// in tests that create FIFOs/block devices then test operations on them.
/// Current: 178/237 files, 5174/8789 tests (58.9%)
/// Update these when adding new POSIX features.
const MIN_FILES_PASS: usize = 170;
const MIN_TESTS_PASS: usize = 5000;

/// Path where pjdfstest is built/cached.
const PJDFSTEST_DIR: &str = "/tmp/pjdfstest";

fn ensure_pjdfstest() -> bool {
    let binary = format!("{}/pjdfstest", PJDFSTEST_DIR);
    if std::path::Path::new(&binary).exists() {
        return true;
    }

    eprintln!("Building pjdfstest...");
    let ok = Command::new("sh")
        .args([
            "-c",
            &format!(
                "git clone --depth 1 https://github.com/pjd/pjdfstest.git {dir} && \
                 cd {dir} && autoreconf -ifs && ./configure && make pjdfstest",
                dir = PJDFSTEST_DIR
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
}

fn run_prove(mount_point: &str) -> ProveResults {
    let tests_dir = format!("{}/tests/", PJDFSTEST_DIR);

    let output = Command::new("sudo")
        .args(["prove", "-r", &tests_dir])
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
}

#[tokio::test]
async fn test_pjdfstest() {
    if !ensure_pjdfstest() {
        eprintln!("Skipping: pjdfstest not available");
        return;
    }

    // prove requires perl
    if Command::new("prove").arg("--version").output().is_err() {
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

    // Non-regression assertions
    let passed_tests = results.total_tests.saturating_sub(results.failed_tests);
    assert!(
        results.passed_files >= MIN_FILES_PASS,
        "Regression: only {}/{} test files passed (minimum: {})",
        results.passed_files,
        results.total_files,
        MIN_FILES_PASS
    );
    assert!(
        passed_tests >= MIN_TESTS_PASS,
        "Regression: only {}/{} tests passed (minimum: {})",
        passed_tests,
        results.total_tests,
        MIN_TESTS_PASS
    );
}
