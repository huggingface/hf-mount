mod common;

use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

struct FioResult {
    name: String,
    bw_mbps: f64,
    iops: f64,
    lat_avg_us: f64,
}

struct FioJob {
    name: &'static str,
    filename: &'static str,
    rw: &'static str,
    bs: &'static str,
    time_based: bool,
}

const FIO_JOBS: &[FioJob] = &[
    FioJob {
        name: "seq-read-100M",
        filename: "large_0.bin",
        rw: "read",
        bs: "128k",
        time_based: false,
    },
    FioJob {
        name: "seq-reread-100M",
        filename: "large_0.bin",
        rw: "read",
        bs: "128k",
        time_based: false,
    },
    FioJob {
        name: "rand-read-4k-100M",
        filename: "large_0.bin",
        rw: "randread",
        bs: "4k",
        time_based: true,
    },
    FioJob {
        name: "seq-read-5x10M",
        filename: "medium_0.bin:medium_1.bin:medium_2.bin:medium_3.bin:medium_4.bin",
        rw: "read",
        bs: "128k",
        time_based: false,
    },
    FioJob {
        name: "rand-read-10x1M",
        filename: "small_0.bin:small_1.bin:small_2.bin:small_3.bin:small_4.bin:small_5.bin:small_6.bin:small_7.bin:small_8.bin:small_9.bin",
        rw: "randread",
        bs: "4k",
        time_based: true,
    },
];

fn run_fio_suite(mount_point: &str) -> Vec<FioResult> {
    FIO_JOBS.iter().map(|job| run_fio(mount_point, job)).collect()
}

fn run_fio(mount_point: &str, job: &FioJob) -> FioResult {
    eprintln!("  fio: {}", job.name);

    let mut args = vec![
        "--name",
        job.name,
        "--directory",
        mount_point,
        "--readonly",
        "--filename",
        job.filename,
        "--rw",
        job.rw,
        "--bs",
        job.bs,
        "--ioengine",
        "sync",
        "--output-format",
        "json",
    ];
    if job.time_based {
        args.extend_from_slice(&["--runtime", "5", "--time_based"]);
    }

    let output = Command::new("fio").args(&args).output().expect("Failed to run fio");

    assert!(
        output.status.success(),
        "fio {} failed: {}",
        job.name,
        String::from_utf8_lossy(&output.stderr)
    );

    let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("fio JSON parse failed");

    let read = &json["jobs"][0]["read"];
    let bw_bytes = read["bw"].as_f64().unwrap_or(0.0); // KiB/s
    let iops = read["iops"].as_f64().unwrap_or(0.0);
    let lat_avg_ns = read["lat_ns"]["mean"].as_f64().unwrap_or(0.0);

    FioResult {
        name: job.name.to_string(),
        bw_mbps: bw_bytes / 1024.0, // KiB/s -> MiB/s
        iops,
        lat_avg_us: lat_avg_ns / 1000.0,
    }
}

fn print_table(fuse_results: &[FioResult], nfs_results: &[FioResult]) {
    eprintln!("\n============================================================");
    eprintln!("  fio Benchmark Results");
    eprintln!("------------------------------------------------------------");
    eprintln!(
        "  {:25} {:>10} {:>10} {:>10} {:>10}",
        "Job", "FUSE MB/s", "NFS MB/s", "FUSE IOPS", "NFS IOPS"
    );
    eprintln!(
        "  {:25} {:>10} {:>10} {:>10} {:>10}",
        "-------------------------", "----------", "----------", "----------", "----------"
    );

    for (f, n) in fuse_results.iter().zip(nfs_results.iter()) {
        let fuse_bw = format!("{:.1}", f.bw_mbps);
        let nfs_bw = format!("{:.1}", n.bw_mbps);

        if f.name.contains("rand") {
            // For random reads, IOPS and latency are more interesting
            let fuse_iops = format!("{:.0}", f.iops);
            let nfs_iops = format!("{:.0}", n.iops);
            eprintln!(
                "  {:25} {:>10} {:>10} {:>10} {:>10}",
                f.name, fuse_bw, nfs_bw, fuse_iops, nfs_iops
            );
        } else {
            eprintln!("  {:25} {:>10} {:>10} {:>10} {:>10}", f.name, fuse_bw, nfs_bw, "", "");
        }
    }

    // Latency sub-table for random reads
    let randoms: Vec<_> = fuse_results
        .iter()
        .zip(nfs_results.iter())
        .filter(|(f, _)| f.name.contains("rand"))
        .collect();

    if !randoms.is_empty() {
        eprintln!("  {:25} {:>12} {:>12}", "Random Read Latency", "FUSE avg", "NFS avg");
        eprintln!(
            "  {:25} {:>12} {:>12}",
            "-------------------------", "------------", "------------"
        );
        for (f, n) in &randoms {
            eprintln!("  {:25} {:>9.1} us {:>9.1} us", f.name, f.lat_avg_us, n.lat_avg_us);
        }
    }

    eprintln!("============================================================");
}

#[tokio::test]
async fn test_fio_compare() {
    if Command::new("fio").arg("--version").output().is_err() {
        eprintln!("Skipping: fio not installed");
        return;
    }

    let guard = match common::setup_bucket("fio-cmp").await {
        Some(g) => g,
        None => return,
    };

    let write_config = common::build_write_config(&guard.hub).await;

    let pid = std::process::id();
    let tmp_dir = std::env::temp_dir().join(format!("hf-fio-cmp-setup-{}", pid));
    std::fs::create_dir_all(&tmp_dir).ok();

    let mut batch_ops = Vec::new();
    let mtime_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let file_specs: Vec<(String, usize)> = (0..10)
        .map(|i| (format!("small_{}.bin", i), 1024 * 1024))
        .chain((0..5).map(|i| (format!("medium_{}.bin", i), 10 * 1024 * 1024)))
        .chain(std::iter::once(("large_0.bin".to_string(), 100 * 1024 * 1024)))
        .collect();

    let total_mb: usize = file_specs.iter().map(|(_, s)| s).sum::<usize>() / (1024 * 1024);
    eprintln!("Uploading {} files ({} MB total)...", file_specs.len(), total_mb);

    for (filename, size) in &file_specs {
        let data = common::generate_pattern(*size);
        let staging_path = tmp_dir.join(filename);
        std::fs::write(&staging_path, &data).expect("write staging file");

        let file_info = common::upload_file(write_config.clone(), &staging_path).await;
        let xet_hash = file_info.hash().to_string();
        eprintln!(
            "  Uploaded {} ({} MB) hash={}",
            filename,
            size / (1024 * 1024),
            &xet_hash[..16]
        );

        batch_ops.push(hf_mount::hub_api::BatchOp::AddFile {
            path: filename.clone(),
            xet_hash,
            mtime: mtime_ms,
            content_type: None,
        });
    }

    guard.hub.batch_operations(&batch_ops).await.expect("batch add failed");
    eprintln!("All files committed to bucket");
    std::fs::remove_dir_all(&tmp_dir).ok();

    // --- FUSE fio ---
    let fuse_mount = format!("/tmp/hf-fio-cmp-fuse-{}", pid);
    let fuse_cache = format!("/tmp/hf-fio-cmp-fuse-cache-{}", pid);

    eprintln!("\nRunning fio suite on FUSE...");
    let fuse_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket(&guard.bucket_id, &fuse_mount, &fuse_cache, &["--read-only"]);
        let results = run_fio_suite(&fuse_mount);
        common::unmount(&fuse_mount, child, 5);
        results
    }));

    std::fs::remove_dir_all(&fuse_mount).ok();
    std::fs::remove_dir_all(&fuse_cache).ok();

    // --- NFS fio ---
    let nfs_mount = format!("/tmp/hf-fio-cmp-nfs-{}", pid);
    let nfs_cache = format!("/tmp/hf-fio-cmp-nfs-cache-{}", pid);

    eprintln!("\nRunning fio suite on NFS...");
    let nfs_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = common::mount_bucket_nfs(&guard.bucket_id, &nfs_mount, &nfs_cache, &["--read-only"]);
        let results = run_fio_suite(&nfs_mount);
        common::unmount_nfs(&nfs_mount, child, 5);
        results
    }));

    std::fs::remove_dir_all(&nfs_mount).ok();
    std::fs::remove_dir_all(&nfs_cache).ok();

    let fuse_results = match fuse_result {
        Ok(r) => r,
        Err(e) => std::panic::resume_unwind(e),
    };
    let nfs_results = match nfs_result {
        Ok(r) => r,
        Err(e) => std::panic::resume_unwind(e),
    };

    print_table(&fuse_results, &nfs_results);
}
