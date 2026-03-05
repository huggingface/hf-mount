/// Phase 0: Measure whether xet-core's global dedup catches unchanged chunks
/// when re-uploading a file with a small modification.
///
/// Run on EC2 with:
/// ```
/// RUST_LOG=data=debug,deduplication=debug \
///   cargo test --release --test dedup_bench -- --nocapture
/// ```
///
/// Look for dedup metrics in the log output:
///   - `deduped_bytes` vs `new_bytes`
///   - `deduped_chunks` vs `new_chunks`
mod common;

use std::time::Instant;

const FILE_SIZE: usize = 200 * 1024 * 1024; // 200 MB
const MODIFY_OFFSET: usize = 100 * 1024 * 1024; // 100 MB

/// Generate pseudo-random content with a given seed. Uses a simple LCG to avoid
/// matching any cached shards from previous test runs.
fn generate_random_content(size: usize, seed: u64) -> Vec<u8> {
    let mut buf = vec![0u8; size];
    let mut state = seed;
    for chunk in buf.chunks_mut(8) {
        // LCG: state = state * 6364136223846793005 + 1442695040888963407
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let bytes = state.to_le_bytes();
        let len = chunk.len().min(8);
        chunk[..len].copy_from_slice(&bytes[..len]);
    }
    buf
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::fmt::format::FmtSpan;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_span_events(FmtSpan::CLOSE)
        .with_writer(std::io::stderr)
        .try_init();
}

#[tokio::test]
async fn dedup_measurement() {
    init_tracing();
    let (token, bucket_id, hub) = match common::setup_bucket("dedup").await {
        Some(cfg) => cfg,
        None => return,
    };

    let ep = common::endpoint();

    // --- Step 1: Generate 200 MB pseudo-random content (unique seed per run) ---
    let seed = std::process::id() as u64
        ^ std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
    eprintln!(
        "\n=== Generating {} MB random content (seed={}) ===",
        FILE_SIZE / (1024 * 1024),
        seed
    );
    let original = generate_random_content(FILE_SIZE, seed);

    // --- Step 2: Upload original file ---
    let write_config = common::build_write_config(&hub).await;

    let tmp_dir = std::env::temp_dir().join(format!("hf-mount-dedup-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).expect("create tmp dir");

    let original_path = tmp_dir.join("original.bin");
    std::fs::write(&original_path, &original).expect("write original file");

    eprintln!(
        "\n=== Uploading original {} MB file (session 1) ===",
        FILE_SIZE / (1024 * 1024)
    );
    let t0 = Instant::now();
    let original_info = common::upload_file(write_config, &original_path).await;
    let upload1_secs = t0.elapsed().as_secs_f64();
    let upload1_mbps = (FILE_SIZE as f64 / (1024.0 * 1024.0)) / upload1_secs;

    let original_hash = original_info.hash().to_string();
    eprintln!(
        "  Original: xet_hash={}, size={}, time={:.1}s ({:.1} MB/s)",
        original_hash,
        original_info.file_size(),
        upload1_secs,
        upload1_mbps
    );

    // Register file with the hub
    let mtime_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    hub.batch_operations(&[hf_mount::hub_api::BatchOp::AddFile {
        path: "test_file.bin".to_string(),
        xet_hash: original_hash.clone(),
        mtime: mtime_ms,
        content_type: None,
    }])
    .await
    .expect("batch add original failed");

    // --- Step 3: Create modified copy (flip 1 byte at offset 100 MB) ---
    eprintln!(
        "\n=== Creating modified copy (1 byte changed at offset {} MB) ===",
        MODIFY_OFFSET / (1024 * 1024)
    );
    let mut modified = original.clone();
    modified[MODIFY_OFFSET] = modified[MODIFY_OFFSET].wrapping_add(1);

    let modified_path = tmp_dir.join("modified.bin");
    std::fs::write(&modified_path, &modified).expect("write modified file");

    // Verify the files differ at exactly one byte
    let diff_count: usize = original.iter().zip(modified.iter()).filter(|(a, b)| a != b).count();
    assert_eq!(diff_count, 1, "files should differ at exactly 1 byte");

    // --- Step 4: Upload modified file (NEW session, simulating a second flush) ---
    let write_config2 = common::build_write_config(&hub).await;

    eprintln!("\n=== Uploading modified file (session 2, watch for dedup in logs) ===");
    let t1 = Instant::now();
    let modified_info = common::upload_file(write_config2, &modified_path).await;
    let upload2_secs = t1.elapsed().as_secs_f64();
    let upload2_mbps = (FILE_SIZE as f64 / (1024.0 * 1024.0)) / upload2_secs;

    let modified_hash = modified_info.hash().to_string();
    eprintln!(
        "  Modified: xet_hash={}, size={}, time={:.1}s ({:.1} MB/s)",
        modified_hash,
        modified_info.file_size(),
        upload2_secs,
        upload2_mbps
    );

    // --- Step 5: Print summary ---
    eprintln!("\n============================================================");
    eprintln!("  Delta Upload Dedup Measurement");
    eprintln!("------------------------------------------------------------");
    eprintln!("  File size:           {} MB", FILE_SIZE / (1024 * 1024));
    eprintln!(
        "  Modification:        1 byte at offset {} MB",
        MODIFY_OFFSET / (1024 * 1024)
    );
    eprintln!("  Original hash:       {}", original_hash);
    eprintln!("  Modified hash:       {}", modified_hash);
    eprintln!("  Upload 1 (original): {:.1}s ({:.1} MB/s)", upload1_secs, upload1_mbps);
    eprintln!("  Upload 2 (modified): {:.1}s ({:.1} MB/s)", upload2_secs, upload2_mbps);
    eprintln!(
        "  Speedup:             {:.1}x",
        if upload2_secs > 0.0 {
            upload1_secs / upload2_secs
        } else {
            0.0
        }
    );
    eprintln!("------------------------------------------------------------");
    eprintln!("  Check RUST_LOG output above for dedup metrics:");
    eprintln!(
        "    deduped_bytes ≈ {} MB = good (global dedup works)",
        FILE_SIZE / (1024 * 1024)
    );
    eprintln!("    new_bytes ≈ 64-128 KB = good (only modified chunks re-uploaded)");
    eprintln!(
        "    new_bytes ≈ {} MB = bad (no dedup, Phase 1 needed)",
        FILE_SIZE / (1024 * 1024)
    );
    eprintln!("============================================================\n");

    // Hashes must differ (different content)
    assert_ne!(
        original_hash, modified_hash,
        "modified file should have a different hash"
    );

    // Cleanup
    std::fs::remove_dir_all(&tmp_dir).ok();
    common::delete_bucket(&ep, &token, &bucket_id).await;
}
