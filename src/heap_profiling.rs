//! Heap profiling via jemalloc, opt-in behind the `heap-profiling` feature.
//!
//! When the feature is enabled, the build:
//!   - links against jemalloc with profiling support,
//!   - installs jemalloc as the global allocator,
//!   - bakes a `malloc_conf` symbol so profiling auto-activates at startup
//!     (no MALLOC_CONF env var required),
//!   - spawns a thread that dumps a profile every 600 s by default
//!     (override with `HEAP_PROF_DUMP_INTERVAL_SECS`).
//!
//! Dumps land at `/tmp/jeprof.<pid>.<seq>.heap`. Copy them out of the pod
//! and analyze with `jeprof --collapsed <bin> <heap-file> | flamegraph`.
//!
//! NOTE: This module compiles to a no-op on non-Linux targets, since
//! jemalloc profiling is Linux-only. Mac/Windows builds remain on the system
//! allocator regardless of the feature.

#[cfg(all(feature = "heap-profiling", target_os = "linux"))]
pub use tikv_jemallocator::Jemalloc;

// Baked-in jemalloc options, read by jemalloc at process init. Equivalent
// to setting MALLOC_CONF in the environment, but lives in the binary so the
// canary deploy is just an image swap.
//
// - prof:true             enable profiling subsystem
// - prof_active:true      start with sampling active
// - prof_prefix:/tmp/jeprof  dump location (writable in our pods)
// - lg_prof_sample:19     ~512 KiB sampling rate (jemalloc default)
// - prof_final:true       dump at process exit
#[cfg(all(feature = "heap-profiling", target_os = "linux"))]
const MALLOC_CONF_BYTES: &[u8] =
    b"prof:true,prof_active:true,prof_prefix:/tmp/jeprof,lg_prof_sample:19,prof_final:true\0";

#[cfg(all(feature = "heap-profiling", target_os = "linux"))]
#[repr(transparent)]
struct MallocConfPtr(*const u8);

// SAFETY: jemalloc only reads the pointer once during init; we never mutate it
// from Rust. The pointee is a static byte string with a trailing NUL.
#[cfg(all(feature = "heap-profiling", target_os = "linux"))]
unsafe impl Sync for MallocConfPtr {}

#[cfg(all(feature = "heap-profiling", target_os = "linux"))]
#[unsafe(no_mangle)]
#[used]
#[allow(non_upper_case_globals)]
static malloc_conf: MallocConfPtr = MallocConfPtr(MALLOC_CONF_BYTES.as_ptr());

#[cfg(all(feature = "heap-profiling", target_os = "linux"))]
pub fn maybe_spawn_periodic_dumps() {
    use std::time::Duration;
    use tikv_jemalloc_ctl::raw;
    use tracing::{info, warn};

    let interval_secs: u64 = std::env::var("HEAP_PROF_DUMP_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&v| v > 0)
        .unwrap_or(600);

    info!("heap-profiling: dumping every {interval_secs}s to /tmp/jeprof.*.heap");
    std::thread::Builder::new()
        .name("heap-prof-dump".into())
        .spawn(move || {
            loop {
                std::thread::sleep(Duration::from_secs(interval_secs));
                // Null filename -> jemalloc uses the configured prof_prefix
                // and an auto-incrementing sequence number.
                let res: Result<(), _> = unsafe { raw::write(b"prof.dump\0", std::ptr::null::<*const i8>()) };
                match res {
                    Ok(_) => info!("heap-profiling: dumped profile"),
                    Err(e) => warn!("heap-profiling: prof.dump failed: {e:?}"),
                }
            }
        })
        .expect("spawn heap-prof-dump thread");
}

#[cfg(not(all(feature = "heap-profiling", target_os = "linux")))]
pub fn maybe_spawn_periodic_dumps() {}
