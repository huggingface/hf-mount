//! Heap profiling via jemalloc, opt-in behind the `heap-profiling` feature.
//!
//! When enabled at compile time AND activated at runtime via `MALLOC_CONF`
//! (e.g. `MALLOC_CONF=prof:true,prof_active:true,prof_prefix:/tmp/jeprof.out`),
//! jemalloc records sampled allocation backtraces. Profiles can be dumped:
//!   - automatically at fixed allocation intervals (`lg_prof_interval`),
//!   - automatically at exit (`prof_final:true`),
//!   - or on demand by calling `dump_profile()` (used by the periodic thread).
//!
//! The crate also re-exports `Jemalloc` so binaries can install it as the
//! `#[global_allocator]`. This must happen at the binary crate root.
//!
//! Analysis: copy the dump files out of the pod and run
//!   `jeprof --collapsed <bin> <heap-file> > out.folded`
//! then visualize with FlameGraph or pprof.
//!
//! NOTE: This module compiles to a no-op (empty) on non-Linux targets, since
//! jemalloc profiling is Linux-only. Mac/Windows builds remain on the system
//! allocator regardless of the feature.

#[cfg(all(feature = "heap-profiling", target_os = "linux"))]
pub use tikv_jemallocator::Jemalloc;

#[cfg(all(feature = "heap-profiling", target_os = "linux"))]
pub fn maybe_spawn_periodic_dumps() {
    use std::time::Duration;
    use tikv_jemalloc_ctl::raw;
    use tracing::{info, warn};

    let interval_secs = match std::env::var("HEAP_PROF_DUMP_INTERVAL_SECS") {
        Ok(s) => match s.parse::<u64>() {
            Ok(v) if v > 0 => v,
            _ => return,
        },
        Err(_) => return,
    };

    // Confirm profiling is active before spawning the thread; otherwise
    // mallctl("prof.dump") will fail every cycle.
    let prof_active: bool = match unsafe { raw::read(b"opt.prof\0") } {
        Ok(v) => v,
        Err(e) => {
            warn!("heap-profiling: opt.prof read failed ({e:?}); not spawning dumper");
            return;
        }
    };
    if !prof_active {
        warn!("heap-profiling: opt.prof=false; set MALLOC_CONF=prof:true to enable");
        return;
    }

    info!("heap-profiling: dumping every {interval_secs}s via prof.dump");
    std::thread::Builder::new()
        .name("heap-prof-dump".into())
        .spawn(move || {
            loop {
                std::thread::sleep(Duration::from_secs(interval_secs));
                // Passing a null filename makes jemalloc use the configured
                // prof_prefix and an auto-incrementing sequence number.
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
