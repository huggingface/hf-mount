//! Windows stub for the daemon module.
//!
//! Windows has no fork/setsid; daemonization is handled by the Service Control
//! Manager or `Start-Process -WindowStyle Hidden`. The stub keeps the API surface
//! intact so the rest of the crate compiles cross-platform, with daemon control
//! commands returning a clear "not supported" error.

use std::path::{Path, PathBuf};

pub struct DaemonGuard;

impl DaemonGuard {
    pub fn pid_file(&self) -> &Path {
        Path::new("")
    }

    pub fn write_fd(&self) -> i32 {
        -1
    }

    pub fn notify_ready(&mut self) {}

    pub fn from_env() -> Option<Self> {
        None
    }
}

pub struct DaemonInfo {
    pub pid: i32,
    pub mount_point: String,
    pub source: Option<String>,
    pub log_file: PathBuf,
}

pub fn list_daemons() -> Vec<DaemonInfo> {
    Vec::new()
}

pub fn stop_daemon(_mount_point: &Path) -> std::io::Result<()> {
    Err(std::io::Error::other(
        "daemon control is not supported on Windows; use Stop-Process or Task Manager",
    ))
}

pub fn daemonize(_mount_point: &Path, _source_label: &str) -> std::io::Result<DaemonGuard> {
    Err(std::io::Error::other(
        "daemonization is not supported on Windows; run hf-mount-nfs directly",
    ))
}
