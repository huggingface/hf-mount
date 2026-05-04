pub mod cached_xet_client;
#[cfg(unix)]
pub mod daemon;
#[cfg(not(unix))]
pub mod daemon {
    //! Windows stub: only the surface used by hf-mount-nfs. The full daemon
    //! controller (hf-mount) is Unix-only and not built on Windows.
    pub struct DaemonGuard;
    impl DaemonGuard {
        pub fn from_env() -> Option<Self> {
            None
        }
        pub fn notify_ready(&mut self) {}
    }
}
pub mod error;
pub mod file_cache;
#[cfg(all(unix, feature = "fuse"))]
pub mod fuse;
pub mod hub_api;
#[cfg(feature = "nfs")]
pub mod nfs;
pub mod setup;
pub mod virtual_fs;
pub mod xet;

#[cfg(test)]
pub(crate) mod test_mocks;
