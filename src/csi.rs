//! Kubernetes CSI driver implementation for Hugging Face Hub mounts.
//!
//! This module provides gRPC services for the Container Storage Interface (CSI):
//! - `HfCsiIdentity`: Plugin identification and capabilities
//! - `HfCsiController`: Volume lifecycle management (create, delete)
//! - `HfCsiNode`: Volume publication (mount/unmount on nodes)
//!
//! The controller uses `HubValidator` to validate that requested repo or bucket IDs
//! exist on the Hugging Face Hub. The node uses `ProcessSpawner` to launch the
//! `hf-mount-nfs` binary that serves the actual mount. Both traits are abstracted
//! so tests can inject mocks instead of making live network calls or spawning real
//! processes.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
#[allow(unused_imports)]
use tracing::{info, warn};

pub mod v1 {
    tonic::include_proto!("csi.v1");
}

/// Trait for validating Hugging Face Hub sources (repos or buckets).
/// This allows mocking in tests to avoid live network calls.
#[async_trait]
pub trait HubValidator: Send + Sync {
    async fn validate_source(&self, source_type: &str, source_id: &str) -> Result<(), Status>;
}

/// Real implementation using reqwest to hit the HF Hub API.
pub struct RealHubValidator {
    endpoint: String,
    token: Option<String>,
    client: reqwest::Client,
}

impl Default for RealHubValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl RealHubValidator {
    pub fn new() -> Self {
        let endpoint = std::env::var("HF_ENDPOINT").unwrap_or_else(|_| "https://huggingface.co".to_string());
        let token = std::env::var("HF_TOKEN").ok();
        Self {
            endpoint,
            token,
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl HubValidator for RealHubValidator {
    async fn validate_source(&self, source_type: &str, source_id: &str) -> Result<(), Status> {
        let endpoint = self.endpoint.trim_end_matches('/');
        let url = match source_type {
            "repo" => {
                // Try repo info endpoint; repo type defaults to model.
                let parts: Vec<&str> = source_id.splitn(2, '/').collect();
                if parts.len() != 2 {
                    return Err(Status::invalid_argument(format!("invalid repo id: {source_id}")));
                }
                format!("{}/api/models/{}", endpoint, source_id)
            }
            "bucket" => format!("{}/api/buckets/{}", endpoint, source_id),
            _ => {
                return Err(Status::invalid_argument(format!(
                    "unknown source type: {source_type} (expected repo or bucket)"
                )));
            }
        };

        let mut req = self.client.get(&url);
        if let Some(ref t) = self.token {
            req = req.header("Authorization", format!("Bearer {t}"));
        }

        let resp = req
            .send()
            .await
            .map_err(|e| Status::internal(format!("hub request failed: {e}")))?;

        if resp.status().is_success() {
            Ok(())
        } else if resp.status().as_u16() == 401 || resp.status().as_u16() == 403 {
            Err(Status::unauthenticated(format!(
                "HF Hub authentication failed for {source_type} {source_id}"
            )))
        } else if resp.status().as_u16() == 404 {
            Err(Status::not_found(format!(
                "{source_type} {source_id} not found on HF Hub"
            )))
        } else {
            Err(Status::internal(format!(
                "HF Hub returned {} for {source_type} {source_id}",
                resp.status()
            )))
        }
    }
}

/// Mock implementation for testing that records spawned commands and optionally
/// returns a configured error. Prevents live network calls in unit tests.
pub struct MockHubValidator {
    /// If set, validate_source will return this error. If None, returns Ok.
    error_to_return: Option<Status>,
}

impl MockHubValidator {
    /// Create a mock validator that succeeds by default.
    pub fn new() -> Self {
        Self { error_to_return: None }
    }

    /// Configure the validator to return a specific error.
    pub fn with_error(mut self, error: Status) -> Self {
        self.error_to_return = Some(error);
        self
    }
}

impl Default for MockHubValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl HubValidator for MockHubValidator {
    async fn validate_source(&self, _source_type: &str, _source_id: &str) -> Result<(), Status> {
        match &self.error_to_return {
            Some(err) => Err(err.clone()),
            None => Ok(()),
        }
    }
}

/// Trait for spawning processes (e.g., hf-mount-nfs).
/// This allows mocking in tests to avoid requiring actual binaries.
#[async_trait]
pub trait ProcessSpawner: Send + Sync {
    /// Spawn a program with the given arguments.
    async fn spawn(&self, program: &str, args: &[&str]) -> Result<(), Status>;
}

/// Real implementation using tokio::process::Command.
pub struct RealProcessSpawner;

#[async_trait]
impl ProcessSpawner for RealProcessSpawner {
    async fn spawn(&self, program: &str, args: &[&str]) -> Result<(), Status> {
        let result = tokio::process::Command::new(program)
            .args(args)
            .spawn()
            .map_err(|e| Status::internal(format!("spawn {program}: {e}")))?;
        // Detach the child process so it continues running in the background.
        let _ = result;
        Ok(())
    }
}

/// Command record stored by the mock spawner for assertion in tests.
pub type SpawnedCommand = (String, Vec<String>);

/// Mock implementation for testing that records spawned commands and optionally
/// returns a configured error. Prevents real process spawning in unit tests.
#[derive(Clone)]
pub struct MockProcessSpawner {
    /// If set, spawn will return this error. If None, returns Ok.
    error_to_return: Option<Status>,
    /// Track spawned commands for verification.
    spawned_commands: Arc<Mutex<Vec<SpawnedCommand>>>,
}

impl MockProcessSpawner {
    /// Create a mock spawner that succeeds by default.
    pub fn new() -> Self {
        Self {
            error_to_return: None,
            spawned_commands: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Configure the spawner to return a specific error.
    pub fn with_error(mut self, error: Status) -> Self {
        self.error_to_return = Some(error);
        self
    }

    /// Retrieve the list of commands that were passed to spawn(), for assertion in tests.
    pub async fn get_spawned_commands(&self) -> Vec<SpawnedCommand> {
        self.spawned_commands.lock().await.clone()
    }
}

impl Default for MockProcessSpawner {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ProcessSpawner for MockProcessSpawner {
    async fn spawn(&self, program: &str, args: &[&str]) -> Result<(), Status> {
        let mut commands = self.spawned_commands.lock().await;
        commands.push((program.to_string(), args.iter().map(|s| s.to_string()).collect()));
        drop(commands);

        match &self.error_to_return {
            Some(err) => Err(err.clone()),
            None => Ok(()),
        }
    }
}

use self::v1::{
    ControllerGetCapabilitiesRequest, ControllerGetCapabilitiesResponse, ControllerServiceCapability,
    CreateVolumeRequest, CreateVolumeResponse, DeleteVolumeRequest, DeleteVolumeResponse, GetPluginCapabilitiesRequest,
    GetPluginCapabilitiesResponse, GetPluginInfoRequest, GetPluginInfoResponse, NodeGetCapabilitiesRequest,
    NodeGetCapabilitiesResponse, NodeGetInfoRequest, NodeGetInfoResponse, NodePublishVolumeRequest,
    NodePublishVolumeResponse, NodeServiceCapability, NodeStageVolumeRequest, NodeStageVolumeResponse,
    NodeUnpublishVolumeRequest, NodeUnpublishVolumeResponse, NodeUnstageVolumeRequest, NodeUnstageVolumeResponse,
    PluginCapability, ProbeRequest, ProbeResponse, Volume, controller_server::Controller, identity_server::Identity,
    node_server::Node,
};

const PLUGIN_NAME: &str = "csi.huggingface.co";
const PLUGIN_VERSION: &str = env!("CARGO_PKG_VERSION");

/// In-memory volume registry. In production this would be backed by the
/// hub API (CreateVolume allocates a bucket, DeleteVolume removes it).
#[derive(Default)]
pub struct VolumeRegistry {
    volumes: Mutex<HashMap<String, u64>>,
}

impl VolumeRegistry {
    /// Create a new volume with the given name and capacity, returning a unique ID.
    pub async fn create(&self, name: &str, capacity: u64) -> String {
        let id = format!("hf-vol-{}", uuid::Uuid::new_v4());
        let mut vols = self.volumes.lock().await;
        vols.insert(id.clone(), capacity);
        info!("Created volume {} (name={})", id, name);
        id
    }

    /// Look up a volume by ID, returning its capacity if found.
    pub async fn get(&self, volume_id: &str) -> Option<u64> {
        let vols = self.volumes.lock().await;
        vols.get(volume_id).copied()
    }

    /// Remove a volume from the registry.
    pub async fn delete(&self, volume_id: &str) {
        let mut vols = self.volumes.lock().await;
        vols.remove(volume_id);
        info!("Deleted volume {}", volume_id);
    }
}

#[derive(Default)]
pub struct HfCsiIdentity;

#[tonic::async_trait]
impl Identity for HfCsiIdentity {
    async fn get_plugin_info(
        &self,
        _request: Request<GetPluginInfoRequest>,
    ) -> Result<Response<GetPluginInfoResponse>, Status> {
        Ok(Response::new(GetPluginInfoResponse {
            name: PLUGIN_NAME.to_string(),
            vendor_version: PLUGIN_VERSION.to_string(),
            manifest: HashMap::new(),
        }))
    }

    async fn get_plugin_capabilities(
        &self,
        _request: Request<GetPluginCapabilitiesRequest>,
    ) -> Result<Response<GetPluginCapabilitiesResponse>, Status> {
        use self::v1::plugin_capability;
        Ok(Response::new(GetPluginCapabilitiesResponse {
            capabilities: vec![PluginCapability {
                r#type: Some(plugin_capability::Type::Service(plugin_capability::Service {
                    r#type: plugin_capability::service::Type::ControllerService as i32,
                })),
            }],
        }))
    }

    async fn probe(&self, _request: Request<ProbeRequest>) -> Result<Response<ProbeResponse>, Status> {
        Ok(Response::new(ProbeResponse {}))
    }
}

#[derive(Clone)]
pub struct HfCsiController {
    registry: Arc<VolumeRegistry>,
    hub_validator: Arc<dyn HubValidator>,
}

impl Default for HfCsiController {
    fn default() -> Self {
        Self {
            registry: Arc::new(VolumeRegistry::default()),
            hub_validator: Arc::new(RealHubValidator::new()),
        }
    }
}

impl HfCsiController {
    /// Create a controller with the real HF Hub validator (makes live network calls).
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a controller with an injected validator, useful for testing.
    pub fn with_validator(hub_validator: Arc<dyn HubValidator>) -> Self {
        Self {
            registry: Arc::new(VolumeRegistry::default()),
            hub_validator,
        }
    }
}

#[tonic::async_trait]
impl Controller for HfCsiController {
    async fn create_volume(
        &self,
        request: Request<CreateVolumeRequest>,
    ) -> Result<Response<CreateVolumeResponse>, Status> {
        let req = request.into_inner();
        let capacity = req.required_bytes;

        // Parse source from parameters.
        let source_type = req.parameters.get("sourceType").map(|s| s.as_str()).unwrap_or("repo");
        let source_id = req
            .parameters
            .get("sourceId")
            .cloned()
            .unwrap_or_else(|| req.name.clone());

        // Local format validation (does not require network).
        if source_type == "repo" {
            let parts: Vec<&str> = source_id.splitn(2, '/').collect();
            if parts.len() != 2 {
                return Err(Status::invalid_argument(format!("invalid repo id: {source_id}")));
            }
        } else if source_type != "bucket" {
            return Err(Status::invalid_argument(format!(
                "unknown source type: {source_type} (expected repo or bucket)"
            )));
        }

        // Validate the source exists on HF Hub.
        self.hub_validator.validate_source(source_type, &source_id).await?;

        let volume_id = self.registry.create(&req.name, capacity).await;
        info!(
            "CreateVolume: name={} source={}/{} volume_id={}",
            req.name, source_type, source_id, volume_id
        );

        Ok(Response::new(CreateVolumeResponse {
            volume: Some(Volume {
                volume_id,
                capacity_bytes: capacity,
            }),
        }))
    }

    async fn delete_volume(
        &self,
        request: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeResponse>, Status> {
        let volume_id = request.into_inner().volume_id;
        info!("DeleteVolume: {}", volume_id);
        self.registry.delete(&volume_id).await;
        Ok(Response::new(DeleteVolumeResponse {}))
    }

    async fn controller_get_capabilities(
        &self,
        _request: Request<ControllerGetCapabilitiesRequest>,
    ) -> Result<Response<ControllerGetCapabilitiesResponse>, Status> {
        use self::v1::controller_service_capability;
        Ok(Response::new(ControllerGetCapabilitiesResponse {
            capabilities: vec![ControllerServiceCapability {
                r#type: Some(controller_service_capability::Type::Rpc(
                    controller_service_capability::Rpc {
                        r#type: controller_service_capability::rpc::Type::CreateDeleteVolume as i32,
                    },
                )),
            }],
        }))
    }
}

/// Tracks active mounts on the node so unpublish/unstage can clean up.
struct NodeMount {
    volume_id: String,
    target_path: String,
}

pub struct HfCsiNode {
    node_id: String,
    mounts: Mutex<Vec<NodeMount>>,
    process_spawner: Arc<dyn ProcessSpawner>,
}

impl HfCsiNode {
    /// Create a node with the real process spawner (spawns actual hf-mount-nfs binaries).
    pub fn new(node_id: String) -> Self {
        Self {
            node_id,
            mounts: Mutex::new(Vec::new()),
            process_spawner: Arc::new(RealProcessSpawner),
        }
    }

    /// Create a node with an injected spawner, useful for testing.
    pub fn with_spawner(node_id: String, process_spawner: Arc<dyn ProcessSpawner>) -> Self {
        Self {
            node_id,
            mounts: Mutex::new(Vec::new()),
            process_spawner,
        }
    }
}

#[tonic::async_trait]
impl Node for HfCsiNode {
    async fn node_stage_volume(
        &self,
        request: Request<NodeStageVolumeRequest>,
    ) -> Result<Response<NodeStageVolumeResponse>, Status> {
        let req = request.into_inner();
        info!(
            "NodeStageVolume: volume_id={} staging_target_path={}",
            req.volume_id, req.staging_target_path
        );
        // For NFS: create staging directory. Actual mount happens in NodePublishVolume.
        tokio::fs::create_dir_all(&req.staging_target_path)
            .await
            .map_err(|e| Status::internal(format!("create staging dir: {e}")))?;
        Ok(Response::new(NodeStageVolumeResponse {}))
    }

    async fn node_unstage_volume(
        &self,
        request: Request<NodeUnstageVolumeRequest>,
    ) -> Result<Response<NodeUnstageVolumeResponse>, Status> {
        let req = request.into_inner();
        info!(
            "NodeUnstageVolume: volume_id={} staging_target_path={}",
            req.volume_id, req.staging_target_path
        );
        let mut mounts = self.mounts.lock().await;
        mounts.retain(|m| m.volume_id != req.volume_id);
        Ok(Response::new(NodeUnstageVolumeResponse {}))
    }

    async fn node_publish_volume(
        &self,
        request: Request<NodePublishVolumeRequest>,
    ) -> Result<Response<NodePublishVolumeResponse>, Status> {
        let req = request.into_inner();
        info!(
            "NodePublishVolume: volume_id={} target_path={}",
            req.volume_id, req.target_path
        );

        // Extract mount parameters from volume_context.
        let source = req.volume_context.get("source").cloned().unwrap_or_default();
        let read_only = req.readonly;

        #[cfg(unix)]
        {
            self.publish_unix(&req.volume_id, &req.target_path, &source, read_only)
                .await?;
        }
        #[cfg(not(unix))]
        {
            self.publish_windows(&req.volume_id, &req.target_path, &source, read_only)
                .await?;
        }

        Ok(Response::new(NodePublishVolumeResponse {}))
    }

    async fn node_unpublish_volume(
        &self,
        request: Request<NodeUnpublishVolumeRequest>,
    ) -> Result<Response<NodeUnpublishVolumeResponse>, Status> {
        let req = request.into_inner();
        info!(
            "NodeUnpublishVolume: volume_id={} target_path={}",
            req.volume_id, req.target_path
        );

        let mut mounts = self.mounts.lock().await;
        if let Some(pos) = mounts
            .iter()
            .position(|m| m.volume_id == req.volume_id && m.target_path == req.target_path)
        {
            let mount = mounts.remove(pos);
            #[cfg(unix)]
            {
                let _ = std::process::Command::new("umount").arg(&mount.target_path).status();
            }
            #[cfg(not(unix))]
            {
                let _ = std::process::Command::new("net")
                    .args(["use", &mount.target_path, "/delete"])
                    .status();
            }
        }

        Ok(Response::new(NodeUnpublishVolumeResponse {}))
    }

    async fn node_get_capabilities(
        &self,
        _request: Request<NodeGetCapabilitiesRequest>,
    ) -> Result<Response<NodeGetCapabilitiesResponse>, Status> {
        use self::v1::node_service_capability;
        Ok(Response::new(NodeGetCapabilitiesResponse {
            capabilities: vec![NodeServiceCapability {
                r#type: Some(node_service_capability::Type::Rpc(node_service_capability::Rpc {
                    r#type: node_service_capability::rpc::Type::StageUnstageVolume as i32,
                })),
            }],
        }))
    }

    async fn node_get_info(
        &self,
        _request: Request<NodeGetInfoRequest>,
    ) -> Result<Response<NodeGetInfoResponse>, Status> {
        Ok(Response::new(NodeGetInfoResponse {
            node_id: self.node_id.clone(),
            max_volumes_per_node: 64,
        }))
    }
}

impl HfCsiNode {
    #[cfg(unix)]
    async fn publish_unix(
        &self,
        volume_id: &str,
        target_path: &str,
        source: &str,
        read_only: bool,
    ) -> Result<(), Status> {
        tokio::fs::create_dir_all(target_path)
            .await
            .map_err(|e| Status::internal(format!("create target dir: {e}")))?;

        // Spawn hf-mount-nfs in the background.
        let mut args = vec![source, target_path, "--nfs-bind", "127.0.0.1:0"];
        if read_only {
            args.push("--read-only");
        }

        self.process_spawner.spawn("hf-mount-nfs", &args).await?;

        let mut mounts = self.mounts.lock().await;
        mounts.push(NodeMount {
            volume_id: volume_id.to_string(),
            target_path: target_path.to_string(),
        });

        Ok(())
    }

    #[cfg(not(unix))]
    async fn publish_windows(
        &self,
        volume_id: &str,
        target_path: &str,
        source: &str,
        read_only: bool,
    ) -> Result<(), Status> {
        tokio::fs::create_dir_all(target_path)
            .await
            .map_err(|e| Status::internal(format!("create target dir: {e}")))?;

        // Spawn hf-mount-nfs binding to 0.0.0.0 so Windows host can reach it.
        let mut args = vec![source, target_path, "--nfs-bind", "0.0.0.0:2049"];
        if read_only {
            args.push("--read-only");
        }

        self.process_spawner.spawn("hf-mount-nfs.exe", &args).await?;

        // Give the NFS server a moment to bind before mounting.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Mount the NFS share using the Windows built-in NFS client.
        // Fix: Format the UNC path correctly - target_path should be a drive letter like "C:"
        let mount_target = format!(r"\\127.0.0.1\{}", target_path.trim_end_matches('\\'));
        let mount_result = tokio::process::Command::new("mount")
            .args(["-o", "anon", &mount_target])
            .output()
            .await;

        match mount_result {
            Ok(output) if output.status.success() => {
                info!("Windows NFS mount succeeded: {} -> {}", volume_id, target_path);
            }
            Ok(output) => {
                let stderr = String::from_utf8_lossy(&output.stderr);
                warn!("Windows NFS mount warning (non-fatal): {}", stderr.trim());
            }
            Err(e) => {
                warn!("Windows NFS mount command failed (non-fatal): {e}");
            }
        }

        let mut mounts = self.mounts.lock().await;
        mounts.push(NodeMount {
            volume_id: volume_id.to_string(),
            target_path: target_path.to_string(),
        });

        Ok(())
    }
}
