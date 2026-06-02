//! CSI conformance integration tests.
//!
//! Spins up the gRPC services in-process and validates each CSI service
//! against the spec requirements.

use std::collections::HashMap;

use tonic::Request;

use hf_mount::csi::{
    HfCsiController, HfCsiIdentity, HfCsiNode, v1::controller_server::Controller, v1::identity_server::Identity,
    v1::node_server::Node, v1::*,
};

// ── Identity Service ────────────────────────────────────────────────

#[tokio::test]
async fn identity_get_plugin_info() {
    let identity = HfCsiIdentity;
    let response = identity
        .get_plugin_info(Request::new(GetPluginInfoRequest {}))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.name, "csi.huggingface.co");
    assert!(!response.vendor_version.is_empty());
}

#[tokio::test]
async fn identity_get_plugin_capabilities() {
    let identity = HfCsiIdentity;
    let response = identity
        .get_plugin_capabilities(Request::new(GetPluginCapabilitiesRequest {}))
        .await
        .unwrap()
        .into_inner();

    assert!(!response.capabilities.is_empty());
    let cap = &response.capabilities[0];
    assert!(cap.r#type.is_some(), "PluginCapability must have a type");
}

#[tokio::test]
async fn identity_probe() {
    let identity = HfCsiIdentity;
    let response = identity
        .probe(Request::new(ProbeRequest {}))
        .await
        .unwrap()
        .into_inner();
    // ProbeResponse is empty; success is the assertion.
    assert_eq!(std::mem::size_of_val(&response), 0);
}

// ── Controller Service ──────────────────────────────────────────────

#[tokio::test]
async fn controller_get_capabilities() {
    let controller = HfCsiController::new();
    let response = controller
        .controller_get_capabilities(Request::new(ControllerGetCapabilitiesRequest {}))
        .await
        .unwrap()
        .into_inner();

    assert!(!response.capabilities.is_empty());
    let cap = &response.capabilities[0];
    assert!(cap.r#type.is_some(), "ControllerServiceCapability must have a type");
}

#[tokio::test]
async fn controller_create_volume_not_found_or_unauthenticated() {
    let controller = HfCsiController::new();

    // CreateVolume with a non-existent bucket returns either Unauthenticated
    // (if HF Hub requires auth before revealing existence) or NotFound.
    let err = controller
        .create_volume(Request::new(CreateVolumeRequest {
            name: "test-bucket-vol".to_string(),
            parameters: HashMap::from([
                ("sourceType".to_string(), "bucket".to_string()),
                ("sourceId".to_string(), "myuser/nonexistent-bucket".to_string()),
            ]),
            required_bytes: 1024 * 1024 * 1024,
        }))
        .await
        .unwrap_err();

    assert!(
        err.code() == tonic::Code::NotFound || err.code() == tonic::Code::Unauthenticated,
        "expected NotFound or Unauthenticated, got {:?}",
        err.code()
    );
}

#[tokio::test]
async fn controller_create_volume_invalid_source() {
    let controller = HfCsiController::new();

    // Invalid source (missing slash in repo id) returns InvalidArgument.
    let err = controller
        .create_volume(Request::new(CreateVolumeRequest {
            name: "bad-vol".to_string(),
            parameters: HashMap::from([
                ("sourceType".to_string(), "repo".to_string()),
                ("sourceId".to_string(), "invalid-repo-id".to_string()),
            ]),
            required_bytes: 1024 * 1024 * 1024,
        }))
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn controller_delete_volume_idempotent() {
    let controller = HfCsiController::new();

    // DeleteVolume on an unknown volume_id is a no-op (idempotent).
    controller
        .delete_volume(Request::new(DeleteVolumeRequest {
            volume_id: "nonexistent-vol-123".to_string(),
        }))
        .await
        .unwrap();
}

// ── Node Service ────────────────────────────────────────────────────

#[tokio::test]
async fn node_get_info() {
    let node = HfCsiNode::new("test-node-1".to_string());
    let response = node
        .node_get_info(Request::new(NodeGetInfoRequest {}))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.node_id, "test-node-1");
    assert!(response.max_volumes_per_node > 0);
}

#[tokio::test]
async fn node_get_capabilities() {
    let node = HfCsiNode::new("test-node-2".to_string());
    let response = node
        .node_get_capabilities(Request::new(NodeGetCapabilitiesRequest {}))
        .await
        .unwrap()
        .into_inner();

    assert!(!response.capabilities.is_empty());
    let cap = &response.capabilities[0];
    assert!(cap.r#type.is_some(), "NodeServiceCapability must have a type");
}

#[tokio::test]
async fn node_stage_unstage_volume() {
    let temp = tempfile::tempdir().unwrap();
    let staging_path = temp.path().join("stage");
    let node = HfCsiNode::new("test-node-3".to_string());

    // Stage creates the staging directory.
    node.node_stage_volume(Request::new(NodeStageVolumeRequest {
        volume_id: "vol-stage-1".to_string(),
        staging_target_path: staging_path.to_string_lossy().to_string(),
        volume_context: HashMap::new(),
    }))
    .await
    .unwrap();

    assert!(staging_path.exists(), "staging_target_path must be created");

    // Unstage succeeds and removes the mount entry.
    node.node_unstage_volume(Request::new(NodeUnstageVolumeRequest {
        volume_id: "vol-stage-1".to_string(),
        staging_target_path: staging_path.to_string_lossy().to_string(),
    }))
    .await
    .unwrap();
}

#[tokio::test]
async fn node_publish_missing_binary_returns_error() {
    let temp = tempfile::tempdir().unwrap();
    let target_path = temp.path().join("publish");
    let node = HfCsiNode::new("test-node-4".to_string());

    // Publish fails when hf-mount-nfs binary is not in PATH (expected in test env).
    let err = node
        .node_publish_volume(Request::new(NodePublishVolumeRequest {
            volume_id: "vol-pub-1".to_string(),
            target_path: target_path.to_string_lossy().to_string(),
            staging_target_path: String::new(),
            volume_context: HashMap::from([("source".to_string(), "repo openai-community/gpt2".to_string())]),
            publish_context: HashMap::new(),
            volume_capability: Some(VolumeCapability {
                access_type: Some(volume_capability::AccessType::Mount(volume_capability::Mount {
                    fs_type: "nfs".to_string(),
                    mount_flags: vec![],
                })),
                access_mode: Some(AccessMode {
                    mode: access_mode::Mode::SingleNodeWriter as i32,
                }),
            }),
            readonly: false,
        }))
        .await
        .unwrap_err();

    // Error should be Internal because the binary is missing.
    assert_eq!(err.code(), tonic::Code::Internal);

    // Unpublish on a non-existent mount still succeeds (idempotent).
    node.node_unpublish_volume(Request::new(NodeUnpublishVolumeRequest {
        volume_id: "vol-pub-1".to_string(),
        target_path: target_path.to_string_lossy().to_string(),
    }))
    .await
    .unwrap();
}
