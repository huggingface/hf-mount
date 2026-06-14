//! Unit tests for CSI services.
//!
//! Tests VolumeRegistry, HfCsiIdentity, HfCsiController, and HfCsiNode
//! with mocked dependencies to avoid network calls and process spawning.

use std::collections::HashMap;
use std::sync::Arc;

use tonic::Code;
use tonic::Request;
use tonic::Status;

use hf_mount::csi::{
    HfCsiController, HfCsiIdentity, HfCsiNode, MockHubValidator, MockProcessSpawner, VolumeRegistry,
    v1::controller_server::Controller, v1::identity_server::Identity, v1::node_server::Node, v1::*,
};

// ── VolumeRegistry Unit Tests ─────────────────────────────────────────────

#[tokio::test]
async fn volume_registry_create_returns_unique_id() {
    let registry = VolumeRegistry::default();
    let id1 = registry.create("vol-1", 1024).await;
    let id2 = registry.create("vol-2", 2048).await;

    assert_ne!(id1, id2, "each create should return a unique id");
    assert!(id1.starts_with("hf-vol-"));
    assert!(id2.starts_with("hf-vol-"));
}

#[tokio::test]
async fn volume_registry_get_returns_capacity() {
    let registry = VolumeRegistry::default();
    let id = registry.create("test-vol", 4096).await;

    let capacity = registry.get(&id).await;
    assert_eq!(capacity, Some(4096));
}

#[tokio::test]
async fn volume_registry_get_missing_returns_none() {
    let registry = VolumeRegistry::default();
    let capacity = registry.get("hf-vol-nonexistent").await;
    assert_eq!(capacity, None);
}

#[tokio::test]
async fn volume_registry_delete_removes_volume() {
    let registry = VolumeRegistry::default();
    let id = registry.create("to-delete", 8192).await;

    assert!(registry.get(&id).await.is_some());
    registry.delete(&id).await;
    assert!(registry.get(&id).await.is_none());
}

#[tokio::test]
async fn volume_registry_concurrent_operations() {
    let registry = Arc::new(VolumeRegistry::default());
    let mut handles = vec![];

    for i in 0..10 {
        let reg = Arc::clone(&registry);
        handles.push(tokio::spawn(async move {
            reg.create(&format!("concurrent-{i}"), i as u64 * 1024).await
        }));
    }

    let ids = futures::future::join_all(handles).await;
    let ids: Vec<String> = ids.into_iter().map(|r| r.unwrap()).collect();

    // All IDs should be unique
    let unique: std::collections::HashSet<_> = ids.iter().collect();
    assert_eq!(unique.len(), ids.len());

    // All volumes should be retrievable
    for id in &ids {
        assert!(registry.get(id).await.is_some());
    }
}

// ── HfCsiIdentity Unit Tests ───────────────────────────────────────────────

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

// ── HfCsiController Unit Tests ────────────────────────────────────────────

#[tokio::test]
async fn controller_create_volume_with_valid_source() {
    let mock_validator = MockHubValidator::new();
    let controller = HfCsiController::with_validator(Arc::new(mock_validator));

    let response = controller
        .create_volume(Request::new(CreateVolumeRequest {
            name: "test-vol".to_string(),
            parameters: HashMap::from([
                ("sourceType".to_string(), "repo".to_string()),
                ("sourceId".to_string(), "openai-community/gpt2".to_string()),
            ]),
            required_bytes: 1024 * 1024 * 1024,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(response.volume.is_some());
    assert_eq!(response.volume.unwrap().capacity_bytes, 1024 * 1024 * 1024);
}

#[tokio::test]
async fn controller_create_volume_with_not_found_error() {
    let mock_validator = MockHubValidator::new().with_error(Status::not_found(
        "repo openai-community/nonexistent not found on HF Hub",
    ));
    let controller = HfCsiController::with_validator(Arc::new(mock_validator));

    let err = controller
        .create_volume(Request::new(CreateVolumeRequest {
            name: "test-vol".to_string(),
            parameters: HashMap::from([
                ("sourceType".to_string(), "repo".to_string()),
                ("sourceId".to_string(), "openai-community/nonexistent".to_string()),
            ]),
            required_bytes: 1024 * 1024 * 1024,
        }))
        .await
        .unwrap_err();

    assert_eq!(err.code(), Code::NotFound);
}

#[tokio::test]
async fn controller_create_volume_with_unauthenticated_error() {
    let mock_validator = MockHubValidator::new().with_error(Status::unauthenticated("HF Hub authentication failed"));
    let controller = HfCsiController::with_validator(Arc::new(mock_validator));

    let err = controller
        .create_volume(Request::new(CreateVolumeRequest {
            name: "test-vol".to_string(),
            parameters: HashMap::from([
                ("sourceType".to_string(), "repo".to_string()),
                ("sourceId".to_string(), "private/repo".to_string()),
            ]),
            required_bytes: 1024 * 1024 * 1024,
        }))
        .await
        .unwrap_err();

    assert_eq!(err.code(), Code::Unauthenticated);
}

#[tokio::test]
async fn controller_create_volume_defaults_to_repo_type() {
    let mock_validator = MockHubValidator::new();
    let controller = HfCsiController::with_validator(Arc::new(mock_validator));

    let response = controller
        .create_volume(Request::new(CreateVolumeRequest {
            name: "test-vol".to_string(),
            parameters: HashMap::from([("sourceId".to_string(), "openai-community/gpt2".to_string())]),
            required_bytes: 1024 * 1024 * 1024,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(response.volume.is_some());
}

#[tokio::test]
async fn controller_create_volume_defaults_source_id_to_name() {
    let mock_validator = MockHubValidator::new();
    let controller = HfCsiController::with_validator(Arc::new(mock_validator));

    let response = controller
        .create_volume(Request::new(CreateVolumeRequest {
            name: "openai-community/gpt2".to_string(),
            parameters: HashMap::new(),
            required_bytes: 1024 * 1024 * 1024,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(response.volume.is_some());
}

#[tokio::test]
async fn controller_delete_volume_idempotent() {
    let mock_validator = MockHubValidator::new();
    let controller = HfCsiController::with_validator(Arc::new(mock_validator));

    // DeleteVolume on an unknown volume_id is a no-op (idempotent).
    controller
        .delete_volume(Request::new(DeleteVolumeRequest {
            volume_id: "nonexistent-vol-123".to_string(),
        }))
        .await
        .unwrap();
}

#[tokio::test]
async fn controller_delete_volume_after_create() {
    let mock_validator = MockHubValidator::new();
    let controller = HfCsiController::with_validator(Arc::new(mock_validator));

    // Create a volume
    let create_response = controller
        .create_volume(Request::new(CreateVolumeRequest {
            name: "test-vol".to_string(),
            parameters: HashMap::from([
                ("sourceType".to_string(), "repo".to_string()),
                ("sourceId".to_string(), "openai-community/gpt2".to_string()),
            ]),
            required_bytes: 1024 * 1024 * 1024,
        }))
        .await
        .unwrap()
        .into_inner();

    let volume_id = create_response.volume.unwrap().volume_id;

    // Delete the volume
    controller
        .delete_volume(Request::new(DeleteVolumeRequest {
            volume_id: volume_id.clone(),
        }))
        .await
        .unwrap();

    // Delete again (idempotent)
    controller
        .delete_volume(Request::new(DeleteVolumeRequest { volume_id }))
        .await
        .unwrap();
}

#[tokio::test]
async fn controller_get_capabilities() {
    let mock_validator = MockHubValidator::new();
    let controller = HfCsiController::with_validator(Arc::new(mock_validator));

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
async fn controller_concurrent_volume_operations() {
    let mock_validator = MockHubValidator::new();
    let controller = HfCsiController::with_validator(Arc::new(mock_validator));

    // Create multiple volumes concurrently
    let mut handles = vec![];
    for i in 0..10 {
        let controller = controller.clone();
        let handle = tokio::spawn(async move {
            controller
                .create_volume(Request::new(CreateVolumeRequest {
                    name: format!("test-vol-{}", i),
                    parameters: HashMap::from([
                        ("sourceType".to_string(), "repo".to_string()),
                        ("sourceId".to_string(), format!("repo-{}/model", i)),
                    ]),
                    required_bytes: 1024 * 1024 * 1024,
                }))
                .await
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap().unwrap();
    }
}

// ── HfCsiNode Unit Tests ──────────────────────────────────────────────────

#[tokio::test]
async fn node_get_info() {
    let mock_spawner = MockProcessSpawner::new();
    let node = HfCsiNode::with_spawner("test-node-1".to_string(), Arc::new(mock_spawner));

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
    let mock_spawner = MockProcessSpawner::new();
    let node = HfCsiNode::with_spawner("test-node-2".to_string(), Arc::new(mock_spawner));

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
    let mock_spawner = MockProcessSpawner::new();
    let node = HfCsiNode::with_spawner("test-node-3".to_string(), Arc::new(mock_spawner));

    let temp = tempfile::tempdir().unwrap();
    let staging_path = temp.path().join("stage");

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
async fn node_publish_volume_with_mock_spawner() {
    let mock_spawner = MockProcessSpawner::new();
    let node = HfCsiNode::with_spawner("test-node-4".to_string(), Arc::new(mock_spawner.clone()));

    let temp = tempfile::tempdir().unwrap();
    let target_path = temp.path().join("publish");

    #[cfg(unix)]
    let binary_name = "hf-mount-nfs";
    #[cfg(not(unix))]
    let binary_name = "hf-mount-nfs.exe";

    // Publish should succeed with mock spawner
    node.node_publish_volume(Request::new(NodePublishVolumeRequest {
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
    .unwrap();

    // Verify the spawner was called with correct arguments
    let spawned = mock_spawner.get_spawned_commands().await;
    assert!(!spawned.is_empty());
    assert_eq!(spawned[0].0, binary_name);
    assert!(spawned[0].1.contains(&"repo openai-community/gpt2".to_string()));
}

#[tokio::test]
async fn node_publish_volume_with_spawner_error() {
    let mock_spawner =
        MockProcessSpawner::new().with_error(Status::internal("spawn hf-mount-nfs: No such file or directory"));
    let node = HfCsiNode::with_spawner("test-node-5".to_string(), Arc::new(mock_spawner));

    let temp = tempfile::tempdir().unwrap();
    let target_path = temp.path().join("publish");

    // Publish should fail when spawner returns error
    let err = node
        .node_publish_volume(Request::new(NodePublishVolumeRequest {
            volume_id: "vol-pub-2".to_string(),
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

    assert_eq!(err.code(), Code::Internal);
}

#[tokio::test]
async fn node_unpublish_volume_idempotent() {
    let mock_spawner = MockProcessSpawner::new();
    let node = HfCsiNode::with_spawner("test-node-6".to_string(), Arc::new(mock_spawner));

    let temp = tempfile::tempdir().unwrap();
    let target_path = temp.path().join("publish");

    // Unpublish on a non-existent mount still succeeds (idempotent).
    node.node_unpublish_volume(Request::new(NodeUnpublishVolumeRequest {
        volume_id: "vol-pub-3".to_string(),
        target_path: target_path.to_string_lossy().to_string(),
    }))
    .await
    .unwrap();
}

#[tokio::test]
async fn node_unpublish_volume_after_publish() {
    let mock_spawner = MockProcessSpawner::new();
    let node = HfCsiNode::with_spawner("test-node-7".to_string(), Arc::new(mock_spawner));

    let temp = tempfile::tempdir().unwrap();
    let target_path = temp.path().join("publish");

    // Publish first
    node.node_publish_volume(Request::new(NodePublishVolumeRequest {
        volume_id: "vol-pub-4".to_string(),
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
    .unwrap();

    // Unpublish should succeed
    node.node_unpublish_volume(Request::new(NodeUnpublishVolumeRequest {
        volume_id: "vol-pub-4".to_string(),
        target_path: target_path.to_string_lossy().to_string(),
    }))
    .await
    .unwrap();
}

#[tokio::test]
async fn node_publish_readonly_volume() {
    let mock_spawner = MockProcessSpawner::new();
    let node = HfCsiNode::with_spawner("test-node-8".to_string(), Arc::new(mock_spawner.clone()));

    let temp = tempfile::tempdir().unwrap();
    let target_path = temp.path().join("publish");

    // Publish with readonly flag
    node.node_publish_volume(Request::new(NodePublishVolumeRequest {
        volume_id: "vol-pub-5".to_string(),
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
        readonly: true,
    }))
    .await
    .unwrap();

    // Verify the spawner was called with --read-only flag
    let spawned = mock_spawner.get_spawned_commands().await;
    assert!(!spawned.is_empty());
    assert!(spawned[0].1.contains(&"--read-only".to_string()));
}
