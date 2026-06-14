//! Bidirectional serialization tests for CSI protobuf types.
//!
//! Ensures that messages containing `repeated` fields and `oneof` types
//! round-trip correctly through prost/tonic encoding and decoding.

use bytes::BytesMut;
use hf_mount::csi::v1::*;
use prost::Message;

fn roundtrip<T: Message + Default>(msg: &T) -> T {
    let mut buf = BytesMut::new();
    msg.encode(&mut buf).expect("encode");
    T::decode(buf).expect("decode")
}

#[test]
fn plugin_capability_service_roundtrip() {
    let original = PluginCapability {
        r#type: Some(plugin_capability::Type::Service(plugin_capability::Service {
            r#type: plugin_capability::service::Type::ControllerService as i32,
        })),
    };
    let decoded = roundtrip(&original);
    assert_eq!(decoded.r#type, original.r#type);
}

#[test]
fn controller_service_capability_rpc_roundtrip() {
    let original = ControllerServiceCapability {
        r#type: Some(controller_service_capability::Type::Rpc(
            controller_service_capability::Rpc {
                r#type: controller_service_capability::rpc::Type::CreateDeleteVolume as i32,
            },
        )),
    };
    let decoded = roundtrip(&original);
    assert_eq!(decoded.r#type, original.r#type);
}

#[test]
fn node_service_capability_rpc_roundtrip() {
    let original = NodeServiceCapability {
        r#type: Some(node_service_capability::Type::Rpc(node_service_capability::Rpc {
            r#type: node_service_capability::rpc::Type::StageUnstageVolume as i32,
        })),
    };
    let decoded = roundtrip(&original);
    assert_eq!(decoded.r#type, original.r#type);
}

#[test]
fn volume_capability_mount_roundtrip() {
    let original = VolumeCapability {
        access_type: Some(volume_capability::AccessType::Mount(volume_capability::Mount {
            fs_type: "nfs".to_string(),
            mount_flags: vec!["vers=4".to_string(), "nolock".to_string()],
        })),
        access_mode: Some(AccessMode {
            mode: access_mode::Mode::SingleNodeWriter as i32,
        }),
    };
    let decoded = roundtrip(&original);
    assert_eq!(decoded.access_type, original.access_type);
    assert_eq!(
        decoded.access_mode.as_ref().unwrap().mode,
        access_mode::Mode::SingleNodeWriter as i32
    );
}

#[test]
fn get_plugin_capabilities_response_roundtrip() {
    let original = GetPluginCapabilitiesResponse {
        capabilities: vec![
            PluginCapability {
                r#type: Some(plugin_capability::Type::Service(plugin_capability::Service {
                    r#type: plugin_capability::service::Type::ControllerService as i32,
                })),
            },
            PluginCapability {
                r#type: Some(plugin_capability::Type::Service(plugin_capability::Service {
                    r#type: plugin_capability::service::Type::VolumeAccessibilityConstraints as i32,
                })),
            },
        ],
    };
    let decoded = roundtrip(&original);
    assert_eq!(decoded.capabilities.len(), 2);
    assert_eq!(decoded.capabilities[0].r#type, original.capabilities[0].r#type);
    assert_eq!(decoded.capabilities[1].r#type, original.capabilities[1].r#type);
}

#[test]
fn controller_get_capabilities_response_roundtrip() {
    let original = ControllerGetCapabilitiesResponse {
        capabilities: vec![ControllerServiceCapability {
            r#type: Some(controller_service_capability::Type::Rpc(
                controller_service_capability::Rpc {
                    r#type: controller_service_capability::rpc::Type::CreateDeleteVolume as i32,
                },
            )),
        }],
    };
    let decoded = roundtrip(&original);
    assert_eq!(decoded.capabilities.len(), 1);
    assert_eq!(decoded.capabilities[0].r#type, original.capabilities[0].r#type);
}

#[test]
fn node_get_capabilities_response_roundtrip() {
    let original = NodeGetCapabilitiesResponse {
        capabilities: vec![
            NodeServiceCapability {
                r#type: Some(node_service_capability::Type::Rpc(node_service_capability::Rpc {
                    r#type: node_service_capability::rpc::Type::StageUnstageVolume as i32,
                })),
            },
            NodeServiceCapability {
                r#type: Some(node_service_capability::Type::Rpc(node_service_capability::Rpc {
                    r#type: node_service_capability::rpc::Type::GetVolumeStats as i32,
                })),
            },
        ],
    };
    let decoded = roundtrip(&original);
    assert_eq!(decoded.capabilities.len(), 2);
    assert_eq!(decoded.capabilities[0].r#type, original.capabilities[0].r#type);
    assert_eq!(decoded.capabilities[1].r#type, original.capabilities[1].r#type);
}

#[test]
fn create_volume_response_roundtrip() {
    let original = CreateVolumeResponse {
        volume: Some(Volume {
            volume_id: "hf-vol-test-123".to_string(),
            capacity_bytes: 1024 * 1024 * 1024, // 1 GiB
        }),
    };
    let decoded = roundtrip(&original);
    assert_eq!(decoded.volume.as_ref().unwrap().volume_id, "hf-vol-test-123");
    assert_eq!(decoded.volume.as_ref().unwrap().capacity_bytes, 1_073_741_824);
}

#[test]
fn node_publish_volume_request_roundtrip() {
    let original = NodePublishVolumeRequest {
        volume_id: "vol-42".to_string(),
        target_path: "/mnt/data".to_string(),
        staging_target_path: "/var/lib/kubelet/staging".to_string(),
        volume_context: std::collections::HashMap::from([(
            "source".to_string(),
            "repo openai-community/gpt2".to_string(),
        )]),
        publish_context: std::collections::HashMap::new(),
        volume_capability: Some(VolumeCapability {
            access_type: Some(volume_capability::AccessType::Mount(volume_capability::Mount {
                fs_type: "nfs".to_string(),
                mount_flags: vec!["vers=4".to_string()],
            })),
            access_mode: Some(AccessMode {
                mode: access_mode::Mode::SingleNodeWriter as i32,
            }),
        }),
        readonly: true,
    };
    let decoded = roundtrip(&original);
    assert_eq!(decoded.volume_id, "vol-42");
    assert!(decoded.readonly);
    assert!(decoded.volume_capability.is_some());
}
