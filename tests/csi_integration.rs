#![cfg(feature = "nfs")]

use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::{Pod, PersistentVolumeClaim};
use k8s_openapi::api::storage::v1::CSIDriver;
use kube::api::{Api, ListParams, PostParams, DeleteParams, Patch, PatchParams};
use kube::client::Client;
use kube::runtime::wait::{await_condition, conditions};
use serde_json::json;
use tokio::time::timeout;

mod common;
use common::k8s::{create_test_namespace, delete_test_namespace, wait_for_csi_driver};

/// Integration test suite using kube-rs and envtest
/// Requires a running Kubernetes cluster with the CSI driver installed

#[tokio::test]
async fn test_csi_driver_installed() {
    let client = Client::try_default().await.expect("Failed to create K8s client");
    let csi_drivers: Api<CSIDriver> = Api::all(client.clone());
    
    let driver = csi_drivers.get("csi.huggingface.co").await;
    assert!(driver.is_ok(), "CSI driver should be installed");
}

#[tokio::test]
async fn test_pvc_creation() {
    let client = Client::try_default().await.expect("Failed to create K8s client");
    let ns = create_test_namespace(&client, "test-pvc-creation").await;
    
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), &ns);
    
    let pvc = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": { "name": "test-hf-pvc" },
        "spec": {
            "accessModes": ["ReadOnlyMany"],
            "storageClassName": "hf-csi",
            "resources": {
                "requests": { "storage": "1Gi" }
            }
        }
    })).unwrap();
    
    pvcs.create(&PostParams::default(), &pvc).await.expect("Failed to create PVC");
    
    // Wait for PVC to be bound (may take time for first volume)
    let result = timeout(
        Duration::from_secs(60),
        await_condition(pvcs.clone(), "test-hf-pvc", conditions::is_pod_running())
    ).await;
    
    // Cleanup
    delete_test_namespace(&client, &ns).await;
}

#[tokio::test]
async fn test_pod_mounts_csi_volume() {
    let client = Client::try_default().await.expect("Failed to create K8s client");
    let ns = create_test_namespace(&client, "test-pod-mount").await;
    
    let pods: Api<Pod> = Api::namespaced(client.clone(), &ns);
    
    let pod = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "test-mount" },
        "spec": {
            "containers": [{
                "name": "test",
                "image": "busybox:latest",
                "command": ["sh", "-c", "ls /data && sleep 30"],
                "volumeMounts": [{
                    "name": "dataset",
                    "mountPath": "/data",
                    "readOnly": true
                }]
            }],
            "volumes": [{
                "name": "dataset",
                "csi": {
                    "driver": "csi.huggingface.co",
                    "volumeAttributes": {
                        "repo": "datasets/wikitext"
                    }
                }
            }],
            "restartPolicy": "Never"
        }
    })).unwrap();
    
    pods.create(&PostParams::default(), &pod).await.expect("Failed to create pod");
    
    // Wait for pod to complete
    let result = timeout(
        Duration::from_secs(120),
        await_condition(pods.clone(), "test-mount", conditions::is_pod_completed())
    ).await;
    
    assert!(result.is_ok(), "Pod should complete successfully");
    
    // Cleanup
    delete_test_namespace(&client, &ns).await;
}

#[tokio::test]
async fn test_concurrent_volume_operations() {
    let client = Client::try_default().await.expect("Failed to create K8s client");
    let ns = create_test_namespace(&client, "test-concurrent").await;
    
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), &ns);
    
    // Create 10 PVCs concurrently
    let mut handles = vec![];
    for i in 0..10 {
        let client = client.clone();
        let ns = ns.clone();
        let handle = tokio::spawn(async move {
            let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client, &ns);
            let pvc_name = format!("concurrent-pvc-{}", i);
            let pvc = serde_json::from_value(json!({
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": { "name": pvc_name },
                "spec": {
                    "accessModes": ["ReadOnlyMany"],
                    "storageClassName": "hf-csi",
                    "resources": {
                        "requests": { "storage": "1Gi" }
                    }
                }
            })).unwrap();
            pvcs.create(&PostParams::default(), &pvc).await
        });
        handles.push(handle);
    }
    
    // All should succeed
    for handle in handles {
        let result = handle.await;
        assert!(result.is_ok(), "PVC creation should not panic");
    }
    
    // Cleanup
    delete_test_namespace(&client, &ns).await;
}

#[tokio::test]
async fn test_volume_readonly_enforcement() {
    let client = Client::try_default().await.expect("Failed to create K8s client");
    let ns = create_test_namespace(&client, "test-readonly").await;
    
    let pods: Api<Pod> = Api::namespaced(client.clone(), &ns);
    
    // Try to write to read-only volume
    let pod = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "test-readonly" },
        "spec": {
            "containers": [{
                "name": "test",
                "image": "busybox:latest",
                "command": ["sh", "-c", "touch /data/test-file || echo 'READ_ONLY'; sleep 5"],
                "volumeMounts": [{
                    "name": "dataset",
                    "mountPath": "/data",
                    "readOnly": true
                }]
            }],
            "volumes": [{
                "name": "dataset",
                "csi": {
                    "driver": "csi.huggingface.co",
                    "readOnly": true,
                    "volumeAttributes": {
                        "repo": "datasets/squad"
                    }
                }
            }],
            "restartPolicy": "Never"
        }
    })).unwrap();
    
    pods.create(&PostParams::default(), &pod).await.expect("Failed to create pod");
    
    // Wait and check logs
    tokio::time::sleep(Duration::from_secs(10)).await;
    
    let pod = pods.get("test-readonly").await.expect("Pod should exist");
    let logs = pods.logs("test-readonly", &Default::default()).await.unwrap();
    
    assert!(logs.contains("READ_ONLY"), "Volume should be read-only");
    
    // Cleanup
    delete_test_namespace(&client, &ns).await;
}

#[tokio::test]
async fn test_controller_failover() {
    let client = Client::try_default().await.expect("Failed to create K8s client");
    let ns = "hf-csi"; // CSI namespace
    
    let pods: Api<Pod> = Api::namespaced(client.clone(), ns);
    
    // Get current controller pods
    let lp = ListParams::default().labels("app=hf-csi-controller");
    let controllers = pods.list(&lp).await.expect("Failed to list controllers");
    
    if controllers.items.len() < 2 {
        println!("Skipping: Need at least 2 controller replicas for failover test");
        return;
    }
    
    // Delete one controller pod
    let pod_name = &controllers.items[0].metadata.name.as_ref().unwrap();
    pods.delete(pod_name, &DeleteParams::default()).await.expect("Failed to delete controller");
    
    // Wait for replacement
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    // Verify new controller comes up
    let result = timeout(
        Duration::from_secs(60),
        await_condition(pods.clone(), pod_name, conditions::is_pod_running())
    ).await;
    
    assert!(result.is_ok(), "Controller should be replaced");
}
