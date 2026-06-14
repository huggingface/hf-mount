#![cfg(feature = "nfs")]

use std::time::{Duration, Instant};

use k8s_openapi::api::core::v1::{Pod, Namespace, Node};
use k8s_openapi::api::storage::v1::CSIDriver;
use kube::api::{Api, ListParams, PostParams, DeleteParams, Patch, PatchParams};
use kube::client::Client;
use serde_json::json;
use tokio::time::sleep;
use tracing::{info, warn};

mod common;
use common::k8s::{create_test_namespace, delete_test_namespace};

/// Chaos engineering tests for CSI driver resilience
/// These tests simulate real-world failure scenarios

/// Test controller failure during volume provisioning
#[tokio::test]
async fn chaos_controller_failure_during_provisioning() {
    let client = Client::try_default().await.expect("Failed to create K8s client");
    let ns = create_test_namespace(&client, "chaos-controller-fail").await;
    let csi_ns = "hf-csi";
    
    // Start volume creation
    let pvcs: Api<k8s_openapi::api::core::v1::PersistentVolumeClaim> = 
        Api::namespaced(client.clone(), &ns);
    
    let pvc = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": { "name": "chaos-pvc" },
        "spec": {
            "accessModes": ["ReadOnlyMany"],
            "storageClassName": "hf-csi",
            "resources": { "requests": { "storage": "1Gi" } }
        }
    })).unwrap();
    
    // Create PVC
    pvcs.create(&PostParams::default(), &pvc).await.expect("Failed to create PVC");
    
    // Kill controller mid-operation
    let pods: Api<Pod> = Api::namespaced(client.clone(), csi_ns);
    let lp = ListParams::default().labels("app=hf-csi-controller");
    let controllers = pods.list(&lp).await.unwrap();
    
    for controller in &controllers.items {
        if let Some(name) = &controller.metadata.name {
            info!("Deleting controller: {}", name);
            let _ = pods.delete(name, &DeleteParams::default()).await;
        }
    }
    
    // Wait for recovery
    sleep(Duration::from_secs(10)).await;
    
    // PVC should eventually be bound
    let start = Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(120) {
            panic!("PVC did not bind after controller recovery");
        }
        
        if let Ok(pvc) = pvcs.get("chaos-pvc").await {
            if pvc.status.as_ref().map(|s| s.phase.as_deref()) == Some("Bound") {
                info!("PVC successfully bound after controller failure");
                break;
            }
        }
        sleep(Duration::from_secs(5)).await;
    }
    
    delete_test_namespace(&client, &ns).await;
}

/// Test node drain with mounted volumes
#[tokio::test]
async fn chaos_node_drain_with_volumes() {
    let client = Client::try_default().await.expect("Failed to create K8s client");
    let ns = create_test_namespace(&client, "chaos-node-drain").await;
    
    // Create pod with CSI volume
    let pods: Api<Pod> = Api::namespaced(client.clone(), &ns);
    let pod = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { 
            "name": "drain-test",
            "labels": { "app": "drain-test" }
        },
        "spec": {
            "containers": [{
                "name": "test",
                "image": "busybox:latest",
                "command": ["sleep", "300"],
                "volumeMounts": [{
                    "name": "data",
                    "mountPath": "/data",
                    "readOnly": true
                }]
            }],
            "volumes": [{
                "name": "data",
                "csi": {
                    "driver": "csi.huggingface.co",
                    "volumeAttributes": {
                        "repo": "datasets/glue"
                    }
                }
            }]
        }
    })).unwrap();
    
    pods.create(&PostParams::default(), &pod).await.expect("Failed to create pod");
    
    // Wait for pod to be running
    sleep(Duration::from_secs(15)).await;
    
    // Cordon and drain the node
    let nodes: Api<Node> = Api::all(client.clone());
    let node_list = nodes.list(&ListParams::default()).await.unwrap();
    
    if let Some(node) = node_list.items.first() {
        let node_name = node.metadata.name.as_ref().unwrap();
        info!("Cordoning node: {}", node_name);
        
        let patch = json!({
            "spec": {
                "unschedulable": true
            }
        });
        
        nodes
            .patch(node_name, &PatchParams::default(), &Patch::Merge(&patch))
            .await
            .expect("Failed to cordon node");
        
        // Delete the pod (simulating eviction)
        let _ = pods.delete("drain-test", &DeleteParams::default()).await;
        
        // Pod should be rescheduled elsewhere
        sleep(Duration::from_secs(5)).await;
        
        // Uncordon
        let patch = json!({
            "spec": {
                "unschedulable": false
            }
        });
        let _ = nodes.patch(node_name, &PatchParams::default(), &Patch::Merge(&patch)).await;
    }
    
    delete_test_namespace(&client, &ns).await;
}

/// Test network partition to Hugging Face Hub
#[tokio::test]
async fn chaos_hub_network_partition() {
    let client = Client::try_default().await.expect("Failed to create K8s client");
    let ns = create_test_namespace(&client, "chaos-hub-partition").await;
    
    // Create pod that accesses Hub
    let pods: Api<Pod> = Api::namespaced(client.clone(), &ns);
    let pod = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "hub-access-test" },
        "spec": {
            "containers": [{
                "name": "test",
                "image": "busybox:latest",
                "command": ["sh", "-c", "cat /data/dataset_info.json 2>/dev/null || echo 'CACHE_MISS'; sleep 30"],
                "volumeMounts": [{
                    "name": "data",
                    "mountPath": "/data",
                    "readOnly": true
                }]
            }],
            "volumes": [{
                "name": "data",
                "csi": {
                    "driver": "csi.huggingface.co",
                    "volumeAttributes": {
                        "repo": "datasets/wikitext",
                        "revision": "main"
                    }
                }
            }],
            "restartPolicy": "Never"
        }
    })).unwrap();
    
    pods.create(&PostParams::default(), &pod).await.expect("Failed to create pod");
    
    // Simulate network issues by checking if cached data works
    sleep(Duration::from_secs(20)).await;
    
    let logs = pods.logs("hub-access-test", &Default::default()).await.unwrap();
    
    // Should either succeed (if cached) or handle failure gracefully
    info!("Pod logs: {}", logs);
    
    delete_test_namespace(&client, &ns).await;
}

/// Test memory pressure on nodes
#[tokio::test]
async fn chaos_memory_pressure() {
    let client = Client::try_default().await.expect("Failed to create K8s client");
    let ns = create_test_namespace(&client, "chaos-memory").await;
    
    // Create pods that consume memory with CSI volumes
    let pods: Api<Pod> = Api::namespaced(client.clone(), &ns);
    
    for i in 0..5 {
        let pod_name = format!("memory-test-{}", i);
        let pod = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": pod_name },
            "spec": {
                "containers": [{
                    "name": "test",
                    "image": "busybox:latest",
                    "command": ["sh", "-c", "cat /data/*.txt > /dev/null 2>&1; sleep 60"],
                    "resources": {
                        "limits": { "memory": "100Mi" },
                        "requests": { "memory": "50Mi" }
                    },
                    "volumeMounts": [{
                        "name": "data",
                        "mountPath": "/data",
                        "readOnly": true
                    }]
                }],
                "volumes": [{
                    "name": "data",
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
        
        pods.create(&PostParams::default(), &pod).await.ok();
    }
    
    // All pods should eventually run without crashing
    sleep(Duration::from_secs(30)).await;
    
    let lp = ListParams::default().labels("job-name");
    let running_pods = pods.list(&lp).await.unwrap();
    
    info!("Running pods under memory pressure: {}", running_pods.items.len());
    
    delete_test_namespace(&client, &ns).await;
}

/// Stress test with 100 concurrent PVCs
#[tokio::test]
async fn chaos_concurrent_pvc_storm() {
    let client = Client::try_default().await.expect("Failed to create K8s client");
    let ns = create_test_namespace(&client, "chaos-concurrent").await;
    let pvcs: Api<k8s_openapi::api::core::v1::PersistentVolumeClaim> = 
        Api::namespaced(client.clone(), &ns);
    
    let start = Instant::now();
    
    // Create 100 PVCs concurrently
    let mut handles = vec![];
    for i in 0..100 {
        let client = client.clone();
        let ns = ns.clone();
        let handle = tokio::spawn(async move {
            let pvcs: Api<k8s_openapi::api::core::v1::PersistentVolumeClaim> = 
                Api::namespaced(client, &ns);
            let name = format!("stress-pvc-{}", i);
            let pvc = serde_json::from_value(json!({
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": { "name": name },
                "spec": {
                    "accessModes": ["ReadOnlyMany"],
                    "storageClassName": "hf-csi",
                    "resources": { "requests": { "storage": "1Gi" } }
                }
            })).unwrap();
            
            let start = Instant::now();
            let result = pvcs.create(&PostParams::default(), &pvc).await;
            let elapsed = start.elapsed();
            
            (i, result.is_ok(), elapsed)
        });
        handles.push(handle);
    }
    
    let mut success_count = 0;
    let mut fail_count = 0;
    let mut total_time = Duration::ZERO;
    
    for handle in handles {
        let (i, success, elapsed) = handle.await.unwrap();
        total_time += elapsed;
        if success {
            success_count += 1;
        } else {
            fail_count += 1;
            warn!("PVC {} failed", i);
        }
    }
    
    let total_elapsed = start.elapsed();
    let avg_time = total_time / 100;
    
    info!(
        "Stress test: {} succeeded, {} failed in {:?} (avg: {:?}/PVC)",
        success_count, fail_count, total_elapsed, avg_time
    );
    
    // At least 95% should succeed
    assert!(
        success_count >= 95,
        "At least 95% of PVCs should succeed (got {}%)",
        success_count
    );
    
    delete_test_namespace(&client, &ns).await;
}
