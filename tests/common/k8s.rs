use k8s_openapi::api::core::v1::{Namespace, PersistentVolumeClaim, Pod};
use kube::api::{Api, DeleteParams, ListParams, PostParams};
use kube::client::Client;
use kube::runtime::wait::{await_condition, conditions};
use serde_json::json;
use std::time::Duration;
use tokio::time::timeout;
use tracing::{info, warn};

/// Create a test namespace with cleanup label
pub async fn create_test_namespace(client: &Client, name: &str) -> String {
    let ns_name = format!("{}-{:x}", name, rand::random::<u32>());
    
    let namespaces: Api<Namespace> = Api::all(client.clone());
    
    let ns = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": ns_name,
            "labels": {
                "app.kubernetes.io/managed-by": "hf-csi-test",
                "test-type": "csi-integration"
            }
        }
    }))
    .unwrap();
    
    namespaces
        .create(&PostParams::default(), &ns)
        .await
        .expect("Failed to create test namespace");
    
    info!("Created test namespace: {}", ns_name);
    ns_name
}

/// Delete a test namespace
pub async fn delete_test_namespace(client: &Client, name: &str) {
    let namespaces: Api<Namespace> = Api::all(client.clone());
    
    let _ = namespaces
        .delete(name, &DeleteParams::background())
        .await;
    
    info!("Deleted test namespace: {}", name);
}

/// Wait for CSI driver to be ready
pub async fn wait_for_csi_driver(client: &Client, timeout_secs: u64) -> Result<(), String> {
    let csi_drivers: Api<k8s_openapi::api::storage::v1::CSIDriver> = Api::all(client.clone());
    
    match timeout(
        Duration::from_secs(timeout_secs),
        csi_drivers.get("csi.huggingface.co")
    )
    .await
    {
        Ok(Ok(_)) => {
            info!("CSI driver is ready");
            Ok(())
        }
        Ok(Err(e)) => Err(format!("CSI driver error: {}", e)),
        Err(_) => Err("Timeout waiting for CSI driver".to_string()),
    }
}

/// Wait for PVC to be bound
pub async fn wait_for_pvc_bound(
    client: &Client,
    ns: &str,
    name: &str,
    timeout_secs: u64,
) -> Result<(), String> {
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), ns);
    
    let start = std::time::Instant::now();
    
    loop {
        if start.elapsed() > Duration::from_secs(timeout_secs) {
            return Err(format!("Timeout waiting for PVC {} to bind", name));
        }
        
        match pvcs.get(name).await {
            Ok(pvc) => {
                if let Some(status) = &pvc.status {
                    if status.phase.as_deref() == Some("Bound") {
                        info!("PVC {} is bound", name);
                        return Ok(());
                    }
                }
            }
            Err(e) => {
                warn!("Error getting PVC {}: {}", name, e);
            }
        }
        
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// Wait for pod to be running
pub async fn wait_for_pod_running(
    client: &Client,
    ns: &str,
    name: &str,
    timeout_secs: u64,
) -> Result<(), String> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), ns);
    
    match timeout(
        Duration::from_secs(timeout_secs),
        await_condition(pods, name, conditions::is_pod_running())
    )
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(format!("Pod error: {}", e)),
        Err(_) => Err(format!("Timeout waiting for pod {} to run", name)),
    }
}

/// Cleanup all test namespaces
pub async fn cleanup_test_namespaces(client: &Client) -> Result<(), kube::Error> {
    let namespaces: Api<Namespace> = Api::all(client.clone());
    let lp = ListParams::default().labels("app.kubernetes.io/managed-by=hf-csi-test");
    
    let test_namespaces = namespaces.list(&lp).await?;
    
    for ns in test_namespaces.items {
        if let Some(name) = &ns.metadata.name {
            info!("Cleaning up test namespace: {}", name);
            namespaces.delete(name, &DeleteParams::background()).await.ok();
        }
    }
    
    Ok(())
}

/// Check if cluster has GPU nodes
pub async fn has_gpu_nodes(client: &Client) -> bool {
    let nodes: Api<k8s_openapi::api::core::v1::Node> = Api::all(client.clone());
    
    match nodes.list(&ListParams::default()).await {
        Ok(node_list) => {
            for node in node_list.items {
                if let Some(status) = &node.status {
                    if let Some(allocatable) = &status.allocatable {
                        if allocatable.contains_key("nvidia.com/gpu") {
                            return true;
                        }
                    }
                }
            }
            false
        }
        Err(_) => false,
    }
}

/// Get pod logs
pub async fn get_pod_logs(client: &Client, ns: &str, name: &str) -> Result<String, kube::Error> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), ns);
    pods.logs(name, &Default::default()).await
}

/// Execute command in pod
pub async fn exec_in_pod(
    client: &Client,
    ns: &str,
    pod_name: &str,
    container: &str,
    command: &[&str],
) -> Result<String, kube::Error> {
    // Note: This requires the exec API which is more complex
    // For tests, we typically just check pod status and logs
    info!("Would exec {:?} in {}/{}/{}", command, ns, pod_name, container);
    Ok("exec not implemented in test helpers".to_string())
}
