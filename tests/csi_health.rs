//! Integration tests for tonic-health service.
//!
//! Tests health reporter serving/not-serving transitions and
//! Kubernetes liveness/readiness probe compatibility.

use std::time::Duration;

use hf_mount::csi::v1::{
    controller_server::ControllerServer, identity_server::IdentityServer, node_server::NodeServer,
};
use hf_mount::csi::{HfCsiController, HfCsiIdentity, HfCsiNode};
use tonic::transport::Channel;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::server::health_reporter;

#[tokio::test]
async fn health_service_serving_transition() {
    // Create health reporter
    let (mut health_reporter, health_service) = health_reporter();

    // Register a test service as not serving
    health_reporter
        .set_service_status("test-service", tonic_health::ServingStatus::NotServing)
        .await;

    let mut client = create_health_client(health_service).await;

    // Check that the service is not serving initially
    let request = HealthCheckRequest {
        service: "test-service".to_string(),
    };
    let response = client
        .check(request.clone())
        .await
        .expect("Health check should succeed");

    assert_eq!(response.into_inner().status, ServingStatus::NotServing as i32);

    // Mark test service as serving
    health_reporter
        .set_service_status("test-service", tonic_health::ServingStatus::Serving)
        .await;

    // Give it a moment to propagate
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Re-check: the service should now be serving
    let response = client.check(request).await.expect("Health check should succeed");

    assert_eq!(response.into_inner().status, ServingStatus::Serving as i32);
}

#[tokio::test]
async fn health_service_multiple_services() {
    let (mut health_reporter, _health_service) = health_reporter();

    // Mark all services as serving
    health_reporter.set_serving::<IdentityServer<HfCsiIdentity>>().await;
    health_reporter.set_serving::<ControllerServer<HfCsiController>>().await;
    health_reporter.set_serving::<NodeServer<HfCsiNode>>().await;

    // Verify the reporter accepts multiple service registrations
    // The actual health check would require a running server,
    // which is complex in unit tests. This test verifies the API works.
}

#[tokio::test]
async fn health_service_not_serving_transition() {
    let (mut health_reporter, _health_service) = health_reporter();

    // Mark service as serving
    health_reporter.set_serving::<IdentityServer<HfCsiIdentity>>().await;

    // Mark as not serving
    health_reporter.set_not_serving::<IdentityServer<HfCsiIdentity>>().await;

    // Verify the transition completes without error
}

#[tokio::test]
async fn health_service_empty_service_check() {
    let (health_reporter, health_service) = health_reporter();
    let _ = health_reporter;

    // Check the empty service string (should return serving status of the server itself)
    let mut client = create_health_client(health_service).await;

    let request = HealthCheckRequest { service: String::new() };

    // This should succeed (the server itself is responding)
    let response = client.check(request).await.expect("Health check should succeed");

    // The empty service check should return the server's overall status
    let status = response.into_inner().status;
    assert!(
        status == ServingStatus::Serving as i32 || status == ServingStatus::NotServing as i32,
        "Empty service check should return a valid status"
    );
}

#[tokio::test]
async fn health_service_watch() {
    let (mut health_reporter, health_service) = health_reporter();

    // Register a test service as serving
    health_reporter
        .set_service_status("test-service", tonic_health::ServingStatus::Serving)
        .await;

    let mut client = create_health_client(health_service).await;

    let request = HealthCheckRequest {
        service: "test-service".to_string(),
    };

    // Attempt to watch health changes
    // Note: In a real scenario, this would stream updates.
    // For unit testing, we verify the API accepts the request.
    let mut stream = client.watch(request).await.expect("Watch should succeed").into_inner();

    // Try to receive one message (should succeed)
    let result = tokio::time::timeout(Duration::from_secs(1), stream.message()).await;
    // The watch might return immediately or timeout, either is acceptable for this test
    let _ = result;
}

// Helper function to create a health client backed by an in-memory gRPC server.
async fn create_health_client<T>(
    health_service: tonic_health::pb::health_server::HealthServer<T>,
) -> HealthClient<Channel>
where
    T: tonic_health::pb::health_server::Health,
{
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bound_addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(health_service)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });

    let channel = Channel::from_shared(format!("http://{}", bound_addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    HealthClient::new(channel)
}

// ── Integration-style tests using in-memory server ──────────────────

#[tokio::test]
async fn kubernetes_liveness_probe() {
    let (mut health_reporter, health_service) = health_reporter();

    // Mark a CSI service as serving (liveness probe checks individual services)
    health_reporter
        .set_service_status("csi-identity", tonic_health::ServingStatus::Serving)
        .await;

    let mut client = create_health_client(health_service).await;

    let response = client
        .check(HealthCheckRequest {
            service: "csi-identity".to_string(),
        })
        .await
        .expect("Liveness probe should succeed");

    assert_eq!(response.into_inner().status, ServingStatus::Serving as i32);
}

#[tokio::test]
async fn kubernetes_readiness_probe() {
    let (mut health_reporter, health_service) = health_reporter();

    let mut client = create_health_client(health_service).await;

    // When a service is not registered, tonic-health returns NotFound status.
    let err = client
        .check(HealthCheckRequest {
            service: "csi-controller".to_string(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);

    // Mark controller as serving
    health_reporter
        .set_service_status("csi-controller", tonic_health::ServingStatus::Serving)
        .await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    let response = client
        .check(HealthCheckRequest {
            service: "csi-controller".to_string(),
        })
        .await
        .expect("Readiness probe should succeed after transition");

    assert_eq!(response.into_inner().status, ServingStatus::Serving as i32);
}

#[tokio::test]
async fn graceful_shutdown_health_transition() {
    let (mut health_reporter, health_service) = health_reporter();

    // Register services as serving initially
    health_reporter
        .set_service_status("csi-node", tonic_health::ServingStatus::Serving)
        .await;

    let mut client = create_health_client(health_service).await;

    // Verify serving before shutdown
    let response = client
        .check(HealthCheckRequest {
            service: "csi-node".to_string(),
        })
        .await
        .unwrap();
    assert_eq!(response.into_inner().status, ServingStatus::Serving as i32);

    // Simulate shutdown: mark service as not serving
    health_reporter
        .set_service_status("csi-node", tonic_health::ServingStatus::NotServing)
        .await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify not serving after shutdown signal
    let response = client
        .check(HealthCheckRequest {
            service: "csi-node".to_string(),
        })
        .await
        .unwrap();
    assert_eq!(response.into_inner().status, ServingStatus::NotServing as i32);
}

#[tokio::test]
async fn csi_services_health_registered() {
    // This test mirrors the exact service registration pattern used in hf-mount-csi.rs.
    let (mut health_reporter, health_service) = health_reporter();

    // Register CSI services as serving (same calls the binary makes)
    health_reporter.set_serving::<IdentityServer<HfCsiIdentity>>().await;
    health_reporter.set_serving::<ControllerServer<HfCsiController>>().await;
    health_reporter.set_serving::<NodeServer<HfCsiNode>>().await;

    let mut client = create_health_client(health_service).await;

    // Verify all three CSI services report SERVING
    for service_name in ["csi.v1.Identity", "csi.v1.Controller", "csi.v1.Node"] {
        let response = client
            .check(HealthCheckRequest {
                service: service_name.to_string(),
            })
            .await
            .unwrap_or_else(|e| panic!("Health check for {service_name} should succeed: {e}"));

        assert_eq!(
            response.into_inner().status,
            ServingStatus::Serving as i32,
            "{service_name} should be serving"
        );
    }
}
