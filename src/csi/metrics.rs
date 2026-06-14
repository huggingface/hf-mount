//! Prometheus metrics for the HF CSI driver.
//!
//! This module provides metrics collection for CSI operations including:
//! - Volume operation counts (create, delete, mount, unmount)
//! - Operation latency histograms
//! - Cache utilization
//! - Hugging Face Hub API requests

use axum::{
    body::Body,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use prometheus::{
    histogram_opts, opts, register_counter_vec, register_gauge, register_histogram_vec,
    register_int_gauge, CounterVec, Encoder, Gauge, HistogramVec, IntGauge, TextEncoder,
};
use std::sync::OnceLock;
use std::time::Instant;
use tracing::{info, warn};

/// Global metrics instance
static METRICS: OnceLock<CsiMetrics> = OnceLock::new();

/// CSI metrics collector
pub struct CsiMetrics {
    // Volume operations
    pub volume_operations_total: CounterVec,
    pub volume_operations_failed_total: CounterVec,
    pub volume_operations_duration_seconds: HistogramVec,
    pub volumes_total: IntGauge,

    // Cache metrics
    pub cache_size_bytes: Gauge,
    pub cache_limit_bytes: Gauge,
    pub cache_hits_total: CounterVec,
    pub cache_misses_total: CounterVec,

    // Hugging Face Hub API
    pub hub_requests_total: CounterVec,
    pub hub_request_duration_seconds: HistogramVec,

    // Active mounts
    pub active_mounts_total: IntGauge,
}

impl CsiMetrics {
    /// Initialize and register all metrics
    pub fn new() -> anyhow::Result<Self> {
        let volume_operations_total = register_counter_vec!(
            opts!(
                "hf_csi_volume_operations_total",
                "Total number of volume operations"
            ),
            &["operation", "status"]
        )?;

        let volume_operations_failed_total = register_counter_vec!(
            opts!(
                "hf_csi_volume_operations_failed_total",
                "Total number of failed volume operations"
            ),
            &["operation"]
        )?;

        let volume_operations_duration_seconds = register_histogram_vec!(
            histogram_opts!(
                "hf_csi_volume_operations_duration_seconds",
                "Volume operation duration in seconds",
                vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
            ),
            &["operation"]
        )?;

        let volumes_total = register_int_gauge!(opts!(
            "hf_csi_volumes_total",
            "Total number of managed volumes"
        ))?;

        let cache_size_bytes = register_gauge!(opts!(
            "hf_csi_cache_size_bytes",
            "Current cache size in bytes"
        ))?;

        let cache_limit_bytes = register_gauge!(opts!(
            "hf_csi_cache_limit_bytes",
            "Cache size limit in bytes"
        ))?;

        let cache_hits_total = register_counter_vec!(
            opts!("hf_csi_cache_hits_total", "Total cache hits"),
            &["cache_type"]
        )?;

        let cache_misses_total = register_counter_vec!(
            opts!("hf_csi_cache_misses_total", "Total cache misses"),
            &["cache_type"]
        )?;

        let hub_requests_total = register_counter_vec!(
            opts!(
                "hf_csi_hub_requests_total",
                "Total Hugging Face Hub API requests"
            ),
            &["method", "status"]
        )?;

        let hub_request_duration_seconds = register_histogram_vec!(
            histogram_opts!(
                "hf_csi_hub_request_duration_seconds",
                "Hub API request duration in seconds",
                vec![0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0]
            ),
            &["method"]
        )?;

        let active_mounts_total = register_int_gauge!(opts!(
            "hf_csi_active_mounts_total",
            "Number of currently active mounts"
        ))?;

        Ok(Self {
            volume_operations_total,
            volume_operations_failed_total,
            volume_operations_duration_seconds,
            volumes_total,
            cache_size_bytes,
            cache_limit_bytes,
            cache_hits_total,
            cache_misses_total,
            hub_requests_total,
            hub_request_duration_seconds,
            active_mounts_total,
        })
    }

    /// Record a volume operation with its duration
    pub fn record_volume_operation(&self, operation: &str, success: bool, duration: f64) {
        let status = if success { "success" } else { "failed" };

        self.volume_operations_total
            .with_label_values(&[operation, status])
            .inc();

        self.volume_operations_duration_seconds
            .with_label_values(&[operation])
            .observe(duration);

        if !success {
            self.volume_operations_failed_total
                .with_label_values(&[operation])
                .inc();
        }
    }

    /// Record a cache hit
    pub fn record_cache_hit(&self, cache_type: &str) {
        self.cache_hits_total
            .with_label_values(&[cache_type])
            .inc();
    }

    /// Record a cache miss
    pub fn record_cache_miss(&self, cache_type: &str) {
        self.cache_misses_total
            .with_label_values(&[cache_type])
            .inc();
    }

    /// Update cache size
    pub fn set_cache_size(&self, bytes: f64) {
        self.cache_size_bytes.set(bytes);
    }

    /// Update cache limit
    pub fn set_cache_limit(&self, bytes: f64) {
        self.cache_limit_bytes.set(bytes);
    }

    /// Record a Hugging Face Hub API request
    pub fn record_hub_request(&self, method: &str, status: u16, duration: f64) {
        let status_label = format!("{}", status);

        self.hub_requests_total
            .with_label_values(&[method, &status_label])
            .inc();

        self.hub_request_duration_seconds
            .with_label_values(&[method])
            .observe(duration);
    }

    /// Update total volumes count
    pub fn set_volumes_total(&self, count: i64) {
        self.volumes_total.set(count);
    }

    /// Update active mounts count
    pub fn set_active_mounts(&self, count: i64) {
        self.active_mounts_total.set(count);
    }
}

impl Default for CsiMetrics {
    fn default() -> Self {
        Self::new().expect("Failed to initialize metrics")
    }
}

/// Get or initialize the global metrics instance
pub fn metrics() -> &'static CsiMetrics {
    METRICS.get_or_init(CsiMetrics::default)
}

/// Initialize metrics system and return the metrics endpoint handler
pub fn init_metrics_server() -> anyhow::Result<Router> {
    // Initialize metrics
    let _ = metrics();

    info!("CSI metrics initialized");

    // Create router for metrics endpoint
    let app = Router::new().route("/metrics", get(metrics_handler));

    Ok(app)
}

/// HTTP handler for Prometheus metrics endpoint
async fn metrics_handler() -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
        warn!("Failed to encode metrics: {}", e);
        return Response::builder()
            .status(500)
            .body(Body::from(format!("Failed to encode metrics: {}", e)))
            .unwrap();
    }

    Response::builder()
        .header("Content-Type", encoder.format_type())
        .body(Body::from(buffer))
        .unwrap()
}

/// Timer for tracking operation duration
pub struct OperationTimer {
    operation: String,
    start: Instant,
}

impl OperationTimer {
    /// Start a new timer for the given operation
    pub fn start(operation: &str) -> Self {
        Self {
            operation: operation.to_string(),
            start: Instant::now(),
        }
    }

    /// Record success and return duration
    pub fn success(self) -> f64 {
        let duration = self.start.elapsed().as_secs_f64();
        metrics().record_volume_operation(&self.operation, true, duration);
        duration
    }

    /// Record failure and return duration
    pub fn failure(self) -> f64 {
        let duration = self.start.elapsed().as_secs_f64();
        metrics().record_volume_operation(&self.operation, false, duration);
        duration
    }

    /// Record with explicit success status
    pub fn finish(self, success: bool) -> f64 {
        let duration = self.start.elapsed().as_secs_f64();
        metrics().record_volume_operation(&self.operation, success, duration);
        duration
    }
}

impl Drop for OperationTimer {
    fn drop(&mut self) {
        // If not explicitly finished, record as success on drop
        // This is a fallback - prefer explicit success/failure calls
    }
}

/// Start the metrics HTTP server on the given address
pub async fn serve_metrics(addr: &str) -> anyhow::Result<()> {
    let app = init_metrics_server()?;

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("Metrics server listening on {}", addr);

    axum::serve(listener, app).await?;

    Ok(())
}

/// Convenience macro to time a block and record metrics
#[macro_export]
macro_rules! timed_operation {
    ($op:expr, $body:block) => {{
        let timer = $crate::csi::metrics::OperationTimer::start($op);
        let result = $body;
        let duration = if result.is_ok() {
            timer.success()
        } else {
            timer.failure()
        };
        (result, duration)
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_initialization() {
        let metrics = CsiMetrics::new();
        assert!(metrics.is_ok());
    }

    #[test]
    fn test_volume_operation_recording() {
        let metrics = CsiMetrics::new().unwrap();

        // Record some operations
        metrics.record_volume_operation("CreateVolume", true, 0.5);
        metrics.record_volume_operation("CreateVolume", true, 0.3);
        metrics.record_volume_operation("CreateVolume", false, 1.0);

        // Record cache operations
        metrics.record_cache_hit("disk");
        metrics.record_cache_miss("disk");

        // Record hub request
        metrics.record_hub_request("GET", 200, 0.1);
        metrics.record_hub_request("GET", 500, 2.0);

        // Set gauges
        metrics.set_volumes_total(10);
        metrics.set_active_mounts(5);
        metrics.set_cache_size(1024.0 * 1024.0 * 100.0); // 100 MB
        metrics.set_cache_limit(1024.0 * 1024.0 * 1024.0); // 1 GB

        // Verify we can gather metrics
        let families = prometheus::gather();
        assert!(!families.is_empty());
    }
}
