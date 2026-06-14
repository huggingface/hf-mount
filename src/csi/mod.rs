//! CSI driver module for Kubernetes integration.
//!
//! This module provides the CSI (Container Storage Interface) implementation
//! for mounting Hugging Face Hub repositories as Kubernetes volumes.

pub mod metrics;

// Re-export metrics for convenience
pub use metrics::{metrics, serve_metrics, CsiMetrics, OperationTimer};
