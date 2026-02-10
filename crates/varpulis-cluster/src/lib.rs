//! # Varpulis Cluster
//!
//! Distributed execution support for Varpulis streaming analytics.
//!
//! Provides a coordinator-based control plane for deploying and managing
//! pipeline groups across multiple worker processes.
//!
//! ## Architecture
//!
//! - **Coordinator**: Central control plane that manages worker registration,
//!   pipeline placement, event routing, and health monitoring.
//! - **Workers**: Standard `varpulis server` processes that register with
//!   a coordinator and run assigned pipelines.
//! - **Pipeline Groups**: Collections of related pipelines deployed together
//!   with routing rules for inter-pipeline communication.
//!
//! ## Usage
//!
//! ```bash
//! # Start coordinator
//! varpulis coordinator --port 9100 --api-key admin
//!
//! # Start workers (they auto-register)
//! varpulis server --port 9000 --api-key test --coordinator http://localhost:9100 --worker-id w0
//! varpulis server --port 9001 --api-key test --coordinator http://localhost:9100 --worker-id w1
//! ```

pub mod api;
pub mod connector_config;
pub mod coordinator;
pub mod health;
pub mod pipeline_group;
pub mod routing;
pub mod worker;

// Re-exports
pub use api::{cluster_routes, shared_coordinator, SharedCoordinator};
pub use connector_config::ClusterConnector;
pub use coordinator::{Coordinator, InjectEventRequest, InjectResponse};
pub use health::{HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT};
pub use pipeline_group::{
    DeployedPipelineGroup, GroupStatus, InterPipelineRoute, PipelineDeployment,
    PipelineDeploymentStatus, PipelineGroupInfo, PipelineGroupSpec, PipelinePlacement,
};
pub use routing::{event_type_matches, find_target_pipeline, RoutingTable};
pub use worker::{
    HeartbeatRequest, HeartbeatResponse, PipelineMetrics, RegisterWorkerRequest,
    RegisterWorkerResponse, WorkerCapacity, WorkerId, WorkerInfo, WorkerNode, WorkerStatus,
};

/// Errors that can occur in the cluster.
#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    #[error("Worker not found: {0}")]
    WorkerNotFound(String),

    #[error("Pipeline group not found: {0}")]
    GroupNotFound(String),

    #[error("No workers available for deployment")]
    NoWorkersAvailable,

    #[error("Pipeline deployment failed: {0}")]
    DeployFailed(String),

    #[error("Event routing failed: {0}")]
    RoutingFailed(String),

    #[error("Connector not found: {0}")]
    ConnectorNotFound(String),

    #[error("Connector validation failed: {0}")]
    ConnectorValidation(String),
}

/// Trait for pipeline placement strategies.
pub trait PlacementStrategy: Send + Sync {
    fn place(&self, pipeline: &PipelinePlacement, workers: &[&WorkerNode]) -> Option<WorkerId>;
}

/// Round-robin placement strategy.
pub struct RoundRobinPlacement {
    counter: std::sync::atomic::AtomicUsize,
}

impl RoundRobinPlacement {
    pub fn new() -> Self {
        Self {
            counter: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl Default for RoundRobinPlacement {
    fn default() -> Self {
        Self::new()
    }
}

impl PlacementStrategy for RoundRobinPlacement {
    fn place(&self, _pipeline: &PipelinePlacement, workers: &[&WorkerNode]) -> Option<WorkerId> {
        if workers.is_empty() {
            return None;
        }
        let idx = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            % workers.len();
        Some(workers[idx].id.clone())
    }
}

/// Least-loaded placement strategy: picks the worker with fewest pipelines.
pub struct LeastLoadedPlacement;

impl PlacementStrategy for LeastLoadedPlacement {
    fn place(&self, _pipeline: &PipelinePlacement, workers: &[&WorkerNode]) -> Option<WorkerId> {
        workers
            .iter()
            .min_by_key(|w| w.capacity.pipelines_running)
            .map(|w| w.id.clone())
    }
}

/// Background task: worker registration loop.
///
/// Registers with the coordinator and sends periodic heartbeats.
/// If a `tenant_manager` is provided, heartbeats include real pipeline metrics.
pub async fn worker_registration_loop(
    coordinator_url: String,
    worker_id: String,
    worker_address: String,
    api_key: String,
    tenant_manager: Option<varpulis_runtime::SharedTenantManager>,
) {
    use tracing::{error, info, warn};

    let client = reqwest::Client::new();
    let register_url = format!("{}/api/v1/cluster/workers/register", coordinator_url);
    let heartbeat_url = format!(
        "{}/api/v1/cluster/workers/{}/heartbeat",
        coordinator_url, worker_id
    );

    // Registration with exponential backoff
    let mut backoff = std::time::Duration::from_secs(1);
    let max_backoff = std::time::Duration::from_secs(30);

    loop {
        let body = RegisterWorkerRequest {
            worker_id: worker_id.clone(),
            address: worker_address.clone(),
            api_key: api_key.clone(),
            capacity: WorkerCapacity::default(),
        };

        match client
            .post(&register_url)
            .header("x-api-key", &api_key)
            .json(&body)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                info!(
                    "Registered with coordinator as '{}' at {}",
                    worker_id, coordinator_url
                );
                break;
            }
            Ok(resp) => {
                warn!(
                    "Registration failed (HTTP {}), retrying in {:?}",
                    resp.status(),
                    backoff
                );
            }
            Err(e) => {
                warn!(
                    "Cannot reach coordinator at {}: {}, retrying in {:?}",
                    coordinator_url, e, backoff
                );
            }
        }

        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }

    // Heartbeat loop
    let interval = HEARTBEAT_INTERVAL;
    loop {
        tokio::time::sleep(interval).await;

        let (pipelines_running, pipeline_metrics) = if let Some(ref tm) = tenant_manager {
            let mgr = tm.read().await;
            let metrics = mgr.collect_pipeline_metrics().await;
            let count = metrics.len();
            let pm: Vec<PipelineMetrics> = metrics
                .into_iter()
                .map(|(name, events_in, events_out)| PipelineMetrics {
                    pipeline_name: name,
                    events_in,
                    events_out,
                })
                .collect();
            (count, pm)
        } else {
            (0, Vec::new())
        };

        let total_events: u64 = pipeline_metrics.iter().map(|m| m.events_in).sum();
        let hb = HeartbeatRequest {
            events_processed: total_events,
            pipelines_running,
            pipeline_metrics,
        };

        match client.post(&heartbeat_url).json(&hb).send().await {
            Ok(resp) if resp.status().is_success() => {
                // heartbeat acknowledged
            }
            Ok(resp) => {
                warn!("Heartbeat rejected (HTTP {})", resp.status());
            }
            Err(e) => {
                error!("Heartbeat failed: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_robin_placement() {
        let placement = RoundRobinPlacement::new();
        let w1 = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        let w2 = WorkerNode::new(
            WorkerId("w2".into()),
            "http://localhost:9001".into(),
            "key".into(),
        );
        let workers = vec![&w1, &w2];
        let pipeline = PipelinePlacement {
            name: "p1".into(),
            source: "".into(),
            worker_affinity: None,
        };

        let first = placement.place(&pipeline, &workers).unwrap();
        let second = placement.place(&pipeline, &workers).unwrap();
        let third = placement.place(&pipeline, &workers).unwrap();

        assert_eq!(first, WorkerId("w1".into()));
        assert_eq!(second, WorkerId("w2".into()));
        assert_eq!(third, WorkerId("w1".into()));
    }

    #[test]
    fn test_least_loaded_placement() {
        let placement = LeastLoadedPlacement;
        let mut w1 = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        w1.capacity.pipelines_running = 3;
        let mut w2 = WorkerNode::new(
            WorkerId("w2".into()),
            "http://localhost:9001".into(),
            "key".into(),
        );
        w2.capacity.pipelines_running = 1;
        let workers = vec![&w1, &w2];
        let pipeline = PipelinePlacement {
            name: "p1".into(),
            source: "".into(),
            worker_affinity: None,
        };

        let selected = placement.place(&pipeline, &workers).unwrap();
        assert_eq!(selected, WorkerId("w2".into()));
    }

    #[test]
    fn test_placement_empty_workers() {
        let placement = RoundRobinPlacement::new();
        let workers: Vec<&WorkerNode> = vec![];
        let pipeline = PipelinePlacement {
            name: "p1".into(),
            source: "".into(),
            worker_affinity: None,
        };
        assert!(placement.place(&pipeline, &workers).is_none());
    }

    #[test]
    fn test_round_robin_single_worker() {
        let placement = RoundRobinPlacement::new();
        let w1 = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        let workers = vec![&w1];
        let pipeline = PipelinePlacement {
            name: "p1".into(),
            source: "".into(),
            worker_affinity: None,
        };

        // All placements should go to the single worker
        for _ in 0..10 {
            assert_eq!(
                placement.place(&pipeline, &workers),
                Some(WorkerId("w1".into()))
            );
        }
    }

    #[test]
    fn test_round_robin_wraps_around() {
        let placement = RoundRobinPlacement::new();
        let w1 = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        let w2 = WorkerNode::new(
            WorkerId("w2".into()),
            "http://localhost:9001".into(),
            "key".into(),
        );
        let w3 = WorkerNode::new(
            WorkerId("w3".into()),
            "http://localhost:9002".into(),
            "key".into(),
        );
        let workers = vec![&w1, &w2, &w3];
        let pipeline = PipelinePlacement {
            name: "p1".into(),
            source: "".into(),
            worker_affinity: None,
        };

        let mut results = Vec::new();
        for _ in 0..9 {
            results.push(placement.place(&pipeline, &workers).unwrap());
        }

        // Should cycle w1, w2, w3, w1, w2, w3, w1, w2, w3
        assert_eq!(results[0], WorkerId("w1".into()));
        assert_eq!(results[1], WorkerId("w2".into()));
        assert_eq!(results[2], WorkerId("w3".into()));
        assert_eq!(results[3], WorkerId("w1".into()));
        assert_eq!(results[4], WorkerId("w2".into()));
        assert_eq!(results[5], WorkerId("w3".into()));
        assert_eq!(results[6], WorkerId("w1".into()));
    }

    #[test]
    fn test_least_loaded_empty_workers() {
        let placement = LeastLoadedPlacement;
        let workers: Vec<&WorkerNode> = vec![];
        let pipeline = PipelinePlacement {
            name: "p1".into(),
            source: "".into(),
            worker_affinity: None,
        };
        assert!(placement.place(&pipeline, &workers).is_none());
    }

    #[test]
    fn test_least_loaded_tied_workers() {
        let placement = LeastLoadedPlacement;
        let mut w1 = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        w1.capacity.pipelines_running = 2;
        let mut w2 = WorkerNode::new(
            WorkerId("w2".into()),
            "http://localhost:9001".into(),
            "key".into(),
        );
        w2.capacity.pipelines_running = 2;
        let workers = vec![&w1, &w2];
        let pipeline = PipelinePlacement {
            name: "p1".into(),
            source: "".into(),
            worker_affinity: None,
        };

        // Should pick one of them (min_by_key picks first in case of tie)
        let result = placement.place(&pipeline, &workers).unwrap();
        assert!(result == WorkerId("w1".into()) || result == WorkerId("w2".into()));
    }

    #[test]
    fn test_least_loaded_picks_zero_load() {
        let placement = LeastLoadedPlacement;
        let mut w1 = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        w1.capacity.pipelines_running = 5;
        let w2 = WorkerNode::new(
            WorkerId("w2".into()),
            "http://localhost:9001".into(),
            "key".into(),
        );
        // w2 has pipelines_running = 0 (default)
        let workers = vec![&w1, &w2];
        let pipeline = PipelinePlacement {
            name: "p1".into(),
            source: "".into(),
            worker_affinity: None,
        };

        assert_eq!(
            placement.place(&pipeline, &workers),
            Some(WorkerId("w2".into()))
        );
    }

    #[test]
    fn test_cluster_error_display() {
        let e = ClusterError::WorkerNotFound("w42".into());
        assert_eq!(e.to_string(), "Worker not found: w42");

        let e = ClusterError::GroupNotFound("g99".into());
        assert_eq!(e.to_string(), "Pipeline group not found: g99");

        let e = ClusterError::NoWorkersAvailable;
        assert_eq!(e.to_string(), "No workers available for deployment");

        let e = ClusterError::DeployFailed("connection refused".into());
        assert_eq!(
            e.to_string(),
            "Pipeline deployment failed: connection refused"
        );

        let e = ClusterError::RoutingFailed("no target".into());
        assert_eq!(e.to_string(), "Event routing failed: no target");
    }

    #[test]
    fn test_round_robin_default() {
        let placement = RoundRobinPlacement::default();
        let w = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        let workers = vec![&w];
        let pipeline = PipelinePlacement {
            name: "p1".into(),
            source: "".into(),
            worker_affinity: None,
        };
        assert_eq!(
            placement.place(&pipeline, &workers),
            Some(WorkerId("w1".into()))
        );
    }
}
