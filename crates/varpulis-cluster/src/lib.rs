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
#[cfg(feature = "k8s")]
pub mod ha;
pub mod health;
#[cfg(feature = "k8s")]
pub mod k8s_watcher;
pub mod metrics;
pub mod migration;
pub mod pipeline_group;
#[cfg(feature = "raft")]
pub mod raft;
pub mod routing;
pub mod worker;
pub mod ws;

// Re-exports
pub use api::{cluster_routes, shared_coordinator, SharedCoordinator};
pub use connector_config::ClusterConnector;
pub use coordinator::{
    Coordinator, HaRole, InjectEventRequest, InjectResponse, ScalingAction, ScalingPolicy,
    ScalingRecommendation,
};
pub use health::{
    DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_HEARTBEAT_TIMEOUT, DEFAULT_WS_GRACE_PERIOD,
    HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT,
};
pub use metrics::ClusterPrometheusMetrics;
pub use migration::{MigrationReason, MigrationStatus, MigrationTask};
pub use pipeline_group::{
    DeployedPipelineGroup, GroupStatus, InterPipelineRoute, PartitionStrategy, PipelineDeployment,
    PipelineDeploymentStatus, PipelineGroupInfo, PipelineGroupSpec, PipelinePlacement,
    ReplicaGroup,
};
pub use routing::{event_type_matches, find_target_pipeline, RoutingTable};
pub use worker::{
    ConnectorHealth, HeartbeatRequest, HeartbeatResponse, PipelineMetrics, RegisterWorkerRequest,
    RegisterWorkerResponse, WorkerCapacity, WorkerId, WorkerInfo, WorkerNode, WorkerStatus,
};
pub use ws::{shared_ws_manager, SharedWsManager};

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

    #[error("Migration failed: {0}")]
    MigrationFailed(String),

    #[error("Worker is draining: {0}")]
    WorkerDraining(String),

    #[error("Not the leader coordinator; forward to: {0}")]
    NotLeader(String),
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

/// Least-loaded placement strategy: picks the worker with the lowest
/// load ratio (pipelines_running / cpu_cores), breaking ties by pipeline count.
pub struct LeastLoadedPlacement;

impl PlacementStrategy for LeastLoadedPlacement {
    fn place(&self, _pipeline: &PipelinePlacement, workers: &[&WorkerNode]) -> Option<WorkerId> {
        workers
            .iter()
            .min_by(|a, b| {
                let cores_a = a.capacity.cpu_cores.max(1) as f64;
                let cores_b = b.capacity.cpu_cores.max(1) as f64;
                let ratio_a = a.capacity.pipelines_running as f64 / cores_a;
                let ratio_b = b.capacity.pipelines_running as f64 / cores_b;
                ratio_a
                    .partial_cmp(&ratio_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| {
                        a.capacity
                            .pipelines_running
                            .cmp(&b.capacity.pipelines_running)
                    })
            })
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
    use tracing::{info, warn};

    let client = reqwest::Client::new();
    let register_url = format!("{}/api/v1/cluster/workers/register", coordinator_url);
    let heartbeat_url = format!(
        "{}/api/v1/cluster/workers/{}/heartbeat",
        coordinator_url, worker_id
    );

    // Registration with exponential backoff
    let mut backoff = std::time::Duration::from_secs(1);
    let max_backoff = std::time::Duration::from_secs(30);
    let mut coordinator_heartbeat_interval: Option<u64> = None;

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
                if let Ok(reg_resp) = resp.json::<RegisterWorkerResponse>().await {
                    coordinator_heartbeat_interval = reg_resp.heartbeat_interval_secs;
                }
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

    // Heartbeat loop — use coordinator-provided interval if available
    let interval = coordinator_heartbeat_interval
        .map(std::time::Duration::from_secs)
        .unwrap_or(HEARTBEAT_INTERVAL);

    // Try WebSocket connection for fast failure detection
    let ws_url = coordinator_url
        .replace("http://", "ws://")
        .replace("https://", "wss://");
    let ws_endpoint = format!("{}/api/v1/cluster/ws", ws_url);

    loop {
        // Attempt WebSocket connection
        match try_ws_heartbeat_loop(
            &ws_endpoint,
            &worker_id,
            &api_key,
            interval,
            &tenant_manager,
        )
        .await
        {
            Ok(()) => {
                // WS loop ended cleanly (shouldn't happen normally)
                warn!("WebSocket heartbeat loop ended, falling back to REST");
            }
            Err(e) => {
                warn!("WebSocket connection failed: {}, using REST heartbeats", e);
            }
        }

        // Fall back to REST heartbeat loop
        rest_heartbeat_loop(
            &client,
            &heartbeat_url,
            interval,
            &tenant_manager,
            &ws_endpoint,
            &worker_id,
            &api_key,
        )
        .await;
    }
}

/// Collect pipeline metrics from the tenant manager (async).
async fn collect_worker_metrics(
    tenant_manager: &Option<varpulis_runtime::SharedTenantManager>,
) -> (usize, Vec<PipelineMetrics>) {
    if let Some(ref tm) = tenant_manager {
        let mgr = tm.read().await;
        let metrics = mgr.collect_pipeline_metrics().await;
        let count = metrics.len();

        let health_data = mgr.collect_connector_health();

        let pm: Vec<PipelineMetrics> = metrics
            .into_iter()
            .map(|(name, events_in, events_out)| {
                let connector_health: Vec<ConnectorHealth> = health_data
                    .iter()
                    .filter(|(pname, _, _, _)| pname == &name)
                    .map(|(_, cname, ctype, report)| ConnectorHealth {
                        connector_name: cname.clone(),
                        connector_type: ctype.clone(),
                        connected: report.connected,
                        last_error: report.last_error.clone(),
                        messages_received: report.messages_received,
                        seconds_since_last_message: report.seconds_since_last_message,
                    })
                    .collect();
                PipelineMetrics {
                    pipeline_name: name,
                    events_in,
                    events_out,
                    connector_health,
                }
            })
            .collect();
        (count, pm)
    } else {
        (0, Vec::new())
    }
}

/// Build a HeartbeatRequest from collected metrics.
fn build_heartbeat(
    pipelines_running: usize,
    pipeline_metrics: Vec<PipelineMetrics>,
) -> HeartbeatRequest {
    let total_events: u64 = pipeline_metrics.iter().map(|m| m.events_in).sum();
    HeartbeatRequest {
        events_processed: total_events,
        pipelines_running,
        pipeline_metrics,
    }
}

/// Try to run heartbeats over WebSocket. Returns Err if WS connection fails.
async fn try_ws_heartbeat_loop(
    ws_endpoint: &str,
    worker_id: &str,
    api_key: &str,
    interval: std::time::Duration,
    tenant_manager: &Option<varpulis_runtime::SharedTenantManager>,
) -> Result<(), String> {
    use futures_util::{SinkExt, StreamExt};
    use tracing::{info, warn};

    let (ws_stream, _) = tokio_tungstenite::connect_async(ws_endpoint)
        .await
        .map_err(|e| format!("WS connect failed: {}", e))?;

    info!("WebSocket connected to coordinator at {}", ws_endpoint);

    let (mut ws_tx, mut ws_rx) = ws_stream.split();

    // Send Identify message
    let identify = ws::WorkerMessage::Identify {
        worker_id: worker_id.to_string(),
        api_key: api_key.to_string(),
    };
    let identify_json = serde_json::to_string(&identify).map_err(|e| e.to_string())?;
    ws_tx
        .send(tokio_tungstenite::tungstenite::Message::Text(identify_json))
        .await
        .map_err(|e| format!("WS send failed: {}", e))?;

    // Wait for IdentifyAck
    let ack_msg = tokio::time::timeout(std::time::Duration::from_secs(5), ws_rx.next())
        .await
        .map_err(|_| "Timeout waiting for IdentifyAck".to_string())?
        .ok_or("WS stream ended before IdentifyAck")?
        .map_err(|e| format!("WS read error: {}", e))?;

    if let tokio_tungstenite::tungstenite::Message::Text(text) = ack_msg {
        let coord_msg: ws::CoordinatorMessage =
            serde_json::from_str(&text).map_err(|e| e.to_string())?;
        match coord_msg {
            ws::CoordinatorMessage::IdentifyAck { .. } => {
                info!("Worker {} identified via WebSocket", worker_id);
            }
            ws::CoordinatorMessage::Error { message } => {
                return Err(format!("Coordinator rejected: {}", message));
            }
            _ => {
                return Err("Unexpected response to Identify".to_string());
            }
        }
    } else {
        return Err("Non-text response to Identify".to_string());
    }

    // Heartbeat loop over WebSocket
    let mut heartbeat_interval = tokio::time::interval(interval);
    loop {
        tokio::select! {
            _ = heartbeat_interval.tick() => {
                let (pipelines_running, pipeline_metrics) =
                    collect_worker_metrics(tenant_manager).await;
                let hb = build_heartbeat(pipelines_running, pipeline_metrics);
                let ws_msg = ws::WorkerMessage::Heartbeat(hb);
                let json = serde_json::to_string(&ws_msg).map_err(|e| e.to_string())?;
                if ws_tx
                    .send(tokio_tungstenite::tungstenite::Message::Text(json))
                    .await
                    .is_err()
                {
                    warn!("WebSocket send failed, reconnecting...");
                    return Ok(());
                }
            }
            msg = ws_rx.next() => {
                match msg {
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_))) | None => {
                        warn!("WebSocket closed by coordinator");
                        return Ok(());
                    }
                    Some(Err(e)) => {
                        warn!("WebSocket error: {}", e);
                        return Ok(());
                    }
                    _ => {
                        // HeartbeatAck or other — continue
                    }
                }
            }
        }
    }
}

/// REST heartbeat fallback loop. Runs until a WS reconnection succeeds.
async fn rest_heartbeat_loop(
    client: &reqwest::Client,
    heartbeat_url: &str,
    interval: std::time::Duration,
    tenant_manager: &Option<varpulis_runtime::SharedTenantManager>,
    ws_endpoint: &str,
    _worker_id: &str,
    api_key: &str,
) {
    use tracing::{error, info, warn};

    let mut rest_cycles = 0u32;
    loop {
        tokio::time::sleep(interval).await;

        let (pipelines_running, pipeline_metrics) = collect_worker_metrics(tenant_manager).await;
        let hb = build_heartbeat(pipelines_running, pipeline_metrics);

        match client
            .post(heartbeat_url)
            .header("x-api-key", api_key)
            .json(&hb)
            .send()
            .await
        {
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

        // Periodically try to reconnect via WebSocket (every 6 REST cycles)
        rest_cycles += 1;
        if rest_cycles.is_multiple_of(6) {
            if let Ok((ws_stream, _)) = tokio_tungstenite::connect_async(ws_endpoint).await {
                info!("WebSocket reconnected, switching back to WS heartbeats");
                drop(ws_stream);
                return; // Exit REST loop to retry WS in outer loop
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
            replicas: 1,
            partition_key: None,
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
            replicas: 1,
            partition_key: None,
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
            replicas: 1,
            partition_key: None,
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
            replicas: 1,
            partition_key: None,
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
            replicas: 1,
            partition_key: None,
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
            replicas: 1,
            partition_key: None,
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
            replicas: 1,
            partition_key: None,
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
            replicas: 1,
            partition_key: None,
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
            replicas: 1,
            partition_key: None,
        };
        assert_eq!(
            placement.place(&pipeline, &workers),
            Some(WorkerId("w1".into()))
        );
    }

    #[test]
    fn test_least_loaded_prefers_more_cores() {
        // Worker w1: 2 pipelines on 8 cores (ratio 0.25)
        // Worker w2: 2 pipelines on 2 cores (ratio 1.0)
        // LeastLoaded should pick w1 (lower ratio)
        let placement = LeastLoadedPlacement;
        let mut w1 = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        w1.capacity.pipelines_running = 2;
        w1.capacity.cpu_cores = 8;
        let mut w2 = WorkerNode::new(
            WorkerId("w2".into()),
            "http://localhost:9001".into(),
            "key".into(),
        );
        w2.capacity.pipelines_running = 2;
        w2.capacity.cpu_cores = 2;
        let workers = vec![&w1, &w2];
        let pipeline = PipelinePlacement {
            name: "p1".into(),
            source: "".into(),
            worker_affinity: None,
            replicas: 1,
            partition_key: None,
        };

        assert_eq!(
            placement.place(&pipeline, &workers),
            Some(WorkerId("w1".into()))
        );
    }

    #[test]
    fn test_least_loaded_same_ratio_picks_fewer_pipelines() {
        // Worker w1: 4 pipelines on 4 cores (ratio 1.0)
        // Worker w2: 2 pipelines on 2 cores (ratio 1.0)
        // Same ratio, so tiebreaker is fewer pipelines → w2
        let placement = LeastLoadedPlacement;
        let mut w1 = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        w1.capacity.pipelines_running = 4;
        w1.capacity.cpu_cores = 4;
        let mut w2 = WorkerNode::new(
            WorkerId("w2".into()),
            "http://localhost:9001".into(),
            "key".into(),
        );
        w2.capacity.pipelines_running = 2;
        w2.capacity.cpu_cores = 2;
        let workers = vec![&w1, &w2];
        let pipeline = PipelinePlacement {
            name: "p1".into(),
            source: "".into(),
            worker_affinity: None,
            replicas: 1,
            partition_key: None,
        };

        assert_eq!(
            placement.place(&pipeline, &workers),
            Some(WorkerId("w2".into()))
        );
    }
}
