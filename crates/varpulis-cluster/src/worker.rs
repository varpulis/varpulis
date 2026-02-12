//! Worker node types and registration protocol.

use serde::{Deserialize, Serialize};
use std::time::Instant;

/// Unique identifier for a worker node.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerId(pub String);

impl std::fmt::Display for WorkerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Status of a worker node in the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerStatus {
    Registering,
    Ready,
    Unhealthy,
    Draining,
}

impl std::fmt::Display for WorkerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Registering => write!(f, "registering"),
            Self::Ready => write!(f, "ready"),
            Self::Unhealthy => write!(f, "unhealthy"),
            Self::Draining => write!(f, "draining"),
        }
    }
}

/// Capacity information for a worker node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerCapacity {
    pub cpu_cores: usize,
    pub pipelines_running: usize,
    pub max_pipelines: usize,
}

impl Default for WorkerCapacity {
    fn default() -> Self {
        Self {
            cpu_cores: num_cpus(),
            pipelines_running: 0,
            max_pipelines: 100,
        }
    }
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// A worker node in the cluster.
pub struct WorkerNode {
    pub id: WorkerId,
    pub address: String,
    pub api_key: String,
    pub status: WorkerStatus,
    pub capacity: WorkerCapacity,
    pub last_heartbeat: Instant,
    pub assigned_pipelines: Vec<String>,
    /// Total events processed by this worker (from heartbeats).
    pub events_processed: u64,
    /// When the WebSocket connection was lost (for grace period before failover).
    pub ws_disconnected_at: Option<Instant>,
}

impl WorkerNode {
    pub fn new(id: WorkerId, address: String, api_key: String) -> Self {
        Self {
            id,
            address,
            api_key,
            status: WorkerStatus::Registering,
            capacity: WorkerCapacity::default(),
            last_heartbeat: Instant::now(),
            assigned_pipelines: Vec::new(),
            events_processed: 0,
            ws_disconnected_at: None,
        }
    }

    pub fn is_available(&self) -> bool {
        self.status == WorkerStatus::Ready
            && self.capacity.pipelines_running < self.capacity.max_pipelines
    }
}

/// Request body for worker registration.
#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterWorkerRequest {
    pub worker_id: String,
    pub address: String,
    pub api_key: String,
    pub capacity: WorkerCapacity,
}

/// Response body for worker registration.
#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterWorkerResponse {
    pub worker_id: String,
    pub status: String,
    /// Optional heartbeat interval override from coordinator (seconds).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heartbeat_interval_secs: Option<u64>,
}

/// Health status of a single connector within a pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorHealth {
    pub connector_name: String,
    pub connector_type: String,
    pub connected: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub messages_received: u64,
    pub seconds_since_last_message: u64,
}

/// Per-pipeline metrics reported in heartbeats.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineMetrics {
    pub pipeline_name: String,
    pub events_in: u64,
    pub events_out: u64,
    #[serde(default)]
    pub connector_health: Vec<ConnectorHealth>,
}

/// Request body for worker heartbeat.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub events_processed: u64,
    pub pipelines_running: usize,
    #[serde(default)]
    pub pipeline_metrics: Vec<PipelineMetrics>,
}

/// Response body for worker heartbeat.
#[derive(Debug, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    pub acknowledged: bool,
}

/// Serializable worker info for API responses.
#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerInfo {
    pub id: String,
    pub address: String,
    pub status: String,
    pub pipelines_running: usize,
    pub max_pipelines: usize,
    pub cpu_cores: usize,
    pub assigned_pipelines: Vec<String>,
    pub events_processed: u64,
}

impl From<&WorkerNode> for WorkerInfo {
    fn from(node: &WorkerNode) -> Self {
        Self {
            id: node.id.0.clone(),
            address: node.address.clone(),
            status: node.status.to_string(),
            pipelines_running: node.capacity.pipelines_running,
            max_pipelines: node.capacity.max_pipelines,
            cpu_cores: node.capacity.cpu_cores,
            assigned_pipelines: node.assigned_pipelines.clone(),
            events_processed: node.events_processed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_node_creation() {
        let node = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        assert_eq!(node.id, WorkerId("w1".into()));
        assert_eq!(node.status, WorkerStatus::Registering);
        assert!(!node.is_available()); // not Ready yet
    }

    #[test]
    fn test_worker_is_available() {
        let mut node = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        node.status = WorkerStatus::Ready;
        assert!(node.is_available());

        node.status = WorkerStatus::Unhealthy;
        assert!(!node.is_available());
    }

    #[test]
    fn test_worker_status_display() {
        assert_eq!(WorkerStatus::Ready.to_string(), "ready");
        assert_eq!(WorkerStatus::Unhealthy.to_string(), "unhealthy");
        assert_eq!(WorkerStatus::Draining.to_string(), "draining");
        assert_eq!(WorkerStatus::Registering.to_string(), "registering");
    }

    #[test]
    fn test_worker_id_display() {
        let id = WorkerId("my-worker-42".into());
        assert_eq!(id.to_string(), "my-worker-42");
        assert_eq!(format!("Worker: {}", id), "Worker: my-worker-42");
    }

    #[test]
    fn test_worker_not_available_when_draining() {
        let mut node = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        node.status = WorkerStatus::Draining;
        assert!(!node.is_available());
    }

    #[test]
    fn test_worker_not_available_at_max_capacity() {
        let mut node = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        node.status = WorkerStatus::Ready;
        node.capacity.max_pipelines = 5;
        node.capacity.pipelines_running = 5;
        assert!(!node.is_available());
    }

    #[test]
    fn test_worker_available_just_below_capacity() {
        let mut node = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        node.status = WorkerStatus::Ready;
        node.capacity.max_pipelines = 5;
        node.capacity.pipelines_running = 4;
        assert!(node.is_available());
    }

    #[test]
    fn test_worker_info_from_node() {
        let mut node = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "secret-key".into(),
        );
        node.status = WorkerStatus::Ready;
        node.capacity.pipelines_running = 3;
        node.capacity.max_pipelines = 10;
        node.capacity.cpu_cores = 8;
        node.assigned_pipelines = vec!["p1".into(), "p2".into(), "p3".into()];
        node.events_processed = 42000;

        let info = WorkerInfo::from(&node);
        assert_eq!(info.id, "w1");
        assert_eq!(info.address, "http://localhost:9000");
        assert_eq!(info.status, "ready");
        assert_eq!(info.pipelines_running, 3);
        assert_eq!(info.max_pipelines, 10);
        assert_eq!(info.cpu_cores, 8);
        assert_eq!(info.assigned_pipelines, vec!["p1", "p2", "p3"]);
        assert_eq!(info.events_processed, 42000);
    }

    #[test]
    fn test_worker_capacity_default() {
        let cap = WorkerCapacity::default();
        assert!(cap.cpu_cores >= 1);
        assert_eq!(cap.pipelines_running, 0);
        assert_eq!(cap.max_pipelines, 100);
    }

    #[test]
    fn test_worker_id_equality() {
        let a = WorkerId("w1".into());
        let b = WorkerId("w1".into());
        let c = WorkerId("w2".into());
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_worker_id_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(WorkerId("w1".into()));
        set.insert(WorkerId("w1".into())); // duplicate
        set.insert(WorkerId("w2".into()));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_register_worker_request_serde() {
        let req = RegisterWorkerRequest {
            worker_id: "w1".into(),
            address: "http://localhost:9000".into(),
            api_key: "key".into(),
            capacity: WorkerCapacity::default(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: RegisterWorkerRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.worker_id, "w1");
        assert_eq!(parsed.address, "http://localhost:9000");
    }

    #[test]
    fn test_heartbeat_request_serde() {
        let hb = HeartbeatRequest {
            events_processed: 42,
            pipelines_running: 3,
            pipeline_metrics: vec![],
        };
        let json = serde_json::to_string(&hb).unwrap();
        let parsed: HeartbeatRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.events_processed, 42);
        assert_eq!(parsed.pipelines_running, 3);
    }

    #[test]
    fn test_connector_health_serde() {
        let ch = ConnectorHealth {
            connector_name: "mqtt_in".into(),
            connector_type: "mqtt".into(),
            connected: true,
            last_error: None,
            messages_received: 42,
            seconds_since_last_message: 5,
        };
        let json = serde_json::to_string(&ch).unwrap();
        let parsed: ConnectorHealth = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.connector_name, "mqtt_in");
        assert!(parsed.connected);
        assert!(parsed.last_error.is_none());
        assert_eq!(parsed.messages_received, 42);
        // last_error should be skipped when None
        assert!(!json.contains("last_error"));
    }

    #[test]
    fn test_pipeline_metrics_backward_compat() {
        // Old heartbeats without connector_health should deserialize with empty vec
        let json = r#"{"pipeline_name":"p1","events_in":100,"events_out":50}"#;
        let parsed: PipelineMetrics = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.pipeline_name, "p1");
        assert_eq!(parsed.events_in, 100);
        assert_eq!(parsed.events_out, 50);
        assert!(parsed.connector_health.is_empty());
    }

    #[test]
    fn test_pipeline_metrics_with_connector_health() {
        let pm = PipelineMetrics {
            pipeline_name: "p1".into(),
            events_in: 100,
            events_out: 50,
            connector_health: vec![ConnectorHealth {
                connector_name: "mqtt_in".into(),
                connector_type: "mqtt".into(),
                connected: false,
                last_error: Some("connection refused".into()),
                messages_received: 0,
                seconds_since_last_message: 120,
            }],
        };
        let json = serde_json::to_string(&pm).unwrap();
        let parsed: PipelineMetrics = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.connector_health.len(), 1);
        assert!(!parsed.connector_health[0].connected);
        assert_eq!(
            parsed.connector_health[0].last_error.as_deref(),
            Some("connection refused")
        );
    }

    #[test]
    fn test_worker_status_serde() {
        let status = WorkerStatus::Ready;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"ready\"");
        let parsed: WorkerStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, WorkerStatus::Ready);

        // All variants round-trip
        for s in [
            WorkerStatus::Registering,
            WorkerStatus::Ready,
            WorkerStatus::Unhealthy,
            WorkerStatus::Draining,
        ] {
            let json = serde_json::to_string(&s).unwrap();
            let parsed: WorkerStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, s);
        }
    }
}
