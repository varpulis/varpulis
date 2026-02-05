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
    Busy,
    Unhealthy,
    Draining,
}

impl std::fmt::Display for WorkerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Registering => write!(f, "registering"),
            Self::Ready => write!(f, "ready"),
            Self::Busy => write!(f, "busy"),
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
}

/// Request body for worker heartbeat.
#[derive(Debug, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub events_processed: u64,
    pub pipelines_running: usize,
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
    pub assigned_pipelines: Vec<String>,
}

impl From<&WorkerNode> for WorkerInfo {
    fn from(node: &WorkerNode) -> Self {
        Self {
            id: node.id.0.clone(),
            address: node.address.clone(),
            status: node.status.to_string(),
            pipelines_running: node.capacity.pipelines_running,
            max_pipelines: node.capacity.max_pipelines,
            assigned_pipelines: node.assigned_pipelines.clone(),
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
        assert_eq!(WorkerStatus::Busy.to_string(), "busy");
    }

    #[test]
    fn test_worker_id_display() {
        let id = WorkerId("my-worker-42".into());
        assert_eq!(id.to_string(), "my-worker-42");
        assert_eq!(format!("Worker: {}", id), "Worker: my-worker-42");
    }

    #[test]
    fn test_worker_not_available_when_busy() {
        let mut node = WorkerNode::new(
            WorkerId("w1".into()),
            "http://localhost:9000".into(),
            "key".into(),
        );
        node.status = WorkerStatus::Busy;
        assert!(!node.is_available());
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
        node.assigned_pipelines = vec!["p1".into(), "p2".into(), "p3".into()];

        let info = WorkerInfo::from(&node);
        assert_eq!(info.id, "w1");
        assert_eq!(info.address, "http://localhost:9000");
        assert_eq!(info.status, "ready");
        assert_eq!(info.pipelines_running, 3);
        assert_eq!(info.max_pipelines, 10);
        assert_eq!(info.assigned_pipelines, vec!["p1", "p2", "p3"]);
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
        };
        let json = serde_json::to_string(&hb).unwrap();
        let parsed: HeartbeatRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.events_processed, 42);
        assert_eq!(parsed.pipelines_running, 3);
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
            WorkerStatus::Busy,
            WorkerStatus::Unhealthy,
            WorkerStatus::Draining,
        ] {
            let json = serde_json::to_string(&s).unwrap();
            let parsed: WorkerStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, s);
        }
    }
}
