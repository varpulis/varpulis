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
    }
}
