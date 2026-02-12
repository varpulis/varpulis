//! Replicated coordinator state and command application logic.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::connector_config::ClusterConnector;

use super::{ClusterCommand, ClusterResponse};

// ---------------------------------------------------------------------------
// Replicated state
// ---------------------------------------------------------------------------

/// The pure-data subset of coordinator state that is replicated via Raft.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CoordinatorState {
    pub workers: HashMap<String, WorkerEntry>,
    pub pipeline_groups: HashMap<String, serde_json::Value>,
    pub connectors: HashMap<String, ClusterConnector>,
    pub active_migrations: HashMap<String, serde_json::Value>,
    pub scaling_policy: Option<serde_json::Value>,
}

/// Serializable worker entry (no `Instant` fields).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerEntry {
    pub id: String,
    pub address: String,
    pub api_key: String,
    pub status: String,
    pub cpu_cores: usize,
    pub pipelines_running: usize,
    pub max_pipelines: usize,
    pub assigned_pipelines: Vec<String>,
    pub events_processed: u64,
}

// ---------------------------------------------------------------------------
// Command application
// ---------------------------------------------------------------------------

/// Apply a single [`ClusterCommand`] to the replicated state.
pub fn apply_command(state: &mut CoordinatorState, cmd: ClusterCommand) -> ClusterResponse {
    match cmd {
        ClusterCommand::RegisterWorker {
            id,
            address,
            api_key,
            capacity,
        } => {
            state.workers.insert(
                id.clone(),
                WorkerEntry {
                    id,
                    address,
                    api_key,
                    status: "ready".to_string(),
                    cpu_cores: capacity.cpu_cores,
                    pipelines_running: capacity.pipelines_running,
                    max_pipelines: capacity.max_pipelines,
                    assigned_pipelines: Vec::new(),
                    events_processed: 0,
                },
            );
            ClusterResponse::Ok
        }

        ClusterCommand::DeregisterWorker { id } => {
            state.workers.remove(&id);
            ClusterResponse::Ok
        }

        ClusterCommand::WorkerStatusChanged { id, status } => {
            if let Some(w) = state.workers.get_mut(&id) {
                w.status = status;
            }
            ClusterResponse::Ok
        }

        ClusterCommand::GroupDeployed { name, group } => {
            state.pipeline_groups.insert(name, group);
            ClusterResponse::Ok
        }

        ClusterCommand::GroupUpdated { name, group } => {
            state.pipeline_groups.insert(name, group);
            ClusterResponse::Ok
        }

        ClusterCommand::GroupRemoved { name } => {
            state.pipeline_groups.remove(&name);
            ClusterResponse::Ok
        }

        ClusterCommand::MigrationStarted { task } => {
            if let Some(id) = task.get("id").and_then(|v| v.as_str()) {
                state.active_migrations.insert(id.to_string(), task);
            }
            ClusterResponse::Ok
        }

        ClusterCommand::MigrationUpdated { id, status } => {
            if let Some(m) = state.active_migrations.get_mut(&id) {
                m["status"] = serde_json::Value::String(status);
            }
            ClusterResponse::Ok
        }

        ClusterCommand::MigrationRemoved { id } => {
            state.active_migrations.remove(&id);
            ClusterResponse::Ok
        }

        ClusterCommand::ConnectorCreated { name, connector } => {
            state.connectors.insert(name, connector);
            ClusterResponse::Ok
        }

        ClusterCommand::ConnectorUpdated { name, connector } => {
            state.connectors.insert(name, connector);
            ClusterResponse::Ok
        }

        ClusterCommand::ConnectorRemoved { name } => {
            state.connectors.remove(&name);
            ClusterResponse::Ok
        }

        ClusterCommand::ScalingPolicySet { policy } => {
            state.scaling_policy = policy;
            ClusterResponse::Ok
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_register_worker() {
        let mut state = CoordinatorState::default();
        let cmd = ClusterCommand::RegisterWorker {
            id: "w1".into(),
            address: "http://localhost:9000".into(),
            api_key: "key".into(),
            capacity: crate::worker::WorkerCapacity {
                cpu_cores: 4,
                pipelines_running: 0,
                max_pipelines: 100,
            },
        };
        let resp = apply_command(&mut state, cmd);
        assert!(matches!(resp, ClusterResponse::Ok));
        assert_eq!(state.workers.len(), 1);
        assert_eq!(state.workers["w1"].status, "ready");
    }

    #[test]
    fn test_apply_deregister_worker() {
        let mut state = CoordinatorState::default();
        state.workers.insert(
            "w1".into(),
            WorkerEntry {
                id: "w1".into(),
                address: "http://localhost:9000".into(),
                api_key: "key".into(),
                status: "ready".into(),
                cpu_cores: 4,
                pipelines_running: 0,
                max_pipelines: 100,
                assigned_pipelines: vec![],
                events_processed: 0,
            },
        );
        let cmd = ClusterCommand::DeregisterWorker { id: "w1".into() };
        apply_command(&mut state, cmd);
        assert!(state.workers.is_empty());
    }

    #[test]
    fn test_apply_worker_status_changed() {
        let mut state = CoordinatorState::default();
        state.workers.insert(
            "w1".into(),
            WorkerEntry {
                id: "w1".into(),
                address: "http://localhost:9000".into(),
                api_key: "key".into(),
                status: "ready".into(),
                cpu_cores: 4,
                pipelines_running: 0,
                max_pipelines: 100,
                assigned_pipelines: vec![],
                events_processed: 0,
            },
        );
        let cmd = ClusterCommand::WorkerStatusChanged {
            id: "w1".into(),
            status: "unhealthy".into(),
        };
        apply_command(&mut state, cmd);
        assert_eq!(state.workers["w1"].status, "unhealthy");
    }

    #[test]
    fn test_apply_connector_crud() {
        let mut state = CoordinatorState::default();

        let connector = ClusterConnector {
            name: "mqtt1".into(),
            connector_type: "mqtt".into(),
            params: [("host".into(), "localhost".into())].into(),
            description: None,
        };

        apply_command(
            &mut state,
            ClusterCommand::ConnectorCreated {
                name: "mqtt1".into(),
                connector: connector.clone(),
            },
        );
        assert_eq!(state.connectors.len(), 1);

        apply_command(
            &mut state,
            ClusterCommand::ConnectorRemoved {
                name: "mqtt1".into(),
            },
        );
        assert!(state.connectors.is_empty());
    }

    #[test]
    fn test_apply_group_lifecycle() {
        let mut state = CoordinatorState::default();

        apply_command(
            &mut state,
            ClusterCommand::GroupDeployed {
                name: "g1".into(),
                group: serde_json::json!({"status": "running"}),
            },
        );
        assert_eq!(state.pipeline_groups.len(), 1);

        apply_command(
            &mut state,
            ClusterCommand::GroupRemoved { name: "g1".into() },
        );
        assert!(state.pipeline_groups.is_empty());
    }

    #[test]
    fn test_apply_scaling_policy() {
        let mut state = CoordinatorState::default();

        apply_command(
            &mut state,
            ClusterCommand::ScalingPolicySet {
                policy: Some(serde_json::json!({"min_workers": 2})),
            },
        );
        assert!(state.scaling_policy.is_some());

        apply_command(
            &mut state,
            ClusterCommand::ScalingPolicySet { policy: None },
        );
        assert!(state.scaling_policy.is_none());
    }
}
