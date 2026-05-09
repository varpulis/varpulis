//! Replicated coordinator state and command application logic.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::{ClusterCommand, ClusterResponse};
use crate::connector_config::ClusterConnector;

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
    #[serde(default)]
    pub models: HashMap<String, crate::model_registry::ModelRegistryEntry>,
    /// Per-worker pipeline metrics from heartbeats (replicated for monitoring).
    #[serde(default)]
    pub worker_pipeline_metrics: HashMap<String, Vec<crate::worker::PipelineMetrics>>,
    /// Latest durable distributed checkpoint id per pipeline group.
    ///
    /// Populated from
    /// [`ClusterCommand::CheckpointCompleted`](super::ClusterCommand::CheckpointCompleted).
    /// On coordinator recovery, the value here points to the most recent
    /// assembled snapshot in the shared state store
    /// (`{prefix}/{group}/{id}.json`).
    #[serde(default)]
    pub latest_checkpoints: HashMap<String, GroupCheckpointStatus>,
}

/// Per-group checkpoint status replicated through Raft.
///
/// `latest_completed` is the last id that was durably persisted; it is the id
/// recovery should restore from. `last_aborted` records the most recent
/// abort for observability — it does not roll back `latest_completed`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GroupCheckpointStatus {
    /// Most recent successfully-persisted checkpoint id, if any.
    #[serde(default)]
    pub latest_completed: Option<u64>,
    /// Most recent aborted checkpoint id, if any.
    #[serde(default)]
    pub last_aborted: Option<u64>,
    /// Recorded reason for the most recent abort.
    #[serde(default)]
    pub last_abort_reason: Option<String>,
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

        ClusterCommand::WorkerPipelinesUpdated {
            id,
            assigned_pipelines,
        } => {
            if let Some(w) = state.workers.get_mut(&id) {
                w.assigned_pipelines = assigned_pipelines;
            }
            ClusterResponse::Ok
        }

        ClusterCommand::WorkerMetricsUpdated {
            id,
            events_processed,
            pipelines_running,
            pipeline_metrics,
        } => {
            if let Some(w) = state.workers.get_mut(&id) {
                w.events_processed = events_processed;
                w.pipelines_running = pipelines_running;
            }
            if !pipeline_metrics.is_empty() {
                state.worker_pipeline_metrics.insert(id, pipeline_metrics);
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

        ClusterCommand::ModelRegistered { name, entry } => {
            state.models.insert(name, entry);
            ClusterResponse::Ok
        }

        ClusterCommand::ModelRemoved { name } => {
            state.models.remove(&name);
            ClusterResponse::Ok
        }

        #[cfg(feature = "distributed-checkpoint")]
        ClusterCommand::CheckpointCompleted {
            group_id,
            checkpoint_id,
        } => {
            let entry = state.latest_checkpoints.entry(group_id).or_default();
            // Monotonic: never go backwards if a stale entry replays.
            if entry.latest_completed.is_none_or(|cur| checkpoint_id > cur) {
                entry.latest_completed = Some(checkpoint_id);
            }
            ClusterResponse::Ok
        }

        #[cfg(feature = "distributed-checkpoint")]
        ClusterCommand::CheckpointAborted {
            group_id,
            checkpoint_id,
            reason,
        } => {
            let entry = state.latest_checkpoints.entry(group_id).or_default();
            entry.last_aborted = Some(checkpoint_id);
            entry.last_abort_reason = Some(reason);
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
                connector,
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
    fn test_apply_worker_metrics_updated() {
        use crate::worker::PipelineMetrics;

        let mut state = CoordinatorState::default();
        // Register a worker first
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
        apply_command(&mut state, cmd);
        assert_eq!(state.workers["w1"].events_processed, 0);
        assert_eq!(state.workers["w1"].pipelines_running, 0);

        // Update metrics via heartbeat replication (with pipeline metrics)
        let cmd = ClusterCommand::WorkerMetricsUpdated {
            id: "w1".into(),
            events_processed: 5000,
            pipelines_running: 3,
            pipeline_metrics: vec![PipelineMetrics {
                pipeline_name: "test-pipeline".into(),
                events_in: 5000,
                events_out: 100,
                connector_health: vec![],
            }],
        };
        apply_command(&mut state, cmd);
        assert_eq!(state.workers["w1"].events_processed, 5000);
        assert_eq!(state.workers["w1"].pipelines_running, 3);
        assert_eq!(state.worker_pipeline_metrics["w1"].len(), 1);
        assert_eq!(state.worker_pipeline_metrics["w1"][0].events_in, 5000);

        // Update again — values should change
        let cmd = ClusterCommand::WorkerMetricsUpdated {
            id: "w1".into(),
            events_processed: 12000,
            pipelines_running: 2,
            pipeline_metrics: vec![],
        };
        apply_command(&mut state, cmd);
        assert_eq!(state.workers["w1"].events_processed, 12000);
        assert_eq!(state.workers["w1"].pipelines_running, 2);
        // Empty pipeline_metrics should not overwrite existing data
        assert_eq!(state.worker_pipeline_metrics["w1"].len(), 1);
    }

    #[test]
    fn test_apply_worker_metrics_updated_unknown_worker() {
        let mut state = CoordinatorState::default();
        // Updating metrics for a non-existent worker should not panic
        let cmd = ClusterCommand::WorkerMetricsUpdated {
            id: "unknown".into(),
            events_processed: 100,
            pipelines_running: 1,
            pipeline_metrics: vec![],
        };
        let resp = apply_command(&mut state, cmd);
        assert!(matches!(resp, ClusterResponse::Ok));
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

    #[cfg(feature = "distributed-checkpoint")]
    #[test]
    fn test_apply_checkpoint_completed() {
        let mut state = CoordinatorState::default();
        apply_command(
            &mut state,
            ClusterCommand::CheckpointCompleted {
                group_id: "g1".into(),
                checkpoint_id: 7,
            },
        );
        let entry = state.latest_checkpoints.get("g1").unwrap();
        assert_eq!(entry.latest_completed, Some(7));
        assert!(entry.last_aborted.is_none());

        // Advancing forward updates the value.
        apply_command(
            &mut state,
            ClusterCommand::CheckpointCompleted {
                group_id: "g1".into(),
                checkpoint_id: 8,
            },
        );
        assert_eq!(state.latest_checkpoints["g1"].latest_completed, Some(8));

        // Stale-id replays must not roll the pointer backwards.
        apply_command(
            &mut state,
            ClusterCommand::CheckpointCompleted {
                group_id: "g1".into(),
                checkpoint_id: 3,
            },
        );
        assert_eq!(state.latest_checkpoints["g1"].latest_completed, Some(8));
    }

    #[cfg(feature = "distributed-checkpoint")]
    #[test]
    fn test_apply_checkpoint_aborted() {
        let mut state = CoordinatorState::default();
        apply_command(
            &mut state,
            ClusterCommand::CheckpointAborted {
                group_id: "g1".into(),
                checkpoint_id: 9,
                reason: "ack timeout".into(),
            },
        );
        let entry = state.latest_checkpoints.get("g1").unwrap();
        assert_eq!(entry.last_aborted, Some(9));
        assert_eq!(entry.last_abort_reason.as_deref(), Some("ack timeout"));
        // Abort does not populate latest_completed.
        assert!(entry.latest_completed.is_none());
    }

    #[cfg(feature = "distributed-checkpoint")]
    #[test]
    fn test_apply_checkpoint_completed_then_aborted_preserves_latest() {
        // An abort after a successful completion records the abort but does
        // not roll back the recovery pointer.
        let mut state = CoordinatorState::default();
        apply_command(
            &mut state,
            ClusterCommand::CheckpointCompleted {
                group_id: "g".into(),
                checkpoint_id: 4,
            },
        );
        apply_command(
            &mut state,
            ClusterCommand::CheckpointAborted {
                group_id: "g".into(),
                checkpoint_id: 5,
                reason: "kafka NACK".into(),
            },
        );
        let entry = &state.latest_checkpoints["g"];
        assert_eq!(entry.latest_completed, Some(4));
        assert_eq!(entry.last_aborted, Some(5));
    }

    #[cfg(feature = "distributed-checkpoint")]
    #[test]
    fn test_checkpoints_are_per_group() {
        let mut state = CoordinatorState::default();
        apply_command(
            &mut state,
            ClusterCommand::CheckpointCompleted {
                group_id: "alpha".into(),
                checkpoint_id: 1,
            },
        );
        apply_command(
            &mut state,
            ClusterCommand::CheckpointCompleted {
                group_id: "beta".into(),
                checkpoint_id: 2,
            },
        );
        assert_eq!(state.latest_checkpoints["alpha"].latest_completed, Some(1));
        assert_eq!(state.latest_checkpoints["beta"].latest_completed, Some(2));
    }
}
