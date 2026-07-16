//! Deterministic simulation tests for Raft state machine convergence.
//!
//! These tests verify that:
//! - All replicas converge to the same state after applying the same commands
//! - Command replay produces identical state regardless of ordering
//! - State snapshots serialize and deserialize faithfully
//! - Complex lifecycle sequences produce correct final state
//!
//! Requires the `raft` feature: `cargo test -p varpulis-cluster --features raft`
#![cfg(feature = "raft")]

use varpulis_cluster::raft::state_machine::{apply_command, CoordinatorState};
use varpulis_cluster::raft::{ClusterCommand, ClusterResponse};
use varpulis_cluster::worker::WorkerCapacity;

fn make_register(id: &str, cores: usize) -> ClusterCommand {
    ClusterCommand::RegisterWorker {
        id: id.into(),
        address: format!("http://{}:9000", id),
        api_key: format!("key-{id}"),
        capacity: WorkerCapacity {
            cpu_cores: cores,
            pipelines_running: 0,
            max_pipelines: 100,
        },
    }
}

fn make_status(id: &str, status: &str) -> ClusterCommand {
    ClusterCommand::WorkerStatusChanged {
        id: id.into(),
        status: status.into(),
    }
}

fn make_metrics(id: &str, events: u64, pipelines: usize) -> ClusterCommand {
    ClusterCommand::WorkerMetricsUpdated {
        id: id.into(),
        events_processed: events,
        pipelines_running: pipelines,
        pipeline_metrics: vec![],
        heartbeat_seq: events,
    }
}

fn make_deploy(name: &str) -> ClusterCommand {
    ClusterCommand::GroupDeployed {
        name: name.into(),
        group: serde_json::json!({ "name": name, "replicas": 1 }),
    }
}

fn make_connector(name: &str, connector_type: &str) -> ClusterCommand {
    let mut params = std::collections::HashMap::new();
    params.insert("host".to_string(), "localhost".to_string());
    ClusterCommand::ConnectorCreated {
        name: name.into(),
        connector: varpulis_cluster::connector_config::ClusterConnector {
            name: name.into(),
            connector_type: connector_type.into(),
            params,
            description: None,
        },
    }
}

// =========================================================================
// Convergence: N replicas applying same commands → same state
// =========================================================================

#[test]
fn three_replicas_converge_on_worker_lifecycle() {
    let commands = vec![
        make_register("w1", 4),
        make_register("w2", 8),
        make_metrics("w1", 1000, 2),
        make_status("w2", "unhealthy"),
        make_register("w3", 16),
        ClusterCommand::DeregisterWorker { id: "w2".into() },
        make_metrics("w3", 500, 1),
    ];

    let mut states: Vec<CoordinatorState> = (0..3).map(|_| CoordinatorState::default()).collect();

    for state in &mut states {
        for cmd in &commands {
            let resp = apply_command(state, cmd.clone());
            assert!(matches!(resp, ClusterResponse::Ok));
        }
    }

    // All replicas must have identical state (use Value comparison for order-independence)
    for i in 1..states.len() {
        let a = serde_json::to_value(&states[0]).unwrap();
        let b = serde_json::to_value(&states[i]).unwrap();
        assert_eq!(a, b, "Replica 0 and {} diverged", i);
    }

    // Verify expected final state
    assert_eq!(states[0].workers.len(), 2); // w1, w3 (w2 deregistered)
    assert_eq!(states[0].workers["w1"].events_processed, 1000);
    assert_eq!(states[0].workers["w3"].events_processed, 500);
    assert!(!states[0].workers.contains_key("w2"));
}

#[test]
fn five_replicas_converge_on_full_cluster_lifecycle() {
    let commands = vec![
        // Workers register
        make_register("w1", 4),
        make_register("w2", 8),
        make_register("w3", 16),
        // Deploy pipeline groups
        make_deploy("fraud-detection"),
        make_deploy("iot-monitoring"),
        // Create connectors
        make_connector("kafka-in", "kafka"),
        make_connector("mqtt-sensors", "mqtt"),
        // Worker metrics
        make_metrics("w1", 5000, 3),
        make_metrics("w2", 10000, 5),
        make_metrics("w3", 2000, 1),
        // Connector update
        ClusterCommand::ConnectorUpdated {
            name: "kafka-in".into(),
            connector: varpulis_cluster::connector_config::ClusterConnector {
                name: "kafka-in".into(),
                connector_type: "kafka".into(),
                params: {
                    let mut p = std::collections::HashMap::new();
                    p.insert("host".to_string(), "new-broker".to_string());
                    p
                },
                description: Some("updated".into()),
            },
        },
        // Migration
        ClusterCommand::MigrationStarted {
            task: serde_json::json!({ "id": "mig-1", "from": "w1", "to": "w3", "pipeline": "fraud-detection" }),
        },
        ClusterCommand::MigrationUpdated {
            id: "mig-1".into(),
            status: "completed".into(),
        },
        ClusterCommand::MigrationRemoved { id: "mig-1".into() },
        // Scaling policy
        ClusterCommand::ScalingPolicySet {
            policy: Some(serde_json::json!({ "min_workers": 2, "max_workers": 10 })),
        },
        // Worker pipeline assignments
        ClusterCommand::WorkerPipelinesUpdated {
            id: "w1".into(),
            assigned_pipelines: vec!["iot-monitoring".into()],
        },
        ClusterCommand::WorkerPipelinesUpdated {
            id: "w3".into(),
            assigned_pipelines: vec!["fraud-detection".into()],
        },
        // Teardown
        ClusterCommand::GroupRemoved {
            name: "iot-monitoring".into(),
        },
        ClusterCommand::ConnectorRemoved {
            name: "mqtt-sensors".into(),
        },
        ClusterCommand::DeregisterWorker { id: "w2".into() },
    ];

    let mut states: Vec<CoordinatorState> = (0..5).map(|_| CoordinatorState::default()).collect();

    for state in &mut states {
        for cmd in &commands {
            apply_command(state, cmd.clone());
        }
    }

    // All 5 replicas must be identical (use Value comparison for order-independence)
    let reference = serde_json::to_value(&states[0]).unwrap();
    for (i, state) in states.iter().enumerate().skip(1) {
        let v = serde_json::to_value(state).unwrap();
        assert_eq!(reference, v, "Replica 0 and {} diverged", i);
    }

    // Final state assertions
    let s = &states[0];
    assert_eq!(s.workers.len(), 2); // w1, w3
    assert_eq!(s.pipeline_groups.len(), 1); // only fraud-detection
    assert!(s.pipeline_groups.contains_key("fraud-detection"));
    assert_eq!(s.connectors.len(), 1); // only kafka-in (updated)
    assert_eq!(s.connectors["kafka-in"].params["host"], "new-broker");
    assert!(s.active_migrations.is_empty()); // migration completed and removed
    assert!(s.scaling_policy.is_some());
    assert_eq!(
        s.workers["w3"].assigned_pipelines,
        vec!["fraud-detection".to_string()]
    );
}

// =========================================================================
// Snapshot: serialize → deserialize → identical state
// =========================================================================

#[test]
fn snapshot_round_trip_preserves_state() {
    let mut state = CoordinatorState::default();

    let commands = vec![
        make_register("w1", 4),
        make_register("w2", 8),
        make_deploy("stream-1"),
        make_connector("kafka-in", "kafka"),
        make_metrics("w1", 1000, 2),
        ClusterCommand::ModelRegistered {
            name: "anomaly-v1".into(),
            entry: varpulis_cluster::model_registry::ModelRegistryEntry {
                name: "anomaly-v1".into(),
                s3_key: "models/anomaly_v1.onnx".into(),
                format: "onnx".into(),
                inputs: vec!["temperature".into()],
                outputs: vec!["is_anomaly".into()],
                size_bytes: 12345,
                uploaded_at: "2026-01-01T00:00:00Z".into(),
                description: "test model".into(),
            },
        },
    ];

    for cmd in commands {
        apply_command(&mut state, cmd);
    }

    // Snapshot: serialize to JSON
    let snapshot_json = serde_json::to_string(&state).unwrap();

    // Restore from snapshot
    let restored: CoordinatorState = serde_json::from_str(&snapshot_json).unwrap();

    // Compare
    assert_eq!(restored.workers.len(), state.workers.len());
    assert_eq!(restored.pipeline_groups.len(), state.pipeline_groups.len());
    assert_eq!(restored.connectors.len(), state.connectors.len());
    assert_eq!(restored.models.len(), state.models.len());
    assert_eq!(
        restored.workers["w1"].events_processed,
        state.workers["w1"].events_processed
    );
    assert_eq!(restored.models["anomaly-v1"].format, "onnx");
}

// =========================================================================
// Idempotency: applying same command twice has consistent behavior
// =========================================================================

#[test]
fn register_worker_overwrites_on_duplicate() {
    let mut state = CoordinatorState::default();

    apply_command(&mut state, make_register("w1", 4));
    apply_command(&mut state, make_metrics("w1", 500, 2));

    // Re-register same worker (should overwrite)
    apply_command(&mut state, make_register("w1", 8));

    assert_eq!(state.workers.len(), 1);
    assert_eq!(state.workers["w1"].cpu_cores, 8);
    // Metrics are reset on re-register
    assert_eq!(state.workers["w1"].events_processed, 0);
}

#[test]
fn status_change_on_nonexistent_worker_is_no_op() {
    let mut state = CoordinatorState::default();
    let resp = apply_command(&mut state, make_status("nonexistent", "unhealthy"));
    assert!(matches!(resp, ClusterResponse::Ok));
    assert!(state.workers.is_empty());
}

#[test]
fn deregister_nonexistent_worker_is_no_op() {
    let mut state = CoordinatorState::default();
    let resp = apply_command(
        &mut state,
        ClusterCommand::DeregisterWorker { id: "ghost".into() },
    );
    assert!(matches!(resp, ClusterResponse::Ok));
}

// =========================================================================
// Complex lifecycle scenarios
// =========================================================================

#[test]
fn worker_failover_scenario() {
    let mut state = CoordinatorState::default();

    // Register 3 workers
    apply_command(&mut state, make_register("w1", 4));
    apply_command(&mut state, make_register("w2", 8));
    apply_command(&mut state, make_register("w3", 4));

    // Deploy pipeline to w1
    apply_command(&mut state, make_deploy("fraud"));
    apply_command(
        &mut state,
        ClusterCommand::WorkerPipelinesUpdated {
            id: "w1".into(),
            assigned_pipelines: vec!["fraud".into()],
        },
    );

    // w1 goes unhealthy
    apply_command(&mut state, make_status("w1", "unhealthy"));
    assert_eq!(state.workers["w1"].status, "unhealthy");

    // Migration starts: w1 → w2
    apply_command(
        &mut state,
        ClusterCommand::MigrationStarted {
            task: serde_json::json!({
                "id": "failover-1",
                "from": "w1",
                "to": "w2",
                "pipeline": "fraud"
            }),
        },
    );

    // Migration completes
    apply_command(
        &mut state,
        ClusterCommand::MigrationUpdated {
            id: "failover-1".into(),
            status: "completed".into(),
        },
    );

    // Update pipeline assignments
    apply_command(
        &mut state,
        ClusterCommand::WorkerPipelinesUpdated {
            id: "w1".into(),
            assigned_pipelines: vec![],
        },
    );
    apply_command(
        &mut state,
        ClusterCommand::WorkerPipelinesUpdated {
            id: "w2".into(),
            assigned_pipelines: vec!["fraud".into()],
        },
    );

    // Deregister failed worker
    apply_command(
        &mut state,
        ClusterCommand::DeregisterWorker { id: "w1".into() },
    );

    // Clean up migration
    apply_command(
        &mut state,
        ClusterCommand::MigrationRemoved {
            id: "failover-1".into(),
        },
    );

    // Final state
    assert_eq!(state.workers.len(), 2); // w2, w3
    assert!(!state.workers.contains_key("w1"));
    assert_eq!(state.workers["w2"].assigned_pipelines, vec!["fraud"]);
    assert!(state.active_migrations.is_empty());
}

#[test]
fn model_registry_lifecycle() {
    let mut state = CoordinatorState::default();

    // Register model v1
    apply_command(
        &mut state,
        ClusterCommand::ModelRegistered {
            name: "anomaly".into(),
            entry: varpulis_cluster::model_registry::ModelRegistryEntry {
                name: "anomaly".into(),
                s3_key: "models/anomaly_v1.onnx".into(),
                format: "onnx".into(),
                inputs: vec!["temperature".into()],
                outputs: vec!["is_anomaly".into()],
                size_bytes: 1000,
                uploaded_at: "2026-01-01T00:00:00Z".into(),
                description: "v1".into(),
            },
        },
    );
    assert_eq!(state.models.len(), 1);
    assert_eq!(state.models["anomaly"].description, "v1");

    // Update to v2 (same name = overwrite)
    apply_command(
        &mut state,
        ClusterCommand::ModelRegistered {
            name: "anomaly".into(),
            entry: varpulis_cluster::model_registry::ModelRegistryEntry {
                name: "anomaly".into(),
                s3_key: "models/anomaly_v2.onnx".into(),
                format: "onnx".into(),
                inputs: vec!["temperature".into(), "pressure".into()],
                outputs: vec!["is_anomaly".into()],
                size_bytes: 2000,
                uploaded_at: "2026-02-01T00:00:00Z".into(),
                description: "v2".into(),
            },
        },
    );
    assert_eq!(state.models.len(), 1);
    assert_eq!(state.models["anomaly"].description, "v2");

    // Remove
    apply_command(
        &mut state,
        ClusterCommand::ModelRemoved {
            name: "anomaly".into(),
        },
    );
    assert!(state.models.is_empty());
}

#[test]
fn scaling_policy_set_and_clear() {
    let mut state = CoordinatorState::default();

    // Set policy
    apply_command(
        &mut state,
        ClusterCommand::ScalingPolicySet {
            policy: Some(serde_json::json!({ "min": 2, "max": 10 })),
        },
    );
    assert!(state.scaling_policy.is_some());

    // Clear policy
    apply_command(
        &mut state,
        ClusterCommand::ScalingPolicySet { policy: None },
    );
    assert!(state.scaling_policy.is_none());
}
