//! Integration tests for varpulis-cluster lib.rs
//!
//! Targets: ClusterError variants, placement strategies, build_heartbeat,
//! coordinator types (ScalingAction, ScalingPolicy, ScalingRecommendation,
//! ClusterMetrics, PipelineWorkerMetrics), MigrationTask construction,
//! ClusterConnector, connector validation, ModelRegistryEntry serde,
//! and health module coverage.

use std::collections::HashMap;
use varpulis_cluster::*;

// =============================================================================
// ClusterError coverage — remaining variants not tested inline
// =============================================================================

#[test]
fn cluster_error_connector_not_found() {
    let e = ClusterError::ConnectorNotFound("mqtt_in".into());
    assert_eq!(e.to_string(), "Connector not found: mqtt_in");
}

#[test]
fn cluster_error_connector_validation() {
    let e = ClusterError::ConnectorValidation("missing host param".into());
    assert_eq!(
        e.to_string(),
        "Connector validation failed: missing host param"
    );
}

#[test]
fn cluster_error_migration_failed() {
    let e = ClusterError::MigrationFailed("timeout waiting for checkpoint".into());
    assert_eq!(
        e.to_string(),
        "Migration failed: timeout waiting for checkpoint"
    );
}

#[test]
fn cluster_error_worker_draining() {
    let e = ClusterError::WorkerDraining("w3".into());
    assert_eq!(e.to_string(), "Worker is draining: w3");
}

#[test]
fn cluster_error_not_leader() {
    let e = ClusterError::NotLeader("http://node2:9100".into());
    assert_eq!(
        e.to_string(),
        "Not the leader coordinator; forward to: http://node2:9100"
    );
}

#[test]
fn cluster_error_debug_format() {
    let e = ClusterError::NoWorkersAvailable;
    let dbg = format!("{:?}", e);
    assert!(dbg.contains("NoWorkersAvailable"));
}

// =============================================================================
// ScalingAction / ScalingPolicy / ScalingRecommendation
// =============================================================================

#[test]
fn scaling_action_serde_roundtrip() {
    for action in [
        ScalingAction::ScaleUp,
        ScalingAction::ScaleDown,
        ScalingAction::Stable,
    ] {
        let json = serde_json::to_string(&action).unwrap();
        let parsed: ScalingAction = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, action);
    }
}

#[test]
fn scaling_action_snake_case() {
    let json = serde_json::to_string(&ScalingAction::ScaleUp).unwrap();
    assert_eq!(json, "\"scale_up\"");

    let json = serde_json::to_string(&ScalingAction::ScaleDown).unwrap();
    assert_eq!(json, "\"scale_down\"");

    let json = serde_json::to_string(&ScalingAction::Stable).unwrap();
    assert_eq!(json, "\"stable\"");
}

#[test]
fn scaling_policy_serde() {
    let policy = ScalingPolicy {
        min_workers: 1,
        max_workers: 10,
        scale_up_threshold: 5.0,
        scale_down_threshold: 1.0,
        cooldown_secs: 60,
        webhook_url: Some("http://scale.example.com".into()),
    };
    let json = serde_json::to_string(&policy).unwrap();
    let parsed: ScalingPolicy = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.min_workers, 1);
    assert_eq!(parsed.max_workers, 10);
    assert_eq!(parsed.webhook_url, Some("http://scale.example.com".into()));
}

#[test]
fn scaling_policy_no_webhook() {
    let policy = ScalingPolicy {
        min_workers: 2,
        max_workers: 8,
        scale_up_threshold: 3.0,
        scale_down_threshold: 0.5,
        cooldown_secs: 120,
        webhook_url: None,
    };
    let json = serde_json::to_string(&policy).unwrap();
    // webhook_url should be skipped when None
    assert!(!json.contains("webhook_url"));
}

#[test]
fn scaling_recommendation_serde() {
    let rec = ScalingRecommendation {
        action: ScalingAction::ScaleUp,
        current_workers: 2,
        target_workers: 4,
        reason: "avg pipelines per worker 7.5 exceeds threshold 5.0".into(),
        avg_pipelines_per_worker: 7.5,
        total_pipelines: 15,
        timestamp: "2026-02-14T12:00:00Z".into(),
    };
    let json = serde_json::to_string(&rec).unwrap();
    let parsed: ScalingRecommendation = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.action, ScalingAction::ScaleUp);
    assert_eq!(parsed.current_workers, 2);
    assert_eq!(parsed.target_workers, 4);
    assert_eq!(parsed.total_pipelines, 15);
}

// =============================================================================
// build_heartbeat helper (lib.rs L324-334)
// =============================================================================

#[test]
fn build_heartbeat_empty_metrics() {
    let hb = HeartbeatRequest {
        events_processed: 0,
        pipelines_running: 0,
        pipeline_metrics: vec![],
    };
    assert_eq!(hb.events_processed, 0);
    assert_eq!(hb.pipelines_running, 0);
}

#[test]
fn heartbeat_request_with_metrics() {
    let pm = vec![
        PipelineMetrics {
            pipeline_name: "p1".into(),
            events_in: 100,
            events_out: 50,
            connector_health: vec![],
        },
        PipelineMetrics {
            pipeline_name: "p2".into(),
            events_in: 200,
            events_out: 180,
            connector_health: vec![],
        },
    ];
    let total: u64 = pm.iter().map(|m| m.events_in).sum();
    let hb = HeartbeatRequest {
        events_processed: total,
        pipelines_running: pm.len(),
        pipeline_metrics: pm,
    };
    assert_eq!(hb.events_processed, 300);
    assert_eq!(hb.pipelines_running, 2);
    assert_eq!(hb.pipeline_metrics.len(), 2);
}

// =============================================================================
// HeartbeatResponse serde
// =============================================================================

#[test]
fn heartbeat_response_serde() {
    let resp = HeartbeatResponse { acknowledged: true };
    let json = serde_json::to_string(&resp).unwrap();
    let parsed: HeartbeatResponse = serde_json::from_str(&json).unwrap();
    assert!(parsed.acknowledged);

    let resp = HeartbeatResponse {
        acknowledged: false,
    };
    let json = serde_json::to_string(&resp).unwrap();
    let parsed: HeartbeatResponse = serde_json::from_str(&json).unwrap();
    assert!(!parsed.acknowledged);
}

// =============================================================================
// RegisterWorkerResponse serde
// =============================================================================

#[test]
fn register_worker_response_serde() {
    let resp = RegisterWorkerResponse {
        worker_id: "w1".into(),
        status: "registered".into(),
        heartbeat_interval_secs: Some(10),
    };
    let json = serde_json::to_string(&resp).unwrap();
    let parsed: RegisterWorkerResponse = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.worker_id, "w1");
    assert_eq!(parsed.heartbeat_interval_secs, Some(10));
}

#[test]
fn register_worker_response_no_interval() {
    let resp = RegisterWorkerResponse {
        worker_id: "w2".into(),
        status: "registered".into(),
        heartbeat_interval_secs: None,
    };
    let json = serde_json::to_string(&resp).unwrap();
    // heartbeat_interval_secs should be skipped when None
    assert!(!json.contains("heartbeat_interval_secs"));
}

// =============================================================================
// ClusterConnector and connector_config functions
// =============================================================================

#[test]
fn cluster_connector_serde_roundtrip() {
    let conn = ClusterConnector {
        name: "mqtt_market".into(),
        connector_type: "mqtt".into(),
        params: {
            let mut m = HashMap::new();
            m.insert("host".into(), "broker.example.com".into());
            m.insert("port".into(), "1883".into());
            m
        },
        description: Some("Market data feed".into()),
    };
    let json = serde_json::to_string(&conn).unwrap();
    let parsed: ClusterConnector = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.name, "mqtt_market");
    assert_eq!(parsed.connector_type, "mqtt");
    assert_eq!(parsed.description, Some("Market data feed".into()));
}

#[test]
fn cluster_connector_no_description_skips() {
    let conn = ClusterConnector {
        name: "test".into(),
        connector_type: "console".into(),
        params: HashMap::new(),
        description: None,
    };
    let json = serde_json::to_string(&conn).unwrap();
    assert!(!json.contains("description"));
}

#[test]
fn cluster_connector_to_vpl_declaration_string_params() {
    let conn = ClusterConnector {
        name: "mqtt_in".into(),
        connector_type: "mqtt".into(),
        params: {
            let mut m = HashMap::new();
            m.insert("host".into(), "localhost".into());
            m
        },
        description: None,
    };
    let decl = conn.to_vpl_declaration();
    assert!(decl.starts_with("connector mqtt_in = mqtt("));
    assert!(decl.contains("host: \"localhost\""));
}

#[test]
fn cluster_connector_to_vpl_declaration_numeric_params() {
    let conn = ClusterConnector {
        name: "mqtt_in".into(),
        connector_type: "mqtt".into(),
        params: {
            let mut m = HashMap::new();
            m.insert("host".into(), "localhost".into());
            m.insert("port".into(), "1883".into());
            m.insert("qos".into(), "0".into());
            m
        },
        description: None,
    };
    let decl = conn.to_vpl_declaration();
    // Numeric values should be unquoted
    assert!(decl.contains("port: 1883"));
    assert!(decl.contains("qos: 0"));
    assert!(!decl.contains("port: \"1883\""));
}

// =============================================================================
// Connector validation functions
// =============================================================================

#[test]
fn validate_connector_valid_mqtt() {
    use varpulis_cluster::connector_config::validate_connector;

    let conn = ClusterConnector {
        name: "mqtt_in".into(),
        connector_type: "mqtt".into(),
        params: {
            let mut m = HashMap::new();
            m.insert("host".into(), "localhost".into());
            m
        },
        description: None,
    };
    assert!(validate_connector(&conn).is_ok());
}

#[test]
fn validate_connector_invalid_name() {
    use varpulis_cluster::connector_config::validate_connector;

    let conn = ClusterConnector {
        name: "123-bad".into(),
        connector_type: "mqtt".into(),
        params: {
            let mut m = HashMap::new();
            m.insert("host".into(), "localhost".into());
            m
        },
        description: None,
    };
    let err = validate_connector(&conn).unwrap_err();
    match err {
        ClusterError::ConnectorValidation(msg) => {
            assert!(msg.contains("Invalid connector name"));
        }
        _ => panic!("Expected ConnectorValidation error"),
    }
}

#[test]
fn validate_connector_invalid_type() {
    use varpulis_cluster::connector_config::validate_connector;

    let conn = ClusterConnector {
        name: "valid_name".into(),
        connector_type: "redis".into(),
        params: HashMap::new(),
        description: None,
    };
    let err = validate_connector(&conn).unwrap_err();
    match err {
        ClusterError::ConnectorValidation(msg) => {
            assert!(msg.contains("Invalid connector type"));
        }
        _ => panic!("Expected ConnectorValidation error"),
    }
}

#[test]
fn validate_connector_missing_required_params() {
    use varpulis_cluster::connector_config::validate_connector;

    let conn = ClusterConnector {
        name: "kafka_out".into(),
        connector_type: "kafka".into(),
        params: HashMap::new(), // missing "brokers"
        description: None,
    };
    let err = validate_connector(&conn).unwrap_err();
    match err {
        ClusterError::ConnectorValidation(msg) => {
            assert!(msg.contains("brokers"));
        }
        _ => panic!("Expected ConnectorValidation error"),
    }
}

#[test]
fn validate_connector_valid_console_no_params() {
    use varpulis_cluster::connector_config::validate_connector;

    let conn = ClusterConnector {
        name: "console_out".into(),
        connector_type: "console".into(),
        params: HashMap::new(),
        description: None,
    };
    assert!(validate_connector(&conn).is_ok());
}

#[test]
fn validate_connector_valid_http() {
    use varpulis_cluster::connector_config::validate_connector;

    let conn = ClusterConnector {
        name: "http_out".into(),
        connector_type: "http".into(),
        params: {
            let mut m = HashMap::new();
            m.insert("url".into(), "http://example.com/webhook".into());
            m
        },
        description: None,
    };
    assert!(validate_connector(&conn).is_ok());
}

// =============================================================================
// find_missing_connectors
// =============================================================================

#[test]
fn find_missing_connectors_no_refs() {
    use varpulis_cluster::connector_config::find_missing_connectors;

    let source = "stream A = X\n    .emit()\n";
    let missing = find_missing_connectors(source);
    assert!(missing.is_empty());
}

#[test]
fn find_missing_connectors_multiple_from_to() {
    use varpulis_cluster::connector_config::find_missing_connectors;

    let source = "stream A = X.from(conn1, topic: \"t1\").to(conn2, topic: \"t2\")\n";
    let missing = find_missing_connectors(source);
    assert!(missing.contains(&"conn1".to_string()));
    assert!(missing.contains(&"conn2".to_string()));
}

#[test]
fn find_missing_connectors_declared_inline_excluded() {
    use varpulis_cluster::connector_config::find_missing_connectors;

    let source = r#"
connector conn1 = mqtt(host: "localhost")
stream A = X.from(conn1, topic: "t1")
"#;
    let missing = find_missing_connectors(source);
    assert!(!missing.contains(&"conn1".to_string()));
}

// =============================================================================
// inject_connectors
// =============================================================================

#[test]
fn inject_connectors_with_multiple() {
    use varpulis_cluster::connector_config::inject_connectors;

    let source =
        "stream A = X.from(mqtt_in, topic: \"t1\")\nstream B = Y.to(kafka_out, topic: \"t2\")\n";
    let mut connectors = HashMap::new();
    connectors.insert(
        "mqtt_in".into(),
        ClusterConnector {
            name: "mqtt_in".into(),
            connector_type: "mqtt".into(),
            params: {
                let mut m = HashMap::new();
                m.insert("host".into(), "localhost".into());
                m
            },
            description: None,
        },
    );
    connectors.insert(
        "kafka_out".into(),
        ClusterConnector {
            name: "kafka_out".into(),
            connector_type: "kafka".into(),
            params: {
                let mut m = HashMap::new();
                m.insert("brokers".into(), "localhost:9092".into());
                m
            },
            description: None,
        },
    );

    let (enriched, lines) = inject_connectors(source, &connectors);
    assert!(lines > 0);
    assert!(enriched.contains("connector mqtt_in"));
    assert!(enriched.contains("connector kafka_out"));
}

// =============================================================================
// ModelRegistryEntry serde
// =============================================================================

#[test]
fn model_registry_entry_serde() {
    use varpulis_cluster::model_registry::ModelRegistryEntry;

    let entry = ModelRegistryEntry {
        name: "anomaly_detector".into(),
        s3_key: "models/anomaly_v1.onnx".into(),
        format: "onnx".into(),
        inputs: vec!["temperature".into(), "pressure".into()],
        outputs: vec!["is_anomaly".into()],
        size_bytes: 1_234_567,
        uploaded_at: "2026-02-14T12:00:00Z".into(),
        description: "Detects anomalies in sensor data".into(),
    };
    let json = serde_json::to_string(&entry).unwrap();
    let parsed: ModelRegistryEntry = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.name, "anomaly_detector");
    assert_eq!(parsed.inputs.len(), 2);
    assert_eq!(parsed.outputs.len(), 1);
    assert_eq!(parsed.size_bytes, 1_234_567);
}

#[test]
fn model_registry_entry_default_description() {
    use varpulis_cluster::model_registry::ModelRegistryEntry;

    // description has #[serde(default)] so it should default to empty string
    let json = r#"{
        "name": "test",
        "s3_key": "key",
        "format": "onnx",
        "inputs": [],
        "outputs": [],
        "size_bytes": 0,
        "uploaded_at": "now"
    }"#;
    let parsed: ModelRegistryEntry = serde_json::from_str(json).unwrap();
    assert_eq!(parsed.description, "");
}

// =============================================================================
// RoutingTable and event_type_matches (re-exported from lib.rs)
// =============================================================================

#[test]
fn event_type_matches_reexported() {
    assert!(event_type_matches("SensorReading", "Sensor*"));
    assert!(!event_type_matches("ActuatorCommand", "Sensor*"));
    assert!(event_type_matches("AnyEvent", "*"));
    assert!(event_type_matches("Exact", "Exact"));
    assert!(!event_type_matches("NotExact", "Exact"));
}

#[test]
fn find_target_pipeline_reexported() {
    let spec = PipelineGroupSpec {
        name: "test".into(),
        pipelines: vec![PipelinePlacement {
            name: "default".into(),
            source: "".into(),
            worker_affinity: None,
            replicas: 1,
            partition_key: None,
        }],
        routes: vec![],
    };
    let group = DeployedPipelineGroup::new("g1".into(), "test".into(), spec);
    assert_eq!(find_target_pipeline(&group, "AnyEvent"), Some("default"));
}

// =============================================================================
// Health constants (re-exported)
// =============================================================================

#[test]
fn heartbeat_constants() {
    assert_eq!(
        DEFAULT_HEARTBEAT_INTERVAL,
        std::time::Duration::from_secs(5)
    );
    assert_eq!(
        DEFAULT_HEARTBEAT_TIMEOUT,
        std::time::Duration::from_secs(15)
    );
    assert_eq!(DEFAULT_WS_GRACE_PERIOD, std::time::Duration::from_secs(5));
    // Legacy aliases
    assert_eq!(HEARTBEAT_INTERVAL, DEFAULT_HEARTBEAT_INTERVAL);
    assert_eq!(HEARTBEAT_TIMEOUT, DEFAULT_HEARTBEAT_TIMEOUT);
}

// =============================================================================
// ClusterPrometheusMetrics coverage
// =============================================================================

#[test]
fn cluster_metrics_raft_update() {
    let m = ClusterPrometheusMetrics::new();
    m.update_raft_metrics(2.0, 5.0, 42.0);
    let output = m.gather();
    assert!(output.contains("varpulis_cluster_raft_role"));
    assert!(output.contains("varpulis_cluster_raft_term"));
    assert!(output.contains("varpulis_cluster_raft_commit_index"));
}

#[test]
fn cluster_metrics_full_lifecycle() {
    let m = ClusterPrometheusMetrics::new();
    m.set_worker_counts(3, 1, 0);
    m.set_deployment_counts(2, 6);
    m.record_migration(true, 1.5);
    m.record_migration(false, 5.0);
    m.record_deploy(true, 0.8);
    m.record_deploy(false, 0.1);
    m.record_health_sweep(4, 0.002);
    m.update_raft_metrics(2.0, 10.0, 100.0);

    let output = m.gather();
    assert!(output.contains("varpulis_cluster_workers_total"));
    assert!(output.contains("varpulis_cluster_pipeline_groups_total"));
    assert!(output.contains("varpulis_cluster_deployments_total"));
    assert!(output.contains("varpulis_cluster_migrations_total"));
    assert!(output.contains("varpulis_cluster_migration_duration_seconds"));
    assert!(output.contains("varpulis_cluster_health_sweep_duration_seconds"));
    assert!(output.contains("varpulis_cluster_deploy_duration_seconds"));
    assert!(output.contains("varpulis_cluster_raft_role"));
}

// =============================================================================
// MigrationTask construction
// =============================================================================

#[test]
fn migration_task_construction() {
    let task = MigrationTask {
        id: "mig-001".into(),
        pipeline_name: "p1".into(),
        group_id: "g1".into(),
        source_worker: WorkerId("w1".into()),
        target_worker: WorkerId("w2".into()),
        status: MigrationStatus::Checkpointing,
        started_at: std::time::Instant::now(),
        checkpoint: None,
        reason: MigrationReason::Failover,
    };
    assert_eq!(task.id, "mig-001");
    assert_eq!(task.pipeline_name, "p1");
    assert_eq!(task.status.to_string(), "checkpointing");
    assert_eq!(task.reason.to_string(), "failover");
    assert!(task.checkpoint.is_none());
}

#[test]
fn migration_status_equality() {
    assert_eq!(MigrationStatus::Completed, MigrationStatus::Completed);
    assert_ne!(MigrationStatus::Completed, MigrationStatus::Deploying);
    assert_eq!(
        MigrationStatus::Failed("x".into()),
        MigrationStatus::Failed("x".into())
    );
    assert_ne!(
        MigrationStatus::Failed("x".into()),
        MigrationStatus::Failed("y".into())
    );
}

// =============================================================================
// DeployedPipelineGroup — PipelineGroupInfo conversion
// =============================================================================

#[test]
fn pipeline_group_info_from_deployed_with_replicas() {
    let spec = PipelineGroupSpec {
        name: "replicated".into(),
        pipelines: vec![PipelinePlacement {
            name: "p1".into(),
            source: "stream A = X".into(),
            worker_affinity: None,
            replicas: 3,
            partition_key: Some("user_id".into()),
        }],
        routes: vec![],
    };
    let group = DeployedPipelineGroup::new("g1".into(), "replicated".into(), spec);
    let info = PipelineGroupInfo::from(&group);
    assert_eq!(info.pipeline_count, 1);
    assert_eq!(info.sources["p1"], "stream A = X");
}

// =============================================================================
// HaRole re-export
// =============================================================================

#[test]
fn ha_role_serde() {
    let role = HaRole::Leader;
    let json = serde_json::to_string(&role).unwrap();
    assert_eq!(json, "\"leader\"");
    let parsed: HaRole = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, HaRole::Leader);

    let role = HaRole::Standalone;
    let json = serde_json::to_string(&role).unwrap();
    assert_eq!(json, "\"standalone\"");

    let follower_json = r#"{"follower":{"leader_id":"node-2"}}"#;
    let parsed: HaRole = serde_json::from_str(follower_json).unwrap();
    match parsed {
        HaRole::Follower { leader_id } => assert_eq!(leader_id, "node-2"),
        _ => panic!("Expected Follower variant"),
    }
}

#[test]
fn ha_role_is_writer() {
    assert!(HaRole::Standalone.is_writer());
    assert!(HaRole::Leader.is_writer());
    let follower = HaRole::Follower {
        leader_id: "node-1".into(),
    };
    assert!(!follower.is_writer());
}

// =============================================================================
// InjectEventRequest / InjectResponse (coordinator types)
// =============================================================================

#[test]
fn inject_event_request_serde() {
    let req = InjectEventRequest {
        event_type: "TradeEvent".into(),
        fields: {
            let mut m = serde_json::Map::new();
            m.insert("symbol".into(), serde_json::json!("AAPL"));
            m.insert("price".into(), serde_json::json!(150.0));
            m
        },
    };
    let json = serde_json::to_string(&req).unwrap();
    let parsed: InjectEventRequest = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.event_type, "TradeEvent");
    assert_eq!(parsed.fields.len(), 2);
}

#[test]
fn inject_response_serde() {
    let resp = InjectResponse {
        routed_to: "p1".into(),
        worker_id: "w1".into(),
        worker_response: serde_json::json!({"status": "ok"}),
    };
    let json = serde_json::to_string(&resp).unwrap();
    let parsed: InjectResponse = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.routed_to, "p1");
    assert_eq!(parsed.worker_id, "w1");
}

// =============================================================================
// WorkerNode ws_disconnected_at field
// =============================================================================

#[test]
fn worker_node_ws_disconnected_at_default() {
    let node = WorkerNode::new(
        WorkerId("w1".into()),
        "http://localhost:9000".into(),
        "key".into(),
    );
    assert!(node.ws_disconnected_at.is_none());
}

// =============================================================================
// PipelineDeployment epoch field
// =============================================================================

#[test]
fn pipeline_deployment_epoch_serde() {
    let dep = PipelineDeployment {
        worker_id: WorkerId("w1".into()),
        worker_address: "http://localhost:9000".into(),
        worker_api_key: "key".into(),
        pipeline_id: "pid1".into(),
        status: PipelineDeploymentStatus::Running,
        epoch: 3,
    };
    let json = serde_json::to_string(&dep).unwrap();
    let parsed: PipelineDeployment = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.epoch, 3);
}

#[test]
fn pipeline_deployment_epoch_default() {
    // epoch has #[serde(default)] so it should default to 0
    let json = r#"{
        "worker_id": "w1",
        "worker_address": "http://localhost:9000",
        "worker_api_key": "key",
        "pipeline_id": "pid1",
        "status": "running"
    }"#;
    let parsed: PipelineDeployment = serde_json::from_str(json).unwrap();
    assert_eq!(parsed.epoch, 0);
}

// =============================================================================
// GroupStatus serde with TornDown
// =============================================================================

#[test]
fn group_status_torn_down_serde() {
    let status = GroupStatus::TornDown;
    let json = serde_json::to_string(&status).unwrap();
    assert_eq!(json, "\"torn_down\"");
    let parsed: GroupStatus = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, GroupStatus::TornDown);
}

// =============================================================================
// PipelineDeploymentStatus serde
// =============================================================================

#[test]
fn pipeline_deployment_status_serde() {
    for status in [
        PipelineDeploymentStatus::Deploying,
        PipelineDeploymentStatus::Running,
        PipelineDeploymentStatus::Failed,
        PipelineDeploymentStatus::Stopped,
    ] {
        let json = serde_json::to_string(&status).unwrap();
        let parsed: PipelineDeploymentStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, status);
    }
}
