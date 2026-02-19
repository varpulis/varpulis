//! Multi-worker E2E integration tests for the NATS cluster transport layer.
//!
//! Tests the full cluster lifecycle: coordinator + 2 workers over NATS —
//! registration, pipeline group deployment across workers, cross-worker event
//! routing, heartbeat tracking, and worker drain.
//!
//! Requires a real `nats-server` running at `nats://localhost:4222`.
//! In CI this is provided by a `services: nats` container; locally run
//! `nats-server &` before executing.

#![cfg(feature = "nats-transport")]

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use uuid::Uuid;

use varpulis_cluster::coordinator::Coordinator;
use varpulis_cluster::nats_coordinator::run_coordinator_nats_handler;
use varpulis_cluster::nats_transport::{
    connect_nats, nats_publish, nats_request, subject_cmd, subject_heartbeat, subject_pipeline,
    subject_register,
};
use varpulis_cluster::nats_worker::run_worker_nats_handler;
use varpulis_cluster::routing::{build_routing_table, event_type_matches};
use varpulis_cluster::worker::{WorkerNode, WorkerStatus};
use varpulis_cluster::{
    DeployedPipelineGroup, GroupStatus, HeartbeatRequest, InterPipelineRoute,
    PipelineDeploymentStatus, PipelineGroupSpec, PipelinePlacement, RegisterWorkerRequest,
    RegisterWorkerResponse, SharedCoordinator, WorkerCapacity, WorkerId,
};

const NATS_URL: &str = "nats://localhost:4222";

/// Helper: connect a raw async-nats client.
async fn raw_client() -> async_nats::Client {
    async_nats::connect(NATS_URL)
        .await
        .expect("Failed to connect to nats-server — is it running on localhost:4222?")
}

/// Helper: create a SharedCoordinator.
fn new_coordinator() -> SharedCoordinator {
    Arc::new(RwLock::new(Coordinator::new()))
}

/// Helper: create a TenantManager with a tenant for the given API key.
async fn new_tenant_manager(api_key: &str) -> varpulis_runtime::SharedTenantManager {
    let tm = Arc::new(RwLock::new(varpulis_runtime::TenantManager::new()));
    {
        let mut mgr = tm.write().await;
        mgr.create_tenant(
            format!("tenant-{}", Uuid::new_v4()),
            api_key.to_string(),
            varpulis_runtime::TenantQuota::default(),
        )
        .unwrap();
    }
    tm
}

// ============================================================================
// Test 1: Multi-worker registration and heartbeat tracking
// ============================================================================

#[tokio::test]
async fn test_multi_worker_registration_and_heartbeat() {
    let coordinator = new_coordinator();
    let client = connect_nats(NATS_URL).await.unwrap();

    // Start coordinator NATS handler
    let handler_client = client.clone();
    let handler_coord = coordinator.clone();
    let handler = tokio::spawn(async move {
        run_coordinator_nats_handler(handler_client, handler_coord).await;
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Register worker-0 and worker-1
    let w0_id = format!("mw-w0-{}", Uuid::new_v4());
    let w1_id = format!("mw-w1-{}", Uuid::new_v4());

    for (wid, addr) in [(&w0_id, "localhost:9000"), (&w1_id, "localhost:9001")] {
        let req = RegisterWorkerRequest {
            worker_id: wid.clone(),
            address: addr.to_string(),
            api_key: format!("key-{wid}"),
            capacity: WorkerCapacity::default(),
        };
        let resp: RegisterWorkerResponse =
            nats_request(&client, &subject_register(), &req, Duration::from_secs(5))
                .await
                .expect("registration failed");
        assert_eq!(resp.worker_id, *wid);
        assert_eq!(resp.status, "registered");
    }

    // Verify coordinator has 2 workers, both Ready
    {
        let coord = coordinator.read().await;
        assert_eq!(coord.workers.len(), 2);
        for wid in [&w0_id, &w1_id] {
            let w = coord
                .workers
                .get(&WorkerId(wid.clone()))
                .expect("worker not found");
            assert_eq!(w.status, WorkerStatus::Ready);
        }
    }

    // Send heartbeats with different metrics
    let hb0 = HeartbeatRequest {
        events_processed: 5000,
        pipelines_running: 3,
        pipeline_metrics: vec![],
    };
    nats_publish(&client, &subject_heartbeat(&w0_id), &hb0)
        .await
        .unwrap();

    let hb1 = HeartbeatRequest {
        events_processed: 12000,
        pipelines_running: 1,
        pipeline_metrics: vec![],
    };
    nats_publish(&client, &subject_heartbeat(&w1_id), &hb1)
        .await
        .unwrap();
    client.flush().await.unwrap();

    // Give coordinator time to process heartbeats
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify per-worker metrics
    {
        let coord = coordinator.read().await;
        let worker0 = coord
            .workers
            .get(&WorkerId(w0_id.clone()))
            .expect("w0 not found");
        assert_eq!(worker0.events_processed, 5000);
        assert_eq!(worker0.capacity.pipelines_running, 3);

        let worker1 = coord
            .workers
            .get(&WorkerId(w1_id.clone()))
            .expect("w1 not found");
        assert_eq!(worker1.events_processed, 12000);
        assert_eq!(worker1.capacity.pipelines_running, 1);
    }

    handler.abort();
}

// ============================================================================
// Test 2: Deploy pipeline group across 2 workers via 3-phase NATS deploy
// ============================================================================

#[tokio::test]
async fn test_deploy_pipeline_group_across_workers() {
    let coordinator = new_coordinator();
    let client = connect_nats(NATS_URL).await.unwrap();

    let w0_id = format!("dg-w0-{}", Uuid::new_v4());
    let w1_id = format!("dg-w1-{}", Uuid::new_v4());
    let w0_key = format!("key-{}", Uuid::new_v4());
    let w1_key = format!("key-{}", Uuid::new_v4());

    // Register workers directly on coordinator (with proper addresses/api_keys)
    {
        let mut coord = coordinator.write().await;
        let node0 = WorkerNode {
            id: WorkerId(w0_id.clone()),
            address: "nats://w0".to_string(),
            api_key: w0_key.clone(),
            status: WorkerStatus::Ready,
            capacity: WorkerCapacity::default(),
            last_heartbeat: std::time::Instant::now(),
            assigned_pipelines: Vec::new(),
            events_processed: 0,
        };
        coord.register_worker(node0);

        let node1 = WorkerNode {
            id: WorkerId(w1_id.clone()),
            address: "nats://w1".to_string(),
            api_key: w1_key.clone(),
            status: WorkerStatus::Ready,
            capacity: WorkerCapacity::default(),
            last_heartbeat: std::time::Instant::now(),
            assigned_pipelines: Vec::new(),
            events_processed: 0,
        };
        coord.register_worker(node1);
    }

    // Create TenantManagers for both workers
    let tm0 = new_tenant_manager(&w0_key).await;
    let tm1 = new_tenant_manager(&w1_key).await;

    // Start worker NATS handlers
    let h0_client = client.clone();
    let h0_wid = w0_id.clone();
    let h0_key = w0_key.clone();
    let h0_tm = tm0.clone();
    let h0 = tokio::spawn(async move {
        run_worker_nats_handler(h0_client, &h0_wid, &h0_key, h0_tm).await;
    });

    let h1_client = client.clone();
    let h1_wid = w1_id.clone();
    let h1_key = w1_key.clone();
    let h1_tm = tm1.clone();
    let h1 = tokio::spawn(async move {
        run_worker_nats_handler(h1_client, &h1_wid, &h1_key, h1_tm).await;
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Build PipelineGroupSpec with worker_affinity pinning each to a specific worker
    let spec = PipelineGroupSpec {
        name: "multi-deploy-test".to_string(),
        pipelines: vec![
            PipelinePlacement {
                name: "pipe-a".to_string(),
                source: "stream A = SensorReading .where(temperature > 100)".to_string(),
                worker_affinity: Some(w0_id.clone()),
                replicas: 1,
                partition_key: None,
            },
            PipelinePlacement {
                name: "pipe-b".to_string(),
                source: "stream B = SensorReading .where(temperature > 50)".to_string(),
                worker_affinity: Some(w1_id.clone()),
                replicas: 1,
                partition_key: None,
            },
        ],
        routes: vec![],
    };

    // Phase 1: Plan
    let plan = {
        let coord = coordinator.read().await;
        coord.plan_deploy_group(&spec).unwrap()
    };
    assert_eq!(plan.tasks.len(), 2);

    // Phase 2: Execute via NATS
    let results = Coordinator::execute_deploy_plan_nats(&client, &plan).await;
    assert_eq!(results.len(), 2);
    for r in &results {
        assert!(
            r.outcome.is_ok(),
            "deploy failed for {}: {:?}",
            r.replica_name,
            r.outcome
        );
    }

    // Phase 3: Commit
    let group_id = {
        let mut coord = coordinator.write().await;
        coord.commit_deploy_group(plan, results).unwrap()
    };

    // Verify group status
    {
        let coord = coordinator.read().await;
        let group = coord.pipeline_groups.get(&group_id).expect("group missing");
        assert_eq!(group.status, GroupStatus::Running);
        assert_eq!(group.placements.len(), 2);

        // Verify each pipeline on correct worker
        let pa = group.placements.get("pipe-a").expect("pipe-a missing");
        assert_eq!(pa.worker_id, WorkerId(w0_id.clone()));
        assert_eq!(pa.status, PipelineDeploymentStatus::Running);

        let pb = group.placements.get("pipe-b").expect("pipe-b missing");
        assert_eq!(pb.worker_id, WorkerId(w1_id.clone()));
        assert_eq!(pb.status, PipelineDeploymentStatus::Running);
    }

    // Verify each worker's TenantManager has the deployed pipeline
    {
        let mgr0 = tm0.read().await;
        let tenants0 = mgr0.list_tenants();
        let has_pipe_a = tenants0
            .iter()
            .any(|t| t.pipelines.values().any(|p| p.name == "pipe-a"));
        assert!(has_pipe_a, "worker-0 TenantManager should have pipe-a");
    }
    {
        let mgr1 = tm1.read().await;
        let tenants1 = mgr1.list_tenants();
        let has_pipe_b = tenants1
            .iter()
            .any(|t| t.pipelines.values().any(|p| p.name == "pipe-b"));
        assert!(has_pipe_b, "worker-1 TenantManager should have pipe-b");
    }

    h0.abort();
    h1.abort();
}

// ============================================================================
// Test 3: Inject events to both workers independently
// ============================================================================

#[tokio::test]
async fn test_inject_events_to_both_workers() {
    let coordinator = new_coordinator();
    let client = connect_nats(NATS_URL).await.unwrap();

    let w0_id = format!("ij-w0-{}", Uuid::new_v4());
    let w1_id = format!("ij-w1-{}", Uuid::new_v4());
    let w0_key = format!("key-{}", Uuid::new_v4());
    let w1_key = format!("key-{}", Uuid::new_v4());

    // Register workers
    {
        let mut coord = coordinator.write().await;
        for (wid, key) in [(&w0_id, &w0_key), (&w1_id, &w1_key)] {
            let node = WorkerNode {
                id: WorkerId(wid.clone()),
                address: "nats://worker".to_string(),
                api_key: key.clone(),
                status: WorkerStatus::Ready,
                capacity: WorkerCapacity::default(),
                last_heartbeat: std::time::Instant::now(),
                assigned_pipelines: Vec::new(),
                events_processed: 0,
            };
            coord.register_worker(node);
        }
    }

    // Create TenantManagers and deploy pipelines on each
    let tm0 = new_tenant_manager(&w0_key).await;
    let tm1 = new_tenant_manager(&w1_key).await;

    let pipe0_id = {
        let mut mgr = tm0.write().await;
        let tid = mgr.get_tenant_by_api_key(&w0_key).cloned().unwrap();
        mgr.deploy_pipeline_on_tenant(
            &tid,
            "sensor-filter-0".to_string(),
            "stream A = SensorReading .where(temperature > 100)".to_string(),
        )
        .await
        .unwrap()
    };

    let pipe1_id = {
        let mut mgr = tm1.write().await;
        let tid = mgr.get_tenant_by_api_key(&w1_key).cloned().unwrap();
        mgr.deploy_pipeline_on_tenant(
            &tid,
            "sensor-filter-1".to_string(),
            "stream A = SensorReading .where(temperature > 100)".to_string(),
        )
        .await
        .unwrap()
    };

    // Start worker NATS handlers
    let h0 = {
        let c = client.clone();
        let wid = w0_id.clone();
        let key = w0_key.clone();
        let tm = tm0.clone();
        tokio::spawn(async move {
            run_worker_nats_handler(c, &wid, &key, tm).await;
        })
    };
    let h1 = {
        let c = client.clone();
        let wid = w1_id.clone();
        let key = w1_key.clone();
        let tm = tm1.clone();
        tokio::spawn(async move {
            run_worker_nats_handler(c, &wid, &key, tm).await;
        })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Inject matching event (temperature > 100) to worker-0
    let inject_match = serde_json::json!({
        "pipeline_id": pipe0_id,
        "event_type": "SensorReading",
        "fields": { "temperature": 150.0, "sensor_id": "s1" }
    });
    let resp0: serde_json::Value = nats_request(
        &client,
        &subject_cmd(&w0_id, "inject"),
        &inject_match,
        Duration::from_secs(5),
    )
    .await
    .expect("inject to w0 failed");
    assert_eq!(resp0["ok"], true, "inject to w0 should succeed: {resp0}");
    assert!(
        resp0.get("output_events").is_some(),
        "response should contain output_events field: {resp0}"
    );

    // Inject non-matching event (temperature < 100) to worker-1
    let inject_no_match = serde_json::json!({
        "pipeline_id": pipe1_id,
        "event_type": "SensorReading",
        "fields": { "temperature": 50.0, "sensor_id": "s2" }
    });
    let resp1: serde_json::Value = nats_request(
        &client,
        &subject_cmd(&w1_id, "inject"),
        &inject_no_match,
        Duration::from_secs(5),
    )
    .await
    .expect("inject to w1 failed");
    assert_eq!(resp1["ok"], true, "inject to w1 should succeed: {resp1}");
    assert!(
        resp1.get("output_events").is_some(),
        "response should contain output_events field: {resp1}"
    );

    // Both workers independently process events through their pipelines.
    // A .where() filter passes matching events through engine processing
    // but output_events depends on whether there's an explicit .emit().
    // The key assertion is that both workers accept and process independently.

    h0.abort();
    h1.abort();
}

// ============================================================================
// Test 4: Cross-worker event routing via NATS subject infrastructure
// ============================================================================

#[tokio::test]
async fn test_cross_worker_event_routing_via_nats_subject() {
    use futures_util::StreamExt;

    let client = connect_nats(NATS_URL).await.unwrap();
    let group_id = format!("rt-grp-{}", Uuid::new_v4());

    // Build routing table and verify subjects generated correctly
    let routes = vec![InterPipelineRoute {
        from_pipeline: "pipeline-a".to_string(),
        to_pipeline: "pipeline-b".to_string(),
        event_types: vec!["SensorData*".to_string()],
        nats_subject: None,
    }];
    let table = build_routing_table(&group_id, &routes);

    let output_routes = table
        .output_routes
        .get("pipeline-a")
        .expect("no output routes for pipeline-a");
    assert_eq!(output_routes.len(), 1);
    let expected_subject = subject_pipeline(&group_id, "pipeline-a", "pipeline-b");
    assert_eq!(output_routes[0].1, expected_subject);

    let input_subs = table
        .input_subscriptions
        .get("pipeline-b")
        .expect("no input subs for pipeline-b");
    assert_eq!(input_subs.len(), 1);
    assert_eq!(input_subs[0].0, expected_subject);

    // Worker-0 publishes a JSON event to the routing subject
    let sub_client = raw_client().await;
    let mut sub = sub_client
        .subscribe(expected_subject.clone())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let event = serde_json::json!({
        "event_type": "SensorDataTemperature",
        "fields": { "value": 42.5, "sensor": "temp-01" }
    });
    nats_publish(&client, &expected_subject, &event)
        .await
        .unwrap();
    client.flush().await.unwrap();

    // Subscriber receives the event
    let msg = tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .expect("timeout waiting for routed event")
        .expect("subscription ended");
    let received: serde_json::Value = serde_json::from_slice(&msg.payload).unwrap();
    assert_eq!(received["event_type"], "SensorDataTemperature");
    assert_eq!(received["fields"]["value"], 42.5);
    assert_eq!(received["fields"]["sensor"], "temp-01");

    // Verify event_type_matches for wildcard patterns
    assert!(event_type_matches("SensorDataTemperature", "SensorData*"));
    assert!(event_type_matches("SensorDataHumidity", "SensorData*"));
    assert!(!event_type_matches("AlertCritical", "SensorData*"));
    assert!(event_type_matches("Anything", "*"));
}

// ============================================================================
// Test 5: Worker drain state transitions
// ============================================================================

#[tokio::test]
async fn test_worker_drain_state_transitions() {
    let coordinator = new_coordinator();

    let w0_id = format!("dr-w0-{}", Uuid::new_v4());
    let w1_id = format!("dr-w1-{}", Uuid::new_v4());

    // Register 2 workers
    {
        let mut coord = coordinator.write().await;
        for (wid, addr) in [
            (&w0_id, "http://localhost:9000"),
            (&w1_id, "http://localhost:9001"),
        ] {
            let node = WorkerNode {
                id: WorkerId(wid.clone()),
                address: addr.to_string(),
                api_key: format!("key-{wid}"),
                status: WorkerStatus::Ready,
                capacity: WorkerCapacity::default(),
                last_heartbeat: std::time::Instant::now(),
                assigned_pipelines: Vec::new(),
                events_processed: 0,
            };
            coord.register_worker(node);
        }
    }

    // Manually insert a DeployedPipelineGroup with a placement on worker-0
    let group_id = format!("drain-grp-{}", Uuid::new_v4());
    {
        let mut coord = coordinator.write().await;
        let spec = PipelineGroupSpec {
            name: "drain-test".to_string(),
            pipelines: vec![PipelinePlacement {
                name: "draining-pipe".to_string(),
                source: "stream A = SensorReading".to_string(),
                worker_affinity: None,
                replicas: 1,
                partition_key: None,
            }],
            routes: vec![],
        };
        let mut group =
            DeployedPipelineGroup::new(group_id.clone(), "drain-test".to_string(), spec);
        group.placements.insert(
            "draining-pipe".to_string(),
            varpulis_cluster::PipelineDeployment {
                worker_id: WorkerId(w0_id.clone()),
                worker_address: "http://localhost:9000".to_string(),
                worker_api_key: format!("key-{w0_id}"),
                pipeline_id: "pid-drain-0".to_string(),
                status: PipelineDeploymentStatus::Running,
                epoch: 0,
            },
        );
        group.status = GroupStatus::Running;
        coord.pipeline_groups.insert(group_id.clone(), group);
    }

    // Verify initial state
    {
        let coord = coordinator.read().await;
        assert_eq!(coord.workers.len(), 2);
        assert_eq!(
            coord.workers[&WorkerId(w0_id.clone())].status,
            WorkerStatus::Ready
        );
        assert_eq!(
            coord.workers[&WorkerId(w1_id.clone())].status,
            WorkerStatus::Ready
        );
    }

    // Call drain_worker — migration will fail (no real HTTP backend) but
    // state transitions still happen
    let drain_result = {
        let mut coord = coordinator.write().await;
        coord
            .drain_worker(&WorkerId(w0_id.clone()), Some(Duration::from_secs(2)))
            .await
    };
    // drain_worker returns Ok even if migrations fail — it deregisters regardless
    assert!(
        drain_result.is_ok(),
        "drain_worker should succeed: {:?}",
        drain_result
    );

    // Verify state transitions:
    // - Worker-0 should be deregistered (removed from workers map)
    // - Worker-1 should remain Ready
    {
        let coord = coordinator.read().await;

        assert!(
            !coord.workers.contains_key(&WorkerId(w0_id.clone())),
            "worker-0 should be deregistered after drain"
        );
        assert!(
            coord.workers.contains_key(&WorkerId(w1_id.clone())),
            "worker-1 should still be present"
        );
        assert_eq!(
            coord.workers[&WorkerId(w1_id.clone())].status,
            WorkerStatus::Ready
        );
    }
}
