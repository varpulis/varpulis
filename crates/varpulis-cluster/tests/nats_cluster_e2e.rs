//! End-to-end integration tests for the NATS cluster transport layer.
//!
//! These tests require a real `nats-server` running at `nats://localhost:4222`.
//! In CI this is provided by a `services: nats` container; locally run
//! `nats-server &` before executing.

#![cfg(feature = "nats-transport")]

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use uuid::Uuid;

use varpulis_cluster::nats_coordinator::run_coordinator_nats_handler;
use varpulis_cluster::nats_transport::{
    connect_nats, nats_publish, nats_request, subject_heartbeat, subject_register,
};
use varpulis_cluster::nats_worker::run_worker_nats_handler;
use varpulis_cluster::{
    HeartbeatRequest, RegisterWorkerRequest, RegisterWorkerResponse, WorkerCapacity, WorkerId,
};

const NATS_URL: &str = "nats://localhost:4222";

/// Helper: connect a raw async-nats client.
async fn raw_client() -> async_nats::Client {
    async_nats::connect(NATS_URL)
        .await
        .expect("Failed to connect to nats-server — is it running on localhost:4222?")
}

// ============================================================================
// Test 1: nats_request / nats_publish roundtrip
// ============================================================================

#[tokio::test]
async fn test_nats_request_reply_roundtrip() {
    use futures_util::StreamExt;

    let subject = format!("test.reqrep.{}", Uuid::new_v4());
    let client = connect_nats(NATS_URL).await.unwrap();

    // Spawn a manual responder
    let responder_client = raw_client().await;
    let mut sub = responder_client.subscribe(subject.clone()).await.unwrap();

    let responder = tokio::spawn(async move {
        if let Some(msg) = sub.next().await {
            let req: serde_json::Value =
                serde_json::from_slice(&msg.payload).expect("bad request JSON");
            let resp = serde_json::json!({ "echo": req["msg"] });
            if let Some(reply) = msg.reply {
                responder_client
                    .publish(reply, serde_json::to_vec(&resp).unwrap().into())
                    .await
                    .unwrap();
            }
        }
    });

    // Small delay for subscriber setup
    tokio::time::sleep(Duration::from_millis(100)).await;

    let req = serde_json::json!({ "msg": "hello" });
    let resp: serde_json::Value = nats_request(&client, &subject, &req, Duration::from_secs(5))
        .await
        .expect("request failed");

    assert_eq!(resp["echo"], "hello");
    responder.await.unwrap();
}

// ============================================================================
// Test 2: nats_publish fire-and-forget
// ============================================================================

#[tokio::test]
async fn test_nats_publish_and_subscribe() {
    use futures_util::StreamExt;

    let subject = format!("test.pubsub.{}", Uuid::new_v4());
    let client = connect_nats(NATS_URL).await.unwrap();

    // Subscribe via raw client
    let sub_client = raw_client().await;
    let mut sub = sub_client.subscribe(subject.clone()).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let payload = serde_json::json!({ "action": "test", "seq": 42 });
    nats_publish(&client, &subject, &payload).await.unwrap();
    client.flush().await.unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .expect("timeout")
        .expect("subscription ended");
    let received: serde_json::Value = serde_json::from_slice(&msg.payload).unwrap();
    assert_eq!(received["action"], "test");
    assert_eq!(received["seq"], 42);
}

// ============================================================================
// Test 3: Worker registration via NATS coordinator handler
// ============================================================================

#[tokio::test]
async fn test_worker_registration_via_nats() {
    let coordinator = varpulis_cluster::shared_coordinator();
    let client = connect_nats(NATS_URL).await.unwrap();

    // Start coordinator handler in background
    let handler_client = client.clone();
    let handler_coord = coordinator.clone();
    let handler = tokio::spawn(async move {
        run_coordinator_nats_handler(handler_client, handler_coord).await;
    });

    // Small delay for subscriptions to establish
    tokio::time::sleep(Duration::from_millis(200)).await;

    let worker_id = format!("worker-{}", Uuid::new_v4());
    let req = RegisterWorkerRequest {
        worker_id: worker_id.clone(),
        address: "localhost:9001".to_string(),
        api_key: "test-key-123".to_string(),
        capacity: WorkerCapacity::default(),
    };

    let resp: RegisterWorkerResponse =
        nats_request(&client, &subject_register(), &req, Duration::from_secs(5))
            .await
            .expect("registration request failed");

    assert_eq!(resp.worker_id, worker_id);
    assert_eq!(resp.status, "registered");
    assert!(resp.heartbeat_interval_secs.is_some());

    // Verify coordinator has the worker
    let coord = coordinator.read().await;
    assert!(
        coord.workers.contains_key(&WorkerId(worker_id.clone())),
        "registered worker not found in coordinator"
    );

    handler.abort();
}

// ============================================================================
// Test 4: Worker heartbeat via NATS
// ============================================================================

#[tokio::test]
async fn test_worker_heartbeat_via_nats() {
    let coordinator = varpulis_cluster::shared_coordinator();
    let client = connect_nats(NATS_URL).await.unwrap();

    // Start coordinator handler
    let handler_client = client.clone();
    let handler_coord = coordinator.clone();
    let handler = tokio::spawn(async move {
        run_coordinator_nats_handler(handler_client, handler_coord).await;
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Register a worker first
    let worker_id = format!("hb-worker-{}", Uuid::new_v4());
    let reg = RegisterWorkerRequest {
        worker_id: worker_id.clone(),
        address: "localhost:9002".to_string(),
        api_key: "hb-key".to_string(),
        capacity: WorkerCapacity::default(),
    };
    let _resp: RegisterWorkerResponse =
        nats_request(&client, &subject_register(), &reg, Duration::from_secs(5))
            .await
            .unwrap();

    // Send a heartbeat
    let hb = HeartbeatRequest {
        events_processed: 1000,
        pipelines_running: 2,
        pipeline_metrics: vec![],
    };
    nats_publish(&client, &subject_heartbeat(&worker_id), &hb)
        .await
        .unwrap();
    client.flush().await.unwrap();

    // Give coordinator time to process
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify coordinator updated the worker
    let coord = coordinator.read().await;
    let wid = WorkerId(worker_id.clone());
    let worker = coord.workers.get(&wid).expect("worker not found");
    assert_eq!(worker.events_processed, 1000);
    assert_eq!(worker.capacity.pipelines_running, 2);

    handler.abort();
}

// ============================================================================
// Test 5: Worker deploy command via NATS
// ============================================================================

#[tokio::test]
async fn test_worker_deploy_command_via_nats() {
    let worker_id = format!("deploy-worker-{}", Uuid::new_v4());
    let api_key = "deploy-test-key";

    // Create a real TenantManager with a tenant
    let tm = Arc::new(RwLock::new(varpulis_runtime::TenantManager::new()));
    {
        let mut mgr = tm.write().await;
        mgr.create_tenant(
            "test-tenant".to_string(),
            api_key.to_string(),
            varpulis_runtime::TenantQuota::default(),
        )
        .unwrap();
    }

    let client = connect_nats(NATS_URL).await.unwrap();

    // Start worker handler
    let handler_client = client.clone();
    let handler_wid = worker_id.clone();
    let handler_key = api_key.to_string();
    let handler_tm = tm.clone();
    let handler = tokio::spawn(async move {
        run_worker_nats_handler(handler_client, &handler_wid, &handler_key, handler_tm).await;
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Send deploy command
    let deploy_req = serde_json::json!({
        "name": "test-pipeline",
        "source": "stream A = SensorReading .where(temperature > 100)"
    });

    let deploy_subject = varpulis_cluster::nats_transport::subject_cmd(&worker_id, "deploy");
    let resp: serde_json::Value = nats_request(
        &client,
        &deploy_subject,
        &deploy_req,
        Duration::from_secs(5),
    )
    .await
    .expect("deploy request failed");

    assert!(
        resp.get("id").is_some(),
        "response should contain pipeline id: {resp}"
    );
    assert_eq!(resp["name"], "test-pipeline");
    assert_eq!(resp["status"], "running");

    handler.abort();
}

// ============================================================================
// Test 6: Worker inject command via NATS
// ============================================================================

#[tokio::test]
async fn test_worker_inject_command_via_nats() {
    let worker_id = format!("inject-worker-{}", Uuid::new_v4());
    let api_key = "inject-test-key";

    // Create TenantManager, tenant, and deploy a pipeline
    let tm = Arc::new(RwLock::new(varpulis_runtime::TenantManager::new()));
    let pipeline_id = {
        let mut mgr = tm.write().await;
        let tenant_id = mgr
            .create_tenant(
                "inject-tenant".to_string(),
                api_key.to_string(),
                varpulis_runtime::TenantQuota::default(),
            )
            .unwrap();
        mgr.deploy_pipeline_on_tenant(
            &tenant_id,
            "inject-pipe".to_string(),
            "stream A = SensorReading .where(temperature > 100)".to_string(),
        )
        .await
        .unwrap()
    };

    let client = connect_nats(NATS_URL).await.unwrap();

    // Start worker handler
    let handler_client = client.clone();
    let handler_wid = worker_id.clone();
    let handler_key = api_key.to_string();
    let handler_tm = tm.clone();
    let handler = tokio::spawn(async move {
        run_worker_nats_handler(handler_client, &handler_wid, &handler_key, handler_tm).await;
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Send inject command
    let inject_req = serde_json::json!({
        "pipeline_id": pipeline_id,
        "event_type": "SensorReading",
        "fields": {
            "temperature": 150.0,
            "sensor_id": "s1"
        }
    });

    let inject_subject = varpulis_cluster::nats_transport::subject_cmd(&worker_id, "inject");
    let resp: serde_json::Value = nats_request(
        &client,
        &inject_subject,
        &inject_req,
        Duration::from_secs(5),
    )
    .await
    .expect("inject request failed");

    assert_eq!(resp["ok"], true, "inject should succeed: {resp}");
    assert!(
        resp.get("output_events").is_some(),
        "response should contain output_events: {resp}"
    );

    handler.abort();
}
