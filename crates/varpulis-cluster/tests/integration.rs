//! Integration tests for varpulis-cluster with mock worker servers.
//!
//! These tests spin up real HTTP servers that mock the worker API, then test
//! the coordinator's deploy_group, teardown_group, and inject_event flows
//! end-to-end.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use varpulis_cluster::*;
use warp::reply::Reply;
use warp::Filter;

// =============================================================================
// Mock Worker Server
// =============================================================================

/// Tracks requests received by a mock worker.
#[derive(Default)]
struct MockWorkerState {
    deploys: Vec<serde_json::Value>,
    deletes: Vec<String>,
    events: Vec<(String, serde_json::Value)>,
    /// If set, deploy requests will fail with this status code.
    fail_deploys: bool,
}

fn mock_worker_routes(
    state: Arc<Mutex<MockWorkerState>>,
    api_key: &str,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    let key = api_key.to_string();

    let deploy_state = state.clone();
    let deploy_key = key.clone();
    let deploy = warp::path("api")
        .and(warp::path("v1"))
        .and(warp::path("pipelines"))
        .and(warp::path::end())
        .and(warp::post())
        .and(warp::header::<String>("x-api-key"))
        .and(warp::body::json::<serde_json::Value>())
        .and(warp::any().map(move || deploy_state.clone()))
        .and(warp::any().map(move || deploy_key.clone()))
        .and_then(
            |provided_key: String,
             body: serde_json::Value,
             state: Arc<Mutex<MockWorkerState>>,
             expected_key: String| async move {
                if provided_key != expected_key {
                    return Ok::<_, warp::Rejection>(
                        warp::reply::with_status(
                            warp::reply::json(&serde_json::json!({"error": "unauthorized"})),
                            warp::http::StatusCode::UNAUTHORIZED,
                        )
                        .into_response(),
                    );
                }

                let mut s = state.lock().await;
                if s.fail_deploys {
                    return Ok(warp::reply::with_status(
                        warp::reply::json(&serde_json::json!({"error": "internal server error"})),
                        warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                    )
                    .into_response());
                }

                let name = body["name"].as_str().unwrap_or("unknown").to_string();
                let id = format!("pid-{}", name);
                s.deploys.push(body);

                let resp = serde_json::json!({
                    "id": id,
                    "name": name,
                    "status": "running",
                });
                Ok(warp::reply::with_status(
                    warp::reply::json(&resp),
                    warp::http::StatusCode::CREATED,
                )
                .into_response())
            },
        );

    let delete_state = state.clone();
    let delete = warp::path("api")
        .and(warp::path("v1"))
        .and(warp::path("pipelines"))
        .and(warp::path::param::<String>())
        .and(warp::path::end())
        .and(warp::delete())
        .and(warp::any().map(move || delete_state.clone()))
        .and_then(
            |pipeline_id: String, state: Arc<Mutex<MockWorkerState>>| async move {
                let mut s = state.lock().await;
                s.deletes.push(pipeline_id);
                Ok::<_, warp::Rejection>(warp::reply::with_status(
                    warp::reply::json(&serde_json::json!({"deleted": true})),
                    warp::http::StatusCode::OK,
                ))
            },
        );

    let event_state = state;
    let inject =
        warp::path("api")
            .and(warp::path("v1"))
            .and(warp::path("pipelines"))
            .and(warp::path::param::<String>())
            .and(warp::path("events"))
            .and(warp::path::end())
            .and(warp::post())
            .and(warp::body::json::<serde_json::Value>())
            .and(warp::any().map(move || event_state.clone()))
            .and_then(
                |pipeline_id: String,
                 body: serde_json::Value,
                 state: Arc<Mutex<MockWorkerState>>| async move {
                    let mut s = state.lock().await;
                    s.events.push((pipeline_id, body));
                    Ok::<_, warp::Rejection>(warp::reply::with_status(
                        warp::reply::json(&serde_json::json!({
                            "processed": true,
                            "alerts_generated": 0,
                        })),
                        warp::http::StatusCode::OK,
                    ))
                },
            );

    deploy.or(delete).or(inject)
}

/// Start a mock worker server on a random port. Returns (port, state_handle).
async fn start_mock_worker(api_key: &str) -> (u16, Arc<Mutex<MockWorkerState>>) {
    let state = Arc::new(Mutex::new(MockWorkerState::default()));
    let routes = mock_worker_routes(state.clone(), api_key);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        warp::serve(routes).run_incoming(incoming).await;
    });

    // Brief pause to ensure server is ready
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (port, state)
}

// =============================================================================
// Integration Tests: Deploy Group
// =============================================================================

#[tokio::test]
async fn test_deploy_group_success() {
    let (port, worker_state) = start_mock_worker("worker-key").await;
    let worker_addr = format!("http://127.0.0.1:{}", port);

    let mut coord = Coordinator::new();
    coord.register_worker(WorkerNode::new(
        WorkerId("w1".into()),
        worker_addr.clone(),
        "worker-key".into(),
    ));

    let spec = PipelineGroupSpec {
        name: "test-group".into(),
        pipelines: vec![
            PipelinePlacement {
                name: "pipeline-a".into(),
                source: "stream A = X".into(),
                worker_affinity: None,
                replicas: 1,
                partition_key: None,
            },
            PipelinePlacement {
                name: "pipeline-b".into(),
                source: "stream B = Y".into(),
                worker_affinity: None,
                replicas: 1,
                partition_key: None,
            },
        ],
        routes: vec![],
    };

    let group_id = coord.deploy_group(spec).await.unwrap();

    // Verify group was created
    let group = &coord.pipeline_groups[&group_id];
    assert_eq!(group.name, "test-group");
    assert_eq!(group.status, GroupStatus::Running);
    assert_eq!(group.placements.len(), 2);

    // Verify both pipelines are running
    for deployment in group.placements.values() {
        assert_eq!(deployment.status, PipelineDeploymentStatus::Running);
        assert!(!deployment.pipeline_id.is_empty());
    }

    // Verify mock worker received both deploy requests
    let state = worker_state.lock().await;
    assert_eq!(state.deploys.len(), 2);
}

#[tokio::test]
async fn test_deploy_group_with_affinity() {
    let (port1, state1) = start_mock_worker("key1").await;
    let (port2, state2) = start_mock_worker("key2").await;

    let mut coord = Coordinator::new();
    coord.register_worker(WorkerNode::new(
        WorkerId("w1".into()),
        format!("http://127.0.0.1:{}", port1),
        "key1".into(),
    ));
    coord.register_worker(WorkerNode::new(
        WorkerId("w2".into()),
        format!("http://127.0.0.1:{}", port2),
        "key2".into(),
    ));

    let spec = PipelineGroupSpec {
        name: "affinity-test".into(),
        pipelines: vec![
            PipelinePlacement {
                name: "goes-to-w1".into(),
                source: "stream A = X".into(),
                worker_affinity: Some("w1".into()),
                replicas: 1,
                partition_key: None,
            },
            PipelinePlacement {
                name: "goes-to-w2".into(),
                source: "stream B = Y".into(),
                worker_affinity: Some("w2".into()),
                replicas: 1,
                partition_key: None,
            },
        ],
        routes: vec![],
    };

    let group_id = coord.deploy_group(spec).await.unwrap();
    let group = &coord.pipeline_groups[&group_id];
    assert_eq!(group.status, GroupStatus::Running);

    // Verify affinity was respected
    assert_eq!(
        group.placements["goes-to-w1"].worker_id,
        WorkerId("w1".into())
    );
    assert_eq!(
        group.placements["goes-to-w2"].worker_id,
        WorkerId("w2".into())
    );

    // Verify each mock received exactly one deploy
    assert_eq!(state1.lock().await.deploys.len(), 1);
    assert_eq!(state2.lock().await.deploys.len(), 1);
}

#[tokio::test]
async fn test_deploy_group_affinity_fallback() {
    let (port, _state) = start_mock_worker("key").await;

    let mut coord = Coordinator::new();
    coord.register_worker(WorkerNode::new(
        WorkerId("w1".into()),
        format!("http://127.0.0.1:{}", port),
        "key".into(),
    ));

    // Request affinity to non-existent worker
    let spec = PipelineGroupSpec {
        name: "fallback-test".into(),
        pipelines: vec![PipelinePlacement {
            name: "p1".into(),
            source: "stream A = X".into(),
            worker_affinity: Some("nonexistent".into()),
            replicas: 1,
            partition_key: None,
        }],
        routes: vec![],
    };

    // Should fall back to placement strategy and succeed
    let group_id = coord.deploy_group(spec).await.unwrap();
    let group = &coord.pipeline_groups[&group_id];
    assert_eq!(group.status, GroupStatus::Running);
    assert_eq!(group.placements["p1"].worker_id, WorkerId("w1".into()));
}

#[tokio::test]
async fn test_deploy_group_no_workers() {
    let mut coord = Coordinator::new();

    let spec = PipelineGroupSpec {
        name: "no-workers".into(),
        pipelines: vec![PipelinePlacement {
            name: "p1".into(),
            source: "stream A = X".into(),
            worker_affinity: None,
            replicas: 1,
            partition_key: None,
        }],
        routes: vec![],
    };

    let result = coord.deploy_group(spec).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        ClusterError::NoWorkersAvailable => {}
        other => panic!("Expected NoWorkersAvailable, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_deploy_group_worker_deploy_fails() {
    let (port, state) = start_mock_worker("key").await;

    // Make the mock worker reject deploy requests
    state.lock().await.fail_deploys = true;

    let mut coord = Coordinator::new();
    coord.register_worker(WorkerNode::new(
        WorkerId("w1".into()),
        format!("http://127.0.0.1:{}", port),
        "key".into(),
    ));

    let spec = PipelineGroupSpec {
        name: "fail-deploy".into(),
        pipelines: vec![PipelinePlacement {
            name: "p1".into(),
            source: "stream A = X".into(),
            worker_affinity: None,
            replicas: 1,
            partition_key: None,
        }],
        routes: vec![],
    };

    // Deploy should succeed at coordinator level but with Failed status
    let group_id = coord.deploy_group(spec).await.unwrap();
    let group = &coord.pipeline_groups[&group_id];
    assert_eq!(group.status, GroupStatus::Failed);
    assert_eq!(
        group.placements["p1"].status,
        PipelineDeploymentStatus::Failed
    );
}

#[tokio::test]
async fn test_deploy_group_partial_failure() {
    let (port_ok, _state_ok) = start_mock_worker("key").await;
    let (port_fail, state_fail) = start_mock_worker("key").await;

    // Make second worker fail
    state_fail.lock().await.fail_deploys = true;

    let mut coord = Coordinator::new();
    coord.register_worker(WorkerNode::new(
        WorkerId("w-ok".into()),
        format!("http://127.0.0.1:{}", port_ok),
        "key".into(),
    ));
    coord.register_worker(WorkerNode::new(
        WorkerId("w-fail".into()),
        format!("http://127.0.0.1:{}", port_fail),
        "key".into(),
    ));

    let spec = PipelineGroupSpec {
        name: "partial-fail".into(),
        pipelines: vec![
            PipelinePlacement {
                name: "good-pipeline".into(),
                source: "stream A = X".into(),
                worker_affinity: Some("w-ok".into()),
                replicas: 1,
                partition_key: None,
            },
            PipelinePlacement {
                name: "bad-pipeline".into(),
                source: "stream B = Y".into(),
                worker_affinity: Some("w-fail".into()),
                replicas: 1,
                partition_key: None,
            },
        ],
        routes: vec![],
    };

    let group_id = coord.deploy_group(spec).await.unwrap();
    let group = &coord.pipeline_groups[&group_id];
    assert_eq!(group.status, GroupStatus::PartiallyRunning);
    assert_eq!(
        group.placements["good-pipeline"].status,
        PipelineDeploymentStatus::Running
    );
    assert_eq!(
        group.placements["bad-pipeline"].status,
        PipelineDeploymentStatus::Failed
    );
}

#[tokio::test]
async fn test_deploy_group_worker_unreachable() {
    let mut coord = Coordinator::new();
    // Register a worker at a port that's not actually listening
    coord.register_worker(WorkerNode::new(
        WorkerId("w1".into()),
        "http://127.0.0.1:1".into(), // port 1 is almost certainly not listening
        "key".into(),
    ));

    let spec = PipelineGroupSpec {
        name: "unreachable".into(),
        pipelines: vec![PipelinePlacement {
            name: "p1".into(),
            source: "stream A = X".into(),
            worker_affinity: None,
            replicas: 1,
            partition_key: None,
        }],
        routes: vec![],
    };

    let group_id = coord.deploy_group(spec).await.unwrap();
    let group = &coord.pipeline_groups[&group_id];
    assert_eq!(group.status, GroupStatus::Failed);
    assert_eq!(
        group.placements["p1"].status,
        PipelineDeploymentStatus::Failed
    );
}

// =============================================================================
// Integration Tests: Teardown Group
// =============================================================================

#[tokio::test]
async fn test_teardown_group_success() {
    let (port, worker_state) = start_mock_worker("key").await;

    let mut coord = Coordinator::new();
    coord.register_worker(WorkerNode::new(
        WorkerId("w1".into()),
        format!("http://127.0.0.1:{}", port),
        "key".into(),
    ));

    let spec = PipelineGroupSpec {
        name: "teardown-test".into(),
        pipelines: vec![
            PipelinePlacement {
                name: "p1".into(),
                source: "stream A = X".into(),
                worker_affinity: None,
                replicas: 1,
                partition_key: None,
            },
            PipelinePlacement {
                name: "p2".into(),
                source: "stream B = Y".into(),
                worker_affinity: None,
                replicas: 1,
                partition_key: None,
            },
        ],
        routes: vec![],
    };

    let group_id = coord.deploy_group(spec).await.unwrap();
    assert_eq!(
        coord.pipeline_groups[&group_id].status,
        GroupStatus::Running
    );

    // Teardown
    coord.teardown_group(&group_id).await.unwrap();

    // Group should be removed after teardown
    assert!(!coord.pipeline_groups.contains_key(&group_id));

    // Verify mock worker received DELETE requests
    let state = worker_state.lock().await;
    assert_eq!(state.deletes.len(), 2);

    // Verify worker's assigned pipelines were cleaned up
    let worker = &coord.workers[&WorkerId("w1".into())];
    assert!(worker.assigned_pipelines.is_empty());
    assert_eq!(worker.capacity.pipelines_running, 0);
}

#[tokio::test]
async fn test_teardown_nonexistent_group() {
    let mut coord = Coordinator::new();
    let result = coord.teardown_group("nonexistent").await;
    assert!(result.is_err());
    match result.unwrap_err() {
        ClusterError::GroupNotFound(id) => assert_eq!(id, "nonexistent"),
        other => panic!("Expected GroupNotFound, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_teardown_skips_undeployed_pipelines() {
    let (port, worker_state) = start_mock_worker("key").await;
    let addr = format!("http://127.0.0.1:{}", port);

    let mut coord = Coordinator::new();
    coord.register_worker(WorkerNode::new(
        WorkerId("w1".into()),
        addr.clone(),
        "key".into(),
    ));

    // Manually create a group with one deployed and one failed pipeline
    let spec = PipelineGroupSpec {
        name: "partial".into(),
        pipelines: vec![
            PipelinePlacement {
                name: "deployed".into(),
                source: "".into(),
                worker_affinity: None,
                replicas: 1,
                partition_key: None,
            },
            PipelinePlacement {
                name: "failed".into(),
                source: "".into(),
                worker_affinity: None,
                replicas: 1,
                partition_key: None,
            },
        ],
        routes: vec![],
    };

    let mut group = DeployedPipelineGroup::new("g1".into(), "partial".into(), spec);
    group.placements.insert(
        "deployed".into(),
        PipelineDeployment {
            worker_id: WorkerId("w1".into()),
            worker_address: addr.clone(),
            worker_api_key: "key".into(),
            pipeline_id: "pid-deployed".into(),
            status: PipelineDeploymentStatus::Running,
            epoch: 0,
        },
    );
    group.placements.insert(
        "failed".into(),
        PipelineDeployment {
            worker_id: WorkerId("w1".into()),
            worker_address: addr,
            worker_api_key: "key".into(),
            pipeline_id: "".into(), // empty = never deployed
            status: PipelineDeploymentStatus::Failed,
            epoch: 0,
        },
    );
    coord.pipeline_groups.insert("g1".into(), group);

    coord.teardown_group("g1").await.unwrap();

    // Only the deployed pipeline should have been deleted
    let state = worker_state.lock().await;
    assert_eq!(state.deletes.len(), 1);
    assert_eq!(state.deletes[0], "pid-deployed");
}

// =============================================================================
// Integration Tests: Inject Event
// =============================================================================

#[tokio::test]
async fn test_inject_event_routed_correctly() {
    let (port1, state1) = start_mock_worker("key").await;
    let (port2, state2) = start_mock_worker("key").await;

    let addr1 = format!("http://127.0.0.1:{}", port1);
    let addr2 = format!("http://127.0.0.1:{}", port2);

    let mut coord = Coordinator::new();
    coord.register_worker(WorkerNode::new(
        WorkerId("w1".into()),
        addr1.clone(),
        "key".into(),
    ));
    coord.register_worker(WorkerNode::new(
        WorkerId("w2".into()),
        addr2.clone(),
        "key".into(),
    ));

    // Deploy with routing rules
    let spec = PipelineGroupSpec {
        name: "routing-test".into(),
        pipelines: vec![
            PipelinePlacement {
                name: "temps".into(),
                source: "stream T = Temperature".into(),
                worker_affinity: Some("w1".into()),
                replicas: 1,
                partition_key: None,
            },
            PipelinePlacement {
                name: "humidity".into(),
                source: "stream H = Humidity".into(),
                worker_affinity: Some("w2".into()),
                replicas: 1,
                partition_key: None,
            },
        ],
        routes: vec![
            InterPipelineRoute {
                from_pipeline: "_external".into(),
                to_pipeline: "temps".into(),
                event_types: vec!["Temperature*".into()],
                mqtt_topic: None,
            },
            InterPipelineRoute {
                from_pipeline: "_external".into(),
                to_pipeline: "humidity".into(),
                event_types: vec!["Humidity*".into()],
                mqtt_topic: None,
            },
        ],
    };

    let group_id = coord.deploy_group(spec).await.unwrap();

    // Inject a Temperature event
    let resp = coord
        .inject_event(
            &group_id,
            InjectEventRequest {
                event_type: "TemperatureReading".into(),
                fields: {
                    let mut m = serde_json::Map::new();
                    m.insert("value".into(), serde_json::Value::from(72.5));
                    m
                },
            },
        )
        .await
        .unwrap();
    assert_eq!(resp.routed_to, "temps");
    assert_eq!(resp.worker_id, "w1");

    // Inject a Humidity event
    let resp = coord
        .inject_event(
            &group_id,
            InjectEventRequest {
                event_type: "HumidityReading".into(),
                fields: serde_json::Map::new(),
            },
        )
        .await
        .unwrap();
    assert_eq!(resp.routed_to, "humidity");
    assert_eq!(resp.worker_id, "w2");

    // Verify mock workers received the events
    let s1 = state1.lock().await;
    assert_eq!(s1.events.len(), 1);
    assert_eq!(s1.events[0].0, "pid-temps");

    let s2 = state2.lock().await;
    assert_eq!(s2.events.len(), 1);
    assert_eq!(s2.events[0].0, "pid-humidity");
}

#[tokio::test]
async fn test_inject_event_fallback_to_first_pipeline() {
    let (port, state) = start_mock_worker("key").await;

    let mut coord = Coordinator::new();
    coord.register_worker(WorkerNode::new(
        WorkerId("w1".into()),
        format!("http://127.0.0.1:{}", port),
        "key".into(),
    ));

    let spec = PipelineGroupSpec {
        name: "fallback-test".into(),
        pipelines: vec![PipelinePlacement {
            name: "default".into(),
            source: "stream A = X".into(),
            worker_affinity: None,
            replicas: 1,
            partition_key: None,
        }],
        routes: vec![], // no explicit routes
    };

    let group_id = coord.deploy_group(spec).await.unwrap();

    // Any event should route to the first (and only) pipeline
    let resp = coord
        .inject_event(
            &group_id,
            InjectEventRequest {
                event_type: "AnyEventType".into(),
                fields: serde_json::Map::new(),
            },
        )
        .await
        .unwrap();
    assert_eq!(resp.routed_to, "default");

    let s = state.lock().await;
    assert_eq!(s.events.len(), 1);
}

#[tokio::test]
async fn test_inject_event_group_not_found() {
    let coord = Coordinator::new();
    let result = coord
        .inject_event(
            "nonexistent",
            InjectEventRequest {
                event_type: "Test".into(),
                fields: serde_json::Map::new(),
            },
        )
        .await;
    assert!(result.is_err());
    match result.unwrap_err() {
        ClusterError::GroupNotFound(id) => assert_eq!(id, "nonexistent"),
        other => panic!("Expected GroupNotFound, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_inject_event_pipeline_not_deployed() {
    let mut coord = Coordinator::new();

    // Create a group with routes but no actual placements
    let spec = PipelineGroupSpec {
        name: "no-placements".into(),
        pipelines: vec![PipelinePlacement {
            name: "phantom".into(),
            source: "".into(),
            worker_affinity: None,
            replicas: 1,
            partition_key: None,
        }],
        routes: vec![InterPipelineRoute {
            from_pipeline: "_external".into(),
            to_pipeline: "phantom".into(),
            event_types: vec!["*".into()],
            mqtt_topic: None,
        }],
    };

    let group = DeployedPipelineGroup::new("g1".into(), "no-placements".into(), spec);
    coord.pipeline_groups.insert("g1".into(), group);

    let result = coord
        .inject_event(
            "g1",
            InjectEventRequest {
                event_type: "Test".into(),
                fields: serde_json::Map::new(),
            },
        )
        .await;
    assert!(result.is_err());
    match result.unwrap_err() {
        ClusterError::RoutingFailed(msg) => {
            assert!(msg.contains("not deployed"), "Got: {}", msg);
        }
        other => panic!("Expected RoutingFailed, got: {:?}", other),
    }
}

// =============================================================================
// Integration Tests: Full Lifecycle
// =============================================================================

#[tokio::test]
async fn test_full_lifecycle_register_deploy_inject_teardown() {
    let (port, worker_state) = start_mock_worker("secret").await;
    let addr = format!("http://127.0.0.1:{}", port);

    let mut coord = Coordinator::new();

    // 1. Register worker
    let id = coord.register_worker(WorkerNode::new(
        WorkerId("worker-0".into()),
        addr,
        "secret".into(),
    ));
    assert_eq!(id, WorkerId("worker-0".into()));
    assert_eq!(coord.workers.len(), 1);

    // 2. Deploy pipeline group
    let spec = PipelineGroupSpec {
        name: "lifecycle-test".into(),
        pipelines: vec![PipelinePlacement {
            name: "analytics".into(),
            source: "stream Alerts = SensorReading .where(value > 100)".into(),
            worker_affinity: Some("worker-0".into()),
            replicas: 1,
            partition_key: None,
        }],
        routes: vec![InterPipelineRoute {
            from_pipeline: "_external".into(),
            to_pipeline: "analytics".into(),
            event_types: vec!["SensorReading".into()],
            mqtt_topic: None,
        }],
    };

    let group_id = coord.deploy_group(spec).await.unwrap();
    assert_eq!(
        coord.pipeline_groups[&group_id].status,
        GroupStatus::Running
    );

    // 3. Inject events
    for i in 0..5 {
        let resp = coord
            .inject_event(
                &group_id,
                InjectEventRequest {
                    event_type: "SensorReading".into(),
                    fields: {
                        let mut m = serde_json::Map::new();
                        m.insert("value".into(), serde_json::Value::from(50 + i * 20));
                        m.insert(
                            "sensor_id".into(),
                            serde_json::Value::from(format!("S{}", i)),
                        );
                        m
                    },
                },
            )
            .await
            .unwrap();
        assert_eq!(resp.routed_to, "analytics");
        assert_eq!(resp.worker_id, "worker-0");
    }

    // 4. Verify worker received all events
    {
        let state = worker_state.lock().await;
        assert_eq!(state.deploys.len(), 1);
        assert_eq!(state.events.len(), 5);
    }

    // 5. Teardown
    coord.teardown_group(&group_id).await.unwrap();
    assert!(!coord.pipeline_groups.contains_key(&group_id));

    {
        let state = worker_state.lock().await;
        assert_eq!(state.deletes.len(), 1);
    }

    // 6. Verify worker is no longer assigned pipelines
    let worker = &coord.workers[&WorkerId("worker-0".into())];
    assert!(worker.assigned_pipelines.is_empty());
    assert_eq!(worker.capacity.pipelines_running, 0);

    // 7. Deregister
    coord
        .deregister_worker(&WorkerId("worker-0".into()))
        .unwrap();
    assert!(coord.workers.is_empty());
}

// =============================================================================
// Integration Tests: Multi-Worker Deployment (Mandelbrot-style)
// =============================================================================

#[tokio::test]
async fn test_distributed_mandelbrot_style_deployment() {
    // Simulate the distributed Mandelbrot scenario: 4 workers, 4 pipelines
    let mut ports = Vec::new();
    let mut states = Vec::new();

    for _ in 0..4 {
        let (port, state) = start_mock_worker("key").await;
        ports.push(port);
        states.push(state);
    }

    let mut coord = Coordinator::new();
    for (i, port) in ports.iter().enumerate() {
        coord.register_worker(WorkerNode::new(
            WorkerId(format!("worker-{}", i)),
            format!("http://127.0.0.1:{}", port),
            "key".into(),
        ));
    }

    let spec = PipelineGroupSpec {
        name: "mandelbrot-distributed".into(),
        pipelines: (0..4)
            .map(|i| PipelinePlacement {
                name: format!("row{}", i),
                source: format!("stream Tile = ComputeTile{}* .process(compute_tile())", i),
                worker_affinity: Some(format!("worker-{}", i)),
                replicas: 1,
                partition_key: None,
            })
            .collect(),
        routes: (0..4)
            .map(|i| InterPipelineRoute {
                from_pipeline: "_external".into(),
                to_pipeline: format!("row{}", i),
                event_types: vec![format!("ComputeTile{}*", i)],
                mqtt_topic: None,
            })
            .collect(),
    };

    let group_id = coord.deploy_group(spec).await.unwrap();
    assert_eq!(
        coord.pipeline_groups[&group_id].status,
        GroupStatus::Running
    );

    // Inject 16 events (4x4 grid)
    let mut routed: HashMap<String, Vec<String>> = HashMap::new();
    for row in 0..4 {
        for col in 0..4 {
            let event_type = format!("ComputeTile{}{}", row, col);
            let resp = coord
                .inject_event(
                    &group_id,
                    InjectEventRequest {
                        event_type: event_type.clone(),
                        fields: serde_json::Map::new(),
                    },
                )
                .await
                .unwrap();
            routed
                .entry(resp.routed_to.clone())
                .or_default()
                .push(event_type);
        }
    }

    // Each pipeline should have received 4 events
    assert_eq!(routed.len(), 4);
    for i in 0..4 {
        let pipeline = format!("row{}", i);
        assert_eq!(
            routed[&pipeline].len(),
            4,
            "Pipeline {} should have 4 events, got: {:?}",
            pipeline,
            routed[&pipeline]
        );
    }

    // Each mock worker should have received 4 events
    for (i, state) in states.iter().enumerate() {
        let s = state.lock().await;
        assert_eq!(
            s.events.len(),
            4,
            "Worker {} should have 4 events, got {}",
            i,
            s.events.len()
        );
    }

    // Teardown
    coord.teardown_group(&group_id).await.unwrap();
    assert!(!coord.pipeline_groups.contains_key(&group_id));
}

// =============================================================================
// Integration Tests: Health + Recovery
// =============================================================================

#[tokio::test]
async fn test_health_sweep_and_heartbeat_recovery() {
    let mut coord = Coordinator::new();
    let mut node = WorkerNode::new(
        WorkerId("w1".into()),
        "http://127.0.0.1:9999".into(),
        "key".into(),
    );
    // Simulate that it was registered long ago
    node.last_heartbeat = std::time::Instant::now() - std::time::Duration::from_secs(20);
    coord.register_worker(node);

    // First sweep: should mark unhealthy
    let result = coord.health_sweep();
    assert_eq!(result.workers_marked_unhealthy.len(), 1);
    assert_eq!(
        coord.workers[&WorkerId("w1".into())].status,
        WorkerStatus::Unhealthy
    );

    // Heartbeat arrives: should recover
    coord
        .heartbeat(
            &WorkerId("w1".into()),
            &HeartbeatRequest {
                events_processed: 0,
                pipelines_running: 0,
                pipeline_metrics: vec![],
            },
        )
        .unwrap();
    assert_eq!(
        coord.workers[&WorkerId("w1".into())].status,
        WorkerStatus::Ready
    );

    // Second sweep: should not mark unhealthy (recent heartbeat)
    let result = coord.health_sweep();
    assert!(result.workers_marked_unhealthy.is_empty());
}

// =============================================================================
// Integration Tests: API E2E with Mock Worker
// =============================================================================

#[tokio::test]
async fn test_api_deploy_inject_teardown_e2e() {
    let (port, worker_state) = start_mock_worker("worker-key").await;
    let worker_addr = format!("http://127.0.0.1:{}", port);

    let coord = shared_coordinator();
    let ws_mgr = varpulis_cluster::shared_ws_manager();
    let routes = cluster_routes(
        coord.clone(),
        Arc::new(RbacConfig::single_key("admin".into())),
        ws_mgr,
    )
    .recover(varpulis_cluster::api::handle_rejection);

    // Register worker via API
    let resp = warp::test::request()
        .method("POST")
        .path("/api/v1/cluster/workers/register")
        .header("x-api-key", "admin")
        .json(&RegisterWorkerRequest {
            worker_id: "w1".into(),
            address: worker_addr,
            api_key: "worker-key".into(),
            capacity: WorkerCapacity::default(),
        })
        .reply(&routes)
        .await;
    assert_eq!(resp.status(), 201);

    // Deploy pipeline group via API
    let resp = warp::test::request()
        .method("POST")
        .path("/api/v1/cluster/pipeline-groups")
        .header("x-api-key", "admin")
        .json(&serde_json::json!({
            "name": "e2e-test",
            "pipelines": [
                {"name": "p1", "source": "stream A = X"}
            ],
            "routes": [
                {
                    "from_pipeline": "_external",
                    "to_pipeline": "p1",
                    "event_types": ["*"]
                }
            ]
        }))
        .reply(&routes)
        .await;
    assert_eq!(resp.status(), 201);
    let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    let group_id = body["id"].as_str().unwrap().to_string();
    assert_eq!(body["status"], "running");

    // Get the deployed group
    let resp = warp::test::request()
        .method("GET")
        .path(&format!("/api/v1/cluster/pipeline-groups/{}", group_id))
        .header("x-api-key", "admin")
        .reply(&routes)
        .await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    assert_eq!(body["name"], "e2e-test");
    assert_eq!(body["pipeline_count"], 1);

    // Inject event via API
    let resp = warp::test::request()
        .method("POST")
        .path(&format!(
            "/api/v1/cluster/pipeline-groups/{}/inject",
            group_id
        ))
        .header("x-api-key", "admin")
        .json(&serde_json::json!({
            "event_type": "TestEvent",
            "fields": {"value": 42}
        }))
        .reply(&routes)
        .await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    assert_eq!(body["routed_to"], "p1");

    // Verify mock worker received the event
    {
        let state = worker_state.lock().await;
        assert_eq!(state.events.len(), 1);
    }

    // Check topology
    let resp = warp::test::request()
        .method("GET")
        .path("/api/v1/cluster/topology")
        .header("x-api-key", "admin")
        .reply(&routes)
        .await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    assert_eq!(body["groups"].as_array().unwrap().len(), 1);

    // Teardown via API
    let resp = warp::test::request()
        .method("DELETE")
        .path(&format!("/api/v1/cluster/pipeline-groups/{}", group_id))
        .header("x-api-key", "admin")
        .reply(&routes)
        .await;
    assert_eq!(resp.status(), 200);

    // Verify group is removed (404)
    let resp = warp::test::request()
        .method("GET")
        .path(&format!("/api/v1/cluster/pipeline-groups/{}", group_id))
        .header("x-api-key", "admin")
        .reply(&routes)
        .await;
    assert_eq!(resp.status(), 404);
}

// =============================================================================
// Integration Tests: Three-Phase Deploy (plan → execute → commit)
// =============================================================================

#[tokio::test]
async fn test_three_phase_deploy_success() {
    let (port, worker_state) = start_mock_worker("worker-key").await;
    let worker_addr = format!("http://127.0.0.1:{}", port);

    let mut coord = Coordinator::new();
    coord.register_worker(WorkerNode::new(
        WorkerId("w1".into()),
        worker_addr.clone(),
        "worker-key".into(),
    ));

    let spec = PipelineGroupSpec {
        name: "three-phase-test".into(),
        pipelines: vec![
            PipelinePlacement {
                name: "p1".into(),
                source: "stream A = X".into(),
                worker_affinity: None,
                replicas: 1,
                partition_key: None,
            },
            PipelinePlacement {
                name: "p2".into(),
                source: "stream B = Y".into(),
                worker_affinity: None,
                replicas: 1,
                partition_key: None,
            },
        ],
        routes: vec![],
    };

    // Phase 1: Plan
    let plan = coord.plan_deploy_group(&spec).unwrap();
    assert_eq!(plan.tasks.len(), 2);
    assert_eq!(plan.spec.name, "three-phase-test");

    // Coordinator state should be unchanged after planning
    assert!(coord.pipeline_groups.is_empty());

    // Phase 2: Execute (HTTP deploys to mock workers)
    let http_client = coord.http_client().clone();
    let results = Coordinator::execute_deploy_plan(&http_client, &plan).await;
    assert_eq!(results.len(), 2);
    for result in &results {
        assert!(
            result.outcome.is_ok(),
            "Deploy should succeed: {:?}",
            result.outcome
        );
    }

    // Coordinator state should still be unchanged after execute
    assert!(coord.pipeline_groups.is_empty());

    // Phase 3: Commit
    let group_id = coord.commit_deploy_group(plan, results).unwrap();
    let group = &coord.pipeline_groups[&group_id];
    assert_eq!(group.name, "three-phase-test");
    assert_eq!(group.status, GroupStatus::Running);
    assert_eq!(group.placements.len(), 2);

    // Verify mock worker received both deploy requests
    let state = worker_state.lock().await;
    assert_eq!(state.deploys.len(), 2);
}

#[tokio::test]
async fn test_three_phase_deploy_worker_unreachable() {
    let mut coord = Coordinator::new();
    // Register a worker at a port that's not listening
    coord.register_worker(WorkerNode::new(
        WorkerId("w1".into()),
        "http://127.0.0.1:1".into(),
        "key".into(),
    ));

    let spec = PipelineGroupSpec {
        name: "unreachable-test".into(),
        pipelines: vec![PipelinePlacement {
            name: "p1".into(),
            source: "stream A = X".into(),
            worker_affinity: None,
            replicas: 1,
            partition_key: None,
        }],
        routes: vec![],
    };

    let plan = coord.plan_deploy_group(&spec).unwrap();
    let http_client = coord.http_client().clone();
    let results = Coordinator::execute_deploy_plan(&http_client, &plan).await;

    // Execute should return error outcomes (not panic)
    assert_eq!(results.len(), 1);
    assert!(results[0].outcome.is_err());

    // Commit should still succeed but with Failed status
    let group_id = coord.commit_deploy_group(plan, results).unwrap();
    let group = &coord.pipeline_groups[&group_id];
    assert_eq!(group.status, GroupStatus::Failed);
    assert_eq!(
        group.placements["p1"].status,
        PipelineDeploymentStatus::Failed
    );
}

// =============================================================================
// Integration Tests: migrate_pipeline
// =============================================================================

#[tokio::test]
async fn test_migrate_pipeline_between_workers() {
    let (port1, state1) = start_mock_worker("key").await;
    let (port2, state2) = start_mock_worker("key").await;
    let addr1 = format!("http://127.0.0.1:{}", port1);
    let addr2 = format!("http://127.0.0.1:{}", port2);

    let mut coord = Coordinator::new();
    coord.register_worker(WorkerNode::new(
        WorkerId("w1".into()),
        addr1.clone(),
        "key".into(),
    ));
    coord.register_worker(WorkerNode::new(
        WorkerId("w2".into()),
        addr2.clone(),
        "key".into(),
    ));

    // Deploy to w1
    let spec = PipelineGroupSpec {
        name: "migrate-test".into(),
        pipelines: vec![PipelinePlacement {
            name: "p1".into(),
            source: "stream A = X".into(),
            worker_affinity: Some("w1".into()),
            replicas: 1,
            partition_key: None,
        }],
        routes: vec![],
    };
    let group_id = coord.deploy_group(spec).await.unwrap();

    // Verify initially on w1
    assert_eq!(
        coord.pipeline_groups[&group_id].placements["p1"].worker_id,
        WorkerId("w1".into())
    );
    assert!(coord.workers[&WorkerId("w1".into())]
        .assigned_pipelines
        .contains(&"p1".to_string()));

    // Migrate from w1 to w2
    let migration_id = coord
        .migrate_pipeline(
            "p1",
            &group_id,
            &WorkerId("w2".into()),
            MigrationReason::Manual,
        )
        .await
        .unwrap();

    // Verify pipeline is now on w2
    assert_eq!(
        coord.pipeline_groups[&group_id].placements["p1"].worker_id,
        WorkerId("w2".into())
    );
    assert!(!coord.workers[&WorkerId("w1".into())]
        .assigned_pipelines
        .contains(&"p1".to_string()));
    assert!(coord.workers[&WorkerId("w2".into())]
        .assigned_pipelines
        .contains(&"p1".to_string()));

    // Migration should be tracked
    assert!(coord.active_migrations.contains_key(&migration_id));
    assert_eq!(
        coord.active_migrations[&migration_id].status,
        MigrationStatus::Completed
    );

    // w1 should have received: 1 deploy + 1 delete
    let s1 = state1.lock().await;
    assert_eq!(s1.deploys.len(), 1);
    assert_eq!(s1.deletes.len(), 1);

    // w2 should have received: 1 deploy (migration target)
    let s2 = state2.lock().await;
    assert_eq!(s2.deploys.len(), 1);
}

// =============================================================================
// Integration Tests: handle_worker_failure with pipelines
// =============================================================================

#[tokio::test]
async fn test_handle_worker_failure_redistributes_pipelines() {
    let (port1, _state1) = start_mock_worker("key").await;
    let (port2, state2) = start_mock_worker("key").await;
    let addr1 = format!("http://127.0.0.1:{}", port1);
    let addr2 = format!("http://127.0.0.1:{}", port2);

    let mut coord = Coordinator::new();
    coord.register_worker(WorkerNode::new(
        WorkerId("w1".into()),
        addr1.clone(),
        "key".into(),
    ));
    coord.register_worker(WorkerNode::new(
        WorkerId("w2".into()),
        addr2.clone(),
        "key".into(),
    ));

    // Deploy pipeline to w1
    let spec = PipelineGroupSpec {
        name: "failover-test".into(),
        pipelines: vec![PipelinePlacement {
            name: "p1".into(),
            source: "stream A = X".into(),
            worker_affinity: Some("w1".into()),
            replicas: 1,
            partition_key: None,
        }],
        routes: vec![],
    };
    let group_id = coord.deploy_group(spec).await.unwrap();
    assert_eq!(
        coord.pipeline_groups[&group_id].placements["p1"].worker_id,
        WorkerId("w1".into())
    );

    // Mark w1 as unhealthy
    coord
        .workers
        .get_mut(&WorkerId("w1".into()))
        .unwrap()
        .status = WorkerStatus::Unhealthy;

    // Handle the failure
    let results = coord.handle_worker_failure(&WorkerId("w1".into())).await;
    assert_eq!(results.len(), 1);
    assert!(results[0].is_ok(), "Migration should succeed");

    // Pipeline should now be on w2
    assert_eq!(
        coord.pipeline_groups[&group_id].placements["p1"].worker_id,
        WorkerId("w2".into())
    );

    // w2 should have received a deploy (for the migrated pipeline)
    let s2 = state2.lock().await;
    assert!(
        !s2.deploys.is_empty(),
        "w2 should have at least 1 deploy for the migrated pipeline"
    );
}

// =============================================================================
// Integration Tests: Rebalance with actual moves
// =============================================================================

#[tokio::test]
async fn test_rebalance_moves_pipelines() {
    let (port1, _state1) = start_mock_worker("key").await;
    let (port2, state2) = start_mock_worker("key").await;
    let addr1 = format!("http://127.0.0.1:{}", port1);
    let addr2 = format!("http://127.0.0.1:{}", port2);

    let mut coord = Coordinator::new();
    coord.register_worker(WorkerNode::new(
        WorkerId("w1".into()),
        addr1.clone(),
        "key".into(),
    ));

    // Deploy 4 pipelines all to w1 (only worker available)
    let spec = PipelineGroupSpec {
        name: "rebalance-test".into(),
        pipelines: (0..4)
            .map(|i| PipelinePlacement {
                name: format!("p{}", i),
                source: format!("stream S{} = X", i),
                worker_affinity: None,
                replicas: 1,
                partition_key: None,
            })
            .collect(),
        routes: vec![],
    };
    let group_id = coord.deploy_group(spec).await.unwrap();

    // Verify all 4 pipelines on w1
    for i in 0..4 {
        assert_eq!(
            coord.pipeline_groups[&group_id].placements[&format!("p{}", i)].worker_id,
            WorkerId("w1".into())
        );
    }

    // Now add w2 (creating imbalance: w1 has 4, w2 has 0)
    coord.register_worker(WorkerNode::new(
        WorkerId("w2".into()),
        addr2.clone(),
        "key".into(),
    ));

    // Trigger rebalance
    let migration_ids = coord.rebalance().await.unwrap();

    // Should have moved some pipelines to w2
    assert!(
        !migration_ids.is_empty(),
        "Rebalance should have triggered migrations"
    );

    // w2 should have received deploys for the moved pipelines
    let s2 = state2.lock().await;
    assert!(
        !s2.deploys.is_empty(),
        "w2 should have received deploys from rebalance"
    );
}
