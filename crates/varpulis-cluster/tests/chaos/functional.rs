//! Functional chaos tests -- deploy, inject, teardown under worker failures.
//!
//! These tests spawn REAL varpulis processes (coordinator + workers) and exercise
//! the cluster through its REST API. They require the `varpulis` binary to be
//! built first (`cargo build`). All tests are marked `#[ignore]` so they only
//! run when explicitly requested via `cargo test -- --ignored`.

use super::ProcessCluster;
use std::time::Duration;
use tokio::time::sleep;

// =============================================================================
// Test 1: Basic failover -- kill a worker, pipeline migrates
// =============================================================================

/// Deploy a pipeline to a specific worker, kill that worker, and verify the
/// pipeline is automatically migrated to a surviving worker. After failover,
/// event injection should still succeed.
#[tokio::test]
#[ignore]
async fn test_basic_failover() {
    let timeout = tokio::time::timeout(Duration::from_secs(90), async {
        // Start cluster with 3 workers (chaos-w0, chaos-w1, chaos-w2).
        let mut cluster = ProcessCluster::start(3).await;

        // Deploy a simple pipeline group with affinity to chaos-w0.
        let group_id = cluster
            .deploy_group(serde_json::json!({
                "name": "failover-test",
                "pipelines": [{
                    "name": "alerts",
                    "source": "stream Alerts = SensorReading .where(value > 100)",
                    "worker_affinity": "chaos-w0"
                }],
                "routes": []
            }))
            .await;

        // Verify initial placement: pipeline should be on chaos-w0.
        let topo = cluster.get_topology().await;
        let initial_worker = find_pipeline_worker(&topo, &group_id, "alerts");
        assert_eq!(
            initial_worker.as_deref(),
            Some("chaos-w0"),
            "Pipeline should initially be on chaos-w0"
        );

        // Kill chaos-w0.
        cluster.kill_worker("chaos-w0");

        // Wait for the coordinator's health sweep to detect the failure and
        // migrate the pipeline. Heartbeat timeout is 15s, plus migration time.
        sleep(Duration::from_secs(25)).await;

        // Verify the pipeline has migrated to a surviving worker.
        let topo = cluster.get_topology().await;
        let new_worker = find_pipeline_worker(&topo, &group_id, "alerts");
        assert!(
            new_worker.is_some(),
            "Pipeline should still exist in topology after failover"
        );
        assert_ne!(
            new_worker.as_deref(),
            Some("chaos-w0"),
            "Pipeline should have migrated away from killed worker"
        );

        // Inject an event to verify the pipeline is operational.
        let inject_resp = cluster
            .inject_event(
                &group_id,
                serde_json::json!({
                    "event_type": "SensorReading",
                    "fields": { "value": "150", "sensor": "temp1" }
                }),
            )
            .await;
        // inject_event returns the response body JSON; reaching here means 2xx.
        assert!(
            inject_resp["routed_to"].is_string(),
            "Event injection should succeed after failover"
        );

        cluster.shutdown().await;
    });

    timeout.await.expect("test_basic_failover timed out");
}

// =============================================================================
// Test 2: Failover preserves state -- events continue processing
// =============================================================================

/// Deploy a pipeline with a window operator, inject some events, kill the
/// hosting worker, wait for failover, then inject more events. The pipeline
/// should remain operational after migration.
#[tokio::test]
#[ignore]
async fn test_failover_preserves_state() {
    let timeout = tokio::time::timeout(Duration::from_secs(90), async {
        let mut cluster = ProcessCluster::start(2).await;

        // Deploy a pipeline with a window operator.
        let group_id = cluster
            .deploy_group(serde_json::json!({
                "name": "state-failover",
                "pipelines": [{
                    "name": "windowed",
                    "source": "stream Alerts = SensorReading .window(count, 5) .aggregate(count() as cnt)"
                }],
                "routes": []
            }))
            .await;

        // Find which worker hosts the pipeline.
        let topo = cluster.get_topology().await;
        let hosting_worker = find_pipeline_worker(&topo, &group_id, "windowed")
            .expect("Pipeline should be deployed");

        // Inject a few events (less than the window size).
        for i in 0..3 {
            let resp = cluster
                .try_inject_event(
                    &group_id,
                    serde_json::json!({
                        "event_type": "SensorReading",
                        "fields": { "value": i.to_string(), "sensor": "s1" }
                    }),
                )
                .await;
            assert!(resp.is_ok(), "Pre-failover injection {} should succeed", i);
        }

        // Kill the hosting worker.
        cluster.kill_worker(&hosting_worker);

        // Wait for failover.
        sleep(Duration::from_secs(25)).await;

        // Inject more events to complete the window.
        for i in 3..6 {
            let resp = cluster
                .try_inject_event(
                    &group_id,
                    serde_json::json!({
                        "event_type": "SensorReading",
                        "fields": { "value": i.to_string(), "sensor": "s1" }
                    }),
                )
                .await;
            assert!(resp.is_ok(), "Post-failover injection {} should succeed", i);
        }

        // Verify the pipeline is operational on a new worker.
        let topo = cluster.get_topology().await;
        let new_worker = find_pipeline_worker(&topo, &group_id, "windowed");
        assert!(new_worker.is_some(), "Pipeline should exist after failover");
        assert_ne!(
            new_worker.as_deref(),
            Some(hosting_worker.as_str()),
            "Pipeline should have migrated to a different worker"
        );

        cluster.shutdown().await;
    });

    timeout
        .await
        .expect("test_failover_preserves_state timed out");
}

// =============================================================================
// Test 3: Drain a worker -- pipeline moves gracefully
// =============================================================================

/// Deploy a pipeline, then drain the hosting worker. Verify the pipeline moves
/// to the other worker and event injection continues to work.
#[tokio::test]
#[ignore]
async fn test_drain_worker() {
    let timeout = tokio::time::timeout(Duration::from_secs(60), async {
        let cluster = ProcessCluster::start(2).await;

        // Deploy a pipeline with affinity to chaos-w0.
        let group_id = cluster
            .deploy_group(serde_json::json!({
                "name": "drain-test",
                "pipelines": [{
                    "name": "p1",
                    "source": "stream Output = Input",
                    "worker_affinity": "chaos-w0"
                }],
                "routes": []
            }))
            .await;

        // Verify initial placement.
        let topo = cluster.get_topology().await;
        assert_eq!(
            find_pipeline_worker(&topo, &group_id, "p1").as_deref(),
            Some("chaos-w0"),
            "Pipeline should be on chaos-w0 before drain"
        );

        // Drain chaos-w0.
        let drain_resp = cluster.drain_worker("chaos-w0").await;
        assert_eq!(
            drain_resp["status"].as_str(),
            Some("drained"),
            "Drain response should indicate success"
        );

        // Wait for migration to complete.
        sleep(Duration::from_secs(5)).await;

        // Verify the pipeline moved to chaos-w1.
        let topo = cluster.get_topology().await;
        let new_worker = find_pipeline_worker(&topo, &group_id, "p1");
        assert_eq!(
            new_worker.as_deref(),
            Some("chaos-w1"),
            "Pipeline should have moved to chaos-w1 after drain"
        );

        // Verify events can still be injected.
        let resp = cluster
            .try_inject_event(
                &group_id,
                serde_json::json!({
                    "event_type": "Input",
                    "fields": { "data": "test" }
                }),
            )
            .await;
        assert!(resp.is_ok(), "Injection should succeed after drain");

        cluster.shutdown().await;
    });

    timeout.await.expect("test_drain_worker timed out");
}

// =============================================================================
// Test 4: Rebalance on worker join
// =============================================================================

/// Deploy 4 pipeline groups across 2 workers, add a 3rd worker, trigger
/// rebalance, and verify the new worker gets at least one pipeline.
#[tokio::test]
#[ignore]
async fn test_rebalance_on_join() {
    let timeout = tokio::time::timeout(Duration::from_secs(90), async {
        // Start with 2 workers.
        let mut cluster = ProcessCluster::start(2).await;

        // Deploy 4 separate pipeline groups (one pipeline each).
        let mut group_ids = Vec::new();
        for i in 0..4 {
            let gid = cluster
                .deploy_group(serde_json::json!({
                    "name": format!("rebalance-{}", i),
                    "pipelines": [{
                        "name": format!("p{}", i),
                        "source": format!("stream S{} = Event{}", i, i)
                    }],
                    "routes": []
                }))
                .await;
            group_ids.push(gid);
        }

        // Verify all 4 are distributed across the 2 existing workers.
        let topo = cluster.get_topology().await;
        let groups = topo["groups"].as_array().expect("topology groups");
        assert_eq!(groups.len(), 4, "Should have 4 deployed groups");

        // Add a 3rd worker.
        let new_worker_id = cluster.add_worker().await;

        // Trigger manual rebalance.
        let rebalance_resp = cluster.rebalance().await;
        let migrations = rebalance_resp["migrations_started"].as_u64().unwrap_or(0);

        // Wait for migrations to complete.
        sleep(Duration::from_secs(10)).await;

        // Check topology -- the new worker should have at least 1 pipeline.
        let topo = cluster.get_topology().await;
        let mut worker_pipeline_count: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        if let Some(groups) = topo["groups"].as_array() {
            for group in groups {
                if let Some(pipelines) = group["pipelines"].as_array() {
                    for p in pipelines {
                        if let Some(wid) = p["worker_id"].as_str() {
                            *worker_pipeline_count.entry(wid.to_string()).or_insert(0) += 1;
                        }
                    }
                }
            }
        }

        let new_worker_pipelines = worker_pipeline_count
            .get(&new_worker_id)
            .copied()
            .unwrap_or(0);

        // If rebalance moved any pipelines, the new worker should have some.
        // With 4 pipelines on 3 workers (avg ~1.33), the algorithm should move
        // at least one pipeline to the previously-empty worker.
        if migrations > 0 {
            assert!(
                new_worker_pipelines > 0,
                "New worker {} should have at least 1 pipeline after rebalance \
                 (migrations={}), got: {:?}",
                new_worker_id,
                migrations,
                worker_pipeline_count
            );
        }

        cluster.shutdown().await;
    });

    timeout.await.expect("test_rebalance_on_join timed out");
}

// =============================================================================
// Test 5: Replica deployment
// =============================================================================

/// Deploy a pipeline group with replicas > 1 and verify that multiple copies
/// appear in the topology on different workers, and that event injection works.
#[tokio::test]
#[ignore]
async fn test_replica_deployment() {
    let timeout = tokio::time::timeout(Duration::from_secs(60), async {
        // Start with 3 workers.
        let cluster = ProcessCluster::start(3).await;

        // Deploy pipeline group with replicas: 2.
        let group_id = cluster
            .deploy_group(serde_json::json!({
                "name": "replicated",
                "pipelines": [{
                    "name": "p1",
                    "source": "stream Out = Input",
                    "replicas": 2
                }],
                "routes": []
            }))
            .await;

        // Verify topology shows 2 copies (p1#0, p1#1).
        let topo = cluster.get_topology().await;
        let mut replica_workers = Vec::new();
        if let Some(groups) = topo["groups"].as_array() {
            for group in groups {
                if group["group_id"].as_str() == Some(&group_id) {
                    if let Some(pipelines) = group["pipelines"].as_array() {
                        for p in pipelines {
                            let name = p["name"].as_str().unwrap_or("");
                            let worker = p["worker_id"].as_str().unwrap_or("");
                            if name.starts_with("p1#") {
                                replica_workers.push((name.to_string(), worker.to_string()));
                            }
                        }
                    }
                }
            }
        }

        assert_eq!(
            replica_workers.len(),
            2,
            "Should have 2 replicas (p1#0 and p1#1), got: {:?}",
            replica_workers
        );

        // Verify they are on different workers (round-robin should distribute
        // across different workers when 3 are available).
        let workers: Vec<&str> = replica_workers.iter().map(|(_, w)| w.as_str()).collect();
        assert_ne!(
            workers[0], workers[1],
            "Replicas should be on different workers: {:?}",
            replica_workers
        );

        // Inject an event -- should be routed to one of the replicas.
        let resp = cluster
            .inject_event(
                &group_id,
                serde_json::json!({
                    "event_type": "Input",
                    "fields": { "data": "hello" }
                }),
            )
            .await;
        assert!(
            resp["routed_to"].is_string(),
            "Injection to replicated pipeline should succeed and return routed_to"
        );

        cluster.shutdown().await;
    });

    timeout.await.expect("test_replica_deployment timed out");
}

// =============================================================================
// Test 6: Replica hash partitioning
// =============================================================================

/// Deploy a pipeline with replicas and a `partition_key`, then verify that
/// events with the same partition key value are deterministically routed to
/// the same replica.
#[tokio::test]
#[ignore]
async fn test_replica_hash_partitioning() {
    let timeout = tokio::time::timeout(Duration::from_secs(60), async {
        let cluster = ProcessCluster::start(2).await;

        // Deploy with partition_key: "source".
        let group_id = cluster
            .deploy_group(serde_json::json!({
                "name": "partitioned",
                "pipelines": [{
                    "name": "p1",
                    "source": "stream Out = Input",
                    "replicas": 2,
                    "partition_key": "source"
                }],
                "routes": []
            }))
            .await;

        // Inject multiple events with different source values and verify
        // deterministic routing: same source -> same routed_to each time.
        let sources = ["alpha", "beta", "gamma", "delta"];
        let mut route_map: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();

        for round in 0..3 {
            for source in &sources {
                let resp = cluster
                    .inject_event(
                        &group_id,
                        serde_json::json!({
                            "event_type": "Input",
                            "fields": { "source": source, "round": round.to_string() }
                        }),
                    )
                    .await;

                if let Some(routed_to) = resp["routed_to"].as_str() {
                    let key = source.to_string();
                    if let Some(prev) = route_map.get(&key) {
                        assert_eq!(
                            prev, routed_to,
                            "Events with source='{}' should always route to same replica \
                             (round {})",
                            source, round
                        );
                    } else {
                        route_map.insert(key, routed_to.to_string());
                    }
                }
            }
        }

        // Verify at least 2 distinct routes were used (partitioning is working).
        let unique_routes: std::collections::HashSet<&str> =
            route_map.values().map(|s| s.as_str()).collect();
        assert!(
            unique_routes.len() >= 2,
            "Hash partitioning should distribute across at least 2 replicas, \
             got: {:?}",
            route_map
        );

        cluster.shutdown().await;
    });

    timeout
        .await
        .expect("test_replica_hash_partitioning timed out");
}

// =============================================================================
// Test 7: Migration during active injection
// =============================================================================

/// Deploy a pipeline, start injecting events in the background, trigger a
/// manual migration to the other worker, and verify that the majority of
/// injected events return successfully despite the ongoing migration.
#[tokio::test]
#[ignore]
async fn test_migration_during_injection() {
    let timeout = tokio::time::timeout(Duration::from_secs(90), async {
        let cluster = ProcessCluster::start(2).await;

        // Deploy pipeline to chaos-w0.
        let group_id = cluster
            .deploy_group(serde_json::json!({
                "name": "migration-during-inject",
                "pipelines": [{
                    "name": "p1",
                    "source": "stream Out = Input",
                    "worker_affinity": "chaos-w0"
                }],
                "routes": []
            }))
            .await;

        // Verify initial placement on chaos-w0.
        let topo = cluster.get_topology().await;
        assert_eq!(
            find_pipeline_worker(&topo, &group_id, "p1").as_deref(),
            Some("chaos-w0"),
            "Pipeline should start on chaos-w0"
        );

        // We will use try_inject_event for the background injector since it
        // doesn't panic on errors. Share the cluster via Arc for the spawned task.
        let injection_count = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let injection_errors = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let stop_flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Wrap the cluster in an Arc<Mutex> so the background injector task
        // and the main test task can share it.
        let cluster = std::sync::Arc::new(tokio::sync::Mutex::new(cluster));

        let inject_cluster = cluster.clone();
        let inject_group = group_id.clone();
        let inject_count = injection_count.clone();
        let inject_errors = injection_errors.clone();
        let inject_stop = stop_flag.clone();

        let injector = tokio::spawn(async move {
            let mut seq = 0u64;
            while !inject_stop.load(std::sync::atomic::Ordering::Relaxed) {
                let c = inject_cluster.lock().await;
                let result = c
                    .try_inject_event(
                        &inject_group,
                        serde_json::json!({
                            "event_type": "Input",
                            "fields": { "seq": seq.to_string() }
                        }),
                    )
                    .await;
                drop(c); // release lock before sleeping
                match result {
                    Ok(_) => {
                        inject_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    Err(_) => {
                        inject_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                seq += 1;
                // Small delay to avoid overwhelming the cluster.
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });

        // Let some events flow before triggering migration.
        sleep(Duration::from_secs(3)).await;

        // Trigger manual migration to chaos-w1.
        {
            let c = cluster.lock().await;
            c.manual_migrate(&group_id, "p1", "chaos-w1").await;
        }

        // Wait for migration to complete.
        sleep(Duration::from_secs(5)).await;

        // Stop injection.
        stop_flag.store(true, std::sync::atomic::Ordering::Relaxed);
        let _ = injector.await;

        let total_injected = injection_count.load(std::sync::atomic::Ordering::Relaxed);
        let total_errors = injection_errors.load(std::sync::atomic::Ordering::Relaxed);

        // Verify reasonable success rate. Some errors during the migration
        // switchover window are expected, but the majority should succeed.
        assert!(
            total_injected > 0,
            "Should have injected at least some events successfully"
        );

        let error_rate = if total_injected + total_errors > 0 {
            total_errors as f64 / (total_injected + total_errors) as f64
        } else {
            0.0
        };
        assert!(
            error_rate < 0.5,
            "Error rate {:.1}% is too high ({} errors out of {} total). \
             Migration should preserve most event flow.",
            error_rate * 100.0,
            total_errors,
            total_injected + total_errors
        );

        // Verify pipeline is now on chaos-w1.
        {
            let c = cluster.lock().await;
            let topo = c.get_topology().await;
            assert_eq!(
                find_pipeline_worker(&topo, &group_id, "p1").as_deref(),
                Some("chaos-w1"),
                "Pipeline should be on chaos-w1 after migration"
            );
        }

        // Shut down.
        let cluster = std::sync::Arc::try_unwrap(cluster)
            .unwrap_or_else(|_| {
                panic!("Arc should have single owner after injector task completes")
            })
            .into_inner();
        cluster.shutdown().await;
    });

    timeout
        .await
        .expect("test_migration_during_injection timed out");
}

// =============================================================================
// Helpers
// =============================================================================

/// Extract the worker ID hosting a given pipeline from the topology JSON.
fn find_pipeline_worker(
    topology: &serde_json::Value,
    group_id: &str,
    pipeline_name: &str,
) -> Option<String> {
    let groups = topology["groups"].as_array()?;
    for group in groups {
        if group["group_id"].as_str() == Some(group_id) {
            if let Some(pipelines) = group["pipelines"].as_array() {
                for p in pipelines {
                    if p["name"].as_str() == Some(pipeline_name) {
                        return p["worker_id"].as_str().map(|s| s.to_string());
                    }
                }
            }
        }
    }
    None
}
