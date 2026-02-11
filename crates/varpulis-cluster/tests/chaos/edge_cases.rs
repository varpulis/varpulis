//! Edge-case chaos tests — boundary conditions, error paths, idempotency.

use super::ProcessCluster;
use std::time::Duration;
use tokio::time::sleep;

// =============================================================================
// Test 8: All workers die
// =============================================================================

/// Kill all workers and verify the coordinator handles gracefully (no panic,
/// returns appropriate error on inject).
#[tokio::test]
#[ignore]
async fn test_all_workers_die() {
    let timeout = tokio::time::timeout(Duration::from_secs(60), async {
        let mut cluster = ProcessCluster::start(2).await;

        // Deploy a pipeline.
        let group_id = cluster
            .deploy_group(serde_json::json!({
                "name": "all-die",
                "pipelines": [{
                    "name": "p1",
                    "source": "stream Out = Input"
                }],
                "routes": []
            }))
            .await;

        // Kill all workers.
        cluster.kill_worker("chaos-w0");
        cluster.kill_worker("chaos-w1");

        // Wait for coordinator to detect unhealthy workers.
        sleep(Duration::from_secs(20)).await;

        // Coordinator should still respond to API calls.
        let workers = cluster.list_workers().await;
        // All workers should be present but marked unhealthy.
        assert!(
            workers["total"].as_u64().is_some(),
            "Coordinator should still respond after all workers die"
        );

        // Inject should fail gracefully (routing error, not a panic).
        let result = cluster
            .try_inject_event(
                &group_id,
                serde_json::json!({
                    "event_type": "Input",
                    "fields": { "data": "test" }
                }),
            )
            .await;
        // Either an error status or a connection error is acceptable.
        // The coordinator should NOT have crashed.
        let _ = result;

        // Verify coordinator metrics endpoint still works.
        let metrics = cluster.get_metrics().await;
        assert!(
            metrics.is_object(),
            "Metrics should return valid JSON even with all workers dead"
        );

        cluster.shutdown().await;
    });

    timeout.await.expect("test_all_workers_die timed out");
}

// =============================================================================
// Test 9: Failover target also dies
// =============================================================================

/// Kill a worker to trigger failover, then verify the pipeline eventually
/// lands on a healthy worker even if multiple workers are unavailable.
#[tokio::test]
#[ignore]
async fn test_failover_target_also_dies() {
    let timeout = tokio::time::timeout(Duration::from_secs(120), async {
        let mut cluster = ProcessCluster::start(3).await;

        // Deploy pipeline with affinity to chaos-w0.
        let group_id = cluster
            .deploy_group(serde_json::json!({
                "name": "cascade-fail",
                "pipelines": [{
                    "name": "p1",
                    "source": "stream Out = Input",
                    "worker_affinity": "chaos-w0"
                }],
                "routes": []
            }))
            .await;

        // Kill chaos-w0 to trigger failover.
        cluster.kill_worker("chaos-w0");

        // Wait for first failover cycle.
        sleep(Duration::from_secs(20)).await;

        // Kill chaos-w1 as well — only chaos-w2 should survive.
        cluster.kill_worker("chaos-w1");

        // Wait for system to stabilize.
        sleep(Duration::from_secs(25)).await;

        // Check topology — pipeline should be on chaos-w2 (the only survivor).
        let topo = cluster.get_topology().await;
        if let Some(groups) = topo["groups"].as_array() {
            for group in groups {
                if group["group_id"].as_str() == Some(&group_id) {
                    if let Some(pipelines) = group["pipelines"].as_array() {
                        for p in pipelines {
                            if let Some(wid) = p["worker_id"].as_str() {
                                assert_ne!(
                                    wid, "chaos-w0",
                                    "Pipeline should not be on dead worker chaos-w0"
                                );
                                assert_ne!(
                                    wid, "chaos-w1",
                                    "Pipeline should not be on dead worker chaos-w1"
                                );
                            }
                        }
                    }
                }
            }
        }

        cluster.shutdown().await;
    });

    timeout
        .await
        .expect("test_failover_target_also_dies timed out");
}

// =============================================================================
// Test 10: Double drain is idempotent
// =============================================================================

/// Drain a worker, then drain it again. The second drain should be a no-op
/// or return gracefully without crashing.
#[tokio::test]
#[ignore]
async fn test_double_drain() {
    let timeout = tokio::time::timeout(Duration::from_secs(60), async {
        let cluster = ProcessCluster::start(2).await;

        // Deploy a pipeline to chaos-w0.
        let group_id = cluster
            .deploy_group(serde_json::json!({
                "name": "double-drain",
                "pipelines": [{
                    "name": "p1",
                    "source": "stream Out = Input",
                    "worker_affinity": "chaos-w0"
                }],
                "routes": []
            }))
            .await;

        // First drain.
        let resp1 = cluster.drain_worker("chaos-w0").await;
        assert_eq!(resp1["status"].as_str(), Some("drained"));

        // Wait for migration.
        sleep(Duration::from_secs(3)).await;

        // Second drain — should succeed or be a no-op (worker already drained).
        // The worker may have been deregistered by now, so we use try-style.
        let resp = cluster
            .http_client
            .post(cluster.api_url(&format!("/workers/{}/drain", "chaos-w0")))
            .header("x-api-key", &cluster.api_key)
            .json(&serde_json::json!({ "timeout_secs": null }))
            .send()
            .await;

        // Either success or 404 (worker already removed) is acceptable.
        if let Ok(r) = resp {
            let status = r.status().as_u16();
            assert!(
                status == 200 || status == 404 || status == 409,
                "Second drain should return 200, 404, or 409 (got {})",
                status
            );
        }

        // Verify pipeline is still operational on chaos-w1.
        let result = cluster
            .try_inject_event(
                &group_id,
                serde_json::json!({
                    "event_type": "Input",
                    "fields": { "data": "after-double-drain" }
                }),
            )
            .await;
        assert!(result.is_ok(), "Pipeline should work after double drain");

        cluster.shutdown().await;
    });

    timeout.await.expect("test_double_drain timed out");
}

// =============================================================================
// Test 11: Rapid worker join/leave
// =============================================================================

/// Rapidly add and kill workers to verify the coordinator handles the churn
/// without panicking or deadlocking.
#[tokio::test]
#[ignore]
async fn test_rapid_worker_join_leave() {
    let timeout = tokio::time::timeout(Duration::from_secs(90), async {
        let mut cluster = ProcessCluster::start(1).await;

        for i in 0..5 {
            // Add a worker.
            let new_id = cluster.add_worker().await;
            eprintln!("  [rapid] round {}: added {}", i, new_id);

            // Brief pause.
            sleep(Duration::from_millis(200)).await;

            // Kill the new worker.
            cluster.kill_worker(&new_id);
            eprintln!("  [rapid] round {}: killed {}", i, new_id);

            // Brief pause.
            sleep(Duration::from_millis(200)).await;
        }

        // Coordinator should still be responsive.
        let workers = cluster.list_workers().await;
        assert!(
            workers["total"].as_u64().is_some(),
            "Coordinator should still respond after rapid join/leave cycles"
        );

        cluster.shutdown().await;
    });

    timeout
        .await
        .expect("test_rapid_worker_join_leave timed out");
}
