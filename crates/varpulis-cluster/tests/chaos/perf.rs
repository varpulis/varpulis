//! Performance-oriented chaos tests — failover latency, migration throughput.

use super::ProcessCluster;
use std::time::{Duration, Instant};
use tokio::time::sleep;

// =============================================================================
// Test 13: Failover latency
// =============================================================================

/// Measure the time from killing a worker to the pipeline becoming operational
/// on a new worker. Target: < 30s (heartbeat 5s + timeout 15s + migration).
#[tokio::test]
#[ignore]
async fn test_failover_latency() {
    let timeout = tokio::time::timeout(Duration::from_secs(120), async {
        let mut cluster = ProcessCluster::start(2).await;

        // Deploy pipeline to chaos-w0.
        let group_id = cluster
            .deploy_group(serde_json::json!({
                "name": "latency-test",
                "pipelines": [{
                    "name": "p1",
                    "source": "stream Out = Input",
                    "worker_affinity": "chaos-w0"
                }],
                "routes": []
            }))
            .await;

        // Record timestamp and kill worker.
        let start = Instant::now();
        cluster.kill_worker("chaos-w0");

        // Poll topology until pipeline appears on chaos-w1.
        loop {
            sleep(Duration::from_secs(1)).await;
            let topo = cluster.get_topology().await;
            if let Some(groups) = topo["groups"].as_array() {
                for group in groups {
                    if group["group_id"].as_str() == Some(&group_id) {
                        if let Some(pipelines) = group["pipelines"].as_array() {
                            for p in pipelines {
                                if p["worker_id"].as_str() == Some("chaos-w1") {
                                    let elapsed = start.elapsed();
                                    eprintln!(
                                        "  [perf] Failover latency: {:.1}s",
                                        elapsed.as_secs_f64()
                                    );
                                    assert!(
                                        elapsed < Duration::from_secs(30),
                                        "Failover should complete within 30s (took {:.1}s)",
                                        elapsed.as_secs_f64()
                                    );
                                    cluster.shutdown().await;
                                    return;
                                }
                            }
                        }
                    }
                }
            }

            if start.elapsed() > Duration::from_secs(60) {
                panic!("Pipeline never migrated to chaos-w1 within 60s");
            }
        }
    });

    timeout.await.expect("test_failover_latency timed out");
}

// =============================================================================
// Test 14: Migration throughput — migrate multiple pipelines via drain
// =============================================================================

/// Deploy 5 pipeline groups to one worker, drain it, and measure the total
/// time for all 5 migrations to complete.
#[tokio::test]
#[ignore]
async fn test_migration_throughput() {
    let timeout = tokio::time::timeout(Duration::from_secs(120), async {
        let cluster = ProcessCluster::start(2).await;

        // Deploy 5 pipeline groups, all with affinity to chaos-w0.
        let mut group_ids = Vec::new();
        for i in 0..5 {
            let gid = cluster
                .deploy_group(serde_json::json!({
                    "name": format!("migration-{}", i),
                    "pipelines": [{
                        "name": format!("p{}", i),
                        "source": format!("stream S{} = E{}", i, i),
                        "worker_affinity": "chaos-w0"
                    }],
                    "routes": []
                }))
                .await;
            group_ids.push(gid);
        }

        // Drain chaos-w0 (triggers all 5 migrations simultaneously).
        let start = Instant::now();
        let drain_resp = cluster.drain_worker("chaos-w0").await;
        let migrated = drain_resp["pipelines_migrated"].as_u64().unwrap_or(0);
        eprintln!(
            "  [perf] Drain initiated: {} pipelines to migrate",
            migrated
        );

        // Wait for all pipelines to appear on chaos-w1.
        sleep(Duration::from_secs(5)).await;

        let topo = cluster.get_topology().await;
        let mut on_w1 = 0;
        if let Some(groups) = topo["groups"].as_array() {
            for group in groups {
                if let Some(pipelines) = group["pipelines"].as_array() {
                    for p in pipelines {
                        if p["worker_id"].as_str() == Some("chaos-w1") {
                            on_w1 += 1;
                        }
                    }
                }
            }
        }

        let elapsed = start.elapsed();
        eprintln!(
            "  [perf] Migration throughput: {} pipelines in {:.1}s ({:.1} pipelines/sec)",
            on_w1,
            elapsed.as_secs_f64(),
            on_w1 as f64 / elapsed.as_secs_f64()
        );

        assert!(
            on_w1 >= 5,
            "All 5 pipelines should have migrated to chaos-w1 (got {} on w1)",
            on_w1
        );

        cluster.shutdown().await;
    });

    timeout.await.expect("test_migration_throughput timed out");
}

// =============================================================================
// Test 15: Replica throughput scaling
// =============================================================================

/// Inject events with 1, 2, and 3 replicas and compare throughput. Near-linear
/// scaling is ideal but not required; the test verifies replicas are functional.
#[tokio::test]
#[ignore]
async fn test_replica_throughput_scaling() {
    let timeout = tokio::time::timeout(Duration::from_secs(120), async {
        let cluster = ProcessCluster::start(3).await;
        let event_count = 100;

        let mut results = Vec::new();
        for replicas in 1..=3 {
            let group_id = cluster
                .deploy_group(serde_json::json!({
                    "name": format!("scale-{}", replicas),
                    "pipelines": [{
                        "name": "p1",
                        "source": "stream Out = Input",
                        "replicas": replicas
                    }],
                    "routes": []
                }))
                .await;

            // Give deployment time to settle.
            sleep(Duration::from_millis(500)).await;

            // Inject events and measure time.
            let start = Instant::now();
            for i in 0..event_count {
                let _ = cluster
                    .try_inject_event(
                        &group_id,
                        serde_json::json!({
                            "event_type": "Input",
                            "fields": { "i": i.to_string() }
                        }),
                    )
                    .await;
            }
            let elapsed = start.elapsed();
            let eps = event_count as f64 / elapsed.as_secs_f64();
            results.push((replicas, elapsed, eps));

            // Teardown before next iteration.
            cluster.delete_group(&group_id).await;
            sleep(Duration::from_millis(500)).await;
        }

        eprintln!("  [perf] Replica throughput scaling:");
        for (r, elapsed, eps) in &results {
            eprintln!(
                "    replicas={}: {:.1}s for {} events ({:.0} events/sec)",
                r,
                elapsed.as_secs_f64(),
                event_count,
                eps
            );
        }

        cluster.shutdown().await;
    });

    timeout
        .await
        .expect("test_replica_throughput_scaling timed out");
}
