//! Sustained / soak chaos tests — long-running stability under random failures.

use super::ProcessCluster;
use std::time::{Duration, Instant};
use tokio::time::sleep;

// =============================================================================
// Test 16: Chaos monkey — random kills and adds for 30 seconds
// =============================================================================

/// Run a 30-second chaos loop that randomly kills and adds workers while
/// injecting events. Verify the coordinator remains stable throughout.
#[tokio::test]
#[ignore]
async fn test_chaos_monkey() {
    let timeout = tokio::time::timeout(Duration::from_secs(120), async {
        let mut cluster = ProcessCluster::start(3).await;

        // Deploy 2 pipeline groups.
        let gid1 = cluster
            .deploy_group(serde_json::json!({
                "name": "chaos-group-1",
                "pipelines": [{
                    "name": "p1",
                    "source": "stream Out1 = Input1"
                }],
                "routes": []
            }))
            .await;
        let gid2 = cluster
            .deploy_group(serde_json::json!({
                "name": "chaos-group-2",
                "pipelines": [{
                    "name": "p2",
                    "source": "stream Out2 = Input2"
                }],
                "routes": []
            }))
            .await;

        let chaos_start = Instant::now();
        let chaos_duration = Duration::from_secs(30);
        let mut round = 0u32;
        let mut successful_injects = 0u64;
        let mut failed_injects = 0u64;

        // Simple pseudo-random using round number.
        while chaos_start.elapsed() < chaos_duration {
            round += 1;
            let action_delay = Duration::from_secs(5 + (round as u64 % 4));
            sleep(action_delay).await;

            let alive_count = cluster.workers.len();
            let should_kill = !round.is_multiple_of(3) && alive_count > 1;

            if should_kill {
                // Kill a random worker (pick by index).
                let idx = (round as usize) % alive_count;
                let worker_id = cluster.workers[idx].id.clone();
                eprintln!(
                    "  [chaos] round {}: killing {} ({} alive)",
                    round, worker_id, alive_count
                );
                cluster.kill_worker(&worker_id);
            } else {
                // Add a new worker.
                let new_id = cluster.add_worker().await;
                eprintln!(
                    "  [chaos] round {}: added {} ({} alive)",
                    round,
                    new_id,
                    cluster.workers.len()
                );
            }

            // Try injecting events after each action.
            for gid in [&gid1, &gid2] {
                for i in 0..5 {
                    let result = cluster
                        .try_inject_event(
                            gid,
                            serde_json::json!({
                                "event_type": if *gid == gid1 { "Input1" } else { "Input2" },
                                "fields": { "round": round.to_string(), "i": i.to_string() }
                            }),
                        )
                        .await;
                    match result {
                        Ok(_) => successful_injects += 1,
                        Err(_) => failed_injects += 1,
                    }
                }
            }
        }

        eprintln!(
            "  [chaos] Completed {} rounds: {} successful injects, {} failed",
            round, successful_injects, failed_injects
        );

        // After chaos: coordinator should still be responsive.
        let workers = cluster.list_workers().await;
        assert!(
            workers["total"].as_u64().is_some(),
            "Coordinator should respond after chaos"
        );

        // At least 1 worker should be alive.
        assert!(
            !cluster.workers.is_empty(),
            "At least 1 worker should still be alive"
        );

        // Topology endpoint should work.
        let topo = cluster.get_topology().await;
        assert!(topo["groups"].is_array(), "Topology should return groups");

        cluster.shutdown().await;
    });

    timeout.await.expect("test_chaos_monkey timed out");
}
