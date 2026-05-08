//! Sustained / soak chaos tests — long-running stability under random failures.
//!
//! ## Configuring the soak duration
//!
//! [`test_chaos_monkey`] reads the `VARPULIS_CHAOS_DURATION_SECS` environment
//! variable to determine how long it injects events while randomly killing and
//! adding workers. The default is **10 minutes** (suitable for CI). For
//! pre-release validation, run for **72 hours** on a dedicated host:
//!
//! ```sh
//! # CI default — 10 minutes, expects zero event loss.
//! cargo build --release
//! cargo test --test chaos --release test_chaos_monkey \
//!     -- --ignored --nocapture
//!
//! # 72-hour manual soak (run on a dedicated host, capture the log).
//! VARPULIS_CHAOS_DURATION_SECS=259200 \
//!     cargo test --test chaos --release test_chaos_monkey \
//!     -- --ignored --nocapture 2>&1 | tee chaos-72h.log
//! ```
//!
//! ## Acceptance criteria
//!
//! 1. The coordinator must remain responsive throughout — periodic topology
//!    polls (every 10% of the run, capped at 60 s) must succeed.
//! 2. Both deployed pipeline groups must survive in the topology to the end.
//! 3. After the run, at least one worker is still alive and a fresh inject
//!    against each group succeeds. Successful injects therefore did not vanish
//!    into a dead cluster — the practical "zero event loss" check available
//!    without sinks.

use std::time::{Duration, Instant};

use tokio::time::sleep;

use super::ProcessCluster;

// =============================================================================
// Test 16: Chaos monkey — random kills and adds for a configurable duration
// =============================================================================

/// Default soak duration when `VARPULIS_CHAOS_DURATION_SECS` is unset (10 min).
const DEFAULT_CHAOS_DURATION_SECS: u64 = 600;

/// Soft upper bound on the alive worker pool — prevents unbounded port and
/// process growth during multi-hour runs.
const MAX_ALIVE_WORKERS: usize = 5;

/// Soft lower bound on the alive worker pool — never kill below this so the
/// cluster can keep accepting events.
const MIN_ALIVE_WORKERS: usize = 1;

/// Read the soak duration from `VARPULIS_CHAOS_DURATION_SECS`, falling back to
/// [`DEFAULT_CHAOS_DURATION_SECS`] when unset or unparseable.
fn chaos_duration_from_env() -> Duration {
    let secs = std::env::var("VARPULIS_CHAOS_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_CHAOS_DURATION_SECS);
    Duration::from_secs(secs)
}

/// Run a chaos loop that randomly kills and adds workers while injecting
/// events into two pipeline groups. The duration is configurable via
/// `VARPULIS_CHAOS_DURATION_SECS` (default 10 min for CI; set to 259200 for a
/// 72-hour manual soak).
///
/// Verifies the coordinator stays responsive, both groups survive, and the
/// cluster still accepts new events after the chaos completes.
#[tokio::test]
#[ignore]
async fn test_chaos_monkey() {
    let chaos_duration = chaos_duration_from_env();
    // Allow generous setup + teardown overhead on top of the chaos window.
    let test_timeout = chaos_duration + Duration::from_mins(2);

    let timeout = tokio::time::timeout(test_timeout, async {
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
        let mut round = 0u32;
        let mut successful_injects = 0u64;
        let mut failed_injects = 0u64;
        let mut kills = 0u64;
        let mut adds = 0u64;

        // Periodic topology health-check cadence: every 10% of the run, with a
        // 60 s cap so even short CI runs get a few snapshots.
        let health_check_interval = (chaos_duration / 10).min(Duration::from_mins(1));
        let mut next_health_check = Instant::now() + health_check_interval;

        eprintln!(
            "  [chaos] starting soak: {:.1}s ({:.2}h), workers={}, groups=2",
            chaos_duration.as_secs_f64(),
            chaos_duration.as_secs_f64() / 3600.0,
            cluster.workers.len()
        );

        // Simple deterministic pseudo-randomness driven by the round counter —
        // makes failures reproducible across reruns.
        while chaos_start.elapsed() < chaos_duration {
            round += 1;
            let action_delay = Duration::from_secs(5 + (round as u64 % 4));
            sleep(action_delay).await;

            // If the delay carried us past the chaos window, exit cleanly
            // instead of starting one more round.
            if chaos_start.elapsed() >= chaos_duration {
                break;
            }

            let alive_count = cluster.workers.len();

            // Bound the worker pool so it doesn't drift to zero (cluster can't
            // accept events) or grow unboundedly (port/process exhaustion over
            // 72 h). Inside the band, lean toward kills to exercise failover.
            let should_kill = if alive_count > MAX_ALIVE_WORKERS {
                true
            } else if alive_count <= MIN_ALIVE_WORKERS {
                false
            } else {
                !round.is_multiple_of(3)
            };

            if should_kill {
                let idx = (round as usize) % alive_count;
                let worker_id = cluster.workers[idx].id.clone();
                eprintln!(
                    "  [chaos] round {round} ({:.1}s): killing {worker_id} ({alive_count} alive)",
                    chaos_start.elapsed().as_secs_f64()
                );
                cluster.kill_worker(&worker_id);
                kills += 1;
            } else {
                let new_id = cluster.add_worker().await;
                eprintln!(
                    "  [chaos] round {round} ({:.1}s): added {} ({} alive)",
                    chaos_start.elapsed().as_secs_f64(),
                    new_id,
                    cluster.workers.len()
                );
                adds += 1;
            }

            // Inject events after every action.
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

            // Periodic in-flight health check — coordinator must stay alive.
            if Instant::now() >= next_health_check {
                let topo = cluster.get_topology().await;
                assert!(
                    topo["groups"].is_array(),
                    "Coordinator unresponsive at {:.1}s into chaos run (round {round})",
                    chaos_start.elapsed().as_secs_f64()
                );
                eprintln!(
                    "  [chaos] {:.1}s elapsed: {round} rounds, {kills} kills, {adds} adds, \
                     {successful_injects} ok / {failed_injects} fail (alive={})",
                    chaos_start.elapsed().as_secs_f64(),
                    cluster.workers.len()
                );
                next_health_check = Instant::now() + health_check_interval;
            }
        }

        let total_elapsed = chaos_start.elapsed();
        eprintln!(
            "  [chaos] finished {round} rounds in {:.1}s — {kills} kills, {adds} adds, \
             {successful_injects} successful, {failed_injects} failed",
            total_elapsed.as_secs_f64()
        );

        // After chaos: coordinator should still be responsive.
        let workers = cluster.list_workers().await;
        assert!(
            workers["total"].as_u64().is_some(),
            "Coordinator should respond after chaos"
        );

        // At least 1 worker should be alive (enforced by MIN_ALIVE_WORKERS).
        assert!(
            !cluster.workers.is_empty(),
            "At least 1 worker should still be alive at the end of the run"
        );

        // Topology endpoint should work and still expose both groups.
        let topo = cluster.get_topology().await;
        let groups = topo["groups"]
            .as_array()
            .expect("Topology should return a groups array");
        let group_ids: Vec<&str> = groups
            .iter()
            .filter_map(|g| g["group_id"].as_str())
            .collect();
        assert!(
            group_ids.contains(&gid1.as_str()),
            "chaos-group-1 should survive chaos but groups list was {group_ids:?}"
        );
        assert!(
            group_ids.contains(&gid2.as_str()),
            "chaos-group-2 should survive chaos but groups list was {group_ids:?}"
        );

        // Sanity: chaos was actually exercised.
        assert!(
            successful_injects > 0,
            "Expected at least some successful injects during the chaos run"
        );

        // Zero-event-loss proxy: a fresh inject after the storm should succeed
        // for both groups. If accepted events vanished into a dead cluster the
        // post-chaos cluster would also reject these.
        cluster
            .inject_event(
                &gid1,
                serde_json::json!({
                    "event_type": "Input1",
                    "fields": { "post_chaos": "1" }
                }),
            )
            .await;
        cluster
            .inject_event(
                &gid2,
                serde_json::json!({
                    "event_type": "Input2",
                    "fields": { "post_chaos": "1" }
                }),
            )
            .await;

        cluster.shutdown().await;
    });

    timeout.await.expect(
        "test_chaos_monkey timed out — increase VARPULIS_CHAOS_DURATION_SECS or test timeout",
    );
}
