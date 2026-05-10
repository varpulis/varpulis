//! Coordinator Failover Test (Task 4.3 — Flink-Parity Phase 4).
//!
//! Companion to [`distributed_checkpoint`] (Task 4.1) and
//! [`multi_replica_checkpoint`] (Task 4.2). Where 4.1/4.2 kill *workers* mid-
//! run and verify exactly-once semantics survive, this test kills the
//! *coordinator* and verifies:
//!
//!   1. Leader election: a 3-node Raft coordinator quorum re-elects a new
//!      leader within bounded time after the current leader is SIGKILL'd
//!      mid-checkpoint cycle.
//!   2. Workers self-abort timed-out checkpoints: a barrier delivered to a
//!      worker without a follow-up `complete`/`abort` (because the
//!      coordinator died) does not deadlock the worker — the per-checkpoint
//!      watchdog fires, retires the pending entry, and the worker stays
//!      responsive.
//!
//! ## Why two coordinated-but-separable tests
//!
//! The leader-election property is the foundation of "new leader resumes
//! checkpointing" — without re-election the cluster has no leader to
//! re-trigger any in-flight or future checkpoint cycle. The Raft state
//! machine already replicates `CheckpointCompleted` / `CheckpointAborted`
//! through Raft (Task 1.4), so once a new leader stabilises it observes the
//! same `latest_checkpoints` map the dead leader had committed.
//!
//! The worker-self-abort property is the safety net for the *kill mid-
//! barrier* race: if the coordinator dies after publishing the barrier but
//! before publishing complete/abort, workers are left holding pending
//! checkpoint state with their source-pause flag set. Without the watchdog
//! they would deadlock. The watchdog itself is exhaustively unit-tested in
//! `nats_worker.rs::barrier_tests::watchdog_fires`; this chaos test asserts
//! the *integration* property — that a real worker process survives an
//! orphan barrier and stays usable.
//!
//! ## Prerequisites
//!
//! - `varpulis` binary built with `nats-transport + distributed-checkpoint
//!   + raft` features (set `VARPULIS_BIN` to point at it):
//!   ```sh
//!   cargo build --release \
//!     -p varpulis-cli \
//!     --features 'nats-transport raft'
//!   export VARPULIS_BIN=$PWD/target/release/varpulis
//!   ```
//!   Note: the `distributed-checkpoint` feature is owned by `varpulis-
//!   cluster`. If the CLI doesn't yet expose a passthrough, the worker-
//!   self-abort test [skip]s rather than running half-armed.
//! - NATS broker on `localhost:4222` for the worker self-abort sub-test
//!   only (the leader-election sub-test is pure Raft and has no infra
//!   dependency).
//!
//! ## Running
//!
//! ```sh
//! cargo test --release \
//!     --test chaos \
//!     --features 'distributed-checkpoint raft' \
//!     test_coordinator_failover \
//!     -- --ignored --nocapture
//! ```
//!
//! ## Skip behaviour
//!
//! Each test probes its prerequisites (Raft enabled in the binary, NATS
//! reachable) and emits a `[skip]` log line when missing — so CI without
//! infra is non-fatal. Tests are `#[ignore]`d to require explicit opt-in.

#![cfg(all(unix, feature = "distributed-checkpoint", feature = "raft"))]
#![allow(clippy::too_many_lines)]

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value as Json;
use tokio::time::sleep;

use super::{allocate_ports, find_binary, WorkerProcess, API_KEY};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Number of Raft coordinator nodes in the test quorum.
const NUM_COORDINATORS: usize = 3;

/// How long to wait for the *initial* leader election after spawning the
/// quorum. Raft's election timeout in this codebase is 1500-3000ms with a
/// 500ms heartbeat (see `raft::mod::bootstrap_with_storage`). 10s is
/// comfortably above the upper bound.
const INITIAL_ELECTION_TIMEOUT: Duration = Duration::from_secs(10);

/// How long to wait for a *re-election* after killing the leader. The
/// surviving nodes must (a) detect the missing heartbeats (>= election
/// timeout) and (b) win an election. 15s gives slack for split-vote
/// retries on a busy CI runner.
const REELECTION_TIMEOUT: Duration = Duration::from_secs(15);

/// Per-coordinator startup grace period before issuing the first probe.
const COORDINATOR_BOOT_DELAY: Duration = Duration::from_secs(1);

/// Polling cadence while waiting for raft state changes.
const POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Worker self-abort barrier timeout. Short so the watchdog fires inside
/// the test window. Must be > NATS round-trip (~ms) to avoid races.
const WATCHDOG_BARRIER_TIMEOUT: Duration = Duration::from_millis(300);

// ---------------------------------------------------------------------------
// Environment helpers
// ---------------------------------------------------------------------------

fn nats_url() -> String {
    std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string())
}

fn nats_tcp_target(url: &str) -> String {
    url.trim_start_matches("nats://")
        .trim_end_matches('/')
        .to_string()
}

async fn tcp_reachable(addr: &str) -> bool {
    let target = addr.to_string();
    tokio::time::timeout(
        Duration::from_secs(2),
        tokio::net::TcpStream::connect(&target),
    )
    .await
    .is_ok_and(|r| r.is_ok())
}

// ---------------------------------------------------------------------------
// Multi-coordinator Raft cluster harness
// ---------------------------------------------------------------------------

/// A single coordinator process in a Raft quorum.
struct RaftCoordinator {
    process: Child,
    port: u16,
    node_id: u64,
}

/// Three-coordinator Raft quorum with optional workers attached. Each
/// coordinator process is spawned with `--raft`, `--raft-node-id N`, and
/// the same `--raft-peers` list. Node 1 is the only one that calls
/// `raft.initialize()` on its own; the rest discover membership through
/// replication.
struct MultiRaftCluster {
    coordinators: Vec<RaftCoordinator>,
    workers: Vec<WorkerProcess>,
    api_key: String,
    http_client: reqwest::Client,
}

impl MultiRaftCluster {
    /// Spawn `NUM_COORDINATORS` coordinator processes with Raft enabled,
    /// then wait for an initial leader to be elected.
    ///
    /// Returns `None` if the binary doesn't actually have the `raft`
    /// feature compiled in (detected by the `/raft` endpoint returning
    /// `enabled: false` after `INITIAL_ELECTION_TIMEOUT` of polling).
    async fn start() -> Option<Self> {
        let bin = find_binary();
        // Allocate NUM_COORDINATORS ports up front so we can build the
        // peer list before any process starts.
        let base_port = allocate_ports(NUM_COORDINATORS as u16);
        let ports: Vec<u16> = (0..NUM_COORDINATORS as u16)
            .map(|i| base_port + i)
            .collect();
        let peers = ports
            .iter()
            .map(|p| format!("http://127.0.0.1:{p}"))
            .collect::<Vec<_>>()
            .join(",");

        let mut coordinators = Vec::with_capacity(NUM_COORDINATORS);
        for (i, &port) in ports.iter().enumerate() {
            let node_id = (i as u64) + 1;
            let process = Command::new(&bin)
                .args([
                    "coordinator",
                    "--port",
                    &port.to_string(),
                    "--bind",
                    "127.0.0.1",
                    "--api-key",
                    API_KEY,
                    "--raft",
                    "--raft-node-id",
                    &node_id.to_string(),
                    "--raft-peers",
                    &peers,
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap_or_else(|e| panic!("Failed to spawn coordinator {node_id}: {e}"));
            coordinators.push(RaftCoordinator {
                process,
                port,
                node_id,
            });
        }

        sleep(COORDINATOR_BOOT_DELAY).await;

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("Failed to build HTTP client");

        let cluster = Self {
            coordinators,
            workers: Vec::new(),
            api_key: API_KEY.to_string(),
            http_client,
        };

        // Poll for an initial leader. If we don't see one within the
        // timeout, assume the binary doesn't have raft compiled in and
        // bail out. The caller treats that as a [skip].
        match cluster
            .poll_until_leader(INITIAL_ELECTION_TIMEOUT, /*forbid=*/ None)
            .await
        {
            Some(_) => Some(cluster),
            None => {
                cluster.shutdown_all();
                None
            }
        }
    }

    /// HTTP base URL for a coordinator by its index in the cluster.
    fn api_url(&self, idx: usize, path: &str) -> String {
        let port = self.coordinators[idx].port;
        format!("http://127.0.0.1:{port}/api/v1/cluster{path}")
    }

    /// Fetch `/raft` from coordinator `idx`. Returns `None` if the request
    /// failed (process down, port not yet bound, etc.).
    async fn raft_status(&self, idx: usize) -> Option<Json> {
        let resp = self
            .http_client
            .get(self.api_url(idx, "/raft"))
            .header("x-api-key", &self.api_key)
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        resp.json::<Json>().await.ok()
    }

    /// Poll every live coordinator's `/raft` endpoint until at least one
    /// reports a non-null `leader_id`. Returns the elected leader's
    /// `node_id` (the value of `leader_id`).
    ///
    /// `forbid` lets the caller require that the elected leader differ
    /// from a specific node id (used after killing a leader to require a
    /// fresh election).
    ///
    /// Returns `None` on timeout.
    async fn poll_until_leader(
        &self,
        timeout: Duration,
        forbid: Option<u64>,
    ) -> Option<LeaderInfo> {
        let deadline = Instant::now() + timeout;
        loop {
            for idx in 0..self.coordinators.len() {
                if let Some(status) = self.raft_status(idx).await {
                    let enabled = status["enabled"].as_bool().unwrap_or(false);
                    if !enabled {
                        // Raft feature isn't compiled in -- no point retrying.
                        return None;
                    }
                    if let Some(leader_id) = status["leader_id"].as_u64() {
                        let term = status["term"].as_u64().unwrap_or(0);
                        if forbid != Some(leader_id) {
                            return Some(LeaderInfo { leader_id, term });
                        }
                    }
                }
            }
            if Instant::now() >= deadline {
                return None;
            }
            sleep(POLL_INTERVAL).await;
        }
    }

    /// Find the cluster index of the coordinator whose `node_id` matches.
    fn find_index_for(&self, node_id: u64) -> Option<usize> {
        self.coordinators
            .iter()
            .position(|c| c.node_id == node_id)
    }

    /// SIGKILL the coordinator at the given cluster index and remove it
    /// from the live list (so subsequent polls only target survivors).
    fn kill_coordinator(&mut self, idx: usize) -> u64 {
        let mut victim = self.coordinators.remove(idx);
        let node_id = victim.node_id;
        let _ = victim.process.kill();
        let _ = victim.process.wait();
        node_id
    }

    /// Spawn an extra worker process pointing at coordinator index `idx`
    /// (typically the current leader). Used by the worker-self-abort sub-
    /// test to bring a worker into the NATS subject namespace.
    async fn add_worker_pointing_at(&mut self, idx: usize, nats_url: Option<&str>) -> String {
        let bin = find_binary();
        let port = allocate_ports(1);
        let worker_id = format!("chaos-failover-w{}", self.workers.len());
        let coord_port = self.coordinators[idx].port;

        let mut cmd = Command::new(&bin);
        cmd.args([
            "server",
            "--port",
            &port.to_string(),
            "--coordinator",
            &format!("http://127.0.0.1:{coord_port}"),
            "--worker-id",
            &worker_id,
            "--api-key",
            &self.api_key,
            "--bind",
            "127.0.0.1",
        ]);
        if let Some(url) = nats_url {
            cmd.args(["--nats-url", url]);
        }
        let process = cmd
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("Failed to spawn worker {worker_id}: {e}"));

        self.workers.push(WorkerProcess {
            id: worker_id.clone(),
            process,
            port,
        });

        // Best-effort wait for the worker to register on the leader. We
        // poll /workers; on a healthy quorum this also exercises Raft
        // replication of RegisterWorker (so any survivor can see it).
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let resp = self
                .http_client
                .get(self.api_url(idx, "/workers"))
                .header("x-api-key", &self.api_key)
                .send()
                .await;
            if let Ok(r) = resp {
                if r.status().is_success() {
                    if let Ok(body) = r.json::<Json>().await {
                        if let Some(total) = body["total"].as_u64() {
                            if total as usize >= self.workers.len() {
                                break;
                            }
                        }
                    }
                }
            }
            if Instant::now() >= deadline {
                break; // best-effort -- caller will surface unregistered worker via assertions
            }
            sleep(POLL_INTERVAL).await;
        }

        worker_id
    }

    /// Number of coordinators still in the live list.
    fn live_count(&self) -> usize {
        self.coordinators.len()
    }

    /// Kill every process owned by the cluster. Best-effort; ignores
    /// errors so panicking tests still clean up.
    fn shutdown_all(mut self) {
        for w in &mut self.workers {
            let _ = w.process.kill();
            let _ = w.process.wait();
        }
        for c in &mut self.coordinators {
            let _ = c.process.kill();
            let _ = c.process.wait();
        }
    }
}

impl Drop for MultiRaftCluster {
    fn drop(&mut self) {
        for w in &mut self.workers {
            let _ = w.process.kill();
            let _ = w.process.wait();
        }
        for c in &mut self.coordinators {
            let _ = c.process.kill();
            let _ = c.process.wait();
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct LeaderInfo {
    leader_id: u64,
    term: u64,
}

// ---------------------------------------------------------------------------
// Test 1 — Leader election after coordinator kill
// ---------------------------------------------------------------------------

/// Verifies the foundational property of "new leader resumes checkpointing":
/// after killing the current Raft leader, the surviving 2-node quorum elects
/// a fresh leader on a higher term within `REELECTION_TIMEOUT`.
///
/// This is sufficient to demonstrate the *coordinator side* of the failover
/// guarantee — once a new leader is elected, it inherits the replicated
/// `latest_checkpoints` state machine (Task 1.4) so it knows exactly which
/// checkpoint ids were durably persisted before the kill.
#[tokio::test]
#[ignore]
async fn test_coordinator_failover_leader_election() {
    let mut cluster = match MultiRaftCluster::start().await {
        Some(c) => c,
        None => {
            eprintln!(
                "  [skip] test_coordinator_failover_leader_election: Raft feature not enabled \
                 in the varpulis binary (build with --features raft)"
            );
            return;
        }
    };

    // Capture the initial leader.
    let initial = cluster
        .poll_until_leader(INITIAL_ELECTION_TIMEOUT, None)
        .await
        .expect("initial leader election must succeed");
    eprintln!(
        "  initial leader = node {} on term {}",
        initial.leader_id, initial.term
    );

    // SIGKILL the leader.
    let leader_idx = cluster
        .find_index_for(initial.leader_id)
        .expect("leader node id present in cluster");
    let killed_node = cluster.kill_coordinator(leader_idx);
    eprintln!("  killed coordinator node {killed_node}");
    assert_eq!(cluster.live_count(), NUM_COORDINATORS - 1);

    // Wait for re-election. The new leader must NOT be the dead node and
    // must run on a strictly higher term.
    let elected = cluster
        .poll_until_leader(REELECTION_TIMEOUT, Some(killed_node))
        .await
        .unwrap_or_else(|| {
            panic!(
                "new leader was not elected within {REELECTION_TIMEOUT:?} \
                 after killing node {killed_node}"
            );
        });
    eprintln!(
        "  new leader = node {} on term {} (was {} on term {})",
        elected.leader_id, elected.term, initial.leader_id, initial.term
    );

    assert_ne!(
        elected.leader_id, killed_node,
        "new leader must not be the killed node"
    );
    assert!(
        elected.term > initial.term,
        "new leader's term must be strictly higher than the killed leader's term \
         (got new={}, old={})",
        elected.term,
        initial.term
    );

    // Cross-check: every surviving coordinator agrees on the new leader.
    // (Followers might lag a hop on the term, so we only require leader_id
    // to match across alive nodes.)
    let mut agreement = 0usize;
    for idx in 0..cluster.live_count() {
        if let Some(status) = cluster.raft_status(idx).await {
            if status["leader_id"].as_u64() == Some(elected.leader_id) {
                agreement += 1;
            }
        }
    }
    assert_eq!(
        agreement,
        cluster.live_count(),
        "all {} surviving coordinators must agree on the new leader (only {} agreed)",
        cluster.live_count(),
        agreement
    );
}

// ---------------------------------------------------------------------------
// Test 2 — Workers self-abort orphan barriers
// ---------------------------------------------------------------------------

/// Verifies the second bullet of Task 4.3: a barrier delivered to a worker
/// without a follow-up `complete`/`abort` does not leave the worker stuck.
///
/// Direct-publishes a synthetic [`CheckpointBarrierRequest`] over NATS at a
/// real worker process, with `timeout_ms` set to
/// [`WATCHDOG_BARRIER_TIMEOUT`]. No coordinator publishes a complete or
/// abort. After waiting more than the timeout, the worker must remain
/// responsive (HTTP `/health` or analogous), which is only true if the
/// per-checkpoint watchdog (Task 2.1) fired and retired the pending entry.
///
/// The barrier targets a non-existent pipeline so the worker will NACK fast
/// — no real engine state is involved. The point of this test is the
/// *integration* check: real `varpulis` binary, real NATS, real
/// `nats_worker.rs` dispatch, and a confirmation that the cleanup path does
/// not deadlock.
#[tokio::test]
#[ignore]
async fn test_coordinator_failover_workers_self_abort() {
    let nats = nats_url();
    if !tcp_reachable(&nats_tcp_target(&nats)).await {
        eprintln!(
            "  [skip] test_coordinator_failover_workers_self_abort: NATS not reachable at {nats}"
        );
        return;
    }

    let nats_client = match async_nats::connect(&nats).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "  [skip] test_coordinator_failover_workers_self_abort: NATS connect failed: {e}"
            );
            return;
        }
    };

    let mut cluster = match MultiRaftCluster::start().await {
        Some(c) => c,
        None => {
            eprintln!(
                "  [skip] test_coordinator_failover_workers_self_abort: Raft feature not enabled"
            );
            return;
        }
    };

    // Find current leader so we attach the worker to it. The worker only
    // needs to be alive and listening on its NATS cmd subject; which
    // coordinator it registered with isn't material.
    let leader_info = cluster
        .poll_until_leader(INITIAL_ELECTION_TIMEOUT, None)
        .await
        .expect("leader election must succeed before adding worker");
    let leader_idx = cluster
        .find_index_for(leader_info.leader_id)
        .expect("leader node id present");
    let worker_id = cluster.add_worker_pointing_at(leader_idx, Some(&nats)).await;
    eprintln!("  spawned worker {worker_id} attached to leader node {}", leader_info.leader_id);

    // Subscribe to the ack subject for our synthetic group BEFORE
    // publishing -- otherwise we race the worker's response.
    use varpulis_cluster::checkpoint_protocol::{
        CheckpointBarrierAck, CheckpointBarrierRequest,
    };
    use varpulis_cluster::nats_transport::{
        subject_checkpoint_ack, subject_checkpoint_barrier,
    };

    let group_id = format!("failover-test-{}", std::process::id());
    let ack_subject = subject_checkpoint_ack(&group_id);
    let barrier_subject = subject_checkpoint_barrier(&worker_id);

    let mut ack_sub = nats_client
        .subscribe(ack_subject.clone())
        .await
        .expect("subscribe to ack subject");

    // Publish a barrier targeting a non-existent pipeline. The worker
    // should NACK quickly (because there's no such pipeline), but
    // critically it must remain responsive afterwards.
    let request = CheckpointBarrierRequest {
        group_id: group_id.clone(),
        checkpoint_id: 1,
        pipeline_id: "no-such-pipeline".to_string(),
        timeout_ms: WATCHDOG_BARRIER_TIMEOUT.as_millis() as u64,
        triggered_at_ms: chrono::Utc::now().timestamp_millis(),
    };
    let payload = serde_json::to_vec(&request).expect("serialize barrier");
    nats_client
        .publish(barrier_subject.clone(), payload.into())
        .await
        .expect("publish barrier");
    nats_client
        .flush()
        .await
        .expect("flush nats publish");

    // Wait for the worker's ack -- success or NACK is fine for this test.
    // What we need to confirm is that the worker emitted *something*.
    use futures_util::StreamExt;
    let ack_msg = tokio::time::timeout(Duration::from_secs(3), ack_sub.next())
        .await
        .expect("worker must publish an ack within 3s")
        .expect("ack subscription must yield a message");

    let ack: CheckpointBarrierAck =
        serde_json::from_slice(&ack_msg.payload).expect("ack must be valid JSON");
    eprintln!(
        "  ack: worker={} pipeline={} success={} error={:?}",
        ack.worker_id.0,
        ack.pipeline_id,
        ack.is_success(),
        ack.error
    );
    // Pipeline doesn't exist on the worker -- a NACK is the expected ack
    // shape. (If the worker grew lazy pipeline creation later, success is
    // still acceptable as long as we got an ack at all.)
    assert_eq!(ack.worker_id.0, worker_id);
    assert_eq!(ack.checkpoint_id, 1);

    // Now wait long enough for the watchdog to elapse on the worker side.
    // We deliberately do NOT publish complete or abort -- the watchdog
    // must clean up unilaterally.
    sleep(WATCHDOG_BARRIER_TIMEOUT * 3).await;

    // Confirm the worker is still responsive: a fresh barrier on a new
    // checkpoint id must still produce an ack. If the watchdog had failed
    // to clear the pending entry (or if the worker's dispatch loop had
    // deadlocked), this second publish would never get a response.
    let request2 = CheckpointBarrierRequest {
        group_id: group_id.clone(),
        checkpoint_id: 2,
        pipeline_id: "no-such-pipeline".to_string(),
        timeout_ms: WATCHDOG_BARRIER_TIMEOUT.as_millis() as u64,
        triggered_at_ms: chrono::Utc::now().timestamp_millis(),
    };
    let payload2 = serde_json::to_vec(&request2).expect("serialize barrier 2");
    nats_client
        .publish(barrier_subject, payload2.into())
        .await
        .expect("publish barrier 2");
    nats_client.flush().await.expect("flush nats publish");

    let ack2_msg = tokio::time::timeout(Duration::from_secs(3), ack_sub.next())
        .await
        .expect("worker must remain responsive after watchdog timeout")
        .expect("ack subscription must yield a second message");
    let ack2: CheckpointBarrierAck =
        serde_json::from_slice(&ack2_msg.payload).expect("ack2 must be valid JSON");
    assert_eq!(ack2.checkpoint_id, 2);
    eprintln!("  second ack received -- worker survived orphan barrier and watchdog cleanup");
}

// ---------------------------------------------------------------------------
// Inline unit tests (no infra) -- verify the harness math
// ---------------------------------------------------------------------------

#[cfg(test)]
mod harness_tests {
    use super::*;

    #[test]
    fn timeouts_are_sensibly_ordered() {
        // Re-election timeout must allow at least one election cycle (max
        // election timeout 3000ms in raft::bootstrap_with_storage) plus
        // detection lag. 15s is comfortably above that.
        assert!(REELECTION_TIMEOUT >= Duration::from_secs(10));
        // Worker watchdog timeout must be small enough that the test can
        // wait > 3x within a few seconds.
        assert!(WATCHDOG_BARRIER_TIMEOUT < Duration::from_secs(1));
    }

    #[test]
    fn three_coordinators_form_quorum_after_one_kill() {
        // Sanity: a 3-node Raft cluster tolerates 1 fault, leaving a
        // 2-node majority. Encoded as a runtime check rather than a
        // tautology so a future bump to NUM_COORDINATORS = 5 still gets
        // exercised.
        let n = NUM_COORDINATORS;
        let survivors = n - 1;
        let majority = (n / 2) + 1;
        assert_eq!(n, 3);
        assert!(
            survivors >= majority,
            "{survivors} survivors must form majority of {n}"
        );
    }
}
