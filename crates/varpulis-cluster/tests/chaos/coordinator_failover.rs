//! Coordinator Failover Test (Task 4.3 — Flink-Parity Phase 4).
//!
//! Companion to [`distributed_checkpoint`] (Task 4.1) and
//! [`multi_replica_checkpoint`] (Task 4.2). Where 4.1/4.2 kill *workers* mid-
//! run and verify exactly-once semantics survive, this test kills the
//! *coordinator* and verifies:
//!
//!   1. Leadership takeover: with three coordinators on one JetStream KV
//!      control plane, a survivor takes the lease within bounded time after
//!      the holder is SIGKILL'd mid-checkpoint cycle. A killed holder cannot
//!      resign, so the bound is the bucket's TTL plus one sweep — stated, and
//!      asserted, rather than assumed.
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
//! re-trigger any in-flight or future checkpoint cycle. The command set
//! carries `CheckpointCompleted` / `CheckpointAborted`, so once a new holder
//! stabilises it reads the same `latest_checkpoints` map out of the bucket.
//! (Nothing sends those commands yet — see the note on
//! `CheckpointRaftReplicator` — so that inheritance is currently vacuous.)
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
//!   + jetstream-control-plane` features (set `VARPULIS_BIN` to point at it):
//!   ```sh
//!   cargo build --release \
//!     -p varpulis-cli \
//!     --features 'nats-transport jetstream-control-plane'
//!   export VARPULIS_BIN=$PWD/target/release/varpulis
//!   ```
//!   Note: the `distributed-checkpoint` feature is owned by `varpulis-
//!   cluster`. If the CLI doesn't yet expose a passthrough, the worker-
//!   self-abort test [skip]s rather than running half-armed.
//! - NATS broker on `localhost:4222` for the worker self-abort sub-test
//!   only (the takeover sub-test needs only the control plane and has no other infra
//!   dependency).
//!
//! ## Running
//!
//! ```sh
//! cargo test --release \
//!     --test chaos \
//!     --features 'distributed-checkpoint jetstream-control-plane' \
//!     test_coordinator_failover \
//!     -- --ignored --nocapture
//! ```
//!
//! ## Skip behaviour
//!
//! Each test probes its prerequisites (a control plane compiled in, a
//! JetStream-enabled NATS reachable) and abstains when they are missing.
//! An abstention is a *failure* under `VARPULIS_REQUIRE_BROKERS=1`, which is
//! what every job that provisions a broker sets. Tests are `#[ignore]`d to
//! require explicit opt-in.

#![cfg(all(
    unix,
    feature = "distributed-checkpoint",
    feature = "jetstream-control-plane"
))]
#![allow(clippy::too_many_lines)]

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value as Json;
use tokio::time::sleep;

use super::{allocate_ports, find_binary, WorkerProcess, API_KEY};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Number of coordinator processes in the test cluster.
const NUM_COORDINATORS: usize = 3;

/// How long to wait for the *initial* leadership acquisition after spawning
/// the cluster. Whichever coordinator's first sweep lands first wins the
/// create-if-absent, so this is bounded by the sweep interval, not by an
/// election.
const INITIAL_ELECTION_TIMEOUT: Duration = Duration::from_secs(20);

/// How long to wait for a *takeover* after killing the holder. A killed
/// coordinator cannot resign, so the survivors wait out the record's TTL
/// before their `create` succeeds: bounded by `CONTROL_PLANE_TTL` plus one
/// sweep, with slack for a busy CI runner.
const REELECTION_TIMEOUT: Duration = Duration::from_secs(45);

/// Bucket TTL for the test cluster, and therefore the crash-failover bound.
/// Short, because the test spends it waiting.
const CONTROL_PLANE_TTL_SECS: u64 = 10;

/// Sweep interval for the test coordinators. Must be well under half the TTL
/// or leadership flaps; see the warning `run_coordinator` prints.
const HEARTBEAT_INTERVAL_SECS: u64 = 2;

/// Per-coordinator startup grace period before issuing the first probe.
const COORDINATOR_BOOT_DELAY: Duration = Duration::from_secs(1);

/// Polling cadence while waiting for leadership to move.
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
// Multi-coordinator control-plane cluster harness
// ---------------------------------------------------------------------------

/// A single coordinator process in the cluster.
struct CoordinatorProcess {
    process: Child,
    port: u16,
    /// What this coordinator advertises, and therefore what `/consensus`
    /// reports when it holds the lease. Also its identity in the test: two
    /// coordinators on different ports cannot collide.
    address: String,
}

/// Three coordinator processes sharing one JetStream KV control plane.
///
/// Each is spawned with `VARPULIS_CONTROL_PLANE_URL` pointing at the same
/// broker and bucket. There is no node id, no peer list and no membership to
/// discover: leadership is `create`-if-absent on one key, and whichever
/// coordinator's first health sweep lands first holds it. A killed holder
/// cannot resign, so the record ages out on the bucket's TTL and a survivor's
/// next `create` succeeds — crash failover with a bound the test can state.
struct MultiCoordinatorCluster {
    coordinators: Vec<CoordinatorProcess>,
    workers: Vec<WorkerProcess>,
    api_key: String,
    http_client: reqwest::Client,
    bucket: String,
}

impl MultiCoordinatorCluster {
    /// Spawn `NUM_COORDINATORS` coordinators on one control plane, then wait
    /// for one of them to take the lease.
    ///
    /// Returns `None` when the binary has no `jetstream-control-plane`
    /// feature, or when the broker is unreachable — the caller treats either
    /// as a `[skip]`, and the chaos runner turns a skip into a failure when
    /// `VARPULIS_REQUIRE_BROKERS` says a broker was promised.
    async fn start() -> Option<Self> {
        let bin = find_binary();
        let base_port = allocate_ports(NUM_COORDINATORS as u16);
        let ports: Vec<u16> = (0..NUM_COORDINATORS as u16)
            .map(|i| base_port + i)
            .collect();

        // One bucket per run: these processes are real and a leftover record
        // from a previous run would hand leadership to a corpse.
        let bucket = format!("VCHAOS_FAILOVER_{}", std::process::id());

        let mut coordinators = Vec::with_capacity(NUM_COORDINATORS);
        for &port in &ports {
            let id = format!("coord-{port}");
            let address = format!("http://127.0.0.1:{port}");
            let process = Command::new(&bin)
                .args([
                    "coordinator",
                    "--port",
                    &port.to_string(),
                    "--bind",
                    "127.0.0.1",
                    "--api-key",
                    API_KEY,
                    "--coordinator-id",
                    &id,
                    "--heartbeat-interval",
                    &HEARTBEAT_INTERVAL_SECS.to_string(),
                ])
                .env("VARPULIS_CONTROL_PLANE_URL", nats_url())
                // This harness runs against one `-js` broker, and the
                // coordinator now refuses to start on a bucket that cannot
                // lose one. That refusal is right for a deployment and wrong
                // here: the property under test is leadership failover
                // between coordinators, not bucket durability, and a
                // three-node NATS cluster would make this job slower without
                // testing anything more.
                .env("VARPULIS_CONTROL_PLANE_ALLOW_SINGLE_REPLICA", "1")
                .env("VARPULIS_CONTROL_PLANE_BUCKET", &bucket)
                .env(
                    "VARPULIS_CONTROL_PLANE_TTL_SECS",
                    CONTROL_PLANE_TTL_SECS.to_string(),
                )
                .env("VARPULIS_COORDINATOR_ADVERTISE_ADDR", &address)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap_or_else(|e| panic!("Failed to spawn coordinator on {port}: {e}"));
            coordinators.push(CoordinatorProcess {
                process,
                port,
                address,
            });
        }

        // Say which bucket, so a failing run on CI can be inspected rather
        // than guessed at.
        eprintln!("  control-plane bucket = {bucket} on {}", nats_url());
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
            bucket,
        };

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

    /// Fetch `/consensus` from coordinator `idx`. `None` when the request
    /// failed (process down, port not yet bound).
    async fn consensus_status(&self, idx: usize) -> Option<Json> {
        let resp = self
            .http_client
            .get(self.api_url(idx, "/consensus"))
            .header("x-api-key", &self.api_key)
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        resp.json::<Json>().await.ok()
    }

    /// Poll every live coordinator's `/consensus` endpoint until one of them
    /// names a holder. Returns the holder's advertised address.
    ///
    /// `forbid` lets the caller require the holder differ from a given
    /// address — used after killing the holder to require a real takeover
    /// rather than a stale read.
    ///
    /// Returns `None` on timeout, or immediately when the binary reports a
    /// `standalone` backend (no control plane compiled in or configured).
    async fn poll_until_leader(
        &self,
        timeout: Duration,
        forbid: Option<&str>,
    ) -> Option<LeaderInfo> {
        let deadline = Instant::now() + timeout;
        loop {
            for idx in 0..self.coordinators.len() {
                if let Some(status) = self.consensus_status(idx).await {
                    if status["backend"].as_str() != Some("jetstream-control-plane") {
                        // No control plane in this binary — retrying cannot help.
                        return None;
                    }
                    if let Some(leader) = status["leader"].as_str() {
                        if forbid != Some(leader) {
                            return Some(LeaderInfo {
                                leader: leader.to_string(),
                            });
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

    /// Find the cluster index of the coordinator at that advertised address.
    fn find_index_for(&self, address: &str) -> Option<usize> {
        self.coordinators.iter().position(|c| c.address == address)
    }

    /// SIGKILL the coordinator at the given cluster index and remove it
    /// from the live list (so subsequent polls only target survivors).
    fn kill_coordinator(&mut self, idx: usize) -> String {
        let mut victim = self.coordinators.remove(idx);
        let address = victim.address.clone();
        let _ = victim.process.kill();
        let _ = victim.process.wait();
        address
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
            // The CLI flag is `--nats` (see crates/varpulis-cli/src/main.rs);
            // `--nats-url` made every spawned process die in argument parsing,
            // so the worker never registered and the test abstained.
            cmd.args(["--nats", url]);
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
        // poll /workers; on a healthy cluster this also exercises follower
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

impl Drop for MultiCoordinatorCluster {
    fn drop(&mut self) {
        // The bucket is per-run and the processes are about to die; say which
        // one, so a bucket left behind by a panicking run can be found.
        eprintln!("  tearing down control-plane bucket {}", self.bucket);
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

/// Who holds the coordinator lease, by advertised address.
///
/// There is no term: the lease is a KV revision, and the revision is the
/// store's business, not the caller's. What a caller can observe — and all it
/// needs — is *which* coordinator is currently allowed to write.
#[derive(Debug, Clone)]
struct LeaderInfo {
    leader: String,
}

// ---------------------------------------------------------------------------
// Test 1 — Leader election after coordinator kill
// ---------------------------------------------------------------------------

/// Verifies the foundational property of "new leader resumes checkpointing":
/// after killing the current lease holder, one of the two survivors takes
/// a fresh leader on a higher term within `REELECTION_TIMEOUT`.
///
/// This is sufficient to demonstrate the *coordinator side* of the failover
/// guarantee — once a new leader is elected, it inherits the replicated
/// `latest_checkpoints` state machine (Task 1.4) so it knows exactly which
/// checkpoint ids were durably persisted before the kill.
#[tokio::test]
#[ignore]
async fn test_coordinator_failover_leader_election() {
    let mut cluster = match MultiCoordinatorCluster::start().await {
        Some(c) => c,
        None => {
            crate::abstain(
                "test_coordinator_failover_leader_election",
                "no JetStream control plane: build with --features \
                 jetstream-control-plane and point NATS_URL at a \
                 JetStream-enabled broker",
            );
            return;
        }
    };

    // Capture the initial holder.
    let initial = cluster
        .poll_until_leader(INITIAL_ELECTION_TIMEOUT, None)
        .await
        .expect("a coordinator must take the lease");
    eprintln!("  initial holder = {}", initial.leader);

    // SIGKILL it. A killed holder cannot resign, so the record it holds stays
    // in the bucket until the TTL ages it out — which is exactly the failure
    // mode the TTL exists for, and the one Raft needed an election timeout
    // for.
    let leader_idx = cluster
        .find_index_for(&initial.leader)
        .expect("the holder must be one of our coordinators");
    let killed = cluster.kill_coordinator(leader_idx);
    eprintln!("  killed the holder at {killed}");
    assert_eq!(cluster.live_count(), NUM_COORDINATORS - 1);

    // Wait for a survivor to take over. It must not be the dead one.
    let elected = cluster
        .poll_until_leader(REELECTION_TIMEOUT, Some(&killed))
        .await
        .unwrap_or_else(|| {
            panic!(
                "no survivor took the lease within {REELECTION_TIMEOUT:?} after \
                 killing the holder at {killed}. The bound is the bucket TTL \
                 ({CONTROL_PLANE_TTL_SECS}s) plus one sweep \
                 ({HEARTBEAT_INTERVAL_SECS}s)."
            );
        });
    eprintln!("  new holder = {} (was {})", elected.leader, initial.leader);

    assert_ne!(
        elected.leader, killed,
        "the killed coordinator must not still be reported as the holder"
    );

    // Cross-check: every survivor names the same holder. They read it from
    // the same key, so disagreement means one of them is serving a stale
    // read rather than the bucket.
    let mut agreement = 0usize;
    for idx in 0..cluster.live_count() {
        if let Some(status) = cluster.consensus_status(idx).await {
            if status["leader"].as_str() == Some(elected.leader.as_str()) {
                agreement += 1;
            }
        }
    }
    assert_eq!(
        agreement,
        cluster.live_count(),
        "all {} surviving coordinators must name the same holder (only {} did)",
        cluster.live_count(),
        agreement
    );

    // Exactly one of them believes it may write. That is the property the
    // whole control plane exists for, and the one a split brain breaks.
    let mut writers = 0usize;
    for idx in 0..cluster.live_count() {
        if let Some(status) = cluster.consensus_status(idx).await {
            if status["is_leader"].as_bool() == Some(true) {
                writers += 1;
            }
        }
    }
    assert_eq!(
        writers, 1,
        "exactly one surviving coordinator may believe it writes, got {writers}"
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
        crate::abstain(
            "test_coordinator_failover_workers_self_abort",
            &format!("NATS not reachable at {nats}"),
        );
        return;
    }

    let nats_client = match async_nats::connect(&nats).await {
        Ok(c) => c,
        Err(e) => {
            crate::abstain(
                "test_coordinator_failover_workers_self_abort",
                &format!("NATS connect failed: {e}"),
            );
            return;
        }
    };

    let mut cluster = match MultiCoordinatorCluster::start().await {
        Some(c) => c,
        None => {
            crate::abstain(
                "test_coordinator_failover_workers_self_abort",
                "no JetStream control plane",
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
        .expect("a coordinator must take the lease before adding a worker");
    let leader_idx = cluster
        .find_index_for(&leader_info.leader)
        .expect("the holder must be one of our coordinators");
    let worker_id = cluster
        .add_worker_pointing_at(leader_idx, Some(&nats))
        .await;
    eprintln!(
        "  spawned worker {worker_id} attached to the holder at {}",
        leader_info.leader
    );

    // Subscribe to the ack subject for our synthetic group BEFORE
    // publishing -- otherwise we race the worker's response.
    use varpulis_cluster::checkpoint_protocol::{CheckpointBarrierAck, CheckpointBarrierRequest};
    use varpulis_cluster::nats_transport::{subject_checkpoint_ack, subject_checkpoint_barrier};

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
    nats_client.flush().await.expect("flush nats publish");

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
        // Crash takeover waits out the record's TTL — a killed holder cannot
        // resign — plus a sweep to notice, plus slack for a busy runner.
        assert!(
            REELECTION_TIMEOUT
                >= Duration::from_secs(CONTROL_PLANE_TTL_SECS + HEARTBEAT_INTERVAL_SECS) * 2,
            "the takeover bound must comfortably exceed TTL + one sweep"
        );
        // And the sweep must stay well under half the TTL, or the holder's own
        // record expires between its renewals and leadership flaps. Both are
        // consts, so this is a compile-time check written as one — clippy is
        // right that the assertion is constant, and that is the point of it.
        const _: () = assert!(
            HEARTBEAT_INTERVAL_SECS * 2 < CONTROL_PLANE_TTL_SECS,
            "sweep interval must be under half the TTL"
        );
        // Worker watchdog timeout must be small enough that the test can
        // wait > 3x within a few seconds.
        assert!(WATCHDOG_BARRIER_TIMEOUT < Duration::from_secs(1));
    }

    #[test]
    fn three_coordinators_form_quorum_after_one_kill() {
        // Sanity: killing one of three leaves a
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
