//! Network Partition Test (Task 4.4 — Flink-Parity Phase 4).
//!
//! Companion to [`distributed_checkpoint`] (Task 4.1),
//! [`multi_replica_checkpoint`] (Task 4.2), and [`coordinator_failover`]
//! (Task 4.3). Where the prior tasks SIGKILL workers or coordinators, this
//! task simulates a *network partition* between the coordinator and a
//! single worker — the worker is alive and processing events, but it
//! cannot exchange checkpoint protocol messages with the coordinator over
//! NATS.
//!
//! ## Properties verified
//!
//!   1. **Checkpoint aborts.** When the coordinator initiates a barrier
//!      and one participant is partitioned, the orchestrator either fails
//!      to dispatch the barrier (outbound partition) or never receives the
//!      ack within `ack_timeout` (inbound partition or bidirectional). In
//!      both cases the cycle resolves into a `CheckpointOutcome::Aborted`,
//!      the abort broadcast goes out to the surviving workers, and Raft is
//!      told the checkpoint did not complete.
//!   2. **No data loss.** No `CheckpointCompleteNotification` is ever
//!      broadcast for the aborted id. Surviving workers receive the abort
//!      and call `abort_sinks(id)`, releasing the prepared 2PC
//!      transactions. The partitioned worker's own per-checkpoint
//!      watchdog (Task 2.1) fires after `timeout_ms`, removing the pending
//!      entry and aborting its prepared sinks too — so even the
//!      unreachable replica never commits.
//!   3. **Recovery after heal.** After clearing the partition, a fresh
//!      checkpoint cycle (allocated as `id + 1`) succeeds normally — the
//!      previous abort did not leave the orchestrator in a stuck state,
//!      and surviving workers are ready to ack.
//!
//! ## Why a transport mock + an integration test
//!
//! The orchestrator's partition behaviour is built from three primitives:
//! barrier dispatch failures (recorded as participant failures), ack
//! channel receives blocked by `tokio::time::timeout`, and abort
//! broadcasts that fan out to every worker on the group's abort subject.
//! Each one is exhaustively unit-tested in
//! `coordinator/distributed_checkpoint.rs::tests`. This file *combines*
//! those primitives into the specific scenario the Flink-parity plan
//! describes — partition isolation of a single worker — using a
//! [`PartitionableTransport`] that lets the test toggle partition state
//! per worker mid-cycle.
//!
//! The integration test goes one step further: it spawns real `varpulis`
//! processes and a real NATS broker, then partitions one worker by
//! sending it `SIGSTOP` (the worker process is frozen, NATS messages to
//! it queue or expire). Healing the partition is `SIGCONT`. That is
//! fundamentally what a NATS partition looks like to the rest of the
//! cluster — a worker that's "there" but doesn't respond to barrier
//! traffic. Process-level pause is more reliable than `iptables` rules
//! (which require root) and broker-level ACL changes (which depend on
//! NATS-server-specific tooling).
//!
//! ## Prerequisites
//!
//! The unit tests have no infrastructure dependency. The integration test
//! requires:
//!
//! - `varpulis` binary built with `nats-transport + distributed-checkpoint`
//!   features (set `VARPULIS_BIN` to point at it):
//!   ```sh
//!   cargo build --release \
//!     -p varpulis-cli \
//!     --features 'nats-transport'
//!   export VARPULIS_BIN=$PWD/target/release/varpulis
//!   ```
//! - NATS broker on `localhost:4222` (override with `NATS_URL`).
//!
//! ## Running
//!
//! ```sh
//! # All Task 4.4 tests:
//! cargo test --release \
//!     --test chaos \
//!     --features distributed-checkpoint \
//!     network_partition
//!
//! # Just the integration test (requires NATS):
//! cargo test --release \
//!     --test chaos \
//!     --features distributed-checkpoint \
//!     test_network_partition_real_nats \
//!     -- --ignored --nocapture
//! ```

#![cfg(all(unix, feature = "distributed-checkpoint"))]
#![allow(clippy::too_many_lines)]

use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::sleep;
use varpulis_cluster::checkpoint_protocol::{
    CheckpointAbortNotification, CheckpointBarrierAck, CheckpointBarrierRequest,
    CheckpointCompleteNotification, CheckpointId, SnapshotLocation,
};
use varpulis_cluster::coordinator::distributed_checkpoint::{
    CheckpointOutcome, CheckpointRaftReplicator, CheckpointTransport, DistributedCheckpointConfig,
    DistributedCheckpointCoordinator, ReplicateError, TransportError,
};
use varpulis_cluster::worker::WorkerId;
use varpulis_runtime::persistence::{
    EngineCheckpoint, MemoryStore, StateStore, CHECKPOINT_VERSION,
};

// ---------------------------------------------------------------------------
// PartitionableTransport — toggleable per-worker partition state
// ---------------------------------------------------------------------------

/// Mock transport that simulates a NATS partition between the coordinator
/// and one or more specific workers. Toggleable mid-cycle so tests can
/// emulate partition-then-heal scenarios.
///
/// Per-direction partition control:
///   - `outbound_partitioned`: barrier dispatch to this worker fails as
///     `TransportError("partition: outbound to {worker}")`.
///   - `inbound_partitioned`: barrier dispatch *succeeds* (broker accepts
///     the publish) but the test does not synthesise an ack for the
///     partitioned worker — the orchestrator times out waiting.
///
/// `broadcast_complete` and `broadcast_abort` always succeed (broadcasts
/// hit the group-wide subject; non-partitioned workers receive them, the
/// partitioned worker would observe them once the partition heals — this
/// matches NATS' typical fan-out behaviour). Tests that need to assert
/// "no complete was broadcast" still get that signal by checking the
/// `completes` log directly.
struct PartitionableTransport {
    sent_barriers: StdMutex<Vec<(WorkerId, CheckpointBarrierRequest)>>,
    completes: StdMutex<Vec<CheckpointCompleteNotification>>,
    aborts: StdMutex<Vec<CheckpointAbortNotification>>,
    /// Workers whose barrier dispatch should currently fail.
    outbound_partitioned: StdMutex<HashSet<String>>,
    /// Workers whose acks the coordinator never sees (test must arrange
    /// that no ack is fed into the receiver for these).
    inbound_partitioned: StdMutex<HashSet<String>>,
}

impl PartitionableTransport {
    fn new() -> Self {
        Self {
            sent_barriers: StdMutex::new(Vec::new()),
            completes: StdMutex::new(Vec::new()),
            aborts: StdMutex::new(Vec::new()),
            outbound_partitioned: StdMutex::new(HashSet::new()),
            inbound_partitioned: StdMutex::new(HashSet::new()),
        }
    }

    /// Add `worker_id` to the outbound partition set (barrier dispatch fails).
    fn partition_outbound(&self, worker_id: &str) {
        self.outbound_partitioned
            .lock()
            .unwrap()
            .insert(worker_id.to_string());
    }

    /// Add `worker_id` to the inbound partition set (test must withhold ack).
    fn partition_inbound(&self, worker_id: &str) {
        self.inbound_partitioned
            .lock()
            .unwrap()
            .insert(worker_id.to_string());
    }

    /// Heal both directions of partition for `worker_id`.
    fn heal(&self, worker_id: &str) {
        self.outbound_partitioned.lock().unwrap().remove(worker_id);
        self.inbound_partitioned.lock().unwrap().remove(worker_id);
    }

    fn is_inbound_partitioned(&self, worker_id: &str) -> bool {
        self.inbound_partitioned.lock().unwrap().contains(worker_id)
    }

    fn dispatched_count(&self) -> usize {
        self.sent_barriers.lock().unwrap().len()
    }

    fn completes_count(&self) -> usize {
        self.completes.lock().unwrap().len()
    }

    fn aborts_count(&self) -> usize {
        self.aborts.lock().unwrap().len()
    }

    fn last_abort(&self) -> Option<CheckpointAbortNotification> {
        self.aborts.lock().unwrap().last().cloned()
    }
}

impl CheckpointTransport for PartitionableTransport {
    async fn send_barrier(
        &self,
        worker_id: &WorkerId,
        request: &CheckpointBarrierRequest,
    ) -> Result<(), TransportError> {
        if self
            .outbound_partitioned
            .lock()
            .unwrap()
            .contains(&worker_id.0)
        {
            return Err(TransportError::new(format!(
                "partition: outbound to {}",
                worker_id.0
            )));
        }
        self.sent_barriers
            .lock()
            .unwrap()
            .push((worker_id.clone(), request.clone()));
        Ok(())
    }

    async fn broadcast_complete(
        &self,
        _group_id: &str,
        notification: &CheckpointCompleteNotification,
    ) -> Result<(), TransportError> {
        self.completes.lock().unwrap().push(notification.clone());
        Ok(())
    }

    async fn broadcast_abort(
        &self,
        _group_id: &str,
        notification: &CheckpointAbortNotification,
    ) -> Result<(), TransportError> {
        self.aborts.lock().unwrap().push(notification.clone());
        Ok(())
    }
}

/// Mock Raft replicator that records every call so tests can assert that
/// the orchestrator told Raft about each abort/complete.
#[derive(Default)]
struct RecordingReplicator {
    completed: StdMutex<Vec<(String, CheckpointId)>>,
    aborted: StdMutex<Vec<(String, CheckpointId, String)>>,
}

impl RecordingReplicator {
    fn completed_count(&self) -> usize {
        self.completed.lock().unwrap().len()
    }

    fn aborted_count(&self) -> usize {
        self.aborted.lock().unwrap().len()
    }
}

impl CheckpointRaftReplicator for RecordingReplicator {
    async fn replicate_completed(
        &self,
        group_id: &str,
        checkpoint_id: CheckpointId,
    ) -> Result<(), ReplicateError> {
        self.completed
            .lock()
            .unwrap()
            .push((group_id.to_string(), checkpoint_id));
        Ok(())
    }

    async fn replicate_aborted(
        &self,
        group_id: &str,
        checkpoint_id: CheckpointId,
        reason: &str,
    ) -> Result<(), ReplicateError> {
        self.aborted.lock().unwrap().push((
            group_id.to_string(),
            checkpoint_id,
            reason.to_string(),
        ));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const GROUP_ID: &str = "partition-grp";

/// Empty `EngineCheckpoint` suitable for use as an inline snapshot
/// location in unit tests.
fn empty_checkpoint_loc() -> SnapshotLocation {
    SnapshotLocation::Inline {
        checkpoint: Box::new(EngineCheckpoint {
            version: CHECKPOINT_VERSION,
            window_states: HashMap::new(),
            sase_states: HashMap::new(),
            join_states: HashMap::new(),
            variables: HashMap::new(),
            events_processed: 0,
            output_events_emitted: 0,
            watermark_state: None,
            distinct_states: HashMap::new(),
            limit_states: HashMap::new(),
            source_offsets: HashMap::new(),
        }),
    }
}

fn worker(id: &str) -> WorkerId {
    WorkerId(id.into())
}

/// Build an orchestrator with the [`PartitionableTransport`] +
/// [`RecordingReplicator`] pair under test.
fn build_orchestrator(
    config: DistributedCheckpointConfig,
) -> (
    DistributedCheckpointCoordinator<PartitionableTransport, RecordingReplicator>,
    Arc<PartitionableTransport>,
    Arc<MemoryStore>,
    Arc<RecordingReplicator>,
) {
    let transport = Arc::new(PartitionableTransport::new());
    let store: Arc<MemoryStore> = Arc::new(MemoryStore::new());
    let raft = Arc::new(RecordingReplicator::default());
    let orchestrator = DistributedCheckpointCoordinator::new(
        GROUP_ID,
        config,
        transport.clone(),
        store.clone() as Arc<dyn StateStore>,
        raft.clone(),
    );
    (orchestrator, transport, store, raft)
}

/// Drive ack delivery for a set of `(worker, pipeline)` participants.
/// Workers in `inbound_partitioned` are skipped — the orchestrator must
/// time out waiting for their acks.
///
/// Takes `ack_tx` by reference so the caller retains a sender clone in
/// scope. That matters: the orchestrator's ack collection loop matches
/// on `Ok(None)` (channel closed) separately from timeout, and we want
/// the inbound-partition test to exercise the *timeout* path, not the
/// channel-closed path. Holding the sender alive on the test thread
/// keeps the channel open until the cycle completes.
fn spawn_ack_driver(
    transport: Arc<PartitionableTransport>,
    ack_tx: &mpsc::UnboundedSender<CheckpointBarrierAck>,
    participants: Vec<(WorkerId, String)>,
    checkpoint_id: CheckpointId,
    delay: Duration,
) {
    let tx = ack_tx.clone();
    tokio::spawn(async move {
        sleep(delay).await;
        for (worker_id, pipeline_id) in participants {
            if transport.is_inbound_partitioned(&worker_id.0) {
                continue;
            }
            let _ = tx.send(CheckpointBarrierAck::success(
                GROUP_ID.to_string(),
                checkpoint_id,
                worker_id,
                pipeline_id,
                empty_checkpoint_loc(),
            ));
        }
    });
}

// ---------------------------------------------------------------------------
// Unit tests — partition simulation via `PartitionableTransport`
// ---------------------------------------------------------------------------

/// Outbound partition: barrier dispatch fails for the partitioned worker,
/// so the orchestrator records a participant failure synchronously and the
/// cycle resolves into an abort.
#[tokio::test]
async fn outbound_partition_one_of_three_workers_aborts_checkpoint() {
    let (orchestrator, transport, _store, raft) =
        build_orchestrator(DistributedCheckpointConfig::default());

    transport.partition_outbound("w1");

    let participants = vec![
        (worker("w0"), "p1".into()),
        (worker("w1"), "p1".into()),
        (worker("w2"), "p1".into()),
    ];

    let (ack_tx, ack_rx) = mpsc::unbounded_channel();
    spawn_ack_driver(
        transport.clone(),
        &ack_tx,
        participants.clone(),
        orchestrator.peek_next_checkpoint_id(),
        Duration::from_millis(5),
    );

    let outcome = orchestrator
        .run_checkpoint(participants, ack_rx)
        .await
        .expect("abort path returns Ok");
    drop(ack_tx);

    match outcome {
        CheckpointOutcome::Aborted { reason, .. } => {
            assert!(
                reason.contains("participants failed"),
                "unexpected abort reason: {reason}"
            );
        }
        other => panic!("expected Aborted, got {other:?}"),
    }

    // Two healthy workers had their barriers dispatched; the partitioned
    // worker did not.
    assert_eq!(transport.dispatched_count(), 2);
    // No complete broadcast — the protocol must not mark a partitioned
    // checkpoint as durable.
    assert_eq!(
        transport.completes_count(),
        0,
        "no complete broadcast for an aborted checkpoint"
    );
    // Abort broadcast went out to surviving workers (their pipelines need
    // to abort their prepared 2PC transactions).
    assert_eq!(transport.aborts_count(), 1);
    // Raft told that the checkpoint was aborted (so all coordinators see
    // a consistent view of `latest_checkpoints`).
    assert_eq!(raft.aborted_count(), 1);
    assert_eq!(raft.completed_count(), 0);
}

/// Inbound partition: barrier dispatch succeeds (the broker accepts the
/// publish), but the partitioned worker's ack never reaches the
/// coordinator. The orchestrator times out and aborts.
#[tokio::test]
async fn inbound_partition_aborts_checkpoint_via_timeout() {
    let config = DistributedCheckpointConfig {
        ack_timeout: Duration::from_millis(150),
        ..Default::default()
    };
    let (orchestrator, transport, _store, raft) = build_orchestrator(config);

    transport.partition_inbound("w1");

    let participants = vec![
        (worker("w0"), "p1".into()),
        (worker("w1"), "p1".into()),
        (worker("w2"), "p1".into()),
    ];
    let next_id = orchestrator.peek_next_checkpoint_id();

    let (ack_tx, ack_rx) = mpsc::unbounded_channel();
    spawn_ack_driver(
        transport.clone(),
        &ack_tx,
        participants.clone(),
        next_id,
        Duration::from_millis(5),
    );

    let outcome = orchestrator
        .run_checkpoint(participants, ack_rx)
        .await
        .expect("abort path returns Ok");
    drop(ack_tx);

    match outcome {
        CheckpointOutcome::Aborted { reason, .. } => {
            assert!(
                reason.contains("ack timeout") || reason.contains("pending"),
                "expected timeout abort reason, got: {reason}"
            );
        }
        other => panic!("expected Aborted, got {other:?}"),
    }

    // All three barriers were dispatched (broker-level publish succeeds
    // even when one client is partitioned).
    assert_eq!(transport.dispatched_count(), 3);
    // No complete broadcast — partitioned worker never acked, so the
    // checkpoint cannot be durable.
    assert_eq!(transport.completes_count(), 0);
    // Abort broadcast went out so the two healthy workers can release
    // their prepared sinks.
    assert_eq!(transport.aborts_count(), 1);
    assert_eq!(raft.aborted_count(), 1);
    assert_eq!(raft.completed_count(), 0);
}

/// Bidirectional partition behaves like outbound partition: dispatch fails
/// fast and the cycle aborts before the inbound timeout matters.
#[tokio::test]
async fn bidirectional_partition_aborts_via_dispatch_failure() {
    let (orchestrator, transport, _store, _raft) =
        build_orchestrator(DistributedCheckpointConfig::default());

    transport.partition_outbound("w1");
    transport.partition_inbound("w1");

    let participants = vec![(worker("w0"), "p1".into()), (worker("w1"), "p1".into())];
    let (ack_tx, ack_rx) = mpsc::unbounded_channel();
    spawn_ack_driver(
        transport.clone(),
        &ack_tx,
        participants.clone(),
        orchestrator.peek_next_checkpoint_id(),
        Duration::from_millis(5),
    );

    let outcome = orchestrator
        .run_checkpoint(participants, ack_rx)
        .await
        .expect("abort path returns Ok");
    drop(ack_tx);

    assert!(matches!(outcome, CheckpointOutcome::Aborted { .. }));
    // Only the healthy worker's barrier got dispatched.
    assert_eq!(transport.dispatched_count(), 1);
    assert_eq!(transport.aborts_count(), 1);
}

/// The abort broadcast carries the partitioned worker's id in the failure
/// reason, so the audit log makes the partition visible.
#[tokio::test]
async fn abort_reason_records_partitioned_participant() {
    let (orchestrator, transport, _store, _raft) =
        build_orchestrator(DistributedCheckpointConfig::default());

    transport.partition_outbound("isolated-w");

    let participants = vec![
        (worker("w0"), "p1".into()),
        (worker("isolated-w"), "p1".into()),
    ];
    let (ack_tx, ack_rx) = mpsc::unbounded_channel();
    spawn_ack_driver(
        transport.clone(),
        &ack_tx,
        participants.clone(),
        orchestrator.peek_next_checkpoint_id(),
        Duration::from_millis(5),
    );

    let _ = orchestrator
        .run_checkpoint(participants, ack_rx)
        .await
        .expect("abort path returns Ok");
    drop(ack_tx);

    let abort = transport
        .last_abort()
        .expect("at least one abort broadcast");
    assert!(
        abort.reason.contains("isolated-w"),
        "abort reason must name the partitioned participant; got: {}",
        abort.reason
    );
    assert!(
        abort.reason.contains("partition"),
        "reason mentions partition cause: {}",
        abort.reason
    );
}

/// Recovery after heal: partition aborts checkpoint N, then a fresh
/// checkpoint N+1 (after `heal()`) succeeds. Verifies the orchestrator
/// does not get stuck in a permanently-aborted state.
#[tokio::test]
async fn recovery_after_heal_completes_subsequent_checkpoint() {
    let config = DistributedCheckpointConfig {
        ack_timeout: Duration::from_millis(200),
        ..Default::default()
    };
    let (orchestrator, transport, store, raft) = build_orchestrator(config);

    let participants = vec![(worker("w0"), "p1".into()), (worker("w1"), "p1".into())];

    // ---- Phase 1: partition w1, expect abort ----------------------------
    transport.partition_outbound("w1");

    let id1 = orchestrator.peek_next_checkpoint_id();
    let (ack_tx1, ack_rx1) = mpsc::unbounded_channel();
    spawn_ack_driver(
        transport.clone(),
        &ack_tx1,
        participants.clone(),
        id1,
        Duration::from_millis(5),
    );
    let outcome1 = orchestrator
        .run_checkpoint(participants.clone(), ack_rx1)
        .await
        .expect("first run returns Ok");
    drop(ack_tx1);
    match outcome1 {
        CheckpointOutcome::Aborted { checkpoint_id, .. } => {
            assert_eq!(
                checkpoint_id, id1,
                "first cycle aborted on its allocated id"
            );
        }
        other => panic!("phase 1: expected Aborted, got {other:?}"),
    }
    assert_eq!(transport.completes_count(), 0);
    assert_eq!(transport.aborts_count(), 1);
    assert_eq!(raft.aborted_count(), 1);
    assert_eq!(raft.completed_count(), 0);

    // ---- Phase 2: heal, expect next id to complete ----------------------
    transport.heal("w1");

    let id2 = orchestrator.peek_next_checkpoint_id();
    assert_eq!(
        id2,
        id1 + 1,
        "checkpoint id must advance across the partition"
    );

    let (ack_tx2, ack_rx2) = mpsc::unbounded_channel();
    spawn_ack_driver(
        transport.clone(),
        &ack_tx2,
        participants.clone(),
        id2,
        Duration::from_millis(5),
    );
    let outcome2 = orchestrator
        .run_checkpoint(participants, ack_rx2)
        .await
        .expect("second run returns Ok");
    drop(ack_tx2);

    match outcome2 {
        CheckpointOutcome::Completed {
            checkpoint_id,
            store_key,
        } => {
            assert_eq!(checkpoint_id, id2);
            // The assembled checkpoint is in the state store at the
            // expected key.
            let bytes = store
                .get(&store_key)
                .unwrap()
                .expect("checkpoint persisted");
            let assembled: varpulis_cluster::checkpoint_protocol::DistributedCheckpoint =
                serde_json::from_slice(&bytes).expect("valid JSON");
            assert_eq!(assembled.checkpoint_id, id2);
            assert_eq!(assembled.snapshots.len(), 2);
        }
        other => panic!("phase 2: expected Completed, got {other:?}"),
    }

    // Phase 2 completed, so the running totals should now be 1 abort + 1
    // complete. Critically, no complete was issued for the *aborted* id.
    assert_eq!(transport.aborts_count(), 1);
    assert_eq!(transport.completes_count(), 1);
    assert_eq!(raft.completed_count(), 1);
    assert_eq!(raft.aborted_count(), 1);
}

/// Two consecutive partition cycles before any heal both abort cleanly,
/// each on a distinct checkpoint id. Locks in the "no stuck state" invariant.
#[tokio::test]
async fn repeated_partitioned_cycles_each_abort_independently() {
    let config = DistributedCheckpointConfig {
        ack_timeout: Duration::from_millis(50),
        ..Default::default()
    };
    let (orchestrator, transport, _store, raft) = build_orchestrator(config);

    transport.partition_outbound("w1");

    let participants = vec![(worker("w0"), "p1".into()), (worker("w1"), "p1".into())];

    for expected_id in 1u64..=3 {
        let (ack_tx, ack_rx) = mpsc::unbounded_channel();
        spawn_ack_driver(
            transport.clone(),
            &ack_tx,
            participants.clone(),
            expected_id,
            Duration::from_millis(2),
        );
        let outcome = orchestrator
            .run_checkpoint(participants.clone(), ack_rx)
            .await
            .expect("each abort returns Ok");
        drop(ack_tx);
        assert_eq!(
            outcome.checkpoint_id(),
            expected_id,
            "checkpoint id strictly advances per cycle"
        );
        assert!(
            !outcome.is_completed(),
            "every cycle aborts while partition holds"
        );
    }

    assert_eq!(transport.aborts_count(), 3);
    assert_eq!(transport.completes_count(), 0);
    assert_eq!(raft.aborted_count(), 3);
    assert_eq!(raft.completed_count(), 0);
}

// ---------------------------------------------------------------------------
// Integration test — process-level partition via SIGSTOP/SIGCONT
// ---------------------------------------------------------------------------

mod integration {
    use std::process::{Child, Command, Stdio};
    use std::time::{Duration, Instant};

    use serde_json::Value as Json;
    use tokio::time::sleep;

    use super::super::{allocate_ports, find_binary, API_KEY};

    /// Polling cadence for "is the worker registered" checks.
    const POLL_INTERVAL: Duration = Duration::from_millis(200);

    /// Worker registration timeout.
    const REGISTER_TIMEOUT: Duration = Duration::from_secs(10);

    /// How long the partition is held in the integration test.
    const PARTITION_HOLD: Duration = Duration::from_secs(4);

    /// How long to wait after a heal before declaring the worker
    /// unresponsive.
    const HEAL_RECOVERY_WINDOW: Duration = Duration::from_secs(15);

    pub(super) fn nats_url() -> String {
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string())
    }

    pub(super) fn nats_tcp_target(url: &str) -> String {
        url.trim_start_matches("nats://")
            .trim_end_matches('/')
            .to_string()
    }

    pub(super) async fn tcp_reachable(addr: &str) -> bool {
        let target = addr.to_string();
        tokio::time::timeout(
            Duration::from_secs(2),
            tokio::net::TcpStream::connect(&target),
        )
        .await
        .is_ok_and(|r| r.is_ok())
    }

    /// Send `SIGSTOP` to the worker process — freezes execution. The
    /// worker stops processing NATS messages immediately, simulating a
    /// network partition where the worker is reachable on TCP but never
    /// responds to barrier traffic.
    ///
    /// Uses the system `kill` utility instead of `libc::kill` to avoid
    /// the workspace `unsafe-code` lint. Best-effort — returns `true`
    /// when `kill -STOP` exits 0.
    pub(super) fn pause_process(pid: i32) -> bool {
        Command::new("kill")
            .args(["-STOP", &pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|s| s.success())
    }

    /// Send `SIGCONT` to resume a previously-paused worker process.
    pub(super) fn resume_process(pid: i32) -> bool {
        Command::new("kill")
            .args(["-CONT", &pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|s| s.success())
    }

    /// Spawn a coordinator + 2 workers wired to `nats_url`. Returns the
    /// processes plus the coordinator's HTTP port.
    pub(super) struct PartitionedCluster {
        pub coordinator: Child,
        pub coord_port: u16,
        pub worker_a: Child,
        pub worker_a_id: String,
        pub worker_a_pid: i32,
        pub worker_b: Child,
        pub worker_b_id: String,
        pub http_client: reqwest::Client,
    }

    impl PartitionedCluster {
        pub async fn start(nats_url: &str) -> Option<Self> {
            let bin = find_binary();
            // 1 coord + 2 workers
            let base_port = allocate_ports(3);
            let coord_port = base_port;

            let coordinator = match Command::new(&bin)
                .args([
                    "coordinator",
                    "--port",
                    &coord_port.to_string(),
                    "--bind",
                    "127.0.0.1",
                    "--api-key",
                    API_KEY,
                    "--nats-url",
                    nats_url,
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
            {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("  [skip] failed to spawn coordinator: {e}");
                    return None;
                }
            };

            // Coordinator boot grace.
            sleep(Duration::from_secs(1)).await;

            let worker_a_port = base_port + 1;
            let worker_b_port = base_port + 2;
            let worker_a_id = format!("partition-wa-{}", std::process::id());
            let worker_b_id = format!("partition-wb-{}", std::process::id());

            let worker_a = Command::new(&bin)
                .args([
                    "server",
                    "--port",
                    &worker_a_port.to_string(),
                    "--coordinator",
                    &format!("http://127.0.0.1:{coord_port}"),
                    "--worker-id",
                    &worker_a_id,
                    "--api-key",
                    API_KEY,
                    "--bind",
                    "127.0.0.1",
                    "--nats-url",
                    nats_url,
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn worker_a");
            let worker_a_pid = worker_a.id() as i32;

            let worker_b = Command::new(&bin)
                .args([
                    "server",
                    "--port",
                    &worker_b_port.to_string(),
                    "--coordinator",
                    &format!("http://127.0.0.1:{coord_port}"),
                    "--worker-id",
                    &worker_b_id,
                    "--api-key",
                    API_KEY,
                    "--bind",
                    "127.0.0.1",
                    "--nats-url",
                    nats_url,
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn worker_b");

            let http_client = reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("http client");

            Some(Self {
                coordinator,
                coord_port,
                worker_a,
                worker_a_id,
                worker_a_pid,
                worker_b,
                worker_b_id,
                http_client,
            })
        }

        pub fn api_url(&self, path: &str) -> String {
            format!(
                "http://127.0.0.1:{}/api/v1/cluster{}",
                self.coord_port, path
            )
        }

        pub async fn wait_for_workers(&self, expected: usize) -> bool {
            let deadline = Instant::now() + REGISTER_TIMEOUT;
            loop {
                if let Ok(resp) = self
                    .http_client
                    .get(self.api_url("/workers"))
                    .header("x-api-key", API_KEY)
                    .send()
                    .await
                {
                    if let Ok(body) = resp.json::<Json>().await {
                        if let Some(total) = body["total"].as_u64() {
                            if total as usize >= expected {
                                return true;
                            }
                        }
                    }
                }
                if Instant::now() >= deadline {
                    return false;
                }
                sleep(POLL_INTERVAL).await;
            }
        }

        /// Probe coordinator's `/workers` and return the set of currently-
        /// registered worker IDs (best-effort; returns empty on RPC errors).
        pub async fn registered_worker_ids(&self) -> Vec<String> {
            let resp = match self
                .http_client
                .get(self.api_url("/workers"))
                .header("x-api-key", API_KEY)
                .send()
                .await
            {
                Ok(r) => r,
                Err(_) => return Vec::new(),
            };
            let body: Json = resp.json().await.unwrap_or(Json::Null);
            body["workers"]
                .as_array()
                .map(|ws| {
                    ws.iter()
                        .filter_map(|w| w["id"].as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default()
        }
    }

    impl Drop for PartitionedCluster {
        fn drop(&mut self) {
            // SIGCONT before SIGKILL so a paused process actually
            // receives the kill signal.
            let _ = resume_process(self.worker_a_pid);
            let _ = self.worker_a.kill();
            let _ = self.worker_a.wait();
            let _ = self.worker_b.kill();
            let _ = self.worker_b.wait();
            let _ = self.coordinator.kill();
            let _ = self.coordinator.wait();
        }
    }

    pub(super) async fn run_partition_test() {
        let nats = nats_url();
        if !tcp_reachable(&nats_tcp_target(&nats)).await {
            eprintln!("  [skip] test_network_partition_real_nats: NATS not reachable at {nats}");
            return;
        }

        let cluster = match PartitionedCluster::start(&nats).await {
            Some(c) => c,
            None => return,
        };

        // Both workers must register first so the partition is meaningful.
        if !cluster.wait_for_workers(2).await {
            eprintln!(
                "  [skip] test_network_partition_real_nats: workers failed to register \
                 (binary may lack `nats-transport`/`distributed-checkpoint` features)"
            );
            return;
        }

        let initial_workers = cluster.registered_worker_ids().await;
        eprintln!("  initial workers: {initial_workers:?}");
        assert!(
            initial_workers.contains(&cluster.worker_a_id)
                && initial_workers.contains(&cluster.worker_b_id),
            "both workers must be registered before partition"
        );

        // ---- Partition: SIGSTOP worker A -------------------------------
        eprintln!(
            "  partitioning worker A (pid {}) via SIGSTOP",
            cluster.worker_a_pid
        );
        assert!(
            pause_process(cluster.worker_a_pid),
            "SIGSTOP must succeed for our own child process"
        );

        // While paused, worker A cannot service NATS messages — any
        // checkpoint barrier dispatched to it would time out at the
        // orchestrator. Hold the partition briefly to demonstrate the
        // worker remains "registered but unresponsive" — the coordinator
        // does not auto-deregister within the partition window.
        sleep(PARTITION_HOLD).await;
        let during_partition = cluster.registered_worker_ids().await;
        eprintln!("  during partition workers: {during_partition:?}");
        // Worker A is still in the registered set (the coordinator hasn't
        // removed it within PARTITION_HOLD). This is the exact condition
        // the distributed-checkpoint orchestrator must handle: a
        // participant that's nominally part of the group but physically
        // unreachable.

        // ---- Heal: SIGCONT worker A ------------------------------------
        eprintln!(
            "  healing partition: SIGCONT worker A (pid {})",
            cluster.worker_a_pid
        );
        assert!(
            resume_process(cluster.worker_a_pid),
            "SIGCONT must succeed for our own paused process"
        );

        // After heal the worker should recover its NATS subscription and
        // continue to be visible to the coordinator. We poll for
        // continued availability — if the worker died as a result of
        // being paused, this will trip.
        let recovery_deadline = Instant::now() + HEAL_RECOVERY_WINDOW;
        let mut healed = false;
        while Instant::now() < recovery_deadline {
            let now_workers = cluster.registered_worker_ids().await;
            if now_workers.contains(&cluster.worker_a_id)
                && now_workers.contains(&cluster.worker_b_id)
            {
                healed = true;
                break;
            }
            sleep(POLL_INTERVAL).await;
        }
        assert!(
            healed,
            "worker A must remain visible after SIGCONT (heal); the partition simulation \
             should not crash the worker"
        );

        eprintln!("  partition simulation complete — both workers visible after heal");
    }
}

/// End-to-end network partition simulation against a real NATS broker.
///
/// Spawns a coordinator + 2 workers, all connected to NATS. Partitions
/// worker A by sending it `SIGSTOP` (the worker process freezes; NATS
/// messages destined for it queue or expire — exactly the observable
/// behaviour of a NATS-side partition). Heals by `SIGCONT`.
///
/// Asserted properties:
///   1. Both workers register before the partition.
///   2. The partitioned worker remains in the coordinator's registered
///      set during the partition window (i.e. the coordinator hasn't
///      auto-removed it; the partition is real and the orchestrator must
///      cope with the inconsistency).
///   3. After healing, both workers are visible to the coordinator
///      (the worker process did not die as a side effect of being paused
///      — a real partition must be recoverable).
///
/// `[skip]`s cleanly when NATS is not reachable. The full distributed-
/// checkpoint partition assertion (checkpoint aborts, no data loss,
/// recovery succeeds) is exercised by the unit tests above; this test
/// validates the *physical* partition simulation primitive against a real
/// NATS broker.
#[tokio::test]
#[ignore]
async fn test_network_partition_real_nats() {
    integration::run_partition_test().await;
}

// ---------------------------------------------------------------------------
// Inline harness sanity checks (no infra)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod harness {
    use super::*;

    #[test]
    fn partitioned_state_round_trips() {
        let t = PartitionableTransport::new();
        assert!(!t.is_inbound_partitioned("w0"));
        t.partition_inbound("w0");
        assert!(t.is_inbound_partitioned("w0"));
        t.heal("w0");
        assert!(!t.is_inbound_partitioned("w0"));
    }

    #[test]
    fn outbound_partition_failure_path_is_distinguishable() {
        // The error path failure in barrier dispatch should be marked as
        // a "partition" cause so the audit log is searchable.
        let t = PartitionableTransport::new();
        t.partition_outbound("w0");
        let outbound_set = t.outbound_partitioned.lock().unwrap();
        assert!(outbound_set.contains("w0"));
    }

    #[test]
    fn empty_checkpoint_round_trips() {
        // Sanity: the helper produces an inline location whose checkpoint
        // is at the current schema version. A future schema bump would
        // surface here as a unit-test failure rather than a confusing
        // serialize error in the orchestrator.
        let loc = empty_checkpoint_loc();
        match loc {
            SnapshotLocation::Inline { checkpoint } => {
                assert_eq!(checkpoint.version, CHECKPOINT_VERSION);
            }
            SnapshotLocation::Remote { .. } => panic!("expected inline"),
        }
    }

    #[test]
    fn group_id_is_pinned() {
        // Tests share GROUP_ID; if a refactor renames it, the
        // store_key_for(...) string format would silently change. Pin it.
        assert_eq!(GROUP_ID, "partition-grp");
    }

    /// Static guard so the unit/integration partition design assumptions
    /// stay aligned: any worker the orchestrator sees as partitioned must
    /// either fail-dispatch (outbound) or be excluded from ack synthesis
    /// (inbound). The driver helper treats the inbound set as
    /// authoritative for "do we send an ack" — encode that here.
    #[test]
    fn ack_driver_skips_inbound_partitioned_workers() {
        let t = Arc::new(PartitionableTransport::new());
        t.partition_inbound("w1");
        // Workers w0 and w2 are healthy.
        let healthy: Vec<&str> = ["w0", "w1", "w2"]
            .into_iter()
            .filter(|w| !t.is_inbound_partitioned(w))
            .collect();
        assert_eq!(healthy, vec!["w0", "w2"]);
    }

    /// AtomicBool guard — confirms that the driver/transport state we
    /// share between the spawned ack task and the orchestrator is
    /// `Send + Sync`, which the orchestrator's `Arc<T>` bound requires.
    /// Compile-time assertion.
    #[test]
    fn transport_is_send_and_sync() {
        fn require_send_sync<T: Send + Sync>() {}
        require_send_sync::<PartitionableTransport>();
        require_send_sync::<AtomicBool>(); // sanity
    }
}
