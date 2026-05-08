//! Distributed checkpoint protocol types.
//!
//! Defines the wire messages exchanged between the coordinator and workers
//! during a coordinated, exactly-once distributed checkpoint, plus a small
//! state machine the coordinator uses to drive the protocol.
//!
//! ## Protocol overview
//!
//! Varpulis does not run a Flink-style streaming DAG; workers run independent
//! pipeline instances and inter-pipeline traffic flows over NATS. So barriers
//! do not propagate through data channels — the coordinator triggers them
//! explicitly. This is closer to a centralised two-phase commit than a true
//! Chandy-Lamport snapshot.
//!
//! ```text
//!   Coordinator                                 Worker(s)
//!  ─────────────                                ──────────
//!     Idle                                          .
//!       │
//!       │ pause_sources + drain + create_checkpoint
//!       │ + prepare_commit_sinks
//!       │   CheckpointBarrierRequest ─────────►   .
//!  BarrierSent                                    │
//!       .                                         │
//!       . ◄─── CheckpointBarrierAck ──────────────│
//! AcksCollecting                                  .
//!       │
//!       │ persist DistributedCheckpoint to StateStore
//!  Persisting
//!       │
//!       │   CheckpointCompleteNotification ────► commit_sinks
//!   Complete                                     resume_sources
//! ```
//!
//! On any failure (timeout, NACK, persistence error) the coordinator emits a
//! [`CheckpointAbortNotification`] and the workers discard the prepared
//! state. Workers also self-abort if no `complete`/`abort` arrives within the
//! barrier timeout — see Task 2.1.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use varpulis_runtime::persistence::EngineCheckpoint;

use crate::worker::WorkerId;

// ---------------------------------------------------------------------------
// Identifiers
// ---------------------------------------------------------------------------

/// Monotonically increasing identifier for a distributed checkpoint within a
/// pipeline group. Matches the `u64` checkpoint id used by the single-engine
/// `varpulis_runtime::persistence::Checkpoint`.
pub type CheckpointId = u64;

/// Identifier for a pipeline group — same string id used by
/// [`crate::pipeline_group::DeployedPipelineGroup::id`].
pub type GroupId = String;

// ---------------------------------------------------------------------------
// Snapshot location
// ---------------------------------------------------------------------------

/// Maximum size at which a snapshot is shipped inline in a NATS message
/// instead of uploaded to the shared state store.
///
/// NATS' default `max_payload` is 1 MiB; staying strictly under that keeps
/// barrier traffic free from broker reconfiguration. Larger snapshots go to
/// S3 / RocksDB and are referenced by `store_key`.
pub const INLINE_SNAPSHOT_THRESHOLD_BYTES: usize = 1024 * 1024;

/// Where a worker's snapshot lives.
///
/// Small snapshots ride inline in the ack message; large snapshots are
/// uploaded to the shared state store first and referenced by key.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum SnapshotLocation {
    /// Snapshot bytes shipped inline (< [`INLINE_SNAPSHOT_THRESHOLD_BYTES`]).
    Inline {
        /// The worker's engine checkpoint.
        checkpoint: Box<EngineCheckpoint>,
    },
    /// Snapshot persisted to the shared state store under `store_key`.
    Remote {
        /// State store key (e.g. `"checkpoints/{group}/{id}/{worker}.json"`).
        store_key: String,
        /// Serialized size in bytes — useful for accounting and debugging.
        size_bytes: u64,
    },
}

impl SnapshotLocation {
    /// Returns `true` if this location has the snapshot bytes embedded.
    pub const fn is_inline(&self) -> bool {
        matches!(self, Self::Inline { .. })
    }

    /// Returns the inline checkpoint, if any.
    pub fn as_inline(&self) -> Option<&EngineCheckpoint> {
        match self {
            Self::Inline { checkpoint } => Some(checkpoint),
            Self::Remote { .. } => None,
        }
    }

    /// Returns the remote store key, if any.
    pub fn as_remote_key(&self) -> Option<&str> {
        match self {
            Self::Remote { store_key, .. } => Some(store_key.as_str()),
            Self::Inline { .. } => None,
        }
    }
}

/// Outcome of [`classify_snapshot`] — tells the worker whether it can ship
/// the checkpoint inline or must upload to the state store first.
#[derive(Debug)]
pub enum SnapshotClassification {
    /// Checkpoint fits inline; a ready-to-send [`SnapshotLocation::Inline`]
    /// is included.
    Inline(SnapshotLocation),
    /// Checkpoint exceeds the inline threshold; the caller must upload the
    /// owned `checkpoint` to the state store and then build a
    /// [`SnapshotLocation::Remote`] referencing it.
    NeedsUpload {
        /// The original checkpoint, returned to the caller for upload.
        checkpoint: Box<EngineCheckpoint>,
        /// Serialized size in bytes — useful when computing the store key.
        size_bytes: usize,
    },
}

/// Decide whether `checkpoint` should ride inline or be uploaded out of band.
///
/// Serializes the checkpoint to JSON to measure its on-the-wire size; this
/// matches the format used by [`varpulis_runtime::persistence::EngineCheckpoint`]
/// elsewhere in the codebase.
pub fn classify_snapshot(
    checkpoint: EngineCheckpoint,
) -> Result<SnapshotClassification, ProtocolError> {
    let size_bytes = serde_json::to_vec(&checkpoint)
        .map(|v| v.len())
        .map_err(|e| ProtocolError::Serialize(e.to_string()))?;
    if size_bytes < INLINE_SNAPSHOT_THRESHOLD_BYTES {
        Ok(SnapshotClassification::Inline(SnapshotLocation::Inline {
            checkpoint: Box::new(checkpoint),
        }))
    } else {
        Ok(SnapshotClassification::NeedsUpload {
            checkpoint: Box::new(checkpoint),
            size_bytes,
        })
    }
}

// ---------------------------------------------------------------------------
// Wire messages
// ---------------------------------------------------------------------------

/// Coordinator → worker: "snapshot pipeline `pipeline_id` at checkpoint
/// `checkpoint_id` and reply with an ack within `timeout_ms`".
///
/// Sent over `varpulis.cluster.cmd.{worker_id}.checkpoint_barrier` (Task 1.2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointBarrierRequest {
    /// Pipeline group this checkpoint belongs to.
    pub group_id: GroupId,
    /// Globally unique checkpoint id within the group.
    pub checkpoint_id: CheckpointId,
    /// Pipeline instance to checkpoint on this worker.
    pub pipeline_id: String,
    /// Worker self-abort budget — if no complete/abort arrives within this
    /// window the worker must roll back locally (Task 2.1).
    pub timeout_ms: u64,
    /// Wall-clock timestamp (ms since Unix epoch) when the coordinator began
    /// the checkpoint. Recorded into [`DistributedCheckpoint::timestamp_ms`].
    pub triggered_at_ms: i64,
}

/// Worker → coordinator: "I have prepared snapshot for `checkpoint_id`".
///
/// Sent over `varpulis.cluster.checkpoint.ack.{group_id}` (Task 1.2). Workers
/// that fail to prepare reply with `error` set instead of `location`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointBarrierAck {
    /// Pipeline group.
    pub group_id: GroupId,
    /// Checkpoint id this ack corresponds to.
    pub checkpoint_id: CheckpointId,
    /// Worker that produced the snapshot.
    pub worker_id: WorkerId,
    /// Pipeline instance (lets the coordinator pair acks per replica).
    pub pipeline_id: String,
    /// Where the snapshot is — set when the worker successfully prepared.
    #[serde(default)]
    pub location: Option<SnapshotLocation>,
    /// Failure reason — set when the worker could not prepare.
    /// Mutually exclusive with `location`; populating both is a protocol
    /// violation that the coordinator should treat as an abort.
    #[serde(default)]
    pub error: Option<String>,
}

impl CheckpointBarrierAck {
    /// Build a successful ack with an inline or remote snapshot.
    pub fn success(
        group_id: GroupId,
        checkpoint_id: CheckpointId,
        worker_id: WorkerId,
        pipeline_id: String,
        location: SnapshotLocation,
    ) -> Self {
        Self {
            group_id,
            checkpoint_id,
            worker_id,
            pipeline_id,
            location: Some(location),
            error: None,
        }
    }

    /// Build a failure ack with an explanatory error.
    pub fn failure(
        group_id: GroupId,
        checkpoint_id: CheckpointId,
        worker_id: WorkerId,
        pipeline_id: String,
        error: impl Into<String>,
    ) -> Self {
        Self {
            group_id,
            checkpoint_id,
            worker_id,
            pipeline_id,
            location: None,
            error: Some(error.into()),
        }
    }

    /// Returns `true` if this ack carries a usable snapshot location.
    pub const fn is_success(&self) -> bool {
        self.location.is_some() && self.error.is_none()
    }
}

/// Coordinator → workers: "checkpoint `checkpoint_id` is durable, commit and
/// resume". Triggers `commit_sinks` + `begin_epoch_sinks(id+1)` +
/// `commit_source_offsets` on each worker (Task 2.1).
///
/// Broadcast over `varpulis.cluster.checkpoint.complete.{group_id}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointCompleteNotification {
    /// Pipeline group.
    pub group_id: GroupId,
    /// Checkpoint that has been durably persisted.
    pub checkpoint_id: CheckpointId,
    /// Wall-clock timestamp (ms since Unix epoch) when the coordinator
    /// finished persisting the assembled checkpoint.
    pub committed_at_ms: i64,
}

/// Coordinator → workers: "abort checkpoint `checkpoint_id`".
///
/// Triggers `abort_sinks` and discards the prepared snapshot. Workers
/// resume processing from the previous committed checkpoint.
///
/// Broadcast over `varpulis.cluster.checkpoint.abort.{group_id}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointAbortNotification {
    /// Pipeline group.
    pub group_id: GroupId,
    /// Checkpoint that is being aborted.
    pub checkpoint_id: CheckpointId,
    /// Why the checkpoint was aborted (timeout, worker NACK, persistence
    /// failure, …) — recorded for observability.
    pub reason: String,
}

// ---------------------------------------------------------------------------
// Assembled snapshot
// ---------------------------------------------------------------------------

/// A complete distributed checkpoint.
///
/// The union of every worker's per-pipeline snapshot for one group at one
/// logical point in time. Serialized to the shared state store (S3 /
/// RocksDB) by the coordinator before [`CheckpointCompleteNotification`] is
/// broadcast.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedCheckpoint {
    /// Pipeline group this snapshot belongs to.
    pub group_id: GroupId,
    /// Globally unique id within the group.
    pub checkpoint_id: CheckpointId,
    /// Wall-clock timestamp (ms since Unix epoch) when the coordinator
    /// initiated the barrier — matches the convention of
    /// [`varpulis_runtime::persistence::Checkpoint::timestamp_ms`].
    pub timestamp_ms: i64,
    /// Per-(worker, pipeline) snapshot locations. The map key is built by
    /// [`participant_key`] so the same worker can hold multiple replicas.
    pub snapshots: HashMap<String, SnapshotLocation>,
}

/// Build the participant map key used in [`DistributedCheckpoint::snapshots`].
///
/// Format: `"{worker_id}/{pipeline_id}"`. Pinned so the coordinator and
/// recovery code agree on it.
pub fn participant_key(worker_id: &WorkerId, pipeline_id: &str) -> String {
    format!("{}/{pipeline_id}", worker_id.0)
}

// ---------------------------------------------------------------------------
// Protocol state machine
// ---------------------------------------------------------------------------

/// States the coordinator's checkpoint orchestrator walks through for one
/// in-flight distributed checkpoint.
///
/// Happy path: `Idle → BarrierSent → AcksCollecting → Persisting → Complete`.
/// Any failure transitions to `Aborted`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointState {
    /// No checkpoint is in flight.
    Idle,
    /// Barriers have been dispatched to all participants; waiting for the
    /// first ack.
    BarrierSent,
    /// At least one ack received; still missing some participants.
    AcksCollecting,
    /// All acks received; persisting the assembled checkpoint to the state
    /// store.
    Persisting,
    /// Checkpoint is durable and the complete notification has been
    /// broadcast.
    Complete,
    /// Checkpoint was aborted (timeout, NACK, persistence failure, …).
    Aborted,
}

impl CheckpointState {
    /// Returns `true` if this is a terminal state (`Complete` or `Aborted`).
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Complete | Self::Aborted)
    }
}

impl std::fmt::Display for CheckpointState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Idle => write!(f, "idle"),
            Self::BarrierSent => write!(f, "barrier_sent"),
            Self::AcksCollecting => write!(f, "acks_collecting"),
            Self::Persisting => write!(f, "persisting"),
            Self::Complete => write!(f, "complete"),
            Self::Aborted => write!(f, "aborted"),
        }
    }
}

/// In-memory tracker the coordinator keeps for each in-flight checkpoint.
///
/// Holds the protocol state, the set of participants we still owe an ack,
/// and the snapshots collected so far. Not serialized over the wire — only
/// persisted indirectly when [`CoordinatorCheckpointTracker::assemble`] is
/// called and the resulting [`DistributedCheckpoint`] is written to the
/// state store.
#[derive(Debug)]
pub struct CoordinatorCheckpointTracker {
    /// Pipeline group.
    pub group_id: GroupId,
    /// Checkpoint id.
    pub checkpoint_id: CheckpointId,
    /// Current protocol state.
    state: CheckpointState,
    /// When the barrier was dispatched (used for timeout detection).
    started_at: Instant,
    /// How long the coordinator will wait for all acks before aborting.
    timeout: Duration,
    /// Wall-clock timestamp recorded into the assembled checkpoint.
    triggered_at_ms: i64,
    /// Participants we expect acks from. A participant is one
    /// `(worker_id, pipeline_id)` pair — built via [`participant_key`].
    pending: HashSet<String>,
    /// Snapshots received so far.
    snapshots: HashMap<String, SnapshotLocation>,
    /// Workers that NACK'd or timed out.
    failed: HashMap<String, String>,
    /// Reason recorded when transitioning to `Aborted`.
    abort_reason: Option<String>,
}

impl CoordinatorCheckpointTracker {
    /// Start tracking a fresh checkpoint.
    ///
    /// `participants` are the `(worker_id, pipeline_id)` pairs the
    /// coordinator dispatched barriers to. Empty participant lists are
    /// accepted but the resulting checkpoint trivially completes after
    /// the first state advance — callers that don't want that should
    /// validate before constructing the tracker.
    pub fn new(
        group_id: GroupId,
        checkpoint_id: CheckpointId,
        participants: impl IntoIterator<Item = (WorkerId, String)>,
        timeout: Duration,
        triggered_at_ms: i64,
    ) -> Self {
        let pending: HashSet<String> = participants
            .into_iter()
            .map(|(w, p)| participant_key(&w, &p))
            .collect();
        Self {
            group_id,
            checkpoint_id,
            state: CheckpointState::Idle,
            started_at: Instant::now(),
            timeout,
            triggered_at_ms,
            pending,
            snapshots: HashMap::new(),
            failed: HashMap::new(),
            abort_reason: None,
        }
    }

    /// Current state.
    pub const fn state(&self) -> CheckpointState {
        self.state
    }

    /// Participants we are still waiting on.
    pub fn pending(&self) -> &HashSet<String> {
        &self.pending
    }

    /// Snapshots collected so far.
    pub fn snapshots(&self) -> &HashMap<String, SnapshotLocation> {
        &self.snapshots
    }

    /// Workers that failed to ack (NACK or timeout-flagged).
    pub fn failed(&self) -> &HashMap<String, String> {
        &self.failed
    }

    /// Reason recorded if the tracker transitioned to `Aborted`.
    pub fn abort_reason(&self) -> Option<&str> {
        self.abort_reason.as_deref()
    }

    /// Transition `Idle → BarrierSent`. Called after the coordinator has
    /// dispatched all barrier messages.
    pub fn mark_barrier_sent(&mut self) -> Result<(), ProtocolError> {
        self.expect_state(CheckpointState::Idle)?;
        self.state = CheckpointState::BarrierSent;
        self.started_at = Instant::now();
        Ok(())
    }

    /// Record a successful ack from one participant.
    ///
    /// Transitions `BarrierSent → AcksCollecting` on the first ack and may
    /// later be followed by [`CoordinatorCheckpointTracker::mark_persisting`]
    /// once [`CoordinatorCheckpointTracker::all_acks_received`] returns true.
    pub fn record_ack(
        &mut self,
        worker_id: &WorkerId,
        pipeline_id: &str,
        location: SnapshotLocation,
    ) -> Result<(), ProtocolError> {
        if !matches!(
            self.state,
            CheckpointState::BarrierSent | CheckpointState::AcksCollecting
        ) {
            return Err(ProtocolError::InvalidTransition {
                from: self.state,
                action: "record_ack",
            });
        }
        let key = participant_key(worker_id, pipeline_id);
        if !self.pending.remove(&key) {
            return Err(ProtocolError::UnknownParticipant(key));
        }
        self.snapshots.insert(key, location);
        if matches!(self.state, CheckpointState::BarrierSent) {
            self.state = CheckpointState::AcksCollecting;
        }
        Ok(())
    }

    /// Record a participant failure (worker NACK).
    ///
    /// Removes the participant from the pending set but does not
    /// automatically abort — the caller decides whether one failure should
    /// fail the whole checkpoint (typical) or be tolerated. The recorded
    /// failures can be inspected via
    /// [`CoordinatorCheckpointTracker::failed`].
    pub fn record_failure(
        &mut self,
        worker_id: &WorkerId,
        pipeline_id: &str,
        error: impl Into<String>,
    ) -> Result<(), ProtocolError> {
        if !matches!(
            self.state,
            CheckpointState::BarrierSent | CheckpointState::AcksCollecting
        ) {
            return Err(ProtocolError::InvalidTransition {
                from: self.state,
                action: "record_failure",
            });
        }
        let key = participant_key(worker_id, pipeline_id);
        if !self.pending.remove(&key) {
            return Err(ProtocolError::UnknownParticipant(key));
        }
        self.failed.insert(key, error.into());
        if matches!(self.state, CheckpointState::BarrierSent) {
            self.state = CheckpointState::AcksCollecting;
        }
        Ok(())
    }

    /// Returns `true` once every expected participant has either acked or
    /// failed.
    pub fn all_acks_received(&self) -> bool {
        self.pending.is_empty()
    }

    /// Returns `true` if the tracker has passed its timeout. The coordinator
    /// uses this to decide whether to abort an in-flight checkpoint.
    pub fn is_timed_out(&self, now: Instant) -> bool {
        if self.state.is_terminal() {
            return false;
        }
        now.duration_since(self.started_at) > self.timeout
    }

    /// Transition `AcksCollecting → Persisting`. Requires that all expected
    /// acks have been received and that no participant has failed.
    pub fn mark_persisting(&mut self) -> Result<(), ProtocolError> {
        self.expect_state(CheckpointState::AcksCollecting)?;
        if !self.pending.is_empty() {
            return Err(ProtocolError::PendingAcks(self.pending.len()));
        }
        if !self.failed.is_empty() {
            return Err(ProtocolError::ParticipantFailures(self.failed.len()));
        }
        self.state = CheckpointState::Persisting;
        Ok(())
    }

    /// Transition `Persisting → Complete` and consume the tracker into the
    /// assembled [`DistributedCheckpoint`] ready to be persisted.
    pub fn mark_complete(&mut self) -> Result<DistributedCheckpoint, ProtocolError> {
        self.expect_state(CheckpointState::Persisting)?;
        self.state = CheckpointState::Complete;
        Ok(DistributedCheckpoint {
            group_id: self.group_id.clone(),
            checkpoint_id: self.checkpoint_id,
            timestamp_ms: self.triggered_at_ms,
            snapshots: std::mem::take(&mut self.snapshots),
        })
    }

    /// Transition any non-terminal state to `Aborted`.
    pub fn mark_aborted(&mut self, reason: impl Into<String>) -> Result<(), ProtocolError> {
        if self.state.is_terminal() {
            return Err(ProtocolError::InvalidTransition {
                from: self.state,
                action: "mark_aborted",
            });
        }
        self.state = CheckpointState::Aborted;
        self.abort_reason = Some(reason.into());
        Ok(())
    }

    fn expect_state(&self, expected: CheckpointState) -> Result<(), ProtocolError> {
        if self.state == expected {
            Ok(())
        } else {
            Err(ProtocolError::InvalidTransition {
                from: self.state,
                action: "expect_state",
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors raised by the protocol state machine and helpers.
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    /// State machine was driven into an illegal transition.
    #[error("invalid state transition from {from} ({action})")]
    InvalidTransition {
        /// The state we were in.
        from: CheckpointState,
        /// What the caller tried to do.
        action: &'static str,
    },

    /// Ack/failure was reported for a worker/pipeline pair the coordinator
    /// did not register as a participant.
    #[error("unknown participant: {0}")]
    UnknownParticipant(String),

    /// `mark_persisting` was called while acks are still outstanding.
    #[error("{0} participants have not acked yet")]
    PendingAcks(usize),

    /// `mark_persisting` was called with at least one participant failure
    /// recorded.
    #[error("{0} participants failed to prepare")]
    ParticipantFailures(usize),

    /// Snapshot serialization failed during classification.
    #[error("serialize: {0}")]
    Serialize(String),
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn worker(id: &str) -> WorkerId {
        WorkerId(id.into())
    }

    fn empty_checkpoint() -> EngineCheckpoint {
        EngineCheckpoint {
            version: varpulis_runtime::persistence::CHECKPOINT_VERSION,
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
        }
    }

    fn inline_loc() -> SnapshotLocation {
        SnapshotLocation::Inline {
            checkpoint: Box::new(empty_checkpoint()),
        }
    }

    #[test]
    fn participant_key_is_stable() {
        assert_eq!(participant_key(&worker("w1"), "ingest"), "w1/ingest");
        assert_ne!(
            participant_key(&worker("w1"), "ingest"),
            participant_key(&worker("w2"), "ingest"),
        );
    }

    #[test]
    fn snapshot_location_inline_helpers() {
        let loc = inline_loc();
        assert!(loc.is_inline());
        assert!(loc.as_inline().is_some());
        assert!(loc.as_remote_key().is_none());

        let remote = SnapshotLocation::Remote {
            store_key: "checkpoints/g/1/w0.json".into(),
            size_bytes: 4_500_000,
        };
        assert!(!remote.is_inline());
        assert_eq!(remote.as_remote_key(), Some("checkpoints/g/1/w0.json"));
        assert!(remote.as_inline().is_none());
    }

    #[test]
    fn classify_small_checkpoint_returns_inline() {
        let outcome = classify_snapshot(empty_checkpoint()).unwrap();
        match outcome {
            SnapshotClassification::Inline(loc) => assert!(loc.is_inline()),
            SnapshotClassification::NeedsUpload { .. } => {
                panic!("empty checkpoint should fit inline")
            }
        }
    }

    #[test]
    fn classify_large_checkpoint_returns_needs_upload() {
        // Stuff one giant string into variables to push past the 1 MiB
        // threshold without depending on any real engine state.
        let mut cp = empty_checkpoint();
        let big = "x".repeat(2 * INLINE_SNAPSHOT_THRESHOLD_BYTES);
        cp.variables.insert(
            "big".into(),
            varpulis_runtime::persistence::SerializableValue::String(big),
        );

        match classify_snapshot(cp).unwrap() {
            SnapshotClassification::NeedsUpload { size_bytes, .. } => {
                assert!(size_bytes > INLINE_SNAPSHOT_THRESHOLD_BYTES);
            }
            SnapshotClassification::Inline(_) => {
                panic!("oversized checkpoint should not fit inline")
            }
        }
    }

    #[test]
    fn ack_success_helpers() {
        let ack =
            CheckpointBarrierAck::success("g".into(), 7, worker("w0"), "p".into(), inline_loc());
        assert!(ack.is_success());

        let nack = CheckpointBarrierAck::failure(
            "g".into(),
            7,
            worker("w0"),
            "p".into(),
            "kafka producer broken",
        );
        assert!(!nack.is_success());
        assert!(nack.error.is_some());
        assert!(nack.location.is_none());
    }

    #[test]
    fn checkpoint_state_terminal() {
        assert!(!CheckpointState::Idle.is_terminal());
        assert!(!CheckpointState::BarrierSent.is_terminal());
        assert!(!CheckpointState::AcksCollecting.is_terminal());
        assert!(!CheckpointState::Persisting.is_terminal());
        assert!(CheckpointState::Complete.is_terminal());
        assert!(CheckpointState::Aborted.is_terminal());
    }

    #[test]
    fn happy_path_state_machine() {
        let mut t = CoordinatorCheckpointTracker::new(
            "g".into(),
            42,
            [
                (worker("w0"), "p".to_string()),
                (worker("w1"), "p".to_string()),
            ],
            Duration::from_secs(30),
            1_700_000_000_000,
        );
        assert_eq!(t.state(), CheckpointState::Idle);
        assert_eq!(t.pending().len(), 2);

        t.mark_barrier_sent().unwrap();
        assert_eq!(t.state(), CheckpointState::BarrierSent);

        t.record_ack(&worker("w0"), "p", inline_loc()).unwrap();
        assert_eq!(t.state(), CheckpointState::AcksCollecting);
        assert!(!t.all_acks_received());

        t.record_ack(&worker("w1"), "p", inline_loc()).unwrap();
        assert!(t.all_acks_received());

        t.mark_persisting().unwrap();
        assert_eq!(t.state(), CheckpointState::Persisting);

        let assembled = t.mark_complete().unwrap();
        assert_eq!(t.state(), CheckpointState::Complete);
        assert_eq!(assembled.checkpoint_id, 42);
        assert_eq!(assembled.snapshots.len(), 2);
        assert_eq!(assembled.timestamp_ms, 1_700_000_000_000);
    }

    #[test]
    fn failure_aborts_persist() {
        let mut t = CoordinatorCheckpointTracker::new(
            "g".into(),
            1,
            [
                (worker("w0"), "p".to_string()),
                (worker("w1"), "p".to_string()),
            ],
            Duration::from_secs(30),
            0,
        );
        t.mark_barrier_sent().unwrap();
        t.record_ack(&worker("w0"), "p", inline_loc()).unwrap();
        t.record_failure(&worker("w1"), "p", "kafka down").unwrap();
        assert!(t.all_acks_received());
        // Cannot persist with failures recorded.
        let err = t.mark_persisting().unwrap_err();
        assert!(matches!(err, ProtocolError::ParticipantFailures(1)));
        // Coordinator aborts.
        t.mark_aborted("worker NACK").unwrap();
        assert_eq!(t.state(), CheckpointState::Aborted);
        assert_eq!(t.abort_reason(), Some("worker NACK"));
    }

    #[test]
    fn cannot_persist_with_pending_acks() {
        let mut t = CoordinatorCheckpointTracker::new(
            "g".into(),
            1,
            [
                (worker("w0"), "p".to_string()),
                (worker("w1"), "p".to_string()),
            ],
            Duration::from_secs(30),
            0,
        );
        t.mark_barrier_sent().unwrap();
        t.record_ack(&worker("w0"), "p", inline_loc()).unwrap();
        let err = t.mark_persisting().unwrap_err();
        assert!(matches!(err, ProtocolError::PendingAcks(1)));
    }

    #[test]
    fn unknown_participant_rejected() {
        let mut t = CoordinatorCheckpointTracker::new(
            "g".into(),
            1,
            [(worker("w0"), "p".to_string())],
            Duration::from_secs(30),
            0,
        );
        t.mark_barrier_sent().unwrap();
        let err = t.record_ack(&worker("w99"), "p", inline_loc()).unwrap_err();
        assert!(matches!(err, ProtocolError::UnknownParticipant(_)));
    }

    #[test]
    fn invalid_transitions_caught() {
        let mut t = CoordinatorCheckpointTracker::new(
            "g".into(),
            1,
            [(worker("w0"), "p".to_string())],
            Duration::from_secs(30),
            0,
        );
        // Cannot record ack while still Idle.
        let err = t.record_ack(&worker("w0"), "p", inline_loc()).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidTransition { .. }));

        // Drive to Complete then verify abort is rejected.
        t.mark_barrier_sent().unwrap();
        t.record_ack(&worker("w0"), "p", inline_loc()).unwrap();
        t.mark_persisting().unwrap();
        let _ = t.mark_complete().unwrap();
        let err = t.mark_aborted("late").unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidTransition { .. }));
    }

    #[test]
    fn timeout_detection() {
        let mut t = CoordinatorCheckpointTracker::new(
            "g".into(),
            1,
            [(worker("w0"), "p".to_string())],
            Duration::from_millis(0),
            0,
        );
        t.mark_barrier_sent().unwrap();
        // Zero-duration timeout means any subsequent moment is past it.
        std::thread::sleep(Duration::from_millis(2));
        assert!(t.is_timed_out(Instant::now()));
        // Terminal states never report timed-out.
        t.mark_aborted("test").unwrap();
        assert!(!t.is_timed_out(Instant::now()));
    }

    #[test]
    fn wire_messages_round_trip_json() {
        let req = CheckpointBarrierRequest {
            group_id: "g".into(),
            checkpoint_id: 9,
            pipeline_id: "p".into(),
            timeout_ms: 30_000,
            triggered_at_ms: 1_700_000_000_000,
        };
        let bytes = serde_json::to_vec(&req).unwrap();
        let back: CheckpointBarrierRequest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back.checkpoint_id, 9);

        let ack =
            CheckpointBarrierAck::success("g".into(), 9, worker("w0"), "p".into(), inline_loc());
        let bytes = serde_json::to_vec(&ack).unwrap();
        let back: CheckpointBarrierAck = serde_json::from_slice(&bytes).unwrap();
        assert!(back.is_success());

        let complete = CheckpointCompleteNotification {
            group_id: "g".into(),
            checkpoint_id: 9,
            committed_at_ms: 1_700_000_000_500,
        };
        let bytes = serde_json::to_vec(&complete).unwrap();
        let _back: CheckpointCompleteNotification = serde_json::from_slice(&bytes).unwrap();

        let abort = CheckpointAbortNotification {
            group_id: "g".into(),
            checkpoint_id: 9,
            reason: "timeout".into(),
        };
        let bytes = serde_json::to_vec(&abort).unwrap();
        let _back: CheckpointAbortNotification = serde_json::from_slice(&bytes).unwrap();
    }
}
