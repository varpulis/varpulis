//! Per-group distributed checkpoint orchestrator.
//!
//! Implements the coordinator side of the protocol defined in
//! [`crate::checkpoint_protocol`]. One [`DistributedCheckpointCoordinator`]
//! instance manages a single exactly-once pipeline group: it dispatches
//! barriers to every participant, collects acks within a configurable
//! timeout, persists the assembled [`DistributedCheckpoint`] to the shared
//! state store, broadcasts completion or abort to the workers, and (when a
//! Raft replicator is wired) replicates the outcome through Raft so other
//! coordinators see the latest durable checkpoint id.
//!
//! Higher-level wiring (NATS subscription, ack routing) is handled by the
//! caller — the orchestrator itself receives acks through an
//! [`tokio::sync::mpsc`] channel, which keeps the type independent of the
//! `nats-transport` feature and trivially mockable in tests.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tracing::{debug, info, warn};
use varpulis_runtime::persistence::{StateStore, StoreError};

use crate::checkpoint_protocol::{
    CheckpointAbortNotification, CheckpointBarrierAck, CheckpointBarrierRequest,
    CheckpointCompleteNotification, CheckpointId, CoordinatorCheckpointTracker,
    DistributedCheckpoint, GroupId, ProtocolError,
};
use crate::worker::WorkerId;

/// Default ack collection timeout. Workers self-abort using the same budget,
/// so 30s is generous for an in-region cluster while still detecting a
/// stuck pipeline before the next checkpoint cycle starts.
pub const DEFAULT_ACK_TIMEOUT: Duration = Duration::from_secs(30);

/// Default key prefix used when persisting [`DistributedCheckpoint`]s in the
/// state store. Resolved into `{prefix}/{group_id}/{checkpoint_id}.json`.
pub const DEFAULT_STATE_STORE_PREFIX: &str = "distributed_checkpoints";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for a per-group distributed checkpoint orchestrator.
#[derive(Debug, Clone)]
pub struct DistributedCheckpointConfig {
    /// How long to wait for every participant to ack before aborting.
    pub ack_timeout: Duration,
    /// Key prefix used when persisting assembled checkpoints to the state
    /// store (e.g. `"distributed_checkpoints"`).
    pub state_store_prefix: String,
    /// Initial checkpoint id. The orchestrator allocates ids monotonically
    /// starting at this value.
    pub starting_checkpoint_id: CheckpointId,
}

impl Default for DistributedCheckpointConfig {
    fn default() -> Self {
        Self {
            ack_timeout: DEFAULT_ACK_TIMEOUT,
            state_store_prefix: DEFAULT_STATE_STORE_PREFIX.to_string(),
            starting_checkpoint_id: 1,
        }
    }
}

// ---------------------------------------------------------------------------
// Transport / replicator traits
// ---------------------------------------------------------------------------

/// Sends barriers and broadcasts complete/abort notifications to workers.
///
/// In production this is implemented over NATS (Task 1.5); the trait keeps
/// the orchestrator testable without a NATS dependency.
pub trait CheckpointTransport: Send + Sync + 'static {
    /// Send a barrier request to a single worker. Failures are recorded as
    /// participant failures and turn the checkpoint into an abort.
    fn send_barrier(
        &self,
        worker_id: &WorkerId,
        request: &CheckpointBarrierRequest,
    ) -> impl std::future::Future<Output = Result<(), TransportError>> + Send;

    /// Broadcast that a checkpoint is durable to every worker in the group.
    fn broadcast_complete(
        &self,
        group_id: &str,
        notification: &CheckpointCompleteNotification,
    ) -> impl std::future::Future<Output = Result<(), TransportError>> + Send;

    /// Broadcast that a checkpoint is aborted to every worker in the group.
    fn broadcast_abort(
        &self,
        group_id: &str,
        notification: &CheckpointAbortNotification,
    ) -> impl std::future::Future<Output = Result<(), TransportError>> + Send;
}

/// Replicates checkpoint outcomes through Raft so every coordinator sees the
/// latest durable checkpoint id for the group.
///
/// Task 1.4 introduces concrete `ClusterCommand::CheckpointCompleted` /
/// `CheckpointAborted` variants and the matching state-machine apply logic;
/// the trait keeps the orchestrator independent of that wiring.
pub trait CheckpointRaftReplicator: Send + Sync + 'static {
    /// Replicate a successful checkpoint.
    fn replicate_completed(
        &self,
        group_id: &str,
        checkpoint_id: CheckpointId,
    ) -> impl std::future::Future<Output = Result<(), ReplicateError>> + Send;

    /// Replicate an aborted checkpoint.
    fn replicate_aborted(
        &self,
        group_id: &str,
        checkpoint_id: CheckpointId,
        reason: &str,
    ) -> impl std::future::Future<Output = Result<(), ReplicateError>> + Send;
}

/// Stand-in replicator used in single-coordinator deployments that do not
/// run Raft. Logs at debug level and reports success.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopRaftReplicator;

impl CheckpointRaftReplicator for NoopRaftReplicator {
    async fn replicate_completed(
        &self,
        group_id: &str,
        checkpoint_id: CheckpointId,
    ) -> Result<(), ReplicateError> {
        debug!(
            group_id,
            checkpoint_id, "no-op replicator: checkpoint completed"
        );
        Ok(())
    }

    async fn replicate_aborted(
        &self,
        group_id: &str,
        checkpoint_id: CheckpointId,
        reason: &str,
    ) -> Result<(), ReplicateError> {
        debug!(
            group_id,
            checkpoint_id, reason, "no-op replicator: checkpoint aborted"
        );
        Ok(())
    }
}

/// Raft-backed replicator that submits checkpoint outcomes through
/// [`crate::raft::VarpulisRaft::client_write`].
///
/// Only available when both the `raft` and `distributed-checkpoint` features
/// are enabled. Each call writes a [`crate::raft::ClusterCommand::CheckpointCompleted`]
/// or [`crate::raft::ClusterCommand::CheckpointAborted`] entry that the
/// state machine applies into [`crate::raft::state_machine::CoordinatorState::latest_checkpoints`].
///
/// Non-leader writes return a `ReplicateError` whose message carries the
/// `ForwardToLeader` payload — callers should typically resolve a leader and
/// retry, mirroring the pattern used by [`crate::coordinator::Coordinator::raft_replicate`].
#[cfg(feature = "raft")]
#[derive(Clone)]
pub struct RaftCheckpointReplicator {
    raft: Arc<crate::raft::VarpulisRaft>,
}

#[cfg(feature = "raft")]
impl RaftCheckpointReplicator {
    /// Build a new replicator backed by the supplied Raft handle.
    pub fn new(raft: Arc<crate::raft::VarpulisRaft>) -> Self {
        Self { raft }
    }
}

#[cfg(feature = "raft")]
impl std::fmt::Debug for RaftCheckpointReplicator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftCheckpointReplicator")
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "raft")]
impl CheckpointRaftReplicator for RaftCheckpointReplicator {
    async fn replicate_completed(
        &self,
        group_id: &str,
        checkpoint_id: CheckpointId,
    ) -> Result<(), ReplicateError> {
        let cmd = crate::raft::ClusterCommand::CheckpointCompleted {
            group_id: group_id.to_string(),
            checkpoint_id,
        };
        self.raft
            .client_write(cmd)
            .await
            .map(|_| ())
            .map_err(|e| ReplicateError::new(format!("client_write completed: {e}")))
    }

    async fn replicate_aborted(
        &self,
        group_id: &str,
        checkpoint_id: CheckpointId,
        reason: &str,
    ) -> Result<(), ReplicateError> {
        let cmd = crate::raft::ClusterCommand::CheckpointAborted {
            group_id: group_id.to_string(),
            checkpoint_id,
            reason: reason.to_string(),
        };
        self.raft
            .client_write(cmd)
            .await
            .map(|_| ())
            .map_err(|e| ReplicateError::new(format!("client_write aborted: {e}")))
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors raised while driving a distributed checkpoint cycle.
#[derive(Debug, thiserror::Error)]
pub enum CoordinatorError {
    /// A transport call failed (barrier dispatch or broadcast).
    #[error("transport: {0}")]
    Transport(#[from] TransportError),
    /// Persisting the assembled checkpoint to the state store failed.
    #[error("state store: {0}")]
    StateStore(String),
    /// The protocol state machine rejected a transition. Usually indicates a
    /// programming error in the orchestrator itself.
    #[error("protocol: {0}")]
    Protocol(#[from] ProtocolError),
    /// Replicating the outcome through Raft failed.
    #[error("raft replicate: {0}")]
    RaftReplicate(#[from] ReplicateError),
    /// Checkpoint serialization failed.
    #[error("serialize: {0}")]
    Serialize(String),
    /// `run_checkpoint` was called with no participants.
    #[error("no participants for group {0}")]
    NoParticipants(GroupId),
}

impl From<StoreError> for CoordinatorError {
    fn from(e: StoreError) -> Self {
        Self::StateStore(e.to_string())
    }
}

/// Transport-layer error surfaced to the orchestrator.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct TransportError(pub String);

impl TransportError {
    /// Construct a transport error from any displayable cause.
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

/// Raft replication error surfaced to the orchestrator.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ReplicateError(pub String);

impl ReplicateError {
    /// Construct a replicate error from any displayable cause.
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

// ---------------------------------------------------------------------------
// Outcome
// ---------------------------------------------------------------------------

/// Final outcome of a single distributed checkpoint cycle.
#[derive(Debug, Clone)]
pub enum CheckpointOutcome {
    /// Checkpoint is durable. `store_key` points at the assembled snapshot
    /// in the state store.
    Completed {
        /// Id of the checkpoint that was just persisted.
        checkpoint_id: CheckpointId,
        /// State store key under which the assembled snapshot lives.
        store_key: String,
    },
    /// Checkpoint was aborted (timeout, NACK, persistence failure, …).
    Aborted {
        /// Id of the aborted checkpoint.
        checkpoint_id: CheckpointId,
        /// Recorded reason — reused as the `reason` field of the
        /// [`CheckpointAbortNotification`] sent to workers.
        reason: String,
    },
}

impl CheckpointOutcome {
    /// Returns `true` if the checkpoint completed durably.
    pub const fn is_completed(&self) -> bool {
        matches!(self, Self::Completed { .. })
    }

    /// The checkpoint id, regardless of outcome.
    pub const fn checkpoint_id(&self) -> CheckpointId {
        match self {
            Self::Completed { checkpoint_id, .. } | Self::Aborted { checkpoint_id, .. } => {
                *checkpoint_id
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Orchestrator
// ---------------------------------------------------------------------------

/// Per-group orchestrator that drives one distributed checkpoint cycle at a
/// time.
///
/// Construct one instance per exactly-once pipeline group. To run a cycle:
///
/// 1. Build an [`mpsc::UnboundedReceiver<CheckpointBarrierAck>`] and route
///    NATS-side acks into the matching sender.
/// 2. Call [`DistributedCheckpointCoordinator::run_checkpoint`] with the
///    list of `(worker, pipeline)` participants.
///
/// The future resolves with a [`CheckpointOutcome`] once the checkpoint is
/// durable or aborted. Concurrent calls per orchestrator are not supported
/// — schedule them sequentially.
pub struct DistributedCheckpointCoordinator<T, R>
where
    T: CheckpointTransport,
    R: CheckpointRaftReplicator,
{
    group_id: GroupId,
    config: DistributedCheckpointConfig,
    transport: Arc<T>,
    state_store: Arc<dyn StateStore>,
    raft: Arc<R>,
    next_checkpoint_id: AtomicU64,
}

impl<T, R> std::fmt::Debug for DistributedCheckpointCoordinator<T, R>
where
    T: CheckpointTransport,
    R: CheckpointRaftReplicator,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedCheckpointCoordinator")
            .field("group_id", &self.group_id)
            .field("config", &self.config)
            .field(
                "next_checkpoint_id",
                &self.next_checkpoint_id.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

impl<T, R> DistributedCheckpointCoordinator<T, R>
where
    T: CheckpointTransport,
    R: CheckpointRaftReplicator,
{
    /// Build a new orchestrator for `group_id`.
    pub fn new(
        group_id: impl Into<GroupId>,
        config: DistributedCheckpointConfig,
        transport: Arc<T>,
        state_store: Arc<dyn StateStore>,
        raft: Arc<R>,
    ) -> Self {
        let starting_id = config.starting_checkpoint_id;
        Self {
            group_id: group_id.into(),
            config,
            transport,
            state_store,
            raft,
            next_checkpoint_id: AtomicU64::new(starting_id),
        }
    }

    /// Pipeline group this orchestrator manages.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Peek the next checkpoint id without consuming it.
    pub fn peek_next_checkpoint_id(&self) -> CheckpointId {
        self.next_checkpoint_id.load(Ordering::Acquire)
    }

    /// Build the state store key for a given checkpoint id.
    pub fn store_key_for(&self, checkpoint_id: CheckpointId) -> String {
        format!(
            "{}/{}/{}.json",
            self.config.state_store_prefix, self.group_id, checkpoint_id
        )
    }

    /// Drive one full distributed checkpoint cycle.
    ///
    /// `participants` lists every `(worker_id, pipeline_id)` that holds part
    /// of the group's state at this point in time. The orchestrator
    /// dispatches a barrier to each, waits for acks via `ack_rx`, persists
    /// the assembled checkpoint, then broadcasts completion or abort.
    ///
    /// `ack_rx` is consumed by the cycle: drop it after the cycle returns
    /// or use a fresh channel for the next call.
    pub async fn run_checkpoint(
        &self,
        participants: Vec<(WorkerId, String)>,
        mut ack_rx: mpsc::UnboundedReceiver<CheckpointBarrierAck>,
    ) -> Result<CheckpointOutcome, CoordinatorError> {
        if participants.is_empty() {
            return Err(CoordinatorError::NoParticipants(self.group_id.clone()));
        }

        let checkpoint_id = self.next_checkpoint_id.fetch_add(1, Ordering::SeqCst);
        let triggered_at_ms = chrono::Utc::now().timestamp_millis();
        info!(
            group_id = %self.group_id,
            checkpoint_id,
            participants = participants.len(),
            "starting distributed checkpoint"
        );

        let mut tracker = CoordinatorCheckpointTracker::new(
            self.group_id.clone(),
            checkpoint_id,
            participants.iter().cloned(),
            self.config.ack_timeout,
            triggered_at_ms,
        );

        // Move into BarrierSent before dispatch so that record_failure is a
        // legal transition if a barrier send fails synchronously.
        tracker.mark_barrier_sent()?;

        // Phase 1: dispatch barriers. A failed dispatch is recorded as a
        // participant failure but we still try the rest so we get a
        // complete picture of which workers are reachable.
        for (worker_id, pipeline_id) in &participants {
            let request = CheckpointBarrierRequest {
                group_id: self.group_id.clone(),
                checkpoint_id,
                pipeline_id: pipeline_id.clone(),
                timeout_ms: self.config.ack_timeout.as_millis() as u64,
                triggered_at_ms,
            };
            if let Err(e) = self.transport.send_barrier(worker_id, &request).await {
                warn!(
                    group_id = %self.group_id,
                    checkpoint_id,
                    %worker_id,
                    pipeline_id = %pipeline_id,
                    error = %e,
                    "barrier dispatch failed"
                );
                tracker.record_failure(
                    worker_id,
                    pipeline_id,
                    format!("barrier dispatch failed: {e}"),
                )?;
            }
        }

        // Phase 2: collect acks until all participants have responded or
        // the deadline expires. Each ack arriving from a stale checkpoint
        // is dropped silently — workers occasionally see late acks after
        // a previous cycle was aborted.
        let deadline = tokio::time::Instant::now() + self.config.ack_timeout;
        let collection = loop {
            if tracker.all_acks_received() {
                break Collection::AllReceived;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break Collection::TimedOut;
            }
            match tokio::time::timeout(remaining, ack_rx.recv()).await {
                Ok(Some(ack)) => {
                    if ack.checkpoint_id != checkpoint_id {
                        debug!(
                            group_id = %self.group_id,
                            expected = checkpoint_id,
                            actual = ack.checkpoint_id,
                            "ignoring ack for stale checkpoint id"
                        );
                        continue;
                    }
                    self.fold_ack_into_tracker(&mut tracker, ack);
                }
                Ok(None) => break Collection::ChannelClosed,
                Err(_) => break Collection::TimedOut,
            }
        };

        match collection {
            Collection::AllReceived if tracker.failed().is_empty() => {
                self.complete(tracker, checkpoint_id).await
            }
            Collection::AllReceived => {
                let reason = format_failure_reason(tracker.failed());
                self.abort(tracker, checkpoint_id, reason).await
            }
            Collection::TimedOut => {
                let reason = format!(
                    "ack timeout after {:?}, {} pending",
                    self.config.ack_timeout,
                    tracker.pending().len()
                );
                self.abort(tracker, checkpoint_id, reason).await
            }
            Collection::ChannelClosed => {
                let reason = "ack channel closed unexpectedly".to_string();
                self.abort(tracker, checkpoint_id, reason).await
            }
        }
    }

    fn fold_ack_into_tracker(
        &self,
        tracker: &mut CoordinatorCheckpointTracker,
        ack: CheckpointBarrierAck,
    ) {
        let CheckpointBarrierAck {
            worker_id,
            pipeline_id,
            location,
            error,
            ..
        } = ack;

        match (location, error) {
            (Some(loc), None) => {
                if let Err(e) = tracker.record_ack(&worker_id, &pipeline_id, loc) {
                    warn!(
                        group_id = %self.group_id,
                        %worker_id,
                        %pipeline_id,
                        error = %e,
                        "record_ack rejected"
                    );
                }
            }
            (None, Some(err)) => {
                if let Err(e) = tracker.record_failure(&worker_id, &pipeline_id, err) {
                    warn!(
                        group_id = %self.group_id,
                        %worker_id,
                        %pipeline_id,
                        error = %e,
                        "record_failure rejected"
                    );
                }
            }
            (Some(_), Some(_)) => {
                warn!(
                    group_id = %self.group_id,
                    %worker_id,
                    %pipeline_id,
                    "ack populated both location and error -- treating as failure"
                );
                if let Err(e) = tracker.record_failure(
                    &worker_id,
                    &pipeline_id,
                    "ack carried both location and error",
                ) {
                    warn!(error = %e, "record_failure rejected for protocol violation");
                }
            }
            (None, None) => {
                warn!(
                    group_id = %self.group_id,
                    %worker_id,
                    %pipeline_id,
                    "ack carried neither location nor error -- treating as failure"
                );
                if let Err(e) = tracker.record_failure(
                    &worker_id,
                    &pipeline_id,
                    "ack carried neither location nor error",
                ) {
                    warn!(error = %e, "record_failure rejected for empty ack");
                }
            }
        }
    }

    async fn complete(
        &self,
        mut tracker: CoordinatorCheckpointTracker,
        checkpoint_id: CheckpointId,
    ) -> Result<CheckpointOutcome, CoordinatorError> {
        tracker.mark_persisting()?;
        let assembled = tracker.mark_complete()?;

        // Persist before broadcasting -- workers must only commit after the
        // assembled checkpoint is durable.
        let store_key = self.persist_checkpoint(&assembled).await.map_err(|e| {
            warn!(
                group_id = %self.group_id,
                checkpoint_id,
                error = %e,
                "checkpoint persistence failed"
            );
            e
        })?;

        info!(
            group_id = %self.group_id,
            checkpoint_id,
            store_key = %store_key,
            participants = assembled.snapshots.len(),
            "checkpoint persisted, broadcasting completion"
        );

        // Broadcast completion. A broadcast failure is logged but does not
        // fail the cycle: the checkpoint is already durable and Raft
        // replication will inform the rest of the cluster. Workers also
        // self-recover on restart by reading the latest checkpoint id from
        // the replicated state.
        let committed_at_ms = chrono::Utc::now().timestamp_millis();
        let notification = CheckpointCompleteNotification {
            group_id: self.group_id.clone(),
            checkpoint_id,
            committed_at_ms,
        };
        if let Err(e) = self
            .transport
            .broadcast_complete(&self.group_id, &notification)
            .await
        {
            warn!(
                group_id = %self.group_id,
                checkpoint_id,
                error = %e,
                "broadcast_complete failed (checkpoint is durable)"
            );
        }

        self.raft
            .replicate_completed(&self.group_id, checkpoint_id)
            .await?;

        Ok(CheckpointOutcome::Completed {
            checkpoint_id,
            store_key,
        })
    }

    async fn abort(
        &self,
        mut tracker: CoordinatorCheckpointTracker,
        checkpoint_id: CheckpointId,
        reason: String,
    ) -> Result<CheckpointOutcome, CoordinatorError> {
        // Already-terminal trackers are ignored: the orchestrator never
        // double-aborts.
        if let Err(e) = tracker.mark_aborted(&reason) {
            debug!(error = %e, "mark_aborted on terminal tracker");
        }

        warn!(
            group_id = %self.group_id,
            checkpoint_id,
            reason = %reason,
            "checkpoint aborted"
        );

        let notification = CheckpointAbortNotification {
            group_id: self.group_id.clone(),
            checkpoint_id,
            reason: reason.clone(),
        };
        if let Err(e) = self
            .transport
            .broadcast_abort(&self.group_id, &notification)
            .await
        {
            warn!(
                group_id = %self.group_id,
                checkpoint_id,
                error = %e,
                "broadcast_abort failed"
            );
        }

        self.raft
            .replicate_aborted(&self.group_id, checkpoint_id, &reason)
            .await?;

        Ok(CheckpointOutcome::Aborted {
            checkpoint_id,
            reason,
        })
    }

    async fn persist_checkpoint(
        &self,
        assembled: &DistributedCheckpoint,
    ) -> Result<String, CoordinatorError> {
        let key = self.store_key_for(assembled.checkpoint_id);
        let bytes = serde_json::to_vec(assembled)
            .map_err(|e| CoordinatorError::Serialize(e.to_string()))?;
        // StateStore::put is sync; offload to a blocking task so the runtime
        // does not stall on potentially-slow backends (S3, RocksDB).
        let store = self.state_store.clone();
        let key_for_task = key.clone();
        tokio::task::spawn_blocking(move || store.put(&key_for_task, &bytes))
            .await
            .map_err(|e| CoordinatorError::StateStore(format!("spawn_blocking: {e}")))??;
        Ok(key)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum Collection {
    AllReceived,
    TimedOut,
    ChannelClosed,
}

fn format_failure_reason(failed: &std::collections::HashMap<String, String>) -> String {
    let mut entries: Vec<&String> = failed.keys().collect();
    entries.sort_unstable();
    let preview: Vec<String> = entries
        .into_iter()
        .take(4)
        .map(|k| format!("{k}: {}", failed[k]))
        .collect();
    if failed.len() > preview.len() {
        format!(
            "{} participants failed; first few: [{}]",
            failed.len(),
            preview.join("; ")
        )
    } else {
        format!(
            "{} participants failed: [{}]",
            failed.len(),
            preview.join("; ")
        )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    use varpulis_runtime::persistence::MemoryStore;

    use super::*;
    use crate::checkpoint_protocol::{CheckpointBarrierAck, SnapshotLocation};

    // ----------------------------------------------------------------------
    // Test doubles
    // ----------------------------------------------------------------------

    /// Records every transport call so tests can assert on them. Lets the
    /// test inject errors per-method by name.
    #[derive(Default)]
    struct MockTransport {
        sent_barriers: StdMutex<Vec<(WorkerId, CheckpointBarrierRequest)>>,
        completes: StdMutex<Vec<CheckpointCompleteNotification>>,
        aborts: StdMutex<Vec<CheckpointAbortNotification>>,
        fail_send: StdMutex<Option<String>>,
        fail_complete: StdMutex<Option<String>>,
        fail_abort: StdMutex<Option<String>>,
    }

    impl MockTransport {
        fn with_send_error(self, msg: &str) -> Self {
            *self.fail_send.lock().unwrap() = Some(msg.to_string());
            self
        }
    }

    impl CheckpointTransport for MockTransport {
        async fn send_barrier(
            &self,
            worker_id: &WorkerId,
            request: &CheckpointBarrierRequest,
        ) -> Result<(), TransportError> {
            let injected = self.fail_send.lock().unwrap().clone();
            if let Some(msg) = injected {
                return Err(TransportError::new(msg));
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
            let injected = self.fail_complete.lock().unwrap().clone();
            if let Some(msg) = injected {
                return Err(TransportError::new(msg));
            }
            self.completes.lock().unwrap().push(notification.clone());
            Ok(())
        }

        async fn broadcast_abort(
            &self,
            _group_id: &str,
            notification: &CheckpointAbortNotification,
        ) -> Result<(), TransportError> {
            let injected = self.fail_abort.lock().unwrap().clone();
            if let Some(msg) = injected {
                return Err(TransportError::new(msg));
            }
            self.aborts.lock().unwrap().push(notification.clone());
            Ok(())
        }
    }

    /// Records replicator calls and lets the test fail them on demand.
    #[derive(Default)]
    struct MockReplicator {
        completed: StdMutex<Vec<(String, CheckpointId)>>,
        aborted: StdMutex<Vec<(String, CheckpointId, String)>>,
        fail: StdMutex<Option<String>>,
    }

    impl CheckpointRaftReplicator for MockReplicator {
        async fn replicate_completed(
            &self,
            group_id: &str,
            checkpoint_id: CheckpointId,
        ) -> Result<(), ReplicateError> {
            let injected = self.fail.lock().unwrap().clone();
            if let Some(msg) = injected {
                return Err(ReplicateError::new(msg));
            }
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
            let injected = self.fail.lock().unwrap().clone();
            if let Some(msg) = injected {
                return Err(ReplicateError::new(msg));
            }
            self.aborted.lock().unwrap().push((
                group_id.to_string(),
                checkpoint_id,
                reason.to_string(),
            ));
            Ok(())
        }
    }

    fn worker(id: &str) -> WorkerId {
        WorkerId(id.into())
    }

    fn empty_checkpoint_loc() -> SnapshotLocation {
        SnapshotLocation::Inline {
            checkpoint: Box::new(varpulis_runtime::persistence::EngineCheckpoint {
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
            }),
        }
    }

    fn build_orchestrator(
        config: DistributedCheckpointConfig,
    ) -> (
        DistributedCheckpointCoordinator<MockTransport, MockReplicator>,
        Arc<MockTransport>,
        Arc<MemoryStore>,
        Arc<MockReplicator>,
    ) {
        let transport = Arc::new(MockTransport::default());
        let store: Arc<MemoryStore> = Arc::new(MemoryStore::new());
        let raft = Arc::new(MockReplicator::default());
        let orchestrator = DistributedCheckpointCoordinator::new(
            "g1",
            config,
            transport.clone(),
            store.clone() as Arc<dyn StateStore>,
            raft.clone(),
        );
        (orchestrator, transport, store, raft)
    }

    // ----------------------------------------------------------------------
    // Tests
    // ----------------------------------------------------------------------

    #[tokio::test]
    async fn happy_path_persists_and_broadcasts_complete() {
        let (orchestrator, transport, store, raft) =
            build_orchestrator(DistributedCheckpointConfig::default());

        let participants = vec![
            (worker("w0"), "p1".to_string()),
            (worker("w1"), "p1".to_string()),
        ];
        let (ack_tx, ack_rx) = mpsc::unbounded_channel();

        // Drive ack delivery from a separate task so run_checkpoint actually
        // awaits the receiver.
        let group_id = orchestrator.group_id().to_string();
        let next_id = orchestrator.peek_next_checkpoint_id();
        tokio::spawn(async move {
            // Brief pause so the orchestrator has time to dispatch barriers.
            tokio::time::sleep(Duration::from_millis(5)).await;
            ack_tx
                .send(CheckpointBarrierAck::success(
                    group_id.clone(),
                    next_id,
                    worker("w0"),
                    "p1".into(),
                    empty_checkpoint_loc(),
                ))
                .unwrap();
            ack_tx
                .send(CheckpointBarrierAck::success(
                    group_id,
                    next_id,
                    worker("w1"),
                    "p1".into(),
                    empty_checkpoint_loc(),
                ))
                .unwrap();
        });

        let outcome = orchestrator
            .run_checkpoint(participants, ack_rx)
            .await
            .expect("run_checkpoint should succeed");

        match outcome {
            CheckpointOutcome::Completed {
                checkpoint_id,
                store_key,
            } => {
                assert_eq!(checkpoint_id, next_id);
                // State store should now hold the assembled checkpoint.
                let bytes = store
                    .get(&store_key)
                    .unwrap()
                    .expect("checkpoint persisted");
                let assembled: DistributedCheckpoint = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(assembled.checkpoint_id, checkpoint_id);
                assert_eq!(assembled.snapshots.len(), 2);
            }
            other => panic!("expected Completed, got {:?}", other),
        }

        // Two barrier requests went out.
        assert_eq!(transport.sent_barriers.lock().unwrap().len(), 2);
        // Exactly one complete broadcast.
        assert_eq!(transport.completes.lock().unwrap().len(), 1);
        assert_eq!(transport.aborts.lock().unwrap().len(), 0);
        // Raft saw the completion.
        assert_eq!(raft.completed.lock().unwrap().len(), 1);
        assert!(raft.aborted.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn timeout_aborts_and_replicates() {
        let config = DistributedCheckpointConfig {
            ack_timeout: Duration::from_millis(20),
            ..Default::default()
        };
        let (orchestrator, transport, _store, raft) = build_orchestrator(config);

        let participants = vec![(worker("w0"), "p1".to_string())];
        let (_ack_tx, ack_rx) = mpsc::unbounded_channel();
        // No acks sent -- timeout fires.

        let outcome = orchestrator
            .run_checkpoint(participants, ack_rx)
            .await
            .expect("abort path returns Ok");

        match outcome {
            CheckpointOutcome::Aborted {
                checkpoint_id,
                reason,
            } => {
                assert!(reason.contains("ack timeout"));
                assert_eq!(checkpoint_id, 1);
            }
            other => panic!("expected Aborted, got {:?}", other),
        }

        // Abort broadcast went out, no complete broadcast.
        assert_eq!(transport.aborts.lock().unwrap().len(), 1);
        assert_eq!(transport.completes.lock().unwrap().len(), 0);
        // Raft saw the abort.
        assert_eq!(raft.aborted.lock().unwrap().len(), 1);
        assert!(raft.completed.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn nack_aborts_and_replicates() {
        let (orchestrator, transport, _store, raft) =
            build_orchestrator(DistributedCheckpointConfig::default());

        let participants = vec![
            (worker("w0"), "p1".to_string()),
            (worker("w1"), "p1".to_string()),
        ];
        let (ack_tx, ack_rx) = mpsc::unbounded_channel();
        let group_id = orchestrator.group_id().to_string();
        let next_id = orchestrator.peek_next_checkpoint_id();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            ack_tx
                .send(CheckpointBarrierAck::success(
                    group_id.clone(),
                    next_id,
                    worker("w0"),
                    "p1".into(),
                    empty_checkpoint_loc(),
                ))
                .unwrap();
            ack_tx
                .send(CheckpointBarrierAck::failure(
                    group_id,
                    next_id,
                    worker("w1"),
                    "p1".into(),
                    "kafka producer broken",
                ))
                .unwrap();
        });

        let outcome = orchestrator
            .run_checkpoint(participants, ack_rx)
            .await
            .expect("abort path returns Ok");

        match outcome {
            CheckpointOutcome::Aborted { reason, .. } => {
                assert!(reason.contains("participants failed"));
            }
            other => panic!("expected Aborted, got {:?}", other),
        }
        assert_eq!(transport.aborts.lock().unwrap().len(), 1);
        assert_eq!(raft.aborted.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn barrier_dispatch_failure_is_recorded_as_participant_failure() {
        let transport = Arc::new(MockTransport::default().with_send_error("nats unreachable"));
        let store: Arc<MemoryStore> = Arc::new(MemoryStore::new());
        let raft = Arc::new(MockReplicator::default());
        let orchestrator = DistributedCheckpointCoordinator::new(
            "g1",
            DistributedCheckpointConfig::default(),
            transport.clone(),
            store as Arc<dyn StateStore>,
            raft.clone(),
        );

        let participants = vec![(worker("w0"), "p1".to_string())];
        let (_ack_tx, ack_rx) = mpsc::unbounded_channel();

        let outcome = orchestrator
            .run_checkpoint(participants, ack_rx)
            .await
            .expect("abort path returns Ok");

        match outcome {
            CheckpointOutcome::Aborted { reason, .. } => {
                assert!(reason.contains("participants failed"));
            }
            other => panic!("expected Aborted, got {:?}", other),
        }
        // No barriers actually recorded; immediate abort.
        assert!(transport.sent_barriers.lock().unwrap().is_empty());
        assert_eq!(transport.aborts.lock().unwrap().len(), 1);
        assert_eq!(raft.aborted.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn empty_participants_rejected() {
        let (orchestrator, _t, _s, _r) = build_orchestrator(DistributedCheckpointConfig::default());
        let (_ack_tx, ack_rx) = mpsc::unbounded_channel();
        let err = orchestrator
            .run_checkpoint(vec![], ack_rx)
            .await
            .unwrap_err();
        assert!(matches!(err, CoordinatorError::NoParticipants(_)));
    }

    #[tokio::test]
    async fn stale_acks_are_dropped() {
        let config = DistributedCheckpointConfig {
            ack_timeout: Duration::from_millis(50),
            ..Default::default()
        };
        let (orchestrator, _t, _s, _r) = build_orchestrator(config);

        let participants = vec![(worker("w0"), "p1".to_string())];
        let (ack_tx, ack_rx) = mpsc::unbounded_channel();
        let group_id = orchestrator.group_id().to_string();
        tokio::spawn(async move {
            // Send an ack with a wildly different checkpoint id -- should be ignored.
            tokio::time::sleep(Duration::from_millis(2)).await;
            ack_tx
                .send(CheckpointBarrierAck::success(
                    group_id,
                    99,
                    worker("w0"),
                    "p1".into(),
                    empty_checkpoint_loc(),
                ))
                .unwrap();
        });

        let outcome = orchestrator
            .run_checkpoint(participants, ack_rx)
            .await
            .unwrap();

        // Stale ack ignored, real ack never arrives -- should time out.
        assert!(matches!(outcome, CheckpointOutcome::Aborted { .. }));
    }

    #[tokio::test]
    async fn checkpoint_id_increments_per_run() {
        let (orchestrator, _t, _s, _r) = build_orchestrator(DistributedCheckpointConfig {
            ack_timeout: Duration::from_millis(15),
            ..Default::default()
        });
        // Two consecutive timeout-aborted runs -- ids should be 1 and 2.
        let (_ack_tx, ack_rx1) = mpsc::unbounded_channel();
        let outcome1 = orchestrator
            .run_checkpoint(vec![(worker("w0"), "p1".into())], ack_rx1)
            .await
            .unwrap();
        assert_eq!(outcome1.checkpoint_id(), 1);
        let (_ack_tx, ack_rx2) = mpsc::unbounded_channel();
        let outcome2 = orchestrator
            .run_checkpoint(vec![(worker("w0"), "p1".into())], ack_rx2)
            .await
            .unwrap();
        assert_eq!(outcome2.checkpoint_id(), 2);
    }

    #[tokio::test]
    async fn raft_failure_propagates_on_complete() {
        let transport = Arc::new(MockTransport::default());
        let store: Arc<MemoryStore> = Arc::new(MemoryStore::new());
        let raft = Arc::new(MockReplicator::default());
        *raft.fail.lock().unwrap() = Some("not leader".into());
        let orchestrator = DistributedCheckpointCoordinator::new(
            "g1",
            DistributedCheckpointConfig::default(),
            transport.clone(),
            store as Arc<dyn StateStore>,
            raft.clone(),
        );

        let participants = vec![(worker("w0"), "p1".to_string())];
        let (ack_tx, ack_rx) = mpsc::unbounded_channel();
        let group_id = orchestrator.group_id().to_string();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(2)).await;
            ack_tx
                .send(CheckpointBarrierAck::success(
                    group_id,
                    1,
                    worker("w0"),
                    "p1".into(),
                    empty_checkpoint_loc(),
                ))
                .unwrap();
        });

        let err = orchestrator
            .run_checkpoint(participants, ack_rx)
            .await
            .unwrap_err();
        assert!(matches!(err, CoordinatorError::RaftReplicate(_)));
        // Checkpoint did persist before the raft failure.
        assert_eq!(transport.completes.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn noop_replicator_returns_ok() {
        let r = NoopRaftReplicator;
        assert!(r.replicate_completed("g", 1).await.is_ok());
        assert!(r.replicate_aborted("g", 1, "x").await.is_ok());
    }

    #[test]
    fn store_key_format_is_pinned() {
        let (orchestrator, _t, _s, _r) = build_orchestrator(DistributedCheckpointConfig::default());
        assert_eq!(
            orchestrator.store_key_for(42),
            "distributed_checkpoints/g1/42.json"
        );
    }

    #[test]
    fn config_defaults_30s_timeout() {
        let cfg = DistributedCheckpointConfig::default();
        assert_eq!(cfg.ack_timeout, Duration::from_secs(30));
        assert_eq!(cfg.state_store_prefix, "distributed_checkpoints");
        assert_eq!(cfg.starting_checkpoint_id, 1);
    }

    #[test]
    fn outcome_is_completed_helper() {
        let c = CheckpointOutcome::Completed {
            checkpoint_id: 7,
            store_key: "k".into(),
        };
        assert!(c.is_completed());
        assert_eq!(c.checkpoint_id(), 7);
        let a = CheckpointOutcome::Aborted {
            checkpoint_id: 8,
            reason: "r".into(),
        };
        assert!(!a.is_completed());
        assert_eq!(a.checkpoint_id(), 8);
    }
}
