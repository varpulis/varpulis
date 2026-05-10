//! Pipeline migration types and status tracking.

use std::time::Instant;

use serde::{Deserialize, Serialize};
use varpulis_runtime::engine::Engine;
use varpulis_runtime::persistence::EngineCheckpoint;

use crate::worker::WorkerId;

/// Status of a pipeline migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationStatus {
    /// Extracting state from source worker
    Checkpointing,
    /// Deploying pipeline to target worker
    Deploying,
    /// Restoring state on target worker
    Restoring,
    /// Updating routing to point to target
    Switching,
    /// Removing pipeline from source worker
    CleaningUp,
    /// Migration completed successfully
    Completed,
    /// Migration failed
    Failed(String),
}

impl std::fmt::Display for MigrationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Checkpointing => write!(f, "checkpointing"),
            Self::Deploying => write!(f, "deploying"),
            Self::Restoring => write!(f, "restoring"),
            Self::Switching => write!(f, "switching"),
            Self::CleaningUp => write!(f, "cleaning_up"),
            Self::Completed => write!(f, "completed"),
            Self::Failed(msg) => write!(f, "failed: {}", msg),
        }
    }
}

/// Reason for initiating a migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationReason {
    /// Source worker died
    Failover,
    /// Load rebalancing
    Rebalance,
    /// Source worker is draining
    Drain,
    /// Operator-initiated
    Manual,
    /// Connector lost connectivity
    ConnectorFailure,
}

impl std::fmt::Display for MigrationReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Failover => write!(f, "failover"),
            Self::Rebalance => write!(f, "rebalance"),
            Self::Drain => write!(f, "drain"),
            Self::Manual => write!(f, "manual"),
            Self::ConnectorFailure => write!(f, "connector_failure"),
        }
    }
}

/// Tracks an in-progress pipeline migration.
#[derive(Debug, Clone)]
pub struct MigrationTask {
    pub id: String,
    pub pipeline_name: String,
    pub group_id: String,
    pub source_worker: WorkerId,
    pub target_worker: WorkerId,
    pub status: MigrationStatus,
    pub started_at: Instant,
    pub checkpoint: Option<EngineCheckpoint>,
    pub reason: MigrationReason,
}

/// Re-issue `commit_sinks` on a freshly-restored engine when the persisted
/// checkpoint is newer than what the engine has already committed.
///
/// ## When this fires
///
/// The distributed checkpoint protocol marks a checkpoint as durable
/// (Raft-replicated) once every participating worker has acked the
/// `prepare_commit` phase. The follow-up `CheckpointCompleteNotification`
/// then triggers `commit_sinks` on each worker, which finalises the prepared
/// 2PC transaction (e.g. Kafka `commit_transaction`).
///
/// If a worker crashes — or a pipeline is migrated — between *prepare* and
/// *commit*, the prepared transaction outlives the engine that opened it.
/// On recovery the new engine instance restores the same checkpoint state
/// but its in-memory [`Engine::last_committed_epoch`] starts at `0`. This
/// helper detects that gap and replays `commit_sinks(checkpoint_id)` so the
/// prepared transaction is finalised.
///
/// ## Idempotence
///
/// Kafka 2PC commits are idempotent on the same `transactional.id` — calling
/// `commit_transaction` against an already-committed transaction returns
/// `ErrorCode::None`. Likewise, a sink that has no prepared transaction at
/// `checkpoint_id` will treat the call as a no-op. So this helper is safe
/// to call unconditionally after any restore, even when the original engine
/// did manage to commit before crashing.
///
/// ## Returns
///
/// `true` if `commit_sinks` was issued, `false` if the engine had already
/// committed at or beyond `checkpoint_id` (e.g. when the helper is invoked
/// twice for the same checkpoint).
pub async fn recovery_commit(engine: &Engine, checkpoint_id: u64) -> bool {
    if checkpoint_id > engine.last_committed_epoch() {
        engine.commit_sinks(checkpoint_id).await;
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migration_status_display() {
        assert_eq!(MigrationStatus::Checkpointing.to_string(), "checkpointing");
        assert_eq!(MigrationStatus::Deploying.to_string(), "deploying");
        assert_eq!(MigrationStatus::Restoring.to_string(), "restoring");
        assert_eq!(MigrationStatus::Switching.to_string(), "switching");
        assert_eq!(MigrationStatus::CleaningUp.to_string(), "cleaning_up");
        assert_eq!(MigrationStatus::Completed.to_string(), "completed");
        assert_eq!(
            MigrationStatus::Failed("timeout".into()).to_string(),
            "failed: timeout"
        );
    }

    #[test]
    fn test_migration_reason_display() {
        assert_eq!(MigrationReason::Failover.to_string(), "failover");
        assert_eq!(MigrationReason::Rebalance.to_string(), "rebalance");
        assert_eq!(MigrationReason::Drain.to_string(), "drain");
        assert_eq!(MigrationReason::Manual.to_string(), "manual");
        assert_eq!(
            MigrationReason::ConnectorFailure.to_string(),
            "connector_failure"
        );
    }

    #[test]
    fn test_migration_status_serde() {
        for status in [
            MigrationStatus::Checkpointing,
            MigrationStatus::Deploying,
            MigrationStatus::Restoring,
            MigrationStatus::Switching,
            MigrationStatus::CleaningUp,
            MigrationStatus::Completed,
            MigrationStatus::Failed("err".into()),
        ] {
            let json = serde_json::to_string(&status).unwrap();
            let parsed: MigrationStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, status);
        }
    }

    #[test]
    fn test_migration_reason_serde() {
        for reason in [
            MigrationReason::Failover,
            MigrationReason::Rebalance,
            MigrationReason::Drain,
            MigrationReason::Manual,
            MigrationReason::ConnectorFailure,
        ] {
            let json = serde_json::to_string(&reason).unwrap();
            let _parsed: MigrationReason = serde_json::from_str(&json).unwrap();
        }
    }

    // ---------------------------------------------------------------------
    // recovery_commit
    // ---------------------------------------------------------------------

    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use varpulis_runtime::event::Event;
    use varpulis_runtime::sink::{Sink, SinkError};

    /// Records the 2PC method calls (`begin_epoch`, `prepare_commit`,
    /// `commit`, `abort`) issued against the sink so tests can assert that
    /// `recovery_commit` actually replays the prepared commit phase.
    #[derive(Debug, Default)]
    struct RecordingSink {
        calls: Mutex<Vec<(String, u64)>>,
    }

    impl RecordingSink {
        fn calls(&self) -> Vec<(String, u64)> {
            self.calls.lock().unwrap().clone()
        }

        fn record(&self, op: &str, id: u64) {
            self.calls.lock().unwrap().push((op.to_string(), id));
        }
    }

    #[async_trait]
    impl Sink for RecordingSink {
        fn name(&self) -> &str {
            "recording"
        }
        async fn send(&self, _event: &Event) -> Result<(), SinkError> {
            Ok(())
        }
        async fn flush(&self) -> Result<(), SinkError> {
            Ok(())
        }
        async fn close(&self) -> Result<(), SinkError> {
            Ok(())
        }
        fn supports_exactly_once(&self) -> bool {
            true
        }
        async fn begin_epoch(&self, id: u64) -> Result<(), SinkError> {
            self.record("begin_epoch", id);
            Ok(())
        }
        async fn prepare_commit(&self, id: u64) -> Result<(), SinkError> {
            self.record("prepare_commit", id);
            Ok(())
        }
        async fn commit(&self, id: u64) -> Result<(), SinkError> {
            self.record("commit", id);
            Ok(())
        }
        async fn abort(&self, id: u64) -> Result<(), SinkError> {
            self.record("abort", id);
            Ok(())
        }
    }

    fn build_engine_with_recording_sink() -> (Engine, Arc<RecordingSink>) {
        let (tx, _rx) = tokio::sync::mpsc::channel(16);
        let mut engine = Engine::new(tx);
        let sink = Arc::new(RecordingSink::default());
        engine.inject_sink("recording", sink.clone() as Arc<dyn Sink>);
        (engine, sink)
    }

    #[tokio::test]
    async fn recovery_commit_issues_commit_when_checkpoint_is_newer() {
        let (engine, sink) = build_engine_with_recording_sink();
        assert_eq!(engine.last_committed_epoch(), 0);

        let did_commit = recovery_commit(&engine, 5).await;

        assert!(
            did_commit,
            "recovery_commit should fire when the checkpoint id is newer than last_committed_epoch"
        );
        let calls = sink.calls();
        assert!(
            calls.contains(&("commit".to_string(), 5)),
            "expected commit(5) to be issued, got {:?}",
            calls
        );
        assert!(
            calls.contains(&("begin_epoch".to_string(), 6)),
            "commit_sinks should also open the next epoch (6), got {:?}",
            calls
        );
        assert_eq!(
            engine.last_committed_epoch(),
            5,
            "last_committed_epoch must advance after a recovery commit"
        );
    }

    #[tokio::test]
    async fn recovery_commit_is_noop_when_already_committed_at_or_beyond() {
        let (engine, sink) = build_engine_with_recording_sink();
        // Simulate a successful first recovery: engine already committed at 7.
        engine.set_last_committed_epoch(7);

        let did_commit_at_7 = recovery_commit(&engine, 7).await;
        assert!(
            !did_commit_at_7,
            "checkpoint_id == last_committed_epoch must be a no-op"
        );
        let did_commit_at_6 = recovery_commit(&engine, 6).await;
        assert!(
            !did_commit_at_6,
            "checkpoint_id < last_committed_epoch must be a no-op"
        );
        assert!(
            sink.calls().is_empty(),
            "no sink calls should be issued for a no-op recovery, got {:?}",
            sink.calls()
        );
    }

    #[tokio::test]
    async fn recovery_commit_is_idempotent_across_repeated_calls() {
        let (engine, sink) = build_engine_with_recording_sink();

        let first = recovery_commit(&engine, 9).await;
        let second = recovery_commit(&engine, 9).await;
        let third = recovery_commit(&engine, 9).await;

        assert!(first, "first recovery_commit should fire");
        assert!(
            !second,
            "second recovery_commit at the same id should be a no-op"
        );
        assert!(
            !third,
            "third recovery_commit at the same id should be a no-op"
        );

        let commit_count = sink
            .calls()
            .iter()
            .filter(|(op, id)| op == "commit" && *id == 9)
            .count();
        assert_eq!(
            commit_count, 1,
            "commit(9) should be issued exactly once across repeated recovery_commit calls"
        );
    }

    #[tokio::test]
    async fn commit_sinks_advances_last_committed_epoch_monotonically() {
        let (engine, _sink) = build_engine_with_recording_sink();

        engine.commit_sinks(3).await;
        assert_eq!(engine.last_committed_epoch(), 3);

        engine.commit_sinks(7).await;
        assert_eq!(engine.last_committed_epoch(), 7);

        // Out-of-order replay must not roll the marker backwards.
        engine.commit_sinks(2).await;
        assert_eq!(
            engine.last_committed_epoch(),
            7,
            "last_committed_epoch must be monotonic"
        );
    }
}
