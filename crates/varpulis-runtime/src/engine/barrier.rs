//! Two-phase-commit checkpoint barrier.
//!
//! Extracted from the CLI `run` loop so the exactly-once sequencing is drivable
//! in-process — by tests and embedders — without a broker or the CLI connector
//! registry. See `audit/EXACTLY_ONCE_DESIGN.md`.
//!
//! This first extract is deliberately behaviour-preserving: every failure is
//! still logged and swallowed exactly as the pre-extract CLI barrier did. The
//! `Result` return type and the [`SourceCommitCoordinator`] seam are the
//! surfaces the later audit-C3/C4 hardening steps build on.

use std::collections::HashMap;

use async_trait::async_trait;

use super::Engine;

/// Error surfaced by [`Engine::barrier_commit_2pc`].
///
/// The behaviour-preserving extract never returns these variants (the barrier
/// still logs-and-continues, returning `Ok`). They exist so the later hardening
/// steps can make the failing paths fatal without another signature change.
#[derive(Debug, thiserror::Error)]
pub enum BarrierError {
    /// A source consumer-group offset commit failed.
    #[error("source offset commit failed: {0}")]
    OffsetCommit(String),
    /// Persisting the state checkpoint failed.
    #[error("checkpoint persist failed: {0}")]
    Checkpoint(String),
    /// A transactional sink failed a two-phase-commit phase.
    #[error("sink 2PC failed: {0}")]
    Sink(String),
}

/// Seam the barrier uses to commit source progress.
///
/// The engine decides *which* offsets to commit (from the checkpoint snapshot)
/// and *where* (topic, resolved from the source binding); the coordinator
/// performs the actual commit. The CLI implements it over the managed connector
/// registry; tests implement it over an in-memory capture.
#[async_trait]
pub trait SourceCommitCoordinator: Send + Sync {
    /// Stage per-source offsets to be folded into the sink transaction (audit
    /// C4). Default no-op: non-transactional coordinators commit out-of-band via
    /// [`commit_offsets`](Self::commit_offsets).
    async fn stage_txn_offsets(
        &self,
        _connector: &str,
        _topic: &str,
        _offsets: &HashMap<i32, i64>,
    ) -> Result<(), BarrierError> {
        Ok(())
    }

    /// Commit source consumer-group offsets out-of-band (the current
    /// at-least-once path).
    async fn commit_offsets(
        &self,
        connector: &str,
        topic: &str,
        offsets: &HashMap<i32, i64>,
    ) -> Result<(), BarrierError>;
}

impl Engine {
    /// Run one full two-phase-commit checkpoint barrier: snapshot state, persist
    /// it, commit transactional sinks, then commit source offsets — as a single
    /// checkpoint epoch.
    ///
    /// Extracted from the CLI `run` loop (behaviour-preserving). All failures are
    /// logged and swallowed exactly as before; the `Result` return is the seam
    /// the later audit-C3/C4 steps use to make failing paths fatal. See the
    /// module docs.
    #[cfg(feature = "async-runtime")]
    pub async fn barrier_commit_2pc(
        &mut self,
        checkpoint_id: u64,
        coordinator: &dyn SourceCommitCoordinator,
    ) -> Result<(), BarrierError> {
        // Phase 1 — state snapshot (fast, in-memory).
        let snapshot = self.create_checkpoint();

        // Resolve the offset-commit targets up front (while we still hold the
        // shared borrows). Topic comes from the binding override or, failing
        // that, the connector config default.
        let bindings = self.source_bindings().to_vec();
        let commit_targets: Vec<(String, String, HashMap<i32, i64>)> = bindings
            .iter()
            .filter_map(|binding| {
                let offsets = snapshot.source_offsets.get(&binding.connector_name)?;
                if offsets.is_empty() {
                    return None;
                }
                let topic = binding
                    .topic_override
                    .as_deref()
                    .or_else(|| {
                        self.get_connector(&binding.connector_name)
                            .and_then(|config| config.topic.as_deref())
                    })
                    .unwrap_or("varpulis/events/#")
                    .to_string();
                Some((binding.connector_name.clone(), topic, offsets.clone()))
            })
            .collect();

        // Phase 1b — persist checkpoint to disk (if checkpointing is enabled).
        // This must happen BEFORE the sink commit so that on crash-recovery the
        // restored engine state is consistent with the last committed offset.
        if self.has_checkpointing() {
            if let Err(e) = self.force_checkpoint() {
                tracing::warn!("Checkpoint persist failed (epoch {checkpoint_id}): {e}");
            }
        }

        // Phase 2 — prepare commit on transactional sinks (flush producer queues).
        self.prepare_commit_sinks(checkpoint_id).await;

        // Phase 3 — commit transactional sinks. This is the point of no return:
        // once the Kafka `commit_transaction` succeeds, downstream consumers can
        // see the emitted events, and we MUST commit the corresponding source
        // offsets or a restart would double-emit.
        self.commit_sinks(checkpoint_id).await;

        // Phase 4 — commit source consumer-group offsets.
        for (connector, topic, offsets) in &commit_targets {
            if let Err(e) = coordinator.commit_offsets(connector, topic, offsets).await {
                tracing::warn!(
                    "Source offset commit failed (checkpoint {checkpoint_id}, connector {connector}): {e}"
                );
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::engine::SourceBinding;
    use crate::event::Event;
    use crate::sink::{Sink, SinkError};

    /// An exactly-once sink that records its 2PC lifecycle so a test can assert
    /// the barrier drove the phases (and their order).
    #[derive(Debug, Default)]
    struct TxnCaptureSink {
        lifecycle: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl Sink for TxnCaptureSink {
        fn name(&self) -> &str {
            "txn-capture"
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
            self.lifecycle.lock().unwrap().push(format!("begin:{id}"));
            Ok(())
        }
        async fn prepare_commit(&self, id: u64) -> Result<(), SinkError> {
            self.lifecycle.lock().unwrap().push(format!("prepare:{id}"));
            Ok(())
        }
        async fn commit(&self, id: u64) -> Result<(), SinkError> {
            self.lifecycle.lock().unwrap().push(format!("commit:{id}"));
            Ok(())
        }
        async fn abort(&self, id: u64) -> Result<(), SinkError> {
            self.lifecycle.lock().unwrap().push(format!("abort:{id}"));
            Ok(())
        }
    }

    /// One `(connector, topic, offsets)` commit the barrier asked for.
    type OffsetCommit = (String, String, HashMap<i32, i64>);

    /// Records every offset commit the barrier asks for.
    #[derive(Default)]
    struct CaptureCoordinator {
        committed: Mutex<Vec<OffsetCommit>>,
    }

    #[async_trait]
    impl SourceCommitCoordinator for CaptureCoordinator {
        async fn commit_offsets(
            &self,
            connector: &str,
            topic: &str,
            offsets: &HashMap<i32, i64>,
        ) -> Result<(), BarrierError> {
            self.committed.lock().unwrap().push((
                connector.to_string(),
                topic.to_string(),
                offsets.clone(),
            ));
            Ok(())
        }
    }

    /// The extracted barrier drives the transactional sink through
    /// prepare→commit (in that order) and commits the snapshot's source offsets
    /// via the coordinator. Fail-before check: deleting `commit_sinks` drops
    /// `commit:1`; deleting the Phase-4 loop leaves the coordinator empty.
    #[tokio::test]
    async fn barrier_drives_2pc_lifecycle_and_commits_source_offsets() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<Event>(16);
        let mut engine = Engine::new(tx);

        // A transactional sink, epoch 1 already open (as run.rs opens it at startup).
        let sink = Arc::new(TxnCaptureSink::default());
        engine.inject_sink("out", sink.clone());
        engine.begin_epoch_sinks(1).await;

        // A source binding + offsets mirrored at ingress, as a live source leaves them.
        engine.source_bindings.push(SourceBinding {
            connector_name: "src".to_string(),
            event_type: "E".to_string(),
            topic_override: None,
            extra_params: HashMap::new(),
        });
        engine
            .source_offsets_handle()
            .lock()
            .unwrap()
            .insert("src".to_string(), HashMap::from([(0, 42)]));

        let coordinator = CaptureCoordinator::default();
        engine
            .barrier_commit_2pc(1, &coordinator)
            .await
            .expect("barrier should succeed");

        // Phases 2+3 ran on the transactional sink, prepare before commit.
        let lifecycle = sink.lifecycle.lock().unwrap().clone();
        let prepare_idx = lifecycle.iter().position(|s| s == "prepare:1");
        let commit_idx = lifecycle.iter().position(|s| s == "commit:1");
        assert!(
            prepare_idx.is_some(),
            "prepare_commit not called: {lifecycle:?}"
        );
        assert!(commit_idx.is_some(), "commit not called: {lifecycle:?}");
        assert!(
            prepare_idx < commit_idx,
            "prepare must precede commit: {lifecycle:?}"
        );

        // Phase 4 committed the snapshot's source offsets via the coordinator,
        // defaulting the topic when the binding has no override / connector config.
        let committed = coordinator.committed.lock().unwrap().clone();
        assert_eq!(
            committed.len(),
            1,
            "expected one offset commit: {committed:?}"
        );
        let (connector, topic, offsets) = &committed[0];
        assert_eq!(connector, "src");
        assert_eq!(topic, "varpulis/events/#");
        assert_eq!(offsets.get(&0), Some(&42));
    }
}
