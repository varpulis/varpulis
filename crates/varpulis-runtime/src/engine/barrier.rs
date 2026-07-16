//! Two-phase-commit checkpoint barrier.
//!
//! Extracted from the CLI `run` loop so the exactly-once sequencing is drivable
//! in-process — by tests and embedders — without a broker or the CLI connector
//! registry. See `audit/EXACTLY_ONCE_DESIGN.md`.
//!
//! Sink and offset failures are still logged and swallowed (the hardening step
//! makes them fatal); the `Result` return type and the [`SourceCommitCoordinator`]
//! seam are the surfaces those steps build on. The barrier pauses the sources
//! and drains everything in flight before snapshotting, so committed offsets
//! never outrun applied state (audit C3).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;

use super::Engine;
use crate::event::Event;

/// Resumes source ingestion on every barrier exit — success, error, or early
/// return — so a checkpoint failure can never strand the sources paused.
///
/// Holds a clone of the engine's pause flag (an `Arc`), so it is independent of
/// the `&mut self` borrow the barrier needs for the drain and snapshot.
struct ResumeGuard(Arc<AtomicBool>);

impl Drop for ResumeGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

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
    /// Extracted from the CLI `run` loop. Sink/offset failures are still logged
    /// and swallowed (the hardening step makes them fatal); the `Result` return
    /// is that seam. See the module docs.
    ///
    /// **C3 — drain before snapshot.** Sources mirror their offsets at channel
    /// ingress, so a naive snapshot can record an offset for an event that has
    /// not yet been applied to engine state. Committing that offset and then
    /// crashing would skip the event on restart (data loss). To prevent it, the
    /// barrier first pauses the sources and drains everything already in flight
    /// (`event_rx`) into engine state, so the snapshot's offsets reflect
    /// *applied* state. Sources are resumed on every exit via [`ResumeGuard`].
    #[cfg(feature = "async-runtime")]
    pub async fn barrier_commit_2pc(
        &mut self,
        checkpoint_id: u64,
        event_rx: &mut tokio::sync::mpsc::Receiver<Vec<Event>>,
        coordinator: &dyn SourceCommitCoordinator,
    ) -> Result<(), BarrierError> {
        // C3 — pause sources and drain all in-flight events before snapshotting.
        // The guard resumes ingestion no matter how this method returns.
        let _resume = ResumeGuard(self.source_pause_handle());
        self.pause_sources();

        while let Ok(batch) = event_rx.try_recv() {
            if let Err(e) = self.process_batch(batch).await {
                tracing::warn!("Barrier drain apply failed (epoch {checkpoint_id}): {e}");
            }
        }

        // Phase 1 — state snapshot (offsets now reflect applied state, post-drain).
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
        let (_evt_tx, mut evt_rx) = tokio::sync::mpsc::channel::<Vec<Event>>(16);
        engine
            .barrier_commit_2pc(1, &mut evt_rx, &coordinator)
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

    /// C3 — the barrier drains in-flight events before snapshotting, so a crash
    /// right after the offset commit cannot skip an event whose offset was
    /// mirrored at ingress but never applied.
    ///
    /// Fail-before: delete the drain loop in `barrier_commit_2pc` and the
    /// recovered engine shows `events_processed == 3` — the event at offset 3
    /// was counted in the committed offset but lost from state.
    #[tokio::test]
    async fn drain_before_snapshot_prevents_inflight_event_loss_across_restart() {
        use crate::persistence::{CheckpointConfig, MemoryStore};

        let vpl = "stream PassThrough = TestEvent\n    .emit(value: value)\n";
        let program = varpulis_parser::parse(vpl).expect("parse passthrough VPL");

        // The durable store survives the "crash".
        let store = Arc::new(MemoryStore::new());

        // --- Run 1: apply offsets 0,1,2; leave offset 3 in flight; barrier; crash. ---
        let (tx1, _rx1) = tokio::sync::mpsc::channel::<Event>(64);
        let mut engine1 = Engine::new(tx1);
        engine1.load(&program).expect("load");
        engine1
            .enable_checkpointing(store.clone(), CheckpointConfig::default())
            .expect("enable checkpointing");

        // A source binding so the barrier commits this source's offsets.
        engine1.source_bindings.push(SourceBinding {
            connector_name: "src".to_string(),
            event_type: "TestEvent".to_string(),
            topic_override: None,
            extra_params: HashMap::new(),
        });

        // Events at offsets 0,1,2 are mirrored AND applied.
        for id in 0..3 {
            engine1
                .process(Event::new("TestEvent").with_field("value", id))
                .await
                .expect("process");
        }
        // The source has also read + mirrored offset 3 (event in flight), but it
        // is not yet applied — it sits unread in the event channel.
        engine1
            .source_offsets_handle()
            .lock()
            .unwrap()
            .insert("src".to_string(), HashMap::from([(0, 3)]));
        let (evt_tx, mut evt_rx) = tokio::sync::mpsc::channel::<Vec<Event>>(16);
        evt_tx
            .send(vec![Event::new("TestEvent").with_field("value", 3)])
            .await
            .expect("queue in-flight event");
        drop(evt_tx);

        assert_eq!(
            engine1.metrics().events_processed,
            3,
            "only offsets 0,1,2 applied before the barrier"
        );

        let coordinator = CaptureCoordinator::default();
        engine1
            .barrier_commit_2pc(1, &mut evt_rx, &coordinator)
            .await
            .expect("barrier");

        // The barrier drained + applied the in-flight event before snapshotting.
        assert_eq!(
            engine1.metrics().events_processed,
            4,
            "drain must apply the in-flight event before the snapshot"
        );
        // The committed source offset is 3, as mirrored at ingress.
        let committed = coordinator.committed.lock().unwrap().clone();
        assert_eq!(
            committed.len(),
            1,
            "expected one offset commit: {committed:?}"
        );
        assert_eq!(committed[0].2.get(&0), Some(&3));

        drop(engine1); // "crash": engine + in-flight channel are gone.

        // --- Restart: a fresh engine recovers from the same store. ---
        let (tx2, _rx2) = tokio::sync::mpsc::channel::<Event>(64);
        let mut engine2 = Engine::new(tx2);
        engine2.load(&program).expect("load");
        engine2
            .enable_checkpointing(store.clone(), CheckpointConfig::default())
            .expect("recover");

        // Committed offset is 3, so the source replays nothing past it. Every
        // event through offset 3 must therefore be reflected in recovered state.
        assert_eq!(
            engine2.metrics().events_processed,
            4,
            "all four events must survive the crash; a short count means the \
             in-flight event was lost (committed offset outran applied state)"
        );
    }
}
