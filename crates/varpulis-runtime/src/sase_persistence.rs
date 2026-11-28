//! Checkpointing and restore for SASE engine state.
//!
//! Extension traits that add checkpoint/restore to varpulis-sase types
//! using runtime persistence types.

use std::sync::Arc;

use chrono::DateTime;
use varpulis_sase::{AndState, KleeneCapture, Run, SaseEngine, SharedEvent, StackEntry, Timestamp};

use crate::persistence;

/// Extension trait for checkpointing/restoring a SASE Run.
pub trait RunCheckpointExt {
    fn checkpoint(&self) -> persistence::RunCheckpoint;
    fn from_checkpoint(rc: &persistence::RunCheckpoint) -> Self;
}

impl RunCheckpointExt for Run {
    /// Create a checkpoint of this run's state.
    fn checkpoint(&self) -> persistence::RunCheckpoint {
        use crate::persistence::{SerializableEvent, StackEntryCheckpoint};

        let stack = self
            .stack
            .iter()
            .map(|se| StackEntryCheckpoint {
                event: SerializableEvent::from(se.event.as_ref()),
                alias: se.alias.clone(),
            })
            .collect();

        let captured = self
            .captured
            .iter()
            .map(|(k, v)| (k.clone(), SerializableEvent::from(v.as_ref())))
            .collect();

        let kleene_events = self.kleene_capture.as_ref().map(|kc| {
            kc.events
                .iter()
                .map(|e| SerializableEvent::from(e.as_ref()))
                .collect()
        });

        persistence::RunCheckpoint {
            current_state: self.current_state,
            stack,
            captured,
            event_time_started_at_ms: self.event_time_started_at.map(|t| t.timestamp_millis()),
            event_time_deadline_ms: self.event_time_deadline.map(|t| t.timestamp_millis()),
            deadline_ms: self.deadline.map(|t| t.as_datetime().timestamp_millis()),
            started_at_ms: Some(self.started_at.as_datetime().timestamp_millis()),
            partition_key: self.partition_key.as_ref().map(persistence::value_to_ser),
            invalidated: self.invalidated,
            pending_negation_count: self.pending_negations.len(),
            kleene_events,
            and_completed: self.and_state.as_ref().map(|a| a.completed.clone()),
            and_waiting: self
                .and_state
                .as_ref()
                .map(|a| {
                    a.waiting
                        .iter()
                        .map(|e| SerializableEvent::from(e.as_ref()))
                        .collect()
                })
                .unwrap_or_default(),
            and_anchored: self.and_state.as_ref().is_some_and(|a| a.anchor.is_some()),
        }
    }

    /// Restore a run from a checkpoint.
    fn from_checkpoint(rc: &persistence::RunCheckpoint) -> Self {
        use crate::event::Event;

        let stack = rc
            .stack
            .iter()
            .map(|se| StackEntry {
                event: Arc::new(Event::from(se.event.clone())),
                alias: se.alias.clone(),
                timestamp: Timestamp::now(),
            })
            .collect();

        let captured = rc
            .captured
            .iter()
            .map(|(k, se)| (k.clone(), Arc::new(Event::from(se.clone())) as SharedEvent))
            .collect();

        let kleene_capture = rc.kleene_events.as_ref().map(|events| {
            let mut kc = KleeneCapture::new();
            for se in events {
                let event = Arc::new(Event::from(se.clone()));
                kc.extend(event, None);
            }
            kc
        });

        Self {
            current_state: rc.current_state,
            stack,
            captured,
            // Both wall-clock fields are absolute instants, not monotonic
            // offsets, so they survive a restart verbatim. Resetting them to
            // `now()` restarted the WITHIN clock on every restore, which made
            // a restored partial match immortal under processing time.
            started_at: rc
                .started_at_ms
                .and_then(DateTime::from_timestamp_millis)
                .map_or_else(Timestamp::now, Timestamp::from_datetime),
            deadline: rc
                .deadline_ms
                .and_then(DateTime::from_timestamp_millis)
                .map(Timestamp::from_datetime),
            event_time_started_at: rc
                .event_time_started_at_ms
                .and_then(DateTime::from_timestamp_millis),
            event_time_deadline: rc
                .event_time_deadline_ms
                .and_then(DateTime::from_timestamp_millis),
            partition_key: rc
                .partition_key
                .as_ref()
                .map(|sv| persistence::ser_to_value(sv.clone())),
            invalidated: rc.invalidated,
            pending_negations: Vec::new(),
            and_state: restore_and_state(rc),
            kleene_capture,
            // A fresh number, taken in checkpoint order: the checkpoint lists
            // runs in creation order, so the restored runs keep theirs.
            seq: Run::next_seq(),
        }
    }
}

/// The `and` a checkpointed run was inside, if any: its matched operands and
/// its waiting events.
fn restore_and_state(rc: &persistence::RunCheckpoint) -> Option<AndState> {
    use crate::event::Event;

    if rc.and_completed.is_none() && rc.and_waiting.is_empty() {
        return None;
    }
    let mut and_state = AndState::from_completed(rc.and_completed.clone().unwrap_or_default());
    and_state.waiting = rc
        .and_waiting
        .iter()
        .map(|se| Arc::new(Event::from(se.clone())) as SharedEvent)
        .collect();
    if rc.and_anchored {
        and_state.anchor = and_state.waiting.first().cloned();
    }
    Some(and_state)
}

/// Extension trait for checkpointing/restoring a SASE Engine.
pub trait SaseCheckpointExt {
    fn checkpoint(&self) -> persistence::SaseCheckpoint;
    fn restore(&mut self, cp: &persistence::SaseCheckpoint);
}

impl SaseCheckpointExt for SaseEngine {
    /// Create a checkpoint of the entire SASE engine state.
    fn checkpoint(&self) -> persistence::SaseCheckpoint {
        // Runs parked in the predicate index are active runs too. They are
        // written in creation order, which a restore keeps (`Run::seq`).
        let mut runs: Vec<&Run> = self.runs.iter().chain(self.parked_runs()).collect();
        runs.sort_by_key(|r| r.seq);
        let active_runs = runs.into_iter().map(|r| r.checkpoint()).collect();

        let partitioned_runs = self
            .partitioned_runs
            .iter()
            .map(|(k, runs)| {
                let run_cps = runs.iter().map(|r| r.checkpoint()).collect();
                (k.clone(), run_cps)
            })
            .collect();

        persistence::SaseCheckpoint {
            active_runs,
            partitioned_runs,
            watermark_ms: self.watermark.map(|w| w.timestamp_millis()),
            max_timestamp_ms: self.max_timestamp.map(|t| t.timestamp_millis()),
            total_runs_created: self.total_runs_created,
            total_runs_completed: self.total_runs_completed,
            total_runs_dropped: self.total_runs_dropped,
            total_runs_evicted: self.total_runs_evicted,
        }
    }

    /// Restore engine state from a checkpoint.
    fn restore(&mut self, cp: &persistence::SaseCheckpoint) {
        // Every run is replaced; the next event files the restored ones in
        // the predicate index again.
        self.discard_parked_runs();
        self.runs = cp.active_runs.iter().map(Run::from_checkpoint).collect();

        self.partitioned_runs = cp
            .partitioned_runs
            .iter()
            .map(|(k, runs)| {
                let restored_runs = runs.iter().map(Run::from_checkpoint).collect();
                (k.clone(), restored_runs)
            })
            .collect();

        self.watermark = cp.watermark_ms.and_then(DateTime::from_timestamp_millis);
        self.max_timestamp = cp
            .max_timestamp_ms
            .and_then(DateTime::from_timestamp_millis);
        self.total_runs_created = cp.total_runs_created;
        self.total_runs_completed = cp.total_runs_completed;
        self.total_runs_dropped = cp.total_runs_dropped;
        self.total_runs_evicted = cp.total_runs_evicted;
        // Not in the checkpoint: rebuilt from the runs.
        self.resume_after_restore();
    }
}
