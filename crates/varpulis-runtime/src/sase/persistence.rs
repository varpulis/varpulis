//! Checkpointing and restore for SASE engine state

use super::engine::SaseEngine;
use super::kleene::KleeneCapture;
use super::run::Run;
use super::types::{SharedEvent, StackEntry};
use chrono::DateTime;
use std::sync::Arc;
use std::time::Instant;

impl Run {
    /// Create a checkpoint of this run's state.
    pub fn checkpoint(&self) -> crate::persistence::RunCheckpoint {
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

        crate::persistence::RunCheckpoint {
            current_state: self.current_state,
            stack,
            captured,
            event_time_started_at_ms: self.event_time_started_at.map(|t| t.timestamp_millis()),
            event_time_deadline_ms: self.event_time_deadline.map(|t| t.timestamp_millis()),
            partition_key: self
                .partition_key
                .as_ref()
                .map(crate::persistence::value_to_ser),
            invalidated: self.invalidated,
            pending_negation_count: self.pending_negations.len(),
            kleene_events,
        }
    }

    /// Restore a run from a checkpoint.
    ///
    /// Note: Wall-clock deadlines (`deadline`, `started_at`) are reset since they
    /// are meaningless after a restart. Event-time deadlines are preserved.
    /// NegationConstraint predicates are NOT restored (they contain closures);
    /// only the count is preserved. The caller must reattach predicates from the NFA
    /// if needed.
    /// KleeneCapture ZDD is NOT rebuilt; only the events are restored.
    pub fn from_checkpoint(rc: &crate::persistence::RunCheckpoint) -> Self {
        use crate::event::Event;

        let stack = rc
            .stack
            .iter()
            .map(|se| StackEntry {
                event: Arc::new(Event::from(se.event.clone())),
                alias: se.alias.clone(),
                timestamp: Instant::now(),
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
            started_at: Instant::now(),
            deadline: None, // Wall-clock deadlines not meaningful after restart
            event_time_started_at: rc
                .event_time_started_at_ms
                .and_then(DateTime::from_timestamp_millis),
            event_time_deadline: rc
                .event_time_deadline_ms
                .and_then(DateTime::from_timestamp_millis),
            partition_key: rc
                .partition_key
                .as_ref()
                .map(|sv| crate::persistence::ser_to_value(sv.clone())),
            invalidated: rc.invalidated,
            pending_negations: Vec::new(), // Predicates cannot be serialized; reattach from NFA
            and_state: None,               // Rebuilt from NFA on next event
            kleene_capture,
        }
    }
}

impl SaseEngine {
    /// Create a checkpoint of the entire SASE engine state.
    pub fn checkpoint(&self) -> crate::persistence::SaseCheckpoint {
        let active_runs = self.runs.iter().map(|r| r.checkpoint()).collect();

        let partitioned_runs = self
            .partitioned_runs
            .iter()
            .map(|(k, runs)| {
                let run_cps = runs.iter().map(|r| r.checkpoint()).collect();
                (k.clone(), run_cps)
            })
            .collect();

        crate::persistence::SaseCheckpoint {
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
    ///
    /// The NFA must already be compiled (from VPL source) before calling restore.
    /// Wall-clock deadlines and NegationConstraint predicates are not restored.
    pub fn restore(&mut self, cp: &crate::persistence::SaseCheckpoint) {
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
    }
}
