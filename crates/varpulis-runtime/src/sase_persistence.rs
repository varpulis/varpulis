//! Checkpointing and restore for SASE engine state.
//!
//! Extension traits that add checkpoint/restore to varpulis-sase types
//! using runtime persistence types.

use chrono::DateTime;
use std::sync::Arc;
use varpulis_sase::{KleeneCapture, Run, SaseEngine, SharedEvent, StackEntry, Timestamp};

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
            partition_key: self.partition_key.as_ref().map(persistence::value_to_ser),
            invalidated: self.invalidated,
            pending_negation_count: self.pending_negations.len(),
            kleene_events,
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
            started_at: Timestamp::now(),
            deadline: None,
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
            and_state: None,
            kleene_capture,
        }
    }
}

/// Extension trait for checkpointing/restoring a SASE Engine.
pub trait SaseCheckpointExt {
    fn checkpoint(&self) -> persistence::SaseCheckpoint;
    fn restore(&mut self, cp: &persistence::SaseCheckpoint);
}

impl SaseCheckpointExt for SaseEngine {
    /// Create a checkpoint of the entire SASE engine state.
    fn checkpoint(&self) -> persistence::SaseCheckpoint {
        let active_runs = self.runs.iter().map(|r| r.checkpoint()).collect();

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
