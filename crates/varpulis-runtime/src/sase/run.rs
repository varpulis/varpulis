//! Active pattern run (partial match in progress)

use super::and_op::AndState;
use super::kleene::KleeneCapture;
use super::negation::NegationConstraint;
use super::types::{SharedEvent, StackEntry};
use chrono::{DateTime, Utc};
use rustc_hash::FxHashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use varpulis_core::Value;

/// An active pattern run (partial match in progress)
#[derive(Debug, Clone)]
pub struct Run {
    /// Current NFA state
    pub current_state: usize,
    /// Stack of matched events (for non-Kleene patterns)
    pub stack: Vec<StackEntry>,
    /// Captured events by alias (Arc for efficient sharing)
    pub captured: FxHashMap<String, SharedEvent>,
    /// When this run started (wall-clock time for metrics)
    pub started_at: Instant,
    /// Deadline for completion (from WITHIN) - wall-clock time (legacy)
    pub deadline: Option<Instant>,
    /// Event timestamp when this run started (for event-time processing)
    pub event_time_started_at: Option<DateTime<Utc>>,
    /// Deadline in event-time (for watermark-based timeout)
    pub event_time_deadline: Option<DateTime<Utc>>,
    /// Partition key (for SASEXT optimization)
    pub partition_key: Option<Value>,
    /// Is this run invalidated by negation?
    pub invalidated: bool,
    /// NEG-01: Pending negation constraints that must be confirmed
    pub pending_negations: Vec<NegationConstraint>,
    /// AND-01: State tracking for AND operator (if in an AND state)
    pub and_state: Option<AndState>,
    /// ZDD-KLEENE: Compact Kleene capture using ZDD (replaces branching)
    pub kleene_capture: Option<KleeneCapture>,
}

impl Default for Run {
    fn default() -> Self {
        Self {
            current_state: 0,
            stack: Vec::new(),
            captured: FxHashMap::default(),
            started_at: Instant::now(),
            deadline: None,
            event_time_started_at: None,
            event_time_deadline: None,
            partition_key: None,
            invalidated: false,
            pending_negations: Vec::new(),
            and_state: None,
            kleene_capture: None,
        }
    }
}

impl Run {
    pub fn new(start_state: usize) -> Self {
        Self {
            current_state: start_state,
            stack: Vec::new(),
            captured: FxHashMap::default(),
            started_at: Instant::now(),
            deadline: None,
            event_time_started_at: None,
            event_time_deadline: None,
            partition_key: None,
            invalidated: false,
            pending_negations: Vec::new(),
            and_state: None,
            kleene_capture: None,
        }
    }

    /// Create a new run with event-time tracking
    pub fn new_with_event_time(start_state: usize, event_timestamp: DateTime<Utc>) -> Self {
        Self {
            current_state: start_state,
            stack: Vec::new(),
            captured: FxHashMap::default(),
            started_at: Instant::now(),
            deadline: None,
            event_time_started_at: Some(event_timestamp),
            event_time_deadline: None,
            partition_key: None,
            invalidated: false,
            pending_negations: Vec::new(),
            and_state: None,
            kleene_capture: None,
        }
    }

    pub fn with_partition(mut self, key: Value) -> Self {
        self.partition_key = Some(key);
        self
    }

    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Set event-time deadline based on timeout duration
    pub fn with_event_time_deadline(mut self, timeout: Duration) -> Self {
        if let Some(started_at) = self.event_time_started_at {
            self.event_time_deadline =
                Some(started_at + chrono::Duration::from_std(timeout).unwrap_or_default());
        }
        self
    }

    pub fn push(&mut self, event: SharedEvent, alias: Option<String>) {
        if let Some(ref a) = alias {
            self.captured.insert(a.clone(), Arc::clone(&event));
        }
        self.stack.push(StackEntry {
            event,
            alias,
            timestamp: Instant::now(),
        });
    }

    /// PERF(Opt2): Push with a pre-captured timestamp to avoid per-event Instant::now()
    #[inline]
    pub fn push_at(&mut self, event: SharedEvent, alias: Option<String>, ts: Instant) {
        if let Some(ref a) = alias {
            self.captured.insert(a.clone(), Arc::clone(&event));
        }
        self.stack.push(StackEntry {
            event,
            alias,
            timestamp: ts,
        });
    }

    /// PERF(Opt4): Push for Kleene self-loop where alias key already exists in captured.
    /// Updates existing captured value via get_mut (no key allocation) and borrows alias.
    #[inline]
    pub fn push_at_kleene(&mut self, event: SharedEvent, alias: &Option<String>, ts: Instant) {
        if let Some(ref a) = alias {
            if let Some(entry) = self.captured.get_mut(a.as_str()) {
                *entry = Arc::clone(&event);
            } else {
                self.captured.insert(a.clone(), Arc::clone(&event));
            }
        }
        self.stack.push(StackEntry {
            event,
            alias: alias.clone(),
            timestamp: ts,
        });
    }

    /// Check if run has timed out based on wall-clock time (legacy mode)
    pub fn is_timed_out(&self) -> bool {
        if let Some(deadline) = self.deadline {
            Instant::now() > deadline
        } else {
            false
        }
    }

    /// Check if run has timed out based on event-time watermark
    pub fn is_timed_out_event_time(&self, watermark: DateTime<Utc>) -> bool {
        if let Some(deadline) = self.event_time_deadline {
            watermark > deadline
        } else {
            false
        }
    }

    /// Clone run for Kleene branching
    pub fn branch(&self) -> Self {
        self.clone()
    }
}
