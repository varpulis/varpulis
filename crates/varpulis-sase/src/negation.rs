//! Temporal negation support (NEG-01)

use super::predicate::eval_predicate;
use super::types::{Predicate, SharedEvent};
use crate::ExprEvaluator;
use chrono::{DateTime, Utc};
use rustc_hash::FxHashMap;
use std::time::Instant;
use varpulis_core::Event;

/// Constraint for temporal negation that must be confirmed via timeout
#[derive(Debug, Clone)]
pub struct NegationConstraint {
    /// Event type that must NOT occur
    pub forbidden_type: String,
    /// Optional predicate for the forbidden event
    pub predicate: Option<Predicate>,
    /// Deadline after which negation is confirmed (processing time)
    pub deadline: Option<Instant>,
    /// Deadline in event-time (for watermark-based timeout)
    pub event_time_deadline: Option<DateTime<Utc>>,
    /// NFA state to transition to once negation is confirmed
    pub next_state: usize,
}

impl NegationConstraint {
    /// Check if this constraint is violated by the given event
    pub fn is_violated_by(
        &self,
        event: &Event,
        captured: &FxHashMap<String, SharedEvent>,
        evaluator: Option<&dyn ExprEvaluator>,
    ) -> bool {
        if *event.event_type != self.forbidden_type {
            return false;
        }
        // If there's a predicate, check it
        match &self.predicate {
            Some(pred) => eval_predicate(pred, event, captured, evaluator),
            None => true, // No predicate means any event of this type violates
        }
    }

    /// Check if this negation constraint is confirmed (deadline passed without violation)
    pub fn is_confirmed_processing_time(&self) -> bool {
        self.deadline.is_some_and(|d| Instant::now() > d)
    }

    /// Check if this negation constraint is confirmed via watermark
    pub fn is_confirmed_event_time(&self, watermark: DateTime<Utc>) -> bool {
        self.event_time_deadline.is_some_and(|d| watermark > d)
    }
}
