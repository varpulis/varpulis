//! Sequence tracking for temporal event correlations
//!
//! Implements the runtime support for the `->` (followed-by) operator.

use crate::event::Event;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// A step in a sequence pattern
pub struct SequenceStep {
    /// Event type to match
    pub event_type: String,
    /// Filter function to apply
    pub filter: Option<Box<dyn Fn(&Event, &SequenceContext) -> bool + Send + Sync>>,
    /// Alias for captured event
    pub alias: Option<String>,
    /// Timeout for this step
    pub timeout: Option<Duration>,
    /// Match all events (true) or just one (false)
    pub match_all: bool,
}

/// Context for evaluating sequence filters
#[derive(Debug, Clone, Default)]
pub struct SequenceContext {
    /// Captured events by alias
    pub captured: HashMap<String, Event>,
    /// Previous event in sequence (accessible as $)
    pub previous: Option<Event>,
}

impl SequenceContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_captured(mut self, alias: String, event: Event) -> Self {
        self.previous = Some(event.clone());
        self.captured.insert(alias, event);
        self
    }

    pub fn get(&self, alias: &str) -> Option<&Event> {
        if alias == "$" {
            self.previous.as_ref()
        } else {
            self.captured.get(alias)
        }
    }
}

/// An active correlation being tracked
#[derive(Debug)]
pub struct ActiveCorrelation {
    /// Current step index in the sequence
    pub current_step: usize,
    /// Context with captured events
    pub context: SequenceContext,
    /// When this correlation was started
    pub started_at: Instant,
    /// Timeout for current step (if any)
    pub step_timeout: Option<Instant>,
}

impl ActiveCorrelation {
    pub fn new() -> Self {
        Self {
            current_step: 0,
            context: SequenceContext::new(),
            started_at: Instant::now(),
            step_timeout: None,
        }
    }

    pub fn advance(&mut self, event: Event, alias: Option<&str>) {
        if let Some(alias) = alias {
            self.context = std::mem::take(&mut self.context)
                .with_captured(alias.to_string(), event);
        } else {
            self.context.previous = Some(event);
        }
        self.current_step += 1;
        self.step_timeout = None;
    }

    pub fn is_timed_out(&self) -> bool {
        if let Some(deadline) = self.step_timeout {
            Instant::now() > deadline
        } else {
            false
        }
    }

    pub fn set_timeout(&mut self, duration: Duration) {
        self.step_timeout = Some(Instant::now() + duration);
    }
}

/// A negation condition that invalidates a sequence
pub struct NegationCondition {
    /// Event type that triggers negation
    pub event_type: String,
    /// Filter function to apply
    pub filter: Option<Box<dyn Fn(&Event, &SequenceContext) -> bool + Send + Sync>>,
}

/// Tracks sequences for a stream
pub struct SequenceTracker {
    /// Steps in the sequence pattern
    steps: Vec<SequenceStep>,
    /// Active correlations being tracked
    active: Vec<ActiveCorrelation>,
    /// Maximum concurrent correlations
    max_active: usize,
    /// Match all for first step
    match_all_first: bool,
    /// Negation conditions that invalidate sequences
    negations: Vec<NegationCondition>,
}

impl SequenceTracker {
    pub fn new(steps: Vec<SequenceStep>, match_all_first: bool) -> Self {
        Self {
            steps,
            active: Vec::new(),
            max_active: 10000,
            match_all_first,
            negations: Vec::new(),
        }
    }

    pub fn with_max_active(mut self, max: usize) -> Self {
        self.max_active = max;
        self
    }

    pub fn with_negation(mut self, negation: NegationCondition) -> Self {
        self.negations.push(negation);
        self
    }

    pub fn add_negation(&mut self, negation: NegationCondition) {
        self.negations.push(negation);
    }

    /// Process an incoming event, returns completed correlations
    pub fn process(&mut self, event: &Event) -> Vec<SequenceContext> {
        let mut completed = Vec::new();

        // First, check if this event triggers any negation condition
        // If so, invalidate matching active correlations
        self.check_negations(event);

        // Track how many correlations existed before starting new ones
        let existing_count = self.active.len();

        // Check if this event starts a new correlation
        if self.should_start_new(event) {
            self.start_correlation(event);
        }

        // Then, advance existing correlations (NOT newly started ones)
        // This prevents the same event from matching multiple steps
        let mut i = 0;
        while i < existing_count.min(self.active.len()) {
            // Remove timed out correlations
            if self.active[i].is_timed_out() {
                self.active.remove(i);
                continue;
            }

            let step_idx = self.active[i].current_step;

            // Check if we're waiting for more steps
            if step_idx >= self.steps.len() {
                i += 1;
                continue;
            }

            // Check if event matches this step (need to avoid borrow issues)
            let matches = {
                let step = &self.steps[step_idx];
                let context = &self.active[i].context;
                self.event_matches_step(event, step, context)
            };

            if matches {
                let step = &self.steps[step_idx];
                let alias = step.alias.clone();
                let step_match_all = step.match_all;

                self.active[i].advance(event.clone(), alias.as_deref());

                // Set timeout for next step if specified
                let current_step = self.active[i].current_step;
                if let Some(next_step) = self.steps.get(current_step) {
                    if let Some(t) = next_step.timeout {
                        self.active[i].set_timeout(t);
                    }
                }

                // Check if sequence is complete
                if self.active[i].current_step >= self.steps.len() {
                    if step_match_all {
                        // match_all: emit result but keep correlation active for more matches
                        completed.push(self.active[i].context.clone());
                        // Reset to previous step to match more events of this type
                        self.active[i].current_step = step_idx;
                    } else {
                        // Normal: remove completed correlation
                        completed.push(self.active.remove(i).context);
                        continue;
                    }
                }
            }

            i += 1;
        }

        completed
    }

    fn should_start_new(&self, event: &Event) -> bool {
        if self.steps.is_empty() {
            return false;
        }

        // Check if we're at capacity
        if self.active.len() >= self.max_active {
            return false;
        }

        // Check if event matches the first step
        let first_step = &self.steps[0];
        if event.event_type != first_step.event_type {
            return false;
        }

        // Apply filter if any
        if let Some(ref filter) = first_step.filter {
            let ctx = SequenceContext::new();
            if !filter(event, &ctx) {
                return false;
            }
        }

        // If not match_all, only start if no active correlations
        if !self.match_all_first && !self.active.is_empty() {
            return false;
        }

        true
    }

    fn start_correlation(&mut self, event: &Event) {
        let mut correlation = ActiveCorrelation::new();
        let first_step = &self.steps[0];

        correlation.advance(event.clone(), first_step.alias.as_deref());

        // Set timeout for second step if specified
        if let Some(second_step) = self.steps.get(1) {
            if let Some(t) = second_step.timeout {
                correlation.set_timeout(t);
            }
        }

        self.active.push(correlation);
    }

    fn event_matches_step(&self, event: &Event, step: &SequenceStep, context: &SequenceContext) -> bool {
        // Check event type
        if event.event_type != step.event_type {
            return false;
        }

        // Apply filter if any
        if let Some(ref filter) = step.filter {
            if !filter(event, context) {
                return false;
            }
        }

        true
    }

    /// Check if event triggers any negation condition and invalidate matching correlations
    fn check_negations(&mut self, event: &Event) {
        if self.negations.is_empty() {
            return;
        }

        // Check each negation condition
        for negation in &self.negations {
            if event.event_type != negation.event_type {
                continue;
            }

            // Remove correlations where the negation filter matches
            self.active.retain(|correlation| {
                if let Some(ref filter) = negation.filter {
                    // If filter returns true, the negation matches -> remove correlation
                    !filter(event, &correlation.context)
                } else {
                    // No filter means any event of this type negates -> remove correlation
                    false
                }
            });
        }
    }

    /// Get count of active correlations
    pub fn active_count(&self) -> usize {
        self.active.len()
    }

    /// Clean up timed out correlations
    pub fn cleanup_timeouts(&mut self) -> usize {
        let before = self.active.len();
        self.active.retain(|c| !c.is_timed_out());
        before - self.active.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use varpulis_core::Value;

    fn make_event(event_type: &str, fields: Vec<(&str, Value)>) -> Event {
        let mut event = Event::new(event_type.to_string());
        for (k, v) in fields {
            event.data.insert(k.to_string(), v);
        }
        event
    }

    #[test]
    fn test_simple_sequence_a_then_b() {
        let steps = vec![
            SequenceStep {
                event_type: "A".to_string(),
                filter: None,
                alias: Some("a".to_string()),
                timeout: None,
                match_all: false,
            },
            SequenceStep {
                event_type: "B".to_string(),
                filter: None,
                alias: Some("b".to_string()),
                timeout: None,
                match_all: false,
            },
        ];

        let mut tracker = SequenceTracker::new(steps, false);

        // Send A - should start correlation
        let a = make_event("A", vec![("id", Value::Int(1))]);
        let completed = tracker.process(&a);
        assert!(completed.is_empty());
        assert_eq!(tracker.active_count(), 1);

        // Send B - should complete sequence
        let b = make_event("B", vec![("id", Value::Int(2))]);
        let completed = tracker.process(&b);
        assert_eq!(completed.len(), 1);
        assert_eq!(tracker.active_count(), 0);

        // Check captured events
        let ctx = &completed[0];
        assert!(ctx.get("a").is_some());
        assert!(ctx.get("b").is_some());
    }

    #[test]
    fn test_sequence_with_filter() {
        let steps = vec![
            SequenceStep {
                event_type: "Order".to_string(),
                filter: None,
                alias: Some("order".to_string()),
                timeout: None,
                match_all: false,
            },
            SequenceStep {
                event_type: "Payment".to_string(),
                filter: Some(Box::new(|event, ctx| {
                    // Payment.order_id == order.id
                    let order = ctx.get("order").unwrap();
                    event.get_int("order_id") == order.get_int("id")
                })),
                alias: Some("payment".to_string()),
                timeout: None,
                match_all: false,
            },
        ];

        let mut tracker = SequenceTracker::new(steps, false);

        // Order 1
        let order1 = make_event("Order", vec![("id", Value::Int(1))]);
        tracker.process(&order1);
        assert_eq!(tracker.active_count(), 1);

        // Payment for wrong order - should not complete
        let wrong_payment = make_event("Payment", vec![("order_id", Value::Int(999))]);
        let completed = tracker.process(&wrong_payment);
        assert!(completed.is_empty());
        assert_eq!(tracker.active_count(), 1);

        // Payment for correct order - should complete
        let correct_payment = make_event("Payment", vec![("order_id", Value::Int(1))]);
        let completed = tracker.process(&correct_payment);
        assert_eq!(completed.len(), 1);
    }

    #[test]
    fn test_sequence_match_all() {
        let steps = vec![
            SequenceStep {
                event_type: "News".to_string(),
                filter: None,
                alias: Some("news".to_string()),
                timeout: None,
                match_all: true,
            },
            SequenceStep {
                event_type: "Tick".to_string(),
                filter: None,
                alias: Some("tick".to_string()),
                timeout: None,
                match_all: false,
            },
        ];

        let mut tracker = SequenceTracker::new(steps, true);

        // Send multiple News events
        let news1 = make_event("News", vec![("id", Value::Int(1))]);
        let news2 = make_event("News", vec![("id", Value::Int(2))]);
        tracker.process(&news1);
        tracker.process(&news2);
        assert_eq!(tracker.active_count(), 2);

        // Send Tick - should complete BOTH correlations
        let tick = make_event("Tick", vec![("price", Value::Float(100.0))]);
        let completed = tracker.process(&tick);
        assert_eq!(completed.len(), 2);
        assert_eq!(tracker.active_count(), 0);
    }

    #[test]
    fn test_sequence_three_steps() {
        let steps = vec![
            SequenceStep {
                event_type: "A".to_string(),
                filter: None,
                alias: Some("a".to_string()),
                timeout: None,
                match_all: false,
            },
            SequenceStep {
                event_type: "B".to_string(),
                filter: None,
                alias: Some("b".to_string()),
                timeout: None,
                match_all: false,
            },
            SequenceStep {
                event_type: "C".to_string(),
                filter: None,
                alias: Some("c".to_string()),
                timeout: None,
                match_all: false,
            },
        ];

        let mut tracker = SequenceTracker::new(steps, false);

        tracker.process(&make_event("A", vec![]));
        assert_eq!(tracker.active_count(), 1);

        tracker.process(&make_event("B", vec![]));
        assert_eq!(tracker.active_count(), 1);

        let completed = tracker.process(&make_event("C", vec![]));
        assert_eq!(completed.len(), 1);
        assert_eq!(tracker.active_count(), 0);

        // All three should be captured
        let ctx = &completed[0];
        assert!(ctx.get("a").is_some());
        assert!(ctx.get("b").is_some());
        assert!(ctx.get("c").is_some());
    }

    #[test]
    fn test_sequence_dollar_reference() {
        let steps = vec![
            SequenceStep {
                event_type: "Start".to_string(),
                filter: None,
                alias: None, // No alias, but should be accessible as $
                timeout: None,
                match_all: false,
            },
            SequenceStep {
                event_type: "End".to_string(),
                filter: Some(Box::new(|event, ctx| {
                    // End.id == $.id (previous event)
                    let prev = ctx.get("$").unwrap();
                    event.get_int("id") == prev.get_int("id")
                })),
                alias: None,
                timeout: None,
                match_all: false,
            },
        ];

        let mut tracker = SequenceTracker::new(steps, false);

        tracker.process(&make_event("Start", vec![("id", Value::Int(42))]));

        // Wrong ID
        let completed = tracker.process(&make_event("End", vec![("id", Value::Int(99))]));
        assert!(completed.is_empty());

        // Correct ID
        let completed = tracker.process(&make_event("End", vec![("id", Value::Int(42))]));
        assert_eq!(completed.len(), 1);
    }

    #[test]
    fn test_sequence_max_active() {
        let steps = vec![
            SequenceStep {
                event_type: "A".to_string(),
                filter: None,
                alias: None,
                timeout: None,
                match_all: true,
            },
            SequenceStep {
                event_type: "B".to_string(),
                filter: None,
                alias: None,
                timeout: None,
                match_all: false,
            },
        ];

        let mut tracker = SequenceTracker::new(steps, true).with_max_active(3);

        // Start 5 correlations, but only 3 should be active
        for i in 0..5 {
            tracker.process(&make_event("A", vec![("id", Value::Int(i))]));
        }
        assert_eq!(tracker.active_count(), 3);
    }

    #[test]
    fn test_sequence_no_match_wrong_type() {
        let steps = vec![
            SequenceStep {
                event_type: "A".to_string(),
                filter: None,
                alias: None,
                timeout: None,
                match_all: false,
            },
            SequenceStep {
                event_type: "B".to_string(),
                filter: None,
                alias: None,
                timeout: None,
                match_all: false,
            },
        ];

        let mut tracker = SequenceTracker::new(steps, false);

        // Send X - should not start
        tracker.process(&make_event("X", vec![]));
        assert_eq!(tracker.active_count(), 0);

        // Send A - should start
        tracker.process(&make_event("A", vec![]));
        assert_eq!(tracker.active_count(), 1);

        // Send X - should not advance
        tracker.process(&make_event("X", vec![]));
        assert_eq!(tracker.active_count(), 1);
    }
}
