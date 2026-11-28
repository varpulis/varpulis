//! AND operator state tracking (AND-01)

use super::types::{Predicate, SharedEvent};

/// One event an AND branch accepts: `A where x > 1 as a`. A branch made of
/// an `or` (`(A as e or B as e) and C`) has one option per alternative.
#[derive(Debug, Clone)]
pub struct AndOption {
    /// Event type this option takes
    pub event_type: String,
    /// Optional predicate
    pub predicate: Option<Predicate>,
    /// Alias for the captured event
    pub alias: Option<String>,
    /// The other branches whose alias `predicate` reads
    /// (`B where id == a.id as b` next to `A as a`). The option is judged
    /// only once all of them have matched; an event that arrives earlier
    /// waits in [`AndState::waiting`].
    pub sibling_refs: Vec<usize>,
}

/// Configuration for an AND branch: the events, any one of which completes it.
#[derive(Debug, Clone)]
pub struct AndBranch {
    /// What the branch accepts, in source order. An event takes the first
    /// option it satisfies.
    pub options: Vec<AndOption>,
}

impl AndBranch {
    /// Whether some option of this branch takes events of `event_type`.
    pub fn accepts_type(&self, event_type: &str) -> bool {
        self.options.iter().any(|o| o.event_type == event_type)
    }
}

/// Configuration for AND states in the NFA
#[derive(Debug, Clone)]
pub struct AndConfig {
    /// The branches that need to be completed
    pub branches: Vec<AndBranch>,
    /// State to transition to after all branches complete
    pub join_state: usize,
}

/// Runtime state tracking for AND pattern completion
#[derive(Debug, Clone)]
pub struct AndState {
    /// Which branches have been completed (indexed by branch position)
    pub completed: Vec<bool>,
    /// Captured events for completed branches
    pub branch_events: Vec<Option<SharedEvent>>,
    /// Events of a branch's type that arrived before the siblings its
    /// predicate reads, oldest first: `B` in `A as a and B where id == a.id`
    /// arriving before any `A`. Each is judged when those siblings have
    /// matched. Bounded by the Kleene event cap; past it the oldest leaves
    /// and [`AndState::waiting_dropped`] counts it.
    pub waiting: Vec<SharedEvent>,
    /// Events that left [`AndState::waiting`] at the cap, unjudged.
    pub waiting_dropped: u32,
    /// The waiting event that opened this run, when an AND at the start of
    /// the pattern opened it with one (see `offer_to_and`).
    pub anchor: Option<SharedEvent>,
}

impl AndState {
    /// Create a new empty AND state with no completed branches.
    pub fn new() -> Self {
        Self {
            completed: Vec::new(),
            branch_events: Vec::new(),
            waiting: Vec::new(),
            waiting_dropped: 0,
            anchor: None,
        }
    }

    /// Check whether the branch at position `idx` has been completed.
    pub fn is_branch_completed(&self, idx: usize) -> bool {
        self.completed.get(idx).copied().unwrap_or(false)
    }

    /// Mark branch `idx` as completed with the given event.
    pub fn complete_branch(&mut self, idx: usize, event: SharedEvent) {
        while self.completed.len() <= idx {
            self.completed.push(false);
            self.branch_events.push(None);
        }
        self.completed[idx] = true;
        self.branch_events[idx] = Some(event);
    }

    /// Restore the completed branches of a checkpointed run. The events
    /// themselves are in the run's captures and stack.
    pub fn from_completed(completed: Vec<bool>) -> Self {
        let branch_events = vec![None; completed.len()];
        Self {
            completed,
            branch_events,
            waiting: Vec::new(),
            waiting_dropped: 0,
            anchor: None,
        }
    }

    /// Return true if all `total` branches have been completed.
    pub fn all_completed(&self, total: usize) -> bool {
        self.completed.len() >= total && self.completed.iter().take(total).all(|&c| c)
    }
}

impl Default for AndState {
    fn default() -> Self {
        Self::new()
    }
}

/// Information about what to forbid in negation states
#[derive(Debug, Clone)]
pub struct NegationInfo {
    /// Event type that must NOT occur
    pub forbidden_type: String,
    /// Optional predicate for the forbidden event
    pub predicate: Option<Predicate>,
    /// NFA state to transition to once negation is confirmed
    pub continue_state: usize,
}
