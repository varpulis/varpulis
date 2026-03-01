//! AND operator state tracking (AND-01)

use super::types::{Predicate, SharedEvent};

/// Configuration for an AND branch
#[derive(Debug, Clone)]
pub struct AndBranch {
    /// Event type expected for this branch
    pub event_type: String,
    /// Optional predicate
    pub predicate: Option<Predicate>,
    /// Alias for the captured event
    pub alias: Option<String>,
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
}

impl AndState {
    /// Create a new empty AND state with no completed branches.
    pub fn new() -> Self {
        Self {
            completed: Vec::new(),
            branch_events: Vec::new(),
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
