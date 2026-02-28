//! Event type indexing (IDX-01)

use super::nfa::{Nfa, StateType};
use rustc_hash::FxHashMap;

/// Index mapping event types to states that expect them
#[derive(Debug, Clone)]
pub struct EventTypeIndex {
    /// event_type -> list of state IDs that can accept this event type
    state_index: FxHashMap<String, Vec<usize>>,
    /// States that can match any event type (no specific type requirement)
    wildcard_states: Vec<usize>,
    /// States that are AND states (need special handling)
    and_states: Vec<usize>,
}

impl EventTypeIndex {
    pub fn new() -> Self {
        Self {
            state_index: FxHashMap::default(),
            wildcard_states: Vec::new(),
            and_states: Vec::new(),
        }
    }

    /// Build index from NFA
    pub fn from_nfa(nfa: &Nfa) -> Self {
        let mut index = Self::new();

        for state in &nfa.states {
            match state.state_type {
                StateType::And => {
                    // AND states handle multiple event types
                    index.and_states.push(state.id);
                    if let Some(ref config) = state.and_config {
                        for branch in &config.branches {
                            if !branch.event_type.is_empty() {
                                index
                                    .state_index
                                    .entry(branch.event_type.clone())
                                    .or_default()
                                    .push(state.id);
                            }
                        }
                    }
                }
                StateType::Negation => {
                    // Negation states track forbidden types
                    if let Some(ref neg_info) = state.negation_info {
                        if !neg_info.forbidden_type.is_empty() {
                            index
                                .state_index
                                .entry(neg_info.forbidden_type.clone())
                                .or_default()
                                .push(state.id);
                        }
                    }
                }
                _ => {
                    if let Some(ref event_type) = state.event_type {
                        index
                            .state_index
                            .entry(event_type.clone())
                            .or_default()
                            .push(state.id);
                    } else if state.state_type != StateType::Start
                        && state.state_type != StateType::Accept
                    {
                        // State can match any event type
                        index.wildcard_states.push(state.id);
                    }
                }
            }
        }

        index
    }

    /// Get states that might be interested in this event type
    pub fn get_candidate_states(&self, event_type: &str) -> impl Iterator<Item = usize> + '_ {
        let typed = self
            .state_index
            .get(event_type)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);

        typed
            .iter()
            .copied()
            .chain(self.wildcard_states.iter().copied())
    }

    /// Check if any state in the NFA expects this event type
    pub fn has_interest(&self, event_type: &str) -> bool {
        self.state_index.contains_key(event_type) || !self.wildcard_states.is_empty()
    }
}

impl Default for EventTypeIndex {
    fn default() -> Self {
        Self::new()
    }
}
