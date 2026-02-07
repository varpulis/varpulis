//! # NFA encoded in ZDD
//!
//! Represents NFA states and transitions as ZDD families.
//!
//! ## Encoding Strategy
//!
//! Each query has its own independent NFA states. The ZDD represents the
//! set of currently active states across all queries.
//!
//! - Each NFA state `s` gets a unique ZDD variable index
//! - A set of active states is represented as a ZDD family
//! - Transitions are applied by ZDD operations
//!
//! ## Key Insight
//!
//! ZDD's canonical reduction automatically shares common substructures.
//! When multiple queries have similar state patterns, the ZDD will
//! share nodes, providing "free" structural sharing.

use crate::greta::QueryId;
use rustc_hash::FxHashMap;
use smallvec::SmallVec;
use std::sync::Arc;
use varpulis_zdd::Zdd;

/// ZDD variable index representing an NFA state
pub type ZddVar = u32;

/// State in the ZDD-encoded NFA
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ZddState {
    /// Query this state belongs to
    pub query_id: QueryId,
    /// State index within the query's NFA
    pub state_index: u16,
    /// ZDD variable index for this state
    pub zdd_var: ZddVar,
}

impl ZddState {
    /// Create a new ZDD state
    pub fn new(query_id: QueryId, state_index: u16, zdd_var: ZddVar) -> Self {
        Self {
            query_id,
            state_index,
            zdd_var,
        }
    }
}

/// Transition in the ZDD-encoded NFA
#[derive(Debug, Clone)]
pub struct ZddTransition {
    /// Source state
    pub from: ZddState,
    /// Target state
    pub to: ZddState,
    /// Event type index that triggers this transition
    pub event_type: u16,
    /// Whether this is a Kleene self-loop
    pub is_kleene: bool,
}

impl ZddTransition {
    /// Create a new transition
    pub fn new(from: ZddState, to: ZddState, event_type: u16) -> Self {
        Self {
            from,
            to,
            event_type,
            is_kleene: from.state_index == to.state_index && from.query_id == to.query_id,
        }
    }
}

/// Per-query state tracking
#[derive(Debug, Clone, Default)]
struct QueryStateTracker {
    /// Initial state variable
    initial_var: ZddVar,
    /// Final state variables
    final_vars: SmallVec<[ZddVar; 4]>,
    /// Current count of completed trends
    count: u64,
    /// Is the query currently in an active state (past initial)?
    is_active: bool,
    /// Current active state variable (if any)
    current_state: Option<ZddVar>,
}

/// NFA structure encoded in ZDD
#[derive(Debug)]
pub struct NfaZdd {
    /// All states indexed by ZDD variable
    states: Vec<ZddState>,
    /// State lookup by (query_id, state_index)
    state_lookup: FxHashMap<(QueryId, u16), ZddVar>,
    /// Transitions indexed by event type
    transitions_by_event: FxHashMap<u16, Vec<ZddTransition>>,
    /// Initial states per query
    initial_states: FxHashMap<QueryId, ZddVar>,
    /// Final states per query
    final_states: FxHashMap<QueryId, SmallVec<[ZddVar; 4]>>,
    /// Event type name to index mapping
    type_indices: FxHashMap<Arc<str>, u16>,
    /// Number of registered queries
    num_queries: usize,
    /// Per-query state trackers
    query_trackers: FxHashMap<QueryId, QueryStateTracker>,
    /// Active states as a simple set (for efficiency)
    active_state_set: FxHashMap<ZddVar, bool>,
    /// ZDD for structural sharing visualization (optional)
    active_states_zdd: Zdd,
}

impl NfaZdd {
    /// Create a new NFA-ZDD
    pub fn new() -> Self {
        Self {
            states: Vec::new(),
            state_lookup: FxHashMap::default(),
            transitions_by_event: FxHashMap::default(),
            initial_states: FxHashMap::default(),
            final_states: FxHashMap::default(),
            type_indices: FxHashMap::default(),
            num_queries: 0,
            query_trackers: FxHashMap::default(),
            active_state_set: FxHashMap::default(),
            active_states_zdd: Zdd::empty(),
        }
    }

    /// Register an event type
    pub fn register_type(&mut self, name: Arc<str>) -> u16 {
        let len = self.type_indices.len() as u16;
        *self.type_indices.entry(name).or_insert(len)
    }

    /// Get type index
    #[inline]
    pub fn type_index(&self, name: &str) -> Option<u16> {
        self.type_indices.get(name).copied()
    }

    /// Add a state to the NFA
    pub fn add_state(&mut self, query_id: QueryId, state_index: u16) -> ZddState {
        let zdd_var = self.states.len() as ZddVar;
        let state = ZddState::new(query_id, state_index, zdd_var);

        self.states.push(state);
        self.state_lookup.insert((query_id, state_index), zdd_var);
        self.num_queries = self.num_queries.max(query_id as usize + 1);

        state
    }

    /// Mark a state as initial for its query
    pub fn set_initial(&mut self, state: ZddState) {
        self.initial_states.insert(state.query_id, state.zdd_var);

        // Initialize query tracker
        self.query_trackers
            .entry(state.query_id)
            .or_insert_with(|| QueryStateTracker {
                initial_var: state.zdd_var,
                final_vars: SmallVec::new(),
                count: 0,
                is_active: false,
                current_state: None,
            })
            .initial_var = state.zdd_var;
    }

    /// Mark a state as final for its query
    pub fn add_final(&mut self, state: ZddState) {
        self.final_states
            .entry(state.query_id)
            .or_default()
            .push(state.zdd_var);

        // Update query tracker
        if let Some(tracker) = self.query_trackers.get_mut(&state.query_id) {
            if !tracker.final_vars.contains(&state.zdd_var) {
                tracker.final_vars.push(state.zdd_var);
            }
        }
    }

    /// Add a transition
    pub fn add_transition(&mut self, from: ZddState, to: ZddState, event_type: u16) {
        let transition = ZddTransition::new(from, to, event_type);
        self.transitions_by_event
            .entry(event_type)
            .or_default()
            .push(transition);
    }

    /// Get a state by (query_id, state_index)
    #[inline]
    pub fn state(&self, query_id: QueryId, state_index: u16) -> Option<&ZddState> {
        self.state_lookup
            .get(&(query_id, state_index))
            .and_then(|&var| self.states.get(var as usize))
    }

    /// Get state by ZDD variable
    #[inline]
    pub fn state_by_var(&self, var: ZddVar) -> Option<&ZddState> {
        self.states.get(var as usize)
    }

    /// Get transitions for an event type
    #[inline]
    pub fn transitions(&self, event_type: u16) -> &[ZddTransition] {
        self.transitions_by_event
            .get(&event_type)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Initialize active states (all initial states)
    pub fn initialize(&mut self) {
        self.active_state_set.clear();

        // Set all initial states as active
        for (&query_id, &var) in &self.initial_states {
            self.active_state_set.insert(var, true);

            if let Some(tracker) = self.query_trackers.get_mut(&query_id) {
                tracker.count = 0;
                tracker.is_active = false;
                tracker.current_state = Some(var);
            }
        }

        // Build ZDD representation
        self.rebuild_zdd();
    }

    /// Rebuild ZDD from active state set
    fn rebuild_zdd(&mut self) {
        let mut zdd = Zdd::empty();
        for (&var, &active) in &self.active_state_set {
            if active {
                let singleton = Zdd::singleton(var);
                zdd = zdd.union(&singleton);
            }
        }
        self.active_states_zdd = zdd;
    }

    /// Process an event and return updated counts
    pub fn process_event(&mut self, event_type: u16) -> Vec<(QueryId, u64)> {
        let transitions = match self.transitions_by_event.get(&event_type) {
            Some(t) => t.clone(),
            None => return Vec::new(),
        };

        let mut new_active: FxHashMap<ZddVar, bool> = FxHashMap::default();
        let mut query_updates: FxHashMap<QueryId, (bool, Option<ZddVar>)> = FxHashMap::default();

        for transition in &transitions {
            // Check if source state is active
            let source_active = self
                .active_state_set
                .get(&transition.from.zdd_var)
                .copied()
                .unwrap_or(false);

            if source_active {
                let query_id = transition.from.query_id;

                // Add target state to new active set
                new_active.insert(transition.to.zdd_var, true);

                // For Kleene, keep source state active too
                if transition.is_kleene {
                    new_active.insert(transition.from.zdd_var, true);
                }

                // Track query state changes
                let entry = query_updates.entry(query_id).or_insert((false, None));

                // Check if we're leaving initial state
                if transition.from.zdd_var
                    == self.initial_states.get(&query_id).copied().unwrap_or(0)
                    && transition.from.zdd_var != transition.to.zdd_var
                {
                    entry.0 = true; // Query became active
                }

                entry.1 = Some(transition.to.zdd_var);

                // Check if target is final
                if self.is_final_state(transition.to) {
                    if let Some(tracker) = self.query_trackers.get_mut(&query_id) {
                        tracker.count += 1;
                    }
                }
            }
        }

        // Merge new active states with existing ones that weren't affected
        // Keep states for queries that didn't have any transitions fire
        let queries_with_transitions: std::collections::HashSet<_> =
            query_updates.keys().copied().collect();

        for (&var, &active) in &self.active_state_set {
            if active {
                if let Some(state) = self.states.get(var as usize) {
                    // Keep this state if its query had no transitions
                    if !queries_with_transitions.contains(&state.query_id) {
                        new_active.insert(var, true);
                    }
                }
            }
        }

        self.active_state_set = new_active;

        // Keep initial states active for queries that haven't started yet
        for (&query_id, &initial_var) in &self.initial_states {
            if !self
                .query_trackers
                .get(&query_id)
                .map(|t| t.is_active)
                .unwrap_or(false)
            {
                self.active_state_set.insert(initial_var, true);
            }
        }

        // Update query trackers
        for (query_id, (became_active, new_state)) in query_updates {
            if let Some(tracker) = self.query_trackers.get_mut(&query_id) {
                if became_active {
                    tracker.is_active = true;
                }
                tracker.current_state = new_state;
            }
        }

        // Rebuild ZDD
        self.rebuild_zdd();

        // Return current counts
        self.query_trackers
            .iter()
            .filter(|(_, tracker)| tracker.count > 0)
            .map(|(&id, tracker)| (id, tracker.count))
            .collect()
    }

    /// Check if a state is final
    fn is_final_state(&self, state: ZddState) -> bool {
        self.final_states
            .get(&state.query_id)
            .map(|finals| finals.contains(&state.zdd_var))
            .unwrap_or(false)
    }

    /// Get final count for a query
    #[inline]
    pub fn count(&self, query_id: QueryId) -> u64 {
        self.query_trackers
            .get(&query_id)
            .map(|t| t.count)
            .unwrap_or(0)
    }

    /// Reset for new window
    pub fn reset(&mut self) {
        self.initialize();
    }

    /// Number of states
    #[inline]
    pub fn num_states(&self) -> usize {
        self.states.len()
    }

    /// Number of queries
    #[inline]
    pub fn num_queries(&self) -> usize {
        self.num_queries
    }

    /// Get the current ZDD size (number of nodes)
    #[inline]
    pub fn zdd_size(&self) -> usize {
        self.active_states_zdd.node_count()
    }

    /// Check if a query is currently active (past initial state)
    pub fn is_query_active(&self, query_id: QueryId) -> bool {
        self.query_trackers
            .get(&query_id)
            .map(|t| t.is_active)
            .unwrap_or(false)
    }
}

impl Default for NfaZdd {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for constructing NFA-ZDD from queries
#[derive(Debug, Default)]
pub struct NfaZddBuilder {
    nfa: NfaZdd,
}

impl NfaZddBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self { nfa: NfaZdd::new() }
    }

    /// Add a simple sequence query (A -> B -> C)
    pub fn add_sequence(&mut self, query_id: QueryId, types: &[&str]) -> &mut Self {
        // Register types and create states
        let mut states: Vec<ZddState> = Vec::new();

        for (i, &type_name) in types.iter().enumerate() {
            let _type_idx = self.nfa.register_type(Arc::from(type_name));
            let state = self.nfa.add_state(query_id, i as u16);
            states.push(state);

            if i == 0 {
                self.nfa.set_initial(state);
            }
        }

        // Add final state
        let final_state = self.nfa.add_state(query_id, types.len() as u16);
        states.push(final_state);
        self.nfa.add_final(final_state);

        // Add transitions
        for (i, &type_name) in types.iter().enumerate() {
            let type_idx = self.nfa.type_index(type_name).unwrap();
            self.nfa.add_transition(states[i], states[i + 1], type_idx);
        }

        self
    }

    /// Add Kleene closure to a state (type+ self-loop)
    pub fn add_kleene(
        &mut self,
        query_id: QueryId,
        type_name: &str,
        at_state_index: u16,
    ) -> &mut Self {
        let type_idx = self.nfa.register_type(Arc::from(type_name));

        if let Some(&state_var) = self.nfa.state_lookup.get(&(query_id, at_state_index)) {
            if let Some(state) = self.nfa.states.get(state_var as usize).cloned() {
                self.nfa.add_transition(state, state, type_idx);
            }
        }

        self
    }

    /// Build the NFA-ZDD
    pub fn build(mut self) -> NfaZdd {
        self.nfa.initialize();
        self.nfa
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nfa_zdd_construction() {
        let mut builder = NfaZddBuilder::new();
        builder.add_sequence(0, &["A", "B"]);
        let nfa = builder.build();

        assert_eq!(nfa.num_queries(), 1);
        assert_eq!(nfa.num_states(), 3); // s0, s1, s2
    }

    #[test]
    fn test_simple_sequence_processing() {
        let mut builder = NfaZddBuilder::new();
        builder.add_sequence(0, &["A", "B"]);
        let mut nfa = builder.build();

        let a_idx = nfa.type_index("A").unwrap();
        let b_idx = nfa.type_index("B").unwrap();

        // Process A
        let results = nfa.process_event(a_idx);
        assert!(results.is_empty()); // No match yet

        // Process B
        let results = nfa.process_event(b_idx);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], (0, 1)); // Query 0, count 1
    }

    #[test]
    fn test_kleene_sequence() {
        let mut builder = NfaZddBuilder::new();
        builder.add_sequence(0, &["A", "B"]);
        builder.add_kleene(0, "B", 1); // B+ at state 1
        let mut nfa = builder.build();

        let a_idx = nfa.type_index("A").unwrap();
        let b_idx = nfa.type_index("B").unwrap();

        // Process A
        nfa.process_event(a_idx);

        // Process B three times (Kleene)
        nfa.process_event(b_idx);
        nfa.process_event(b_idx);
        let results = nfa.process_event(b_idx);

        // Should have 3 matches (one for each B reaching final state)
        assert_eq!(results[0].1, 3);
    }

    #[test]
    fn test_multi_query() {
        let mut builder = NfaZddBuilder::new();

        // q0: A -> B
        builder.add_sequence(0, &["A", "B"]);

        // q1: C -> B
        builder.add_sequence(1, &["C", "B"]);

        let mut nfa = builder.build();

        let a_idx = nfa.type_index("A").unwrap();
        let b_idx = nfa.type_index("B").unwrap();
        let c_idx = nfa.type_index("C").unwrap();

        // Process A (activates q0)
        nfa.process_event(a_idx);
        assert!(nfa.is_query_active(0));
        assert!(!nfa.is_query_active(1));

        // Process C (activates q1)
        nfa.process_event(c_idx);
        assert!(nfa.is_query_active(1));

        // Process B (completes both)
        let results = nfa.process_event(b_idx);

        // Both queries should match
        assert_eq!(results.len(), 2);
        assert!(results.iter().any(|(id, count)| *id == 0 && *count == 1));
        assert!(results.iter().any(|(id, count)| *id == 1 && *count == 1));
    }
}
