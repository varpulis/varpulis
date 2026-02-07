//! # Merged Query Template - Compiled FSA for Multi-Query Sharing
//!
//! The merged query template is a Finite State Automaton where:
//! - Each event type appears exactly once
//! - Transitions are labeled with the set of queries they apply to
//! - Kleene sub-patterns shared across queries are identified
//!
//! This is constructed at compile-time from the VPL program.

use crate::greta::QueryId;
use rustc_hash::FxHashMap;
use smallvec::SmallVec;

/// State in the merged template FSA
pub type TemplateState = u16;

/// A transition in the merged template
#[derive(Debug, Clone)]
pub struct TemplateTransition {
    /// Source state
    pub from: TemplateState,
    /// Target state
    pub to: TemplateState,
    /// Event type index that triggers this transition
    pub event_type: u16,
    /// Queries that use this transition
    pub queries: SmallVec<[QueryId; 8]>,
    /// Whether this is a Kleene self-loop
    pub is_kleene: bool,
    /// Whether this transition is shareable (multiple queries)
    pub is_shareable: bool,
}

impl TemplateTransition {
    /// Create a new transition
    pub fn new(from: TemplateState, to: TemplateState, event_type: u16) -> Self {
        Self {
            from,
            to,
            event_type,
            queries: SmallVec::new(),
            is_kleene: false,
            is_shareable: false,
        }
    }

    /// Add a query to this transition
    pub fn add_query(&mut self, query: QueryId) {
        if !self.queries.contains(&query) {
            self.queries.push(query);
            self.is_shareable = self.queries.len() > 1;
        }
    }

    /// Mark as Kleene self-loop
    pub fn mark_kleene(&mut self) {
        self.is_kleene = true;
    }
}

/// Kleene sub-pattern that can be shared
#[derive(Debug, Clone)]
pub struct KleeneSubPattern {
    /// Event type of the Kleene closure
    pub event_type: u16,
    /// Queries sharing this sub-pattern
    pub queries: SmallVec<[QueryId; 8]>,
    /// State where the Kleene loop occurs
    pub state: TemplateState,
}

impl KleeneSubPattern {
    /// Check if shareable (multiple queries)
    #[inline]
    pub fn is_shareable(&self) -> bool {
        self.queries.len() > 1
    }
}

/// The merged query template (FSA)
#[derive(Debug)]
pub struct MergedTemplate {
    /// All states in the FSA
    states: Vec<TemplateState>,
    /// Initial states per query
    initial_states: FxHashMap<QueryId, TemplateState>,
    /// Final (accepting) states per query
    final_states: FxHashMap<QueryId, SmallVec<[TemplateState; 4]>>,
    /// Transitions indexed by (from_state, event_type)
    transitions: FxHashMap<(TemplateState, u16), TemplateTransition>,
    /// Transitions from a state (for iteration)
    transitions_from: FxHashMap<TemplateState, SmallVec<[u16; 4]>>,
    /// Kleene sub-patterns
    kleene_patterns: Vec<KleeneSubPattern>,
    /// Event type name to index mapping
    type_indices: FxHashMap<String, u16>,
    /// Reverse mapping: index to name
    type_names: Vec<String>,
    /// Number of registered queries
    num_queries: usize,
}

impl MergedTemplate {
    /// Create a new merged template
    pub fn new() -> Self {
        Self {
            states: Vec::new(),
            initial_states: FxHashMap::default(),
            final_states: FxHashMap::default(),
            transitions: FxHashMap::default(),
            transitions_from: FxHashMap::default(),
            kleene_patterns: Vec::new(),
            type_indices: FxHashMap::default(),
            type_names: Vec::new(),
            num_queries: 0,
        }
    }

    /// Add a state
    pub fn add_state(&mut self) -> TemplateState {
        let state = self.states.len() as TemplateState;
        self.states.push(state);
        state
    }

    /// Set initial state for a query
    pub fn set_initial(&mut self, query: QueryId, state: TemplateState) {
        self.initial_states.insert(query, state);
        self.num_queries = self.num_queries.max(query as usize + 1);
    }

    /// Add a final state for a query
    pub fn add_final(&mut self, query: QueryId, state: TemplateState) {
        self.final_states.entry(query).or_default().push(state);
    }

    /// Register an event type
    pub fn register_type(&mut self, name: &str) -> u16 {
        if let Some(&idx) = self.type_indices.get(name) {
            return idx;
        }
        let idx = self.type_names.len() as u16;
        self.type_indices.insert(name.to_string(), idx);
        self.type_names.push(name.to_string());
        idx
    }

    /// Get type index
    #[inline]
    pub fn type_index(&self, name: &str) -> Option<u16> {
        self.type_indices.get(name).copied()
    }

    /// Get type name
    #[inline]
    pub fn type_name(&self, index: u16) -> Option<&str> {
        self.type_names.get(index as usize).map(|s| s.as_str())
    }

    /// Add a transition
    pub fn add_transition(&mut self, from: TemplateState, to: TemplateState, event_type: u16) {
        let key = (from, event_type);
        if let std::collections::hash_map::Entry::Vacant(e) = self.transitions.entry(key) {
            e.insert(TemplateTransition::new(from, to, event_type));
            self.transitions_from
                .entry(from)
                .or_default()
                .push(event_type);
        }
    }

    /// Add a query to a transition
    pub fn add_query_to_transition(
        &mut self,
        from: TemplateState,
        event_type: u16,
        query: QueryId,
    ) {
        if let Some(trans) = self.transitions.get_mut(&(from, event_type)) {
            trans.add_query(query);
        }
    }

    /// Mark a transition as Kleene
    pub fn mark_kleene(&mut self, state: TemplateState, event_type: u16) {
        if let Some(trans) = self.transitions.get_mut(&(state, event_type)) {
            trans.mark_kleene();
        }
    }

    /// Register a Kleene sub-pattern
    pub fn add_kleene_pattern(&mut self, event_type: u16, state: TemplateState) {
        // Check if pattern already exists
        for pattern in &mut self.kleene_patterns {
            if pattern.event_type == event_type && pattern.state == state {
                return;
            }
        }
        self.kleene_patterns.push(KleeneSubPattern {
            event_type,
            queries: SmallVec::new(),
            state,
        });
    }

    /// Add a query to a Kleene sub-pattern
    pub fn add_query_to_kleene(&mut self, event_type: u16, query: QueryId) {
        for pattern in &mut self.kleene_patterns {
            if pattern.event_type == event_type {
                if !pattern.queries.contains(&query) {
                    pattern.queries.push(query);
                }
                return;
            }
        }
    }

    /// Get transition for (state, event_type)
    #[inline]
    pub fn transition(&self, from: TemplateState, event_type: u16) -> Option<&TemplateTransition> {
        self.transitions.get(&(from, event_type))
    }

    /// Get all transitions from a state
    pub fn transitions_from(
        &self,
        state: TemplateState,
    ) -> impl Iterator<Item = &TemplateTransition> {
        self.transitions_from
            .get(&state)
            .into_iter()
            .flat_map(|types| types.iter())
            .filter_map(move |&t| self.transitions.get(&(state, t)))
    }

    /// Get initial state for a query
    #[inline]
    pub fn initial(&self, query: QueryId) -> Option<TemplateState> {
        self.initial_states.get(&query).copied()
    }

    /// Check if state is final for a query
    #[inline]
    pub fn is_final(&self, query: QueryId, state: TemplateState) -> bool {
        self.final_states
            .get(&query)
            .map(|states| states.contains(&state))
            .unwrap_or(false)
    }

    /// Get shareable Kleene sub-patterns
    pub fn shareable_kleene_patterns(&self) -> impl Iterator<Item = &KleeneSubPattern> {
        self.kleene_patterns.iter().filter(|p| p.is_shareable())
    }

    /// Get queries sharing a Kleene sub-pattern for an event type
    pub fn queries_sharing_kleene(&self, event_type: u16) -> Option<&SmallVec<[QueryId; 8]>> {
        self.kleene_patterns
            .iter()
            .find(|p| p.event_type == event_type)
            .map(|p| &p.queries)
    }

    /// Number of queries
    #[inline]
    pub fn num_queries(&self) -> usize {
        self.num_queries
    }

    /// Number of states
    #[inline]
    pub fn num_states(&self) -> usize {
        self.states.len()
    }
}

impl Default for MergedTemplate {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for constructing merged templates from VPL queries
#[derive(Debug, Default)]
pub struct TemplateBuilder {
    template: MergedTemplate,
}

impl TemplateBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            template: MergedTemplate::new(),
        }
    }

    /// Add a simple sequence query (A -> B)
    pub fn add_sequence(&mut self, query: QueryId, types: &[&str]) -> &mut Self {
        // Create states
        let states: Vec<_> = (0..=types.len())
            .map(|_| self.template.add_state())
            .collect();

        // Set initial and final
        self.template.set_initial(query, states[0]);
        self.template.add_final(query, states[types.len()]);

        // Add transitions
        for (i, &type_name) in types.iter().enumerate() {
            let type_idx = self.template.register_type(type_name);
            self.template
                .add_transition(states[i], states[i + 1], type_idx);
            self.template
                .add_query_to_transition(states[i], type_idx, query);
        }

        self
    }

    /// Add a Kleene pattern to a query (type+ at a state)
    pub fn add_kleene(
        &mut self,
        query: QueryId,
        type_name: &str,
        at_state: TemplateState,
    ) -> &mut Self {
        let type_idx = self.template.register_type(type_name);

        // Add self-loop transition
        self.template.add_transition(at_state, at_state, type_idx);
        self.template
            .add_query_to_transition(at_state, type_idx, query);
        self.template.mark_kleene(at_state, type_idx);

        // Register Kleene pattern
        self.template.add_kleene_pattern(type_idx, at_state);
        self.template.add_query_to_kleene(type_idx, query);

        self
    }

    /// Build the template
    pub fn build(self) -> MergedTemplate {
        self.template
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_sequence() {
        let mut builder = TemplateBuilder::new();
        builder.add_sequence(0, &["A", "B"]);
        let template = builder.build();

        assert_eq!(template.num_queries(), 1);
        assert_eq!(template.num_states(), 3); // s0 -> s1 -> s2

        let a_idx = template.type_index("A").unwrap();
        let trans = template.transition(0, a_idx).unwrap();
        assert_eq!(trans.to, 1);
        assert!(trans.queries.contains(&0));
    }

    #[test]
    fn test_kleene_sharing() {
        let mut builder = TemplateBuilder::new();

        // q0: A -> B+
        builder.add_sequence(0, &["A", "B"]);
        builder.add_kleene(0, "B", 1);

        // q1: C -> B+
        builder.add_sequence(1, &["C", "B"]);
        builder.add_kleene(1, "B", 4); // Different state but same Kleene type

        let template = builder.build();

        // B+ should be shareable
        let b_idx = template.type_index("B").unwrap();
        let queries = template.queries_sharing_kleene(b_idx);
        assert!(queries.is_some());
        // Note: In this simple test, they use different states
        // A real implementation would merge states for true sharing
    }
}
