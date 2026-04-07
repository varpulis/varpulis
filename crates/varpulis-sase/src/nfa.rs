//! NFA state machine and compiler

use std::time::Duration;

use super::and_op::{AndBranch, AndConfig, NegationInfo};
use super::predicate::{classify_predicate, needs_deferred_evaluation, PredicateClass};
use super::types::{Predicate, SasePattern};

// ============================================================================
// NFA STATE MACHINE
// ============================================================================

/// NFA state types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateType {
    /// Initial state
    Start,
    /// Normal matching state
    Normal,
    /// Kleene closure state (loops back)
    Kleene,
    /// Negation state (invalidates on match)
    Negation,
    /// AND-01: State that waits for multiple events in any order
    And,
    /// Final accepting state
    Accept,
}

/// A single state in the SASE+ NFA.
#[derive(Debug, Clone)]
pub struct State {
    /// Unique identifier for this state within the NFA.
    pub id: usize,
    /// Classification of this state (Normal, Kleene, Negation, etc.).
    pub state_type: StateType,
    /// Event type to match (None for epsilon transitions)
    pub event_type: Option<String>,
    /// Predicate to evaluate
    pub predicate: Option<Predicate>,
    /// Alias for captured event
    pub alias: Option<String>,
    /// Epsilon transitions (no event consumed)
    pub epsilon_transitions: Vec<usize>,
    /// Event transitions: next state on match
    pub transitions: Vec<usize>,
    /// For Kleene states: self-loop transition
    pub self_loop: bool,
    /// Timeout for negation states
    pub timeout: Option<Duration>,
    /// AND-01: Configuration for AND states
    pub and_config: Option<AndConfig>,
    /// NEG-01: For negation states - info about what to forbid
    pub negation_info: Option<NegationInfo>,
    /// SIGMOD 2014: Inconsistent predicate postponed to enumeration phase
    pub postponed_predicate: Option<Predicate>,
    /// PERF: Pre-computed flag — true when an epsilon transition leads to Accept.
    /// Avoids per-event iteration over epsilon_transitions in Kleene hot paths.
    pub has_epsilon_to_accept: bool,
    /// Greedy emission mode for Kleene states with self-referencing predicates
    /// (e.g., `.increasing(field)` / `.decreasing(field)`). When true, the run
    /// accumulates events silently and emits the longest match when the self-loop
    /// predicate breaks. When false (default), follows standard SASE+ STAM
    /// semantics: emit on every Kleene match.
    pub is_greedy: bool,
}

impl State {
    /// Create a new state with the given id and type, no transitions.
    pub fn new(id: usize, state_type: StateType) -> Self {
        Self {
            id,
            state_type,
            event_type: None,
            predicate: None,
            alias: None,
            epsilon_transitions: Vec::new(),
            transitions: Vec::new(),
            self_loop: false,
            timeout: None,
            and_config: None,
            negation_info: None,
            postponed_predicate: None,
            has_epsilon_to_accept: false,
            is_greedy: false,
        }
    }

    /// Set the event type this state matches.
    pub fn with_event(mut self, event_type: String) -> Self {
        self.event_type = Some(event_type);
        self
    }

    /// Attach a predicate that must hold for a match at this state.
    pub fn with_predicate(mut self, predicate: Predicate) -> Self {
        self.predicate = Some(predicate);
        self
    }

    /// Set the alias under which the matched event is captured.
    pub fn with_alias(mut self, alias: String) -> Self {
        self.alias = Some(alias);
        self
    }

    /// Mark this state as having a self-loop (for Kleene closure).
    pub fn with_self_loop(mut self) -> Self {
        self.self_loop = true;
        self
    }
}

/// Non-deterministic finite automaton for SASE+ pattern matching.
#[derive(Debug)]
pub struct Nfa {
    /// All states in the automaton.
    pub states: Vec<State>,
    /// Index of the initial start state.
    pub start_state: usize,
    /// Indices of accepting (final) states.
    pub accept_states: Vec<usize>,
}

impl Nfa {
    /// Create a new NFA with a single start state.
    pub fn new() -> Self {
        let start = State::new(0, StateType::Start);
        Self {
            states: vec![start],
            start_state: 0,
            accept_states: Vec::new(),
        }
    }

    /// Add a state to the NFA, returning its assigned id.
    pub fn add_state(&mut self, state: State) -> usize {
        let id = self.states.len();
        let mut state = state;
        state.id = id;
        self.states.push(state);
        id
    }

    /// Add an event-consuming transition from state `from` to state `to`.
    pub fn add_transition(&mut self, from: usize, to: usize) {
        if let Some(state) = self.states.get_mut(from) {
            state.transitions.push(to);
        }
    }

    /// Add an epsilon (event-free) transition from state `from` to state `to`.
    pub fn add_epsilon(&mut self, from: usize, to: usize) {
        if let Some(state) = self.states.get_mut(from) {
            state.epsilon_transitions.push(to);
        }
    }

    /// Returns true if the NFA contains any greedy Kleene state (i.e., the
    /// pattern uses .increasing() / .decreasing() or a self-ref Kleene alias).
    /// Used by the engine to disable overlapping runs via skip-till-any-match.
    pub fn has_greedy_kleene(&self) -> bool {
        self.states.iter().any(|s| s.is_greedy)
    }

    /// Mark a state as an accepting (final) state.
    pub fn set_accept(&mut self, state_id: usize) {
        if let Some(state) = self.states.get_mut(state_id) {
            state.state_type = StateType::Accept;
        }
        if !self.accept_states.contains(&state_id) {
            self.accept_states.push(state_id);
        }
    }
}

impl Default for Nfa {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// NFA COMPILER
// ============================================================================

/// Compiles a SasePattern into an NFA
#[derive(Debug)]
pub struct NfaCompiler {
    nfa: Nfa,
}

impl NfaCompiler {
    /// Create a new NFA compiler with a fresh empty NFA.
    pub fn new() -> Self {
        Self { nfa: Nfa::new() }
    }

    /// Compile a [`SasePattern`] into a ready-to-use NFA.
    pub fn compile(mut self, pattern: &SasePattern) -> Nfa {
        let (_, end) = self.compile_pattern(pattern, self.nfa.start_state);
        self.nfa.set_accept(end);
        // PERF: Pre-compute has_epsilon_to_accept for each state
        let accept_flags: Vec<(usize, bool)> = self
            .nfa
            .states
            .iter()
            .map(|s| {
                let flag = s
                    .epsilon_transitions
                    .iter()
                    .any(|&eps_id| self.nfa.states[eps_id].state_type == StateType::Accept);
                (s.id, flag)
            })
            .collect();
        for (id, flag) in accept_flags {
            self.nfa.states[id].has_epsilon_to_accept = flag;
        }
        // Mark Kleene states whose predicate self-references their own alias
        // (the .increasing()/.decreasing() monotonic operator pattern). These
        // states use greedy emission semantics — accumulate then emit on break.
        let greedy_flags: Vec<(usize, bool)> = self
            .nfa
            .states
            .iter()
            .map(|s| {
                let is_self_ref_kleene = s.state_type == StateType::Kleene
                    && s.self_loop
                    && s.alias.as_ref().is_some_and(|a| {
                        s.predicate
                            .as_ref()
                            .is_some_and(|p| super::predicate::predicate_references_alias(p, a))
                    });
                (s.id, is_self_ref_kleene)
            })
            .collect();
        for (id, flag) in greedy_flags {
            self.nfa.states[id].is_greedy = flag;
        }
        self.nfa
    }

    /// Returns (start_state, end_state) for the compiled pattern
    fn compile_pattern(&mut self, pattern: &SasePattern, prev: usize) -> (usize, usize) {
        match pattern {
            SasePattern::Event {
                event_type,
                predicate,
                alias,
            } => {
                let mut state = State::new(0, StateType::Normal).with_event(event_type.clone());
                if let Some(p) = predicate {
                    state = state.with_predicate(p.clone());
                }
                if let Some(a) = alias {
                    state = state.with_alias(a.clone());
                }
                let id = self.nfa.add_state(state);
                self.nfa.add_transition(prev, id);
                (id, id)
            }

            SasePattern::Seq(patterns) => {
                let mut current = prev;
                let mut first_state = None;
                for p in patterns {
                    let (start, end) = self.compile_pattern(p, current);
                    if first_state.is_none() {
                        first_state = Some(start);
                    }
                    current = end;
                }
                (first_state.unwrap_or(prev), current)
            }

            SasePattern::And(left, right) => {
                // AND-01: Both patterns must match, in any order
                // Extract event info from both branches
                let (left_type, left_pred) = extract_event_info(left);
                let (right_type, right_pred) = extract_event_info(right);

                // Extract aliases
                let left_alias = extract_alias(left);
                let right_alias = extract_alias(right);

                // Create join state (where we go after both branches complete)
                let join = self.nfa.add_state(State::new(0, StateType::Normal));

                // Create AND state with configuration
                let mut and_state = State::new(0, StateType::And);
                and_state.and_config = Some(AndConfig {
                    branches: vec![
                        AndBranch {
                            event_type: left_type.unwrap_or_default(),
                            predicate: left_pred,
                            alias: left_alias,
                        },
                        AndBranch {
                            event_type: right_type.unwrap_or_default(),
                            predicate: right_pred,
                            alias: right_alias,
                        },
                    ],
                    join_state: join,
                });

                let and_state_id = self.nfa.add_state(and_state);
                self.nfa.add_transition(prev, and_state_id);

                (and_state_id, join)
            }

            SasePattern::Or(left, right) => {
                // For OR, either branch can match
                let fork = self.nfa.add_state(State::new(0, StateType::Normal));
                self.nfa.add_epsilon(prev, fork);

                let (_, left_end) = self.compile_pattern(left, fork);
                let (_, right_end) = self.compile_pattern(right, fork);

                let join = self.nfa.add_state(State::new(0, StateType::Normal));
                self.nfa.add_epsilon(left_end, join);
                self.nfa.add_epsilon(right_end, join);

                (fork, join)
            }

            SasePattern::Not(inner) => {
                // NEG-01: Temporal negation - pattern must NOT match during the window
                // Extract event type from inner pattern
                let (forbidden_type, predicate) = extract_event_info(inner);

                // Create negation state
                let mut neg_state = State::new(0, StateType::Negation);

                // Continue state (reached after negation is confirmed via timeout)
                let continue_state = self.nfa.add_state(State::new(0, StateType::Normal));

                // Store negation info for runtime processing
                neg_state.negation_info = Some(NegationInfo {
                    forbidden_type: forbidden_type.unwrap_or_default(),
                    predicate,
                    continue_state,
                });

                let neg_state_id = self.nfa.add_state(neg_state);
                self.nfa.add_transition(prev, neg_state_id);

                // Note: No epsilon transition to continue_state!
                // The transition happens only when negation is confirmed via watermark/timeout

                (neg_state_id, continue_state)
            }

            SasePattern::KleenePlus(inner) => {
                // A+: one or more - must match at least once, then can loop
                let (inner_start, inner_end) = self.compile_pattern(inner, prev);

                // Mark as Kleene state with self-loop
                if let Some(state) = self.nfa.states.get_mut(inner_end) {
                    state.state_type = StateType::Kleene;
                    state.self_loop = true;
                    // SIGMOD 2014 §5.2: Split predicate into eager vs postponed.
                    // Self-referencing CompareRef predicates (e.g., field > alias.field)
                    // are evaluated eagerly: captured[alias] tracks the last Kleene event
                    // via push_at_kleene(), enabling sliding-window comparison.
                    // Only Expr-based cross-references need deferred ZDD evaluation.
                    if let Some(ref pred) = state.predicate {
                        let alias = state.alias.as_deref();
                        if classify_predicate(pred, alias) == PredicateClass::Inconsistent
                            && needs_deferred_evaluation(pred)
                        {
                            state.postponed_predicate = state.predicate.take();
                        }
                    }
                }

                // Add back-edge for loop
                self.nfa.add_epsilon(inner_end, inner_start);

                // Continue state
                let continue_state = self.nfa.add_state(State::new(0, StateType::Normal));
                self.nfa.add_epsilon(inner_end, continue_state);

                (inner_start, continue_state)
            }

            SasePattern::KleeneStar(inner) => {
                // A*: zero or more - can skip entirely
                let skip_state = self.nfa.add_state(State::new(0, StateType::Normal));
                self.nfa.add_epsilon(prev, skip_state);

                let (inner_start, inner_end) = self.compile_pattern(inner, prev);

                if let Some(state) = self.nfa.states.get_mut(inner_end) {
                    state.state_type = StateType::Kleene;
                    state.self_loop = true;
                    // Same eager evaluation fix as KleenePlus
                    if let Some(ref pred) = state.predicate {
                        let alias = state.alias.as_deref();
                        if classify_predicate(pred, alias) == PredicateClass::Inconsistent
                            && needs_deferred_evaluation(pred)
                        {
                            state.postponed_predicate = state.predicate.take();
                        }
                    }
                }

                // Add back-edge for loop
                self.nfa.add_epsilon(inner_end, inner_start);

                // Both paths lead to continue
                let continue_state = self.nfa.add_state(State::new(0, StateType::Normal));
                self.nfa.add_epsilon(skip_state, continue_state);
                self.nfa.add_epsilon(inner_end, continue_state);

                (prev, continue_state)
            }

            SasePattern::Within(inner, duration) => {
                // Compile inner with timeout tracking
                let (start, end) = self.compile_pattern(inner, prev);
                // Timeout is tracked at runtime, not in NFA structure
                // We store it for runtime use
                if let Some(state) = self.nfa.states.get_mut(start) {
                    state.timeout = Some(*duration);
                }
                (start, end)
            }
        }
    }
}

impl Default for NfaCompiler {
    fn default() -> Self {
        Self::new()
    }
}

/// Extract event type and predicate from a pattern (for AND/NOT compilation)
fn extract_event_info(pattern: &SasePattern) -> (Option<String>, Option<Predicate>) {
    match pattern {
        SasePattern::Event {
            event_type,
            predicate,
            ..
        } => (Some(event_type.clone()), predicate.clone()),
        SasePattern::Seq(patterns) if !patterns.is_empty() => extract_event_info(&patterns[0]),
        _ => (None, None),
    }
}

/// Extract alias from a pattern
fn extract_alias(pattern: &SasePattern) -> Option<String> {
    match pattern {
        SasePattern::Event { alias, .. } => alias.clone(),
        SasePattern::Seq(patterns) if !patterns.is_empty() => extract_alias(&patterns[0]),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use varpulis_core::Value;

    use super::*;
    use crate::types::CompareOp;

    #[test]
    fn test_nfa_new_has_start_state() {
        let nfa = Nfa::new();
        assert_eq!(nfa.states.len(), 1);
        assert_eq!(nfa.start_state, 0);
        assert_eq!(nfa.states[0].state_type, StateType::Start);
        assert!(nfa.accept_states.is_empty());
    }

    #[test]
    fn test_nfa_add_state_assigns_sequential_ids() {
        let mut nfa = Nfa::new();
        let s1 = State::new(0, StateType::Normal).with_event("A".to_string());
        let s2 = State::new(0, StateType::Normal).with_event("B".to_string());
        let id1 = nfa.add_state(s1);
        let id2 = nfa.add_state(s2);
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        // State ids should be corrected to their actual position
        assert_eq!(nfa.states[id1].id, 1);
        assert_eq!(nfa.states[id2].id, 2);
        assert_eq!(nfa.states[id1].event_type.as_deref(), Some("A"));
        assert_eq!(nfa.states[id2].event_type.as_deref(), Some("B"));
    }

    #[test]
    fn test_nfa_add_transition_and_epsilon() {
        let mut nfa = Nfa::new();
        let id1 = nfa.add_state(State::new(0, StateType::Normal));
        let id2 = nfa.add_state(State::new(0, StateType::Normal));

        nfa.add_transition(0, id1);
        nfa.add_epsilon(id1, id2);

        assert_eq!(nfa.states[0].transitions, vec![id1]);
        assert_eq!(nfa.states[id1].epsilon_transitions, vec![id2]);
        // Ensure the other direction has no spurious transitions
        assert!(nfa.states[id2].transitions.is_empty());
        assert!(nfa.states[id2].epsilon_transitions.is_empty());
    }

    #[test]
    fn test_nfa_set_accept_marks_state() {
        let mut nfa = Nfa::new();
        let id = nfa.add_state(State::new(0, StateType::Normal));
        assert_eq!(nfa.states[id].state_type, StateType::Normal);

        nfa.set_accept(id);
        assert_eq!(nfa.states[id].state_type, StateType::Accept);
        assert_eq!(nfa.accept_states, vec![id]);

        // Calling set_accept again should not duplicate
        nfa.set_accept(id);
        assert_eq!(nfa.accept_states, vec![id]);
    }

    #[test]
    fn test_state_builder_methods() {
        let state = State::new(0, StateType::Normal)
            .with_event("Trade".to_string())
            .with_alias("t".to_string())
            .with_self_loop();

        assert_eq!(state.event_type.as_deref(), Some("Trade"));
        assert_eq!(state.alias.as_deref(), Some("t"));
        assert!(state.self_loop);
        assert!(state.predicate.is_none());
    }

    #[test]
    fn test_compile_simple_sequence() {
        // SEQ(A, B) => start --transition--> A --transition--> B (accept)
        let pattern = SasePattern::Seq(vec![
            SasePattern::Event {
                event_type: "A".to_string(),
                predicate: None,
                alias: Some("a".to_string()),
            },
            SasePattern::Event {
                event_type: "B".to_string(),
                predicate: None,
                alias: Some("b".to_string()),
            },
        ]);

        let nfa = NfaCompiler::new().compile(&pattern);

        // Should have: start(0), A(1), B(2)
        assert_eq!(nfa.states.len(), 3);
        assert_eq!(nfa.start_state, 0);
        assert_eq!(nfa.accept_states, vec![2]);
        assert_eq!(nfa.states[1].event_type.as_deref(), Some("A"));
        assert_eq!(nfa.states[1].alias.as_deref(), Some("a"));
        assert_eq!(nfa.states[2].event_type.as_deref(), Some("B"));
        assert_eq!(nfa.states[2].state_type, StateType::Accept);
        // start -> A transition
        assert!(nfa.states[0].transitions.contains(&1));
        // A -> B transition
        assert!(nfa.states[1].transitions.contains(&2));
    }

    #[test]
    fn test_compile_kleene_plus_has_self_loop() {
        // A+ => must match at least once, then loops
        let pattern = SasePattern::KleenePlus(Box::new(SasePattern::Event {
            event_type: "A".to_string(),
            predicate: None,
            alias: Some("a".to_string()),
        }));

        let nfa = NfaCompiler::new().compile(&pattern);

        // Find the Kleene state
        let kleene_states: Vec<_> = nfa
            .states
            .iter()
            .filter(|s| s.state_type == StateType::Kleene)
            .collect();
        assert_eq!(
            kleene_states.len(),
            1,
            "should have exactly one Kleene state"
        );
        assert!(
            kleene_states[0].self_loop,
            "Kleene state should have self-loop set"
        );

        // Kleene state should have an epsilon transition leading to the accept state
        assert!(
            nfa.accept_states.len() == 1,
            "should have exactly one accept state"
        );
    }

    #[test]
    fn test_compile_within_stores_timeout() {
        let pattern = SasePattern::Within(
            Box::new(SasePattern::Event {
                event_type: "A".to_string(),
                predicate: None,
                alias: None,
            }),
            Duration::from_secs(30),
        );

        let nfa = NfaCompiler::new().compile(&pattern);

        // The first compiled state (state 1) should carry the timeout
        let state_with_timeout = nfa.states.iter().find(|s| s.timeout.is_some());
        assert!(
            state_with_timeout.is_some(),
            "should have a state with timeout"
        );
        assert_eq!(
            state_with_timeout.unwrap().timeout,
            Some(Duration::from_secs(30))
        );
    }

    #[test]
    fn test_extract_event_info_from_simple_event() {
        let pattern = SasePattern::Event {
            event_type: "Trade".to_string(),
            predicate: Some(Predicate::Compare {
                field: "price".to_string(),
                op: CompareOp::Gt,
                value: Value::Float(100.0),
            }),
            alias: Some("t".to_string()),
        };
        let (etype, pred) = extract_event_info(&pattern);
        assert_eq!(etype.as_deref(), Some("Trade"));
        assert!(pred.is_some());
    }

    #[test]
    fn test_extract_event_info_from_seq() {
        // For a Seq, extract_event_info returns info from the first element
        let pattern = SasePattern::Seq(vec![
            SasePattern::Event {
                event_type: "First".to_string(),
                predicate: None,
                alias: None,
            },
            SasePattern::Event {
                event_type: "Second".to_string(),
                predicate: None,
                alias: None,
            },
        ]);
        let (etype, _) = extract_event_info(&pattern);
        assert_eq!(etype.as_deref(), Some("First"));
    }

    #[test]
    fn test_extract_alias_from_event() {
        let pattern = SasePattern::Event {
            event_type: "A".to_string(),
            predicate: None,
            alias: Some("myalias".to_string()),
        };
        assert_eq!(extract_alias(&pattern), Some("myalias".to_string()));
    }
}
