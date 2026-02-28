//! NFA state machine and compiler

use super::and_op::{AndBranch, AndConfig, NegationInfo};
use super::predicate::{classify_predicate, PredicateClass};
use super::types::{Predicate, SasePattern};
use std::time::Duration;

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

/// NFA state
#[derive(Debug, Clone)]
pub struct State {
    pub id: usize,
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
}

impl State {
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
        }
    }

    pub fn with_event(mut self, event_type: String) -> Self {
        self.event_type = Some(event_type);
        self
    }

    pub fn with_predicate(mut self, predicate: Predicate) -> Self {
        self.predicate = Some(predicate);
        self
    }

    pub fn with_alias(mut self, alias: String) -> Self {
        self.alias = Some(alias);
        self
    }

    pub fn with_self_loop(mut self) -> Self {
        self.self_loop = true;
        self
    }
}

/// NFA for pattern matching
#[derive(Debug)]
pub struct Nfa {
    pub states: Vec<State>,
    pub start_state: usize,
    pub accept_states: Vec<usize>,
}

impl Nfa {
    pub fn new() -> Self {
        let start = State::new(0, StateType::Start);
        Self {
            states: vec![start],
            start_state: 0,
            accept_states: Vec::new(),
        }
    }

    pub fn add_state(&mut self, state: State) -> usize {
        let id = self.states.len();
        let mut state = state;
        state.id = id;
        self.states.push(state);
        id
    }

    pub fn add_transition(&mut self, from: usize, to: usize) {
        if let Some(state) = self.states.get_mut(from) {
            state.transitions.push(to);
        }
    }

    pub fn add_epsilon(&mut self, from: usize, to: usize) {
        if let Some(state) = self.states.get_mut(from) {
            state.epsilon_transitions.push(to);
        }
    }

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
pub struct NfaCompiler {
    nfa: Nfa,
}

impl NfaCompiler {
    pub fn new() -> Self {
        Self { nfa: Nfa::new() }
    }

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
                    if let Some(ref pred) = state.predicate {
                        let alias = state.alias.as_deref();
                        if classify_predicate(pred, alias) == PredicateClass::Inconsistent {
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
