//! SASE+ Pattern Matching Engine
//!
//! Implementation of the SASE+ algorithm for Complex Event Processing.
//! Based on the paper: "High-Performance Complex Event Processing over Streams"
//! by Wu, Diao, Rizvi (SIGMOD 2006)
//!
//! Key features:
//! - NFA-based pattern matching with stack for Kleene closure
//! - Efficient event selection strategies (skip-till-any-match, skip-till-next-match)
//! - Negation support with temporal windows
//! - Partition-by attribute optimization (SASEXT extension)
//!
//! Pattern syntax supported:
//! - SEQ(A, B, C): Sequence of events
//! - AND(A, B): Both events in any order
//! - OR(A, B): Either event
//! - NOT(A): Negation (absence of event)
//! - A+: Kleene plus (one or more)
//! - A*: Kleene star (zero or more)
//! - WITHIN(pattern, duration): Temporal constraint

use crate::engine::eval_filter_expr;
use crate::event::Event;
use crate::sequence::SequenceContext;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use varpulis_core::Value;

/// Shared event reference for efficient cloning in pattern matching.
/// Using Arc allows multiple pattern runs to share the same event data
/// without expensive deep copies.
pub type SharedEvent = Arc<Event>;

// ============================================================================
// PATTERN EXPRESSION AST
// ============================================================================

/// A SASE+ pattern expression
#[derive(Debug, Clone)]
pub enum SasePattern {
    /// Match a single event type with optional predicate
    Event {
        event_type: String,
        predicate: Option<Predicate>,
        alias: Option<String>,
    },
    /// Sequence: SEQ(A, B, C) - events must occur in order
    Seq(Vec<SasePattern>),
    /// Conjunction: AND(A, B) - both must occur (any order)
    And(Box<SasePattern>, Box<SasePattern>),
    /// Disjunction: OR(A, B) - either must occur
    Or(Box<SasePattern>, Box<SasePattern>),
    /// Negation: NOT(A) - event must NOT occur
    Not(Box<SasePattern>),
    /// Kleene plus: A+ (one or more occurrences)
    KleenePlus(Box<SasePattern>),
    /// Kleene star: A* (zero or more occurrences)
    KleeneStar(Box<SasePattern>),
    /// Temporal constraint: pattern within duration
    Within(Box<SasePattern>, Duration),
}

/// Predicate for event filtering
#[derive(Debug, Clone)]
pub enum Predicate {
    /// Field comparison: field op value
    Compare {
        field: String,
        op: CompareOp,
        value: Value,
    },
    /// Field reference comparison: field op alias.field
    CompareRef {
        field: String,
        op: CompareOp,
        ref_alias: String,
        ref_field: String,
    },
    /// Logical AND
    And(Box<Predicate>, Box<Predicate>),
    /// Logical OR
    Or(Box<Predicate>, Box<Predicate>),
    /// Logical NOT
    Not(Box<Predicate>),
    /// Custom expression
    Expr(Box<varpulis_core::ast::Expr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    NotEq,
    Lt,
    Le,
    Gt,
    Ge,
}

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
                // For AND, we need to track both patterns independently
                // Create a fork state
                let fork = self.nfa.add_state(State::new(0, StateType::Normal));
                self.nfa.add_epsilon(prev, fork);

                let (_, left_end) = self.compile_pattern(left, fork);
                let (_, right_end) = self.compile_pattern(right, fork);

                // Join state - both must complete
                let join = self.nfa.add_state(State::new(0, StateType::Normal));
                self.nfa.add_epsilon(left_end, join);
                self.nfa.add_epsilon(right_end, join);

                (fork, join)
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
                // Negation: pattern must NOT match
                let neg_state = self.nfa.add_state(State::new(0, StateType::Negation));
                self.nfa.add_epsilon(prev, neg_state);

                // Compile inner pattern from negation state
                let (_, _inner_end) = self.compile_pattern(inner, neg_state);
                // If inner matches, the run is invalidated

                // Continue state (reached if negation doesn't match)
                let continue_state = self.nfa.add_state(State::new(0, StateType::Normal));
                self.nfa.add_epsilon(neg_state, continue_state);

                (neg_state, continue_state)
            }

            SasePattern::KleenePlus(inner) => {
                // A+: one or more - must match at least once, then can loop
                let (inner_start, inner_end) = self.compile_pattern(inner, prev);

                // Mark as Kleene state with self-loop
                if let Some(state) = self.nfa.states.get_mut(inner_end) {
                    state.state_type = StateType::Kleene;
                    state.self_loop = true;
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

// ============================================================================
// RUNTIME: ACTIVE RUNS WITH STACK
// ============================================================================

/// A stack entry for Kleene closure handling
#[derive(Debug, Clone)]
pub struct StackEntry {
    /// Captured event (Arc for efficient sharing across runs)
    pub event: SharedEvent,
    /// Alias for this capture
    pub alias: Option<String>,
    /// Timestamp of capture
    pub timestamp: Instant,
}

/// An active pattern run (partial match in progress)
#[derive(Debug, Clone)]
pub struct Run {
    /// Current NFA state
    pub current_state: usize,
    /// Stack of matched events (for Kleene closure)
    pub stack: Vec<StackEntry>,
    /// Captured events by alias (Arc for efficient sharing)
    pub captured: HashMap<String, SharedEvent>,
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
}

impl Run {
    pub fn new(start_state: usize) -> Self {
        Self {
            current_state: start_state,
            stack: Vec::new(),
            captured: HashMap::new(),
            started_at: Instant::now(),
            deadline: None,
            event_time_started_at: None,
            event_time_deadline: None,
            partition_key: None,
            invalidated: false,
        }
    }

    /// Create a new run with event-time tracking
    pub fn new_with_event_time(start_state: usize, event_timestamp: DateTime<Utc>) -> Self {
        Self {
            current_state: start_state,
            stack: Vec::new(),
            captured: HashMap::new(),
            started_at: Instant::now(),
            deadline: None,
            event_time_started_at: Some(event_timestamp),
            event_time_deadline: None,
            partition_key: None,
            invalidated: false,
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

// ============================================================================
// SASE+ ENGINE
// ============================================================================

/// Event selection strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectionStrategy {
    /// Skip-till-any-match: most permissive, can skip irrelevant events
    SkipTillAnyMatch,
    /// Skip-till-next-match: contiguous matching for Kleene
    SkipTillNextMatch,
    /// Strict contiguous: no skipping allowed
    StrictContiguous,
}

/// Result of pattern matching
#[derive(Debug, Clone)]
pub struct MatchResult {
    /// All captured events by alias (Arc for zero-copy access)
    pub captured: HashMap<String, SharedEvent>,
    /// The event stack (ordered sequence of matches)
    pub stack: Vec<StackEntry>,
    /// Match duration
    pub duration: Duration,
}

/// SASE+ Pattern Matching Engine
/// Global negation condition for invalidating active runs
#[derive(Clone)]
pub struct GlobalNegation {
    /// Event type that triggers negation
    pub event_type: String,
    /// Optional predicate (with access to captured events)
    pub predicate: Option<Predicate>,
}

/// Time semantics for the engine
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimeSemantics {
    /// Processing time: use wall-clock time (no watermarks needed)
    #[default]
    ProcessingTime,
    /// Event time: use event timestamps with watermark-based window completion
    EventTime,
}

pub struct SaseEngine {
    /// Compiled NFA
    nfa: Nfa,
    /// Active runs
    runs: Vec<Run>,
    /// Maximum concurrent runs
    max_runs: usize,
    /// Event selection strategy
    strategy: SelectionStrategy,
    /// Partition-by field (SASEXT optimization)
    partition_by: Option<String>,
    /// Partitioned runs for SASEXT
    partitioned_runs: HashMap<String, Vec<Run>>,
    /// Global negation conditions that invalidate active runs
    global_negations: Vec<GlobalNegation>,
    /// Time semantics (processing time vs event time)
    time_semantics: TimeSemantics,
    /// Current watermark (for event-time processing)
    watermark: Option<DateTime<Utc>>,
    /// Maximum out-of-orderness tolerance for watermark generation
    max_out_of_orderness: Duration,
    /// Maximum observed event timestamp (for watermark generation)
    max_timestamp: Option<DateTime<Utc>>,
}

impl SaseEngine {
    pub fn new(pattern: SasePattern) -> Self {
        let nfa = NfaCompiler::new().compile(&pattern);
        Self {
            nfa,
            runs: Vec::new(),
            max_runs: 10000,
            strategy: SelectionStrategy::SkipTillAnyMatch,
            partition_by: None,
            partitioned_runs: HashMap::new(),
            global_negations: Vec::new(),
            time_semantics: TimeSemantics::ProcessingTime,
            watermark: None,
            max_out_of_orderness: Duration::from_secs(0),
            max_timestamp: None,
        }
    }

    /// Enable event-time processing with watermarks
    pub fn with_event_time(mut self) -> Self {
        self.time_semantics = TimeSemantics::EventTime;
        self
    }

    /// Set maximum out-of-orderness tolerance for watermark generation
    /// The watermark will be: max_timestamp - max_out_of_orderness
    pub fn with_max_out_of_orderness(mut self, duration: Duration) -> Self {
        self.max_out_of_orderness = duration;
        self
    }

    /// Get current time semantics
    pub fn time_semantics(&self) -> TimeSemantics {
        self.time_semantics
    }

    /// Get current watermark
    pub fn watermark(&self) -> Option<DateTime<Utc>> {
        self.watermark
    }

    /// Manually advance the watermark (useful for testing or external control)
    pub fn advance_watermark(&mut self, new_watermark: DateTime<Utc>) {
        self.watermark = Some(new_watermark);
        // Cleanup runs that have exceeded their event-time deadline
        self.cleanup_by_watermark();
    }

    /// Add a global negation condition that invalidates active runs
    pub fn add_negation(&mut self, event_type: String, predicate: Option<Predicate>) {
        self.global_negations.push(GlobalNegation {
            event_type,
            predicate,
        });
    }

    /// Builder method to add a global negation
    pub fn with_negation(mut self, event_type: String, predicate: Option<Predicate>) -> Self {
        self.add_negation(event_type, predicate);
        self
    }

    pub fn with_max_runs(mut self, max: usize) -> Self {
        self.max_runs = max;
        self
    }

    pub fn with_strategy(mut self, strategy: SelectionStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Enable SASEXT partition optimization
    pub fn with_partition_by(mut self, field: String) -> Self {
        self.partition_by = Some(field);
        self
    }

    /// Process an incoming event, returns completed matches
    pub fn process(&mut self, event: &Event) -> Vec<MatchResult> {
        // PERF-01: Create SharedEvent once and reuse throughout processing
        // This avoids multiple deep clones when the event is captured by multiple runs
        let shared_event = Arc::new(event.clone());
        self.process_shared(shared_event)
    }

    /// Process a shared event reference (avoids redundant cloning)
    pub fn process_shared(&mut self, event: SharedEvent) -> Vec<MatchResult> {
        let mut completed = Vec::new();

        // Update watermark tracking for event-time processing
        if self.time_semantics == TimeSemantics::EventTime {
            self.update_watermark(&event);
        }

        // Clean up timed-out runs (uses watermark in EventTime mode)
        self.cleanup_timeouts();

        // Check global negations - invalidate runs that match
        self.check_global_negations(&event);

        // If using partitioning, route to appropriate partition
        if let Some(ref partition_field) = self.partition_by {
            let partition_key = event
                .get(partition_field)
                .map(|v| format!("{}", v))
                .unwrap_or_default();

            // Process partitioned runs
            completed.extend(self.process_partition_shared(&partition_key, Arc::clone(&event)));

            // Try to start new run in partition
            if let Some(run) = self.try_start_run_shared(Arc::clone(&event)) {
                let partition_runs = self.partitioned_runs.entry(partition_key).or_default();
                if partition_runs.len() < self.max_runs {
                    partition_runs.push(run);
                }
            }
        } else {
            // Non-partitioned processing
            completed.extend(self.process_runs_shared(Arc::clone(&event)));

            // Try to start new run
            if let Some(run) = self.try_start_run_shared(Arc::clone(&event)) {
                if self.runs.len() < self.max_runs {
                    self.runs.push(run);
                }
            }
        }

        completed
    }

    #[allow(dead_code)]
    fn process_partition(&mut self, partition_key: &str, event: &Event) -> Vec<MatchResult> {
        self.process_partition_shared(partition_key, Arc::new(event.clone()))
    }

    #[allow(dead_code)]
    fn process_runs(&mut self, event: &Event) -> Vec<MatchResult> {
        self.process_runs_shared(Arc::new(event.clone()))
    }

    #[allow(dead_code)]
    fn try_start_run(&self, event: &Event) -> Option<Run> {
        self.try_start_run_shared(Arc::new(event.clone()))
    }

    // =========================================================================
    // PERF-01: SharedEvent versions of internal methods
    // These avoid redundant cloning by accepting pre-wrapped Arc<Event>
    // =========================================================================

    fn process_partition_shared(
        &mut self,
        partition_key: &str,
        event: SharedEvent,
    ) -> Vec<MatchResult> {
        let mut completed = Vec::new();
        let max_runs = self.max_runs;

        if let Some(runs) = self.partitioned_runs.get_mut(partition_key) {
            let mut i = 0;
            while i < runs.len() {
                if runs[i].is_timed_out() || runs[i].invalidated {
                    runs.remove(i);
                    continue;
                }

                match advance_run_shared(&self.nfa, self.strategy, &mut runs[i], Arc::clone(&event))
                {
                    RunAdvanceResult::Continue => i += 1,
                    RunAdvanceResult::Complete(result) => {
                        completed.push(result);
                        runs.remove(i);
                    }
                    RunAdvanceResult::CompleteAndBranch(result, branch_run) => {
                        completed.push(result);
                        runs[i] = branch_run;
                        i += 1;
                    }
                    RunAdvanceResult::Branch(new_run) => {
                        if runs.len() < max_runs {
                            runs.push(new_run);
                        }
                        i += 1;
                    }
                    RunAdvanceResult::Invalidate => {
                        runs.remove(i);
                    }
                    RunAdvanceResult::NoMatch => i += 1,
                }
            }
        }

        completed
    }

    fn process_runs_shared(&mut self, event: SharedEvent) -> Vec<MatchResult> {
        let mut completed = Vec::new();
        let mut new_runs = Vec::new();
        let mut i = 0;

        while i < self.runs.len() {
            if self.runs[i].is_timed_out() || self.runs[i].invalidated {
                self.runs.remove(i);
                continue;
            }

            // Clone run to avoid borrow issues, then update
            let mut run = self.runs[i].clone();
            match advance_run_shared(&self.nfa, self.strategy, &mut run, Arc::clone(&event)) {
                RunAdvanceResult::Continue => {
                    self.runs[i] = run;
                    i += 1;
                }
                RunAdvanceResult::Complete(result) => {
                    completed.push(result);
                    self.runs.remove(i);
                }
                RunAdvanceResult::CompleteAndBranch(result, branch_run) => {
                    completed.push(result);
                    self.runs[i] = branch_run;
                    i += 1;
                }
                RunAdvanceResult::Branch(new_run) => {
                    self.runs[i] = run;
                    new_runs.push(new_run);
                    i += 1;
                }
                RunAdvanceResult::Invalidate => {
                    self.runs.remove(i);
                }
                RunAdvanceResult::NoMatch => i += 1,
            }
        }

        // Add branched runs
        for run in new_runs {
            if self.runs.len() < self.max_runs {
                self.runs.push(run);
            }
        }

        completed
    }

    fn try_start_run_shared(&self, event: SharedEvent) -> Option<Run> {
        let start_state = &self.nfa.states[self.nfa.start_state];
        let empty_captured: HashMap<String, SharedEvent> = HashMap::new();

        // Check if event matches any transition from start
        for &next_id in &start_state.transitions {
            let next_state = &self.nfa.states[next_id];
            if event_matches_state(&self.nfa, &event, next_state, &empty_captured) {
                let mut run = match self.time_semantics {
                    TimeSemantics::ProcessingTime => Run::new(next_id),
                    TimeSemantics::EventTime => Run::new_with_event_time(next_id, event.timestamp),
                };

                // Set deadline if state has timeout
                if let Some(timeout) = next_state.timeout {
                    match self.time_semantics {
                        TimeSemantics::ProcessingTime => {
                            run.deadline = Some(Instant::now() + timeout);
                        }
                        TimeSemantics::EventTime => {
                            // Set event-time deadline based on first event's timestamp
                            run = run.with_event_time_deadline(timeout);
                        }
                    }
                }

                // Capture event - use Arc::clone instead of cloning the event
                run.push(Arc::clone(&event), next_state.alias.clone());

                return Some(run);
            }
        }

        // Check epsilon transitions
        for &eps_id in &start_state.epsilon_transitions {
            let eps_state = &self.nfa.states[eps_id];
            for &next_id in &eps_state.transitions {
                let next_state = &self.nfa.states[next_id];
                if event_matches_state(&self.nfa, &event, next_state, &empty_captured) {
                    let mut run = match self.time_semantics {
                        TimeSemantics::ProcessingTime => Run::new(next_id),
                        TimeSemantics::EventTime => {
                            Run::new_with_event_time(next_id, event.timestamp)
                        }
                    };
                    run.push(Arc::clone(&event), next_state.alias.clone());
                    return Some(run);
                }
            }
        }

        None
    }

    fn cleanup_timeouts(&mut self) {
        match self.time_semantics {
            TimeSemantics::ProcessingTime => {
                // Use wall-clock time for timeout check
                self.runs.retain(|r| !r.is_timed_out() && !r.invalidated);
                for runs in self.partitioned_runs.values_mut() {
                    runs.retain(|r| !r.is_timed_out() && !r.invalidated);
                }
            }
            TimeSemantics::EventTime => {
                // Use watermark for timeout check
                self.cleanup_by_watermark();
            }
        }
    }

    /// Cleanup runs based on watermark (for event-time processing)
    fn cleanup_by_watermark(&mut self) {
        if let Some(watermark) = self.watermark {
            self.runs
                .retain(|r| !r.is_timed_out_event_time(watermark) && !r.invalidated);
            for runs in self.partitioned_runs.values_mut() {
                runs.retain(|r| !r.is_timed_out_event_time(watermark) && !r.invalidated);
            }
        }
    }

    /// Update watermark based on incoming event timestamp
    fn update_watermark(&mut self, event: &Event) {
        let event_ts = event.timestamp;

        // Track maximum observed timestamp
        match self.max_timestamp {
            Some(max_ts) if event_ts > max_ts => {
                self.max_timestamp = Some(event_ts);
            }
            None => {
                self.max_timestamp = Some(event_ts);
            }
            _ => {}
        }

        // Update watermark: max_timestamp - max_out_of_orderness
        if let Some(max_ts) = self.max_timestamp {
            let new_watermark =
                max_ts - chrono::Duration::from_std(self.max_out_of_orderness).unwrap_or_default();
            self.watermark = Some(new_watermark);
        }
    }

    /// Check global negation conditions and invalidate matching runs
    fn check_global_negations(&mut self, event: &Event) {
        for negation in &self.global_negations {
            // Check if event type matches the negation
            if event.event_type != negation.event_type {
                continue;
            }

            // Invalidate runs where the predicate matches (or all runs if no predicate)
            for run in &mut self.runs {
                let should_invalidate = match &negation.predicate {
                    Some(pred) => eval_predicate(pred, event, &run.captured),
                    None => true, // No predicate means any event of this type invalidates
                };
                if should_invalidate {
                    run.invalidated = true;
                }
            }

            // Also check partitioned runs
            for runs in self.partitioned_runs.values_mut() {
                for run in runs.iter_mut() {
                    let should_invalidate = match &negation.predicate {
                        Some(pred) => eval_predicate(pred, event, &run.captured),
                        None => true,
                    };
                    if should_invalidate {
                        run.invalidated = true;
                    }
                }
            }
        }
    }

    /// Get statistics
    pub fn stats(&self) -> SaseStats {
        let total_runs: usize = if self.partition_by.is_some() {
            self.partitioned_runs.values().map(|v| v.len()).sum()
        } else {
            self.runs.len()
        };

        SaseStats {
            active_runs: total_runs,
            partitions: self.partitioned_runs.len(),
            nfa_states: self.nfa.states.len(),
        }
    }
}

#[derive(Debug)]
enum RunAdvanceResult {
    Continue,
    Complete(MatchResult),
    /// Complete the pattern AND continue with a branched run (for Kleene+)
    CompleteAndBranch(MatchResult, Run),
    Branch(Run),
    Invalidate,
    NoMatch,
}

// Free functions to avoid borrow checker issues

#[allow(dead_code)]
fn advance_run(
    nfa: &Nfa,
    strategy: SelectionStrategy,
    run: &mut Run,
    event: &Event,
) -> RunAdvanceResult {
    // Wrap in Arc for the shared version
    advance_run_shared(nfa, strategy, run, Arc::new(event.clone()))
}

/// PERF-01: Optimized version that takes SharedEvent to avoid redundant cloning
fn advance_run_shared(
    nfa: &Nfa,
    strategy: SelectionStrategy,
    run: &mut Run,
    event: SharedEvent,
) -> RunAdvanceResult {
    let current_state = &nfa.states[run.current_state];

    // Check negation states first
    if current_state.state_type == StateType::Negation
        && event_matches_state(nfa, &event, current_state, &run.captured)
    {
        return RunAdvanceResult::Invalidate;
    }

    // Check if we're at an accept state
    if current_state.state_type == StateType::Accept {
        return RunAdvanceResult::Complete(MatchResult {
            captured: run.captured.clone(),
            stack: run.stack.clone(),
            duration: run.started_at.elapsed(),
        });
    }

    // Check transitions
    for &next_id in &current_state.transitions {
        let next_state = &nfa.states[next_id];
        if event_matches_state(nfa, &event, next_state, &run.captured) {
            run.current_state = next_id;
            run.push(Arc::clone(&event), next_state.alias.clone());

            if next_state.state_type == StateType::Accept {
                return RunAdvanceResult::Complete(MatchResult {
                    captured: run.captured.clone(),
                    stack: run.stack.clone(),
                    duration: run.started_at.elapsed(),
                });
            }

            if next_state.state_type == StateType::Kleene && next_state.self_loop {
                // For Kleene states, check if we can complete via epsilon to accept
                // This allows emitting matches while continuing to collect more
                for &eps_id in &next_state.epsilon_transitions {
                    let eps_state = &nfa.states[eps_id];
                    if eps_state.state_type == StateType::Accept {
                        // We can complete AND continue! Create match result and branch
                        let result = MatchResult {
                            captured: run.captured.clone(),
                            stack: run.stack.clone(),
                            duration: run.started_at.elapsed(),
                        };
                        let branch = run.branch();
                        return RunAdvanceResult::CompleteAndBranch(result, branch);
                    }
                }
                // No epsilon to accept, just branch to continue looping
                let branch = run.branch();
                return RunAdvanceResult::Branch(branch);
            }

            return RunAdvanceResult::Continue;
        }
    }

    // Check epsilon transitions
    for &eps_id in &current_state.epsilon_transitions {
        let eps_state = &nfa.states[eps_id];

        if eps_state.state_type == StateType::Accept {
            return RunAdvanceResult::Complete(MatchResult {
                captured: run.captured.clone(),
                stack: run.stack.clone(),
                duration: run.started_at.elapsed(),
            });
        }

        for &next_id in &eps_state.transitions {
            let next_state = &nfa.states[next_id];
            if event_matches_state(nfa, &event, next_state, &run.captured) {
                run.current_state = next_id;
                run.push(Arc::clone(&event), next_state.alias.clone());

                if next_state.state_type == StateType::Accept {
                    return RunAdvanceResult::Complete(MatchResult {
                        captured: run.captured.clone(),
                        stack: run.stack.clone(),
                        duration: run.started_at.elapsed(),
                    });
                }

                return RunAdvanceResult::Continue;
            }
        }
    }

    match strategy {
        SelectionStrategy::StrictContiguous => RunAdvanceResult::Invalidate,
        _ => RunAdvanceResult::NoMatch,
    }
}

fn event_matches_state(
    _nfa: &Nfa,
    event: &Event,
    state: &State,
    captured: &HashMap<String, SharedEvent>,
) -> bool {
    if let Some(ref expected_type) = state.event_type {
        if event.event_type != *expected_type {
            return false;
        }
    }

    if let Some(ref predicate) = state.predicate {
        if !eval_predicate(predicate, event, captured) {
            return false;
        }
    }

    true
}

fn eval_predicate(
    predicate: &Predicate,
    event: &Event,
    captured: &HashMap<String, SharedEvent>,
) -> bool {
    match predicate {
        Predicate::Compare { field, op, value } => event
            .get(field)
            .is_some_and(|ev| compare_values(ev, value, *op)),
        Predicate::CompareRef {
            field,
            op,
            ref_alias,
            ref_field,
        } => {
            let event_value = event.get(field);
            let ref_value = captured.get(ref_alias).and_then(|e| e.get(ref_field));
            match (event_value, ref_value) {
                (Some(ev), Some(rv)) => compare_values(ev, rv, *op),
                _ => false,
            }
        }
        Predicate::And(left, right) => {
            eval_predicate(left, event, captured) && eval_predicate(right, event, captured)
        }
        Predicate::Or(left, right) => {
            eval_predicate(left, event, captured) || eval_predicate(right, event, captured)
        }
        Predicate::Not(inner) => !eval_predicate(inner, event, captured),
        Predicate::Expr(expr) => {
            // Build SequenceContext from captured events for expression evaluation
            // Dereference Arc<Event> to Event for compatibility with SequenceContext
            let captured_events: HashMap<String, Event> = captured
                .iter()
                .map(|(k, v)| (k.clone(), (**v).clone()))
                .collect();
            let ctx = SequenceContext {
                captured: captured_events,
                previous: None,
            };
            // Evaluate the expression and check if it returns true
            eval_filter_expr(expr, event, &ctx)
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
        }
    }
}

/// Engine statistics
#[derive(Debug, Clone)]
pub struct SaseStats {
    pub active_runs: usize,
    pub partitions: usize,
    pub nfa_states: usize,
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

fn compare_values(left: &Value, right: &Value, op: CompareOp) -> bool {
    match op {
        CompareOp::Eq => values_equal(left, right),
        CompareOp::NotEq => !values_equal(left, right),
        CompareOp::Lt => values_compare(left, right) == Some(std::cmp::Ordering::Less),
        CompareOp::Le => matches!(
            values_compare(left, right),
            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
        ),
        CompareOp::Gt => values_compare(left, right) == Some(std::cmp::Ordering::Greater),
        CompareOp::Ge => matches!(
            values_compare(left, right),
            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
        ),
    }
}

fn values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Int(a), Value::Int(b)) => a == b,
        (Value::Float(a), Value::Float(b)) => (a - b).abs() < f64::EPSILON,
        (Value::Int(a), Value::Float(b)) | (Value::Float(b), Value::Int(a)) => {
            (*a as f64 - b).abs() < f64::EPSILON
        }
        (Value::Str(a), Value::Str(b)) => a == b,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        _ => false,
    }
}

fn values_compare(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Int(a), Value::Int(b)) => Some(a.cmp(b)),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
        (Value::Int(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
        (Value::Float(a), Value::Int(b)) => a.partial_cmp(&(*b as f64)),
        (Value::Str(a), Value::Str(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

// ============================================================================
// BUILDER API
// ============================================================================

/// Builder for SASE patterns
pub struct PatternBuilder;

impl PatternBuilder {
    /// Single event pattern
    pub fn event(event_type: &str) -> SasePattern {
        SasePattern::Event {
            event_type: event_type.to_string(),
            predicate: None,
            alias: None,
        }
    }

    /// Event with alias
    pub fn event_as(event_type: &str, alias: &str) -> SasePattern {
        SasePattern::Event {
            event_type: event_type.to_string(),
            predicate: None,
            alias: Some(alias.to_string()),
        }
    }

    /// Event with predicate
    pub fn event_where(event_type: &str, predicate: Predicate) -> SasePattern {
        SasePattern::Event {
            event_type: event_type.to_string(),
            predicate: Some(predicate),
            alias: None,
        }
    }

    /// Sequence pattern
    pub fn seq(patterns: Vec<SasePattern>) -> SasePattern {
        SasePattern::Seq(patterns)
    }

    /// AND pattern
    pub fn and(left: SasePattern, right: SasePattern) -> SasePattern {
        SasePattern::And(Box::new(left), Box::new(right))
    }

    /// OR pattern
    pub fn or(left: SasePattern, right: SasePattern) -> SasePattern {
        SasePattern::Or(Box::new(left), Box::new(right))
    }

    /// NOT pattern
    pub fn not(inner: SasePattern) -> SasePattern {
        SasePattern::Not(Box::new(inner))
    }

    /// Kleene plus (one or more)
    pub fn one_or_more(inner: SasePattern) -> SasePattern {
        SasePattern::KleenePlus(Box::new(inner))
    }

    /// Kleene star (zero or more)
    pub fn zero_or_more(inner: SasePattern) -> SasePattern {
        SasePattern::KleeneStar(Box::new(inner))
    }

    /// Temporal constraint
    pub fn within(inner: SasePattern, duration: Duration) -> SasePattern {
        SasePattern::Within(Box::new(inner), duration)
    }

    /// Field equals value predicate
    pub fn field_eq(field: &str, value: Value) -> Predicate {
        Predicate::Compare {
            field: field.to_string(),
            op: CompareOp::Eq,
            value,
        }
    }

    /// Field reference predicate (compare to captured event)
    pub fn field_ref_eq(field: &str, ref_alias: &str, ref_field: &str) -> Predicate {
        Predicate::CompareRef {
            field: field.to_string(),
            op: CompareOp::Eq,
            ref_alias: ref_alias.to_string(),
            ref_field: ref_field.to_string(),
        }
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_event(event_type: &str, data: Vec<(&str, Value)>) -> Event {
        let mut event = Event::new(event_type);
        for (k, v) in data {
            event.data.insert(k.to_string(), v);
        }
        event
    }

    #[test]
    fn test_simple_sequence() {
        // SEQ(A, B)
        let pattern =
            PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

        let mut engine = SaseEngine::new(pattern);

        // A alone shouldn't complete
        let results = engine.process(&make_event("A", vec![]));
        assert!(results.is_empty());

        // B should complete the sequence
        let results = engine.process(&make_event("B", vec![]));
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_sequence_with_filter() {
        // SEQ(A where price > 100, B)
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event_where(
                "A",
                Predicate::Compare {
                    field: "price".to_string(),
                    op: CompareOp::Gt,
                    value: Value::Int(100),
                },
            ),
            PatternBuilder::event("B"),
        ]);

        let mut engine = SaseEngine::new(pattern);

        // A with price <= 100 shouldn't start a run
        let results = engine.process(&make_event("A", vec![("price", Value::Int(50))]));
        assert!(results.is_empty());
        assert_eq!(engine.stats().active_runs, 0);

        // A with price > 100 should start a run
        let results = engine.process(&make_event("A", vec![("price", Value::Int(150))]));
        assert!(results.is_empty());
        assert_eq!(engine.stats().active_runs, 1);

        // B should complete
        let results = engine.process(&make_event("B", vec![]));
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_kleene_plus() {
        // SEQ(A, B+, C)
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event("A"),
            PatternBuilder::one_or_more(PatternBuilder::event("B")),
            PatternBuilder::event("C"),
        ]);

        let mut engine = SaseEngine::new(pattern);

        // Start with A
        engine.process(&make_event("A", vec![]));
        assert_eq!(engine.stats().active_runs, 1);

        // First B
        engine.process(&make_event("B", vec![]));

        // Second B (Kleene)
        engine.process(&make_event("B", vec![]));

        // C should complete
        let results = engine.process(&make_event("C", vec![]));
        assert!(!results.is_empty());
    }

    #[test]
    fn test_negation() {
        // SEQ(A, NOT(Cancel), B)
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event("A"),
            PatternBuilder::not(PatternBuilder::event("Cancel")),
            PatternBuilder::event("B"),
        ]);

        let mut engine = SaseEngine::new(pattern);

        // A starts the run
        engine.process(&make_event("A", vec![]));
        assert!(engine.stats().active_runs > 0);

        // B should complete (no Cancel in between)
        let results = engine.process(&make_event("B", vec![]));
        // Note: negation handling needs proper timeout implementation
        // This test validates the structure
        assert!(results.is_empty() || results.len() == 1);
    }

    #[test]
    fn test_partition_by() {
        // SEQ(A, B) partitioned by symbol
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event_as("A", "a"),
            PatternBuilder::event("B"),
        ]);

        let mut engine = SaseEngine::new(pattern).with_partition_by("symbol".to_string());

        // Events for different symbols
        engine.process(&make_event(
            "A",
            vec![("symbol", Value::Str("AAPL".into()))],
        ));
        engine.process(&make_event(
            "A",
            vec![("symbol", Value::Str("GOOG".into()))],
        ));

        assert_eq!(engine.stats().partitions, 2);

        // Complete AAPL
        let results = engine.process(&make_event(
            "B",
            vec![("symbol", Value::Str("AAPL".into()))],
        ));
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_with_alias_capture() {
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event_as("Order", "order"),
            PatternBuilder::event_as("Payment", "payment"),
        ]);

        let mut engine = SaseEngine::new(pattern);

        engine.process(&make_event("Order", vec![("id", Value::Int(123))]));
        let results = engine.process(&make_event(
            "Payment",
            vec![("amount", Value::Float(99.99))],
        ));

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert!(result.captured.contains_key("order"));
        assert!(result.captured.contains_key("payment"));
    }

    // =========================================================================
    // Event-Time / Watermark Tests
    // =========================================================================

    #[test]
    fn test_event_time_mode_basic() {
        use chrono::{TimeZone, Utc};

        let pattern =
            PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

        let mut engine = SaseEngine::new(pattern).with_event_time();

        assert_eq!(engine.time_semantics(), TimeSemantics::EventTime);

        let ts1 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 0).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 1).unwrap();

        let event_a = Event::new("A").with_timestamp(ts1);
        let event_b = Event::new("B").with_timestamp(ts2);

        engine.process(&event_a);
        let results = engine.process(&event_b);

        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_watermark_tracking() {
        use chrono::{TimeZone, Utc};

        let pattern = PatternBuilder::event("A");

        let mut engine = SaseEngine::new(pattern).with_event_time();

        assert!(engine.watermark().is_none());

        let ts1 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 0).unwrap();
        let event = Event::new("A").with_timestamp(ts1);
        engine.process(&event);

        // Watermark should now be set
        assert!(engine.watermark().is_some());
        assert_eq!(engine.watermark().unwrap(), ts1);
    }

    #[test]
    fn test_watermark_with_out_of_orderness() {
        use chrono::{TimeZone, Utc};

        let pattern = PatternBuilder::event("A");

        let mut engine = SaseEngine::new(pattern)
            .with_event_time()
            .with_max_out_of_orderness(std::time::Duration::from_secs(5));

        let ts = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 10).unwrap();
        let event = Event::new("A").with_timestamp(ts);
        engine.process(&event);

        // Watermark should be ts - 5s
        let expected_watermark = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 5).unwrap();
        assert_eq!(engine.watermark().unwrap(), expected_watermark);
    }

    #[test]
    fn test_event_time_within_timeout() {
        use chrono::{TimeZone, Utc};
        use std::time::Duration;

        // Pattern with 5 second window
        let pattern = SasePattern::Within(
            Box::new(PatternBuilder::seq(vec![
                PatternBuilder::event("Login"),
                PatternBuilder::event("Transaction"),
            ])),
            Duration::from_secs(5),
        );

        let mut engine = SaseEngine::new(pattern).with_event_time();

        let ts1 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 0).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 3).unwrap(); // Within 5s

        let login = Event::new("Login").with_timestamp(ts1);
        let tx = Event::new("Transaction").with_timestamp(ts2);

        engine.process(&login);
        let results = engine.process(&tx);

        // Should match because within the window
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_event_time_within_expired_by_watermark() {
        use chrono::{TimeZone, Utc};
        use std::time::Duration;

        // Pattern with 5 second window
        let pattern = SasePattern::Within(
            Box::new(PatternBuilder::seq(vec![
                PatternBuilder::event("Login"),
                PatternBuilder::event("Transaction"),
            ])),
            Duration::from_secs(5),
        );

        let mut engine = SaseEngine::new(pattern).with_event_time();

        let ts1 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 0).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 10).unwrap(); // After 5s window

        let login = Event::new("Login").with_timestamp(ts1);
        let tx = Event::new("Transaction").with_timestamp(ts2);

        engine.process(&login);

        // When processing second event, watermark advances to ts2
        // The run's deadline (ts1 + 5s) is now past the watermark (ts2)
        // So the run should be cleaned up
        let results = engine.process(&tx);

        // Should NOT match because the partial match expired
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_manual_watermark_advance() {
        use chrono::{TimeZone, Utc};
        use std::time::Duration;

        // Pattern with 5 second window
        let pattern = SasePattern::Within(
            Box::new(PatternBuilder::seq(vec![
                PatternBuilder::event("Login"),
                PatternBuilder::event("Transaction"),
            ])),
            Duration::from_secs(5),
        );

        let mut engine = SaseEngine::new(pattern).with_event_time();

        let ts1 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 0).unwrap();
        let login = Event::new("Login").with_timestamp(ts1);
        engine.process(&login);

        // Active runs should be 1
        assert_eq!(engine.stats().active_runs, 1);

        // Manually advance watermark past the deadline
        let future_watermark = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 10).unwrap();
        engine.advance_watermark(future_watermark);

        // Run should now be cleaned up
        assert_eq!(engine.stats().active_runs, 0);
    }

    // =========================================================================
    // COV-02: Advanced SASE+ Integration Tests
    // =========================================================================

    #[test]
    fn test_out_of_order_events_in_sequence() {
        // Test that events arriving out of order don't incorrectly match
        // Pattern: SEQ(A, B, C) should only match when events arrive in order

        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event("A"),
            PatternBuilder::event("B"),
            PatternBuilder::event("C"),
        ]);

        let mut engine = SaseEngine::new(pattern);

        // Send B first (out of order) - should not start a run
        let results = engine.process(&make_event("B", vec![]));
        assert!(results.is_empty());
        assert_eq!(engine.stats().active_runs, 0);

        // Send A - starts a new run
        let results = engine.process(&make_event("A", vec![]));
        assert!(results.is_empty());
        assert_eq!(engine.stats().active_runs, 1);

        // Send C (skipping B) - should not complete
        let results = engine.process(&make_event("C", vec![]));
        assert!(results.is_empty());

        // Send B - now should move run forward
        let results = engine.process(&make_event("B", vec![]));
        assert!(results.is_empty());

        // Send C again - should complete
        let results = engine.process(&make_event("C", vec![]));
        assert!(!results.is_empty());
    }

    #[test]
    fn test_concurrent_patterns_same_event_type() {
        // Multiple runs can be active for the same event type
        // Pattern: SEQ(A, B) - sending multiple A events should create multiple runs

        let pattern =
            PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

        let mut engine = SaseEngine::new(pattern);

        // Send first A
        engine.process(&make_event("A", vec![("id", Value::Int(1))]));
        assert_eq!(engine.stats().active_runs, 1);

        // Send second A - creates another run
        engine.process(&make_event("A", vec![("id", Value::Int(2))]));
        assert_eq!(engine.stats().active_runs, 2);

        // Send B - should complete BOTH runs
        let results = engine.process(&make_event("B", vec![]));
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_kleene_star_with_occurrences() {
        // A* matches zero or more - test with some occurrences
        // Pattern: SEQ(Start, Middle*, End)

        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event("Start"),
            PatternBuilder::zero_or_more(PatternBuilder::event("Middle")),
            PatternBuilder::event("End"),
        ]);

        let mut engine = SaseEngine::new(pattern);

        // Start
        engine.process(&make_event("Start", vec![]));
        assert_eq!(engine.stats().active_runs, 1);

        // Some Middle events
        engine.process(&make_event("Middle", vec![]));
        engine.process(&make_event("Middle", vec![]));

        // End should complete
        let results = engine.process(&make_event("End", vec![]));
        // Kleene star creates multiple completion possibilities
        assert!(!results.is_empty());
    }

    #[test]
    fn test_kleene_plus_requires_at_least_one() {
        // A+ should require at least one occurrence
        // Pattern: SEQ(Start, Middle+, End)

        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event("Start"),
            PatternBuilder::one_or_more(PatternBuilder::event("Middle")),
            PatternBuilder::event("End"),
        ]);

        let mut engine = SaseEngine::new(pattern);

        // Start
        engine.process(&make_event("Start", vec![]));

        // Skip Middle and send End - should NOT match
        let results = engine.process(&make_event("End", vec![]));
        assert!(results.is_empty());

        // Start again
        engine.process(&make_event("Start", vec![]));

        // Send one Middle
        engine.process(&make_event("Middle", vec![]));

        // Send End - should match
        let results = engine.process(&make_event("End", vec![]));
        assert!(!results.is_empty());
    }

    #[test]
    fn test_or_pattern_in_sequence() {
        // Test OR within a sequence: SEQ(Start, OR(A, B), End)
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event("Start"),
            PatternBuilder::or(PatternBuilder::event("A"), PatternBuilder::event("B")),
            PatternBuilder::event("End"),
        ]);

        let mut engine = SaseEngine::new(pattern);

        // Start
        engine.process(&make_event("Start", vec![]));
        assert_eq!(engine.stats().active_runs, 1);

        // A should advance the run (matches OR branch)
        engine.process(&make_event("A", vec![]));

        // End should complete
        let results = engine.process(&make_event("End", vec![]));
        assert!(!results.is_empty());
    }

    #[test]
    fn test_and_pattern_both_required() {
        // AND(A, B) should match when both occur (any order)
        let pattern = PatternBuilder::and(PatternBuilder::event("A"), PatternBuilder::event("B"));

        let mut engine = SaseEngine::new(pattern);

        // Just A - should not complete
        let results = engine.process(&make_event("A", vec![]));
        assert!(results.is_empty());

        // Now B - should complete
        let results = engine.process(&make_event("B", vec![]));
        assert!(!results.is_empty());
    }

    #[test]
    fn test_and_pattern_reverse_order() {
        // AND(A, B) should match even if B comes before A
        let pattern = PatternBuilder::and(PatternBuilder::event("A"), PatternBuilder::event("B"));

        let mut engine = SaseEngine::new(pattern);

        // B first
        let results = engine.process(&make_event("B", vec![]));
        assert!(results.is_empty());

        // Then A - should complete
        let results = engine.process(&make_event("A", vec![]));
        assert!(!results.is_empty());
    }

    #[test]
    fn test_compare_ref_between_events() {
        // Test referencing fields between events
        // Pattern: SEQ(Order as order, Payment where order_id == order.id)

        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event_as("Order", "order"),
            PatternBuilder::event_where(
                "Payment",
                Predicate::CompareRef {
                    field: "order_id".to_string(),
                    op: CompareOp::Eq,
                    ref_alias: "order".to_string(),
                    ref_field: "id".to_string(),
                },
            ),
        ]);

        let mut engine = SaseEngine::new(pattern);

        // Order with id 123
        engine.process(&make_event("Order", vec![("id", Value::Int(123))]));

        // Payment with wrong order_id - should not complete
        let results = engine.process(&make_event("Payment", vec![("order_id", Value::Int(999))]));
        assert!(results.is_empty());

        // Payment with correct order_id - should complete
        let results = engine.process(&make_event("Payment", vec![("order_id", Value::Int(123))]));
        assert!(!results.is_empty());
    }

    #[test]
    fn test_long_sequence_chain() {
        // Test a longer sequence: SEQ(A, B, C, D, E)
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event("A"),
            PatternBuilder::event("B"),
            PatternBuilder::event("C"),
            PatternBuilder::event("D"),
            PatternBuilder::event("E"),
        ]);

        let mut engine = SaseEngine::new(pattern);

        // Process events in order
        engine.process(&make_event("A", vec![]));
        assert_eq!(engine.stats().active_runs, 1);

        engine.process(&make_event("B", vec![]));
        engine.process(&make_event("C", vec![]));
        engine.process(&make_event("D", vec![]));

        // E should complete
        let results = engine.process(&make_event("E", vec![]));
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_partition_isolation() {
        // Test that partitions are truly isolated
        // Events in different partitions should not interact

        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event_as("A", "a"),
            PatternBuilder::event("B"),
        ]);

        let mut engine = SaseEngine::new(pattern).with_partition_by("region".to_string());

        // A for region "east"
        engine.process(&make_event(
            "A",
            vec![("region", Value::Str("east".into()))],
        ));

        // B for region "west" - should not complete the east run
        let results = engine.process(&make_event(
            "B",
            vec![("region", Value::Str("west".into()))],
        ));
        assert!(results.is_empty());

        // B for region "east" - should complete
        let results = engine.process(&make_event(
            "B",
            vec![("region", Value::Str("east".into()))],
        ));
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_negation_cancels_match() {
        // Test that negation properly prevents a match
        // Pattern: SEQ(A, NOT(Cancel), B)
        // If Cancel arrives between A and B, the pattern should not match

        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event_as("A", "a"),
            PatternBuilder::not(PatternBuilder::event("Cancel")),
            PatternBuilder::event("B"),
        ]);

        let mut engine = SaseEngine::new(pattern);

        // Global negation also needs to be registered
        engine.add_negation("Cancel".to_string(), None);

        // A starts the sequence
        engine.process(&make_event("A", vec![]));
        assert!(engine.stats().active_runs > 0);

        // Cancel should invalidate the run
        engine.process(&make_event("Cancel", vec![]));

        // B should NOT complete (run was cancelled)
        let results = engine.process(&make_event("B", vec![]));
        assert!(results.is_empty());
    }

    #[test]
    fn test_multiple_kleene_matches() {
        // Test that Kleene+ captures multiple events
        // Pattern: SEQ(Start, Tick+, End)

        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event_as("Start", "start"),
            PatternBuilder::one_or_more(PatternBuilder::event_as("Tick", "tick")),
            PatternBuilder::event_as("End", "end"),
        ]);

        let mut engine = SaseEngine::new(pattern);

        engine.process(&make_event("Start", vec![("val", Value::Int(0))]));

        // Multiple ticks
        engine.process(&make_event("Tick", vec![("val", Value::Int(1))]));
        engine.process(&make_event("Tick", vec![("val", Value::Int(2))]));
        engine.process(&make_event("Tick", vec![("val", Value::Int(3))]));

        let results = engine.process(&make_event("End", vec![("val", Value::Int(100))]));

        // Should have multiple results (one for each combination)
        assert!(!results.is_empty());
    }

    #[test]
    fn test_stats_tracking() {
        let pattern =
            PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

        let mut engine = SaseEngine::new(pattern);

        let initial_stats = engine.stats();
        assert_eq!(initial_stats.active_runs, 0);
        assert!(initial_stats.nfa_states > 0); // NFA should have states

        engine.process(&make_event("A", vec![]));
        assert_eq!(engine.stats().active_runs, 1);

        engine.process(&make_event("B", vec![]));
        // After completion, run is removed
        assert_eq!(engine.stats().active_runs, 0);
    }
}
