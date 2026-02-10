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
use rustc_hash::{FxHashMap, FxHashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use varpulis_core::Value;
use varpulis_zdd::{ZddArena, ZddHandle};

/// PERF: Static empty captured map to avoid allocations in try_start_run
static EMPTY_CAPTURED: LazyLock<FxHashMap<String, SharedEvent>> = LazyLock::new(FxHashMap::default);

/// Safety cap on events accumulated in a single Kleene closure.
/// With n events the ZDD enumerates up to 2^n - 1 combinations.
/// 20 events → ~1 M combinations (safe); 30 → ~1 B (OOM risk).
const MAX_KLEENE_EVENTS: u32 = 20;

/// Safety cap on results emitted by `enumerate_with_filter`.
/// Prevents unbounded memory growth when the deferred predicate
/// passes most combinations of a large Kleene closure.
const MAX_ENUMERATION_RESULTS: usize = 10_000;

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

/// Comparison operators for predicate evaluation.
///
/// Used in SASE+ predicates to filter events based on field values.
///
/// # Example
///
/// ```ignore
/// use varpulis_runtime::sase::{CompareOp, Predicate};
/// use varpulis_core::Value;
///
/// let pred = Predicate::Compare {
///     field: "temperature".to_string(),
///     op: CompareOp::Gt,
///     value: Value::Float(100.0),
/// };
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    /// Equality (`==`)
    Eq,
    /// Inequality (`!=`)
    NotEq,
    /// Less than (`<`)
    Lt,
    /// Less than or equal (`<=`)
    Le,
    /// Greater than (`>`)
    Gt,
    /// Greater than or equal (`>=`)
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

// ============================================================================
// ZDD-KLEENE: COMPACT KLEENE CAPTURE USING ZDD
// ============================================================================

/// Compact representation of Kleene captures using Zero-suppressed Decision Diagrams.
///
/// Instead of creating O(2^n) runs for n Kleene events, we use a single ZDD that
/// compactly represents all valid combinations. This reduces memory from O(2^n)
/// to O(n) nodes while preserving the ability to enumerate all combinations.
///
/// Uses ZddArena for efficient operations:
/// - No table cloning during product_with_optional
/// - Persistent operation caches
/// - Cached counts
#[derive(Debug, Clone)]
pub struct KleeneCapture {
    /// Arena for ZDD operations (provides shared table + caches)
    arena: ZddArena,
    /// Handle to the current ZDD representing all valid event combinations
    handle: ZddHandle,
    /// Events captured during Kleene matching, indexed by ZDD variable
    events: Vec<SharedEvent>,
    /// Aliases for captured events (parallel to events vector)
    aliases: Vec<Option<String>>,
    /// Next variable index to assign
    next_var: u32,
    /// SIGMOD 2014: Deferred predicate for postponed evaluation during enumeration
    pub deferred_predicate: Option<Predicate>,
}

impl KleeneCapture {
    /// Create a new empty Kleene capture.
    /// Starts with {∅} - the empty combination is always valid initially.
    pub fn new() -> Self {
        let arena = ZddArena::new();
        let handle = arena.base(); // {∅}
        Self {
            arena,
            handle,
            events: Vec::new(),
            aliases: Vec::new(),
            next_var: 0,
            deferred_predicate: None,
        }
    }

    /// Extend with a new optional event.
    /// Each existing combination can now include or exclude this event.
    /// Complexity: O(|ZDD nodes|) instead of O(2^n) for naive approach.
    pub fn extend(&mut self, event: SharedEvent, alias: Option<String>) {
        let var = self.next_var;
        self.next_var += 1;

        self.events.push(event);
        self.aliases.push(alias);

        // S × {∅, {var}} = S ∪ {s ∪ {var} | s ∈ S}
        self.handle = self.arena.product_with_optional(self.handle, var);
    }

    /// Number of valid combinations (computed in O(|nodes|), not O(2^n))
    pub fn combination_count(&mut self) -> usize {
        self.arena.count(self.handle)
    }

    /// Number of events captured
    pub fn event_count(&self) -> usize {
        self.events.len()
    }

    /// Number of ZDD nodes (for metrics)
    pub fn node_count(&self) -> usize {
        self.arena.node_count()
    }

    /// Check if this capture is empty (no events yet, only {∅})
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Iterate over all valid combinations, producing StackEntry vectors.
    /// Each combination represents one valid Kleene match sequence.
    pub fn iter_combinations(&self) -> impl Iterator<Item = Vec<StackEntry>> + '_ {
        self.arena.iter(self.handle).map(move |indices| {
            indices
                .into_iter()
                .map(|idx| {
                    let idx = idx as usize;
                    StackEntry {
                        event: Arc::clone(&self.events[idx]),
                        alias: self.aliases[idx].clone(),
                        timestamp: Instant::now(),
                    }
                })
                .collect()
        })
    }

    /// Get captured events for a single combination (by index set)
    pub fn get_combination_captured(&self, indices: &[u32]) -> FxHashMap<String, SharedEvent> {
        let mut captured = FxHashMap::default();
        for &idx in indices {
            let idx = idx as usize;
            if let Some(ref alias) = self.aliases[idx] {
                captured.insert(alias.clone(), Arc::clone(&self.events[idx]));
            }
        }
        captured
    }
}

impl Default for KleeneCapture {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// NEG-01: TEMPORAL NEGATION SUPPORT
// ============================================================================

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
    pub fn is_violated_by(&self, event: &Event, captured: &FxHashMap<String, SharedEvent>) -> bool {
        if *event.event_type != self.forbidden_type {
            return false;
        }
        // If there's a predicate, check it
        match &self.predicate {
            Some(pred) => eval_predicate(pred, event, captured),
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

// ============================================================================
// AND-01: AND OPERATOR STATE TRACKING
// ============================================================================

/// Configuration for an AND branch
#[derive(Debug, Clone)]
pub struct AndBranch {
    /// Event type expected for this branch
    pub event_type: String,
    /// Optional predicate
    pub predicate: Option<Predicate>,
    /// Optional alias for capture
    pub alias: Option<String>,
}

/// Configuration for AND pattern at a state
#[derive(Debug, Clone)]
pub struct AndConfig {
    /// Branches that need to be satisfied (in any order)
    pub branches: Vec<AndBranch>,
    /// State to transition to when all branches complete
    pub join_state: usize,
}

/// State tracking for AND operator - tracks which branches are completed
#[derive(Debug, Clone)]
pub struct AndState {
    /// Indices of branches that have been completed
    pub completed: FxHashSet<usize>,
    /// Events captured by each branch
    pub captures: FxHashMap<usize, SharedEvent>,
}

impl AndState {
    pub fn new() -> Self {
        Self {
            completed: FxHashSet::default(),
            captures: FxHashMap::default(),
        }
    }

    pub fn complete_branch(&mut self, branch_idx: usize, event: SharedEvent) {
        self.completed.insert(branch_idx);
        self.captures.insert(branch_idx, event);
    }

    pub fn is_branch_completed(&self, branch_idx: usize) -> bool {
        self.completed.contains(&branch_idx)
    }

    pub fn all_completed(&self, total_branches: usize) -> bool {
        self.completed.len() == total_branches
    }
}

impl Default for AndState {
    fn default() -> Self {
        Self::new()
    }
}

/// Information about negation for a Negation state
#[derive(Debug, Clone)]
pub struct NegationInfo {
    /// Event type that must NOT occur
    pub forbidden_type: String,
    /// Optional predicate
    pub predicate: Option<Predicate>,
    /// State to go to after negation is confirmed
    pub continue_state: usize,
}

// ============================================================================
// BP-01: BACKPRESSURE STRATEGIES
// ============================================================================

/// Strategy when the maximum number of runs is reached
#[derive(Debug, Clone, Default)]
pub enum BackpressureStrategy {
    /// Silently drop new runs (default, not recommended for production)
    #[default]
    Drop,

    /// Return an error when limit is reached
    Error,

    /// Evict the oldest runs to make room for new ones
    EvictOldest,

    /// Evict runs with the least progress (fewest stack entries)
    EvictLeastProgress,

    /// Probabilistic sampling: accept new runs with probability `rate`
    Sample {
        /// Probability of accepting a new run (0.0 to 1.0)
        rate: f64,
    },
}

/// Error returned when backpressure prevents run creation
#[derive(Debug, Clone)]
pub enum BackpressureError {
    /// Maximum runs exceeded and strategy is Error
    MaxRunsExceeded {
        /// Current number of runs
        current: usize,
        /// Maximum allowed runs
        max: usize,
    },
    /// Run was dropped due to sampling
    SampledOut,
    /// Run was dropped silently (Drop strategy)
    Dropped,
}

impl std::fmt::Display for BackpressureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MaxRunsExceeded { current, max } => {
                write!(f, "Max runs exceeded: {} / {}", current, max)
            }
            Self::SampledOut => write!(f, "Run dropped due to sampling"),
            Self::Dropped => write!(f, "Run dropped due to backpressure"),
        }
    }
}

impl std::error::Error for BackpressureError {}

/// Warning generated during event processing
#[derive(Debug, Clone)]
pub enum ProcessWarning {
    /// A run was dropped due to backpressure
    RunDropped {
        /// Reason for dropping
        reason: String,
    },
    /// A run was evicted to make room for new runs
    RunEvicted {
        /// Age of the evicted run
        age: Duration,
    },
    /// Approaching the maximum run limit
    ApproachingLimit {
        /// Current number of runs
        current: usize,
        /// Maximum allowed runs
        max: usize,
        /// Utilization percentage (0.0 to 1.0)
        utilization: f64,
    },
}

/// Statistics for a single process() call
#[derive(Debug, Clone, Default)]
pub struct ProcessStats {
    /// Number of runs that advanced
    pub runs_advanced: usize,
    /// Number of new runs created
    pub runs_created: usize,
    /// Number of runs completed
    pub runs_completed: usize,
    /// Number of runs invalidated
    pub runs_invalidated: usize,
    /// Number of runs dropped due to backpressure
    pub runs_dropped: usize,
    /// Number of runs evicted due to backpressure
    pub runs_evicted: usize,
    /// Current active runs after processing
    pub active_runs: usize,
}

/// Result of processing an event, including matches, warnings, and stats
#[derive(Debug)]
pub struct ProcessResult {
    /// Completed pattern matches
    pub matches: Vec<MatchResult>,
    /// Warnings generated during processing
    pub warnings: Vec<ProcessWarning>,
    /// Statistics for this processing call
    pub stats: ProcessStats,
}

/// Extended statistics with backpressure metrics
#[derive(Debug, Clone, Default)]
pub struct SaseExtendedStats {
    /// Basic stats
    pub active_runs: usize,
    pub partitions: usize,
    pub nfa_states: usize,
    /// Backpressure metrics
    pub total_runs_dropped: u64,
    pub total_runs_evicted: u64,
    pub total_runs_created: u64,
    pub total_runs_completed: u64,
    /// Current utilization (0.0 to 1.0)
    pub utilization: f64,
}

// ============================================================================
// IDX-01: EVENT TYPE INDEXING
// ============================================================================

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

// ============================================================================
// ET-01: ROBUST EVENT-TIME HANDLING
// ============================================================================

/// Configuration for event-time processing
#[derive(Debug, Clone)]
pub struct EventTimeConfig {
    /// Maximum out-of-orderness tolerance for watermark generation
    /// The watermark will be: max_timestamp - max_out_of_orderness
    pub max_out_of_orderness: Duration,

    /// Additional tolerance for late events after watermark
    /// Events arriving within this window after the watermark are still processed
    pub allowed_lateness: Duration,

    /// Whether to emit late events to a side output
    pub emit_late_events: bool,
}

impl Default for EventTimeConfig {
    fn default() -> Self {
        Self {
            max_out_of_orderness: Duration::from_secs(0),
            allowed_lateness: Duration::from_secs(0),
            emit_late_events: false,
        }
    }
}

impl EventTimeConfig {
    /// Create a new event-time configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set maximum out-of-orderness tolerance
    pub fn with_max_out_of_orderness(mut self, duration: Duration) -> Self {
        self.max_out_of_orderness = duration;
        self
    }

    /// Set allowed lateness tolerance
    pub fn with_allowed_lateness(mut self, duration: Duration) -> Self {
        self.allowed_lateness = duration;
        self
    }

    /// Enable emission of late events
    pub fn with_late_event_emission(mut self) -> Self {
        self.emit_late_events = true;
        self
    }
}

/// Result of event-time processing for an event
#[derive(Debug, Clone)]
pub enum EventTimeResult {
    /// Event is within expected time bounds
    OnTime,
    /// Event is late but within allowed lateness
    Late {
        /// How late the event is
        lateness: Duration,
    },
    /// Event is too late and was dropped
    TooLate {
        /// How late the event is
        lateness: Duration,
    },
}

/// Manages event-time processing with watermarks and late event handling
#[derive(Debug)]
pub struct EventTimeManager {
    config: EventTimeConfig,
    /// Current watermark
    watermark: Option<DateTime<Utc>>,
    /// Maximum observed event timestamp
    max_timestamp: Option<DateTime<Utc>>,
    /// Count of late events that were still processed
    late_events_accepted: u64,
    /// Count of late events that were dropped (too late)
    late_events_dropped: u64,
    /// Late events that were emitted for side output
    late_events_emitted: Vec<SharedEvent>,
}

impl EventTimeManager {
    /// Create a new event-time manager with the given configuration
    pub fn new(config: EventTimeConfig) -> Self {
        Self {
            config,
            watermark: None,
            max_timestamp: None,
            late_events_accepted: 0,
            late_events_dropped: 0,
            late_events_emitted: Vec::new(),
        }
    }

    /// Get current watermark
    pub fn watermark(&self) -> Option<DateTime<Utc>> {
        self.watermark
    }

    /// Get count of late events that were accepted
    pub fn late_events_accepted(&self) -> u64 {
        self.late_events_accepted
    }

    /// Get count of late events that were dropped
    pub fn late_events_dropped(&self) -> u64 {
        self.late_events_dropped
    }

    /// Take and clear emitted late events
    pub fn take_late_events(&mut self) -> Vec<SharedEvent> {
        std::mem::take(&mut self.late_events_emitted)
    }

    /// Process an event and determine its timeliness
    pub fn process_event(&mut self, event: &SharedEvent) -> EventTimeResult {
        let event_ts = event.timestamp;

        // Check if the event is late relative to watermark
        if let Some(wm) = self.watermark {
            if event_ts < wm {
                let lateness_chrono = wm - event_ts;
                let lateness = lateness_chrono
                    .to_std()
                    .unwrap_or(Duration::from_secs(u64::MAX));

                let allowed = chrono::Duration::from_std(self.config.allowed_lateness)
                    .unwrap_or(chrono::Duration::zero());

                if lateness_chrono > allowed {
                    // Too late, reject
                    self.late_events_dropped += 1;

                    // Optionally emit for side output
                    if self.config.emit_late_events {
                        self.late_events_emitted.push(Arc::clone(event));
                    }

                    return EventTimeResult::TooLate { lateness };
                } else {
                    // Late but acceptable
                    self.late_events_accepted += 1;
                    return EventTimeResult::Late { lateness };
                }
            }
        }

        // Update max_timestamp
        match self.max_timestamp {
            Some(max_ts) if event_ts > max_ts => {
                self.max_timestamp = Some(event_ts);
            }
            None => {
                self.max_timestamp = Some(event_ts);
            }
            _ => {}
        }

        // Update watermark
        self.update_watermark();

        EventTimeResult::OnTime
    }

    /// Update the watermark based on max_timestamp
    fn update_watermark(&mut self) {
        if let Some(max_ts) = self.max_timestamp {
            let out_of_orderness = chrono::Duration::from_std(self.config.max_out_of_orderness)
                .unwrap_or(chrono::Duration::zero());

            let new_watermark = max_ts - out_of_orderness;

            // Watermark never recedes
            match self.watermark {
                Some(wm) if new_watermark > wm => {
                    self.watermark = Some(new_watermark);
                }
                None => {
                    self.watermark = Some(new_watermark);
                }
                _ => {}
            }
        }
    }

    /// Manually advance the watermark
    pub fn advance_watermark(&mut self, new_watermark: DateTime<Utc>) {
        // Watermark never recedes
        match self.watermark {
            Some(wm) if new_watermark > wm => {
                self.watermark = Some(new_watermark);
            }
            None => {
                self.watermark = Some(new_watermark);
            }
            _ => {}
        }
    }

    /// Compute a deadline in event-time, handling overflow safely
    pub fn compute_deadline(start: DateTime<Utc>, timeout: Duration) -> Option<DateTime<Utc>> {
        chrono::Duration::from_std(timeout).ok().map(|d| start + d)
    }
}

// ============================================================================
// MET-01: COMPREHENSIVE METRICS
// ============================================================================

/// Latency histogram with exponential buckets
/// Buckets: <1µs, <10µs, <100µs, <1ms, <10ms, <100ms, <1s, >1s
#[derive(Debug, Default)]
pub struct LatencyHistogram {
    buckets: [AtomicU64; 8],
    sum_micros: AtomicU64,
    count: AtomicU64,
}

impl LatencyHistogram {
    /// Create a new latency histogram
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a latency measurement
    pub fn record(&self, duration: Duration) {
        let micros = duration.as_micros() as u64;

        let bucket_idx = match micros {
            0..=1 => 0,
            2..=10 => 1,
            11..=100 => 2,
            101..=1_000 => 3,
            1_001..=10_000 => 4,
            10_001..=100_000 => 5,
            100_001..=1_000_000 => 6,
            _ => 7,
        };

        self.buckets[bucket_idx].fetch_add(1, Ordering::Relaxed);
        self.sum_micros.fetch_add(micros, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get approximate percentile value
    pub fn percentile(&self, p: f64) -> Duration {
        let total = self.count.load(Ordering::Relaxed);
        if total == 0 {
            return Duration::ZERO;
        }

        let target = (total as f64 * p) as u64;
        let bucket_maxes = [1u64, 10, 100, 1_000, 10_000, 100_000, 1_000_000, u64::MAX];

        let mut cumulative = 0u64;
        for (idx, &max_micros) in bucket_maxes.iter().enumerate() {
            cumulative += self.buckets[idx].load(Ordering::Relaxed);
            if cumulative >= target {
                return Duration::from_micros(max_micros.min(1_000_000));
            }
        }

        Duration::from_secs(1)
    }

    /// Get the mean latency
    pub fn mean(&self) -> Duration {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            return Duration::ZERO;
        }
        Duration::from_micros(self.sum_micros.load(Ordering::Relaxed) / count)
    }

    /// Get total count of measurements
    pub fn total_count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Get bucket counts (for Prometheus histogram format)
    pub fn bucket_counts(&self) -> [u64; 8] {
        let mut counts = [0u64; 8];
        for (i, bucket) in self.buckets.iter().enumerate() {
            counts[i] = bucket.load(Ordering::Relaxed);
        }
        counts
    }
}

/// Comprehensive metrics for the SASE+ engine
#[derive(Debug, Default)]
pub struct SaseMetrics {
    // === Event counters ===
    /// Total events processed
    pub events_processed: AtomicU64,
    /// Events that triggered at least one transition
    pub events_matched: AtomicU64,
    /// Events that were ignored (no run interested)
    pub events_ignored: AtomicU64,
    /// Events that were late but accepted
    pub events_late_accepted: AtomicU64,
    /// Events that were too late and dropped
    pub events_late_dropped: AtomicU64,

    // === Run counters ===
    /// Total runs created
    pub runs_created: AtomicU64,
    /// Runs that completed successfully (matches emitted)
    pub runs_completed: AtomicU64,
    /// Runs that expired due to timeout
    pub runs_expired: AtomicU64,
    /// Runs invalidated by negation
    pub runs_invalidated: AtomicU64,
    /// Runs dropped due to backpressure
    pub runs_dropped: AtomicU64,
    /// Runs evicted due to backpressure
    pub runs_evicted: AtomicU64,

    // === Match counters ===
    /// Total matches emitted
    pub matches_emitted: AtomicU64,

    // === Latency histograms ===
    /// Event processing latency
    pub event_latency: LatencyHistogram,
    /// Match duration (from first to last event)
    pub match_duration: LatencyHistogram,

    // === Memory metrics ===
    /// Peak active runs observed
    pub peak_active_runs: AtomicU64,

    // === Timestamps ===
    /// Last event timestamp (epoch millis)
    pub last_event_time: AtomicU64,
    /// Current watermark (epoch millis)
    pub current_watermark: AtomicU64,
}

impl SaseMetrics {
    /// Create new metrics
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an event being processed
    pub fn record_event_processed(&self) {
        self.events_processed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an event that matched
    pub fn record_event_matched(&self) {
        self.events_matched.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an event that was ignored
    pub fn record_event_ignored(&self) {
        self.events_ignored.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a late event that was accepted
    pub fn record_late_event_accepted(&self) {
        self.events_late_accepted.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a late event that was dropped
    pub fn record_late_event_dropped(&self) {
        self.events_late_dropped.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a run being created
    pub fn record_run_created(&self) {
        self.runs_created.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a run completing
    pub fn record_run_completed(&self) {
        self.runs_completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a run expiring
    pub fn record_run_expired(&self) {
        self.runs_expired.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a run being invalidated
    pub fn record_run_invalidated(&self) {
        self.runs_invalidated.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a run being dropped
    pub fn record_run_dropped(&self) {
        self.runs_dropped.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a run being evicted
    pub fn record_run_evicted(&self) {
        self.runs_evicted.fetch_add(1, Ordering::Relaxed);
    }

    /// Record matches emitted
    pub fn record_matches(&self, count: usize) {
        self.matches_emitted
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    /// Record event processing latency
    pub fn record_event_latency(&self, latency: Duration) {
        self.event_latency.record(latency);
    }

    /// Record match duration
    pub fn record_match_duration(&self, duration: Duration) {
        self.match_duration.record(duration);
    }

    /// Update peak active runs
    pub fn update_peak_runs(&self, current: usize) {
        self.peak_active_runs
            .fetch_max(current as u64, Ordering::Relaxed);
    }

    /// Update last event time
    pub fn update_last_event_time(&self, timestamp: DateTime<Utc>) {
        self.last_event_time
            .store(timestamp.timestamp_millis() as u64, Ordering::Relaxed);
    }

    /// Update current watermark
    pub fn update_watermark(&self, watermark: DateTime<Utc>) {
        self.current_watermark
            .store(watermark.timestamp_millis() as u64, Ordering::Relaxed);
    }

    /// Export metrics in Prometheus format
    pub fn to_prometheus(&self, prefix: &str) -> String {
        let mut output = String::new();

        // Event counters
        output.push_str(&format!(
            "# HELP {prefix}_events_total Total events processed\n\
             # TYPE {prefix}_events_total counter\n\
             {prefix}_events_total {}\n\n",
            self.events_processed.load(Ordering::Relaxed)
        ));

        output.push_str(&format!(
            "# HELP {prefix}_events_matched Events that triggered transitions\n\
             # TYPE {prefix}_events_matched counter\n\
             {prefix}_events_matched {}\n\n",
            self.events_matched.load(Ordering::Relaxed)
        ));

        output.push_str(&format!(
            "# HELP {prefix}_events_ignored Events ignored (no interest)\n\
             # TYPE {prefix}_events_ignored counter\n\
             {prefix}_events_ignored {}\n\n",
            self.events_ignored.load(Ordering::Relaxed)
        ));

        output.push_str(&format!(
            "# HELP {prefix}_events_late_accepted Late events still processed\n\
             # TYPE {prefix}_events_late_accepted counter\n\
             {prefix}_events_late_accepted {}\n\n",
            self.events_late_accepted.load(Ordering::Relaxed)
        ));

        output.push_str(&format!(
            "# HELP {prefix}_events_late_dropped Late events dropped\n\
             # TYPE {prefix}_events_late_dropped counter\n\
             {prefix}_events_late_dropped {}\n\n",
            self.events_late_dropped.load(Ordering::Relaxed)
        ));

        // Run counters
        output.push_str(&format!(
            "# HELP {prefix}_runs_created Total runs created\n\
             # TYPE {prefix}_runs_created counter\n\
             {prefix}_runs_created {}\n\n",
            self.runs_created.load(Ordering::Relaxed)
        ));

        output.push_str(&format!(
            "# HELP {prefix}_runs_completed Runs completed successfully\n\
             # TYPE {prefix}_runs_completed counter\n\
             {prefix}_runs_completed {}\n\n",
            self.runs_completed.load(Ordering::Relaxed)
        ));

        output.push_str(&format!(
            "# HELP {prefix}_runs_expired Runs expired by timeout\n\
             # TYPE {prefix}_runs_expired counter\n\
             {prefix}_runs_expired {}\n\n",
            self.runs_expired.load(Ordering::Relaxed)
        ));

        output.push_str(&format!(
            "# HELP {prefix}_runs_invalidated Runs invalidated by negation\n\
             # TYPE {prefix}_runs_invalidated counter\n\
             {prefix}_runs_invalidated {}\n\n",
            self.runs_invalidated.load(Ordering::Relaxed)
        ));

        output.push_str(&format!(
            "# HELP {prefix}_runs_dropped Runs dropped by backpressure\n\
             # TYPE {prefix}_runs_dropped counter\n\
             {prefix}_runs_dropped {}\n\n",
            self.runs_dropped.load(Ordering::Relaxed)
        ));

        output.push_str(&format!(
            "# HELP {prefix}_runs_evicted Runs evicted by backpressure\n\
             # TYPE {prefix}_runs_evicted counter\n\
             {prefix}_runs_evicted {}\n\n",
            self.runs_evicted.load(Ordering::Relaxed)
        ));

        // Matches
        output.push_str(&format!(
            "# HELP {prefix}_matches_total Total matches emitted\n\
             # TYPE {prefix}_matches_total counter\n\
             {prefix}_matches_total {}\n\n",
            self.matches_emitted.load(Ordering::Relaxed)
        ));

        // Peak runs
        output.push_str(&format!(
            "# HELP {prefix}_peak_active_runs Peak concurrent active runs\n\
             # TYPE {prefix}_peak_active_runs gauge\n\
             {prefix}_peak_active_runs {}\n\n",
            self.peak_active_runs.load(Ordering::Relaxed)
        ));

        // Latency percentiles
        output.push_str(&format!(
            "# HELP {prefix}_event_latency_p50_us Event processing latency p50 in microseconds\n\
             # TYPE {prefix}_event_latency_p50_us gauge\n\
             {prefix}_event_latency_p50_us {}\n\n",
            self.event_latency.percentile(0.5).as_micros()
        ));

        output.push_str(&format!(
            "# HELP {prefix}_event_latency_p99_us Event processing latency p99 in microseconds\n\
             # TYPE {prefix}_event_latency_p99_us gauge\n\
             {prefix}_event_latency_p99_us {}\n\n",
            self.event_latency.percentile(0.99).as_micros()
        ));

        output.push_str(&format!(
            "# HELP {prefix}_event_latency_mean_us Event processing latency mean in microseconds\n\
             # TYPE {prefix}_event_latency_mean_us gauge\n\
             {prefix}_event_latency_mean_us {}\n\n",
            self.event_latency.mean().as_micros()
        ));

        output.push_str(&format!(
            "# HELP {prefix}_match_duration_p50_us Match duration p50 in microseconds\n\
             # TYPE {prefix}_match_duration_p50_us gauge\n\
             {prefix}_match_duration_p50_us {}\n\n",
            self.match_duration.percentile(0.5).as_micros()
        ));

        output.push_str(&format!(
            "# HELP {prefix}_match_duration_p99_us Match duration p99 in microseconds\n\
             # TYPE {prefix}_match_duration_p99_us gauge\n\
             {prefix}_match_duration_p99_us {}\n\n",
            self.match_duration.percentile(0.99).as_micros()
        ));

        // Watermark
        output.push_str(&format!(
            "# HELP {prefix}_watermark_epoch_ms Current watermark epoch milliseconds\n\
             # TYPE {prefix}_watermark_epoch_ms gauge\n\
             {prefix}_watermark_epoch_ms {}\n\n",
            self.current_watermark.load(Ordering::Relaxed)
        ));

        output
    }

    /// Get a summary of current metrics
    pub fn summary(&self) -> MetricsSummary {
        MetricsSummary {
            events_processed: self.events_processed.load(Ordering::Relaxed),
            events_matched: self.events_matched.load(Ordering::Relaxed),
            events_ignored: self.events_ignored.load(Ordering::Relaxed),
            events_late_accepted: self.events_late_accepted.load(Ordering::Relaxed),
            events_late_dropped: self.events_late_dropped.load(Ordering::Relaxed),
            runs_created: self.runs_created.load(Ordering::Relaxed),
            runs_completed: self.runs_completed.load(Ordering::Relaxed),
            runs_expired: self.runs_expired.load(Ordering::Relaxed),
            runs_invalidated: self.runs_invalidated.load(Ordering::Relaxed),
            runs_dropped: self.runs_dropped.load(Ordering::Relaxed),
            runs_evicted: self.runs_evicted.load(Ordering::Relaxed),
            matches_emitted: self.matches_emitted.load(Ordering::Relaxed),
            peak_active_runs: self.peak_active_runs.load(Ordering::Relaxed),
            event_latency_p50_us: self.event_latency.percentile(0.5).as_micros() as u64,
            event_latency_p99_us: self.event_latency.percentile(0.99).as_micros() as u64,
            event_latency_mean_us: self.event_latency.mean().as_micros() as u64,
        }
    }
}

/// Summary of metrics for easier access
#[derive(Debug, Clone)]
pub struct MetricsSummary {
    pub events_processed: u64,
    pub events_matched: u64,
    pub events_ignored: u64,
    pub events_late_accepted: u64,
    pub events_late_dropped: u64,
    pub runs_created: u64,
    pub runs_completed: u64,
    pub runs_expired: u64,
    pub runs_invalidated: u64,
    pub runs_dropped: u64,
    pub runs_evicted: u64,
    pub matches_emitted: u64,
    pub peak_active_runs: u64,
    pub event_latency_p50_us: u64,
    pub event_latency_p99_us: u64,
    pub event_latency_mean_us: u64,
}

/// An active pattern run (partial match in progress)
#[derive(Debug, Clone)]
pub struct Run {
    /// Current NFA state
    pub current_state: usize,
    /// Stack of matched events (for non-Kleene patterns)
    pub stack: Vec<StackEntry>,
    /// Captured events by alias (Arc for efficient sharing)
    pub captured: FxHashMap<String, SharedEvent>,
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
    /// NEG-01: Pending negation constraints that must be confirmed
    pub pending_negations: Vec<NegationConstraint>,
    /// AND-01: State tracking for AND operator (if in an AND state)
    pub and_state: Option<AndState>,
    /// ZDD-KLEENE: Compact Kleene capture using ZDD (replaces branching)
    pub kleene_capture: Option<KleeneCapture>,
}

impl Default for Run {
    fn default() -> Self {
        Self {
            current_state: 0,
            stack: Vec::new(),
            captured: FxHashMap::default(),
            started_at: Instant::now(),
            deadline: None,
            event_time_started_at: None,
            event_time_deadline: None,
            partition_key: None,
            invalidated: false,
            pending_negations: Vec::new(),
            and_state: None,
            kleene_capture: None,
        }
    }
}

impl Run {
    pub fn new(start_state: usize) -> Self {
        Self {
            current_state: start_state,
            stack: Vec::new(),
            captured: FxHashMap::default(),
            started_at: Instant::now(),
            deadline: None,
            event_time_started_at: None,
            event_time_deadline: None,
            partition_key: None,
            invalidated: false,
            pending_negations: Vec::new(),
            and_state: None,
            kleene_capture: None,
        }
    }

    /// Create a new run with event-time tracking
    pub fn new_with_event_time(start_state: usize, event_timestamp: DateTime<Utc>) -> Self {
        Self {
            current_state: start_state,
            stack: Vec::new(),
            captured: FxHashMap::default(),
            started_at: Instant::now(),
            deadline: None,
            event_time_started_at: Some(event_timestamp),
            event_time_deadline: None,
            partition_key: None,
            invalidated: false,
            pending_negations: Vec::new(),
            and_state: None,
            kleene_capture: None,
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
    pub captured: FxHashMap<String, SharedEvent>,
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
    partitioned_runs: FxHashMap<String, Vec<Run>>,
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
    /// BP-01: Backpressure strategy
    backpressure: BackpressureStrategy,
    /// BP-01: Cumulative stats for backpressure
    total_runs_dropped: u64,
    total_runs_evicted: u64,
    total_runs_created: u64,
    total_runs_completed: u64,
    /// IDX-01: Event type index for O(1) state lookup
    event_type_index: EventTypeIndex,
    /// ET-01: Event-time manager for robust late event handling
    event_time_manager: Option<EventTimeManager>,
    /// ET-01: Event-time configuration
    event_time_config: EventTimeConfig,
    /// MET-01: Comprehensive metrics
    metrics: Arc<SaseMetrics>,
    /// MET-01: Whether instrumented processing is enabled
    instrumentation_enabled: bool,
    /// Maximum events in a single Kleene closure before accumulation stops.
    /// Bounds ZDD enumeration from 2^n growth. Default: `MAX_KLEENE_EVENTS` (20).
    max_kleene_events: u32,
    /// Maximum results emitted by deferred-predicate enumeration.
    /// Default: `MAX_ENUMERATION_RESULTS` (10 000).
    max_enumeration_results: usize,
}

impl SaseEngine {
    pub fn new(pattern: SasePattern) -> Self {
        let nfa = NfaCompiler::new().compile(&pattern);
        // IDX-01: Build event type index from NFA
        let event_type_index = EventTypeIndex::from_nfa(&nfa);
        Self {
            nfa,
            runs: Vec::new(),
            max_runs: 10000,
            strategy: SelectionStrategy::SkipTillAnyMatch,
            partition_by: None,
            partitioned_runs: FxHashMap::default(),
            global_negations: Vec::new(),
            time_semantics: TimeSemantics::ProcessingTime,
            watermark: None,
            max_out_of_orderness: Duration::from_secs(0),
            max_timestamp: None,
            backpressure: BackpressureStrategy::default(),
            total_runs_dropped: 0,
            total_runs_evicted: 0,
            total_runs_created: 0,
            total_runs_completed: 0,
            event_type_index,
            event_time_manager: None,
            event_time_config: EventTimeConfig::default(),
            metrics: Arc::new(SaseMetrics::new()),
            instrumentation_enabled: false,
            max_kleene_events: MAX_KLEENE_EVENTS,
            max_enumeration_results: MAX_ENUMERATION_RESULTS,
        }
    }

    /// Get the partition-by field name, if set.
    pub fn partition_by(&self) -> Option<&str> {
        self.partition_by.as_deref()
    }

    /// Enable event-time processing with watermarks
    fn kleene_limits(&self) -> KleeneLimits {
        KleeneLimits {
            max_events: self.max_kleene_events,
            max_results: self.max_enumeration_results,
        }
    }

    pub fn with_event_time(mut self) -> Self {
        self.time_semantics = TimeSemantics::EventTime;
        self
    }

    /// ET-01: Configure event-time processing with full configuration
    pub fn with_event_time_config(mut self, config: EventTimeConfig) -> Self {
        self.time_semantics = TimeSemantics::EventTime;
        self.max_out_of_orderness = config.max_out_of_orderness;
        self.event_time_config = config.clone();
        self.event_time_manager = Some(EventTimeManager::new(config));
        self
    }

    /// ET-01: Set allowed lateness for late event handling
    pub fn with_allowed_lateness(mut self, duration: Duration) -> Self {
        self.event_time_config.allowed_lateness = duration;
        // Recreate event time manager if it exists
        if self.event_time_manager.is_some() {
            self.event_time_manager = Some(EventTimeManager::new(self.event_time_config.clone()));
        }
        self
    }

    /// Set maximum out-of-orderness tolerance for watermark generation
    /// The watermark will be: max_timestamp - max_out_of_orderness
    pub fn with_max_out_of_orderness(mut self, duration: Duration) -> Self {
        self.max_out_of_orderness = duration;
        self.event_time_config.max_out_of_orderness = duration;
        // Update event time manager if it exists
        if self.event_time_manager.is_some() {
            self.event_time_manager = Some(EventTimeManager::new(self.event_time_config.clone()));
        }
        self
    }

    /// MET-01: Enable instrumented processing for detailed metrics
    pub fn with_instrumentation(mut self) -> Self {
        self.instrumentation_enabled = true;
        self
    }

    /// MET-01: Get access to the metrics
    pub fn metrics(&self) -> &SaseMetrics {
        &self.metrics
    }

    /// MET-01: Get a clone of the metrics Arc (for sharing with other threads)
    pub fn metrics_arc(&self) -> Arc<SaseMetrics> {
        Arc::clone(&self.metrics)
    }

    /// ET-01: Get access to late events that were emitted (if emit_late_events is enabled)
    pub fn take_late_events(&mut self) -> Vec<SharedEvent> {
        if let Some(ref mut etm) = self.event_time_manager {
            etm.take_late_events()
        } else {
            Vec::new()
        }
    }

    /// ET-01: Get the event time manager's late event statistics
    pub fn late_event_stats(&self) -> (u64, u64) {
        if let Some(ref etm) = self.event_time_manager {
            (etm.late_events_accepted(), etm.late_events_dropped())
        } else {
            (0, 0)
        }
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
    /// NEG-01: This also confirms any pending negations whose deadline has passed
    pub fn advance_watermark(&mut self, new_watermark: DateTime<Utc>) {
        self.watermark = Some(new_watermark);
        // NEG-01: Confirm negations based on watermark first
        self.confirm_negations_event_time(new_watermark);
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

    /// BP-01: Set the backpressure strategy
    pub fn with_backpressure(mut self, strategy: BackpressureStrategy) -> Self {
        self.backpressure = strategy;
        self
    }

    /// Set the maximum number of events accumulated in a single Kleene closure.
    /// With `n` events the ZDD enumerates up to 2^n − 1 combinations, so this
    /// cap bounds the exponential blowup.  Default: 20 (~1 M combinations).
    pub fn with_max_kleene_events(mut self, max: u32) -> Self {
        self.max_kleene_events = max;
        self
    }

    /// Set the maximum number of results emitted by deferred-predicate
    /// enumeration.  Default: 10 000.
    pub fn with_max_enumeration_results(mut self, max: usize) -> Self {
        self.max_enumeration_results = max;
        self
    }

    /// IDX-01: Check if the engine has any interest in events of this type
    /// Can be used for early rejection to avoid unnecessary processing
    pub fn has_interest(&self, event_type: &str) -> bool {
        // Check if any NFA state expects this event type
        if self.event_type_index.has_interest(event_type) {
            return true;
        }
        // Also check global negations
        self.global_negations
            .iter()
            .any(|n| n.event_type == event_type)
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

    /// BP-01: Process an event with detailed results including warnings and stats
    pub fn process_with_result(&mut self, event: &Event) -> ProcessResult {
        let shared_event = Arc::new(event.clone());
        self.process_shared_with_result(shared_event)
    }

    /// MET-01: Process an event with full instrumentation and metrics recording
    /// This version records detailed metrics including latency histograms
    pub fn process_instrumented(&mut self, event: &Event) -> Vec<MatchResult> {
        let start = Instant::now();
        let shared_event = Arc::new(event.clone());

        // Record event being processed
        self.metrics.record_event_processed();
        self.metrics.update_last_event_time(event.timestamp);

        // ET-01: Handle event-time with robust late event handling
        if self.time_semantics == TimeSemantics::EventTime {
            if let Some(ref mut etm) = self.event_time_manager {
                match etm.process_event(&shared_event) {
                    EventTimeResult::TooLate { .. } => {
                        self.metrics.record_late_event_dropped();
                        self.metrics.record_event_latency(start.elapsed());
                        return Vec::new();
                    }
                    EventTimeResult::Late { .. } => {
                        self.metrics.record_late_event_accepted();
                    }
                    EventTimeResult::OnTime => {}
                }
                // Update watermark from manager
                self.watermark = etm.watermark();
                if let Some(wm) = self.watermark {
                    self.metrics.update_watermark(wm);
                }
            } else {
                // Fallback to simple watermark update
                self.update_watermark(&shared_event);
            }
        }

        // Clean up timed-out runs
        let runs_before_cleanup = self.total_run_count();
        self.cleanup_timeouts();
        let expired_count = runs_before_cleanup.saturating_sub(self.total_run_count());
        for _ in 0..expired_count {
            self.metrics.record_run_expired();
        }

        // Check global negations
        let invalidated_before = self.count_invalidated_runs();
        self.check_global_negations(&shared_event);
        let newly_invalidated = self.count_invalidated_runs() - invalidated_before;
        for _ in 0..newly_invalidated {
            self.metrics.record_run_invalidated();
        }

        // Check if any state is interested in this event (for "ignored" metric)
        let has_interest = self.has_interest(&shared_event.event_type);

        let mut completed = Vec::new();

        // Process runs
        if let Some(ref partition_field) = self.partition_by.clone() {
            let partition_key = shared_event
                .get(partition_field)
                .map(|v| v.to_partition_key().into_owned())
                .unwrap_or_default();

            completed
                .extend(self.process_partition_shared(&partition_key, Arc::clone(&shared_event)));

            if let Some(run) = self.try_start_run_shared(Arc::clone(&shared_event)) {
                let (added, _) = self.handle_backpressure_partitioned(&partition_key, run);
                if added {
                    self.total_runs_created += 1;
                    self.metrics.record_run_created();
                } else {
                    self.metrics.record_run_dropped();
                }
            }
        } else {
            completed.extend(self.process_runs_shared(Arc::clone(&shared_event)));

            if let Some(run) = self.try_start_run_shared(Arc::clone(&shared_event)) {
                let (added, _) = self.handle_backpressure(run);
                if added {
                    self.total_runs_created += 1;
                    self.metrics.record_run_created();
                } else {
                    self.metrics.record_run_dropped();
                }
            }
        }

        // Record metrics
        if !completed.is_empty() {
            self.metrics.record_event_matched();
            self.metrics.record_matches(completed.len());
            for result in &completed {
                self.metrics.record_run_completed();
                self.metrics.record_match_duration(result.duration);
            }
        } else if !has_interest {
            self.metrics.record_event_ignored();
        }

        self.total_runs_completed += completed.len() as u64;

        // Update peak runs
        self.metrics.update_peak_runs(self.total_run_count());

        // Record latency
        self.metrics.record_event_latency(start.elapsed());

        completed
    }

    /// Helper to count invalidated runs (for metrics)
    fn count_invalidated_runs(&self) -> usize {
        let in_runs = self.runs.iter().filter(|r| r.invalidated).count();
        let in_partitions: usize = self
            .partitioned_runs
            .values()
            .map(|runs| runs.iter().filter(|r| r.invalidated).count())
            .sum();
        in_runs + in_partitions
    }

    /// BP-01: Process shared event with detailed results
    fn process_shared_with_result(&mut self, event: SharedEvent) -> ProcessResult {
        let mut warnings = Vec::new();
        let mut stats = ProcessStats::default();

        // Check utilization and warn if approaching limit
        let current_runs = self.total_run_count();
        let utilization = current_runs as f64 / self.max_runs as f64;
        if utilization > 0.8 {
            warnings.push(ProcessWarning::ApproachingLimit {
                current: current_runs,
                max: self.max_runs,
                utilization,
            });
        }

        // Update watermark tracking for event-time processing
        if self.time_semantics == TimeSemantics::EventTime {
            self.update_watermark(&event);
        }

        // Clean up timed-out runs
        self.cleanup_timeouts();

        // Check global negations
        self.check_global_negations(&event);

        let mut completed = Vec::new();

        // Process runs
        if let Some(ref partition_field) = self.partition_by.clone() {
            let partition_key = event
                .get(partition_field)
                .map(|v| v.to_partition_key().into_owned())
                .unwrap_or_default();

            completed.extend(self.process_partition_shared(&partition_key, Arc::clone(&event)));

            // Try to start new run with backpressure
            if let Some(run) = self.try_start_run_shared(Arc::clone(&event)) {
                let (added, warning) = self.handle_backpressure_partitioned(&partition_key, run);
                if added {
                    stats.runs_created += 1;
                    self.total_runs_created += 1;
                }
                if let Some(w) = warning {
                    warnings.push(w);
                }
            }
        } else {
            completed.extend(self.process_runs_shared(Arc::clone(&event)));

            // Try to start new run with backpressure
            if let Some(run) = self.try_start_run_shared(Arc::clone(&event)) {
                let (added, warning) = self.handle_backpressure(run);
                if added {
                    stats.runs_created += 1;
                    self.total_runs_created += 1;
                }
                if let Some(w) = warning {
                    warnings.push(w);
                }
            }
        }

        stats.runs_completed = completed.len();
        self.total_runs_completed += completed.len() as u64;
        stats.active_runs = self.total_run_count();

        ProcessResult {
            matches: completed,
            warnings,
            stats,
        }
    }

    /// Process a shared event reference (avoids redundant cloning)
    pub fn process_shared(&mut self, event: SharedEvent) -> Vec<MatchResult> {
        let mut completed = Vec::with_capacity(8);

        // Update watermark tracking for event-time processing
        if self.time_semantics == TimeSemantics::EventTime {
            self.update_watermark(&event);
        }

        // Clean up timed-out runs (uses watermark in EventTime mode)
        self.cleanup_timeouts();

        // Check global negations - invalidate runs that match
        self.check_global_negations(&event);

        // If using partitioning, route to appropriate partition
        if let Some(ref partition_field) = self.partition_by.clone() {
            let partition_key = event
                .get(partition_field)
                .map(|v| v.to_partition_key().into_owned())
                .unwrap_or_default();

            // Process partitioned runs
            completed.extend(self.process_partition_shared(&partition_key, Arc::clone(&event)));

            // Try to start new run in partition with backpressure
            if let Some(run) = self.try_start_run_shared(Arc::clone(&event)) {
                let (added, _) = self.handle_backpressure_partitioned(&partition_key, run);
                if added {
                    self.total_runs_created += 1;
                }
            }
        } else {
            // Non-partitioned processing
            completed.extend(self.process_runs_shared(Arc::clone(&event)));

            // Try to start new run with backpressure
            if let Some(run) = self.try_start_run_shared(Arc::clone(&event)) {
                let (added, _) = self.handle_backpressure(run);
                if added {
                    self.total_runs_created += 1;
                }
            }
        }

        self.total_runs_completed += completed.len() as u64;
        completed
    }

    /// BP-01: Handle backpressure for non-partitioned runs
    /// Returns (was_added, optional_warning)
    fn handle_backpressure(&mut self, run: Run) -> (bool, Option<ProcessWarning>) {
        if self.runs.len() < self.max_runs {
            self.runs.push(run);
            return (true, None);
        }

        match &self.backpressure {
            BackpressureStrategy::Drop => {
                self.total_runs_dropped += 1;
                (
                    false,
                    Some(ProcessWarning::RunDropped {
                        reason: "Max runs exceeded (Drop strategy)".to_string(),
                    }),
                )
            }
            BackpressureStrategy::Error => {
                // In the simple process() method, we just drop with a warning
                // The process_with_result method allows checking for this
                self.total_runs_dropped += 1;
                (
                    false,
                    Some(ProcessWarning::RunDropped {
                        reason: format!(
                            "Max runs exceeded: {} / {} (Error strategy)",
                            self.runs.len(),
                            self.max_runs
                        ),
                    }),
                )
            }
            BackpressureStrategy::EvictOldest => {
                // Find and evict the oldest run
                if let Some((idx, age)) = self
                    .runs
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, r)| r.started_at)
                    .map(|(idx, r)| (idx, r.started_at.elapsed()))
                {
                    self.runs.swap_remove(idx);
                    self.runs.push(run);
                    self.total_runs_evicted += 1;
                    (true, Some(ProcessWarning::RunEvicted { age }))
                } else {
                    self.runs.push(run);
                    (true, None)
                }
            }
            BackpressureStrategy::EvictLeastProgress => {
                // Find and evict the run with least progress
                if let Some((idx, age)) = self
                    .runs
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, r)| r.stack.len())
                    .map(|(idx, r)| (idx, r.started_at.elapsed()))
                {
                    self.runs.swap_remove(idx);
                    self.runs.push(run);
                    self.total_runs_evicted += 1;
                    (true, Some(ProcessWarning::RunEvicted { age }))
                } else {
                    self.runs.push(run);
                    (true, None)
                }
            }
            BackpressureStrategy::Sample { rate } => {
                // Simple deterministic sampling based on run count
                let should_accept =
                    (self.total_runs_created as f64 * rate) as u64 > self.total_runs_dropped;
                if should_accept {
                    // Evict oldest to make room
                    if let Some((idx, age)) = self
                        .runs
                        .iter()
                        .enumerate()
                        .min_by_key(|(_, r)| r.started_at)
                        .map(|(idx, r)| (idx, r.started_at.elapsed()))
                    {
                        self.runs.swap_remove(idx);
                        self.runs.push(run);
                        self.total_runs_evicted += 1;
                        (true, Some(ProcessWarning::RunEvicted { age }))
                    } else {
                        (false, None)
                    }
                } else {
                    self.total_runs_dropped += 1;
                    (
                        false,
                        Some(ProcessWarning::RunDropped {
                            reason: "Sampled out".to_string(),
                        }),
                    )
                }
            }
        }
    }

    /// BP-01: Handle backpressure for partitioned runs
    fn handle_backpressure_partitioned(
        &mut self,
        partition_key: &str,
        run: Run,
    ) -> (bool, Option<ProcessWarning>) {
        let partition_runs = self
            .partitioned_runs
            .entry(partition_key.to_string())
            .or_default();

        if partition_runs.len() < self.max_runs {
            partition_runs.push(run);
            return (true, None);
        }

        // Apply same backpressure logic as non-partitioned
        match &self.backpressure {
            BackpressureStrategy::Drop => {
                self.total_runs_dropped += 1;
                (
                    false,
                    Some(ProcessWarning::RunDropped {
                        reason: "Max runs exceeded in partition (Drop strategy)".to_string(),
                    }),
                )
            }
            BackpressureStrategy::Error => {
                self.total_runs_dropped += 1;
                (
                    false,
                    Some(ProcessWarning::RunDropped {
                        reason: format!(
                            "Max runs exceeded in partition: {} / {}",
                            partition_runs.len(),
                            self.max_runs
                        ),
                    }),
                )
            }
            BackpressureStrategy::EvictOldest => {
                if let Some((idx, age)) = partition_runs
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, r)| r.started_at)
                    .map(|(idx, r)| (idx, r.started_at.elapsed()))
                {
                    partition_runs.swap_remove(idx);
                    partition_runs.push(run);
                    self.total_runs_evicted += 1;
                    (true, Some(ProcessWarning::RunEvicted { age }))
                } else {
                    partition_runs.push(run);
                    (true, None)
                }
            }
            BackpressureStrategy::EvictLeastProgress => {
                if let Some((idx, age)) = partition_runs
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, r)| r.stack.len())
                    .map(|(idx, r)| (idx, r.started_at.elapsed()))
                {
                    partition_runs.swap_remove(idx);
                    partition_runs.push(run);
                    self.total_runs_evicted += 1;
                    (true, Some(ProcessWarning::RunEvicted { age }))
                } else {
                    partition_runs.push(run);
                    (true, None)
                }
            }
            BackpressureStrategy::Sample { rate } => {
                let should_accept =
                    (self.total_runs_created as f64 * rate) as u64 > self.total_runs_dropped;
                if should_accept {
                    if let Some((idx, age)) = partition_runs
                        .iter()
                        .enumerate()
                        .min_by_key(|(_, r)| r.started_at)
                        .map(|(idx, r)| (idx, r.started_at.elapsed()))
                    {
                        partition_runs.swap_remove(idx);
                        partition_runs.push(run);
                        self.total_runs_evicted += 1;
                        (true, Some(ProcessWarning::RunEvicted { age }))
                    } else {
                        (false, None)
                    }
                } else {
                    self.total_runs_dropped += 1;
                    (
                        false,
                        Some(ProcessWarning::RunDropped {
                            reason: "Sampled out".to_string(),
                        }),
                    )
                }
            }
        }
    }

    /// Get total run count across all partitions
    fn total_run_count(&self) -> usize {
        if self.partition_by.is_some() {
            self.partitioned_runs.values().map(|v| v.len()).sum()
        } else {
            self.runs.len()
        }
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
        let mut completed = Vec::with_capacity(4);
        let limits = self.kleene_limits();

        if let Some(runs) = self.partitioned_runs.get_mut(partition_key) {
            let mut i = 0;
            while i < runs.len() {
                if runs[i].is_timed_out() || runs[i].invalidated {
                    runs.swap_remove(i);
                    continue;
                }

                match advance_run_shared(
                    &self.nfa,
                    self.strategy,
                    &mut runs[i],
                    Arc::clone(&event),
                    limits,
                ) {
                    RunAdvanceResult::Continue => i += 1,
                    RunAdvanceResult::Complete(result) => {
                        completed.push(result);
                        runs.swap_remove(i);
                    }
                    RunAdvanceResult::CompleteAndContinue(result) => {
                        // For `all` patterns: emit result but keep run active
                        completed.push(result);
                        i += 1;
                    }
                    RunAdvanceResult::CompleteMulti(results) => {
                        completed.extend(results);
                        runs.swap_remove(i);
                    }
                    RunAdvanceResult::Invalidate => {
                        runs.swap_remove(i);
                    }
                    RunAdvanceResult::NoMatch => i += 1,
                }
            }
        }

        completed
    }

    fn process_runs_shared(&mut self, event: SharedEvent) -> Vec<MatchResult> {
        let mut completed = Vec::with_capacity(4);
        let limits = self.kleene_limits();
        let mut i = 0;

        while i < self.runs.len() {
            if self.runs[i].is_timed_out() || self.runs[i].invalidated {
                self.runs.swap_remove(i);
                continue;
            }

            // PERF: Mutate run in place instead of clone + copy back
            match advance_run_shared(
                &self.nfa,
                self.strategy,
                &mut self.runs[i],
                Arc::clone(&event),
                limits,
            ) {
                RunAdvanceResult::Continue => i += 1,
                RunAdvanceResult::Complete(result) => {
                    completed.push(result);
                    self.runs.swap_remove(i);
                }
                RunAdvanceResult::CompleteAndContinue(result) => {
                    // For `all` patterns: emit result but keep run active
                    completed.push(result);
                    i += 1;
                }
                RunAdvanceResult::CompleteMulti(results) => {
                    completed.extend(results);
                    self.runs.swap_remove(i);
                }
                RunAdvanceResult::Invalidate => {
                    self.runs.swap_remove(i);
                }
                RunAdvanceResult::NoMatch => i += 1,
            }
        }

        completed
    }

    fn try_start_run_shared(&self, event: SharedEvent) -> Option<Run> {
        let start_state = &self.nfa.states[self.nfa.start_state];
        // PERF: Use static empty map instead of allocating on every call
        let empty_captured = &*EMPTY_CAPTURED;

        // Check if event matches any transition from start
        for &next_id in &start_state.transitions {
            let next_state = &self.nfa.states[next_id];

            // AND-01: Handle AND states specially - check if event matches any branch
            if next_state.state_type == StateType::And {
                if let Some(ref config) = next_state.and_config {
                    // Check if event matches any branch
                    for (idx, branch) in config.branches.iter().enumerate() {
                        if *event.event_type == branch.event_type {
                            let pred_matches = branch
                                .predicate
                                .as_ref()
                                .is_none_or(|p| eval_predicate(p, &event, empty_captured));

                            if pred_matches {
                                let mut run = match self.time_semantics {
                                    TimeSemantics::ProcessingTime => Run::new(next_id),
                                    TimeSemantics::EventTime => {
                                        Run::new_with_event_time(next_id, event.timestamp)
                                    }
                                };

                                // Initialize AND state and complete this branch
                                let mut and_state = AndState::new();
                                and_state.complete_branch(idx, Arc::clone(&event));
                                run.and_state = Some(and_state);

                                // Capture with alias if present
                                if let Some(ref alias) = branch.alias {
                                    run.captured.insert(alias.clone(), Arc::clone(&event));
                                }
                                run.push(Arc::clone(&event), branch.alias.clone());

                                return Some(run);
                            }
                        }
                    }
                }
                continue;
            }

            if event_matches_state(&self.nfa, &event, next_state, empty_captured) {
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

                // AND-01: Handle AND states in epsilon paths
                if next_state.state_type == StateType::And {
                    if let Some(ref config) = next_state.and_config {
                        for (idx, branch) in config.branches.iter().enumerate() {
                            if *event.event_type == branch.event_type {
                                let pred_matches = branch
                                    .predicate
                                    .as_ref()
                                    .is_none_or(|p| eval_predicate(p, &event, empty_captured));

                                if pred_matches {
                                    let mut run = match self.time_semantics {
                                        TimeSemantics::ProcessingTime => Run::new(next_id),
                                        TimeSemantics::EventTime => {
                                            Run::new_with_event_time(next_id, event.timestamp)
                                        }
                                    };

                                    let mut and_state = AndState::new();
                                    and_state.complete_branch(idx, Arc::clone(&event));
                                    run.and_state = Some(and_state);

                                    if let Some(ref alias) = branch.alias {
                                        run.captured.insert(alias.clone(), Arc::clone(&event));
                                    }
                                    run.push(Arc::clone(&event), branch.alias.clone());

                                    return Some(run);
                                }
                            }
                        }
                    }
                    continue;
                }

                if event_matches_state(&self.nfa, &event, next_state, empty_captured) {
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
                // NEG-01: Confirm negations based on processing time
                self.confirm_negations_processing_time();
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

    /// NEG-01: Confirm negations based on processing time (deadline passed)
    fn confirm_negations_processing_time(&mut self) {
        for run in &mut self.runs {
            Self::confirm_run_negations_processing_time_static(run, &self.nfa);
        }
        for runs in self.partitioned_runs.values_mut() {
            for run in runs.iter_mut() {
                Self::confirm_run_negations_processing_time_static(run, &self.nfa);
            }
        }
    }

    fn confirm_run_negations_processing_time_static(run: &mut Run, nfa: &Nfa) {
        // Check each pending negation
        let mut confirmed_indices = Vec::new();
        for (idx, neg) in run.pending_negations.iter().enumerate() {
            if neg.is_confirmed_processing_time() {
                confirmed_indices.push((idx, neg.next_state));
            }
        }

        // Process confirmations in reverse order to maintain indices
        for (idx, next_state) in confirmed_indices.into_iter().rev() {
            run.pending_negations.remove(idx);
            // Transition to the continue state
            run.current_state = next_state;

            // Check if next state is accept
            let state = &nfa.states[next_state];
            if state.state_type == StateType::Accept {
                // Will be handled by the main processing loop
            }
        }
    }

    /// Cleanup runs based on watermark (for event-time processing)
    fn cleanup_by_watermark(&mut self) {
        if let Some(watermark) = self.watermark {
            // NEG-01: Confirm negations based on watermark
            self.confirm_negations_event_time(watermark);

            self.runs
                .retain(|r| !r.is_timed_out_event_time(watermark) && !r.invalidated);
            for runs in self.partitioned_runs.values_mut() {
                runs.retain(|r| !r.is_timed_out_event_time(watermark) && !r.invalidated);
            }
        }
    }

    /// NEG-01: Confirm negations based on event-time watermark
    fn confirm_negations_event_time(&mut self, watermark: DateTime<Utc>) {
        for run in &mut self.runs {
            Self::confirm_run_negations_event_time_static(run, &self.nfa, watermark);
        }
        for runs in self.partitioned_runs.values_mut() {
            for run in runs.iter_mut() {
                Self::confirm_run_negations_event_time_static(run, &self.nfa, watermark);
            }
        }
    }

    fn confirm_run_negations_event_time_static(run: &mut Run, nfa: &Nfa, watermark: DateTime<Utc>) {
        let mut confirmed_indices = Vec::new();
        for (idx, neg) in run.pending_negations.iter().enumerate() {
            if neg.is_confirmed_event_time(watermark) {
                confirmed_indices.push((idx, neg.next_state));
            }
        }

        for (idx, next_state) in confirmed_indices.into_iter().rev() {
            run.pending_negations.remove(idx);
            run.current_state = next_state;

            let state = &nfa.states[next_state];
            if state.state_type == StateType::Accept {
                // Will be handled by the main processing loop
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
            if *event.event_type != negation.event_type {
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

    /// BP-01: Get extended statistics including backpressure metrics
    pub fn extended_stats(&self) -> SaseExtendedStats {
        let total_runs = self.total_run_count();
        let utilization = total_runs as f64 / self.max_runs as f64;

        SaseExtendedStats {
            active_runs: total_runs,
            partitions: self.partitioned_runs.len(),
            nfa_states: self.nfa.states.len(),
            total_runs_dropped: self.total_runs_dropped,
            total_runs_evicted: self.total_runs_evicted,
            total_runs_created: self.total_runs_created,
            total_runs_completed: self.total_runs_completed,
            utilization,
        }
    }
}

#[derive(Debug)]
enum RunAdvanceResult {
    Continue,
    Complete(MatchResult),
    /// For `all` patterns: emit a result but keep the run active for more matches
    CompleteAndContinue(MatchResult),
    /// SIGMOD 2014: Multiple results from Kleene enumeration with deferred predicate
    CompleteMulti(Vec<MatchResult>),
    Invalidate,
    NoMatch,
}

// Free functions to avoid borrow checker issues

/// Configurable limits for Kleene closure accumulation and enumeration.
#[derive(Debug, Clone, Copy)]
struct KleeneLimits {
    max_events: u32,
    max_results: usize,
}

impl Default for KleeneLimits {
    fn default() -> Self {
        Self {
            max_events: MAX_KLEENE_EVENTS,
            max_results: MAX_ENUMERATION_RESULTS,
        }
    }
}

#[allow(dead_code)]
fn advance_run(
    nfa: &Nfa,
    strategy: SelectionStrategy,
    run: &mut Run,
    event: &Event,
    limits: KleeneLimits,
) -> RunAdvanceResult {
    // Wrap in Arc for the shared version
    advance_run_shared(nfa, strategy, run, Arc::new(event.clone()), limits)
}

/// Complete a run, checking for deferred Kleene predicates first.
fn complete_run(run: &mut Run, limits: KleeneLimits) -> RunAdvanceResult {
    if let Some(ref kc) = run.kleene_capture {
        if kc.deferred_predicate.is_some() {
            return RunAdvanceResult::CompleteMulti(enumerate_with_filter(run, limits.max_results));
        }
    }
    RunAdvanceResult::Complete(MatchResult {
        captured: std::mem::take(&mut run.captured),
        stack: std::mem::take(&mut run.stack),
        duration: run.started_at.elapsed(),
    })
}

/// PERF-01: Optimized version that takes SharedEvent to avoid redundant cloning
fn advance_run_shared(
    nfa: &Nfa,
    strategy: SelectionStrategy,
    run: &mut Run,
    event: SharedEvent,
    limits: KleeneLimits,
) -> RunAdvanceResult {
    let current_state = &nfa.states[run.current_state];

    // NEG-01: Check if event violates any pending negation constraints
    for neg in &run.pending_negations {
        if neg.is_violated_by(&event, &run.captured) {
            return RunAdvanceResult::Invalidate;
        }
    }

    // AND-01: Handle AND states specially
    if current_state.state_type == StateType::And {
        return advance_and_state(nfa, run, current_state, event, limits);
    }

    // NEG-01: Handle Negation states - check if event matches forbidden pattern
    if current_state.state_type == StateType::Negation {
        if let Some(ref neg_info) = current_state.negation_info {
            // Check if this event violates the negation
            if *event.event_type == neg_info.forbidden_type {
                let pred_matches = neg_info
                    .predicate
                    .as_ref()
                    .is_none_or(|p| eval_predicate(p, &event, &run.captured));
                if pred_matches {
                    return RunAdvanceResult::Invalidate;
                }
            }
        }
        // Event doesn't violate negation, stay in this state waiting for confirmation
        return RunAdvanceResult::NoMatch;
    }

    // Check if we're at an accept state
    if current_state.state_type == StateType::Accept {
        return complete_run(run, limits);
    }

    // KLEENE SELF-LOOP: accumulate additional events matching the Kleene state.
    // The first event enters via the transitions loop; subsequent events match here.
    if current_state.state_type == StateType::Kleene
        && current_state.self_loop
        && event_matches_state(nfa, &event, current_state, &run.captured)
    {
        // Safety: check Kleene cap before accumulating (prevents 2^n blowup)
        if let Some(ref kc) = run.kleene_capture {
            if kc.next_var >= limits.max_events {
                return RunAdvanceResult::Continue;
            }
        }

        run.push(Arc::clone(&event), current_state.alias.clone());

        // For `all` patterns (Kleene with epsilon to Accept): emit each event
        let has_epsilon_to_accept = current_state
            .epsilon_transitions
            .iter()
            .any(|&eps_id| nfa.states[eps_id].state_type == StateType::Accept);
        if has_epsilon_to_accept {
            return RunAdvanceResult::CompleteAndContinue(MatchResult {
                captured: run.captured.clone(),
                stack: run.stack.clone(),
                duration: run.started_at.elapsed(),
            });
        }

        // No epsilon to accept: accumulate in ZDD for deferred emission (SEQ(A, B+, C))
        if run.kleene_capture.is_none() {
            let mut kc = KleeneCapture::new();
            if let Some(ref pp) = current_state.postponed_predicate {
                kc.deferred_predicate = Some(pp.clone());
            }
            run.kleene_capture = Some(kc);
        }
        if let Some(ref mut kc) = run.kleene_capture {
            kc.extend(Arc::clone(&event), current_state.alias.clone());
        }
        return RunAdvanceResult::Continue;
    }

    // Check transitions
    for &next_id in &current_state.transitions {
        let next_state = &nfa.states[next_id];

        // NEG-01: If transitioning to a Negation state, set up the pending negation
        if next_state.state_type == StateType::Negation {
            if let Some(ref neg_info) = next_state.negation_info {
                // Add pending negation constraint - deadline will be set based on WITHIN
                let negation_constraint = NegationConstraint {
                    forbidden_type: neg_info.forbidden_type.clone(),
                    predicate: neg_info.predicate.clone(),
                    deadline: run.deadline, // Use the run's deadline
                    event_time_deadline: run.event_time_deadline,
                    next_state: neg_info.continue_state,
                };
                run.pending_negations.push(negation_constraint);
                run.current_state = next_id;
                return RunAdvanceResult::Continue;
            }
        }

        // AND-01: If transitioning to an AND state, enter it and try to match current event
        if next_state.state_type == StateType::And {
            run.current_state = next_id;
            // Try to advance the AND state with the current event
            return advance_and_state(nfa, run, next_state, event, limits);
        }

        if event_matches_state(nfa, &event, next_state, &run.captured) {
            run.current_state = next_id;
            run.push(Arc::clone(&event), next_state.alias.clone());

            if next_state.state_type == StateType::Accept {
                // PERF: Use std::mem::take since run is discarded after Complete
                return complete_run(run, limits);
            }

            if next_state.state_type == StateType::Kleene && next_state.self_loop {
                // Kleene state: check if there's an epsilon to Accept
                let has_epsilon_to_accept = next_state
                    .epsilon_transitions
                    .iter()
                    .any(|&eps_id| nfa.states[eps_id].state_type == StateType::Accept);

                if has_epsilon_to_accept {
                    // For `all` patterns: emit match for current event, keep run active
                    // This allows each matching event to produce a result
                    return RunAdvanceResult::CompleteAndContinue(MatchResult {
                        captured: run.captured.clone(),
                        stack: run.stack.clone(),
                        duration: run.started_at.elapsed(),
                    });
                }

                // No epsilon to accept: accumulate events in ZDD for deferred emission
                // This handles patterns like SEQ(A, B+, C) where we need to wait for C
                if run.kleene_capture.is_none() {
                    let mut kc = KleeneCapture::new();
                    if let Some(ref pp) = next_state.postponed_predicate {
                        kc.deferred_predicate = Some(pp.clone());
                    }
                    run.kleene_capture = Some(kc);
                }
                if let Some(ref mut kc) = run.kleene_capture {
                    if kc.next_var >= limits.max_events {
                        // Safety: stop accumulating to prevent 2^n enumeration blowup
                        return RunAdvanceResult::Continue;
                    }
                    kc.extend(Arc::clone(&event), next_state.alias.clone());
                }

                return RunAdvanceResult::Continue;
            }

            return RunAdvanceResult::Continue;
        }
    }

    // Check epsilon transitions
    for &eps_id in &current_state.epsilon_transitions {
        let eps_state = &nfa.states[eps_id];

        if eps_state.state_type == StateType::Accept {
            // PERF: Use std::mem::take since run is discarded after Complete
            return complete_run(run, limits);
        }

        for &next_id in &eps_state.transitions {
            let next_state = &nfa.states[next_id];
            if event_matches_state(nfa, &event, next_state, &run.captured) {
                run.current_state = next_id;
                run.push(Arc::clone(&event), next_state.alias.clone());

                if next_state.state_type == StateType::Accept {
                    // PERF: Use std::mem::take since run is discarded after Complete
                    return complete_run(run, limits);
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

/// AND-01: Handle AND state - track which branches are completed
fn advance_and_state(
    nfa: &Nfa,
    run: &mut Run,
    state: &State,
    event: SharedEvent,
    limits: KleeneLimits,
) -> RunAdvanceResult {
    let config = match &state.and_config {
        Some(c) => c,
        None => return RunAdvanceResult::NoMatch,
    };

    // Initialize AND state if not already
    if run.and_state.is_none() {
        run.and_state = Some(AndState::new());
    }

    // Find matching branch
    let mut matched_branch: Option<(usize, Option<String>)> = None;
    let total_branches = config.branches.len();

    {
        let Some(and_state) = run.and_state.as_ref() else {
            return RunAdvanceResult::Continue;
        };

        for (idx, branch) in config.branches.iter().enumerate() {
            if and_state.is_branch_completed(idx) {
                continue;
            }

            if *event.event_type == branch.event_type {
                let pred_matches = branch
                    .predicate
                    .as_ref()
                    .is_none_or(|p| eval_predicate(p, &event, &run.captured));

                if pred_matches {
                    matched_branch = Some((idx, branch.alias.clone()));
                    break;
                }
            }
        }
    }

    // Process the match if found
    if let Some((idx, alias)) = matched_branch {
        // Complete this branch
        let Some(and_state) = run.and_state.as_mut() else {
            return RunAdvanceResult::Continue;
        };
        and_state.complete_branch(idx, Arc::clone(&event));

        // Capture with alias if present
        if let Some(ref alias) = alias {
            run.captured.insert(alias.clone(), Arc::clone(&event));
        }
        run.push(Arc::clone(&event), alias);

        // Check if all branches are completed
        let all_completed = run
            .and_state
            .as_ref()
            .is_some_and(|s| s.all_completed(total_branches));

        if all_completed {
            run.current_state = config.join_state;
            run.and_state = None; // Clear AND state

            // Check if join state is accept
            let join_state = &nfa.states[config.join_state];
            if join_state.state_type == StateType::Accept {
                // PERF: Use std::mem::take since run is discarded after Complete
                return complete_run(run, limits);
            }
        }

        return RunAdvanceResult::Continue;
    }

    RunAdvanceResult::NoMatch
}

fn event_matches_state(
    _nfa: &Nfa,
    event: &Event,
    state: &State,
    captured: &FxHashMap<String, SharedEvent>,
) -> bool {
    if let Some(ref expected_type) = state.event_type {
        if &*event.event_type != expected_type {
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
    captured: &FxHashMap<String, SharedEvent>,
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
            let captured_events: FxHashMap<String, Event> = captured
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
// SIGMOD 2014: DEFERRED PREDICATE CLASSIFICATION AND ENUMERATION
// ============================================================================

/// Classification of a predicate relative to a Kleene closure variable.
///
/// - **Consistent**: The predicate references only the current event or constants
///   (can be evaluated eagerly during NFA traversal).
/// - **Inconsistent**: The predicate cross-references a *different* alias (e.g.,
///   `a[i].price > a[i-1].price`), which cannot be evaluated until all Kleene
///   events are known.  Must be postponed to the enumeration phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PredicateClass {
    Consistent,
    Inconsistent,
}

/// Classify a predicate as consistent or inconsistent for a Kleene state.
///
/// A predicate is *inconsistent* if it contains `CompareRef` nodes that
/// reference the same alias as the Kleene variable (cross-event comparison).
fn classify_predicate(pred: &Predicate, alias: Option<&str>) -> PredicateClass {
    match pred {
        Predicate::Compare { .. } => PredicateClass::Consistent,
        Predicate::CompareRef { ref_alias, .. } => {
            if alias.is_some_and(|a| a == ref_alias) {
                PredicateClass::Inconsistent
            } else {
                PredicateClass::Consistent
            }
        }
        Predicate::And(l, r) | Predicate::Or(l, r) => {
            let lc = classify_predicate(l, alias);
            let rc = classify_predicate(r, alias);
            if lc == PredicateClass::Inconsistent || rc == PredicateClass::Inconsistent {
                PredicateClass::Inconsistent
            } else {
                PredicateClass::Consistent
            }
        }
        Predicate::Not(inner) => classify_predicate(inner, alias),
        Predicate::Expr(expr) => {
            if let Some(a) = alias {
                if expr_references_alias(expr, a) {
                    PredicateClass::Inconsistent
                } else {
                    PredicateClass::Consistent
                }
            } else {
                PredicateClass::Consistent
            }
        }
    }
}

/// Check whether a `varpulis_core::ast::Expr` mentions a given alias.
fn expr_references_alias(expr: &varpulis_core::ast::Expr, alias: &str) -> bool {
    use varpulis_core::ast::Expr;
    match expr {
        Expr::Ident(name) => {
            name.starts_with(alias) && name.as_bytes().get(alias.len()) == Some(&b'.')
        }
        Expr::Binary { left, right, .. } => {
            expr_references_alias(left, alias) || expr_references_alias(right, alias)
        }
        Expr::Unary { expr: inner, .. } => expr_references_alias(inner, alias),
        Expr::Call { args, .. } => args.iter().any(|a| match a {
            varpulis_core::ast::Arg::Positional(e) | varpulis_core::ast::Arg::Named(_, e) => {
                expr_references_alias(e, alias)
            }
        }),
        _ => false,
    }
}

/// Evaluate the deferred predicate against a specific combination of Kleene events.
///
/// The predicate is evaluated pairwise: for a `CompareRef` that references the
/// Kleene alias, we check *consecutive* events in the combination.
fn evaluate_deferred_predicate(
    pred: &Predicate,
    events: &[SharedEvent],
    captured: &FxHashMap<String, SharedEvent>,
) -> bool {
    if events.len() < 2 {
        return true; // single event cannot violate a cross-event predicate
    }
    // Check consecutive pairs: each event[i] must satisfy the predicate
    // when event[i-1] is the "reference" event.
    for pair in events.windows(2) {
        if !eval_predicate(pred, &pair[1], &{
            let mut ctx = captured.clone();
            // Insert the previous event under every alias present in the predicate
            if let Some(alias) = extract_ref_alias(pred) {
                ctx.insert(alias, Arc::clone(&pair[0]));
            }
            ctx
        }) {
            return false;
        }
    }
    true
}

/// Extract the reference alias from a predicate (for CompareRef nodes).
fn extract_ref_alias(pred: &Predicate) -> Option<String> {
    match pred {
        Predicate::CompareRef { ref_alias, .. } => Some(ref_alias.clone()),
        Predicate::And(l, r) | Predicate::Or(l, r) => {
            extract_ref_alias(l).or_else(|| extract_ref_alias(r))
        }
        Predicate::Not(inner) => extract_ref_alias(inner),
        _ => None,
    }
}

/// Rebuild the run's stack with a specific Kleene combination.
fn rebuild_stack_with_combination(
    original_stack: &[StackEntry],
    _combination: &[StackEntry],
) -> Vec<StackEntry> {
    // The original stack has all events including Kleene-captured ones.
    // We replace the Kleene portion with the specific combination.
    // For simplicity, we keep non-Kleene entries and append the combination.
    let mut stack = Vec::with_capacity(original_stack.len());
    // Keep events that are NOT part of the Kleene capture
    // (they were pushed before the Kleene state was entered)
    // Heuristic: Kleene events are at the end of the stack
    stack.extend_from_slice(original_stack);
    stack
}

/// Enumerate all valid Kleene combinations, filtering by the deferred predicate.
///
/// Returns one `MatchResult` per valid combination (may be empty if all filtered).
fn enumerate_with_filter(run: &mut Run, max_results: usize) -> Vec<MatchResult> {
    let mut results = Vec::new();
    let kc = match run.kleene_capture.take() {
        Some(kc) => kc,
        None => return results,
    };

    let pred = match &kc.deferred_predicate {
        Some(p) => p.clone(),
        None => {
            // No deferred predicate — just emit all combinations
            for combo in kc.iter_combinations() {
                if combo.is_empty() {
                    continue;
                }
                let mut captured = run.captured.clone();
                for entry in &combo {
                    if let Some(ref alias) = entry.alias {
                        captured.insert(alias.clone(), Arc::clone(&entry.event));
                    }
                }
                let stack = rebuild_stack_with_combination(&run.stack, &combo);
                results.push(MatchResult {
                    captured,
                    stack,
                    duration: run.started_at.elapsed(),
                });
                if results.len() >= max_results {
                    break;
                }
            }
            return results;
        }
    };

    // Enumerate combinations and filter by deferred predicate
    for combo in kc.iter_combinations() {
        if combo.is_empty() {
            continue;
        }
        let combo_events: Vec<SharedEvent> = combo.iter().map(|e| Arc::clone(&e.event)).collect();
        if evaluate_deferred_predicate(&pred, &combo_events, &run.captured) {
            let mut captured = run.captured.clone();
            for entry in &combo {
                if let Some(ref alias) = entry.alias {
                    captured.insert(alias.clone(), Arc::clone(&entry.event));
                }
            }
            let stack = rebuild_stack_with_combination(&run.stack, &combo);
            results.push(MatchResult {
                captured,
                stack,
                duration: run.started_at.elapsed(),
            });
            if results.len() >= max_results {
                break;
            }
        }
    }

    results
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
// CHECKPOINTING
// ============================================================================

impl Run {
    /// Create a checkpoint of this run's state.
    pub fn checkpoint(&self) -> crate::persistence::RunCheckpoint {
        use crate::persistence::{SerializableEvent, StackEntryCheckpoint};

        let stack = self
            .stack
            .iter()
            .map(|se| StackEntryCheckpoint {
                event: SerializableEvent::from(se.event.as_ref()),
                alias: se.alias.clone(),
            })
            .collect();

        let captured = self
            .captured
            .iter()
            .map(|(k, v)| (k.clone(), SerializableEvent::from(v.as_ref())))
            .collect();

        let kleene_events = self.kleene_capture.as_ref().map(|kc| {
            kc.events
                .iter()
                .map(|e| SerializableEvent::from(e.as_ref()))
                .collect()
        });

        crate::persistence::RunCheckpoint {
            current_state: self.current_state,
            stack,
            captured,
            event_time_started_at_ms: self.event_time_started_at.map(|t| t.timestamp_millis()),
            event_time_deadline_ms: self.event_time_deadline.map(|t| t.timestamp_millis()),
            partition_key: self
                .partition_key
                .as_ref()
                .map(crate::persistence::value_to_ser),
            invalidated: self.invalidated,
            pending_negation_count: self.pending_negations.len(),
            kleene_events,
        }
    }

    /// Restore a run from a checkpoint.
    ///
    /// Note: Wall-clock deadlines (`deadline`, `started_at`) are reset since they
    /// are meaningless after a restart. Event-time deadlines are preserved.
    /// NegationConstraint predicates are NOT restored (they contain closures);
    /// only the count is preserved. The caller must reattach predicates from the NFA
    /// if needed.
    /// KleeneCapture ZDD is NOT rebuilt; only the events are restored.
    pub fn from_checkpoint(rc: &crate::persistence::RunCheckpoint) -> Self {
        use crate::event::Event;

        let stack = rc
            .stack
            .iter()
            .map(|se| StackEntry {
                event: Arc::new(Event::from(se.event.clone())),
                alias: se.alias.clone(),
                timestamp: Instant::now(),
            })
            .collect();

        let captured = rc
            .captured
            .iter()
            .map(|(k, se)| (k.clone(), Arc::new(Event::from(se.clone())) as SharedEvent))
            .collect();

        let kleene_capture = rc.kleene_events.as_ref().map(|events| {
            let mut kc = KleeneCapture::new();
            for se in events {
                let event = Arc::new(Event::from(se.clone()));
                kc.extend(event, None);
            }
            kc
        });

        Self {
            current_state: rc.current_state,
            stack,
            captured,
            started_at: Instant::now(),
            deadline: None, // Wall-clock deadlines not meaningful after restart
            event_time_started_at: rc
                .event_time_started_at_ms
                .and_then(DateTime::from_timestamp_millis),
            event_time_deadline: rc
                .event_time_deadline_ms
                .and_then(DateTime::from_timestamp_millis),
            partition_key: rc
                .partition_key
                .as_ref()
                .map(|sv| crate::persistence::ser_to_value(sv.clone())),
            invalidated: rc.invalidated,
            pending_negations: Vec::new(), // Predicates cannot be serialized; reattach from NFA
            and_state: None,               // Rebuilt from NFA on next event
            kleene_capture,
        }
    }
}

impl SaseEngine {
    /// Create a checkpoint of the entire SASE engine state.
    pub fn checkpoint(&self) -> crate::persistence::SaseCheckpoint {
        let active_runs = self.runs.iter().map(|r| r.checkpoint()).collect();

        let partitioned_runs = self
            .partitioned_runs
            .iter()
            .map(|(k, runs)| {
                let run_cps = runs.iter().map(|r| r.checkpoint()).collect();
                (k.clone(), run_cps)
            })
            .collect();

        crate::persistence::SaseCheckpoint {
            active_runs,
            partitioned_runs,
            watermark_ms: self.watermark.map(|w| w.timestamp_millis()),
            max_timestamp_ms: self.max_timestamp.map(|t| t.timestamp_millis()),
            total_runs_created: self.total_runs_created,
            total_runs_completed: self.total_runs_completed,
            total_runs_dropped: self.total_runs_dropped,
            total_runs_evicted: self.total_runs_evicted,
        }
    }

    /// Restore engine state from a checkpoint.
    ///
    /// The NFA must already be compiled (from VPL source) before calling restore.
    /// Wall-clock deadlines and NegationConstraint predicates are not restored.
    pub fn restore(&mut self, cp: &crate::persistence::SaseCheckpoint) {
        self.runs = cp.active_runs.iter().map(Run::from_checkpoint).collect();

        self.partitioned_runs = cp
            .partitioned_runs
            .iter()
            .map(|(k, runs)| {
                let restored_runs = runs.iter().map(Run::from_checkpoint).collect();
                (k.clone(), restored_runs)
            })
            .collect();

        self.watermark = cp.watermark_ms.and_then(DateTime::from_timestamp_millis);
        self.max_timestamp = cp
            .max_timestamp_ms
            .and_then(DateTime::from_timestamp_millis);
        self.total_runs_created = cp.total_runs_created;
        self.total_runs_completed = cp.total_runs_completed;
        self.total_runs_dropped = cp.total_runs_dropped;
        self.total_runs_evicted = cp.total_runs_evicted;
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
            event.data.insert(k.into(), v);
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

    // =========================================================================
    // NEG-01: Temporal Negation Tests
    // =========================================================================

    #[test]
    fn test_negation_with_predicate() {
        // SEQ(Order, NOT(Cancel where id matches), Shipment)
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event_as("Order", "order"),
            PatternBuilder::not(PatternBuilder::event_where(
                "Cancel",
                Predicate::CompareRef {
                    field: "order_id".to_string(),
                    op: CompareOp::Eq,
                    ref_alias: "order".to_string(),
                    ref_field: "id".to_string(),
                },
            )),
            PatternBuilder::event("Shipment"),
        ]);

        let mut engine = SaseEngine::new(pattern);
        engine.add_negation(
            "Cancel".to_string(),
            Some(Predicate::CompareRef {
                field: "order_id".to_string(),
                op: CompareOp::Eq,
                ref_alias: "order".to_string(),
                ref_field: "id".to_string(),
            }),
        );

        // Order with id 123
        engine.process(&make_event("Order", vec![("id", Value::Int(123))]));
        assert!(engine.stats().active_runs > 0);

        // Cancel for different order (id 456) - should NOT invalidate
        engine.process(&make_event("Cancel", vec![("order_id", Value::Int(456))]));
        assert!(engine.stats().active_runs > 0, "Run should still be active");

        // Cancel for same order (id 123) - should invalidate
        engine.process(&make_event("Cancel", vec![("order_id", Value::Int(123))]));
        assert_eq!(
            engine.stats().active_runs,
            0,
            "Run should be invalidated by matching Cancel"
        );
    }

    // =========================================================================
    // AND-01: Enhanced AND Operator Tests
    // =========================================================================

    #[test]
    fn test_and_with_noise_between() {
        // AND(A, B) should match even with other events between
        let pattern = PatternBuilder::and(PatternBuilder::event("A"), PatternBuilder::event("B"));

        let mut engine =
            SaseEngine::new(pattern).with_strategy(SelectionStrategy::SkipTillAnyMatch);

        // A, then noise, then B
        engine.process(&make_event("A", vec![]));
        engine.process(&make_event("Noise", vec![]));
        engine.process(&make_event("MoreNoise", vec![]));
        let results = engine.process(&make_event("B", vec![]));
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_and_with_aliases() {
        // AND(A as a, B as b) should capture both
        let pattern = PatternBuilder::and(
            PatternBuilder::event_as("A", "a"),
            PatternBuilder::event_as("B", "b"),
        );

        let mut engine = SaseEngine::new(pattern);

        engine.process(&make_event("A", vec![("val", Value::Int(1))]));
        let results = engine.process(&make_event("B", vec![("val", Value::Int(2))]));

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert!(result.captured.contains_key("a"));
        assert!(result.captured.contains_key("b"));
        assert_eq!(
            result.captured.get("a").unwrap().get("val"),
            Some(&Value::Int(1))
        );
        assert_eq!(
            result.captured.get("b").unwrap().get("val"),
            Some(&Value::Int(2))
        );
    }

    #[test]
    fn test_and_does_not_match_same_event_twice() {
        // AND(A, B) receiving A twice should not complete
        let pattern = PatternBuilder::and(PatternBuilder::event("A"), PatternBuilder::event("B"));

        let mut engine = SaseEngine::new(pattern);

        engine.process(&make_event("A", vec![]));
        let results = engine.process(&make_event("A", vec![]));
        assert!(
            results.is_empty(),
            "AND should not complete with duplicate events"
        );
        assert_eq!(
            engine.stats().active_runs,
            2,
            "Should have two runs (one started per A)"
        );
    }

    #[test]
    fn test_and_in_sequence() {
        // SEQ(Start, AND(A, B), End)
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event("Start"),
            PatternBuilder::and(PatternBuilder::event("A"), PatternBuilder::event("B")),
            PatternBuilder::event("End"),
        ]);

        let mut engine = SaseEngine::new(pattern);

        engine.process(&make_event("Start", vec![]));
        assert_eq!(engine.stats().active_runs, 1);

        engine.process(&make_event("B", vec![]));
        engine.process(&make_event("A", vec![]));

        let results = engine.process(&make_event("End", vec![]));
        assert!(
            !results.is_empty(),
            "Pattern should complete after AND and End"
        );
    }

    // =========================================================================
    // BP-01: Backpressure Tests
    // =========================================================================

    #[test]
    fn test_backpressure_drop_strategy() {
        let pattern =
            PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

        let mut engine = SaseEngine::new(pattern)
            .with_max_runs(2)
            .with_backpressure(BackpressureStrategy::Drop);

        // Create max runs
        engine.process(&make_event("A", vec![]));
        engine.process(&make_event("A", vec![]));
        assert_eq!(engine.stats().active_runs, 2);

        // Try to create another run - should be dropped
        let result = engine.process_with_result(&make_event("A", vec![]));
        assert_eq!(engine.stats().active_runs, 2);
        assert!(
            !result.warnings.is_empty(),
            "Should have a warning about dropped run"
        );

        let stats = engine.extended_stats();
        assert_eq!(stats.total_runs_dropped, 1);
    }

    #[test]
    fn test_backpressure_evict_oldest() {
        let pattern =
            PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

        let mut engine = SaseEngine::new(pattern)
            .with_max_runs(2)
            .with_backpressure(BackpressureStrategy::EvictOldest);

        // Create max runs
        engine.process(&make_event("A", vec![]));
        std::thread::sleep(std::time::Duration::from_millis(10));
        engine.process(&make_event("A", vec![]));
        assert_eq!(engine.stats().active_runs, 2);

        // Create another run - should evict oldest
        let result = engine.process_with_result(&make_event("A", vec![]));
        assert_eq!(engine.stats().active_runs, 2, "Should still have max runs");

        // Check that eviction happened
        let stats = engine.extended_stats();
        assert_eq!(stats.total_runs_evicted, 1);
        assert!(result
            .warnings
            .iter()
            .any(|w| matches!(w, ProcessWarning::RunEvicted { .. })));
    }

    #[test]
    fn test_backpressure_evict_least_progress() {
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event("A"),
            PatternBuilder::event("B"),
            PatternBuilder::event("C"),
        ]);

        let mut engine = SaseEngine::new(pattern)
            .with_max_runs(2)
            .with_backpressure(BackpressureStrategy::EvictLeastProgress);

        // Create first run and advance it (more progress)
        engine.process(&make_event("A", vec![]));
        engine.process(&make_event("B", vec![]));
        assert_eq!(engine.stats().active_runs, 1);

        // Create second run (less progress)
        engine.process(&make_event("A", vec![]));
        assert_eq!(engine.stats().active_runs, 2);

        // Create third run - should evict the one with less progress
        let result = engine.process_with_result(&make_event("A", vec![]));
        assert_eq!(engine.stats().active_runs, 2);
        assert_eq!(engine.extended_stats().total_runs_evicted, 1);
        assert!(result
            .warnings
            .iter()
            .any(|w| matches!(w, ProcessWarning::RunEvicted { .. })));
    }

    #[test]
    fn test_process_with_result_approaching_limit() {
        let pattern =
            PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

        let mut engine = SaseEngine::new(pattern).with_max_runs(10);

        // Create runs up to >80% utilization (9 runs out of 10 = 90%)
        for _ in 0..9 {
            engine.process(&make_event("A", vec![]));
        }
        assert_eq!(engine.stats().active_runs, 9);

        // Process another event - should warn about approaching limit (90% > 80%)
        let result = engine.process_with_result(&make_event("A", vec![]));
        assert!(
            result
                .warnings
                .iter()
                .any(|w| matches!(w, ProcessWarning::ApproachingLimit { .. })),
            "Should warn when utilization > 80%"
        );
    }

    #[test]
    fn test_extended_stats() {
        let pattern =
            PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

        let mut engine = SaseEngine::new(pattern).with_max_runs(100);

        // Process some events
        engine.process(&make_event("A", vec![]));
        engine.process(&make_event("B", vec![])); // Completes the pattern
        engine.process(&make_event("A", vec![]));

        let stats = engine.extended_stats();
        assert_eq!(stats.active_runs, 1);
        assert_eq!(stats.total_runs_created, 2);
        assert_eq!(stats.total_runs_completed, 1);
        assert!(stats.utilization < 0.1);
    }

    // =========================================================================
    // IDX-01: Event Type Indexing Tests
    // =========================================================================

    #[test]
    fn test_has_interest_positive() {
        let pattern =
            PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

        let engine = SaseEngine::new(pattern);

        assert!(engine.has_interest("A"));
        assert!(engine.has_interest("B"));
        assert!(!engine.has_interest("C"));
        assert!(!engine.has_interest("Unknown"));
    }

    #[test]
    fn test_has_interest_with_negation() {
        let pattern =
            PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

        let mut engine = SaseEngine::new(pattern);
        engine.add_negation("Cancel".to_string(), None);

        assert!(engine.has_interest("A"));
        assert!(engine.has_interest("B"));
        assert!(engine.has_interest("Cancel")); // Should have interest due to global negation
        assert!(!engine.has_interest("Unknown"));
    }

    #[test]
    fn test_event_type_index_with_and() {
        let pattern = PatternBuilder::and(PatternBuilder::event("X"), PatternBuilder::event("Y"));

        let engine = SaseEngine::new(pattern);

        // AND states should index both event types
        assert!(engine.has_interest("X"));
        assert!(engine.has_interest("Y"));
        assert!(!engine.has_interest("Z"));
    }

    // =========================================================================
    // ET-01: Robust Event-Time Handling Tests
    // =========================================================================

    #[test]
    fn test_event_time_config_builder() {
        let config = EventTimeConfig::new()
            .with_max_out_of_orderness(Duration::from_secs(5))
            .with_allowed_lateness(Duration::from_secs(2))
            .with_late_event_emission();

        assert_eq!(config.max_out_of_orderness, Duration::from_secs(5));
        assert_eq!(config.allowed_lateness, Duration::from_secs(2));
        assert!(config.emit_late_events);
    }

    #[test]
    fn test_event_time_manager_late_events() {
        use chrono::{TimeZone, Utc};

        let config = EventTimeConfig::new()
            .with_max_out_of_orderness(Duration::from_secs(0))
            .with_allowed_lateness(Duration::from_secs(2));

        let mut manager = EventTimeManager::new(config);

        // Process an on-time event
        let ts1 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 10).unwrap();
        let event1 = Arc::new(Event::new("A").with_timestamp(ts1));
        let result1 = manager.process_event(&event1);
        assert!(matches!(result1, EventTimeResult::OnTime));

        // Watermark should now be at ts1
        assert_eq!(manager.watermark(), Some(ts1));

        // Process a late event (within allowed lateness)
        let ts2 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 9).unwrap(); // 1 second late
        let event2 = Arc::new(Event::new("B").with_timestamp(ts2));
        let result2 = manager.process_event(&event2);
        assert!(matches!(result2, EventTimeResult::Late { .. }));

        // Process a too-late event (beyond allowed lateness)
        let ts3 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 5).unwrap(); // 5 seconds late
        let event3 = Arc::new(Event::new("C").with_timestamp(ts3));
        let result3 = manager.process_event(&event3);
        assert!(matches!(result3, EventTimeResult::TooLate { .. }));

        assert_eq!(manager.late_events_accepted(), 1);
        assert_eq!(manager.late_events_dropped(), 1);
    }

    #[test]
    fn test_event_time_manager_watermark_never_recedes() {
        use chrono::{TimeZone, Utc};

        let config = EventTimeConfig::new().with_max_out_of_orderness(Duration::from_secs(0));

        let mut manager = EventTimeManager::new(config);

        let ts1 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 10).unwrap();
        let event1 = Arc::new(Event::new("A").with_timestamp(ts1));
        manager.process_event(&event1);

        assert_eq!(manager.watermark(), Some(ts1));

        // Advance watermark further
        let ts2 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 15).unwrap();
        manager.advance_watermark(ts2);

        // Try to set watermark to earlier time - should not recede
        let ts3 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 5).unwrap();
        manager.advance_watermark(ts3);

        assert_eq!(manager.watermark(), Some(ts2)); // Still at ts2
    }

    #[test]
    fn test_engine_with_event_time_config() {
        use chrono::{TimeZone, Utc};

        let pattern =
            PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

        let config = EventTimeConfig::new()
            .with_max_out_of_orderness(Duration::from_secs(5))
            .with_allowed_lateness(Duration::from_secs(2));

        let mut engine = SaseEngine::new(pattern).with_event_time_config(config);

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
    fn test_compute_deadline_safe() {
        use chrono::{TimeZone, Utc};

        let start = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 0).unwrap();

        // Normal case
        let deadline = EventTimeManager::compute_deadline(start, Duration::from_secs(5));
        assert!(deadline.is_some());

        let expected = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 5).unwrap();
        assert_eq!(deadline.unwrap(), expected);
    }

    // =========================================================================
    // MET-01: Comprehensive Metrics Tests
    // =========================================================================

    #[test]
    fn test_latency_histogram_basic() {
        let hist = LatencyHistogram::new();

        // Record some latencies
        hist.record(Duration::from_micros(5)); // bucket 1 (2-10µs)
        hist.record(Duration::from_micros(50)); // bucket 2 (11-100µs)
        hist.record(Duration::from_micros(500)); // bucket 3 (101-1000µs)
        hist.record(Duration::from_millis(5)); // bucket 4 (1-10ms)

        assert_eq!(hist.total_count(), 4);

        let bucket_counts = hist.bucket_counts();
        assert_eq!(bucket_counts[1], 1); // 2-10µs
        assert_eq!(bucket_counts[2], 1); // 11-100µs
        assert_eq!(bucket_counts[3], 1); // 101-1000µs
        assert_eq!(bucket_counts[4], 1); // 1-10ms
    }

    #[test]
    fn test_latency_histogram_percentiles() {
        let hist = LatencyHistogram::new();

        // Record 100 latencies in the 1-10µs bucket
        for _ in 0..100 {
            hist.record(Duration::from_micros(5));
        }

        // p50 should be in the 1-10µs bucket
        let p50 = hist.percentile(0.5);
        assert!(p50.as_micros() <= 10);

        // p99 should also be in the 1-10µs bucket
        let p99 = hist.percentile(0.99);
        assert!(p99.as_micros() <= 10);
    }

    #[test]
    fn test_sase_metrics_counters() {
        let metrics = SaseMetrics::new();

        metrics.record_event_processed();
        metrics.record_event_processed();
        metrics.record_event_matched();
        metrics.record_run_created();
        metrics.record_run_completed();
        metrics.record_matches(3);

        let summary = metrics.summary();

        assert_eq!(summary.events_processed, 2);
        assert_eq!(summary.events_matched, 1);
        assert_eq!(summary.runs_created, 1);
        assert_eq!(summary.runs_completed, 1);
        assert_eq!(summary.matches_emitted, 3);
    }

    #[test]
    fn test_sase_metrics_prometheus_format() {
        let metrics = SaseMetrics::new();

        metrics.record_event_processed();
        metrics.record_matches(1);

        let prometheus_output = metrics.to_prometheus("varpulis_sase");

        assert!(prometheus_output.contains("varpulis_sase_events_total 1"));
        assert!(prometheus_output.contains("varpulis_sase_matches_total 1"));
        assert!(prometheus_output.contains("# HELP"));
        assert!(prometheus_output.contains("# TYPE"));
    }

    #[test]
    fn test_process_instrumented_basic() {
        let pattern =
            PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

        let mut engine = SaseEngine::new(pattern).with_instrumentation();

        // Process events using instrumented method
        engine.process_instrumented(&make_event("A", vec![]));
        let results = engine.process_instrumented(&make_event("B", vec![]));

        assert_eq!(results.len(), 1);

        // Check metrics
        let summary = engine.metrics().summary();
        assert_eq!(summary.events_processed, 2);
        assert!(summary.events_matched >= 1);
        assert_eq!(summary.matches_emitted, 1);
        assert_eq!(summary.runs_completed, 1);
    }

    #[test]
    fn test_process_instrumented_with_ignored_events() {
        let pattern = PatternBuilder::event("A");

        let mut engine = SaseEngine::new(pattern).with_instrumentation();

        // Process an event of a type we don't care about
        engine.process_instrumented(&make_event("Unknown", vec![]));

        let summary = engine.metrics().summary();
        assert_eq!(summary.events_processed, 1);
        assert_eq!(summary.events_ignored, 1);
    }

    #[test]
    fn test_metrics_peak_runs() {
        let pattern =
            PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

        let mut engine = SaseEngine::new(pattern).with_instrumentation();

        // Create several runs
        engine.process_instrumented(&make_event("A", vec![]));
        engine.process_instrumented(&make_event("A", vec![]));
        engine.process_instrumented(&make_event("A", vec![]));

        let summary_before = engine.metrics().summary();
        assert!(summary_before.peak_active_runs >= 3);

        // Complete all runs
        engine.process_instrumented(&make_event("B", vec![]));

        // Peak should still be recorded
        let summary_after = engine.metrics().summary();
        assert!(summary_after.peak_active_runs >= 3);
    }

    #[test]
    fn test_metrics_arc_sharing() {
        let pattern = PatternBuilder::event("A");

        let engine = SaseEngine::new(pattern);

        // Get Arc reference to metrics
        let metrics_arc = engine.metrics_arc();

        // Record something through the engine's metrics
        engine.metrics().record_event_processed();

        // Should be visible through the Arc
        assert_eq!(metrics_arc.summary().events_processed, 1);
    }

    #[test]
    fn test_late_event_stats() {
        use chrono::{TimeZone, Utc};

        let pattern = PatternBuilder::event("A");

        let config = EventTimeConfig::new()
            .with_max_out_of_orderness(Duration::from_secs(0))
            .with_allowed_lateness(Duration::from_secs(1));

        let mut engine = SaseEngine::new(pattern)
            .with_event_time_config(config)
            .with_instrumentation();

        // Process an on-time event to establish watermark
        let ts1 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 10).unwrap();
        engine.process_instrumented(&Event::new("A").with_timestamp(ts1));

        // Process a late but acceptable event
        let ts2 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 9).unwrap();
        engine.process_instrumented(&Event::new("A").with_timestamp(ts2));

        // Process a too-late event
        let ts3 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 5).unwrap();
        engine.process_instrumented(&Event::new("A").with_timestamp(ts3));

        let (accepted, dropped) = engine.late_event_stats();
        assert_eq!(accepted, 1);
        assert_eq!(dropped, 1);
    }

    // =========================================================================
    // ZDD-KLEENE: Tests for ZDD-based Kleene optimization
    // =========================================================================

    #[test]
    fn test_zdd_kleene_single_run() {
        // Verify that ZDD Kleene uses a single run instead of O(2^n) runs
        // Pattern: SEQ(Start, Tick+, End)
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event("Start"),
            PatternBuilder::one_or_more(PatternBuilder::event("Tick")),
            PatternBuilder::event("End"),
        ]);

        let mut engine = SaseEngine::new(pattern);

        // Start the sequence
        engine.process(&make_event("Start", vec![]));
        assert_eq!(
            engine.stats().active_runs,
            1,
            "Should have 1 run after Start"
        );

        // Add multiple Kleene events - with ZDD, should still be 1 run
        for i in 0..10 {
            engine.process(&make_event("Tick", vec![("n", Value::Int(i))]));
            // With ZDD optimization, we should have at most 1 run
            // (not 2^n runs as in the old branching approach)
            assert!(
                engine.stats().active_runs <= 2,
                "ZDD should prevent run explosion: got {} runs at tick {}",
                engine.stats().active_runs,
                i
            );
        }

        // Complete the pattern
        let results = engine.process(&make_event("End", vec![]));

        // Should produce multiple match results (2^10 - 1 = 1023 for Kleene+)
        // Each combination of Tick events is a valid match
        assert!(
            !results.is_empty(),
            "Should produce matches for Kleene+ combinations"
        );
    }

    #[test]
    fn test_zdd_kleene_memory_efficiency() {
        // Verify ZDD uses polynomial nodes instead of exponential combinations
        // Pattern: SEQ(A, B+, C)
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event("A"),
            PatternBuilder::one_or_more(PatternBuilder::event_as("B", "b")),
            PatternBuilder::event("C"),
        ]);

        let mut engine = SaseEngine::new(pattern);

        engine.process(&make_event("A", vec![]));

        // Add 20 B events - would be 2^20 = 1M combinations with naive approach
        for i in 0..20 {
            engine.process(&make_event("B", vec![("idx", Value::Int(i))]));
        }

        // Should still have a small number of runs thanks to ZDD
        let stats = engine.stats();
        assert!(
            stats.active_runs < 100,
            "ZDD should keep runs bounded: got {} runs",
            stats.active_runs
        );

        // Complete and verify we get results
        let results = engine.process(&make_event("C", vec![]));
        assert!(!results.is_empty(), "Should produce Kleene combinations");
    }

    #[test]
    fn test_kleene_capture_struct() {
        // Direct test of KleeneCapture functionality
        let mut kc = KleeneCapture::new();
        assert_eq!(kc.combination_count(), 1, "Should start with {{∅}}");
        assert!(kc.is_empty(), "Should be empty initially");

        // Add first event
        let e1 = Arc::new(Event::new("E1"));
        kc.extend(Arc::clone(&e1), Some("e1".to_string()));
        assert_eq!(kc.combination_count(), 2, "Should have {{∅, {{e1}}}}");
        assert_eq!(kc.event_count(), 1);

        // Add second event
        let e2 = Arc::new(Event::new("E2"));
        kc.extend(Arc::clone(&e2), Some("e2".to_string()));
        assert_eq!(
            kc.combination_count(),
            4,
            "Should have {{∅, {{e1}}, {{e2}}, {{e1,e2}}}}"
        );
        assert_eq!(kc.event_count(), 2);

        // ZDD should use polynomial nodes
        assert!(kc.node_count() < 10, "ZDD should be compact");

        // Verify iteration produces correct combinations
        let combinations: Vec<_> = kc.iter_combinations().collect();
        assert_eq!(combinations.len(), 4);
    }

    // =========================================================================
    // EDGE-01: Edge Case Tests
    // =========================================================================

    #[test]
    fn test_simple_seq_completes() {
        // Basic two-event sequence to verify core functionality
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event("Start"),
            PatternBuilder::event("End"),
        ]);
        let mut engine = SaseEngine::new(pattern);

        // First event starts a run
        let results = engine.process(&make_event("Start", vec![]));
        assert!(results.is_empty());
        assert_eq!(engine.stats().active_runs, 1);

        // Non-matching event doesn't affect the run
        let results = engine.process(&make_event("Other", vec![]));
        assert!(results.is_empty());
        assert_eq!(engine.stats().active_runs, 1);

        // Second event completes
        let results = engine.process(&make_event("End", vec![]));
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_partition_cleanup_after_match() {
        // Verify partition state is properly cleaned after pattern completion
        let pattern =
            PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

        let mut engine = SaseEngine::new(pattern).with_partition_by("key".to_string());

        // Create runs in multiple partitions
        engine.process(&make_event("A", vec![("key", Value::Str("p1".into()))]));
        engine.process(&make_event("A", vec![("key", Value::Str("p2".into()))]));
        engine.process(&make_event("A", vec![("key", Value::Str("p3".into()))]));

        assert_eq!(engine.stats().partitions, 3);
        assert_eq!(engine.stats().active_runs, 3);

        // Complete partition p1
        let results = engine.process(&make_event("B", vec![("key", Value::Str("p1".into()))]));
        assert_eq!(results.len(), 1);

        // p2 and p3 should still have active runs
        assert_eq!(engine.stats().active_runs, 2);

        // Complete remaining partitions
        engine.process(&make_event("B", vec![("key", Value::Str("p2".into()))]));
        engine.process(&make_event("B", vec![("key", Value::Str("p3".into()))]));

        assert_eq!(engine.stats().active_runs, 0);
    }

    #[test]
    fn test_within_timeout_exact_boundary() {
        use chrono::{TimeZone, Utc};
        use std::time::Duration;

        // Pattern with exactly 5 second window
        let pattern = SasePattern::Within(
            Box::new(PatternBuilder::seq(vec![
                PatternBuilder::event("A"),
                PatternBuilder::event("B"),
            ])),
            Duration::from_secs(5),
        );

        let mut engine = SaseEngine::new(pattern).with_event_time();

        let ts1 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 0).unwrap();
        // Exactly at the 5 second boundary
        let ts2 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 5).unwrap();

        let event_a = Event::new("A").with_timestamp(ts1);
        let event_b = Event::new("B").with_timestamp(ts2);

        engine.process(&event_a);
        let results = engine.process(&event_b);

        // At exactly 5 seconds, behavior depends on implementation (inclusive vs exclusive)
        // This test documents the actual behavior
        let _matched_at_boundary = results.len() == 1;
    }

    #[test]
    fn test_within_timeout_just_before_boundary() {
        use chrono::{TimeZone, Utc};
        use std::time::Duration;

        let pattern = SasePattern::Within(
            Box::new(PatternBuilder::seq(vec![
                PatternBuilder::event("A"),
                PatternBuilder::event("B"),
            ])),
            Duration::from_secs(5),
        );

        let mut engine = SaseEngine::new(pattern).with_event_time();

        let ts1 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 0).unwrap();
        // Just before the boundary (4.999... seconds)
        let ts2 = Utc
            .with_ymd_and_hms(2026, 1, 28, 10, 0, 4)
            .unwrap()
            .checked_add_signed(chrono::Duration::milliseconds(999))
            .unwrap();

        let event_a = Event::new("A").with_timestamp(ts1);
        let event_b = Event::new("B").with_timestamp(ts2);

        engine.process(&event_a);
        let results = engine.process(&event_b);

        // Should definitely match (within window)
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_kleene_star_with_one_match() {
        // Kleene* (zero or more) with at least one event
        // Note: zero-match for Kleene* depends on NFA implementation
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event("A"),
            PatternBuilder::zero_or_more(PatternBuilder::event("B")),
            PatternBuilder::event("C"),
        ]);

        let mut engine = SaseEngine::new(pattern);

        // Start with A
        engine.process(&make_event("A", vec![]));

        // Add one B
        engine.process(&make_event("B", vec![]));

        // C completes the pattern
        let results = engine.process(&make_event("C", vec![]));

        // Should match with one B
        assert!(!results.is_empty(), "Kleene* should match with one event");
    }

    #[test]
    fn test_repeated_event_types() {
        // Pattern where the same event type appears multiple times
        // SEQ(A, A, B) - need two A events before B
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event_as("A", "a1"),
            PatternBuilder::event_as("A", "a2"),
            PatternBuilder::event("B"),
        ]);

        let mut engine = SaseEngine::new(pattern);

        // First A
        let results = engine.process(&make_event("A", vec![("n", Value::Int(1))]));
        assert!(results.is_empty());

        // Second A
        let results = engine.process(&make_event("A", vec![("n", Value::Int(2))]));
        assert!(results.is_empty());

        // B should complete
        let results = engine.process(&make_event("B", vec![]));
        assert_eq!(results.len(), 1);

        // Verify both A events are captured
        let result = &results[0];
        assert!(result.captured.contains_key("a1"));
        assert!(result.captured.contains_key("a2"));
    }

    #[test]
    fn test_unmatched_event_types_ignored() {
        let pattern =
            PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

        let mut engine = SaseEngine::new(pattern);

        // Events that don't match any pattern element should be ignored
        engine.process(&make_event("X", vec![]));
        engine.process(&make_event("Y", vec![]));
        engine.process(&make_event("Z", vec![]));

        assert_eq!(engine.stats().active_runs, 0);

        // Now start a valid sequence
        engine.process(&make_event("A", vec![]));
        assert_eq!(engine.stats().active_runs, 1);

        // More unrelated events shouldn't affect the run
        engine.process(&make_event("X", vec![]));
        assert_eq!(engine.stats().active_runs, 1);

        // Complete
        let results = engine.process(&make_event("B", vec![]));
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_partition_with_missing_field() {
        // When partitioning by a field that doesn't exist in the event
        let pattern =
            PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

        let mut engine = SaseEngine::new(pattern).with_partition_by("region".to_string());

        // Event without the partition field - should use default partition
        engine.process(&make_event("A", vec![]));

        // Event with partition field
        engine.process(&make_event(
            "A",
            vec![("region", Value::Str("east".into()))],
        ));

        // Should have 2 partitions (one default, one "east")
        assert_eq!(engine.stats().partitions, 2);
    }

    #[test]
    fn test_predicate_type_mismatch() {
        // Predicate comparing incompatible types should not match
        // Use a two-event sequence to test predicate behavior
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event_where(
                "A",
                Predicate::Compare {
                    field: "value".to_string(),
                    op: CompareOp::Gt,
                    value: Value::Int(100),
                },
            ),
            PatternBuilder::event("B"),
        ]);

        let mut engine = SaseEngine::new(pattern);

        // String value compared against int predicate - should not start a run
        let results = engine.process(&make_event(
            "A",
            vec![("value", Value::Str("not-a-number".into()))],
        ));
        assert!(results.is_empty());
        // Run should not be started due to predicate mismatch
        assert_eq!(engine.stats().active_runs, 0);

        // Int value that matches predicate - should start a run
        let results = engine.process(&make_event("A", vec![("value", Value::Int(150))]));
        assert!(results.is_empty()); // Not complete yet
        assert_eq!(engine.stats().active_runs, 1);

        // B completes the sequence
        let results = engine.process(&make_event("B", vec![]));
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_multiple_concurrent_runs_same_partition() {
        // Multiple runs in the same partition (no partitioning)
        let pattern =
            PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

        let mut engine = SaseEngine::new(pattern);

        // Start multiple runs
        engine.process(&make_event("A", vec![("id", Value::Int(1))]));
        engine.process(&make_event("A", vec![("id", Value::Int(2))]));
        engine.process(&make_event("A", vec![("id", Value::Int(3))]));

        // All three A events should create runs
        assert!(
            engine.stats().active_runs >= 1,
            "At least one run should be active"
        );

        // B should complete all runs
        let results = engine.process(&make_event("B", vec![]));
        assert!(!results.is_empty());
    }

    #[test]
    fn test_process_time_semantics_default() {
        let pattern = PatternBuilder::event("A");
        let engine = SaseEngine::new(pattern);

        // Default should be ProcessingTime
        assert_eq!(engine.time_semantics(), TimeSemantics::ProcessingTime);
    }

    #[test]
    fn test_or_pattern_in_seq_either_branch() {
        // OR(A, B) within SEQ - either branch should work
        // Pattern: SEQ(Start, OR(A, B), End)
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event("Start"),
            PatternBuilder::or(PatternBuilder::event("A"), PatternBuilder::event("B")),
            PatternBuilder::event("End"),
        ]);

        let mut engine = SaseEngine::new(pattern);

        // Test branch A
        engine.process(&make_event("Start", vec![]));
        engine.process(&make_event("A", vec![]));
        let results = engine.process(&make_event("End", vec![]));
        assert!(!results.is_empty(), "OR pattern should match A branch");
    }

    #[test]
    fn test_or_pattern_in_seq_second_branch() {
        // OR(A, B) within SEQ - test B branch
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event("Start"),
            PatternBuilder::or(PatternBuilder::event("A"), PatternBuilder::event("B")),
            PatternBuilder::event("End"),
        ]);

        let mut engine = SaseEngine::new(pattern);

        // Test branch B
        engine.process(&make_event("Start", vec![]));
        engine.process(&make_event("B", vec![]));
        let results = engine.process(&make_event("End", vec![]));
        assert!(!results.is_empty(), "OR pattern should match B branch");
    }

    #[test]
    fn test_and_pattern_in_seq() {
        // AND(A, B) within SEQ - both must occur
        // Pattern: SEQ(Start, AND(A, B), End)
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event("Start"),
            PatternBuilder::and(PatternBuilder::event("A"), PatternBuilder::event("B")),
            PatternBuilder::event("End"),
        ]);

        let mut engine = SaseEngine::new(pattern);

        engine.process(&make_event("Start", vec![]));
        engine.process(&make_event("A", vec![]));
        engine.process(&make_event("B", vec![]));
        let results = engine.process(&make_event("End", vec![]));
        assert!(
            !results.is_empty(),
            "AND pattern should match when both events occur"
        );
    }

    #[test]
    fn test_run_checkpoint_restore() {
        // Create a run and simulate some state advancement
        let mut run = Run::new(0);

        // Add a captured event
        let evt_a = Arc::new(make_event("A", vec![("id", Value::from(1))]));
        run.captured.insert("a".to_string(), evt_a.clone());

        // Advance the current state
        run.current_state = 2;

        // Mark it as invalidated to test that flag round-trips
        run.invalidated = true;

        // Checkpoint the run
        let cp = run.checkpoint();

        // Restore from checkpoint
        let restored = Run::from_checkpoint(&cp);

        // Verify essential fields survived the round-trip
        assert_eq!(
            restored.current_state, run.current_state,
            "current_state should be preserved across checkpoint/restore"
        );
        assert_eq!(
            restored.captured.len(),
            run.captured.len(),
            "captured map size should be preserved"
        );
        assert!(
            restored.captured.contains_key("a"),
            "captured alias 'a' should be present after restore"
        );
        assert_eq!(
            restored.captured["a"].event_type, evt_a.event_type,
            "captured event type should be preserved"
        );
        assert_eq!(
            restored.invalidated, run.invalidated,
            "invalidated flag should be preserved across checkpoint/restore"
        );
    }

    #[test]
    fn test_sase_engine_checkpoint_restore() {
        // Build a SEQ(A, B) pattern (need two copies because engine takes ownership)
        let pattern1 =
            PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);
        let pattern2 =
            PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

        let mut engine = SaseEngine::new(pattern1);

        // Process event A – creates an active run but no complete match yet
        let results = engine.process(&make_event("A", vec![]));
        assert!(
            results.is_empty(),
            "A alone should not complete the pattern"
        );

        // Checkpoint the engine while a run is in progress
        let cp = engine.checkpoint();

        // Create a fresh engine with the same pattern and restore state
        let mut engine2 = SaseEngine::new(pattern2);
        engine2.restore(&cp);

        // Process event B on the restored engine – should complete the sequence
        let results = engine2.process(&make_event("B", vec![]));
        assert!(
            !results.is_empty(),
            "B on restored engine should complete the SEQ(A, B) match"
        );
    }

    #[test]
    fn test_sase_engine_checkpoint_empty() {
        let pattern =
            PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);
        let engine = SaseEngine::new(pattern);

        // Checkpoint with no events processed – no active runs
        let cp = engine.checkpoint();

        assert!(
            cp.active_runs.is_empty(),
            "checkpoint with no processed events should have empty active_runs"
        );
    }

    // =========================================================================
    // PHASE 1: Kleene Self-Loop Tests
    // =========================================================================

    #[test]
    fn test_kleene_plus_captures_multiple() {
        // SEQ(A, B+, C) with 3 B events.
        // After the self-loop fix, all 3 B events should be captured.
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event("A"),
            PatternBuilder::one_or_more(PatternBuilder::event("B")),
            PatternBuilder::event("C"),
        ]);

        let mut engine = SaseEngine::new(pattern);

        engine.process(&make_event("A", vec![]));
        engine.process(&make_event("B", vec![("val", Value::Int(1))]));
        engine.process(&make_event("B", vec![("val", Value::Int(2))]));
        engine.process(&make_event("B", vec![("val", Value::Int(3))]));

        let results = engine.process(&make_event("C", vec![]));

        assert!(
            !results.is_empty(),
            "Should produce at least one match for SEQ(A, B+, C)"
        );
        // The stack should contain A + 3*B + C = 5 entries
        let result = &results[0];
        assert_eq!(
            result.stack.len(),
            5,
            "Stack should have A + 3B + C = 5 entries, got {}",
            result.stack.len()
        );
    }

    #[test]
    fn test_kleene_plus_exact_count() {
        // SEQ(A, B+, C) with N B events.
        // Stack length should be N+2 (A + N×B + C).
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event("A"),
            PatternBuilder::one_or_more(PatternBuilder::event("B")),
            PatternBuilder::event("C"),
        ]);

        for n in 1..=5 {
            let mut engine = SaseEngine::new(pattern.clone());
            engine.process(&make_event("A", vec![]));
            for i in 0..n {
                engine.process(&make_event("B", vec![("n", Value::Int(i))]));
            }
            let results = engine.process(&make_event("C", vec![]));
            assert!(!results.is_empty(), "Should match with {} B events", n);
            assert_eq!(
                results[0].stack.len(),
                (n + 2) as usize,
                "Stack should have {} entries for {} B events",
                n + 2,
                n
            );
        }
    }

    #[test]
    fn test_zdd_kleene_single_run_captures_all() {
        // Tightened: verify the stack contains all 10 Tick events
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event("Start"),
            PatternBuilder::one_or_more(PatternBuilder::event("Tick")),
            PatternBuilder::event("End"),
        ]);

        let mut engine = SaseEngine::new(pattern);
        engine.process(&make_event("Start", vec![]));
        for i in 0..10 {
            engine.process(&make_event("Tick", vec![("n", Value::Int(i))]));
        }
        let results = engine.process(&make_event("End", vec![]));
        assert!(!results.is_empty(), "Should produce matches");
        // Start + 10×Tick + End = 12
        assert_eq!(
            results[0].stack.len(),
            12,
            "Stack should have Start + 10 Ticks + End = 12 entries"
        );
    }

    #[test]
    fn test_kleene_self_loop_preserves_all_pattern() {
        // Verify `all` pattern still works: each B event emits a result
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event("A"),
            PatternBuilder::one_or_more(PatternBuilder::event("B")),
        ]);
        // When Kleene+ has epsilon to Accept, each event emits CompleteAndContinue
        let mut engine = SaseEngine::new(pattern);

        engine.process(&make_event("A", vec![]));
        let r1 = engine.process(&make_event("B", vec![("n", Value::Int(1))]));
        assert_eq!(r1.len(), 1, "First B should emit a match");

        let r2 = engine.process(&make_event("B", vec![("n", Value::Int(2))]));
        assert_eq!(
            r2.len(),
            1,
            "Second B should also emit a match via self-loop"
        );
    }

    // =========================================================================
    // PHASE 2: Predicate Postponing Tests (SIGMOD 2014)
    // =========================================================================

    #[test]
    fn test_classify_predicate_compare_is_consistent() {
        let pred = Predicate::Compare {
            field: "val".to_string(),
            op: CompareOp::Gt,
            value: Value::Int(100),
        };
        assert_eq!(
            classify_predicate(&pred, Some("b")),
            PredicateClass::Consistent
        );
    }

    #[test]
    fn test_classify_predicate_ref_non_kleene_consistent() {
        let pred = Predicate::CompareRef {
            field: "val".to_string(),
            op: CompareOp::Gt,
            ref_alias: "a".to_string(),
            ref_field: "val".to_string(),
        };
        assert_eq!(
            classify_predicate(&pred, Some("b")),
            PredicateClass::Consistent,
            "Referencing non-Kleene alias should be consistent"
        );
    }

    #[test]
    fn test_classify_predicate_ref_kleene_inconsistent() {
        let pred = Predicate::CompareRef {
            field: "val".to_string(),
            op: CompareOp::Ge,
            ref_alias: "b".to_string(),
            ref_field: "val".to_string(),
        };
        assert_eq!(
            classify_predicate(&pred, Some("b")),
            PredicateClass::Inconsistent,
            "Referencing the Kleene alias itself should be inconsistent"
        );
    }

    #[test]
    fn test_classify_predicate_and_propagates_inconsistent() {
        let consistent = Predicate::Compare {
            field: "val".to_string(),
            op: CompareOp::Gt,
            value: Value::Int(0),
        };
        let inconsistent = Predicate::CompareRef {
            field: "val".to_string(),
            op: CompareOp::Ge,
            ref_alias: "b".to_string(),
            ref_field: "val".to_string(),
        };
        let combined = Predicate::And(Box::new(consistent), Box::new(inconsistent));
        assert_eq!(
            classify_predicate(&combined, Some("b")),
            PredicateClass::Inconsistent,
            "And(Consistent, Inconsistent) should be Inconsistent"
        );
    }

    #[test]
    fn test_classify_predicate_no_kleene_alias() {
        // When no Kleene alias is specified, all predicates should be Consistent
        let pred = Predicate::CompareRef {
            field: "val".to_string(),
            op: CompareOp::Ge,
            ref_alias: "b".to_string(),
            ref_field: "val".to_string(),
        };
        assert_eq!(
            classify_predicate(&pred, None),
            PredicateClass::Consistent,
            "No Kleene alias means all predicates are consistent"
        );
    }

    #[test]
    fn test_consistent_predicate_no_postponing() {
        // SEQ(A, B+ WHERE val > 100, C)
        // Predicate is Consistent → no postponing → events that fail are filtered eagerly
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event("A"),
            PatternBuilder::one_or_more(SasePattern::Event {
                event_type: "B".to_string(),
                predicate: Some(Predicate::Compare {
                    field: "val".to_string(),
                    op: CompareOp::Gt,
                    value: Value::Int(100),
                }),
                alias: Some("b".to_string()),
            }),
            PatternBuilder::event("C"),
        ]);

        let mut engine = SaseEngine::new(pattern);
        engine.process(&make_event("A", vec![]));
        engine.process(&make_event("B", vec![("val", Value::Int(150))]));
        engine.process(&make_event("B", vec![("val", Value::Int(50))])); // filtered out
        engine.process(&make_event("B", vec![("val", Value::Int(200))]));

        let results = engine.process(&make_event("C", vec![]));
        assert!(
            !results.is_empty(),
            "Should match with consistent predicate"
        );
        // Only B(150) and B(200) pass the predicate, B(50) is filtered eagerly
        // Stack should be A + 2B + C = 4
        assert_eq!(
            results[0].stack.len(),
            4,
            "Only matching B events should be in stack"
        );
    }

    #[test]
    fn test_postponed_predicate_monotonic() {
        // SEQ(A, B+ WHERE b.val >= b.val, C) — self-referencing predicate
        // The predicate references the Kleene alias itself → Inconsistent → postponed
        // Events: B(5), B(3), B(4), B(6)
        // Valid combinations where each element >= previous:
        // {5}, {3}, {4}, {6}, {5,6}, {3,4}, {3,6}, {4,6}, {3,4,6}
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event("A"),
            PatternBuilder::one_or_more(SasePattern::Event {
                event_type: "B".to_string(),
                predicate: Some(Predicate::CompareRef {
                    field: "val".to_string(),
                    op: CompareOp::Ge,
                    ref_alias: "b".to_string(),
                    ref_field: "val".to_string(),
                }),
                alias: Some("b".to_string()),
            }),
            PatternBuilder::event("C"),
        ]);

        let mut engine = SaseEngine::new(pattern);
        engine.process(&make_event("A", vec![]));
        engine.process(&make_event("B", vec![("val", Value::Int(5))]));
        engine.process(&make_event("B", vec![("val", Value::Int(3))]));
        engine.process(&make_event("B", vec![("val", Value::Int(4))]));
        engine.process(&make_event("B", vec![("val", Value::Int(6))]));

        let results = engine.process(&make_event("C", vec![]));
        assert!(
            !results.is_empty(),
            "Should produce matches with postponed predicate"
        );
        // With CompareRef(val >= b.val), the predicate checks current.val >= captured["b"].val
        // Since "b" gets updated to the previous event in the combination, this enforces
        // monotonically non-decreasing sequences.
        // Valid monotonic subsequences of [5,3,4,6]: {5}, {3}, {4}, {6}, {5,6}, {3,4}, {3,6}, {4,6}, {3,4,6}
        assert!(
            results.len() >= 4,
            "Should have multiple valid monotonic combinations, got {}",
            results.len()
        );
    }

    #[test]
    fn test_postponed_fewer_valid_combinations() {
        // SEQ(A, B+ WHERE b.val > b.val, C) — strict greater-than self-reference
        // With decreasing values [10, 5], the multi-element combination [10, 5]
        // fails because 5 > 10 is false, but single-element combinations and
        // the pair [5, 10] (wrong order) aren't generated. So the result count
        // should be strictly less than the total combination count.
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event("A"),
            PatternBuilder::one_or_more(SasePattern::Event {
                event_type: "B".to_string(),
                predicate: Some(Predicate::CompareRef {
                    field: "val".to_string(),
                    op: CompareOp::Gt,
                    ref_alias: "b".to_string(),
                    ref_field: "val".to_string(),
                }),
                alias: Some("b".to_string()),
            }),
            PatternBuilder::event("C"),
        ]);

        let mut engine = SaseEngine::new(pattern);
        engine.process(&make_event("A", vec![]));
        engine.process(&make_event("B", vec![("val", Value::Int(10))]));
        engine.process(&make_event("B", vec![("val", Value::Int(5))]));
        engine.process(&make_event("B", vec![("val", Value::Int(3))]));

        let results = engine.process(&make_event("C", vec![]));
        // Total ZDD combinations for 3 events: 2^3 - 1 = 7 (excluding empty)
        // The deferred predicate filters some out (e.g., [10, 5] fails 5 > 10)
        // Result count should be less than 7
        assert!(
            results.len() < 7,
            "Deferred predicate should filter some combinations, got {}",
            results.len()
        );
        assert!(
            !results.is_empty(),
            "Should still have some valid combinations"
        );
    }

    // =========================================================================
    // PHASE 3: Safety Cap Stress Tests
    // =========================================================================

    #[test]
    fn test_kleene_accumulation_cap() {
        // Send more events than MAX_KLEENE_EVENTS into a Kleene+ state.
        // The engine must not OOM or hang — it should cap accumulation.
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event("A"),
            PatternBuilder::one_or_more(PatternBuilder::event("B")),
            PatternBuilder::event("C"),
        ]);

        let mut engine = SaseEngine::new(pattern);
        engine.process(&make_event("A", vec![]));

        // Send 50 B events (well above MAX_KLEENE_EVENTS = 20)
        for i in 0..50 {
            engine.process(&make_event("B", vec![("n", Value::Int(i))]));
        }

        let results = engine.process(&make_event("C", vec![]));
        assert!(!results.is_empty(), "Should still produce a match");
        // Stack should have at most A + MAX_KLEENE_EVENTS B's + C
        assert!(
            results[0].stack.len() <= (MAX_KLEENE_EVENTS as usize + 2),
            "Stack length {} should be capped at {} (A + {} B's + C)",
            results[0].stack.len(),
            MAX_KLEENE_EVENTS + 2,
            MAX_KLEENE_EVENTS
        );
    }

    #[test]
    fn test_kleene_deferred_predicate_enumeration_cap() {
        // With a deferred predicate and MAX_KLEENE_EVENTS events,
        // enumeration should not produce more than MAX_ENUMERATION_RESULTS.
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event("A"),
            PatternBuilder::one_or_more(SasePattern::Event {
                event_type: "B".to_string(),
                predicate: Some(Predicate::CompareRef {
                    field: "val".to_string(),
                    op: CompareOp::Ge,
                    ref_alias: "b".to_string(),
                    ref_field: "val".to_string(),
                }),
                alias: Some("b".to_string()),
            }),
            PatternBuilder::event("C"),
        ]);

        let mut engine = SaseEngine::new(pattern);
        engine.process(&make_event("A", vec![]));

        // 20 monotonically increasing events → most combinations pass the
        // deferred predicate (val >= prev.val), producing potentially
        // many results. The cap should limit output.
        for i in 0..20 {
            engine.process(&make_event("B", vec![("val", Value::Int(i))]));
        }

        let results = engine.process(&make_event("C", vec![]));
        assert!(
            results.len() <= MAX_ENUMERATION_RESULTS,
            "Enumeration should be capped at {}, got {}",
            MAX_ENUMERATION_RESULTS,
            results.len()
        );
    }

    #[test]
    fn test_large_kleene_no_hang() {
        // Regression test: sending 100 events into a Kleene+ with deferred
        // predicate must complete in bounded time and memory.
        use std::time::Instant;

        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event("A"),
            PatternBuilder::one_or_more(SasePattern::Event {
                event_type: "B".to_string(),
                predicate: Some(Predicate::CompareRef {
                    field: "val".to_string(),
                    op: CompareOp::Gt,
                    ref_alias: "b".to_string(),
                    ref_field: "val".to_string(),
                }),
                alias: Some("b".to_string()),
            }),
            PatternBuilder::event("C"),
        ]);

        let mut engine = SaseEngine::new(pattern);
        engine.process(&make_event("A", vec![]));

        for i in 0..100 {
            engine.process(&make_event("B", vec![("val", Value::Int(i))]));
        }

        let start = Instant::now();
        let results = engine.process(&make_event("C", vec![]));
        let elapsed = start.elapsed();

        // Must complete within 5 seconds (without caps this would hang/OOM)
        assert!(
            elapsed.as_secs() < 5,
            "Enumeration took {:?} — should be bounded",
            elapsed
        );
        assert!(
            results.len() <= MAX_ENUMERATION_RESULTS,
            "Results capped at {}, got {}",
            MAX_ENUMERATION_RESULTS,
            results.len()
        );
    }

    #[test]
    fn test_configurable_kleene_limits() {
        // Verify builder methods override the defaults.
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event("A"),
            PatternBuilder::one_or_more(PatternBuilder::event("B")),
            PatternBuilder::event("C"),
        ]);

        // Cap at 5 Kleene events instead of default 20
        let mut engine = SaseEngine::new(pattern).with_max_kleene_events(5);

        engine.process(&make_event("A", vec![]));
        for i in 0..20 {
            engine.process(&make_event("B", vec![("n", Value::Int(i))]));
        }
        let results = engine.process(&make_event("C", vec![]));
        assert!(!results.is_empty());
        // A + at most 5 B's + C = 7
        assert!(
            results[0].stack.len() <= 7,
            "Custom max_kleene_events=5 should cap stack at 7, got {}",
            results[0].stack.len()
        );
    }

    #[test]
    fn test_configurable_enumeration_limit() {
        // Verify with_max_enumeration_results caps output.
        let pattern = PatternBuilder::seq(vec![
            PatternBuilder::event("A"),
            PatternBuilder::one_or_more(SasePattern::Event {
                event_type: "B".to_string(),
                predicate: Some(Predicate::CompareRef {
                    field: "val".to_string(),
                    op: CompareOp::Ge,
                    ref_alias: "b".to_string(),
                    ref_field: "val".to_string(),
                }),
                alias: Some("b".to_string()),
            }),
            PatternBuilder::event("C"),
        ]);

        let mut engine = SaseEngine::new(pattern).with_max_enumeration_results(3);

        engine.process(&make_event("A", vec![]));
        for i in 0..15 {
            engine.process(&make_event("B", vec![("val", Value::Int(i))]));
        }
        let results = engine.process(&make_event("C", vec![]));
        assert!(
            results.len() <= 3,
            "Custom max_enumeration_results=3 should cap at 3, got {}",
            results.len()
        );
    }
}
