//! Core SASE+ types and constants

use crate::event::Event;
use rustc_hash::FxHashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use varpulis_core::Value;

/// Shared event reference for efficient cloning in pattern matching.
/// Using Arc allows multiple pattern runs to share the same event data
/// without expensive deep copies.
pub type SharedEvent = Arc<Event>;

/// Safety cap on events accumulated in a single Kleene closure.
/// With n events the ZDD enumerates up to 2^n - 1 combinations.
/// 20 events → ~1 M combinations (safe); 30 → ~1 B (OOM risk).
pub const MAX_KLEENE_EVENTS: u32 = 20;

/// Safety cap on results emitted by `enumerate_with_filter`.
/// Prevents unbounded memory growth when the deferred predicate
/// passes most combinations of a large Kleene closure.
pub const MAX_ENUMERATION_RESULTS: usize = 10_000;

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
/// ```rust,no_run
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

/// Engine statistics
#[derive(Debug, Clone)]
pub struct SaseStats {
    pub active_runs: usize,
    pub partitions: usize,
    pub nfa_states: usize,
}
