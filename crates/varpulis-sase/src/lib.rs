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

#![warn(missing_docs)]

// Module declarations
mod advance;
mod and_op;
mod backpressure;
mod builder;
mod engine;
mod enumeration;
mod event_index;
mod event_time;
mod kleene;
mod metrics;
mod negation;
mod nfa;
pub(crate) mod predicate;
mod run;
mod types;

// Re-export all public types to maintain API compatibility
pub use and_op::{AndBranch, AndConfig, AndState, NegationInfo};
pub use backpressure::{
    BackpressureError, BackpressureStrategy, ProcessResult, ProcessStats, ProcessWarning,
    SaseExtendedStats,
};
pub use builder::PatternBuilder;
pub use engine::SaseEngine;
pub use event_index::EventTypeIndex;
pub use event_time::{EventTimeConfig, EventTimeManager, EventTimeResult};
pub use kleene::KleeneCapture;
pub use metrics::{LatencyHistogram, MetricsSummary, SaseMetrics};
pub use negation::NegationConstraint;
pub use nfa::{Nfa, NfaCompiler, State, StateType};
pub use run::Run;
pub use types::{
    CompareOp, GlobalNegation, MatchResult, Predicate, RunSnapshot, SasePattern, SaseStats,
    SelectionStrategy, SharedEvent, StackEntry, TimeSemantics, MAX_ENUMERATION_RESULTS,
    MAX_KLEENE_EVENTS,
};

pub use predicate::{classify_predicate, PredicateClass};

// ---------------------------------------------------------------------------
// Expression evaluator trait (abstraction for engine dependency)
// ---------------------------------------------------------------------------

use rustc_hash::FxHashMap;
use varpulis_core::{Event, Value};

/// Context for evaluating sequence expressions within SASE predicates.
///
/// This provides captured events from prior sequence steps so that
/// cross-event comparisons (e.g., `a.price < b.price`) can be evaluated.
#[derive(Debug, Clone, Default)]
pub struct SequenceContext {
    /// Captured events by alias
    pub captured: FxHashMap<String, Event>,
    /// Previous event in sequence (accessible as `$`)
    pub previous: Option<Event>,
}

/// Trait for evaluating custom expressions in SASE predicates.
///
/// Implementations bridge the SASE engine to a host expression evaluator
/// (e.g., the VPL runtime engine's `eval_filter_expr`).
pub trait ExprEvaluator: Send + Sync {
    /// Evaluate an expression against an event and captured context.
    ///
    /// Returns `Some(Value)` on success, `None` if the expression cannot be evaluated.
    fn eval(
        &self,
        expr: &varpulis_core::ast::Expr,
        event: &Event,
        captured: &FxHashMap<String, SharedEvent>,
    ) -> Option<Value>;
}
