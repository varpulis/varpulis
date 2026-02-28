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
mod persistence;
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
    CompareOp, GlobalNegation, MatchResult, Predicate, SasePattern, SaseStats, SelectionStrategy,
    SharedEvent, StackEntry, TimeSemantics, MAX_ENUMERATION_RESULTS, MAX_KLEENE_EVENTS,
};

// Re-export internal items needed by tests
#[cfg(test)]
pub(crate) use predicate::{classify_predicate, PredicateClass};

#[cfg(test)]
mod tests;
