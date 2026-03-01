//! Backpressure strategies (BP-01)

use super::types::MatchResult;
use std::time::Duration;

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
    /// Number of currently active partial-match runs.
    pub active_runs: usize,
    /// Number of partition buckets in use.
    pub partitions: usize,
    /// Number of states in the compiled NFA.
    pub nfa_states: usize,
    /// Total runs dropped due to backpressure since engine creation.
    pub total_runs_dropped: u64,
    /// Total runs evicted due to backpressure since engine creation.
    pub total_runs_evicted: u64,
    /// Total runs created since engine creation.
    pub total_runs_created: u64,
    /// Total runs that completed with a match since engine creation.
    pub total_runs_completed: u64,
    /// Current utilization (0.0 to 1.0)
    pub utilization: f64,
}
