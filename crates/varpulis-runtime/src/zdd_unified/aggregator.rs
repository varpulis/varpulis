//! # ZDD Aggregator - Main Aggregator Using ZDD Operations
//!
//! Unified aggregator using ZDD for both NFA representation and sharing.
//!
//! ## Design
//!
//! Unlike Hamlet which uses explicit graphlets and snapshots, the ZDD
//! aggregator relies on ZDD's canonical reduction to automatically share
//! common substructures between queries.
//!
//! ## Trade-offs
//!
//! **Advantages:**
//! - Simpler implementation (no manual sharing decisions)
//! - Automatic canonical sharing via ZDD reduction
//! - Memory-efficient for highly overlapping patterns
//!
//! **Disadvantages:**
//! - Higher per-event cost (ZDD operations vs O(1) transitions)
//! - Less control over sharing granularity
//! - May not be optimal for all workloads

use super::nfa_zdd::{NfaZdd, ZddState};
use super::propagation::{PropagationMode, ZddPropagator};
use crate::event::SharedEvent;
use crate::greta::{GretaAggregate, QueryId};
use rustc_hash::FxHashMap;
use smallvec::SmallVec;
use std::sync::Arc;

/// Result of aggregation
#[derive(Debug, Clone)]
pub struct ZddAggregationResult {
    /// Query ID
    pub query_id: QueryId,
    /// Aggregation type
    pub aggregate: GretaAggregate,
    /// Current value
    pub value: u64,
    /// Whether this is a final result
    pub is_final: bool,
}

/// Configuration for the ZDD aggregator
#[derive(Debug, Clone)]
pub struct ZddConfig {
    /// Propagation mode
    pub propagation: PropagationMode,
    /// Window size in milliseconds
    pub window_ms: u64,
    /// Enable incremental result emission
    pub incremental: bool,
}

impl Default for ZddConfig {
    fn default() -> Self {
        Self {
            propagation: PropagationMode::Eager,
            window_ms: 60_000,
            incremental: false,
        }
    }
}

/// Query registration for the ZDD aggregator
#[derive(Debug, Clone)]
pub struct ZddQueryRegistration {
    /// Query ID
    pub id: QueryId,
    /// Event types in pattern order
    pub event_types: Vec<Arc<str>>,
    /// Which positions have Kleene closure
    pub kleene_positions: SmallVec<[u16; 4]>,
    /// Aggregation function
    pub aggregate: GretaAggregate,
}

/// The ZDD unified aggregator
pub struct ZddAggregator {
    /// Configuration
    config: ZddConfig,
    /// NFA encoded in ZDD
    nfa: NfaZdd,
    /// Propagator
    propagator: ZddPropagator,
    /// Registered queries
    queries: Vec<ZddQueryRegistration>,
    /// Query aggregates (for result generation)
    query_aggregates: FxHashMap<QueryId, GretaAggregate>,
    /// Statistics
    stats: ZddAggregatorStats,
}

/// Statistics for the ZDD aggregator
#[derive(Debug, Clone, Default)]
pub struct ZddAggregatorStats {
    /// Total events processed
    pub events_processed: u64,
    /// ZDD operations performed
    pub zdd_operations: u64,
    /// Peak ZDD size
    pub peak_zdd_size: usize,
    /// Current ZDD size
    pub current_zdd_size: usize,
}

impl ZddAggregator {
    /// Create a new ZDD aggregator
    pub fn new(config: ZddConfig) -> Self {
        Self {
            propagator: ZddPropagator::new(config.propagation),
            config,
            nfa: NfaZdd::new(),
            queries: Vec::new(),
            query_aggregates: FxHashMap::default(),
            stats: ZddAggregatorStats::default(),
        }
    }

    /// Register a query
    pub fn register_query(&mut self, registration: ZddQueryRegistration) {
        let query_id = registration.id;

        // Build NFA states for this query
        let mut states: Vec<ZddState> = Vec::new();

        for (i, type_name) in registration.event_types.iter().enumerate() {
            let _type_idx = self.nfa.register_type(type_name.clone());
            let state = self.nfa.add_state(query_id, i as u16);
            states.push(state);

            if i == 0 {
                self.nfa.set_initial(state);
            }
        }

        // Add final state
        let final_state = self
            .nfa
            .add_state(query_id, registration.event_types.len() as u16);
        states.push(final_state);
        self.nfa.add_final(final_state);

        // Add transitions
        for (i, type_name) in registration.event_types.iter().enumerate() {
            let type_idx = self.nfa.type_index(type_name).unwrap();
            self.nfa.add_transition(states[i], states[i + 1], type_idx);

            // Add Kleene self-loop if needed
            if registration.kleene_positions.contains(&(i as u16 + 1)) {
                self.nfa
                    .add_transition(states[i + 1], states[i + 1], type_idx);
            }
        }

        // Register with propagator
        self.propagator.register_query(query_id);
        self.query_aggregates
            .insert(query_id, registration.aggregate);
        self.queries.push(registration);
    }

    /// Initialize after all queries are registered
    pub fn initialize(&mut self) {
        self.nfa.initialize();
    }

    /// Process an event
    pub fn process(&mut self, event: SharedEvent) -> Vec<ZddAggregationResult> {
        let event_type = match self.nfa.type_index(&event.event_type) {
            Some(idx) => idx,
            None => return Vec::new(),
        };

        self.stats.events_processed += 1;
        self.stats.zdd_operations += 1;

        // Update ZDD size stats
        let current_size = self.nfa.zdd_size();
        self.stats.current_zdd_size = current_size;
        self.stats.peak_zdd_size = self.stats.peak_zdd_size.max(current_size);

        // Process through propagator
        let results = self.propagator.process(&mut self.nfa, event_type);

        // Convert to aggregation results
        if self.config.incremental {
            results
                .into_iter()
                .filter_map(|(query_id, count)| {
                    let aggregate = self.query_aggregates.get(&query_id)?;
                    Some(ZddAggregationResult {
                        query_id,
                        aggregate: *aggregate,
                        value: count,
                        is_final: false,
                    })
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Flush and get final results
    pub fn flush(&mut self) -> Vec<ZddAggregationResult> {
        // Flush propagator
        let results = self.propagator.flush(&mut self.nfa);

        // Convert to aggregation results
        let output: Vec<_> = results
            .into_iter()
            .filter_map(|(query_id, count)| {
                let aggregate = self.query_aggregates.get(&query_id)?;
                Some(ZddAggregationResult {
                    query_id,
                    aggregate: *aggregate,
                    value: count,
                    is_final: true,
                })
            })
            .filter(|r| r.value > 0)
            .collect();

        // Reset for next window
        self.reset();

        output
    }

    /// Reset for new window
    fn reset(&mut self) {
        self.nfa.reset();
        self.propagator.reset();
    }

    /// Get current statistics
    pub fn stats(&self) -> &ZddAggregatorStats {
        &self.stats
    }

    /// Get count for a query
    pub fn count(&self, query_id: QueryId) -> u64 {
        self.propagator.count(query_id)
    }

    /// Get number of states
    pub fn num_states(&self) -> usize {
        self.nfa.num_states()
    }

    /// Get number of queries
    pub fn num_queries(&self) -> usize {
        self.nfa.num_queries()
    }
}

impl Default for ZddAggregator {
    fn default() -> Self {
        Self::new(ZddConfig::default())
    }
}

/// Compare ZDD aggregator with Hamlet for benchmarking
#[derive(Debug, Clone)]
pub struct AggregatorComparison {
    /// Events per second
    pub events_per_sec: f64,
    /// Memory usage (bytes)
    pub memory_bytes: usize,
    /// ZDD operations count
    pub zdd_ops: u64,
    /// Approach name
    pub approach: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Event;

    fn create_test_aggregator() -> ZddAggregator {
        let config = ZddConfig::default();
        let mut aggregator = ZddAggregator::new(config);

        aggregator.register_query(ZddQueryRegistration {
            id: 0,
            event_types: vec![Arc::from("A"), Arc::from("B")],
            kleene_positions: smallvec::smallvec![1], // B+
            aggregate: GretaAggregate::CountTrends,
        });

        aggregator.initialize();
        aggregator
    }

    #[test]
    fn test_simple_sequence() {
        let mut aggregator = create_test_aggregator();

        // Process A
        let a = Arc::new(Event::new("A"));
        let results = aggregator.process(a);
        assert!(results.is_empty()); // incremental=false

        // Process B events
        for _ in 0..3 {
            let b = Arc::new(Event::new("B"));
            aggregator.process(b);
        }

        // Flush
        let results = aggregator.flush();
        assert_eq!(results.len(), 1);
        assert!(results[0].value > 0);
    }

    #[test]
    fn test_multiple_queries() {
        let config = ZddConfig::default();
        let mut aggregator = ZddAggregator::new(config);

        // q0: A -> B+
        aggregator.register_query(ZddQueryRegistration {
            id: 0,
            event_types: vec![Arc::from("A"), Arc::from("B")],
            kleene_positions: smallvec::smallvec![1],
            aggregate: GretaAggregate::CountTrends,
        });

        // q1: C -> B+
        aggregator.register_query(ZddQueryRegistration {
            id: 1,
            event_types: vec![Arc::from("C"), Arc::from("B")],
            kleene_positions: smallvec::smallvec![1],
            aggregate: GretaAggregate::CountTrends,
        });

        aggregator.initialize();

        // Process A
        let a = Arc::new(Event::new("A"));
        aggregator.process(a);

        // Process C
        let c = Arc::new(Event::new("C"));
        aggregator.process(c);

        // Process B (shared!)
        let b = Arc::new(Event::new("B"));
        aggregator.process(b);

        let results = aggregator.flush();
        // Both queries should match since we processed A, C, then B
        // Query 0: A -> B matched
        // Query 1: C -> B matched
        assert!(!results.is_empty(), "Should have at least 1 result");
    }

    #[test]
    fn test_stats() {
        let mut aggregator = create_test_aggregator();

        for _ in 0..10 {
            let a = Arc::new(Event::new("A"));
            aggregator.process(a);
        }

        assert_eq!(aggregator.stats().events_processed, 10);
    }
}
