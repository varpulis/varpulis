//! # Hamlet Aggregator - Shared Online Trend Aggregation
//!
//! Main executor combining:
//! - Hamlet graph for event storage
//! - Merged template for pattern matching
//! - Snapshot mechanism for shared propagation
//! - Dynamic optimizer for sharing decisions
//!
//! This implements Approach 2b: Hamlet for structural sharing + ZDD for run management.
//!
//! ## Algorithm Overview
//!
//! 1. Events arrive and are grouped into graphlets by type
//! 2. When a graphlet closes (different type arrives), it's processed:
//!    - **Shared mode**: Compute propagation coefficients once, resolve per-query with snapshots
//!    - **Non-shared mode**: Propagate counts independently per query
//! 3. Snapshots capture intermediate state at graphlet boundaries
//! 4. Final counts are computed at window boundaries

use super::graph::HamletGraph;
use super::graphlet::GraphletId;
use super::optimizer::{HamletOptimizer, OptimizerConfig, SharingDecision};
use super::snapshot::{PropagationCoefficients, Snapshot};
use super::template::MergedTemplate;
use crate::event::SharedEvent;
use crate::greta::{GretaAggregate, QueryId};
use rustc_hash::FxHashMap;
use smallvec::SmallVec;

/// Result of processing an event
#[derive(Debug, Clone)]
pub struct AggregationResult {
    /// Query ID
    pub query_id: QueryId,
    /// Aggregation type
    pub aggregate: GretaAggregate,
    /// Current value
    pub value: u64,
    /// Whether this is a final result (window closed)
    pub is_final: bool,
}

/// Configuration for the Hamlet aggregator
#[derive(Debug, Clone)]
pub struct HamletConfig {
    /// Optimizer configuration
    pub optimizer: OptimizerConfig,
    /// Window size in milliseconds
    pub window_ms: u64,
    /// Enable incremental result emission
    pub incremental: bool,
}

impl Default for HamletConfig {
    fn default() -> Self {
        Self {
            optimizer: OptimizerConfig::default(),
            window_ms: 60_000,
            incremental: false,
        }
    }
}

/// Query registration for the aggregator
#[derive(Debug, Clone)]
pub struct QueryRegistration {
    /// Query ID
    pub id: QueryId,
    /// Event types in pattern order
    pub event_types: SmallVec<[u16; 4]>,
    /// Which types have Kleene closure
    pub kleene_types: SmallVec<[u16; 4]>,
    /// Aggregation function
    pub aggregate: GretaAggregate,
}

/// Per-query state tracking
#[derive(Debug, Clone, Default)]
struct QueryState {
    /// Current state in the template FSA
    current_state: u16,
    /// Running count of trends
    count: u64,
    /// Whether we're in an active trend (seen start event)
    in_trend: bool,
    /// Snapshot value from predecessor graphlets
    snapshot_value: u64,
}

/// The Hamlet aggregator
pub struct HamletAggregator {
    /// Configuration
    config: HamletConfig,
    /// Merged query template
    template: MergedTemplate,
    /// Hamlet graph
    graph: HamletGraph,
    /// Optimizer
    optimizer: HamletOptimizer,
    /// Registered queries
    queries: Vec<QueryRegistration>,
    /// State per query
    query_states: FxHashMap<QueryId, QueryState>,
    /// Last seen event type (for graphlet management)
    last_event_type: Option<u16>,
    /// Pending results
    pending_results: Vec<AggregationResult>,
    /// Precomputed propagation coefficients per graphlet (cached)
    coefficients_cache: FxHashMap<GraphletId, PropagationCoefficients>,
    /// Final trend counts per query (accumulated across graphlets)
    final_counts: FxHashMap<QueryId, u64>,
}

impl HamletAggregator {
    /// Create a new Hamlet aggregator
    pub fn new(config: HamletConfig, template: MergedTemplate) -> Self {
        Self {
            optimizer: HamletOptimizer::new(config.optimizer.clone()),
            config,
            template,
            graph: HamletGraph::new(),
            queries: Vec::new(),
            query_states: FxHashMap::default(),
            last_event_type: None,
            pending_results: Vec::new(),
            coefficients_cache: FxHashMap::default(),
            final_counts: FxHashMap::default(),
        }
    }

    /// Register a query
    pub fn register_query(&mut self, registration: QueryRegistration) {
        let id = registration.id;

        // Initialize query state
        let initial_state = self.template.initial(id).unwrap_or(0);
        self.query_states.insert(
            id,
            QueryState {
                current_state: initial_state,
                count: 0,
                in_trend: false,
                snapshot_value: 1, // Initial snapshot value is 1 (one way to reach start)
            },
        );
        self.final_counts.insert(id, 0);

        // Register Kleene patterns with optimizer
        for &kleene_type in &registration.kleene_types {
            // Collect all queries sharing this Kleene type
            let sharing: Vec<_> = self
                .queries
                .iter()
                .filter(|q| q.kleene_types.contains(&kleene_type))
                .map(|q| q.id)
                .chain(std::iter::once(id))
                .collect();

            self.optimizer.register_kleene(kleene_type, &sharing);
        }

        self.queries.push(registration);
    }

    /// Process an event
    pub fn process(&mut self, event: SharedEvent) -> Vec<AggregationResult> {
        let event_type_name = &event.event_type;
        let type_index = match self.template.type_index(event_type_name) {
            Some(idx) => idx,
            None => return Vec::new(), // Unknown event type
        };

        // Check if we need to close the previous graphlet
        if let Some(last_type) = self.last_event_type {
            if last_type != type_index {
                // Different type arrived - close the previous graphlet
                if let Some(graphlet_id) = self.graph.close_graphlet_for_type(last_type) {
                    self.process_closed_graphlet(graphlet_id);
                }
            }
        }
        self.last_event_type = Some(type_index);

        // Add event to graph
        let (_node_id, graphlet_id, _local_index) =
            self.graph.add_event(event.clone(), type_index);

        // Set up sharing for this graphlet if needed
        self.setup_graphlet_sharing(graphlet_id, type_index);

        // Update query states based on transitions
        let mut results = Vec::new();
        let query_ids: Vec<_> = self.queries.iter().map(|q| q.id).collect();

        for query_id in query_ids {
            if let Some(result) = self.update_query_state(query_id, type_index) {
                results.push(result);
            }
        }

        // Add any pending results
        results.append(&mut self.pending_results);

        results
    }

    /// Set up sharing for a graphlet based on optimizer decision
    fn setup_graphlet_sharing(&mut self, graphlet_id: GraphletId, type_index: u16) {
        let decision = self.optimizer.decision(type_index);

        if let Some(queries) = self.template.queries_sharing_kleene(type_index) {
            self.graph.set_sharing_queries(graphlet_id, queries.clone());
        }

        if let Some(graphlet) = self.graph.graphlet_mut(graphlet_id) {
            graphlet.is_shared =
                matches!(decision, SharingDecision::Shared | SharingDecision::Merge);
        }
    }

    /// Update a query's state based on the current event type
    fn update_query_state(
        &mut self,
        query_id: QueryId,
        type_index: u16,
    ) -> Option<AggregationResult> {
        let state = self.query_states.get_mut(&query_id)?;
        let current_state = state.current_state;

        // Check if there's a transition for this event type
        let transition = self.template.transition(current_state, type_index)?;

        // Verify query is part of this transition
        if !transition.queries.contains(&query_id) {
            return None;
        }

        // Update state
        state.current_state = transition.to;

        // Check if this is the start of a trend
        if transition.from == self.template.initial(query_id).unwrap_or(0) {
            state.in_trend = true;
            state.snapshot_value = 1;
        }

        // For Kleene transitions, track the multiplication factor
        if transition.is_kleene && state.in_trend {
            // Each Kleene event doubles the number of possible trends
            // (can include or exclude this event)
            state.count = state.count.saturating_add(state.snapshot_value);
        }

        // Check if we reached a final state
        if self.template.is_final(query_id, transition.to) {
            if state.in_trend {
                // Complete a trend
                let final_count = self.final_counts.entry(query_id).or_insert(0);
                *final_count = final_count.saturating_add(state.count.max(1));
            }

            if self.config.incremental {
                let query = self.queries.iter().find(|q| q.id == query_id)?;
                return Some(AggregationResult {
                    query_id,
                    aggregate: query.aggregate,
                    value: *self.final_counts.get(&query_id).unwrap_or(&0),
                    is_final: false,
                });
            }
        }

        None
    }

    /// Process a closed graphlet
    fn process_closed_graphlet(&mut self, graphlet_id: GraphletId) {
        let (is_shared, type_index, graphlet_size, sharing_queries) = {
            let graphlet = match self.graph.graphlet(graphlet_id) {
                Some(g) => g,
                None => return,
            };
            (
                graphlet.is_shared,
                graphlet.type_index,
                graphlet.len(),
                graphlet.sharing_queries.clone(),
            )
        };

        if graphlet_size == 0 {
            return;
        }

        if is_shared && sharing_queries.len() > 1 {
            self.process_shared_graphlet(graphlet_id, &sharing_queries);
        } else {
            self.process_non_shared_graphlet(graphlet_id, &sharing_queries);
        }

        // Report to optimizer
        let num_snapshots = self
            .graph
            .snapshots()
            .snapshots_for_target(graphlet_id)
            .count();
        self.optimizer
            .report_graphlet(type_index, graphlet_size, num_snapshots);

        // Mark as processed
        self.graph.mark_processed(graphlet_id);
    }

    /// Process a graphlet in shared mode
    ///
    /// Uses snapshot propagation: compute coefficients once, resolve per-query
    fn process_shared_graphlet(&mut self, graphlet_id: GraphletId, queries: &[QueryId]) {
        let graphlet_size = match self.graph.graphlet(graphlet_id) {
            Some(g) => g.len(),
            None => return,
        };

        // Compute propagation coefficients (shared across all queries)
        let mut coeffs = PropagationCoefficients::new(graphlet_size);
        coeffs.compute_kleene();

        // Create snapshot with each query's incoming value
        let mut snapshot = Snapshot::new(0, graphlet_id, graphlet_id);
        for &query_id in queries {
            if let Some(state) = self.query_states.get(&query_id) {
                snapshot.set_value(query_id, state.snapshot_value);
            }
        }

        // Resolve counts for each query
        let results = coeffs.resolve_final_counts(&[graphlet_size - 1], &snapshot);

        // Update query states with resolved counts
        for result in results {
            if let Some(state) = self.query_states.get_mut(&result.query_id) {
                state.count = state.count.saturating_add(result.count);
                // Update snapshot value for next graphlet
                state.snapshot_value = coeffs.resolve_count(state.snapshot_value);
            }
        }

        // Cache coefficients for potential reuse
        self.coefficients_cache.insert(graphlet_id, coeffs);

        // Add Kleene edges to graphlet
        if let Some(graphlet) = self.graph.graphlet_mut(graphlet_id) {
            graphlet.add_kleene_edges();
            graphlet.propagate_shared();
        }
    }

    /// Process a graphlet in non-shared mode (per-query)
    fn process_non_shared_graphlet(&mut self, graphlet_id: GraphletId, queries: &[QueryId]) {
        let graphlet_size = match self.graph.graphlet(graphlet_id) {
            Some(g) => g.len(),
            None => return,
        };

        // Process independently for each query
        for &query_id in queries {
            if let Some(state) = self.query_states.get_mut(&query_id) {
                if state.in_trend {
                    // For non-shared, compute Kleene count directly
                    // With n events in a Kleene pattern, there are 2^n - 1 non-empty combinations
                    // But with count propagation, we sum up all paths
                    let kleene_count = compute_kleene_count(graphlet_size, state.snapshot_value);
                    state.count = state.count.saturating_add(kleene_count);
                    state.snapshot_value = kleene_count;
                }
            }
        }
    }

    /// Flush and get final results (e.g., at window boundary)
    pub fn flush(&mut self) -> Vec<AggregationResult> {
        // Close all active graphlets
        let closed = self.graph.close_all_active();
        for graphlet_id in closed {
            self.process_closed_graphlet(graphlet_id);
        }

        // Finalize any in-progress trends
        for (&query_id, state) in &self.query_states {
            if state.in_trend && state.count > 0 {
                let final_count = self.final_counts.entry(query_id).or_insert(0);
                *final_count = final_count.saturating_add(state.count);
            }
        }

        // Generate final results
        let results: Vec<_> = self
            .queries
            .iter()
            .filter_map(|query| {
                let count = self.final_counts.get(&query.id).copied().unwrap_or(0);
                let state_count = self
                    .query_states
                    .get(&query.id)
                    .map(|s| s.count)
                    .unwrap_or(0);
                let total = count.max(state_count);

                if total > 0 {
                    Some(AggregationResult {
                        query_id: query.id,
                        aggregate: query.aggregate,
                        value: total,
                        is_final: true,
                    })
                } else {
                    None
                }
            })
            .collect();

        // Reset state
        self.reset();

        results
    }

    /// Reset for new window
    fn reset(&mut self) {
        self.graph.clear();
        self.last_event_type = None;
        self.optimizer.reset_stats();
        self.coefficients_cache.clear();
        self.final_counts.clear();

        // Reset query states to initial
        for query in &self.queries {
            let initial_state = self.template.initial(query.id).unwrap_or(0);
            self.query_states.insert(
                query.id,
                QueryState {
                    current_state: initial_state,
                    count: 0,
                    in_trend: false,
                    snapshot_value: 1,
                },
            );
            self.final_counts.insert(query.id, 0);
        }
    }

    /// Get current optimizer statistics
    pub fn optimizer_stats(&self) -> impl Iterator<Item = (&u16, &super::optimizer::KleeneStats)> {
        self.optimizer.all_stats()
    }

    /// Get the number of graphlets
    pub fn num_graphlets(&self) -> usize {
        self.graph.num_graphlets()
    }

    /// Get the number of events
    pub fn num_events(&self) -> usize {
        self.graph.num_events()
    }

    /// Get query state (for debugging/testing)
    pub fn query_state(&self, query_id: QueryId) -> Option<(u16, u64, bool)> {
        self.query_states
            .get(&query_id)
            .map(|s| (s.current_state, s.count, s.in_trend))
    }
}

/// Compute the total count for a Kleene graphlet with n events
///
/// For a Kleene pattern E+ with n events and incoming snapshot value s:
/// - Event 0: count = s
/// - Event 1: count = s (predecessor is event 0)
/// - Event 2: count = 2s (predecessors are events 0, 1)
/// - Event n: count = 2^(n-1) * s
///
/// Total = s + s + 2s + 4s + ... + 2^(n-1)s = s * (2^n - 1)
#[inline]
fn compute_kleene_count(num_events: usize, snapshot_value: u64) -> u64 {
    if num_events == 0 {
        return 0;
    }
    // 2^n - 1, but cap at u64::MAX
    let power = 1u64.checked_shl(num_events as u32).unwrap_or(u64::MAX);
    power.saturating_sub(1).saturating_mul(snapshot_value)
}

impl Default for HamletAggregator {
    fn default() -> Self {
        Self::new(HamletConfig::default(), MergedTemplate::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Event;
    use crate::hamlet::template::TemplateBuilder;
    use std::sync::Arc;

    fn create_test_aggregator() -> HamletAggregator {
        let mut builder = TemplateBuilder::new();

        // q0: A -> B+ (sequence with Kleene)
        builder.add_sequence(0, &["A", "B"]);
        builder.add_kleene(0, "B", 1);

        let template = builder.build();
        let config = HamletConfig::default();

        let mut aggregator = HamletAggregator::new(config, template);

        aggregator.register_query(QueryRegistration {
            id: 0,
            event_types: smallvec::smallvec![0, 1],
            kleene_types: smallvec::smallvec![1],
            aggregate: GretaAggregate::CountTrends,
        });

        aggregator
    }

    #[test]
    fn test_simple_sequence() {
        let mut aggregator = create_test_aggregator();

        // Process A event
        let a = Arc::new(Event::new("A"));
        let results = aggregator.process(a);
        assert!(results.is_empty()); // No output yet

        // Process B events (Kleene)
        for _ in 0..3 {
            let b = Arc::new(Event::new("B"));
            aggregator.process(b);
        }

        // Flush to get results
        let results = aggregator.flush();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].query_id, 0);
        assert!(results[0].value > 0);
    }

    #[test]
    fn test_graphlet_management() {
        let mut aggregator = create_test_aggregator();

        // Process A
        let a = Arc::new(Event::new("A"));
        aggregator.process(a);
        assert_eq!(aggregator.num_graphlets(), 1);

        // Process B - should close A graphlet and open B graphlet
        let b = Arc::new(Event::new("B"));
        aggregator.process(b);
        assert_eq!(aggregator.num_graphlets(), 2);

        // Process more B - same graphlet
        let b = Arc::new(Event::new("B"));
        aggregator.process(b);
        assert_eq!(aggregator.num_graphlets(), 2);
    }

    #[test]
    fn test_kleene_count() {
        // 3 events with snapshot value 1: 2^3 - 1 = 7
        assert_eq!(compute_kleene_count(3, 1), 7);

        // 4 events with snapshot value 2: 2 * (2^4 - 1) = 2 * 15 = 30
        assert_eq!(compute_kleene_count(4, 2), 30);

        // 0 events
        assert_eq!(compute_kleene_count(0, 1), 0);
    }

    #[test]
    fn test_multi_query_sharing() {
        let mut builder = TemplateBuilder::new();

        // q0: A -> B+
        builder.add_sequence(0, &["A", "B"]);
        builder.add_kleene(0, "B", 1);

        // q1: C -> B+ (shares B+ with q0)
        builder.add_sequence(1, &["C", "B"]);
        builder.add_kleene(1, "B", 4);

        let template = builder.build();
        let mut aggregator = HamletAggregator::new(HamletConfig::default(), template);

        aggregator.register_query(QueryRegistration {
            id: 0,
            event_types: smallvec::smallvec![0, 1],
            kleene_types: smallvec::smallvec![1],
            aggregate: GretaAggregate::CountTrends,
        });

        aggregator.register_query(QueryRegistration {
            id: 1,
            event_types: smallvec::smallvec![2, 1],
            kleene_types: smallvec::smallvec![1],
            aggregate: GretaAggregate::CountTrends,
        });

        // Process A (starts q0)
        aggregator.process(Arc::new(Event::new("A")));

        // Process C (starts q1)
        aggregator.process(Arc::new(Event::new("C")));

        // Process B (shared by both!)
        aggregator.process(Arc::new(Event::new("B")));
        aggregator.process(Arc::new(Event::new("B")));

        let results = aggregator.flush();
        // Both queries should have results (though values may differ based on when they started)
        assert!(results.len() >= 1);
    }
}
