//! # GRETA - Graph-based Real-time Event Trend Aggregation
//!
//! Foundation for online trend aggregation without explicit trend construction.
//!
//! ## References
//!
//! This implementation is based on:
//!
//! > **Olga Poppe, Chuan Lei, Elke A. Rundensteiner, and David Maier.**
//! > *GRETA: Graph-based Real-time Event Trend Aggregation.*
//! > Proceedings of the VLDB Endowment, Vol. 11, No. 1, pp. 80-92, 2017.
//! > DOI: [10.14778/3151113.3151120](https://doi.org/10.14778/3151113.3151120)
//!
//! ## Overview
//!
//! GRETA solves the problem of computing aggregations over event trends (sequences
//! matching a pattern) without explicitly constructing all matching trends, which
//! can be exponential in the number of events.
//!
//! The key insight is that aggregations like COUNT, SUM, AVG can be computed
//! incrementally by propagating partial results through an event graph, where:
//! - Nodes represent events
//! - Edges represent adjacency relations (event e' can precede event e in a trend)
//!
//! ## Key Concepts
//!
//! - **Event Graph**: Events as nodes, adjacency relations as edges
//! - **Incremental Count Propagation**: `count(e) = start(e) + Σ count(e')` for predecessors e'
//! - **Online Aggregation**: Results computed incrementally without constructing all trends
//!
//! ## Complexity
//!
//! - **Time**: O(n²) per query (vs O(2^n) for explicit trend construction)
//! - **Space**: O(n) per query
//!
//! ## Usage in Varpulis
//!
//! This module provides the baseline non-shared aggregation that both the
//! [`hamlet`](crate::hamlet) and [`zdd_unified`](crate::zdd_unified) approaches
//! build upon for multi-query optimization.

use crate::event::SharedEvent;
use rustc_hash::FxHashMap;
use smallvec::SmallVec;
use std::sync::Arc;
use std::time::Instant;

/// Type alias for query identifiers
pub type QueryId = u32;

/// Type alias for event node identifiers within the graph
pub type NodeId = u32;

/// Type alias for graphlet identifiers
pub type GraphletId = u32;

/// An event node in the GRETA graph
#[derive(Debug, Clone)]
pub struct EventNode {
    /// Unique identifier within the graph
    pub id: NodeId,
    /// The actual event
    pub event: SharedEvent,
    /// Event type index (for fast matching)
    pub type_index: u16,
    /// Timestamp for ordering
    pub timestamp: Instant,
    /// Predecessor edges (events that can precede this in a trend)
    pub predecessors: SmallVec<[NodeId; 8]>,
    /// Count of trends ending at this node, per query
    /// Uses SmallVec for queries <= 8, heap for more
    pub counts: SmallVec<[u64; 8]>,
    /// Whether this is a start event for each query
    pub is_start: SmallVec<[bool; 8]>,
    /// Whether this is an end event for each query
    pub is_end: SmallVec<[bool; 8]>,
}

impl EventNode {
    /// Create a new event node
    pub fn new(id: NodeId, event: SharedEvent, type_index: u16, num_queries: usize) -> Self {
        Self {
            id,
            event,
            type_index,
            timestamp: Instant::now(),
            predecessors: SmallVec::new(),
            counts: SmallVec::from_elem(0, num_queries),
            is_start: SmallVec::from_elem(false, num_queries),
            is_end: SmallVec::from_elem(false, num_queries),
        }
    }

    /// Get the count for a specific query
    #[inline]
    pub fn count(&self, query: QueryId) -> u64 {
        self.counts.get(query as usize).copied().unwrap_or(0)
    }

    /// Set the count for a specific query
    #[inline]
    pub fn set_count(&mut self, query: QueryId, count: u64) {
        if let Some(c) = self.counts.get_mut(query as usize) {
            *c = count;
        }
    }

    /// Add a predecessor edge
    #[inline]
    pub fn add_predecessor(&mut self, pred_id: NodeId) {
        self.predecessors.push(pred_id);
    }
}

/// The GRETA event graph for a single partition/window
#[derive(Debug)]
pub struct EventGraph {
    /// All nodes in the graph, indexed by NodeId
    nodes: Vec<EventNode>,
    /// Nodes grouped by event type for fast lookup
    nodes_by_type: FxHashMap<u16, Vec<NodeId>>,
    /// Number of queries being tracked
    num_queries: usize,
    /// Final aggregated counts per query
    final_counts: SmallVec<[u64; 8]>,
    /// Node ID counter
    next_node_id: NodeId,
}

impl EventGraph {
    /// Create a new event graph
    pub fn new(num_queries: usize) -> Self {
        Self {
            nodes: Vec::with_capacity(1024),
            nodes_by_type: FxHashMap::default(),
            num_queries,
            final_counts: SmallVec::from_elem(0, num_queries),
            next_node_id: 0,
        }
    }

    /// Add an event to the graph
    pub fn add_event(&mut self, event: SharedEvent, type_index: u16) -> NodeId {
        let id = self.next_node_id;
        self.next_node_id += 1;

        let node = EventNode::new(id, event, type_index, self.num_queries);
        self.nodes.push(node);

        self.nodes_by_type
            .entry(type_index)
            .or_insert_with(Vec::new)
            .push(id);

        id
    }

    /// Get a node by ID
    #[inline]
    pub fn node(&self, id: NodeId) -> Option<&EventNode> {
        self.nodes.get(id as usize)
    }

    /// Get a mutable node by ID
    #[inline]
    pub fn node_mut(&mut self, id: NodeId) -> Option<&mut EventNode> {
        self.nodes.get_mut(id as usize)
    }

    /// Get all nodes of a specific type
    #[inline]
    pub fn nodes_of_type(&self, type_index: u16) -> &[NodeId] {
        self.nodes_by_type
            .get(&type_index)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Propagate counts for a query (GRETA core algorithm)
    ///
    /// For each event e:
    ///   count(e, q) = start(e, q) + Σ count(e', q) for all predecessors e'
    pub fn propagate_counts(&mut self, query: QueryId) {
        // Process nodes in timestamp order (they're already sorted by insertion)
        for i in 0..self.nodes.len() {
            let node = &self.nodes[i];
            let is_start = node.is_start.get(query as usize).copied().unwrap_or(false);
            let predecessors = node.predecessors.clone();

            // Calculate count: start + sum of predecessor counts
            let mut count = if is_start { 1u64 } else { 0u64 };
            for pred_id in predecessors {
                if let Some(pred) = self.nodes.get(pred_id as usize) {
                    count = count.saturating_add(pred.count(query));
                }
            }

            // Update count
            if let Some(node) = self.nodes.get_mut(i) {
                node.set_count(query, count);

                // Update final count if this is an end node
                if node.is_end.get(query as usize).copied().unwrap_or(false) {
                    if let Some(fc) = self.final_counts.get_mut(query as usize) {
                        *fc = fc.saturating_add(count);
                    }
                }
            }
        }
    }

    /// Get the final aggregated count for a query
    #[inline]
    pub fn final_count(&self, query: QueryId) -> u64 {
        self.final_counts.get(query as usize).copied().unwrap_or(0)
    }

    /// Clear the graph for reuse
    pub fn clear(&mut self) {
        self.nodes.clear();
        self.nodes_by_type.clear();
        self.final_counts.fill(0);
        self.next_node_id = 0;
    }

    /// Number of nodes in the graph
    #[inline]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Check if graph is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

/// Aggregation function types supported by GRETA
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GretaAggregate {
    /// COUNT(*) - number of trends
    CountTrends,
    /// COUNT(E) - number of events of type E in all trends
    CountEvents(u16),
    /// SUM(E.attr)
    Sum { type_index: u16, field_index: u16 },
    /// AVG(E.attr)
    Avg { type_index: u16, field_index: u16 },
    /// MIN(E.attr)
    Min { type_index: u16, field_index: u16 },
    /// MAX(E.attr)
    Max { type_index: u16, field_index: u16 },
}

/// Pattern ID type alias
pub type PatternId = u32;

/// Query definition for GRETA
#[derive(Debug, Clone)]
pub struct GretaQuery {
    /// Unique query identifier
    pub id: QueryId,
    /// Pattern to match (references SASE+ pattern)
    pub pattern_id: PatternId,
    /// Event type indices in the pattern (in order)
    pub event_types: SmallVec<[u16; 4]>,
    /// Which types have Kleene closure
    pub kleene_types: SmallVec<[u16; 4]>,
    /// Aggregation function
    pub aggregate: GretaAggregate,
    /// Window size in milliseconds
    pub window_ms: u64,
    /// Slide size in milliseconds (for sliding windows)
    pub slide_ms: u64,
}

impl GretaQuery {
    /// Check if this query has Kleene patterns
    #[inline]
    pub fn has_kleene(&self) -> bool {
        !self.kleene_types.is_empty()
    }

    /// Check if an event type is a start type for this query
    #[inline]
    pub fn is_start_type(&self, type_index: u16) -> bool {
        self.event_types.first().copied() == Some(type_index)
    }

    /// Check if an event type is an end type for this query
    #[inline]
    pub fn is_end_type(&self, type_index: u16) -> bool {
        self.event_types.last().copied() == Some(type_index)
    }
}

/// GRETA executor for non-shared online trend aggregation
pub struct GretaExecutor {
    /// Registered queries
    queries: Vec<GretaQuery>,
    /// Event type name to index mapping
    type_indices: FxHashMap<Arc<str>, u16>,
    /// Current event graph
    graph: EventGraph,
}

impl GretaExecutor {
    /// Create a new GRETA executor
    pub fn new() -> Self {
        Self {
            queries: Vec::new(),
            type_indices: FxHashMap::default(),
            graph: EventGraph::new(0),
        }
    }

    /// Register a query
    pub fn register_query(&mut self, query: GretaQuery) {
        self.queries.push(query);
        // Recreate graph with new query count
        self.graph = EventGraph::new(self.queries.len());
    }

    /// Register an event type and get its index
    pub fn register_type(&mut self, name: Arc<str>) -> u16 {
        let len = self.type_indices.len() as u16;
        *self.type_indices.entry(name).or_insert(len)
    }

    /// Get the type index for an event type name
    #[inline]
    pub fn type_index(&self, name: &str) -> Option<u16> {
        self.type_indices.get(name).copied()
    }

    /// Process an event
    pub fn process(&mut self, event: SharedEvent) -> Vec<(QueryId, u64)> {
        let type_index = match self.type_index(&event.event_type) {
            Some(idx) => idx,
            None => return Vec::new(),
        };

        // Add event to graph
        let node_id = self.graph.add_event(event, type_index);

        // Mark start/end status for each query
        for query in &self.queries {
            if let Some(node) = self.graph.node_mut(node_id) {
                if query.is_start_type(type_index) {
                    if let Some(is_start) = node.is_start.get_mut(query.id as usize) {
                        *is_start = true;
                    }
                }
                if query.is_end_type(type_index) {
                    if let Some(is_end) = node.is_end.get_mut(query.id as usize) {
                        *is_end = true;
                    }
                }
            }
        }

        // Add predecessor edges based on pattern structure
        self.add_predecessor_edges(node_id, type_index);

        // Propagate counts for all queries
        let mut results = Vec::new();
        for query in &self.queries {
            self.graph.propagate_counts(query.id);
            let count = self.graph.final_count(query.id);
            if count > 0 {
                results.push((query.id, count));
            }
        }

        results
    }

    /// Add predecessor edges for a new node
    fn add_predecessor_edges(&mut self, node_id: NodeId, type_index: u16) {
        // Collect all predecessor IDs first to avoid borrow issues
        let mut all_pred_ids: SmallVec<[NodeId; 16]> = SmallVec::new();

        // For each query, find valid predecessors based on pattern structure
        for query in &self.queries {
            // Find position of this type in the pattern
            let pos = query
                .event_types
                .iter()
                .position(|&t| t == type_index);

            if let Some(pos) = pos {
                // Predecessors are events of the previous type in the pattern
                // For Kleene, same type can also be a predecessor
                let mut pred_types: SmallVec<[u16; 4]> = SmallVec::new();

                if pos > 0 {
                    pred_types.push(query.event_types[pos - 1]);
                }

                // If this type has Kleene, add self-loop
                if query.kleene_types.contains(&type_index) {
                    pred_types.push(type_index);
                }

                // Collect predecessor IDs
                for pred_type in pred_types {
                    for &pred_id in self.graph.nodes_of_type(pred_type) {
                        if pred_id < node_id && !all_pred_ids.contains(&pred_id) {
                            all_pred_ids.push(pred_id);
                        }
                    }
                }
            }
        }

        // Now add all predecessors
        if let Some(node) = self.graph.node_mut(node_id) {
            for pred_id in all_pred_ids {
                node.add_predecessor(pred_id);
            }
        }
    }

    /// Flush window and get final results
    pub fn flush(&mut self) -> Vec<(QueryId, u64)> {
        let results: Vec<_> = self
            .queries
            .iter()
            .map(|q| (q.id, self.graph.final_count(q.id)))
            .filter(|(_, count)| *count > 0)
            .collect();

        self.graph.clear();
        results
    }
}

impl Default for GretaExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Event;

    #[test]
    fn test_event_graph_basic() {
        let mut graph = EventGraph::new(2);
        assert!(graph.is_empty());

        let event = Arc::new(Event::new("A"));
        let id = graph.add_event(event, 0);
        assert_eq!(id, 0);
        assert_eq!(graph.len(), 1);
    }

    #[test]
    fn test_greta_query() {
        let query = GretaQuery {
            id: 0,
            pattern_id: 0,
            event_types: smallvec::smallvec![0, 1],
            kleene_types: smallvec::smallvec![1],
            aggregate: GretaAggregate::CountTrends,
            window_ms: 60000,
            slide_ms: 60000,
        };

        assert!(query.has_kleene());
        assert!(query.is_start_type(0));
        assert!(query.is_end_type(1));
    }
}
