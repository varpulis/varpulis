//! # Graphlet - Atomic Unit of Sharing in Hamlet
//!
//! A graphlet is a sub-graph containing events of the same type during
//! an interval where no events of other types in the pattern are matched.
//!
//! Graphlets are the fundamental unit of sharing in Hamlet - when a Kleene
//! sub-pattern (e.g., B+) is shareable across queries, all queries share
//! the same graphlet structure but with different snapshot values.

use crate::event::SharedEvent;
use crate::greta::{NodeId, QueryId};
use smallvec::SmallVec;
use std::time::Instant;

/// Unique identifier for a graphlet
pub type GraphletId = u32;

/// Status of a graphlet
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphletStatus {
    /// Graphlet is still receiving events
    Active,
    /// Graphlet is complete (no more events will be added)
    Inactive,
    /// Graphlet has been processed and can be cleaned up
    Processed,
}

/// A graphlet node (event within a graphlet)
#[derive(Debug, Clone)]
pub struct GraphletNode {
    /// Global node ID
    pub node_id: NodeId,
    /// Local index within graphlet
    pub local_index: u16,
    /// The event
    pub event: SharedEvent,
    /// Predecessors within the same graphlet (local indices)
    pub local_predecessors: SmallVec<[u16; 8]>,
    /// Count coefficient for snapshot propagation
    /// count(e) = coeff * snapshot_value + local_sum
    pub snapshot_coeff: u64,
    /// Local count contribution (from predecessors within graphlet)
    pub local_sum: u64,
}

impl GraphletNode {
    /// Create a new graphlet node
    pub fn new(node_id: NodeId, local_index: u16, event: SharedEvent) -> Self {
        Self {
            node_id,
            local_index,
            event,
            local_predecessors: SmallVec::new(),
            snapshot_coeff: 0,
            local_sum: 0,
        }
    }

    /// Compute the actual count given a snapshot value
    #[inline]
    pub fn count(&self, snapshot_value: u64) -> u64 {
        self.snapshot_coeff
            .saturating_mul(snapshot_value)
            .saturating_add(self.local_sum)
    }
}

/// A graphlet containing events of a single type
#[derive(Debug)]
pub struct Graphlet {
    /// Unique identifier
    pub id: GraphletId,
    /// Event type index
    pub type_index: u16,
    /// Status
    pub status: GraphletStatus,
    /// Events in the graphlet
    pub nodes: Vec<GraphletNode>,
    /// Queries sharing this graphlet's Kleene sub-pattern
    pub sharing_queries: SmallVec<[QueryId; 8]>,
    /// Whether this graphlet is being processed in shared mode
    pub is_shared: bool,
    /// Start time of the graphlet
    pub start_time: Instant,
    /// End time (set when graphlet becomes inactive)
    pub end_time: Option<Instant>,
    /// Predecessor graphlet IDs (graphlets that can precede this one)
    pub predecessor_graphlets: SmallVec<[GraphletId; 4]>,
    /// Successor graphlet IDs (graphlets that can follow this one)
    pub successor_graphlets: SmallVec<[GraphletId; 4]>,
}

impl Graphlet {
    /// Create a new graphlet
    pub fn new(id: GraphletId, type_index: u16) -> Self {
        Self {
            id,
            type_index,
            status: GraphletStatus::Active,
            nodes: Vec::with_capacity(64),
            sharing_queries: SmallVec::new(),
            is_shared: false,
            start_time: Instant::now(),
            end_time: None,
            predecessor_graphlets: SmallVec::new(),
            successor_graphlets: SmallVec::new(),
        }
    }

    /// Add an event to the graphlet
    pub fn add_event(&mut self, node_id: NodeId, event: SharedEvent) -> u16 {
        let local_index = self.nodes.len() as u16;
        self.nodes.push(GraphletNode::new(node_id, local_index, event));
        local_index
    }

    /// Mark graphlet as inactive
    pub fn deactivate(&mut self) {
        self.status = GraphletStatus::Inactive;
        self.end_time = Some(Instant::now());
    }

    /// Mark graphlet as processed
    pub fn mark_processed(&mut self) {
        self.status = GraphletStatus::Processed;
    }

    /// Get node by local index
    #[inline]
    pub fn node(&self, local_index: u16) -> Option<&GraphletNode> {
        self.nodes.get(local_index as usize)
    }

    /// Get mutable node by local index
    #[inline]
    pub fn node_mut(&mut self, local_index: u16) -> Option<&mut GraphletNode> {
        self.nodes.get_mut(local_index as usize)
    }

    /// Number of events in the graphlet
    #[inline]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Check if graphlet is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Check if this graphlet is shareable (has multiple queries)
    #[inline]
    pub fn is_shareable(&self) -> bool {
        self.sharing_queries.len() > 1
    }

    /// Propagate snapshot through the graphlet (shared mode)
    ///
    /// This computes snapshot_coeff and local_sum for each node,
    /// allowing counts to be resolved for any query by substituting
    /// the query-specific snapshot value.
    ///
    /// For a Kleene graphlet with self-loops:
    /// - First event: coeff = 1, local_sum = 0
    /// - Subsequent events: coeff = prev_coeff, local_sum = prev_sum + prev_count
    pub fn propagate_shared(&mut self) {
        if self.nodes.is_empty() {
            return;
        }

        // First node: coefficient is 1 (directly uses snapshot)
        if let Some(first) = self.nodes.first_mut() {
            first.snapshot_coeff = 1;
            first.local_sum = 0;
        }

        // Propagate through remaining nodes
        for i in 1..self.nodes.len() {
            // Sum up contributions from predecessors
            let predecessors = self.nodes[i].local_predecessors.clone();
            let mut total_coeff = 0u64;
            let mut total_local = 0u64;

            for &pred_idx in &predecessors {
                if let Some(pred) = self.nodes.get(pred_idx as usize) {
                    total_coeff = total_coeff.saturating_add(pred.snapshot_coeff);
                    total_local = total_local.saturating_add(pred.local_sum);
                }
            }

            if let Some(node) = self.nodes.get_mut(i) {
                node.snapshot_coeff = total_coeff;
                node.local_sum = total_local;
            }
        }
    }

    /// Compute the sum of counts at the end of this graphlet for a snapshot value
    pub fn sum_counts(&self, snapshot_value: u64) -> u64 {
        self.nodes
            .iter()
            .map(|n| n.count(snapshot_value))
            .fold(0u64, |acc, c| acc.saturating_add(c))
    }

    /// Add local predecessor edges (for Kleene self-loops)
    pub fn add_kleene_edges(&mut self) {
        // In a Kleene graphlet, each event can be preceded by all earlier events
        for i in 1..self.nodes.len() {
            let predecessors: SmallVec<[u16; 8]> = (0..i as u16).collect();
            if let Some(node) = self.nodes.get_mut(i) {
                node.local_predecessors = predecessors;
            }
        }
    }
}

/// Pool for reusing graphlets to avoid allocations
#[derive(Debug, Default)]
pub struct GraphletPool {
    /// Available graphlets for reuse
    pool: Vec<Graphlet>,
    /// Next graphlet ID to assign
    next_id: GraphletId,
}

impl GraphletPool {
    /// Create a new graphlet pool
    pub fn new() -> Self {
        Self {
            pool: Vec::with_capacity(32),
            next_id: 0,
        }
    }

    /// Get or create a graphlet
    pub fn acquire(&mut self, type_index: u16) -> Graphlet {
        if let Some(mut graphlet) = self.pool.pop() {
            // Reuse existing graphlet
            graphlet.id = self.next_id;
            graphlet.type_index = type_index;
            graphlet.status = GraphletStatus::Active;
            graphlet.nodes.clear();
            graphlet.sharing_queries.clear();
            graphlet.is_shared = false;
            graphlet.start_time = Instant::now();
            graphlet.end_time = None;
            graphlet.predecessor_graphlets.clear();
            graphlet.successor_graphlets.clear();
            self.next_id += 1;
            graphlet
        } else {
            // Create new graphlet
            let id = self.next_id;
            self.next_id += 1;
            Graphlet::new(id, type_index)
        }
    }

    /// Return a graphlet to the pool
    pub fn release(&mut self, graphlet: Graphlet) {
        if self.pool.len() < 64 {
            self.pool.push(graphlet);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Event;
    use std::sync::Arc;

    #[test]
    fn test_graphlet_basic() {
        let mut graphlet = Graphlet::new(0, 0);
        assert!(graphlet.is_empty());
        assert_eq!(graphlet.status, GraphletStatus::Active);

        let event = Arc::new(Event::new("B"));
        graphlet.add_event(0, event);
        assert_eq!(graphlet.len(), 1);
    }

    #[test]
    fn test_graphlet_kleene_propagation() {
        let mut graphlet = Graphlet::new(0, 0);

        // Add 3 events
        for i in 0..3 {
            let event = Arc::new(Event::new("B"));
            graphlet.add_event(i, event);
        }

        // Add Kleene edges (each event preceded by all earlier)
        graphlet.add_kleene_edges();

        // Propagate with snapshot value
        graphlet.propagate_shared();

        // With snapshot value 1:
        // e0: count = 1 * 1 + 0 = 1
        // e1: count = 1 * 1 + 0 = 1 (pred is e0)
        // e2: count = 2 * 1 + 0 = 2 (preds are e0, e1)
        assert_eq!(graphlet.nodes[0].count(1), 1);
        assert_eq!(graphlet.nodes[1].count(1), 1);
        assert_eq!(graphlet.nodes[2].count(1), 2);
    }

    #[test]
    fn test_graphlet_pool() {
        let mut pool = GraphletPool::new();

        let g1 = pool.acquire(0);
        assert_eq!(g1.id, 0);

        let g2 = pool.acquire(1);
        assert_eq!(g2.id, 1);

        pool.release(g1);

        let g3 = pool.acquire(2);
        assert_eq!(g3.id, 2); // New ID even though reused
        assert_eq!(g3.type_index, 2);
    }
}
