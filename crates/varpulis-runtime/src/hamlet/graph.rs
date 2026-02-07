//! # Hamlet Graph - Partitioned Event Graph Structure
//!
//! The Hamlet graph extends GRETA's event graph with:
//! - Partitioning into graphlets by event type
//! - Support for shared vs non-shared execution modes
//! - Integration with snapshot propagation

use super::graphlet::{Graphlet, GraphletId, GraphletPool, GraphletStatus};
use super::snapshot::{SnapshotId, SnapshotManager};
use crate::event::SharedEvent;
use crate::greta::{NodeId, QueryId};
use rustc_hash::FxHashMap;
use smallvec::SmallVec;

/// The Hamlet graph structure
#[derive(Debug)]
pub struct HamletGraph {
    /// Active graphlets by type (only one active per type at a time)
    active_graphlets: FxHashMap<u16, GraphletId>,
    /// All graphlets (including inactive ones within window)
    graphlets: FxHashMap<GraphletId, Graphlet>,
    /// Graphlet pool for reuse
    graphlet_pool: GraphletPool,
    /// Snapshot manager
    snapshots: SnapshotManager,
    /// Global node ID counter
    next_node_id: NodeId,
    /// Event type to graphlet mapping for fast lookup
    type_to_graphlets: FxHashMap<u16, SmallVec<[GraphletId; 16]>>,
}

impl HamletGraph {
    /// Create a new Hamlet graph
    pub fn new() -> Self {
        Self {
            active_graphlets: FxHashMap::default(),
            graphlets: FxHashMap::default(),
            graphlet_pool: GraphletPool::new(),
            snapshots: SnapshotManager::new(),
            next_node_id: 0,
            type_to_graphlets: FxHashMap::default(),
        }
    }

    /// Get or create the active graphlet for an event type
    fn get_or_create_active_graphlet(&mut self, type_index: u16) -> GraphletId {
        if let Some(&id) = self.active_graphlets.get(&type_index) {
            return id;
        }

        // Create new graphlet
        let graphlet = self.graphlet_pool.acquire(type_index);
        let id = graphlet.id;
        self.graphlets.insert(id, graphlet);
        self.active_graphlets.insert(type_index, id);

        self.type_to_graphlets
            .entry(type_index)
            .or_default()
            .push(id);

        id
    }

    /// Add an event to the graph
    ///
    /// Returns (global_node_id, graphlet_id, local_index)
    pub fn add_event(&mut self, event: SharedEvent, type_index: u16) -> (NodeId, GraphletId, u16) {
        let node_id = self.next_node_id;
        self.next_node_id += 1;

        let graphlet_id = self.get_or_create_active_graphlet(type_index);
        let local_index = if let Some(graphlet) = self.graphlets.get_mut(&graphlet_id) {
            graphlet.add_event(node_id, event)
        } else {
            0
        };

        (node_id, graphlet_id, local_index)
    }

    /// Signal that a different event type has arrived, potentially closing the current graphlet
    ///
    /// This is called when we see an event of a different type than the current active graphlet.
    /// The active graphlet for the previous type becomes inactive.
    pub fn close_graphlet_for_type(&mut self, type_index: u16) -> Option<GraphletId> {
        if let Some(graphlet_id) = self.active_graphlets.remove(&type_index) {
            if let Some(graphlet) = self.graphlets.get_mut(&graphlet_id) {
                graphlet.deactivate();
            }
            Some(graphlet_id)
        } else {
            None
        }
    }

    /// Close all active graphlets (e.g., at window boundary)
    pub fn close_all_active(&mut self) -> Vec<GraphletId> {
        let ids: Vec<_> = self.active_graphlets.values().copied().collect();
        for &id in &ids {
            if let Some(graphlet) = self.graphlets.get_mut(&id) {
                graphlet.deactivate();
            }
        }
        self.active_graphlets.clear();
        ids
    }

    /// Get a graphlet by ID
    #[inline]
    pub fn graphlet(&self, id: GraphletId) -> Option<&Graphlet> {
        self.graphlets.get(&id)
    }

    /// Get a mutable graphlet by ID
    #[inline]
    pub fn graphlet_mut(&mut self, id: GraphletId) -> Option<&mut Graphlet> {
        self.graphlets.get_mut(&id)
    }

    /// Get all graphlets of a specific type
    pub fn graphlets_of_type(&self, type_index: u16) -> impl Iterator<Item = &Graphlet> {
        self.type_to_graphlets
            .get(&type_index)
            .into_iter()
            .flat_map(|ids| ids.iter())
            .filter_map(|&id| self.graphlets.get(&id))
    }

    /// Get the active graphlet for a type
    pub fn active_graphlet(&self, type_index: u16) -> Option<&Graphlet> {
        self.active_graphlets
            .get(&type_index)
            .and_then(|&id| self.graphlets.get(&id))
    }

    /// Create a snapshot between graphlets
    pub fn create_snapshot(&mut self, source: GraphletId, target: GraphletId) -> SnapshotId {
        self.snapshots.create(source, target)
    }

    /// Get the snapshot manager
    #[inline]
    pub fn snapshots(&self) -> &SnapshotManager {
        &self.snapshots
    }

    /// Get mutable snapshot manager
    #[inline]
    pub fn snapshots_mut(&mut self) -> &mut SnapshotManager {
        &mut self.snapshots
    }

    /// Mark a graphlet as processed and potentially return it to the pool
    pub fn mark_processed(&mut self, id: GraphletId) {
        if let Some(graphlet) = self.graphlets.get_mut(&id) {
            graphlet.mark_processed();
        }
    }

    /// Clean up processed graphlets
    pub fn cleanup_processed(&mut self) {
        let to_remove: Vec<_> = self
            .graphlets
            .iter()
            .filter(|(_, g)| g.status == GraphletStatus::Processed)
            .map(|(&id, _)| id)
            .collect();

        for id in to_remove {
            if let Some(graphlet) = self.graphlets.remove(&id) {
                // Remove from type mapping
                if let Some(ids) = self.type_to_graphlets.get_mut(&graphlet.type_index) {
                    ids.retain(|i| *i != id);
                }
                // Clear snapshots
                self.snapshots.clear_for_graphlet(id);
                // Return to pool
                self.graphlet_pool.release(graphlet);
            }
        }
    }

    /// Clear the entire graph
    pub fn clear(&mut self) {
        // Return graphlets to pool
        for (_, graphlet) in self.graphlets.drain() {
            self.graphlet_pool.release(graphlet);
        }
        self.active_graphlets.clear();
        self.type_to_graphlets.clear();
        self.snapshots.clear();
        self.next_node_id = 0;
    }

    /// Number of graphlets
    #[inline]
    pub fn num_graphlets(&self) -> usize {
        self.graphlets.len()
    }

    /// Number of active graphlets
    #[inline]
    pub fn num_active(&self) -> usize {
        self.active_graphlets.len()
    }

    /// Total number of events across all graphlets
    pub fn num_events(&self) -> usize {
        self.graphlets.values().map(|g| g.len()).sum()
    }

    /// Set sharing queries for a graphlet
    pub fn set_sharing_queries(
        &mut self,
        graphlet_id: GraphletId,
        queries: SmallVec<[QueryId; 8]>,
    ) {
        if let Some(graphlet) = self.graphlets.get_mut(&graphlet_id) {
            graphlet.sharing_queries = queries;
            graphlet.is_shared = graphlet.sharing_queries.len() > 1;
        }
    }

    /// Link graphlets (predecessor -> successor relationship)
    pub fn link_graphlets(&mut self, predecessor: GraphletId, successor: GraphletId) {
        if let Some(pred) = self.graphlets.get_mut(&predecessor) {
            if !pred.successor_graphlets.contains(&successor) {
                pred.successor_graphlets.push(successor);
            }
        }
        if let Some(succ) = self.graphlets.get_mut(&successor) {
            if !succ.predecessor_graphlets.contains(&predecessor) {
                succ.predecessor_graphlets.push(predecessor);
            }
        }
    }
}

impl Default for HamletGraph {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Event;
    use std::sync::Arc;

    #[test]
    fn test_hamlet_graph_basic() {
        let mut graph = HamletGraph::new();

        let e1 = Arc::new(Event::new("A"));
        let (n1, g1, l1) = graph.add_event(e1, 0);
        assert_eq!(n1, 0);
        assert_eq!(l1, 0);

        let e2 = Arc::new(Event::new("A"));
        let (n2, g2, l2) = graph.add_event(e2, 0);
        assert_eq!(n2, 1);
        assert_eq!(g2, g1); // Same graphlet (same type, still active)
        assert_eq!(l2, 1);
    }

    #[test]
    fn test_graphlet_closing() {
        let mut graph = HamletGraph::new();

        // Add A event
        let e1 = Arc::new(Event::new("A"));
        let (_, g1, _) = graph.add_event(e1, 0);

        // Close A graphlet when B arrives
        let closed = graph.close_graphlet_for_type(0);
        assert_eq!(closed, Some(g1));

        // Add B event
        let e2 = Arc::new(Event::new("B"));
        let (_, g2, _) = graph.add_event(e2, 1);
        assert_ne!(g1, g2);

        // Check A graphlet is inactive
        assert_eq!(graph.graphlet(g1).unwrap().status, GraphletStatus::Inactive);
    }

    #[test]
    fn test_graphlet_linking() {
        let mut graph = HamletGraph::new();

        let e1 = Arc::new(Event::new("A"));
        let (_, g1, _) = graph.add_event(e1, 0);
        graph.close_graphlet_for_type(0);

        let e2 = Arc::new(Event::new("B"));
        let (_, g2, _) = graph.add_event(e2, 1);

        graph.link_graphlets(g1, g2);

        assert!(graph
            .graphlet(g1)
            .unwrap()
            .successor_graphlets
            .contains(&g2));
        assert!(graph
            .graphlet(g2)
            .unwrap()
            .predecessor_graphlets
            .contains(&g1));
    }
}
