//! UniqueTable for ZDD canonicalization
//!
//! The UniqueTable ensures that each unique (var, lo, hi) triple maps to exactly
//! one node, enabling maximal structure sharing and canonical form.

use crate::node::ZddNode;
use crate::refs::ZddRef;
use rustc_hash::FxHashMap;

/// UniqueTable for ZDD node management
///
/// Guarantees:
/// 1. Canonicity: identical (var, lo, hi) → same node ID
/// 2. Maximal sharing: identical substructures share memory
/// 3. Zero-suppression: if hi == Empty, the node is skipped
#[derive(Clone)]
pub struct UniqueTable {
    /// Storage for all nodes
    nodes: Vec<ZddNode>,
    /// Index: (var, lo, hi) → node_id for O(1) lookup
    index: FxHashMap<ZddNode, u32>,
}

impl UniqueTable {
    /// Create a new empty UniqueTable
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            index: FxHashMap::default(),
        }
    }

    /// Create a UniqueTable with preallocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            nodes: Vec::with_capacity(capacity),
            index: FxHashMap::with_capacity_and_hasher(capacity, Default::default()),
        }
    }

    /// Get or create a canonical node.
    ///
    /// Applies the zero-suppression rule automatically:
    /// if hi == Empty, returns lo directly (node is suppressed).
    ///
    /// # Arguments
    /// * `var` - The variable index
    /// * `lo` - The LO branch (element not included)
    /// * `hi` - The HI branch (element included)
    ///
    /// # Returns
    /// A canonical ZddRef for the given triple
    pub fn get_or_create(&mut self, var: u32, lo: ZddRef, hi: ZddRef) -> ZddRef {
        // ZERO-SUPPRESSION RULE: if hi points to Empty, skip this node
        if hi == ZddRef::Empty {
            return lo;
        }

        let node = ZddNode::new(var, lo, hi);

        // Check if node already exists
        if let Some(&existing_id) = self.index.get(&node) {
            return ZddRef::Node(existing_id);
        }

        // Create new node
        let id = self.nodes.len() as u32;
        self.nodes.push(node);
        self.index.insert(node, id);

        ZddRef::Node(id)
    }

    /// Get a node by its ID
    ///
    /// # Panics
    /// Panics if the ID is invalid
    #[inline]
    pub fn get_node(&self, id: u32) -> &ZddNode {
        &self.nodes[id as usize]
    }

    /// Try to get a node by its ID
    #[inline]
    pub fn try_get_node(&self, id: u32) -> Option<&ZddNode> {
        self.nodes.get(id as usize)
    }

    /// Get a node from a ZddRef
    ///
    /// # Panics
    /// Panics if the ref is not a Node or the ID is invalid
    #[inline]
    pub fn get_node_from_ref(&self, r: ZddRef) -> &ZddNode {
        match r {
            ZddRef::Node(id) => self.get_node(id),
            _ => panic!("Expected Node, got {:?}", r),
        }
    }

    /// Number of nodes in the table
    #[inline]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Check if the table is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Clear all nodes (for testing/reset)
    pub fn clear(&mut self) {
        self.nodes.clear();
        self.index.clear();
    }

    /// Get statistics about the table
    pub fn stats(&self) -> TableStats {
        TableStats {
            node_count: self.nodes.len(),
            capacity: self.nodes.capacity(),
            index_capacity: self.index.capacity(),
        }
    }
}

impl Default for UniqueTable {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about the UniqueTable
#[derive(Debug, Clone)]
pub struct TableStats {
    pub node_count: usize,
    pub capacity: usize,
    pub index_capacity: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zero_suppression_rule() {
        let mut table = UniqueTable::new();

        // If hi == Empty, should return lo directly
        let result = table.get_or_create(5, ZddRef::Base, ZddRef::Empty);
        assert_eq!(result, ZddRef::Base);
        assert_eq!(table.len(), 0); // No node created

        let result = table.get_or_create(5, ZddRef::Empty, ZddRef::Empty);
        assert_eq!(result, ZddRef::Empty);
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn test_node_creation() {
        let mut table = UniqueTable::new();

        let r1 = table.get_or_create(1, ZddRef::Empty, ZddRef::Base);
        assert!(r1.is_node());
        assert_eq!(table.len(), 1);

        // Same triple should return same node
        let r2 = table.get_or_create(1, ZddRef::Empty, ZddRef::Base);
        assert_eq!(r1, r2);
        assert_eq!(table.len(), 1);

        // Different triple should create new node
        let r3 = table.get_or_create(2, ZddRef::Empty, ZddRef::Base);
        assert_ne!(r1, r3);
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn test_get_node() {
        let mut table = UniqueTable::new();

        let r = table.get_or_create(5, ZddRef::Empty, ZddRef::Base);
        let id = r.node_id().unwrap();

        let node = table.get_node(id);
        assert_eq!(node.var, 5);
        assert_eq!(node.lo, ZddRef::Empty);
        assert_eq!(node.hi, ZddRef::Base);
    }

    #[test]
    fn test_canonicity() {
        let mut table = UniqueTable::new();

        // Create nodes in different orders, should get same structure
        let n1 = table.get_or_create(1, ZddRef::Empty, ZddRef::Base);
        let n2 = table.get_or_create(2, n1, ZddRef::Base);
        let n3 = table.get_or_create(3, n2, n1);

        // Create same structure again
        let m1 = table.get_or_create(1, ZddRef::Empty, ZddRef::Base);
        let m2 = table.get_or_create(2, m1, ZddRef::Base);
        let m3 = table.get_or_create(3, m2, m1);

        assert_eq!(n1, m1);
        assert_eq!(n2, m2);
        assert_eq!(n3, m3);
    }
}
