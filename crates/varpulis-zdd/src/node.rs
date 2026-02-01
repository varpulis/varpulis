//! ZDD internal node structure
//!
//! Each node represents a decision point for a variable (event index in CEP context).

use crate::refs::ZddRef;

/// Internal node of a ZDD
///
/// Each node has:
/// - `var`: the variable (event index) this node decides on
/// - `lo`: branch taken when the variable is NOT included
/// - `hi`: branch taken when the variable IS included
///
/// The ordering invariant: in any path from root to terminal,
/// variables appear in strictly increasing order.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ZddNode {
    /// Variable index (event index in Kleene context)
    pub var: u32,
    /// LO branch: element not included in the combination
    pub lo: ZddRef,
    /// HI branch: element included in the combination
    pub hi: ZddRef,
}

impl ZddNode {
    /// Create a new ZDD node
    #[inline]
    pub fn new(var: u32, lo: ZddRef, hi: ZddRef) -> Self {
        Self { var, lo, hi }
    }

    /// Returns true if both branches lead to the same target
    #[inline]
    pub fn is_redundant(&self) -> bool {
        self.lo == self.hi
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_creation() {
        let node = ZddNode::new(5, ZddRef::Empty, ZddRef::Base);
        assert_eq!(node.var, 5);
        assert_eq!(node.lo, ZddRef::Empty);
        assert_eq!(node.hi, ZddRef::Base);
    }

    #[test]
    fn test_node_equality() {
        let n1 = ZddNode::new(1, ZddRef::Empty, ZddRef::Base);
        let n2 = ZddNode::new(1, ZddRef::Empty, ZddRef::Base);
        let n3 = ZddNode::new(2, ZddRef::Empty, ZddRef::Base);

        assert_eq!(n1, n2);
        assert_ne!(n1, n3);
    }

    #[test]
    fn test_node_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        let n1 = ZddNode::new(1, ZddRef::Empty, ZddRef::Base);
        let n2 = ZddNode::new(1, ZddRef::Empty, ZddRef::Base);

        set.insert(n1);
        assert!(set.contains(&n2));
    }

    #[test]
    fn test_redundant_node() {
        let redundant = ZddNode::new(1, ZddRef::Base, ZddRef::Base);
        let non_redundant = ZddNode::new(1, ZddRef::Empty, ZddRef::Base);

        assert!(redundant.is_redundant());
        assert!(!non_redundant.is_redundant());
    }
}
