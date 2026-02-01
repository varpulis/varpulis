//! ZDD reference types
//!
//! A ZddRef represents either a terminal (Empty/Base) or a reference to an internal node.

/// Reference to a ZDD node or terminal
///
/// - `Empty` (⊥): represents the empty family (no valid combinations)
/// - `Base` (⊤): represents the family containing only the empty set: {∅}
/// - `Node(id)`: reference to an internal node in the UniqueTable
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, PartialOrd, Ord, Default)]
pub enum ZddRef {
    /// Terminal ⊥: empty family, no valid combinations
    #[default]
    Empty,
    /// Terminal ⊤: {∅}, contains only the empty set (base case for Kleene)
    Base,
    /// Reference to an internal node by its ID
    Node(u32),
}

impl ZddRef {
    /// Returns true if this is the Empty terminal
    #[inline]
    pub fn is_empty(&self) -> bool {
        matches!(self, ZddRef::Empty)
    }

    /// Returns true if this is the Base terminal
    #[inline]
    pub fn is_base(&self) -> bool {
        matches!(self, ZddRef::Base)
    }

    /// Returns true if this is a node reference
    #[inline]
    pub fn is_node(&self) -> bool {
        matches!(self, ZddRef::Node(_))
    }

    /// Returns the node ID if this is a Node, None otherwise
    #[inline]
    pub fn node_id(&self) -> Option<u32> {
        match self {
            ZddRef::Node(id) => Some(*id),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zdd_ref_equality() {
        assert_eq!(ZddRef::Empty, ZddRef::Empty);
        assert_eq!(ZddRef::Base, ZddRef::Base);
        assert_eq!(ZddRef::Node(42), ZddRef::Node(42));
        assert_ne!(ZddRef::Empty, ZddRef::Base);
        assert_ne!(ZddRef::Node(1), ZddRef::Node(2));
    }

    #[test]
    fn test_zdd_ref_predicates() {
        assert!(ZddRef::Empty.is_empty());
        assert!(!ZddRef::Empty.is_base());
        assert!(!ZddRef::Empty.is_node());

        assert!(!ZddRef::Base.is_empty());
        assert!(ZddRef::Base.is_base());
        assert!(!ZddRef::Base.is_node());

        assert!(!ZddRef::Node(0).is_empty());
        assert!(!ZddRef::Node(0).is_base());
        assert!(ZddRef::Node(0).is_node());
    }

    #[test]
    fn test_node_id() {
        assert_eq!(ZddRef::Empty.node_id(), None);
        assert_eq!(ZddRef::Base.node_id(), None);
        assert_eq!(ZddRef::Node(42).node_id(), Some(42));
    }
}
