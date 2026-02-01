//! Main Zdd structure
//!
//! The Zdd struct provides the public API for working with ZDDs.

use crate::node::ZddNode;
use crate::refs::ZddRef;
use crate::table::UniqueTable;
use std::collections::HashMap;

/// Zero-suppressed Decision Diagram
///
/// Represents a family of sets (set of sets) in a compact canonical form.
/// In CEP context, each set represents a valid combination of events
/// in a Kleene pattern.
///
/// # Example
/// ```
/// use varpulis_zdd::Zdd;
///
/// // Start with the base case: {∅}
/// let mut zdd = Zdd::base();
///
/// // Extend with optional elements (simulating Kleene+ matching)
/// zdd = zdd.product_with_optional(0);  // {∅, {0}}
/// zdd = zdd.product_with_optional(1);  // {∅, {0}, {1}, {0,1}}
///
/// assert_eq!(zdd.count(), 4);
/// ```
#[derive(Clone)]
pub struct Zdd {
    /// The root reference of this ZDD
    root: ZddRef,
    /// The unique table containing all nodes
    table: UniqueTable,
}

impl Zdd {
    /// Create an empty ZDD representing ∅ (no valid combinations)
    pub fn empty() -> Self {
        Self {
            root: ZddRef::Empty,
            table: UniqueTable::new(),
        }
    }

    /// Create the base ZDD representing {∅} (one valid combination: the empty set)
    ///
    /// This is the starting point for Kleene pattern matching.
    pub fn base() -> Self {
        Self {
            root: ZddRef::Base,
            table: UniqueTable::new(),
        }
    }

    /// Create a singleton ZDD representing {{var}}
    pub fn singleton(var: u32) -> Self {
        let mut table = UniqueTable::new();
        let root = table.get_or_create(var, ZddRef::Empty, ZddRef::Base);
        Self { root, table }
    }

    /// Create a ZDD from a single set of elements
    ///
    /// # Example
    /// ```
    /// use varpulis_zdd::Zdd;
    ///
    /// let zdd = Zdd::from_set(&[1, 3, 5]);
    /// assert_eq!(zdd.count(), 1);
    /// assert!(zdd.contains(&[1, 3, 5]));
    /// ```
    pub fn from_set(elements: &[u32]) -> Self {
        if elements.is_empty() {
            return Self::base();
        }

        let mut sorted: Vec<u32> = elements.to_vec();
        sorted.sort_unstable();
        sorted.dedup();

        // Build chain from highest to lowest variable
        let mut table = UniqueTable::new();
        let mut current = ZddRef::Base;

        for &var in sorted.iter().rev() {
            current = table.get_or_create(var, ZddRef::Empty, current);
        }

        Self {
            root: current,
            table,
        }
    }

    /// Check if this ZDD is empty (represents ∅)
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.root == ZddRef::Empty
    }

    /// Check if this ZDD is the base (represents {∅})
    #[inline]
    pub fn is_base(&self) -> bool {
        self.root == ZddRef::Base
    }

    /// Get the root reference
    #[inline]
    pub fn root(&self) -> ZddRef {
        self.root
    }

    /// Get the number of nodes in this ZDD
    #[inline]
    pub fn node_count(&self) -> usize {
        self.table.len()
    }

    /// Check if a specific combination is in the family
    ///
    /// # Example
    /// ```
    /// use varpulis_zdd::Zdd;
    ///
    /// let zdd = Zdd::from_set(&[1, 2, 3]);
    /// assert!(zdd.contains(&[1, 2, 3]));
    /// assert!(!zdd.contains(&[1, 2]));
    /// ```
    pub fn contains(&self, elements: &[u32]) -> bool {
        let mut sorted: Vec<u32> = elements.to_vec();
        sorted.sort_unstable();
        sorted.dedup();

        self.contains_sorted(&sorted)
    }

    fn contains_sorted(&self, sorted_elements: &[u32]) -> bool {
        let mut current = self.root;
        let mut elem_idx = 0;

        loop {
            match current {
                ZddRef::Empty => return false,
                ZddRef::Base => return elem_idx == sorted_elements.len(),
                ZddRef::Node(id) => {
                    let node = self.table.get_node(id);

                    if elem_idx < sorted_elements.len() && node.var == sorted_elements[elem_idx] {
                        // Variable matches, take HI branch
                        current = node.hi;
                        elem_idx += 1;
                    } else if elem_idx < sorted_elements.len()
                        && node.var > sorted_elements[elem_idx]
                    {
                        // Variable we need is missing (was zero-suppressed)
                        return false;
                    } else {
                        // Variable not in our set, take LO branch
                        current = node.lo;
                    }
                }
            }
        }
    }

    /// Count the number of sets in the family without enumerating them
    ///
    /// Uses memoization for efficiency.
    pub fn count(&self) -> usize {
        let mut cache: HashMap<ZddRef, usize> = HashMap::new();
        self.count_rec(self.root, &mut cache)
    }

    fn count_rec(&self, r: ZddRef, cache: &mut HashMap<ZddRef, usize>) -> usize {
        match r {
            ZddRef::Empty => 0,
            ZddRef::Base => 1,
            ZddRef::Node(id) => {
                if let Some(&cached) = cache.get(&r) {
                    return cached;
                }

                let node = self.table.get_node(id);
                let count = self.count_rec(node.lo, cache) + self.count_rec(node.hi, cache);

                cache.insert(r, count);
                count
            }
        }
    }

    // ========================================================================
    // Internal helpers for operations
    // ========================================================================

    /// Get a node by ID
    #[inline]
    pub(crate) fn get_node(&self, id: u32) -> &ZddNode {
        self.table.get_node(id)
    }

    /// Get the internal table (for operations that need it)
    #[inline]
    pub(crate) fn table(&self) -> &UniqueTable {
        &self.table
    }

    /// Get mutable access to the table
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn table_mut(&mut self) -> &mut UniqueTable {
        &mut self.table
    }

    /// Create a new ZDD with the given root, inheriting this table
    #[allow(dead_code)]
    pub(crate) fn with_root(&self, root: ZddRef) -> Self {
        Self {
            root,
            table: self.table.clone(),
        }
    }

    /// Create a new ZDD with the given root and table
    pub(crate) fn from_parts(root: ZddRef, table: UniqueTable) -> Self {
        Self { root, table }
    }
}

impl Default for Zdd {
    fn default() -> Self {
        Self::empty()
    }
}

impl std::fmt::Debug for Zdd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Zdd")
            .field("root", &self.root)
            .field("node_count", &self.node_count())
            .field("set_count", &self.count())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty() {
        let zdd = Zdd::empty();
        assert!(zdd.is_empty());
        assert!(!zdd.is_base());
        assert_eq!(zdd.count(), 0);
        assert_eq!(zdd.node_count(), 0);
    }

    #[test]
    fn test_base() {
        let zdd = Zdd::base();
        assert!(!zdd.is_empty());
        assert!(zdd.is_base());
        assert_eq!(zdd.count(), 1);
        assert_eq!(zdd.node_count(), 0);
        assert!(zdd.contains(&[]));
        assert!(!zdd.contains(&[0]));
    }

    #[test]
    fn test_singleton() {
        let zdd = Zdd::singleton(5);
        assert_eq!(zdd.count(), 1);
        assert_eq!(zdd.node_count(), 1);
        assert!(zdd.contains(&[5]));
        assert!(!zdd.contains(&[]));
        assert!(!zdd.contains(&[4]));
    }

    #[test]
    fn test_from_set() {
        let zdd = Zdd::from_set(&[3, 1, 2]);
        assert_eq!(zdd.count(), 1);
        assert!(zdd.contains(&[1, 2, 3]));
        assert!(zdd.contains(&[3, 2, 1])); // Order doesn't matter
        assert!(!zdd.contains(&[1, 2]));
        assert!(!zdd.contains(&[1, 2, 3, 4]));
    }

    #[test]
    fn test_from_empty_set() {
        let zdd = Zdd::from_set(&[]);
        assert!(zdd.is_base());
        assert_eq!(zdd.count(), 1);
        assert!(zdd.contains(&[]));
    }

    #[test]
    fn test_from_set_with_duplicates() {
        let zdd = Zdd::from_set(&[1, 1, 2, 2, 3]);
        assert_eq!(zdd.count(), 1);
        assert!(zdd.contains(&[1, 2, 3]));
    }
}
