//! ZDD Difference operation
//!
//! Computes S₁ \ S₂ (set difference of families)

use crate::refs::ZddRef;
use crate::table::UniqueTable;
use crate::zdd::Zdd;
use std::collections::HashMap;

impl Zdd {
    /// Compute the set difference of two ZDDs: S₁ \ S₂
    ///
    /// The result contains sets that are in self but not in other.
    ///
    /// # Example
    /// ```
    /// use varpulis_zdd::Zdd;
    ///
    /// let a = Zdd::from_set(&[1, 2]).union(&Zdd::from_set(&[3]));  // {{1,2}, {3}}
    /// let b = Zdd::from_set(&[3]);                                  // {{3}}
    /// let c = a.difference(&b);                                     // {{1,2}}
    /// assert_eq!(c.count(), 1);
    /// ```
    pub fn difference(&self, other: &Zdd) -> Zdd {
        // Build a combined table with nodes from both ZDDs
        let mut table = self.table().clone();
        let mut node_map: HashMap<u32, ZddRef> = HashMap::new();

        // Remap other's nodes into our table
        let other_root = remap_nodes(other, &mut table, &mut node_map);

        // Now compute difference with both ZDDs sharing the same table
        let mut cache: HashMap<(ZddRef, ZddRef), ZddRef> = HashMap::new();
        let result_root = difference_rec(self.root(), other_root, &mut table, &mut cache);

        Zdd::from_parts(result_root, table)
    }
}

/// Remap nodes from source ZDD into target table
fn remap_nodes(
    source: &Zdd,
    target_table: &mut UniqueTable,
    node_map: &mut HashMap<u32, ZddRef>,
) -> ZddRef {
    remap_ref(source.root(), source, target_table, node_map)
}

fn remap_ref(
    r: ZddRef,
    source: &Zdd,
    target_table: &mut UniqueTable,
    node_map: &mut HashMap<u32, ZddRef>,
) -> ZddRef {
    match r {
        ZddRef::Empty => ZddRef::Empty,
        ZddRef::Base => ZddRef::Base,
        ZddRef::Node(id) => {
            if let Some(&mapped) = node_map.get(&id) {
                return mapped;
            }

            let node = source.get_node(id);
            let new_lo = remap_ref(node.lo, source, target_table, node_map);
            let new_hi = remap_ref(node.hi, source, target_table, node_map);
            let new_ref = target_table.get_or_create(node.var, new_lo, new_hi);

            node_map.insert(id, new_ref);
            new_ref
        }
    }
}

fn difference_rec(
    a: ZddRef,
    b: ZddRef,
    table: &mut UniqueTable,
    cache: &mut HashMap<(ZddRef, ZddRef), ZddRef>,
) -> ZddRef {
    // Terminal cases
    if a == ZddRef::Empty {
        return ZddRef::Empty;
    }
    if b == ZddRef::Empty {
        return a;
    }
    if a == b {
        return ZddRef::Empty;
    }

    // Check cache (difference is NOT commutative)
    if let Some(&cached) = cache.get(&(a, b)) {
        return cached;
    }

    // Base \ Base = Empty (handled by a == b above)
    // Base \ Node = need to check if empty set is in Node

    // Get node info
    let (a_var, a_lo, a_hi) = get_node_info(a, table);
    let (b_var, b_lo, b_hi) = get_node_info(b, table);

    let result = match (a_var, b_var) {
        (Some(av), Some(bv)) => {
            if av < bv {
                // a's variable comes first - b doesn't have this variable
                let new_lo = difference_rec(a_lo, b, table, cache);
                let new_hi = a_hi; // Keep hi unchanged since b doesn't have this var
                table.get_or_create(av, new_lo, new_hi)
            } else if av > bv {
                // b's variable comes first - a doesn't have this variable
                // Sets in a without bv are in a, sets in a with bv need to check b_hi
                difference_rec(a, b_lo, table, cache)
            } else {
                // Same variable
                let new_lo = difference_rec(a_lo, b_lo, table, cache);
                let new_hi = difference_rec(a_hi, b_hi, table, cache);
                table.get_or_create(av, new_lo, new_hi)
            }
        }
        (Some(av), None) => {
            // b is Base - remove empty set from a if present
            let new_lo = difference_rec(a_lo, b, table, cache);
            table.get_or_create(av, new_lo, a_hi)
        }
        (None, Some(_bv)) => {
            // a is Base - if b contains empty set, result is empty
            // b has a variable, so empty set is only in b_lo path
            difference_rec(a, b_lo, table, cache)
        }
        (None, None) => {
            // Both are terminals (already handled above)
            unreachable!()
        }
    };

    cache.insert((a, b), result);
    result
}

fn get_node_info(r: ZddRef, table: &UniqueTable) -> (Option<u32>, ZddRef, ZddRef) {
    match r {
        ZddRef::Empty => (None, ZddRef::Empty, ZddRef::Empty),
        ZddRef::Base => (None, ZddRef::Base, ZddRef::Empty),
        ZddRef::Node(id) => {
            let node = table.get_node(id);
            (Some(node.var), node.lo, node.hi)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_difference_empty() {
        let empty = Zdd::empty();
        let base = Zdd::base();
        let singleton = Zdd::singleton(1);

        assert!(empty.difference(&base).is_empty());
        assert!(empty.difference(&singleton).is_empty());
        assert!(base.difference(&empty).is_base());
    }

    #[test]
    fn test_difference_same() {
        let a = Zdd::from_set(&[1, 2, 3]);
        let result = a.difference(&a);
        assert!(result.is_empty());
    }

    #[test]
    fn test_difference_disjoint() {
        let a = Zdd::from_set(&[1, 2]);
        let b = Zdd::from_set(&[3, 4]);
        let result = a.difference(&b);

        assert_eq!(result.count(), 1);
        assert!(result.contains(&[1, 2]));
    }

    #[test]
    fn test_difference_overlapping() {
        // Create {{1,2}, {3}}
        let a = Zdd::from_set(&[1, 2]).union(&Zdd::from_set(&[3]));
        // Create {{3}}
        let b = Zdd::from_set(&[3]);
        let result = a.difference(&b);

        assert_eq!(result.count(), 1);
        assert!(result.contains(&[1, 2]));
        assert!(!result.contains(&[3]));
    }

    #[test]
    fn test_difference_superset() {
        // Create {{1,2}, {3}, {4}}
        let a = Zdd::from_set(&[1, 2])
            .union(&Zdd::from_set(&[3]))
            .union(&Zdd::from_set(&[4]));
        // Create {{3}, {4}}
        let b = Zdd::from_set(&[3]).union(&Zdd::from_set(&[4]));
        let result = a.difference(&b);

        assert_eq!(result.count(), 1);
        assert!(result.contains(&[1, 2]));
    }

    #[test]
    fn test_difference_with_base() {
        // Create {{}, {1,2}}
        let a = Zdd::base().union(&Zdd::from_set(&[1, 2]));
        let b = Zdd::base();
        let result = a.difference(&b);

        assert_eq!(result.count(), 1);
        assert!(result.contains(&[1, 2]));
        assert!(!result.contains(&[]));
    }

    #[test]
    fn test_difference_not_commutative() {
        let a = Zdd::from_set(&[1, 2]).union(&Zdd::from_set(&[3]));
        let b = Zdd::from_set(&[3]);

        let a_minus_b = a.difference(&b);
        let b_minus_a = b.difference(&a);

        assert_eq!(a_minus_b.count(), 1);
        assert!(a_minus_b.contains(&[1, 2]));

        assert!(b_minus_a.is_empty());
    }
}
