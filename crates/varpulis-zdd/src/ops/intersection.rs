//! ZDD Intersection operation
//!
//! Computes S₁ ∩ S₂ (set intersection of families)

use crate::refs::ZddRef;
use crate::table::UniqueTable;
use crate::zdd::Zdd;
use std::collections::HashMap;

impl Zdd {
    /// Compute the intersection of two ZDDs: S₁ ∩ S₂
    ///
    /// The result contains only sets that are in both self and other.
    ///
    /// # Example
    /// ```
    /// use varpulis_zdd::Zdd;
    ///
    /// let a = Zdd::from_set(&[1, 2]).union(&Zdd::from_set(&[3]));  // {{1,2}, {3}}
    /// let b = Zdd::from_set(&[1, 2]).union(&Zdd::from_set(&[4]));  // {{1,2}, {4}}
    /// let c = a.intersection(&b);                                   // {{1,2}}
    /// assert_eq!(c.count(), 1);
    /// ```
    pub fn intersection(&self, other: &Zdd) -> Zdd {
        // Build a combined table with nodes from both ZDDs
        let mut table = self.table().clone();
        let mut node_map: HashMap<u32, ZddRef> = HashMap::new();

        // Remap other's nodes into our table
        let other_root = remap_nodes(other, &mut table, &mut node_map);

        // Now compute intersection with both ZDDs sharing the same table
        let mut cache: HashMap<(ZddRef, ZddRef), ZddRef> = HashMap::new();
        let result_root = intersection_rec(self.root(), other_root, &mut table, &mut cache);

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

fn intersection_rec(
    a: ZddRef,
    b: ZddRef,
    table: &mut UniqueTable,
    cache: &mut HashMap<(ZddRef, ZddRef), ZddRef>,
) -> ZddRef {
    // Terminal cases
    if a == ZddRef::Empty || b == ZddRef::Empty {
        return ZddRef::Empty;
    }
    if a == b {
        return a;
    }

    // Normalize order for cache (intersection is commutative)
    let (a, b) = if a <= b { (a, b) } else { (b, a) };

    // Check cache
    if let Some(&cached) = cache.get(&(a, b)) {
        return cached;
    }

    // Both are Base - intersection is Base
    if a == ZddRef::Base && b == ZddRef::Base {
        return ZddRef::Base;
    }

    // Get node info
    let (a_var, a_lo, a_hi) = get_node_info(a, table);
    let (b_var, b_lo, b_hi) = get_node_info(b, table);

    let result = match (a_var, b_var) {
        (Some(av), Some(bv)) => {
            if av < bv {
                // a's variable comes first - must skip it in intersection
                intersection_rec(a_lo, b, table, cache)
            } else if av > bv {
                // b's variable comes first - must skip it in intersection
                intersection_rec(a, b_lo, table, cache)
            } else {
                // Same variable
                let new_lo = intersection_rec(a_lo, b_lo, table, cache);
                let new_hi = intersection_rec(a_hi, b_hi, table, cache);
                table.get_or_create(av, new_lo, new_hi)
            }
        }
        (Some(_av), None) => {
            // b is Base - only empty set can be in intersection
            intersection_rec(a_lo, b, table, cache)
        }
        (None, Some(_bv)) => {
            // a is Base - only empty set can be in intersection
            intersection_rec(a, b_lo, table, cache)
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
    fn test_intersection_empty() {
        let empty = Zdd::empty();
        let base = Zdd::base();
        let singleton = Zdd::singleton(1);

        assert!(empty.intersection(&base).is_empty());
        assert!(empty.intersection(&singleton).is_empty());
        assert!(base.intersection(&empty).is_empty());
    }

    #[test]
    fn test_intersection_same() {
        let a = Zdd::from_set(&[1, 2, 3]);
        let result = a.intersection(&a);
        assert_eq!(result.count(), 1);
        assert!(result.contains(&[1, 2, 3]));
    }

    #[test]
    fn test_intersection_disjoint() {
        let a = Zdd::from_set(&[1, 2]);
        let b = Zdd::from_set(&[3, 4]);
        let result = a.intersection(&b);

        assert!(result.is_empty());
    }

    #[test]
    fn test_intersection_overlapping() {
        // Create {{1,2}, {3}}
        let a = Zdd::from_set(&[1, 2]).union(&Zdd::from_set(&[3]));
        // Create {{1,2}, {4}}
        let b = Zdd::from_set(&[1, 2]).union(&Zdd::from_set(&[4]));
        let result = a.intersection(&b);

        assert_eq!(result.count(), 1);
        assert!(result.contains(&[1, 2]));
    }

    #[test]
    fn test_intersection_with_base() {
        // {{1,2}} has no empty set, so intersection with {∅} is empty
        let a = Zdd::from_set(&[1, 2]);
        let b = Zdd::base();
        let result = a.intersection(&b);

        assert!(result.is_empty());
    }

    #[test]
    fn test_intersection_family_with_base() {
        // Create {{}, {1}} by doing base.product_with_optional(1)
        let a = Zdd::base().union(&Zdd::singleton(1));
        let b = Zdd::base();
        let result = a.intersection(&b);

        assert_eq!(result.count(), 1);
        assert!(result.contains(&[]));
    }

    #[test]
    fn test_intersection_commutative() {
        let a = Zdd::from_set(&[1, 2]).union(&Zdd::from_set(&[3]));
        let b = Zdd::from_set(&[1, 2]).union(&Zdd::from_set(&[4]));

        let ab = a.intersection(&b);
        let ba = b.intersection(&a);

        assert_eq!(ab.count(), ba.count());
    }
}
