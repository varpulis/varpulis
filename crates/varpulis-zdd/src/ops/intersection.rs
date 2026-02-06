//! ZDD Intersection operation
//!
//! Computes S₁ ∩ S₂ (set intersection of families)

use super::common::{get_node_info, remap_nodes};
use crate::refs::ZddRef;
use crate::table::UniqueTable;
use crate::zdd::Zdd;
use rustc_hash::FxHashMap;

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
        let mut node_map: FxHashMap<u32, ZddRef> = FxHashMap::default();

        // Remap other's nodes into our table
        let other_root = remap_nodes(other, &mut table, &mut node_map);

        // Now compute intersection with both ZDDs sharing the same table
        let mut cache: FxHashMap<(ZddRef, ZddRef), ZddRef> = FxHashMap::default();
        let result_root = intersection_rec(self.root(), other_root, &mut table, &mut cache);

        Zdd::from_parts(result_root, table)
    }
}

fn intersection_rec(
    a: ZddRef,
    b: ZddRef,
    table: &mut UniqueTable,
    cache: &mut FxHashMap<(ZddRef, ZddRef), ZddRef>,
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
