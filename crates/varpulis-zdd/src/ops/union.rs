//! ZDD Union operation
//!
//! Computes S₁ ∪ S₂ (set union of families)

use super::common::{get_node_info, remap_nodes};
use crate::refs::ZddRef;
use crate::table::UniqueTable;
use crate::zdd::Zdd;
use rustc_hash::FxHashMap;

impl Zdd {
    /// Compute the union of two ZDDs: S₁ ∪ S₂
    ///
    /// The result contains all sets that are in either self or other.
    ///
    /// # Example
    /// ```
    /// use varpulis_zdd::Zdd;
    ///
    /// let a = Zdd::from_set(&[1, 2]);  // {{1,2}}
    /// let b = Zdd::from_set(&[3]);     // {{3}}
    /// let c = a.union(&b);             // {{1,2}, {3}}
    /// assert_eq!(c.count(), 2);
    /// ```
    pub fn union(&self, other: &Zdd) -> Zdd {
        // Build a combined table with nodes from both ZDDs
        let mut table = self.table().clone();
        let mut node_map: FxHashMap<u32, ZddRef> = FxHashMap::default();

        // Remap other's nodes into our table
        let other_root = remap_nodes(other, &mut table, &mut node_map);

        // Now compute union with both ZDDs sharing the same table
        let mut cache: FxHashMap<(ZddRef, ZddRef), ZddRef> = FxHashMap::default();
        let result_root = union_rec(self.root(), other_root, &mut table, &mut cache);

        Zdd::from_parts(result_root, table)
    }
}

fn union_rec(
    a: ZddRef,
    b: ZddRef,
    table: &mut UniqueTable,
    cache: &mut FxHashMap<(ZddRef, ZddRef), ZddRef>,
) -> ZddRef {
    // Terminal cases
    if a == ZddRef::Empty {
        return b;
    }
    if b == ZddRef::Empty {
        return a;
    }
    if a == b {
        return a;
    }

    // Normalize order for cache (union is commutative)
    let (a, b) = if a <= b { (a, b) } else { (b, a) };

    // Check cache
    if let Some(&cached) = cache.get(&(a, b)) {
        return cached;
    }

    // Both are Base - union is Base
    if a == ZddRef::Base && b == ZddRef::Base {
        return ZddRef::Base;
    }

    // Get node info
    let (a_var, a_lo, a_hi) = get_node_info(a, table);
    let (b_var, b_lo, b_hi) = get_node_info(b, table);

    let result = match (a_var, b_var) {
        (Some(av), Some(bv)) => {
            if av < bv {
                // a's variable comes first
                let new_lo = union_rec(a_lo, b, table, cache);
                let new_hi = a_hi;
                table.get_or_create(av, new_lo, new_hi)
            } else if av > bv {
                // b's variable comes first
                let new_lo = union_rec(a, b_lo, table, cache);
                let new_hi = b_hi;
                table.get_or_create(bv, new_lo, new_hi)
            } else {
                // Same variable
                let new_lo = union_rec(a_lo, b_lo, table, cache);
                let new_hi = union_rec(a_hi, b_hi, table, cache);
                table.get_or_create(av, new_lo, new_hi)
            }
        }
        (Some(av), None) => {
            // b is Base
            let new_lo = union_rec(a_lo, b, table, cache);
            table.get_or_create(av, new_lo, a_hi)
        }
        (None, Some(bv)) => {
            // a is Base
            let new_lo = union_rec(a, b_lo, table, cache);
            table.get_or_create(bv, new_lo, b_hi)
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
    fn test_union_empty() {
        let empty = Zdd::empty();
        let base = Zdd::base();

        let result = empty.union(&base);
        assert!(result.is_base());

        let result = base.union(&empty);
        assert!(result.is_base());
    }

    #[test]
    fn test_union_same() {
        let a = Zdd::from_set(&[1, 2, 3]);
        let result = a.union(&a);
        assert_eq!(result.count(), 1);
        assert!(result.contains(&[1, 2, 3]));
    }

    #[test]
    fn test_union_disjoint() {
        let a = Zdd::from_set(&[1, 2]);
        let b = Zdd::from_set(&[3, 4]);
        let result = a.union(&b);

        assert_eq!(result.count(), 2);
        assert!(result.contains(&[1, 2]));
        assert!(result.contains(&[3, 4]));
    }

    #[test]
    fn test_union_overlapping() {
        let a = Zdd::from_set(&[1, 2]);
        let b = Zdd::from_set(&[2, 3]);
        let result = a.union(&b);

        assert_eq!(result.count(), 2);
        assert!(result.contains(&[1, 2]));
        assert!(result.contains(&[2, 3]));
    }

    #[test]
    fn test_union_with_base() {
        let a = Zdd::from_set(&[1, 2]);
        let b = Zdd::base();
        let result = a.union(&b);

        assert_eq!(result.count(), 2);
        assert!(result.contains(&[1, 2]));
        assert!(result.contains(&[]));
    }

    #[test]
    fn test_union_singleton() {
        let a = Zdd::singleton(1);
        let b = Zdd::singleton(2);
        let result = a.union(&b);

        assert_eq!(result.count(), 2);
        assert!(result.contains(&[1]));
        assert!(result.contains(&[2]));
    }

    #[test]
    fn test_union_commutative() {
        let a = Zdd::from_set(&[1, 3]);
        let b = Zdd::from_set(&[2, 4]);

        let ab = a.union(&b);
        let ba = b.union(&a);

        assert_eq!(ab.count(), ba.count());
    }

    #[test]
    fn test_union_associative() {
        let a = Zdd::from_set(&[1]);
        let b = Zdd::from_set(&[2]);
        let c = Zdd::from_set(&[3]);

        let ab_c = a.union(&b).union(&c);
        let a_bc = a.union(&b.union(&c));

        assert_eq!(ab_c.count(), 3);
        assert_eq!(a_bc.count(), 3);
    }
}
