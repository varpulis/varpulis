//! ZDD Product operations
//!
//! The product_with_optional operation is the key operation for Kleene pattern matching.
//! It extends each set in the family with an optional new element.

use super::common::{get_node_info, remap_nodes, union_refs};
use crate::refs::ZddRef;
use crate::table::UniqueTable;
use crate::zdd::Zdd;
use rustc_hash::FxHashMap;

impl Zdd {
    /// Extends each combination with an optional new element.
    ///
    /// Semantically: S × {∅, {var}} = S ∪ {s ∪ {var} | s ∈ S}
    ///
    /// This is the core operation for Kleene pattern matching in CEP.
    /// Each existing combination can either keep its current state or include
    /// the new element.
    ///
    /// # Example
    /// ```
    /// use varpulis_zdd::Zdd;
    ///
    /// // Start with {∅}
    /// let mut zdd = Zdd::base();
    ///
    /// // After first element: {∅, {0}}
    /// zdd = zdd.product_with_optional(0);
    /// assert_eq!(zdd.count(), 2);
    ///
    /// // After second element: {∅, {0}, {1}, {0,1}}
    /// zdd = zdd.product_with_optional(1);
    /// assert_eq!(zdd.count(), 4);
    ///
    /// // After third element: 8 combinations
    /// zdd = zdd.product_with_optional(2);
    /// assert_eq!(zdd.count(), 8);
    /// ```
    ///
    /// # Complexity
    /// O(|Z|) where |Z| is the number of nodes in the ZDD.
    /// This is much better than the naive O(2^n) for n elements.
    pub fn product_with_optional(&self, var: u32) -> Zdd {
        let mut table = self.table().clone();
        let mut cache: FxHashMap<ZddRef, ZddRef> = FxHashMap::default();

        let new_root = product_with_optional_rec(self.root(), var, &mut table, self, &mut cache);

        Zdd::from_parts(new_root, table)
    }

    /// Cartesian product of two families: {a ∪ b | a ∈ S₁, b ∈ S₂}
    ///
    /// # Example
    /// ```
    /// use varpulis_zdd::Zdd;
    ///
    /// let a = Zdd::base().union(&Zdd::singleton(0));  // {∅, {0}}
    /// let b = Zdd::base().union(&Zdd::singleton(1));  // {∅, {1}}
    /// let c = a.product(&b);                          // {∅, {0}, {1}, {0,1}}
    /// assert_eq!(c.count(), 4);
    /// ```
    pub fn product(&self, other: &Zdd) -> Zdd {
        let mut table = self.table().clone();
        let mut node_map: FxHashMap<u32, ZddRef> = FxHashMap::default();

        // Remap other's nodes into our table
        let other_root = remap_nodes(other, &mut table, &mut node_map);

        let mut cache: FxHashMap<(ZddRef, ZddRef), ZddRef> = FxHashMap::default();
        let result_root = product_rec(
            self.root(),
            other_root,
            &mut table,
            self,
            &node_map,
            &mut cache,
        );

        Zdd::from_parts(result_root, table)
    }
}

fn product_with_optional_rec(
    node: ZddRef,
    var: u32,
    table: &mut UniqueTable,
    zdd: &Zdd,
    cache: &mut FxHashMap<ZddRef, ZddRef>,
) -> ZddRef {
    // Terminal cases
    match node {
        ZddRef::Empty => return ZddRef::Empty,
        ZddRef::Base => {
            // {∅} × {∅, {var}} = {∅, {var}}
            // Create node: var? -> (Base, Base) meaning "empty set or {var}"
            return table.get_or_create(var, ZddRef::Base, ZddRef::Base);
        }
        ZddRef::Node(_) => {}
    }

    // Check cache
    if let Some(&cached) = cache.get(&node) {
        return cached;
    }

    let n = zdd.get_node(node.node_id().unwrap());

    let result = if n.var < var {
        // Current variable comes before var in order
        // Recursively process both branches
        let new_lo = product_with_optional_rec(n.lo, var, table, zdd, cache);
        let new_hi = product_with_optional_rec(n.hi, var, table, zdd, cache);
        table.get_or_create(n.var, new_lo, new_hi)
    } else if n.var == var {
        // Node already decides on this variable
        // n.lo = sets that don't contain var
        // n.hi = sets that contain var
        //
        // After product_with_optional(var):
        // - Sets without var can stay without var → n.lo
        // - Sets without var can add var → go to HI
        // - Sets with var keep var → stay in HI
        //
        // new_lo = n.lo (sets that chose not to add var)
        // new_hi = union(n.lo, n.hi) (sets with var: originally had it OR added it)
        let new_hi = union_refs(n.lo, n.hi, table);
        table.get_or_create(var, n.lo, new_hi)
    } else {
        // n.var > var: we need to insert var above this node
        // Every existing combination can now have var or not
        // new_lo = node (combinations without var)
        // new_hi = node (combinations with var added)
        table.get_or_create(var, node, node)
    };

    cache.insert(node, result);
    result
}

fn product_rec(
    a: ZddRef,
    b: ZddRef,
    table: &mut UniqueTable,
    _zdd: &Zdd,
    _node_map: &FxHashMap<u32, ZddRef>,
    cache: &mut FxHashMap<(ZddRef, ZddRef), ZddRef>,
) -> ZddRef {
    // Terminal cases
    if a == ZddRef::Empty || b == ZddRef::Empty {
        return ZddRef::Empty;
    }
    if a == ZddRef::Base {
        return b;
    }
    if b == ZddRef::Base {
        return a;
    }

    // Normalize for cache (product is commutative)
    let (a, b) = if a <= b { (a, b) } else { (b, a) };

    if let Some(&cached) = cache.get(&(a, b)) {
        return cached;
    }

    let (a_var, a_lo, a_hi) = get_node_info(a, table);
    let (b_var, b_lo, b_hi) = get_node_info(b, table);

    let result = match (a_var, b_var) {
        (Some(av), Some(bv)) => {
            if av < bv {
                // Process a's variable first
                let new_lo = product_rec(a_lo, b, table, _zdd, _node_map, cache);
                let new_hi = product_rec(a_hi, b, table, _zdd, _node_map, cache);
                table.get_or_create(av, new_lo, new_hi)
            } else if av > bv {
                // Process b's variable first
                let new_lo = product_rec(a, b_lo, table, _zdd, _node_map, cache);
                let new_hi = product_rec(a, b_hi, table, _zdd, _node_map, cache);
                table.get_or_create(bv, new_lo, new_hi)
            } else {
                // Same variable: product of (lo×lo ∪ lo×hi ∪ hi×lo) and hi×hi
                // Actually for set product: result has var if either input has var
                // new_lo = product(a_lo, b_lo) - neither has var
                // new_hi = product(a_hi, b_lo) ∪ product(a_lo, b_hi) ∪ product(a_hi, b_hi)
                let lo_lo = product_rec(a_lo, b_lo, table, _zdd, _node_map, cache);
                let hi_lo = product_rec(a_hi, b_lo, table, _zdd, _node_map, cache);
                let lo_hi = product_rec(a_lo, b_hi, table, _zdd, _node_map, cache);
                let hi_hi = product_rec(a_hi, b_hi, table, _zdd, _node_map, cache);

                let new_hi = union_refs(union_refs(hi_lo, lo_hi, table), hi_hi, table);
                table.get_or_create(av, lo_lo, new_hi)
            }
        }
        (Some(_), None) => {
            // b is Base, which is identity for product
            a
        }
        (None, Some(_bv)) => {
            // a is Base, which is identity for product
            b
        }
        (None, None) => unreachable!(),
    };

    cache.insert((a, b), result);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn test_product_with_optional_empty() {
        let empty = Zdd::empty();
        let result = empty.product_with_optional(0);
        assert!(result.is_empty());
    }

    #[test]
    fn test_product_with_optional_base() {
        let base = Zdd::base();
        let result = base.product_with_optional(0);

        assert_eq!(result.count(), 2);
        assert!(result.contains(&[]));
        assert!(result.contains(&[0]));
    }

    #[test]
    fn test_product_with_optional_sequence() {
        let mut zdd = Zdd::base();

        // After first: {∅, {0}}
        zdd = zdd.product_with_optional(0);
        assert_eq!(zdd.count(), 2);

        // After second: {∅, {0}, {1}, {0,1}}
        zdd = zdd.product_with_optional(1);
        assert_eq!(zdd.count(), 4);

        // After third: 8 combinations
        zdd = zdd.product_with_optional(2);
        assert_eq!(zdd.count(), 8);

        // Verify all combinations
        assert!(zdd.contains(&[]));
        assert!(zdd.contains(&[0]));
        assert!(zdd.contains(&[1]));
        assert!(zdd.contains(&[2]));
        assert!(zdd.contains(&[0, 1]));
        assert!(zdd.contains(&[0, 2]));
        assert!(zdd.contains(&[1, 2]));
        assert!(zdd.contains(&[0, 1, 2]));
    }

    #[test]
    fn test_product_with_optional_power_of_two() {
        let mut zdd = Zdd::base();

        for i in 0..10 {
            zdd = zdd.product_with_optional(i);
            assert_eq!(zdd.count(), 1 << (i + 1));
        }

        // 2^10 = 1024 combinations, but polynomial nodes
        assert!(zdd.node_count() < 100);
    }

    #[test]
    fn test_product_with_optional_equivalence() {
        // Compare with naive implementation
        let mut naive: std::collections::HashSet<BTreeSet<u32>> = std::collections::HashSet::new();
        naive.insert(BTreeSet::new());

        let mut zdd = Zdd::base();

        for var in 0..8 {
            // Naive
            let mut new_naive = naive.clone();
            for s in &naive {
                let mut extended = s.clone();
                extended.insert(var);
                new_naive.insert(extended);
            }
            naive = new_naive;

            // ZDD
            zdd = zdd.product_with_optional(var);

            // Verify count
            assert_eq!(zdd.count(), naive.len());
        }
    }

    #[test]
    fn test_product_empty() {
        let empty = Zdd::empty();
        let base = Zdd::base();

        assert!(empty.product(&base).is_empty());
        assert!(base.product(&empty).is_empty());
    }

    #[test]
    fn test_product_base() {
        let base = Zdd::base();
        let singleton = Zdd::singleton(1);

        let result = base.product(&singleton);
        assert_eq!(result.count(), 1);
        assert!(result.contains(&[1]));
    }

    #[test]
    fn test_product_families() {
        // {∅, {0}} × {∅, {1}} = {∅, {0}, {1}, {0,1}}
        let a = Zdd::base().product_with_optional(0);
        let b = Zdd::base().product_with_optional(1);
        let result = a.product(&b);

        assert_eq!(result.count(), 4);
        assert!(result.contains(&[]));
        assert!(result.contains(&[0]));
        assert!(result.contains(&[1]));
        assert!(result.contains(&[0, 1]));
    }

    #[test]
    fn test_product_same_variable() {
        // {{1}} × {{1}} = {{1}}
        let a = Zdd::singleton(1);
        let result = a.product(&a);

        assert_eq!(result.count(), 1);
        assert!(result.contains(&[1]));
    }

    #[test]
    fn test_product_with_optional_same_variable() {
        // singleton(0).product_with_optional(0) should return {{0}}
        // NOT {{}, {0}} - the empty set should NOT be added
        //
        // Semantics: {{0}} × {∅, {0}} = {{0} ∪ ∅, {0} ∪ {0}} = {{0}, {0}} = {{0}}
        let singleton = Zdd::singleton(0);
        let result = singleton.product_with_optional(0);

        assert_eq!(
            result.count(),
            1,
            "Should only contain {{{{0}}}}, not {{{{}}}} and {{{{0}}}}"
        );
        assert!(result.contains(&[0]));
        assert!(!result.contains(&[]), "Empty set should NOT be in result");
    }

    #[test]
    fn test_product_disjoint() {
        // {{1,2}} × {{3,4}} = {{1,2,3,4}}
        let a = Zdd::from_set(&[1, 2]);
        let b = Zdd::from_set(&[3, 4]);
        let result = a.product(&b);

        assert_eq!(result.count(), 1);
        assert!(result.contains(&[1, 2, 3, 4]));
    }
}
