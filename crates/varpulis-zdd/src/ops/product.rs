//! ZDD Product operations
//!
//! The product_with_optional operation is the key operation for Kleene pattern matching.
//! It extends each set in the family with an optional new element.

use crate::refs::ZddRef;
use crate::table::UniqueTable;
use crate::zdd::Zdd;
use std::collections::HashMap;

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
        let mut cache: HashMap<ZddRef, ZddRef> = HashMap::new();

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
        let mut node_map: HashMap<u32, ZddRef> = HashMap::new();

        // Remap other's nodes into our table
        let other_root = remap_nodes(other, &mut table, &mut node_map);

        let mut cache: HashMap<(ZddRef, ZddRef), ZddRef> = HashMap::new();
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
    cache: &mut HashMap<ZddRef, ZddRef>,
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
        // Each path can now have var or not have var
        // LO paths: keep as is (without var) OR add var
        // HI paths: already have var, keep as is OR... (they already have var)
        //
        // Actually: union of (lo, hi) for new_lo (without var),
        // and union of (lo, hi) for new_hi (with var)
        // This means: every combination can either have var or not
        let union_lo_hi = union_refs(n.lo, n.hi, table);
        table.get_or_create(var, union_lo_hi, union_lo_hi)
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

/// Union of two refs (helper for product_with_optional)
fn union_refs(a: ZddRef, b: ZddRef, table: &mut UniqueTable) -> ZddRef {
    let mut cache: HashMap<(ZddRef, ZddRef), ZddRef> = HashMap::new();
    union_refs_rec(a, b, table, &mut cache)
}

fn union_refs_rec(
    a: ZddRef,
    b: ZddRef,
    table: &mut UniqueTable,
    cache: &mut HashMap<(ZddRef, ZddRef), ZddRef>,
) -> ZddRef {
    if a == ZddRef::Empty {
        return b;
    }
    if b == ZddRef::Empty {
        return a;
    }
    if a == b {
        return a;
    }

    let (a, b) = if a <= b { (a, b) } else { (b, a) };

    if let Some(&cached) = cache.get(&(a, b)) {
        return cached;
    }

    if a == ZddRef::Base && b == ZddRef::Base {
        return ZddRef::Base;
    }

    let (a_var, a_lo, a_hi) = get_node_info(a, table);
    let (b_var, b_lo, b_hi) = get_node_info(b, table);

    let result = match (a_var, b_var) {
        (Some(av), Some(bv)) => {
            if av < bv {
                let new_lo = union_refs_rec(a_lo, b, table, cache);
                table.get_or_create(av, new_lo, a_hi)
            } else if av > bv {
                let new_lo = union_refs_rec(a, b_lo, table, cache);
                table.get_or_create(bv, new_lo, b_hi)
            } else {
                let new_lo = union_refs_rec(a_lo, b_lo, table, cache);
                let new_hi = union_refs_rec(a_hi, b_hi, table, cache);
                table.get_or_create(av, new_lo, new_hi)
            }
        }
        (Some(av), None) => {
            let new_lo = union_refs_rec(a_lo, b, table, cache);
            table.get_or_create(av, new_lo, a_hi)
        }
        (None, Some(bv)) => {
            let new_lo = union_refs_rec(a, b_lo, table, cache);
            table.get_or_create(bv, new_lo, b_hi)
        }
        (None, None) => unreachable!(),
    };

    cache.insert((a, b), result);
    result
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

fn product_rec(
    a: ZddRef,
    b: ZddRef,
    table: &mut UniqueTable,
    _zdd: &Zdd,
    _node_map: &HashMap<u32, ZddRef>,
    cache: &mut HashMap<(ZddRef, ZddRef), ZddRef>,
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
    fn test_product_disjoint() {
        // {{1,2}} × {{3,4}} = {{1,2,3,4}}
        let a = Zdd::from_set(&[1, 2]);
        let b = Zdd::from_set(&[3, 4]);
        let result = a.product(&b);

        assert_eq!(result.count(), 1);
        assert!(result.contains(&[1, 2, 3, 4]));
    }
}
