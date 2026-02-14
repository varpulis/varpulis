//! Common helper functions for ZDD operations
//!
//! These helpers are shared across union, intersection, difference, and product operations.

use crate::refs::ZddRef;
use crate::table::UniqueTable;
use crate::zdd::Zdd;
use rustc_hash::FxHashMap;

/// Extract node information from a ZddRef.
///
/// Returns (Option<var>, lo, hi) where:
/// - For Empty: (None, Empty, Empty)
/// - For Base: (None, Base, Empty)
/// - For Node: (Some(var), lo, hi)
#[inline]
pub fn get_node_info(r: ZddRef, table: &UniqueTable) -> (Option<u32>, ZddRef, ZddRef) {
    match r {
        ZddRef::Empty => (None, ZddRef::Empty, ZddRef::Empty),
        ZddRef::Base => (None, ZddRef::Base, ZddRef::Empty),
        ZddRef::Node(id) => {
            let node = table.get_node(id);
            (Some(node.var), node.lo, node.hi)
        }
    }
}

/// Remap all nodes from source ZDD into target table.
///
/// Returns the remapped root reference.
pub fn remap_nodes(
    source: &Zdd,
    target_table: &mut UniqueTable,
    node_map: &mut FxHashMap<u32, ZddRef>,
) -> ZddRef {
    remap_ref(source.root(), source, target_table, node_map)
}

/// Recursively remap a single reference from source to target table.
pub fn remap_ref(
    r: ZddRef,
    source: &Zdd,
    target_table: &mut UniqueTable,
    node_map: &mut FxHashMap<u32, ZddRef>,
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

/// Union of two refs (helper for operations that need inline union).
///
/// Creates a new cache for each call - use arena's union_refs for cached version.
pub fn union_refs(a: ZddRef, b: ZddRef, table: &mut UniqueTable) -> ZddRef {
    let mut cache: FxHashMap<(ZddRef, ZddRef), ZddRef> = FxHashMap::default();
    union_refs_rec(a, b, table, &mut cache)
}

fn union_refs_rec(
    a: ZddRef,
    b: ZddRef,
    table: &mut UniqueTable,
    cache: &mut FxHashMap<(ZddRef, ZddRef), ZddRef>,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_table() -> UniqueTable {
        UniqueTable::new()
    }

    // ── get_node_info ────────────────────────────────────────────

    #[test]
    fn get_node_info_empty() {
        let table = make_table();
        let (var, lo, hi) = get_node_info(ZddRef::Empty, &table);
        assert_eq!(var, None);
        assert_eq!(lo, ZddRef::Empty);
        assert_eq!(hi, ZddRef::Empty);
    }

    #[test]
    fn get_node_info_base() {
        let table = make_table();
        let (var, lo, hi) = get_node_info(ZddRef::Base, &table);
        assert_eq!(var, None);
        assert_eq!(lo, ZddRef::Base);
        assert_eq!(hi, ZddRef::Empty);
    }

    #[test]
    fn get_node_info_node() {
        let mut table = make_table();
        // hi must be non-Empty due to ZDD zero-suppression rule
        let node_ref = table.get_or_create(5, ZddRef::Empty, ZddRef::Base);
        let (var, lo, hi) = get_node_info(node_ref, &table);
        assert_eq!(var, Some(5));
        assert_eq!(lo, ZddRef::Empty);
        assert_eq!(hi, ZddRef::Base);
    }

    // ── union_refs ───────────────────────────────────────────────

    #[test]
    fn union_refs_empty_empty() {
        let mut table = make_table();
        let result = union_refs(ZddRef::Empty, ZddRef::Empty, &mut table);
        assert_eq!(result, ZddRef::Empty);
    }

    #[test]
    fn union_refs_empty_base() {
        let mut table = make_table();
        let result = union_refs(ZddRef::Empty, ZddRef::Base, &mut table);
        assert_eq!(result, ZddRef::Base);
    }

    #[test]
    fn union_refs_base_empty() {
        let mut table = make_table();
        let result = union_refs(ZddRef::Base, ZddRef::Empty, &mut table);
        assert_eq!(result, ZddRef::Base);
    }

    #[test]
    fn union_refs_base_base() {
        let mut table = make_table();
        let result = union_refs(ZddRef::Base, ZddRef::Base, &mut table);
        assert_eq!(result, ZddRef::Base);
    }

    #[test]
    fn union_refs_same_node() {
        let mut table = make_table();
        let n = table.get_or_create(1, ZddRef::Base, ZddRef::Empty);
        let result = union_refs(n, n, &mut table);
        assert_eq!(result, n);
    }

    #[test]
    fn union_refs_node_with_empty() {
        let mut table = make_table();
        let n = table.get_or_create(1, ZddRef::Base, ZddRef::Empty);
        let result = union_refs(n, ZddRef::Empty, &mut table);
        assert_eq!(result, n);
    }

    #[test]
    fn union_refs_empty_with_node() {
        let mut table = make_table();
        let n = table.get_or_create(1, ZddRef::Base, ZddRef::Empty);
        let result = union_refs(ZddRef::Empty, n, &mut table);
        assert_eq!(result, n);
    }

    #[test]
    fn union_refs_node_with_base() {
        let mut table = make_table();
        // Node(var=1, lo=Empty, hi=Base) represents set {{1}}
        let n = table.get_or_create(1, ZddRef::Empty, ZddRef::Base);
        // Union with Base (which represents {{}})
        // Result should be Node(var=1, lo=Base, hi=Base) = {{}, {1}}
        let result = union_refs(n, ZddRef::Base, &mut table);
        // (None, Some(1)) path: a=Base, b=Node → should create node
        match result {
            ZddRef::Node(id) => {
                let node = table.get_node(id);
                assert_eq!(node.var, 1);
                assert_eq!(node.lo, ZddRef::Base); // union of Empty and Base = Base
                assert_eq!(node.hi, ZddRef::Base);
            }
            _ => panic!("Expected Node, got {:?}", result),
        }
    }

    #[test]
    fn union_refs_base_with_node() {
        let mut table = make_table();
        let n = table.get_or_create(1, ZddRef::Empty, ZddRef::Base);
        let result = union_refs(ZddRef::Base, n, &mut table);
        // Normalization should produce same result as node_with_base
        match result {
            ZddRef::Node(id) => {
                let node = table.get_node(id);
                assert_eq!(node.var, 1);
                assert_eq!(node.lo, ZddRef::Base);
            }
            _ => panic!("Expected Node, got {:?}", result),
        }
    }

    #[test]
    fn union_refs_different_vars() {
        let mut table = make_table();
        // n1: var=1, lo=Base, hi=Base → {{}, {1}}
        let n1 = table.get_or_create(1, ZddRef::Base, ZddRef::Base);
        // n2: var=2, lo=Base, hi=Base → {{}, {2}}
        let n2 = table.get_or_create(2, ZddRef::Base, ZddRef::Base);
        // Union should produce {{}, {1}, {2}} or similar
        let result = union_refs(n1, n2, &mut table);
        assert_ne!(result, ZddRef::Empty);
        assert_ne!(result, ZddRef::Base);
        // Should be a node since we have multiple vars
        match result {
            ZddRef::Node(_) => {}
            _ => panic!("Expected Node for union of different vars"),
        }
    }

    #[test]
    fn union_refs_same_var() {
        let mut table = make_table();
        // n1: var=1, lo=Empty, hi=Base → {{1}}
        let n1 = table.get_or_create(1, ZddRef::Empty, ZddRef::Base);
        // n2: var=1, lo=Base, hi=Empty → {{}}  (encoded differently)
        let n2 = table.get_or_create(1, ZddRef::Base, ZddRef::Empty);
        let result = union_refs(n1, n2, &mut table);
        // Should be var=1, lo=Base, hi=Base → {{}, {1}}
        match result {
            ZddRef::Node(id) => {
                let node = table.get_node(id);
                assert_eq!(node.var, 1);
                assert_eq!(node.lo, ZddRef::Base);
                assert_eq!(node.hi, ZddRef::Base);
            }
            _ => panic!("Expected Node, got {:?}", result),
        }
    }

    #[test]
    fn union_refs_cache_hit() {
        let mut table = make_table();
        let n1 = table.get_or_create(1, ZddRef::Base, ZddRef::Base);
        let n2 = table.get_or_create(2, ZddRef::Base, ZddRef::Base);
        let r1 = union_refs(n1, n2, &mut table);
        let r2 = union_refs(n1, n2, &mut table);
        assert_eq!(r1, r2, "Repeated unions should produce same result");
    }

    // ── remap_nodes / remap_ref ──────────────────────────────────

    #[test]
    fn remap_empty_zdd() {
        let source = Zdd::empty();
        let mut target_table = make_table();
        let mut node_map = FxHashMap::default();
        let result = remap_nodes(&source, &mut target_table, &mut node_map);
        assert_eq!(result, ZddRef::Empty);
    }

    #[test]
    fn remap_base_zdd() {
        let source = Zdd::base();
        let mut target_table = make_table();
        let mut node_map = FxHashMap::default();
        let result = remap_nodes(&source, &mut target_table, &mut node_map);
        assert_eq!(result, ZddRef::Base);
    }

    #[test]
    fn remap_single_node() {
        let source = Zdd::from_set(&[1]);
        let mut target_table = make_table();
        let mut node_map = FxHashMap::default();
        let result = remap_nodes(&source, &mut target_table, &mut node_map);
        assert_ne!(result, ZddRef::Empty);
        assert_ne!(result, ZddRef::Base);
    }

    #[test]
    fn remap_multi_node_preserves_structure() {
        let a = Zdd::from_set(&[1, 2]);
        let b = Zdd::from_set(&[1, 3]);
        let source = a.union(&b);
        let mut target_table = make_table();
        let mut node_map = FxHashMap::default();
        let result = remap_nodes(&source, &mut target_table, &mut node_map);
        assert_ne!(result, ZddRef::Empty);
        // Node map should have entries
        assert!(!node_map.is_empty());
    }

    #[test]
    fn remap_cache_hit() {
        let source = Zdd::from_set(&[1, 2, 3]);
        let mut target_table = make_table();
        let mut node_map = FxHashMap::default();
        let r1 = remap_nodes(&source, &mut target_table, &mut node_map);
        // Second remap with same node_map should hit cache
        let r2 = remap_nodes(&source, &mut target_table, &mut node_map);
        assert_eq!(r1, r2, "Cached remap should produce same result");
    }

    #[test]
    fn remap_into_prepopulated_table() {
        let mut target_table = make_table();
        // Pre-populate with some nodes
        target_table.get_or_create(10, ZddRef::Base, ZddRef::Empty);
        target_table.get_or_create(20, ZddRef::Base, ZddRef::Base);

        let source = Zdd::from_set(&[1, 2]);
        let mut node_map = FxHashMap::default();
        let result = remap_nodes(&source, &mut target_table, &mut node_map);
        assert_ne!(result, ZddRef::Empty);
    }
}
