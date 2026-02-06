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
