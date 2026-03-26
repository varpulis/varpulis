//! Coverage tests for ZddArena — targeting uncovered SharedArena and edge cases.

use varpulis_zdd::{SharedArena, Zdd, ZddArena, ZddHandle};

// =============================================================================
// SharedArena full API coverage
// =============================================================================

#[test]
fn shared_arena_new() {
    let arena = SharedArena::new();
    assert_eq!(arena.node_count(), 0);
}

#[test]
fn shared_arena_with_capacity() {
    let arena = SharedArena::with_capacity(100);
    assert_eq!(arena.node_count(), 0);
}

#[test]
fn shared_arena_base_and_empty() {
    let arena = SharedArena::new();
    let base = arena.base();
    let empty = arena.empty();

    assert!(base.is_base());
    assert!(!base.is_empty());
    assert!(empty.is_empty());
    assert!(!empty.is_base());
}

#[test]
fn shared_arena_singleton() {
    let arena = SharedArena::new();
    let s = arena.singleton(42);
    assert_eq!(arena.count(s), 1);
    assert!(arena.contains(s, &[42]));
    assert!(!arena.contains(s, &[]));
}

#[test]
fn shared_arena_from_set() {
    let arena = SharedArena::new();
    let s = arena.from_set(&[1, 3, 5]);
    assert_eq!(arena.count(s), 1);
    assert!(arena.contains(s, &[1, 3, 5]));
}

#[test]
fn shared_arena_from_set_empty() {
    let arena = SharedArena::new();
    let s = arena.from_set(&[]);
    assert!(s.is_base()); // base = {∅}
    assert_eq!(arena.count(s), 1);
    assert!(arena.contains(s, &[]));
}

#[test]
fn shared_arena_product_with_optional() {
    let arena = SharedArena::new();
    let base = arena.base();
    let z1 = arena.product_with_optional(base, 0);
    let z2 = arena.product_with_optional(z1, 1);

    assert_eq!(arena.count(z2), 4);
    assert!(arena.contains(z2, &[]));
    assert!(arena.contains(z2, &[0]));
    assert!(arena.contains(z2, &[1]));
    assert!(arena.contains(z2, &[0, 1]));
}

#[test]
fn shared_arena_union() {
    let arena = SharedArena::new();
    let a = arena.from_set(&[1]);
    let b = arena.from_set(&[2]);
    let c = arena.union(a, b);

    assert_eq!(arena.count(c), 2);
    assert!(arena.contains(c, &[1]));
    assert!(arena.contains(c, &[2]));
}

#[test]
fn shared_arena_intersection() {
    let arena = SharedArena::new();
    let a = arena.from_set(&[1, 2]);
    let b = arena.from_set(&[1, 2]);
    let c = arena.intersection(a, b);

    assert_eq!(arena.count(c), 1);
    assert!(arena.contains(c, &[1, 2]));
}

#[test]
fn shared_arena_intersection_disjoint() {
    let arena = SharedArena::new();
    let a = arena.from_set(&[1]);
    let b = arena.from_set(&[2]);
    let c = arena.intersection(a, b);

    assert_eq!(arena.count(c), 0);
    assert!(c.is_empty());
}

#[test]
fn shared_arena_difference() {
    let arena = SharedArena::new();
    let a = arena.from_set(&[1]);
    let b = arena.from_set(&[2]);
    let ab = arena.union(a, b);
    let result = arena.difference(ab, a);

    assert_eq!(arena.count(result), 1);
    assert!(arena.contains(result, &[2]));
}

#[test]
fn shared_arena_contains_sorted() {
    let arena = SharedArena::new();
    let base = arena.base();
    let z = arena.product_with_optional(base, 5);

    assert!(arena.contains_sorted(z, &[]));
    assert!(arena.contains_sorted(z, &[5]));
    assert!(!arena.contains_sorted(z, &[3]));
}

#[test]
fn shared_arena_count_cached() {
    let arena = SharedArena::new();
    let base = arena.base();
    let z = arena.product_with_optional(base, 0);
    let z = arena.product_with_optional(z, 1);

    // First call fills cache
    assert_eq!(arena.count_cached(z), 4);
    // Second call should hit cache
    assert_eq!(arena.count_cached(z), 4);
}

#[test]
fn shared_arena_gc() {
    let arena = SharedArena::new();
    let a = arena.from_set(&[1, 2]);
    let _b = arena.from_set(&[3, 4]); // will be garbage

    let (stats, new_handles) = arena.gc(&[a]);
    assert!(stats.nodes_after <= stats.nodes_before);
    assert_eq!(new_handles.len(), 1);
    assert!(arena.contains(new_handles[0], &[1, 2]));
}

#[test]
fn shared_arena_gc_caches_only() {
    let arena = SharedArena::new();
    let a = arena.from_set(&[1]);
    let b = arena.from_set(&[2]);
    let _ = arena.union(a, b); // fills cache

    let cleared = arena.gc_caches_only();
    assert!(cleared > 0);
}

#[test]
fn shared_arena_stats() {
    let arena = SharedArena::new();
    let stats = arena.stats();
    assert_eq!(stats.node_count, 0);
    assert_eq!(stats.union_cache_size, 0);
}

#[test]
fn shared_arena_clone_arc() {
    let arena = SharedArena::new();
    let cloned = arena.clone_arc();

    // Both should share state
    let s = arena.from_set(&[1]);
    assert_eq!(cloned.count(s), 1);
}

#[test]
fn shared_arena_clone_trait() {
    let arena = SharedArena::new();
    let cloned = arena.clone();

    let s = arena.from_set(&[1]);
    assert_eq!(cloned.count(s), 1);
}

#[test]
fn shared_arena_default() {
    let arena = SharedArena::default();
    assert_eq!(arena.node_count(), 0);
}

// =============================================================================
// ZddArena edge cases
// =============================================================================

#[test]
fn arena_with_capacity() {
    let mut arena = ZddArena::with_capacity(100);
    let zdd = arena.base();
    assert_eq!(arena.count(zdd), 1);
}

#[test]
fn arena_default_impl() {
    let mut arena = ZddArena::default();
    let base = arena.base();
    assert_eq!(arena.count(base), 1);
}

#[test]
fn arena_debug_fmt() {
    let arena = ZddArena::new();
    let dbg = format!("{arena:?}");
    assert!(dbg.contains("ZddArena"));
    assert!(dbg.contains("node_count"));
}

#[test]
fn arena_from_set_with_duplicates() {
    let mut arena = ZddArena::new();
    // Duplicates should be deduped
    let s = arena.from_set(&[3, 1, 2, 1, 3]);
    assert_eq!(arena.count(s), 1);
    assert!(arena.contains(s, &[1, 2, 3]));
}

#[test]
fn arena_from_set_unsorted() {
    let mut arena = ZddArena::new();
    // Should sort internally
    let s = arena.from_set(&[5, 2, 8, 1]);
    assert_eq!(arena.count(s), 1);
    assert!(arena.contains(s, &[1, 2, 5, 8]));
}

#[test]
fn arena_count_uncached() {
    let mut arena = ZddArena::new();
    let mut zdd = arena.base();
    zdd = arena.product_with_optional(zdd, 0);
    zdd = arena.product_with_optional(zdd, 1);

    // count_uncached uses &self (no cache mutation)
    assert_eq!(arena.count_uncached(zdd), 4);
    assert_eq!(arena.count_uncached(zdd), 4);
}

#[test]
fn arena_intersection_with_base() {
    let mut arena = ZddArena::new();
    let base = arena.base();
    let s = arena.from_set(&[1, 2]);
    let ab = arena.union(base, s);

    // Intersection with base: only the empty set survives
    let result = arena.intersection(ab, base);
    assert_eq!(arena.count(result), 1);
    assert!(arena.contains(result, &[]));
}

#[test]
fn arena_difference_base_from_union() {
    let mut arena = ZddArena::new();
    let base = arena.base();
    let s = arena.from_set(&[1]);
    let with_base = arena.union(base, s);

    // Remove base (empty set) from the family
    let result = arena.difference(with_base, base);
    assert_eq!(arena.count(result), 1);
    assert!(arena.contains(result, &[1]));
    assert!(!arena.contains(result, &[]));
}

#[test]
fn arena_difference_with_different_vars() {
    let mut arena = ZddArena::new();
    let a = arena.from_set(&[1, 2]);
    let b = arena.from_set(&[3, 4]);
    let ab = arena.union(a, b);

    // Difference with a set having different top variable
    let c = arena.from_set(&[5]);
    let result = arena.difference(ab, c);
    assert_eq!(arena.count(result), 2); // No overlap, so nothing removed
}

#[test]
fn arena_intersection_with_empty() {
    let mut arena = ZddArena::new();
    let s = arena.from_set(&[1, 2]);
    let empty = arena.empty();
    let result = arena.intersection(s, empty);
    assert!(result.is_empty());
}

#[test]
fn arena_difference_of_empty() {
    let mut arena = ZddArena::new();
    let empty = arena.empty();
    let s = arena.from_set(&[1]);
    let result = arena.difference(empty, s);
    assert!(result.is_empty());
}

#[test]
fn arena_iter_empty() {
    let arena = ZddArena::new();
    let empty = arena.empty();
    let sets: Vec<Vec<u32>> = arena.iter(empty).collect();
    assert!(sets.is_empty());
}

#[test]
fn arena_iter_base_only() {
    let arena = ZddArena::new();
    let base = arena.base();
    let sets: Vec<Vec<u32>> = arena.iter(base).collect();
    assert_eq!(sets.len(), 1);
    assert_eq!(sets[0], Vec::<u32>::new()); // base = {∅}
}

#[test]
fn zdd_handle_default() {
    let h = ZddHandle::default();
    assert!(h.is_empty());
}

#[test]
fn arena_gc_multiple_live_handles() {
    let mut arena = ZddArena::new();
    let a = arena.from_set(&[1]);
    let b = arena.from_set(&[2]);
    let _c = arena.from_set(&[3]); // garbage

    let (stats, new) = arena.gc(&[a, b]);
    assert_eq!(new.len(), 2);
    assert!(arena.contains(new[0], &[1]));
    assert!(arena.contains(new[1], &[2]));
    assert!(stats.nodes_after <= stats.nodes_before);
}

#[test]
fn arena_gc_empty_live_set() {
    let mut arena = ZddArena::new();
    let _a = arena.from_set(&[1, 2]);
    let (stats, new) = arena.gc(&[]);
    assert_eq!(new.len(), 0);
    assert_eq!(stats.nodes_after, 0);
}

// =============================================================================
// Zdd::to_dot() with multiple nodes
// =============================================================================

#[test]
fn zdd_to_dot_multiple_nodes() {
    // Build a ZDD with multiple internal nodes via product_with_optional chain
    let zdd = Zdd::base()
        .product_with_optional(0)
        .product_with_optional(1)
        .product_with_optional(2);

    let dot = zdd.to_dot();

    // Verify DOT structure
    assert!(
        dot.starts_with("digraph ZDD {"),
        "should start with digraph header"
    );
    assert!(
        dot.contains("rankdir=TB"),
        "should specify top-to-bottom layout"
    );
    assert!(
        dot.contains("node [shape=circle]"),
        "should set default node shape"
    );

    // Terminal nodes are always declared
    assert!(
        dot.contains("Empty [shape=box"),
        "should define Empty terminal"
    );
    assert!(
        dot.contains("Base [shape=box"),
        "should define Base terminal"
    );

    // Internal nodes for variables 0, 1, 2
    assert!(dot.contains("label=\"0\""), "should have node for var 0");
    assert!(dot.contains("label=\"1\""), "should have node for var 1");
    assert!(dot.contains("label=\"2\""), "should have node for var 2");

    // Edges: LO (dashed) and HI (solid)
    assert!(dot.contains("style=dashed"), "should have dashed LO edges");

    // Should have edges pointing to Base terminal (product_with_optional always
    // reaches Base since the empty set is always included)
    assert!(
        dot.contains("-> Base"),
        "should have edges to Base terminal"
    );

    // Now test with a union that also produces Empty edges
    let union_zdd = Zdd::singleton(10).union(&Zdd::singleton(20));
    let union_dot = union_zdd.to_dot();
    assert!(
        union_dot.contains("label=\"10\""),
        "should have node for var 10"
    );
    assert!(
        union_dot.contains("label=\"20\""),
        "should have node for var 20"
    );
    assert!(
        union_dot.contains("-> Empty"),
        "union ZDD should have edges to Empty"
    );
}

// =============================================================================
// Larger product_with_optional chains followed by iter() verification
// =============================================================================

#[test]
fn arena_product_with_optional_chain_iter() {
    let mut arena = ZddArena::new();
    let mut zdd = arena.base();

    // Build a chain of 4 optional variables: 2^4 = 16 sets
    for var in 0..4 {
        zdd = arena.product_with_optional(zdd, var);
    }

    assert_eq!(arena.count_uncached(zdd), 16);

    // Collect all sets and verify properties
    let sets: Vec<Vec<u32>> = arena.iter(zdd).collect();
    assert_eq!(sets.len(), 16);

    // Every set should only contain elements from {0, 1, 2, 3}
    for set in &sets {
        for &elem in set {
            assert!(elem < 4, "element {elem} should be in range 0..4");
        }
        // Each set should be sorted
        for window in set.windows(2) {
            assert!(window[0] < window[1], "set should be sorted: {set:?}");
        }
    }

    // Verify specific membership: empty set and full set should both be present
    assert!(arena.contains(zdd, &[]), "should contain the empty set");
    assert!(
        arena.contains(zdd, &[0, 1, 2, 3]),
        "should contain the full set"
    );

    // All singleton sets should be present
    for var in 0..4 {
        assert!(
            arena.contains(zdd, &[var]),
            "should contain singleton {{{var}}}"
        );
    }

    // All pairs should be present (C(4,2) = 6)
    let pairs: Vec<Vec<u32>> = sets.iter().filter(|s| s.len() == 2).cloned().collect();
    assert_eq!(pairs.len(), 6, "should have 6 pairs from 4 variables");
}

#[test]
fn arena_product_with_optional_chain_6_vars() {
    let mut arena = ZddArena::new();
    let mut zdd = arena.base();

    // Build a chain of 6 optional variables: 2^6 = 64 sets
    for var in 0..6 {
        zdd = arena.product_with_optional(zdd, var * 10); // non-contiguous vars
    }

    assert_eq!(arena.count_uncached(zdd), 64);

    // Iterate and verify count matches
    let sets: Vec<Vec<u32>> = arena.iter(zdd).collect();
    assert_eq!(sets.len(), 64);

    // Verify a few specific sets with non-contiguous variable IDs
    assert!(arena.contains(zdd, &[]));
    assert!(arena.contains(zdd, &[0]));
    assert!(arena.contains(zdd, &[10]));
    assert!(arena.contains(zdd, &[0, 10, 20, 30, 40, 50]));
    assert!(!arena.contains(zdd, &[1])); // 1 was never added, only 0,10,20,...
}
