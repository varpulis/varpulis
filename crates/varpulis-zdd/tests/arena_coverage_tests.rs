//! Coverage tests for ZddArena — targeting uncovered SharedArena and edge cases.

use varpulis_zdd::{SharedArena, ZddArena, ZddHandle};

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
    let dbg = format!("{:?}", arena);
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
