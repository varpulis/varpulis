//! ZDD Arena - Shared UniqueTable for efficient ZDD operations
//!
//! The ZddArena provides a shared table for all ZDDs, enabling:
//! - O(1) operations (no table cloning or node remapping)
//! - Structural sharing between ZDDs
//! - Persistent operation caches
//! - Centralized garbage collection
//!
//! # Example
//! ```
//! use varpulis_zdd::arena::{ZddArena, ZddHandle};
//!
//! let mut arena = ZddArena::new();
//!
//! // Start with base {∅}
//! let mut zdd = arena.base();
//!
//! // Extend with optional elements - O(|Z|) per operation, no cloning
//! zdd = arena.product_with_optional(zdd, 0);
//! zdd = arena.product_with_optional(zdd, 1);
//! zdd = arena.product_with_optional(zdd, 2);
//!
//! assert_eq!(arena.count(zdd), 8);
//! ```

use crate::refs::ZddRef;
use crate::table::UniqueTable;
use rustc_hash::{FxHashMap, FxHashSet};
use std::sync::{Arc, RwLock};

/// A lightweight handle to a ZDD within an arena.
///
/// This is just a root reference - all operations go through the arena.
/// Handles are Copy and very cheap to pass around.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ZddHandle {
    root: ZddRef,
}

impl ZddHandle {
    /// Get the root reference
    #[inline]
    pub fn root(&self) -> ZddRef {
        self.root
    }

    /// Check if this is the empty ZDD
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.root == ZddRef::Empty
    }

    /// Check if this is the base ZDD {∅}
    #[inline]
    pub fn is_base(&self) -> bool {
        self.root == ZddRef::Base
    }
}

impl Default for ZddHandle {
    fn default() -> Self {
        Self {
            root: ZddRef::Empty,
        }
    }
}

/// Shared arena for ZDD operations.
///
/// All ZDDs created through this arena share the same UniqueTable,
/// enabling O(1) operations and structural sharing.
#[derive(Clone)]
pub struct ZddArena {
    /// The shared unique table
    table: UniqueTable,

    /// Persistent cache for union operations
    union_cache: FxHashMap<(ZddRef, ZddRef), ZddRef>,

    /// Persistent cache for intersection operations
    intersection_cache: FxHashMap<(ZddRef, ZddRef), ZddRef>,

    /// Persistent cache for difference operations
    difference_cache: FxHashMap<(ZddRef, ZddRef), ZddRef>,

    /// Cached counts for ZDD roots
    count_cache: FxHashMap<ZddRef, usize>,
}

impl std::fmt::Debug for ZddArena {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZddArena")
            .field("node_count", &self.table.len())
            .field("union_cache_size", &self.union_cache.len())
            .field("count_cache_size", &self.count_cache.len())
            .finish()
    }
}

impl ZddArena {
    /// Create a new empty arena
    pub fn new() -> Self {
        Self {
            table: UniqueTable::new(),
            union_cache: FxHashMap::default(),
            intersection_cache: FxHashMap::default(),
            difference_cache: FxHashMap::default(),
            count_cache: FxHashMap::default(),
        }
    }

    /// Create an arena with preallocated capacity
    pub fn with_capacity(node_capacity: usize) -> Self {
        Self {
            table: UniqueTable::with_capacity(node_capacity),
            union_cache: FxHashMap::with_capacity_and_hasher(node_capacity, Default::default()),
            intersection_cache: FxHashMap::default(),
            difference_cache: FxHashMap::default(),
            count_cache: FxHashMap::with_capacity_and_hasher(node_capacity, Default::default()),
        }
    }

    // ========================================================================
    // ZDD Creation
    // ========================================================================

    /// Create an empty ZDD (∅ - no valid combinations)
    #[inline]
    pub fn empty(&self) -> ZddHandle {
        ZddHandle {
            root: ZddRef::Empty,
        }
    }

    /// Create the base ZDD ({∅} - one valid combination: empty set)
    #[inline]
    pub fn base(&self) -> ZddHandle {
        ZddHandle { root: ZddRef::Base }
    }

    /// Create a singleton ZDD ({{var}})
    pub fn singleton(&mut self, var: u32) -> ZddHandle {
        let root = self.table.get_or_create(var, ZddRef::Empty, ZddRef::Base);
        self.invalidate_caches();
        ZddHandle { root }
    }

    /// Create a ZDD from a single set of elements
    pub fn from_set(&mut self, elements: &[u32]) -> ZddHandle {
        if elements.is_empty() {
            return self.base();
        }

        let mut sorted: Vec<u32> = elements.to_vec();
        sorted.sort_unstable();
        sorted.dedup();

        // Build chain from highest to lowest variable
        let mut current = ZddRef::Base;
        for &var in sorted.iter().rev() {
            current = self.table.get_or_create(var, ZddRef::Empty, current);
        }

        self.invalidate_caches();
        ZddHandle { root: current }
    }

    // ========================================================================
    // Core Operations
    // ========================================================================

    /// Extend each combination with an optional new element.
    ///
    /// Semantically: S × {∅, {var}} = S ∪ {s ∪ {var} | s ∈ S}
    ///
    /// This is the key operation for Kleene pattern matching.
    /// Complexity: O(|Z|) where |Z| is the number of nodes.
    pub fn product_with_optional(&mut self, handle: ZddHandle, var: u32) -> ZddHandle {
        let mut cache: FxHashMap<ZddRef, ZddRef> = FxHashMap::default();
        let new_root = self.product_with_optional_rec(handle.root, var, &mut cache);
        self.invalidate_caches();
        ZddHandle { root: new_root }
    }

    fn product_with_optional_rec(
        &mut self,
        node: ZddRef,
        var: u32,
        cache: &mut FxHashMap<ZddRef, ZddRef>,
    ) -> ZddRef {
        // Terminal cases
        match node {
            ZddRef::Empty => return ZddRef::Empty,
            ZddRef::Base => {
                // {∅} × {∅, {var}} = {∅, {var}}
                return self.table.get_or_create(var, ZddRef::Base, ZddRef::Base);
            }
            ZddRef::Node(_) => {}
        }

        // Check cache
        if let Some(&cached) = cache.get(&node) {
            return cached;
        }

        let n = *self.table.get_node(node.node_id().unwrap());

        let result = if n.var < var {
            let new_lo = self.product_with_optional_rec(n.lo, var, cache);
            let new_hi = self.product_with_optional_rec(n.hi, var, cache);
            self.table.get_or_create(n.var, new_lo, new_hi)
        } else if n.var == var {
            // Bug fix: LO stays n.lo, HI gets union(n.lo, n.hi)
            let new_hi = self.union_refs(n.lo, n.hi);
            self.table.get_or_create(var, n.lo, new_hi)
        } else {
            // n.var > var: insert var above this node
            self.table.get_or_create(var, node, node)
        };

        cache.insert(node, result);
        result
    }

    /// Compute union of two ZDDs: S₁ ∪ S₂
    pub fn union(&mut self, a: ZddHandle, b: ZddHandle) -> ZddHandle {
        let root = self.union_refs(a.root, b.root);
        ZddHandle { root }
    }

    fn union_refs(&mut self, a: ZddRef, b: ZddRef) -> ZddRef {
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

        // Normalize for cache (union is commutative)
        let (a, b) = if a <= b { (a, b) } else { (b, a) };

        // Check persistent cache
        if let Some(&cached) = self.union_cache.get(&(a, b)) {
            return cached;
        }

        if a == ZddRef::Base && b == ZddRef::Base {
            return ZddRef::Base;
        }

        let (a_var, a_lo, a_hi) = self.get_node_info(a);
        let (b_var, b_lo, b_hi) = self.get_node_info(b);

        let result = match (a_var, b_var) {
            (Some(av), Some(bv)) => {
                if av < bv {
                    let new_lo = self.union_refs(a_lo, b);
                    self.table.get_or_create(av, new_lo, a_hi)
                } else if av > bv {
                    let new_lo = self.union_refs(a, b_lo);
                    self.table.get_or_create(bv, new_lo, b_hi)
                } else {
                    let new_lo = self.union_refs(a_lo, b_lo);
                    let new_hi = self.union_refs(a_hi, b_hi);
                    self.table.get_or_create(av, new_lo, new_hi)
                }
            }
            (Some(av), None) => {
                let new_lo = self.union_refs(a_lo, b);
                self.table.get_or_create(av, new_lo, a_hi)
            }
            (None, Some(bv)) => {
                let new_lo = self.union_refs(a, b_lo);
                self.table.get_or_create(bv, new_lo, b_hi)
            }
            (None, None) => unreachable!(),
        };

        self.union_cache.insert((a, b), result);
        result
    }

    /// Compute intersection of two ZDDs: S₁ ∩ S₂
    pub fn intersection(&mut self, a: ZddHandle, b: ZddHandle) -> ZddHandle {
        let root = self.intersection_refs(a.root, b.root);
        ZddHandle { root }
    }

    fn intersection_refs(&mut self, a: ZddRef, b: ZddRef) -> ZddRef {
        // Terminal cases
        if a == ZddRef::Empty || b == ZddRef::Empty {
            return ZddRef::Empty;
        }
        if a == b {
            return a;
        }

        // Normalize for cache (intersection is commutative)
        let (a, b) = if a <= b { (a, b) } else { (b, a) };

        // Check persistent cache
        if let Some(&cached) = self.intersection_cache.get(&(a, b)) {
            return cached;
        }

        if a == ZddRef::Base && b == ZddRef::Base {
            return ZddRef::Base;
        }

        let (a_var, a_lo, a_hi) = self.get_node_info(a);
        let (b_var, b_lo, b_hi) = self.get_node_info(b);

        let result = match (a_var, b_var) {
            (Some(av), Some(bv)) => {
                if av < bv {
                    // b doesn't have av, so sets with av can't be in b
                    self.intersection_refs(a_lo, b)
                } else if av > bv {
                    // a doesn't have bv
                    self.intersection_refs(a, b_lo)
                } else {
                    let new_lo = self.intersection_refs(a_lo, b_lo);
                    let new_hi = self.intersection_refs(a_hi, b_hi);
                    self.table.get_or_create(av, new_lo, new_hi)
                }
            }
            (Some(_), None) => {
                // b is Base (empty set)
                if b == ZddRef::Base {
                    self.intersection_refs(a_lo, ZddRef::Base)
                } else {
                    ZddRef::Empty
                }
            }
            (None, Some(_)) => {
                if a == ZddRef::Base {
                    self.intersection_refs(ZddRef::Base, b_lo)
                } else {
                    ZddRef::Empty
                }
            }
            (None, None) => {
                if a == ZddRef::Base && b == ZddRef::Base {
                    ZddRef::Base
                } else {
                    ZddRef::Empty
                }
            }
        };

        self.intersection_cache.insert((a, b), result);
        result
    }

    /// Compute difference of two ZDDs: S₁ \ S₂
    pub fn difference(&mut self, a: ZddHandle, b: ZddHandle) -> ZddHandle {
        let root = self.difference_refs(a.root, b.root);
        ZddHandle { root }
    }

    fn difference_refs(&mut self, a: ZddRef, b: ZddRef) -> ZddRef {
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

        // Check persistent cache (not commutative, no normalization)
        if let Some(&cached) = self.difference_cache.get(&(a, b)) {
            return cached;
        }

        let (a_var, a_lo, a_hi) = self.get_node_info(a);
        let (b_var, b_lo, b_hi) = self.get_node_info(b);

        let result = match (a_var, b_var) {
            (Some(av), Some(bv)) => {
                if av < bv {
                    let new_lo = self.difference_refs(a_lo, b);
                    let new_hi = self.difference_refs(a_hi, b);
                    self.table.get_or_create(av, new_lo, new_hi)
                } else if av > bv {
                    // b has bv but a doesn't, only b_lo can match a
                    self.difference_refs(a, b_lo)
                } else {
                    let new_lo = self.difference_refs(a_lo, b_lo);
                    let new_hi = self.difference_refs(a_hi, b_hi);
                    self.table.get_or_create(av, new_lo, new_hi)
                }
            }
            (Some(av), None) => {
                if b == ZddRef::Base {
                    // Remove empty set from a
                    let new_lo = self.difference_refs(a_lo, ZddRef::Base);
                    self.table.get_or_create(av, new_lo, a_hi)
                } else {
                    a
                }
            }
            (None, Some(_)) => {
                if a == ZddRef::Base {
                    // Check if empty set is in b
                    self.difference_refs(ZddRef::Base, b_lo)
                } else {
                    ZddRef::Empty
                }
            }
            (None, None) => {
                if a == ZddRef::Base && b == ZddRef::Base {
                    ZddRef::Empty
                } else {
                    a
                }
            }
        };

        self.difference_cache.insert((a, b), result);
        result
    }

    // ========================================================================
    // Query Operations
    // ========================================================================

    /// Count the number of sets in the family (with caching).
    ///
    /// Uses an internal cache for repeated calls. Takes `&mut self`.
    /// For read-only access (e.g., in SharedArena), use [`count_uncached`](Self::count_uncached).
    pub fn count(&mut self, handle: ZddHandle) -> usize {
        self.count_ref(handle.root)
    }

    fn count_ref(&mut self, r: ZddRef) -> usize {
        match r {
            ZddRef::Empty => 0,
            ZddRef::Base => 1,
            ZddRef::Node(_) => {
                if let Some(&cached) = self.count_cache.get(&r) {
                    return cached;
                }

                let node = *self.table.get_node(r.node_id().unwrap());
                let count = self.count_ref(node.lo) + self.count_ref(node.hi);
                self.count_cache.insert(r, count);
                count
            }
        }
    }

    /// Count the number of sets in the family (without caching).
    ///
    /// This method takes `&self` and can be used for read-only access.
    /// Useful for SharedArena to avoid write lock contention.
    ///
    /// For repeated counts on the same ZDD, prefer [`count`](Self::count) with caching.
    pub fn count_uncached(&self, handle: ZddHandle) -> usize {
        let mut local_cache: FxHashMap<ZddRef, usize> = FxHashMap::default();
        self.count_ref_uncached(handle.root, &mut local_cache)
    }

    fn count_ref_uncached(&self, r: ZddRef, cache: &mut FxHashMap<ZddRef, usize>) -> usize {
        match r {
            ZddRef::Empty => 0,
            ZddRef::Base => 1,
            ZddRef::Node(_) => {
                if let Some(&cached) = cache.get(&r) {
                    return cached;
                }

                let node = *self.table.get_node(r.node_id().unwrap());
                let count = self.count_ref_uncached(node.lo, cache)
                    + self.count_ref_uncached(node.hi, cache);
                cache.insert(r, count);
                count
            }
        }
    }

    /// Check if a specific combination is in the family
    ///
    /// This method allocates a temporary Vec to sort the elements.
    /// For hot paths with pre-sorted data, use [`contains_sorted`](Self::contains_sorted).
    pub fn contains(&self, handle: ZddHandle, elements: &[u32]) -> bool {
        let mut sorted: Vec<u32> = elements.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        self.contains_sorted(handle, &sorted)
    }

    /// Check if a specific combination is in the family (pre-sorted input).
    ///
    /// This is the allocation-free version of [`contains`](Self::contains).
    ///
    /// # Arguments
    /// * `handle` - The ZDD handle to check
    /// * `sorted_elements` - Elements **must be sorted in ascending order** with no duplicates
    ///
    /// # Panics
    /// Debug builds will panic if elements are not sorted.
    ///
    /// # Example
    /// ```
    /// use varpulis_zdd::ZddArena;
    ///
    /// let mut arena = ZddArena::new();
    /// let mut zdd = arena.base();
    /// zdd = arena.product_with_optional(zdd, 0);
    /// zdd = arena.product_with_optional(zdd, 1);
    ///
    /// // Pre-sorted query (no allocation)
    /// assert!(arena.contains_sorted(zdd, &[0, 1]));
    /// assert!(arena.contains_sorted(zdd, &[]));
    /// ```
    pub fn contains_sorted(&self, handle: ZddHandle, sorted_elements: &[u32]) -> bool {
        debug_assert!(
            sorted_elements.windows(2).all(|w| w[0] < w[1]),
            "elements must be sorted in ascending order with no duplicates"
        );

        let mut current = handle.root;
        let mut elem_idx = 0;

        loop {
            match current {
                ZddRef::Empty => return false,
                ZddRef::Base => return elem_idx == sorted_elements.len(),
                ZddRef::Node(id) => {
                    let node = self.table.get_node(id);

                    if elem_idx < sorted_elements.len() && node.var == sorted_elements[elem_idx] {
                        current = node.hi;
                        elem_idx += 1;
                    } else if elem_idx < sorted_elements.len()
                        && node.var > sorted_elements[elem_idx]
                    {
                        return false;
                    } else {
                        current = node.lo;
                    }
                }
            }
        }
    }

    /// Get the number of nodes in the arena
    #[inline]
    pub fn node_count(&self) -> usize {
        self.table.len()
    }

    // ========================================================================
    // Iteration
    // ========================================================================

    /// Iterate over all sets in the family
    pub fn iter(&self, handle: ZddHandle) -> ArenaIterator<'_> {
        ArenaIterator::new(self, handle)
    }

    // ========================================================================
    // Garbage Collection
    // ========================================================================

    /// Perform garbage collection: compact the table to only include reachable nodes.
    ///
    /// Returns GcStats and a mapping from old handles to new handles.
    /// IMPORTANT: After calling gc(), the provided live_handles are invalid!
    /// Use the returned remapped handles instead.
    ///
    /// # Example
    /// ```ignore
    /// let (stats, new_handles) = arena.gc(&[handle1, handle2]);
    /// handle1 = new_handles[0];
    /// handle2 = new_handles[1];
    /// ```
    pub fn gc(&mut self, live_handles: &[ZddHandle]) -> (GcStats, Vec<ZddHandle>) {
        let nodes_before = self.table.len();
        let cache_entries = self.union_cache.len()
            + self.intersection_cache.len()
            + self.difference_cache.len()
            + self.count_cache.len();

        // Phase 1: Mark all reachable nodes
        let mut marked: FxHashSet<u32> = FxHashSet::default();
        for handle in live_handles {
            self.mark_reachable(handle.root, &mut marked);
        }

        // Phase 2: Create new table with only marked nodes
        let mut new_table = UniqueTable::with_capacity(marked.len());
        let mut remap: FxHashMap<u32, ZddRef> = FxHashMap::default();

        // Remap all live nodes to new table
        let new_roots: Vec<ZddHandle> = live_handles
            .iter()
            .map(|h| ZddHandle {
                root: self.remap_to_new_table(h.root, &mut new_table, &mut remap),
            })
            .collect();

        // Phase 3: Replace table and clear caches
        let nodes_after = new_table.len();
        self.table = new_table;
        self.union_cache.clear();
        self.intersection_cache.clear();
        self.difference_cache.clear();
        self.count_cache.clear();

        (
            GcStats {
                nodes_before,
                nodes_after,
                cache_entries_cleared: cache_entries,
            },
            new_roots,
        )
    }

    /// Mark all nodes reachable from a root
    fn mark_reachable(&self, r: ZddRef, marked: &mut FxHashSet<u32>) {
        match r {
            ZddRef::Empty | ZddRef::Base => {}
            ZddRef::Node(id) => {
                if !marked.insert(id) {
                    return; // Already marked
                }
                let node = self.table.get_node(id);
                self.mark_reachable(node.lo, marked);
                self.mark_reachable(node.hi, marked);
            }
        }
    }

    /// Remap a ref to a new table
    fn remap_to_new_table(
        &self,
        r: ZddRef,
        new_table: &mut UniqueTable,
        remap: &mut FxHashMap<u32, ZddRef>,
    ) -> ZddRef {
        match r {
            ZddRef::Empty => ZddRef::Empty,
            ZddRef::Base => ZddRef::Base,
            ZddRef::Node(id) => {
                if let Some(&remapped) = remap.get(&id) {
                    return remapped;
                }
                let node = self.table.get_node(id);
                let new_lo = self.remap_to_new_table(node.lo, new_table, remap);
                let new_hi = self.remap_to_new_table(node.hi, new_table, remap);
                let new_ref = new_table.get_or_create(node.var, new_lo, new_hi);
                remap.insert(id, new_ref);
                new_ref
            }
        }
    }

    /// Perform a lightweight GC that only clears caches (no table compaction)
    pub fn gc_caches_only(&mut self) -> usize {
        let entries = self.union_cache.len()
            + self.intersection_cache.len()
            + self.difference_cache.len()
            + self.count_cache.len();

        self.union_cache.clear();
        self.intersection_cache.clear();
        self.difference_cache.clear();
        self.count_cache.clear();

        entries
    }

    // ========================================================================
    // Internal Helpers
    // ========================================================================

    #[inline]
    fn get_node_info(&self, r: ZddRef) -> (Option<u32>, ZddRef, ZddRef) {
        match r {
            ZddRef::Empty => (None, ZddRef::Empty, ZddRef::Empty),
            ZddRef::Base => (None, ZddRef::Base, ZddRef::Empty),
            ZddRef::Node(id) => {
                let node = self.table.get_node(id);
                (Some(node.var), node.lo, node.hi)
            }
        }
    }

    /// Invalidate caches after structural changes.
    ///
    /// # Why this is a no-op
    ///
    /// The UniqueTable uses monotonically increasing node IDs and never reuses them.
    /// When new nodes are created (via singleton, from_set, product_with_optional, etc.),
    /// they get fresh IDs that don't conflict with existing cache entries.
    ///
    /// Old cache entries remain valid because:
    /// - `union_cache[(a, b)] = c` - if a, b existed before, c is still correct
    /// - `intersection_cache` - same reasoning
    /// - `difference_cache` - same reasoning
    /// - `count_cache[r] = n` - counts don't change when new nodes are added elsewhere
    ///
    /// This invariant holds as long as:
    /// 1. Node IDs are never reused (guaranteed by UniqueTable appending to Vec)
    /// 2. ZddRef values are never modified after creation (they're immutable)
    ///
    /// If these invariants ever change (e.g., adding node compaction without GC),
    /// this method would need to clear the caches.
    #[inline]
    fn invalidate_caches(&mut self) {
        // No-op: see documentation above.
        // Caches remain valid due to monotonic ID allocation.
    }

    /// Get table statistics
    pub fn stats(&self) -> ArenaStats {
        let table_stats = self.table.stats();
        ArenaStats {
            node_count: table_stats.node_count,
            node_capacity: table_stats.capacity,
            union_cache_size: self.union_cache.len(),
            intersection_cache_size: self.intersection_cache.len(),
            difference_cache_size: self.difference_cache.len(),
            count_cache_size: self.count_cache.len(),
        }
    }
}

impl Default for ZddArena {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about the arena
#[derive(Debug, Clone)]
pub struct ArenaStats {
    pub node_count: usize,
    pub node_capacity: usize,
    pub union_cache_size: usize,
    pub intersection_cache_size: usize,
    pub difference_cache_size: usize,
    pub count_cache_size: usize,
}

/// Result of garbage collection
#[derive(Debug, Clone)]
pub struct GcStats {
    pub nodes_before: usize,
    pub nodes_after: usize,
    pub cache_entries_cleared: usize,
}

// ============================================================================
// Thread-Safe Arena (wrapper with RwLock)
// ============================================================================

/// Thread-safe wrapper around ZddArena
pub struct SharedArena {
    inner: Arc<RwLock<ZddArena>>,
}

impl SharedArena {
    /// Create a new shared arena
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(ZddArena::new())),
        }
    }

    /// Create a shared arena with capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Arc::new(RwLock::new(ZddArena::with_capacity(capacity))),
        }
    }

    /// Get the base ZDD
    pub fn base(&self) -> ZddHandle {
        self.inner.read().unwrap_or_else(|e| e.into_inner()).base()
    }

    /// Get the empty ZDD
    pub fn empty(&self) -> ZddHandle {
        self.inner.read().unwrap_or_else(|e| e.into_inner()).empty()
    }

    /// Create a singleton
    pub fn singleton(&self, var: u32) -> ZddHandle {
        self.inner
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .singleton(var)
    }

    /// Create from a set
    pub fn from_set(&self, elements: &[u32]) -> ZddHandle {
        self.inner
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .from_set(elements)
    }

    /// Product with optional
    pub fn product_with_optional(&self, handle: ZddHandle, var: u32) -> ZddHandle {
        self.inner
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .product_with_optional(handle, var)
    }

    /// Union
    pub fn union(&self, a: ZddHandle, b: ZddHandle) -> ZddHandle {
        self.inner
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .union(a, b)
    }

    /// Intersection
    pub fn intersection(&self, a: ZddHandle, b: ZddHandle) -> ZddHandle {
        self.inner
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .intersection(a, b)
    }

    /// Difference
    pub fn difference(&self, a: ZddHandle, b: ZddHandle) -> ZddHandle {
        self.inner
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .difference(a, b)
    }

    /// Count sets (read lock, no caching).
    ///
    /// Uses a read lock for better concurrency. Each call recomputes the count.
    /// For repeated counts on the same ZDD, consider using [`count_cached`](Self::count_cached).
    pub fn count(&self, handle: ZddHandle) -> usize {
        self.inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .count_uncached(handle)
    }

    /// Count sets (write lock, with caching).
    ///
    /// Uses the arena's internal cache for repeated calls on the same ZDD.
    /// Takes a write lock, so may block concurrent reads.
    pub fn count_cached(&self, handle: ZddHandle) -> usize {
        self.inner
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .count(handle)
    }

    /// Check containment
    pub fn contains(&self, handle: ZddHandle, elements: &[u32]) -> bool {
        self.inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .contains(handle, elements)
    }

    /// Check containment (pre-sorted, allocation-free)
    pub fn contains_sorted(&self, handle: ZddHandle, sorted_elements: &[u32]) -> bool {
        self.inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .contains_sorted(handle, sorted_elements)
    }

    /// Get node count
    pub fn node_count(&self) -> usize {
        self.inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .node_count()
    }

    /// Run garbage collection
    pub fn gc(&self, live_handles: &[ZddHandle]) -> (GcStats, Vec<ZddHandle>) {
        self.inner
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .gc(live_handles)
    }

    /// Clear caches only (no table compaction)
    pub fn gc_caches_only(&self) -> usize {
        self.inner
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .gc_caches_only()
    }

    /// Get statistics
    pub fn stats(&self) -> ArenaStats {
        self.inner.read().unwrap_or_else(|e| e.into_inner()).stats()
    }

    /// Clone the Arc for sharing
    pub fn clone_arc(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Default for SharedArena {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for SharedArena {
    fn clone(&self) -> Self {
        self.clone_arc()
    }
}

// ============================================================================
// Arena Iterator
// ============================================================================

/// Iterator over sets in a ZDD using push/pop for efficiency
pub struct ArenaIterator<'a> {
    arena: &'a ZddArena,
    stack: Vec<(ZddRef, u8)>, // (node, next_branch: 0=lo, 1=hi, 2=done)
    path: Vec<u32>,           // Current path (reused via push/pop)
}

impl<'a> ArenaIterator<'a> {
    fn new(arena: &'a ZddArena, handle: ZddHandle) -> Self {
        let mut iter = Self {
            arena,
            stack: Vec::new(),
            path: Vec::new(),
        };

        if !handle.is_empty() {
            iter.stack.push((handle.root, 0));
        }

        iter
    }
}

impl<'a> Iterator for ArenaIterator<'a> {
    type Item = Vec<u32>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((node, branch)) = self.stack.pop() {
            match node {
                ZddRef::Empty => continue,
                ZddRef::Base => {
                    // Reached a terminal - emit current path
                    return Some(self.path.clone());
                }
                ZddRef::Node(id) => {
                    let n = self.arena.table.get_node(id);

                    match branch {
                        0 => {
                            // First visit: explore LO branch (without this variable)
                            self.stack.push((node, 1));
                            self.stack.push((n.lo, 0));
                        }
                        1 => {
                            // Second visit: explore HI branch (with this variable)
                            self.path.push(n.var);
                            self.stack.push((node, 2));
                            self.stack.push((n.hi, 0));
                        }
                        _ => {
                            // Done with this node: pop the variable
                            self.path.pop();
                        }
                    }
                }
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arena_base() {
        let arena = ZddArena::new();
        let base = arena.base();
        assert!(base.is_base());
        assert!(!base.is_empty());
    }

    #[test]
    fn test_arena_empty() {
        let arena = ZddArena::new();
        let empty = arena.empty();
        assert!(empty.is_empty());
        assert!(!empty.is_base());
    }

    #[test]
    fn test_arena_singleton() {
        let mut arena = ZddArena::new();
        let s = arena.singleton(5);
        assert_eq!(arena.count(s), 1);
        assert!(arena.contains(s, &[5]));
        assert!(!arena.contains(s, &[]));
    }

    #[test]
    fn test_arena_product_with_optional() {
        let mut arena = ZddArena::new();
        let mut zdd = arena.base();

        // {∅} -> {∅, {0}}
        zdd = arena.product_with_optional(zdd, 0);
        assert_eq!(arena.count(zdd), 2);

        // -> {∅, {0}, {1}, {0,1}}
        zdd = arena.product_with_optional(zdd, 1);
        assert_eq!(arena.count(zdd), 4);

        // -> 8 combinations
        zdd = arena.product_with_optional(zdd, 2);
        assert_eq!(arena.count(zdd), 8);

        assert!(arena.contains(zdd, &[]));
        assert!(arena.contains(zdd, &[0, 1, 2]));
    }

    #[test]
    fn test_arena_product_with_optional_same_var() {
        let mut arena = ZddArena::new();
        let s = arena.singleton(0);

        // {{0}} × {∅, {0}} = {{0}}
        let result = arena.product_with_optional(s, 0);
        assert_eq!(arena.count(result), 1);
        assert!(arena.contains(result, &[0]));
        assert!(!arena.contains(result, &[]));
    }

    #[test]
    fn test_arena_union() {
        let mut arena = ZddArena::new();
        let a = arena.from_set(&[1, 2]);
        let b = arena.from_set(&[3, 4]);
        let c = arena.union(a, b);

        assert_eq!(arena.count(c), 2);
        assert!(arena.contains(c, &[1, 2]));
        assert!(arena.contains(c, &[3, 4]));
    }

    #[test]
    fn test_arena_intersection() {
        let mut arena = ZddArena::new();
        let a = arena.from_set(&[1, 2]);
        let b = arena.from_set(&[1, 2]);
        let c = arena.intersection(a, b);

        assert_eq!(arena.count(c), 1);
        assert!(arena.contains(c, &[1, 2]));
    }

    #[test]
    fn test_arena_difference() {
        let mut arena = ZddArena::new();
        let set1 = arena.from_set(&[1, 2]);
        let set2 = arena.from_set(&[3, 4]);
        let a = arena.union(set1, set2);
        let b = arena.from_set(&[1, 2]);
        let c = arena.difference(a, b);

        assert_eq!(arena.count(c), 1);
        assert!(arena.contains(c, &[3, 4]));
        assert!(!arena.contains(c, &[1, 2]));
    }

    #[test]
    fn test_arena_iterator() {
        let mut arena = ZddArena::new();
        let mut zdd = arena.base();
        zdd = arena.product_with_optional(zdd, 0);
        zdd = arena.product_with_optional(zdd, 1);

        let sets: Vec<Vec<u32>> = arena.iter(zdd).collect();
        assert_eq!(sets.len(), 4);

        // Should contain: [], [0], [1], [0,1]
        assert!(sets.contains(&vec![]));
        assert!(sets.contains(&vec![0]));
        assert!(sets.contains(&vec![1]));
        assert!(sets.contains(&vec![0, 1]));
    }

    #[test]
    fn test_arena_large_scale() {
        let mut arena = ZddArena::new();
        let mut zdd = arena.base();

        for i in 0..20 {
            zdd = arena.product_with_optional(zdd, i);
        }

        assert_eq!(arena.count(zdd), 1 << 20);
        assert!(arena.node_count() < 500);
    }

    #[test]
    fn test_contains_sorted() {
        let mut arena = ZddArena::new();
        let mut zdd = arena.base();
        zdd = arena.product_with_optional(zdd, 0);
        zdd = arena.product_with_optional(zdd, 1);
        zdd = arena.product_with_optional(zdd, 2);

        // Pre-sorted queries (allocation-free)
        assert!(arena.contains_sorted(zdd, &[]));
        assert!(arena.contains_sorted(zdd, &[0]));
        assert!(arena.contains_sorted(zdd, &[1]));
        assert!(arena.contains_sorted(zdd, &[0, 1]));
        assert!(arena.contains_sorted(zdd, &[0, 1, 2]));

        // Non-existent combinations
        assert!(!arena.contains_sorted(zdd, &[3]));
        assert!(!arena.contains_sorted(zdd, &[0, 3]));
    }

    #[test]
    fn test_shared_arena() {
        let arena = SharedArena::new();
        let base = arena.base();
        let zdd = arena.product_with_optional(base, 0);

        assert_eq!(arena.count(zdd), 2);
        assert!(arena.contains(zdd, &[]));
        assert!(arena.contains(zdd, &[0]));
    }

    #[test]
    fn test_arena_no_clone_overhead() {
        let mut arena = ZddArena::new();

        // Build two ZDDs
        let a = arena.product_with_optional(arena.base(), 0);
        let b = arena.product_with_optional(arena.base(), 1);

        // Union should not clone tables
        let node_count_before = arena.node_count();
        let _c = arena.union(a, b);

        // Should only add minimal new nodes, not double
        let node_count_after = arena.node_count();
        assert!(
            node_count_after - node_count_before < 5,
            "Should add minimal nodes, got {}",
            node_count_after - node_count_before
        );
    }

    #[test]
    fn test_arena_cache_persistence() {
        let mut arena = ZddArena::new();
        let a = arena.from_set(&[1, 2]);
        let b = arena.from_set(&[3, 4]);

        // First union
        let _c = arena.union(a, b);
        let stats1 = arena.stats();

        // Second union (should hit cache)
        let _d = arena.union(a, b);
        let stats2 = arena.stats();

        // Cache should have been used
        assert!(stats2.union_cache_size >= 1);
        assert_eq!(stats1.union_cache_size, stats2.union_cache_size);
    }

    #[test]
    fn test_arena_gc() {
        let mut arena = ZddArena::new();

        // Create several ZDDs
        let a = arena.from_set(&[1, 2]);
        let b = arena.from_set(&[3, 4]);
        let c = arena.from_set(&[5, 6]);
        let _d = arena.from_set(&[7, 8]); // This one will be garbage

        // Do some operations to fill caches
        let ab = arena.union(a, b);
        let _ = arena.union(ab, c);

        let nodes_before = arena.node_count();

        // GC keeping only 'c'
        let (stats, new_handles) = arena.gc(&[c]);
        let c = new_handles[0];

        // Should have fewer nodes now (only nodes for {5,6})
        assert!(stats.nodes_after < stats.nodes_before);
        assert!(stats.cache_entries_cleared > 0);

        // 'c' should still work
        assert_eq!(arena.count(c), 1);
        assert!(arena.contains(c, &[5, 6]));

        // Nodes should be compacted
        assert!(arena.node_count() < nodes_before);
    }

    #[test]
    fn test_arena_gc_caches_only() {
        let mut arena = ZddArena::new();
        let a = arena.from_set(&[1, 2]);
        let b = arena.from_set(&[3, 4]);

        // Fill cache
        let _ = arena.union(a, b);
        assert!(arena.stats().union_cache_size > 0);

        // Clear only caches
        let cleared = arena.gc_caches_only();
        assert!(cleared > 0);
        assert_eq!(arena.stats().union_cache_size, 0);

        // Nodes should still be there
        assert!(arena.node_count() > 0);
    }

    #[test]
    fn test_cache_consistency_after_structural_changes() {
        // Verify that caches remain valid after adding new nodes
        let mut arena = ZddArena::new();

        // Create initial ZDDs and compute union
        let a = arena.from_set(&[1, 2]);
        let b = arena.from_set(&[3, 4]);
        let ab = arena.union(a, b);
        assert_eq!(arena.count(ab), 2);

        // Remember cache state
        let cache_size = arena.stats().union_cache_size;
        assert!(cache_size > 0);

        // Add more nodes to the arena (structural change)
        let c = arena.from_set(&[5, 6]);
        let d = arena.product_with_optional(c, 7);

        // Cache should still be valid - same result for same inputs
        let ab2 = arena.union(a, b);
        assert_eq!(ab, ab2, "Cached result should be reused");
        assert_eq!(arena.count(ab2), 2);

        // Original handles should still work
        assert!(arena.contains(a, &[1, 2]));
        assert!(arena.contains(b, &[3, 4]));
        assert!(arena.contains(ab, &[1, 2]));
        assert!(arena.contains(ab, &[3, 4]));

        // New handles should also work
        assert!(arena.contains(c, &[5, 6]));
        assert!(arena.contains(d, &[5, 6]));
        assert!(arena.contains(d, &[5, 6, 7]));
    }
}
