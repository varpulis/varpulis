//! ZDD-Kleene: Compact Kleene capture using ZDD

use super::types::{
    Predicate, SharedEvent, StackEntry, MAX_ENUMERATION_RESULTS, MAX_KLEENE_EVENTS,
};
use rustc_hash::FxHashMap;
use std::sync::Arc;
use std::time::Instant;
use varpulis_zdd::{ZddArena, ZddHandle};

/// Compact representation of Kleene captures using Zero-suppressed Decision Diagrams.
///
/// Instead of creating O(2^n) runs for n Kleene events, we use a single ZDD that
/// compactly represents all valid combinations. This reduces memory from O(2^n)
/// to O(n) nodes while preserving the ability to enumerate all combinations.
///
/// Uses ZddArena for efficient operations:
/// - No table cloning during product_with_optional
/// - Persistent operation caches
/// - Cached counts
#[derive(Debug, Clone)]
pub struct KleeneCapture {
    /// Arena for ZDD operations (provides shared table + caches)
    arena: ZddArena,
    /// Handle to the current ZDD representing all valid event combinations
    handle: ZddHandle,
    /// Events captured during Kleene matching, indexed by ZDD variable
    pub(crate) events: Vec<SharedEvent>,
    /// Aliases for captured events (parallel to events vector)
    aliases: Vec<Option<String>>,
    /// Next variable index to assign
    pub(crate) next_var: u32,
    /// SIGMOD 2014: Deferred predicate for postponed evaluation during enumeration
    pub deferred_predicate: Option<Predicate>,
    /// PERF: When true, ZDD operations are needed (inconsistent predicate).
    /// When false, we skip expensive `product_with_optional` calls.
    pub(crate) needs_zdd: bool,
}

impl KleeneCapture {
    /// Create a new empty Kleene capture.
    /// Starts with {∅} - the empty combination is always valid initially.
    pub fn new() -> Self {
        let arena = ZddArena::new();
        let handle = arena.base(); // {∅}
        Self {
            arena,
            handle,
            events: Vec::new(),
            aliases: Vec::new(),
            next_var: 0,
            deferred_predicate: None,
            needs_zdd: false,
        }
    }

    /// Extend with a new optional event.
    /// Each existing combination can now include or exclude this event.
    /// Complexity: O(|ZDD nodes|) instead of O(2^n) for naive approach.
    pub fn extend(&mut self, event: SharedEvent, alias: Option<String>) {
        let var = self.next_var;
        self.next_var += 1;

        self.events.push(event);
        self.aliases.push(alias);

        // S × {∅, {var}} = S ∪ {s ∪ {var} | s ∈ S}
        self.handle = self.arena.product_with_optional(self.handle, var);
    }

    /// PERF: Lightweight extend that only appends to vectors — no ZDD operation.
    /// Used when `needs_zdd` is false (no deferred/postponed predicate),
    /// because `complete_run()` returns a single MatchResult without enumerating.
    #[inline]
    pub fn extend_simple(&mut self, event: SharedEvent, alias: Option<String>) {
        self.next_var += 1;
        self.events.push(event);
        self.aliases.push(alias);
    }

    /// Number of valid combinations (computed in O(|nodes|), not O(2^n))
    pub fn combination_count(&mut self) -> usize {
        self.arena.count(self.handle)
    }

    /// Number of events captured
    pub fn event_count(&self) -> usize {
        self.events.len()
    }

    /// Number of ZDD nodes (for metrics)
    pub fn node_count(&self) -> usize {
        self.arena.node_count()
    }

    /// Check if this capture is empty (no events yet, only {∅})
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Iterate over all valid combinations, producing StackEntry vectors.
    /// Each combination represents one valid Kleene match sequence.
    pub fn iter_combinations(&self) -> impl Iterator<Item = Vec<StackEntry>> + '_ {
        self.arena.iter(self.handle).map(move |indices| {
            indices
                .into_iter()
                .map(|idx| {
                    let idx = idx as usize;
                    StackEntry {
                        event: Arc::clone(&self.events[idx]),
                        alias: self.aliases[idx].clone(),
                        timestamp: Instant::now(),
                    }
                })
                .collect()
        })
    }

    /// Get captured events for a single combination (by index set)
    pub fn get_combination_captured(&self, indices: &[u32]) -> FxHashMap<String, SharedEvent> {
        let mut captured = FxHashMap::default();
        for &idx in indices {
            let idx = idx as usize;
            if let Some(ref alias) = self.aliases[idx] {
                captured.insert(alias.clone(), Arc::clone(&self.events[idx]));
            }
        }
        captured
    }
}

impl Default for KleeneCapture {
    fn default() -> Self {
        Self::new()
    }
}

/// Configurable limits for Kleene closure accumulation and enumeration.
#[derive(Debug, Clone, Copy)]
pub(crate) struct KleeneLimits {
    pub(crate) max_events: u32,
    pub(crate) max_results: usize,
}

impl Default for KleeneLimits {
    fn default() -> Self {
        Self {
            max_events: MAX_KLEENE_EVENTS,
            max_results: MAX_ENUMERATION_RESULTS,
        }
    }
}
