//! # ZDD Propagation - Count Propagation via ZDD Traversal
//!
//! Implements count propagation through the NFA-ZDD structure.
//!
//! ## Key Difference from Hamlet
//!
//! In Hamlet, count propagation is O(1) per transition (direct NFA edge).
//! In ZDD-unified, it's O(ZDD_size) per event, but with automatic sharing
//! of common substructures via canonical reduction.
//!
//! ## Propagation Modes
//!
//! - **Eager**: Propagate counts on every event (low latency, higher cost)
//! - **Lazy**: Batch events, propagate periodically (higher latency, lower cost)
//! - **Hybrid**: Eager for small ZDDs, lazy when ZDD grows large

use super::nfa_zdd::{NfaZdd, ZddVar};
use crate::greta::QueryId;
use rustc_hash::FxHashMap;
use varpulis_zdd::Zdd;

/// Propagation mode for count computation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PropagationMode {
    /// Propagate on every event
    #[default]
    Eager,
    /// Batch and propagate periodically
    Lazy { batch_size: usize },
    /// Switch based on ZDD size
    Hybrid { threshold: usize },
}

/// Count accumulator for a query
#[derive(Debug, Clone, Default)]
pub struct QueryAccumulator {
    /// Running count
    pub count: u64,
    /// Partial counts at different states
    pub state_counts: FxHashMap<ZddVar, u64>,
}

impl QueryAccumulator {
    /// Create a new accumulator
    pub fn new() -> Self {
        Self::default()
    }

    /// Add count at a state
    pub fn add_at_state(&mut self, state: ZddVar, count: u64) {
        *self.state_counts.entry(state).or_insert(0) += count;
    }

    /// Get count at a state
    pub fn count_at_state(&self, state: ZddVar) -> u64 {
        self.state_counts.get(&state).copied().unwrap_or(0)
    }

    /// Finalize count (sum all state counts)
    pub fn finalize(&mut self) {
        self.count = self.state_counts.values().sum();
    }

    /// Reset
    pub fn reset(&mut self) {
        self.count = 0;
        self.state_counts.clear();
    }
}

/// Propagator for count computation via ZDD traversal
#[derive(Debug)]
pub struct ZddPropagator {
    /// Propagation mode
    mode: PropagationMode,
    /// Accumulators per query
    accumulators: FxHashMap<QueryId, QueryAccumulator>,
    /// Event buffer for lazy mode
    event_buffer: Vec<u16>,
    /// ZDD size at last propagation
    last_zdd_size: usize,
}

impl ZddPropagator {
    /// Create a new propagator
    pub fn new(mode: PropagationMode) -> Self {
        Self {
            mode,
            accumulators: FxHashMap::default(),
            event_buffer: Vec::new(),
            last_zdd_size: 0,
        }
    }

    /// Register a query
    pub fn register_query(&mut self, query_id: QueryId) {
        self.accumulators.insert(query_id, QueryAccumulator::new());
    }

    /// Process an event through the NFA-ZDD
    pub fn process(&mut self, nfa: &mut NfaZdd, event_type: u16) -> Vec<(QueryId, u64)> {
        match self.mode {
            PropagationMode::Eager => self.process_eager(nfa, event_type),
            PropagationMode::Lazy { batch_size } => self.process_lazy(nfa, event_type, batch_size),
            PropagationMode::Hybrid { threshold } => {
                self.process_hybrid(nfa, event_type, threshold)
            }
        }
    }

    /// Eager propagation - process immediately
    fn process_eager(&mut self, nfa: &mut NfaZdd, event_type: u16) -> Vec<(QueryId, u64)> {
        let results = nfa.process_event(event_type);

        // Update accumulators
        for &(query_id, count) in &results {
            if let Some(acc) = self.accumulators.get_mut(&query_id) {
                acc.count = count;
            }
        }

        results
    }

    /// Lazy propagation - batch and process periodically
    fn process_lazy(
        &mut self,
        nfa: &mut NfaZdd,
        event_type: u16,
        batch_size: usize,
    ) -> Vec<(QueryId, u64)> {
        self.event_buffer.push(event_type);

        if self.event_buffer.len() >= batch_size {
            // Process all buffered events
            let mut results = Vec::new();
            for &et in &self.event_buffer {
                results = nfa.process_event(et);
            }
            self.event_buffer.clear();

            // Update accumulators
            for &(query_id, count) in &results {
                if let Some(acc) = self.accumulators.get_mut(&query_id) {
                    acc.count = count;
                }
            }

            results
        } else {
            Vec::new() // No results until batch is processed
        }
    }

    /// Hybrid propagation - switch based on ZDD size
    fn process_hybrid(
        &mut self,
        nfa: &mut NfaZdd,
        event_type: u16,
        threshold: usize,
    ) -> Vec<(QueryId, u64)> {
        let zdd_size = nfa.zdd_size();

        if zdd_size < threshold {
            // Small ZDD - use eager
            self.process_eager(nfa, event_type)
        } else {
            // Large ZDD - use lazy with dynamic batch size
            let batch_size = (zdd_size / threshold).max(10);
            self.process_lazy(nfa, event_type, batch_size)
        }
    }

    /// Flush any pending events (for lazy mode)
    pub fn flush(&mut self, nfa: &mut NfaZdd) -> Vec<(QueryId, u64)> {
        if self.event_buffer.is_empty() {
            return self
                .accumulators
                .iter()
                .map(|(&q, acc)| (q, acc.count))
                .collect();
        }

        // Process remaining events
        let mut results = Vec::new();
        for &et in &self.event_buffer {
            results = nfa.process_event(et);
        }
        self.event_buffer.clear();

        // Update accumulators
        for &(query_id, count) in &results {
            if let Some(acc) = self.accumulators.get_mut(&query_id) {
                acc.count = count;
            }
        }

        results
    }

    /// Get current count for a query
    pub fn count(&self, query_id: QueryId) -> u64 {
        self.accumulators
            .get(&query_id)
            .map(|acc| acc.count)
            .unwrap_or(0)
    }

    /// Reset all accumulators
    pub fn reset(&mut self) {
        for acc in self.accumulators.values_mut() {
            acc.reset();
        }
        self.event_buffer.clear();
        self.last_zdd_size = 0;
    }
}

impl Default for ZddPropagator {
    fn default() -> Self {
        Self::new(PropagationMode::default())
    }
}

/// Traversal-based count computation
///
/// Computes counts by traversing the ZDD and accumulating at each node.
/// This is more expensive than Hamlet's direct propagation but provides
/// automatic sharing via the ZDD structure.
pub struct ZddTraversal {
    memo: FxHashMap<usize, u64>,
}

impl ZddTraversal {
    /// Create a new traversal
    pub fn new() -> Self {
        Self {
            memo: FxHashMap::default(),
        }
    }

    /// Count the number of sets (trends) in the ZDD
    pub fn count_sets(&mut self, zdd: &Zdd) -> u64 {
        // The varpulis_zdd crate provides count() directly
        zdd.count() as u64
    }

    /// Count sets containing a specific variable
    pub fn count_with_var(&mut self, zdd: &Zdd, var: ZddVar) -> u64 {
        // Check if the ZDD contains this variable
        // This is a simplified implementation
        if zdd.contains(&[var]) {
            // At least one set contains the variable
            // For accurate count, we'd need ZDD traversal
            1
        } else {
            0
        }
    }

    /// Clear memoization cache
    pub fn clear(&mut self) {
        self.memo.clear();
    }
}

impl Default for ZddTraversal {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::super::nfa_zdd::NfaZddBuilder;
    use super::*;

    #[test]
    fn test_eager_propagation() {
        let mut builder = NfaZddBuilder::new();
        builder.add_sequence(0, &["A", "B"]);
        let mut nfa = builder.build();

        let mut propagator = ZddPropagator::new(PropagationMode::Eager);
        propagator.register_query(0);

        let a_idx = nfa.type_index("A").unwrap();
        let b_idx = nfa.type_index("B").unwrap();

        // Process A
        let results = propagator.process(&mut nfa, a_idx);
        assert!(results.is_empty());

        // Process B
        let results = propagator.process(&mut nfa, b_idx);
        assert_eq!(results.len(), 1);
        assert_eq!(propagator.count(0), 1);
    }

    #[test]
    fn test_lazy_propagation() {
        let mut builder = NfaZddBuilder::new();
        builder.add_sequence(0, &["A", "B"]);
        let mut nfa = builder.build();

        let mut propagator = ZddPropagator::new(PropagationMode::Lazy { batch_size: 3 });
        propagator.register_query(0);

        let a_idx = nfa.type_index("A").unwrap();
        let b_idx = nfa.type_index("B").unwrap();

        // Process A - no result yet (batched)
        let results = propagator.process(&mut nfa, a_idx);
        assert!(results.is_empty());

        // Process B - still batched
        let results = propagator.process(&mut nfa, b_idx);
        assert!(results.is_empty());

        // Flush to get results
        let results = propagator.flush(&mut nfa);
        assert!(!results.is_empty());
    }
}
