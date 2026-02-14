//! Incremental online learning for PST.
//!
//! Maintains a running context buffer and updates the PST
//! as new symbols arrive from the event stream.

use super::pruning::PruningStrategy;
use super::tree::{PredictionSuffixTree, SymbolId};

/// Online learner that incrementally updates a PST from streaming events.
pub struct OnlinePSTLearner {
    /// Rolling context buffer of recent symbols.
    context_buffer: Vec<SymbolId>,
    /// Maximum context buffer length (= PST max_depth).
    max_context: usize,
    /// How often to prune (every N updates; 0 = never).
    prune_interval: u64,
    /// Pruning strategy.
    pruning_strategy: PruningStrategy,
    /// Total updates performed.
    updates: u64,
}

impl OnlinePSTLearner {
    /// Create a new online learner.
    pub fn new(max_context: usize) -> Self {
        Self {
            context_buffer: Vec::with_capacity(max_context + 1),
            max_context,
            prune_interval: 10_000,
            pruning_strategy: PruningStrategy::default(),
            updates: 0,
        }
    }

    /// Set the pruning interval (0 to disable pruning).
    pub fn with_prune_interval(mut self, interval: u64) -> Self {
        self.prune_interval = interval;
        self
    }

    /// Set the pruning strategy.
    pub fn with_pruning_strategy(mut self, strategy: PruningStrategy) -> Self {
        self.pruning_strategy = strategy;
        self
    }

    /// Process a new symbol: update the PST with all relevant suffixes.
    pub fn update(&mut self, pst: &mut PredictionSuffixTree, symbol: SymbolId) {
        // Update counts at the root (empty context) and all context suffixes
        let max_depth = pst.max_depth();
        for ctx_len in 0..=self.context_buffer.len().min(max_depth) {
            let ctx_start = self.context_buffer.len().saturating_sub(ctx_len);
            let context = &self.context_buffer[ctx_start..];

            // Navigate to the node and update counts
            let mut current = 0; // root
            for &sym in context {
                let next = if let Some(&child_idx) = pst.nodes[current].children.get(&sym) {
                    child_idx
                } else {
                    // Create new child node
                    let mut child_ctx: smallvec::SmallVec<[SymbolId; 4]> =
                        pst.nodes[current].context.clone();
                    child_ctx.push(sym);
                    let child_idx = pst.nodes.len();
                    let child = super::tree::PSTNode::new(child_ctx, Some(current));
                    pst.nodes.push(child);
                    pst.nodes[current].children.insert(sym, child_idx);
                    child_idx
                };
                current = next;
            }
            *pst.nodes[current].counts.entry(symbol).or_insert(0) += 1;
            pst.nodes[current].total += 1;
        }

        // Append to context buffer, keeping it bounded
        self.context_buffer.push(symbol);
        if self.context_buffer.len() > self.max_context {
            self.context_buffer.remove(0);
        }

        self.updates += 1;

        // Periodic pruning
        if self.prune_interval > 0 && self.updates.is_multiple_of(self.prune_interval) {
            pst.prune(&self.pruning_strategy);
        }
    }

    /// Get the current context (most recent symbols).
    pub fn current_context(&self) -> &[SymbolId] {
        &self.context_buffer
    }

    /// Total number of updates performed.
    pub fn total_updates(&self) -> u64 {
        self.updates
    }

    /// Reset the context buffer (e.g., on window boundary).
    pub fn reset_context(&mut self) {
        self.context_buffer.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pst::tree::PSTConfig;

    #[test]
    fn test_online_learning() {
        let mut pst = PredictionSuffixTree::new(PSTConfig {
            max_depth: 3,
            smoothing: 0.01,
        });
        let a = pst.register_symbol("A");
        let b = pst.register_symbol("B");

        let mut learner = OnlinePSTLearner::new(3).with_prune_interval(0);

        // Feed alternating A, B sequence
        for _ in 0..50 {
            learner.update(&mut pst, a);
            learner.update(&mut pst, b);
        }

        // After seeing A, B should be very likely
        let p_b = pst.predict_symbol(&[a], b);
        assert!(p_b > 0.6, "P(B|A) = {} should be > 0.6", p_b);
    }

    #[test]
    fn test_context_buffer_bounded() {
        let mut pst = PredictionSuffixTree::new(PSTConfig {
            max_depth: 3,
            smoothing: 0.01,
        });
        let a = pst.register_symbol("A");

        let mut learner = OnlinePSTLearner::new(3).with_prune_interval(0);

        // Feed many symbols
        for _ in 0..100 {
            learner.update(&mut pst, a);
        }

        // Context buffer should never exceed max_context
        assert!(learner.current_context().len() <= 3);
    }
}
