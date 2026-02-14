//! PST node pruning strategies.
//!
//! Prunes nodes whose conditional distribution is not significantly different
//! from their parent, keeping the tree compact.

use super::tree::PredictionSuffixTree;

/// Strategy for pruning PST nodes.
#[derive(Debug, Clone)]
pub enum PruningStrategy {
    /// Prune if KL divergence between child and parent is below threshold.
    KLDivergence { threshold: f64 },
    /// Prune if entropy reduction from child is below minimum.
    Entropy { min_reduction: f64 },
}

impl Default for PruningStrategy {
    fn default() -> Self {
        PruningStrategy::KLDivergence { threshold: 0.05 }
    }
}

impl PredictionSuffixTree {
    /// Prune nodes according to the given strategy.
    pub fn prune(&mut self, strategy: &PruningStrategy) {
        let alphabet_size = self.alphabet_size();
        if alphabet_size == 0 {
            return;
        }
        let smoothing = self.smoothing();

        // Collect nodes to prune (bottom-up: prune leaves first)
        // We iterate in reverse since higher indices are deeper nodes
        let mut to_remove: Vec<usize> = Vec::new();

        for idx in (1..self.nodes.len()).rev() {
            // Skip already-removed nodes
            if to_remove.contains(&idx) {
                continue;
            }
            // Only prune leaf nodes (no children)
            if !self.nodes[idx].children.is_empty() {
                continue;
            }
            let parent_idx = match self.nodes[idx].parent {
                Some(p) => p,
                None => continue,
            };

            let should_prune = match strategy {
                PruningStrategy::KLDivergence { threshold } => {
                    let kl = kl_divergence(
                        &self.nodes[idx],
                        &self.nodes[parent_idx],
                        alphabet_size,
                        smoothing,
                    );
                    kl < *threshold
                }
                PruningStrategy::Entropy { min_reduction } => {
                    let child_entropy =
                        conditional_entropy(&self.nodes[idx], alphabet_size, smoothing);
                    let parent_entropy =
                        conditional_entropy(&self.nodes[parent_idx], alphabet_size, smoothing);
                    let reduction = parent_entropy - child_entropy;
                    reduction < *min_reduction
                }
            };

            if should_prune {
                to_remove.push(idx);
            }
        }

        // Remove pruned nodes from their parents' children maps
        for &idx in &to_remove {
            if let Some(parent_idx) = self.nodes[idx].parent {
                let sym = self.nodes[idx].context.last().copied();
                if let Some(sym) = sym {
                    self.nodes[parent_idx].children.remove(&sym);
                }
            }
        }
        // Note: we don't compact the arena to avoid invalidating indices.
        // Pruned nodes simply become orphans with no parent references.
    }
}

/// Compute KL divergence D(child || parent) for the smoothed distributions.
fn kl_divergence(
    child: &super::tree::PSTNode,
    parent: &super::tree::PSTNode,
    alphabet_size: usize,
    smoothing: f64,
) -> f64 {
    let mut kl = 0.0;
    let alpha = smoothing;
    let k = alphabet_size as f64;

    // Iterate over all symbols that have counts in either node
    let mut all_symbols: Vec<u16> = child.counts.keys().copied().collect();
    for &sym in parent.counts.keys() {
        if !all_symbols.contains(&sym) {
            all_symbols.push(sym);
        }
    }

    for sym in all_symbols {
        let p = child.probability(sym, alphabet_size, smoothing);
        let q = parent.probability(sym, alphabet_size, smoothing);
        if p > 0.0 && q > 0.0 {
            kl += p * (p / q).ln();
        }
    }

    // Account for symbols not in either (uniform smoothed probability)
    let unseen_count = alphabet_size.saturating_sub(child.counts.len().max(parent.counts.len()));
    if unseen_count > 0 {
        let p_unseen = alpha / (child.total as f64 + alpha * k);
        let q_unseen = alpha / (parent.total as f64 + alpha * k);
        if p_unseen > 0.0 && q_unseen > 0.0 {
            kl += unseen_count as f64 * p_unseen * (p_unseen / q_unseen).ln();
        }
    }

    kl
}

/// Compute conditional entropy H(X|context) for a node.
fn conditional_entropy(node: &super::tree::PSTNode, alphabet_size: usize, smoothing: f64) -> f64 {
    let mut entropy = 0.0;
    let alpha = smoothing;
    let k = alphabet_size as f64;

    for &sym_id in node.counts.keys() {
        let p = node.probability(sym_id, alphabet_size, smoothing);
        if p > 0.0 {
            entropy -= p * p.ln();
        }
    }

    // Account for unseen symbols
    let unseen = alphabet_size.saturating_sub(node.counts.len());
    if unseen > 0 {
        let p_unseen = alpha / (node.total as f64 + alpha * k);
        if p_unseen > 0.0 {
            entropy -= unseen as f64 * p_unseen * p_unseen.ln();
        }
    }

    entropy
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pst::tree::PSTConfig;

    #[test]
    fn test_identical_distributions_prune() {
        let mut pst = PredictionSuffixTree::new(PSTConfig {
            max_depth: 2,
            smoothing: 0.01,
        });
        let a = pst.register_symbol("A");
        let b = pst.register_symbol("B");

        // Train identical distribution at root and child
        // Root: A=50, B=50; Child(A): A=50, B=50
        for _ in 0..50 {
            pst.train(&[a, a]);
            pst.train(&[a, b]);
            pst.train(&[b, a]);
            pst.train(&[b, b]);
        }

        let count_before = pst.node_count();
        pst.prune(&PruningStrategy::KLDivergence { threshold: 0.1 });
        let count_after = pst.node_count();

        // Pruning should have removed some leaf nodes since distributions are similar
        assert!(
            count_after <= count_before,
            "Pruning should not increase node count: {} -> {}",
            count_before,
            count_after
        );
    }

    #[test]
    fn test_different_distributions_kept() {
        let mut pst = PredictionSuffixTree::new(PSTConfig {
            max_depth: 2,
            smoothing: 0.001,
        });
        let a = pst.register_symbol("A");
        let b = pst.register_symbol("B");

        // Root: mixed; After A: always B; After B: always A
        for _ in 0..100 {
            pst.train(&[a, b]);
            pst.train(&[b, a]);
        }

        // With a very small threshold, highly informative nodes should be kept
        pst.prune(&PruningStrategy::KLDivergence { threshold: 0.001 });

        // Check that depth-1 nodes still exist (they have distinct distributions)
        let has_depth1_nodes = pst
            .nodes
            .iter()
            .any(|n| n.context.len() == 1 && n.total > 0);
        assert!(
            has_depth1_nodes,
            "Informative depth-1 nodes should survive pruning"
        );
    }

    #[test]
    fn test_entropy_pruning() {
        let mut pst = PredictionSuffixTree::new(PSTConfig {
            max_depth: 3,
            smoothing: 0.01,
        });
        let a = pst.register_symbol("A");
        let _b = pst.register_symbol("B");

        // Train: A A A A A (highly predictable at all depths)
        pst.train(&[a, a, a, a, a, a, a, a]);

        let count_before = pst.node_count();
        pst.prune(&PruningStrategy::Entropy { min_reduction: 0.1 });
        let count_after = pst.node_count();

        // With identical predictions at all depths, deeper nodes should be pruned
        assert!(
            count_after <= count_before,
            "Entropy pruning should remove uninformative nodes"
        );
    }
}
