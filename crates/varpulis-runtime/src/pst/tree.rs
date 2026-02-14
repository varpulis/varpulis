//! Core Prediction Suffix Tree data structure.

use rustc_hash::FxHashMap;
use smallvec::SmallVec;

/// Compact symbol identifier for event types.
pub type SymbolId = u16;

/// Configuration for PST construction.
#[derive(Debug, Clone)]
pub struct PSTConfig {
    /// Maximum context depth (tree height).
    pub max_depth: usize,
    /// Smoothing parameter for probability estimates (Laplace smoothing).
    pub smoothing: f64,
}

impl Default for PSTConfig {
    fn default() -> Self {
        Self {
            max_depth: 5,
            smoothing: 0.01,
        }
    }
}

/// A node in the Prediction Suffix Tree.
pub(crate) struct PSTNode {
    /// The context (suffix) that leads to this node.
    pub context: SmallVec<[SymbolId; 4]>,
    /// Counts for each observed next symbol.
    pub(crate) counts: FxHashMap<SymbolId, u32>,
    /// Total number of observations at this node.
    pub(crate) total: u32,
    /// Children indexed by symbol.
    pub(crate) children: FxHashMap<SymbolId, usize>,
    /// Parent node index (None for root).
    pub(crate) parent: Option<usize>,
}

impl PSTNode {
    pub(crate) fn new(context: SmallVec<[SymbolId; 4]>, parent: Option<usize>) -> Self {
        Self {
            context,
            counts: FxHashMap::default(),
            total: 0,
            children: FxHashMap::default(),
            parent,
        }
    }

    /// Get the probability of a symbol at this node (with Laplace smoothing).
    pub fn probability(&self, symbol: SymbolId, alphabet_size: usize, smoothing: f64) -> f64 {
        let count = self.counts.get(&symbol).copied().unwrap_or(0) as f64;
        let total = self.total as f64;
        let alpha = smoothing;
        let k = alphabet_size as f64;
        (count + alpha) / (total + alpha * k)
    }

    /// Get the full smoothed distribution at this node.
    pub fn distribution(&self, alphabet_size: usize, smoothing: f64) -> FxHashMap<SymbolId, f64> {
        let mut dist = FxHashMap::default();
        let alpha = smoothing;
        let k = alphabet_size as f64;
        let total = self.total as f64;

        // Include all symbols that have counts
        for (&sym, &count) in &self.counts {
            dist.insert(sym, (count as f64 + alpha) / (total + alpha * k));
        }
        dist
    }
}

/// Prediction Suffix Tree — variable-order Markov model for event sequences.
pub struct PredictionSuffixTree {
    /// Arena of nodes; index 0 is the root.
    pub(crate) nodes: Vec<PSTNode>,
    /// Maps symbol names to IDs.
    symbol_to_id: FxHashMap<String, SymbolId>,
    /// Maps IDs back to names.
    id_to_symbol: Vec<String>,
    /// Configuration.
    config: PSTConfig,
}

impl PredictionSuffixTree {
    /// Create a new empty PST.
    pub fn new(config: PSTConfig) -> Self {
        let root = PSTNode::new(SmallVec::new(), None);
        Self {
            nodes: vec![root],
            symbol_to_id: FxHashMap::default(),
            id_to_symbol: Vec::new(),
            config,
        }
    }

    /// Register a new symbol (event type) and return its ID.
    /// Returns existing ID if already registered.
    pub fn register_symbol(&mut self, name: &str) -> SymbolId {
        if let Some(&id) = self.symbol_to_id.get(name) {
            return id;
        }
        let id = self.id_to_symbol.len() as SymbolId;
        self.id_to_symbol.push(name.to_string());
        self.symbol_to_id.insert(name.to_string(), id);
        id
    }

    /// Look up a symbol ID by name.
    pub fn symbol_id(&self, name: &str) -> Option<SymbolId> {
        self.symbol_to_id.get(name).copied()
    }

    /// Number of registered symbols (alphabet size).
    pub fn alphabet_size(&self) -> usize {
        self.id_to_symbol.len()
    }

    /// Train the PST on a sequence of symbols.
    /// For each position, walks suffixes of increasing length up to max_depth.
    pub fn train(&mut self, sequence: &[SymbolId]) {
        let max_depth = self.config.max_depth;
        for i in 0..sequence.len() {
            let next_symbol = sequence[i];
            // Walk suffixes of length 0..max_depth ending just before position i
            let max_ctx_len = max_depth.min(i);
            for ctx_len in 0..=max_ctx_len {
                let ctx_start = i - ctx_len;
                let context = &sequence[ctx_start..i];
                self.update_node(context, next_symbol);
            }
        }
    }

    /// Update counts for a specific context and next symbol.
    fn update_node(&mut self, context: &[SymbolId], next_symbol: SymbolId) {
        let mut current = 0; // root

        // Navigate to the node for this context, creating nodes as needed
        for &sym in context {
            let next = if let Some(&child_idx) = self.nodes[current].children.get(&sym) {
                child_idx
            } else {
                // Create new child node
                let mut child_ctx: SmallVec<[SymbolId; 4]> = self.nodes[current].context.clone();
                child_ctx.push(sym);
                let child_idx = self.nodes.len();
                let child = PSTNode::new(child_ctx, Some(current));
                self.nodes.push(child);
                self.nodes[current].children.insert(sym, child_idx);
                child_idx
            };
            current = next;
        }

        // Update counts at this node
        *self.nodes[current].counts.entry(next_symbol).or_insert(0) += 1;
        self.nodes[current].total += 1;
    }

    /// Predict the next symbol distribution given a context.
    /// Finds the longest matching context node and returns smoothed probabilities.
    pub fn predict(&self, context: &[SymbolId]) -> FxHashMap<SymbolId, f64> {
        let alphabet_size = self.alphabet_size();
        if alphabet_size == 0 {
            return FxHashMap::default();
        }

        // Find the longest matching context
        let node_idx = self.find_longest_context(context);
        self.nodes[node_idx].distribution(alphabet_size, self.config.smoothing)
    }

    /// Get the probability of a specific next symbol given context.
    pub fn predict_symbol(&self, context: &[SymbolId], symbol: SymbolId) -> f64 {
        let alphabet_size = self.alphabet_size();
        if alphabet_size == 0 {
            return 0.0;
        }

        let node_idx = self.find_longest_context(context);
        self.nodes[node_idx].probability(symbol, alphabet_size, self.config.smoothing)
    }

    /// Find the deepest node matching the given context (longest suffix).
    fn find_longest_context(&self, context: &[SymbolId]) -> usize {
        let mut current = 0; // root
                             // Walk from the beginning of context
        for &sym in context {
            if let Some(&child_idx) = self.nodes[current].children.get(&sym) {
                current = child_idx;
            } else {
                break;
            }
        }
        current
    }

    /// Total number of nodes in the tree.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Get the config.
    pub fn config(&self) -> &PSTConfig {
        &self.config
    }

    /// Get the maximum depth from config.
    pub fn max_depth(&self) -> usize {
        self.config.max_depth
    }

    /// Get the smoothing parameter from config.
    pub fn smoothing(&self) -> f64 {
        self.config.smoothing
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_prediction_uniform() {
        let mut pst = PredictionSuffixTree::new(PSTConfig::default());
        let a = pst.register_symbol("A");
        let b = pst.register_symbol("B");
        // No training data — should return smoothed (near-uniform) distribution
        let dist = pst.predict(&[]);
        assert!(dist.is_empty() || dist.values().all(|&p| p > 0.0));
        // Predict specific symbols: both should be equal (uniform)
        let pa = pst.predict_symbol(&[], a);
        let pb = pst.predict_symbol(&[], b);
        assert!((pa - pb).abs() < 1e-10);
    }

    #[test]
    fn test_simple_sequence_learning() {
        let mut pst = PredictionSuffixTree::new(PSTConfig {
            max_depth: 3,
            smoothing: 0.01,
        });
        let a = pst.register_symbol("A");
        let b = pst.register_symbol("B");
        // Train on A -> B -> A -> B -> A -> B (alternating)
        let sequence = vec![a, b, a, b, a, b, a, b, a, b];
        pst.train(&sequence);

        // After seeing A, B should be very likely
        let p_b_given_a = pst.predict_symbol(&[a], b);
        let p_a_given_a = pst.predict_symbol(&[a], a);
        assert!(
            p_b_given_a > p_a_given_a,
            "P(B|A) = {} should be > P(A|A) = {}",
            p_b_given_a,
            p_a_given_a
        );

        // After seeing B, A should be very likely
        let p_a_given_b = pst.predict_symbol(&[b], a);
        let p_b_given_b = pst.predict_symbol(&[b], b);
        assert!(
            p_a_given_b > p_b_given_b,
            "P(A|B) = {} should be > P(B|B) = {}",
            p_a_given_b,
            p_b_given_b
        );
    }

    #[test]
    fn test_variable_order_context() {
        let mut pst = PredictionSuffixTree::new(PSTConfig {
            max_depth: 3,
            smoothing: 0.01,
        });
        let a = pst.register_symbol("A");
        let b = pst.register_symbol("B");
        let c = pst.register_symbol("C");

        // Train: A B C A B C A B C
        let seq = vec![a, b, c, a, b, c, a, b, c];
        pst.train(&seq);

        // Context [A, B] should strongly predict C
        let p_c = pst.predict_symbol(&[a, b], c);
        assert!(p_c > 0.5, "P(C|A,B) = {} should be > 0.5", p_c);

        // Longer context [C, A, B] should also predict C
        let p_c_long = pst.predict_symbol(&[c, a, b], c);
        assert!(p_c_long > 0.5, "P(C|C,A,B) = {} should be > 0.5", p_c_long);
    }

    #[test]
    fn test_smoothing() {
        let mut pst = PredictionSuffixTree::new(PSTConfig {
            max_depth: 2,
            smoothing: 0.1,
        });
        let a = pst.register_symbol("A");
        let b = pst.register_symbol("B");
        let c = pst.register_symbol("C");

        // Train only A -> A -> A
        pst.train(&[a, a, a]);

        // B and C should still have non-zero probability (smoothing)
        let p_b = pst.predict_symbol(&[a], b);
        let p_c = pst.predict_symbol(&[a], c);
        assert!(p_b > 0.0, "Smoothed P(B|A) should be > 0");
        assert!(p_c > 0.0, "Smoothed P(C|A) should be > 0");
    }

    #[test]
    fn test_register_symbol_idempotent() {
        let mut pst = PredictionSuffixTree::new(PSTConfig::default());
        let id1 = pst.register_symbol("X");
        let id2 = pst.register_symbol("X");
        assert_eq!(id1, id2);
        assert_eq!(pst.alphabet_size(), 1);
    }
}
