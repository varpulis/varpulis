#![allow(missing_docs)]

use proptest::prelude::*;
use varpulis_pst::{OnlinePSTLearner, PSTConfig, PredictionSuffixTree};

// ---------------------------------------------------------------------------
// PST Tree properties
// ---------------------------------------------------------------------------

proptest! {
    /// After training on any non-empty sequence, the predicted probability
    /// distribution at the root (empty context) sums to approximately 1.0.
    #[test]
    fn probability_distribution_sums_to_one(
        num_symbols in 2usize..=8,
        max_depth in 1usize..=6,
        sequence in prop::collection::vec(0u16..8, 10..200),
    ) {
        let mut pst = PredictionSuffixTree::new(PSTConfig {
            max_depth,
            smoothing: 0.01,
            ..Default::default()
        });

        // Register symbols and clamp sequence values to valid range
        for i in 0..num_symbols {
            pst.register_symbol(&format!("S{i}"));
        }
        let clamped: Vec<u16> = sequence.iter().map(|&s| s % num_symbols as u16).collect();
        pst.train(&clamped);

        // Predict at root (empty context) — sum over ALL registered symbols
        let _dist = pst.predict(&[]);
        let mut total = 0.0;
        for sym_id in 0..num_symbols as u16 {
            total += pst.predict_symbol(&[], sym_id);
        }
        // With Laplace smoothing, the sum over the full alphabet should be ~1.0
        prop_assert!((total - 1.0).abs() < 0.01,
            "Distribution sum at root should be ~1.0, got {total}");
    }

    /// predict_symbol(ctx, s) must equal the value in predict(ctx) for every
    /// symbol that appears in the distribution map.
    #[test]
    fn predict_symbol_matches_predict(
        num_symbols in 2usize..=6,
        max_depth in 1usize..=4,
        sequence in prop::collection::vec(0u16..6, 20..150),
        ctx_len in 0usize..=3,
    ) {
        let mut pst = PredictionSuffixTree::new(PSTConfig {
            max_depth,
            smoothing: 0.01,
            ..Default::default()
        });
        for i in 0..num_symbols {
            pst.register_symbol(&format!("S{i}"));
        }
        let clamped: Vec<u16> = sequence.iter().map(|&s| s % num_symbols as u16).collect();
        pst.train(&clamped);

        // Build a context from the tail of the sequence
        let actual_ctx_len = ctx_len.min(clamped.len());
        let ctx = &clamped[clamped.len() - actual_ctx_len..];

        let dist = pst.predict(ctx);
        for (&sym, &prob) in &dist {
            let single = pst.predict_symbol(ctx, sym);
            prop_assert!((prob - single).abs() < 1e-12,
                "predict({sym}) = {prob} but predict_symbol = {single}");
        }
    }

    /// All predicted probabilities must be non-negative.
    #[test]
    fn all_probabilities_non_negative(
        num_symbols in 2usize..=8,
        max_depth in 1usize..=6,
        sequence in prop::collection::vec(0u16..8, 10..200),
    ) {
        let mut pst = PredictionSuffixTree::new(PSTConfig {
            max_depth,
            smoothing: 0.01,
            ..Default::default()
        });
        for i in 0..num_symbols {
            pst.register_symbol(&format!("S{i}"));
        }
        let clamped: Vec<u16> = sequence.iter().map(|&s| s % num_symbols as u16).collect();
        pst.train(&clamped);

        for sym_id in 0..num_symbols as u16 {
            let p = pst.predict_symbol(&[], sym_id);
            prop_assert!(p >= 0.0, "Probability for symbol {sym_id} is negative: {p}");
        }
        // Also check with a non-empty context
        if clamped.len() >= 2 {
            let ctx = &clamped[..2.min(max_depth)];
            for sym_id in 0..num_symbols as u16 {
                let p = pst.predict_symbol(ctx, sym_id);
                prop_assert!(p >= 0.0,
                    "Probability for symbol {sym_id} with context is negative: {p}");
            }
        }
    }

    /// register_symbol is idempotent: calling it twice with the same name
    /// returns the same ID and does not grow the alphabet.
    #[test]
    fn register_symbol_idempotent(
        names in prop::collection::vec("[a-z]{1,8}", 1..20),
    ) {
        let mut pst = PredictionSuffixTree::new(PSTConfig::default());

        // First pass: register all names
        let mut first_ids: Vec<u16> = Vec::new();
        for name in &names {
            first_ids.push(pst.register_symbol(name));
        }
        let size_after_first = pst.alphabet_size();

        // Second pass: register again — IDs and size must be identical
        for (i, name) in names.iter().enumerate() {
            let id = pst.register_symbol(name);
            prop_assert_eq!(id, first_ids[i],
                "Second register of '{}' returned different ID: {} vs {}",
                name, id, first_ids[i]);
        }
        prop_assert_eq!(pst.alphabet_size(), size_after_first,
            "Alphabet size changed after duplicate registrations");
    }

    /// Training can only increase or maintain the node count — never decrease it.
    #[test]
    fn train_increases_or_maintains_node_count(
        num_symbols in 2usize..=6,
        max_depth in 1usize..=4,
        seq1 in prop::collection::vec(0u16..6, 10..100),
        seq2 in prop::collection::vec(0u16..6, 10..100),
    ) {
        let mut pst = PredictionSuffixTree::new(PSTConfig {
            max_depth,
            smoothing: 0.01,
            ..Default::default()
        });
        for i in 0..num_symbols {
            pst.register_symbol(&format!("S{i}"));
        }
        let s1: Vec<u16> = seq1.iter().map(|&s| s % num_symbols as u16).collect();
        let s2: Vec<u16> = seq2.iter().map(|&s| s % num_symbols as u16).collect();

        pst.train(&s1);
        let count_after_first = pst.node_count();

        pst.train(&s2);
        let count_after_second = pst.node_count();

        prop_assert!(count_after_second >= count_after_first,
            "Node count decreased after second training: {} -> {}",
            count_after_first, count_after_second);
    }

    /// compact() must preserve prediction accuracy: predictions before and
    /// after compaction should be identical (compact only removes unreachable
    /// nodes, which do not affect predictions).
    #[test]
    fn compact_preserves_predictions(
        num_symbols in 2usize..=6,
        max_depth in 1usize..=4,
        sequence in prop::collection::vec(0u16..6, 20..150),
    ) {
        let mut pst = PredictionSuffixTree::new(PSTConfig {
            max_depth,
            smoothing: 0.01,
            ..Default::default()
        });
        for i in 0..num_symbols {
            pst.register_symbol(&format!("S{i}"));
        }
        let clamped: Vec<u16> = sequence.iter().map(|&s| s % num_symbols as u16).collect();
        pst.train(&clamped);

        // Collect predictions before compaction
        let mut before: Vec<f64> = Vec::new();
        for sym_id in 0..num_symbols as u16 {
            before.push(pst.predict_symbol(&[], sym_id));
        }

        pst.compact();

        // Collect predictions after compaction
        for (sym_id, &expected) in (0..num_symbols as u16).zip(before.iter()) {
            let actual = pst.predict_symbol(&[], sym_id);
            prop_assert!((actual - expected).abs() < 1e-12,
                "compact() changed prediction for symbol {sym_id}: {expected} -> {actual}");
        }
    }
}

// ---------------------------------------------------------------------------
// OnlinePSTLearner properties
// ---------------------------------------------------------------------------

proptest! {
    /// The online learner's context buffer length must never exceed max_context.
    #[test]
    fn context_buffer_length_bounded(
        max_context in 1usize..=6,
        num_symbols in 2usize..=6,
        sequence in prop::collection::vec(0u16..6, 10..200),
    ) {
        let mut pst = PredictionSuffixTree::new(PSTConfig {
            max_depth: max_context,
            smoothing: 0.01,
            ..Default::default()
        });
        for i in 0..num_symbols {
            pst.register_symbol(&format!("S{i}"));
        }

        let mut learner = OnlinePSTLearner::new(max_context).with_prune_interval(0);

        for &s in &sequence {
            let sym = s % num_symbols as u16;
            learner.update(&mut pst, sym);
            prop_assert!(learner.current_context().len() <= max_context,
                "Context buffer length {} exceeds max_context {}",
                learner.current_context().len(), max_context);
        }
    }

    /// Online training must produce valid probability distributions (sum ~1.0
    /// over the full alphabet, all values non-negative).
    #[test]
    fn online_training_valid_distributions(
        num_symbols in 2usize..=6,
        max_depth in 1usize..=4,
        sequence in prop::collection::vec(0u16..6, 20..200),
    ) {
        let mut pst = PredictionSuffixTree::new(PSTConfig {
            max_depth,
            smoothing: 0.01,
            ..Default::default()
        });
        for i in 0..num_symbols {
            pst.register_symbol(&format!("S{i}"));
        }

        let mut learner = OnlinePSTLearner::new(max_depth).with_prune_interval(0);
        for &s in &sequence {
            let sym = s % num_symbols as u16;
            learner.update(&mut pst, sym);
        }

        // Check distribution at root
        let mut total = 0.0;
        for sym_id in 0..num_symbols as u16 {
            let p = pst.predict_symbol(&[], sym_id);
            prop_assert!(p >= 0.0, "Negative probability {p} for symbol {sym_id}");
            total += p;
        }
        prop_assert!((total - 1.0).abs() < 0.01,
            "Online-trained distribution sum at root should be ~1.0, got {total}");

        // Also check with the learner's current context
        let ctx = learner.current_context();
        let mut ctx_total = 0.0;
        for sym_id in 0..num_symbols as u16 {
            let p = pst.predict_symbol(ctx, sym_id);
            prop_assert!(p >= 0.0,
                "Negative probability {p} for symbol {sym_id} with context");
            ctx_total += p;
        }
        prop_assert!((ctx_total - 1.0).abs() < 0.01,
            "Online-trained distribution sum with context should be ~1.0, got {ctx_total}");
    }
}

// ---------------------------------------------------------------------------
// General properties
// ---------------------------------------------------------------------------

proptest! {
    /// alphabet_size must equal the number of unique registered symbols.
    #[test]
    fn alphabet_size_equals_unique_symbols(
        names in prop::collection::vec("[a-z]{1,6}", 1..30),
    ) {
        let mut pst = PredictionSuffixTree::new(PSTConfig::default());
        for name in &names {
            pst.register_symbol(name);
        }

        let unique: std::collections::HashSet<&String> = names.iter().collect();
        prop_assert_eq!(pst.alphabet_size(), unique.len(),
            "alphabet_size {} != unique symbol count {}",
            pst.alphabet_size(), unique.len());
    }

    /// After training any non-empty sequence, node_count must be >= 1
    /// (at minimum the root node exists).
    #[test]
    fn node_count_at_least_one(
        num_symbols in 2usize..=8,
        max_depth in 1usize..=6,
        sequence in prop::collection::vec(0u16..8, 10..200),
    ) {
        let mut pst = PredictionSuffixTree::new(PSTConfig {
            max_depth,
            smoothing: 0.01,
            ..Default::default()
        });
        for i in 0..num_symbols {
            pst.register_symbol(&format!("S{i}"));
        }
        let clamped: Vec<u16> = sequence.iter().map(|&s| s % num_symbols as u16).collect();
        pst.train(&clamped);

        prop_assert!(pst.node_count() >= 1,
            "Node count should be >= 1 after training, got {}", pst.node_count());
    }
}
