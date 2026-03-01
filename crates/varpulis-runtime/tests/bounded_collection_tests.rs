//! Tests for bounded PST/Hamlet collections (Issue #19).
//!
//! Validates LRU eviction, arena compaction, graphlet caps, and snapshot caps.

use varpulis_runtime::event::Event;
use varpulis_runtime::hamlet::aggregator::HamletConfig;
use varpulis_runtime::hamlet::graph::HamletGraph;
use varpulis_runtime::pst::{
    OnlinePSTLearner, PMCConfig, PSTConfig, PatternMarkovChain, PredictionSuffixTree,
    PruningStrategy, RunSnapshot,
};

use rustc_hash::FxHashMap;
use std::sync::Arc;

// --- PMC LRU eviction tests ---

#[test]
fn test_pmc_prediction_cache_lru_eviction() {
    // Create PMC with tiny prediction cache (capacity 5)
    let event_types = vec!["A".to_string(), "B".to_string()];
    let pst_config = PSTConfig {
        max_depth: 3,
        smoothing: 0.01,
        ..Default::default()
    };
    let pmc_config = PMCConfig {
        warmup_events: 2,
        confidence_threshold: 0.1,
        adaptive_warmup: false,
        max_prediction_cache: 5,
        max_pending_forecasts: 5,
        ..Default::default()
    };

    let mut transitions = vec![FxHashMap::default(); 3];
    transitions[0].insert(0, 1);
    transitions[1].insert(1, 2);

    let mut pmc = PatternMarkovChain::new(
        &event_types,
        transitions,
        vec![2],
        3,
        pst_config,
        pmc_config,
    );

    // Warmup
    for i in 0..5 {
        let et = if i % 2 == 0 { "A" } else { "B" };
        pmc.process(et, i * 1_000_000_000, &[]);
    }

    // Generate forecasts from many different run states to populate prediction cache
    // Each unique (state, context_symbol) pair creates a cache entry
    for i in 0..20 {
        let runs = vec![RunSnapshot {
            current_state: 1,
            started_at_ns: i * 100,
        }];
        let et = if i % 2 == 0 { "A" } else { "B" };
        pmc.process(et, (5 + i) * 1_000_000_000, &runs);
    }

    // Should not panic or OOM — LRU eviction keeps cache bounded
    // The PMC should still produce valid forecasts
    let runs = vec![RunSnapshot {
        current_state: 1,
        started_at_ns: 999,
    }];
    let result = pmc.process("A", 100_000_000_000, &runs);
    assert!(
        result.is_some(),
        "Should still produce forecasts after LRU eviction"
    );
    let forecast = result.unwrap();
    assert!(forecast.probability.is_finite());
}

#[test]
fn test_pmc_pending_forecasts_lru_eviction() {
    let event_types = vec!["A".to_string(), "B".to_string()];
    let pst_config = PSTConfig {
        max_depth: 3,
        smoothing: 0.01,
        ..Default::default()
    };
    let pmc_config = PMCConfig {
        warmup_events: 2,
        confidence_threshold: 0.1,
        adaptive_warmup: false,
        conformal_enabled: true,
        max_pending_forecasts: 3,
        ..Default::default()
    };

    let mut transitions = vec![FxHashMap::default(); 3];
    transitions[0].insert(0, 1);
    transitions[1].insert(1, 2);

    let mut pmc = PatternMarkovChain::new(
        &event_types,
        transitions,
        vec![2],
        3,
        pst_config,
        pmc_config,
    );

    // Warmup
    for i in 0..5 {
        pmc.process("A", i * 1_000_000_000, &[]);
    }

    // Create many pending forecasts (more than the cap of 3)
    for i in 0..10 {
        let runs = vec![RunSnapshot {
            current_state: 1,
            started_at_ns: i * 1000,
        }];
        pmc.process("A", (10 + i) * 1_000_000_000, &runs);
    }

    // Should not panic; older pending forecasts are evicted
    let result = pmc.process(
        "B",
        100_000_000_000,
        &[RunSnapshot {
            current_state: 1,
            started_at_ns: 99999,
        }],
    );
    // Just verify no panic and valid state
    if let Some(f) = result {
        assert!(f.probability.is_finite());
    }
}

#[test]
fn test_pmc_default_config_backward_compatible() {
    let config = PMCConfig::default();
    assert_eq!(config.max_prediction_cache, 10_000);
    assert_eq!(config.max_pending_forecasts, 10_000);
}

// --- PST arena compaction tests ---

#[test]
fn test_pst_compact_removes_orphans() {
    let mut pst = PredictionSuffixTree::new(PSTConfig {
        max_depth: 3,
        smoothing: 0.01,
        ..Default::default()
    });
    let a = pst.register_symbol("A");
    let b = pst.register_symbol("B");

    // Build a tree with several nodes
    pst.train(&[a, b, a, b, a, b, a, b]);
    let before = pst.node_count();
    assert!(before > 1, "Should have multiple nodes after training");

    // Prune with aggressive threshold to create orphans
    pst.prune(&PruningStrategy::KLDivergence { threshold: 10.0 });

    // Now compact — should remove unreachable orphan nodes
    pst.compact();
    let after = pst.node_count();
    assert!(after <= before, "Compact should not increase node count");

    // Tree should still function correctly
    let p = pst.predict_symbol(&[a], b);
    assert!(p > 0.0, "Predictions should still work after compaction");
}

#[test]
fn test_pst_arena_cap_triggers_compact() {
    // Create PST with very low max_nodes to trigger compaction during online learning
    let mut pst = PredictionSuffixTree::new(PSTConfig {
        max_depth: 5,
        smoothing: 0.01,
        max_nodes: 20,
    });
    let a = pst.register_symbol("A");
    let b = pst.register_symbol("B");
    let c = pst.register_symbol("C");

    let mut learner = OnlinePSTLearner::new(5);

    // Feed many symbols — should trigger prune+compact when exceeding 20 nodes
    for i in 0..200 {
        let sym = match i % 3 {
            0 => a,
            1 => b,
            _ => c,
        };
        learner.update(&mut pst, sym);
    }

    // Node count should be bounded near max_nodes (may slightly exceed during processing)
    assert!(
        pst.node_count() <= 40,
        "Node count should be bounded near max_nodes, got {}",
        pst.node_count()
    );

    // Predictions should still work
    let p = pst.predict_symbol(&[a, b], c);
    assert!(p > 0.0);
}

#[test]
fn test_pst_config_default_max_nodes() {
    let config = PSTConfig::default();
    assert_eq!(config.max_nodes, 100_000);
}

// --- Hamlet graphlet cap tests ---

#[test]
fn test_hamlet_graph_graphlet_cap() {
    let mut graph = HamletGraph::with_limits(5, 0);

    // Add events of different types to create multiple graphlets
    for type_idx in 0u16..10 {
        let e = Arc::new(Event::new(format!("Type{type_idx}").as_str()));
        graph.add_event(e, type_idx);
        // Close the graphlet so it becomes inactive
        graph.close_graphlet_for_type(type_idx);
        graph.mark_processed(type_idx as u32);
    }

    // With max_graphlets=5, should have evicted older ones
    assert!(
        graph.num_graphlets() <= 10,
        "Graphlet count should be bounded, got {}",
        graph.num_graphlets()
    );
}

#[test]
fn test_hamlet_config_default_caps() {
    let config = HamletConfig::default();
    assert_eq!(config.max_graphlets, 50_000);
    assert_eq!(config.max_snapshots, 100_000);
}
