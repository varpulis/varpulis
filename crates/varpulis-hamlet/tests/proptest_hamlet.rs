#![allow(missing_docs)]

use std::sync::Arc;

use proptest::prelude::*;
use varpulis_core::Event;
use varpulis_hamlet::aggregator::{HamletAggregator, HamletConfig, QueryRegistration};
use varpulis_hamlet::greta::GretaAggregate;
use varpulis_hamlet::optimizer::{HamletOptimizer, KleeneStats, OptimizerConfig};
use varpulis_hamlet::snapshot::{PropagationCoefficients, Snapshot};
use varpulis_hamlet::template::TemplateBuilder;

// ---------------------------------------------------------------------------
// Snapshot properties
// ---------------------------------------------------------------------------

proptest! {
    /// set_value followed by get_value returns the value that was set.
    #[test]
    fn snapshot_set_get_roundtrip(
        query_id in 0u32..100,
        value in 0u64..=u64::MAX,
    ) {
        let mut snapshot = Snapshot::new(0, 0, 1);
        snapshot.set_value(query_id, value);
        prop_assert_eq!(snapshot.get_value(query_id), Some(value));
    }

    /// Setting the same query_id twice keeps only the last value (last-write-wins).
    #[test]
    fn snapshot_last_write_wins(
        query_id in 0u32..100,
        first_value in 0u64..=u64::MAX,
        second_value in 0u64..=u64::MAX,
    ) {
        let mut snapshot = Snapshot::new(0, 0, 1);
        snapshot.set_value(query_id, first_value);
        snapshot.set_value(query_id, second_value);
        prop_assert_eq!(snapshot.get_value(query_id), Some(second_value));
        // Should still only have one entry for this query
        prop_assert_eq!(snapshot.num_queries(), 1);
    }

    /// Getting a value for a query_id that was never set returns None.
    #[test]
    fn snapshot_missing_query_returns_none(
        set_id in 0u32..50,
        missing_id in 50u32..100,
        value in 0u64..=u64::MAX,
    ) {
        let mut snapshot = Snapshot::new(0, 0, 1);
        snapshot.set_value(set_id, value);
        prop_assert_eq!(snapshot.get_value(missing_id), None);
    }
}

// ---------------------------------------------------------------------------
// PropagationCoefficients properties
// ---------------------------------------------------------------------------

proptest! {
    /// resolve_count is monotonically non-decreasing with respect to snapshot_value.
    /// That is, resolve_count(a) <= resolve_count(b) whenever a <= b.
    #[test]
    fn propagation_resolve_count_monotonic(
        size in 1usize..=16,
        a in 0u64..1_000_000,
        b in 0u64..1_000_000,
    ) {
        let lo = a.min(b);
        let hi = a.max(b);

        let mut coeffs = PropagationCoefficients::new(size);
        coeffs.compute_kleene();

        let count_lo = coeffs.resolve_count(lo);
        let count_hi = coeffs.resolve_count(hi);
        prop_assert!(
            count_lo <= count_hi,
            "resolve_count({}) = {} > resolve_count({}) = {} for size={}",
            lo, count_lo, hi, count_hi, size,
        );
    }

    /// For a Kleene graphlet of size n with snapshot_value=1,
    /// resolve_count(1) should equal 2^n - 1 (for small n where no overflow occurs).
    ///
    /// Derivation: coefficients are [1, 1, 2, 4, ..., 2^(n-2)] and their sum is 2^(n-1)
    /// Wait — let's check more carefully.
    ///   coeffs[0] = 1
    ///   coeffs[1] = 1
    ///   coeffs[2] = 1+1 = 2
    ///   coeffs[3] = 1+1+2 = 4
    ///   ...coeffs[i] = 2^(i-1) for i>=1, and coeffs[0]=1.
    /// Sum = 1 + 1 + 2 + 4 + ... + 2^(n-2) = 1 + (2^(n-1) - 1) = 2^(n-1).
    /// Wait, actually 1 + sum(2^k, k=0..n-2) = 1 + (2^(n-1) - 1) = 2^(n-1).
    ///
    /// So resolve_count(1) == 2^(n-1) for n >= 1.
    /// But for snapshot_value s, it's s * 2^(n-1) (since local_sums are all 0 for Kleene).
    ///
    /// Let's verify: for n=4, coeffs = [1,1,2,4], sum = 8 = 2^3 = 2^(4-1). Correct.
    #[test]
    fn propagation_kleene_formula(n in 1usize..=30) {
        let mut coeffs = PropagationCoefficients::new(n);
        coeffs.compute_kleene();

        let expected = 1u64.checked_shl(n as u32 - 1).unwrap_or(u64::MAX);

        // With snapshot_value = 1, resolve_count should be 2^(n-1)
        let resolved = coeffs.resolve_count(1);
        // For very large n, saturating arithmetic may differ, so cap at 62 bits
        if n <= 62 {
            prop_assert_eq!(
                resolved, expected,
                "resolve_count(1) for n={}: got {}, expected {}",
                n, resolved, expected,
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Aggregator properties
// ---------------------------------------------------------------------------

/// Strategy to produce an event type name from a small alphabet.
fn event_type_strategy() -> impl Strategy<Value = &'static str> {
    prop_oneof![Just("A"), Just("B"), Just("C"),]
}

proptest! {
    /// Processing arbitrary sequences of valid events never panics.
    #[test]
    fn aggregator_process_never_panics(
        event_types in prop::collection::vec(event_type_strategy(), 1..50),
    ) {
        let mut builder = TemplateBuilder::new();
        builder.add_sequence(0, &["A", "B"]);
        builder.add_kleene(0, "B", 1);
        let template = builder.build();

        let mut aggregator = HamletAggregator::new(HamletConfig::default(), template);
        aggregator.register_query(QueryRegistration {
            id: 0,
            event_types: smallvec::smallvec![0, 1],
            kleene_types: smallvec::smallvec![1],
            aggregate: GretaAggregate::CountTrends,
        });

        for et in &event_types {
            let event = Arc::new(Event::new(*et));
            let _results = aggregator.process(event);
        }
        // If we get here without panicking, the property holds.
    }

    /// num_events() increments by exactly 1 for each recognised event type.
    /// Events with unknown types are silently ignored, so the count may stay the same.
    #[test]
    fn aggregator_num_events_increments(
        event_types in prop::collection::vec(event_type_strategy(), 1..30),
    ) {
        let mut builder = TemplateBuilder::new();
        builder.add_sequence(0, &["A", "B"]);
        builder.add_kleene(0, "B", 1);
        let template = builder.build();

        let mut aggregator = HamletAggregator::new(HamletConfig::default(), template);
        aggregator.register_query(QueryRegistration {
            id: 0,
            event_types: smallvec::smallvec![0, 1],
            kleene_types: smallvec::smallvec![1],
            aggregate: GretaAggregate::CountTrends,
        });

        let mut expected_count = 0usize;
        for et in &event_types {
            let before = aggregator.num_events();
            let event = Arc::new(Event::new(*et));
            let _results = aggregator.process(event);
            let after = aggregator.num_events();

            // "A" and "B" are known types; "C" is unknown — it won't be counted.
            if *et == "A" || *et == "B" {
                expected_count += 1;
                prop_assert_eq!(
                    after,
                    before + 1,
                    "num_events should increment by 1 for known type '{}'",
                    et,
                );
            } else {
                prop_assert_eq!(
                    after, before,
                    "num_events should not change for unknown type '{}'",
                    et,
                );
            }
        }
        prop_assert_eq!(aggregator.num_events(), expected_count);
    }

    /// flush() resets internal state: after flushing, num_events returns 0
    /// and num_graphlets returns 0.
    #[test]
    fn aggregator_flush_resets_state(
        event_types in prop::collection::vec(
            prop::sample::select(vec!["A", "B"]),
            1..20,
        ),
    ) {
        let mut builder = TemplateBuilder::new();
        builder.add_sequence(0, &["A", "B"]);
        builder.add_kleene(0, "B", 1);
        let template = builder.build();

        let mut aggregator = HamletAggregator::new(HamletConfig::default(), template);
        aggregator.register_query(QueryRegistration {
            id: 0,
            event_types: smallvec::smallvec![0, 1],
            kleene_types: smallvec::smallvec![1],
            aggregate: GretaAggregate::CountTrends,
        });

        for et in &event_types {
            let event = Arc::new(Event::new(*et));
            aggregator.process(event);
        }

        let _results = aggregator.flush();

        prop_assert_eq!(aggregator.num_events(), 0, "num_events should be 0 after flush");
        prop_assert_eq!(aggregator.num_graphlets(), 0, "num_graphlets should be 0 after flush");
    }
}

// ---------------------------------------------------------------------------
// Optimizer properties
// ---------------------------------------------------------------------------

proptest! {
    /// sharing_benefit always returns a finite f64 (no NaN, no Inf).
    #[test]
    fn optimizer_sharing_benefit_is_finite(
        num_queries in 1usize..=20,
        num_updates in 0usize..50,
        graphlet_sizes in prop::collection::vec(1usize..500, 0..50),
        snapshot_counts in prop::collection::vec(0usize..100, 0..50),
    ) {
        let mut stats = KleeneStats::new(0, num_queries);

        let len = num_updates.min(graphlet_sizes.len()).min(snapshot_counts.len());
        for i in 0..len {
            stats.update(graphlet_sizes[i], snapshot_counts[i]);
        }

        let benefit = stats.sharing_benefit();
        prop_assert!(
            benefit.is_finite(),
            "sharing_benefit() returned non-finite value: {} (num_queries={}, updates={})",
            benefit, num_queries, len,
        );
    }

    /// The same KleeneStats inputs produce the same SharingDecision (determinism).
    #[test]
    fn optimizer_decision_deterministic(
        _num_queries in 1usize..=10,
        updates in prop::collection::vec((1usize..200, 0usize..50), 1..20),
    ) {
        let make_optimizer = || {
            let config = OptimizerConfig {
                min_graphlet_size: 4,
                min_queries: 2,
                switch_threshold: 100.0,
                reevaluate_interval: 5,
                adaptive: true,
            };
            let mut optimizer = HamletOptimizer::new(config);
            optimizer.register_kleene(0, &[0, 1, 2]);

            for &(gs, sc) in &updates {
                optimizer.report_graphlet(0, gs, sc);
            }
            optimizer.decision(0)
        };

        let d1 = make_optimizer();
        let d2 = make_optimizer();
        prop_assert_eq!(d1, d2, "Same inputs should yield the same SharingDecision");
    }
}
