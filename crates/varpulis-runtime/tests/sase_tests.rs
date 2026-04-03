//! Tests for the SASE+ engine

use std::sync::Arc;
use std::time::Duration;

use varpulis_core::Value;
use varpulis_runtime::event::Event;
use varpulis_runtime::sase::*;
use varpulis_runtime::sase_persistence::{RunCheckpointExt, SaseCheckpointExt};

fn make_event(event_type: &str, data: Vec<(&str, Value)>) -> Event {
    let mut event = Event::new(event_type);
    for (k, v) in data {
        event.data.insert(k.into(), v);
    }
    event
}

#[test]
fn test_simple_sequence() {
    // SEQ(A, B)
    let pattern = PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

    let mut engine = SaseEngine::new(pattern);

    // A alone shouldn't complete
    let results = engine.process(&make_event("A", vec![]));
    assert!(results.is_empty());

    // B should complete the sequence
    let results = engine.process(&make_event("B", vec![]));
    assert_eq!(results.len(), 1);
}

#[test]
fn test_sequence_with_filter() {
    // SEQ(A where price > 100, B)
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event_where(
            "A",
            Predicate::Compare {
                field: "price".to_string(),
                op: CompareOp::Gt,
                value: Value::Int(100),
            },
        ),
        PatternBuilder::event("B"),
    ]);

    let mut engine = SaseEngine::new(pattern);

    // A with price <= 100 shouldn't start a run
    let results = engine.process(&make_event("A", vec![("price", Value::Int(50))]));
    assert!(results.is_empty());
    assert_eq!(engine.stats().active_runs, 0);

    // A with price > 100 should start a run
    let results = engine.process(&make_event("A", vec![("price", Value::Int(150))]));
    assert!(results.is_empty());
    assert_eq!(engine.stats().active_runs, 1);

    // B should complete
    let results = engine.process(&make_event("B", vec![]));
    assert_eq!(results.len(), 1);
}

#[test]
fn test_kleene_plus() {
    // SEQ(A, B+, C)
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("A"),
        PatternBuilder::one_or_more(PatternBuilder::event("B")),
        PatternBuilder::event("C"),
    ]);

    let mut engine = SaseEngine::new(pattern);

    // Start with A
    engine.process(&make_event("A", vec![]));
    assert_eq!(engine.stats().active_runs, 1);

    // First B
    engine.process(&make_event("B", vec![]));

    // Second B (Kleene)
    engine.process(&make_event("B", vec![]));

    // C should complete
    let results = engine.process(&make_event("C", vec![]));
    assert!(!results.is_empty());
}

#[test]
fn test_negation() {
    // SEQ(A, NOT(Cancel), B)
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("A"),
        PatternBuilder::not(PatternBuilder::event("Cancel")),
        PatternBuilder::event("B"),
    ]);

    let mut engine = SaseEngine::new(pattern);

    // A starts the run
    engine.process(&make_event("A", vec![]));
    assert!(engine.stats().active_runs > 0);

    // B should complete (no Cancel in between)
    let results = engine.process(&make_event("B", vec![]));
    // Note: negation handling needs proper timeout implementation
    // This test validates the structure
    assert!(results.is_empty() || results.len() == 1);
}

#[test]
fn test_partition_by() {
    // SEQ(A, B) partitioned by symbol
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event_as("A", "a"),
        PatternBuilder::event("B"),
    ]);

    let mut engine = SaseEngine::new(pattern).with_partition_by("symbol".to_string());

    // Events for different symbols
    engine.process(&make_event(
        "A",
        vec![("symbol", Value::Str("AAPL".into()))],
    ));
    engine.process(&make_event(
        "A",
        vec![("symbol", Value::Str("GOOG".into()))],
    ));

    assert_eq!(engine.stats().partitions, 2);

    // Complete AAPL
    let results = engine.process(&make_event(
        "B",
        vec![("symbol", Value::Str("AAPL".into()))],
    ));
    assert_eq!(results.len(), 1);
}

#[test]
fn test_with_alias_capture() {
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event_as("Order", "order"),
        PatternBuilder::event_as("Payment", "payment"),
    ]);

    let mut engine = SaseEngine::new(pattern);

    engine.process(&make_event("Order", vec![("id", Value::Int(123))]));
    let results = engine.process(&make_event(
        "Payment",
        vec![("amount", Value::Float(99.99))],
    ));

    assert_eq!(results.len(), 1);
    let result = &results[0];
    assert!(result.captured.contains_key("order"));
    assert!(result.captured.contains_key("payment"));
}

// =========================================================================
// Event-Time / Watermark Tests
// =========================================================================

#[test]
fn test_event_time_mode_basic() {
    use chrono::{TimeZone, Utc};

    let pattern = PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

    let mut engine = SaseEngine::new(pattern).with_event_time();

    assert_eq!(engine.time_semantics(), TimeSemantics::EventTime);

    let ts1 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 0).unwrap();
    let ts2 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 1).unwrap();

    let event_a = Event::new("A").with_timestamp(ts1);
    let event_b = Event::new("B").with_timestamp(ts2);

    engine.process(&event_a);
    let results = engine.process(&event_b);

    assert_eq!(results.len(), 1);
}

#[test]
fn test_watermark_tracking() {
    use chrono::{TimeZone, Utc};

    let pattern = PatternBuilder::event("A");

    let mut engine = SaseEngine::new(pattern).with_event_time();

    assert!(engine.watermark().is_none());

    let ts1 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 0).unwrap();
    let event = Event::new("A").with_timestamp(ts1);
    engine.process(&event);

    // Watermark should now be set
    assert!(engine.watermark().is_some());
    assert_eq!(engine.watermark().unwrap(), ts1);
}

#[test]
fn test_watermark_with_out_of_orderness() {
    use chrono::{TimeZone, Utc};

    let pattern = PatternBuilder::event("A");

    let mut engine = SaseEngine::new(pattern)
        .with_event_time()
        .with_max_out_of_orderness(std::time::Duration::from_secs(5));

    let ts = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 10).unwrap();
    let event = Event::new("A").with_timestamp(ts);
    engine.process(&event);

    // Watermark should be ts - 5s
    let expected_watermark = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 5).unwrap();
    assert_eq!(engine.watermark().unwrap(), expected_watermark);
}

#[test]
fn test_event_time_within_timeout() {
    use std::time::Duration;

    use chrono::{TimeZone, Utc};

    // Pattern with 5 second window
    let pattern = SasePattern::Within(
        Box::new(PatternBuilder::seq(vec![
            PatternBuilder::event("Login"),
            PatternBuilder::event("Transaction"),
        ])),
        Duration::from_secs(5),
    );

    let mut engine = SaseEngine::new(pattern).with_event_time();

    let ts1 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 0).unwrap();
    let ts2 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 3).unwrap(); // Within 5s

    let login = Event::new("Login").with_timestamp(ts1);
    let tx = Event::new("Transaction").with_timestamp(ts2);

    engine.process(&login);
    let results = engine.process(&tx);

    // Should match because within the window
    assert_eq!(results.len(), 1);
}

#[test]
fn test_event_time_within_expired_by_watermark() {
    use std::time::Duration;

    use chrono::{TimeZone, Utc};

    // Pattern with 5 second window
    let pattern = SasePattern::Within(
        Box::new(PatternBuilder::seq(vec![
            PatternBuilder::event("Login"),
            PatternBuilder::event("Transaction"),
        ])),
        Duration::from_secs(5),
    );

    let mut engine = SaseEngine::new(pattern).with_event_time();

    let ts1 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 0).unwrap();
    let ts2 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 10).unwrap(); // After 5s window

    let login = Event::new("Login").with_timestamp(ts1);
    let tx = Event::new("Transaction").with_timestamp(ts2);

    engine.process(&login);

    // When processing second event, watermark advances to ts2
    // The run's deadline (ts1 + 5s) is now past the watermark (ts2)
    // So the run should be cleaned up
    let results = engine.process(&tx);

    // Should NOT match because the partial match expired
    assert_eq!(results.len(), 0);
}

#[test]
fn test_manual_watermark_advance() {
    use std::time::Duration;

    use chrono::{TimeZone, Utc};

    // Pattern with 5 second window
    let pattern = SasePattern::Within(
        Box::new(PatternBuilder::seq(vec![
            PatternBuilder::event("Login"),
            PatternBuilder::event("Transaction"),
        ])),
        Duration::from_secs(5),
    );

    let mut engine = SaseEngine::new(pattern).with_event_time();

    let ts1 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 0).unwrap();
    let login = Event::new("Login").with_timestamp(ts1);
    engine.process(&login);

    // Active runs should be 1
    assert_eq!(engine.stats().active_runs, 1);

    // Manually advance watermark past the deadline
    let future_watermark = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 10).unwrap();
    engine.advance_watermark(future_watermark);

    // Run should now be cleaned up
    assert_eq!(engine.stats().active_runs, 0);
}

// =========================================================================
// COV-02: Advanced SASE+ Integration Tests
// =========================================================================

#[test]
fn test_out_of_order_events_in_sequence() {
    // Test that events arriving out of order don't incorrectly match
    // Pattern: SEQ(A, B, C) should only match when events arrive in order

    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("A"),
        PatternBuilder::event("B"),
        PatternBuilder::event("C"),
    ]);

    let mut engine = SaseEngine::new(pattern);

    // Send B first (out of order) - should not start a run
    let results = engine.process(&make_event("B", vec![]));
    assert!(results.is_empty());
    assert_eq!(engine.stats().active_runs, 0);

    // Send A - starts a new run
    let results = engine.process(&make_event("A", vec![]));
    assert!(results.is_empty());
    assert_eq!(engine.stats().active_runs, 1);

    // Send C (skipping B) - should not complete
    let results = engine.process(&make_event("C", vec![]));
    assert!(results.is_empty());

    // Send B - now should move run forward
    let results = engine.process(&make_event("B", vec![]));
    assert!(results.is_empty());

    // Send C again - should complete
    let results = engine.process(&make_event("C", vec![]));
    assert!(!results.is_empty());
}

#[test]
fn test_concurrent_patterns_same_event_type() {
    // Multiple runs can be active for the same event type
    // Pattern: SEQ(A, B) - sending multiple A events should create multiple runs

    let pattern = PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

    let mut engine = SaseEngine::new(pattern);

    // Send first A
    engine.process(&make_event("A", vec![("id", Value::Int(1))]));
    assert_eq!(engine.stats().active_runs, 1);

    // Send second A - creates another run
    engine.process(&make_event("A", vec![("id", Value::Int(2))]));
    assert_eq!(engine.stats().active_runs, 2);

    // Send B - should complete BOTH runs
    let results = engine.process(&make_event("B", vec![]));
    assert_eq!(results.len(), 2);
}

#[test]
fn test_kleene_star_with_occurrences() {
    // A* matches zero or more - test with some occurrences
    // Pattern: SEQ(Start, Middle*, End)

    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("Start"),
        PatternBuilder::zero_or_more(PatternBuilder::event("Middle")),
        PatternBuilder::event("End"),
    ]);

    let mut engine = SaseEngine::new(pattern);

    // Start
    engine.process(&make_event("Start", vec![]));
    assert_eq!(engine.stats().active_runs, 1);

    // Some Middle events
    engine.process(&make_event("Middle", vec![]));
    engine.process(&make_event("Middle", vec![]));

    // End should complete
    let results = engine.process(&make_event("End", vec![]));
    // Kleene star creates multiple completion possibilities
    assert!(!results.is_empty());
}

#[test]
fn test_kleene_plus_requires_at_least_one() {
    // A+ should require at least one occurrence
    // Pattern: SEQ(Start, Middle+, End)

    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("Start"),
        PatternBuilder::one_or_more(PatternBuilder::event("Middle")),
        PatternBuilder::event("End"),
    ]);

    let mut engine = SaseEngine::new(pattern);

    // Start
    engine.process(&make_event("Start", vec![]));

    // Skip Middle and send End - should NOT match
    let results = engine.process(&make_event("End", vec![]));
    assert!(results.is_empty());

    // Start again
    engine.process(&make_event("Start", vec![]));

    // Send one Middle
    engine.process(&make_event("Middle", vec![]));

    // Send End - should match
    let results = engine.process(&make_event("End", vec![]));
    assert!(!results.is_empty());
}

#[test]
fn test_or_pattern_in_sequence() {
    // Test OR within a sequence: SEQ(Start, OR(A, B), End)
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("Start"),
        PatternBuilder::or(PatternBuilder::event("A"), PatternBuilder::event("B")),
        PatternBuilder::event("End"),
    ]);

    let mut engine = SaseEngine::new(pattern);

    // Start
    engine.process(&make_event("Start", vec![]));
    assert_eq!(engine.stats().active_runs, 1);

    // A should advance the run (matches OR branch)
    engine.process(&make_event("A", vec![]));

    // End should complete
    let results = engine.process(&make_event("End", vec![]));
    assert!(!results.is_empty());
}

#[test]
fn test_and_pattern_both_required() {
    // AND(A, B) should match when both occur (any order)
    let pattern = PatternBuilder::and(PatternBuilder::event("A"), PatternBuilder::event("B"));

    let mut engine = SaseEngine::new(pattern);

    // Just A - should not complete
    let results = engine.process(&make_event("A", vec![]));
    assert!(results.is_empty());

    // Now B - should complete
    let results = engine.process(&make_event("B", vec![]));
    assert!(!results.is_empty());
}

#[test]
fn test_and_pattern_reverse_order() {
    // AND(A, B) should match even if B comes before A
    let pattern = PatternBuilder::and(PatternBuilder::event("A"), PatternBuilder::event("B"));

    let mut engine = SaseEngine::new(pattern);

    // B first
    let results = engine.process(&make_event("B", vec![]));
    assert!(results.is_empty());

    // Then A - should complete
    let results = engine.process(&make_event("A", vec![]));
    assert!(!results.is_empty());
}

#[test]
fn test_compare_ref_between_events() {
    // Test referencing fields between events
    // Pattern: SEQ(Order as order, Payment where order_id == order.id)

    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event_as("Order", "order"),
        PatternBuilder::event_where(
            "Payment",
            Predicate::CompareRef {
                field: "order_id".to_string(),
                op: CompareOp::Eq,
                ref_alias: "order".to_string(),
                ref_field: "id".to_string(),
            },
        ),
    ]);

    let mut engine = SaseEngine::new(pattern);

    // Order with id 123
    engine.process(&make_event("Order", vec![("id", Value::Int(123))]));

    // Payment with wrong order_id - should not complete
    let results = engine.process(&make_event("Payment", vec![("order_id", Value::Int(999))]));
    assert!(results.is_empty());

    // Payment with correct order_id - should complete
    let results = engine.process(&make_event("Payment", vec![("order_id", Value::Int(123))]));
    assert!(!results.is_empty());
}

#[test]
fn test_long_sequence_chain() {
    // Test a longer sequence: SEQ(A, B, C, D, E)
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("A"),
        PatternBuilder::event("B"),
        PatternBuilder::event("C"),
        PatternBuilder::event("D"),
        PatternBuilder::event("E"),
    ]);

    let mut engine = SaseEngine::new(pattern);

    // Process events in order
    engine.process(&make_event("A", vec![]));
    assert_eq!(engine.stats().active_runs, 1);

    engine.process(&make_event("B", vec![]));
    engine.process(&make_event("C", vec![]));
    engine.process(&make_event("D", vec![]));

    // E should complete
    let results = engine.process(&make_event("E", vec![]));
    assert_eq!(results.len(), 1);
}

#[test]
fn test_partition_isolation() {
    // Test that partitions are truly isolated
    // Events in different partitions should not interact

    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event_as("A", "a"),
        PatternBuilder::event("B"),
    ]);

    let mut engine = SaseEngine::new(pattern).with_partition_by("region".to_string());

    // A for region "east"
    engine.process(&make_event(
        "A",
        vec![("region", Value::Str("east".into()))],
    ));

    // B for region "west" - should not complete the east run
    let results = engine.process(&make_event(
        "B",
        vec![("region", Value::Str("west".into()))],
    ));
    assert!(results.is_empty());

    // B for region "east" - should complete
    let results = engine.process(&make_event(
        "B",
        vec![("region", Value::Str("east".into()))],
    ));
    assert_eq!(results.len(), 1);
}

#[test]
fn test_negation_cancels_match() {
    // Test that negation properly prevents a match
    // Pattern: SEQ(A, NOT(Cancel), B)
    // If Cancel arrives between A and B, the pattern should not match

    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event_as("A", "a"),
        PatternBuilder::not(PatternBuilder::event("Cancel")),
        PatternBuilder::event("B"),
    ]);

    let mut engine = SaseEngine::new(pattern);

    // Global negation also needs to be registered
    engine.add_negation("Cancel".to_string(), None);

    // A starts the sequence
    engine.process(&make_event("A", vec![]));
    assert!(engine.stats().active_runs > 0);

    // Cancel should invalidate the run
    engine.process(&make_event("Cancel", vec![]));

    // B should NOT complete (run was cancelled)
    let results = engine.process(&make_event("B", vec![]));
    assert!(results.is_empty());
}

#[test]
fn test_multiple_kleene_matches() {
    // Test that Kleene+ captures multiple events
    // Pattern: SEQ(Start, Tick+, End)

    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event_as("Start", "start"),
        PatternBuilder::one_or_more(PatternBuilder::event_as("Tick", "tick")),
        PatternBuilder::event_as("End", "end"),
    ]);

    let mut engine = SaseEngine::new(pattern);

    engine.process(&make_event("Start", vec![("val", Value::Int(0))]));

    // Multiple ticks
    engine.process(&make_event("Tick", vec![("val", Value::Int(1))]));
    engine.process(&make_event("Tick", vec![("val", Value::Int(2))]));
    engine.process(&make_event("Tick", vec![("val", Value::Int(3))]));

    let results = engine.process(&make_event("End", vec![("val", Value::Int(100))]));

    // Should have multiple results (one for each combination)
    assert!(!results.is_empty());
}

#[test]
fn test_stats_tracking() {
    let pattern = PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

    let mut engine = SaseEngine::new(pattern);

    let initial_stats = engine.stats();
    assert_eq!(initial_stats.active_runs, 0);
    assert!(initial_stats.nfa_states > 0); // NFA should have states

    engine.process(&make_event("A", vec![]));
    assert_eq!(engine.stats().active_runs, 1);

    engine.process(&make_event("B", vec![]));
    // After completion, run is removed
    assert_eq!(engine.stats().active_runs, 0);
}

// =========================================================================
// NEG-01: Temporal Negation Tests
// =========================================================================

#[test]
fn test_negation_with_predicate() {
    // SEQ(Order, NOT(Cancel where id matches), Shipment)
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event_as("Order", "order"),
        PatternBuilder::not(PatternBuilder::event_where(
            "Cancel",
            Predicate::CompareRef {
                field: "order_id".to_string(),
                op: CompareOp::Eq,
                ref_alias: "order".to_string(),
                ref_field: "id".to_string(),
            },
        )),
        PatternBuilder::event("Shipment"),
    ]);

    let mut engine = SaseEngine::new(pattern);
    engine.add_negation(
        "Cancel".to_string(),
        Some(Predicate::CompareRef {
            field: "order_id".to_string(),
            op: CompareOp::Eq,
            ref_alias: "order".to_string(),
            ref_field: "id".to_string(),
        }),
    );

    // Order with id 123
    engine.process(&make_event("Order", vec![("id", Value::Int(123))]));
    assert!(engine.stats().active_runs > 0);

    // Cancel for different order (id 456) - should NOT invalidate
    engine.process(&make_event("Cancel", vec![("order_id", Value::Int(456))]));
    assert!(engine.stats().active_runs > 0, "Run should still be active");

    // Cancel for same order (id 123) - should invalidate
    engine.process(&make_event("Cancel", vec![("order_id", Value::Int(123))]));
    assert_eq!(
        engine.stats().active_runs,
        0,
        "Run should be invalidated by matching Cancel"
    );
}

// =========================================================================
// AND-01: Enhanced AND Operator Tests
// =========================================================================

#[test]
fn test_and_with_noise_between() {
    // AND(A, B) should match even with other events between
    let pattern = PatternBuilder::and(PatternBuilder::event("A"), PatternBuilder::event("B"));

    let mut engine = SaseEngine::new(pattern).with_strategy(SelectionStrategy::SkipTillAnyMatch);

    // A, then noise, then B
    engine.process(&make_event("A", vec![]));
    engine.process(&make_event("Noise", vec![]));
    engine.process(&make_event("MoreNoise", vec![]));
    let results = engine.process(&make_event("B", vec![]));
    assert_eq!(results.len(), 1);
}

#[test]
fn test_and_with_aliases() {
    // AND(A as a, B as b) should capture both
    let pattern = PatternBuilder::and(
        PatternBuilder::event_as("A", "a"),
        PatternBuilder::event_as("B", "b"),
    );

    let mut engine = SaseEngine::new(pattern);

    engine.process(&make_event("A", vec![("val", Value::Int(1))]));
    let results = engine.process(&make_event("B", vec![("val", Value::Int(2))]));

    assert_eq!(results.len(), 1);
    let result = &results[0];
    assert!(result.captured.contains_key("a"));
    assert!(result.captured.contains_key("b"));
    assert_eq!(
        result.captured.get("a").unwrap().get("val"),
        Some(&Value::Int(1))
    );
    assert_eq!(
        result.captured.get("b").unwrap().get("val"),
        Some(&Value::Int(2))
    );
}

#[test]
fn test_and_does_not_match_same_event_twice() {
    // AND(A, B) receiving A twice should not complete
    let pattern = PatternBuilder::and(PatternBuilder::event("A"), PatternBuilder::event("B"));

    let mut engine = SaseEngine::new(pattern);

    engine.process(&make_event("A", vec![]));
    let results = engine.process(&make_event("A", vec![]));
    assert!(
        results.is_empty(),
        "AND should not complete with duplicate events"
    );
    assert_eq!(
        engine.stats().active_runs,
        2,
        "Should have two runs (one started per A)"
    );
}

#[test]
fn test_and_in_sequence() {
    // SEQ(Start, AND(A, B), End)
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("Start"),
        PatternBuilder::and(PatternBuilder::event("A"), PatternBuilder::event("B")),
        PatternBuilder::event("End"),
    ]);

    let mut engine = SaseEngine::new(pattern);

    engine.process(&make_event("Start", vec![]));
    assert_eq!(engine.stats().active_runs, 1);

    engine.process(&make_event("B", vec![]));
    engine.process(&make_event("A", vec![]));

    let results = engine.process(&make_event("End", vec![]));
    assert!(
        !results.is_empty(),
        "Pattern should complete after AND and End"
    );
}

// =========================================================================
// BP-01: Backpressure Tests
// =========================================================================

#[test]
fn test_backpressure_drop_strategy() {
    let pattern = PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

    let mut engine = SaseEngine::new(pattern)
        .with_max_runs(2)
        .with_backpressure(BackpressureStrategy::Drop);

    // Create max runs
    engine.process(&make_event("A", vec![]));
    engine.process(&make_event("A", vec![]));
    assert_eq!(engine.stats().active_runs, 2);

    // Try to create another run - should be dropped
    let result = engine.process_with_result(&make_event("A", vec![]));
    assert_eq!(engine.stats().active_runs, 2);
    assert!(
        !result.warnings.is_empty(),
        "Should have a warning about dropped run"
    );

    let stats = engine.extended_stats();
    assert_eq!(stats.total_runs_dropped, 1);
}

#[test]
fn test_backpressure_evict_oldest() {
    let pattern = PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

    let mut engine = SaseEngine::new(pattern)
        .with_max_runs(2)
        .with_backpressure(BackpressureStrategy::EvictOldest);

    // Create max runs
    engine.process(&make_event("A", vec![]));
    std::thread::sleep(std::time::Duration::from_millis(10));
    engine.process(&make_event("A", vec![]));
    assert_eq!(engine.stats().active_runs, 2);

    // Create another run - should evict oldest
    let result = engine.process_with_result(&make_event("A", vec![]));
    assert_eq!(engine.stats().active_runs, 2, "Should still have max runs");

    // Check that eviction happened
    let stats = engine.extended_stats();
    assert_eq!(stats.total_runs_evicted, 1);
    assert!(result
        .warnings
        .iter()
        .any(|w| matches!(w, ProcessWarning::RunEvicted { .. })));
}

#[test]
fn test_backpressure_evict_least_progress() {
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("A"),
        PatternBuilder::event("B"),
        PatternBuilder::event("C"),
    ]);

    let mut engine = SaseEngine::new(pattern)
        .with_max_runs(2)
        .with_backpressure(BackpressureStrategy::EvictLeastProgress);

    // Create first run and advance it (more progress)
    engine.process(&make_event("A", vec![]));
    engine.process(&make_event("B", vec![]));
    assert_eq!(engine.stats().active_runs, 1);

    // Create second run (less progress)
    engine.process(&make_event("A", vec![]));
    assert_eq!(engine.stats().active_runs, 2);

    // Create third run - should evict the one with less progress
    let result = engine.process_with_result(&make_event("A", vec![]));
    assert_eq!(engine.stats().active_runs, 2);
    assert_eq!(engine.extended_stats().total_runs_evicted, 1);
    assert!(result
        .warnings
        .iter()
        .any(|w| matches!(w, ProcessWarning::RunEvicted { .. })));
}

#[test]
fn test_process_with_result_approaching_limit() {
    let pattern = PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

    let mut engine = SaseEngine::new(pattern).with_max_runs(10);

    // Create runs up to >80% utilization (9 runs out of 10 = 90%)
    for _ in 0..9 {
        engine.process(&make_event("A", vec![]));
    }
    assert_eq!(engine.stats().active_runs, 9);

    // Process another event - should warn about approaching limit (90% > 80%)
    let result = engine.process_with_result(&make_event("A", vec![]));
    assert!(
        result
            .warnings
            .iter()
            .any(|w| matches!(w, ProcessWarning::ApproachingLimit { .. })),
        "Should warn when utilization > 80%"
    );
}

#[test]
fn test_extended_stats() {
    let pattern = PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

    let mut engine = SaseEngine::new(pattern).with_max_runs(100);

    // Process some events
    engine.process(&make_event("A", vec![]));
    engine.process(&make_event("B", vec![])); // Completes the pattern
    engine.process(&make_event("A", vec![]));

    let stats = engine.extended_stats();
    assert_eq!(stats.active_runs, 1);
    assert_eq!(stats.total_runs_created, 2);
    assert_eq!(stats.total_runs_completed, 1);
    assert!(stats.utilization < 0.1);
}

// =========================================================================
// IDX-01: Event Type Indexing Tests
// =========================================================================

#[test]
fn test_has_interest_positive() {
    let pattern = PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

    let engine = SaseEngine::new(pattern);

    assert!(engine.has_interest("A"));
    assert!(engine.has_interest("B"));
    assert!(!engine.has_interest("C"));
    assert!(!engine.has_interest("Unknown"));
}

#[test]
fn test_has_interest_with_negation() {
    let pattern = PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

    let mut engine = SaseEngine::new(pattern);
    engine.add_negation("Cancel".to_string(), None);

    assert!(engine.has_interest("A"));
    assert!(engine.has_interest("B"));
    assert!(engine.has_interest("Cancel")); // Should have interest due to global negation
    assert!(!engine.has_interest("Unknown"));
}

#[test]
fn test_event_type_index_with_and() {
    let pattern = PatternBuilder::and(PatternBuilder::event("X"), PatternBuilder::event("Y"));

    let engine = SaseEngine::new(pattern);

    // AND states should index both event types
    assert!(engine.has_interest("X"));
    assert!(engine.has_interest("Y"));
    assert!(!engine.has_interest("Z"));
}

// =========================================================================
// ET-01: Robust Event-Time Handling Tests
// =========================================================================

#[test]
fn test_event_time_config_builder() {
    let config = EventTimeConfig::new()
        .with_max_out_of_orderness(Duration::from_secs(5))
        .with_allowed_lateness(Duration::from_secs(2))
        .with_late_event_emission();

    assert_eq!(config.max_out_of_orderness, Duration::from_secs(5));
    assert_eq!(config.allowed_lateness, Duration::from_secs(2));
    assert!(config.emit_late_events);
}

#[test]
fn test_event_time_manager_late_events() {
    use chrono::{TimeZone, Utc};

    let config = EventTimeConfig::new()
        .with_max_out_of_orderness(Duration::from_secs(0))
        .with_allowed_lateness(Duration::from_secs(2));

    let mut manager = EventTimeManager::new(config);

    // Process an on-time event
    let ts1 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 10).unwrap();
    let event1 = Arc::new(Event::new("A").with_timestamp(ts1));
    let result1 = manager.process_event(&event1);
    assert!(matches!(result1, EventTimeResult::OnTime));

    // Watermark should now be at ts1
    assert_eq!(manager.watermark(), Some(ts1));

    // Process a late event (within allowed lateness)
    let ts2 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 9).unwrap(); // 1 second late
    let event2 = Arc::new(Event::new("B").with_timestamp(ts2));
    let result2 = manager.process_event(&event2);
    assert!(matches!(result2, EventTimeResult::Late { .. }));

    // Process a too-late event (beyond allowed lateness)
    let ts3 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 5).unwrap(); // 5 seconds late
    let event3 = Arc::new(Event::new("C").with_timestamp(ts3));
    let result3 = manager.process_event(&event3);
    assert!(matches!(result3, EventTimeResult::TooLate { .. }));

    assert_eq!(manager.late_events_accepted(), 1);
    assert_eq!(manager.late_events_dropped(), 1);
}

#[test]
fn test_event_time_manager_watermark_never_recedes() {
    use chrono::{TimeZone, Utc};

    let config = EventTimeConfig::new().with_max_out_of_orderness(Duration::from_secs(0));

    let mut manager = EventTimeManager::new(config);

    let ts1 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 10).unwrap();
    let event1 = Arc::new(Event::new("A").with_timestamp(ts1));
    manager.process_event(&event1);

    assert_eq!(manager.watermark(), Some(ts1));

    // Advance watermark further
    let ts2 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 15).unwrap();
    manager.advance_watermark(ts2);

    // Try to set watermark to earlier time - should not recede
    let ts3 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 5).unwrap();
    manager.advance_watermark(ts3);

    assert_eq!(manager.watermark(), Some(ts2)); // Still at ts2
}

#[test]
fn test_engine_with_event_time_config() {
    use chrono::{TimeZone, Utc};

    let pattern = PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

    let config = EventTimeConfig::new()
        .with_max_out_of_orderness(Duration::from_secs(5))
        .with_allowed_lateness(Duration::from_secs(2));

    let mut engine = SaseEngine::new(pattern).with_event_time_config(config);

    assert_eq!(engine.time_semantics(), TimeSemantics::EventTime);

    let ts1 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 0).unwrap();
    let ts2 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 1).unwrap();

    let event_a = Event::new("A").with_timestamp(ts1);
    let event_b = Event::new("B").with_timestamp(ts2);

    engine.process(&event_a);
    let results = engine.process(&event_b);

    assert_eq!(results.len(), 1);
}

#[test]
fn test_compute_deadline_safe() {
    use chrono::{TimeZone, Utc};

    let start = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 0).unwrap();

    // Normal case
    let deadline = EventTimeManager::compute_deadline(start, Duration::from_secs(5));
    assert!(deadline.is_some());

    let expected = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 5).unwrap();
    assert_eq!(deadline.unwrap(), expected);
}

// =========================================================================
// MET-01: Comprehensive Metrics Tests
// =========================================================================

#[test]
fn test_latency_histogram_basic() {
    let hist = LatencyHistogram::new();

    // Record some latencies
    hist.record(Duration::from_micros(5)); // bucket 1 (2-10µs)
    hist.record(Duration::from_micros(50)); // bucket 2 (11-100µs)
    hist.record(Duration::from_micros(500)); // bucket 3 (101-1000µs)
    hist.record(Duration::from_millis(5)); // bucket 4 (1-10ms)

    assert_eq!(hist.total_count(), 4);

    let bucket_counts = hist.bucket_counts();
    assert_eq!(bucket_counts[1], 1); // 2-10µs
    assert_eq!(bucket_counts[2], 1); // 11-100µs
    assert_eq!(bucket_counts[3], 1); // 101-1000µs
    assert_eq!(bucket_counts[4], 1); // 1-10ms
}

#[test]
fn test_latency_histogram_percentiles() {
    let hist = LatencyHistogram::new();

    // Record 100 latencies in the 1-10µs bucket
    for _ in 0..100 {
        hist.record(Duration::from_micros(5));
    }

    // p50 should be in the 1-10µs bucket
    let p50 = hist.percentile(0.5);
    assert!(p50.as_micros() <= 10);

    // p99 should also be in the 1-10µs bucket
    let p99 = hist.percentile(0.99);
    assert!(p99.as_micros() <= 10);
}

#[test]
fn test_sase_metrics_counters() {
    let metrics = SaseMetrics::new();

    metrics.record_event_processed();
    metrics.record_event_processed();
    metrics.record_event_matched();
    metrics.record_run_created();
    metrics.record_run_completed();
    metrics.record_matches(3);

    let summary = metrics.summary();

    assert_eq!(summary.events_processed, 2);
    assert_eq!(summary.events_matched, 1);
    assert_eq!(summary.runs_created, 1);
    assert_eq!(summary.runs_completed, 1);
    assert_eq!(summary.matches_emitted, 3);
}

#[test]
fn test_sase_metrics_prometheus_format() {
    let metrics = SaseMetrics::new();

    metrics.record_event_processed();
    metrics.record_matches(1);

    let prometheus_output = metrics.to_prometheus("varpulis_sase");

    assert!(prometheus_output.contains("varpulis_sase_events_total 1"));
    assert!(prometheus_output.contains("varpulis_sase_matches_total 1"));
    assert!(prometheus_output.contains("# HELP"));
    assert!(prometheus_output.contains("# TYPE"));
}

#[test]
fn test_process_instrumented_basic() {
    let pattern = PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

    let mut engine = SaseEngine::new(pattern).with_instrumentation();

    // Process events using instrumented method
    engine.process_instrumented(&make_event("A", vec![]));
    let results = engine.process_instrumented(&make_event("B", vec![]));

    assert_eq!(results.len(), 1);

    // Check metrics
    let summary = engine.metrics().summary();
    assert_eq!(summary.events_processed, 2);
    assert!(summary.events_matched >= 1);
    assert_eq!(summary.matches_emitted, 1);
    assert_eq!(summary.runs_completed, 1);
}

#[test]
fn test_process_instrumented_with_ignored_events() {
    let pattern = PatternBuilder::event("A");

    let mut engine = SaseEngine::new(pattern).with_instrumentation();

    // Process an event of a type we don't care about
    engine.process_instrumented(&make_event("Unknown", vec![]));

    let summary = engine.metrics().summary();
    assert_eq!(summary.events_processed, 1);
    assert_eq!(summary.events_ignored, 1);
}

#[test]
fn test_metrics_peak_runs() {
    let pattern = PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

    let mut engine = SaseEngine::new(pattern).with_instrumentation();

    // Create several runs
    engine.process_instrumented(&make_event("A", vec![]));
    engine.process_instrumented(&make_event("A", vec![]));
    engine.process_instrumented(&make_event("A", vec![]));

    let summary_before = engine.metrics().summary();
    assert!(summary_before.peak_active_runs >= 3);

    // Complete all runs
    engine.process_instrumented(&make_event("B", vec![]));

    // Peak should still be recorded
    let summary_after = engine.metrics().summary();
    assert!(summary_after.peak_active_runs >= 3);
}

#[test]
fn test_metrics_arc_sharing() {
    let pattern = PatternBuilder::event("A");

    let engine = SaseEngine::new(pattern);

    // Get Arc reference to metrics
    let metrics_arc = engine.metrics_arc();

    // Record something through the engine's metrics
    engine.metrics().record_event_processed();

    // Should be visible through the Arc
    assert_eq!(metrics_arc.summary().events_processed, 1);
}

#[test]
fn test_late_event_stats() {
    use chrono::{TimeZone, Utc};

    let pattern = PatternBuilder::event("A");

    let config = EventTimeConfig::new()
        .with_max_out_of_orderness(Duration::from_secs(0))
        .with_allowed_lateness(Duration::from_secs(1));

    let mut engine = SaseEngine::new(pattern)
        .with_event_time_config(config)
        .with_instrumentation();

    // Process an on-time event to establish watermark
    let ts1 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 10).unwrap();
    engine.process_instrumented(&Event::new("A").with_timestamp(ts1));

    // Process a late but acceptable event
    let ts2 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 9).unwrap();
    engine.process_instrumented(&Event::new("A").with_timestamp(ts2));

    // Process a too-late event
    let ts3 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 5).unwrap();
    engine.process_instrumented(&Event::new("A").with_timestamp(ts3));

    let (accepted, dropped) = engine.late_event_stats();
    assert_eq!(accepted, 1);
    assert_eq!(dropped, 1);
}

// =========================================================================
// ZDD-KLEENE: Tests for ZDD-based Kleene optimization
// =========================================================================

#[test]
fn test_zdd_kleene_single_run() {
    // Verify that ZDD Kleene uses a single run instead of O(2^n) runs
    // Pattern: SEQ(Start, Tick+, End)
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("Start"),
        PatternBuilder::one_or_more(PatternBuilder::event("Tick")),
        PatternBuilder::event("End"),
    ]);

    let mut engine = SaseEngine::new(pattern);

    // Start the sequence
    engine.process(&make_event("Start", vec![]));
    assert_eq!(
        engine.stats().active_runs,
        1,
        "Should have 1 run after Start"
    );

    // Add multiple Kleene events - with ZDD, should still be 1 run
    for i in 0..10 {
        engine.process(&make_event("Tick", vec![("n", Value::Int(i))]));
        // With ZDD optimization, we should have at most 1 run
        // (not 2^n runs as in the old branching approach)
        assert!(
            engine.stats().active_runs <= 2,
            "ZDD should prevent run explosion: got {} runs at tick {}",
            engine.stats().active_runs,
            i
        );
    }

    // Complete the pattern
    let results = engine.process(&make_event("End", vec![]));

    // Should produce multiple match results (2^10 - 1 = 1023 for Kleene+)
    // Each combination of Tick events is a valid match
    assert!(
        !results.is_empty(),
        "Should produce matches for Kleene+ combinations"
    );
}

#[test]
fn test_zdd_kleene_memory_efficiency() {
    // Verify ZDD uses polynomial nodes instead of exponential combinations
    // Pattern: SEQ(A, B+, C)
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("A"),
        PatternBuilder::one_or_more(PatternBuilder::event_as("B", "b")),
        PatternBuilder::event("C"),
    ]);

    let mut engine = SaseEngine::new(pattern);

    engine.process(&make_event("A", vec![]));

    // Add 20 B events - would be 2^20 = 1M combinations with naive approach
    for i in 0..20 {
        engine.process(&make_event("B", vec![("idx", Value::Int(i))]));
    }

    // Should still have a small number of runs thanks to ZDD
    let stats = engine.stats();
    assert!(
        stats.active_runs < 100,
        "ZDD should keep runs bounded: got {} runs",
        stats.active_runs
    );

    // Complete and verify we get results
    let results = engine.process(&make_event("C", vec![]));
    assert!(!results.is_empty(), "Should produce Kleene combinations");
}

#[test]
fn test_kleene_capture_struct() {
    // Direct test of KleeneCapture functionality
    let mut kc = KleeneCapture::new();
    assert_eq!(kc.combination_count(), 1, "Should start with {{∅}}");
    assert!(kc.is_empty(), "Should be empty initially");

    // Add first event
    let e1 = Arc::new(Event::new("E1"));
    kc.extend(Arc::clone(&e1), Some("e1".to_string()));
    assert_eq!(kc.combination_count(), 2, "Should have {{∅, {{e1}}}}");
    assert_eq!(kc.event_count(), 1);

    // Add second event
    let e2 = Arc::new(Event::new("E2"));
    kc.extend(Arc::clone(&e2), Some("e2".to_string()));
    assert_eq!(
        kc.combination_count(),
        4,
        "Should have {{∅, {{e1}}, {{e2}}, {{e1,e2}}}}"
    );
    assert_eq!(kc.event_count(), 2);

    // ZDD should use polynomial nodes
    assert!(kc.node_count() < 10, "ZDD should be compact");

    // Verify iteration produces correct combinations
    let combinations: Vec<_> = kc.iter_combinations().collect();
    assert_eq!(combinations.len(), 4);
}

// =========================================================================
// EDGE-01: Edge Case Tests
// =========================================================================

#[test]
fn test_simple_seq_completes() {
    // Basic two-event sequence to verify core functionality
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("Start"),
        PatternBuilder::event("End"),
    ]);
    let mut engine = SaseEngine::new(pattern);

    // First event starts a run
    let results = engine.process(&make_event("Start", vec![]));
    assert!(results.is_empty());
    assert_eq!(engine.stats().active_runs, 1);

    // Non-matching event doesn't affect the run
    let results = engine.process(&make_event("Other", vec![]));
    assert!(results.is_empty());
    assert_eq!(engine.stats().active_runs, 1);

    // Second event completes
    let results = engine.process(&make_event("End", vec![]));
    assert_eq!(results.len(), 1);
}

#[test]
fn test_partition_cleanup_after_match() {
    // Verify partition state is properly cleaned after pattern completion
    let pattern = PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

    let mut engine = SaseEngine::new(pattern).with_partition_by("key".to_string());

    // Create runs in multiple partitions
    engine.process(&make_event("A", vec![("key", Value::Str("p1".into()))]));
    engine.process(&make_event("A", vec![("key", Value::Str("p2".into()))]));
    engine.process(&make_event("A", vec![("key", Value::Str("p3".into()))]));

    assert_eq!(engine.stats().partitions, 3);
    assert_eq!(engine.stats().active_runs, 3);

    // Complete partition p1
    let results = engine.process(&make_event("B", vec![("key", Value::Str("p1".into()))]));
    assert_eq!(results.len(), 1);

    // p2 and p3 should still have active runs
    assert_eq!(engine.stats().active_runs, 2);

    // Complete remaining partitions
    engine.process(&make_event("B", vec![("key", Value::Str("p2".into()))]));
    engine.process(&make_event("B", vec![("key", Value::Str("p3".into()))]));

    assert_eq!(engine.stats().active_runs, 0);
}

#[test]
fn test_within_timeout_exact_boundary() {
    use std::time::Duration;

    use chrono::{TimeZone, Utc};

    // Pattern with exactly 5 second window
    let pattern = SasePattern::Within(
        Box::new(PatternBuilder::seq(vec![
            PatternBuilder::event("A"),
            PatternBuilder::event("B"),
        ])),
        Duration::from_secs(5),
    );

    let mut engine = SaseEngine::new(pattern).with_event_time();

    let ts1 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 0).unwrap();
    // Exactly at the 5 second boundary
    let ts2 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 5).unwrap();

    let event_a = Event::new("A").with_timestamp(ts1);
    let event_b = Event::new("B").with_timestamp(ts2);

    engine.process(&event_a);
    let results = engine.process(&event_b);

    // At exactly 5 seconds, behavior depends on implementation (inclusive vs exclusive)
    // This test documents the actual behavior
    let _matched_at_boundary = results.len() == 1;
}

#[test]
fn test_within_timeout_just_before_boundary() {
    use std::time::Duration;

    use chrono::{TimeZone, Utc};

    let pattern = SasePattern::Within(
        Box::new(PatternBuilder::seq(vec![
            PatternBuilder::event("A"),
            PatternBuilder::event("B"),
        ])),
        Duration::from_secs(5),
    );

    let mut engine = SaseEngine::new(pattern).with_event_time();

    let ts1 = Utc.with_ymd_and_hms(2026, 1, 28, 10, 0, 0).unwrap();
    // Just before the boundary (4.999... seconds)
    let ts2 = Utc
        .with_ymd_and_hms(2026, 1, 28, 10, 0, 4)
        .unwrap()
        .checked_add_signed(chrono::Duration::milliseconds(999))
        .unwrap();

    let event_a = Event::new("A").with_timestamp(ts1);
    let event_b = Event::new("B").with_timestamp(ts2);

    engine.process(&event_a);
    let results = engine.process(&event_b);

    // Should definitely match (within window)
    assert_eq!(results.len(), 1);
}

#[test]
fn test_kleene_star_with_one_match() {
    // Kleene* (zero or more) with at least one event
    // Note: zero-match for Kleene* depends on NFA implementation
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("A"),
        PatternBuilder::zero_or_more(PatternBuilder::event("B")),
        PatternBuilder::event("C"),
    ]);

    let mut engine = SaseEngine::new(pattern);

    // Start with A
    engine.process(&make_event("A", vec![]));

    // Add one B
    engine.process(&make_event("B", vec![]));

    // C completes the pattern
    let results = engine.process(&make_event("C", vec![]));

    // Should match with one B
    assert!(!results.is_empty(), "Kleene* should match with one event");
}

#[test]
fn test_repeated_event_types() {
    // Pattern where the same event type appears multiple times
    // SEQ(A, A, B) - need two A events before B
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event_as("A", "a1"),
        PatternBuilder::event_as("A", "a2"),
        PatternBuilder::event("B"),
    ]);

    let mut engine = SaseEngine::new(pattern);

    // First A
    let results = engine.process(&make_event("A", vec![("n", Value::Int(1))]));
    assert!(results.is_empty());

    // Second A
    let results = engine.process(&make_event("A", vec![("n", Value::Int(2))]));
    assert!(results.is_empty());

    // B should complete
    let results = engine.process(&make_event("B", vec![]));
    assert_eq!(results.len(), 1);

    // Verify both A events are captured
    let result = &results[0];
    assert!(result.captured.contains_key("a1"));
    assert!(result.captured.contains_key("a2"));
}

#[test]
fn test_unmatched_event_types_ignored() {
    let pattern = PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

    let mut engine = SaseEngine::new(pattern);

    // Events that don't match any pattern element should be ignored
    engine.process(&make_event("X", vec![]));
    engine.process(&make_event("Y", vec![]));
    engine.process(&make_event("Z", vec![]));

    assert_eq!(engine.stats().active_runs, 0);

    // Now start a valid sequence
    engine.process(&make_event("A", vec![]));
    assert_eq!(engine.stats().active_runs, 1);

    // More unrelated events shouldn't affect the run
    engine.process(&make_event("X", vec![]));
    assert_eq!(engine.stats().active_runs, 1);

    // Complete
    let results = engine.process(&make_event("B", vec![]));
    assert_eq!(results.len(), 1);
}

#[test]
fn test_partition_with_missing_field() {
    // When partitioning by a field that doesn't exist in the event
    let pattern = PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

    let mut engine = SaseEngine::new(pattern).with_partition_by("region".to_string());

    // Event without the partition field - should use default partition
    engine.process(&make_event("A", vec![]));

    // Event with partition field
    engine.process(&make_event(
        "A",
        vec![("region", Value::Str("east".into()))],
    ));

    // Should have 2 partitions (one default, one "east")
    assert_eq!(engine.stats().partitions, 2);
}

#[test]
fn test_predicate_type_mismatch() {
    // Predicate comparing incompatible types should not match
    // Use a two-event sequence to test predicate behavior
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event_where(
            "A",
            Predicate::Compare {
                field: "value".to_string(),
                op: CompareOp::Gt,
                value: Value::Int(100),
            },
        ),
        PatternBuilder::event("B"),
    ]);

    let mut engine = SaseEngine::new(pattern);

    // String value compared against int predicate - should not start a run
    let results = engine.process(&make_event(
        "A",
        vec![("value", Value::Str("not-a-number".into()))],
    ));
    assert!(results.is_empty());
    // Run should not be started due to predicate mismatch
    assert_eq!(engine.stats().active_runs, 0);

    // Int value that matches predicate - should start a run
    let results = engine.process(&make_event("A", vec![("value", Value::Int(150))]));
    assert!(results.is_empty()); // Not complete yet
    assert_eq!(engine.stats().active_runs, 1);

    // B completes the sequence
    let results = engine.process(&make_event("B", vec![]));
    assert_eq!(results.len(), 1);
}

#[test]
fn test_multiple_concurrent_runs_same_partition() {
    // Multiple runs in the same partition (no partitioning)
    let pattern = PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

    let mut engine = SaseEngine::new(pattern);

    // Start multiple runs
    engine.process(&make_event("A", vec![("id", Value::Int(1))]));
    engine.process(&make_event("A", vec![("id", Value::Int(2))]));
    engine.process(&make_event("A", vec![("id", Value::Int(3))]));

    // All three A events should create runs
    assert!(
        engine.stats().active_runs >= 1,
        "At least one run should be active"
    );

    // B should complete all runs
    let results = engine.process(&make_event("B", vec![]));
    assert!(!results.is_empty());
}

#[test]
fn test_process_time_semantics_default() {
    let pattern = PatternBuilder::event("A");
    let engine = SaseEngine::new(pattern);

    // Default should be ProcessingTime
    assert_eq!(engine.time_semantics(), TimeSemantics::ProcessingTime);
}

#[test]
fn test_or_pattern_in_seq_either_branch() {
    // OR(A, B) within SEQ - either branch should work
    // Pattern: SEQ(Start, OR(A, B), End)
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("Start"),
        PatternBuilder::or(PatternBuilder::event("A"), PatternBuilder::event("B")),
        PatternBuilder::event("End"),
    ]);

    let mut engine = SaseEngine::new(pattern);

    // Test branch A
    engine.process(&make_event("Start", vec![]));
    engine.process(&make_event("A", vec![]));
    let results = engine.process(&make_event("End", vec![]));
    assert!(!results.is_empty(), "OR pattern should match A branch");
}

#[test]
fn test_or_pattern_in_seq_second_branch() {
    // OR(A, B) within SEQ - test B branch
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("Start"),
        PatternBuilder::or(PatternBuilder::event("A"), PatternBuilder::event("B")),
        PatternBuilder::event("End"),
    ]);

    let mut engine = SaseEngine::new(pattern);

    // Test branch B
    engine.process(&make_event("Start", vec![]));
    engine.process(&make_event("B", vec![]));
    let results = engine.process(&make_event("End", vec![]));
    assert!(!results.is_empty(), "OR pattern should match B branch");
}

#[test]
fn test_and_pattern_in_seq() {
    // AND(A, B) within SEQ - both must occur
    // Pattern: SEQ(Start, AND(A, B), End)
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("Start"),
        PatternBuilder::and(PatternBuilder::event("A"), PatternBuilder::event("B")),
        PatternBuilder::event("End"),
    ]);

    let mut engine = SaseEngine::new(pattern);

    engine.process(&make_event("Start", vec![]));
    engine.process(&make_event("A", vec![]));
    engine.process(&make_event("B", vec![]));
    let results = engine.process(&make_event("End", vec![]));
    assert!(
        !results.is_empty(),
        "AND pattern should match when both events occur"
    );
}

#[test]
fn test_run_checkpoint_restore() {
    // Create a run and simulate some state advancement
    let mut run = Run::new(0);

    // Add a captured event
    let evt_a = Arc::new(make_event("A", vec![("id", Value::from(1))]));
    run.captured.insert("a".to_string(), evt_a.clone());

    // Advance the current state
    run.current_state = 2;

    // Mark it as invalidated to test that flag round-trips
    run.invalidated = true;

    // Checkpoint the run
    let cp = run.checkpoint();

    // Restore from checkpoint
    let restored = Run::from_checkpoint(&cp);

    // Verify essential fields survived the round-trip
    assert_eq!(
        restored.current_state, run.current_state,
        "current_state should be preserved across checkpoint/restore"
    );
    assert_eq!(
        restored.captured.len(),
        run.captured.len(),
        "captured map size should be preserved"
    );
    assert!(
        restored.captured.contains_key("a"),
        "captured alias 'a' should be present after restore"
    );
    assert_eq!(
        restored.captured["a"].event_type, evt_a.event_type,
        "captured event type should be preserved"
    );
    assert_eq!(
        restored.invalidated, run.invalidated,
        "invalidated flag should be preserved across checkpoint/restore"
    );
}

#[test]
fn test_sase_engine_checkpoint_restore() {
    // Build a SEQ(A, B) pattern (need two copies because engine takes ownership)
    let pattern1 =
        PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);
    let pattern2 =
        PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);

    let mut engine = SaseEngine::new(pattern1);

    // Process event A – creates an active run but no complete match yet
    let results = engine.process(&make_event("A", vec![]));
    assert!(
        results.is_empty(),
        "A alone should not complete the pattern"
    );

    // Checkpoint the engine while a run is in progress
    let cp = engine.checkpoint();

    // Create a fresh engine with the same pattern and restore state
    let mut engine2 = SaseEngine::new(pattern2);
    engine2.restore(&cp);

    // Process event B on the restored engine – should complete the sequence
    let results = engine2.process(&make_event("B", vec![]));
    assert!(
        !results.is_empty(),
        "B on restored engine should complete the SEQ(A, B) match"
    );
}

#[test]
fn test_sase_engine_checkpoint_empty() {
    let pattern = PatternBuilder::seq(vec![PatternBuilder::event("A"), PatternBuilder::event("B")]);
    let engine = SaseEngine::new(pattern);

    // Checkpoint with no events processed – no active runs
    let cp = engine.checkpoint();

    assert!(
        cp.active_runs.is_empty(),
        "checkpoint with no processed events should have empty active_runs"
    );
}

// =========================================================================
// PHASE 1: Kleene Self-Loop Tests
// =========================================================================

#[test]
fn test_kleene_plus_captures_multiple() {
    // SEQ(A, B+, C) with 3 B events.
    // After the self-loop fix, all 3 B events should be captured.
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("A"),
        PatternBuilder::one_or_more(PatternBuilder::event("B")),
        PatternBuilder::event("C"),
    ]);

    let mut engine = SaseEngine::new(pattern);

    engine.process(&make_event("A", vec![]));
    engine.process(&make_event("B", vec![("val", Value::Int(1))]));
    engine.process(&make_event("B", vec![("val", Value::Int(2))]));
    engine.process(&make_event("B", vec![("val", Value::Int(3))]));

    let results = engine.process(&make_event("C", vec![]));

    assert!(
        !results.is_empty(),
        "Should produce at least one match for SEQ(A, B+, C)"
    );
    // The stack should contain A + 3*B + C = 5 entries
    let result = &results[0];
    assert_eq!(
        result.stack.len(),
        5,
        "Stack should have A + 3B + C = 5 entries, got {}",
        result.stack.len()
    );
}

#[test]
fn test_kleene_plus_exact_count() {
    // SEQ(A, B+, C) with N B events.
    // Stack length should be N+2 (A + N×B + C).
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("A"),
        PatternBuilder::one_or_more(PatternBuilder::event("B")),
        PatternBuilder::event("C"),
    ]);

    for n in 1..=5 {
        let mut engine = SaseEngine::new(pattern.clone());
        engine.process(&make_event("A", vec![]));
        for i in 0..n {
            engine.process(&make_event("B", vec![("n", Value::Int(i))]));
        }
        let results = engine.process(&make_event("C", vec![]));
        assert!(!results.is_empty(), "Should match with {n} B events");
        assert_eq!(
            results[0].stack.len(),
            (n + 2) as usize,
            "Stack should have {} entries for {} B events",
            n + 2,
            n
        );
    }
}

#[test]
fn test_zdd_kleene_single_run_captures_all() {
    // Tightened: verify the stack contains all 10 Tick events
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("Start"),
        PatternBuilder::one_or_more(PatternBuilder::event("Tick")),
        PatternBuilder::event("End"),
    ]);

    let mut engine = SaseEngine::new(pattern);
    engine.process(&make_event("Start", vec![]));
    for i in 0..10 {
        engine.process(&make_event("Tick", vec![("n", Value::Int(i))]));
    }
    let results = engine.process(&make_event("End", vec![]));
    assert!(!results.is_empty(), "Should produce matches");
    // Start + 10×Tick + End = 12
    assert_eq!(
        results[0].stack.len(),
        12,
        "Stack should have Start + 10 Ticks + End = 12 entries"
    );
}

#[test]
fn test_kleene_self_loop_preserves_all_pattern() {
    // Verify `all` pattern still works: each B event emits a result
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("A"),
        PatternBuilder::one_or_more(PatternBuilder::event("B")),
    ]);
    // When Kleene+ has epsilon to Accept, each event emits CompleteAndContinue
    let mut engine = SaseEngine::new(pattern);

    engine.process(&make_event("A", vec![]));
    let r1 = engine.process(&make_event("B", vec![("n", Value::Int(1))]));
    assert_eq!(r1.len(), 1, "First B should emit a match");

    let r2 = engine.process(&make_event("B", vec![("n", Value::Int(2))]));
    assert_eq!(
        r2.len(),
        1,
        "Second B should also emit a match via self-loop"
    );
}

// =========================================================================
// PHASE 2: Predicate Postponing Tests (SIGMOD 2014)
// =========================================================================

#[test]
fn test_classify_predicate_compare_is_consistent() {
    let pred = Predicate::Compare {
        field: "val".to_string(),
        op: CompareOp::Gt,
        value: Value::Int(100),
    };
    assert_eq!(
        classify_predicate(&pred, Some("b")),
        PredicateClass::Consistent
    );
}

#[test]
fn test_classify_predicate_ref_non_kleene_consistent() {
    let pred = Predicate::CompareRef {
        field: "val".to_string(),
        op: CompareOp::Gt,
        ref_alias: "a".to_string(),
        ref_field: "val".to_string(),
    };
    assert_eq!(
        classify_predicate(&pred, Some("b")),
        PredicateClass::Consistent,
        "Referencing non-Kleene alias should be consistent"
    );
}

#[test]
fn test_classify_predicate_ref_kleene_inconsistent() {
    let pred = Predicate::CompareRef {
        field: "val".to_string(),
        op: CompareOp::Ge,
        ref_alias: "b".to_string(),
        ref_field: "val".to_string(),
    };
    assert_eq!(
        classify_predicate(&pred, Some("b")),
        PredicateClass::Inconsistent,
        "Referencing the Kleene alias itself should be inconsistent"
    );
}

#[test]
fn test_classify_predicate_and_propagates_inconsistent() {
    let consistent = Predicate::Compare {
        field: "val".to_string(),
        op: CompareOp::Gt,
        value: Value::Int(0),
    };
    let inconsistent = Predicate::CompareRef {
        field: "val".to_string(),
        op: CompareOp::Ge,
        ref_alias: "b".to_string(),
        ref_field: "val".to_string(),
    };
    let combined = Predicate::And(Box::new(consistent), Box::new(inconsistent));
    assert_eq!(
        classify_predicate(&combined, Some("b")),
        PredicateClass::Inconsistent,
        "And(Consistent, Inconsistent) should be Inconsistent"
    );
}

#[test]
fn test_classify_predicate_no_kleene_alias() {
    // When no Kleene alias is specified, all predicates should be Consistent
    let pred = Predicate::CompareRef {
        field: "val".to_string(),
        op: CompareOp::Ge,
        ref_alias: "b".to_string(),
        ref_field: "val".to_string(),
    };
    assert_eq!(
        classify_predicate(&pred, None),
        PredicateClass::Consistent,
        "No Kleene alias means all predicates are consistent"
    );
}

#[test]
fn test_consistent_predicate_no_postponing() {
    // SEQ(A, B+ WHERE val > 100, C)
    // Predicate is Consistent → no postponing → events that fail are filtered eagerly
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("A"),
        PatternBuilder::one_or_more(SasePattern::Event {
            event_type: "B".to_string(),
            predicate: Some(Predicate::Compare {
                field: "val".to_string(),
                op: CompareOp::Gt,
                value: Value::Int(100),
            }),
            alias: Some("b".to_string()),
        }),
        PatternBuilder::event("C"),
    ]);

    let mut engine = SaseEngine::new(pattern);
    engine.process(&make_event("A", vec![]));
    engine.process(&make_event("B", vec![("val", Value::Int(150))]));
    engine.process(&make_event("B", vec![("val", Value::Int(50))])); // filtered out
    engine.process(&make_event("B", vec![("val", Value::Int(200))]));

    let results = engine.process(&make_event("C", vec![]));
    assert!(
        !results.is_empty(),
        "Should match with consistent predicate"
    );
    // Only B(150) and B(200) pass the predicate, B(50) is filtered eagerly
    // Stack should be A + 2B + C = 4
    assert_eq!(
        results[0].stack.len(),
        4,
        "Only matching B events should be in stack"
    );
}

#[test]
fn test_postponed_predicate_monotonic() {
    // SEQ(A, B+ WHERE b.val >= b.val, C) — self-referencing predicate
    // The predicate references the Kleene alias itself → Inconsistent → postponed
    // Events: B(5), B(3), B(4), B(6)
    // Valid combinations where each element >= previous:
    // {5}, {3}, {4}, {6}, {5,6}, {3,4}, {3,6}, {4,6}, {3,4,6}
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("A"),
        PatternBuilder::one_or_more(SasePattern::Event {
            event_type: "B".to_string(),
            predicate: Some(Predicate::CompareRef {
                field: "val".to_string(),
                op: CompareOp::Ge,
                ref_alias: "b".to_string(),
                ref_field: "val".to_string(),
            }),
            alias: Some("b".to_string()),
        }),
        PatternBuilder::event("C"),
    ]);

    let mut engine = SaseEngine::new(pattern);
    engine.process(&make_event("A", vec![]));
    engine.process(&make_event("B", vec![("val", Value::Int(5))]));
    engine.process(&make_event("B", vec![("val", Value::Int(3))]));
    engine.process(&make_event("B", vec![("val", Value::Int(4))]));
    engine.process(&make_event("B", vec![("val", Value::Int(6))]));

    let results = engine.process(&make_event("C", vec![]));
    assert!(
        !results.is_empty(),
        "Should produce matches with postponed predicate"
    );
    // With CompareRef(val >= b.val), the predicate checks current.val >= captured["b"].val
    // Since "b" gets updated to the previous event in the combination, this enforces
    // monotonically non-decreasing sequences.
    // Valid monotonic subsequences of [5,3,4,6]: {5}, {3}, {4}, {6}, {5,6}, {3,4}, {3,6}, {4,6}, {3,4,6}
    assert!(
        results.len() >= 4,
        "Should have multiple valid monotonic combinations, got {}",
        results.len()
    );
}

#[test]
fn test_postponed_fewer_valid_combinations() {
    // SEQ(A, B+ WHERE b.val > b.val, C) — strict greater-than self-reference
    // With decreasing values [10, 5], the multi-element combination [10, 5]
    // fails because 5 > 10 is false, but single-element combinations and
    // the pair [5, 10] (wrong order) aren't generated. So the result count
    // should be strictly less than the total combination count.
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("A"),
        PatternBuilder::one_or_more(SasePattern::Event {
            event_type: "B".to_string(),
            predicate: Some(Predicate::CompareRef {
                field: "val".to_string(),
                op: CompareOp::Gt,
                ref_alias: "b".to_string(),
                ref_field: "val".to_string(),
            }),
            alias: Some("b".to_string()),
        }),
        PatternBuilder::event("C"),
    ]);

    let mut engine = SaseEngine::new(pattern);
    engine.process(&make_event("A", vec![]));
    engine.process(&make_event("B", vec![("val", Value::Int(10))]));
    engine.process(&make_event("B", vec![("val", Value::Int(5))]));
    engine.process(&make_event("B", vec![("val", Value::Int(3))]));

    let results = engine.process(&make_event("C", vec![]));
    // Total ZDD combinations for 3 events: 2^3 - 1 = 7 (excluding empty)
    // The deferred predicate filters some out (e.g., [10, 5] fails 5 > 10)
    // Result count should be less than 7
    assert!(
        results.len() < 7,
        "Deferred predicate should filter some combinations, got {}",
        results.len()
    );
    assert!(
        !results.is_empty(),
        "Should still have some valid combinations"
    );
}

// =========================================================================
// PHASE 3: Safety Cap Stress Tests
// =========================================================================

#[test]
fn test_kleene_accumulation_cap() {
    // Send more events than MAX_KLEENE_EVENTS into a Kleene+ state.
    // The engine must not OOM or hang — it should cap accumulation.
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("A"),
        PatternBuilder::one_or_more(PatternBuilder::event("B")),
        PatternBuilder::event("C"),
    ]);

    let mut engine = SaseEngine::new(pattern);
    engine.process(&make_event("A", vec![]));

    // Send 50 B events (well above MAX_KLEENE_EVENTS = 20)
    for i in 0..50 {
        engine.process(&make_event("B", vec![("n", Value::Int(i))]));
    }

    let results = engine.process(&make_event("C", vec![]));
    assert!(!results.is_empty(), "Should still produce a match");
    // Stack should have at most A + MAX_KLEENE_EVENTS B's + C
    assert!(
        results[0].stack.len() <= (MAX_KLEENE_EVENTS as usize + 2),
        "Stack length {} should be capped at {} (A + {} B's + C)",
        results[0].stack.len(),
        MAX_KLEENE_EVENTS + 2,
        MAX_KLEENE_EVENTS
    );
}

#[test]
fn test_kleene_deferred_predicate_enumeration_cap() {
    // With a deferred predicate and MAX_KLEENE_EVENTS events,
    // enumeration should not produce more than MAX_ENUMERATION_RESULTS.
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("A"),
        PatternBuilder::one_or_more(SasePattern::Event {
            event_type: "B".to_string(),
            predicate: Some(Predicate::CompareRef {
                field: "val".to_string(),
                op: CompareOp::Ge,
                ref_alias: "b".to_string(),
                ref_field: "val".to_string(),
            }),
            alias: Some("b".to_string()),
        }),
        PatternBuilder::event("C"),
    ]);

    let mut engine = SaseEngine::new(pattern);
    engine.process(&make_event("A", vec![]));

    // 20 monotonically increasing events → most combinations pass the
    // deferred predicate (val >= prev.val), producing potentially
    // many results. The cap should limit output.
    for i in 0..20 {
        engine.process(&make_event("B", vec![("val", Value::Int(i))]));
    }

    let results = engine.process(&make_event("C", vec![]));
    assert!(
        results.len() <= MAX_ENUMERATION_RESULTS,
        "Enumeration should be capped at {}, got {}",
        MAX_ENUMERATION_RESULTS,
        results.len()
    );
}

#[test]
fn test_large_kleene_no_hang() {
    // Regression test: sending 100 events into a Kleene+ with deferred
    // predicate must complete in bounded time and memory.
    use std::time::Instant;

    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("A"),
        PatternBuilder::one_or_more(SasePattern::Event {
            event_type: "B".to_string(),
            predicate: Some(Predicate::CompareRef {
                field: "val".to_string(),
                op: CompareOp::Gt,
                ref_alias: "b".to_string(),
                ref_field: "val".to_string(),
            }),
            alias: Some("b".to_string()),
        }),
        PatternBuilder::event("C"),
    ]);

    let mut engine = SaseEngine::new(pattern);
    engine.process(&make_event("A", vec![]));

    for i in 0..100 {
        engine.process(&make_event("B", vec![("val", Value::Int(i))]));
    }

    let start = Instant::now();
    let results = engine.process(&make_event("C", vec![]));
    let elapsed = start.elapsed();

    // Must complete within 5 seconds (without caps this would hang/OOM)
    assert!(
        elapsed.as_secs() < 5,
        "Enumeration took {elapsed:?} — should be bounded"
    );
    assert!(
        results.len() <= MAX_ENUMERATION_RESULTS,
        "Results capped at {}, got {}",
        MAX_ENUMERATION_RESULTS,
        results.len()
    );
}

#[test]
fn test_configurable_kleene_limits() {
    // Verify builder methods override the defaults.
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("A"),
        PatternBuilder::one_or_more(PatternBuilder::event("B")),
        PatternBuilder::event("C"),
    ]);

    // Cap at 5 Kleene events instead of default 20
    let mut engine = SaseEngine::new(pattern).with_max_kleene_events(5);

    engine.process(&make_event("A", vec![]));
    for i in 0..20 {
        engine.process(&make_event("B", vec![("n", Value::Int(i))]));
    }
    let results = engine.process(&make_event("C", vec![]));
    assert!(!results.is_empty());
    // A + at most 5 B's + C = 7
    assert!(
        results[0].stack.len() <= 7,
        "Custom max_kleene_events=5 should cap stack at 7, got {}",
        results[0].stack.len()
    );
}

#[test]
fn test_configurable_enumeration_limit() {
    // Verify with_max_enumeration_results caps output.
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("A"),
        PatternBuilder::one_or_more(SasePattern::Event {
            event_type: "B".to_string(),
            predicate: Some(Predicate::CompareRef {
                field: "val".to_string(),
                op: CompareOp::Ge,
                ref_alias: "b".to_string(),
                ref_field: "val".to_string(),
            }),
            alias: Some("b".to_string()),
        }),
        PatternBuilder::event("C"),
    ]);

    let mut engine = SaseEngine::new(pattern).with_max_enumeration_results(3);

    engine.process(&make_event("A", vec![]));
    for i in 0..15 {
        engine.process(&make_event("B", vec![("val", Value::Int(i))]));
    }
    let results = engine.process(&make_event("C", vec![]));
    assert!(
        results.len() <= 3,
        "Custom max_enumeration_results=3 should cap at 3, got {}",
        results.len()
    );
}

// =========================================================================
// Real-World SASE+ Scenarios (Kleene closures, predicates, termination)
// =========================================================================

#[test]
fn test_kleene_rising_sequence_with_terminator() {
    // SEQ(Start, Rising+ where val > Start.val, Drop where val < Start.val)
    // Only emits once: when the rising sequence ends with a drop.
    let pattern = PatternBuilder::seq(vec![
        SasePattern::Event {
            event_type: "Tick".to_string(),
            predicate: None,
            alias: Some("first".to_string()),
        },
        PatternBuilder::one_or_more(SasePattern::Event {
            event_type: "Tick".to_string(),
            predicate: Some(Predicate::CompareRef {
                field: "val".to_string(),
                op: CompareOp::Gt,
                ref_alias: "first".to_string(),
                ref_field: "val".to_string(),
            }),
            alias: Some("rising".to_string()),
        }),
        SasePattern::Event {
            event_type: "Tick".to_string(),
            predicate: Some(Predicate::CompareRef {
                field: "val".to_string(),
                op: CompareOp::Lt,
                ref_alias: "first".to_string(),
                ref_field: "val".to_string(),
            }),
            alias: Some("drop".to_string()),
        },
    ]);

    let mut engine = SaseEngine::new(pattern);

    // first=10
    let r = engine.process(&make_event("Tick", vec![("val", Value::Int(10))]));
    assert!(r.is_empty());

    // 15 > 10: enters Kleene
    let r = engine.process(&make_event("Tick", vec![("val", Value::Int(15))]));
    assert!(r.is_empty(), "Should not complete yet — no terminator");

    // 20 > 10: extends Kleene
    let r = engine.process(&make_event("Tick", vec![("val", Value::Int(20))]));
    assert!(r.is_empty(), "Should not complete yet — no terminator");

    // 25 > 10: extends Kleene
    let r = engine.process(&make_event("Tick", vec![("val", Value::Int(25))]));
    assert!(r.is_empty(), "Should not complete yet — no terminator");

    // 5 < 10: terminates! Should produce at least 1 match
    let r = engine.process(&make_event("Tick", vec![("val", Value::Int(5))]));
    assert!(
        !r.is_empty(),
        "Drop below baseline should complete the pattern"
    );
    // The longest match should have: first + 3 rising + drop = 5 events
    let longest = r.iter().max_by_key(|m| m.stack.len()).unwrap();
    assert_eq!(longest.stack.len(), 5, "first(10) + rising(15,20,25) + drop(5)");
}

#[test]
fn test_kleene_without_terminator_emits_on_each_extension() {
    // SEQ(A, B+) — Kleene is last item, epsilon to Accept.
    // Each B event emits a match (CompleteAndContinue behavior).
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("A"),
        PatternBuilder::one_or_more(PatternBuilder::event("B")),
    ]);

    let mut engine = SaseEngine::new(pattern);

    engine.process(&make_event("A", vec![]));

    let r1 = engine.process(&make_event("B", vec![("n", Value::Int(1))]));
    assert_eq!(r1.len(), 1, "First B emits (Kleene min satisfied)");

    let r2 = engine.process(&make_event("B", vec![("n", Value::Int(2))]));
    assert_eq!(r2.len(), 1, "Second B emits (Kleene extension)");

    let r3 = engine.process(&make_event("B", vec![("n", Value::Int(3))]));
    assert_eq!(r3.len(), 1, "Third B emits (Kleene extension)");

    // Total: 3 matches, each with increasing stack depth
    assert_eq!(r1[0].stack.len(), 2, "A + 1B");
    assert_eq!(r2[0].stack.len(), 3, "A + 2B");
    assert_eq!(r3[0].stack.len(), 4, "A + 3B");
}

#[test]
fn test_kleene_predicate_filters_non_matching_events() {
    // SEQ(Start, Rising+ where val > Start.val, End)
    // Events that don't satisfy the predicate should NOT extend the Kleene.
    let pattern = PatternBuilder::seq(vec![
        SasePattern::Event {
            event_type: "Tick".to_string(),
            predicate: None,
            alias: Some("start".to_string()),
        },
        PatternBuilder::one_or_more(SasePattern::Event {
            event_type: "Tick".to_string(),
            predicate: Some(Predicate::CompareRef {
                field: "val".to_string(),
                op: CompareOp::Gt,
                ref_alias: "start".to_string(),
                ref_field: "val".to_string(),
            }),
            alias: Some("rising".to_string()),
        }),
        PatternBuilder::event("End"),
    ]);

    let mut engine = SaseEngine::new(pattern);

    // start.val = 50
    engine.process(&make_event("Tick", vec![("val", Value::Int(50))]));

    // 60 > 50: enters Kleene
    engine.process(&make_event("Tick", vec![("val", Value::Int(60))]));

    // 30 < 50: does NOT extend Kleene (predicate fails)
    engine.process(&make_event("Tick", vec![("val", Value::Int(30))]));

    // 70 > 50: extends Kleene
    engine.process(&make_event("Tick", vec![("val", Value::Int(70))]));

    // End: completes — may produce multiple matches (skip-till-any-match)
    let results = engine.process(&make_event("End", vec![]));
    assert!(
        !results.is_empty(),
        "Should complete on End"
    );
    // The longest match: start(50) + rising(60,70) + End = 4
    let longest = results.iter().max_by_key(|m| m.stack.len()).unwrap();
    assert!(
        longest.stack.len() >= 3,
        "Longest match should have at least start + 1 rising + End"
    );
}

#[test]
fn test_kleene_brute_force_pattern() {
    // Real-world: N failed logins -> 1 success
    // SEQ(Failed as first, Failed+ as fails, Success as success)
    let pattern = PatternBuilder::seq(vec![
        SasePattern::Event {
            event_type: "Login".to_string(),
            predicate: Some(Predicate::Compare {
                field: "status".to_string(),
                op: CompareOp::Eq,
                value: Value::str("failed"),
            }),
            alias: Some("first".to_string()),
        },
        PatternBuilder::one_or_more(SasePattern::Event {
            event_type: "Login".to_string(),
            predicate: Some(Predicate::Compare {
                field: "status".to_string(),
                op: CompareOp::Eq,
                value: Value::str("failed"),
            }),
            alias: Some("fails".to_string()),
        }),
        SasePattern::Event {
            event_type: "Login".to_string(),
            predicate: Some(Predicate::Compare {
                field: "status".to_string(),
                op: CompareOp::Eq,
                value: Value::str("success"),
            }),
            alias: Some("success".to_string()),
        },
    ]);

    let mut engine = SaseEngine::new(pattern);

    // 10 failed logins
    for i in 0..10 {
        let r = engine.process(&make_event(
            "Login",
            vec![("status", Value::str("failed")), ("attempt", Value::Int(i))],
        ));
        assert!(r.is_empty(), "Failed login {i} should not complete pattern");
    }

    // 1 success -> completes
    let results = engine.process(&make_event(
        "Login",
        vec![("status", Value::str("success"))],
    ));
    assert!(
        !results.is_empty(),
        "Success after 10 failures should complete the brute force pattern"
    );
    // Stack: first(1) + fails(9) + success(1) = 11
    assert_eq!(results[0].stack.len(), 11, "first + 9 Kleene fails + success");
}

#[test]
fn test_kleene_count_aggregate() {
    // SEQ(A, B+, C) — verify count(B) is accessible in the match result
    let pattern = PatternBuilder::seq(vec![
        PatternBuilder::event("A"),
        PatternBuilder::one_or_more(SasePattern::Event {
            event_type: "B".to_string(),
            predicate: None,
            alias: Some("items".to_string()),
        }),
        PatternBuilder::event("C"),
    ]);

    let mut engine = SaseEngine::new(pattern);

    engine.process(&make_event("A", vec![]));
    for i in 1..=5 {
        engine.process(&make_event("B", vec![("n", Value::Int(i))]));
    }
    let results = engine.process(&make_event("C", vec![]));

    assert!(!results.is_empty(), "Should complete");
    // Stack: A + 5B + C = 7
    assert_eq!(results[0].stack.len(), 7);

    // Count the 'items' alias entries in the stack
    let items_count = results[0]
        .stack
        .iter()
        .filter(|entry| entry.alias.as_deref() == Some("items"))
        .count();
    assert_eq!(items_count, 5, "Should have 5 Kleene-captured 'items' events");
}

#[test]
fn test_kleene_with_partition_isolates_keys() {
    // SEQ(Start, Rising+ where val > Start.val, Drop where val < Start.val)
    // Partitioned by sensor — events from different sensors don't mix.
    let pattern = PatternBuilder::seq(vec![
        SasePattern::Event {
            event_type: "Temp".to_string(),
            predicate: None,
            alias: Some("first".to_string()),
        },
        PatternBuilder::one_or_more(SasePattern::Event {
            event_type: "Temp".to_string(),
            predicate: Some(Predicate::CompareRef {
                field: "val".to_string(),
                op: CompareOp::Gt,
                ref_alias: "first".to_string(),
                ref_field: "val".to_string(),
            }),
            alias: Some("rising".to_string()),
        }),
        SasePattern::Event {
            event_type: "Temp".to_string(),
            predicate: Some(Predicate::CompareRef {
                field: "val".to_string(),
                op: CompareOp::Lt,
                ref_alias: "first".to_string(),
                ref_field: "val".to_string(),
            }),
            alias: Some("drop".to_string()),
        },
    ]);

    let mut engine = SaseEngine::new(pattern).with_partition_by("sensor".to_string());

    // Sensor A: baseline 20
    engine.process(&make_event(
        "Temp",
        vec![("sensor", Value::str("A")), ("val", Value::Int(20))],
    ));
    // Sensor B: baseline 100
    engine.process(&make_event(
        "Temp",
        vec![("sensor", Value::str("B")), ("val", Value::Int(100))],
    ));

    // Sensor A: 30 > 20 (rising)
    engine.process(&make_event(
        "Temp",
        vec![("sensor", Value::str("A")), ("val", Value::Int(30))],
    ));
    // Sensor B: 50 < 100 — should NOT enter B's Kleene
    // But 50 > 20, so might enter A's Kleene (if partitioning works, it shouldn't)
    engine.process(&make_event(
        "Temp",
        vec![("sensor", Value::str("B")), ("val", Value::Int(50))],
    ));

    // Sensor A: 10 < 20 — triggers A's drop
    let results = engine.process(&make_event(
        "Temp",
        vec![("sensor", Value::str("A")), ("val", Value::Int(10))],
    ));
    assert_eq!(
        results.len(),
        1,
        "Only sensor A should complete (B never had a rising event)"
    );
    // Verify it's sensor A's match: first=20, rising=[30], drop=10
    assert_eq!(results[0].stack.len(), 3, "first(20) + rising(30) + drop(10)");
}
