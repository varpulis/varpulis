//! Comprehensive coverage tests for aggregation functions.
//!
//! Tests every `AggregateFunc` implementation: Count, Sum, Avg, Min, Max,
//! StdDev, First, Last, CountDistinct, Ema, and ExprAggregate.
//! Covers basic usage, edge cases (empty, single event, missing fields,
//! null values, large inputs), `apply_shared`, and binary expressions.

use std::sync::Arc;
use varpulis_core::Value;
use varpulis_runtime::aggregation::*;
use varpulis_runtime::columnar::ColumnarBuffer;
use varpulis_runtime::Event;

// ==========================================================================
// Helpers
// ==========================================================================

fn make_events(values: &[f64]) -> Vec<Event> {
    values
        .iter()
        .map(|&v| Event::new("Test").with_field("value", v))
        .collect()
}

fn make_shared_events(values: &[f64]) -> Vec<Arc<Event>> {
    values
        .iter()
        .map(|&v| Arc::new(Event::new("Test").with_field("value", v)))
        .collect()
}

fn assert_float_near(val: &Value, expected: f64, tolerance: f64) {
    match val {
        Value::Float(v) => {
            assert!(
                (v - expected).abs() < tolerance,
                "Expected ~{expected}, got {v}"
            );
        }
        other => panic!("Expected Value::Float, got {:?}", other),
    }
}

// ==========================================================================
// 1. Count
// ==========================================================================

#[test]
fn count_empty_events() {
    let events: Vec<Event> = vec![];
    assert_eq!(Count.apply(&events, None), Value::Int(0));
}

#[test]
fn count_multiple_events() {
    let events = make_events(&[1.0, 2.0, 3.0, 4.0, 5.0]);
    assert_eq!(Count.apply(&events, None), Value::Int(5));
}

#[test]
fn count_ignores_field_parameter() {
    let events = make_events(&[10.0, 20.0]);
    // Count should return the number of events regardless of field
    assert_eq!(Count.apply(&events, Some("nonexistent")), Value::Int(2));
    assert_eq!(Count.apply(&events, None), Value::Int(2));
}

#[test]
fn count_single_event() {
    let events = make_events(&[42.0]);
    assert_eq!(Count.apply(&events, None), Value::Int(1));
}

// ==========================================================================
// 2. Sum
// ==========================================================================

#[test]
fn sum_empty_events() {
    let events: Vec<Event> = vec![];
    assert_eq!(Sum.apply(&events, Some("value")), Value::Float(0.0));
}

#[test]
fn sum_numeric_values() {
    let events = make_events(&[10.0, 20.0, 30.0]);
    assert_eq!(Sum.apply(&events, Some("value")), Value::Float(60.0));
}

#[test]
fn sum_mixed_types_missing_field() {
    // Events where some have the field and some do not
    let events = vec![
        Event::new("Test").with_field("value", 10.0),
        Event::new("Test").with_field("other", 99.0), // missing "value"
        Event::new("Test").with_field("value", 30.0),
    ];
    // Sum should only include events that have a float "value"
    assert_eq!(Sum.apply(&events, Some("value")), Value::Float(40.0));
}

#[test]
fn sum_defaults_to_value_field() {
    let events = make_events(&[5.0, 15.0]);
    assert_eq!(Sum.apply(&events, None), Value::Float(20.0));
}

#[test]
fn sum_single_event() {
    let events = make_events(&[42.5]);
    assert_eq!(Sum.apply(&events, Some("value")), Value::Float(42.5));
}

// ==========================================================================
// 3. Avg
// ==========================================================================

#[test]
fn avg_empty_events() {
    let events: Vec<Event> = vec![];
    assert_eq!(Avg.apply(&events, Some("value")), Value::Null);
}

#[test]
fn avg_numeric_values() {
    let events = make_events(&[10.0, 20.0, 30.0]);
    assert_eq!(Avg.apply(&events, Some("value")), Value::Float(20.0));
}

#[test]
fn avg_single_event() {
    let events = make_events(&[42.0]);
    assert_eq!(Avg.apply(&events, Some("value")), Value::Float(42.0));
}

#[test]
fn avg_with_missing_fields() {
    let events = vec![
        Event::new("Test").with_field("value", 10.0),
        Event::new("Test").with_field("other", 100.0),
        Event::new("Test").with_field("value", 30.0),
    ];
    // Average of [10.0, 30.0] = 20.0 (the middle event is skipped)
    assert_eq!(Avg.apply(&events, Some("value")), Value::Float(20.0));
}

#[test]
fn avg_defaults_to_value_field() {
    let events = make_events(&[100.0, 200.0]);
    assert_eq!(Avg.apply(&events, None), Value::Float(150.0));
}

// ==========================================================================
// 4. Min
// ==========================================================================

#[test]
fn min_empty_events() {
    let events: Vec<Event> = vec![];
    assert_eq!(Min.apply(&events, Some("value")), Value::Null);
}

#[test]
fn min_numeric_values() {
    let events = make_events(&[30.0, 10.0, 20.0]);
    assert_eq!(Min.apply(&events, Some("value")), Value::Float(10.0));
}

#[test]
fn min_single_event() {
    let events = make_events(&[55.5]);
    assert_eq!(Min.apply(&events, Some("value")), Value::Float(55.5));
}

#[test]
fn min_with_negative_values() {
    let events = make_events(&[-5.0, 0.0, 5.0, -10.0]);
    assert_eq!(Min.apply(&events, Some("value")), Value::Float(-10.0));
}

#[test]
fn min_all_same_value() {
    let events = make_events(&[7.0, 7.0, 7.0]);
    assert_eq!(Min.apply(&events, Some("value")), Value::Float(7.0));
}

// ==========================================================================
// 5. Max
// ==========================================================================

#[test]
fn max_empty_events() {
    let events: Vec<Event> = vec![];
    assert_eq!(Max.apply(&events, Some("value")), Value::Null);
}

#[test]
fn max_numeric_values() {
    let events = make_events(&[10.0, 30.0, 20.0]);
    assert_eq!(Max.apply(&events, Some("value")), Value::Float(30.0));
}

#[test]
fn max_single_event() {
    let events = make_events(&[55.5]);
    assert_eq!(Max.apply(&events, Some("value")), Value::Float(55.5));
}

#[test]
fn max_with_negative_values() {
    let events = make_events(&[-5.0, -1.0, -10.0]);
    assert_eq!(Max.apply(&events, Some("value")), Value::Float(-1.0));
}

#[test]
fn max_all_same_value() {
    let events = make_events(&[7.0, 7.0, 7.0]);
    assert_eq!(Max.apply(&events, Some("value")), Value::Float(7.0));
}

// ==========================================================================
// 6. StdDev (Welford's)
// ==========================================================================

#[test]
fn stddev_empty_events() {
    let events: Vec<Event> = vec![];
    assert_eq!(StdDev.apply(&events, Some("value")), Value::Null);
}

#[test]
fn stddev_single_value() {
    let events = make_events(&[42.0]);
    // Less than 2 values => Null
    assert_eq!(StdDev.apply(&events, Some("value")), Value::Null);
}

#[test]
fn stddev_two_values() {
    let events = make_events(&[10.0, 20.0]);
    // Sample stddev of [10, 20]: sqrt((10-15)^2 + (20-15)^2 / 1) = sqrt(50) ~ 7.071
    let result = StdDev.apply(&events, Some("value"));
    assert_float_near(&result, 7.0710678, 0.001);
}

#[test]
fn stddev_multiple_values() {
    let events = make_events(&[10.0, 20.0, 30.0]);
    // Sample stddev of [10, 20, 30] = 10.0
    let result = StdDev.apply(&events, Some("value"));
    assert_float_near(&result, 10.0, 0.01);
}

#[test]
fn stddev_identical_values() {
    let events = make_events(&[5.0, 5.0, 5.0, 5.0]);
    // Stddev of identical values = 0.0
    let result = StdDev.apply(&events, Some("value"));
    assert_float_near(&result, 0.0, 0.0001);
}

#[test]
fn stddev_with_missing_fields() {
    let events = vec![
        Event::new("Test").with_field("value", 10.0),
        Event::new("Test").with_field("other", 999.0), // no "value" field
        Event::new("Test").with_field("value", 30.0),
    ];
    // Only two valid values: [10, 30], stddev = sqrt((10-20)^2/1) = ~14.14
    let result = StdDev.apply(&events, Some("value"));
    assert_float_near(&result, 14.1421, 0.01);
}

// ==========================================================================
// 7. First
// ==========================================================================

#[test]
fn first_empty_events() {
    let events: Vec<Event> = vec![];
    assert_eq!(First.apply(&events, Some("value")), Value::Null);
}

#[test]
fn first_single_event() {
    let events = make_events(&[42.0]);
    assert_eq!(First.apply(&events, Some("value")), Value::Float(42.0));
}

#[test]
fn first_multiple_events() {
    let events = make_events(&[100.0, 200.0, 300.0]);
    assert_eq!(First.apply(&events, Some("value")), Value::Float(100.0));
}

#[test]
fn first_with_string_values() {
    let events = vec![
        Event::new("Test").with_field("name", "Alice"),
        Event::new("Test").with_field("name", "Bob"),
    ];
    assert_eq!(
        First.apply(&events, Some("name")),
        Value::Str("Alice".into())
    );
}

#[test]
fn first_missing_field_returns_null() {
    let events = vec![Event::new("Test").with_field("other", 10.0)];
    assert_eq!(First.apply(&events, Some("value")), Value::Null);
}

// ==========================================================================
// 8. Last
// ==========================================================================

#[test]
fn last_empty_events() {
    let events: Vec<Event> = vec![];
    assert_eq!(Last.apply(&events, Some("value")), Value::Null);
}

#[test]
fn last_single_event() {
    let events = make_events(&[42.0]);
    assert_eq!(Last.apply(&events, Some("value")), Value::Float(42.0));
}

#[test]
fn last_multiple_events() {
    let events = make_events(&[100.0, 200.0, 300.0]);
    assert_eq!(Last.apply(&events, Some("value")), Value::Float(300.0));
}

#[test]
fn last_with_string_values() {
    let events = vec![
        Event::new("Test").with_field("name", "Alice"),
        Event::new("Test").with_field("name", "Bob"),
    ];
    assert_eq!(Last.apply(&events, Some("name")), Value::Str("Bob".into()));
}

#[test]
fn last_missing_field_returns_null() {
    let events = vec![Event::new("Test").with_field("other", 10.0)];
    assert_eq!(Last.apply(&events, Some("value")), Value::Null);
}

// ==========================================================================
// 9. CountDistinct
// ==========================================================================

#[test]
fn count_distinct_empty() {
    let events: Vec<Event> = vec![];
    assert_eq!(CountDistinct.apply(&events, Some("value")), Value::Int(0));
}

#[test]
fn count_distinct_all_unique() {
    let events = vec![
        Event::new("Test").with_field("category", "A"),
        Event::new("Test").with_field("category", "B"),
        Event::new("Test").with_field("category", "C"),
    ];
    assert_eq!(
        CountDistinct.apply(&events, Some("category")),
        Value::Int(3)
    );
}

#[test]
fn count_distinct_with_duplicates() {
    let events = vec![
        Event::new("Test").with_field("category", "A"),
        Event::new("Test").with_field("category", "B"),
        Event::new("Test").with_field("category", "A"),
        Event::new("Test").with_field("category", "C"),
        Event::new("Test").with_field("category", "B"),
        Event::new("Test").with_field("category", "A"),
    ];
    assert_eq!(
        CountDistinct.apply(&events, Some("category")),
        Value::Int(3)
    );
}

#[test]
fn count_distinct_numeric_values() {
    let events = make_events(&[1.0, 2.0, 1.0, 3.0, 2.0]);
    assert_eq!(CountDistinct.apply(&events, Some("value")), Value::Int(3));
}

#[test]
fn count_distinct_single_event() {
    let events = vec![Event::new("Test").with_field("value", "X")];
    assert_eq!(CountDistinct.apply(&events, Some("value")), Value::Int(1));
}

#[test]
fn count_distinct_missing_fields_excluded() {
    let events = vec![
        Event::new("Test").with_field("value", "A"),
        Event::new("Test").with_field("other", 99.0), // no "value" field
        Event::new("Test").with_field("value", "B"),
    ];
    // Only two events have "value" => 2 distinct
    assert_eq!(CountDistinct.apply(&events, Some("value")), Value::Int(2));
}

#[test]
fn count_distinct_all_same() {
    let events = vec![
        Event::new("Test").with_field("value", "X"),
        Event::new("Test").with_field("value", "X"),
        Event::new("Test").with_field("value", "X"),
    ];
    assert_eq!(CountDistinct.apply(&events, Some("value")), Value::Int(1));
}

// ==========================================================================
// 10. Ema (Exponential Moving Average)
// ==========================================================================

#[test]
fn ema_empty_events() {
    let events: Vec<Event> = vec![];
    assert_eq!(Ema::new(3).apply(&events, Some("value")), Value::Null);
}

#[test]
fn ema_single_event() {
    let events = make_events(&[100.0]);
    // EMA of a single event is the event's value
    assert_eq!(
        Ema::new(3).apply(&events, Some("value")),
        Value::Float(100.0)
    );
}

#[test]
fn ema_period_zero_clamps_to_one() {
    let ema = Ema::new(0);
    assert_eq!(ema.period, 1);
}

#[test]
fn ema_period_one() {
    // k = 2 / (1 + 1) = 1.0 => each new value fully replaces EMA
    let events = make_events(&[10.0, 20.0, 30.0]);
    let result = Ema::new(1).apply(&events, Some("value"));
    // With k=1, EMA becomes the last value
    assert_eq!(result, Value::Float(30.0));
}

#[test]
fn ema_period_three() {
    // k = 2 / (3 + 1) = 0.5
    let events = make_events(&[100.0, 110.0, 120.0, 130.0, 140.0]);
    let result = Ema::new(3).apply(&events, Some("value"));
    // Step by step:
    // ema[0] = 100.0
    // ema[1] = 110 * 0.5 + 100 * 0.5 = 105.0
    // ema[2] = 120 * 0.5 + 105 * 0.5 = 112.5
    // ema[3] = 130 * 0.5 + 112.5 * 0.5 = 121.25
    // ema[4] = 140 * 0.5 + 121.25 * 0.5 = 130.625
    assert_float_near(&result, 130.625, 0.001);
}

#[test]
fn ema_period_ten() {
    // k = 2 / (10 + 1) ~= 0.1818
    let events = make_events(&[50.0, 60.0, 70.0]);
    let result = Ema::new(10).apply(&events, Some("value"));
    let k = 2.0 / 11.0;
    // ema[0] = 50.0
    // ema[1] = 60 * k + 50 * (1-k)
    let ema1 = 60.0 * k + 50.0 * (1.0 - k);
    // ema[2] = 70 * k + ema1 * (1-k)
    let ema2 = 70.0 * k + ema1 * (1.0 - k);
    assert_float_near(&result, ema2, 0.001);
}

#[test]
fn ema_with_missing_fields() {
    let events = vec![
        Event::new("Test").with_field("value", 100.0),
        Event::new("Test").with_field("other", 999.0), // no "value"
        Event::new("Test").with_field("value", 200.0),
    ];
    // period=3, _k=0.5
    // ema[0] = 100.0 (first valid)
    // ema[1] skipped (no value)
    // ema[2] = 200 * 0.5 + 100 * 0.5 = 150.0
    let _k = 2.0 / 4.0; // period=3, k=0.5
    let result = Ema::new(3).apply(&events, Some("value"));
    assert_float_near(&result, 150.0, 0.001);
}

// ==========================================================================
// 11. Edge cases: null values (Value::Null in data)
// ==========================================================================

#[test]
fn sum_with_null_values() {
    let events = vec![
        Event::new("Test").with_field("value", 10.0),
        Event::new("Test").with_field("value", Value::Null),
        Event::new("Test").with_field("value", 30.0),
    ];
    // Null values should not be parsed as floats => sum of [10, 30] = 40
    // (get_float on Null returns None)
    assert_eq!(Sum.apply(&events, Some("value")), Value::Float(40.0));
}

#[test]
fn avg_with_null_values() {
    let events = vec![
        Event::new("Test").with_field("value", 10.0),
        Event::new("Test").with_field("value", Value::Null),
        Event::new("Test").with_field("value", 30.0),
    ];
    // Avg of [10, 30] = 20
    assert_eq!(Avg.apply(&events, Some("value")), Value::Float(20.0));
}

#[test]
fn min_with_null_values() {
    let events = vec![
        Event::new("Test").with_field("value", Value::Null),
        Event::new("Test").with_field("value", 25.0),
    ];
    assert_eq!(Min.apply(&events, Some("value")), Value::Float(25.0));
}

#[test]
fn max_with_null_values() {
    let events = vec![
        Event::new("Test").with_field("value", Value::Null),
        Event::new("Test").with_field("value", 25.0),
    ];
    assert_eq!(Max.apply(&events, Some("value")), Value::Float(25.0));
}

#[test]
fn first_with_null_value() {
    // First event has the field set to Null
    let events = vec![
        Event::new("Test").with_field("value", Value::Null),
        Event::new("Test").with_field("value", 10.0),
    ];
    // First.apply returns whatever is at index 0's "value" field
    assert_eq!(First.apply(&events, Some("value")), Value::Null);
}

#[test]
fn last_with_null_value() {
    let events = vec![
        Event::new("Test").with_field("value", 10.0),
        Event::new("Test").with_field("value", Value::Null),
    ];
    assert_eq!(Last.apply(&events, Some("value")), Value::Null);
}

// ==========================================================================
// 12. Large number of events
// ==========================================================================

#[test]
fn sum_large_event_set() {
    let n = 10_000;
    let events: Vec<Event> = (0..n)
        .map(|i| Event::new("Test").with_field("value", i as f64))
        .collect();
    // Sum of 0..9999 = 9999 * 10000 / 2 = 49995000
    assert_eq!(
        Sum.apply(&events, Some("value")),
        Value::Float(49_995_000.0)
    );
}

#[test]
fn avg_large_event_set() {
    let n = 10_000;
    let events: Vec<Event> = (0..n)
        .map(|i| Event::new("Test").with_field("value", i as f64))
        .collect();
    // Avg of 0..9999 = 4999.5
    assert_float_near(&Avg.apply(&events, Some("value")), 4999.5, 0.01);
}

#[test]
fn count_large_event_set() {
    let n = 10_000;
    let events: Vec<Event> = (0..n)
        .map(|_| Event::new("Test").with_field("value", 1.0))
        .collect();
    assert_eq!(Count.apply(&events, None), Value::Int(n));
}

#[test]
fn min_max_large_event_set() {
    let n = 10_000;
    let events: Vec<Event> = (0..n)
        .map(|i| Event::new("Test").with_field("value", i as f64))
        .collect();
    assert_eq!(Min.apply(&events, Some("value")), Value::Float(0.0));
    assert_eq!(
        Max.apply(&events, Some("value")),
        Value::Float((n - 1) as f64)
    );
}

#[test]
fn count_distinct_large_event_set() {
    // 10000 events with 100 distinct categories
    let events: Vec<Event> = (0..10_000)
        .map(|i| Event::new("Test").with_field("category", format!("cat_{}", i % 100)))
        .collect();
    assert_eq!(
        CountDistinct.apply(&events, Some("category")),
        Value::Int(100)
    );
}

// ==========================================================================
// 13. apply_shared variant
// ==========================================================================

#[test]
fn count_apply_shared() {
    let events = make_shared_events(&[10.0, 20.0, 30.0]);
    assert_eq!(Count.apply_shared(&events, None), Value::Int(3));
}

#[test]
fn sum_apply_shared() {
    let events = make_shared_events(&[10.0, 20.0, 30.0]);
    assert_eq!(Sum.apply_shared(&events, Some("value")), Value::Float(60.0));
}

#[test]
fn avg_apply_shared() {
    let events = make_shared_events(&[10.0, 20.0, 30.0]);
    assert_eq!(Avg.apply_shared(&events, Some("value")), Value::Float(20.0));
}

#[test]
fn min_apply_shared() {
    let events = make_shared_events(&[30.0, 10.0, 20.0]);
    assert_eq!(Min.apply_shared(&events, Some("value")), Value::Float(10.0));
}

#[test]
fn max_apply_shared() {
    let events = make_shared_events(&[10.0, 30.0, 20.0]);
    assert_eq!(Max.apply_shared(&events, Some("value")), Value::Float(30.0));
}

#[test]
fn stddev_apply_shared() {
    let events = make_shared_events(&[10.0, 20.0, 30.0]);
    let result = StdDev.apply_shared(&events, Some("value"));
    assert_float_near(&result, 10.0, 0.01);
}

#[test]
fn first_apply_shared() {
    let events = make_shared_events(&[100.0, 200.0, 300.0]);
    assert_eq!(
        First.apply_shared(&events, Some("value")),
        Value::Float(100.0)
    );
}

#[test]
fn last_apply_shared() {
    let events = make_shared_events(&[100.0, 200.0, 300.0]);
    assert_eq!(
        Last.apply_shared(&events, Some("value")),
        Value::Float(300.0)
    );
}

#[test]
fn count_distinct_apply_shared() {
    let events: Vec<Arc<Event>> = vec![
        Arc::new(Event::new("Test").with_field("cat", "A")),
        Arc::new(Event::new("Test").with_field("cat", "B")),
        Arc::new(Event::new("Test").with_field("cat", "A")),
    ];
    assert_eq!(
        CountDistinct.apply_shared(&events, Some("cat")),
        Value::Int(2)
    );
}

#[test]
fn ema_apply_shared() {
    let events = make_shared_events(&[100.0, 110.0, 120.0]);
    // period=3, k=0.5
    // ema[0] = 100.0
    // ema[1] = 110*0.5 + 100*0.5 = 105.0
    // ema[2] = 120*0.5 + 105*0.5 = 112.5
    let result = Ema::new(3).apply_shared(&events, Some("value"));
    assert_float_near(&result, 112.5, 0.001);
}

#[test]
fn apply_shared_empty() {
    let events: Vec<Arc<Event>> = vec![];
    assert_eq!(Count.apply_shared(&events, None), Value::Int(0));
    assert_eq!(Sum.apply_shared(&events, Some("value")), Value::Float(0.0));
    assert_eq!(Avg.apply_shared(&events, Some("value")), Value::Null);
    assert_eq!(Min.apply_shared(&events, Some("value")), Value::Null);
    assert_eq!(Max.apply_shared(&events, Some("value")), Value::Null);
    assert_eq!(First.apply_shared(&events, Some("value")), Value::Null);
    assert_eq!(Last.apply_shared(&events, Some("value")), Value::Null);
    assert_eq!(StdDev.apply_shared(&events, Some("value")), Value::Null);
    assert_eq!(
        Ema::new(3).apply_shared(&events, Some("value")),
        Value::Null
    );
    assert_eq!(
        CountDistinct.apply_shared(&events, Some("value")),
        Value::Int(0)
    );
}

// ==========================================================================
// 14. apply_refs variant
// ==========================================================================

#[test]
fn count_apply_refs() {
    let events = make_events(&[10.0, 20.0, 30.0]);
    let refs: Vec<&Event> = events.iter().collect();
    assert_eq!(Count.apply_refs(&refs, None), Value::Int(3));
}

#[test]
fn sum_apply_refs() {
    let events = make_events(&[10.0, 20.0, 30.0]);
    let refs: Vec<&Event> = events.iter().collect();
    assert_eq!(Sum.apply_refs(&refs, Some("value")), Value::Float(60.0));
}

#[test]
fn avg_apply_refs() {
    let events = make_events(&[10.0, 20.0, 30.0]);
    let refs: Vec<&Event> = events.iter().collect();
    assert_eq!(Avg.apply_refs(&refs, Some("value")), Value::Float(20.0));
}

#[test]
fn min_max_apply_refs() {
    let events = make_events(&[30.0, 10.0, 20.0]);
    let refs: Vec<&Event> = events.iter().collect();
    assert_eq!(Min.apply_refs(&refs, Some("value")), Value::Float(10.0));
    assert_eq!(Max.apply_refs(&refs, Some("value")), Value::Float(30.0));
}

#[test]
fn stddev_apply_refs() {
    let events = make_events(&[10.0, 20.0, 30.0]);
    let refs: Vec<&Event> = events.iter().collect();
    let result = StdDev.apply_refs(&refs, Some("value"));
    assert_float_near(&result, 10.0, 0.01);
}

#[test]
fn first_last_apply_refs() {
    let events = make_events(&[100.0, 200.0, 300.0]);
    let refs: Vec<&Event> = events.iter().collect();
    assert_eq!(First.apply_refs(&refs, Some("value")), Value::Float(100.0));
    assert_eq!(Last.apply_refs(&refs, Some("value")), Value::Float(300.0));
}

#[test]
fn count_distinct_apply_refs() {
    let events = [
        Event::new("Test").with_field("cat", "A"),
        Event::new("Test").with_field("cat", "B"),
        Event::new("Test").with_field("cat", "A"),
    ];
    let refs: Vec<&Event> = events.iter().collect();
    assert_eq!(CountDistinct.apply_refs(&refs, Some("cat")), Value::Int(2));
}

#[test]
fn ema_apply_refs() {
    let events = make_events(&[100.0, 110.0, 120.0]);
    let refs: Vec<&Event> = events.iter().collect();
    let result = Ema::new(3).apply_refs(&refs, Some("value"));
    assert_float_near(&result, 112.5, 0.001);
}

// ==========================================================================
// 15. ExprAggregate binary expressions
// ==========================================================================

#[test]
fn expr_last_minus_ema() {
    let events = make_events(&[100.0, 110.0, 120.0, 130.0, 140.0]);
    let expr = ExprAggregate::new(
        Box::new(Last),
        Some("value".to_string()),
        AggBinOp::Sub,
        Box::new(Ema::new(3)),
        Some("value".to_string()),
    );
    // last = 140.0
    // ema(3): k=0.5 => 100, 105, 112.5, 121.25, 130.625
    // 140.0 - 130.625 = 9.375
    let result = expr.apply(&events, None);
    assert_float_near(&result, 9.375, 0.001);
}

#[test]
fn expr_sum_plus_count() {
    let events = make_events(&[10.0, 20.0, 30.0]);
    let expr = ExprAggregate::new(
        Box::new(Sum),
        Some("value".to_string()),
        AggBinOp::Add,
        Box::new(Count),
        None,
    );
    // sum=60.0 (Float) + count=3 (Int) => 63.0 (Float)
    assert_eq!(expr.apply(&events, None), Value::Float(63.0));
}

#[test]
fn expr_max_minus_min() {
    let events = make_events(&[5.0, 15.0, 25.0]);
    let expr = ExprAggregate::new(
        Box::new(Max),
        Some("value".to_string()),
        AggBinOp::Sub,
        Box::new(Min),
        Some("value".to_string()),
    );
    // 25.0 - 5.0 = 20.0
    assert_eq!(expr.apply(&events, None), Value::Float(20.0));
}

#[test]
fn expr_count_mul_count() {
    let events = make_events(&[1.0, 2.0, 3.0]);
    let expr = ExprAggregate::new(Box::new(Count), None, AggBinOp::Mul, Box::new(Count), None);
    // Int * Int: 3 * 3 = 9
    assert_eq!(expr.apply(&events, None), Value::Int(9));
}

#[test]
fn expr_count_div_count() {
    let events = make_events(&[1.0, 2.0, 3.0, 4.0]);
    let expr = ExprAggregate::new(Box::new(Count), None, AggBinOp::Div, Box::new(Count), None);
    // Int / Int: 4 / 4 = 1
    assert_eq!(expr.apply(&events, None), Value::Int(1));
}

#[test]
fn expr_int_div_by_zero() {
    let events: Vec<Event> = vec![];
    let expr = ExprAggregate::new(Box::new(Count), None, AggBinOp::Div, Box::new(Count), None);
    // 0 / 0 for integers => 0 (as defined in the implementation)
    assert_eq!(expr.apply(&events, None), Value::Int(0));
}

#[test]
fn expr_float_div_by_zero() {
    let events: Vec<Event> = vec![];
    let expr = ExprAggregate::new(
        Box::new(Sum),
        Some("value".to_string()),
        AggBinOp::Div,
        Box::new(Sum),
        Some("value".to_string()),
    );
    // 0.0 / 0.0 => NaN
    if let Value::Float(v) = expr.apply(&events, None) {
        assert!(v.is_nan());
    } else {
        panic!("Expected Value::Float(NaN)");
    }
}

#[test]
fn expr_null_operand_returns_null() {
    // Avg of empty => Null; combining Null with anything => Null
    let events: Vec<Event> = vec![];
    let expr = ExprAggregate::new(
        Box::new(Avg),
        Some("value".to_string()),
        AggBinOp::Add,
        Box::new(Count),
        None,
    );
    // Null + Int(0) => Null (no matching arm)
    assert_eq!(expr.apply(&events, None), Value::Null);
}

#[test]
fn expr_int_float_mixed() {
    // Count (Int) + Sum (Float)
    let events = make_events(&[10.0, 20.0]);
    let expr = ExprAggregate::new(
        Box::new(Count),
        None,
        AggBinOp::Add,
        Box::new(Sum),
        Some("value".to_string()),
    );
    // Int(2) + Float(30.0) => Float(32.0)
    assert_eq!(expr.apply(&events, None), Value::Float(32.0));
}

#[test]
fn expr_float_int_mixed() {
    // Sum (Float) + Count (Int) — tests Float+Int arm
    let events = make_events(&[10.0, 20.0]);
    let expr = ExprAggregate::new(
        Box::new(Sum),
        Some("value".to_string()),
        AggBinOp::Mul,
        Box::new(Count),
        None,
    );
    // Float(30.0) * Int(2) => Float(60.0)
    assert_eq!(expr.apply(&events, None), Value::Float(60.0));
}

#[test]
fn expr_apply_refs() {
    let events = make_events(&[10.0, 20.0, 30.0]);
    let refs: Vec<&Event> = events.iter().collect();
    let expr = ExprAggregate::new(
        Box::new(Max),
        Some("value".to_string()),
        AggBinOp::Sub,
        Box::new(Min),
        Some("value".to_string()),
    );
    // 30.0 - 10.0 = 20.0
    assert_eq!(expr.apply_refs(&refs, None), Value::Float(20.0));
}

#[test]
fn expr_apply_refs_int_int() {
    let events = make_events(&[1.0, 2.0, 3.0]);
    let refs: Vec<&Event> = events.iter().collect();
    let expr = ExprAggregate::new(Box::new(Count), None, AggBinOp::Add, Box::new(Count), None);
    assert_eq!(expr.apply_refs(&refs, None), Value::Int(6));
}

#[test]
fn expr_apply_refs_int_div_by_zero() {
    let events: Vec<Event> = vec![];
    let refs: Vec<&Event> = events.iter().collect();
    let expr = ExprAggregate::new(Box::new(Count), None, AggBinOp::Div, Box::new(Count), None);
    assert_eq!(expr.apply_refs(&refs, None), Value::Int(0));
}

#[test]
fn expr_apply_refs_float_div_by_zero() {
    let events: Vec<Event> = vec![];
    let refs: Vec<&Event> = events.iter().collect();
    let expr = ExprAggregate::new(
        Box::new(Sum),
        Some("value".to_string()),
        AggBinOp::Div,
        Box::new(Sum),
        Some("value".to_string()),
    );
    if let Value::Float(v) = expr.apply_refs(&refs, None) {
        assert!(v.is_nan());
    } else {
        panic!("Expected Value::Float(NaN)");
    }
}

#[test]
fn expr_apply_refs_null_operand() {
    let events: Vec<Event> = vec![];
    let refs: Vec<&Event> = events.iter().collect();
    let expr = ExprAggregate::new(
        Box::new(Avg),
        Some("value".to_string()),
        AggBinOp::Sub,
        Box::new(Min),
        Some("value".to_string()),
    );
    // Null - Null => Null
    assert_eq!(expr.apply_refs(&refs, None), Value::Null);
}

#[test]
fn expr_apply_refs_int_float_mixed() {
    let events = make_events(&[10.0, 20.0]);
    let refs: Vec<&Event> = events.iter().collect();
    let expr = ExprAggregate::new(
        Box::new(Count),
        None,
        AggBinOp::Sub,
        Box::new(Sum),
        Some("value".to_string()),
    );
    // Int(2) - Float(30.0) => Float(-28.0)
    assert_eq!(expr.apply_refs(&refs, None), Value::Float(-28.0));
}

#[test]
fn expr_apply_refs_float_int_mixed() {
    let events = make_events(&[10.0, 20.0]);
    let refs: Vec<&Event> = events.iter().collect();
    let expr = ExprAggregate::new(
        Box::new(Sum),
        Some("value".to_string()),
        AggBinOp::Div,
        Box::new(Count),
        None,
    );
    // Float(30.0) / Int(2) => Float(15.0)
    assert_eq!(expr.apply_refs(&refs, None), Value::Float(15.0));
}

#[test]
fn expr_apply_refs_int_float_div_by_zero() {
    let events: Vec<Event> = vec![];
    let refs: Vec<&Event> = events.iter().collect();
    let expr = ExprAggregate::new(
        Box::new(Count),
        None,
        AggBinOp::Div,
        Box::new(Sum),
        Some("value".to_string()),
    );
    // Int(0) / Float(0.0) => Float(NaN) [Int/Float arm, r==0.0]
    if let Value::Float(v) = expr.apply_refs(&refs, None) {
        assert!(v.is_nan());
    } else {
        panic!("Expected Value::Float(NaN)");
    }
}

#[test]
fn expr_apply_refs_float_int_div_by_zero() {
    let events: Vec<Event> = vec![];
    let refs: Vec<&Event> = events.iter().collect();
    let expr = ExprAggregate::new(
        Box::new(Sum),
        Some("value".to_string()),
        AggBinOp::Div,
        Box::new(Count),
        None,
    );
    // Float(0.0) / Int(0) => Float(NaN) [Float/Int arm, r==0.0]
    if let Value::Float(v) = expr.apply_refs(&refs, None) {
        assert!(v.is_nan());
    } else {
        panic!("Expected Value::Float(NaN)");
    }
}

// ==========================================================================
// 16. Aggregation function names
// ==========================================================================

#[test]
fn all_names_are_correct() {
    assert_eq!(Count.name(), "count");
    assert_eq!(Sum.name(), "sum");
    assert_eq!(Avg.name(), "avg");
    assert_eq!(Min.name(), "min");
    assert_eq!(Max.name(), "max");
    assert_eq!(StdDev.name(), "stddev");
    assert_eq!(First.name(), "first");
    assert_eq!(Last.name(), "last");
    assert_eq!(CountDistinct.name(), "count_distinct");
    assert_eq!(Ema::new(5).name(), "ema");
    let expr = ExprAggregate::new(Box::new(Sum), None, AggBinOp::Add, Box::new(Count), None);
    assert_eq!(expr.name(), "expr");
}

// ==========================================================================
// 17. NaN handling
// ==========================================================================

#[test]
fn sum_with_nan_values() {
    let events = vec![
        Event::new("Test").with_field("value", f64::NAN),
        Event::new("Test").with_field("value", 10.0),
        Event::new("Test").with_field("value", f64::NAN),
        Event::new("Test").with_field("value", 20.0),
    ];
    // NaN values should be filtered; sum = 30
    assert_eq!(Sum.apply(&events, Some("value")), Value::Float(30.0));
}

#[test]
fn avg_with_nan_values() {
    let events = vec![
        Event::new("Test").with_field("value", f64::NAN),
        Event::new("Test").with_field("value", 10.0),
        Event::new("Test").with_field("value", 20.0),
    ];
    // NaN filtered; avg of [10, 20] = 15
    assert_eq!(Avg.apply(&events, Some("value")), Value::Float(15.0));
}

#[test]
fn min_with_nan_filters_correctly() {
    let events = vec![
        Event::new("Test").with_field("value", f64::NAN),
        Event::new("Test").with_field("value", 50.0),
        Event::new("Test").with_field("value", 10.0),
    ];
    assert_eq!(Min.apply(&events, Some("value")), Value::Float(10.0));
}

#[test]
fn max_with_nan_filters_correctly() {
    let events = vec![
        Event::new("Test").with_field("value", f64::NAN),
        Event::new("Test").with_field("value", 50.0),
        Event::new("Test").with_field("value", 10.0),
    ];
    assert_eq!(Max.apply(&events, Some("value")), Value::Float(50.0));
}

#[test]
fn min_all_nan_returns_null() {
    let events = vec![
        Event::new("Test").with_field("value", f64::NAN),
        Event::new("Test").with_field("value", f64::NAN),
    ];
    assert_eq!(Min.apply(&events, Some("value")), Value::Null);
}

#[test]
fn max_all_nan_returns_null() {
    let events = vec![
        Event::new("Test").with_field("value", f64::NAN),
        Event::new("Test").with_field("value", f64::NAN),
    ];
    assert_eq!(Max.apply(&events, Some("value")), Value::Null);
}

// ==========================================================================
// 18. Aggregator composite tests
// ==========================================================================

#[test]
fn aggregator_apply_shared() {
    let events = make_shared_events(&[10.0, 20.0, 30.0]);
    let aggregator = Aggregator::new()
        .add("count", Box::new(Count), None)
        .add("sum", Box::new(Sum), Some("value".to_string()))
        .add("avg", Box::new(Avg), Some("value".to_string()));

    let result = aggregator.apply_shared(&events);
    assert_eq!(result.get("count"), Some(&Value::Int(3)));
    assert_eq!(result.get("sum"), Some(&Value::Float(60.0)));
    assert_eq!(result.get("avg"), Some(&Value::Float(20.0)));
}

#[test]
fn aggregator_apply_columnar() {
    let events = vec![
        Arc::new(Event::new("Test").with_field("value", 10.0)),
        Arc::new(Event::new("Test").with_field("value", 20.0)),
        Arc::new(Event::new("Test").with_field("value", 30.0)),
    ];
    let mut buffer = ColumnarBuffer::from_events(events);

    let aggregator = Aggregator::new()
        .add("count", Box::new(Count), None)
        .add("sum", Box::new(Sum), Some("value".to_string()))
        .add("avg", Box::new(Avg), Some("value".to_string()))
        .add("min", Box::new(Min), Some("value".to_string()))
        .add("max", Box::new(Max), Some("value".to_string()));

    let result = aggregator.apply_columnar(&mut buffer);
    assert_eq!(result.get("count"), Some(&Value::Int(3)));
    assert_eq!(result.get("sum"), Some(&Value::Float(60.0)));
    assert_eq!(result.get("avg"), Some(&Value::Float(20.0)));
    assert_eq!(result.get("min"), Some(&Value::Float(10.0)));
    assert_eq!(result.get("max"), Some(&Value::Float(30.0)));
}

#[test]
fn aggregator_default_is_empty() {
    let agg = Aggregator::default();
    let events = make_events(&[1.0]);
    let result = agg.apply(&events);
    assert!(result.is_empty());
}

// ==========================================================================
// 19. Default field ("value") behavior
// ==========================================================================

#[test]
fn all_aggs_default_to_value_field() {
    let events = vec![
        Event::new("Test").with_field("value", 10.0),
        Event::new("Test").with_field("value", 20.0),
        Event::new("Test").with_field("value", 30.0),
    ];

    // When field=None, aggregations should use "value" as default
    assert_eq!(Sum.apply(&events, None), Value::Float(60.0));
    assert_eq!(Avg.apply(&events, None), Value::Float(20.0));
    assert_eq!(Min.apply(&events, None), Value::Float(10.0));
    assert_eq!(Max.apply(&events, None), Value::Float(30.0));
    assert_eq!(First.apply(&events, None), Value::Float(10.0));
    assert_eq!(Last.apply(&events, None), Value::Float(30.0));
    let stddev_result = StdDev.apply(&events, None);
    assert_float_near(&stddev_result, 10.0, 0.01);
    let ema_result = Ema::new(3).apply(&events, None);
    // EMA(3) k=0.5: 10, 15, 22.5
    assert_float_near(&ema_result, 22.5, 0.001);
}
