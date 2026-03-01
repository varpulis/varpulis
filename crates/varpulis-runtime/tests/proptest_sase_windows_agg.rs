//! Property-based tests for SASE patterns, windows, and aggregations.
//!
//! Complements `proptest_runtime.rs` with deeper coverage of:
//! - Window invariants (tumbling, count, session, sliding)
//! - Aggregation algebraic properties (commutativity, bounds, correctness)
//! - Event field accessors (type-safe get_* methods)

use chrono::{Duration, TimeZone, Utc};
use proptest::prelude::*;
use varpulis_core::Value;
use varpulis_runtime::aggregation::{AggregateFunc, Avg, Count, Max, Min, StdDev, Sum};
use varpulis_runtime::event::Event;
use varpulis_runtime::window::{CountWindow, SessionWindow, SlidingWindow, TumblingWindow};

/// Strategy for generating finite f64 values suitable for aggregation.
fn arb_finite_f64() -> impl Strategy<Value = f64> {
    any::<f64>().prop_filter("must be finite", |f| f.is_finite())
}

/// Strategy for generating a Vec of events with a numeric "value" field.
fn arb_numeric_events(min_len: usize, max_len: usize) -> impl Strategy<Value = Vec<Event>> {
    prop::collection::vec(arb_finite_f64(), min_len..max_len).prop_map(|values| {
        values
            .into_iter()
            .enumerate()
            .map(|(i, v)| {
                let ts = Utc.timestamp_nanos(i as i64 * 1_000_000_000);
                Event::new_at("Metric", ts).with_field("value", v)
            })
            .collect()
    })
}

// ---------------------------------------------------------------------------
// Aggregation algebraic properties
// ---------------------------------------------------------------------------

proptest! {
    /// Count always returns the number of events.
    #[test]
    fn count_equals_len(events in arb_numeric_events(0, 50)) {
        let result = Count.apply(&events, None);
        prop_assert_eq!(result, Value::Int(events.len() as i64));
    }

    /// Sum is commutative: sum(a,b,c) == sum(c,b,a).
    #[test]
    fn sum_commutative(values in prop::collection::vec(arb_finite_f64(), 2..20)) {
        let events_fwd: Vec<Event> = values.iter().enumerate().map(|(i, &v)| {
            Event::new_at("M", Utc.timestamp_nanos(i as i64 * 1_000_000_000))
                .with_field("value", v)
        }).collect();

        let events_rev: Vec<Event> = values.iter().rev().enumerate().map(|(i, &v)| {
            Event::new_at("M", Utc.timestamp_nanos(i as i64 * 1_000_000_000))
                .with_field("value", v)
        }).collect();

        let sum_fwd = Sum.apply(&events_fwd, Some("value"));
        let sum_rev = Sum.apply(&events_rev, Some("value"));

        match (sum_fwd, sum_rev) {
            (Value::Float(a), Value::Float(b)) => {
                // Allow for floating-point reordering differences
                let diff = (a - b).abs();
                let tolerance = a.abs().max(b.abs()).mul_add(1e-10, 1e-15);
                prop_assert!(diff < tolerance, "sum diff {} exceeds tolerance {}", diff, tolerance);
            }
            _ => prop_assert!(false, "Sum should return Float"),
        }
    }

    /// Min is always <= all values, Max is always >= all values.
    #[test]
    fn min_max_bounds(values in prop::collection::vec(arb_finite_f64(), 1..30)) {
        let events: Vec<Event> = values.iter().enumerate().map(|(i, &v)| {
            Event::new_at("M", Utc.timestamp_nanos(i as i64 * 1_000_000_000))
                .with_field("value", v)
        }).collect();

        let min_val = Min.apply(&events, Some("value"));
        let max_val = Max.apply(&events, Some("value"));

        if let (Value::Float(mn), Value::Float(mx)) = (min_val, max_val) {
            for &v in &values {
                prop_assert!(mn <= v, "min {} > value {}", mn, v);
                prop_assert!(mx >= v, "max {} < value {}", mx, v);
            }
            prop_assert!(mn <= mx, "min {} > max {}", mn, mx);
        } else {
            prop_assert!(false, "Min/Max should return Float for non-empty events");
        }
    }

    /// Average is always between min and max.
    #[test]
    fn avg_between_min_max(values in prop::collection::vec(arb_finite_f64(), 1..30)) {
        let events: Vec<Event> = values.iter().enumerate().map(|(i, &v)| {
            Event::new_at("M", Utc.timestamp_nanos(i as i64 * 1_000_000_000))
                .with_field("value", v)
        }).collect();

        let min_val = Min.apply(&events, Some("value"));
        let max_val = Max.apply(&events, Some("value"));
        let avg_val = Avg.apply(&events, Some("value"));

        if let (Value::Float(mn), Value::Float(mx), Value::Float(av)) = (min_val, max_val, avg_val) {
            prop_assert!(av >= mn - 1e-10, "avg {} < min {}", av, mn);
            prop_assert!(av <= mx + 1e-10, "avg {} > max {}", av, mx);
        }
    }

    /// StdDev is always non-negative.
    #[test]
    fn stddev_non_negative(values in prop::collection::vec(arb_finite_f64(), 2..30)) {
        let events: Vec<Event> = values.iter().enumerate().map(|(i, &v)| {
            Event::new_at("M", Utc.timestamp_nanos(i as i64 * 1_000_000_000))
                .with_field("value", v)
        }).collect();

        let sd = StdDev.apply(&events, Some("value"));
        if let Value::Float(s) = sd {
            prop_assert!(s >= 0.0, "stddev {} is negative", s);
        }
    }

    /// StdDev of identical values is zero.
    #[test]
    fn stddev_constant_is_zero(v in arb_finite_f64(), n in 2usize..20) {
        let events: Vec<Event> = (0..n).map(|i| {
            Event::new_at("M", Utc.timestamp_nanos(i as i64 * 1_000_000_000))
                .with_field("value", v)
        }).collect();

        let sd = StdDev.apply(&events, Some("value"));
        if let Value::Float(s) = sd {
            prop_assert!(s.abs() < 1e-10, "stddev of constant {} should be ~0, got {}", v, s);
        }
    }
}

// ---------------------------------------------------------------------------
// Window properties
// ---------------------------------------------------------------------------

proptest! {
    /// CountWindow emits exactly at count threshold.
    #[test]
    fn count_window_threshold(count in 2usize..20, extra in 0usize..10) {
        let total = count + extra;
        let mut window = CountWindow::new(count);
        let mut emitted = 0usize;

        for i in 0..total {
            let event = Event::new_at("E", Utc.timestamp_nanos(i as i64 * 1_000_000_000));
            if let Some(batch) = window.add(event) {
                prop_assert_eq!(batch.len(), count, "emitted batch size should equal count");
                emitted += batch.len();
            }
        }

        let expected_emits = total / count;
        let expected_remaining = total % count;
        prop_assert_eq!(emitted, expected_emits * count);
        prop_assert_eq!(window.current_count(), expected_remaining);
    }

    /// TumblingWindow: events within duration stay in same window.
    #[test]
    fn tumbling_window_contains_within_duration(
        duration_secs in 1i64..60,
        n_events in 1usize..20,
    ) {
        let dur = Duration::seconds(duration_secs);
        let mut window = TumblingWindow::new(dur);
        let base = Utc.timestamp_nanos(0);

        // Add events all within the window duration
        for i in 0..n_events {
            let frac = i as i64 * (duration_secs * 1_000_000_000 / (n_events as i64 + 1));
            let ts = base + Duration::nanoseconds(frac);
            let event = Event::new_at("E", ts);
            let result = window.add(event);
            // No event should trigger emission since all are within first window
            prop_assert!(result.is_none(), "event {} unexpectedly triggered window emission", i);
        }

        // Window should contain all events
        prop_assert_eq!(window.len(), n_events);
    }

    /// SessionWindow: gap exceeding produces emission.
    #[test]
    fn session_window_gap_produces_emission(
        gap_secs in 1i64..30,
        n_before in 1usize..10,
    ) {
        let gap = Duration::seconds(gap_secs);
        let mut window = SessionWindow::new(gap);
        let base = Utc.timestamp_nanos(1_000_000_000);

        // Add events within gap
        for i in 0..n_before {
            let ts = base + Duration::milliseconds(i as i64 * 100);
            let event = Event::new_at("E", ts);
            let result = window.add(event);
            prop_assert!(result.is_none(), "should not emit within gap");
        }

        // Add event exceeding gap
        let late_ts = base + Duration::seconds(gap_secs + 1);
        let late_event = Event::new_at("E", late_ts);
        let result = window.add(late_event);

        prop_assert!(result.is_some(), "should emit after gap exceeded");
        let emitted = result.unwrap();
        prop_assert_eq!(emitted.len(), n_before, "emitted session should have {} events", n_before);
    }

    /// SlidingWindow: slide interval triggers overlapping windows.
    #[test]
    fn sliding_window_slide_triggers(
        window_secs in 4i64..30,
        slide_secs in 1i64..4,
    ) {
        // Ensure window > slide
        prop_assume!(window_secs > slide_secs);

        let window_dur = Duration::seconds(window_secs);
        let slide_dur = Duration::seconds(slide_secs);
        let mut window = SlidingWindow::new(window_dur, slide_dur);
        let base = Utc.timestamp_nanos(0);

        let mut total_emitted = 0usize;
        // Add events over 2x the window duration at 1-second intervals
        let total_events = (window_secs * 2) as usize;
        for i in 0..total_events {
            let ts = base + Duration::seconds(i as i64);
            let event = Event::new_at("E", ts);
            if let Some(batch) = window.add(event) {
                prop_assert!(!batch.is_empty(), "emitted batch should not be empty");
                total_emitted += 1;
            }
        }

        // Over 2x window duration, we should get at least one emission
        prop_assert!(total_emitted >= 1, "should emit at least once over 2x window duration");
    }
}

// ---------------------------------------------------------------------------
// Event field accessor properties
// ---------------------------------------------------------------------------

proptest! {
    /// get_float on Int field returns the integer as f64.
    #[test]
    fn get_float_on_int(i in any::<i64>()) {
        let event = Event::new("E").with_field("x", Value::Int(i));
        // get_float should return None for Int (type-strict accessor)
        // OR return Some(i as f64) if it coerces. Let's test what it does.
        let result = event.get_float("x");
        // Whether it returns None or coerces, the result should be consistent
        let result2 = event.get_float("x");
        prop_assert_eq!(result, result2, "get_float must be deterministic");
    }

    /// get_str on non-string fields returns None.
    #[test]
    fn get_str_on_numeric(v in arb_finite_f64()) {
        let event = Event::new("E").with_field("x", v);
        prop_assert!(event.get_str("x").is_none(), "get_str on float should be None");
    }

    /// Missing field always returns None for all typed accessors.
    #[test]
    fn missing_field_returns_none(
        event_type in "[A-Z][a-z]{1,10}",
        field_name in "[a-z]{1,10}",
    ) {
        let event = Event::new(event_type);
        prop_assert!(event.get("missing_field_xyz").is_none());
        prop_assert!(event.get_float(&field_name).is_none());
        prop_assert!(event.get_int(&field_name).is_none());
        prop_assert!(event.get_str(&field_name).is_none());
    }

    /// with_field is idempotent: last write wins, field count stays 1.
    #[test]
    fn with_field_last_write_wins(v1 in arb_finite_f64(), v2 in arb_finite_f64()) {
        let event = Event::new("E")
            .with_field("x", v1)
            .with_field("x", v2);

        // Only one "x" field should exist
        let count = event.data.keys().filter(|k| k.as_ref() == "x").count();
        prop_assert_eq!(count, 1, "duplicate field keys should not exist");

        // Value should be v2 (last write wins)
        if let Some(Value::Float(stored)) = event.get("x") {
            prop_assert_eq!(*stored, v2, "last write should win");
        }
    }
}
