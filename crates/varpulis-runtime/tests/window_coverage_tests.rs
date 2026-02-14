//! Additional coverage tests for window.rs
//!
//! Targets uncovered paths in:
//! - PartitionedTumblingWindow: partition key extraction, multiple partitions, watermark
//! - PartitionedSlidingWindow: overlapping windows, watermark, checkpoint/restore
//! - PartitionedSessionWindow: session timeout per partition, cleanup, checkpoint/restore
//! - IncrementalSlidingWindow: incremental aggregation, add/remove, boundary conditions
//! - DelayBuffer: delay and release events, watermark advance, flush
//! - PartitionedDelayBuffer: per-partition delay buffers
//! - PreviousValueTracker: partitioned variant
//! - SlidingCountWindow: count-based sliding windows
//! - Edge cases: zero-size windows, single-event, empty partitions, watermark at exact boundary

use chrono::Duration;
use std::sync::Arc;
use varpulis_core::Value;
use varpulis_runtime::event::Event;
use varpulis_runtime::window::*;

// ============================================================================
// HELPERS
// ============================================================================

fn make_event(event_type: &str, ts_offset_sec: i64, fields: Vec<(&str, Value)>) -> Event {
    let base_time = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
    let mut e = Event::new(event_type).with_timestamp(base_time + Duration::seconds(ts_offset_sec));
    for (k, v) in fields {
        e = e.with_field(k, v);
    }
    e
}

fn shared(event: Event) -> Arc<Event> {
    Arc::new(event)
}

// ============================================================================
// 1. PartitionedTumblingWindow Tests
// ============================================================================

#[test]
fn partitioned_tumbling_multiple_partitions_independent_windows() {
    // Each partition should maintain its own window start and trigger independently
    let mut w = PartitionedTumblingWindow::new("symbol".to_string(), Duration::seconds(5));

    // Partition "AAPL" at t=0
    let r = w.add_shared(shared(make_event(
        "T",
        0,
        vec![("symbol", Value::Str("AAPL".into()))],
    )));
    assert!(r.is_none());

    // Partition "GOOG" at t=1
    let r = w.add_shared(shared(make_event(
        "T",
        1,
        vec![("symbol", Value::Str("GOOG".into()))],
    )));
    assert!(r.is_none());

    // AAPL at t=2
    let r = w.add_shared(shared(make_event(
        "T",
        2,
        vec![("symbol", Value::Str("AAPL".into()))],
    )));
    assert!(r.is_none());

    // AAPL at t=6 triggers AAPL window (5s from t=0)
    let r = w.add_shared(shared(make_event(
        "T",
        6,
        vec![("symbol", Value::Str("AAPL".into()))],
    )));
    assert!(r.is_some());
    assert_eq!(r.unwrap().len(), 2, "AAPL window should have 2 events");

    // GOOG at t=3 should still be within GOOG window (started at t=1, ends at t=6)
    let r = w.add_shared(shared(make_event(
        "T",
        3,
        vec![("symbol", Value::Str("GOOG".into()))],
    )));
    assert!(r.is_none());
}

#[test]
fn partitioned_tumbling_default_partition_key() {
    // Events without the partition key field go into "default" partition
    let mut w = PartitionedTumblingWindow::new("region".to_string(), Duration::seconds(5));

    let r = w.add_shared(shared(make_event("T", 0, vec![("other", Value::Int(1))])));
    assert!(r.is_none());

    let r = w.add_shared(shared(make_event("T", 6, vec![("other", Value::Int(2))])));
    assert!(r.is_some());
    assert_eq!(r.unwrap().len(), 1);
}

#[test]
fn partitioned_tumbling_flush_shared_all_partitions() {
    let mut w = PartitionedTumblingWindow::new("region".to_string(), Duration::seconds(10));

    w.add_shared(shared(make_event(
        "T",
        0,
        vec![("region", Value::Str("east".into()))],
    )));
    w.add_shared(shared(make_event(
        "T",
        1,
        vec![("region", Value::Str("west".into()))],
    )));
    w.add_shared(shared(make_event(
        "T",
        2,
        vec![("region", Value::Str("east".into()))],
    )));

    let flushed = w.flush_shared();
    assert_eq!(
        flushed.len(),
        3,
        "flush_shared should return all events from all partitions"
    );
}

#[test]
fn partitioned_tumbling_checkpoint_restore() {
    let mut w = PartitionedTumblingWindow::new("region".to_string(), Duration::seconds(5));

    w.add_shared(shared(make_event(
        "T",
        0,
        vec![("region", Value::Str("east".into()))],
    )));
    w.add_shared(shared(make_event(
        "T",
        1,
        vec![("region", Value::Str("west".into()))],
    )));

    let cp = w.checkpoint();

    let mut restored = PartitionedTumblingWindow::new("region".to_string(), Duration::seconds(5));
    restored.restore(&cp);

    // Trigger east window (t=0 + 5s)
    let r = restored.add_shared(shared(make_event(
        "T",
        6,
        vec![("region", Value::Str("east".into()))],
    )));
    assert!(r.is_some());
    assert_eq!(r.unwrap().len(), 1);
}

#[test]
fn partitioned_tumbling_advance_watermark() {
    let mut w = PartitionedTumblingWindow::new("region".to_string(), Duration::seconds(5));
    let base_time = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();

    w.add_shared(shared(make_event(
        "T",
        0,
        vec![("region", Value::Str("east".into()))],
    )));
    w.add_shared(shared(make_event(
        "T",
        1,
        vec![("region", Value::Str("west".into()))],
    )));

    // Watermark at t=6 should close both windows
    let expired = w.advance_watermark(base_time + Duration::seconds(6));
    assert!(
        !expired.is_empty(),
        "Watermark should close expired partitions"
    );
}

// ============================================================================
// 2. PartitionedSlidingWindow Tests
// ============================================================================

#[test]
fn partitioned_sliding_overlapping_windows_per_partition() {
    let mut w = PartitionedSlidingWindow::new(
        "region".to_string(),
        Duration::seconds(10),
        Duration::seconds(3),
    );

    // Add east events
    let r = w.add_shared(shared(make_event(
        "T",
        0,
        vec![("region", Value::Str("east".into()))],
    )));
    assert!(r.is_some(), "First event in partition should emit");

    let r = w.add_shared(shared(make_event(
        "T",
        1,
        vec![("region", Value::Str("east".into()))],
    )));
    assert!(r.is_none(), "Within slide interval");

    let r = w.add_shared(shared(make_event(
        "T",
        4,
        vec![("region", Value::Str("east".into()))],
    )));
    assert!(r.is_some(), "After slide interval for east");
    assert_eq!(r.unwrap().len(), 3, "All 3 east events in window");
}

#[test]
fn partitioned_sliding_current_all_shared() {
    let mut w = PartitionedSlidingWindow::new(
        "region".to_string(),
        Duration::seconds(10),
        Duration::seconds(1),
    );

    w.add_shared(shared(make_event(
        "T",
        0,
        vec![("region", Value::Str("east".into()))],
    )));
    w.add_shared(shared(make_event(
        "T",
        1,
        vec![("region", Value::Str("west".into()))],
    )));
    w.add_shared(shared(make_event(
        "T",
        2,
        vec![("region", Value::Str("east".into()))],
    )));

    let all = w.current_all_shared();
    assert_eq!(
        all.len(),
        3,
        "current_all_shared should include all partitions"
    );
}

#[test]
fn partitioned_sliding_checkpoint_restore() {
    let mut w = PartitionedSlidingWindow::new(
        "region".to_string(),
        Duration::seconds(10),
        Duration::seconds(2),
    );

    w.add_shared(shared(make_event(
        "T",
        0,
        vec![("region", Value::Str("east".into()))],
    )));
    w.add_shared(shared(make_event(
        "T",
        1,
        vec![("region", Value::Str("west".into()))],
    )));

    let cp = w.checkpoint();

    let mut restored = PartitionedSlidingWindow::new(
        "region".to_string(),
        Duration::seconds(10),
        Duration::seconds(2),
    );
    restored.restore(&cp);

    let all = restored.current_all_shared();
    assert_eq!(all.len(), 2, "Restored should have 2 events");
}

#[test]
fn partitioned_sliding_advance_watermark() {
    let mut w = PartitionedSlidingWindow::new(
        "region".to_string(),
        Duration::seconds(5),
        Duration::seconds(2),
    );
    let base_time = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();

    w.add_shared(shared(make_event(
        "T",
        0,
        vec![("region", Value::Str("east".into()))],
    )));
    w.add_shared(shared(make_event(
        "T",
        1,
        vec![("region", Value::Str("west".into()))],
    )));

    // Advance watermark past slide interval
    let results = w.advance_watermark(base_time + Duration::seconds(3));
    // Should emit for partitions where slide interval passed
    // (east last_emit=t0, wm=t3 >= t0+2; west last_emit=t1, wm=t3 >= t1+2)
    assert!(!results.is_empty());
}

// ============================================================================
// 3. PartitionedSessionWindow Tests
// ============================================================================

#[test]
fn partitioned_session_independent_timeouts() {
    let mut w = PartitionedSessionWindow::new("user".to_string(), Duration::seconds(5));
    let base_time = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();

    // User A at t=0
    w.add_shared(shared(make_event(
        "T",
        0,
        vec![("user", Value::Str("A".into()))],
    )));
    // User B at t=3
    w.add_shared(shared(make_event(
        "T",
        3,
        vec![("user", Value::Str("B".into()))],
    )));

    // At t=6: user A expired (last=0, gap=5), user B still active (last=3, gap=5 -> 8)
    let expired = w.check_expired(base_time + Duration::seconds(6));
    assert_eq!(expired.len(), 1, "Only user A should be expired");
}

#[test]
fn partitioned_session_flush_shared_all() {
    let mut w = PartitionedSessionWindow::new("user".to_string(), Duration::seconds(10));

    w.add_shared(shared(make_event(
        "T",
        0,
        vec![("user", Value::Str("A".into()))],
    )));
    w.add_shared(shared(make_event(
        "T",
        1,
        vec![("user", Value::Str("B".into()))],
    )));
    w.add_shared(shared(make_event(
        "T",
        2,
        vec![("user", Value::Str("A".into()))],
    )));

    let flushed = w.flush_shared();
    assert_eq!(flushed.len(), 3, "flush_shared should return all events");
}

#[test]
fn partitioned_session_checkpoint_restore() {
    let mut w = PartitionedSessionWindow::new("user".to_string(), Duration::seconds(5));

    w.add_shared(shared(make_event(
        "T",
        0,
        vec![("user", Value::Str("A".into()))],
    )));
    w.add_shared(shared(make_event(
        "T",
        1,
        vec![("user", Value::Str("B".into()))],
    )));

    let cp = w.checkpoint();

    let mut restored = PartitionedSessionWindow::new("user".to_string(), Duration::seconds(5));
    restored.restore(&cp);

    // Trigger session close for A by adding event with gap > 5s
    let r = restored.add_shared(shared(make_event(
        "T",
        7,
        vec![("user", Value::Str("A".into()))],
    )));
    assert!(r.is_some(), "Should close restored session for A");
    assert_eq!(r.unwrap().len(), 1);
}

#[test]
fn partitioned_session_advance_watermark() {
    let mut w = PartitionedSessionWindow::new("user".to_string(), Duration::seconds(3));
    let base_time = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();

    w.add_shared(shared(make_event(
        "T",
        0,
        vec![("user", Value::Str("A".into()))],
    )));
    w.add_shared(shared(make_event(
        "T",
        1,
        vec![("user", Value::Str("B".into()))],
    )));

    // Advance watermark to t=5 -> both sessions should expire (gap=3)
    let expired = w.advance_watermark(base_time + Duration::seconds(5));
    assert_eq!(
        expired.len(),
        2,
        "Both sessions should expire at watermark t=5"
    );
}

#[test]
fn partitioned_session_check_expired_removes_partitions() {
    let mut w = PartitionedSessionWindow::new("user".to_string(), Duration::seconds(3));
    let base_time = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();

    w.add_shared(shared(make_event(
        "T",
        0,
        vec![("user", Value::Str("A".into()))],
    )));

    // Expire all
    let expired = w.check_expired(base_time + Duration::seconds(5));
    assert_eq!(expired.len(), 1);

    // After expiry, should be empty
    let expired2 = w.check_expired(base_time + Duration::seconds(10));
    assert_eq!(expired2.len(), 0);
}

// ============================================================================
// 4. IncrementalSlidingWindow Tests
// ============================================================================

#[test]
fn incremental_sliding_add_and_remove_buckets() {
    let mut w = IncrementalSlidingWindow::new(Duration::seconds(3), Duration::seconds(1), "value");
    let base_time = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();

    // Add events at t=0, t=1, t=2 with values 10, 20, 30
    for i in 0..3 {
        let e = Event::new("M")
            .with_timestamp(base_time + Duration::seconds(i))
            .with_field("value", Value::Float(10.0 * (i + 1) as f64));
        w.add(Arc::new(e));
    }

    let agg = w.current_aggregates();
    assert_eq!(agg.count, 3);
    assert!((agg.sum - 60.0).abs() < 0.001);
    assert_eq!(agg.min, Some(10.0));
    assert_eq!(agg.max, Some(30.0));

    // Add event at t=4 -> t=0 (value=10) should expire
    let e = Event::new("M")
        .with_timestamp(base_time + Duration::seconds(4))
        .with_field("value", Value::Float(40.0));
    w.add(Arc::new(e));

    let agg = w.current_aggregates();
    assert_eq!(agg.count, 3); // t=1,2,4
    assert!((agg.sum - 90.0).abs() < 0.001); // 20+30+40
    assert_eq!(agg.min, Some(20.0));
    assert_eq!(agg.max, Some(40.0));
}

#[test]
fn incremental_sliding_nan_values_ignored() {
    let mut w = IncrementalSlidingWindow::new(Duration::seconds(10), Duration::seconds(1), "value");
    let base_time = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();

    let e = Event::new("M")
        .with_timestamp(base_time)
        .with_field("value", Value::Float(f64::NAN));
    w.add(Arc::new(e));

    let agg = w.current_aggregates();
    assert_eq!(agg.count, 0, "NaN values should not be counted");
    assert!(agg.avg.is_none());
}

#[test]
fn incremental_sliding_missing_field() {
    let mut w = IncrementalSlidingWindow::new(Duration::seconds(10), Duration::seconds(1), "value");
    let base_time = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();

    // Event without "value" field
    let e = Event::new("M")
        .with_timestamp(base_time)
        .with_field("other", Value::Float(100.0));
    w.add(Arc::new(e));

    let agg = w.current_aggregates();
    assert_eq!(agg.count, 0, "Missing field should not be counted");
    assert_eq!(agg.event_count, 1, "But event should be in window");
}

#[test]
fn incremental_sliding_reset() {
    let mut w = IncrementalSlidingWindow::new(Duration::seconds(10), Duration::seconds(1), "value");
    let base_time = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();

    let e = Event::new("M")
        .with_timestamp(base_time)
        .with_field("value", Value::Float(100.0));
    w.add(Arc::new(e));

    w.reset();

    let agg = w.current_aggregates();
    assert_eq!(agg.count, 0);
    assert_eq!(agg.event_count, 0);
    assert!((agg.sum - 0.0).abs() < 0.001);
}

#[test]
fn incremental_sliding_events_iterator() {
    let mut w = IncrementalSlidingWindow::new(Duration::seconds(10), Duration::seconds(1), "value");
    let base_time = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();

    for i in 0..5 {
        let e = Event::new("M")
            .with_timestamp(base_time + Duration::seconds(i))
            .with_field("value", Value::Float(i as f64));
        w.add(Arc::new(e));
    }

    let event_count = w.events().count();
    assert_eq!(event_count, 5);
}

#[test]
fn incremental_sliding_boundary_exact_window_edge() {
    // Event exactly at window boundary should be included
    let mut w = IncrementalSlidingWindow::new(Duration::seconds(5), Duration::seconds(1), "value");
    let base_time = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();

    let e0 = Event::new("M")
        .with_timestamp(base_time)
        .with_field("value", Value::Float(10.0));
    w.add(Arc::new(e0));

    // At t=5, cutoff is t=0, so t=0 event is NOT expired (cutoff = t5 - 5s = t0, and t0 >= t0 is true)
    let e5 = Event::new("M")
        .with_timestamp(base_time + Duration::seconds(5))
        .with_field("value", Value::Float(20.0));
    w.add(Arc::new(e5));

    let agg = w.current_aggregates();
    assert_eq!(agg.count, 2, "Event at exact boundary should be included");
}

// ============================================================================
// 5. DelayBuffer Tests
// ============================================================================

#[test]
fn delay_buffer_minimum_delay_enforced() {
    // Delay of 0 should be clamped to 1
    let mut delay: DelayBuffer<i32> = DelayBuffer::new(0);

    assert_eq!(delay.push(10), None);
    assert_eq!(delay.push(20), Some(10));
}

#[test]
fn delay_buffer_large_delay() {
    let mut delay: DelayBuffer<i32> = DelayBuffer::new(5);

    for i in 0..5 {
        assert_eq!(delay.push(i), None, "Buffer should be filling");
    }
    assert_eq!(delay.len(), 5);
    assert!(delay.is_ready());

    assert_eq!(delay.push(5), Some(0));
    assert_eq!(delay.push(6), Some(1));
}

#[test]
fn delay_buffer_oldest() {
    let mut delay: DelayBuffer<i32> = DelayBuffer::new(3);

    delay.push(10);
    assert_eq!(delay.oldest(), Some(&10));

    delay.push(20);
    assert_eq!(delay.oldest(), Some(&10));

    delay.push(30);
    assert_eq!(delay.oldest(), Some(&10));

    // After push that pops, oldest changes
    delay.push(40);
    assert_eq!(delay.oldest(), Some(&20));
}

#[test]
fn delay_buffer_previous_with_single_item() {
    let mut delay: DelayBuffer<i32> = DelayBuffer::new(1);
    delay.push(10);
    assert!(
        delay.previous().is_none(),
        "Single item should have no previous"
    );
}

#[test]
fn delay_buffer_empty_flush() {
    let mut delay: DelayBuffer<i32> = DelayBuffer::new(1);
    let flushed = delay.flush();
    assert!(flushed.is_empty());
}

// ============================================================================
// 6. PartitionedDelayBuffer Tests
// ============================================================================

#[test]
fn partitioned_delay_buffer_many_partitions() {
    let mut buffer: PartitionedDelayBuffer<f64> = PartitionedDelayBuffer::new(1);

    for i in 0..10 {
        let key = format!("p{}", i);
        assert_eq!(buffer.push(&key, i as f64), None);
    }

    // Each partition has 1 item, not ready yet (delay=1 means need >= 1 item)
    assert!(buffer.is_ready("p0"));

    // Push second item to p0
    assert_eq!(buffer.push("p0", 100.0), Some(0.0));
    assert_eq!(buffer.current("p0"), Some(&100.0));
}

#[test]
fn partitioned_delay_buffer_nonexistent_partition_queries() {
    let buffer: PartitionedDelayBuffer<i32> = PartitionedDelayBuffer::new(1);

    assert!(buffer.current("nonexistent").is_none());
    assert!(buffer.previous("nonexistent").is_none());
    assert!(!buffer.is_ready("nonexistent"));
}

// ============================================================================
// 7. PreviousValueTracker Tests
// ============================================================================

#[test]
fn previous_value_tracker_with_string_values() {
    let mut tracker: PreviousValueTracker<String> = PreviousValueTracker::new();

    tracker.update("first".to_string());
    assert_eq!(tracker.current(), Some(&"first".to_string()));
    assert!(tracker.previous().is_none());

    tracker.update("second".to_string());
    assert_eq!(tracker.current(), Some(&"second".to_string()));
    assert_eq!(tracker.previous(), Some(&"first".to_string()));
}

#[test]
fn previous_value_tracker_default_trait() {
    let tracker: PreviousValueTracker<f64> = PreviousValueTracker::default();
    assert!(tracker.current().is_none());
    assert!(tracker.previous().is_none());
    assert!(!tracker.has_both());
}

// ============================================================================
// 8. PartitionedPreviousValueTracker Tests
// ============================================================================

#[test]
fn partitioned_previous_tracker_default_trait() {
    let tracker: PartitionedPreviousValueTracker<f64> = PartitionedPreviousValueTracker::default();
    assert!(!tracker.has_both("any"));
    assert!(tracker.current("any").is_none());
    assert!(tracker.previous("any").is_none());
    assert!(tracker.get_pair("any").is_none());
}

#[test]
fn partitioned_previous_tracker_many_partitions() {
    let mut tracker: PartitionedPreviousValueTracker<i32> = PartitionedPreviousValueTracker::new();

    for i in 0..20 {
        let key = format!("partition_{}", i);
        tracker.update(&key, i * 10);
        tracker.update(&key, i * 10 + 1);
    }

    for i in 0..20 {
        let key = format!("partition_{}", i);
        assert!(tracker.has_both(&key));
        let (curr, prev) = tracker.get_pair(&key).unwrap();
        assert_eq!(*curr, i * 10 + 1);
        assert_eq!(*prev, i * 10);
    }
}

#[test]
fn partitioned_previous_tracker_single_update() {
    let mut tracker: PartitionedPreviousValueTracker<f64> = PartitionedPreviousValueTracker::new();

    tracker.update("x", 1.0);
    assert!(!tracker.has_both("x"));
    assert_eq!(tracker.current("x"), Some(&1.0));
    assert!(tracker.previous("x").is_none());
}

// ============================================================================
// 9. SlidingCountWindow Tests
// ============================================================================

#[test]
fn sliding_count_window_slide_equals_window() {
    // slide_size == window_size => non-overlapping (tumbling behavior)
    let mut w = SlidingCountWindow::new(3, 3);

    for i in 0..3 {
        let e = Event::new("T").with_field("v", i as i64);
        let r = w.add(e);
        if i < 2 {
            assert!(r.is_none());
        } else {
            assert!(r.is_some());
            assert_eq!(r.unwrap().len(), 3);
        }
    }

    // Next batch
    for i in 3..6 {
        let e = Event::new("T").with_field("v", i as i64);
        let r = w.add(e);
        if i < 5 {
            assert!(r.is_none());
        } else {
            assert!(r.is_some());
            assert_eq!(r.unwrap().len(), 3);
        }
    }
}

#[test]
fn sliding_count_window_slide_one() {
    // slide_size = 1 means emit on every event once window is full
    let mut w = SlidingCountWindow::new(3, 1);

    // Fill window
    let r = w.add(Event::new("T").with_field("v", 1i64));
    assert!(r.is_none());
    let r = w.add(Event::new("T").with_field("v", 2i64));
    assert!(r.is_none());
    let r = w.add(Event::new("T").with_field("v", 3i64));
    assert!(r.is_some());
    assert_eq!(r.unwrap().len(), 3);

    // Next event should also emit (slide=1)
    let r = w.add(Event::new("T").with_field("v", 4i64));
    assert!(r.is_some());
    let events = r.unwrap();
    assert_eq!(events.len(), 3);
}

#[test]
fn sliding_count_window_current_count() {
    let mut w = SlidingCountWindow::new(5, 2);

    for i in 0..3 {
        w.add(Event::new("T").with_field("v", i as i64));
    }

    assert_eq!(w.current_count(), 3);
}

#[test]
fn sliding_count_window_checkpoint_restore_resets_events_since_emit() {
    let mut w = SlidingCountWindow::new(4, 2);

    // Add 3 events
    for i in 0..3 {
        w.add(Event::new("T").with_field("v", i as i64));
    }

    let cp = w.checkpoint();

    let mut restored = SlidingCountWindow::new(4, 2);
    restored.restore(&cp);

    // events_since_emit is reset to 0 by restore
    // Need to fill window (4) and reach slide (2)
    let r = restored.add(Event::new("T").with_field("v", 3i64));
    assert!(r.is_none()); // window full (4) but events_since_emit = 1 < 2

    let r = restored.add(Event::new("T").with_field("v", 4i64));
    assert!(r.is_some()); // events_since_emit = 2 == slide_size
}

// ============================================================================
// 10. Edge Cases
// ============================================================================

#[test]
fn tumbling_window_single_event_then_trigger() {
    let mut w = TumblingWindow::new(Duration::seconds(1));
    let base_time = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();

    let e = Event::new("T").with_timestamp(base_time);
    assert!(w.add(e).is_none());
    assert_eq!(w.len(), 1);
    assert!(!w.is_empty());

    // Next window
    let e = Event::new("T").with_timestamp(base_time + Duration::seconds(2));
    let r = w.add(e);
    assert!(r.is_some());
    assert_eq!(r.unwrap().len(), 1);
}

#[test]
fn tumbling_window_flush_columnar() {
    let mut w = TumblingWindow::new(Duration::seconds(10));
    let base_time = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();

    w.add(Event::new("T").with_timestamp(base_time));
    w.add(Event::new("T").with_timestamp(base_time + Duration::seconds(1)));

    let buf = w.flush_columnar();
    assert_eq!(buf.len(), 2);
    assert!(w.is_empty());
}

#[test]
fn session_window_flush_columnar() {
    let mut w = SessionWindow::new(Duration::seconds(5));
    let base_time = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();

    w.add(Event::new("T").with_timestamp(base_time));
    w.add(Event::new("T").with_timestamp(base_time + Duration::seconds(1)));

    let buf = w.flush_columnar();
    assert_eq!(buf.len(), 2);
    // After flush_columnar, session should be empty
    assert_eq!(w.flush_shared().len(), 0);
}

#[test]
fn count_window_flush_columnar() {
    let mut w = CountWindow::new(10);
    w.add(Event::new("T").with_field("v", 1i64));
    w.add(Event::new("T").with_field("v", 2i64));

    let buf = w.flush_columnar();
    assert_eq!(buf.len(), 2);
    assert_eq!(w.current_count(), 0);
}

#[test]
fn sliding_window_checkpoint_restore() {
    let mut w = SlidingWindow::new(Duration::seconds(10), Duration::seconds(2));
    let base_time = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();

    w.add(Event::new("T").with_timestamp(base_time));
    w.add(Event::new("T").with_timestamp(base_time + Duration::seconds(1)));

    let cp = w.checkpoint();

    let mut restored = SlidingWindow::new(Duration::seconds(10), Duration::seconds(2));
    restored.restore(&cp);

    let current = restored.current();
    assert_eq!(current.len(), 2);
}

#[test]
fn sliding_window_advance_watermark_no_events() {
    let mut w = SlidingWindow::new(Duration::seconds(5), Duration::seconds(1));
    let base_time = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();

    let r = w.advance_watermark(base_time);
    assert!(r.is_none(), "Empty window should not emit on watermark");
}

#[test]
fn sliding_window_advance_watermark_with_events() {
    let mut w = SlidingWindow::new(Duration::seconds(5), Duration::seconds(2));
    let base_time = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();

    w.add(Event::new("T").with_timestamp(base_time));
    w.add(Event::new("T").with_timestamp(base_time + Duration::seconds(1)));

    // Watermark at t=3 should trigger emit (last_emit was t=1 from second add, slide=2, 3 >= 1+2)
    let r = w.advance_watermark(base_time + Duration::seconds(3));
    assert!(r.is_some());
}

#[test]
fn tumbling_window_advance_watermark_empty() {
    let mut w = TumblingWindow::new(Duration::seconds(5));
    let base_time = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();

    // No events, no window_start
    let r = w.advance_watermark(base_time + Duration::seconds(10));
    assert!(r.is_none());
}

#[test]
fn tumbling_window_advance_watermark_not_yet() {
    let mut w = TumblingWindow::new(Duration::seconds(5));
    let base_time = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();

    w.add(Event::new("T").with_timestamp(base_time));

    // Watermark before window end
    let r = w.advance_watermark(base_time + Duration::seconds(3));
    assert!(r.is_none(), "Watermark before window end should not emit");
}

#[test]
fn session_window_advance_watermark_empty() {
    let mut w = SessionWindow::new(Duration::seconds(5));
    let base_time = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();

    let r = w.advance_watermark(base_time);
    assert!(r.is_none(), "Empty session should not emit on watermark");
}

#[test]
fn session_window_advance_watermark_not_yet() {
    let mut w = SessionWindow::new(Duration::seconds(5));
    let base_time = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();

    w.add(Event::new("T").with_timestamp(base_time));

    // Watermark within gap
    let r = w.advance_watermark(base_time + Duration::seconds(3));
    assert!(r.is_none());
}

#[test]
fn partitioned_session_advance_watermark_partial() {
    // Only some partitions should expire
    let mut w = PartitionedSessionWindow::new("user".to_string(), Duration::seconds(5));
    let base_time = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();

    w.add_shared(shared(make_event(
        "T",
        0,
        vec![("user", Value::Str("A".into()))],
    )));
    w.add_shared(shared(make_event(
        "T",
        4,
        vec![("user", Value::Str("B".into()))],
    )));

    // At t=6: A expired (0+5=5, wm=6>=5), B not expired (4+5=9, wm=6<9)
    let expired = w.advance_watermark(base_time + Duration::seconds(6));
    assert_eq!(expired.len(), 1);
    // B should still be accessible
    let all = w.flush_shared();
    assert_eq!(all.len(), 1, "B should still be in window");
}

#[test]
fn count_window_single_event_window() {
    let mut w = CountWindow::new(1);

    let r = w.add(Event::new("T").with_field("v", 1i64));
    assert!(r.is_some());
    assert_eq!(r.unwrap().len(), 1);

    let r = w.add(Event::new("T").with_field("v", 2i64));
    assert!(r.is_some());
    assert_eq!(r.unwrap().len(), 1);
}
