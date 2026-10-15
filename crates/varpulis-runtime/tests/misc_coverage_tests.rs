//! Miscellaneous coverage tests for multiple smaller modules.
//!
//! Targets uncovered paths in:
//!   watermark.rs, worker_pool.rs, columnar.rs, join.rs, sequence.rs,

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use varpulis_runtime::event::Event;

// ===========================================================================
// Module 1: watermark.rs — PerSourceWatermarkTracker
// ===========================================================================

mod watermark_tests {
    use varpulis_runtime::watermark::PerSourceWatermarkTracker;

    use super::*;

    #[test]
    fn test_default_trait() {
        let tracker = PerSourceWatermarkTracker::default();
        assert!(!tracker.has_sources());
        assert!(tracker.effective_watermark().is_none());
    }

    #[test]
    fn test_has_sources_empty_and_nonempty() {
        let mut tracker = PerSourceWatermarkTracker::new();
        assert!(!tracker.has_sources());

        tracker.register_source("s1", chrono::Duration::seconds(1));
        assert!(tracker.has_sources());
    }

    #[test]
    fn test_auto_register_unknown_source() {
        // observe_event on an unregistered source should auto-register with zero OOO
        let mut tracker = PerSourceWatermarkTracker::new();
        let ts = Utc::now();
        tracker.observe_event("unknown", ts);

        // After auto-registration, effective watermark should equal ts (ooo=0)
        let wm = tracker.effective_watermark().unwrap();
        assert_eq!(wm, ts);
        assert!(tracker.has_sources());
    }

    #[test]
    fn test_advance_watermark_does_not_recede() {
        let mut tracker = PerSourceWatermarkTracker::new();
        tracker.register_source("s1", chrono::Duration::seconds(0));

        let base = Utc::now();
        tracker.advance_source_watermark("s1", base + chrono::Duration::seconds(10));
        assert_eq!(
            tracker.effective_watermark(),
            Some(base + chrono::Duration::seconds(10))
        );

        // Attempt to advance to an earlier time — should not recede
        tracker.advance_source_watermark("s1", base + chrono::Duration::seconds(5));
        assert_eq!(
            tracker.effective_watermark(),
            Some(base + chrono::Duration::seconds(10))
        );
    }

    #[test]
    fn test_advance_unknown_source_is_noop() {
        let mut tracker = PerSourceWatermarkTracker::new();
        tracker.register_source("s1", chrono::Duration::seconds(0));
        let ts = Utc::now();

        // Advance a non-existent source — should have no effect
        tracker.advance_source_watermark("nonexistent", ts);
        assert!(tracker.effective_watermark().is_none());
    }

    #[test]
    fn test_effective_watermark_with_uninitialized_source() {
        let mut tracker = PerSourceWatermarkTracker::new();
        tracker.register_source("fast", chrono::Duration::seconds(0));
        tracker.register_source("slow", chrono::Duration::seconds(5));

        let ts = Utc::now();
        // Only update "fast"
        tracker.observe_event("fast", ts + chrono::Duration::seconds(20));

        // effective watermark should be the fast source's watermark since
        // the slow source has None watermark and doesn't block
        let wm = tracker.effective_watermark().unwrap();
        assert_eq!(wm, ts + chrono::Duration::seconds(20));
    }

    #[test]
    fn test_checkpoint_restore_with_none_watermarks() {
        let mut tracker = PerSourceWatermarkTracker::new();
        tracker.register_source("s1", chrono::Duration::seconds(5));
        // Do NOT observe any events — watermarks remain None

        let cp = tracker.checkpoint();

        let mut restored = PerSourceWatermarkTracker::new();
        restored.restore(&cp);

        assert!(restored.effective_watermark().is_none());
        assert!(restored.has_sources());
    }

    #[test]
    fn test_observe_event_late_does_not_update_max() {
        let mut tracker = PerSourceWatermarkTracker::new();
        tracker.register_source("s1", chrono::Duration::seconds(2));

        let base = Utc::now();
        tracker.observe_event("s1", base + chrono::Duration::seconds(10));
        let wm1 = tracker.effective_watermark().unwrap();

        // Late event (earlier timestamp) — watermark should not change
        tracker.observe_event("s1", base + chrono::Duration::seconds(3));
        let wm2 = tracker.effective_watermark().unwrap();
        assert_eq!(wm1, wm2);
    }
}

// ===========================================================================
// Module 2: worker_pool.rs — PoolBackpressureError, WorkerPoolConfig
// ===========================================================================

mod worker_pool_tests {
    use varpulis_runtime::worker_pool::{
        BackpressureStrategy, PoolBackpressureError, WorkerPoolConfig, WorkerState,
    };

    #[test]
    fn test_backpressure_error_display() {
        let err = PoolBackpressureError {
            pool_name: "mypool".to_string(),
            queue_depth: 42,
        };
        let msg = format!("{err}");
        assert!(msg.contains("mypool"));
        assert!(msg.contains("42"));
    }

    #[test]
    fn test_backpressure_error_is_std_error() {
        let err = PoolBackpressureError {
            pool_name: "test".to_string(),
            queue_depth: 10,
        };
        // Ensure std::error::Error is implemented
        let _: &dyn std::error::Error = &err;
    }

    #[test]
    fn test_worker_pool_config_default() {
        let config = WorkerPoolConfig::default();
        assert_eq!(config.name, "default");
        assert_eq!(config.workers, 4);
        assert_eq!(config.queue_size, 1000);
        assert_eq!(config.backpressure, BackpressureStrategy::Block);
    }

    #[test]
    fn test_backpressure_strategy_eq() {
        assert_eq!(BackpressureStrategy::Block, BackpressureStrategy::Block);
        assert_ne!(
            BackpressureStrategy::Block,
            BackpressureStrategy::DropOldest
        );
        assert_ne!(
            BackpressureStrategy::DropNewest,
            BackpressureStrategy::Error
        );
    }

    #[test]
    fn test_worker_state_eq() {
        assert_eq!(WorkerState::Idle, WorkerState::Idle);
        assert_ne!(WorkerState::Idle, WorkerState::Processing);
        assert_ne!(WorkerState::Processing, WorkerState::Draining);
        assert_ne!(WorkerState::Draining, WorkerState::Stopped);
    }

    #[test]
    fn test_worker_pool_config_clone() {
        let config = WorkerPoolConfig {
            name: "clone_test".to_string(),
            workers: 8,
            queue_size: 500,
            backpressure: BackpressureStrategy::DropOldest,
        };
        let cloned = config;
        assert_eq!(cloned.name, "clone_test");
        assert_eq!(cloned.workers, 8);
        assert_eq!(cloned.backpressure, BackpressureStrategy::DropOldest);
    }

    #[test]
    fn test_backpressure_error_clone() {
        let err = PoolBackpressureError {
            pool_name: "pool1".to_string(),
            queue_depth: 100,
        };
        let cloned = err;
        assert_eq!(cloned.pool_name, "pool1");
        assert_eq!(cloned.queue_depth, 100);
    }
}

// ===========================================================================
// Module 3: columnar.rs — ColumnarBuffer, Column edge cases
// ===========================================================================

mod columnar_tests {
    use varpulis_runtime::columnar::{Column, ColumnarBuffer};

    use super::*;

    #[test]
    fn test_column_is_empty() {
        let empty_float = Column::Float64(vec![]);
        assert!(empty_float.is_empty());

        let non_empty = Column::Int64(vec![1, 2]);
        assert!(!non_empty.is_empty());
    }

    #[test]
    fn test_column_as_bool() {
        let col = Column::Bool(vec![Some(true), Some(false), None]);
        let bools = col.as_bool().unwrap();
        assert_eq!(bools.len(), 3);
        assert_eq!(bools[0], Some(true));
        assert_eq!(bools[2], None);
    }

    #[test]
    fn test_column_as_string_on_wrong_type() {
        let col = Column::Float64(vec![1.0]);
        assert!(col.as_string().is_none());
        assert!(col.as_bool().is_none());
    }

    #[test]
    fn test_column_as_int64_on_wrong_type() {
        let col = Column::String(vec![Some("hello".into())]);
        assert!(col.as_int64().is_none());
        assert!(col.as_float64().is_none());
    }

    #[test]
    fn test_columnar_buffer_push_invalidates_cache() {
        let mut buffer = ColumnarBuffer::new();
        buffer.push(Arc::new(Event::new("T").with_field("x", 1.0)));

        // Access column to populate cache
        let _ = buffer.ensure_float_column("x");
        assert!(buffer.has_column("x"));

        // Push a new event — cache should be cleared
        buffer.push(Arc::new(Event::new("T").with_field("x", 2.0)));
        assert!(!buffer.has_column("x"));
    }

    #[test]
    fn test_columnar_buffer_drain_more_than_available() {
        let mut buffer = ColumnarBuffer::new();
        buffer.push(Arc::new(Event::new("T")));
        buffer.push(Arc::new(Event::new("T")));

        // Drain more than available — should clamp to available
        let drained = buffer.drain_front(100);
        assert_eq!(drained.len(), 2);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_columnar_buffer_get_column_before_ensure() {
        let buffer = ColumnarBuffer::new();
        // get_column should return None if column was never extracted
        assert!(buffer.get_column("nonexistent").is_none());
    }

    #[test]
    fn test_columnar_buffer_ensure_int_column_missing_field() {
        let mut buffer = ColumnarBuffer::new();
        buffer.push(Arc::new(Event::new("T").with_field("name", "foo")));

        // Requesting an int column for a string field should give i64::MIN
        let ints = buffer.ensure_int_column("name");
        assert_eq!(ints[0], i64::MIN);
    }

    #[test]
    fn test_columnar_buffer_ensure_string_column_missing_field() {
        let mut buffer = ColumnarBuffer::new();
        buffer.push(Arc::new(Event::new("T").with_field("val", 42i64)));

        // Requesting a string column for a missing field should give None
        let strs = buffer.ensure_string_column("missing");
        assert_eq!(strs[0], None);
    }

    #[test]
    fn test_columnar_buffer_default() {
        let buffer = ColumnarBuffer::default();
        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);
    }
}

// ===========================================================================
// Module 4: join.rs — JoinBuffer edge cases
// ===========================================================================

mod join_tests {
    use rustc_hash::FxHashMap;
    use varpulis_runtime::join::JoinBuffer;

    use super::*;

    #[test]
    fn test_join_buffer_unknown_source_with_common_key() {
        // Source "C" is not registered, but has "id" field -> auto-detect
        let sources = vec!["A".to_string(), "B".to_string()];
        let mut join_keys = FxHashMap::default();
        join_keys.insert("A".to_string(), "id".to_string());
        join_keys.insert("B".to_string(), "id".to_string());

        let mut buffer = JoinBuffer::new(sources, join_keys, chrono::Duration::minutes(1));

        let event_a = Event::new("A")
            .with_field("id", "k1")
            .with_field("val", 1.0);
        buffer.add_event("A", event_a);

        // Source "C" not in the join, add_event should still accept via common-key detection
        let event_c = Event::new("C")
            .with_field("id", "k1")
            .with_field("extra", 99.0);
        let result = buffer.add_event("C", event_c);
        // Won't produce a correlation because "C" is not in sources list
        assert!(result.is_none());
    }

    #[test]
    fn test_join_buffer_event_without_any_common_key() {
        let sources = vec!["A".to_string(), "B".to_string()];
        let join_keys = FxHashMap::default(); // No configured keys

        let mut buffer = JoinBuffer::new(sources, join_keys, chrono::Duration::minutes(1));

        // Event with no common key fields (no symbol, key, id, user_id, order_id)
        let event = Event::new("A").with_field("temperature", 100.0);
        let result = buffer.add_event("A", event);
        assert!(result.is_none());
    }

    #[test]
    fn test_join_buffer_stats_empty() {
        let sources = vec!["X".to_string(), "Y".to_string()];
        let join_keys = FxHashMap::default();
        let buffer = JoinBuffer::new(sources, join_keys, chrono::Duration::minutes(1));

        let stats = buffer.stats();
        assert_eq!(stats.total_events, 0);
        assert_eq!(stats.sources.len(), 2);
    }

    #[test]
    fn test_join_buffer_gc_interval_tiny_window() {
        // Very tiny window should have gc_interval clamped to 10ms minimum
        let sources = vec!["A".to_string(), "B".to_string()];
        let mut join_keys = FxHashMap::default();
        join_keys.insert("A".to_string(), "symbol".to_string());
        join_keys.insert("B".to_string(), "symbol".to_string());

        let mut buffer = JoinBuffer::new(sources, join_keys, chrono::Duration::milliseconds(10));

        // Just verify it works without panicking
        let event = Event::new("A")
            .with_field("symbol", "BTC")
            .with_field("val", 1.0);
        let _ = buffer.add_event("A", event);
    }
}

// ===========================================================================
// Module 5: sequence.rs — SequenceTracker edge cases
// ===========================================================================

mod sequence_tests {
    use varpulis_core::Value;
    use varpulis_runtime::sequence::{
        ActiveCorrelation, SequenceContext, SequenceStep, SequenceTracker,
    };

    use super::*;

    fn make_event(event_type: &str, fields: Vec<(&str, Value)>) -> Event {
        let mut event = Event::new(event_type);
        for (k, v) in fields {
            event.data.insert(k.into(), v);
        }
        event
    }

    #[test]
    fn test_context_empty_static_ref() {
        let ctx = SequenceContext::empty();
        assert!(ctx.previous.is_none());
        assert!(ctx.captured.is_empty());
    }

    #[test]
    fn test_context_get_dollar_returns_previous() {
        let event = make_event("X", vec![("val", Value::Int(7))]);
        let ctx = SequenceContext::new().with_captured("alias".to_string(), event);
        assert!(ctx.get("$").is_some());
        assert_eq!(ctx.get("$").unwrap().get_int("val"), Some(7));
    }

    #[test]
    fn test_active_correlation_advance_without_alias() {
        let mut corr = ActiveCorrelation::new();
        let event = make_event("E", vec![]);

        corr.advance(event, None);
        assert_eq!(corr.current_step, 1);
        // Previous should be set even without alias
        assert!(corr.context.previous.is_some());
    }

    #[test]
    fn test_active_correlation_timeout_cleared_on_advance() {
        let mut corr = ActiveCorrelation::new();
        corr.set_timeout(Duration::from_mins(1));
        assert!(!corr.is_timed_out());

        // Advance should clear timeout
        let event = make_event("E", vec![]);
        corr.advance(event, Some("e"));
        assert!(corr.step_timeout.is_none());
    }

    #[test]
    fn test_sequence_tracker_match_all_last_step_keeps_correlation() {
        // When the last step has match_all=true, the sequence emits but keeps
        // the correlation active at the previous step
        let steps = vec![
            SequenceStep {
                event_type: "A".to_string(),
                filter: None,
                alias: Some("a".to_string()),
                timeout: None,
                match_all: false,
            },
            SequenceStep {
                event_type: "B".to_string(),
                filter: None,
                alias: Some("b".to_string()),
                timeout: None,
                match_all: true,
            },
        ];

        let mut tracker = SequenceTracker::new(steps, false);

        tracker.process(&make_event("A", vec![]));
        assert_eq!(tracker.active_count(), 1);

        // First B completes sequence but keeps correlation
        let completed = tracker.process(&make_event("B", vec![]));
        assert_eq!(completed.len(), 1);
        assert_eq!(tracker.active_count(), 1); // Kept active

        // Second B should also produce a result
        let completed = tracker.process(&make_event("B", vec![]));
        assert_eq!(completed.len(), 1);
    }

    #[test]
    fn test_sequence_tracker_cleanup_timeouts_returns_count() {
        let steps = vec![
            SequenceStep {
                event_type: "A".to_string(),
                filter: None,
                alias: None,
                timeout: None,
                match_all: true,
            },
            SequenceStep {
                event_type: "B".to_string(),
                filter: None,
                alias: None,
                timeout: Some(Duration::from_millis(1)),
                match_all: false,
            },
        ];

        let mut tracker = SequenceTracker::new(steps, true);

        // Start 3 correlations
        for _ in 0..3 {
            tracker.process(&make_event("A", vec![]));
        }
        assert_eq!(tracker.active_count(), 3);

        std::thread::sleep(Duration::from_millis(10));

        let cleaned = tracker.cleanup_timeouts();
        assert_eq!(cleaned, 3);
        assert_eq!(tracker.active_count(), 0);
    }
}
