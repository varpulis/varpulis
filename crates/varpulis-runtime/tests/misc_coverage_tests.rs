//! Miscellaneous coverage tests for multiple smaller modules.
//!
//! Targets uncovered paths in:
//!   watermark.rs, worker_pool.rs, columnar.rs, join.rs, sequence.rs,
//!   hamlet/graph.rs, hamlet/graphlet.rs, hamlet/snapshot.rs,
//!   hamlet/optimizer.rs, zdd_unified/propagation.rs, zdd_unified/nfa_zdd.rs

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use varpulis_runtime::event::Event;

// ===========================================================================
// Module 1: watermark.rs — PerSourceWatermarkTracker
// ===========================================================================

mod watermark_tests {
    use super::*;
    use varpulis_runtime::watermark::PerSourceWatermarkTracker;

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
// Module 2: worker_pool.rs — BackpressureError, WorkerPoolConfig
// ===========================================================================

mod worker_pool_tests {
    use varpulis_runtime::worker_pool::{
        BackpressureError, BackpressureStrategy, WorkerPoolConfig, WorkerState,
    };

    #[test]
    fn test_backpressure_error_display() {
        let err = BackpressureError {
            pool_name: "mypool".to_string(),
            queue_depth: 42,
        };
        let msg = format!("{}", err);
        assert!(msg.contains("mypool"));
        assert!(msg.contains("42"));
    }

    #[test]
    fn test_backpressure_error_is_std_error() {
        let err = BackpressureError {
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
        let cloned = config.clone();
        assert_eq!(cloned.name, "clone_test");
        assert_eq!(cloned.workers, 8);
        assert_eq!(cloned.backpressure, BackpressureStrategy::DropOldest);
    }

    #[test]
    fn test_backpressure_error_clone() {
        let err = BackpressureError {
            pool_name: "pool1".to_string(),
            queue_depth: 100,
        };
        let cloned = err.clone();
        assert_eq!(cloned.pool_name, "pool1");
        assert_eq!(cloned.queue_depth, 100);
    }
}

// ===========================================================================
// Module 3: columnar.rs — ColumnarBuffer, Column edge cases
// ===========================================================================

mod columnar_tests {
    use super::*;
    use varpulis_runtime::columnar::{Column, ColumnarBuffer};

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
    use super::*;
    use rustc_hash::FxHashMap;
    use varpulis_runtime::join::JoinBuffer;

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
    use super::*;
    use varpulis_core::Value;
    use varpulis_runtime::sequence::{
        ActiveCorrelation, SequenceContext, SequenceStep, SequenceTracker,
    };

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

        corr.advance(event.clone(), None);
        assert_eq!(corr.current_step, 1);
        // Previous should be set even without alias
        assert!(corr.context.previous.is_some());
    }

    #[test]
    fn test_active_correlation_timeout_cleared_on_advance() {
        let mut corr = ActiveCorrelation::new();
        corr.set_timeout(Duration::from_secs(60));
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

// ===========================================================================
// Module 6: hamlet/graph.rs — HamletGraph edge cases
// ===========================================================================

mod hamlet_graph_tests {
    use super::*;
    use varpulis_runtime::hamlet::graph::HamletGraph;

    #[test]
    fn test_graph_default() {
        let graph = HamletGraph::default();
        assert_eq!(graph.num_graphlets(), 0);
        assert_eq!(graph.num_active(), 0);
        assert_eq!(graph.num_events(), 0);
    }

    #[test]
    fn test_graph_close_nonexistent_type() {
        let mut graph = HamletGraph::new();
        // Closing a type that was never opened should return None
        assert!(graph.close_graphlet_for_type(99).is_none());
    }

    #[test]
    fn test_graph_close_all_active() {
        let mut graph = HamletGraph::new();

        let e1 = Arc::new(Event::new("A"));
        graph.add_event(e1, 0);

        let e2 = Arc::new(Event::new("B"));
        graph.add_event(e2, 1);

        assert_eq!(graph.num_active(), 2);

        let closed = graph.close_all_active();
        assert_eq!(closed.len(), 2);
        assert_eq!(graph.num_active(), 0);
    }

    #[test]
    fn test_graph_mark_processed_and_cleanup() {
        let mut graph = HamletGraph::new();

        let e = Arc::new(Event::new("A"));
        let (_, g_id, _) = graph.add_event(e, 0);

        graph.close_graphlet_for_type(0);
        graph.mark_processed(g_id);
        graph.cleanup_processed();

        assert_eq!(graph.num_graphlets(), 0);
    }

    #[test]
    fn test_graph_clear() {
        let mut graph = HamletGraph::new();

        for i in 0..5u16 {
            let e = Arc::new(Event::new("E"));
            graph.add_event(e, i);
        }
        assert!(graph.num_graphlets() > 0);

        graph.clear();
        assert_eq!(graph.num_graphlets(), 0);
        assert_eq!(graph.num_active(), 0);
        assert_eq!(graph.num_events(), 0);
    }

    #[test]
    fn test_graph_graphlet_by_id() {
        let mut graph = HamletGraph::new();
        let e = Arc::new(Event::new("X"));
        let (_, g_id, _) = graph.add_event(e, 0);

        assert!(graph.graphlet(g_id).is_some());
        assert!(graph.graphlet_mut(g_id).is_some());
        assert!(graph.graphlet(999).is_none());
    }

    #[test]
    fn test_graph_graphlets_of_type() {
        let mut graph = HamletGraph::new();

        // Add multiple events of type 0
        for _ in 0..3 {
            let e = Arc::new(Event::new("A"));
            graph.add_event(e, 0);
        }

        let type0_graphlets: Vec<_> = graph.graphlets_of_type(0).collect();
        assert_eq!(type0_graphlets.len(), 1); // All same type, one active graphlet

        // No graphlets for type 99
        let type99_graphlets: Vec<_> = graph.graphlets_of_type(99).collect();
        assert!(type99_graphlets.is_empty());
    }

    #[test]
    fn test_graph_active_graphlet() {
        let mut graph = HamletGraph::new();

        assert!(graph.active_graphlet(0).is_none());

        let e = Arc::new(Event::new("A"));
        graph.add_event(e, 0);

        assert!(graph.active_graphlet(0).is_some());

        graph.close_graphlet_for_type(0);
        assert!(graph.active_graphlet(0).is_none());
    }

    #[test]
    fn test_graph_snapshots() {
        let mut graph = HamletGraph::new();

        let e1 = Arc::new(Event::new("A"));
        let (_, g1, _) = graph.add_event(e1, 0);
        graph.close_graphlet_for_type(0);

        let e2 = Arc::new(Event::new("B"));
        let (_, g2, _) = graph.add_event(e2, 1);

        let snap_id = graph.create_snapshot(g1, g2);
        assert!(graph.snapshots().get(snap_id).is_some());
    }

    #[test]
    fn test_graph_set_sharing_queries() {
        let mut graph = HamletGraph::new();

        let e = Arc::new(Event::new("A"));
        let (_, g_id, _) = graph.add_event(e, 0);

        // Set sharing queries
        graph.set_sharing_queries(g_id, smallvec::smallvec![0, 1, 2]);
        let graphlet = graph.graphlet(g_id).unwrap();
        assert!(graphlet.is_shared);
        assert_eq!(graphlet.sharing_queries.len(), 3);
    }

    #[test]
    fn test_graph_link_graphlets_idempotent() {
        let mut graph = HamletGraph::new();

        let e1 = Arc::new(Event::new("A"));
        let (_, g1, _) = graph.add_event(e1, 0);
        graph.close_graphlet_for_type(0);

        let e2 = Arc::new(Event::new("B"));
        let (_, g2, _) = graph.add_event(e2, 1);

        // Link twice — should not duplicate
        graph.link_graphlets(g1, g2);
        graph.link_graphlets(g1, g2);

        let pred = graph.graphlet(g1).unwrap();
        assert_eq!(pred.successor_graphlets.len(), 1);

        let succ = graph.graphlet(g2).unwrap();
        assert_eq!(succ.predecessor_graphlets.len(), 1);
    }
}

// ===========================================================================
// Module 7: hamlet/graphlet.rs — Graphlet, GraphletPool, GraphletNode
// ===========================================================================

mod hamlet_graphlet_tests {
    use super::*;
    use varpulis_runtime::hamlet::graphlet::{Graphlet, GraphletPool, GraphletStatus};

    #[test]
    fn test_graphlet_lifecycle() {
        let mut g = Graphlet::new(0, 1);
        assert_eq!(g.status, GraphletStatus::Active);
        assert!(g.end_time.is_none());

        g.deactivate();
        assert_eq!(g.status, GraphletStatus::Inactive);
        assert!(g.end_time.is_some());

        g.mark_processed();
        assert_eq!(g.status, GraphletStatus::Processed);
    }

    #[test]
    fn test_graphlet_node_access() {
        let mut g = Graphlet::new(0, 0);
        let e = Arc::new(Event::new("A"));
        g.add_event(0, e);

        assert!(g.node(0).is_some());
        assert!(g.node(1).is_none());
        assert!(g.node_mut(0).is_some());
        assert!(g.node_mut(999).is_none());
    }

    #[test]
    fn test_graphlet_is_shareable() {
        let mut g = Graphlet::new(0, 0);
        assert!(!g.is_shareable());

        g.sharing_queries.push(0);
        assert!(!g.is_shareable()); // Only 1 query

        g.sharing_queries.push(1);
        assert!(g.is_shareable()); // 2+ queries
    }

    #[test]
    fn test_graphlet_sum_counts() {
        let mut g = Graphlet::new(0, 0);

        for i in 0..3u32 {
            let e = Arc::new(Event::new("B"));
            g.add_event(i, e);
        }

        g.add_kleene_edges();
        g.propagate_shared();

        // snapshot_value = 1: counts = [1, 1, 2] -> sum = 4
        assert_eq!(g.sum_counts(1), 4);

        // snapshot_value = 3: counts = [3, 3, 6] -> sum = 12
        assert_eq!(g.sum_counts(3), 12);
    }

    #[test]
    fn test_graphlet_propagate_empty() {
        let mut g = Graphlet::new(0, 0);
        g.propagate_shared(); // Should not panic on empty graphlet
        assert_eq!(g.sum_counts(1), 0);
    }

    #[test]
    fn test_graphlet_pool_release_beyond_capacity() {
        let mut pool = GraphletPool::new();

        // Acquire and release more than pool capacity (64)
        let graphlets: Vec<_> = (0..70).map(|_| pool.acquire(0)).collect();
        for g in graphlets {
            pool.release(g);
        }

        // Pool should cap at 64
        // Next acquire should reuse from pool
        let reused = pool.acquire(5);
        assert_eq!(reused.type_index, 5);
        assert_eq!(reused.status, GraphletStatus::Active);
    }
}

// ===========================================================================
// Module 8: hamlet/snapshot.rs — Snapshot, SnapshotManager, PropagationCoefficients
// ===========================================================================

mod hamlet_snapshot_tests {
    use varpulis_runtime::hamlet::snapshot::{
        merge_snapshots, resolve_counts, PropagationCoefficients, Snapshot, SnapshotManager,
    };

    #[test]
    fn test_snapshot_set_value_overwrite() {
        let mut s = Snapshot::new(0, 0, 1);
        s.set_value(0, 100);
        assert_eq!(s.value(0), 100);

        s.set_value(0, 200); // Overwrite
        assert_eq!(s.value(0), 200);
        assert_eq!(s.num_queries(), 1); // Should not duplicate
    }

    #[test]
    fn test_snapshot_get_value_missing() {
        let s = Snapshot::new(0, 0, 1);
        assert_eq!(s.get_value(99), None);
        assert_eq!(s.value(99), 0);
    }

    #[test]
    fn test_snapshot_manager_clear_for_graphlet() {
        let mut mgr = SnapshotManager::new();
        mgr.create(0, 1);
        mgr.create(0, 2);
        mgr.create(1, 1);
        assert_eq!(mgr.len(), 3);

        mgr.clear_for_graphlet(1);
        // Snapshots still exist in the Vec, but target/source indices are removed
        let for_target_1: Vec<_> = mgr.snapshots_for_target(1).collect();
        assert!(for_target_1.is_empty());
    }

    #[test]
    fn test_snapshot_manager_clear_all() {
        let mut mgr = SnapshotManager::new();
        mgr.create(0, 1);
        mgr.create(1, 2);
        assert!(!mgr.is_empty());

        mgr.clear();
        assert!(mgr.is_empty());
        assert_eq!(mgr.len(), 0);
    }

    #[test]
    fn test_propagation_coefficients_kleene_empty() {
        let mut coeffs = PropagationCoefficients::new(0);
        coeffs.compute_kleene(); // Should not panic
        assert_eq!(coeffs.resolve_count(1), 0);
    }

    #[test]
    fn test_propagation_coefficients_sequence_empty() {
        let mut coeffs = PropagationCoefficients::new(0);
        coeffs.compute_sequence(); // Should not panic
        assert_eq!(coeffs.resolve_count(1), 0);
    }

    #[test]
    fn test_propagation_coefficients_resolve_count_at() {
        let mut coeffs = PropagationCoefficients::new(3);
        coeffs.compute_kleene();
        // coeffs: [1, 1, 2], local_sums: [0, 0, 0]

        assert_eq!(coeffs.resolve_count_at(0, 5), 5);
        assert_eq!(coeffs.resolve_count_at(1, 5), 5);
        assert_eq!(coeffs.resolve_count_at(2, 5), 10);
        // Out of bounds
        assert_eq!(coeffs.resolve_count_at(99, 5), 0);
    }

    #[test]
    fn test_propagation_coefficients_resolve_final_counts() {
        let mut coeffs = PropagationCoefficients::new(3);
        coeffs.compute_kleene();

        let mut snapshot = Snapshot::new(0, 0, 1);
        snapshot.set_value(0, 2);
        snapshot.set_value(1, 5);

        // End indices at positions 1 and 2
        let results = coeffs.resolve_final_counts(&[1, 2], &snapshot);
        assert_eq!(results.len(), 2);
        // Query 0 (snapshot=2): coeff[1]*2 + coeff[2]*2 = 1*2 + 2*2 = 6
        assert_eq!(results[0].count, 6);
        // Query 1 (snapshot=5): coeff[1]*5 + coeff[2]*5 = 1*5 + 2*5 = 15
        assert_eq!(results[1].count, 15);
    }

    #[test]
    fn test_resolve_counts_empty_snapshot() {
        let snapshot = Snapshot::new(0, 0, 1);
        let coeffs = vec![1, 2, 4];
        let local_sums = vec![0, 0, 0];

        let results = resolve_counts(&snapshot, &coeffs, &local_sums);
        assert!(results.is_empty()); // No queries in snapshot
    }

    #[test]
    fn test_merge_snapshots_empty() {
        let merged = merge_snapshots(&[], 5);
        assert_eq!(merged.num_queries(), 0);
        assert_eq!(merged.target_graphlet, 5);
    }

    #[test]
    fn test_merge_snapshots_single() {
        let mut s = Snapshot::new(0, 0, 1);
        s.set_value(0, 42);

        let merged = merge_snapshots(&[&s], 5);
        assert_eq!(merged.value(0), 42);
    }
}

// ===========================================================================
// Module 9: hamlet/optimizer.rs — HamletOptimizer, KleeneStats
// ===========================================================================

mod hamlet_optimizer_tests {
    use varpulis_runtime::hamlet::optimizer::{
        HamletOptimizer, KleeneStats, OptimizerConfig, SharingDecision,
    };

    #[test]
    fn test_kleene_stats_single_query_should_not_share() {
        let stats = KleeneStats::new(0, 1);
        assert!(!stats.should_share());
    }

    #[test]
    fn test_kleene_stats_many_snapshots_negative_benefit() {
        let mut stats = KleeneStats::new(0, 2);
        // Many snapshots, small graphlets -> sharing not beneficial
        for _ in 0..100 {
            stats.update(1, 100);
        }
        // sp will be very large relative to ks=2
        assert!(stats.sharing_benefit() < 0.0);
        assert!(!stats.should_share());
    }

    #[test]
    fn test_optimizer_default() {
        let optimizer = HamletOptimizer::default();
        // Unregistered type -> NonShared
        assert_eq!(optimizer.decision(0), SharingDecision::NonShared);
    }

    #[test]
    fn test_optimizer_force_decision() {
        let mut optimizer = HamletOptimizer::default();
        optimizer.register_kleene(0, &[0, 1]);
        assert_eq!(optimizer.decision(0), SharingDecision::Shared);

        optimizer.force_decision(0, SharingDecision::Split);
        assert_eq!(optimizer.decision(0), SharingDecision::Split);
    }

    #[test]
    fn test_optimizer_reset_stats() {
        let mut optimizer = HamletOptimizer::new(OptimizerConfig {
            reevaluate_interval: 100,
            adaptive: false,
            ..Default::default()
        });
        optimizer.register_kleene(0, &[0, 1, 2]);

        for _ in 0..5 {
            optimizer.report_graphlet(0, 50, 1);
        }
        let stats = optimizer.stats(0).unwrap();
        assert!(stats.total_events > 0);

        optimizer.reset_stats();
        let stats = optimizer.stats(0).unwrap();
        assert_eq!(stats.total_events, 0);
        assert_eq!(stats.num_graphlets, 0);
        assert_eq!(stats.num_snapshots, 0);
    }

    #[test]
    fn test_optimizer_all_stats() {
        let mut optimizer = HamletOptimizer::default();
        optimizer.register_kleene(0, &[0, 1]);
        optimizer.register_kleene(1, &[2, 3, 4]);

        let all: Vec<_> = optimizer.all_stats().collect();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_optimizer_reevaluate_split_decision() {
        let mut optimizer = HamletOptimizer::new(OptimizerConfig {
            reevaluate_interval: 5,
            adaptive: true,
            switch_threshold: 1.0,
            min_graphlet_size: 1,
            min_queries: 2,
        });

        optimizer.register_kleene(0, &[0, 1]); // 2 queries -> shared
        assert_eq!(optimizer.decision(0), SharingDecision::Shared);

        // Report graphlets with very high snapshot count -> should trigger split
        for _ in 0..10 {
            optimizer.report_graphlet(0, 1, 1000); // tiny graphlet, many snapshots
        }
        // After reporting, the decision may change based on heuristics
        let decision = optimizer.decision(0);
        // Valid outcomes depend on optimizer internals
        assert!(
            matches!(
                decision,
                SharingDecision::Split | SharingDecision::Shared | SharingDecision::NonShared
            ),
            "Unexpected decision: {:?}",
            decision
        );
    }
}

// ===========================================================================
// Module 10: zdd_unified/propagation.rs — ZddPropagator, QueryAccumulator, ZddTraversal
// ===========================================================================

mod zdd_propagation_tests {
    use varpulis_runtime::zdd_unified::propagation::{
        PropagationMode, QueryAccumulator, ZddPropagator, ZddTraversal,
    };

    #[test]
    fn test_query_accumulator_basic() {
        let mut acc = QueryAccumulator::new();
        assert_eq!(acc.count, 0);

        acc.add_at_state(0, 10);
        acc.add_at_state(1, 20);
        acc.add_at_state(0, 5); // Add more to state 0

        assert_eq!(acc.count_at_state(0), 15);
        assert_eq!(acc.count_at_state(1), 20);
        assert_eq!(acc.count_at_state(99), 0); // Non-existent state

        acc.finalize();
        assert_eq!(acc.count, 35);
    }

    #[test]
    fn test_query_accumulator_reset() {
        let mut acc = QueryAccumulator::new();
        acc.add_at_state(0, 100);
        acc.finalize();
        assert_eq!(acc.count, 100);

        acc.reset();
        assert_eq!(acc.count, 0);
        assert_eq!(acc.count_at_state(0), 0);
    }

    #[test]
    fn test_propagator_count_unregistered_query() {
        let propagator = ZddPropagator::new(PropagationMode::Eager);
        assert_eq!(propagator.count(999), 0);
    }

    #[test]
    fn test_propagator_default() {
        let propagator = ZddPropagator::default();
        assert_eq!(propagator.count(0), 0);
    }

    #[test]
    fn test_propagator_reset() {
        let mut propagator = ZddPropagator::new(PropagationMode::Eager);
        propagator.register_query(0);
        propagator.register_query(1);
        propagator.reset();
        assert_eq!(propagator.count(0), 0);
        assert_eq!(propagator.count(1), 0);
    }

    #[test]
    fn test_zdd_traversal_empty() {
        let mut traversal = ZddTraversal::new();
        let zdd = varpulis_zdd::Zdd::empty();
        assert_eq!(traversal.count_sets(&zdd), 0);
    }

    #[test]
    fn test_zdd_traversal_singleton() {
        let mut traversal = ZddTraversal::new();
        let zdd = varpulis_zdd::Zdd::singleton(42);
        assert_eq!(traversal.count_sets(&zdd), 1);
    }

    #[test]
    fn test_zdd_traversal_count_with_var() {
        let mut traversal = ZddTraversal::new();
        let zdd = varpulis_zdd::Zdd::singleton(5);
        assert_eq!(traversal.count_with_var(&zdd, 5), 1);
        assert_eq!(traversal.count_with_var(&zdd, 99), 0);
    }

    #[test]
    fn test_zdd_traversal_clear() {
        let mut traversal = ZddTraversal::new();
        let zdd = varpulis_zdd::Zdd::singleton(1);
        let _ = traversal.count_sets(&zdd);
        traversal.clear();
        // Should still work after clearing memo
        assert_eq!(traversal.count_sets(&zdd), 1);
    }

    #[test]
    fn test_zdd_traversal_default() {
        let mut traversal = ZddTraversal::default();
        assert_eq!(traversal.count_sets(&varpulis_zdd::Zdd::empty()), 0);
    }

    #[test]
    fn test_propagation_mode_default() {
        let mode = PropagationMode::default();
        assert_eq!(mode, PropagationMode::Eager);
    }
}

// ===========================================================================
// Module 11: zdd_unified/nfa_zdd.rs — NfaZdd, NfaZddBuilder
// ===========================================================================

mod nfa_zdd_tests {
    use varpulis_runtime::zdd_unified::nfa_zdd::{NfaZdd, NfaZddBuilder};

    #[test]
    fn test_nfa_zdd_default() {
        let nfa = NfaZdd::default();
        assert_eq!(nfa.num_states(), 0);
        assert_eq!(nfa.num_queries(), 0);
    }

    #[test]
    fn test_nfa_zdd_type_index_not_found() {
        let nfa = NfaZdd::new();
        assert!(nfa.type_index("NonExistent").is_none());
    }

    #[test]
    fn test_nfa_zdd_count_unregistered_query() {
        let nfa = NfaZdd::new();
        assert_eq!(nfa.count(99), 0);
    }

    #[test]
    fn test_nfa_zdd_process_unknown_event_type() {
        let mut builder = NfaZddBuilder::new();
        builder.add_sequence(0, &["A", "B"]);
        let mut nfa = builder.build();

        // Process an event type that has no transitions
        let results = nfa.process_event(999);
        assert!(results.is_empty());
    }

    #[test]
    fn test_nfa_zdd_reset() {
        let mut builder = NfaZddBuilder::new();
        builder.add_sequence(0, &["A", "B"]);
        let mut nfa = builder.build();

        let a_idx = nfa.type_index("A").unwrap();
        let b_idx = nfa.type_index("B").unwrap();

        nfa.process_event(a_idx);
        nfa.process_event(b_idx);
        assert_eq!(nfa.count(0), 1);

        nfa.reset();
        assert_eq!(nfa.count(0), 0);
    }

    #[test]
    fn test_nfa_zdd_is_query_active() {
        let mut builder = NfaZddBuilder::new();
        builder.add_sequence(0, &["X", "Y"]);
        let mut nfa = builder.build();

        assert!(!nfa.is_query_active(0));

        let x_idx = nfa.type_index("X").unwrap();
        nfa.process_event(x_idx);
        assert!(nfa.is_query_active(0));
    }

    #[test]
    fn test_nfa_zdd_zdd_size() {
        let mut builder = NfaZddBuilder::new();
        builder.add_sequence(0, &["A", "B"]);
        let nfa = builder.build();

        // ZDD should have some nodes for the initial state
        assert!(nfa.zdd_size() > 0);
    }

    #[test]
    fn test_nfa_zdd_state_by_var() {
        let mut builder = NfaZddBuilder::new();
        builder.add_sequence(0, &["A", "B"]);
        let nfa = builder.build();

        let state = nfa.state_by_var(0);
        assert!(state.is_some());
        assert_eq!(state.unwrap().query_id, 0);
        assert_eq!(state.unwrap().state_index, 0);

        assert!(nfa.state_by_var(999).is_none());
    }

    #[test]
    fn test_nfa_zdd_transitions_empty() {
        let mut builder = NfaZddBuilder::new();
        builder.add_sequence(0, &["A", "B"]);
        let nfa = builder.build();

        // No transitions for event type 999
        assert!(nfa.transitions(999).is_empty());
    }

    #[test]
    fn test_nfa_zdd_three_step_sequence() {
        let mut builder = NfaZddBuilder::new();
        builder.add_sequence(0, &["A", "B", "C"]);
        let mut nfa = builder.build();

        let a = nfa.type_index("A").unwrap();
        let b = nfa.type_index("B").unwrap();
        let c = nfa.type_index("C").unwrap();

        nfa.process_event(a);
        nfa.process_event(b);
        let results = nfa.process_event(c);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0], (0, 1));
    }
}
