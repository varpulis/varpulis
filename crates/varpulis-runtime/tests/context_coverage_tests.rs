//! Coverage tests for the context module (context.rs).
//!
//! Tests cover: ContextMap, ContextConfig, CheckpointCoordinator,
//! CheckpointBarrier, ContextMessage, filter_program_for_context,
//! and edge cases. EventTypeRouter is tested via ContextOrchestrator.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use varpulis_runtime::context::*;
use varpulis_runtime::event::Event;
use varpulis_runtime::persistence::{
    CheckpointConfig, CheckpointManager, EngineCheckpoint, MemoryStore,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
fn make_event(event_type: &str) -> Arc<Event> {
    Arc::new(Event::new(event_type))
}

fn make_engine_checkpoint() -> EngineCheckpoint {
    EngineCheckpoint {
        window_states: HashMap::new(),
        sase_states: HashMap::new(),
        join_states: HashMap::new(),
        variables: HashMap::new(),
        events_processed: 0,
        output_events_emitted: 0,
        watermark_state: None,
        distinct_states: HashMap::new(),
        limit_states: HashMap::new(),
    }
}

fn make_checkpoint_manager() -> CheckpointManager {
    let store = Arc::new(MemoryStore::new());
    let config = CheckpointConfig {
        interval: Duration::from_secs(300),
        max_checkpoints: 5,
        checkpoint_on_shutdown: false,
        key_prefix: "test".to_string(),
    };
    CheckpointManager::new(store, config).unwrap()
}

// ===========================================================================
// ContextMap tests
// ===========================================================================

#[test]
fn test_context_map_default() {
    let map = ContextMap::default();
    assert!(!map.has_contexts());
    assert!(map.contexts().is_empty());
    assert!(map.stream_assignments().is_empty());
    assert!(map.cross_context_emits().is_empty());
}

#[test]
fn test_context_map_register_multiple_contexts() {
    let mut map = ContextMap::new();
    map.register_context(ContextConfig {
        name: "ctx_a".to_string(),
        cores: Some(vec![0]),
    });
    map.register_context(ContextConfig {
        name: "ctx_b".to_string(),
        cores: Some(vec![1, 2]),
    });
    map.register_context(ContextConfig {
        name: "ctx_c".to_string(),
        cores: None,
    });

    assert!(map.has_contexts());
    assert_eq!(map.contexts().len(), 3);

    let ctx_a = map.contexts().get("ctx_a").unwrap();
    assert_eq!(ctx_a.cores, Some(vec![0]));

    let ctx_c = map.contexts().get("ctx_c").unwrap();
    assert!(ctx_c.cores.is_none());
}

#[test]
fn test_context_map_overwrite_context() {
    let mut map = ContextMap::new();
    map.register_context(ContextConfig {
        name: "ctx".to_string(),
        cores: Some(vec![0]),
    });
    // Re-register with different cores
    map.register_context(ContextConfig {
        name: "ctx".to_string(),
        cores: Some(vec![3, 4]),
    });

    assert_eq!(map.contexts().len(), 1);
    let ctx = map.contexts().get("ctx").unwrap();
    assert_eq!(ctx.cores, Some(vec![3, 4]));
}

#[test]
fn test_context_map_stream_assignment_multiple() {
    let mut map = ContextMap::new();
    map.register_context(ContextConfig {
        name: "fast".to_string(),
        cores: None,
    });
    map.register_context(ContextConfig {
        name: "slow".to_string(),
        cores: None,
    });

    map.assign_stream("Stream1".to_string(), "fast".to_string());
    map.assign_stream("Stream2".to_string(), "slow".to_string());
    map.assign_stream("Stream3".to_string(), "fast".to_string());

    assert_eq!(map.stream_context("Stream1"), Some("fast"));
    assert_eq!(map.stream_context("Stream2"), Some("slow"));
    assert_eq!(map.stream_context("Stream3"), Some("fast"));
    assert_eq!(map.stream_context("Stream4"), None);
    assert_eq!(map.stream_assignments().len(), 3);
}

#[test]
fn test_context_map_reassign_stream() {
    let mut map = ContextMap::new();
    map.assign_stream("MyStream".to_string(), "ctx1".to_string());
    assert_eq!(map.stream_context("MyStream"), Some("ctx1"));

    // Reassign to a different context
    map.assign_stream("MyStream".to_string(), "ctx2".to_string());
    assert_eq!(map.stream_context("MyStream"), Some("ctx2"));
    assert_eq!(map.stream_assignments().len(), 1);
}

#[test]
fn test_context_map_cross_context_emits_multiple() {
    let mut map = ContextMap::new();
    map.add_cross_context_emit("Source".to_string(), 0, "target_ctx".to_string());
    map.add_cross_context_emit("Source".to_string(), 1, "other_ctx".to_string());
    map.add_cross_context_emit("OtherStream".to_string(), 0, "target_ctx".to_string());

    let emits = map.cross_context_emits();
    assert_eq!(emits.len(), 3);
    assert_eq!(
        emits.get(&("Source".to_string(), 0)),
        Some(&"target_ctx".to_string())
    );
    assert_eq!(
        emits.get(&("Source".to_string(), 1)),
        Some(&"other_ctx".to_string())
    );
    assert_eq!(
        emits.get(&("OtherStream".to_string(), 0)),
        Some(&"target_ctx".to_string())
    );
}

#[test]
fn test_context_map_no_contexts_backward_compat() {
    let map = ContextMap::new();
    assert!(!map.has_contexts());
    assert!(map.stream_context("anything").is_none());
}

#[test]
fn test_context_map_clone() {
    let mut map = ContextMap::new();
    map.register_context(ContextConfig {
        name: "original".to_string(),
        cores: Some(vec![0]),
    });
    map.assign_stream("S1".to_string(), "original".to_string());

    let cloned = map.clone();
    assert!(cloned.has_contexts());
    assert_eq!(cloned.stream_context("S1"), Some("original"));
}

// ===========================================================================
// ContextConfig tests
// ===========================================================================

#[test]
fn test_context_config_with_cores() {
    let config = ContextConfig {
        name: "compute".to_string(),
        cores: Some(vec![0, 1, 2, 3]),
    };
    assert_eq!(config.name, "compute");
    assert_eq!(config.cores.as_ref().unwrap().len(), 4);
}

#[test]
fn test_context_config_debug_and_clone() {
    let config = ContextConfig {
        name: "test".to_string(),
        cores: Some(vec![5]),
    };
    let debug_str = format!("{:?}", config);
    assert!(debug_str.contains("test"));

    let cloned = config.clone();
    assert_eq!(cloned.name, "test");
    assert_eq!(cloned.cores, Some(vec![5]));
}

// ===========================================================================
// CheckpointBarrier tests
// ===========================================================================

#[test]
fn test_checkpoint_barrier_create() {
    let barrier = CheckpointBarrier {
        checkpoint_id: 42,
        timestamp_ms: 1700000000000,
    };
    assert_eq!(barrier.checkpoint_id, 42);
    assert_eq!(barrier.timestamp_ms, 1700000000000);
}

#[test]
fn test_checkpoint_barrier_clone_and_debug() {
    let barrier = CheckpointBarrier {
        checkpoint_id: 1,
        timestamp_ms: 999,
    };
    let cloned = barrier.clone();
    assert_eq!(cloned.checkpoint_id, 1);
    assert_eq!(cloned.timestamp_ms, 999);

    let debug = format!("{:?}", barrier);
    assert!(debug.contains("checkpoint_id"));
}

// ===========================================================================
// ContextMessage tests
// ===========================================================================

#[test]
fn test_context_message_event_variant() {
    let event = make_event("TestEvent");
    let msg = ContextMessage::Event(event);
    match msg {
        ContextMessage::Event(e) => assert_eq!(&*e.event_type, "TestEvent"),
        _ => panic!("Expected Event variant"),
    }
}

#[test]
fn test_context_message_checkpoint_barrier_variant() {
    let barrier = CheckpointBarrier {
        checkpoint_id: 5,
        timestamp_ms: 1000,
    };
    let msg = ContextMessage::CheckpointBarrier(barrier);
    match msg {
        ContextMessage::CheckpointBarrier(b) => assert_eq!(b.checkpoint_id, 5),
        _ => panic!("Expected CheckpointBarrier variant"),
    }
}

#[test]
fn test_context_message_watermark_variant() {
    let msg = ContextMessage::WatermarkUpdate {
        source_context: "upstream".to_string(),
        watermark_ms: 5000,
    };
    match msg {
        ContextMessage::WatermarkUpdate {
            source_context,
            watermark_ms,
        } => {
            assert_eq!(source_context, "upstream");
            assert_eq!(watermark_ms, 5000);
        }
        _ => panic!("Expected WatermarkUpdate variant"),
    }
}

#[test]
fn test_context_message_clone() {
    let event = make_event("Cloneable");
    let msg = ContextMessage::Event(event);
    let cloned = msg.clone();
    match cloned {
        ContextMessage::Event(e) => assert_eq!(&*e.event_type, "Cloneable"),
        _ => panic!("Expected Event variant"),
    }
}

#[test]
fn test_context_message_debug() {
    let event = make_event("Debug");
    let msg = ContextMessage::Event(event);
    let debug = format!("{:?}", msg);
    assert!(debug.contains("Event"));
}

// ===========================================================================
// CheckpointCoordinator tests
// ===========================================================================

#[test]
fn test_coordinator_new_no_pending() {
    let manager = make_checkpoint_manager();
    let names = vec!["ctx1".to_string(), "ctx2".to_string()];
    let coord = CheckpointCoordinator::new(manager, names);
    assert!(!coord.has_pending());
}

#[test]
fn test_coordinator_ack_sender() {
    let manager = make_checkpoint_manager();
    let names = vec!["ctx1".to_string()];
    let coord = CheckpointCoordinator::new(manager, names);
    let _sender = coord.ack_sender();
    // Verify ack_sender returns a usable sender without panic
}

#[test]
fn test_coordinator_initiate_sets_pending() {
    let manager = make_checkpoint_manager();
    let names = vec!["ctx1".to_string(), "ctx2".to_string()];
    let mut coord = CheckpointCoordinator::new(manager, names);

    let mut txs = rustc_hash::FxHashMap::default();
    let (tx1, _rx1) = tokio::sync::mpsc::channel(10);
    let (tx2, _rx2) = tokio::sync::mpsc::channel(10);
    txs.insert("ctx1".to_string(), tx1);
    txs.insert("ctx2".to_string(), tx2);

    assert!(!coord.has_pending());
    coord.initiate(&txs);
    assert!(coord.has_pending());
}

#[test]
fn test_coordinator_double_initiate_skips() {
    let manager = make_checkpoint_manager();
    let names = vec!["ctx1".to_string()];
    let mut coord = CheckpointCoordinator::new(manager, names);

    let mut txs = rustc_hash::FxHashMap::default();
    let (tx, _rx) = tokio::sync::mpsc::channel(10);
    txs.insert("ctx1".to_string(), tx);

    coord.initiate(&txs);
    assert!(coord.has_pending());

    // Second initiate should be skipped (warning logged)
    coord.initiate(&txs);
    assert!(coord.has_pending());
}

#[test]
fn test_coordinator_receive_ack_completes_checkpoint() {
    let manager = make_checkpoint_manager();
    let names = vec!["ctx1".to_string(), "ctx2".to_string()];
    let mut coord = CheckpointCoordinator::new(manager, names);

    let mut txs = rustc_hash::FxHashMap::default();
    let (tx1, _rx1) = tokio::sync::mpsc::channel(10);
    let (tx2, _rx2) = tokio::sync::mpsc::channel(10);
    txs.insert("ctx1".to_string(), tx1);
    txs.insert("ctx2".to_string(), tx2);

    coord.initiate(&txs);

    // First ack: not complete yet
    let result1 = coord.receive_ack(CheckpointAck {
        context_name: "ctx1".to_string(),
        checkpoint_id: 1,
        engine_checkpoint: make_engine_checkpoint(),
    });
    assert!(result1.is_none());
    assert!(coord.has_pending());

    // Second ack: completes checkpoint
    let result2 = coord.receive_ack(CheckpointAck {
        context_name: "ctx2".to_string(),
        checkpoint_id: 1,
        engine_checkpoint: make_engine_checkpoint(),
    });
    assert!(result2.is_some());
    assert!(!coord.has_pending());

    let checkpoint = result2.unwrap();
    assert_eq!(checkpoint.id, 1);
    assert_eq!(checkpoint.context_states.len(), 2);
    assert!(checkpoint.context_states.contains_key("ctx1"));
    assert!(checkpoint.context_states.contains_key("ctx2"));
}

#[test]
fn test_coordinator_receive_ack_wrong_checkpoint_id() {
    let manager = make_checkpoint_manager();
    let names = vec!["ctx1".to_string()];
    let mut coord = CheckpointCoordinator::new(manager, names);

    let mut txs = rustc_hash::FxHashMap::default();
    let (tx, _rx) = tokio::sync::mpsc::channel(10);
    txs.insert("ctx1".to_string(), tx);

    coord.initiate(&txs);

    // Ack with wrong checkpoint ID
    let result = coord.receive_ack(CheckpointAck {
        context_name: "ctx1".to_string(),
        checkpoint_id: 999,
        engine_checkpoint: make_engine_checkpoint(),
    });
    assert!(result.is_none());
    assert!(coord.has_pending()); // Still pending
}

#[test]
fn test_coordinator_receive_ack_no_pending() {
    let manager = make_checkpoint_manager();
    let names = vec!["ctx1".to_string()];
    let mut coord = CheckpointCoordinator::new(manager, names);

    // No checkpoint initiated
    let result = coord.receive_ack(CheckpointAck {
        context_name: "ctx1".to_string(),
        checkpoint_id: 1,
        engine_checkpoint: make_engine_checkpoint(),
    });
    assert!(result.is_none());
}

#[test]
fn test_coordinator_try_complete_no_pending() {
    let manager = make_checkpoint_manager();
    let names = vec!["ctx1".to_string()];
    let mut coord = CheckpointCoordinator::new(manager, names);

    // No pending checkpoint, try_complete should succeed silently
    let result = coord.try_complete();
    assert!(result.is_ok());
}

#[test]
fn test_coordinator_should_checkpoint_interval() {
    let store = Arc::new(MemoryStore::new());
    let config = CheckpointConfig {
        interval: Duration::from_millis(1), // Very short interval
        max_checkpoints: 5,
        checkpoint_on_shutdown: false,
        key_prefix: "test".to_string(),
    };
    let manager = CheckpointManager::new(store, config).unwrap();
    let names = vec!["ctx1".to_string()];
    let coord = CheckpointCoordinator::new(manager, names);

    // With a 1ms interval, should_checkpoint should quickly become true
    std::thread::sleep(Duration::from_millis(5));
    assert!(coord.should_checkpoint());
}

#[test]
fn test_coordinator_should_not_checkpoint_when_pending() {
    let store = Arc::new(MemoryStore::new());
    let config = CheckpointConfig {
        interval: Duration::from_millis(1),
        max_checkpoints: 5,
        checkpoint_on_shutdown: false,
        key_prefix: "test".to_string(),
    };
    let manager = CheckpointManager::new(store, config).unwrap();
    let names = vec!["ctx1".to_string()];
    let mut coord = CheckpointCoordinator::new(manager, names);

    let mut txs = rustc_hash::FxHashMap::default();
    let (tx, _rx) = tokio::sync::mpsc::channel(10);
    txs.insert("ctx1".to_string(), tx);

    coord.initiate(&txs);
    std::thread::sleep(Duration::from_millis(5));

    // Even though interval passed, should_checkpoint is false because there is a pending one
    assert!(!coord.should_checkpoint());
}

#[test]
fn test_coordinator_single_context_flow() {
    let manager = make_checkpoint_manager();
    let names = vec!["solo".to_string()];
    let mut coord = CheckpointCoordinator::new(manager, names);

    let mut txs = rustc_hash::FxHashMap::default();
    let (tx, _rx) = tokio::sync::mpsc::channel(10);
    txs.insert("solo".to_string(), tx);

    coord.initiate(&txs);
    assert!(coord.has_pending());

    let result = coord.receive_ack(CheckpointAck {
        context_name: "solo".to_string(),
        checkpoint_id: 1,
        engine_checkpoint: make_engine_checkpoint(),
    });
    assert!(result.is_some());
    assert!(!coord.has_pending());

    let cp = result.unwrap();
    assert_eq!(cp.context_states.len(), 1);
    assert!(cp.context_states.contains_key("solo"));
}

// ===========================================================================
// filter_program_for_context tests
// ===========================================================================

#[test]
fn test_filter_program_empty_program() {
    use varpulis_core::ast::Program;

    let program = Program { statements: vec![] };
    let map = ContextMap::new();

    let filtered = filter_program_for_context(&program, "any", &map);
    assert!(filtered.statements.is_empty());
}

#[test]
fn test_filter_program_unassigned_streams_kept() {
    use varpulis_core::ast::{Program, Stmt, StreamSource};
    use varpulis_core::span::{Span, Spanned};

    let program = Program {
        statements: vec![Spanned {
            node: Stmt::StreamDecl {
                name: "Orphan".to_string(),
                type_annotation: None,
                source: StreamSource::Ident("Event".to_string()),
                ops: vec![],
            },
            span: Span::dummy(),
        }],
    };

    let mut map = ContextMap::new();
    map.register_context(ContextConfig {
        name: "ctx1".to_string(),
        cores: None,
    });
    // "Orphan" is NOT assigned to any context

    let filtered = filter_program_for_context(&program, "ctx1", &map);
    // Unassigned streams are kept in all contexts for backward compat
    assert_eq!(filtered.statements.len(), 1);
}

#[test]
fn test_filter_program_non_stream_stmts_preserved() {
    use varpulis_core::ast::{Program, Stmt};
    use varpulis_core::span::{Span, Spanned};

    let program = Program {
        statements: vec![
            Spanned {
                node: Stmt::ContextDecl {
                    name: "ctx1".to_string(),
                    cores: None,
                },
                span: Span::dummy(),
            },
            Spanned {
                node: Stmt::ContextDecl {
                    name: "ctx2".to_string(),
                    cores: None,
                },
                span: Span::dummy(),
            },
        ],
    };

    let map = ContextMap::new();
    let filtered = filter_program_for_context(&program, "ctx1", &map);
    // Non-stream statements (ContextDecl) are always preserved
    assert_eq!(filtered.statements.len(), 2);
}

#[test]
fn test_filter_program_excludes_other_context_streams() {
    use varpulis_core::ast::{Program, Stmt, StreamSource};
    use varpulis_core::span::{Span, Spanned};

    let program = Program {
        statements: vec![
            Spanned {
                node: Stmt::StreamDecl {
                    name: "StreamA".to_string(),
                    type_annotation: None,
                    source: StreamSource::Ident("EventA".to_string()),
                    ops: vec![],
                },
                span: Span::dummy(),
            },
            Spanned {
                node: Stmt::StreamDecl {
                    name: "StreamB".to_string(),
                    type_annotation: None,
                    source: StreamSource::Ident("EventB".to_string()),
                    ops: vec![],
                },
                span: Span::dummy(),
            },
        ],
    };

    let mut map = ContextMap::new();
    map.register_context(ContextConfig {
        name: "ctx1".to_string(),
        cores: None,
    });
    map.register_context(ContextConfig {
        name: "ctx2".to_string(),
        cores: None,
    });
    map.assign_stream("StreamA".to_string(), "ctx1".to_string());
    map.assign_stream("StreamB".to_string(), "ctx2".to_string());

    let filtered_ctx1 = filter_program_for_context(&program, "ctx1", &map);
    let filtered_ctx2 = filter_program_for_context(&program, "ctx2", &map);

    // ctx1 should have StreamA only
    let ctx1_streams: Vec<_> = filtered_ctx1
        .statements
        .iter()
        .filter_map(|s| {
            if let Stmt::StreamDecl { name, .. } = &s.node {
                Some(name.as_str())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(ctx1_streams, vec!["StreamA"]);

    // ctx2 should have StreamB only
    let ctx2_streams: Vec<_> = filtered_ctx2
        .statements
        .iter()
        .filter_map(|s| {
            if let Stmt::StreamDecl { name, .. } = &s.node {
                Some(name.as_str())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(ctx2_streams, vec!["StreamB"]);
}

#[test]
fn test_filter_program_mixed_stmts() {
    use varpulis_core::ast::{Program, Stmt, StreamSource};
    use varpulis_core::span::{Span, Spanned};

    let program = Program {
        statements: vec![
            Spanned {
                node: Stmt::ContextDecl {
                    name: "ctx1".to_string(),
                    cores: None,
                },
                span: Span::dummy(),
            },
            Spanned {
                node: Stmt::StreamDecl {
                    name: "Assigned".to_string(),
                    type_annotation: None,
                    source: StreamSource::Ident("E".to_string()),
                    ops: vec![],
                },
                span: Span::dummy(),
            },
            Spanned {
                node: Stmt::StreamDecl {
                    name: "NotAssigned".to_string(),
                    type_annotation: None,
                    source: StreamSource::Ident("F".to_string()),
                    ops: vec![],
                },
                span: Span::dummy(),
            },
        ],
    };

    let mut map = ContextMap::new();
    map.register_context(ContextConfig {
        name: "ctx1".to_string(),
        cores: None,
    });
    map.assign_stream("Assigned".to_string(), "ctx1".to_string());
    // "NotAssigned" has no context assignment

    let filtered = filter_program_for_context(&program, "ctx1", &map);
    // ContextDecl (1) + Assigned stream (1) + unassigned stream (1) = 3
    assert_eq!(filtered.statements.len(), 3);
}
