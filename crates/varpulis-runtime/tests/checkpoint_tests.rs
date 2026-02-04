//! Integration tests for engine checkpoint/restore
//!
//! Each test is backed by a `.vpl` file in `tests/scenarios/` and optionally
//! by `.evt` event files. The VPL programs are loaded from disk, validated,
//! and tested end-to-end.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;
use varpulis_runtime::event_file::EventFileParser;
use varpulis_runtime::persistence::{
    Checkpoint, CheckpointConfig, CheckpointManager, EngineCheckpoint, MemoryStore,
};
use varpulis_runtime::StateStore;

/// Path to the test scenarios directory (relative to workspace root)
const SCENARIOS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/scenarios");

/// Load a VPL program from a scenario file
fn load_vpl(filename: &str) -> varpulis_core::ast::Program {
    let path = format!("{}/{}", SCENARIOS_DIR, filename);
    let source =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("Failed to read {}: {}", path, e));
    parse(&source).unwrap_or_else(|e| panic!("Failed to parse {}: {:?}", path, e))
}

/// Load and parse an event file from the scenarios directory
fn load_events(filename: &str) -> Vec<varpulis_runtime::event_file::TimedEvent> {
    let path = format!("{}/{}", SCENARIOS_DIR, filename);
    let source =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("Failed to read {}: {}", path, e));
    EventFileParser::parse(&source)
        .unwrap_or_else(|e| panic!("Failed to parse events {}: {}", path, e))
}

/// Helper to create a test engine and output receiver
fn create_engine() -> (Engine, mpsc::Receiver<Event>) {
    let (tx, rx) = mpsc::channel::<Event>(1000);
    (Engine::new(tx), rx)
}

// =============================================================================
// Basic Checkpoint/Restore Tests
// =============================================================================

#[tokio::test]
async fn test_engine_checkpoint_restore_count_window() {
    // Uses: checkpoint_count_window.vpl + checkpoint_count_window_phase1.evt
    let program = load_vpl("checkpoint_count_window.vpl");

    // Phase 1: Process 2 events, then checkpoint
    let (mut engine1, mut rx1) = create_engine();
    engine1.load(&program).expect("Failed to load program");

    // Use first 2 events from phase1 event file
    let phase1_events = load_events("checkpoint_count_window_phase1.evt");
    for timed in phase1_events.iter().take(2) {
        engine1
            .process(timed.event.clone())
            .await
            .expect("Failed to process");
    }

    // No output yet (window needs 5 events, only 2 sent)
    assert!(
        rx1.try_recv().is_err(),
        "Window should not have emitted yet"
    );

    // Checkpoint
    let checkpoint = engine1.create_checkpoint();
    assert_eq!(checkpoint.events_processed, 2);

    // Phase 2: Restore into new engine, send 3rd event from phase1 + 2 from phase2
    let (mut engine2, mut rx2) = create_engine();
    engine2.load(&program).expect("Failed to load program");
    engine2.restore_checkpoint(&checkpoint);

    // Send remaining event from phase1
    engine2
        .process(phase1_events[2].event.clone())
        .await
        .expect("Failed to process");

    // Send 2 events from phase2 to fill the window (2 restored + 1 + 2 = 5)
    let phase2_events = load_events("checkpoint_count_window_phase2.evt");
    for timed in &phase2_events {
        engine2
            .process(timed.event.clone())
            .await
            .expect("Failed to process");
    }

    // Window should now emit (5 events total)
    let output = rx2
        .try_recv()
        .expect("Should have emitted after window fill");
    assert_eq!(
        output.data.get("n"),
        Some(&varpulis_core::Value::Int(5)),
        "Count should be 5"
    );
}

#[tokio::test]
async fn test_engine_checkpoint_restore_metrics() {
    // Uses: checkpoint_passthrough.vpl + checkpoint_passthrough.evt
    let program = load_vpl("checkpoint_passthrough.vpl");
    let events = load_events("checkpoint_passthrough.evt");

    let (mut engine1, _rx1) = create_engine();
    engine1.load(&program).expect("Failed to load program");

    for timed in &events {
        engine1
            .process(timed.event.clone())
            .await
            .expect("Failed to process");
    }

    let checkpoint = engine1.create_checkpoint();
    assert_eq!(checkpoint.events_processed, 5);
    assert_eq!(checkpoint.output_events_emitted, 5);

    // Restore and verify metrics continue
    let (mut engine2, _rx2) = create_engine();
    engine2.load(&program).expect("Failed to load program");
    engine2.restore_checkpoint(&checkpoint);

    let metrics = engine2.metrics();
    assert_eq!(metrics.events_processed, 5);
    assert_eq!(metrics.output_events_emitted, 5);

    // Process more events
    for i in 5..8 {
        let event = Event::new("TestEvent").with_field("value", i);
        engine2.process(event).await.expect("Failed to process");
    }

    let metrics2 = engine2.metrics();
    assert_eq!(metrics2.events_processed, 8);
    assert_eq!(metrics2.output_events_emitted, 8);
}

#[tokio::test]
async fn test_engine_checkpoint_serialization_roundtrip() {
    // Uses: checkpoint_serialization.vpl
    let program = load_vpl("checkpoint_serialization.vpl");

    let (mut engine, _rx) = create_engine();
    engine.load(&program).expect("Failed to load program");

    for i in 0..3 {
        let event = Event::new("TestEvent").with_field("value", i * 10);
        engine.process(event).await.expect("Failed to process");
    }

    let checkpoint = engine.create_checkpoint();

    // Serialize to JSON and back
    let json = serde_json::to_string(&checkpoint).expect("Failed to serialize");
    let restored: EngineCheckpoint = serde_json::from_str(&json).expect("Failed to deserialize");

    assert_eq!(restored.events_processed, checkpoint.events_processed);
    assert_eq!(
        restored.output_events_emitted,
        checkpoint.output_events_emitted
    );
}

#[tokio::test]
async fn test_engine_checkpoint_empty_state() {
    // Uses: checkpoint_passthrough.vpl (but no events)
    let program = load_vpl("checkpoint_passthrough.vpl");
    let (mut engine, _rx) = create_engine();
    engine.load(&program).expect("Failed to load program");

    // Checkpoint with no events processed
    let checkpoint = engine.create_checkpoint();
    assert_eq!(checkpoint.events_processed, 0);
    assert_eq!(checkpoint.output_events_emitted, 0);

    // Restore empty checkpoint and verify engine works
    let (mut engine2, mut rx2) = create_engine();
    engine2.load(&program).expect("Failed to load program");
    engine2.restore_checkpoint(&checkpoint);

    let event = Event::new("TestEvent").with_field("value", 42);
    engine2.process(event).await.expect("Failed to process");

    let output = rx2.try_recv().expect("Should have emitted");
    assert_eq!(
        output.data.get("value"),
        Some(&varpulis_core::Value::Int(42))
    );
}

// =============================================================================
// Kill/Restart Persistence Tests
//
// These tests simulate the full persistence lifecycle:
// 1. Start engine, load VPL from file, process events from .evt file
// 2. Checkpoint to a StateStore (MemoryStore)
// 3. Drop the engine (simulating kill)
// 4. Create a new engine, load checkpoint from store
// 5. Restore state and verify continuity
// =============================================================================

/// Helper: wrap an EngineCheckpoint into a Checkpoint and persist via CheckpointManager
fn persist_engine_checkpoint(manager: &mut CheckpointManager, engine_cp: EngineCheckpoint) {
    let checkpoint = Checkpoint {
        id: 0, // manager assigns the real ID
        timestamp_ms: 0,
        events_processed: engine_cp.events_processed,
        window_states: HashMap::new(),
        pattern_states: HashMap::new(),
        metadata: HashMap::new(),
        context_states: {
            let mut m = HashMap::new();
            m.insert("default".to_string(), engine_cp);
            m
        },
    };
    manager
        .checkpoint(checkpoint)
        .expect("Failed to persist checkpoint");
}

/// Helper: recover EngineCheckpoint from store via CheckpointManager
fn recover_engine_checkpoint(manager: &CheckpointManager) -> EngineCheckpoint {
    let cp = manager
        .recover()
        .expect("Failed to load checkpoint")
        .expect("No checkpoint found in store");
    cp.context_states
        .get("default")
        .cloned()
        .expect("No 'default' context in checkpoint")
}

#[tokio::test]
async fn test_kill_restart_count_window_state_continuity() {
    // Uses: checkpoint_count_window.vpl + phase1/phase2 event files
    let program = load_vpl("checkpoint_count_window.vpl");

    let store = Arc::new(MemoryStore::new());
    let config = CheckpointConfig::default();
    let mut manager =
        CheckpointManager::new(store.clone(), config).expect("Failed to create manager");

    // --- Phase 1: Process 3 events from file, then "kill" ---
    {
        let (mut engine, mut rx) = create_engine();
        engine.load(&program).expect("Failed to load");

        let phase1_events = load_events("checkpoint_count_window_phase1.evt");
        for timed in &phase1_events {
            engine
                .process(timed.event.clone())
                .await
                .expect("Failed to process");
        }

        // Window not full yet (need 5)
        assert!(
            rx.try_recv().is_err(),
            "Window should not emit with only 3 events"
        );

        // Checkpoint and persist
        let engine_cp = engine.create_checkpoint();
        assert_eq!(engine_cp.events_processed, 3);
        persist_engine_checkpoint(&mut manager, engine_cp);

        // Engine is dropped here (simulating kill)
    }

    // --- Phase 2: Restart from checkpoint, continue processing ---
    {
        let engine_cp = recover_engine_checkpoint(&manager);
        assert_eq!(engine_cp.events_processed, 3);

        let (mut engine, mut rx) = create_engine();
        engine.load(&program).expect("Failed to load");
        engine.restore_checkpoint(&engine_cp);

        // Process 2 more events from phase2 file to fill the window (3 restored + 2 new = 5)
        let phase2_events = load_events("checkpoint_count_window_phase2.evt");
        for timed in &phase2_events {
            engine
                .process(timed.event.clone())
                .await
                .expect("Failed to process");
        }

        // Window should emit now with sum = 10+20+30+40+50 = 150
        let output = rx
            .try_recv()
            .expect("Window should have emitted after 5 total events");
        assert_eq!(
            output.data.get("n"),
            Some(&varpulis_core::Value::Int(5)),
            "Count should be 5"
        );
        assert_eq!(
            output.data.get("sum"),
            Some(&varpulis_core::Value::Float(150.0)),
            "Sum should be 10+20+30+40+50=150"
        );
    }
}

#[tokio::test]
async fn test_kill_restart_multiple_checkpoints_latest_wins() {
    // Uses: checkpoint_passthrough.vpl + checkpoint_passthrough.evt
    let program = load_vpl("checkpoint_passthrough.vpl");
    let events = load_events("checkpoint_passthrough.evt");

    let store = Arc::new(MemoryStore::new());
    let config = CheckpointConfig {
        max_checkpoints: 3,
        ..Default::default()
    };
    let mut manager =
        CheckpointManager::new(store.clone(), config).expect("Failed to create manager");

    // Checkpoint after 2 events
    {
        let (mut engine, _rx) = create_engine();
        engine.load(&program).expect("Failed to load");

        for timed in events.iter().take(2) {
            engine
                .process(timed.event.clone())
                .await
                .expect("Failed to process");
        }
        persist_engine_checkpoint(&mut manager, engine.create_checkpoint());
    }

    // Checkpoint after all 5 events (total 5 in second engine)
    {
        let (mut engine, _rx) = create_engine();
        engine.load(&program).expect("Failed to load");

        for timed in &events {
            engine
                .process(timed.event.clone())
                .await
                .expect("Failed to process");
        }
        persist_engine_checkpoint(&mut manager, engine.create_checkpoint());
    }

    // Recovery should return the latest checkpoint (5 events)
    let recovered = recover_engine_checkpoint(&manager);
    assert_eq!(
        recovered.events_processed, 5,
        "Latest checkpoint should have 5 events processed"
    );
}

#[tokio::test]
async fn test_kill_restart_session_window_state() {
    // Uses: checkpoint_session_window.vpl + phase1/phase2 event files
    let program = load_vpl("checkpoint_session_window.vpl");

    let store = Arc::new(MemoryStore::new());
    let config = CheckpointConfig::default();
    let mut manager =
        CheckpointManager::new(store.clone(), config).expect("Failed to create manager");

    let base_time = chrono::Utc::now();

    // --- Phase 1: Start a session with 3 events, then kill ---
    {
        let (mut engine, mut rx) = create_engine();
        engine.load(&program).expect("Failed to load");

        // Use phase1 timestamps but apply relative to base_time
        for i in 0..3 {
            let event = Event::new("SensorEvent")
                .with_field("value", 100)
                .with_timestamp(base_time + chrono::Duration::seconds(i));
            engine.process(event).await.expect("Failed to process");
        }

        // Session still open (no gap exceeded)
        assert!(rx.try_recv().is_err(), "Session should still be open");

        persist_engine_checkpoint(&mut manager, engine.create_checkpoint());
    }

    // --- Phase 2: Restart, close the session with a gap ---
    {
        let engine_cp = recover_engine_checkpoint(&manager);

        let (mut engine, mut rx) = create_engine();
        engine.load(&program).expect("Failed to load");
        engine.restore_checkpoint(&engine_cp);

        // Send event with 6s gap from last event (closes the session)
        let event = Event::new("SensorEvent")
            .with_field("value", 999)
            .with_timestamp(base_time + chrono::Duration::seconds(9));
        engine.process(event).await.expect("Failed to process");

        // The session with the 3 restored events should have closed
        let output = rx.try_recv().expect("Session should have closed after gap");
        assert_eq!(
            output.data.get("n"),
            Some(&varpulis_core::Value::Int(3)),
            "Session should contain the 3 events from before restart"
        );
        assert_eq!(
            output.data.get("sum"),
            Some(&varpulis_core::Value::Float(300.0)),
            "Sum should be 3 * 100 = 300"
        );
    }
}

#[tokio::test]
async fn test_kill_restart_variables_preserved() {
    // Uses: checkpoint_variables.vpl
    let program = load_vpl("checkpoint_variables.vpl");

    let store = Arc::new(MemoryStore::new());
    let config = CheckpointConfig::default();
    let mut manager =
        CheckpointManager::new(store.clone(), config).expect("Failed to create manager");

    // Phase 1: Load program with variable, checkpoint
    {
        let (mut engine, _rx) = create_engine();
        engine.load(&program).expect("Failed to load");

        // Verify variable exists
        assert!(engine.get_variable("counter").is_some());

        engine
            .process(Event::new("TestEvent").with_field("value", 1))
            .await
            .expect("Failed to process");

        persist_engine_checkpoint(&mut manager, engine.create_checkpoint());
    }

    // Phase 2: Restore and verify variable is still there
    {
        let engine_cp = recover_engine_checkpoint(&manager);

        let (mut engine, _rx) = create_engine();
        engine.load(&program).expect("Failed to load");
        engine.restore_checkpoint(&engine_cp);

        let val = engine.get_variable("counter");
        assert!(val.is_some(), "Variable 'counter' should survive restart");
        assert_eq!(
            *val.unwrap(),
            varpulis_core::Value::Int(0),
            "Variable value should be preserved"
        );
    }
}

#[tokio::test]
async fn test_kill_restart_checkpoint_pruning() {
    // Uses: checkpoint_passthrough.vpl + checkpoint_passthrough.evt
    let program = load_vpl("checkpoint_passthrough.vpl");
    let events = load_events("checkpoint_passthrough.evt");

    let store = Arc::new(MemoryStore::new());
    let config = CheckpointConfig {
        max_checkpoints: 2,
        ..Default::default()
    };
    let mut manager =
        CheckpointManager::new(store.clone(), config).expect("Failed to create manager");

    // Create 4 checkpoints with increasing event counts
    for batch in 0..4u64 {
        let (mut engine, _rx) = create_engine();
        engine.load(&program).expect("Failed to load");

        let take_count = (batch + 1).min(events.len() as u64) as usize;
        for timed in events.iter().take(take_count) {
            engine
                .process(timed.event.clone())
                .await
                .expect("Failed to process");
        }
        persist_engine_checkpoint(&mut manager, engine.create_checkpoint());
    }

    // Only 2 checkpoints should remain
    let checkpoint_ids = store.list_checkpoints().expect("Failed to list");
    assert_eq!(
        checkpoint_ids.len(),
        2,
        "Should have pruned to 2 checkpoints, got {:?}",
        checkpoint_ids
    );
}

#[tokio::test]
async fn test_kill_restart_watermark_state_preserved() {
    // Uses: watermark_basic.vpl + watermark_basic.evt
    let program = load_vpl("watermark_basic.vpl");

    let store = Arc::new(MemoryStore::new());
    let config = CheckpointConfig::default();
    let mut manager =
        CheckpointManager::new(store.clone(), config).expect("Failed to create manager");

    let base_time = chrono::Utc::now();

    // Phase 1: Process events with watermark tracking
    {
        let (mut engine, _rx) = create_engine();
        engine.load(&program).expect("Failed to load");

        for i in 0..5 {
            let event = Event::new("SensorEvent")
                .with_field("value", i)
                .with_timestamp(base_time + chrono::Duration::seconds(i));
            engine.process(event).await.expect("Failed to process");
        }

        let engine_cp = engine.create_checkpoint();
        assert!(
            engine_cp.watermark_state.is_some(),
            "Watermark state should be present in checkpoint"
        );
        persist_engine_checkpoint(&mut manager, engine_cp);
    }

    // Phase 2: Restore and verify watermark state
    {
        let engine_cp = recover_engine_checkpoint(&manager);
        assert!(
            engine_cp.watermark_state.is_some(),
            "Watermark state should survive store round-trip"
        );

        let (mut engine, _rx) = create_engine();
        engine.load(&program).expect("Failed to load");
        engine.restore_checkpoint(&engine_cp);

        // Continue processing - should work with restored watermark
        let event = Event::new("SensorEvent")
            .with_field("value", 99_i64)
            .with_timestamp(base_time + chrono::Duration::seconds(10));
        engine
            .process(event)
            .await
            .expect("Should process after watermark restore");
    }
}
