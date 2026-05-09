//! Tests for the engine source pause/resume flag (Task 2.2).
//!
//! Covers the `AtomicBool` pause flag exposed via [`Engine::pause_sources`] /
//! [`Engine::resume_sources`] / [`Engine::source_pause_handle`], its
//! observation by the source-ingestion hot path, and Arc-handle propagation.

use std::sync::atomic::Ordering;

use varpulis_core::Value;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;

const STREAM_VPL: &str = r"
    stream S = Tick
        .where(x > 0)
        .emit(val: x)
";

fn make_engine() -> Engine {
    let program = parse(STREAM_VPL).expect("parse");
    let mut engine = Engine::new_benchmark();
    engine.load(&program).expect("load");
    engine
}

fn tick(i: i64) -> Event {
    Event::new("Tick").with_field("x", Value::Int(i))
}

#[test]
fn default_state_is_not_paused() {
    let engine = make_engine();
    assert!(!engine.is_sources_paused());
    assert_eq!(engine.events_ingested_while_paused(), 0);
}

#[test]
fn pause_then_resume_flips_flag() {
    let engine = make_engine();

    engine.pause_sources();
    assert!(engine.is_sources_paused());

    engine.resume_sources();
    assert!(!engine.is_sources_paused());
}

#[test]
fn handle_is_shared_with_engine() {
    let engine = make_engine();
    let handle = engine.source_pause_handle();
    assert!(!handle.load(Ordering::Acquire));

    engine.pause_sources();
    assert!(handle.load(Ordering::Acquire));

    engine.resume_sources();
    assert!(!handle.load(Ordering::Acquire));
}

#[test]
fn handle_can_be_cloned_and_observed_after_engine_drops() {
    let handle = {
        let engine = make_engine();
        engine.pause_sources();
        engine.source_pause_handle()
    };
    // Engine dropped; cloned handle still observes the last published state.
    assert!(handle.load(Ordering::Acquire));
}

#[test]
fn sync_batch_drains_while_paused_and_increments_observed_counter() {
    let mut engine = make_engine();
    engine.pause_sources();

    let events: Vec<Event> = (1..=5).map(tick).collect();
    engine
        .process_batch_sync(events)
        .expect("process_batch_sync");

    // Pause is cooperative — in-flight events drain so the checkpoint is
    // coherent. The counter records that the pause flag was set on entry.
    assert_eq!(engine.metrics().events_processed, 5);
    assert_eq!(engine.events_ingested_while_paused(), 5);
}

#[test]
fn sync_batch_does_not_increment_counter_when_unpaused() {
    let mut engine = make_engine();
    let events: Vec<Event> = (1..=5).map(tick).collect();
    engine
        .process_batch_sync(events)
        .expect("process_batch_sync");

    assert_eq!(engine.metrics().events_processed, 5);
    assert_eq!(engine.events_ingested_while_paused(), 0);
}

#[test]
fn counter_accumulates_across_paused_batches() {
    let mut engine = make_engine();
    engine.pause_sources();

    engine
        .process_batch_sync((1..=3).map(tick).collect())
        .unwrap();
    engine
        .process_batch_sync((4..=10).map(tick).collect())
        .unwrap();

    assert_eq!(engine.events_ingested_while_paused(), 10);
}

#[test]
fn counter_freezes_after_resume() {
    let mut engine = make_engine();
    engine.pause_sources();
    engine
        .process_batch_sync((1..=4).map(tick).collect())
        .unwrap();

    engine.resume_sources();
    engine
        .process_batch_sync((5..=10).map(tick).collect())
        .unwrap();

    // Only the first 4 events arrived while the flag was set.
    assert_eq!(engine.events_ingested_while_paused(), 4);
    assert_eq!(engine.metrics().events_processed, 10);
}

#[tokio::test]
async fn async_batch_observes_pause() {
    let mut engine = make_engine();
    engine.pause_sources();

    let events: Vec<Event> = (1..=8).map(tick).collect();
    engine.process_batch(events).await.expect("process_batch");

    assert_eq!(engine.events_ingested_while_paused(), 8);
}

#[tokio::test]
async fn async_single_event_observes_pause() {
    let mut engine = make_engine();
    engine.pause_sources();

    for i in 1..=3 {
        engine.process(tick(i)).await.expect("process");
    }

    assert_eq!(engine.events_ingested_while_paused(), 3);
}

#[test]
fn empty_batch_does_not_increment_counter_when_paused() {
    let mut engine = make_engine();
    engine.pause_sources();

    engine.process_batch_sync(Vec::new()).unwrap();

    assert_eq!(engine.events_ingested_while_paused(), 0);
}
