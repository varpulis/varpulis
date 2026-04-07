//! Regression tests for output channel backpressure correctness.
//!
//! Background: prior to the fix in this PR, `Engine::send_output_shared` and
//! `Engine::send_output` used `try_send` and silently dropped events when the
//! output channel was full, only emitting a `warn!` log. This meant that
//! anyone running `varpulis simulate ... | jq` (or any other slow consumer)
//! would silently lose data — the engine's internal `output_events_emitted`
//! counter would still increment, but the receiver would never see those
//! events.
//!
//! The fix replaces `try_send` with a retry loop using `try_send + yield_now`
//! so that backpressure is applied cooperatively and events are NEVER dropped.
//! These tests pin that behaviour.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use varpulis_core::Event;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::SharedEvent;

/// Reproduces the original bug: a 1k-buffer channel with a slow receiver
/// previously dropped ~10k of 100k events. With the fix, all 100k are received.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_receiver_does_not_lose_events() {
    let (tx, mut rx) = mpsc::channel::<SharedEvent>(1_000); // small buffer
    let engine = Engine::new_shared(tx);

    // Spawn a deliberately slow consumer that yields a lot
    let receiver = tokio::spawn(async move {
        let mut count = 0u64;
        while let Some(_evt) = rx.recv().await {
            count += 1;
            if count % 1_000 == 0 {
                // Slow drain — emulate a downstream JSON serializer + stdout pipe
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        count
    });

    // Push events through the engine using a VPL program that just emits.
    let vpl = r#"
        event Tick:
            n: int

        stream Out = Tick
            .emit(n: n)
    "#;
    let mut engine = engine;
    let program = parse(vpl).expect("parse");
    engine.load(&program).expect("load");

    let total = 100_000u64;
    let events: Vec<Event> = (0..total)
        .map(|i| {
            let mut e = Event::new("Tick");
            e.data
                .insert(Arc::from("n"), varpulis_core::Value::Int(i as i64));
            e
        })
        .collect();

    // Drive in batches so the worker doesn't hold a long sync borrow
    for chunk in events.chunks(1_000) {
        engine
            .process_batch_sync(chunk.to_vec())
            .expect("process_batch_sync");
    }

    // Drop the engine's sender (close the channel) so the receiver loop exits
    drop(engine);

    let received = tokio::time::timeout(Duration::from_secs(30), receiver)
        .await
        .expect("receiver timed out")
        .expect("receiver task panicked");

    assert_eq!(
        received,
        total,
        "expected exactly {total} events, got {received} — output channel dropped {} events",
        total - received
    );
}
