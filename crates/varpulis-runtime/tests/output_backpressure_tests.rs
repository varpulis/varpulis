//! Output backpressure must yield to the tokio scheduler, not the OS thread.
//!
//! `Engine::send_output*` spins on `try_send` when the output channel is full.
//! The sync version parks with `std::thread::yield_now()`, which does not let
//! any other task on the runtime be polled — so on a current-thread runtime the
//! task that drains the channel can never run and the engine spins forever.
//! The engine's own doc comment said as much; the async variant that fixes it
//! shipped annotated "not yet wired into process_batch".
//!
//! Reachable from `POST /api/v1/pipelines/{id}/events` and from the cluster
//! worker's inject handler, both of which hold a process-wide lock while
//! calling in — so one request wedges every tenant on the host.

use std::time::Duration;

use tokio::sync::mpsc;
use varpulis_core::Event;
use varpulis_runtime::engine::Engine;

/// Emit more events from a single `process_batch` call than the output channel
/// can hold, with the only consumer running as a separate task.
///
/// Fail-before: on a current-thread runtime this never returns.
#[tokio::test]
async fn process_batch_does_not_deadlock_when_output_channel_fills() {
    // Deliberately tiny: the batch below emits far more than this.
    const CAPACITY: usize = 4;
    const EVENTS: usize = 64;

    let (tx, mut rx) = mpsc::channel::<Event>(CAPACITY);
    let mut engine = Engine::new_with_optional_output(Some(tx));

    let program = varpulis_parser::parse(
        r"event Tick:
    n: int

stream Out = Tick
    .emit(n: n)
",
    )
    .expect("program must parse");
    engine.load(&program).expect("program must load");

    // The drain task is the only consumer, and it can only make progress if
    // the engine yields to the scheduler rather than to the OS thread.
    let drain = tokio::spawn(async move {
        let mut seen = 0usize;
        while rx.recv().await.is_some() {
            seen += 1;
            if seen == EVENTS {
                break;
            }
        }
        seen
    });

    let batch: Vec<Event> = (0..EVENTS)
        .map(|i| {
            let mut e = Event::new("Tick");
            e.data
                .insert("n".into(), varpulis_core::Value::Int(i as i64));
            e
        })
        .collect();

    let processed = tokio::time::timeout(Duration::from_secs(10), engine.process_batch(batch))
        .await
        .expect("process_batch deadlocked: it never yielded to the drain task");
    processed.expect("process_batch must succeed");

    let seen = tokio::time::timeout(Duration::from_secs(10), drain)
        .await
        .expect("drain task did not finish")
        .expect("drain task panicked");

    assert_eq!(
        seen, EVENTS,
        "every emitted event must reach the consumer; backpressure must never drop"
    );
}
