//! A fenced engine must stop acting on its partition, not just stop writing to
//! the control plane.
//!
//! The control plane's CAS revision refuses a returning zombie's *control-plane
//! write*. On its own that closes nothing a user can see: the zombie is still
//! consuming its sources, still mutating window and pattern state, still
//! advancing offsets, and still emitting to its sinks. For a detection engine
//! that is duplicate alerts for as long as the process lives.
//!
//! The refusal therefore belongs at the engine's ingestion entry points. A
//! fenced worker has lost ownership of the partition, so there is nothing it
//! may correctly do with an event — emitting, aggregating and acknowledging are
//! all wrong.

use tokio::sync::mpsc;
use varpulis_core::{Event, Value};
use varpulis_runtime::engine::Engine;

const PROGRAM: &str = r"event Tick:
    n: int

stream Alerts = Tick
    .where(n > 0)
    .emit(n: n)
";

fn load() -> (Engine, mpsc::Receiver<Event>) {
    let program = varpulis_parser::parse(PROGRAM).expect("program must parse");
    let (tx, rx) = mpsc::channel::<Event>(64);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("program must load");
    (engine, rx)
}

fn tick(n: i64) -> Event {
    let mut e = Event::new("Tick");
    e.data.insert("n".into(), Value::Int(n));
    e
}

/// Fail-before: a fenced engine happily processes and emits.
#[tokio::test]
async fn a_fenced_engine_refuses_to_process() {
    let (mut engine, mut rx) = load();

    // Healthy: the engine works, so the test cannot pass by being broken.
    engine
        .process_batch(vec![tick(1)])
        .await
        .expect("an unfenced engine must process");
    assert!(!engine.is_fenced());

    engine.fence();
    assert!(engine.is_fenced(), "fence() must be observable");

    let err = engine
        .process_batch(vec![tick(2)])
        .await
        .expect_err("a fenced engine must refuse to process");
    let msg = err.to_string();
    assert!(
        msg.contains("fenced"),
        "the refusal must name the reason; got: {msg}"
    );

    drop(engine);
    let mut seen = Vec::new();
    while let Some(e) = rx.recv().await {
        seen.push(e);
    }
    assert_eq!(
        seen.len(),
        1,
        "only the pre-fence event may reach the sink; got {seen:?}"
    );
}

/// Every ingestion entry point must refuse, not just the one the CLI happens to
/// use — `simulate` and the browser build take the sync path, `run` and the
/// cluster worker take the async ones.
#[tokio::test]
async fn every_ingestion_entry_point_refuses_once_fenced() {
    let (mut engine, _rx) = load();
    engine.fence();

    assert!(
        engine.process(tick(1)).await.is_err(),
        "process() must refuse"
    );
    assert!(
        engine.process_batch(vec![tick(2)]).await.is_err(),
        "process_batch() must refuse"
    );
    assert!(
        engine.process_batch_sync(vec![tick(3)]).is_err(),
        "process_batch_sync() must refuse — this is the default CLI path"
    );
}

/// The cluster layer sets the flag through a handle, because the dependency
/// runs cluster -> runtime and the runtime cannot name the cluster's guard.
#[tokio::test]
async fn the_cluster_can_fence_through_the_handle() {
    let (mut engine, _rx) = load();
    let handle = engine.fence_handle();

    engine
        .process_batch(vec![tick(1)])
        .await
        .expect("healthy before fencing");

    // What the control plane's failed lease renewal does.
    handle.store(true, std::sync::atomic::Ordering::Release);

    assert!(engine.is_fenced(), "the handle must fence the engine");
    assert!(
        engine.process_batch(vec![tick(2)]).await.is_err(),
        "a fence set through the handle must stop ingestion"
    );
}
