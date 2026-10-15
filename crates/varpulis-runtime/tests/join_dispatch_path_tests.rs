//! A join must never be silently discarded by the dispatch-path choice.
//!
//! `Engine::process_stream_sync` once did not implement join sources, and the
//! CLI chose between the synchronous and asynchronous paths with
//! `has_sink_operations()`, which asks whether the program has `.to()` or
//! `.enrich()` — a different question. A join is not a sink, so a program
//! whose only join had no sink took the sync path and every match was thrown
//! away, with a successful exit code and an empty result.
//!
//! Both paths run a join now: the synchronous one, which `simulate`, a Vejas
//! detect unit and `varpulis-engine-wasm` take, correlates through the same
//! buffer as the asynchronous one. A sink-less join needs no asynchronous
//! runtime, and the selector says so.

use tokio::sync::mpsc;
use varpulis_core::{Event, Value};
use varpulis_runtime::engine::Engine;

const JOIN_PROGRAM: &str = r"event Trade:
    sym: str
    px: float

event Quote:
    sym: str
    bid: float

stream Matched = join(Trade, Quote)
    .on(Trade.sym == Quote.sym)
    .window(5s)
    .select(sym: Trade.sym, px: Trade.px, bid: Quote.bid)

stream Alerts = Matched
    .emit(sym: sym, px: px)
";

fn load(src: &str) -> (Engine, mpsc::Receiver<Event>) {
    let program = varpulis_parser::parse(src).expect("program must parse");
    let (tx, rx) = mpsc::channel::<Event>(100);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("program must load");
    (engine, rx)
}

fn trade(sym: &str, px: f64) -> Event {
    let mut e = Event::new("Trade");
    e.data.insert("sym".into(), Value::Str(sym.into()));
    e.data.insert("px".into(), Value::Float(px));
    e
}

fn quote(sym: &str, bid: f64) -> Event {
    let mut e = Event::new("Quote");
    e.data.insert("sym".into(), Value::Str(sym.into()));
    e.data.insert("bid".into(), Value::Float(bid));
    e
}

/// A sink-less join takes the synchronous path, which runs it (see
/// `the_sync_path_runs_the_join`); only a sink or a lookup needs the
/// asynchronous one.
#[tokio::test]
async fn a_sinkless_join_takes_the_sync_path() {
    let (engine, _rx) = load(JOIN_PROGRAM);

    assert!(
        !engine.has_sink_operations(),
        "the program has no .to()/.enrich()"
    );
    assert!(
        engine.has_join_sources(),
        "the program does have a join source"
    );
    assert!(
        !engine.requires_async_dispatch(),
        "a join alone needs no asynchronous runtime"
    );
}

/// The async path must produce the matches, so the fix is observable end to end.
#[tokio::test]
async fn the_async_path_produces_join_matches() {
    let (mut engine, mut rx) = load(JOIN_PROGRAM);

    let batch = vec![
        trade("AAPL", 100.0),
        quote("AAPL", 99.5),
        trade("MSFT", 200.0),
        quote("MSFT", 199.0),
    ];
    engine
        .process_batch(batch)
        .await
        .expect("processing must succeed");
    drop(engine); // close the channel so the drain terminates

    let mut emitted = Vec::new();
    while let Some(e) = rx.recv().await {
        emitted.push(e);
    }

    assert_eq!(emitted.len(), 2, "both symbols must join; got {emitted:?}");
}

/// The synchronous path — `varpulis simulate`, a Vejas detect unit, the WASM
/// build — runs the join too: it used to refuse it (and before that, to
/// return `Ok` with an empty result).
#[test]
fn the_sync_path_runs_the_join() {
    let (mut engine, mut rx) = load(JOIN_PROGRAM);

    engine
        .process_batch_sync(vec![
            trade("AAPL", 100.0),
            quote("AAPL", 99.5),
            trade("MSFT", 200.0),
        ])
        .expect("processing must succeed");

    let mut emitted = Vec::new();
    while let Ok(e) = rx.try_recv() {
        emitted.push(e);
    }
    assert_eq!(emitted.len(), 1, "AAPL joins, MSFT waits; got {emitted:?}");
    assert_eq!(&*emitted[0].event_type, "Alerts");
    assert_eq!(emitted[0].data.get("sym"), Some(&Value::str("AAPL")));
    assert_eq!(emitted[0].data.get("px"), Some(&Value::Float(100.0)));
}
