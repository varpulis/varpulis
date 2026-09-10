//! A join must never be silently discarded by the dispatch-path choice.
//!
//! `Engine::process_stream_sync` does not implement join sources. The CLI chose
//! between the synchronous and asynchronous paths with `has_sink_operations()`,
//! which asks whether the program has `.to()` or `.enrich()` — a different
//! question. A join is not a sink, so a program whose only join had no sink
//! took the sync path and every match was thrown away, with a successful exit
//! code and an empty result.
//!
//! That is the default path for `simulate`, `detect`, `analyze` and `repl`, and
//! the only path in `varpulis-engine-wasm`, so the shipped join example emitted
//! nothing when run with the command printed in its own header.

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

/// Fail-before: a sink-less join reported `requires_async_dispatch == false`
/// via `has_sink_operations`, so the CLI ran it on the path that discards it.
#[tokio::test]
async fn a_sinkless_join_requires_the_async_path() {
    let (engine, _rx) = load(JOIN_PROGRAM);

    assert!(
        !engine.has_sink_operations(),
        "the program has no .to()/.enrich() — this is what misled the selector"
    );
    assert!(
        engine.has_join_sources(),
        "the program does have a join source"
    );
    assert!(
        engine.requires_async_dispatch(),
        "a join must force the async path even with no sink"
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

/// Anything genuinely confined to the sync path — the WASM build — must get a
/// loud error rather than a successful empty result.
///
/// Fail-before: this returned `Ok` with an empty result.
#[test]
fn the_sync_path_refuses_a_join_instead_of_dropping_it() {
    let (mut engine, _rx) = load(JOIN_PROGRAM);

    let err = engine
        .process_batch_sync(vec![trade("AAPL", 100.0), quote("AAPL", 99.5)])
        .expect_err("the sync path must refuse a join, not silently drop it");

    let msg = err.to_string();
    assert!(
        msg.contains("join") && msg.contains("synchronous"),
        "the error must say what is unsupported and why; got: {msg}"
    );
}
