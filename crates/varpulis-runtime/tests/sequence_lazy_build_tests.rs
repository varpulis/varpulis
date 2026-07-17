//! Tests for lazy per-alias structure building in the Sequence op.
//!
//! When a Kleene/sequence match completes, `pipeline.rs` used to build, for
//! every alias, a `_events_{alias}` array (a deep clone of every captured
//! event) plus per-field aggregates — O(events) per match, O(events²) over a
//! Kleene run, which OOMs on long runs. A compile-time analysis
//! (`engine::sequence_analysis`) now records which aliases the downstream
//! pipeline actually reads via positional access / `collect(...)` / aggregate
//! calls, and the builder skips the expensive structures for the rest. It also
//! caps the materialized `_events_{alias}` array at `MAX_ARRAY_ELEMENTS`.
//!
//! Observation strategy: a stream whose only terminal op is `.to(<connector>)`
//! (with no `.emit()`) forwards the *raw* `SequenceMatch` event — internal
//! `_count_`/`_events_`/`_agg_` keys intact — to the output channel. The
//! connector is intentionally unregistered: `.to()` warns at runtime and leaves
//! the event untouched, which is exactly what we want to inspect. (`.emit(...)`
//! would rebuild a fresh event containing only the emitted fields, hiding the
//! internal keys.)
#![allow(clippy::needless_raw_string_hashes)]

use tokio::sync::mpsc;
use varpulis_core::Value;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;

/// Load `code`, feed `events` one at a time through the async engine, and
/// return everything that reached the output channel.
async fn run(code: &str, events: Vec<Event>) -> Vec<Event> {
    let program = parse(code).expect("parse");
    let (tx, mut rx) = mpsc::channel(16_384);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");
    for e in events {
        engine.process(e).await.expect("process");
    }
    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        out.push(e);
    }
    out
}

fn news(id: i64) -> Event {
    Event::new("News").with_field("id", id)
}

fn tick(price: f64) -> Event {
    Event::new("Tick").with_field("price", price)
}

fn key_names(e: &Event) -> Vec<String> {
    e.data.keys().map(|k| k.to_string()).collect()
}

/// Primary (fast): when nothing downstream indexes or collects the alias, the
/// expensive `_events_{alias}` array must be skipped, while the cheap
/// `_count_{alias}` scalar is still built.
///
/// Fail-before: with the build ungated (always runs), `_events_tick` is present
/// and the `is_none()` assertion fails. Pass-after: it is absent.
#[tokio::test]
async fn lazy_skips_events_array_when_unreferenced() {
    let code = r#"
        stream S = News as news -> all Tick as tick .to(NullSink)
    "#;
    let out = run(code, vec![news(1), tick(100.0), tick(101.0), tick(102.0)]).await;
    assert!(
        !out.is_empty(),
        "expected raw SequenceMatch events on the output channel"
    );
    for e in &out {
        assert!(
            e.get("_count_tick").is_some(),
            "cheap _count_tick must always be built; keys={:?}",
            key_names(e)
        );
        assert!(
            e.get("_events_tick").is_none(),
            "expensive _events_tick must be SKIPPED when unreferenced; keys={:?}",
            key_names(e)
        );
    }
}

/// A downstream reference to the alias array (`tick[0]` in a where-clause) must
/// still cause `_events_{alias}` to be built, with correct contents.
///
/// Fail-before: if the analysis fails to flag the reference (or the build is
/// force-skipped), `_events_tick` is absent and the `panic!` in the `match`
/// fires. Pass-after: it is present and correct.
#[tokio::test]
async fn referenced_alias_still_builds_events_array() {
    let code = r#"
        stream S = News as news -> all Tick as tick
            .where(tick[0].price >= 0.0)
            .to(NullSink)
    "#;
    let out = run(code, vec![news(1), tick(100.0), tick(101.0), tick(102.0)]).await;
    assert!(!out.is_empty(), "expected SequenceMatch events");

    // The last incremental match carries all three ticks.
    let best = out
        .iter()
        .max_by_key(|e| e.get("_count_tick").and_then(Value::as_int).unwrap_or(0))
        .expect("some output");

    let arr = match best.get("_events_tick") {
        Some(Value::Array(a)) => a,
        other => panic!("_events_tick must be present as an Array when referenced, got {other:?}"),
    };
    assert_eq!(arr.len(), 3, "array should hold all 3 captured ticks");
    match &arr[0] {
        Value::Map(m) => assert_eq!(
            m.get("price"),
            Some(&Value::Float(100.0)),
            "first captured tick price"
        ),
        other => panic!("expected Map element, got {other:?}"),
    }
    assert_eq!(best.get("_count_tick"), Some(&Value::Int(3)));
}

// NOTE on the array cap: the `_events_{alias}` array is truncated at
// `MAX_ARRAY_ELEMENTS` (10_000). That cap cannot be exercised end-to-end
// through VPL, because SASE bounds every Kleene closure at `MAX_KLEENE_EVENTS`
// (20) to keep ZDD enumeration from blowing up — so no alias ever groups more
// than ~20 events here. The truncation is a defensive backstop and is unit
// tested directly against the extracted `build_events_array` helper in
// `engine::pipeline::tests` (`build_events_array_truncates_at_cap`).
