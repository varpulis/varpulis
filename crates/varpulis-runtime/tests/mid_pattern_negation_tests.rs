//! "A, then C, with no B in between": `A -> NOT B -> C` through the async
//! engine.
//!
//! A negated step with a step after it was compiled as a trailing absence:
//! the run reached C only once the pattern's deadline had confirmed the
//! absence, and that deadline also ended the run. The pattern never matched.

use tokio::sync::mpsc;
use varpulis_core::Value;
use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;

const EVENTS: &str = "
event A:
    id: str
event B:
    id: str
event C:
    id: str
";

fn source(pattern: &str) -> String {
    format!("{EVENTS}\npattern P =\n{pattern}\n\nstream Out = P\n    .emit(a: a.id)\n")
}

fn at(kind: &str, id: &str, ms: i64) -> Event {
    use chrono::{TimeZone, Utc};
    let mut e = Event::new(kind);
    e.data.insert("id".into(), Value::Str(id.into()));
    e.timestamp = Utc.timestamp_millis_opt(1_700_000_000_000 + ms).unwrap();
    e
}

fn engine(pattern: &str) -> (Engine, mpsc::Receiver<Event>) {
    let program = parse(&source(pattern)).expect("parse");
    let (tx, rx) = mpsc::channel(256);
    let mut engine = Engine::new(tx);
    engine.load(&program).expect("load");
    (engine, rx)
}

fn drain(rx: &mut mpsc::Receiver<Event>) -> Vec<String> {
    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        if let Some(Value::Str(s)) = e.data.get("a") {
            out.push(s.to_string());
        }
    }
    out
}

async fn run(pattern: &str, events: Vec<Event>) -> Vec<String> {
    let (mut engine, mut rx) = engine(pattern);
    for e in events {
        engine.process(e).await.expect("process");
    }
    drain(&mut rx)
}

const KEYED: &str = "    A as a
    -> NOT B where id == a.id
    -> C as c
    within 1m";

#[tokio::test]
async fn the_step_after_the_absence_completes_the_run() {
    let out = run(
        KEYED,
        vec![
            at("A", "o1", 0),
            at("A", "o2", 100),
            at("B", "o2", 200),
            at("B", "other", 300),
            at("C", "c", 1_000),
        ],
    )
    .await;
    assert_eq!(out, ["o1"]);
}

#[tokio::test]
async fn partitioned_the_absence_is_judged_per_key() {
    let out = run(
        "    A as a
    -> NOT B
    -> C where id == a.id as c
    within 1m
    partition by id",
        vec![
            at("A", "o1", 0),
            at("A", "o2", 100),
            at("B", "o2", 200),
            at("C", "o2", 300),
            at("C", "o1", 400),
        ],
    )
    .await;
    assert_eq!(out, ["o1"]);
}

#[tokio::test]
async fn a_checkpoint_taken_mid_pattern_keeps_the_absence() {
    let (mut before, _rx) = engine(KEYED);
    before.process(at("A", "o1", 0)).await.unwrap();
    before.process(at("A", "o2", 100)).await.unwrap();
    let checkpoint = before.create_checkpoint();

    let (mut after, mut rx) = engine(KEYED);
    after.restore_checkpoint(&checkpoint).unwrap();
    after.process(at("B", "o1", 200)).await.unwrap();
    after.process(at("C", "c", 300)).await.unwrap();
    assert_eq!(drain(&mut rx), ["o2"]);
}
