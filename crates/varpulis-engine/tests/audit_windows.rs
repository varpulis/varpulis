//! The windows as the reference describes them, through the engine as it
//! ships, and their state through a snapshot.
//!
//! These pin the semantics a host gets. They must hold whatever features the
//! runtime was built with: a workspace test run builds it with the columnar
//! `arrow` feature, which fused tumbling windows into bins aligned on the
//! epoch instead of on the window's first event, and kept their state out of
//! the snapshot.

mod common;

use common::{at, end, feed, only};
use varpulis_engine::Program;

const READINGS: &str = r"
event R:
    k: str
    v: int
";

fn reading(time: &str, k: &str, v: i64) -> String {
    at(time, &format!(r#""k": "{k}", "v": {v}"#))
}

/// Snapshot `program` and restore it into a fresh program of the same source.
fn reborn(program: &Program, source: &str) -> Program {
    let bytes = program.snapshot().unwrap();
    let mut fresh = Program::compile(source).unwrap();
    fresh.restore(&bytes).unwrap();
    fresh
}

const TUMBLING: &str = r"
stream S = R
    .window(1m)
    .aggregate(n: count(), s: sum(v))
    .emit(n: n, s: s)
";

#[test]
fn a_tumbling_window_opens_with_its_first_event_and_closes_on_the_clock() {
    let source = format!("{READINGS}{TUMBLING}");
    let mut p = Program::compile(&source).unwrap();
    assert!(feed(&mut p, "R", &reading("10:00:50", "a", 1)).is_empty());
    // 10:01:10 is inside [10:00:50, 10:01:50): same window.
    assert!(feed(&mut p, "R", &reading("10:01:10", "a", 2)).is_empty());
    // 10:02:10 is past the window's end: it closes, and opens the next.
    let out = only(feed(&mut p, "R", &reading("10:02:10", "a", 3)));
    assert_eq!(out["n"], 2);
    assert_eq!(out["s"], 3.0);
    assert_eq!(out["timestamp"], "2026-10-14T10:01:10Z");
    let out = only(end(&mut p));
    assert_eq!(out["n"], 1);
    assert_eq!(out["s"], 3.0);
}

#[test]
fn a_tumbling_window_survives_a_snapshot_mid_window() {
    let source = format!("{READINGS}{TUMBLING}");
    let mut p = Program::compile(&source).unwrap();
    assert!(feed(&mut p, "R", &reading("10:00:50", "a", 1)).is_empty());
    let mut p = reborn(&p, &source);
    assert!(feed(&mut p, "R", &reading("10:01:10", "a", 2)).is_empty());
    let out = only(feed(&mut p, "R", &reading("10:02:10", "a", 3)));
    assert_eq!(
        out["n"], 2,
        "the event fed before the snapshot is in the window"
    );
    assert_eq!(out["s"], 3.0);
}

#[test]
fn a_partitioned_tumbling_window_survives_a_snapshot_mid_window() {
    let source = format!(
        r"{READINGS}
stream S = R
    .partition_by(k)
    .window(1m)
    .aggregate(k: last(k), n: count(), s: sum(v))
    .emit(k: k, n: n, s: s)
"
    );
    let mut p = Program::compile(&source).unwrap();
    assert!(feed(&mut p, "R", &reading("10:00:00", "a", 1)).is_empty());
    assert!(feed(&mut p, "R", &reading("10:00:10", "b", 10)).is_empty());
    let mut p = reborn(&p, &source);
    assert!(feed(&mut p, "R", &reading("10:00:20", "a", 2)).is_empty());
    // 10:01:30 passes both windows' ends: both close, b with its one event.
    let mut out = feed(&mut p, "R", &reading("10:01:30", "c", 100));
    out.sort_by_key(|e| e["k"].as_str().unwrap().to_string());
    assert_eq!(out.len(), 2, "{out:#?}");
    assert_eq!(
        (
            out[0]["k"].as_str(),
            out[0]["n"].as_i64(),
            out[0]["s"].as_f64()
        ),
        (Some("a"), Some(2), Some(3.0))
    );
    assert_eq!(
        (
            out[1]["k"].as_str(),
            out[1]["n"].as_i64(),
            out[1]["s"].as_f64()
        ),
        (Some("b"), Some(1), Some(10.0))
    );
}

#[test]
fn a_count_window_survives_a_snapshot() {
    let source = format!(
        r"{READINGS}
stream S = R
    .partition_by(k)
    .window(3)
    .aggregate(k: last(k), s: sum(v))
    .emit(k: k, s: s)
"
    );
    let mut p = Program::compile(&source).unwrap();
    assert!(feed(&mut p, "R", &reading("10:00:00", "a", 1)).is_empty());
    assert!(feed(&mut p, "R", &reading("10:00:01", "a", 2)).is_empty());
    let mut p = reborn(&p, &source);
    let out = only(feed(&mut p, "R", &reading("10:00:02", "a", 3)));
    assert_eq!(out["s"], 6.0);
}

#[test]
fn a_sliding_window_survives_a_snapshot() {
    let source = format!(
        r"{READINGS}
stream S = R
    .window(10s, sliding: 5s)
    .aggregate(n: count(), s: sum(v))
    .emit(n: n, s: s)
"
    );
    let mut p = Program::compile(&source).unwrap();
    let out = only(feed(&mut p, "R", &reading("10:00:00", "a", 1)));
    assert_eq!(out["n"], 1, "the first event emits the first slide");
    assert!(feed(&mut p, "R", &reading("10:00:03", "a", 2)).is_empty());
    let mut p = reborn(&p, &source);
    let out = only(feed(&mut p, "R", &reading("10:00:06", "a", 3)));
    assert_eq!(out["n"], 3);
    assert_eq!(out["s"], 6.0);
}

#[test]
fn a_session_window_survives_a_snapshot() {
    let source = format!(
        r"{READINGS}
stream S = R
    .window(session: 1m)
    .aggregate(n: count(), s: sum(v))
    .emit(n: n, s: s)
"
    );
    let mut p = Program::compile(&source).unwrap();
    assert!(feed(&mut p, "R", &reading("10:00:00", "a", 1)).is_empty());
    assert!(feed(&mut p, "R", &reading("10:00:30", "a", 2)).is_empty());
    let mut p = reborn(&p, &source);
    // More than a minute after the last event: the session closes on the
    // clock, and the new event opens the next one.
    let out = only(feed(&mut p, "R", &reading("10:03:00", "a", 3)));
    assert_eq!(out["n"], 2);
    assert_eq!(out["s"], 3.0);
}
