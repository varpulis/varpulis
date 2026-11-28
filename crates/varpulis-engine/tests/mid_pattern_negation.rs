//! A negated step in the middle of a pattern: `A -> NOT B -> C`.
//!
//! "A, then C, with no B in between." A run that has taken A is invalidated
//! by a B that matches the negated step's `where`, from A until the run takes
//! C; C then completes it at once, without waiting for the `within`.
//!
//! The engine used to treat every negated step as trailing: a run reached the
//! step after it only when the pattern's deadline confirmed the absence, and
//! that same deadline ended the run. A pattern with a step after its `NOT`
//! compiled, loaded, and never matched.

use chrono::{DateTime, Duration, TimeZone, Utc};
use varpulis_engine::{CompileOptions, Emit, Program, TimerClock};

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 28, 10, 0, 0).unwrap()
}

fn at_ms(ms: i64) -> DateTime<Utc> {
    t0() + Duration::milliseconds(ms)
}

fn compiled(vpl: &str) -> Program {
    Program::compile(vpl).unwrap_or_else(|e| panic!("{vpl}\n{e}"))
}

fn host(vpl: &str) -> Program {
    Program::compile_with(vpl, &CompileOptions::new().timer_clock(TimerClock::Host))
        .unwrap_or_else(|e| panic!("{vpl}\n{e}"))
}

/// Feed `event_type` stamped `ms` after `t0`, with the given fields.
fn feed(program: &mut Program, event_type: &str, ms: i64, fields: &str) -> Vec<Emit> {
    let sep = if fields.is_empty() { "" } else { ", " };
    let json = format!(
        r#"{{"event_type": "{event_type}", "@timestamp": "{}"{sep}{fields}}}"#,
        at_ms(ms).to_rfc3339()
    );
    program
        .feed_json(event_type, json.as_bytes())
        .unwrap_or_else(|e| panic!("{json}: {e}"))
}

fn id(program: &mut Program, event_type: &str, ms: i64, id: &str) -> Vec<Emit> {
    feed(program, event_type, ms, &format!(r#""id": "{id}""#))
}

/// The emits of `stream`, as the value of `field`.
fn of(emits: &[Emit], stream: &str, field: &str) -> Vec<String> {
    emits
        .iter()
        .filter(|e| e.stream == stream)
        .map(|e| match &e.to_json()[field] {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        })
        .collect()
}

const EVENTS: &str = "
event A:
    id: str
event B:
    id: str
event C:
    id: str
event D:
    id: str
event E:
    id: str
event X:
    id: str
";

fn program(pattern: &str, emit: &str) -> Program {
    compiled(&format!(
        "{EVENTS}\npattern P =\n{pattern}\n\nstream Out = P\n    .emit({emit})\n"
    ))
}

// ---------------------------------------------------------------------------
// The report: a timer after the absence
// ---------------------------------------------------------------------------

const STALE: &str = r#"
event Order:
    id: str
event Shipment:
    id: str

stream Beat = timer(1s)
    .emit(kind: "beat", at: timestamp)

pattern U =
    Order as o
    -> NOT Shipment where id == o.id
    -> Beat as b1
    within 1m

stream Stale = U
    .emit(order: o.id)
"#;

#[test]
fn an_order_with_no_shipment_before_the_next_beat_is_stale() {
    let mut p = host(STALE);
    assert!(p.fire_due(t0()).unwrap().is_empty());
    assert!(id(&mut p, "Order", 100, "o1").is_empty());
    assert!(id(&mut p, "Order", 200, "o2").is_empty());
    assert!(id(&mut p, "Shipment", 300, "o2").is_empty());

    let first = p.fire_due(at_ms(1_000)).unwrap();
    assert_eq!(of(&first, "Beat", "kind"), ["beat"]);
    assert_eq!(of(&first, "Stale", "order"), ["o1"], "{first:?}");

    // The run completed: later beats raise nothing more.
    for s in [2_000, 3_000] {
        let later = p.fire_due(at_ms(s)).unwrap();
        assert_eq!(of(&later, "Beat", "kind"), ["beat"]);
        assert!(of(&later, "Stale", "order").is_empty(), "{later:?}");
    }
}

#[test]
fn a_beat_then_an_absence_then_a_beat() {
    let mut p = host(
        r#"
event Order:
    id: str
event Shipment:
    id: str

stream Beat = timer(1s)
    .emit(kind: "beat")

pattern U =
    Order as o
    -> Beat as b1
    -> NOT Shipment where id == o.id
    -> Beat as b2
    within 1m

stream Stale = U
    .emit(order: o.id)
"#,
    );
    p.fire_due(t0()).unwrap();
    id(&mut p, "Order", 100, "o1");
    id(&mut p, "Order", 200, "o2");
    // Before the first beat: not between b1 and b2, cancels nothing.
    id(&mut p, "Shipment", 300, "o1");
    assert!(of(&p.fire_due(at_ms(1_000)).unwrap(), "Stale", "order").is_empty());
    // Between the two beats: cancels o2.
    id(&mut p, "Shipment", 1_500, "o2");
    let second = p.fire_due(at_ms(2_000)).unwrap();
    assert_eq!(of(&second, "Stale", "order"), ["o1"], "{second:?}");
}

#[test]
fn beat_no_ping_beat_matches_when_no_ping_comes() {
    let mut p = host(
        r"
event Ping:
    id: str

stream Beat = timer(1s)
    .emit(n: 1)

pattern Quiet =
    Beat as b1
    -> NOT Ping
    -> Beat as b2
    within 1m

stream Silent = Quiet
    .emit(n: 1)
",
    );
    p.fire_due(t0()).unwrap();
    assert!(of(&p.fire_due(at_ms(1_000)).unwrap(), "Silent", "n").is_empty());
    // Beat 1 -> Beat 2, nothing in between.
    assert_eq!(
        of(&p.fire_due(at_ms(2_000)).unwrap(), "Silent", "n").len(),
        1
    );
    // A ping between beats 2 and 3 cancels the run that beat 2 opened.
    id(&mut p, "Ping", 2_500, "x");
    assert!(of(&p.fire_due(at_ms(3_000)).unwrap(), "Silent", "n").is_empty());
    assert_eq!(
        of(&p.fire_due(at_ms(4_000)).unwrap(), "Silent", "n").len(),
        1
    );
}

// ---------------------------------------------------------------------------
// Plain events
// ---------------------------------------------------------------------------

const A_NOT_B_C: &str = "    A as a
    -> NOT B where id == a.id
    -> C as c
    within 1m";

#[test]
fn plain_events_match_unless_the_forbidden_event_comes_between() {
    let mut p = program(A_NOT_B_C, "a: a.id, c: c.id");
    id(&mut p, "A", 0, "o1");
    id(&mut p, "A", 100, "o2");
    // A B for another key, and the forbidden B for o2.
    id(&mut p, "B", 200, "zz");
    id(&mut p, "B", 300, "o2");
    let out = id(&mut p, "C", 1_000, "c1");
    assert_eq!(of(&out, "Out", "a"), ["o1"]);
    // Each run completes once.
    assert!(id(&mut p, "C", 2_000, "c2").is_empty());
}

#[test]
fn a_forbidden_event_after_the_next_step_changes_nothing() {
    let mut p = program(A_NOT_B_C, "a: a.id");
    id(&mut p, "A", 0, "o1");
    assert_eq!(of(&id(&mut p, "C", 100, "c"), "Out", "a"), ["o1"]);
    assert!(id(&mut p, "B", 200, "o1").is_empty());
    // A new A after the B is a new run, not cancelled by it.
    id(&mut p, "A", 300, "o1");
    assert_eq!(of(&id(&mut p, "C", 400, "c"), "Out", "a"), ["o1"]);
}

#[test]
fn the_window_still_bounds_the_run() {
    let mut p = program(A_NOT_B_C, "a: a.id");
    id(&mut p, "A", 0, "o1");
    // Past the minute: the run is gone, the C opens nothing.
    assert!(id(&mut p, "C", 61_000, "c").is_empty());
}

#[test]
fn an_unconditional_negation_cancels_on_any_event_of_its_type() {
    let mut p = program(
        "    A as a\n    -> NOT B\n    -> C as c\n    within 1m",
        "a: a.id",
    );
    id(&mut p, "A", 0, "o1");
    id(&mut p, "B", 100, "anything");
    id(&mut p, "A", 200, "o2");
    assert_eq!(of(&id(&mut p, "C", 300, "c"), "Out", "a"), ["o2"]);
}

#[test]
fn the_step_after_the_absence_keeps_its_own_where() {
    let mut p = program(
        "    A as a
    -> NOT B where id == a.id
    -> C where id == a.id as c
    within 1m",
        "a: a.id",
    );
    id(&mut p, "A", 0, "o1");
    id(&mut p, "A", 10, "o2");
    assert!(id(&mut p, "C", 100, "o3").is_empty());
    id(&mut p, "B", 200, "o1");
    assert!(id(&mut p, "C", 300, "o1").is_empty());
    assert_eq!(of(&id(&mut p, "C", 400, "o2"), "Out", "a"), ["o2"]);
}

#[test]
fn partitioned_runs_are_cancelled_by_their_own_key_only() {
    let mut p = program(
        "    A as a
    -> NOT B
    -> C as c
    within 1m
    partition by id",
        "a: a.id",
    );
    id(&mut p, "A", 0, "o1");
    id(&mut p, "A", 10, "o2");
    // An unconditional B, but in o2's partition only.
    id(&mut p, "B", 100, "o2");
    assert!(id(&mut p, "C", 200, "o2").is_empty());
    assert_eq!(of(&id(&mut p, "C", 300, "o1"), "Out", "a"), ["o1"]);
}

#[test]
fn two_absences_in_a_row_forbid_both_until_the_next_step() {
    let pattern = "    A as a
    -> NOT B where id == a.id
    -> NOT D where id == a.id
    -> C as c
    within 1m";
    let mut p = program(pattern, "a: a.id");
    id(&mut p, "A", 0, "o1");
    id(&mut p, "A", 10, "o2");
    id(&mut p, "A", 20, "o3");
    id(&mut p, "B", 100, "o1");
    id(&mut p, "D", 200, "o2");
    assert_eq!(of(&id(&mut p, "C", 300, "c"), "Out", "a"), ["o3"]);
}

#[test]
fn an_absence_in_the_middle_and_one_at_the_end() {
    // `A -> NOT B -> C -> NOT D`: no B between A and C, then no D after C
    // until the deadline, which raises the match.
    let pattern = "    A as a
    -> NOT B where id == a.id
    -> C where id == a.id as c
    -> NOT D where id == a.id
    within 10s";
    let mut p = program(pattern, "a: a.id");
    id(&mut p, "A", 0, "o1");
    id(&mut p, "A", 10, "o2");
    id(&mut p, "A", 20, "o3");
    id(&mut p, "B", 100, "o1");
    assert!(id(&mut p, "C", 200, "o1").is_empty());
    assert!(id(&mut p, "C", 300, "o2").is_empty());
    assert!(id(&mut p, "C", 400, "o3").is_empty());
    id(&mut p, "D", 500, "o3");
    // Event time past the ten seconds, moved by an event the pattern reads:
    // o2 completes, o3 had its D.
    let out = id(&mut p, "D", 11_000, "zz");
    assert_eq!(of(&out, "Out", "a"), ["o2"], "{out:?}");
}

// ---------------------------------------------------------------------------
// Neighbours: a closure, `or`, `and`
// ---------------------------------------------------------------------------

#[test]
fn a_closure_before_the_absence() {
    let pattern = "    A as a
    -> all B where id == a.id as bs
    -> NOT X where id == a.id
    -> C where id == a.id as c
    within 1m";
    let mut p = program(pattern, "a: a.id, n: count(bs)");

    id(&mut p, "A", 0, "o1");
    id(&mut p, "B", 100, "o1");
    id(&mut p, "B", 200, "o1");
    let out = id(&mut p, "C", 300, "o1");
    assert!(!of(&out, "Out", "a").is_empty(), "{out:?}");
    assert!(of(&out, "Out", "a").iter().all(|a| a == "o1"));

    // An X during the closure cancels the run.
    id(&mut p, "A", 1_000, "o2");
    id(&mut p, "B", 1_100, "o2");
    id(&mut p, "X", 1_150, "o2");
    id(&mut p, "B", 1_200, "o2");
    assert!(id(&mut p, "C", 1_300, "o2").is_empty());

    // So does an X after the last B, before C.
    id(&mut p, "A", 2_000, "o3");
    id(&mut p, "B", 2_100, "o3");
    id(&mut p, "X", 2_200, "o3");
    assert!(id(&mut p, "C", 2_300, "o3").is_empty());
}

#[test]
fn an_or_after_the_absence() {
    let pattern = "    A as a
    -> NOT X where id == a.id
    -> (C where id == a.id as e or D where id == a.id as e)
    within 1m";
    let mut p = program(pattern, "a: a.id");
    id(&mut p, "A", 0, "o1");
    id(&mut p, "A", 10, "o2");
    id(&mut p, "A", 20, "o3");
    id(&mut p, "X", 50, "o3");
    assert_eq!(of(&id(&mut p, "C", 100, "o1"), "Out", "a"), ["o1"]);
    assert_eq!(of(&id(&mut p, "D", 200, "o2"), "Out", "a"), ["o2"]);
    assert!(id(&mut p, "C", 300, "o3").is_empty());
    assert!(id(&mut p, "D", 300, "o3").is_empty());
}

#[test]
fn an_or_of_sequences_after_the_absence() {
    // The absence ends with the first step of either alternative.
    let pattern = "    A as a
    -> NOT X where id == a.id
    -> ((C where id == a.id as c -> D where id == a.id as d)
        or (E where id == a.id as c -> D where id == a.id as d))
    within 1m";
    let mut p = program(pattern, "a: a.id");
    id(&mut p, "A", 0, "o1");
    id(&mut p, "A", 10, "o2");
    id(&mut p, "C", 100, "o1");
    // After C: o1's absence is over.
    id(&mut p, "X", 150, "o1");
    id(&mut p, "X", 160, "o2");
    assert_eq!(of(&id(&mut p, "D", 200, "o1"), "Out", "a"), ["o1"]);
    id(&mut p, "E", 300, "o2");
    assert!(id(&mut p, "D", 400, "o2").is_empty());
}

#[test]
fn an_or_before_the_absence() {
    let pattern = "    (A as a or E as a)
    -> NOT X where id == a.id
    -> C where id == a.id as c
    within 1m";
    let mut p = program(pattern, "a: a.id");
    id(&mut p, "A", 0, "o1");
    id(&mut p, "E", 10, "o2");
    id(&mut p, "E", 20, "o3");
    id(&mut p, "X", 50, "o3");
    assert_eq!(of(&id(&mut p, "C", 100, "o1"), "Out", "a"), ["o1"]);
    assert_eq!(of(&id(&mut p, "C", 200, "o2"), "Out", "a"), ["o2"]);
    assert!(id(&mut p, "C", 300, "o3").is_empty());
}

#[test]
fn an_and_before_the_absence() {
    // The absence starts once both operands have come.
    let pattern = "    (A as a and E where id == a.id as e)
    -> NOT X where id == a.id
    -> C where id == a.id as c
    within 1m";
    let mut p = program(pattern, "a: a.id");
    id(&mut p, "A", 0, "o1");
    id(&mut p, "E", 10, "o1");
    id(&mut p, "A", 20, "o2");
    // Before o2's E: not yet watched.
    id(&mut p, "X", 30, "o2");
    id(&mut p, "E", 40, "o2");
    id(&mut p, "A", 50, "o3");
    id(&mut p, "E", 60, "o3");
    id(&mut p, "X", 70, "o3");
    let out = id(&mut p, "C", 100, "o1");
    assert_eq!(of(&out, "Out", "a"), ["o1"]);
    assert_eq!(of(&id(&mut p, "C", 200, "o2"), "Out", "a"), ["o2"]);
    assert!(id(&mut p, "C", 300, "o3").is_empty());
}

#[test]
fn an_and_after_the_absence() {
    // The absence ends with the first operand the run takes.
    let pattern = "    A as a
    -> NOT X where id == a.id
    -> (C where id == a.id as c and D where id == a.id as d)
    within 1m";
    let mut p = program(pattern, "a: a.id");
    id(&mut p, "A", 0, "o1");
    id(&mut p, "A", 10, "o2");
    id(&mut p, "X", 20, "o2");
    id(&mut p, "D", 100, "o1");
    id(&mut p, "X", 150, "o1");
    assert_eq!(of(&id(&mut p, "C", 200, "o1"), "Out", "a"), ["o1"]);
    id(&mut p, "D", 300, "o2");
    assert!(id(&mut p, "C", 400, "o2").is_empty());
}

// ---------------------------------------------------------------------------
// Event time out of order, and snapshots
// ---------------------------------------------------------------------------

#[test]
fn out_of_order_events_are_judged_in_the_order_they_arrive() {
    // A sequence is the order events arrive in; their stamps bound the
    // `within`. A forbidden event that arrives between A and C cancels the
    // run whatever its stamp, and one that arrives after C, even stamped
    // before it, cancels nothing.
    let mut p = program(A_NOT_B_C, "a: a.id");
    id(&mut p, "A", 5_000, "o1");
    id(&mut p, "A", 5_000, "o2");
    // Late: stamped before o1's A, arriving after it.
    id(&mut p, "B", 4_000, "o1");
    let out = id(&mut p, "C", 6_000, "c");
    assert_eq!(of(&out, "Out", "a"), ["o2"]);

    id(&mut p, "A", 7_000, "o3");
    // C stamped later than the B that follows it.
    assert_eq!(of(&id(&mut p, "C", 9_000, "c"), "Out", "a"), ["o3"]);
    assert!(id(&mut p, "B", 8_000, "o3").is_empty());
}

#[test]
fn a_late_step_is_taken_as_the_same_sequence_without_the_absence_takes_it() {
    // The absence changes which runs survive, not how a step's stamp is
    // judged: with no forbidden event, `A -> NOT B -> C` and `A -> C` give
    // the same matches for the same events, late ones included.
    let feeds: &[(&str, i64, &str)] = &[
        ("A", 70_000, "o1"),
        ("C", 5_000, "late"),
        ("A", 72_000, "o2"),
        ("C", 71_000, "c"),
        ("C", 200_000, "far"),
    ];
    let mut with = program(A_NOT_B_C, "a: a.id, c: c.id");
    let mut without = program(
        "    A as a\n    -> C as c\n    within 1m",
        "a: a.id, c: c.id",
    );
    for &(event_type, ms, key) in feeds {
        let a = id(&mut with, event_type, ms, key);
        let b = id(&mut without, event_type, ms, key);
        assert_eq!(
            (of(&a, "Out", "a"), of(&a, "Out", "c")),
            (of(&b, "Out", "a"), of(&b, "Out", "c")),
            "{event_type} at {ms}"
        );
    }
}

#[test]
fn a_snapshot_taken_mid_pattern_keeps_the_absence() {
    let mut before = program(A_NOT_B_C, "a: a.id");
    id(&mut before, "A", 0, "o1");
    id(&mut before, "A", 10, "o2");
    let bytes = before.snapshot().unwrap();

    // Restored, the forbidden event still cancels its run...
    let mut after = program(A_NOT_B_C, "a: a.id");
    after.restore(&bytes).unwrap();
    id(&mut after, "B", 100, "o1");
    // ...and the step after the absence still completes the other one.
    assert_eq!(of(&id(&mut after, "C", 200, "c"), "Out", "a"), ["o2"]);

    // The same for two absences in a row and a closure before one.
    for pattern in [
        "    A as a
    -> NOT B where id == a.id
    -> NOT D where id == a.id
    -> C as c
    within 1m",
        "    A as a
    -> all E where id == a.id as es
    -> NOT B where id == a.id
    -> C as c
    within 1m",
    ] {
        let mut before = program(pattern, "a: a.id");
        for o in ["o1", "o2", "o3"] {
            id(&mut before, "A", 0, o);
            id(&mut before, "E", 1, o);
        }
        let bytes = before.snapshot().unwrap();
        let mut after = program(pattern, "a: a.id");
        after.restore(&bytes).unwrap();
        id(&mut after, "B", 100, "o1");
        id(&mut after, "D", 110, "o2");
        let out = of(&id(&mut after, "C", 200, "c"), "Out", "a");
        let expected_two = pattern.contains("NOT D");
        if expected_two {
            assert_eq!(out, ["o3"], "{pattern}");
        } else {
            // The D is no forbidden event here.
            assert!(!out.is_empty() && !out.contains(&"o1".to_string()));
            assert!(out.contains(&"o2".to_string()) && out.contains(&"o3".to_string()));
        }
    }
}
