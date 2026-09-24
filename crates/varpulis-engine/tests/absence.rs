//! An absence (`-> NOT B within X`) fires once event time passes its
//! deadline, not only when a later event reaches the pattern.
//!
//! The deadline was checked against the pattern's own watermark, which only
//! an event the pattern receives moves. On a live stream that went quiet after
//! the trigger ("no acknowledgement within 4h", "no heartbeat within 5
//! minutes") the alert never came, and when the pattern read a derived stream
//! the events that stream filtered out never moved it either.

use std::time::{Duration, Instant};

use varpulis_engine::Program;

fn feed(program: &mut Program, event_type: &str, json: &str) -> Vec<serde_json::Value> {
    program
        .feed_json(event_type, json.as_bytes())
        .unwrap()
        .iter()
        .map(|emit| emit.to_json())
        .collect()
}

fn tick_at(program: &mut Program, now: Instant) -> Vec<serde_json::Value> {
    program
        .tick_at(now)
        .unwrap()
        .iter()
        .map(|emit| emit.to_json())
        .collect()
}

const UNACKED: &str = r#"
event Order:
    id: str
event Ack:
    id: str

pattern Unacked =
    Order as o
    -> NOT Ack where id == o.id
    within 4h
    partition by id

stream Alerts = Unacked
    .emit(order: o.id, violation: "no acknowledgement within 4h")
"#;

fn order(program: &mut Program, event_type: &str, time: &str, id: &str) -> Vec<serde_json::Value> {
    feed(
        program,
        event_type,
        &format!(r#"{{"@timestamp": "2026-09-24T{time}Z", "id": "{id}"}}"#),
    )
}

#[test]
fn an_absence_on_a_quiet_stream_fires_once_the_grace_has_passed() {
    let mut program = Program::compile(UNACKED).unwrap();
    program.set_idle_grace(Some(Duration::from_mins(1)));
    assert!(order(&mut program, "Order", "10:00:00", "o-1").is_empty());
    let quiet_since = Instant::now();
    // Three hours of silence: still within the four.
    assert!(tick_at(&mut program, quiet_since + Duration::from_hours(3)).is_empty());
    let out = tick_at(
        &mut program,
        quiet_since + Duration::from_hours(4) + Duration::from_mins(2),
    );
    assert_eq!(out.len(), 1, "{out:?}");
    assert_eq!(out[0]["order"], "o-1");
}

#[test]
fn an_absence_whose_event_came_does_not_fire() {
    // Guard: the acknowledgement cancels the run; time passing afterwards
    // completes nothing.
    let mut program = Program::compile(UNACKED).unwrap();
    program.set_idle_grace(Some(Duration::from_mins(1)));
    order(&mut program, "Order", "10:00:00", "o-1");
    assert!(order(&mut program, "Ack", "10:30:00", "o-1").is_empty());
    assert!(tick_at(&mut program, Instant::now() + Duration::from_hours(5)).is_empty());
}

#[test]
fn an_absence_still_fires_with_the_next_event_past_its_deadline() {
    // Guard: the arrival path, unchanged.
    let mut program = Program::compile(UNACKED).unwrap();
    order(&mut program, "Order", "10:00:00", "o-1");
    let out = order(&mut program, "Order", "14:00:01", "o-2");
    assert_eq!(out.len(), 1, "{out:?}");
    assert_eq!(out[0]["order"], "o-1");
}

#[test]
fn without_a_grace_the_wall_clock_completes_no_absence() {
    // Guard: a replay is judged in event time alone.
    let mut program = Program::compile(UNACKED).unwrap();
    order(&mut program, "Order", "10:00:00", "o-1");
    assert!(tick_at(&mut program, Instant::now() + Duration::from_hours(5)).is_empty());
}

#[test]
fn the_end_of_the_input_confirms_no_absence() {
    // Guard: a log that stops ten minutes after the order says nothing about
    // the hours that followed.
    let mut program = Program::compile(UNACKED).unwrap();
    order(&mut program, "Order", "10:00:00", "o-1");
    order(&mut program, "Order", "10:10:00", "o-2");
    let out: Vec<serde_json::Value> = program
        .end_of_input()
        .unwrap()
        .iter()
        .map(|emit| emit.to_json())
        .collect();
    assert!(out.is_empty(), "{out:?}");
}

#[test]
fn an_absence_over_a_derived_stream_fires_on_the_events_that_stream_filters_out() {
    // The pattern reads what `Burst` outputs; the logons that keep coming never
    // reach it, but they are Security events all the same, and move its time.
    let mut program = Program::compile(
        r"
stream Failed = Security
    .where(EventID == 4625)

stream Burst = Failed
    .partition_by(IpAddress)
    .window(5m)
    .aggregate(ip: last(IpAddress), n: count())
    .where(n >= 3)
    .emit(ip: ip, n: n)

pattern Unanswered =
    Burst as b
    -> NOT Lockout where ip == b.ip
    within 10m

stream Alerts = Unanswered
    .emit(unanswered: b.ip, failures: b.n)
",
    )
    .unwrap();
    let security = |program: &mut Program, time: &str, event_id: i64, ip: &str| {
        feed(
            program,
            "Security",
            &format!(
                r#"{{"@timestamp": "2026-09-24T{time}Z", "EventID": {event_id}, "IpAddress": "{ip}"}}"#
            ),
        )
    };
    let mut out = Vec::new();
    for time in ["10:00:00", "10:00:10", "10:00:20"] {
        out.extend(security(&mut program, time, 4625, "10.0.0.66"));
    }
    // Logons from elsewhere: the burst closes, then its ten minutes pass.
    for time in ["10:06:00", "10:12:00", "10:17:00"] {
        out.extend(security(&mut program, time, 4624, "10.0.0.7"));
    }
    let alerts: Vec<_> = out
        .iter()
        .filter(|e| e.get("unanswered").is_some())
        .collect();
    assert_eq!(alerts.len(), 1, "{out:?}");
    assert_eq!(alerts[0]["unanswered"], "10.0.0.66");
    assert_eq!(alerts[0]["failures"], 3);
}

/// A program that saw order o-1 at 10:00, restarted from its snapshot.
fn restored_after_an_order() -> Program {
    let mut program = Program::compile(UNACKED).unwrap();
    order(&mut program, "Order", "10:00:00", "o-1");
    let snapshot = program.snapshot().unwrap();
    let mut restored = Program::compile(UNACKED).unwrap();
    restored.restore(&snapshot).unwrap();
    restored
}

#[test]
fn a_restored_absence_fires_with_the_next_event_past_its_deadline() {
    // The snapshot kept the run but not what its negated step waits for, so
    // at the deadline the run was dropped instead of completing.
    let mut restored = restored_after_an_order();
    let out = order(&mut restored, "Order", "14:00:01", "o-2");
    assert_eq!(out.len(), 1, "{out:?}");
    assert_eq!(out[0]["order"], "o-1");
}

#[test]
fn a_restored_absence_is_still_cancelled_by_its_event() {
    // Guard: the rebuilt step still knows what it forbids.
    let mut restored = restored_after_an_order();
    assert!(order(&mut restored, "Ack", "10:30:00", "o-1").is_empty());
    let out = order(&mut restored, "Order", "14:00:01", "o-2");
    assert!(out.is_empty(), "{out:?}");
}

#[test]
fn a_restored_absence_on_a_quiet_stream_fires_once_the_grace_has_passed() {
    let mut restored = restored_after_an_order();
    restored.set_idle_grace(Some(Duration::from_mins(1)));
    let out = tick_at(
        &mut restored,
        Instant::now() + Duration::from_hours(4) + Duration::from_mins(2),
    );
    assert_eq!(out.len(), 1, "{out:?}");
    assert_eq!(out[0]["order"], "o-1");
}
