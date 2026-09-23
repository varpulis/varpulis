//! A window whose sources go quiet still closes in a live program.
//!
//! A window closes when the event time of the types feeding it passes its
//! end, and only an event of those types moves that time. On a live stream a
//! brute force on a sparse source (VPN logons, one application's log) was
//! raised only when that source's next event came, maybe hours later. A host
//! that runs live sets an idle grace: once a source has sent nothing for that
//! long, its event time moves on with the wall clock (less the grace, for
//! events still in flight), so its windows close. Replays set none, and stay
//! judged in event time alone.

use std::thread::sleep;
use std::time::{Duration, Instant};

use varpulis_engine::Program;

const BRUTE_FORCE: &str = r#"
stream Failed = Vpn
    .where(status == "failure")

stream Brute = Failed
    .partition_by(ip)
    .window(5m)
    .aggregate(ip: last(ip), n: count())
    .where(n >= 3)
    .emit(ip: ip, n: n)
"#;

fn feed(program: &mut Program, event_type: &str, json: &str) -> Vec<serde_json::Value> {
    program
        .feed_json(event_type, json.as_bytes())
        .unwrap()
        .iter()
        .map(|emit| emit.to_json())
        .collect()
}

fn vpn(time: &str, ip: &str, status: &str) -> String {
    format!(r#"{{"@timestamp": "2026-09-23T{time}Z", "ip": "{ip}", "status": "{status}"}}"#)
}

fn attack(program: &mut Program) {
    for time in ["10:00:00", "10:00:10", "10:00:20"] {
        assert!(feed(program, "Vpn", &vpn(time, "10.0.0.66", "failure")).is_empty());
    }
}

fn tick_at(program: &mut Program, now: Instant) -> Vec<serde_json::Value> {
    program
        .tick_at(now)
        .unwrap()
        .iter()
        .map(|emit| emit.to_json())
        .collect()
}

#[test]
fn a_quiet_source_closes_its_windows_once_the_grace_has_passed() {
    let mut program = Program::compile(BRUTE_FORCE).unwrap();
    program.set_idle_grace(Some(Duration::from_mins(1)));
    attack(&mut program);
    let quiet_since = Instant::now();
    // Two minutes of silence: event time has moved one minute past the last
    // failure (less the grace), the window is still running.
    assert!(tick_at(&mut program, quiet_since + Duration::from_mins(2)).is_empty());
    // Six minutes: past the window's end.
    let out = tick_at(&mut program, quiet_since + Duration::from_mins(6));
    assert_eq!(out.len(), 1, "{out:?}");
    assert_eq!(out[0]["ip"], "10.0.0.66");
    assert_eq!(out[0]["n"], 3);
}

#[test]
fn without_a_grace_a_program_is_judged_in_event_time_alone() {
    // Guard: a replay sets no grace; the wall clock never closes anything.
    let mut program = Program::compile(BRUTE_FORCE).unwrap();
    attack(&mut program);
    assert!(tick_at(&mut program, Instant::now() + Duration::from_hours(1)).is_empty());
}

#[test]
fn a_quiet_source_closes_while_another_keeps_the_program_busy() {
    let mut program = Program::compile(
        r#"
stream Failed = Vpn
    .where(status == "failure")

stream Brute = Failed
    .window(100ms)
    .aggregate(n: count())
    .where(n >= 3)
    .emit(n: n)

stream Pages = Web
    .where(uri == "/")
"#,
    )
    .unwrap();
    program.set_idle_grace(Some(Duration::from_millis(20)));
    for ms in ["000", "010", "020"] {
        feed(
            &mut program,
            "Vpn",
            &format!(r#"{{"@timestamp": "2026-09-23T10:00:00.{ms}Z", "status": "failure"}}"#),
        );
    }
    sleep(Duration::from_millis(300));
    // Web traffic keeps coming; the VPN has been quiet well past the grace.
    let out = feed(
        &mut program,
        "Web",
        r#"{"@timestamp": "2026-09-23T10:00:00.030Z", "uri": "/index"}"#,
    );
    assert!(out.iter().any(|e| e["n"] == 3), "{out:?}");
}

#[test]
fn the_grace_holds_the_window_for_events_still_in_flight() {
    let mut program = Program::compile(BRUTE_FORCE).unwrap();
    program.set_idle_grace(Some(Duration::from_mins(10)));
    attack(&mut program);
    // Six minutes of silence is within the ten-minute grace: nothing moves.
    assert!(tick_at(&mut program, Instant::now() + Duration::from_mins(6)).is_empty());
}

#[test]
fn a_restored_program_knows_where_its_sources_were() {
    let mut program = Program::compile(BRUTE_FORCE).unwrap();
    attack(&mut program);
    let snapshot = program.snapshot().unwrap();

    // The restarted host reads nothing more from the VPN.
    let mut restored = Program::compile(BRUTE_FORCE).unwrap();
    restored.restore(&snapshot).unwrap();
    restored.set_idle_grace(Some(Duration::from_mins(1)));
    let out = tick_at(&mut restored, Instant::now() + Duration::from_mins(6));
    assert_eq!(out.len(), 1, "{out:?}");
    assert_eq!(out[0]["n"], 3);
}
