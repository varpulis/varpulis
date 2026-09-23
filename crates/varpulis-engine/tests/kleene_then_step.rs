//! A Kleene closure followed by another step: `A -> all B -> C`.
//!
//! Under the default emission mode (`.each()`) the engine emitted a complete
//! match at every B, as if the closure ended the pattern, and then dropped
//! the run when C arrived because "the matches were already emitted". The
//! shipped brute-force rule (failed logins, then a success) raised a
//! critical "brute force succeeded" alert on every failure, with no success
//! at all, and never on the success.

use varpulis_engine::Program;

const RULE: &str = r#"
event AuthEvent:
    source_ip: str
    username: str
    status: str

pattern BruteForceChain = AuthEvent where status == "failed" as first
    -> all AuthEvent where status == "failed" as fails
    -> AuthEvent where status == "success" as success
    within 30m partition by source_ip

stream BruteForceTakeover = BruteForceChain
    .emit(ip: first.source_ip, user: success.username, failed: count(fails) + 1)
"#;

fn auth(second: u32, status: &str) -> Vec<u8> {
    format!(
        r#"{{"type": "AuthEvent", "@timestamp": "2026-09-22T10:00:{second:02}Z", "source_ip": "10.0.0.66", "username": "admin", "status": "{status}"}}"#
    )
    .into_bytes()
}

fn run(program: &str, events: &[Vec<u8>]) -> Vec<serde_json::Value> {
    let mut program = Program::compile(program).unwrap();
    let mut out = Vec::new();
    for e in events {
        out.extend(
            program
                .feed_json("AuthEvent", e)
                .unwrap()
                .iter()
                .map(|m| m.to_json()),
        );
    }
    out.extend(program.end_of_input().unwrap().iter().map(|m| m.to_json()));
    out
}

#[test]
fn failures_alone_are_not_a_takeover() {
    let failures: Vec<_> = (0..4).map(|s| auth(s, "failed")).collect();
    let alerts = run(RULE, &failures);
    assert!(alerts.is_empty(), "no success, no alert: {alerts:?}");
}

#[test]
fn the_success_completes_the_match_and_carries_it() {
    let mut events: Vec<_> = (0..4).map(|s| auth(s, "failed")).collect();
    events.push(auth(10, "success"));
    let alerts = run(RULE, &events);
    assert!(!alerts.is_empty(), "the success completes the chain");
    for alert in &alerts {
        assert_eq!(
            alert["user"], "admin",
            "every alert is bound to the success: {alert}"
        );
        assert!(alert["failed"].as_i64().unwrap() >= 2, "{alert}");
    }
    assert!(
        alerts.iter().any(|a| a["failed"] == 4),
        "the run anchored on the first failure saw all four: {alerts:?}"
    );
}

#[test]
fn a_closure_that_ends_the_pattern_still_emits_at_every_event() {
    let src = r#"
event AuthEvent:
    source_ip: str
    username: str
    status: str

stream Failures = AuthEvent where status == "success" as ok
    -> all AuthEvent where status == "failed" as fails
    .within(30m)
    .emit(n: count(fails))
"#;
    let mut events = vec![auth(0, "success")];
    events.extend((1..4).map(|s| auth(s, "failed")));
    let alerts = run(src, &events);
    let counts: Vec<i64> = alerts.iter().filter_map(|a| a["n"].as_i64()).collect();
    assert_eq!(
        counts,
        vec![1, 2, 3],
        "one match per closure event, as .each() says: {alerts:?}"
    );
}

/// A host snapshots mid-closure and restores into a fresh program (Vejas
/// after a crash): the success must produce what it would have produced
/// without the interruption.
#[test]
fn a_restored_run_emits_what_the_live_one_would() {
    let failures: Vec<_> = (0..4).map(|s| auth(s, "failed")).collect();
    let success = auth(10, "success");

    let mut live = Program::compile(RULE).unwrap();
    for e in &failures {
        live.feed_json("AuthEvent", e).unwrap();
    }
    let mut expected: Vec<i64> = live
        .feed_json("AuthEvent", &success)
        .unwrap()
        .iter()
        .map(|m| m.to_json()["failed"].as_i64().unwrap())
        .collect();

    let mut before = Program::compile(RULE).unwrap();
    for e in &failures {
        before.feed_json("AuthEvent", e).unwrap();
    }
    let snapshot = before.snapshot().unwrap();
    let mut after = Program::compile(RULE).unwrap();
    after.restore(&snapshot).unwrap();
    let mut got: Vec<i64> = after
        .feed_json("AuthEvent", &success)
        .unwrap()
        .iter()
        .map(|m| m.to_json()["failed"].as_i64().unwrap())
        .collect();

    expected.sort_unstable();
    got.sort_unstable();
    assert!(!expected.is_empty());
    assert_eq!(got, expected, "restored vs live");
}
