//! `.stnm()`: an event that extends a run does not also start a new one.
//!
//! The patterns guide promises that under skip-till-next-match "each event
//! participates in at most one match". Every event that could start the
//! pattern started a run anyway, whatever the strategy, so a brute force of
//! four failures and a success raised one alert per failure under `.stnm()`
//! exactly as under the default. A detection wants one alert per attack.

use varpulis_engine::Program;

fn rule(modes: &str) -> String {
    format!(
        r#"
event AuthEvent:
    source_ip: str
    username: str
    status: str

stream Takeover = AuthEvent where status == "failed" as first
    -> all AuthEvent where status == "failed" as fails
    -> AuthEvent where status == "success" as success
    .within(30m)
    .partition_by(source_ip)
    {modes}
    .emit(ip: first.source_ip, failed: count(fails) + 1)
"#
    )
}

fn auth(second: u32, ip: &str, status: &str) -> Vec<u8> {
    format!(
        r#"{{"type": "AuthEvent", "@timestamp": "2026-09-22T10:00:{second:02}Z", "source_ip": "{ip}", "username": "admin", "status": "{status}"}}"#
    )
    .into_bytes()
}

fn failed_counts(modes: &str, events: &[Vec<u8>]) -> Vec<i64> {
    let mut program = Program::compile(&rule(modes)).unwrap();
    let mut out = Vec::new();
    for e in events {
        out.extend(
            program
                .feed_json("AuthEvent", e)
                .unwrap()
                .iter()
                .map(|m| m.to_json()["failed"].as_i64().unwrap()),
        );
    }
    out
}

fn attack(ip: &str, from: u32) -> Vec<Vec<u8>> {
    let mut events: Vec<_> = (0..4).map(|s| auth(from + s, ip, "failed")).collect();
    events.push(auth(from + 10, ip, "success"));
    events
}

#[test]
fn one_attack_is_one_alert_under_stnm() {
    assert_eq!(
        failed_counts(".stnm()\n    .longest()", &attack("10.0.0.66", 0)),
        vec![4]
    );
}

#[test]
fn the_default_still_anchors_a_run_on_every_failure() {
    let mut counts = failed_counts(".longest()", &attack("10.0.0.66", 0));
    counts.sort_unstable();
    assert_eq!(counts, vec![2, 3, 4]);
}

#[test]
fn a_new_attack_after_the_first_one_is_seen_under_stnm() {
    let mut events = attack("10.0.0.66", 0);
    events.extend(attack("10.0.0.66", 20));
    events.extend(attack("10.0.0.99", 40));
    assert_eq!(
        failed_counts(".stnm()\n    .longest()", &events),
        vec![4, 4, 4]
    );
}
