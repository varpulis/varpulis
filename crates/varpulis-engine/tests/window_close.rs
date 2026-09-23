//! A time window closes when the event time of the types feeding it passes
//! its end, not only when a later event reaches that same window.
//!
//! Windows used to close on arrival: a window over a filtered stream stayed
//! open until the next event that passed the filter, and a partitioned window
//! until the next event of the same partition. On a live stream a brute force
//! that ended in a success, or simply stopped, never raised its count, since
//! nothing reached its window again; `simulate` hid it by closing every window
//! at the end of the file. With `.watermark()` the window did close on event
//! time, but on the slowest of the program's event types: one type that went
//! quiet held back every window of the others.

use varpulis_engine::Program;

fn feed(program: &mut Program, event_type: &str, json: &str) -> Vec<serde_json::Value> {
    program
        .feed_json(event_type, json.as_bytes())
        .unwrap()
        .iter()
        .map(|emit| emit.to_json())
        .collect()
}

fn security(time: &str, event_id: i64, ip: &str) -> String {
    format!(r#"{{"@timestamp": "2026-09-23T{time}Z", "EventID": {event_id}, "IpAddress": "{ip}"}}"#)
}

const BRUTE_FORCE: &str = r"
stream Failed = Security
    .where(EventID == 4625)

stream Brute = Failed
    .partition_by(IpAddress)
    .window(5m)
    .aggregate(ip: last(IpAddress), n: count())
    .where(n >= 3)
    .emit(ip: ip, n: n)
";

#[test]
fn a_brute_force_that_ends_in_a_success_is_raised_once_its_window_is_over() {
    let mut program = Program::compile(BRUTE_FORCE).unwrap();
    for time in ["10:00:00", "10:00:10", "10:00:20"] {
        assert!(feed(&mut program, "Security", &security(time, 4625, "10.0.0.66")).is_empty());
    }
    // The attacker gets in and stops: no failure ever reaches the window again.
    assert!(feed(
        &mut program,
        "Security",
        &security("10:00:30", 4624, "10.0.0.66")
    )
    .is_empty());
    // Any later Security event moves the clock of Security past the window's end.
    let alerts = feed(
        &mut program,
        "Security",
        &security("10:06:00", 4624, "10.0.0.7"),
    );
    assert_eq!(alerts.len(), 1, "{alerts:?}");
    assert_eq!(alerts[0]["ip"], "10.0.0.66");
    assert_eq!(alerts[0]["n"], 3);
}

#[test]
fn each_partition_closes_on_the_clock_with_its_own_count() {
    let mut program = Program::compile(BRUTE_FORCE).unwrap();
    for time in ["10:00:00", "10:00:10", "10:00:20"] {
        feed(&mut program, "Security", &security(time, 4625, "10.0.0.66"));
    }
    // A failure from another address, well after the first window: the first
    // address's window closes on its own, not merged with this one.
    let alerts = feed(
        &mut program,
        "Security",
        &security("10:06:00", 4625, "10.0.0.9"),
    );
    assert_eq!(alerts.len(), 1, "{alerts:?}");
    assert_eq!(alerts[0]["ip"], "10.0.0.66");
    assert_eq!(alerts[0]["n"], 3);
}

#[test]
fn a_window_closes_after_the_windows_above_it_have_passed_on_what_they_held() {
    let mut program = Program::compile(
        r#"
stream Uploads = Web
    .where(uri == "/upload")

stream Burst = Uploads
    .window(30s)
    .aggregate(n: count())
    .where(n >= 3)
    .emit(n: n)

stream BurstsPerMinute = Burst
    .window(1m)
    .aggregate(bursts: count())
    .emit(bursts: bursts)
"#,
    )
    .unwrap();
    for time in ["10:00:05", "10:00:06", "10:00:07"] {
        feed(
            &mut program,
            "Web",
            &format!(r#"{{"@timestamp": "2026-09-23T{time}Z", "uri": "/upload"}}"#),
        );
    }
    // One later event is past both windows. The burst closes first and its
    // result reaches the minute window, which then closes holding it.
    let out = feed(
        &mut program,
        "Web",
        r#"{"@timestamp": "2026-09-23T10:02:00Z", "uri": "/logout"}"#,
    );
    assert!(out.iter().any(|e| e["n"] == 3), "burst not closed: {out:?}");
    assert!(
        out.iter().any(|e| e["bursts"] == 1),
        "the minute window closed before the burst reached it: {out:?}"
    );
}

#[test]
fn a_quiet_event_type_does_not_hold_back_the_windows_of_the_others() {
    let mut program = Program::compile(
        r"
stream CountA = TypeA
    .watermark(out_of_order: 0s)
    .window(30s)
    .aggregate(n: count())
    .emit(a: n)

stream CountB = TypeB
    .watermark(out_of_order: 0s)
    .window(30s)
    .aggregate(n: count())
    .emit(b: n)
",
    )
    .unwrap();
    feed(
        &mut program,
        "TypeB",
        r#"{"@timestamp": "2026-09-23T09:59:00Z"}"#,
    );
    feed(
        &mut program,
        "TypeA",
        r#"{"@timestamp": "2026-09-23T10:00:00Z"}"#,
    );
    feed(
        &mut program,
        "TypeA",
        r#"{"@timestamp": "2026-09-23T10:00:05Z"}"#,
    );
    // TypeB has gone quiet; TypeA's first window is over all the same.
    let out = feed(
        &mut program,
        "TypeA",
        r#"{"@timestamp": "2026-09-23T10:05:00Z"}"#,
    );
    assert!(
        out.iter().any(|e| e["a"] == 2),
        "window of TypeA still open: {out:?}"
    );
}

#[test]
fn a_declared_out_of_orderness_holds_the_window_that_much_longer() {
    let mut program = Program::compile(
        r"
stream Count = Log
    .watermark(out_of_order: 1m)
    .window(30s)
    .aggregate(n: count())
    .emit(n: n)
",
    )
    .unwrap();
    feed(
        &mut program,
        "Log",
        r#"{"@timestamp": "2026-09-23T10:00:00Z"}"#,
    );
    feed(
        &mut program,
        "Log",
        r#"{"@timestamp": "2026-09-23T10:00:10Z"}"#,
    );
    // 10:01:00 is past the window's end but not past end + 1 minute.
    let early = feed(
        &mut program,
        "Log",
        r#"{"@timestamp": "2026-09-23T10:01:00Z"}"#,
    );
    assert!(
        early.is_empty(),
        "closed before its out-of-orderness: {early:?}"
    );
    let late = feed(
        &mut program,
        "Log",
        r#"{"@timestamp": "2026-09-23T10:01:31Z"}"#,
    );
    assert!(late.iter().any(|e| e["n"] == 2), "{late:?}");
}

#[test]
fn a_sliding_window_fed_by_its_own_stream_emits_as_it_did() {
    // Guard: for events of the window's own stream, emissions are unchanged —
    // each includes the event that triggered it.
    let mut program = Program::compile(
        r"
stream Rate = Log
    .window(5m, sliding: 1m)
    .aggregate(n: count())
    .emit(n: n)
",
    )
    .unwrap();
    let counts: Vec<i64> = ["10:00:00", "10:00:30", "10:01:00", "10:01:10", "10:02:05"]
        .iter()
        .flat_map(|time| {
            feed(
                &mut program,
                "Log",
                &format!(r#"{{"@timestamp": "2026-09-23T{time}Z"}}"#),
            )
        })
        .map(|e| e["n"].as_i64().unwrap())
        .collect();
    assert_eq!(counts, vec![1, 3, 5]);
}

const SESSIONS: &str = r#"
stream Visits = Web
    .where(uri != "/health")

stream Session = Visits
    .partition_by(user)
    .window(session: 5m)
    .aggregate(user: last(user), pages: count())
    .emit(user: user, pages: pages)
"#;

fn visit(time: &str, user: &str, uri: &str) -> String {
    format!(r#"{{"@timestamp": "2026-09-23T{time}Z", "user": "{user}", "uri": "{uri}"}}"#)
}

#[test]
fn a_session_ends_on_the_clock_once_the_gap_has_passed() {
    let mut program = Program::compile(SESSIONS).unwrap();
    feed(&mut program, "Web", &visit("10:00:00", "alice", "/a"));
    feed(&mut program, "Web", &visit("10:02:00", "alice", "/b"));
    // Only health checks from then on: they never reach the session's stream.
    assert!(feed(&mut program, "Web", &visit("10:06:00", "probe", "/health")).is_empty());
    let out = feed(&mut program, "Web", &visit("10:07:01", "probe", "/health"));
    assert_eq!(out.len(), 1, "{out:?}");
    assert_eq!(out[0]["user"], "alice");
    assert_eq!(out[0]["pages"], 2);
}

#[test]
fn an_event_right_at_the_gap_still_belongs_to_the_session() {
    // Guard: the clock closes a session as an arrival would, only once more
    // than the gap has passed.
    let mut program = Program::compile(SESSIONS).unwrap();
    feed(&mut program, "Web", &visit("10:00:00", "alice", "/a"));
    assert!(feed(&mut program, "Web", &visit("10:05:00", "alice", "/b")).is_empty());
    let out = feed(&mut program, "Web", &visit("10:10:01", "bob", "/c"));
    assert_eq!(out.len(), 1, "{out:?}");
    assert_eq!(out[0]["pages"], 2);
}
