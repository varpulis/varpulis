//! A sequence step that names a derived stream reads what that stream
//! outputs.
//!
//! Such a step was compiled onto the stream's source type, with the stream's
//! first `.where()` as its predicate. For a stream that only filters, that is
//! the same thing, and cheaper. For any other stream it was not: over an
//! aggregate the step matched the raw events instead of the results
//! (`Burst as b -> ...` never fired when the aggregate had a `.where()`, and
//! `b.n` was null when it had none), and a stream with two `.where()` lost
//! the second.

use varpulis_engine::Program;

fn feed(program: &mut Program, event_type: &str, json: &str) -> Vec<serde_json::Value> {
    program
        .feed_json(event_type, json.as_bytes())
        .unwrap()
        .iter()
        .map(|emit| emit.to_json())
        .collect()
}

fn web(time: &str, uri: &str) -> String {
    format!(r#"{{"@timestamp": "2026-09-23T{time}Z", "uri": "{uri}"}}"#)
}

#[test]
fn a_sequence_over_an_aggregate_reads_its_results() {
    let mut program = Program::compile(
        r#"
stream Uploads = Web
    .where(uri == "/upload")

stream Burst = Uploads
    .window(30s)
    .aggregate(n: count())
    .where(n >= 3)
    .emit(n: n)

stream Logouts = Web
    .where(uri == "/logout")

stream BurstThenLogout = Burst as b
    -> Logouts as l
    .within(5m)
    .emit(uploads: b.n)
"#,
    )
    .unwrap();
    let mut out = Vec::new();
    for time in ["10:00:05", "10:00:06", "10:00:07"] {
        out.extend(feed(&mut program, "Web", &web(time, "/upload")));
    }
    // A later upload closes the burst's window: the burst is out.
    out.extend(feed(&mut program, "Web", &web("10:00:50", "/upload")));
    out.extend(feed(&mut program, "Web", &web("10:01:00", "/logout")));
    assert!(
        out.iter().any(|e| e["uploads"] == 3),
        "no burst-then-logout alert with its count: {out:?}"
    );
}

const ADMIN_THEN_DELETE: &str = r#"
stream Admins = Auth
    .where(status == "success")
    .where(user == "admin")

stream AdminThenDelete = Admins as a
    -> Audit where action == "delete" as d
    .within(5m)
    .emit(user: a.user)
"#;

#[test]
fn every_where_of_the_stream_a_step_names_applies() {
    let mut program = Program::compile(ADMIN_THEN_DELETE).unwrap();
    feed(
        &mut program,
        "Auth",
        r#"{"@timestamp": "2026-09-23T10:00:00Z", "status": "success", "user": "bob"}"#,
    );
    let out = feed(
        &mut program,
        "Audit",
        r#"{"@timestamp": "2026-09-23T10:01:00Z", "action": "delete"}"#,
    );
    assert!(out.is_empty(), "bob is not admin: {out:?}");

    feed(
        &mut program,
        "Auth",
        r#"{"@timestamp": "2026-09-23T10:02:00Z", "status": "success", "user": "admin"}"#,
    );
    let out = feed(
        &mut program,
        "Audit",
        r#"{"@timestamp": "2026-09-23T10:03:00Z", "action": "delete"}"#,
    );
    assert_eq!(out.len(), 1, "{out:?}");
    assert_eq!(out[0]["user"], "admin");
}

#[test]
fn a_step_sees_what_the_stream_it_names_outputs() {
    // A stream that only filters lets its events through as they are; one
    // that emits outputs what it emits, to a step as to any stream below it.
    for (filter, field, expected) in [
        (
            "stream Uploads = Web\n    .where(uri == \"/upload\")\n",
            "uri",
            "/upload",
        ),
        (
            "stream Uploads = Web\n    .where(uri == \"/upload\")\n    .emit(path: uri)\n",
            "path",
            "/upload",
        ),
    ] {
        let mut program = Program::compile(&format!(
            r#"
{filter}
stream UploadThenLogout = Uploads as a
    -> Web where uri == "/logout" as l
    .within(5m)
    .emit(first: a.{field})
"#
        ))
        .unwrap();
        feed(&mut program, "Web", &web("10:00:00", "/upload"));
        feed(&mut program, "Web", &web("10:00:10", "/index"));
        let out = feed(&mut program, "Web", &web("10:00:20", "/logout"));
        let firsts: Vec<_> = out.iter().filter_map(|e| e.get("first").cloned()).collect();
        assert_eq!(
            firsts,
            vec![serde_json::json!(expected)],
            "{filter}: {out:?}"
        );
    }
}

#[test]
fn a_first_step_filter_adds_to_the_filter_of_the_stream_it_names() {
    // `Uploads where host == "a"` is an upload from host a, not any event
    // from host a: the step's own filter used to replace the stream's.
    let mut program = Program::compile(
        r#"
stream Uploads = Web
    .where(uri == "/upload")

stream UploadThenLogout = Uploads where host == "a" as u
    -> Web where uri == "/logout" as l
    .within(5m)
    .emit(first: u.uri)
"#,
    )
    .unwrap();
    feed(
        &mut program,
        "Web",
        r#"{"@timestamp": "2026-09-23T10:00:00Z", "uri": "/index", "host": "a"}"#,
    );
    let out = feed(
        &mut program,
        "Web",
        r#"{"@timestamp": "2026-09-23T10:00:10Z", "uri": "/logout", "host": "a"}"#,
    );
    assert!(
        out.is_empty(),
        "matched a page that is not an upload: {out:?}"
    );
}
