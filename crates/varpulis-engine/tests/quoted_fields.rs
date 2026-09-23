//! A field whose name is not an identifier, written between backticks.
//!
//! Web servers and proxies log in W3C extended format: `cs-uri-query`,
//! `cs-method`, `sc-status`, `c-useragent`. A decoded event carries those
//! names as they are, and VPL had no way to say them, so a rule over proxy or
//! web logs could not be written at all (139 of the SigmaHQ rules).

use varpulis_engine::Program;

const HIT: &[u8] = br#"{"type": "Proxy", "@timestamp": "2026-09-22T10:00:01Z", "cs-method": "POST", "cs-uri-query": "cmd=whoami", "sc-status": 200, "c-ip": "10.0.0.5"}"#;
const START: &[u8] =
    br#"{"type": "Login", "@timestamp": "2026-09-22T10:00:00Z", "ip": "10.0.0.5"}"#;

fn emits(src: &str, events: &[(&str, &[u8])]) -> Vec<serde_json::Value> {
    let mut program = Program::compile(src).unwrap_or_else(|e| panic!("{src}\n{e}"));
    let mut out = Vec::new();
    for (ty, json) in events {
        out.extend(
            program
                .feed_json(ty, json)
                .unwrap()
                .iter()
                .map(|e| e.to_json()),
        );
    }
    out
}

#[test]
fn a_backticked_field_is_read_in_where_and_emit() {
    let src = r"
stream WebShell = Proxy
    .where(`cs-method` == 'POST' and contains(lower(`cs-uri-query`), 'cmd=') and `sc-status` == 200)
    .emit(method: `cs-method`, query: `cs-uri-query`)
";
    let out = emits(src, &[("Proxy", HIT)]);
    assert_eq!(out.len(), 1, "{out:?}");
    assert_eq!(out[0]["method"], "POST");
    assert_eq!(out[0]["query"], "cmd=whoami");
}

#[test]
fn a_backticked_field_is_read_in_a_sequence_step_and_through_an_alias() {
    let src = r"
stream LoginThenShell = Login as l
    -> Proxy where `c-ip` == l.ip and `cs-method` == 'POST' as p
    .within(1m)
    .emit(ip: l.ip, query: p.`cs-uri-query`)
";
    let out = emits(src, &[("Login", START), ("Proxy", HIT)]);
    assert_eq!(out.len(), 1, "{out:?}");
    assert_eq!(out[0]["query"], "cmd=whoami");
}

#[test]
fn a_backticked_field_can_key_a_partition() {
    let src = r"
stream Posts = Proxy
    .where(`cs-method` == 'POST')
    .partition_by(`c-ip`)
    .window(2)
    .aggregate(ip: last(`c-ip`), n: count())
    .emit(ip: ip, n: n)
";
    let out = emits(src, &[("Proxy", HIT), ("Proxy", HIT)]);
    assert_eq!(out.len(), 1, "{out:?}");
    assert_eq!(out[0]["ip"], "10.0.0.5");
    assert_eq!(out[0]["n"], 2);
}
