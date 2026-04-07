//! Regression test for native-JSONL @timestamp parsing.
//!
//! Background: prior to the fix in this PR, `EventFileParser::parse_jsonl_line`
//! only called `apply_json_timestamp` for the Sysmon and generic-flat-JSON
//! formats. The Varpulis native format `{"event_type": "X", "data": {...}}`
//! silently ignored a top-level `@timestamp` field, meaning every parsed
//! event got `Utc::now()` and time-based windows never advanced when replaying
//! historical data through `varpulis simulate`.
//!
//! The fix calls `apply_json_timestamp` in the native path as well. This test
//! pins that behaviour.

use varpulis_runtime::event_file::EventFileParser;

#[test]
fn native_jsonl_with_at_timestamp_is_preserved() {
    let line =
        r#"{"@timestamp":"2023-06-15T12:00:00.000Z","event_type":"Tick","data":{"value":42}}"#;
    let parsed = EventFileParser::parse(line).expect("parse");
    assert_eq!(parsed.len(), 1);
    let evt = &parsed[0].event;
    assert_eq!(evt.event_type.as_ref(), "Tick");

    let expected = chrono::DateTime::parse_from_rfc3339("2023-06-15T12:00:00.000Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    assert_eq!(
        evt.timestamp, expected,
        "native JSONL with @timestamp should preserve event-time, not stamp with wall-clock"
    );
}

#[test]
fn native_jsonl_without_at_timestamp_uses_wall_clock() {
    let line = r#"{"event_type":"Tick","data":{"value":42}}"#;
    let before = chrono::Utc::now();
    let parsed = EventFileParser::parse(line).expect("parse");
    let after = chrono::Utc::now();
    assert_eq!(parsed.len(), 1);
    let evt = &parsed[0].event;
    assert!(
        evt.timestamp >= before && evt.timestamp <= after,
        "native JSONL without @timestamp should fall back to wall-clock"
    );
}

#[test]
fn native_jsonl_with_event_time_window() {
    // Two events 2 seconds apart (event-time) should land in different
    // 1-second tumbling windows once parsed.
    let line1 =
        r#"{"@timestamp":"2023-06-15T12:00:00.000Z","event_type":"Tick","data":{"value":1}}"#;
    let line2 =
        r#"{"@timestamp":"2023-06-15T12:00:02.000Z","event_type":"Tick","data":{"value":2}}"#;
    let parsed1 = EventFileParser::parse(line1).expect("parse 1");
    let parsed2 = EventFileParser::parse(line2).expect("parse 2");
    let delta = parsed2[0]
        .event
        .timestamp
        .signed_duration_since(parsed1[0].event.timestamp);
    assert_eq!(delta.num_seconds(), 2);
}
