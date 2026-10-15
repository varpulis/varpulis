//! `varpulis simulate` reads a JSON line as a live source does.
//!
//! The event file parser had rules of its own for a JSON line: with an
//! `event_type` it read fields from a `data` object only, so a flat line lost
//! every field; and it took the time from `@timestamp` only, so `ts` and
//! `timestamp` in epoch milliseconds — the rules every connector and
//! `Program::feed_json` follow — were ignored and the event was stamped
//! with the wall clock.

use chrono::{DateTime, Utc};
use varpulis_core::decode::EventDecoder;
use varpulis_core::Value;
use varpulis_runtime::event_file::EventFileParser;

fn parse_one(line: &str) -> varpulis_runtime::Event {
    let mut events = EventFileParser::parse(line).unwrap_or_else(|e| panic!("{line}: {e}"));
    assert_eq!(events.len(), 1);
    events.remove(0).event
}

#[test]
fn a_flat_json_line_keeps_its_fields_and_its_epoch_time() {
    let event = parse_one(
        r#"{"event_type": "Order", "ts": 1000, "id": "o1", "shipping": {"country": "FR"}, "items": [1, 2]}"#,
    );
    assert_eq!(&*event.event_type, "Order");
    assert_eq!(
        event.timestamp,
        DateTime::<Utc>::from_timestamp_millis(1000).unwrap()
    );
    assert_eq!(event.get("id"), Some(&Value::str("o1")));
    match event.get("shipping") {
        Some(Value::Map(m)) => assert_eq!(m.get("country"), Some(&Value::str("FR"))),
        other => panic!("nested objects stay maps: {other:?}"),
    }
    assert!(matches!(event.get("items"), Some(Value::Array(a)) if a.len() == 2));
    assert!(event.get("event_type").is_none(), "the type is not a field");
}

#[test]
fn the_time_keys_have_the_connectors_priority() {
    let at_wins = parse_one(
        r#"{"event_type": "T", "@timestamp": "1970-01-01T01:00:00Z", "ts": 5000, "timestamp": 6000}"#,
    );
    assert_eq!(
        at_wins.timestamp,
        DateTime::<Utc>::from_timestamp_millis(3_600_000).unwrap()
    );
    let ts_wins = parse_one(r#"{"event_type": "T", "ts": 5000, "timestamp": 6000}"#);
    assert_eq!(
        ts_wins.timestamp,
        DateTime::<Utc>::from_timestamp_millis(5000).unwrap()
    );
    let timestamp = parse_one(r#"{"event_type": "T", "timestamp": 6000}"#);
    assert_eq!(
        timestamp.timestamp,
        DateTime::<Utc>::from_timestamp_millis(6000).unwrap()
    );
}

#[test]
fn the_native_wrapper_still_unwraps_its_data_object() {
    let event = parse_one(
        r#"{"event_type": "Tick", "ts": 1000, "data": {"price": 100.5, "symbol": "AAPL"}}"#,
    );
    assert_eq!(event.get("symbol"), Some(&Value::str("AAPL")));
    assert_eq!(event.get_float("price"), Some(100.5));
    assert_eq!(
        event.timestamp,
        DateTime::<Utc>::from_timestamp_millis(1000).unwrap()
    );
}

#[test]
fn a_flat_line_decodes_as_the_connector_decoder_decodes_it() {
    let line = r#"{"event_type": "Order", "ts": 1000, "timestamp": 2000, "@timestamp": "2026-01-01T00:00:00Z", "id": "o1", "n": 2, "ok": true, "x": null, "shipping": {"country": "FR"}}"#;
    let replay = parse_one(line);
    let live = EventDecoder::new()
        .decode("Order", line.as_bytes())
        .unwrap();
    assert_eq!(replay.event_type, live.event_type);
    assert_eq!(replay.timestamp, live.timestamp);
    let mut replay_fields: Vec<(String, Value)> = replay
        .data
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    let mut live_fields: Vec<(String, Value)> = live
        .data
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    replay_fields.sort_by(|a, b| a.0.cmp(&b.0));
    live_fields.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(replay_fields, live_fields);
}

#[test]
fn a_timing_prefix_still_overrides_the_payloads_time() {
    let events = EventFileParser::parse(r#"@5s {"event_type": "T", "ts": 1000}"#).unwrap();
    assert_eq!(
        events[0].event.timestamp,
        DateTime::<Utc>::from_timestamp_millis(5000).unwrap()
    );
}
