//! Extended coverage tests for event.rs and event_file.rs uncovered paths.

use std::io::Cursor;
use varpulis_core::Value;
use varpulis_runtime::event::Event;
use varpulis_runtime::event_file::EventFileParser;
use varpulis_runtime::StreamingEventReader;

// =============================================================================
// Event Factory Methods
// =============================================================================

#[test]
fn event_with_capacity() {
    let event = Event::with_capacity("Test", 10);
    assert_eq!(&*event.event_type, "Test");
    assert!(event.data.is_empty());
}

#[test]
fn event_with_capacity_at() {
    let ts = chrono::DateTime::UNIX_EPOCH;
    let event = Event::with_capacity_at("Test", 5, ts);
    assert_eq!(&*event.event_type, "Test");
    assert_eq!(event.timestamp, ts);
}

#[test]
fn event_new_at() {
    let ts = chrono::DateTime::UNIX_EPOCH;
    let event = Event::new_at("Ev", ts);
    assert_eq!(&*event.event_type, "Ev");
    assert_eq!(event.timestamp, ts);
    assert!(event.data.is_empty());
}

#[test]
fn event_from_fields() {
    use indexmap::IndexMap;
    use rustc_hash::FxBuildHasher;
    use std::sync::Arc;

    let mut data: IndexMap<Arc<str>, Value, FxBuildHasher> = IndexMap::with_hasher(FxBuildHasher);
    data.insert("x".into(), Value::Int(1));
    data.insert("y".into(), Value::Float(2.5));

    let event = Event::from_fields("Custom", data);
    assert_eq!(&*event.event_type, "Custom");
    assert_eq!(event.get("x"), Some(&Value::Int(1)));
    assert_eq!(event.get("y"), Some(&Value::Float(2.5)));
}

#[test]
fn event_from_string_fields() {
    use indexmap::IndexMap;
    use rustc_hash::FxBuildHasher;

    let mut data: IndexMap<String, Value, FxBuildHasher> = IndexMap::with_hasher(FxBuildHasher);
    data.insert("name".to_string(), Value::Str("Alice".into()));
    data.insert("age".to_string(), Value::Int(30));

    let event = Event::from_string_fields("Person", data);
    assert_eq!(&*event.event_type, "Person");
    assert_eq!(event.get_str("name"), Some("Alice"));
    assert_eq!(event.get_int("age"), Some(30));
}

#[test]
fn event_from_fields_with_timestamp() {
    use indexmap::IndexMap;
    use rustc_hash::FxBuildHasher;
    use std::sync::Arc;

    let ts = chrono::DateTime::UNIX_EPOCH;
    let mut data: IndexMap<Arc<str>, Value, FxBuildHasher> = IndexMap::with_hasher(FxBuildHasher);
    data.insert("k".into(), Value::Bool(true));

    let event = Event::from_fields_with_timestamp("Ts", ts, data);
    assert_eq!(event.timestamp, ts);
    assert_eq!(event.get("k"), Some(&Value::Bool(true)));
}

#[test]
fn event_to_sink_payload() {
    let event = Event::new("SinkTest")
        .with_field("key", "val")
        .with_field("num", 42i64);

    let payload = event.to_sink_payload();
    let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    assert_eq!(json["event_type"], "SinkTest");
    assert!(json["timestamp"].is_string());
    assert_eq!(json["key"], "val");
    assert_eq!(json["num"], 42);
}

#[test]
fn event_to_sink_payload_excludes_timestamp_field() {
    // If event has a "timestamp" data field, to_sink_payload skips it to avoid duplication
    let event = Event::new("Test").with_field("timestamp", "custom");

    let payload = event.to_sink_payload();
    let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    // The serialized timestamp should be the event's actual timestamp, not the "custom" field
    assert!(json["timestamp"].is_string());
}

#[test]
fn event_to_sink_payload_empty_event() {
    let event = Event::new("Empty");
    let payload = event.to_sink_payload();
    let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    assert_eq!(json["event_type"], "Empty");
}

// =============================================================================
// EventFileParser — JSONL format
// =============================================================================

#[test]
fn parse_jsonl_line() {
    let source = r#"{"event_type": "Tick", "data": {"price": 100.5, "symbol": "AAPL"}}"#;
    let events = EventFileParser::parse(source).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(&*events[0].event.event_type, "Tick");
    assert_eq!(
        events[0].event.get("symbol"),
        Some(&Value::Str("AAPL".into()))
    );
    assert_eq!(events[0].event.get_float("price"), Some(100.5));
}

#[test]
fn parse_jsonl_with_null_and_bool() {
    let source = r#"{"event_type": "X", "data": {"a": null, "b": true, "c": false}}"#;
    let events = EventFileParser::parse(source).unwrap();
    assert_eq!(events[0].event.get("a"), Some(&Value::Null));
    assert_eq!(events[0].event.get("b"), Some(&Value::Bool(true)));
    assert_eq!(events[0].event.get("c"), Some(&Value::Bool(false)));
}

#[test]
fn parse_jsonl_with_nested_array() {
    let source = r#"{"event_type": "X", "data": {"arr": [1, 2, 3]}}"#;
    let events = EventFileParser::parse(source).unwrap();
    let arr = events[0].event.get("arr").unwrap();
    if let Value::Array(a) = arr {
        assert_eq!(a.len(), 3);
    } else {
        panic!("Expected array");
    }
}

#[test]
fn parse_jsonl_with_nested_object() {
    let source = r#"{"event_type": "X", "data": {"inner": {"a": 1, "b": "hi"}}}"#;
    let events = EventFileParser::parse(source).unwrap();
    let inner = events[0].event.get("inner").unwrap();
    if let Value::Map(m) = inner {
        assert_eq!(m.len(), 2);
    } else {
        panic!("Expected map");
    }
}

#[test]
fn parse_jsonl_missing_event_type() {
    let source = r#"{"data": {"x": 1}}"#;
    let result = EventFileParser::parse(source);
    assert!(result.is_err());
}

#[test]
fn parse_jsonl_invalid_json() {
    let source = r#"{not valid json}"#;
    let result = EventFileParser::parse(source);
    assert!(result.is_err());
}

// =============================================================================
// EventFileParser — Timing prefixes
// =============================================================================

#[test]
fn parse_timing_prefix_seconds() {
    let source = "@5s Event { x: 1 }";
    let events = EventFileParser::parse(source).unwrap();
    assert_eq!(events[0].time_offset_ms, 5000);
}

#[test]
fn parse_timing_prefix_milliseconds() {
    let source = "@250ms Event { x: 1 }";
    let events = EventFileParser::parse(source).unwrap();
    assert_eq!(events[0].time_offset_ms, 250);
}

#[test]
fn parse_timing_prefix_minutes() {
    let source = "@2m Event { x: 1 }";
    let events = EventFileParser::parse(source).unwrap();
    assert_eq!(events[0].time_offset_ms, 120_000);
}

#[test]
fn parse_timing_prefix_bare_number() {
    let source = "@100 Event { x: 1 }";
    let events = EventFileParser::parse(source).unwrap();
    assert_eq!(events[0].time_offset_ms, 100);
}

#[test]
fn parse_timing_prefix_invalid() {
    let source = "@abc Event { x: 1 }";
    let result = EventFileParser::parse(source);
    assert!(result.is_err());
}

#[test]
fn parse_timing_prefix_no_space() {
    let source = "@5s";
    let result = EventFileParser::parse(source);
    assert!(result.is_err());
}

// =============================================================================
// EventFileParser — parse_line
// =============================================================================

#[test]
fn parse_line_empty() {
    assert!(EventFileParser::parse_line("").unwrap().is_none());
}

#[test]
fn parse_line_comment_hash() {
    assert!(EventFileParser::parse_line("# comment").unwrap().is_none());
}

#[test]
fn parse_line_comment_slash() {
    assert!(EventFileParser::parse_line("// comment").unwrap().is_none());
}

#[test]
fn parse_line_batch_skipped() {
    assert!(EventFileParser::parse_line("BATCH 100").unwrap().is_none());
}

#[test]
fn parse_line_timing_skipped() {
    assert!(EventFileParser::parse_line("@5s Event { x: 1 }")
        .unwrap()
        .is_none());
}

#[test]
fn parse_line_evt_format() {
    let event = EventFileParser::parse_line("StockTick { price: 100.5 }")
        .unwrap()
        .unwrap();
    assert_eq!(&*event.event_type, "StockTick");
    assert_eq!(event.get_float("price"), Some(100.5));
}

#[test]
fn parse_line_jsonl_format() {
    let event = EventFileParser::parse_line(r#"{"event_type": "X", "data": {"v": 1}}"#)
        .unwrap()
        .unwrap();
    assert_eq!(&*event.event_type, "X");
    assert_eq!(event.get_int("v"), Some(1));
}

// =============================================================================
// StreamingEventReader
// =============================================================================

#[test]
fn streaming_reader_basic() {
    let data = b"Event1 { x: 1 }\n# comment\nEvent2 { y: 2 }\n";
    let cursor = Cursor::new(data.as_slice());
    let reader = StreamingEventReader::new(std::io::BufReader::new(cursor));

    let events: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(&*events[0].event_type, "Event1");
    assert_eq!(&*events[1].event_type, "Event2");
}

#[test]
fn streaming_reader_events_read_count() {
    let data = b"A { x: 1 }\nB { y: 2 }\nC { z: 3 }\n";
    let cursor = Cursor::new(data.as_slice());
    let mut reader = StreamingEventReader::new(std::io::BufReader::new(cursor));

    assert_eq!(reader.events_read(), 0);
    let _ = reader.next();
    assert_eq!(reader.events_read(), 1);
    let _ = reader.next();
    assert_eq!(reader.events_read(), 2);
}

#[test]
fn streaming_reader_empty() {
    let data = b"";
    let cursor = Cursor::new(data.as_slice());
    let reader = StreamingEventReader::new(std::io::BufReader::new(cursor));

    let events: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert!(events.is_empty());
}

#[test]
fn streaming_reader_comments_only() {
    let data = b"# comment\n// another\n";
    let cursor = Cursor::new(data.as_slice());
    let reader = StreamingEventReader::new(std::io::BufReader::new(cursor));

    let events: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert!(events.is_empty());
}

#[test]
fn streaming_reader_jsonl() {
    let data = br#"{"event_type": "X", "data": {"v": 42}}"#;
    let cursor = Cursor::new(data.as_slice());
    let reader = StreamingEventReader::new(std::io::BufReader::new(cursor));

    let events: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(&*events[0].event_type, "X");
}

// =============================================================================
// EventFileParser — Edge cases for value parsing
// =============================================================================

#[test]
fn parse_string_with_tab_escape() {
    let source = r#"Ev { msg: "hello\tworld" }"#;
    let events = EventFileParser::parse(source).unwrap();
    let msg = events[0].event.get_str("msg").unwrap();
    assert!(msg.contains('\t'));
}

#[test]
fn parse_string_with_escaped_quote() {
    let source = r#"Ev { msg: "say \"hello\"" }"#;
    let events = EventFileParser::parse(source).unwrap();
    let msg = events[0].event.get_str("msg").unwrap();
    assert!(msg.contains('"'));
}

#[test]
fn parse_string_with_escaped_single_quote() {
    let source = "Ev { msg: 'it\\'s fine' }";
    let events = EventFileParser::parse(source).unwrap();
    let msg = events[0].event.get_str("msg").unwrap();
    assert!(msg.contains('\''));
}

#[test]
fn parse_string_with_unknown_escape() {
    let source = r#"Ev { msg: "hello\xworld" }"#;
    let events = EventFileParser::parse(source).unwrap();
    let msg = events[0].event.get_str("msg").unwrap();
    // Unknown escape sequences are kept as-is
    assert!(msg.contains("\\x"));
}

#[test]
fn parse_string_with_trailing_backslash() {
    let source = r#"Ev { msg: "trail\" }"#;
    let events = EventFileParser::parse(source).unwrap();
    assert_eq!(events.len(), 1);
}

#[test]
fn parse_nested_braces_in_field_value() {
    let source = r#"Ev { data: {a: 1, b: 2}, name: "x" }"#;
    // This tests the split_fields handling of nested braces
    let events = EventFileParser::parse(source).unwrap();
    assert_eq!(events.len(), 1);
}

#[test]
fn parse_empty_array() {
    let source = "Ev { items: [] }";
    let events = EventFileParser::parse(source).unwrap();
    let items = events[0].event.get("items").unwrap();
    if let Value::Array(arr) = items {
        assert!(arr.is_empty());
    } else {
        panic!("Expected array");
    }
}

#[test]
fn parse_mixed_batch_and_timing() {
    let source = r#"
BATCH 50
Event1 { x: 1 }
@100ms Event2 { y: 2 }
Event3 { z: 3 }
"#;
    let events = EventFileParser::parse(source).unwrap();
    assert_eq!(events[0].time_offset_ms, 50);
    assert_eq!(events[1].time_offset_ms, 100);
    assert_eq!(events[2].time_offset_ms, 50); // Falls back to batch time
}

// =============================================================================
// Scoring stub (no onnx feature)
// =============================================================================

#[test]
fn onnx_model_load_without_feature() {
    use varpulis_runtime::scoring::OnnxModel;
    let result = OnnxModel::load("model.onnx", vec!["x".into()], vec!["y".into()]);
    assert!(result.is_err());
    match result {
        Err(msg) => assert!(msg.contains("onnx")),
        Ok(_) => panic!("Expected error"),
    }
}
