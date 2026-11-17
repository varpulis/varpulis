//! Helpers shared by the audit tests: feed a JSON payload stamped at a time
//! of day, read what came out as JSON.

#![allow(dead_code)]

use varpulis_engine::Program;

/// A payload at `time` (`HH:MM:SS`) on a fixed day, with `fields` as the
/// rest of the JSON object (`"k": "x", "n": 1`).
pub fn at(time: &str, fields: &str) -> String {
    if fields.trim().is_empty() {
        format!(r#"{{"@timestamp": "2026-10-14T{time}Z"}}"#)
    } else {
        format!(r#"{{"@timestamp": "2026-10-14T{time}Z", {fields}}}"#)
    }
}

/// Feed one payload and return every emit as JSON.
pub fn feed(program: &mut Program, event_type: &str, json: &str) -> Vec<serde_json::Value> {
    program
        .feed_json(event_type, json.as_bytes())
        .unwrap_or_else(|e| panic!("feeding {event_type} {json}: {e}"))
        .iter()
        .map(|emit| emit.to_json())
        .collect()
}

/// Everything the end of a bounded input emits.
pub fn end(program: &mut Program) -> Vec<serde_json::Value> {
    program
        .end_of_input()
        .unwrap()
        .iter()
        .map(|emit| emit.to_json())
        .collect()
}

/// The one emit a step must produce, or a panic naming what came instead.
pub fn only(emits: Vec<serde_json::Value>) -> serde_json::Value {
    assert_eq!(emits.len(), 1, "expected exactly one emit, got {emits:#?}");
    emits.into_iter().next().unwrap()
}
