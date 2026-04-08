//! Helper functions for JSON-to-Event conversion with resource limits.

use indexmap::IndexMap;
use rustc_hash::FxBuildHasher;
use tracing::warn;
use varpulis_core::Event;

use crate::limits;

/// Convert JSON value to Event with resource limits enforced.
///
/// The resulting event's `timestamp` field is populated from the first
/// available of `@timestamp` (RFC3339 string), `ts` (integer epoch
/// milliseconds), or `timestamp` (integer epoch milliseconds). This is
/// critical for time-based windows: without an event-time timestamp,
/// Kafka-sourced events all get wall-clock time and collapse into a
/// single window.
pub fn json_to_event(event_type: &str, json: &serde_json::Value) -> Event {
    let mut event = Event::new(event_type);

    if let Some(obj) = json.as_object() {
        // Extract event time from well-known fields. Tried in the order
        // `@timestamp` > `ts` > `timestamp` so that explicit ISO8601
        // tagging wins over generator-specific integer fields.
        if let Some(ts) = extract_event_time(obj) {
            event.timestamp = ts;
        }

        let mut field_count = 0;
        for (key, value) in obj {
            if key != "event_type" {
                if field_count >= limits::MAX_FIELDS_PER_EVENT {
                    warn!(
                        event_type,
                        "Event exceeded max field count ({}), remaining fields dropped",
                        limits::MAX_FIELDS_PER_EVENT
                    );
                    break;
                }
                if let Some(v) = json_to_value(value) {
                    event = event.with_field(key.as_str(), v);
                    field_count += 1;
                }
            }
        }
    }

    event
}

/// Try to extract an event-time timestamp from a JSON object's well-known
/// fields. Returns `None` if none of the candidate fields are present or
/// parseable. Used by [`json_to_event`] to seed `Event::timestamp` so
/// time-based windows advance.
fn extract_event_time(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Option<chrono::DateTime<chrono::Utc>> {
    // `@timestamp` as RFC3339 / ISO8601 string (Sysmon / Varpulis native).
    if let Some(s) = obj.get("@timestamp").and_then(|v| v.as_str()) {
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
            return Some(dt.with_timezone(&chrono::Utc));
        }
    }

    // `ts` or `timestamp` as integer epoch milliseconds. This is what our
    // benchmark event generators emit and what most internal pipelines
    // use. We treat the integer as milliseconds because that's the JVM /
    // JavaScript convention and matches Arroyo's `TO_TIMESTAMP_MILLIS`
    // default.
    for key in &["ts", "timestamp"] {
        if let Some(n) = obj.get(*key).and_then(|v| v.as_i64()) {
            if let Some(dt) = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(n) {
                return Some(dt);
            }
        }
    }

    None
}

/// Convert `serde_json::Value` to varpulis `Value` with depth and size limits.
pub fn json_to_value(json: &serde_json::Value) -> Option<varpulis_core::Value> {
    json_to_value_bounded(json, limits::MAX_JSON_DEPTH)
}

/// Depth-bounded JSON to Value conversion. Returns None if depth or size limits exceeded.
fn json_to_value_bounded(json: &serde_json::Value, depth: usize) -> Option<varpulis_core::Value> {
    use varpulis_core::Value;

    if depth == 0 {
        return None;
    }

    match json {
        serde_json::Value::Null => Some(Value::Null),
        serde_json::Value::Bool(b) => Some(Value::Bool(*b)),
        serde_json::Value::Number(n) => n
            .as_i64()
            .map(Value::Int)
            .or_else(|| n.as_f64().map(Value::Float)),
        serde_json::Value::String(s) => {
            if s.len() > limits::MAX_STRING_VALUE_BYTES {
                warn!(
                    len = s.len(),
                    "String value exceeds max size ({}), truncated",
                    limits::MAX_STRING_VALUE_BYTES
                );
                let truncated = &s[..s.floor_char_boundary(limits::MAX_STRING_VALUE_BYTES)];
                Some(Value::Str(truncated.into()))
            } else {
                Some(Value::Str(s.clone().into()))
            }
        }
        serde_json::Value::Array(arr) => {
            let capped_len = arr.len().min(limits::MAX_ARRAY_ELEMENTS);
            if arr.len() > limits::MAX_ARRAY_ELEMENTS {
                warn!(
                    len = arr.len(),
                    "Array exceeds max elements ({}), truncated",
                    limits::MAX_ARRAY_ELEMENTS
                );
            }
            let values: Vec<Value> = arr
                .iter()
                .take(capped_len)
                .filter_map(|v| json_to_value_bounded(v, depth - 1))
                .collect();
            Some(Value::array(values))
        }
        serde_json::Value::Object(obj) => {
            let mut map: IndexMap<std::sync::Arc<str>, Value, FxBuildHasher> =
                IndexMap::with_hasher(FxBuildHasher);
            let mut count = 0;
            for (key, value) in obj {
                if count >= limits::MAX_FIELDS_PER_EVENT {
                    break;
                }
                if let Some(v) = json_to_value_bounded(value, depth - 1) {
                    map.insert(key.as_str().into(), v);
                    count += 1;
                }
            }
            Some(Value::map(map))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_to_event_picks_up_ts_epoch_millis() {
        let json = serde_json::json!({
            "ts": 1_775_990_400_000_i64,
            "device_id": "dev_0",
            "temperature": 20.5,
        });
        let event = json_to_event("Reading", &json);
        assert_eq!(event.timestamp.timestamp_millis(), 1_775_990_400_000);
        // And the `ts` field is still preserved as a regular field so
        // user expressions like `where ts > ...` still work.
        assert!(event.data.contains_key("ts"));
    }

    #[test]
    fn json_to_event_picks_up_at_timestamp_rfc3339() {
        let json = serde_json::json!({
            "@timestamp": "2030-01-01T00:00:00Z",
            "device_id": "dev_0",
        });
        let event = json_to_event("Reading", &json);
        // 2030-01-01T00:00:00Z == 1_893_456_000 seconds since epoch
        assert_eq!(event.timestamp.timestamp(), 1_893_456_000);
    }

    #[test]
    fn json_to_event_prefers_at_timestamp_over_ts() {
        let json = serde_json::json!({
            "@timestamp": "2030-01-01T00:00:00Z",
            "ts": 1_775_990_400_000_i64, // a different time (2026-04-12)
            "device_id": "dev_0",
        });
        let event = json_to_event("Reading", &json);
        // @timestamp wins: 2030-01-01T00:00:00Z, not 2026-04-12
        assert_eq!(event.timestamp.timestamp(), 1_893_456_000);
    }

    #[test]
    fn json_to_event_no_timestamp_fields_leaves_default() {
        let json = serde_json::json!({ "device_id": "dev_0" });
        let event = json_to_event("Reading", &json);
        // Should fall back to the event's default timestamp (not panic,
        // not stay at unix epoch 0). We just assert it parsed and
        // didn't crash.
        assert!(event.timestamp.timestamp() > 0);
    }
}
