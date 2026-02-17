//! Helper functions for JSON-to-Event conversion with resource limits.

use crate::event::Event;
use crate::limits;
use indexmap::IndexMap;
use rustc_hash::FxBuildHasher;
use tracing::warn;

/// Convert JSON value to Event with resource limits enforced.
pub(crate) fn json_to_event(event_type: &str, json: &serde_json::Value) -> Event {
    let mut event = Event::new(event_type);

    if let Some(obj) = json.as_object() {
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

/// Convert serde_json::Value to varpulis Value with depth and size limits.
pub(crate) fn json_to_value(json: &serde_json::Value) -> Option<varpulis_core::Value> {
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
                // Truncate at char boundary
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
