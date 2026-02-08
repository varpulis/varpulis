//! Helper functions for JSON-to-Event conversion

use crate::event::Event;
use indexmap::IndexMap;
use rustc_hash::FxBuildHasher;

/// Convert JSON value to Event
pub(crate) fn json_to_event(event_type: &str, json: &serde_json::Value) -> Event {
    let mut event = Event::new(event_type);

    if let Some(obj) = json.as_object() {
        for (key, value) in obj {
            if key != "event_type" {
                if let Some(v) = json_to_value(value) {
                    event = event.with_field(key.as_str(), v);
                }
            }
        }
    }

    event
}

/// Convert serde_json::Value to varpulis Value
pub(crate) fn json_to_value(json: &serde_json::Value) -> Option<varpulis_core::Value> {
    use varpulis_core::Value;

    match json {
        serde_json::Value::Null => Some(Value::Null),
        serde_json::Value::Bool(b) => Some(Value::Bool(*b)),
        serde_json::Value::Number(n) => n
            .as_i64()
            .map(Value::Int)
            .or_else(|| n.as_f64().map(Value::Float)),
        serde_json::Value::String(s) => Some(Value::Str(s.clone().into())),
        serde_json::Value::Array(arr) => {
            let values: Vec<Value> = arr.iter().filter_map(json_to_value).collect();
            Some(Value::array(values))
        }
        serde_json::Value::Object(obj) => {
            let mut map: IndexMap<std::sync::Arc<str>, Value, FxBuildHasher> =
                IndexMap::with_hasher(FxBuildHasher);
            for (key, value) in obj {
                if let Some(v) = json_to_value(value) {
                    map.insert(key.as_str().into(), v);
                }
            }
            Some(Value::map(map))
        }
    }
}
