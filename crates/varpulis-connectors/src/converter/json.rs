//! JSON converter — wraps existing `helpers::json_to_event()`.

use super::{Converter, ConverterError};
use crate::helpers::json_to_event;
use varpulis_core::Event;

/// JSON converter that delegates to the existing `json_to_event` helper.
pub struct JsonConverter;

impl Converter for JsonConverter {
    fn name(&self) -> &str {
        "json"
    }

    fn deserialize(&self, event_type: &str, payload: &[u8]) -> Result<Vec<Event>, ConverterError> {
        let text = std::str::from_utf8(payload)
            .map_err(|e| ConverterError::DeserializeFailed(e.to_string()))?;

        let json: serde_json::Value = serde_json::from_str(text)
            .map_err(|e| ConverterError::DeserializeFailed(e.to_string()))?;

        // Handle JSON arrays as batches
        if let serde_json::Value::Array(arr) = &json {
            let events: Vec<Event> = arr
                .iter()
                .map(|item| {
                    let et = item
                        .get("event_type")
                        .or_else(|| item.get("type"))
                        .and_then(|v| v.as_str())
                        .unwrap_or(event_type);
                    json_to_event(et, item)
                })
                .collect();
            Ok(events)
        } else {
            let et = json
                .get("event_type")
                .or_else(|| json.get("type"))
                .and_then(|v| v.as_str())
                .unwrap_or(event_type);
            Ok(vec![json_to_event(et, &json)])
        }
    }

    fn serialize(&self, event: &Event) -> Result<Vec<u8>, ConverterError> {
        let mut map = serde_json::Map::new();
        map.insert(
            "event_type".to_string(),
            serde_json::Value::String(event.event_type.to_string()),
        );

        for (key, value) in &event.data {
            if let Some(json_val) = value_to_json(value) {
                map.insert(key.to_string(), json_val);
            }
        }

        serde_json::to_vec(&serde_json::Value::Object(map))
            .map_err(|e| ConverterError::SerializeFailed(e.to_string()))
    }
}

/// Convert a varpulis Value to serde_json::Value.
fn value_to_json(value: &varpulis_core::Value) -> Option<serde_json::Value> {
    match value {
        varpulis_core::Value::Null => Some(serde_json::Value::Null),
        varpulis_core::Value::Bool(b) => Some(serde_json::Value::Bool(*b)),
        varpulis_core::Value::Int(i) => Some(serde_json::json!(*i)),
        varpulis_core::Value::Float(f) => {
            serde_json::Number::from_f64(*f).map(serde_json::Value::Number)
        }
        varpulis_core::Value::Str(s) => Some(serde_json::Value::String(s.to_string())),
        varpulis_core::Value::Timestamp(ts) => Some(serde_json::json!(*ts)),
        varpulis_core::Value::Duration(d) => Some(serde_json::json!(*d)),
        varpulis_core::Value::Array(arr) => {
            let items: Vec<serde_json::Value> = arr.iter().filter_map(value_to_json).collect();
            Some(serde_json::Value::Array(items))
        }
        varpulis_core::Value::Map(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .filter_map(|(k, v)| value_to_json(v).map(|jv| (k.to_string(), jv)))
                .collect();
            Some(serde_json::Value::Object(obj))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_deserialize_single() {
        let converter = JsonConverter;
        let payload = br#"{"event_type": "Temp", "value": 42}"#;
        let events = converter.deserialize("Default", payload).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type.as_ref(), "Temp");
    }

    #[test]
    fn test_json_deserialize_array() {
        let converter = JsonConverter;
        let payload = br#"[{"event_type": "A"}, {"event_type": "B"}]"#;
        let events = converter.deserialize("Default", payload).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type.as_ref(), "A");
        assert_eq!(events[1].event_type.as_ref(), "B");
    }

    #[test]
    fn test_json_deserialize_uses_fallback_type() {
        let converter = JsonConverter;
        let payload = br#"{"value": 42}"#;
        let events = converter.deserialize("Fallback", payload).unwrap();
        assert_eq!(events[0].event_type.as_ref(), "Fallback");
    }

    #[test]
    fn test_json_serialize_roundtrip() {
        let converter = JsonConverter;
        let event = Event::new("Test")
            .with_field("x", 42)
            .with_field("name", "hello");
        let bytes = converter.serialize(&event).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["event_type"], "Test");
        assert_eq!(json["x"], 42);
        assert_eq!(json["name"], "hello");
    }

    #[test]
    fn test_json_deserialize_invalid_utf8() {
        let converter = JsonConverter;
        let payload = &[0xFF, 0xFE];
        assert!(converter.deserialize("Test", payload).is_err());
    }

    #[test]
    fn test_json_deserialize_invalid_json() {
        let converter = JsonConverter;
        let payload = b"not json at all";
        assert!(converter.deserialize("Test", payload).is_err());
    }
}
