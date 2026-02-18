#![no_main]
use libfuzzer_sys::fuzz_target;

const MAX_DEPTH: usize = 32;

/// Fuzz the MQTT/JSON message parsing path.
///
/// This exercises the same JSON deserialization and Value conversion
/// that the MQTT connector uses when receiving messages.
fuzz_target!(|data: &[u8]| {
    if let Ok(payload) = std::str::from_utf8(data) {
        // Try to parse as JSON (mirrors what MQTT connector does)
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(payload) {
            // Exercise the JSON-to-Value conversion path
            json_to_value(&json, MAX_DEPTH);
        }

        // Also exercise the EventFileParser JSONL path
        let _ = varpulis_runtime::event_file::EventFileParser::parse_line(payload);
    }
});

fn json_to_value(v: &serde_json::Value, depth: usize) -> varpulis_core::Value {
    if depth == 0 {
        return varpulis_core::Value::Null;
    }
    match v {
        serde_json::Value::Null => varpulis_core::Value::Null,
        serde_json::Value::Bool(b) => varpulis_core::Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                varpulis_core::Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                varpulis_core::Value::Float(f)
            } else {
                varpulis_core::Value::Null
            }
        }
        serde_json::Value::String(s) => varpulis_core::Value::Str(s.clone().into()),
        serde_json::Value::Array(arr) => {
            varpulis_core::Value::array(arr.iter().map(|v| json_to_value(v, depth - 1)).collect())
        }
        serde_json::Value::Object(_) => varpulis_core::Value::Null,
    }
}
