//! # Varpulis Engine WASM
//!
//! Full VPL engine compiled to WebAssembly.  Parse VPL programs, process events,
//! and receive output events — all in the browser or any JS/TS runtime.
//!
//! ## Usage (JavaScript/TypeScript)
//!
//! ```javascript
//! import { WasmEngine } from '@varpulis/engine';
//!
//! const engine = new WasmEngine();
//! engine.load(`
//!   event Sensor:
//!     temperature: float
//!     zone: str
//!
//!   stream Hot = Sensor
//!     .where(temperature > 100)
//!     .emit(zone: zone, temp: temperature)
//! `);
//!
//! const outputs = engine.processEvent('{"event_type":"Sensor","temperature":150,"zone":"A"}');
//! // outputs: [{"stream":"Hot","event":{"zone":"A","temp":150},"timestamp":"..."}]
//! ```

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

use varpulis_runtime::engine::graph::program_to_graph;
use varpulis_runtime::Engine;

/// WASM-accessible VPL engine.
///
/// Create with `new WasmEngine()`, load VPL via `load()`, process events via
/// `processEvent()` or `processBatch()`, and receive output events as JSON.
#[wasm_bindgen]
pub struct WasmEngine {
    engine: Engine,
    program_source: String,
    program_loaded: bool,
}

/// JSON result for load/reload operations.
#[derive(Serialize)]
struct LoadResult {
    ok: bool,
    streams: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// JSON result for event processing.
#[derive(Serialize)]
struct OutputEvent {
    stream: String,
    event: serde_json::Value,
    timestamp: String,
}

/// JSON result for processing.
#[derive(Serialize)]
struct ProcessResult {
    ok: bool,
    outputs: Vec<OutputEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[wasm_bindgen]
impl WasmEngine {
    /// Create a new engine instance.
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            engine: Engine::new_sync(),
            program_source: String::new(),
            program_loaded: false,
        }
    }

    /// Load a VPL program.  Returns JSON: `{ ok, streams, error? }`.
    ///
    /// Replaces any previously loaded program.  Call this before processing events.
    pub fn load(&mut self, vpl: &str) -> String {
        let program = match varpulis_parser::parse(vpl) {
            Ok(p) => p,
            Err(e) => {
                return json(&LoadResult {
                    ok: false,
                    streams: vec![],
                    error: Some(format!("Parse error: {e}")),
                });
            }
        };

        match self.engine.load(&program) {
            Ok(()) => {
                self.program_source = vpl.to_string();
                self.program_loaded = true;
                let streams = self
                    .engine
                    .stream_names()
                    .into_iter()
                    .map(String::from)
                    .collect();
                json(&LoadResult {
                    ok: true,
                    streams,
                    error: None,
                })
            }
            Err(e) => json(&LoadResult {
                ok: false,
                streams: vec![],
                error: Some(format!("Load error: {e}")),
            }),
        }
    }

    /// Process a single event (JSON object).  Returns JSON: `{ ok, outputs, error? }`.
    ///
    /// The event JSON must include an `event_type` field matching a declared event type.
    /// Additional fields are the event data.
    ///
    /// ```json
    /// {"event_type": "Sensor", "temperature": 150, "zone": "A"}
    /// ```
    #[wasm_bindgen(js_name = "processEvent")]
    pub fn process_event(&mut self, event_json: &str) -> String {
        let event = match parse_event(event_json) {
            Ok(e) => e,
            Err(msg) => {
                return json(&ProcessResult {
                    ok: false,
                    outputs: vec![],
                    error: Some(msg),
                });
            }
        };

        match self.engine.process_batch_sync_collect(vec![event]) {
            Ok(outputs) => {
                let output_events = outputs.into_iter().map(event_to_output).collect();
                json(&ProcessResult {
                    ok: true,
                    outputs: output_events,
                    error: None,
                })
            }
            Err(e) => json(&ProcessResult {
                ok: false,
                outputs: vec![],
                error: Some(format!("Processing error: {e}")),
            }),
        }
    }

    /// Process a batch of events (JSON array of objects).  Returns JSON: `{ ok, outputs, error? }`.
    #[wasm_bindgen(js_name = "processBatch")]
    pub fn process_batch(&mut self, events_json: &str) -> String {
        let events: Vec<serde_json::Value> = match serde_json::from_str(events_json) {
            Ok(v) => v,
            Err(e) => {
                return json(&ProcessResult {
                    ok: false,
                    outputs: vec![],
                    error: Some(format!("Invalid JSON array: {e}")),
                });
            }
        };

        let parsed: Result<Vec<_>, _> = events
            .iter()
            .map(|v| {
                let s = serde_json::to_string(v).unwrap_or_default();
                parse_event(&s)
            })
            .collect();

        let events = match parsed {
            Ok(e) => e,
            Err(msg) => {
                return json(&ProcessResult {
                    ok: false,
                    outputs: vec![],
                    error: Some(msg),
                });
            }
        };

        match self.engine.process_batch_sync_collect(events) {
            Ok(outputs) => {
                let output_events = outputs.into_iter().map(event_to_output).collect();
                json(&ProcessResult {
                    ok: true,
                    outputs: output_events,
                    error: None,
                })
            }
            Err(e) => json(&ProcessResult {
                ok: false,
                outputs: vec![],
                error: Some(format!("Processing error: {e}")),
            }),
        }
    }

    /// Get the pipeline topology as a JSON graph.
    #[wasm_bindgen(js_name = "getTopology")]
    pub fn get_topology(&self) -> String {
        if self.program_source.is_empty() {
            return r#"{"nodes":[],"edges":[]}"#.to_string();
        }
        match varpulis_parser::parse(&self.program_source) {
            Ok(program) => {
                let graph = program_to_graph(&program);
                serde_json::to_string(&graph)
                    .unwrap_or_else(|_| r#"{"nodes":[],"edges":[]}"#.into())
            }
            Err(_) => r#"{"nodes":[],"edges":[]}"#.to_string(),
        }
    }

    /// Get loaded stream names as a JSON array.
    #[wasm_bindgen(js_name = "getStreams")]
    pub fn get_streams(&self) -> String {
        let streams: Vec<&str> = self.engine.stream_names();
        serde_json::to_string(&streams).unwrap_or_else(|_| "[]".into())
    }

    /// Enable or disable trace mode.
    #[wasm_bindgen(js_name = "setTrace")]
    pub fn set_trace(&mut self, enabled: bool) {
        self.engine.set_trace_enabled(enabled);
    }

    /// Drain trace entries as a JSON array.
    #[wasm_bindgen(js_name = "drainTrace")]
    pub fn drain_trace(&mut self) -> String {
        let entries = self.engine.drain_trace();
        let json_entries: Vec<serde_json::Value> = entries
            .into_iter()
            .map(|e| serde_json::to_value(&format!("{e:?}")).unwrap_or_default())
            .collect();
        serde_json::to_string(&json_entries).unwrap_or_else(|_| "[]".into())
    }

    /// Validate VPL syntax without loading.  Returns JSON: `{ ok, error? }`.
    pub fn validate(&self, vpl: &str) -> String {
        match varpulis_parser::parse(vpl) {
            Ok(_) => r#"{"ok":true}"#.to_string(),
            Err(e) => {
                let error = format!("{e}");
                json(&serde_json::json!({"ok": false, "error": error}))
            }
        }
    }

    /// Check if a program is loaded.
    #[wasm_bindgen(js_name = "isLoaded")]
    pub fn is_loaded(&self) -> bool {
        self.program_loaded
    }
}

// =============================================================================
// Helpers
// =============================================================================

fn json<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value)
        .unwrap_or_else(|_| r#"{"ok":false,"error":"serialization failed"}"#.into())
}

fn parse_event(json_str: &str) -> Result<varpulis_core::Event, String> {
    let value: serde_json::Value =
        serde_json::from_str(json_str).map_err(|e| format!("Invalid JSON: {e}"))?;

    let event_type = value
        .get("event_type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Missing 'event_type' field".to_string())?;

    let mut event = varpulis_core::Event::new(event_type);

    if let Some(obj) = value.as_object() {
        for (key, val) in obj {
            if key == "event_type" || key == "timestamp" {
                continue;
            }
            if let Some(v) = json_value_to_core_value(val) {
                event = event.with_field(key.as_str(), v);
            }
        }
    }

    Ok(event)
}

fn json_value_to_core_value(v: &serde_json::Value) -> Option<varpulis_core::Value> {
    match v {
        serde_json::Value::Null => Some(varpulis_core::Value::Null),
        serde_json::Value::Bool(b) => Some(varpulis_core::Value::Bool(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(varpulis_core::Value::Int(i))
            } else if let Some(f) = n.as_f64() {
                Some(varpulis_core::Value::Float(f))
            } else {
                None
            }
        }
        serde_json::Value::String(s) => Some(varpulis_core::Value::str(s)),
        _ => None,
    }
}

fn event_to_output(event: varpulis_core::Event) -> OutputEvent {
    let mut fields = serde_json::Map::new();
    for (key, value) in &event.data {
        fields.insert(key.to_string(), core_value_to_json(value));
    }
    OutputEvent {
        stream: event.event_type.to_string(),
        event: serde_json::Value::Object(fields),
        timestamp: event.timestamp.to_rfc3339(),
    }
}

fn core_value_to_json(v: &varpulis_core::Value) -> serde_json::Value {
    match v {
        varpulis_core::Value::Null => serde_json::Value::Null,
        varpulis_core::Value::Bool(b) => serde_json::Value::Bool(*b),
        varpulis_core::Value::Int(i) => serde_json::json!(*i),
        varpulis_core::Value::Float(f) => serde_json::json!(*f),
        varpulis_core::Value::Str(s) => serde_json::Value::String(s.to_string()),
        _ => serde_json::Value::String(format!("{v:?}")),
    }
}

// =============================================================================
// Tests (run natively, not in WASM)
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_engine() {
        let engine = WasmEngine::new();
        assert!(!engine.is_loaded());
        assert_eq!(engine.get_streams(), "[]");
    }

    #[test]
    fn test_load_valid_vpl() {
        let mut engine = WasmEngine::new();
        let result = engine.load("event T:\n    x: int\n\nstream S = T .where(x > 10) .emit(v: x)");
        let json: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(json["ok"], true);
        assert!(json["streams"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("S")));
        assert!(engine.is_loaded());
    }

    #[test]
    fn test_load_invalid_vpl() {
        let mut engine = WasmEngine::new();
        let result = engine.load("this is not valid vpl {{{{");
        let json: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(json["ok"], false);
        assert!(json["error"].as_str().unwrap().contains("Parse error"));
    }

    #[test]
    fn test_process_event_matching() {
        let mut engine = WasmEngine::new();
        engine.load("event T:\n    x: int\n\nstream S = T .where(x > 10) .emit(v: x)");
        let result = engine.process_event(r#"{"event_type":"T","x":42}"#);
        let json: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(json["ok"], true);
        let outputs = json["outputs"].as_array().unwrap();
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0]["stream"], "S");
        assert_eq!(outputs[0]["event"]["v"], 42);
    }

    #[test]
    fn test_process_event_filtered() {
        let mut engine = WasmEngine::new();
        engine.load("event T:\n    x: int\n\nstream S = T .where(x > 10) .emit(v: x)");
        let result = engine.process_event(r#"{"event_type":"T","x":5}"#);
        let json: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(json["ok"], true);
        assert!(json["outputs"].as_array().unwrap().is_empty());
    }

    #[test]
    fn test_process_batch() {
        let mut engine = WasmEngine::new();
        engine.load("event T:\n    x: int\n\nstream S = T .where(x > 10) .emit(v: x)");
        let result = engine.process_batch(
            r#"[{"event_type":"T","x":5},{"event_type":"T","x":42},{"event_type":"T","x":100}]"#,
        );
        let json: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(json["ok"], true);
        assert_eq!(json["outputs"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_get_topology() {
        let mut engine = WasmEngine::new();
        engine.load("event T:\n    x: int\n\nstream S = T .where(x > 10) .emit(v: x)");
        let topo = engine.get_topology();
        let json: serde_json::Value = serde_json::from_str(&topo).unwrap();
        assert!(!json["nodes"].as_array().unwrap().is_empty());
        assert!(!json["edges"].as_array().unwrap().is_empty());
    }

    #[test]
    fn test_validate() {
        let engine = WasmEngine::new();
        let valid = engine.validate("event T:\n    x: int");
        let json: serde_json::Value = serde_json::from_str(&valid).unwrap();
        assert_eq!(json["ok"], true);

        let invalid = engine.validate("not valid {{");
        let json: serde_json::Value = serde_json::from_str(&invalid).unwrap();
        assert_eq!(json["ok"], false);
    }
}
