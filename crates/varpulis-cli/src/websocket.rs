//! WebSocket module for Varpulis CLI
//!
//! Provides WebSocket server functionality for the VS Code extension and other clients.

use futures_util::{SinkExt, StreamExt};
use indexmap::IndexMap;
use rustc_hash::FxBuildHasher;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use warp::ws::{Message, WebSocket};

use varpulis_parser::parse;
use varpulis_runtime::engine::Engine;
use varpulis_runtime::event::Event;

use crate::security;

// =============================================================================
// Message Types
// =============================================================================

/// WebSocket message types for client-server communication
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsMessage {
    // Client -> Server messages
    /// Load a VarpulisQL file
    LoadFile { path: String },
    /// Inject an event into the engine
    InjectEvent {
        event_type: String,
        data: serde_json::Value,
    },
    /// Request list of streams
    GetStreams,
    /// Request current metrics
    GetMetrics,

    // Server -> Client messages
    /// Result of loading a file
    LoadResult {
        success: bool,
        streams_loaded: usize,
        error: Option<String>,
    },
    /// List of streams
    Streams { data: Vec<StreamInfo> },
    /// Event notification
    Event {
        id: String,
        event_type: String,
        timestamp: String,
        data: serde_json::Value,
    },
    /// Output event notification
    OutputEvent {
        event_type: String,
        data: serde_json::Value,
        timestamp: String,
    },
    /// Current metrics
    Metrics {
        events_processed: u64,
        output_events_emitted: u64,
        active_streams: usize,
        uptime: f64,
        memory_usage: u64,
        cpu_usage: f64,
    },
    /// Result of event injection
    EventInjected { event_type: String, success: bool },
    /// Error message
    Error { message: String },
}

/// Information about a stream
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamInfo {
    pub name: String,
    pub source: String,
    pub operations: Vec<String>,
    pub events_per_second: f64,
    pub status: String,
}

// =============================================================================
// Server State
// =============================================================================

/// Shared state for the WebSocket server
pub struct ServerState {
    /// The loaded engine (if any)
    pub engine: Option<Engine>,
    /// List of streams
    pub streams: Vec<StreamInfo>,
    /// Server start time
    pub start_time: std::time::Instant,
    /// Channel to send output events
    pub output_tx: mpsc::Sender<Event>,
    /// Allowed working directory for file operations
    pub workdir: PathBuf,
}

impl ServerState {
    /// Create a new server state
    pub fn new(output_tx: mpsc::Sender<Event>, workdir: PathBuf) -> Self {
        Self {
            engine: None,
            streams: Vec::new(),
            start_time: std::time::Instant::now(),
            output_tx,
            workdir,
        }
    }

    /// Get the uptime in seconds
    pub fn uptime_secs(&self) -> f64 {
        self.start_time.elapsed().as_secs_f64()
    }
}

// =============================================================================
// Message Handlers
// =============================================================================

/// Handle a WebSocket message and return a response
pub async fn handle_message(msg: WsMessage, state: &Arc<RwLock<ServerState>>) -> WsMessage {
    match msg {
        WsMessage::LoadFile { path } => handle_load_file(&path, state).await,
        WsMessage::GetStreams => handle_get_streams(state).await,
        WsMessage::GetMetrics => handle_get_metrics(state).await,
        WsMessage::InjectEvent { event_type, data } => {
            handle_inject_event(&event_type, data, state).await
        }
        _ => WsMessage::Error {
            message: "Unknown message type".to_string(),
        },
    }
}

/// Handle LoadFile message
async fn handle_load_file(path: &str, state: &Arc<RwLock<ServerState>>) -> WsMessage {
    // Get workdir from state
    let workdir = {
        let state = state.read().await;
        state.workdir.clone()
    };

    // Validate path
    let validated_path = match security::validate_path(path, &workdir) {
        Ok(p) => p,
        Err(e) => {
            return WsMessage::LoadResult {
                success: false,
                streams_loaded: 0,
                error: Some(e.to_string()),
            };
        }
    };

    // Read file
    let source = match std::fs::read_to_string(&validated_path) {
        Ok(s) => s,
        Err(_) => {
            return WsMessage::LoadResult {
                success: false,
                streams_loaded: 0,
                // Generic error to avoid information disclosure
                error: Some("Failed to read file".to_string()),
            };
        }
    };

    // Parse program
    let program = match parse(&source) {
        Ok(p) => p,
        Err(e) => {
            return WsMessage::LoadResult {
                success: false,
                streams_loaded: 0,
                error: Some(format!("Parse error: {}", e)),
            };
        }
    };

    // Load into engine
    let mut state = state.write().await;
    let output_tx = state.output_tx.clone();
    let mut engine = Engine::new(output_tx);

    match engine.load(&program) {
        Ok(()) => {
            let streams_count = engine.metrics().streams_count;
            state.engine = Some(engine);
            state.streams = vec![]; // TODO: populate from engine

            WsMessage::LoadResult {
                success: true,
                streams_loaded: streams_count,
                error: None,
            }
        }
        Err(e) => WsMessage::LoadResult {
            success: false,
            streams_loaded: 0,
            error: Some(e),
        },
    }
}

/// Handle GetStreams message
async fn handle_get_streams(state: &Arc<RwLock<ServerState>>) -> WsMessage {
    let state = state.read().await;
    WsMessage::Streams {
        data: state.streams.clone(),
    }
}

/// Handle GetMetrics message
async fn handle_get_metrics(state: &Arc<RwLock<ServerState>>) -> WsMessage {
    let state = state.read().await;
    let (events_processed, output_events_emitted, active_streams) = state
        .engine
        .as_ref()
        .map(|engine| {
            let m = engine.metrics();
            (m.events_processed, m.output_events_emitted, m.streams_count)
        })
        .unwrap_or((0, 0, 0));

    WsMessage::Metrics {
        events_processed,
        output_events_emitted,
        active_streams,
        uptime: state.uptime_secs(),
        memory_usage: 0, // TODO: implement
        cpu_usage: 0.0,  // TODO: implement
    }
}

/// Handle InjectEvent message
async fn handle_inject_event(
    event_type: &str,
    data: serde_json::Value,
    state: &Arc<RwLock<ServerState>>,
) -> WsMessage {
    let mut state = state.write().await;

    let engine = match state.engine.as_mut() {
        Some(e) => e,
        None => {
            return WsMessage::Error {
                message: "No engine loaded. Load a .vpl file first.".to_string(),
            };
        }
    };

    // Create event from injected data
    let mut event = Event::new(event_type);

    // Convert JSON data to event fields
    if let Some(obj) = data.as_object() {
        for (key, value) in obj {
            let v = json_to_value(value);
            event.data.insert(key.as_str().into(), v);
        }
    }

    // Process the event
    match engine.process(event).await {
        Ok(()) => WsMessage::EventInjected {
            event_type: event_type.to_string(),
            success: true,
        },
        Err(e) => WsMessage::Error {
            message: format!("Failed to process event: {}", e),
        },
    }
}

// =============================================================================
// WebSocket Connection Handler
// =============================================================================

/// Handle a WebSocket connection
pub async fn handle_connection(
    ws: WebSocket,
    state: Arc<RwLock<ServerState>>,
    broadcast_tx: Arc<tokio::sync::broadcast::Sender<String>>,
) {
    let (ws_tx, mut ws_rx) = ws.split();
    let ws_tx = Arc::new(tokio::sync::Mutex::new(ws_tx));
    let mut broadcast_rx = broadcast_tx.subscribe();

    // Spawn task to forward broadcasts to this client
    let ws_tx_clone = ws_tx.clone();
    let forward_task = tokio::spawn(async move {
        while let Ok(msg) = broadcast_rx.recv().await {
            let mut tx = ws_tx_clone.lock().await;
            if tx.send(Message::text(msg)).await.is_err() {
                break;
            }
        }
    });

    // Handle incoming messages
    while let Some(result) = ws_rx.next().await {
        let msg = match result {
            Ok(msg) => msg,
            Err(e) => {
                tracing::warn!("WebSocket error: {}", e);
                break;
            }
        };

        if msg.is_text() {
            let text = msg.to_str().unwrap_or("");
            if let Ok(ws_msg) = serde_json::from_str::<WsMessage>(text) {
                let response = handle_message(ws_msg, &state).await;
                if let Ok(json) = serde_json::to_string(&response) {
                    let mut tx = ws_tx.lock().await;
                    if tx.send(Message::text(json)).await.is_err() {
                        break;
                    }
                }
            }
        } else if msg.is_close() {
            break;
        }
    }

    forward_task.abort();
    tracing::info!("WebSocket client disconnected");
}

// =============================================================================
// Output Event Forwarder
// =============================================================================

/// Create an output event message from an Event
pub fn create_output_event_message(event: &Event) -> WsMessage {
    WsMessage::OutputEvent {
        event_type: event.event_type.to_string(),
        data: serde_json::to_value(&event.data).unwrap_or_default(),
        timestamp: event.timestamp.to_rfc3339(),
    }
}

/// Spawn an output event forwarder task
pub fn forward_output_events_to_websocket(
    mut output_rx: mpsc::Receiver<Event>,
    broadcast_tx: Arc<tokio::sync::broadcast::Sender<String>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(event) = output_rx.recv().await {
            let msg = create_output_event_message(&event);
            if let Ok(json) = serde_json::to_string(&msg) {
                let _ = broadcast_tx.send(json);
            }
        }
    })
}

// =============================================================================
// Utility Functions
// =============================================================================

/// Convert a serde_json::Value to varpulis_core::Value
pub fn json_to_value(json: &serde_json::Value) -> varpulis_core::Value {
    use varpulis_core::Value;
    match json {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::Str(s.clone().into()),
        serde_json::Value::Array(arr) => Value::array(arr.iter().map(json_to_value).collect()),
        serde_json::Value::Object(obj) => {
            let map: IndexMap<std::sync::Arc<str>, Value, FxBuildHasher> = obj
                .iter()
                .map(|(k, v)| (std::sync::Arc::from(k.as_str()), json_to_value(v)))
                .collect();
            Value::map(map)
        }
    }
}

/// Convert a varpulis_core::Value to serde_json::Value
pub fn value_to_json(value: &varpulis_core::Value) -> serde_json::Value {
    use varpulis_core::Value;
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::json!(*i),
        Value::Float(f) => serde_json::json!(*f),
        Value::Str(s) => serde_json::Value::String(s.to_string()),
        Value::Array(arr) => serde_json::Value::Array(arr.iter().map(value_to_json).collect()),
        Value::Map(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.to_string(), value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
        // Timestamp is nanoseconds since epoch (i64)
        Value::Timestamp(ts) => serde_json::json!(*ts),
        // Duration is nanoseconds (u64)
        Value::Duration(d) => serde_json::json!(*d),
    }
}

// =============================================================================
// Tests - TDD approach
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------------------------------------------------------------
    // WsMessage serialization tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_ws_message_serialize_load_file() {
        let msg = WsMessage::LoadFile {
            path: "test.vpl".to_string(),
        };
        let json = serde_json::to_string(&msg).expect("should serialize");
        assert!(json.contains("load_file"));
        assert!(json.contains("test.vpl"));
    }

    #[test]
    fn test_ws_message_deserialize_load_file() {
        let json = r#"{"type": "load_file", "path": "example.vpl"}"#;
        let msg: WsMessage = serde_json::from_str(json).expect("should deserialize");

        match msg {
            WsMessage::LoadFile { path } => {
                assert_eq!(path, "example.vpl");
            }
            _ => panic!("Expected LoadFile message"),
        }
    }

    #[test]
    fn test_ws_message_serialize_inject_event() {
        let msg = WsMessage::InjectEvent {
            event_type: "Temperature".to_string(),
            data: serde_json::json!({"value": 25.5}),
        };
        let json = serde_json::to_string(&msg).expect("should serialize");
        assert!(json.contains("inject_event"));
        assert!(json.contains("Temperature"));
        assert!(json.contains("25.5"));
    }

    #[test]
    fn test_ws_message_deserialize_inject_event() {
        let json = r#"{"type": "inject_event", "event_type": "Sensor", "data": {"id": 42}}"#;
        let msg: WsMessage = serde_json::from_str(json).expect("should deserialize");

        match msg {
            WsMessage::InjectEvent { event_type, data } => {
                assert_eq!(event_type, "Sensor");
                assert_eq!(data["id"], 42);
            }
            _ => panic!("Expected InjectEvent message"),
        }
    }

    #[test]
    fn test_ws_message_serialize_get_streams() {
        let msg = WsMessage::GetStreams;
        let json = serde_json::to_string(&msg).expect("should serialize");
        assert!(json.contains("get_streams"));
    }

    #[test]
    fn test_ws_message_serialize_get_metrics() {
        let msg = WsMessage::GetMetrics;
        let json = serde_json::to_string(&msg).expect("should serialize");
        assert!(json.contains("get_metrics"));
    }

    #[test]
    fn test_ws_message_serialize_load_result_success() {
        let msg = WsMessage::LoadResult {
            success: true,
            streams_loaded: 5,
            error: None,
        };
        let json = serde_json::to_string(&msg).expect("should serialize");
        assert!(json.contains("load_result"));
        assert!(json.contains("true"));
        assert!(json.contains("5"));
    }

    #[test]
    fn test_ws_message_serialize_load_result_error() {
        let msg = WsMessage::LoadResult {
            success: false,
            streams_loaded: 0,
            error: Some("Parse error at line 5".to_string()),
        };
        let json = serde_json::to_string(&msg).expect("should serialize");
        assert!(json.contains("false"));
        assert!(json.contains("Parse error"));
    }

    #[test]
    fn test_ws_message_serialize_streams() {
        let msg = WsMessage::Streams {
            data: vec![StreamInfo {
                name: "HighTemp".to_string(),
                source: "TempReading".to_string(),
                operations: vec!["where".to_string(), "emit".to_string()],
                events_per_second: 100.5,
                status: "active".to_string(),
            }],
        };
        let json = serde_json::to_string(&msg).expect("should serialize");
        assert!(json.contains("HighTemp"));
        assert!(json.contains("TempReading"));
    }

    #[test]
    fn test_ws_message_serialize_output_event() {
        let msg = WsMessage::OutputEvent {
            event_type: "HighTemperature".to_string(),
            data: serde_json::json!({"value": 35.5}),
            timestamp: "2026-01-29T12:00:00Z".to_string(),
        };
        let json = serde_json::to_string(&msg).expect("should serialize");
        assert!(json.contains("output_event"));
        assert!(json.contains("HighTemperature"));
        assert!(json.contains("35.5"));
    }

    #[test]
    fn test_ws_message_serialize_metrics() {
        let msg = WsMessage::Metrics {
            events_processed: 1000,
            output_events_emitted: 50,
            active_streams: 3,
            uptime: 3600.5,
            memory_usage: 1024000,
            cpu_usage: 25.5,
        };
        let json = serde_json::to_string(&msg).expect("should serialize");
        assert!(json.contains("metrics"));
        assert!(json.contains("1000"));
        assert!(json.contains("50"));
    }

    #[test]
    fn test_ws_message_serialize_error() {
        let msg = WsMessage::Error {
            message: "Something went wrong".to_string(),
        };
        let json = serde_json::to_string(&msg).expect("should serialize");
        assert!(json.contains("error"));
        assert!(json.contains("Something went wrong"));
    }

    // -------------------------------------------------------------------------
    // json_to_value tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_json_to_value_null() {
        let json = serde_json::Value::Null;
        let value = json_to_value(&json);
        assert_eq!(value, varpulis_core::Value::Null);
    }

    #[test]
    fn test_json_to_value_bool() {
        let json = serde_json::json!(true);
        let value = json_to_value(&json);
        assert_eq!(value, varpulis_core::Value::Bool(true));
    }

    #[test]
    fn test_json_to_value_int() {
        let json = serde_json::json!(42);
        let value = json_to_value(&json);
        assert_eq!(value, varpulis_core::Value::Int(42));
    }

    #[test]
    fn test_json_to_value_float() {
        let json = serde_json::json!(3.15);
        let value = json_to_value(&json);
        match value {
            varpulis_core::Value::Float(f) => {
                assert!((f - 3.15).abs() < 0.001);
            }
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_json_to_value_string() {
        let json = serde_json::json!("hello");
        let value = json_to_value(&json);
        assert_eq!(value, varpulis_core::Value::Str("hello".into()));
    }

    #[test]
    fn test_json_to_value_array() {
        let json = serde_json::json!([1, 2, 3]);
        let value = json_to_value(&json);
        match value {
            varpulis_core::Value::Array(arr) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], varpulis_core::Value::Int(1));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_json_to_value_object() {
        let json = serde_json::json!({"key": "value"});
        let value = json_to_value(&json);
        match value {
            varpulis_core::Value::Map(map) => {
                assert_eq!(
                    map.get("key"),
                    Some(&varpulis_core::Value::Str("value".into()))
                );
            }
            _ => panic!("Expected Map"),
        }
    }

    #[test]
    fn test_json_to_value_nested() {
        let json = serde_json::json!({
            "name": "sensor",
            "values": [1, 2, 3],
            "config": {"enabled": true}
        });
        let value = json_to_value(&json);
        match value {
            varpulis_core::Value::Map(map) => {
                assert!(map.contains_key("name"));
                assert!(map.contains_key("values"));
                assert!(map.contains_key("config"));
            }
            _ => panic!("Expected Map"),
        }
    }

    // -------------------------------------------------------------------------
    // value_to_json tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_value_to_json_null() {
        let value = varpulis_core::Value::Null;
        let json = value_to_json(&value);
        assert_eq!(json, serde_json::Value::Null);
    }

    #[test]
    fn test_value_to_json_bool() {
        let value = varpulis_core::Value::Bool(false);
        let json = value_to_json(&value);
        assert_eq!(json, serde_json::json!(false));
    }

    #[test]
    fn test_value_to_json_int() {
        let value = varpulis_core::Value::Int(100);
        let json = value_to_json(&value);
        assert_eq!(json, serde_json::json!(100));
    }

    #[test]
    fn test_value_to_json_float() {
        let value = varpulis_core::Value::Float(2.72);
        let json = value_to_json(&value);
        assert_eq!(json, serde_json::json!(2.72));
    }

    #[test]
    fn test_value_to_json_string() {
        let value = varpulis_core::Value::Str("world".into());
        let json = value_to_json(&value);
        assert_eq!(json, serde_json::json!("world"));
    }

    #[test]
    fn test_value_to_json_array() {
        let value = varpulis_core::Value::array(vec![
            varpulis_core::Value::Int(1),
            varpulis_core::Value::Int(2),
        ]);
        let json = value_to_json(&value);
        assert_eq!(json, serde_json::json!([1, 2]));
    }

    // -------------------------------------------------------------------------
    // StreamInfo tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_stream_info_serialize() {
        let info = StreamInfo {
            name: "TestStream".to_string(),
            source: "Events".to_string(),
            operations: vec!["where".to_string(), "select".to_string()],
            events_per_second: 50.0,
            status: "active".to_string(),
        };
        let json = serde_json::to_string(&info).expect("should serialize");
        assert!(json.contains("TestStream"));
        assert!(json.contains("Events"));
        assert!(json.contains("active"));
    }

    #[test]
    fn test_stream_info_deserialize() {
        let json = r#"{
            "name": "MyStream",
            "source": "Sensors",
            "operations": ["filter"],
            "events_per_second": 10.0,
            "status": "idle"
        }"#;
        let info: StreamInfo = serde_json::from_str(json).expect("should deserialize");
        assert_eq!(info.name, "MyStream");
        assert_eq!(info.source, "Sensors");
        assert_eq!(info.status, "idle");
    }

    // -------------------------------------------------------------------------
    // ServerState tests
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_server_state_new() {
        let (output_tx, _) = mpsc::channel(100);
        let workdir = std::env::current_dir().expect("should get current dir");
        let state = ServerState::new(output_tx, workdir.clone());

        assert!(state.engine.is_none());
        assert!(state.streams.is_empty());
        assert_eq!(state.workdir, workdir);
    }

    #[tokio::test]
    async fn test_server_state_uptime() {
        let (output_tx, _) = mpsc::channel(100);
        let workdir = std::env::current_dir().expect("should get current dir");
        let state = ServerState::new(output_tx, workdir);

        // Small delay
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let uptime = state.uptime_secs();
        assert!(uptime >= 0.01);
    }

    // -------------------------------------------------------------------------
    // handle_message tests
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_handle_message_unknown_type() {
        let (output_tx, _) = mpsc::channel(100);
        let workdir = std::env::current_dir().expect("should get current dir");
        let state = Arc::new(RwLock::new(ServerState::new(output_tx, workdir)));

        // Create a response-type message (which shouldn't be sent from client)
        let msg = WsMessage::Error {
            message: "test".to_string(),
        };

        let response = handle_message(msg, &state).await;

        match response {
            WsMessage::Error { message } => {
                assert!(message.contains("Unknown"));
            }
            _ => panic!("Expected Error response"),
        }
    }

    #[tokio::test]
    async fn test_handle_get_metrics_no_engine() {
        let (output_tx, _) = mpsc::channel(100);
        let workdir = std::env::current_dir().expect("should get current dir");
        let state = Arc::new(RwLock::new(ServerState::new(output_tx, workdir)));

        let msg = WsMessage::GetMetrics;
        let response = handle_message(msg, &state).await;

        match response {
            WsMessage::Metrics {
                events_processed,
                output_events_emitted,
                active_streams,
                ..
            } => {
                assert_eq!(events_processed, 0);
                assert_eq!(output_events_emitted, 0);
                assert_eq!(active_streams, 0);
            }
            _ => panic!("Expected Metrics response"),
        }
    }

    #[tokio::test]
    async fn test_handle_get_streams_empty() {
        let (output_tx, _) = mpsc::channel(100);
        let workdir = std::env::current_dir().expect("should get current dir");
        let state = Arc::new(RwLock::new(ServerState::new(output_tx, workdir)));

        let msg = WsMessage::GetStreams;
        let response = handle_message(msg, &state).await;

        match response {
            WsMessage::Streams { data } => {
                assert!(data.is_empty());
            }
            _ => panic!("Expected Streams response"),
        }
    }

    #[tokio::test]
    async fn test_handle_inject_event_no_engine() {
        let (output_tx, _) = mpsc::channel(100);
        let workdir = std::env::current_dir().expect("should get current dir");
        let state = Arc::new(RwLock::new(ServerState::new(output_tx, workdir)));

        let msg = WsMessage::InjectEvent {
            event_type: "Test".to_string(),
            data: serde_json::json!({}),
        };
        let response = handle_message(msg, &state).await;

        match response {
            WsMessage::Error { message } => {
                assert!(message.contains("No engine loaded"));
            }
            _ => panic!("Expected Error response"),
        }
    }

    #[tokio::test]
    async fn test_handle_load_file_path_traversal() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let workdir = temp_dir.path().join("allowed");
        std::fs::create_dir(&workdir).expect("Failed to create workdir");

        let (output_tx, _) = mpsc::channel(100);
        let state = Arc::new(RwLock::new(ServerState::new(output_tx, workdir)));

        let msg = WsMessage::LoadFile {
            path: "../../../etc/passwd".to_string(),
        };
        let response = handle_message(msg, &state).await;

        match response {
            WsMessage::LoadResult { success, error, .. } => {
                assert!(!success);
                assert!(error.is_some());
                // Should NOT reveal the actual path in error
                let err = error.expect("should have error");
                assert!(!err.contains("passwd"));
            }
            _ => panic!("Expected LoadResult response"),
        }
    }

    // -------------------------------------------------------------------------
    // create_output_event_message tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_create_output_event_message() {
        let mut event = Event::new("HighTemp");
        event.data.insert(
            "sensor_id".into(),
            varpulis_core::Value::Str("S1".into()),
        );

        let msg = create_output_event_message(&event);

        match msg {
            WsMessage::OutputEvent {
                event_type,
                data,
                timestamp,
            } => {
                assert_eq!(event_type, "HighTemp");
                assert!(data.get("sensor_id").is_some());
                assert!(!timestamp.is_empty());
            }
            _ => panic!("Expected OutputEvent message"),
        }
    }
}
