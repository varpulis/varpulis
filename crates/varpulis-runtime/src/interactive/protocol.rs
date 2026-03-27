//! Protocol types for interactive session communication.
//!
//! Defines the command and response enums used by all transports (JSON-line,
//! TUI, MCP). Serialization uses `#[serde(tag = ...)]` for unambiguous
//! JSON round-tripping.

use serde::{Deserialize, Serialize};

use crate::engine::graph::{GraphEdge, GraphNode};

// =============================================================================
// Commands (client → session)
// =============================================================================

/// A command sent to the interactive session.
#[derive(Debug, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum SessionCommand {
    /// Load a VPL program from an inline string (replaces current program).
    LoadVpl { vpl: String },
    /// Append VPL declarations to the current program and recompile.
    AppendVpl { vpl: String },
    /// Load a VPL program from a file path.
    LoadFile { path: String },
    /// Inject a single event.
    Inject {
        event_type: String,
        data: serde_json::Value,
    },
    /// Inject events from a file (.evt or JSONL).
    InjectFile { path: String },
    /// Start event generation with the given schema.
    Generate {
        schema: String,
        #[serde(default = "default_rate")]
        rate: u64,
        duration: Option<u64>,
    },
    /// Stop the running event generator.
    StopGenerate,
    /// Subscribe to output events from a specific stream (None = all).
    Subscribe { stream: Option<String> },
    /// Unsubscribe from a specific stream (None = all).
    Unsubscribe { stream: Option<String> },
    /// List all loaded streams.
    GetStreams,
    /// Get engine metrics snapshot.
    GetMetrics,
    /// Get the pipeline topology graph.
    GetTopology,
    /// Enable or disable pipeline tracing.
    SetTrace { enabled: bool },
    /// Save the current VPL buffer to a file.
    Save { path: String },
    /// Get the current accumulated VPL source.
    GetSource,
    /// Terminate the session.
    Quit,
}

fn default_rate() -> u64 {
    1000
}

// =============================================================================
// Responses (session → client)
// =============================================================================

/// A response emitted by the interactive session.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SessionResponse {
    /// Session is ready to accept commands.
    Ready { version: String },
    /// A VPL program was loaded (or reloaded).
    Loaded {
        streams: Vec<String>,
        added: Vec<String>,
        removed: Vec<String>,
        preserved: Vec<String>,
    },
    /// An output event from the pipeline.
    Output {
        stream: String,
        event: serde_json::Value,
        timestamp: String,
    },
    /// Trace entries from the last operation.
    Trace { entries: Vec<TraceEntryJson> },
    /// Engine metrics snapshot.
    Metrics {
        events_processed: u64,
        output_events: u64,
        streams_count: usize,
    },
    /// Pipeline topology graph.
    Topology {
        nodes: Vec<GraphNode>,
        edges: Vec<GraphEdge>,
    },
    /// List of loaded streams.
    Streams { streams: Vec<StreamInfo> },
    /// An error occurred processing a command.
    Error { message: String },
    /// Generator status update.
    GeneratorStatus { running: bool, generated: u64 },
    /// VPL source saved to file.
    Saved { path: String, lines: usize },
    /// Current accumulated VPL source.
    Source { vpl: String },
    /// Session is shutting down.
    Bye,
}

/// A single trace entry serialized as JSON.
#[derive(Debug, Clone, Serialize)]
pub struct TraceEntryJson {
    pub kind: String,
    pub stream: Option<String>,
    pub detail: Option<String>,
}

/// Information about a loaded stream.
#[derive(Debug, Clone, Serialize)]
pub struct StreamInfo {
    pub name: String,
    pub source: String,
    pub ops_count: usize,
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ---- SessionCommand round-trip tests ----

    #[test]
    fn test_deserialize_load_vpl() {
        let json = r#"{"cmd":"load_vpl","vpl":"stream X = A .where(x > 1)"}"#;
        let cmd: SessionCommand = serde_json::from_str(json).unwrap();
        assert!(matches!(cmd, SessionCommand::LoadVpl { vpl } if vpl.contains("stream X")));
    }

    #[test]
    fn test_deserialize_load_file() {
        let json = r#"{"cmd":"load_file","path":"/tmp/test.vpl"}"#;
        let cmd: SessionCommand = serde_json::from_str(json).unwrap();
        assert!(matches!(cmd, SessionCommand::LoadFile { path } if path == "/tmp/test.vpl"));
    }

    #[test]
    fn test_deserialize_inject() {
        let json = r#"{"cmd":"inject","event_type":"Tick","data":{"price":42.5}}"#;
        let cmd: SessionCommand = serde_json::from_str(json).unwrap();
        assert!(
            matches!(cmd, SessionCommand::Inject { event_type, data } if event_type == "Tick" && data["price"] == 42.5)
        );
    }

    #[test]
    fn test_deserialize_inject_file() {
        let json = r#"{"cmd":"inject_file","path":"/tmp/events.evt"}"#;
        let cmd: SessionCommand = serde_json::from_str(json).unwrap();
        assert!(matches!(cmd, SessionCommand::InjectFile { path } if path == "/tmp/events.evt"));
    }

    #[test]
    fn test_deserialize_generate_defaults() {
        let json = r#"{"cmd":"generate","schema":"fraud"}"#;
        let cmd: SessionCommand = serde_json::from_str(json).unwrap();
        assert!(
            matches!(cmd, SessionCommand::Generate { schema, rate, duration } if schema == "fraud" && rate == 1000 && duration.is_none())
        );
    }

    #[test]
    fn test_deserialize_generate_full() {
        let json = r#"{"cmd":"generate","schema":"iot","rate":500,"duration":30}"#;
        let cmd: SessionCommand = serde_json::from_str(json).unwrap();
        assert!(
            matches!(cmd, SessionCommand::Generate { schema, rate, duration } if schema == "iot" && rate == 500 && duration == Some(30))
        );
    }

    #[test]
    fn test_deserialize_stop_generate() {
        let json = r#"{"cmd":"stop_generate"}"#;
        let cmd: SessionCommand = serde_json::from_str(json).unwrap();
        assert!(matches!(cmd, SessionCommand::StopGenerate));
    }

    #[test]
    fn test_deserialize_subscribe_all() {
        let json = r#"{"cmd":"subscribe","stream":null}"#;
        let cmd: SessionCommand = serde_json::from_str(json).unwrap();
        assert!(matches!(cmd, SessionCommand::Subscribe { stream: None }));
    }

    #[test]
    fn test_deserialize_subscribe_specific() {
        let json = r#"{"cmd":"subscribe","stream":"Alerts"}"#;
        let cmd: SessionCommand = serde_json::from_str(json).unwrap();
        assert!(matches!(cmd, SessionCommand::Subscribe { stream: Some(s) } if s == "Alerts"));
    }

    #[test]
    fn test_deserialize_unsubscribe() {
        let json = r#"{"cmd":"unsubscribe","stream":"Alerts"}"#;
        let cmd: SessionCommand = serde_json::from_str(json).unwrap();
        assert!(matches!(cmd, SessionCommand::Unsubscribe { stream: Some(s) } if s == "Alerts"));
    }

    #[test]
    fn test_deserialize_get_streams() {
        let json = r#"{"cmd":"get_streams"}"#;
        let cmd: SessionCommand = serde_json::from_str(json).unwrap();
        assert!(matches!(cmd, SessionCommand::GetStreams));
    }

    #[test]
    fn test_deserialize_get_metrics() {
        let json = r#"{"cmd":"get_metrics"}"#;
        let cmd: SessionCommand = serde_json::from_str(json).unwrap();
        assert!(matches!(cmd, SessionCommand::GetMetrics));
    }

    #[test]
    fn test_deserialize_get_topology() {
        let json = r#"{"cmd":"get_topology"}"#;
        let cmd: SessionCommand = serde_json::from_str(json).unwrap();
        assert!(matches!(cmd, SessionCommand::GetTopology));
    }

    #[test]
    fn test_deserialize_set_trace() {
        let json = r#"{"cmd":"set_trace","enabled":true}"#;
        let cmd: SessionCommand = serde_json::from_str(json).unwrap();
        assert!(matches!(cmd, SessionCommand::SetTrace { enabled: true }));
    }

    #[test]
    fn test_deserialize_quit() {
        let json = r#"{"cmd":"quit"}"#;
        let cmd: SessionCommand = serde_json::from_str(json).unwrap();
        assert!(matches!(cmd, SessionCommand::Quit));
    }

    // ---- SessionResponse serialization tests ----

    #[test]
    fn test_serialize_ready() {
        let resp = SessionResponse::Ready {
            version: "0.8.0".into(),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["type"], "ready");
        assert_eq!(json["version"], "0.8.0");
    }

    #[test]
    fn test_serialize_loaded() {
        let resp = SessionResponse::Loaded {
            streams: vec!["A".into(), "B".into()],
            added: vec!["A".into(), "B".into()],
            removed: vec![],
            preserved: vec![],
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["type"], "loaded");
        assert_eq!(json["streams"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_serialize_output() {
        let resp = SessionResponse::Output {
            stream: "Alerts".into(),
            event: serde_json::json!({"temp": 105}),
            timestamp: "2026-01-01T00:00:00Z".into(),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["type"], "output");
        assert_eq!(json["stream"], "Alerts");
        assert_eq!(json["event"]["temp"], 105);
    }

    #[test]
    fn test_serialize_trace() {
        let resp = SessionResponse::Trace {
            entries: vec![TraceEntryJson {
                kind: "stream_matched".into(),
                stream: Some("Alerts".into()),
                detail: None,
            }],
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["type"], "trace");
        assert_eq!(json["entries"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_serialize_metrics() {
        let resp = SessionResponse::Metrics {
            events_processed: 1000,
            output_events: 42,
            streams_count: 3,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["type"], "metrics");
        assert_eq!(json["events_processed"], 1000);
        assert_eq!(json["output_events"], 42);
        assert_eq!(json["streams_count"], 3);
    }

    #[test]
    fn test_serialize_topology() {
        let resp = SessionResponse::Topology {
            nodes: vec![GraphNode {
                id: "n1".into(),
                label: "Source".into(),
                node_type: "source".into(),
                config: serde_json::json!({}),
                position: None,
            }],
            edges: vec![GraphEdge {
                id: "e1".into(),
                source: "n1".into(),
                target: "n2".into(),
            }],
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["type"], "topology");
        assert_eq!(json["nodes"].as_array().unwrap().len(), 1);
        assert_eq!(json["edges"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_serialize_streams() {
        let resp = SessionResponse::Streams {
            streams: vec![StreamInfo {
                name: "Alerts".into(),
                source: "event:Temperature".into(),
                ops_count: 2,
            }],
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["type"], "streams");
        assert_eq!(json["streams"][0]["name"], "Alerts");
    }

    #[test]
    fn test_serialize_error() {
        let resp = SessionResponse::Error {
            message: "parse error".into(),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["type"], "error");
        assert_eq!(json["message"], "parse error");
    }

    #[test]
    fn test_serialize_generator_status() {
        let resp = SessionResponse::GeneratorStatus {
            running: true,
            generated: 5000,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["type"], "generator_status");
        assert!(json["running"].as_bool().unwrap());
        assert_eq!(json["generated"], 5000);
    }

    #[test]
    fn test_serialize_bye() {
        let resp = SessionResponse::Bye;
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["type"], "bye");
    }
}
