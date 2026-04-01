//! Splunk HEC input connector for Varpulis.
//!
//! Acts as a Splunk HTTP Event Collector (HEC) compatible endpoint,
//! receiving events via HTTP POST and forwarding them to the Varpulis engine.
//! This allows Splunk Universal Forwarders and other HEC clients to send
//! events directly to Varpulis.

use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;
use tokio::sync::{mpsc, Mutex};
use tracing::info;
use varpulis_connector_api::{
    ConfigParamInfo, ConnectorComponentInfo, ConnectorConfig, ConnectorError, ConnectorFactory,
    Sink, SinkConnector, SourceConnector,
};
use varpulis_core::event::Event;
use varpulis_core::value::FxIndexMap;
use varpulis_core::Value;

// ---------------------------------------------------------------------------
// HEC Event Parser
// ---------------------------------------------------------------------------

/// Parse a Splunk HEC event payload into a Varpulis event.
///
/// HEC format: `{"event": {...}, "host": "...", "source": "...", "sourcetype": "...", "time": N}`
pub fn parse_hec_event(payload: &serde_json::Value) -> Event {
    let mut data = FxIndexMap::default();

    // Extract HEC metadata fields
    if let Some(host) = payload.get("host").and_then(|v| v.as_str()) {
        data.insert("host".into(), Value::str(host));
    }
    if let Some(source) = payload.get("source").and_then(|v| v.as_str()) {
        data.insert("source".into(), Value::str(source));
    }
    if let Some(sourcetype) = payload.get("sourcetype").and_then(|v| v.as_str()) {
        data.insert("sourcetype".into(), Value::str(sourcetype));
    }
    if let Some(index) = payload.get("index").and_then(|v| v.as_str()) {
        data.insert("index".into(), Value::str(index));
    }

    // Extract the event body
    let event_type = match payload.get("event") {
        Some(serde_json::Value::Object(obj)) => {
            // Structured event — flatten fields into the data map
            for (key, val) in obj {
                let v = json_to_value(val);
                data.insert(Arc::from(key.as_str()), v);
            }
            // Use sourcetype as event type if available, otherwise "SplunkHec"
            payload
                .get("sourcetype")
                .and_then(|v| v.as_str())
                .unwrap_or("SplunkHec")
                .to_string()
        }
        Some(serde_json::Value::String(s)) => {
            // Raw string event
            data.insert("message".into(), Value::str(s.as_str()));
            "SplunkHec".to_string()
        }
        _ => {
            // Missing or null event
            data.insert("raw".into(), Value::str(payload.to_string()));
            "SplunkHec".to_string()
        }
    };

    // Extract timestamp if present
    let timestamp = payload
        .get("time")
        .and_then(|v| v.as_f64())
        .map(|epoch| {
            chrono::DateTime::from_timestamp(epoch as i64, ((epoch.fract()) * 1e9) as u32)
                .unwrap_or_else(chrono::Utc::now)
        })
        .unwrap_or_else(chrono::Utc::now);

    Event {
        event_type: Arc::from(event_type),
        timestamp,
        data,
    }
}

/// Convert a JSON value to a Varpulis Value.
fn json_to_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::str(n.to_string())
            }
        }
        serde_json::Value::String(s) => Value::str(s.as_str()),
        other => Value::str(other.to_string()),
    }
}

// ---------------------------------------------------------------------------
// HEC Source Connector
// ---------------------------------------------------------------------------

/// Shared state for the HEC HTTP server.
struct HecState {
    tx: mpsc::Sender<Event>,
    token: Option<String>,
}

/// Splunk HEC source — acts as an HEC-compatible HTTP endpoint.
pub struct SplunkHecSource {
    name: String,
    bind_addr: String,
    token: Option<String>,
    running: Arc<std::sync::atomic::AtomicBool>,
    shutdown_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl std::fmt::Debug for SplunkHecSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SplunkHecSource")
            .field("name", &self.name)
            .field("bind_addr", &self.bind_addr)
            .finish_non_exhaustive()
    }
}

impl SplunkHecSource {
    /// Create a new Splunk HEC source.
    pub fn new(name: &str, bind_addr: &str, token: Option<String>) -> Self {
        Self {
            name: name.to_string(),
            bind_addr: bind_addr.to_string(),
            token,
            running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            shutdown_tx: Mutex::new(None),
        }
    }
}

/// HEC event endpoint handler.
async fn hec_event_handler(
    State(state): State<Arc<HecState>>,
    headers: HeaderMap,
    body: String,
) -> StatusCode {
    // Validate HEC token if configured
    if let Some(ref expected_token) = state.token {
        let auth = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let provided = auth.strip_prefix("Splunk ").unwrap_or("");
        if provided != expected_token {
            return StatusCode::FORBIDDEN;
        }
    }

    // Parse the body — HEC supports newline-delimited JSON for batch
    for line in body.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<serde_json::Value>(trimmed) {
            Ok(payload) => {
                let event = parse_hec_event(&payload);
                if state.tx.send(event).await.is_err() {
                    return StatusCode::SERVICE_UNAVAILABLE;
                }
            }
            Err(_) => {
                return StatusCode::BAD_REQUEST;
            }
        }
    }

    StatusCode::OK
}

/// HEC health check endpoint.
async fn hec_health_handler() -> StatusCode {
    StatusCode::OK
}

#[async_trait]
impl SourceConnector for SplunkHecSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&mut self, tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
        let state = Arc::new(HecState {
            tx,
            token: self.token.clone(),
        });

        let app = Router::new()
            .route("/services/collector/event", post(hec_event_handler))
            .route(
                "/services/collector/health",
                axum::routing::get(hec_health_handler),
            )
            .with_state(state);

        let listener = tokio::net::TcpListener::bind(&self.bind_addr)
            .await
            .map_err(|e| {
                ConnectorError::ConnectionFailed(format!("Bind {}: {e}", self.bind_addr))
            })?;

        info!("Splunk HEC source listening on {}", self.bind_addr);
        self.running
            .store(true, std::sync::atomic::Ordering::Relaxed);

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        *self.shutdown_tx.lock().await = Some(shutdown_tx);

        let running = self.running.clone();

        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .ok();
            running.store(false, std::sync::atomic::Ordering::Relaxed);
        });

        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        let shutdown = self.shutdown_tx.lock().await.take();
        if let Some(tx) = shutdown {
            let _ = tx.send(());
        }
        self.running
            .store(false, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.running.load(std::sync::atomic::Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// Factory & Registration
// ---------------------------------------------------------------------------

static SPLUNK_CONFIG_PARAMS: &[ConfigParamInfo] = &[
    ConfigParamInfo {
        name: "bind",
        description: "Bind address for HEC endpoint (host:port)",
        required: false,
        default_value: Some("0.0.0.0:8088"),
    },
    ConfigParamInfo {
        name: "token",
        description: "HEC authentication token",
        required: false,
        default_value: None,
    },
];

static SPLUNK_INFO: ConnectorComponentInfo = ConnectorComponentInfo {
    connector_type: "splunk-hec",
    display_name: "Splunk HEC",
    description: "Splunk HTTP Event Collector compatible input endpoint",
    feature_flag: "",
    supports_source: true,
    supports_sink: false,
    supports_managed: false,
    config_params: SPLUNK_CONFIG_PARAMS,
};

struct SplunkFactory;

impl ConnectorFactory for SplunkFactory {
    fn info(&self) -> &ConnectorComponentInfo {
        &SPLUNK_INFO
    }

    fn create_managed(
        &self,
        _name: &str,
        _config: &ConnectorConfig,
    ) -> Result<Box<dyn varpulis_connector_api::ManagedConnector>, ConnectorError> {
        Err(ConnectorError::NotAvailable(
            "splunk-hec managed connector not supported".to_string(),
        ))
    }

    fn create_sink_connector(
        &self,
        _config: &ConnectorConfig,
    ) -> Result<Box<dyn SinkConnector>, ConnectorError> {
        Err(ConnectorError::NotAvailable(
            "splunk-hec is a source-only connector".to_string(),
        ))
    }

    fn create_engine_sink(
        &self,
        _name: &str,
        _config: &ConnectorConfig,
        _topic_override: Option<&str>,
        _context_name: Option<&str>,
    ) -> Result<Arc<dyn Sink>, ConnectorError> {
        Err(ConnectorError::NotAvailable(
            "splunk-hec is a source-only connector".to_string(),
        ))
    }
}

inventory::submit! { &SplunkFactory as &dyn ConnectorFactory }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_hec_structured_event() {
        let payload = serde_json::json!({
            "event": {
                "EventID": 1,
                "Image": "cmd.exe",
                "CommandLine": "cmd /c whoami"
            },
            "host": "WS01",
            "sourcetype": "SysmonProcessCreate",
            "time": 1711929600.123
        });

        let event = parse_hec_event(&payload);
        assert_eq!(&*event.event_type, "SysmonProcessCreate");
        assert_eq!(event.get("host"), Some(&Value::str("WS01")));
        assert_eq!(event.get("EventID"), Some(&Value::Int(1)));
        assert_eq!(event.get("Image"), Some(&Value::str("cmd.exe")));
    }

    #[test]
    fn test_parse_hec_string_event() {
        let payload = serde_json::json!({
            "event": "raw syslog message here",
            "host": "fw01"
        });

        let event = parse_hec_event(&payload);
        assert_eq!(&*event.event_type, "SplunkHec");
        assert_eq!(
            event.get("message"),
            Some(&Value::str("raw syslog message here"))
        );
        assert_eq!(event.get("host"), Some(&Value::str("fw01")));
    }

    #[tokio::test]
    async fn test_hec_source_lifecycle() {
        let port = 18088u16;
        let mut source = SplunkHecSource::new(
            "test",
            &format!("127.0.0.1:{port}"),
            Some("test-token".into()),
        );
        let (tx, mut rx) = mpsc::channel(100);

        source.start(tx).await.unwrap();
        assert!(source.is_running());

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Send an HEC event
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://127.0.0.1:{port}/services/collector/event"))
            .header("Authorization", "Splunk test-token")
            .json(&serde_json::json!({
                "event": {"action": "login", "user": "admin"},
                "host": "web01",
                "sourcetype": "webapp"
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        // Receive the event
        let event = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&*event.event_type, "webapp");
        assert_eq!(event.get("action"), Some(&Value::str("login")));
        assert_eq!(event.get("host"), Some(&Value::str("web01")));

        // Test auth rejection
        let resp = client
            .post(format!("http://127.0.0.1:{port}/services/collector/event"))
            .header("Authorization", "Splunk wrong-token")
            .json(&serde_json::json!({"event": "test"}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 403);

        source.stop().await.unwrap();
    }
}
