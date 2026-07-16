//! HTTP connector: webhook source and HTTP sink

use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use indexmap::IndexMap;
use tokio::sync::mpsc;
use tower_http::limit::RequestBodyLimitLayer;
use tracing::{error, info, warn};
use varpulis_connector_api::helpers::json_to_value;
use varpulis_connector_api::{
    ConnectorComponentInfo, ConnectorConfig, ConnectorError, ConnectorFactory, Sink, SinkConnector,
    SinkConnectorAdapter, SourceConnector,
};
use varpulis_core::Event;

// ---------------------------------------------------------------------------
// Declarative registration
// ---------------------------------------------------------------------------

static HTTP_INFO: ConnectorComponentInfo = ConnectorComponentInfo {
    connector_type: "http",
    display_name: "HTTP",
    description: "HTTP webhook source and HTTP POST sink",
    feature_flag: "",
    supports_source: true,
    supports_sink: true,
    supports_managed: false,
    config_params: &[],
};

struct HttpFactory;

impl ConnectorFactory for HttpFactory {
    fn info(&self) -> &ConnectorComponentInfo {
        &HTTP_INFO
    }

    fn create_sink_connector(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Box<dyn SinkConnector>, ConnectorError> {
        Ok(Box::new(HttpSink::new("http", &config.url)))
    }

    fn create_engine_sink(
        &self,
        name: &str,
        config: &ConnectorConfig,
        _topic_override: Option<&str>,
        _context_name: Option<&str>,
    ) -> Result<Arc<dyn Sink>, ConnectorError> {
        if config.url.is_empty() {
            return Err(ConnectorError::ConfigError(format!(
                "HTTP connector '{name}' has no URL configured"
            )));
        }
        Ok(Arc::new(SinkConnectorAdapter::new(
            name,
            Box::new(HttpSink::new(name, &config.url)),
        )))
    }
}

inventory::submit! { &HttpFactory as &dyn ConnectorFactory }

// =============================================================================
// HTTP Sink (sends events via HTTP POST)
// =============================================================================

/// HTTP sink that sends events via HTTP POST to a configured URL.
#[derive(Debug)]
pub struct HttpSink {
    name: String,
    url: String,
    client: reqwest::Client,
    headers: IndexMap<String, String>,
}

impl HttpSink {
    /// Create a new HTTP sink targeting the given URL.
    pub fn new(name: &str, url: &str) -> Self {
        Self {
            name: name.to_string(),
            url: url.to_string(),
            client: reqwest::Client::new(),
            headers: IndexMap::new(),
        }
    }

    /// Add a custom HTTP header to all requests.
    pub fn with_header(mut self, key: &str, value: &str) -> Self {
        self.headers.insert(key.to_string(), value.to_string());
        self
    }
}

#[async_trait]
impl SinkConnector for HttpSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
        let mut req = self.client.post(&self.url);
        for (k, v) in &self.headers {
            req = req.header(k.as_str(), v.as_str());
        }
        req = req.header("Content-Type", "application/json");
        req = req.json(event);

        match req.send().await {
            Ok(resp) => {
                if !resp.status().is_success() {
                    warn!("HTTP sink {} got status {}", self.name, resp.status());
                }
                Ok(())
            }
            Err(e) => {
                error!("HTTP sink {} error: {}", self.name, e);
                Err(ConnectorError::SendFailed(e.to_string()))
            }
        }
    }

    async fn flush(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

// =============================================================================
// HTTP Webhook Source (receives events via HTTP POST)
// =============================================================================

/// HTTP webhook configuration
#[derive(Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct HttpWebhookConfig {
    /// Port to listen on
    pub port: u16,
    /// Bind address
    pub bind_address: String,
    /// API key for authentication (optional)
    pub api_key: Option<String>,
    /// Maximum requests per second (0 = unlimited)
    pub rate_limit: u32,
    /// Maximum batch size for batch endpoint
    pub max_batch_size: usize,
    /// Path for single event endpoint
    pub event_path: String,
    /// Path for batch event endpoint
    pub batch_path: String,
}

/// Hand-written so the `api_key` never reaches logs / errors / panic output via
/// `{:?}`. The derived Debug printed it in full. The Some/None distinction is
/// preserved so it's still visible *whether* a key is configured.
impl std::fmt::Debug for HttpWebhookConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpWebhookConfig")
            .field("port", &self.port)
            .field("bind_address", &self.bind_address)
            .field("api_key", &self.api_key.as_ref().map(|_| "***REDACTED***"))
            .field("rate_limit", &self.rate_limit)
            .field("max_batch_size", &self.max_batch_size)
            .field("event_path", &self.event_path)
            .field("batch_path", &self.batch_path)
            .finish()
    }
}

impl Default for HttpWebhookConfig {
    fn default() -> Self {
        Self {
            port: 8080,
            bind_address: "0.0.0.0".to_string(),
            api_key: None,
            rate_limit: 0,
            max_batch_size: 1000,
            event_path: "/event".to_string(),
            batch_path: "/events".to_string(),
        }
    }
}

impl HttpWebhookConfig {
    /// Create a new webhook configuration listening on the given port.
    pub fn new(port: u16) -> Self {
        Self {
            port,
            ..Default::default()
        }
    }

    /// Set an API key for request authentication.
    pub fn with_api_key(mut self, key: &str) -> Self {
        self.api_key = Some(key.to_string());
        self
    }

    /// Set the maximum requests per second (0 = unlimited).
    pub const fn with_rate_limit(mut self, rps: u32) -> Self {
        self.rate_limit = rps;
        self
    }

    /// Set the network interface bind address.
    pub fn with_bind_address(mut self, addr: &str) -> Self {
        self.bind_address = addr.to_string();
        self
    }
}

// =============================================================================
// Axum shared state and error types
// =============================================================================

/// Shared state passed to all axum handlers.
#[derive(Clone)]
struct HttpState {
    tx: mpsc::Sender<Event>,
    config: HttpWebhookConfig,
    rate_limiter: Option<Arc<tokio::sync::Mutex<RateLimiter>>>,
}

/// Error type for HTTP webhook handlers, implements `IntoResponse`.
enum HttpError {
    Auth,
    RateLimit,
    ChannelClosed,
    BatchTooLarge { actual: usize, max: usize },
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::Auth => (
                StatusCode::UNAUTHORIZED,
                "Invalid or missing API key".to_string(),
            ),
            Self::RateLimit => (
                StatusCode::TOO_MANY_REQUESTS,
                "Rate limit exceeded".to_string(),
            ),
            Self::ChannelClosed => (
                StatusCode::SERVICE_UNAVAILABLE,
                "Service unavailable".to_string(),
            ),
            Self::BatchTooLarge { actual, max } => (
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("Batch too large: {actual} events (max: {max})"),
            ),
        };
        (status, Json(serde_json::json!({"error": message}))).into_response()
    }
}

// =============================================================================
// Auth helper
// =============================================================================

/// Validate the API key from headers against the expected key in config.
/// Returns `Ok(())` if authentication passes, `Err(HttpError::Auth)` otherwise.
fn check_auth(headers: &HeaderMap, expected: &Option<String>) -> Result<(), HttpError> {
    let Some(expected_key) = expected else {
        return Ok(());
    };

    let provided = headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .or_else(|| {
            headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(|h| h.strip_prefix("Bearer ").map(|s| s.to_string()))
        });

    match provided {
        Some(key) if key == *expected_key => Ok(()),
        _ => Err(HttpError::Auth),
    }
}

// =============================================================================
// Axum handlers
// =============================================================================

/// POST /{event_path} — receive a single event
async fn handle_single_event(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(json): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, HttpError> {
    check_auth(&headers, &state.config.api_key)?;

    if let Some(limiter) = &state.rate_limiter {
        if !limiter.lock().await.allow() {
            return Err(HttpError::RateLimit);
        }
    }

    let event = json_to_event_from_json(&json);
    state
        .tx
        .send(event)
        .await
        .map_err(|_| HttpError::ChannelClosed)?;

    Ok(Json(serde_json::json!({"status": "ok"})))
}

/// POST /{batch_path} — receive a batch of events
async fn handle_batch_events(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(events): Json<Vec<serde_json::Value>>,
) -> Result<Json<serde_json::Value>, HttpError> {
    check_auth(&headers, &state.config.api_key)?;

    if events.len() > state.config.max_batch_size {
        return Err(HttpError::BatchTooLarge {
            actual: events.len(),
            max: state.config.max_batch_size,
        });
    }

    if let Some(limiter) = &state.rate_limiter {
        if !limiter.lock().await.allow() {
            return Err(HttpError::RateLimit);
        }
    }

    let mut count = 0;
    for json in events {
        let event = json_to_event_from_json(&json);
        state
            .tx
            .send(event)
            .await
            .map_err(|_| HttpError::ChannelClosed)?;
        count += 1;
    }

    Ok(Json(serde_json::json!({"status": "ok", "count": count})))
}

/// GET /health — health check
async fn handle_health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "healthy"}))
}

// =============================================================================
// HTTP Webhook Source (SourceConnector impl)
// =============================================================================

/// HTTP webhook source that receives events via HTTP POST
pub struct HttpWebhookSource {
    name: String,
    config: HttpWebhookConfig,
    running: std::sync::Arc<std::sync::atomic::AtomicBool>,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl std::fmt::Debug for HttpWebhookSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpWebhookSource").finish_non_exhaustive()
    }
}

impl HttpWebhookSource {
    /// Create a new HTTP webhook source with the given configuration.
    pub fn new(name: &str, config: HttpWebhookConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
            running: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            shutdown_tx: None,
        }
    }
}

#[async_trait]
impl SourceConnector for HttpWebhookSource {
    fn name(&self) -> &str {
        &self.name
    }

    #[tracing::instrument(level = "debug", skip(self, tx))]
    async fn start(&mut self, tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
        use std::sync::atomic::Ordering;

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);
        self.running.store(true, Ordering::SeqCst);

        let name = self.name.clone();
        let config = self.config.clone();
        let bind_addr: std::net::IpAddr = config
            .bind_address
            .parse()
            .map_err(|e| ConnectorError::ConfigError(format!("Invalid bind address: {e}")))?;

        // Rate limiter state (simple token bucket)
        let rate_limiter = (config.rate_limit > 0)
            .then(|| Arc::new(tokio::sync::Mutex::new(RateLimiter::new(config.rate_limit))));

        let event_path = config.event_path.trim_start_matches('/').to_string();
        let batch_path = config.batch_path.trim_start_matches('/').to_string();

        // Body size limits
        let single_body_limit = varpulis_connector_api::limits::MAX_EVENT_PAYLOAD_BYTES;
        let batch_body_limit =
            config.max_batch_size * varpulis_connector_api::limits::MAX_EVENT_PAYLOAD_BYTES;

        let state = HttpState {
            tx,
            config: config.clone(),
            rate_limiter,
        };

        let app = Router::new()
            .route(
                &format!("/{event_path}"),
                post(handle_single_event).layer(RequestBodyLimitLayer::new(single_body_limit)),
            )
            .route(
                &format!("/{batch_path}"),
                post(handle_batch_events).layer(RequestBodyLimitLayer::new(batch_body_limit)),
            )
            .route("/health", get(handle_health))
            .with_state(state);

        let addr = std::net::SocketAddr::new(bind_addr, config.port);

        info!("HTTP webhook source {} starting on {}", name, addr);
        info!("  Single event: POST {}", config.event_path);
        info!("  Batch events: POST {}", config.batch_path);
        if config.api_key.is_some() {
            info!("  Authentication: API key required (X-API-Key header)");
        }
        if config.rate_limit > 0 {
            info!("  Rate limit: {} req/s", config.rate_limit);
        }

        let name_clone = name;
        tokio::spawn(async move {
            let listener = match tokio::net::TcpListener::bind(addr).await {
                Ok(l) => l,
                Err(e) => {
                    error!(
                        "HTTP webhook source {} failed to bind to {}: {}",
                        name_clone, addr, e
                    );
                    return;
                }
            };

            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    shutdown_rx.await.ok();
                })
                .await
                .ok();

            info!("HTTP webhook source {} stopped", name_clone);
        });

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn stop(&mut self) -> Result<(), ConnectorError> {
        use std::sync::atomic::Ordering;
        self.running.store(false, Ordering::SeqCst);
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        info!("HTTP webhook source {} stopping", self.name);
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.running.load(std::sync::atomic::Ordering::SeqCst)
    }
}

// =============================================================================
// Rate Limiter
// =============================================================================

/// Simple rate limiter using token bucket
struct RateLimiter {
    tokens: f64,
    max_tokens: f64,
    refill_rate: f64,
    last_refill: std::time::Instant,
}

impl RateLimiter {
    fn new(rps: u32) -> Self {
        Self {
            tokens: rps as f64,
            max_tokens: rps as f64,
            refill_rate: rps as f64,
            last_refill: std::time::Instant::now(),
        }
    }

    fn allow(&mut self) -> bool {
        let now = std::time::Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = elapsed
            .mul_add(self.refill_rate, self.tokens)
            .min(self.max_tokens);
        self.last_refill = now;

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

// =============================================================================
// JSON-to-Event Helper
// =============================================================================

/// Convert JSON to Event (helper for webhook) with resource limits.
fn json_to_event_from_json(json: &serde_json::Value) -> Event {
    let event_type = json
        .get("event_type")
        .or_else(|| json.get("type"))
        .and_then(|v| v.as_str())
        .unwrap_or("WebhookEvent")
        .to_string();

    let mut event = Event::new(event_type);

    // Handle nested "data" field or top-level fields
    let fields = json.get("data").or(Some(json));
    if let Some(obj) = fields.and_then(|v| v.as_object()) {
        let mut count = 0;
        for (key, value) in obj {
            if key != "event_type" && key != "type" {
                if count >= varpulis_connector_api::limits::MAX_FIELDS_PER_EVENT {
                    break;
                }
                if let Some(v) = json_to_value(value) {
                    event = event.with_field(key.as_str(), v);
                    count += 1;
                }
            }
        }
    }

    event
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_config_debug_redacts_api_key() {
        let config = HttpWebhookConfig::default().with_api_key("secret-key-123");
        let rendered = format!("{config:?}");
        assert!(
            !rendered.contains("secret-key-123"),
            "api_key leaked in Debug: {rendered}"
        );
        assert!(
            rendered.contains("***REDACTED***"),
            "configured key should be masked"
        );
        // With no key, Debug shows None (not REDACTED) so it's clear none is set.
        let no_key = format!("{:?}", HttpWebhookConfig::default());
        assert!(no_key.contains("api_key: None"), "got: {no_key}");
    }

    #[test]
    fn test_http_webhook_config_default() {
        let config = HttpWebhookConfig::default();
        assert_eq!(config.port, 8080);
        assert_eq!(config.bind_address, "0.0.0.0");
        assert!(config.api_key.is_none());
        assert_eq!(config.rate_limit, 0);
        assert_eq!(config.max_batch_size, 1000);
        assert_eq!(config.event_path, "/event");
        assert_eq!(config.batch_path, "/events");
    }

    #[test]
    fn test_http_webhook_config_new() {
        let config = HttpWebhookConfig::new(9090);
        assert_eq!(config.port, 9090);
        assert_eq!(config.bind_address, "0.0.0.0");
    }

    #[test]
    fn test_http_webhook_config_builder_chain() {
        let config = HttpWebhookConfig::new(3000)
            .with_api_key("secret-key-123")
            .with_rate_limit(100)
            .with_bind_address("127.0.0.1");

        assert_eq!(config.port, 3000);
        assert_eq!(config.api_key.as_deref(), Some("secret-key-123"));
        assert_eq!(config.rate_limit, 100);
        assert_eq!(config.bind_address, "127.0.0.1");
    }

    #[test]
    fn test_http_webhook_config_serialization_roundtrip() {
        let config = HttpWebhookConfig::new(8081).with_api_key("key");
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: HttpWebhookConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.port, 8081);
        assert_eq!(deserialized.api_key.as_deref(), Some("key"));
    }

    #[test]
    fn test_http_sink_new() {
        let sink = HttpSink::new("test-sink", "http://localhost:8080/ingest");
        assert_eq!(sink.name, "test-sink");
        assert_eq!(sink.url, "http://localhost:8080/ingest");
        assert!(sink.headers.is_empty());
    }

    #[test]
    fn test_http_sink_with_headers() {
        let sink = HttpSink::new("s", "http://example.com")
            .with_header("Authorization", "Bearer token123")
            .with_header("X-Custom", "value");
        assert_eq!(sink.headers.len(), 2);
        assert_eq!(
            sink.headers.get("Authorization").unwrap(),
            "Bearer token123"
        );
        assert_eq!(sink.headers.get("X-Custom").unwrap(), "value");
    }

    #[test]
    fn test_check_auth_no_key_required() {
        let headers = HeaderMap::new();
        assert!(check_auth(&headers, &None).is_ok());
    }

    #[test]
    fn test_check_auth_valid_api_key() {
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", "my-secret".parse().unwrap());
        let expected = Some("my-secret".to_string());
        assert!(check_auth(&headers, &expected).is_ok());
    }

    #[test]
    fn test_check_auth_invalid_api_key() {
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", "wrong-key".parse().unwrap());
        let expected = Some("correct-key".to_string());
        assert!(check_auth(&headers, &expected).is_err());
    }

    #[test]
    fn test_check_auth_bearer_token() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer my-token".parse().unwrap());
        let expected = Some("my-token".to_string());
        assert!(check_auth(&headers, &expected).is_ok());
    }

    #[test]
    fn test_check_auth_missing_header() {
        let headers = HeaderMap::new();
        let expected = Some("required-key".to_string());
        assert!(check_auth(&headers, &expected).is_err());
    }

    #[test]
    fn test_json_to_event_from_json_basic() {
        let json = serde_json::json!({
            "event_type": "LoginEvent",
            "user": "alice",
            "ip": "10.0.0.1"
        });
        let event = json_to_event_from_json(&json);
        assert_eq!(event.event_type.as_ref(), "LoginEvent");
        assert!(event.get("user").is_some());
        assert!(event.get("ip").is_some());
    }

    #[test]
    fn test_json_to_event_from_json_with_type_field() {
        let json = serde_json::json!({
            "type": "OrderEvent",
            "amount": 42
        });
        let event = json_to_event_from_json(&json);
        assert_eq!(event.event_type.as_ref(), "OrderEvent");
    }

    #[test]
    fn test_json_to_event_from_json_no_type() {
        let json = serde_json::json!({"foo": "bar"});
        let event = json_to_event_from_json(&json);
        assert_eq!(event.event_type.as_ref(), "WebhookEvent");
    }

    #[test]
    fn test_json_to_event_from_json_nested_data() {
        let json = serde_json::json!({
            "event_type": "NestedEvent",
            "data": {
                "key1": "val1",
                "key2": 99
            }
        });
        let event = json_to_event_from_json(&json);
        assert_eq!(event.event_type.as_ref(), "NestedEvent");
        assert!(event.get("key1").is_some());
    }

    #[test]
    fn test_rate_limiter_allows_within_limit() {
        let mut limiter = RateLimiter::new(10);
        // First request should always be allowed
        assert!(limiter.allow());
    }

    #[test]
    fn test_rate_limiter_exhausts_tokens() {
        let mut limiter = RateLimiter::new(2);
        assert!(limiter.allow());
        assert!(limiter.allow());
        // Third request should be denied (no tokens left immediately)
        assert!(!limiter.allow());
    }

    #[test]
    fn test_http_info_static() {
        assert_eq!(HTTP_INFO.connector_type, "http");
        assert!(HTTP_INFO.supports_source);
        assert!(HTTP_INFO.supports_sink);
        assert!(!HTTP_INFO.supports_managed);
    }
}
