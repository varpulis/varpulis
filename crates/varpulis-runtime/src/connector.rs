//! External system connectors (MQTT, HTTP, Kafka, etc.)
//!
//! This module provides source and sink abstractions for connecting Varpulis
//! to external systems for event ingestion and output.
//!
//! # Architecture
//!
//! ```text
//! External System ─────> SourceConnector ─────> Engine ─────> SinkConnector ─────> External System
//!   (MQTT broker)          (MqttSource)                        (HttpSink)           (Webhook)
//! ```
//!
//! # Available Connectors
//!
//! | Connector | Feature Flag | Description |
//! |-----------|--------------|-------------|
//! | `MqttSource`/`MqttSink` | `mqtt` | MQTT broker connectivity |
//! | `HttpSink` | default | HTTP webhook output |
//! | `ConsoleSource`/`ConsoleSink` | default | Debug/testing connectors |
//!
//! # Example: MQTT Source
//!
//! ```rust,ignore
//! use varpulis_runtime::connector::{MqttConfig, MqttSource, SourceConnector};
//! use tokio::sync::mpsc;
//!
//! let config = MqttConfig::new("localhost", "events/#")
//!     .with_port(1883)
//!     .with_client_id("my-client");
//!
//! let mut source = MqttSource::new("mqtt-in", config);
//! let (tx, mut rx) = mpsc::channel(100);
//!
//! source.start(tx).await?;
//!
//! while let Some(event) = rx.recv().await {
//!     println!("Received: {:?}", event);
//! }
//! ```
//!
//! # Example: HTTP Sink
//!
//! ```rust,ignore
//! use varpulis_runtime::connector::{HttpSink, SinkConnector};
//! use varpulis_runtime::Event;
//!
//! let sink = HttpSink::new("webhook", "https://example.com/events");
//! let event = Event::new("Alert").with_field("message", "High temperature");
//!
//! sink.send(&event).await?;
//! ```

use crate::event::Event;
use async_trait::async_trait;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// Generic connector configuration.
///
/// This struct provides a unified configuration format for all connector types,
/// allowing dynamic connector creation based on configuration files.
///
/// # Fields
///
/// - `connector_type`: The type of connector (`"mqtt"`, `"kafka"`, `"http"`, etc.)
/// - `url`: Connection URL or address
/// - `topic`: Optional topic, channel, or path for the connection
/// - `properties`: Additional key-value properties specific to the connector type
///
/// # Example
///
/// ```rust
/// use varpulis_runtime::connector::ConnectorConfig;
///
/// let config = ConnectorConfig::new("mqtt", "localhost:1883")
///     .with_topic("sensors/#")
///     .with_property("qos", "1")
///     .with_property("client_id", "varpulis-prod");
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorConfig {
    /// Connector type identifier (e.g., "mqtt", "kafka", "http", "file")
    pub connector_type: String,
    /// Connection URL or address (format depends on connector type)
    pub url: String,
    /// Topic, channel, queue name, or file path (optional, depends on connector)
    pub topic: Option<String>,
    /// Additional connector-specific properties
    pub properties: IndexMap<String, String>,
}

impl ConnectorConfig {
    pub fn new(connector_type: &str, url: &str) -> Self {
        Self {
            connector_type: connector_type.to_string(),
            url: url.to_string(),
            topic: None,
            properties: IndexMap::new(),
        }
    }

    pub fn with_topic(mut self, topic: &str) -> Self {
        self.topic = Some(topic.to_string());
        self
    }

    pub fn with_property(mut self, key: &str, value: &str) -> Self {
        self.properties.insert(key.to_string(), value.to_string());
        self
    }
}

/// Trait for source connectors that ingest events from external systems.
///
/// Source connectors receive events from external systems (MQTT brokers, Kafka,
/// HTTP webhooks, files) and forward them to the Varpulis engine via a channel.
///
/// # Lifecycle
///
/// 1. Create the connector with configuration
/// 2. Call [`start`](Self::start) with a channel sender
/// 3. Connector runs asynchronously, sending events to the channel
/// 4. Call [`stop`](Self::stop) to gracefully shutdown
///
/// # Implementing a Custom Source
///
/// ```rust,ignore
/// use varpulis_runtime::connector::{SourceConnector, ConnectorError};
/// use varpulis_runtime::Event;
/// use async_trait::async_trait;
/// use tokio::sync::mpsc;
///
/// struct MySource {
///     name: String,
///     running: bool,
/// }
///
/// #[async_trait]
/// impl SourceConnector for MySource {
///     fn name(&self) -> &str { &self.name }
///
///     async fn start(&mut self, tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
///         self.running = true;
///         // Spawn task to receive events and send to tx
///         Ok(())
///     }
///
///     async fn stop(&mut self) -> Result<(), ConnectorError> {
///         self.running = false;
///         Ok(())
///     }
///
///     fn is_running(&self) -> bool { self.running }
/// }
/// ```
#[async_trait]
pub trait SourceConnector: Send + Sync {
    /// Returns the name/identifier of this connector instance.
    fn name(&self) -> &str;

    /// Start receiving events and forward them to the provided channel.
    ///
    /// This method should spawn background tasks to receive events from the
    /// external system. Events are sent via the `tx` channel to the engine.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConnectionFailed` if unable to connect.
    async fn start(&mut self, tx: mpsc::Sender<Event>) -> Result<(), ConnectorError>;

    /// Stop the connector gracefully.
    ///
    /// This should stop all background tasks and close connections.
    /// Outstanding events may be lost.
    async fn stop(&mut self) -> Result<(), ConnectorError>;

    /// Check if the connector is currently running.
    fn is_running(&self) -> bool;
}

/// Trait for sink connectors that send events to external systems.
///
/// Sink connectors receive events from the Varpulis engine and forward them
/// to external systems (HTTP webhooks, MQTT, Kafka, files, databases).
///
/// # Thread Safety
///
/// Sink connectors must be `Send + Sync` to allow sharing across async tasks.
/// The `send` method takes `&self` to allow concurrent sends.
///
/// # Implementing a Custom Sink
///
/// ```rust,ignore
/// use varpulis_runtime::connector::{SinkConnector, ConnectorError};
/// use varpulis_runtime::Event;
/// use async_trait::async_trait;
///
/// struct MySink {
///     name: String,
///     client: reqwest::Client,
///     url: String,
/// }
///
/// #[async_trait]
/// impl SinkConnector for MySink {
///     fn name(&self) -> &str { &self.name }
///
///     async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
///         self.client.post(&self.url)
///             .json(event)
///             .send()
///             .await
///             .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;
///         Ok(())
///     }
///
///     async fn flush(&self) -> Result<(), ConnectorError> { Ok(()) }
///     async fn close(&self) -> Result<(), ConnectorError> { Ok(()) }
/// }
/// ```
#[async_trait]
pub trait SinkConnector: Send + Sync {
    /// Returns the name/identifier of this connector instance.
    fn name(&self) -> &str;

    /// Establish connection to the external system.
    ///
    /// Called once after sink creation to establish any necessary connections.
    /// Implementations that connect eagerly in `new()` can return Ok(()) here.
    /// This method may be called multiple times; implementations should handle
    /// reconnection gracefully.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConnectionFailed` if connection cannot be established.
    async fn connect(&mut self) -> Result<(), ConnectorError> {
        // Default: no-op for sinks that connect eagerly
        Ok(())
    }

    /// Send an event to the external system.
    ///
    /// This method should be idempotent if possible. Implementations may
    /// buffer events internally for batching.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::SendFailed` on transmission errors.
    /// Returns `ConnectorError::NotConnected` if not connected.
    async fn send(&self, event: &Event) -> Result<(), ConnectorError>;

    /// Flush any internally buffered events.
    ///
    /// Call this to ensure all events have been transmitted before shutdown.
    async fn flush(&self) -> Result<(), ConnectorError>;

    /// Close the connector and release resources.
    ///
    /// This should flush remaining events and close connections.
    async fn close(&self) -> Result<(), ConnectorError>;
}

/// Errors that can occur during connector operations.
///
/// This enum covers all error conditions for both source and sink connectors,
/// including connection, transmission, and configuration errors.
///
/// # Error Handling Example
///
/// ```rust,ignore
/// use varpulis_runtime::connector::{SinkConnector, ConnectorError};
///
/// async fn send_with_retry(sink: &impl SinkConnector, event: &Event) -> Result<(), ConnectorError> {
///     for attempt in 0..3 {
///         match sink.send(event).await {
///             Ok(()) => return Ok(()),
///             Err(ConnectorError::SendFailed(msg)) if attempt < 2 => {
///                 eprintln!("Retry {}: {}", attempt + 1, msg);
///                 tokio::time::sleep(Duration::from_millis(100)).await;
///             }
///             Err(e) => return Err(e),
///         }
///     }
///     unreachable!()
/// }
/// ```
#[derive(Debug, thiserror::Error)]
pub enum ConnectorError {
    /// Failed to establish connection to the external system.
    /// Contains a description of the connection failure.
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    /// Failed to send/publish an event.
    /// This may be a transient error (network) or permanent (invalid event).
    #[error("Send failed: {0}")]
    SendFailed(String),

    /// Failed to receive an event from the source.
    /// May indicate connection loss or message parsing errors.
    #[error("Receive failed: {0}")]
    ReceiveFailed(String),

    /// Invalid or incomplete configuration.
    /// Check the connector documentation for required settings.
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// Operation attempted on a disconnected connector.
    /// Call `start()` or `connect()` first.
    #[error("Not connected")]
    NotConnected,

    /// Requested connector type is not available.
    /// May require enabling a feature flag (e.g., `mqtt`, `kafka`).
    #[error("Connector not available: {0}")]
    NotAvailable(String),
}

// =============================================================================
// Console Connector (for testing/debugging)
// =============================================================================

/// Console source - reads events from stdin (for testing)
pub struct ConsoleSource {
    name: String,
    running: bool,
}

impl ConsoleSource {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            running: false,
        }
    }
}

#[async_trait]
impl SourceConnector for ConsoleSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&mut self, _tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
        self.running = true;
        info!("Console source started: {}", self.name);
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        self.running = false;
        info!("Console source stopped: {}", self.name);
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.running
    }
}

/// Console sink - writes events to stdout
pub struct ConsoleSink {
    name: String,
    pretty: bool,
}

impl ConsoleSink {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            pretty: true,
        }
    }

    pub fn compact(mut self) -> Self {
        self.pretty = false;
        self
    }
}

#[async_trait]
impl SinkConnector for ConsoleSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
        if self.pretty {
            println!(
                "[{}] {} | {:?}",
                event.timestamp.format("%H:%M:%S"),
                event.event_type,
                event.data
            );
        } else {
            println!("{}", serde_json::to_string(event).unwrap_or_default());
        }
        Ok(())
    }

    async fn flush(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

// =============================================================================
// HTTP Webhook Connector
// =============================================================================

/// HTTP webhook sink - sends events to an HTTP endpoint
pub struct HttpSink {
    name: String,
    url: String,
    client: reqwest::Client,
    headers: IndexMap<String, String>,
}

impl HttpSink {
    pub fn new(name: &str, url: &str) -> Self {
        Self {
            name: name.to_string(),
            url: url.to_string(),
            client: reqwest::Client::new(),
            headers: IndexMap::new(),
        }
    }

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
#[derive(Debug, Clone)]
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
    pub fn new(port: u16) -> Self {
        Self {
            port,
            ..Default::default()
        }
    }

    pub fn with_api_key(mut self, key: &str) -> Self {
        self.api_key = Some(key.to_string());
        self
    }

    pub fn with_rate_limit(mut self, rps: u32) -> Self {
        self.rate_limit = rps;
        self
    }

    pub fn with_bind_address(mut self, addr: &str) -> Self {
        self.bind_address = addr.to_string();
        self
    }
}

/// HTTP webhook source that receives events via HTTP POST
pub struct HttpWebhookSource {
    name: String,
    config: HttpWebhookConfig,
    running: std::sync::Arc<std::sync::atomic::AtomicBool>,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl HttpWebhookSource {
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

    async fn start(&mut self, tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
        use std::sync::atomic::Ordering;
        use warp::Filter;

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);
        self.running.store(true, Ordering::SeqCst);

        let _running = self.running.clone();
        let name = self.name.clone();
        let config = self.config.clone();
        let bind_addr: std::net::IpAddr = config
            .bind_address
            .parse()
            .map_err(|e| ConnectorError::ConfigError(format!("Invalid bind address: {}", e)))?;

        // Rate limiter state (simple token bucket)
        let rate_limiter = (config.rate_limit > 0).then(|| {
            std::sync::Arc::new(tokio::sync::Mutex::new(RateLimiter::new(config.rate_limit)))
        });

        // Clone paths as owned strings for 'static lifetime
        let event_path: &'static str = Box::leak(
            config
                .event_path
                .trim_start_matches('/')
                .to_string()
                .into_boxed_str(),
        );
        let batch_path: &'static str = Box::leak(
            config
                .batch_path
                .trim_start_matches('/')
                .to_string()
                .into_boxed_str(),
        );

        // Clone for filters
        let tx_single = tx.clone();
        let tx_batch = tx;
        let api_key = config.api_key.clone();
        let api_key_batch = api_key.clone();
        let rate_limiter_single = rate_limiter.clone();
        let rate_limiter_batch = rate_limiter;
        let max_batch = config.max_batch_size;

        // API key validation filter
        let with_auth = warp::header::optional::<String>("x-api-key")
            .and(warp::header::optional::<String>("authorization"))
            .and_then(
                move |api_key_header: Option<String>, auth_header: Option<String>| {
                    let expected_key = api_key.clone();
                    async move {
                        if let Some(expected) = expected_key {
                            let provided = api_key_header.or_else(|| {
                                auth_header
                                    .and_then(|h| h.strip_prefix("Bearer ").map(|s| s.to_string()))
                            });

                            match provided {
                                Some(key) if key == expected => Ok(()),
                                _ => Err(warp::reject::custom(AuthError)),
                            }
                        } else {
                            Ok(())
                        }
                    }
                },
            );

        // Single event endpoint: POST /event
        let single_event = warp::post()
            .and(warp::path(event_path))
            .and(warp::path::end())
            .and(with_auth.clone())
            .and(warp::body::json::<serde_json::Value>())
            .and(warp::any().map(move || tx_single.clone()))
            .and(warp::any().map(move || rate_limiter_single.clone()))
            .and_then(
                |_: (),
                 json: serde_json::Value,
                 tx: mpsc::Sender<Event>,
                 limiter: Option<std::sync::Arc<tokio::sync::Mutex<RateLimiter>>>| async move {
                    // Rate limiting
                    if let Some(limiter) = limiter {
                        if !limiter.lock().await.allow() {
                            return Err(warp::reject::custom(RateLimitError));
                        }
                    }

                    let event = json_to_event_from_json(&json);
                    if tx.send(event).await.is_err() {
                        return Err(warp::reject::custom(ChannelClosedError));
                    }
                    Ok::<_, warp::Rejection>(warp::reply::json(
                        &serde_json::json!({"status": "ok"}),
                    ))
                },
            );

        // Batch auth filter
        let with_auth_batch = warp::header::optional::<String>("x-api-key")
            .and(warp::header::optional::<String>("authorization"))
            .and_then(
                move |api_key_header: Option<String>, auth_header: Option<String>| {
                    let expected_key = api_key_batch.clone();
                    async move {
                        if let Some(expected) = expected_key {
                            let provided = api_key_header.or_else(|| {
                                auth_header
                                    .and_then(|h| h.strip_prefix("Bearer ").map(|s| s.to_string()))
                            });

                            match provided {
                                Some(key) if key == expected => Ok(()),
                                _ => Err(warp::reject::custom(AuthError)),
                            }
                        } else {
                            Ok(())
                        }
                    }
                },
            );

        // Batch event endpoint: POST /events
        let batch_events = warp::post()
            .and(warp::path(batch_path))
            .and(warp::path::end())
            .and(with_auth_batch)
            .and(warp::body::json::<Vec<serde_json::Value>>())
            .and(warp::any().map(move || tx_batch.clone()))
            .and(warp::any().map(move || rate_limiter_batch.clone()))
            .and(warp::any().map(move || max_batch))
            .and_then(
                |_: (),
                 events: Vec<serde_json::Value>,
                 tx: mpsc::Sender<Event>,
                 limiter: Option<std::sync::Arc<tokio::sync::Mutex<RateLimiter>>>,
                 max_batch: usize| async move {
                    // Check batch size
                    if events.len() > max_batch {
                        return Err(warp::reject::custom(BatchTooLargeError(
                            events.len(),
                            max_batch,
                        )));
                    }

                    // Rate limiting (counts as one request)
                    if let Some(limiter) = limiter {
                        if !limiter.lock().await.allow() {
                            return Err(warp::reject::custom(RateLimitError));
                        }
                    }

                    let mut count = 0;
                    for json in events {
                        let event = json_to_event_from_json(&json);
                        if tx.send(event).await.is_err() {
                            return Err(warp::reject::custom(ChannelClosedError));
                        }
                        count += 1;
                    }

                    Ok::<_, warp::Rejection>(warp::reply::json(&serde_json::json!({
                        "status": "ok",
                        "count": count
                    })))
                },
            );

        // Health endpoint
        let health = warp::get()
            .and(warp::path("health"))
            .map(|| warp::reply::json(&serde_json::json!({"status": "healthy"})));

        // Combine routes with error handling
        let routes = single_event
            .or(batch_events)
            .or(health)
            .recover(handle_rejection);

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
            let (_, server) = warp::serve(routes).bind_with_graceful_shutdown(addr, async {
                shutdown_rx.await.ok();
            });

            server.await;
            info!("HTTP webhook source {} stopped", name_clone);
        });

        Ok(())
    }

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
        self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.max_tokens);
        self.last_refill = now;

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

// Custom rejection types
#[derive(Debug)]
struct AuthError;
impl warp::reject::Reject for AuthError {}

#[derive(Debug)]
struct RateLimitError;
impl warp::reject::Reject for RateLimitError {}

#[derive(Debug)]
struct ChannelClosedError;
impl warp::reject::Reject for ChannelClosedError {}

#[derive(Debug)]
struct BatchTooLargeError(usize, usize);
impl warp::reject::Reject for BatchTooLargeError {}

async fn handle_rejection(
    err: warp::Rejection,
) -> Result<impl warp::Reply, std::convert::Infallible> {
    let (code, message) = if err.is_not_found() {
        (warp::http::StatusCode::NOT_FOUND, "Not found")
    } else if err.find::<AuthError>().is_some() {
        (
            warp::http::StatusCode::UNAUTHORIZED,
            "Invalid or missing API key",
        )
    } else if err.find::<RateLimitError>().is_some() {
        (
            warp::http::StatusCode::TOO_MANY_REQUESTS,
            "Rate limit exceeded",
        )
    } else if err.find::<ChannelClosedError>().is_some() {
        (
            warp::http::StatusCode::SERVICE_UNAVAILABLE,
            "Service unavailable",
        )
    } else if let Some(e) = err.find::<BatchTooLargeError>() {
        return Ok(warp::reply::with_status(
            warp::reply::json(&serde_json::json!({
                "error": format!("Batch too large: {} events (max: {})", e.0, e.1)
            })),
            warp::http::StatusCode::PAYLOAD_TOO_LARGE,
        ));
    } else if err.find::<warp::reject::PayloadTooLarge>().is_some() {
        (
            warp::http::StatusCode::PAYLOAD_TOO_LARGE,
            "Payload too large",
        )
    } else if err.find::<warp::reject::InvalidHeader>().is_some() {
        (warp::http::StatusCode::BAD_REQUEST, "Invalid header")
    } else {
        (warp::http::StatusCode::BAD_REQUEST, "Bad request")
    };

    Ok(warp::reply::with_status(
        warp::reply::json(&serde_json::json!({"error": message})),
        code,
    ))
}

/// Convert JSON to Event (helper for webhook)
fn json_to_event_from_json(json: &serde_json::Value) -> Event {
    let event_type = json
        .get("event_type")
        .or_else(|| json.get("type"))
        .and_then(|v| v.as_str())
        .unwrap_or("WebhookEvent")
        .to_string();

    let mut event = Event::new(&event_type);

    // Handle nested "data" field or top-level fields
    let fields = json.get("data").or(Some(json));
    if let Some(obj) = fields.and_then(|v| v.as_object()) {
        for (key, value) in obj {
            if key != "event_type" && key != "type" {
                if let Some(v) = json_to_value(value) {
                    event = event.with_field(key, v);
                }
            }
        }
    }

    event
}

// =============================================================================
// Kafka Connector (stub - requires rdkafka feature)
// =============================================================================

/// Kafka configuration
#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub brokers: String,
    pub topic: String,
    pub group_id: Option<String>,
    pub properties: IndexMap<String, String>,
}

impl KafkaConfig {
    pub fn new(brokers: &str, topic: &str) -> Self {
        Self {
            brokers: brokers.to_string(),
            topic: topic.to_string(),
            group_id: None,
            properties: IndexMap::new(),
        }
    }

    pub fn with_group_id(mut self, group_id: &str) -> Self {
        self.group_id = Some(group_id.to_string());
        self
    }
}

/// Kafka source connector (stub implementation)
pub struct KafkaSource {
    name: String,
    config: KafkaConfig,
    running: bool,
}

impl KafkaSource {
    pub fn new(name: &str, config: KafkaConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
            running: false,
        }
    }
}

#[async_trait]
impl SourceConnector for KafkaSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&mut self, _tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
        // NOTE: Full implementation requires rdkafka crate
        // This is a stub that shows the interface
        warn!(
            "Kafka source {} starting (stub implementation - requires rdkafka feature)",
            self.name
        );
        warn!("  Brokers: {}", self.config.brokers);
        warn!("  Topic: {}", self.config.topic);

        self.running = true;

        // In a real implementation, this would:
        // 1. Create a Kafka consumer
        // 2. Subscribe to the topic
        // 3. Poll for messages and convert to Events
        // 4. Send events through the tx channel

        Err(ConnectorError::NotAvailable(
            "Kafka connector requires 'kafka' feature. Enable with: cargo build --features kafka"
                .to_string(),
        ))
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        self.running = false;
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.running
    }
}

/// Kafka sink connector (stub implementation)
pub struct KafkaSink {
    name: String,
    config: KafkaConfig,
}

impl KafkaSink {
    pub fn new(name: &str, config: KafkaConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
        }
    }
}

#[async_trait]
impl SinkConnector for KafkaSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
        // NOTE: Full implementation requires rdkafka crate
        warn!(
            "Kafka sink {} send (stub) to {}/{} - event: {}",
            self.name, self.config.brokers, self.config.topic, event.event_type
        );

        Err(ConnectorError::NotAvailable(
            "Kafka connector requires 'kafka' feature".to_string(),
        ))
    }

    async fn flush(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

// =============================================================================
// MQTT Connector
// =============================================================================

/// MQTT configuration
#[derive(Debug, Clone)]
pub struct MqttConfig {
    pub broker: String,
    pub port: u16,
    pub topic: String,
    pub client_id: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub qos: u8,
}

impl MqttConfig {
    pub fn new(broker: &str, topic: &str) -> Self {
        Self {
            broker: broker.to_string(),
            port: 1883,
            topic: topic.to_string(),
            client_id: None,
            username: None,
            password: None,
            qos: 1,
        }
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn with_client_id(mut self, client_id: &str) -> Self {
        self.client_id = Some(client_id.to_string());
        self
    }

    pub fn with_credentials(mut self, username: &str, password: &str) -> Self {
        self.username = Some(username.to_string());
        self.password = Some(password.to_string());
        self
    }

    pub fn with_qos(mut self, qos: u8) -> Self {
        self.qos = qos.min(2);
        self
    }
}

// -----------------------------------------------------------------------------
// MQTT with rumqttc feature enabled
// -----------------------------------------------------------------------------
#[cfg(feature = "mqtt")]
mod mqtt_impl {
    use super::*;
    use rumqttc::{AsyncClient, Event as MqttEvent, MqttOptions, Packet, QoS};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    fn qos_from_u8(qos: u8) -> QoS {
        match qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            _ => QoS::ExactlyOnce,
        }
    }

    /// MQTT source connector with rumqttc
    pub struct MqttSource {
        name: String,
        config: MqttConfig,
        running: Arc<AtomicBool>,
        client: Option<AsyncClient>,
    }

    impl MqttSource {
        pub fn new(name: &str, config: MqttConfig) -> Self {
            Self {
                name: name.to_string(),
                config,
                running: Arc::new(AtomicBool::new(false)),
                client: None,
            }
        }
    }

    #[async_trait]
    impl SourceConnector for MqttSource {
        fn name(&self) -> &str {
            &self.name
        }

        async fn start(&mut self, tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
            let client_id = self
                .config
                .client_id
                .clone()
                .unwrap_or_else(|| format!("varpulis-src-{}", std::process::id()));

            let mut mqtt_opts = MqttOptions::new(client_id, &self.config.broker, self.config.port);
            mqtt_opts.set_keep_alive(Duration::from_secs(60));

            if let (Some(user), Some(pass)) = (&self.config.username, &self.config.password) {
                mqtt_opts.set_credentials(user, pass);
            }

            let (client, mut eventloop) = AsyncClient::new(mqtt_opts, 100);

            client
                .subscribe(&self.config.topic, qos_from_u8(self.config.qos))
                .await
                .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

            self.client = Some(client);
            self.running.store(true, Ordering::SeqCst);

            info!(
                "MQTT source {} connected to {}:{}",
                self.name, self.config.broker, self.config.port
            );
            info!("  Subscribed to: {}", self.config.topic);

            let running = self.running.clone();
            let name = self.name.clone();

            tokio::spawn(async move {
                let mut consecutive_errors: u32 = 0;
                const MAX_CONSECUTIVE_ERRORS: u32 = 10;
                const MAX_BACKOFF_SECS: u64 = 30;

                while running.load(Ordering::SeqCst) {
                    match eventloop.poll().await {
                        Ok(MqttEvent::Incoming(Packet::Publish(publish))) => {
                            // Reset error counter on successful message
                            consecutive_errors = 0;
                            if let Ok(payload) = std::str::from_utf8(&publish.payload) {
                                if let Ok(json) = serde_json::from_str::<serde_json::Value>(payload)
                                {
                                    // Extract event type from topic (last segment) or from JSON
                                    let topic = &publish.topic;
                                    let event = json_to_event_with_topic(&json, topic);
                                    if tx.send(event).await.is_err() {
                                        warn!("MQTT source {} channel closed", name);
                                        break;
                                    }
                                }
                            }
                        }
                        Ok(_) => {
                            // Other successful events (ConnAck, SubAck, etc.) reset error counter
                            consecutive_errors = 0;
                        }
                        Err(e) => {
                            consecutive_errors += 1;

                            // Check if we've exceeded the error limit
                            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                                error!(
                                    "MQTT source {} exceeded max consecutive errors ({}), stopping",
                                    name, MAX_CONSECUTIVE_ERRORS
                                );
                                running.store(false, Ordering::SeqCst);
                                break;
                            }

                            // Exponential backoff: 1s, 2s, 4s, 8s, ... up to MAX_BACKOFF_SECS
                            let backoff_secs =
                                (1u64 << (consecutive_errors - 1).min(5)).min(MAX_BACKOFF_SECS);

                            warn!(
                                "MQTT source {} error (attempt {}/{}): {:?}, retrying in {}s",
                                name, consecutive_errors, MAX_CONSECUTIVE_ERRORS, e, backoff_secs
                            );

                            tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                        }
                    }
                }
                info!("MQTT source {} eventloop stopped", name);
            });

            Ok(())
        }

        async fn stop(&mut self) -> Result<(), ConnectorError> {
            self.running.store(false, Ordering::SeqCst);
            if let Some(client) = &self.client {
                let _ = client.disconnect().await;
            }
            info!("MQTT source {} stopped", self.name);
            Ok(())
        }

        fn is_running(&self) -> bool {
            self.running.load(Ordering::SeqCst)
        }
    }

    /// MQTT sink connector with rumqttc
    pub struct MqttSink {
        name: String,
        config: MqttConfig,
        client: Option<AsyncClient>,
    }

    impl MqttSink {
        pub fn new(name: &str, config: MqttConfig) -> Self {
            Self {
                name: name.to_string(),
                config,
                client: None,
            }
        }

        pub async fn connect(&mut self) -> Result<(), ConnectorError> {
            let client_id = self
                .config
                .client_id
                .clone()
                .unwrap_or_else(|| format!("varpulis-sink-{}", std::process::id()));

            let mut mqtt_opts = MqttOptions::new(client_id, &self.config.broker, self.config.port);
            mqtt_opts.set_keep_alive(Duration::from_secs(60));

            if let (Some(user), Some(pass)) = (&self.config.username, &self.config.password) {
                mqtt_opts.set_credentials(user, pass);
            }

            let (client, mut eventloop) = AsyncClient::new(mqtt_opts, 100);
            self.client = Some(client);

            // Spawn eventloop handler
            tokio::spawn(async move {
                loop {
                    if eventloop.poll().await.is_err() {
                        break;
                    }
                }
            });

            info!(
                "MQTT sink {} connected to {}:{}",
                self.name, self.config.broker, self.config.port
            );
            Ok(())
        }
    }

    #[async_trait]
    impl SinkConnector for MqttSink {
        fn name(&self) -> &str {
            &self.name
        }

        async fn connect(&mut self) -> Result<(), ConnectorError> {
            // Delegate to the inherent connect method
            MqttSink::connect(self).await
        }

        async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
            let client = self.client.as_ref().ok_or(ConnectorError::NotConnected)?;

            let payload =
                serde_json::to_vec(event).map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

            let topic = format!("{}/{}", self.config.topic, event.event_type);

            client
                .publish(&topic, qos_from_u8(self.config.qos), false, payload)
                .await
                .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

            Ok(())
        }

        async fn flush(&self) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn close(&self) -> Result<(), ConnectorError> {
            if let Some(client) = &self.client {
                let _ = client.disconnect().await;
            }
            Ok(())
        }
    }

    /// Convert JSON to Event, using the MQTT topic to determine event type if not in JSON
    fn json_to_event_with_topic(json: &serde_json::Value, topic: &str) -> Event {
        // Priority: 1) event_type field, 2) type field, 3) last segment of MQTT topic
        let event_type = json
            .get("event_type")
            .or_else(|| json.get("type"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| {
                // Extract event type from MQTT topic (last segment)
                // e.g., "benchmark/input/MarketATick" -> "MarketATick"
                topic
                    .rsplit('/')
                    .next()
                    .filter(|s| !s.is_empty())
                    .unwrap_or("Unknown")
                    .to_string()
            });

        let mut event = Event::new(&event_type);

        // First try nested "data" object, then fall back to top-level fields
        if let Some(data) = json.get("data").and_then(|v| v.as_object()) {
            for (k, v) in data {
                event = event.with_field(k, json_value_to_native(v));
            }
        } else if let Some(obj) = json.as_object() {
            // Parse fields directly from root object (excluding type fields)
            for (k, v) in obj {
                if k != "event_type" && k != "type" {
                    event = event.with_field(k, json_value_to_native(v));
                }
            }
        }

        event
    }

    fn json_value_to_native(v: &serde_json::Value) -> impl Into<varpulis_core::Value> {
        match v {
            serde_json::Value::Bool(b) => varpulis_core::Value::Bool(*b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    varpulis_core::Value::Int(i)
                } else {
                    varpulis_core::Value::Float(n.as_f64().unwrap_or(0.0))
                }
            }
            serde_json::Value::String(s) => varpulis_core::Value::Str(s.clone()),
            _ => varpulis_core::Value::Null,
        }
    }
}

// -----------------------------------------------------------------------------
// MQTT stub when feature disabled
// -----------------------------------------------------------------------------
#[cfg(not(feature = "mqtt"))]
mod mqtt_impl {
    use super::*;

    pub struct MqttSource {
        name: String,
        #[allow(dead_code)]
        config: MqttConfig,
        running: bool,
    }

    impl MqttSource {
        pub fn new(name: &str, config: MqttConfig) -> Self {
            Self {
                name: name.to_string(),
                config,
                running: false,
            }
        }
    }

    #[async_trait]
    impl SourceConnector for MqttSource {
        fn name(&self) -> &str {
            &self.name
        }

        async fn start(&mut self, _tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
            Err(ConnectorError::NotAvailable(
                "MQTT requires 'mqtt' feature. Build with: cargo build --features mqtt".to_string(),
            ))
        }

        async fn stop(&mut self) -> Result<(), ConnectorError> {
            self.running = false;
            Ok(())
        }

        fn is_running(&self) -> bool {
            self.running
        }
    }

    pub struct MqttSink {
        name: String,
        #[allow(dead_code)]
        config: MqttConfig,
    }

    impl MqttSink {
        pub fn new(name: &str, config: MqttConfig) -> Self {
            Self {
                name: name.to_string(),
                config,
            }
        }
    }

    #[async_trait]
    impl SinkConnector for MqttSink {
        fn name(&self) -> &str {
            &self.name
        }

        async fn send(&self, _event: &Event) -> Result<(), ConnectorError> {
            Err(ConnectorError::NotAvailable(
                "MQTT requires 'mqtt' feature".to_string(),
            ))
        }

        async fn flush(&self) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn close(&self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }
}

pub use mqtt_impl::{MqttSink, MqttSource};

// =============================================================================
// Kafka Connector (full implementation with rdkafka feature)
// =============================================================================

#[cfg(feature = "kafka")]
mod kafka_impl {
    use super::*;
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
    use rdkafka::Message;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    /// Kafka source connector with rdkafka
    pub struct KafkaSourceImpl {
        name: String,
        config: KafkaConfig,
        running: Arc<AtomicBool>,
    }

    impl KafkaSourceImpl {
        pub fn new(name: &str, config: KafkaConfig) -> Self {
            Self {
                name: name.to_string(),
                config,
                running: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    #[async_trait]
    impl SourceConnector for KafkaSourceImpl {
        fn name(&self) -> &str {
            &self.name
        }

        async fn start(&mut self, tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
            let group_id = self.config.group_id.clone().unwrap_or_else(|| {
                // Generate a unique group ID using process ID and timestamp
                let ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0);
                format!("varpulis-{}-{}", std::process::id(), ts)
            });

            let consumer: StreamConsumer = ClientConfig::new()
                .set("bootstrap.servers", &self.config.brokers)
                .set("group.id", &group_id)
                .set("enable.auto.commit", "true")
                .set("auto.offset.reset", "latest")
                .create()
                .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

            consumer
                .subscribe(&[&self.config.topic])
                .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

            self.running.store(true, Ordering::SeqCst);
            let running = self.running.clone();
            let name = self.name.clone();

            tokio::spawn(async move {
                info!("Kafka source {} started, consuming from topic", name);

                use futures::StreamExt;
                let mut stream = consumer.stream();

                while running.load(Ordering::SeqCst) {
                    match tokio::time::timeout(Duration::from_millis(100), stream.next()).await {
                        Ok(Some(Ok(msg))) => {
                            if let Some(payload) = msg.payload() {
                                match serde_json::from_slice::<serde_json::Value>(payload) {
                                    Ok(json) => {
                                        let event_type = json
                                            .get("event_type")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or("KafkaEvent")
                                            .to_string();

                                        let mut event = Event::new(&event_type);

                                        if let Some(obj) = json.as_object() {
                                            for (key, value) in obj {
                                                if key != "event_type" {
                                                    if let Some(v) =
                                                        crate::connector::json_to_value(value)
                                                    {
                                                        event = event.with_field(key, v);
                                                    }
                                                }
                                            }
                                        }

                                        if tx.send(event).await.is_err() {
                                            warn!("Kafka source {}: channel closed", name);
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        warn!("Kafka source {}: failed to parse JSON: {}", name, e);
                                    }
                                }
                            }
                        }
                        Ok(Some(Err(e))) => {
                            error!("Kafka source {} error: {}", name, e);
                        }
                        Ok(None) => break,
                        Err(_) => {} // Timeout, continue
                    }
                }

                info!("Kafka source {} stopped", name);
            });

            Ok(())
        }

        async fn stop(&mut self) -> Result<(), ConnectorError> {
            self.running.store(false, Ordering::SeqCst);
            Ok(())
        }

        fn is_running(&self) -> bool {
            self.running.load(Ordering::SeqCst)
        }
    }

    /// Kafka sink connector with rdkafka
    pub struct KafkaSinkImpl {
        name: String,
        config: KafkaConfig,
        producer: FutureProducer,
    }

    impl KafkaSinkImpl {
        pub fn new(name: &str, config: KafkaConfig) -> Result<Self, ConnectorError> {
            let producer: FutureProducer = ClientConfig::new()
                .set("bootstrap.servers", &config.brokers)
                .set("message.timeout.ms", "5000")
                .create()
                .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

            Ok(Self {
                name: name.to_string(),
                config,
                producer,
            })
        }
    }

    #[async_trait]
    impl SinkConnector for KafkaSinkImpl {
        fn name(&self) -> &str {
            &self.name
        }

        async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
            let payload = serde_json::to_string(event)
                .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

            let record = FutureRecord::to(&self.config.topic)
                .payload(&payload)
                .key(&event.event_type);

            self.producer
                .send(record, Duration::from_secs(5))
                .await
                .map_err(|(e, _)| ConnectorError::SendFailed(e.to_string()))?;

            Ok(())
        }

        async fn flush(&self) -> Result<(), ConnectorError> {
            self.producer
                .flush(Duration::from_secs(5))
                .map_err(|e| ConnectorError::SendFailed(format!("Flush failed: {}", e)))
        }

        async fn close(&self) -> Result<(), ConnectorError> {
            self.flush().await
        }
    }
}

#[cfg(feature = "kafka")]
pub use kafka_impl::{KafkaSinkImpl as KafkaSinkFull, KafkaSourceImpl as KafkaSourceFull};

// =============================================================================
// REST API Client (for calling external APIs)
// =============================================================================

/// REST API client configuration
#[derive(Debug, Clone)]
pub struct RestApiConfig {
    pub base_url: String,
    pub headers: IndexMap<String, String>,
    pub timeout_ms: u64,
    pub retry_count: u32,
}

impl RestApiConfig {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.to_string(),
            headers: IndexMap::new(),
            timeout_ms: 5000,
            retry_count: 3,
        }
    }

    pub fn with_header(mut self, key: &str, value: &str) -> Self {
        self.headers.insert(key.to_string(), value.to_string());
        self
    }

    pub fn with_bearer_token(mut self, token: &str) -> Self {
        self.headers
            .insert("Authorization".to_string(), format!("Bearer {}", token));
        self
    }

    pub fn with_api_key(mut self, key: &str, value: &str) -> Self {
        self.headers.insert(key.to_string(), value.to_string());
        self
    }

    pub fn with_timeout(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }
}

/// REST API client for calling external APIs
pub struct RestApiClient {
    #[allow(dead_code)]
    name: String,
    config: RestApiConfig,
    client: reqwest::Client,
}

impl RestApiClient {
    pub fn new(name: &str, config: RestApiConfig) -> Result<Self, ConnectorError> {
        let mut headers = reqwest::header::HeaderMap::new();
        for (key, value) in &config.headers {
            headers.insert(
                reqwest::header::HeaderName::from_bytes(key.as_bytes())
                    .map_err(|e| ConnectorError::ConfigError(e.to_string()))?,
                reqwest::header::HeaderValue::from_str(value)
                    .map_err(|e| ConnectorError::ConfigError(e.to_string()))?,
            );
        }

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(std::time::Duration::from_millis(config.timeout_ms))
            .build()
            .map_err(|e| ConnectorError::ConfigError(e.to_string()))?;

        Ok(Self {
            name: name.to_string(),
            config,
            client,
        })
    }

    /// GET request returning JSON as Event
    pub async fn get(&self, path: &str) -> Result<Event, ConnectorError> {
        let url = format!("{}{}", self.config.base_url, path);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| ConnectorError::ReceiveFailed(e.to_string()))?;

        if !response.status().is_success() {
            return Err(ConnectorError::ReceiveFailed(format!(
                "HTTP {}: {}",
                response.status(),
                url
            )));
        }

        let json: serde_json::Value = response
            .json()
            .await
            .map_err(|e| ConnectorError::ReceiveFailed(e.to_string()))?;

        Ok(json_to_event("ApiResponse", &json))
    }

    /// POST request with Event data
    pub async fn post(&self, path: &str, event: &Event) -> Result<Event, ConnectorError> {
        let url = format!("{}{}", self.config.base_url, path);
        let response = self
            .client
            .post(&url)
            .json(event)
            .send()
            .await
            .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

        if !response.status().is_success() {
            return Err(ConnectorError::SendFailed(format!(
                "HTTP {}: {}",
                response.status(),
                url
            )));
        }

        let json: serde_json::Value = response
            .json()
            .await
            .map_err(|e| ConnectorError::ReceiveFailed(e.to_string()))?;

        Ok(json_to_event("ApiResponse", &json))
    }

    /// PUT request with Event data
    pub async fn put(&self, path: &str, event: &Event) -> Result<Event, ConnectorError> {
        let url = format!("{}{}", self.config.base_url, path);
        let response = self
            .client
            .put(&url)
            .json(event)
            .send()
            .await
            .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

        if !response.status().is_success() {
            return Err(ConnectorError::SendFailed(format!(
                "HTTP {}: {}",
                response.status(),
                url
            )));
        }

        let json: serde_json::Value = response
            .json()
            .await
            .map_err(|e| ConnectorError::ReceiveFailed(e.to_string()))?;

        Ok(json_to_event("ApiResponse", &json))
    }

    /// DELETE request
    pub async fn delete(&self, path: &str) -> Result<(), ConnectorError> {
        let url = format!("{}{}", self.config.base_url, path);
        let response = self
            .client
            .delete(&url)
            .send()
            .await
            .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

        if !response.status().is_success() {
            return Err(ConnectorError::SendFailed(format!(
                "HTTP {}: {}",
                response.status(),
                url
            )));
        }

        Ok(())
    }
}

/// REST API sink that POSTs events to an endpoint
pub struct RestApiSink {
    name: String,
    client: RestApiClient,
    path: String,
}

impl RestApiSink {
    pub fn new(name: &str, config: RestApiConfig, path: &str) -> Result<Self, ConnectorError> {
        Ok(Self {
            name: name.to_string(),
            client: RestApiClient::new(name, config)?,
            path: path.to_string(),
        })
    }
}

#[async_trait]
impl SinkConnector for RestApiSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
        self.client.post(&self.path, event).await?;
        Ok(())
    }

    async fn flush(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

// =============================================================================
// Database Connector (PostgreSQL/MySQL/SQLite with sqlx)
// =============================================================================

/// Database configuration
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub connection_string: String,
    pub table: String,
    pub max_connections: u32,
}

impl DatabaseConfig {
    pub fn new(connection_string: &str, table: &str) -> Self {
        Self {
            connection_string: connection_string.to_string(),
            table: table.to_string(),
            max_connections: 5,
        }
    }

    pub fn with_max_connections(mut self, max: u32) -> Self {
        self.max_connections = max;
        self
    }
}

#[cfg(feature = "database")]
mod database_impl {
    use super::*;
    use sqlx::pool::PoolOptions;
    use sqlx::{AnyPool, Row};

    /// Ensure default Any drivers are installed (idempotent).
    fn ensure_drivers() {
        sqlx::any::install_default_drivers();
    }

    /// Database source that polls for new events
    pub struct DatabaseSource {
        name: String,
        config: DatabaseConfig,
        pool: Option<AnyPool>,
        running: bool,
        last_id: i64,
    }

    impl DatabaseSource {
        pub fn new(name: &str, config: DatabaseConfig) -> Self {
            Self {
                name: name.to_string(),
                config,
                pool: None,
                running: false,
                last_id: 0,
            }
        }
    }

    #[async_trait]
    impl SourceConnector for DatabaseSource {
        fn name(&self) -> &str {
            &self.name
        }

        async fn start(&mut self, tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
            ensure_drivers();
            let pool = PoolOptions::<sqlx::Any>::new()
                .max_connections(self.config.max_connections)
                .connect(&self.config.connection_string)
                .await
                .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

            self.pool = Some(pool.clone());
            self.running = true;

            let table = self.config.table.clone();
            let name = self.name.clone();
            let mut last_id = self.last_id;

            tokio::spawn(async move {
                info!("Database source {} started, polling table {}", name, table);

                while let Ok(_) = tx.reserve().await {
                    let query = format!(
                        "SELECT * FROM {} WHERE id > {} ORDER BY id LIMIT 100",
                        table, last_id
                    );

                    match sqlx::query(&query).fetch_all(&pool).await {
                        Ok(rows) => {
                            for row in rows {
                                let id: i64 = row.try_get("id").unwrap_or(0);
                                last_id = last_id.max(id);

                                let event_type: String = row
                                    .try_get("event_type")
                                    .unwrap_or_else(|_| "DatabaseEvent".to_string());

                                let mut event = Event::new(&event_type);

                                // Try to get common columns
                                if let Ok(data) = row.try_get::<String, _>("data") {
                                    if let Ok(json) =
                                        serde_json::from_str::<serde_json::Value>(&data)
                                    {
                                        if let Some(obj) = json.as_object() {
                                            for (key, value) in obj {
                                                if let Some(v) = json_to_value(value) {
                                                    event = event.with_field(key, v);
                                                }
                                            }
                                        }
                                    }
                                }

                                if tx.send(event).await.is_err() {
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            error!("Database source {} query error: {}", name, e);
                        }
                    }

                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }

                info!("Database source {} stopped", name);
            });

            Ok(())
        }

        async fn stop(&mut self) -> Result<(), ConnectorError> {
            self.running = false;
            if let Some(pool) = self.pool.take() {
                pool.close().await;
            }
            Ok(())
        }

        fn is_running(&self) -> bool {
            self.running
        }
    }

    /// Database sink that inserts events
    pub struct DatabaseSink {
        name: String,
        config: DatabaseConfig,
        pool: AnyPool,
    }

    impl DatabaseSink {
        pub async fn new(name: &str, config: DatabaseConfig) -> Result<Self, ConnectorError> {
            ensure_drivers();
            let pool = PoolOptions::<sqlx::Any>::new()
                .max_connections(config.max_connections)
                .connect(&config.connection_string)
                .await
                .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

            Ok(Self {
                name: name.to_string(),
                config,
                pool,
            })
        }
    }

    #[async_trait]
    impl SinkConnector for DatabaseSink {
        fn name(&self) -> &str {
            &self.name
        }

        async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
            let data = serde_json::to_string(event)
                .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

            let query = format!(
                "INSERT INTO {} (event_type, data, timestamp) VALUES ($1, $2, $3)",
                self.config.table
            );

            sqlx::query(&query)
                .bind(&event.event_type)
                .bind(&data)
                .bind(event.timestamp.to_rfc3339())
                .execute(&self.pool)
                .await
                .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

            Ok(())
        }

        async fn flush(&self) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn close(&self) -> Result<(), ConnectorError> {
            self.pool.close().await;
            Ok(())
        }
    }
}

#[cfg(feature = "database")]
pub use database_impl::{DatabaseSink, DatabaseSource};

#[cfg(not(feature = "database"))]
pub struct DatabaseSource {
    name: String,
    #[allow(dead_code)]
    config: DatabaseConfig,
}

#[cfg(not(feature = "database"))]
impl DatabaseSource {
    pub fn new(name: &str, config: DatabaseConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
        }
    }
}

#[cfg(not(feature = "database"))]
#[async_trait]
impl SourceConnector for DatabaseSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&mut self, _tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
        Err(ConnectorError::NotAvailable(
            "Database connector requires 'database' feature".to_string(),
        ))
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn is_running(&self) -> bool {
        false
    }
}

#[cfg(not(feature = "database"))]
pub struct DatabaseSink {
    name: String,
    #[allow(dead_code)]
    config: DatabaseConfig,
}

#[cfg(not(feature = "database"))]
impl DatabaseSink {
    pub async fn new(name: &str, config: DatabaseConfig) -> Result<Self, ConnectorError> {
        Ok(Self {
            name: name.to_string(),
            config,
        })
    }
}

#[cfg(not(feature = "database"))]
#[async_trait]
impl SinkConnector for DatabaseSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, _event: &Event) -> Result<(), ConnectorError> {
        Err(ConnectorError::NotAvailable(
            "Database connector requires 'database' feature".to_string(),
        ))
    }

    async fn flush(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

// =============================================================================
// Redis Connector
// =============================================================================

/// Redis configuration
#[derive(Debug, Clone)]
pub struct RedisConfig {
    pub url: String,
    pub channel: String,
    pub key_prefix: Option<String>,
}

impl RedisConfig {
    pub fn new(url: &str, channel: &str) -> Self {
        Self {
            url: url.to_string(),
            channel: channel.to_string(),
            key_prefix: None,
        }
    }

    pub fn with_key_prefix(mut self, prefix: &str) -> Self {
        self.key_prefix = Some(prefix.to_string());
        self
    }
}

#[cfg(feature = "redis")]
mod redis_impl {
    use super::*;
    use redis::aio::ConnectionManager;
    use redis::AsyncCommands;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    /// Redis source that subscribes to a channel
    pub struct RedisSource {
        name: String,
        config: RedisConfig,
        running: Arc<AtomicBool>,
    }

    impl RedisSource {
        pub fn new(name: &str, config: RedisConfig) -> Self {
            Self {
                name: name.to_string(),
                config,
                running: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    #[async_trait]
    impl SourceConnector for RedisSource {
        fn name(&self) -> &str {
            &self.name
        }

        async fn start(&mut self, tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
            let client = redis::Client::open(self.config.url.as_str())
                .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

            let mut pubsub = client
                .get_async_pubsub()
                .await
                .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

            pubsub
                .subscribe(&self.config.channel)
                .await
                .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

            self.running.store(true, Ordering::SeqCst);
            let running = self.running.clone();
            let name = self.name.clone();

            tokio::spawn(async move {
                info!("Redis source {} started, subscribed to channel", name);

                use futures::StreamExt;
                let mut stream = pubsub.on_message();

                while running.load(Ordering::SeqCst) {
                    match tokio::time::timeout(std::time::Duration::from_millis(100), stream.next())
                        .await
                    {
                        Ok(Some(msg)) => {
                            let payload: String = match msg.get_payload() {
                                Ok(p) => p,
                                Err(e) => {
                                    warn!("Redis source {}: failed to get payload: {}", name, e);
                                    continue;
                                }
                            };

                            match serde_json::from_str::<serde_json::Value>(&payload) {
                                Ok(json) => {
                                    let event_type = json
                                        .get("event_type")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("RedisEvent")
                                        .to_string();

                                    let event = json_to_event(&event_type, &json);

                                    if tx.send(event).await.is_err() {
                                        break;
                                    }
                                }
                                Err(e) => {
                                    warn!("Redis source {}: failed to parse JSON: {}", name, e);
                                }
                            }
                        }
                        Ok(None) => break,
                        Err(_) => {} // Timeout
                    }
                }

                info!("Redis source {} stopped", name);
            });

            Ok(())
        }

        async fn stop(&mut self) -> Result<(), ConnectorError> {
            self.running.store(false, Ordering::SeqCst);
            Ok(())
        }

        fn is_running(&self) -> bool {
            self.running.load(Ordering::SeqCst)
        }
    }

    /// Redis sink that publishes to a channel or sets keys
    pub struct RedisSink {
        name: String,
        config: RedisConfig,
        conn: ConnectionManager,
    }

    impl RedisSink {
        pub async fn new(name: &str, config: RedisConfig) -> Result<Self, ConnectorError> {
            let client = redis::Client::open(config.url.as_str())
                .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

            let conn = ConnectionManager::new(client)
                .await
                .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

            Ok(Self {
                name: name.to_string(),
                config,
                conn,
            })
        }

        /// Set a key-value pair
        pub async fn set(&mut self, key: &str, value: &str) -> Result<(), ConnectorError> {
            let full_key = match &self.config.key_prefix {
                Some(prefix) => format!("{}:{}", prefix, key),
                None => key.to_string(),
            };

            self.conn
                .set::<_, _, ()>(&full_key, value)
                .await
                .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

            Ok(())
        }

        /// Get a value by key
        pub async fn get(&mut self, key: &str) -> Result<Option<String>, ConnectorError> {
            let full_key = match &self.config.key_prefix {
                Some(prefix) => format!("{}:{}", prefix, key),
                None => key.to_string(),
            };

            let value: Option<String> = self
                .conn
                .get(&full_key)
                .await
                .map_err(|e| ConnectorError::ReceiveFailed(e.to_string()))?;

            Ok(value)
        }
    }

    #[async_trait]
    impl SinkConnector for RedisSink {
        fn name(&self) -> &str {
            &self.name
        }

        async fn send(&self, event: &Event) -> Result<(), ConnectorError> {
            let payload = serde_json::to_string(event)
                .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

            let mut conn = self.conn.clone();
            conn.publish::<_, _, ()>(&self.config.channel, &payload)
                .await
                .map_err(|e| ConnectorError::SendFailed(e.to_string()))?;

            Ok(())
        }

        async fn flush(&self) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn close(&self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }
}

#[cfg(feature = "redis")]
pub use redis_impl::{RedisSink, RedisSource};

#[cfg(not(feature = "redis"))]
pub struct RedisSource {
    name: String,
    #[allow(dead_code)]
    config: RedisConfig,
}

#[cfg(not(feature = "redis"))]
impl RedisSource {
    pub fn new(name: &str, config: RedisConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
        }
    }
}

#[cfg(not(feature = "redis"))]
#[async_trait]
impl SourceConnector for RedisSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&mut self, _tx: mpsc::Sender<Event>) -> Result<(), ConnectorError> {
        Err(ConnectorError::NotAvailable(
            "Redis connector requires 'redis' feature".to_string(),
        ))
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn is_running(&self) -> bool {
        false
    }
}

#[cfg(not(feature = "redis"))]
pub struct RedisSink {
    name: String,
    #[allow(dead_code)]
    config: RedisConfig,
}

#[cfg(not(feature = "redis"))]
impl RedisSink {
    pub fn new(name: &str, config: RedisConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
        }
    }
}

#[cfg(not(feature = "redis"))]
#[async_trait]
impl SinkConnector for RedisSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, _event: &Event) -> Result<(), ConnectorError> {
        Err(ConnectorError::NotAvailable(
            "Redis connector requires 'redis' feature".to_string(),
        ))
    }

    async fn flush(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Convert JSON value to Event
fn json_to_event(event_type: &str, json: &serde_json::Value) -> Event {
    let mut event = Event::new(event_type);

    if let Some(obj) = json.as_object() {
        for (key, value) in obj {
            if key != "event_type" {
                if let Some(v) = json_to_value(value) {
                    event = event.with_field(key, v);
                }
            }
        }
    }

    event
}

/// Convert serde_json::Value to varpulis Value
fn json_to_value(json: &serde_json::Value) -> Option<varpulis_core::Value> {
    use varpulis_core::Value;

    match json {
        serde_json::Value::Null => Some(Value::Null),
        serde_json::Value::Bool(b) => Some(Value::Bool(*b)),
        serde_json::Value::Number(n) => n
            .as_i64()
            .map(Value::Int)
            .or_else(|| n.as_f64().map(Value::Float)),
        serde_json::Value::String(s) => Some(Value::Str(s.clone())),
        serde_json::Value::Array(arr) => {
            let values: Vec<Value> = arr.iter().filter_map(json_to_value).collect();
            Some(Value::Array(values))
        }
        serde_json::Value::Object(obj) => {
            let mut map = indexmap::IndexMap::new();
            for (key, value) in obj {
                if let Some(v) = json_to_value(value) {
                    map.insert(key.clone(), v);
                }
            }
            Some(Value::Map(map))
        }
    }
}

// =============================================================================
// Connector Registry
// =============================================================================

/// Registry of available connectors
pub struct ConnectorRegistry {
    sources: IndexMap<String, Box<dyn SourceConnector>>,
    sinks: IndexMap<String, Box<dyn SinkConnector>>,
}

impl ConnectorRegistry {
    pub fn new() -> Self {
        Self {
            sources: IndexMap::new(),
            sinks: IndexMap::new(),
        }
    }

    pub fn register_source(&mut self, name: &str, source: Box<dyn SourceConnector>) {
        self.sources.insert(name.to_string(), source);
    }

    pub fn register_sink(&mut self, name: &str, sink: Box<dyn SinkConnector>) {
        self.sinks.insert(name.to_string(), sink);
    }

    pub fn get_source(&mut self, name: &str) -> Option<&mut Box<dyn SourceConnector>> {
        self.sources.get_mut(name)
    }

    pub fn get_sink(&self, name: &str) -> Option<&dyn SinkConnector> {
        self.sinks.get(name).map(|b| b.as_ref())
    }

    /// Create a connector from configuration
    pub async fn create_from_config(
        config: &ConnectorConfig,
    ) -> Result<Box<dyn SinkConnector>, ConnectorError> {
        match config.connector_type.as_str() {
            "console" => Ok(Box::new(ConsoleSink::new("console"))),
            "http" => Ok(Box::new(HttpSink::new("http", &config.url))),
            "kafka" => {
                let topic = config.topic.clone().unwrap_or_else(|| "events".to_string());
                Ok(Box::new(KafkaSink::new(
                    "kafka",
                    KafkaConfig::new(&config.url, &topic),
                )))
            }
            "mqtt" => {
                let topic = config.topic.clone().unwrap_or_else(|| "events".to_string());
                Ok(Box::new(MqttSink::new(
                    "mqtt",
                    MqttConfig::new(&config.url, &topic),
                )))
            }
            "rest" | "api" => {
                let path = config
                    .topic
                    .clone()
                    .unwrap_or_else(|| "/events".to_string());
                let api_config = RestApiConfig::new(&config.url);
                Ok(Box::new(RestApiSink::new("rest", api_config, &path)?))
            }
            "redis" => {
                let channel = config.topic.clone().unwrap_or_else(|| "events".to_string());
                Ok(Box::new(RedisSink::new(
                    "redis",
                    RedisConfig::new(&config.url, &channel),
                )))
            }
            "database" | "postgres" | "mysql" | "sqlite" => {
                let table = config.topic.clone().unwrap_or_else(|| "events".to_string());
                let sink =
                    DatabaseSink::new("database", DatabaseConfig::new(&config.url, &table)).await?;
                Ok(Box::new(sink))
            }
            _ => Err(ConnectorError::ConfigError(format!(
                "Unknown connector type: {}",
                config.connector_type
            ))),
        }
    }
}

impl Default for ConnectorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_console_sink() {
        let sink = ConsoleSink::new("test");
        let event = Event::new("TestEvent").with_field("value", 42i64);
        assert!(sink.send(&event).await.is_ok());
    }

    #[test]
    fn test_connector_config() {
        let config = ConnectorConfig::new("kafka", "localhost:9092")
            .with_topic("events")
            .with_property("group.id", "test-group");

        assert_eq!(config.connector_type, "kafka");
        assert_eq!(config.url, "localhost:9092");
        assert_eq!(config.topic, Some("events".to_string()));
        assert_eq!(
            config.properties.get("group.id"),
            Some(&"test-group".to_string())
        );
    }

    #[test]
    fn test_kafka_config() {
        let config = KafkaConfig::new("broker:9092", "my-topic").with_group_id("my-group");

        assert_eq!(config.brokers, "broker:9092");
        assert_eq!(config.topic, "my-topic");
        assert_eq!(config.group_id, Some("my-group".to_string()));
    }

    #[test]
    fn test_mqtt_config() {
        let config = MqttConfig::new("mqtt.example.com", "sensors/#")
            .with_port(8883)
            .with_credentials("user", "pass");

        assert_eq!(config.broker, "mqtt.example.com");
        assert_eq!(config.port, 8883);
        assert_eq!(config.topic, "sensors/#");
        assert_eq!(config.username, Some("user".to_string()));
    }

    #[test]
    fn test_registry() {
        let mut registry = ConnectorRegistry::new();
        registry.register_sink("console", Box::new(ConsoleSink::new("console")));
        assert!(registry.get_sink("console").is_some());
        assert!(registry.get_sink("unknown").is_none());
    }

    #[tokio::test]
    async fn test_create_from_config() {
        let config = ConnectorConfig::new("console", "");
        let sink = ConnectorRegistry::create_from_config(&config).await;
        assert!(sink.is_ok());

        let config = ConnectorConfig::new("unknown", "");
        let sink = ConnectorRegistry::create_from_config(&config).await;
        assert!(sink.is_err());
    }

    // ==========================================================================
    // ConsoleSource Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_console_source_lifecycle() {
        let mut source = ConsoleSource::new("test_console");
        assert_eq!(source.name(), "test_console");
        assert!(!source.is_running());

        let (tx, _rx) = tokio::sync::mpsc::channel(10);
        let result = source.start(tx).await;
        assert!(result.is_ok());
        assert!(source.is_running());

        let result = source.stop().await;
        assert!(result.is_ok());
        assert!(!source.is_running());
    }

    // ==========================================================================
    // ConsoleSink Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_console_sink_compact() {
        let sink = ConsoleSink::new("compact_sink").compact();
        let event = Event::new("TestEvent").with_field("value", 123i64);

        // Should not fail
        let result = sink.send(&event).await;
        assert!(result.is_ok());

        // flush and close should work
        assert!(sink.flush().await.is_ok());
        assert!(sink.close().await.is_ok());
    }

    #[test]
    fn test_console_sink_name() {
        let sink = ConsoleSink::new("my_sink");
        assert_eq!(sink.name(), "my_sink");
    }

    // ==========================================================================
    // HttpSink Tests
    // ==========================================================================

    #[test]
    fn test_http_sink_creation() {
        let sink = HttpSink::new("http_sink", "http://example.com/webhook")
            .with_header("Authorization", "Bearer token123")
            .with_header("X-Custom", "value");

        assert_eq!(sink.name(), "http_sink");
        assert_eq!(sink.url, "http://example.com/webhook");
        assert_eq!(sink.headers.len(), 2);
    }

    #[tokio::test]
    async fn test_http_sink_flush_and_close() {
        let sink = HttpSink::new("test", "http://localhost:9999");
        // flush and close should work even without connection
        assert!(sink.flush().await.is_ok());
        assert!(sink.close().await.is_ok());
    }

    // ==========================================================================
    // KafkaSource Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_kafka_source_lifecycle() {
        let config = KafkaConfig::new("localhost:9092", "test-topic");
        let mut source = KafkaSource::new("kafka_src", config);

        assert_eq!(source.name(), "kafka_src");
        assert!(!source.is_running());

        // Start should fail (stub implementation)
        let (tx, _rx) = tokio::sync::mpsc::channel(10);
        let result = source.start(tx).await;
        assert!(result.is_err());

        // But stop should work
        let result = source.stop().await;
        assert!(result.is_ok());
    }

    // ==========================================================================
    // KafkaSink Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_kafka_sink_operations() {
        let config = KafkaConfig::new("localhost:9092", "events");
        let sink = KafkaSink::new("kafka_sink", config);

        assert_eq!(sink.name(), "kafka_sink");

        // Send should fail (stub implementation)
        let event = Event::new("Test");
        let result = sink.send(&event).await;
        assert!(result.is_err());

        // But flush and close should work
        assert!(sink.flush().await.is_ok());
        assert!(sink.close().await.is_ok());
    }

    // ==========================================================================
    // MqttConfig Tests
    // ==========================================================================

    #[test]
    fn test_mqtt_config_with_qos() {
        let config = MqttConfig::new("broker", "topic")
            .with_qos(0)
            .with_client_id("client-123");

        assert_eq!(config.qos, 0);
        assert_eq!(config.client_id, Some("client-123".to_string()));

        // QoS should be capped at 2
        let config = MqttConfig::new("broker", "topic").with_qos(5);
        assert_eq!(config.qos, 2);
    }

    #[test]
    fn test_mqtt_config_defaults() {
        let config = MqttConfig::new("mqtt.local", "sensors/#");

        assert_eq!(config.broker, "mqtt.local");
        assert_eq!(config.port, 1883);
        assert_eq!(config.topic, "sensors/#");
        assert_eq!(config.client_id, None);
        assert_eq!(config.username, None);
        assert_eq!(config.password, None);
        assert_eq!(config.qos, 1);
    }

    // ==========================================================================
    // MqttSource Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_mqtt_source_lifecycle() {
        let config = MqttConfig::new("localhost", "test/#");
        let mut source = MqttSource::new("mqtt_src", config);

        assert_eq!(source.name(), "mqtt_src");
        assert!(!source.is_running());

        // Start will fail without mqtt feature
        let (tx, _rx) = tokio::sync::mpsc::channel(10);
        let _result = source.start(tx).await;

        // Stop should work
        let result = source.stop().await;
        assert!(result.is_ok());
    }

    // ==========================================================================
    // MqttSink Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_mqtt_sink_operations() {
        let config = MqttConfig::new("localhost", "events");
        let sink = MqttSink::new("mqtt_sink", config);

        assert_eq!(sink.name(), "mqtt_sink");

        // flush and close should work
        assert!(sink.flush().await.is_ok());
        assert!(sink.close().await.is_ok());
    }

    // ==========================================================================
    // Registry Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_registry_with_sources() {
        let mut registry = ConnectorRegistry::new();
        registry.register_source("console", Box::new(ConsoleSource::new("console")));

        let source = registry.get_source("console");
        assert!(source.is_some());

        if let Some(source) = source {
            assert_eq!(source.name(), "console");
        }

        assert!(registry.get_source("unknown").is_none());
    }

    #[test]
    fn test_registry_default() {
        let registry = ConnectorRegistry::default();
        assert!(registry.get_sink("anything").is_none());
    }

    #[tokio::test]
    async fn test_create_from_config_http() {
        let config = ConnectorConfig::new("http", "http://example.com");
        let sink = ConnectorRegistry::create_from_config(&config).await;
        assert!(sink.is_ok());
    }

    #[tokio::test]
    async fn test_create_from_config_kafka() {
        let config = ConnectorConfig::new("kafka", "localhost:9092").with_topic("events");
        let sink = ConnectorRegistry::create_from_config(&config).await;
        assert!(sink.is_ok());
    }

    #[tokio::test]
    async fn test_create_from_config_mqtt() {
        let config = ConnectorConfig::new("mqtt", "mqtt.local").with_topic("sensors");
        let sink = ConnectorRegistry::create_from_config(&config).await;
        assert!(sink.is_ok());
    }

    // ==========================================================================
    // ConnectorError Tests
    // ==========================================================================

    #[test]
    fn test_connector_error_display() {
        let err = ConnectorError::ConnectionFailed("timeout".to_string());
        assert!(err.to_string().contains("Connection failed"));

        let err = ConnectorError::SendFailed("buffer full".to_string());
        assert!(err.to_string().contains("Send failed"));

        let err = ConnectorError::ReceiveFailed("channel closed".to_string());
        assert!(err.to_string().contains("Receive failed"));

        let err = ConnectorError::ConfigError("missing field".to_string());
        assert!(err.to_string().contains("Configuration error"));

        let err = ConnectorError::NotConnected;
        assert!(err.to_string().contains("Not connected"));

        let err = ConnectorError::NotAvailable("kafka".to_string());
        assert!(err.to_string().contains("Connector not available"));
    }
}
