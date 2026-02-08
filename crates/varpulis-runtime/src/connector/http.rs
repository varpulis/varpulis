//! HTTP connector: webhook source and HTTP sink

use super::helpers::json_to_value;
use super::types::{ConnectorError, SinkConnector, SourceConnector};
use crate::event::Event;
use async_trait::async_trait;
use indexmap::IndexMap;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

// =============================================================================
// HTTP Sink (sends events via HTTP POST)
// =============================================================================

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

// =============================================================================
// Warp Rejection Types
// =============================================================================

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

// =============================================================================
// JSON-to-Event Helper
// =============================================================================

/// Convert JSON to Event (helper for webhook)
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
        for (key, value) in obj {
            if key != "event_type" && key != "type" {
                if let Some(v) = json_to_value(value) {
                    event = event.with_field(key.as_str(), v);
                }
            }
        }
    }

    event
}
