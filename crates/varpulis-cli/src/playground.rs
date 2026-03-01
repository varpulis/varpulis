//! Playground API — zero-friction VPL execution for the interactive playground.
//!
//! Provides ephemeral sessions with no authentication required.
//! Sessions auto-expire after inactivity and have conservative resource quotas.

use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use indexmap::IndexMap;
use rustc_hash::FxBuildHasher;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use uuid::Uuid;
use varpulis_core::Value;
use varpulis_runtime::event::Event;

// =============================================================================
// Constants
// =============================================================================

/// Maximum events per playground run request.
const MAX_EVENTS_PER_RUN: usize = 10_000;

/// Maximum execution timeout per request (seconds).
const MAX_EXECUTION_SECS: u64 = 10;

/// Session expiry after inactivity.
const SESSION_EXPIRY: Duration = Duration::from_secs(3600); // 1 hour

/// Reaper interval — how often to clean up expired sessions.
const REAPER_INTERVAL: Duration = Duration::from_secs(300); // 5 minutes

/// Maximum VPL source length.
const MAX_VPL_LENGTH: usize = 50_000;

// =============================================================================
// Types
// =============================================================================

/// Shared playground state.
pub type SharedPlayground = Arc<RwLock<PlaygroundState>>;

/// State for the playground backend.
#[derive(Debug)]
pub struct PlaygroundState {
    sessions: HashMap<String, PlaygroundSession>,
}

#[derive(Debug)]
struct PlaygroundSession {
    last_active: Instant,
}

impl Default for PlaygroundState {
    fn default() -> Self {
        Self::new()
    }
}

impl PlaygroundState {
    pub fn new() -> Self {
        Self {
            sessions: HashMap::new(),
        }
    }

    fn get_or_create_session(&mut self, session_id: &str) -> &mut PlaygroundSession {
        self.sessions
            .entry(session_id.to_string())
            .or_insert_with(|| PlaygroundSession {
                last_active: Instant::now(),
            })
    }

    fn reap_expired(&mut self) -> usize {
        let before = self.sessions.len();
        self.sessions
            .retain(|_, s| s.last_active.elapsed() < SESSION_EXPIRY);
        before - self.sessions.len()
    }
}

// =============================================================================
// Request/Response types
// =============================================================================

#[derive(Debug, Serialize)]
pub struct SessionResponse {
    pub session_id: String,
}

#[derive(Debug, Deserialize)]
pub struct PlaygroundRunRequest {
    pub vpl: String,
    #[serde(default)]
    pub events: Vec<PlaygroundEvent>,
}

#[derive(Debug, Deserialize)]
pub struct PlaygroundEvent {
    pub event_type: String,
    #[serde(default)]
    pub fields: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct PlaygroundValidateRequest {
    pub vpl: String,
}

#[derive(Debug, Serialize)]
pub struct PlaygroundRunResponse {
    pub ok: bool,
    pub events_processed: usize,
    pub output_events: Vec<serde_json::Value>,
    pub latency_ms: u64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<PlaygroundDiagnostic>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct PlaygroundValidateResponse {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ast: Option<serde_json::Value>,
    pub diagnostics: Vec<PlaygroundDiagnostic>,
}

#[derive(Debug, Serialize)]
pub struct PlaygroundDiagnostic {
    pub severity: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    pub start_line: u32,
    pub start_col: u32,
    pub end_line: u32,
    pub end_col: u32,
}

#[derive(Debug, Serialize)]
pub struct PlaygroundExample {
    pub id: String,
    pub name: String,
    pub description: String,
    pub category: String,
}

#[derive(Debug, Serialize)]
pub struct PlaygroundExampleDetail {
    pub id: String,
    pub name: String,
    pub description: String,
    pub category: String,
    pub vpl: String,
    pub events: Vec<serde_json::Value>,
    pub expected_output_count: Option<usize>,
}

#[derive(Debug, Serialize)]
struct PlaygroundError {
    error: String,
    code: String,
}

// =============================================================================
// Built-in examples
// =============================================================================

fn builtin_examples() -> Vec<PlaygroundExampleDetail> {
    vec![
        PlaygroundExampleDetail {
            id: "hvac-alert".into(),
            name: "HVAC Alert".into(),
            description: "Simple temperature threshold alert — the 'Hello World' of CEP.".into(),
            category: "Getting Started".into(),
            vpl: r#"stream HighTemp = TempReading
    .where(temperature > 30)
    .emit(alert: "High temperature detected", sensor: sensor_id, temp: temperature)"#.into(),
            events: vec![
                serde_json::json!({"event_type": "TempReading", "sensor_id": "HVAC-01", "temperature": 22}),
                serde_json::json!({"event_type": "TempReading", "sensor_id": "HVAC-02", "temperature": 35}),
                serde_json::json!({"event_type": "TempReading", "sensor_id": "HVAC-03", "temperature": 28}),
                serde_json::json!({"event_type": "TempReading", "sensor_id": "HVAC-01", "temperature": 41}),
                serde_json::json!({"event_type": "TempReading", "sensor_id": "HVAC-04", "temperature": 19}),
                serde_json::json!({"event_type": "TempReading", "sensor_id": "HVAC-02", "temperature": 33}),
            ],
            expected_output_count: Some(3),
        },
        PlaygroundExampleDetail {
            id: "fraud-detection".into(),
            name: "Fraud Detection".into(),
            description: "Detect login followed by large transfer within 5 minutes — sequence pattern matching.".into(),
            category: "Finance".into(),
            vpl: r#"stream FraudAlert = login as l -> transfer as t .within(5m)
    .where(l.user_id == t.user_id && t.amount > 5000)
    .emit(alert: "Suspicious transfer after login", user: l.user_id, amount: t.amount, city: l.city)"#.into(),
            events: vec![
                serde_json::json!({"event_type": "login", "user_id": "alice", "city": "New York", "device": "mobile"}),
                serde_json::json!({"event_type": "transfer", "user_id": "bob", "amount": 200, "to_account": "ext_001"}),
                serde_json::json!({"event_type": "transfer", "user_id": "alice", "amount": 15000, "to_account": "ext_099"}),
                serde_json::json!({"event_type": "login", "user_id": "charlie", "city": "London", "device": "desktop"}),
                serde_json::json!({"event_type": "transfer", "user_id": "charlie", "amount": 8500, "to_account": "ext_042"}),
                serde_json::json!({"event_type": "transfer", "user_id": "alice", "amount": 100, "to_account": "ext_005"}),
            ],
            expected_output_count: Some(2),
        },
        PlaygroundExampleDetail {
            id: "iot-anomaly".into(),
            name: "IoT Sensor Anomaly".into(),
            description: "Detect temperature spikes in sensor data using window aggregation.".into(),
            category: "IoT".into(),
            vpl: r#"stream TempSpike = sensor_reading
    .where(temperature > 50)
    .emit(alert: "Temperature spike", sensor: sensor_id, temp: temperature, zone: zone)"#.into(),
            events: vec![
                serde_json::json!({"event_type": "sensor_reading", "sensor_id": "S001", "zone": "zone_a", "temperature": 22.5}),
                serde_json::json!({"event_type": "sensor_reading", "sensor_id": "S002", "zone": "zone_b", "temperature": 65.3}),
                serde_json::json!({"event_type": "sensor_reading", "sensor_id": "S001", "zone": "zone_a", "temperature": 23.1}),
                serde_json::json!({"event_type": "sensor_reading", "sensor_id": "S003", "zone": "zone_c", "temperature": 55.0}),
                serde_json::json!({"event_type": "sensor_reading", "sensor_id": "S002", "zone": "zone_b", "temperature": 24.0}),
            ],
            expected_output_count: Some(2),
        },
        PlaygroundExampleDetail {
            id: "trading-signal".into(),
            name: "Trading Signal".into(),
            description: "Detect large trades on a single symbol — volume spike alert.".into(),
            category: "Finance".into(),
            vpl: r#"stream VolumeSpike = trade
    .where(volume > 10000)
    .emit(alert: "Large trade detected", symbol: symbol, vol: volume, price: price, side: side)"#.into(),
            events: vec![
                serde_json::json!({"event_type": "trade", "symbol": "AAPL", "price": 185.50, "volume": 500, "side": "buy"}),
                serde_json::json!({"event_type": "trade", "symbol": "GOOGL", "price": 142.30, "volume": 25000, "side": "sell"}),
                serde_json::json!({"event_type": "trade", "symbol": "AAPL", "price": 185.60, "volume": 15000, "side": "buy"}),
                serde_json::json!({"event_type": "trade", "symbol": "TSLA", "price": 250.10, "volume": 800, "side": "sell"}),
                serde_json::json!({"event_type": "trade", "symbol": "MSFT", "price": 420.00, "volume": 50000, "side": "buy"}),
            ],
            expected_output_count: Some(3),
        },
        PlaygroundExampleDetail {
            id: "cyber-killchain".into(),
            name: "Cyber Kill Chain".into(),
            description: "Detect a 3-stage attack sequence: scan → exploit → exfiltrate within 10 minutes.".into(),
            category: "Security".into(),
            vpl: r#"stream KillChain = scan as s -> exploit as e -> exfiltrate as x .within(10m)
    .where(s.target_ip == e.target_ip && e.target_ip == x.source_ip)
    .emit(alert: "Kill chain detected", target: s.target_ip, attacker: s.source_ip)"#.into(),
            events: vec![
                serde_json::json!({"event_type": "scan", "source_ip": "10.0.0.5", "target_ip": "192.168.1.100", "port": 443}),
                serde_json::json!({"event_type": "exploit", "source_ip": "10.0.0.5", "target_ip": "192.168.1.100", "cve": "CVE-2024-1234"}),
                serde_json::json!({"event_type": "exfiltrate", "source_ip": "192.168.1.100", "dest_ip": "10.0.0.5", "bytes": 50000000}),
                serde_json::json!({"event_type": "scan", "source_ip": "10.0.0.9", "target_ip": "192.168.1.200", "port": 80}),
                serde_json::json!({"event_type": "login", "user_id": "admin", "ip": "192.168.1.200"}),
            ],
            expected_output_count: Some(1),
        },
        PlaygroundExampleDetail {
            id: "kleene-pattern".into(),
            name: "Kleene Pattern".into(),
            description: "Match one or more failed logins followed by a successful login — brute force detection.".into(),
            category: "Security".into(),
            vpl: r#"stream BruteForce = failed_login+ as fails -> successful_login as success .within(5m)
    .where(fails.user_id == success.user_id)
    .emit(alert: "Possible brute force", user: success.user_id)"#.into(),
            events: vec![
                serde_json::json!({"event_type": "failed_login", "user_id": "admin", "ip": "10.0.0.5"}),
                serde_json::json!({"event_type": "failed_login", "user_id": "admin", "ip": "10.0.0.5"}),
                serde_json::json!({"event_type": "failed_login", "user_id": "admin", "ip": "10.0.0.5"}),
                serde_json::json!({"event_type": "successful_login", "user_id": "admin", "ip": "10.0.0.5"}),
                serde_json::json!({"event_type": "successful_login", "user_id": "bob", "ip": "10.0.0.9"}),
            ],
            expected_output_count: None,
        },
        PlaygroundExampleDetail {
            id: "merge-stream".into(),
            name: "Merge Streams".into(),
            description: "Combine events from multiple sources into a single alert stream.".into(),
            category: "Getting Started".into(),
            vpl: r#"stream TempAlerts = TempReading
    .where(temperature > 30)
    .emit(alert: "High temp", source: "temp", value: temperature)

stream HumidAlerts = HumidityReading
    .where(humidity > 80)
    .emit(alert: "High humidity", source: "humidity", value: humidity)

stream AllAlerts = merge(TempAlerts, HumidAlerts)
    .emit()"#.into(),
            events: vec![
                serde_json::json!({"event_type": "TempReading", "sensor_id": "S1", "temperature": 35}),
                serde_json::json!({"event_type": "HumidityReading", "sensor_id": "S2", "humidity": 85}),
                serde_json::json!({"event_type": "TempReading", "sensor_id": "S3", "temperature": 22}),
                serde_json::json!({"event_type": "HumidityReading", "sensor_id": "S4", "humidity": 45}),
                serde_json::json!({"event_type": "TempReading", "sensor_id": "S5", "temperature": 38}),
            ],
            expected_output_count: Some(3),
        },
        PlaygroundExampleDetail {
            id: "forecast-fraud".into(),
            name: "Fraud Forecasting".into(),
            description: "Predict fraud patterns using PST-based forecasting — sequence prediction with confidence scores.".into(),
            category: "Advanced".into(),
            vpl: r"stream FraudForecast = login as l -> transfer as t .within(5m)
    .forecast(confidence: 0.7, horizon: 2m, warmup: 50, max_depth: 3)
    .where(forecast_probability > 0.5)
    .emit(probability: forecast_probability, state: forecast_state)".into(),
            events: {
                // Generate a training sequence: alternating login/transfer pairs
                let mut events = Vec::new();
                for i in 0..60 {
                    events.push(serde_json::json!({"event_type": "login", "user_id": format!("user_{}", i % 10), "city": "NYC"}));
                    events.push(serde_json::json!({"event_type": "transfer", "user_id": format!("user_{}", i % 10), "amount": 100 + i * 10}));
                }
                // Add a final login to trigger forecast
                events.push(serde_json::json!({"event_type": "login", "user_id": "user_0", "city": "NYC"}));
                events
            },
            expected_output_count: None, // Depends on PST learning convergence
        },
    ]
}

// =============================================================================
// Route construction
// =============================================================================

pub fn playground_routes(playground: SharedPlayground) -> Router {
    Router::new()
        .route("/api/v1/playground/session", post(handle_create_session))
        .route(
            "/api/v1/playground/run",
            post(handle_run).layer(tower_http::limit::RequestBodyLimitLayer::new(1024 * 1024)),
        )
        .route(
            "/api/v1/playground/validate",
            post(handle_validate).layer(tower_http::limit::RequestBodyLimitLayer::new(256 * 1024)),
        )
        .route("/api/v1/playground/examples", get(handle_list_examples))
        .route("/api/v1/playground/examples/{id}", get(handle_get_example))
        .with_state(playground)
}

// =============================================================================
// Handlers
// =============================================================================

async fn handle_create_session(State(playground): State<SharedPlayground>) -> impl IntoResponse {
    let session_id = Uuid::new_v4().to_string();
    {
        let mut pg = playground.write().await;
        pg.get_or_create_session(&session_id);
    }
    let resp = SessionResponse { session_id };
    (StatusCode::CREATED, Json(resp))
}

async fn handle_run(
    State(playground): State<SharedPlayground>,
    Json(body): Json<PlaygroundRunRequest>,
) -> Response {
    // Validate input size
    if body.vpl.len() > MAX_VPL_LENGTH {
        return pg_error_response(
            StatusCode::BAD_REQUEST,
            "vpl_too_large",
            &format!("VPL source exceeds maximum size of {MAX_VPL_LENGTH} bytes"),
        );
    }

    if body.events.len() > MAX_EVENTS_PER_RUN {
        return pg_error_response(
            StatusCode::BAD_REQUEST,
            "too_many_events",
            &format!("Maximum {MAX_EVENTS_PER_RUN} events per run"),
        );
    }

    // Track session (auto-create if needed)
    {
        let mut pg = playground.write().await;
        let session_id = Uuid::new_v4().to_string();
        pg.get_or_create_session(&session_id);
    }

    let start = Instant::now();

    // Convert playground events to runtime events
    let events: Vec<Event> = body
        .events
        .iter()
        .map(|pe| {
            let mut event = Event::new(pe.event_type.clone());
            for (key, value) in &pe.fields {
                let v = json_to_runtime_value(value);
                event = event.with_field(key.as_str(), v);
            }
            event
        })
        .collect();
    let event_count = events.len();

    // Execute with timeout
    let run_result = tokio::time::timeout(
        Duration::from_secs(MAX_EXECUTION_SECS),
        crate::simulate_from_source(&body.vpl, events),
    )
    .await;

    let latency_ms = start.elapsed().as_millis() as u64;

    match run_result {
        Ok(Ok(output_events)) => {
            let output: Vec<serde_json::Value> = output_events
                .iter()
                .map(|e| {
                    let mut flat = serde_json::Map::new();
                    flat.insert(
                        "event_type".to_string(),
                        serde_json::Value::String(e.event_type.to_string()),
                    );
                    for (k, v) in &e.data {
                        flat.insert(k.to_string(), crate::websocket::value_to_json(v));
                    }
                    serde_json::Value::Object(flat)
                })
                .collect();

            let resp = PlaygroundRunResponse {
                ok: true,
                events_processed: event_count,
                output_events: output,
                latency_ms,
                diagnostics: vec![],
                error: None,
            };
            (StatusCode::OK, Json(resp)).into_response()
        }
        Ok(Err(e)) => {
            let resp = PlaygroundRunResponse {
                ok: false,
                events_processed: 0,
                output_events: vec![],
                latency_ms,
                diagnostics: vec![],
                error: Some(e.to_string()),
            };
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(_timeout) => {
            let resp = PlaygroundRunResponse {
                ok: false,
                events_processed: 0,
                output_events: vec![],
                latency_ms,
                diagnostics: vec![],
                error: Some(format!("Execution timed out after {MAX_EXECUTION_SECS}s")),
            };
            (StatusCode::REQUEST_TIMEOUT, Json(resp)).into_response()
        }
    }
}

async fn handle_validate(
    State(_playground): State<SharedPlayground>,
    Json(body): Json<PlaygroundValidateRequest>,
) -> Response {
    if body.vpl.len() > MAX_VPL_LENGTH {
        return pg_error_response(
            StatusCode::BAD_REQUEST,
            "vpl_too_large",
            &format!("VPL source exceeds maximum size of {MAX_VPL_LENGTH} bytes"),
        );
    }

    let result = match varpulis_parser::parse(&body.vpl) {
        Ok(program) => {
            let ast = serde_json::to_value(&program).ok();
            let validation = varpulis_core::validate::validate(&body.vpl, &program);
            let diagnostics: Vec<PlaygroundDiagnostic> = validation
                .diagnostics
                .iter()
                .map(|d| {
                    let (sl, sc) = position_to_line_col(&body.vpl, d.span.start);
                    let (el, ec) = position_to_line_col(&body.vpl, d.span.end);
                    PlaygroundDiagnostic {
                        severity: match d.severity {
                            varpulis_core::validate::Severity::Error => "error".into(),
                            varpulis_core::validate::Severity::Warning => "warning".into(),
                        },
                        message: d.message.clone(),
                        hint: d.hint.clone(),
                        code: d.code.map(|c| c.to_string()),
                        start_line: sl as u32,
                        start_col: sc as u32,
                        end_line: el as u32,
                        end_col: ec as u32,
                    }
                })
                .collect();
            let has_errors = diagnostics.iter().any(|d| d.severity == "error");
            PlaygroundValidateResponse {
                ok: !has_errors,
                ast,
                diagnostics,
            }
        }
        Err(error) => {
            let diag = parse_error_to_diagnostic(&body.vpl, &error);
            PlaygroundValidateResponse {
                ok: false,
                ast: None,
                diagnostics: vec![diag],
            }
        }
    };

    (StatusCode::OK, Json(result)).into_response()
}

async fn handle_list_examples() -> impl IntoResponse {
    let examples: Vec<PlaygroundExample> = builtin_examples()
        .into_iter()
        .map(|e| PlaygroundExample {
            id: e.id,
            name: e.name,
            description: e.description,
            category: e.category,
        })
        .collect();
    (StatusCode::OK, Json(examples))
}

async fn handle_get_example(Path(id): Path<String>) -> Response {
    let examples = builtin_examples();
    match examples.into_iter().find(|e| e.id == id) {
        Some(example) => (StatusCode::OK, Json(example)).into_response(),
        None => pg_error_response(
            StatusCode::NOT_FOUND,
            "example_not_found",
            &format!("Example '{id}' not found"),
        ),
    }
}

// =============================================================================
// Session reaper task
// =============================================================================

/// Spawn a background task that periodically cleans up expired sessions.
pub fn spawn_session_reaper(playground: SharedPlayground) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(REAPER_INTERVAL).await;
            let mut pg = playground.write().await;
            let reaped = pg.reap_expired();
            if reaped > 0 {
                tracing::debug!("Playground: reaped {} expired sessions", reaped);
            }
        }
    });
}

// =============================================================================
// Helpers
// =============================================================================

fn pg_error_response(status: StatusCode, code: &str, message: &str) -> Response {
    let body = PlaygroundError {
        error: message.to_string(),
        code: code.to_string(),
    };
    (status, Json(body)).into_response()
}

fn json_to_runtime_value(v: &serde_json::Value) -> Value {
    match v {
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
        serde_json::Value::Array(arr) => {
            Value::array(arr.iter().map(json_to_runtime_value).collect())
        }
        serde_json::Value::Object(map) => {
            let mut m: IndexMap<std::sync::Arc<str>, Value, FxBuildHasher> =
                IndexMap::with_hasher(FxBuildHasher);
            for (k, v) in map {
                m.insert(k.as_str().into(), json_to_runtime_value(v));
            }
            Value::map(m)
        }
    }
}

fn parse_error_to_diagnostic(
    source: &str,
    error: &varpulis_parser::ParseError,
) -> PlaygroundDiagnostic {
    use varpulis_parser::ParseError;
    match error {
        ParseError::Located {
            line,
            column,
            message,
            hint,
            ..
        } => PlaygroundDiagnostic {
            severity: "error".into(),
            message: message.clone(),
            hint: hint.clone(),
            code: None,
            start_line: line.saturating_sub(1) as u32,
            start_col: column.saturating_sub(1) as u32,
            end_line: line.saturating_sub(1) as u32,
            end_col: *column as u32,
        },
        ParseError::UnexpectedToken {
            position,
            expected,
            found,
        } => {
            let (line, col) = position_to_line_col(source, *position);
            PlaygroundDiagnostic {
                severity: "error".into(),
                message: format!("Unexpected token: expected {expected}, found '{found}'"),
                hint: None,
                code: None,
                start_line: line as u32,
                start_col: col as u32,
                end_line: line as u32,
                end_col: (col + found.len()) as u32,
            }
        }
        ParseError::UnexpectedEof => {
            let line = source.lines().count().saturating_sub(1);
            let col = source.lines().last().map_or(0, |l| l.len());
            PlaygroundDiagnostic {
                severity: "error".into(),
                message: "Unexpected end of input".into(),
                hint: None,
                code: None,
                start_line: line as u32,
                start_col: col as u32,
                end_line: line as u32,
                end_col: col as u32,
            }
        }
        ParseError::InvalidToken { position, message } => {
            let (line, col) = position_to_line_col(source, *position);
            PlaygroundDiagnostic {
                severity: "error".into(),
                message: message.clone(),
                hint: None,
                code: None,
                start_line: line as u32,
                start_col: col as u32,
                end_line: line as u32,
                end_col: (col + 10) as u32,
            }
        }
        ParseError::InvalidNumber(msg)
        | ParseError::InvalidDuration(msg)
        | ParseError::InvalidTimestamp(msg)
        | ParseError::InvalidEscape(msg) => PlaygroundDiagnostic {
            severity: "error".into(),
            message: msg.clone(),
            hint: None,
            code: None,
            start_line: 0,
            start_col: 0,
            end_line: 0,
            end_col: 0,
        },
        ParseError::UnterminatedString(position) => {
            let (line, col) = position_to_line_col(source, *position);
            PlaygroundDiagnostic {
                severity: "error".into(),
                message: "Unterminated string literal".into(),
                hint: None,
                code: None,
                start_line: line as u32,
                start_col: col as u32,
                end_line: line as u32,
                end_col: source.lines().nth(line).map_or(col, |l| l.len()) as u32,
            }
        }
        ParseError::Custom { span, message } => {
            let (sl, sc) = position_to_line_col(source, span.start);
            let (el, ec) = position_to_line_col(source, span.end);
            PlaygroundDiagnostic {
                severity: "error".into(),
                message: message.clone(),
                hint: None,
                code: None,
                start_line: sl as u32,
                start_col: sc as u32,
                end_line: el as u32,
                end_col: ec as u32,
            }
        }
    }
}

fn position_to_line_col(source: &str, position: usize) -> (usize, usize) {
    let mut line = 0;
    let mut col = 0;
    let mut pos = 0;

    for ch in source.chars() {
        if pos >= position {
            break;
        }
        if ch == '\n' {
            line += 1;
            col = 0;
        } else {
            col += 1;
        }
        pos += ch.len_utf8();
    }

    (line, col)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builtin_examples_valid() {
        let examples = builtin_examples();
        assert!(!examples.is_empty());
        for example in &examples {
            assert!(!example.id.is_empty());
            assert!(!example.name.is_empty());
            assert!(!example.vpl.is_empty());
            assert!(!example.events.is_empty());
        }
    }

    #[test]
    fn test_builtin_examples_unique_ids() {
        let examples = builtin_examples();
        let mut ids: Vec<&str> = examples.iter().map(|e| e.id.as_str()).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), examples.len(), "Duplicate example IDs found");
    }

    #[test]
    fn test_session_reaping() {
        let mut state = PlaygroundState::new();
        state.get_or_create_session("test-1");
        state.get_or_create_session("test-2");
        assert_eq!(state.sessions.len(), 2);

        // Sessions should not be reaped immediately
        let reaped = state.reap_expired();
        assert_eq!(reaped, 0);
        assert_eq!(state.sessions.len(), 2);
    }

    #[test]
    fn test_validate_response_for_valid_vpl() {
        let vpl = r#"
event SensorReading:
    temperature: int

stream HighTemp = SensorReading
    .where(temperature > 30)
    .emit(alert: "high_temp")
"#;
        match varpulis_parser::parse(vpl) {
            Ok(program) => {
                let validation = varpulis_core::validate::validate(vpl, &program);
                let has_errors = validation
                    .diagnostics
                    .iter()
                    .any(|d| d.severity == varpulis_core::validate::Severity::Error);
                assert!(!has_errors);
            }
            Err(e) => panic!("Parse failed: {e}"),
        }
    }
}
