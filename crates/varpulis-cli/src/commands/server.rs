use anyhow::Result;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::info;

use varpulis_cli::auth::{self, AuthConfig};
use varpulis_cli::websocket::{self, ServerState};
use varpulis_cli::{billing, oauth, playground, rate_limit, users};
use varpulis_runtime::event::Event;
use varpulis_runtime::metrics::{Metrics, MetricsServer};

use varpulis_cli::audit;

// =============================================================================
// mTLS Client Configuration
// =============================================================================

/// Build a reqwest client configured for mTLS communication.
///
/// - `ca_cert`: CA certificate to verify the server's TLS certificate
/// - `client_cert`: Client certificate for mutual authentication
/// - `client_key`: Client private key
///
/// Returns `None` when no TLS parameters are provided.
pub fn build_mtls_client_config(
    ca_cert: Option<&std::path::Path>,
    client_cert: Option<&std::path::Path>,
    client_key: Option<&std::path::Path>,
) -> Result<Option<reqwest::Client>> {
    // If no TLS params at all, return None (plain HTTP)
    if ca_cert.is_none() && client_cert.is_none() && client_key.is_none() {
        return Ok(None);
    }

    let mut builder = reqwest::Client::builder().timeout(std::time::Duration::from_secs(10));

    // Add CA certificate for server verification
    if let Some(ca_path) = ca_cert {
        let ca_pem = std::fs::read(ca_path).map_err(|e| {
            anyhow::anyhow!("Failed to read CA cert '{}': {}", ca_path.display(), e)
        })?;
        let ca = reqwest::Certificate::from_pem(&ca_pem)
            .map_err(|e| anyhow::anyhow!("Invalid CA cert: {}", e))?;
        builder = builder.add_root_certificate(ca);
    }

    // Add client certificate + key for mTLS
    match (client_cert, client_key) {
        (Some(cert_path), Some(key_path)) => {
            let cert_pem = std::fs::read(cert_path).map_err(|e| {
                anyhow::anyhow!(
                    "Failed to read client cert '{}': {}",
                    cert_path.display(),
                    e
                )
            })?;
            let key_pem = std::fs::read(key_path).map_err(|e| {
                anyhow::anyhow!("Failed to read client key '{}': {}", key_path.display(), e)
            })?;

            // reqwest expects a combined PEM with both cert and key
            let mut combined = cert_pem;
            combined.push(b'\n');
            combined.extend_from_slice(&key_pem);

            let identity = reqwest::Identity::from_pem(&combined)
                .map_err(|e| anyhow::anyhow!("Invalid client identity: {}", e))?;
            builder = builder.identity(identity);
        }
        (None, None) => {} // No mTLS, just CA verification
        _ => {
            anyhow::bail!("Both --tls-client-cert and --tls-client-key must be provided together");
        }
    }

    Ok(Some(builder.build()?))
}

// =============================================================================
// Axum handler state types and handler functions (Server)
// =============================================================================

/// Shared state for the server WebSocket route.
#[derive(Clone)]
struct WsRouteState {
    server_state: Arc<RwLock<ServerState>>,
    broadcast_tx: Arc<tokio::sync::broadcast::Sender<String>>,
}

/// Shared state for server health/ready endpoints.
#[derive(Clone)]
struct HealthReadyState {
    server_state: Arc<RwLock<ServerState>>,
    relay_metrics: Arc<websocket::RelayMetrics>,
}

/// Combined application state for the server axum Router.
#[derive(Clone)]
#[allow(dead_code)]
struct ServerAppState {
    ws: Arc<WsRouteState>,
    health: Arc<HealthReadyState>,
    auth_config: Arc<auth::AuthConfig>,
    rate_limiter: Arc<rate_limit::RateLimiter>,
}

/// WebSocket upgrade handler for the server.
///
/// Performs auth check inline (via headers) before upgrading the WebSocket connection.
async fn ws_upgrade_handler(
    axum::extract::State(app): axum::extract::State<ServerAppState>,
    headers: axum::http::HeaderMap,
    uri: axum::http::Uri,
    ws: axum::extract::ws::WebSocketUpgrade,
) -> axum::response::Response {
    use axum::response::IntoResponse;

    // Auth check using headers + URI (avoids holding Request<Body> across awaits)
    let auth_state = auth::AuthState {
        config: app.auth_config.clone(),
        oauth_state: None,
    };
    if let Err(rejection) = auth::check_auth_from_parts(&auth_state, &headers, &uri).await {
        return rejection.into_response();
    }

    let state = app.ws.server_state.clone();
    let broadcast_tx = app.ws.broadcast_tx.clone();
    let response = ws
        .max_frame_size(1024 * 1024)
        .max_message_size(1024 * 1024)
        .on_upgrade(move |socket| websocket::handle_connection(socket, state, broadcast_tx));
    // Echo the varpulis-v1 subprotocol so the browser accepts the upgrade
    // when auth was provided via Sec-WebSocket-Protocol header.
    (
        [(axum::http::header::SEC_WEBSOCKET_PROTOCOL, "varpulis-v1")],
        response,
    )
        .into_response()
}

/// Liveness probe: /health - is the process alive?
async fn server_health_handler(
    axum::extract::State(app): axum::extract::State<ServerAppState>,
) -> axum::Json<serde_json::Value> {
    let state = app.health.server_state.read().await;
    let uptime = state.start_time.elapsed().as_secs_f64();
    let relay = app.health.relay_metrics.snapshot();
    axum::Json(serde_json::json!({
        "status": "healthy",
        "uptime_seconds": uptime,
        "version": env!("CARGO_PKG_VERSION"),
        "relay": relay,
    }))
}

/// Readiness probe: /ready - is the engine ready to process events?
async fn server_ready_handler(
    axum::extract::State(app): axum::extract::State<ServerAppState>,
) -> impl axum::response::IntoResponse {
    let state = app.health.server_state.read().await;
    let streams_count = state.streams.len();

    if let Some(engine) = state.engine.as_ref() {
        let metrics = engine.metrics();
        let response = serde_json::json!({
            "status": "ready",
            "engine_loaded": true,
            "streams_count": streams_count,
            "events_processed": metrics.events_processed,
            "output_events_emitted": metrics.output_events_emitted,
        });
        (axum::http::StatusCode::OK, axum::Json(response))
    } else {
        let response = serde_json::json!({
            "status": "not_ready",
            "engine_loaded": false,
            "reason": "No VPL program loaded",
        });
        (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(response),
        )
    }
}

/// Warn at startup if a secret looks weak or is a known placeholder.
fn warn_weak_secret(secret: &str, name: &str) {
    const WEAK_PATTERNS: &[&str] = &[
        "change-me",
        "changeme",
        "secret",
        "password",
        "default",
        "test",
        "example",
    ];
    let lower = secret.to_lowercase();
    let is_weak = secret.len() < 32 || WEAK_PATTERNS.iter().any(|p| lower.contains(p));
    if is_weak {
        tracing::warn!(
            "{} appears weak (< 32 chars or matches a known placeholder). \
             Set a strong random value for production use.",
            name,
        );
    }
}

// =============================================================================
// Server Mode - WebSocket API for VSCode Extension
// =============================================================================

#[allow(clippy::too_many_arguments)]
pub async fn run_server(
    port: u16,
    enable_metrics: bool,
    metrics_port: u16,
    bind: &str,
    workdir: PathBuf,
    auth_config: AuthConfig,
    tls_config: Option<(PathBuf, PathBuf)>,
    rate_limit_config: rate_limit::RateLimitConfig,
    cors_origins: Option<Vec<String>>,
    state_dir: Option<PathBuf>,
    coordinator_url: Option<String>,
    worker_id: Option<String>,
    advertise_address: Option<String>,
    nats_url: Option<String>,
    mtls_client: Option<reqwest::Client>,
    max_queue_depth: u64,
    user_store: Option<users::SharedUserStore>,
) -> Result<()> {
    let tls_enabled = tls_config.is_some();
    let protocol = if tls_enabled { "wss" } else { "ws" };

    let http_protocol = if tls_enabled { "https" } else { "http" };
    println!("Varpulis Server");
    println!("==================");
    println!("WebSocket: {}://{}:{}/ws", protocol, bind, port);
    println!("REST API:  {}://{}:{}/api/v1/", http_protocol, bind, port);
    println!("Workdir:   {}", workdir.display());
    println!(
        "TLS:       {}",
        if tls_enabled { "enabled" } else { "disabled" }
    );
    println!(
        "Auth:      {}",
        if auth_config.is_required() {
            "enabled (API key required)"
        } else {
            "disabled"
        }
    );
    println!(
        "Rate Limit: {}",
        if rate_limit_config.enabled {
            format!("{} req/s per client", rate_limit_config.requests_per_second)
        } else {
            "disabled".to_string()
        }
    );
    println!(
        "State:     {}",
        match &state_dir {
            Some(dir) => format!("{}", dir.display()),
            None => "in-memory (no persistence)".to_string(),
        }
    );
    if enable_metrics {
        println!("Metrics:   http://{}:{}/metrics", bind, metrics_port);
    }
    println!(
        "Backpressure: {}",
        if max_queue_depth > 0 {
            format!("enabled (max queue depth: {})", max_queue_depth)
        } else {
            "disabled".to_string()
        }
    );
    println!();

    // Create output event channel
    let (output_tx, output_rx) = mpsc::channel::<Event>(100);

    // Create shared state using websocket module
    let state = Arc::new(RwLock::new(ServerState::new(output_tx.clone(), workdir)));

    // Create metrics if enabled
    let prom_metrics = enable_metrics.then(|| {
        let metrics = Metrics::new();
        let server = MetricsServer::new(metrics.clone(), format!("{}:{}", bind, metrics_port));
        tokio::spawn(async move {
            if let Err(e) = server.run().await {
                tracing::error!("Metrics server error: {}", e);
            }
        });
        metrics
    });

    // Broadcast channel for output events to all connected clients
    // 10K buffer gives ~100ms slack at 100K events/sec (~2MB memory)
    let (broadcast_tx, _) = tokio::sync::broadcast::channel::<String>(10_000);
    let broadcast_tx = Arc::new(broadcast_tx);

    // Relay metrics for monitoring the output event pipeline
    let relay_metrics = Arc::new(websocket::RelayMetrics::new());

    // Spawn output event forwarder using websocket module
    websocket::forward_output_events_to_websocket(
        output_rx,
        broadcast_tx.clone(),
        relay_metrics.clone(),
    );

    // Create auth config Arc for sharing
    let auth_config = Arc::new(auth_config);

    // Create rate limiter
    let rate_limiter = Arc::new(rate_limit::RateLimiter::new(rate_limit_config));

    // WebSocket route state (shared across handlers via axum State)
    let ws_state = Arc::new(WsRouteState {
        server_state: state.clone(),
        broadcast_tx: broadcast_tx.clone(),
    });

    // Health/ready check state (shared via axum State)
    let health_ready_state = Arc::new(HealthReadyState {
        server_state: state.clone(),
        relay_metrics: relay_metrics.clone(),
    });

    // REST API routes (multi-tenant pipeline management)
    let tenant_manager = if let Some(ref state_dir) = state_dir {
        std::fs::create_dir_all(state_dir)?;
        let store: Arc<dyn varpulis_runtime::StateStore> =
            Arc::new(varpulis_runtime::FileStore::open(state_dir)?);
        info!("State persistence enabled: {}", state_dir.display());
        varpulis_runtime::shared_tenant_manager_with_store(store)
    } else {
        varpulis_runtime::shared_tenant_manager()
    };

    // Pass Prometheus metrics to tenant manager for engine instrumentation
    if let Some(ref m) = prom_metrics {
        tenant_manager
            .write()
            .await
            .set_prometheus_metrics(m.clone());
    }

    // Configure backpressure queue depth limit
    {
        let mut mgr = tenant_manager.write().await;
        mgr.set_max_queue_depth(max_queue_depth);
        if max_queue_depth > 0 {
            info!(
                "Backpressure enabled: max queue depth = {}",
                max_queue_depth
            );
        }
    }

    // Connect tenant manager output events to WebSocket broadcast channel
    // so deployed pipelines' output events are relayed to WS clients.
    {
        let mut mgr = tenant_manager.write().await;
        mgr.set_ws_broadcast(broadcast_tx.clone());
    }

    // Auto-provision a default tenant if an API key is configured
    if auth_config.is_required() {
        if let Some(key) = auth_config.api_key() {
            let mut mgr = tenant_manager.write().await;
            if mgr.get_tenant_by_api_key(key).is_none() {
                let _ = mgr.create_tenant(
                    "default".to_string(),
                    key.to_string(),
                    varpulis_runtime::TenantQuota::enterprise(),
                );
                info!("Auto-provisioned default tenant with configured API key");
            }
        }
    }

    let admin_key = auth_config.api_key().map(|s| s.to_string());
    let tenant_manager_for_heartbeat = tenant_manager.clone();
    // api_routes is built later, after billing_state is created
    let api_tenant_manager = tenant_manager;
    let api_admin_key = admin_key;

    // Playground routes (no auth required)
    let playground_state =
        std::sync::Arc::new(tokio::sync::RwLock::new(playground::PlaygroundState::new()));
    let pg_routes = playground::playground_routes(playground_state.clone());
    playground::spawn_session_reaper(playground_state);

    // SaaS database pool (optional — enabled with --features saas and DATABASE_URL)
    #[cfg(feature = "saas")]
    let db_pool: Option<varpulis_db::PgPool> = {
        if let Ok(database_url) = std::env::var("DATABASE_URL") {
            match varpulis_db::pool::create_pool(&database_url).await {
                Ok(pool) => {
                    if let Err(e) = varpulis_db::pool::run_migrations(&pool).await {
                        tracing::error!("Database migration failed: {}", e);
                    }
                    info!("SaaS database connected");
                    Some(pool)
                }
                Err(e) => {
                    tracing::error!("Failed to connect to database: {}", e);
                    None
                }
            }
        } else {
            None
        }
    };

    // Audit log (created early so OAuth and billing can reference it)
    let audit_logger: Option<audit::SharedAuditLogger> = {
        let audit_path = std::path::PathBuf::from("data/audit.jsonl");
        if let Some(parent) = audit_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        match audit::AuditLogger::open(audit_path).await {
            Ok(logger) => Some(logger),
            Err(e) => {
                tracing::warn!("Audit logging disabled: {}", e);
                None
            }
        }
    };
    let audit_r = audit::audit_routes(audit_logger.clone());

    // OAuth routes — enabled when GITHUB_CLIENT_ID is set OR user_store is provided.
    // Both GitHub OAuth and local auth share the same JWT infrastructure.
    let oauth_state: Option<oauth::SharedOAuthState> = {
        let github_config = oauth::OAuthConfig::from_env();
        let has_github = github_config.is_some();
        let has_local = user_store.is_some();

        if has_github || has_local {
            // If no GitHub config, create a minimal config for JWT operations only
            let oauth_config = github_config.unwrap_or_else(|| {
                let jwt_secret =
                    std::env::var("JWT_SECRET").unwrap_or_else(|_| auth::generate_api_key());
                oauth::OAuthConfig {
                    github_client_id: String::new(),
                    github_client_secret: String::new(),
                    jwt_secret,
                    frontend_url: std::env::var("FRONTEND_URL")
                        .unwrap_or_else(|_| "http://localhost:5173".to_string()),
                    server_url: std::env::var("SERVER_URL")
                        .unwrap_or_else(|_| format!("http://localhost:{}", port)),
                }
            });

            warn_weak_secret(&oauth_config.jwt_secret, "JWT_SECRET");
            if has_github {
                info!(
                    "GitHub OAuth enabled (client_id: {}...)",
                    &oauth_config.github_client_id[..8.min(oauth_config.github_client_id.len())]
                );
            }
            if has_local {
                info!("Local username/password authentication enabled");
            }

            #[allow(unused_mut)]
            let mut oauth =
                oauth::OAuthState::new(oauth_config).with_audit_logger(audit_logger.clone());

            if let Some(ref store) = user_store {
                oauth = oauth.with_user_store(store.clone());
            }

            #[cfg(feature = "saas")]
            if let Some(ref pool) = db_pool {
                oauth = oauth.with_db_pool(pool.clone());
            }

            let state = std::sync::Arc::new(oauth);
            oauth::spawn_session_cleanup(state.clone());

            // Spawn periodic user session cleanup
            if let Some(ref store) = user_store {
                let store_cleanup = store.clone();
                tokio::spawn(async move {
                    let mut interval = tokio::time::interval(std::time::Duration::from_secs(300));
                    loop {
                        interval.tick().await;
                        let removed = store_cleanup.write().await.cleanup_expired();
                        if removed > 0 {
                            tracing::debug!("Cleaned up {} expired sessions", removed);
                        }
                    }
                });
            }

            Some(state)
        } else {
            None
        }
    };
    let oauth_r = oauth::oauth_routes(oauth_state.clone());

    // Billing routes (optional — enabled when STRIPE_SECRET_KEY is set)
    let billing_state: Option<billing::SharedBillingState> = billing::BillingConfig::from_env()
        .map(|billing_config| {
            info!("Stripe billing enabled");
            #[allow(unused_mut)]
            let mut billing =
                billing::BillingState::new(billing_config).with_audit_logger(audit_logger.clone());
            #[cfg(feature = "saas")]
            if let Some(ref pool) = db_pool {
                billing = billing.with_db_pool(pool.clone());
            }
            std::sync::Arc::new(billing)
        });
    let billing_r = billing::billing_routes(billing_state.clone());

    // Org + API key routes (saas only)
    #[cfg(feature = "saas")]
    let org_r = {
        use varpulis_cli::org;
        let org_pool = db_pool.clone();
        let org_oauth = oauth_state.clone();
        org::org_routes(org_pool, org_oauth)
    };

    // Usage flush task (saas only)
    #[cfg(feature = "saas")]
    if let (Some(ref bs), Some(ref pool)) = (&billing_state, &db_pool) {
        billing::spawn_usage_flush(bs.clone(), pool.clone());
    }

    // Build API routes (after billing_state so inject handlers can check usage limits)
    let api_routes = varpulis_cli::api::api_routes(
        api_tenant_manager,
        api_admin_key,
        cors_origins,
        billing_state.clone(),
    );

    // Combined routes using axum Router
    //
    // Build the stateful routes first (ws, health, ready use ServerAppState),
    // then apply `.with_state()` to produce Router<()>, and finally merge the
    // sub-module routers that are already Router<()>.
    let server_state = ServerAppState {
        ws: ws_state,
        health: health_ready_state,
        auth_config: auth_config.clone(),
        rate_limiter,
    };

    let stateful_app: axum::Router<ServerAppState> = axum::Router::new()
        .route("/ws", axum::routing::get(ws_upgrade_handler))
        .route("/health", axum::routing::get(server_health_handler))
        .route("/ready", axum::routing::get(server_ready_handler));

    let app: axum::Router = stateful_app
        .with_state(server_state)
        // Sub-module routes (already Router<()>)
        .merge(pg_routes)
        .merge(oauth_r)
        .merge(billing_r)
        .merge(audit_r);

    #[cfg(feature = "saas")]
    let app = app.merge(org_r);

    let app = app.merge(api_routes);

    // Parse bind address - NO unwrap()!
    let bind_addr: std::net::IpAddr = bind
        .parse()
        .map_err(|e| anyhow::anyhow!("Invalid bind address '{}': {}", bind, e))?;

    // Optionally register with a coordinator
    if let Some(coordinator_url) = coordinator_url {
        let worker_id = worker_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let worker_addr =
            advertise_address.unwrap_or_else(|| format!("{}://{}:{}", http_protocol, bind, port));
        let worker_api_key = auth_config.api_key().unwrap_or("no-key").to_string();

        // Forward output events to coordinator for live WebSocket relay
        websocket::forward_output_events_to_coordinator(
            broadcast_tx.clone(),
            coordinator_url.clone(),
            worker_api_key.clone(),
            relay_metrics.clone(),
        );

        // NATS transport: use NATS for registration, heartbeats, and command dispatch
        #[cfg(feature = "nats-transport")]
        if let Some(ref nats_url) = nats_url {
            info!(
                "Registering with coordinator via NATS ({}) as worker '{}'",
                nats_url, worker_id
            );
            let nats_url = nats_url.clone();
            let wid = worker_id.clone();
            let wkey = worker_api_key.clone();
            let tm = tenant_manager_for_heartbeat.clone();
            tokio::spawn(async move {
                varpulis_cluster::worker_nats_registration_loop(&nats_url, &wid, &wkey, Some(tm))
                    .await;
            });
        } else {
            info!(
                "Registering with coordinator at {} as worker '{}'",
                coordinator_url, worker_id
            );
            if let Some(client) = mtls_client {
                tokio::spawn(varpulis_cluster::worker_registration_loop_with_client(
                    coordinator_url,
                    worker_id,
                    worker_addr,
                    worker_api_key,
                    Some(tenant_manager_for_heartbeat.clone()),
                    client,
                ));
            } else {
                tokio::spawn(varpulis_cluster::worker_registration_loop(
                    coordinator_url,
                    worker_id,
                    worker_addr,
                    worker_api_key,
                    Some(tenant_manager_for_heartbeat.clone()),
                ));
            }
        }

        // Without nats-transport feature: always use HTTP
        #[cfg(not(feature = "nats-transport"))]
        {
            let _ = &nats_url; // suppress unused warning
            info!(
                "Registering with coordinator at {} as worker '{}'",
                coordinator_url, worker_id
            );
            if let Some(client) = mtls_client {
                tokio::spawn(varpulis_cluster::worker_registration_loop_with_client(
                    coordinator_url,
                    worker_id,
                    worker_addr,
                    worker_api_key,
                    Some(tenant_manager_for_heartbeat.clone()),
                    client,
                ));
            } else {
                tokio::spawn(varpulis_cluster::worker_registration_loop(
                    coordinator_url,
                    worker_id,
                    worker_addr,
                    worker_api_key,
                    Some(tenant_manager_for_heartbeat.clone()),
                ));
            }
        }
    }

    info!("Server listening on {}:{}", bind, port);

    // Graceful shutdown signal
    let shutdown_signal = async {
        tokio::signal::ctrl_c().await.ok();
        info!("Received shutdown signal, draining connections...");
    };

    // Start server with or without TLS.
    let addr = std::net::SocketAddr::new(bind_addr, port);
    if let Some((cert_path, key_path)) = tls_config {
        info!("TLS enabled with cert: {}", cert_path.display());
        let tls_config =
            axum_server::tls_rustls::RustlsConfig::from_pem_file(&cert_path, &key_path).await?;
        axum_server::bind_rustls(addr, tls_config)
            .serve(app.into_make_service_with_connect_info::<std::net::SocketAddr>())
            .await?;
    } else {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .with_graceful_shutdown(shutdown_signal)
        .await?;
    }

    info!("Server shutdown complete");
    Ok(())
}
