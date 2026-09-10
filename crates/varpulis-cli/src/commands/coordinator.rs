use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use tracing::info;
use varpulis_cli::{audit, oauth, playground, users, websocket};

// =============================================================================
// Coordinator axum handler state types and handler functions
// =============================================================================

/// Combined application state for the coordinator axum Router.
#[derive(Clone)]
struct CoordinatorAppState {
    coordinator: varpulis_cluster::SharedCoordinator,
    broadcast_tx: Arc<tokio::sync::broadcast::Sender<String>>,
    expected_api_key: String,
}

/// Coordinator readiness probe — returns 200 for standalone/leader coordinators.
async fn coordinator_ready_handler() -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({
        "status": "ready",
        "role": "coordinator",
    }))
}

/// Prometheus /metrics endpoint for the coordinator.
async fn coordinator_metrics_handler(
    axum::extract::State(state): axum::extract::State<Arc<CoordinatorAppState>>,
) -> impl axum::response::IntoResponse {
    let coord = state.coordinator.read().await;
    let text = coord.cluster_metrics.gather();
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        text,
    )
}

/// WebSocket route for coordinator — relays output events from workers.
///
/// Authenticates before upgrading. This socket carries the live detection feed
/// for every pipeline and every tenant on the cluster, with no per-tenant
/// filtering, so an unauthenticated upgrade hands the whole alert stream to
/// anyone who can reach the port. The server's own `/ws` already checks
/// (`commands/server.rs`); the coordinator's did not.
async fn coordinator_ws_handler(
    axum::extract::State(state): axum::extract::State<Arc<CoordinatorAppState>>,
    headers: axum::http::HeaderMap,
    ws: axum::extract::ws::WebSocketUpgrade,
) -> axum::response::Response {
    use axum::response::IntoResponse;

    let key = headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    // Constant-time, same as the internal output-events endpoint below.
    if !varpulis_core::security::constant_time_compare(key, &state.expected_api_key) {
        return (
            axum::http::StatusCode::UNAUTHORIZED,
            axum::Json(serde_json::json!({"error": "unauthorized"})),
        )
            .into_response();
    }

    let broadcast_tx = state.broadcast_tx.clone();
    ws.max_frame_size(1024 * 1024)
        .max_message_size(1024 * 1024)
        .on_upgrade(move |socket| websocket::handle_coordinator_connection(socket, broadcast_tx))
        .into_response()
}

/// Internal endpoint: workers POST output events here for relaying to WS clients.
async fn coordinator_output_events_handler(
    axum::extract::State(state): axum::extract::State<Arc<CoordinatorAppState>>,
    headers: axum::http::HeaderMap,
    axum::Json(events): axum::Json<Vec<String>>,
) -> impl axum::response::IntoResponse {
    let key = headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    // Constant-time compare: a plain `!=` on strings short-circuits at the first
    // differing byte, leaking per-byte timing that lets an attacker recover the
    // internal API key one byte at a time.
    if !varpulis_core::security::constant_time_compare(key, &state.expected_api_key) {
        return (
            axum::http::StatusCode::UNAUTHORIZED,
            axum::Json(serde_json::json!({"error": "unauthorized"})),
        );
    }
    let tx = &state.broadcast_tx;
    let mut relayed = 0u64;
    let mut dropped = 0u64;
    for event_json in &events {
        match tx.send(event_json.clone()) {
            Ok(_) => relayed += 1,
            Err(_) => dropped += 1,
        }
    }
    if dropped > 0 {
        tracing::debug!(
            "Coordinator relay: {} relayed, {} dropped (no WS subscribers)",
            relayed,
            dropped
        );
    }
    (
        axum::http::StatusCode::OK,
        axum::Json(serde_json::json!({"relayed": relayed, "dropped": dropped})),
    )
}

// =============================================================================
// Coordinator Mode
// =============================================================================

#[allow(clippy::too_many_arguments)]
pub async fn run_coordinator(
    port: u16,
    bind: &str,
    rbac: std::sync::Arc<varpulis_cluster::RbacConfig>,
    heartbeat_interval_secs: u64,
    heartbeat_timeout_secs: u64,
    scaling_policy: Option<varpulis_cluster::ScalingPolicy>,
    _ha: bool,
    _coordinator_id: Option<String>,
    _pod_namespace: Option<String>,
    _worker_label_selector: String,
    _raft_enabled: bool,
    _raft_node_id: Option<u64>,
    _raft_peers: Option<String>,
    _raft_data_dir: Option<String>,
    llm_endpoint: Option<String>,
    llm_model: String,
    llm_api_key: Option<String>,
    llm_provider: String,
    tls_config: Option<(PathBuf, PathBuf)>,
    tls_ca_cert: Option<PathBuf>,
    nats_url: Option<String>,
    rate_limit_rps: u32,
    cors_origins: Option<Vec<String>>,
) -> Result<()> {
    let tls_enabled = tls_config.is_some();
    let http_protocol = if tls_enabled { "https" } else { "http" };
    println!("Varpulis Coordinator");
    println!("=======================");
    println!("API:       {http_protocol}://{bind}:{port}/api/v1/cluster/");
    if let Some(ref nurl) = nats_url {
        println!("NATS:      {nurl}");
    }
    println!(
        "Auth:      {}",
        if rbac.allow_anonymous {
            "disabled"
        } else if rbac.key_count() > 1 {
            "enabled (RBAC multi-key)"
        } else {
            "enabled (API key required)"
        }
    );
    if tls_enabled {
        println!(
            "TLS:       enabled{}",
            if tls_ca_cert.is_some() {
                " (mTLS: client certificates required)"
            } else {
                ""
            }
        );
    }
    println!("Heartbeat: {heartbeat_interval_secs}s interval, {heartbeat_timeout_secs}s timeout");

    // `VARPULIS_CONTROL_PLANE_URL` selects the JetStream KV control plane. It
    // is read here and the plane is opened below, next to the Raft handle, so
    // a failure to open stops startup rather than silently leaving the
    // operator on the backend they were trying to leave.
    #[cfg(feature = "jetstream-control-plane")]
    let control_plane_config =
        varpulis_cluster::jetstream_control_plane::ControlPlaneConfig::from_env();

    // Built without the feature, a set variable would otherwise do nothing at
    // all, which is the failure this whole wiring exists to remove.
    #[cfg(not(feature = "jetstream-control-plane"))]
    if std::env::var("VARPULIS_CONTROL_PLANE_URL").is_ok_and(|u| !u.is_empty()) {
        anyhow::bail!(
            "VARPULIS_CONTROL_PLANE_URL is set but this binary was built without \
             the `jetstream-control-plane` feature, so coordination would \
             silently stay on Raft or standalone. Rebuild with \
             `--features jetstream-control-plane`, or unset the variable."
        );
    }
    if let Some(ref sp) = scaling_policy {
        println!(
            "Scaling:   min={}, max={}, up={:.1}, down={:.1}",
            sp.min_workers, sp.max_workers, sp.scale_up_threshold, sp.scale_down_threshold
        );
    }
    if _ha {
        let id = _coordinator_id.as_deref().unwrap_or("unknown");
        println!("HA:        enabled (id={id})");
    }

    // Build rate limiter for API routes
    let coordinator_rate_limiter = if rate_limit_rps > 0 {
        println!("Rate limit: {rate_limit_rps} req/s per client (mutating endpoints)");
        Some(Arc::new(varpulis_cluster::rate_limit::RateLimiter::new(
            varpulis_cluster::rate_limit::RateLimitConfig::new(rate_limit_rps),
        )))
    } else {
        None
    };

    // -----------------------------------------------------------------------
    // Raft consensus bootstrap (when feature enabled + --raft flag set)
    // -----------------------------------------------------------------------
    #[cfg(feature = "raft")]
    let (raft_handle, raft_peer_addrs_map) = {
        let handle: Option<varpulis_cluster::raft::RaftBootstrapResult>;
        let peer_map: std::collections::BTreeMap<u64, String>;

        if _raft_enabled {
            let node_id = _raft_node_id
                .ok_or_else(|| anyhow::anyhow!("--raft-node-id is required when --raft is set"))?;

            let peers_str = _raft_peers
                .ok_or_else(|| anyhow::anyhow!("--raft-peers is required when --raft is set"))?;

            let peer_addrs: Vec<String> =
                peers_str.split(',').map(|s| s.trim().to_string()).collect();

            // Build NodeId -> address map for leader forwarding
            peer_map = peer_addrs
                .iter()
                .enumerate()
                .map(|(i, addr)| ((i + 1) as u64, addr.clone()))
                .collect();

            println!("Raft:      node_id={}, peers={}", node_id, peers_str);

            let result = if let Some(ref data_dir) = _raft_data_dir {
                println!("Raft:      persistent storage at {}", data_dir);
                #[cfg(feature = "persistent")]
                {
                    varpulis_cluster::raft::bootstrap_persistent(
                        node_id,
                        &peer_addrs,
                        rbac.any_admin_key(),
                        data_dir,
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!("Raft bootstrap failed: {e}"))?
                }
                #[cfg(not(feature = "persistent"))]
                {
                    anyhow::bail!(
                        "--raft-data-dir requires the 'persistent' feature (build with --features persistent)"
                    );
                }
            } else {
                varpulis_cluster::raft::bootstrap(node_id, &peer_addrs, rbac.any_admin_key())
                    .await
                    .map_err(|e| anyhow::anyhow!("Raft bootstrap failed: {e}"))?
            };

            println!("Raft:      initialized (node {})", node_id);
            handle = Some(result);
        } else {
            handle = None;
            peer_map = std::collections::BTreeMap::new();
        }
        (handle, peer_map)
    };

    #[cfg(not(feature = "raft"))]
    let _ = (_raft_enabled, _raft_node_id, _raft_peers, _raft_data_dir);

    println!();

    let coordinator = varpulis_cluster::shared_coordinator();

    // Connect NATS and spawn the inbound coordinator handler if a NATS URL is
    // configured. Keep a clone of the client so it can be stored on the
    // coordinator below for the OUTBOUND command path (deploy/teardown/inject);
    // the handler task gets its own clone.
    #[cfg(feature = "nats-transport")]
    let coordinator_nats_client = if let Some(ref nurl) = nats_url {
        let nats_client = varpulis_cluster::nats_transport::connect_nats(nurl)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to NATS: {}", e))?;
        info!("Coordinator connected to NATS at {}", nurl);
        // Delivery substrate for worker heartbeats. Defaults to core pub/sub so
        // an existing deployment on a plain nats-server is unaffected; set
        // VARPULIS_NATS_SUBSTRATE=jetstream so a heartbeat published while this
        // coordinator is restarting is still there when it comes back.
        let substrate =
            varpulis_cluster::Substrate::from_env().map_err(|e| anyhow::anyhow!("{e}"))?;
        info!("Coordinator NATS substrate: {}", substrate.as_str());
        // Durable identity for the heartbeat consumer: stable across restarts,
        // and distinct per coordinator so two coordinators each see every
        // heartbeat rather than load-balancing them.
        let coordinator_durable_id =
            std::env::var("VARPULIS_COORDINATOR_ID").unwrap_or_else(|_| "coordinator".to_string());
        let coord_for_nats = coordinator.clone();
        let handler_client = nats_client.clone();
        tokio::spawn(async move {
            varpulis_cluster::nats_coordinator::run_coordinator_nats_handler_with(
                handler_client,
                coord_for_nats,
                substrate,
                &coordinator_durable_id,
            )
            .await;
        });
        Some(nats_client)
    } else {
        None
    };
    #[cfg(not(feature = "nats-transport"))]
    let _ = &nats_url;

    {
        let mut coord = coordinator.write().await;
        coord.heartbeat_interval = std::time::Duration::from_secs(heartbeat_interval_secs);
        coord.heartbeat_timeout = std::time::Duration::from_secs(heartbeat_timeout_secs);
        coord.scaling_policy = scaling_policy;

        // Configure LLM for AI chat assistant
        if let Some(ref endpoint) = llm_endpoint {
            let provider = match llm_provider.as_str() {
                "anthropic" => varpulis_cluster::chat::LlmProvider::Anthropic,
                _ => varpulis_cluster::chat::LlmProvider::OpenAiCompatible,
            };
            coord.llm_config = Some(varpulis_cluster::chat::LlmConfig {
                endpoint: endpoint.clone(),
                model: llm_model.clone(),
                api_key: llm_api_key
                    .clone()
                    .map(varpulis_core::security::SecretString::new),
                provider,
            });
            println!("AI Chat:   {llm_model} ({llm_provider})");
        }

        // Attach Raft handle to coordinator
        #[cfg(feature = "raft")]
        if let Some(ref rh) = raft_handle {
            coord.raft_handle = Some(varpulis_cluster::coordinator::RaftHandle {
                raft: rh.raft.clone(),
                store_state: rh.shared_state.clone(),
                peer_addrs: raft_peer_addrs_map.clone(),
                admin_key: rbac.any_admin_key(),
            });
        }

        // Store the outbound NATS client so the deploy/teardown/inject REST
        // handlers route commands over NATS (not HTTP) in a NATS deployment.
        #[cfg(feature = "nats-transport")]
        {
            coord.nats_client = coordinator_nats_client;
        }

        // Open the JetStream KV control plane when one is configured. Every
        // replicated write then goes there instead of to Raft — `replicate`
        // picks one destination, never both.
        #[cfg(feature = "jetstream-control-plane")]
        if let Some(cfg) = control_plane_config {
            use varpulis_cluster::jetstream_control_plane::{Applier, ControlPlane};
            match ControlPlane::connect(&cfg).await {
                Ok(cp) => {
                    let replicas = cp.replicas().await.unwrap_or(cfg.num_replicas);
                    if replicas < 3 {
                        eprintln!(
                            "WARNING: the control-plane bucket has {replicas} replica(s). \
                             It is the only copy of the cluster's control state; a \
                             deployment replacing Raft with it should set \
                             VARPULIS_CONTROL_PLANE_REPLICAS=3 against a NATS cluster."
                        );
                    }
                    println!(
                        "Control:   JetStream KV bucket '{}' ({} replica(s))",
                        cp.bucket(),
                        replicas
                    );
                    coord.control_plane = Some(Applier::new(cp));
                }
                Err(e) => {
                    // Refusing rather than falling back: an operator who
                    // configured the control plane and silently got Raft would
                    // find out during an incident.
                    return Err(anyhow::anyhow!(
                        "VARPULIS_CONTROL_PLANE_URL is set but the control plane \
                         could not be opened: {e}"
                    ));
                }
            }
        }
    }

    // Spawn periodic health sweep with automatic failover and rebalancing
    let health_coordinator = coordinator.clone();
    let sweep_interval = std::time::Duration::from_secs(heartbeat_interval_secs);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(sweep_interval);
        loop {
            interval.tick().await;
            let mut coord = health_coordinator.write().await;

            // Update Raft role if enabled
            #[cfg(feature = "raft")]
            coord.update_raft_role();

            // Update Raft Prometheus metrics
            #[cfg(feature = "raft")]
            if let Some(ref handle) = coord.raft_handle {
                let metrics = handle.raft.metrics().borrow().clone();
                let role = if metrics.current_leader == Some(metrics.id) {
                    2.0 // leader
                } else {
                    0.0 // follower
                };
                coord.cluster_metrics.update_raft_metrics(
                    role,
                    metrics.current_term as f64,
                    metrics.last_applied.map(|l| l.index as f64).unwrap_or(0.0),
                );
            }

            // Sync from Raft on ALL nodes: followers get updated state,
            // leader refreshes heartbeat timestamps for remote workers
            // (heartbeat proxy — prevents false unhealthy for workers
            // connected to other coordinators via WS).
            #[cfg(feature = "raft")]
            coord.sync_from_raft();

            // Only the leader (or standalone) runs health sweeps and failover
            if !coord.ha_role.is_writer() {
                continue;
            }

            let result = coord.health_sweep();

            // Trigger automatic failover for newly unhealthy workers
            if !result.workers_marked_unhealthy.is_empty() {
                let failed_workers: Vec<varpulis_cluster::WorkerId> =
                    result.workers_marked_unhealthy.clone();
                for wid in &failed_workers {
                    tracing::warn!("Worker {} marked unhealthy -- triggering failover", wid);
                }

                // Propagate unhealthy status to Raft for cross-coordinator visibility
                #[cfg(feature = "raft")]
                if let Some(ref handle) = coord.raft_handle {
                    for wid in &failed_workers {
                        let cmd = varpulis_cluster::raft::ClusterCommand::WorkerStatusChanged {
                            id: wid.0.clone(),
                            status: "unhealthy".to_string(),
                        };
                        if let Err(e) = handle.raft.client_write(cmd).await {
                            tracing::warn!("Failed to propagate {} unhealthy to Raft: {e}", wid);
                        }
                    }
                }

                for wid in failed_workers {
                    coord.handle_worker_failure(&wid).await;
                }
            }

            // Check connector health — log warnings for dead connectors
            let unhealthy_connectors = coord.check_connector_health();
            for (pipeline_name, worker_id, connector_name) in &unhealthy_connectors {
                tracing::warn!(
                    "Connector '{}' on pipeline '{}' (worker {}) is disconnected",
                    connector_name,
                    pipeline_name,
                    worker_id
                );
            }

            // Clean up stale completed migrations (older than 1 hour)
            coord.cleanup_completed_migrations(std::time::Duration::from_hours(1));

            // Reconcile placements on EVERY sweep, not only when a rebalance
            // is pending.
            //
            // `reconcile_placements` is what drives the cluster back to "every
            // running pipeline sits on an available worker". Gating it on
            // `pending_rebalance` made it edge-triggered, and `rebalance()`
            // clears that flag at the end of its own pass — so a placement
            // that was not fixed on the one pass the flag happened to be set
            // stayed wrong until something else set it again. In the chaos
            // soak that meant pipelines stranded on dead workers and a cluster
            // that never came back.
            //
            // It costs an in-memory scan when nothing has drifted, and only
            // issues HTTP when something has.
            let n = coord.reconcile_placements().await;
            if n > 0 {
                tracing::info!("Reconciled {n} pipeline placement(s)");
            }

            // Rebalancing across workers is the expensive, disruptive half and
            // stays behind the flag.
            if coord.pending_rebalance {
                match coord.rebalance().await {
                    Ok(ids) if !ids.is_empty() => {
                        tracing::info!("Auto-rebalance: {} migration(s) started", ids.len());
                    }
                    Ok(_) => {} // nothing to rebalance
                    Err(e) => {
                        tracing::error!("Auto-rebalance failed: {}", e);
                    }
                }
            }

            // Evaluate auto-scaling and fire webhook if needed
            if let Some(rec) = coord.evaluate_scaling() {
                if rec.action != varpulis_cluster::ScalingAction::Stable {
                    tracing::info!(
                        "Scaling recommendation: {:?} (current={}, target={}, reason={})",
                        rec.action,
                        rec.current_workers,
                        rec.target_workers,
                        rec.reason
                    );
                }
            }
            coord.fire_scaling_webhook().await;
        }
    });

    // Playground routes for coordinator (no auth required)
    let coord_playground_state =
        std::sync::Arc::new(tokio::sync::RwLock::new(playground::PlaygroundState::new()));
    let coord_pg_routes = playground::playground_routes(coord_playground_state.clone());
    playground::spawn_session_reaper(coord_playground_state);

    // -----------------------------------------------------------------------
    // Auth / OAuth routes (login, register, JWT)
    // -----------------------------------------------------------------------
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

    let session_manager: Option<users::SharedSessionManager> = {
        let session_config = users::SessionConfig::default();
        let mgr = users::SessionManager::new(session_config);
        Some(Arc::new(tokio::sync::RwLock::new(mgr)))
    };

    let oauth_state: Option<oauth::SharedOAuthState> = {
        let github_config = oauth::OAuthConfig::from_env();
        let has_github = github_config.is_some();
        let has_local = session_manager.is_some();

        if has_github || has_local {
            let oauth_config = github_config.unwrap_or_else(|| {
                let jwt_secret = std::env::var("JWT_SECRET")
                    .unwrap_or_else(|_| varpulis_cli::auth::generate_api_key());
                oauth::OAuthConfig {
                    github_client_id: String::new(),
                    github_client_secret: String::new(),
                    jwt_secret,
                    frontend_url: std::env::var("FRONTEND_URL")
                        .unwrap_or_else(|_| "http://localhost:5173".to_string()),
                    server_url: std::env::var("SERVER_URL")
                        .unwrap_or_else(|_| format!("http://localhost:{port}")),
                }
            });

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
            let mut oauth_st =
                oauth::OAuthState::new(oauth_config).with_audit_logger(audit_logger.clone());

            if let Some(ref mgr) = session_manager {
                oauth_st = oauth_st.with_session_manager(mgr.clone());
            }

            #[cfg(feature = "saas")]
            {
                // Connect to SaaS DB if DATABASE_URL is set
                if let Ok(database_url) = std::env::var("DATABASE_URL") {
                    match varpulis_db::pool::create_pool(&database_url).await {
                        Ok(pool) => {
                            if let Err(e) = varpulis_db::pool::run_migrations(&pool).await {
                                tracing::warn!("DB migrations failed: {e}");
                            }
                            oauth_st = oauth_st.with_db_pool(pool);
                            info!("SaaS database connected for auth");
                        }
                        Err(e) => {
                            tracing::warn!(
                                "SaaS DB pool failed: {e} — auth will use in-memory only"
                            );
                        }
                    }
                }

                let email_sender: Option<varpulis_cli::email::SharedEmailSender> =
                    varpulis_cli::email::SmtpConfig::from_env().and_then(|smtp_config| {
                        match varpulis_cli::email::EmailSender::new(smtp_config) {
                            Ok(sender) => {
                                info!("SMTP email sender configured");
                                Some(std::sync::Arc::new(sender))
                            }
                            Err(e) => {
                                tracing::warn!("SMTP configuration failed: {e}");
                                None
                            }
                        }
                    });
                oauth_st = oauth_st.with_email_sender(email_sender);
            }

            let state = std::sync::Arc::new(oauth_st);
            oauth::spawn_session_cleanup(state.clone());

            if let Some(ref mgr) = session_manager {
                let mgr_cleanup = mgr.clone();
                tokio::spawn(async move {
                    let mut interval = tokio::time::interval(std::time::Duration::from_mins(5));
                    loop {
                        interval.tick().await;
                        let removed = mgr_cleanup.write().await.cleanup_expired();
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

    // Mounted after `oauth_state` so the audit log can be gated by the same
    // admin guard as the /api/v1/admin routes. It is a cross-tenant log.
    let audit_r = audit::audit_routes(audit_logger.clone(), oauth_state.clone());
    let oauth_r = oauth::oauth_routes(oauth_state.clone());

    // Broadcast channel for relaying worker output events to WebSocket clients
    let (coord_broadcast_tx, _) = tokio::sync::broadcast::channel::<String>(1000);
    let coord_broadcast_tx = Arc::new(coord_broadcast_tx);

    // Coordinator app state for axum handlers
    let coord_state = Arc::new(CoordinatorAppState {
        coordinator: coordinator.clone(),
        broadcast_tx: coord_broadcast_tx.clone(),
        expected_api_key: rbac.any_admin_key().unwrap_or_default(),
    });

    // Build coordinator-local routes (health, ready, metrics, ws, internal output-events)
    let coord_local_routes = axum::Router::new()
        .route("/ready", axum::routing::get(coordinator_ready_handler))
        .route("/metrics", axum::routing::get(coordinator_metrics_handler))
        .route("/ws", axum::routing::get(coordinator_ws_handler))
        .route(
            "/api/v1/internal/output-events",
            axum::routing::post(coordinator_output_events_handler),
        )
        .with_state(coord_state);

    let bind_addr: std::net::IpAddr = bind
        .parse()
        .map_err(|e| anyhow::anyhow!("Invalid bind address '{bind}': {e}"))?;
    info!("Coordinator listening on {}:{}", bind, port);

    // Macro to start axum with or without TLS (avoids duplicating the TLS branching
    // across the raft/non-raft cfg blocks).
    macro_rules! serve_coordinator {
        ($app:expr) => {{
            let addr = std::net::SocketAddr::new(bind_addr, port);
            if let Some((ref cert_path, ref key_path)) = tls_config {
                info!("Coordinator TLS enabled with cert: {}", cert_path.display());
                // When a client CA is supplied, build a config that actually
                // requires and verifies a client certificate. `from_pem_file`
                // installs no client verifier, which is why the flag used to
                // change nothing but the startup banner.
                let tls_config = if let Some(ref ca) = tls_ca_cert {
                    info!(
                        "Coordinator mTLS enabled: client certificates verified against {}",
                        ca.display()
                    );
                    let server_config =
                        crate::commands::server::build_mtls_server_config(cert_path, key_path, ca)?;
                    axum_server::tls_rustls::RustlsConfig::from_config(server_config)
                } else {
                    axum_server::tls_rustls::RustlsConfig::from_pem_file(cert_path, key_path)
                        .await?
                };
                axum_server::bind_rustls(addr, tls_config)
                    .serve($app.into_make_service_with_connect_info::<std::net::SocketAddr>())
                    .await?;
            } else {
                let shutdown_signal = async {
                    tokio::signal::ctrl_c().await.ok();
                    info!("Received shutdown signal, draining coordinator connections...");
                };
                let listener = tokio::net::TcpListener::bind(addr).await?;
                axum::serve(
                    listener,
                    $app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
                )
                .with_graceful_shutdown(shutdown_signal)
                .await?;
            }
        }};
    }

    #[cfg(feature = "raft")]
    {
        if let Some(ref rh) = raft_handle {
            let api_routes = varpulis_cluster::api::cluster_routes_with_raft(
                coordinator,
                rbac,
                rh.raft.clone(),
                coordinator_rate_limiter,
                cors_origins,
            );
            let app = coord_local_routes
                .merge(coord_pg_routes)
                .merge(oauth_r)
                .merge(audit_r)
                .merge(api_routes);
            serve_coordinator!(app);
        } else {
            let api_routes = varpulis_cluster::cluster_routes(
                coordinator,
                rbac,
                coordinator_rate_limiter,
                cors_origins,
            );
            let app = coord_local_routes
                .merge(coord_pg_routes)
                .merge(oauth_r)
                .merge(audit_r)
                .merge(api_routes);
            serve_coordinator!(app);
        }
    }

    #[cfg(not(feature = "raft"))]
    {
        let api_routes = varpulis_cluster::cluster_routes(
            coordinator,
            rbac,
            coordinator_rate_limiter,
            cors_origins,
        );
        let app = coord_local_routes
            .merge(coord_pg_routes)
            .merge(oauth_r)
            .merge(audit_r)
            .merge(api_routes);
        serve_coordinator!(app);
    }

    info!("Coordinator shutdown complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_state(expected_key: &str) -> Arc<CoordinatorAppState> {
        let (tx, _rx) = tokio::sync::broadcast::channel::<String>(16);
        Arc::new(CoordinatorAppState {
            coordinator: Arc::new(tokio::sync::RwLock::new(
                varpulis_cluster::Coordinator::new(),
            )),
            broadcast_tx: Arc::new(tx),
            expected_api_key: expected_key.to_string(),
        })
    }

    async fn call_status(state: Arc<CoordinatorAppState>, key: Option<&str>) -> u16 {
        use axum::response::IntoResponse;
        let mut headers = axum::http::HeaderMap::new();
        if let Some(k) = key {
            headers.insert("x-api-key", k.parse().unwrap());
        }
        coordinator_output_events_handler(
            axum::extract::State(state),
            headers,
            axum::Json(vec!["{}".to_string()]),
        )
        .await
        .into_response()
        .status()
        .as_u16()
    }

    #[tokio::test]
    async fn internal_output_events_endpoint_enforces_api_key() {
        let state = test_state("s3cr3t-internal-key");
        // Wrong key → 401.
        assert_eq!(call_status(state.clone(), Some("wrong")).await, 401);
        // Missing key → 401.
        assert_eq!(call_status(state.clone(), None).await, 401);
        // A prefix of the real key (would pass a byte-by-byte short-circuit at
        // the shared prefix but still be rejected) → 401.
        assert_eq!(call_status(state.clone(), Some("s3cr3t")).await, 401);
        // Correct key → 200.
        assert_eq!(call_status(state, Some("s3cr3t-internal-key")).await, 200);
    }
}
