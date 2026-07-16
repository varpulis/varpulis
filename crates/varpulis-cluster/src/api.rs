//! Coordinator REST API routes (axum-based).

use std::sync::Arc;

use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use varpulis_core::pagination::{PaginationParams, MAX_LIMIT};
use varpulis_core::security::{JSON_BODY_LIMIT, LARGE_BODY_LIMIT};
#[allow(unused_imports)]
use varpulis_parser::ParseError;

use crate::connector_config::{self, ClusterConnector};
use crate::coordinator::{Coordinator, InjectBatchRequest, InjectEventRequest};
use crate::migration::MigrationReason;
use crate::pipeline_group::{PipelineGroupInfo, PipelineGroupSpec};
use crate::rate_limit::RateLimiter;
use crate::rbac::{RbacConfig, Role};
use crate::routing::{
    GroupTopology, PipelineTopologyEntry, RouteTopologyEntry, TopologyInfo, TopologyRouteEntry,
    TopologyWorkerEntry,
};
use crate::worker::{
    HeartbeatRequest, HeartbeatResponse, RegisterWorkerRequest, RegisterWorkerResponse, WorkerId,
    WorkerInfo, WorkerNode,
};
use crate::ClusterError;

/// Query parameters for DLQ listing.
#[derive(Debug, Deserialize)]
struct DlqQueryParams {
    #[serde(default)]
    offset: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
}

/// Shared coordinator state.
pub type SharedCoordinator = Arc<RwLock<Coordinator>>;

/// Create a new shared coordinator.
pub fn shared_coordinator() -> SharedCoordinator {
    Arc::new(RwLock::new(Coordinator::new()))
}

/// Build a tower-http CORS layer from an optional list of allowed origins.
///
/// - Explicit list of origins: restrict to those origins.
/// - A list containing `"*"`: allow any origin (must be explicitly opted into).
/// - `None` (default): allow only localhost origins for safety.
fn build_cors(origins: Option<Vec<String>>) -> tower_http::cors::CorsLayer {
    use tower_http::cors::{AllowHeaders, AllowMethods, AllowOrigin, CorsLayer};

    let methods = AllowMethods::list([
        axum::http::Method::GET,
        axum::http::Method::POST,
        axum::http::Method::PUT,
        axum::http::Method::DELETE,
        axum::http::Method::OPTIONS,
    ]);

    let headers = AllowHeaders::list([
        "content-type".parse().unwrap(),
        "x-api-key".parse().unwrap(),
        "authorization".parse().unwrap(),
        "x-request-id".parse().unwrap(),
        "traceparent".parse().unwrap(),
    ]);

    let origin = match origins {
        Some(ref list) if list.iter().any(|o| o == "*") => {
            tracing::warn!(
                "CORS configured with allow_any_origin — this is unsafe for production. \
                 Set --cors-origins to restrict to specific origins."
            );
            AllowOrigin::any()
        }
        Some(ref list) if !list.is_empty() => {
            let origins: Vec<axum::http::HeaderValue> =
                list.iter().filter_map(|s| s.parse().ok()).collect();
            AllowOrigin::list(origins)
        }
        _ => {
            // Default: only allow localhost origins for safety
            AllowOrigin::list([
                "http://localhost:5173".parse().unwrap(),
                "http://localhost:8080".parse().unwrap(),
                "http://127.0.0.1:5173".parse().unwrap(),
                "http://127.0.0.1:8080".parse().unwrap(),
            ])
        }
    };

    CorsLayer::new()
        .allow_methods(methods)
        .allow_headers(headers)
        .allow_origin(origin)
}

/// Build all Raft + cluster API routes.
///
/// When the `raft` feature is enabled and a Raft handle is provided,
/// the `/raft/*` routes are included for inter-coordinator RPCs.
#[cfg(feature = "raft")]
pub fn cluster_routes_with_raft(
    coordinator: SharedCoordinator,
    rbac: Arc<RbacConfig>,
    raft: crate::raft::routes::SharedRaft,
    rate_limiter: Option<Arc<RateLimiter>>,
    cors_origins: Option<Vec<String>>,
) -> Router {
    let raft_router = crate::raft::routes::raft_routes(raft, rbac.any_admin_key());
    let cluster = cluster_routes(coordinator, rbac, rate_limiter, cors_origins);
    cluster.merge(raft_router)
}

/// Shared application state for all cluster API routes.
#[derive(Debug, Clone)]
pub struct AppState {
    pub coordinator: SharedCoordinator,
    pub rbac: Arc<RbacConfig>,
    pub rate_limiter: Option<Arc<RateLimiter>>,
}

/// Build all coordinator API routes under `/api/v1/cluster/`.
///
/// CORS origins can be restricted via the `cors_origins` parameter.
/// When `None`, allows any origin (backward-compatible default).
///
/// When `rate_limiter` is `Some`, rate limiting is applied to all mutating
/// (POST/PUT/DELETE) routes. Read-only GET routes are exempt.
pub fn cluster_routes(
    coordinator: SharedCoordinator,
    rbac: Arc<RbacConfig>,
    rate_limiter: Option<Arc<RateLimiter>>,
    cors_origins: Option<Vec<String>>,
) -> Router {
    let state = AppState {
        coordinator,
        rbac,
        rate_limiter,
    };

    let json_limit = tower_http::limit::RequestBodyLimitLayer::new(JSON_BODY_LIMIT as usize);
    let large_limit = tower_http::limit::RequestBodyLimitLayer::new(LARGE_BODY_LIMIT as usize);

    // Rate-limited mutating routes (POST/PUT/DELETE)
    let rate_limited = Router::new()
        // Workers
        .route(
            "/api/v1/cluster/workers/register",
            post(handle_register_worker),
        )
        .route(
            "/api/v1/cluster/workers/{worker_id}/heartbeat",
            post(handle_heartbeat),
        )
        .route(
            "/api/v1/cluster/workers/{worker_id}",
            delete(handle_delete_worker),
        )
        .route(
            "/api/v1/cluster/workers/{worker_id}/drain",
            post(handle_drain_worker),
        )
        // Pipeline groups
        .route("/api/v1/cluster/pipeline-groups", post(handle_deploy_group))
        .route(
            "/api/v1/cluster/pipeline-groups/{group_id}",
            delete(handle_delete_group),
        )
        .route(
            "/api/v1/cluster/pipeline-groups/{group_id}/inject",
            post(handle_inject_event),
        )
        .route(
            "/api/v1/cluster/pipeline-groups/{group_id}/inject-batch",
            post(handle_inject_batch).layer(large_limit),
        )
        .route(
            "/api/v1/cluster/pipeline-groups/{group_id}/dlq/replay",
            post(handle_dlq_replay),
        )
        .route(
            "/api/v1/cluster/pipeline-groups/{group_id}/dlq",
            delete(handle_dlq_clear),
        )
        // Validate
        .route("/api/v1/cluster/validate", post(handle_validate))
        // Pipeline graph (visual builder)
        .route(
            "/api/v1/cluster/pipeline/graph",
            post(handle_pipeline_to_graph),
        )
        .route(
            "/api/v1/cluster/pipeline/generate",
            post(handle_graph_to_pipeline),
        )
        // Migration / Rebalance / Rescale
        .route("/api/v1/cluster/rebalance", post(handle_rebalance))
        .route("/api/v1/cluster/rescale", post(handle_rescale))
        .route(
            "/api/v1/cluster/pipelines/{group_id}/{pipeline_name}/migrate",
            post(handle_manual_migrate),
        )
        // Connectors
        .route("/api/v1/cluster/connectors", post(handle_create_connector))
        .route(
            "/api/v1/cluster/connectors/{name}",
            put(handle_update_connector),
        )
        .route(
            "/api/v1/cluster/connectors/{name}",
            delete(handle_delete_connector),
        )
        // Models
        .route(
            "/api/v1/cluster/models",
            post(handle_upload_model).layer(large_limit),
        )
        .route("/api/v1/cluster/models/{name}", delete(handle_delete_model))
        // Chat
        .route("/api/v1/cluster/chat", post(handle_chat))
        .route(
            "/api/v1/cluster/chat/config",
            put(handle_update_chat_config),
        )
        .layer(json_limit)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            rate_limit_middleware_fn,
        ));

    // Read-only routes (no rate limiting)
    let read_only = Router::new()
        .route("/api/v1/cluster/workers", get(handle_list_workers))
        .route(
            "/api/v1/cluster/workers/{worker_id}",
            get(handle_get_worker),
        )
        .route("/api/v1/cluster/pipeline-groups", get(handle_list_groups))
        .route(
            "/api/v1/cluster/pipeline-groups/{group_id}",
            get(handle_get_group),
        )
        .route(
            "/api/v1/cluster/pipeline-groups/{group_id}/dlq",
            get(handle_dlq_get),
        )
        .route("/api/v1/cluster/topology", get(handle_topology))
        .route(
            "/api/v1/cluster/pipeline-groups/{group_id}/topology",
            get(handle_pipeline_topology),
        )
        .route("/api/v1/cluster/migrations", get(handle_list_migrations))
        .route(
            "/api/v1/cluster/migrations/{migration_id}",
            get(handle_get_migration),
        )
        .route("/api/v1/cluster/connectors", get(handle_list_connectors))
        .route(
            "/api/v1/cluster/connectors/{name}",
            get(handle_get_connector),
        )
        .route("/api/v1/cluster/metrics", get(handle_metrics))
        .route("/api/v1/cluster/prometheus", get(handle_prometheus_metrics))
        .route("/api/v1/cluster/scaling", get(handle_scaling))
        .route("/api/v1/cluster/summary", get(handle_cluster_summary))
        .route("/api/v1/cluster/raft", get(handle_raft_status))
        .route("/api/v1/cluster/models", get(handle_list_models))
        .route(
            "/api/v1/cluster/models/{name}/download",
            get(handle_download_model),
        )
        .route("/api/v1/cluster/chat/config", get(handle_get_chat_config));

    // Health probe routes (no auth, no rate limiting)
    let health_routes = Router::new()
        .route("/health/live", get(handle_health_live))
        .route("/health/ready", get(handle_health_ready))
        .route("/health/started", get(handle_health_started))
        .route("/health", get(handle_health_detailed));

    #[allow(unused_mut)]
    let mut router = rate_limited.merge(read_only).merge(health_routes);

    // Federation endpoints (behind feature flag)
    #[cfg(feature = "federation")]
    {
        let fed_read = Router::new()
            .route("/api/v1/federation/status", get(handle_federation_status))
            .route("/api/v1/federation/regions", get(handle_federation_regions))
            .route("/api/v1/federation/catalog", get(handle_federation_catalog));

        let fed_mutate = Router::new()
            .route(
                "/api/v1/federation/regions",
                post(handle_federation_add_region),
            )
            .route(
                "/api/v1/federation/regions/{region_name}",
                delete(handle_federation_remove_region),
            )
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                rate_limit_middleware_fn,
            ));

        router = router.merge(fed_read).merge(fed_mutate);
    }

    let cors = build_cors(cors_origins);

    // Request logging via tower-http trace layer
    let trace_layer = tower_http::trace::TraceLayer::new_for_http()
        .on_response(tower_http::trace::DefaultOnResponse::new().level(tracing::Level::INFO));

    router.layer(cors).layer(trace_layer).with_state(state)
}

/// Rate limiting middleware function for axum.
async fn rate_limit_middleware_fn(
    State(state): State<AppState>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let connect_info = req
        .extensions()
        .get::<ConnectInfo<std::net::SocketAddr>>()
        .copied();
    crate::rate_limit::rate_limit_middleware(connect_info, state.rate_limiter, req, next).await
}

// =============================================================================
// Extractors
// =============================================================================

/// Extractor that resolves a request ID from headers or generates a UUID.
///
/// Prefers `x-request-id`, then extracts trace-id from W3C `traceparent`, else
/// generates a new UUID.
struct RequestId(String);

impl<S> axum::extract::FromRequestParts<S> for RequestId
where
    S: Send + Sync,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        let req_id = parts
            .headers
            .get("x-request-id")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let traceparent = parts
            .headers
            .get("traceparent")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let id = req_id.unwrap_or_else(|| {
            traceparent
                .and_then(|tp| {
                    let parts: Vec<&str> = tp.split('-').collect();
                    if parts.len() >= 2 {
                        Some(parts[1].to_string())
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string())
        });

        Ok(RequestId(id))
    }
}

/// Verify RBAC authorization from request headers against a required role.
#[allow(clippy::result_large_err)]
fn check_rbac(
    parts: &axum::http::request::Parts,
    rbac: &RbacConfig,
    required: Role,
) -> Result<(), Response> {
    let api_key = parts
        .headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let auth_header = parts
        .headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let cookie_header = parts
        .headers
        .get("cookie")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // 1. Try X-API-Key header
    if let Some(role) = rbac.authenticate(api_key.as_deref()) {
        if role.has_permission(required) {
            return Ok(());
        }
        return Err(error_response(
            StatusCode::FORBIDDEN,
            "Insufficient permissions for this operation",
        ));
    }

    // 2. Try Authorization: Bearer <jwt> header
    if let Some(ref header) = auth_header {
        if let Some(token) = header.strip_prefix("Bearer ") {
            let token = token.trim();
            if !token.is_empty() {
                if let Some(role) = rbac.authenticate_jwt(token) {
                    if role.has_permission(required) {
                        return Ok(());
                    }
                    return Err(error_response(
                        StatusCode::FORBIDDEN,
                        "Insufficient permissions for this operation",
                    ));
                }
            }
        }
    }

    // 3. Try Cookie: varpulis_session=<jwt>
    if let Some(ref cookie) = cookie_header {
        if let Some(token) = RbacConfig::extract_jwt_from_cookie(cookie) {
            if let Some(role) = rbac.authenticate_jwt(&token) {
                if role.has_permission(required) {
                    return Ok(());
                }
                return Err(error_response(
                    StatusCode::FORBIDDEN,
                    "Insufficient permissions for this operation",
                ));
            }
        }
    }

    Err(error_response(
        StatusCode::UNAUTHORIZED,
        "Invalid or missing API key",
    ))
}

/// Viewer-level RBAC extractor
struct RbacViewer;

impl axum::extract::FromRequestParts<AppState> for RbacViewer {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        check_rbac(parts, &state.rbac, Role::Viewer)?;
        Ok(RbacViewer)
    }
}

/// Operator-level RBAC extractor
struct RbacOperator;

impl axum::extract::FromRequestParts<AppState> for RbacOperator {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        check_rbac(parts, &state.rbac, Role::Operator)?;
        Ok(RbacOperator)
    }
}

/// Admin-level RBAC extractor
struct RbacAdmin;

impl axum::extract::FromRequestParts<AppState> for RbacAdmin {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        check_rbac(parts, &state.rbac, Role::Admin)?;
        Ok(RbacAdmin)
    }
}

// =============================================================================
// Leader forwarding helper
// =============================================================================

/// Forward a request to the Raft leader if this node is a follower.
/// Returns `Some(response)` if forwarded, `None` if this node is the leader.
/// Used for both read and write endpoints so the Raft topology is invisible.
#[cfg(feature = "raft")]
async fn forward_to_leader(
    coordinator: &SharedCoordinator,
    method: &str,
    path: &str,
    body: Option<serde_json::Value>,
) -> Option<Response> {
    let coord = coordinator.read().await;
    coord.raft_handle.as_ref()?;
    if coord.is_raft_leader() {
        return None;
    }
    let leader_addr = match coord.raft_leader_addr() {
        Some(addr) => addr,
        None => {
            return Some(cluster_error_response(ClusterError::NotLeader(
                "no leader elected yet".into(),
            )));
        }
    };
    let client = coord.http_client.clone();
    let admin_key = coord.raft_handle.as_ref().and_then(|h| h.admin_key.clone());
    drop(coord);

    let url = format!("{}{}", leader_addr, path);
    let mut req = match method {
        "GET" => client.get(&url),
        "PUT" => client.put(&url),
        "DELETE" => client.delete(&url),
        _ => client.post(&url),
    };
    if let Some(key) = &admin_key {
        req = req.header("x-api-key", key);
    }
    if let Some(b) = body {
        req = req.json(&b);
    }

    match req.send().await {
        Ok(resp) => {
            let status = resp.status();
            let content_type = resp
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("application/json")
                .to_string();
            let body_text = resp.text().await.unwrap_or_default();
            let axum_status =
                StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            Some((axum_status, [("content-type", content_type)], body_text).into_response())
        }
        Err(e) => {
            tracing::warn!("Cannot reach Raft leader at {leader_addr}: {e}");
            Some(cluster_error_response(ClusterError::NotLeader(format!(
                "cannot reach leader: {e}"
            ))))
        }
    }
}

#[cfg(not(feature = "raft"))]
async fn forward_to_leader(
    _coordinator: &SharedCoordinator,
    _method: &str,
    _path: &str,
    _body: Option<serde_json::Value>,
) -> Option<Response> {
    None
}

// =============================================================================
// Handlers
// =============================================================================

async fn handle_register_worker(
    State(state): State<AppState>,
    _auth: RbacOperator,
    Json(body): Json<RegisterWorkerRequest>,
) -> Response {
    let coordinator = state.coordinator.clone();
    // In Raft mode, forward to leader if we're a follower.
    // Workers always connect to their home coordinator, so we must transparently
    // forward the registration to the Raft leader.
    #[cfg(feature = "raft")]
    {
        let coord = coordinator.read().await;
        if let Some(ref handle) = coord.raft_handle {
            if !coord.is_raft_leader() {
                if let Some(leader_addr) = coord.raft_leader_addr() {
                    let client = coord.http_client.clone();
                    let admin_key = handle.admin_key.clone();
                    drop(coord); // Release lock before HTTP call

                    let url = format!("{}/api/v1/cluster/workers/register", leader_addr);
                    let mut forward_req = client.post(&url).json(&body);
                    if let Some(key) = &admin_key {
                        forward_req = forward_req.header("x-api-key", key);
                    }
                    let forward_result: Result<reqwest::Response, reqwest::Error> =
                        forward_req.send().await;
                    match forward_result {
                        Ok(forward_resp) if forward_resp.status().is_success() => {
                            // Leader accepted the registration. Also register locally
                            // so heartbeats work immediately (before next Raft sync).
                            let mut coord = coordinator.write().await;
                            let node = WorkerNode {
                                id: WorkerId(body.worker_id.clone()),
                                address: body.address,
                                api_key: varpulis_core::security::SecretString::new(body.api_key),
                                status: crate::worker::WorkerStatus::Registering,
                                capacity: body.capacity,
                                last_heartbeat: std::time::Instant::now(),
                                assigned_pipelines: Vec::new(),
                                events_processed: 0,
                                heartbeat_seq: 0,
                                last_seen_hb_seq: 0,
                            };
                            let id = coord.register_worker(node);
                            let reg_resp = RegisterWorkerResponse {
                                worker_id: id.0,
                                status: "registered".into(),
                                heartbeat_interval_secs: None,
                            };
                            return reply_json_status(&reg_resp, StatusCode::CREATED);
                        }
                        Ok(forward_resp) => {
                            let status = forward_resp.status();
                            let text = forward_resp.text().await.unwrap_or_default();
                            tracing::warn!("Leader forwarding failed (HTTP {status}): {text}");
                            return cluster_error_response(ClusterError::NotLeader(format!(
                                "leader returned HTTP {status}"
                            )));
                        }
                        Err(e) => {
                            tracing::warn!("Cannot reach Raft leader at {leader_addr}: {e}");
                            return cluster_error_response(ClusterError::NotLeader(format!(
                                "cannot reach leader: {e}"
                            )));
                        }
                    }
                }

                drop(coord);
                return cluster_error_response(ClusterError::NotLeader(
                    "no leader elected yet".into(),
                ));
            }
        }
    }

    let mut coord = coordinator.write().await;

    // Replicate through Raft if enabled (we're the leader here)
    #[cfg(feature = "raft")]
    if let Some(ref handle) = coord.raft_handle {
        let cmd = crate::raft::ClusterCommand::RegisterWorker {
            id: body.worker_id.clone(),
            address: body.address.clone(),
            api_key: body.api_key.clone(),
            capacity: body.capacity.clone(),
        };
        if let Err(e) = handle.raft.client_write(cmd).await {
            return cluster_error_response(ClusterError::NotLeader(e.to_string()));
        }
    }

    let node = WorkerNode {
        id: WorkerId(body.worker_id.clone()),
        address: body.address,
        api_key: varpulis_core::security::SecretString::new(body.api_key),
        status: crate::worker::WorkerStatus::Registering,
        capacity: body.capacity,
        last_heartbeat: std::time::Instant::now(),
        assigned_pipelines: Vec::new(),
        events_processed: 0,
        heartbeat_seq: 0,
        last_seen_hb_seq: 0,
    };
    let id = coord.register_worker(node);
    let resp = RegisterWorkerResponse {
        worker_id: id.0,
        status: "registered".into(),
        heartbeat_interval_secs: None,
    };
    reply_json_status(&resp, StatusCode::CREATED)
}

/// Replicate a worker's latest heartbeat metrics — including the monotonic
/// `heartbeat_seq` liveness counter that [`Coordinator::heartbeat`] just
/// advanced — through Raft, so that every coordinator (not only the one that
/// received the heartbeat) can tell a live worker from a dead one via
/// `Coordinator::sync_from_raft`.
///
/// Mirrors the leader-write-or-forward pattern used across this module: the
/// leader writes straight to its Raft log; a follower forwards the command to
/// the leader's `/raft/write` endpoint. Shared verbatim by both heartbeat
/// ingress paths — the HTTP handler ([`handle_heartbeat`]) and the NATS handler
/// (`nats_coordinator::handle_heartbeat_message`) — so worker liveness reaches
/// Raft identically regardless of transport (audit C5). Consumes the write
/// guard so the follower branch can release the coordinator lock before its
/// HTTP round-trip. Replication failures are logged, not fatal.
#[cfg(feature = "raft")]
pub(crate) async fn replicate_heartbeat_to_raft(
    coord: tokio::sync::RwLockWriteGuard<'_, Coordinator>,
    worker_id: &str,
    body: &HeartbeatRequest,
) {
    // Read back the monotonic heartbeat counter that `heartbeat()` just
    // advanced for this worker; replicating it is what lets other coordinators
    // detect liveness (see `sync_from_raft`).
    let heartbeat_seq = coord
        .workers
        .get(&WorkerId(worker_id.to_string()))
        .map(|w| w.heartbeat_seq)
        .unwrap_or(0);
    let cmd = crate::raft::ClusterCommand::WorkerMetricsUpdated {
        id: worker_id.to_string(),
        events_processed: body.events_processed,
        pipelines_running: body.pipelines_running,
        pipeline_metrics: body.pipeline_metrics.clone(),
        heartbeat_seq,
    };
    if coord.is_raft_leader() {
        // Leader: write directly to the Raft log.
        if let Some(ref handle) = coord.raft_handle {
            if let Err(e) = handle.raft.client_write(cmd).await {
                tracing::debug!("Failed to replicate heartbeat metrics to Raft: {e}");
            }
        }
    } else if let Some(leader_addr) = coord.raft_leader_addr() {
        // Follower: forward to the leader's /raft/write endpoint.
        let client = coord.http_client.clone();
        let admin_key = coord.raft_handle.as_ref().and_then(|h| h.admin_key.clone());
        // Don't hold the lock during HTTP I/O.
        drop(coord);
        let url = format!("{}/raft/write", leader_addr);
        let mut req = client.post(&url).json(&cmd);
        if let Some(key) = admin_key {
            req = req.header("x-api-key", key);
        }
        if let Err(e) = req.send().await {
            tracing::debug!("Failed to forward heartbeat metrics to Raft leader: {e}");
        }
    }
}

async fn handle_heartbeat(
    State(state): State<AppState>,
    Path(worker_id): Path<String>,
    _auth: RbacOperator,
    Json(body): Json<HeartbeatRequest>,
) -> Response {
    let coordinator = state.coordinator.clone();
    let mut coord = coordinator.write().await;
    if let Err(e) = coord.heartbeat(&WorkerId(worker_id.clone()), &body) {
        return error_response(StatusCode::NOT_FOUND, &e.to_string());
    }
    // Replicate the heartbeat metrics + monotonic liveness counter through Raft
    // so all coordinators (including the leader that serves
    // /api/v1/cluster/workers) see up-to-date events_processed,
    // pipelines_running, and heartbeat_seq. No-op when `raft` is off.
    #[cfg(feature = "raft")]
    replicate_heartbeat_to_raft(coord, &worker_id, &body).await;
    reply_with_status(
        Json(&HeartbeatResponse { acknowledged: true }),
        StatusCode::OK,
    )
}

async fn handle_list_workers(
    State(state): State<AppState>,
    _auth: RbacViewer,
    Query(pagination): Query<PaginationParams>,
) -> Response {
    let coordinator = state.coordinator.clone();
    if pagination.exceeds_max() {
        return error_response(
            StatusCode::BAD_REQUEST,
            &format!("limit must not exceed {MAX_LIMIT}"),
        );
    }
    if let Some(resp) =
        forward_to_leader(&coordinator, "GET", "/api/v1/cluster/workers", None).await
    {
        return resp;
    }
    let coord = coordinator.read().await;
    let all_workers: Vec<WorkerInfo> = coord.workers.values().map(WorkerInfo::from).collect();
    let (workers, meta) = pagination.paginate(all_workers);
    let resp = serde_json::json!({
        "workers": workers,
        "total": meta.total,
        "pagination": meta,
    });
    reply_json_status(&resp, StatusCode::OK)
}

async fn handle_get_worker(
    State(state): State<AppState>,
    Path(worker_id): Path<String>,
    _auth: RbacViewer,
) -> Response {
    let coordinator = state.coordinator.clone();
    if let Some(resp) = forward_to_leader(
        &coordinator,
        "GET",
        &format!("/api/v1/cluster/workers/{worker_id}"),
        None,
    )
    .await
    {
        return resp;
    }
    let coord = coordinator.read().await;
    match coord.workers.get(&WorkerId(worker_id)) {
        Some(node) => {
            let info = WorkerInfo::from(node);
            reply_json_status(&info, StatusCode::OK)
        }
        None => error_response(StatusCode::NOT_FOUND, "Worker not found"),
    }
}

async fn handle_delete_worker(
    State(state): State<AppState>,
    Path(worker_id): Path<String>,
    _auth: RbacAdmin,
) -> Response {
    let coordinator = state.coordinator.clone();
    if let Some(resp) = forward_to_leader(
        &coordinator,
        "DELETE",
        &format!("/api/v1/cluster/workers/{worker_id}"),
        None,
    )
    .await
    {
        return resp;
    }

    let mut coord = coordinator.write().await;

    // Replicate through Raft if enabled
    #[cfg(feature = "raft")]
    if let Some(ref handle) = coord.raft_handle {
        let cmd = crate::raft::ClusterCommand::DeregisterWorker {
            id: worker_id.clone(),
        };
        if let Err(e) = handle.raft.client_write(cmd).await {
            return cluster_error_response(ClusterError::NotLeader(e.to_string()));
        }
    }

    match coord.deregister_worker(&WorkerId(worker_id)) {
        Ok(()) => reply_with_status(Json(&serde_json::json!({"deleted": true})), StatusCode::OK),
        Err(e) => error_response(StatusCode::NOT_FOUND, &e.to_string()),
    }
}

async fn handle_deploy_group(
    State(state): State<AppState>,
    _auth: RbacOperator,
    RequestId(request_id): RequestId,
    Json(body): Json<PipelineGroupSpec>,
) -> Response {
    let coordinator = state.coordinator.clone();
    tracing::info!(request_id = %request_id, "deploy_group");
    if let Some(resp) = forward_to_leader(
        &coordinator,
        "POST",
        "/api/v1/cluster/pipeline-groups",
        serde_json::to_value(&body).ok(),
    )
    .await
    {
        return resp;
    }

    // Phase 1 (plan, read lock) + Phase 2 (execute, no lock).
    // Transport is NATS when a client is configured, else HTTP (default).
    #[cfg(feature = "nats-transport")]
    let (plan, results) = {
        let (plan, http_client, nats_client) = {
            let coord = coordinator.read().await;
            match coord.plan_deploy_group(&body) {
                Ok(plan) => (plan, coord.http_client.clone(), coord.nats_client.clone()),
                Err(e) => return cluster_error_response(e),
            }
        };
        // Read lock released here
        let results = crate::coordinator::Coordinator::execute_deploy_plan_dispatch(
            nats_client.as_ref(),
            &http_client,
            &plan,
        )
        .await;
        (plan, results)
    };
    #[cfg(not(feature = "nats-transport"))]
    let (plan, results) = {
        let (plan, http_client) = {
            let coord = coordinator.read().await;
            match coord.plan_deploy_group(&body) {
                Ok(plan) => (plan, coord.http_client.clone()),
                Err(e) => return cluster_error_response(e),
            }
        };
        // Read lock released here
        let results =
            crate::coordinator::Coordinator::execute_deploy_plan(&http_client, &plan).await;
        (plan, results)
    };

    // Phase 3: Commit results (write lock)
    let mut coord = coordinator.write().await;
    match coord.commit_deploy_group(plan, results) {
        Ok(group_id) => {
            // Replicate the deployment result through Raft
            #[cfg(feature = "raft")]
            if let Some(ref handle) = coord.raft_handle {
                if let Some(group) = coord.pipeline_groups.get(&group_id) {
                    let group_json = serde_json::to_value(group).unwrap_or_default();
                    let cmd = crate::raft::ClusterCommand::GroupDeployed {
                        name: group_id.clone(),
                        group: group_json,
                    };
                    if let Err(e) = handle.raft.client_write(cmd).await {
                        tracing::error!("Raft replication failed for deploy_group: {e}");
                        return reply_with_status(
                            Json(&serde_json::json!({
                                "error": format!("Operation applied locally but Raft replication failed: {e}")
                            })),
                            StatusCode::INTERNAL_SERVER_ERROR,
                        );
                    }
                }
            }

            let Some(group) = coord.pipeline_groups.get(&group_id) else {
                return cluster_error_response(ClusterError::GroupNotFound(group_id));
            };
            let info = PipelineGroupInfo::from(group);
            reply_json_status(&info, StatusCode::CREATED)
        }
        Err(e) => cluster_error_response(e),
    }
}

async fn handle_list_groups(
    State(state): State<AppState>,
    _auth: RbacViewer,
    Query(pagination): Query<PaginationParams>,
) -> Response {
    let coordinator = state.coordinator.clone();
    if pagination.exceeds_max() {
        return error_response(
            StatusCode::BAD_REQUEST,
            &format!("limit must not exceed {MAX_LIMIT}"),
        );
    }
    if let Some(resp) =
        forward_to_leader(&coordinator, "GET", "/api/v1/cluster/pipeline-groups", None).await
    {
        return resp;
    }
    let coord = coordinator.read().await;
    let all_groups: Vec<PipelineGroupInfo> = coord
        .pipeline_groups
        .values()
        .map(PipelineGroupInfo::from)
        .collect();
    let (pipeline_groups, meta) = pagination.paginate(all_groups);
    let resp = serde_json::json!({
        "pipeline_groups": pipeline_groups,
        "total": meta.total,
        "pagination": meta,
    });
    reply_json_status(&resp, StatusCode::OK)
}

async fn handle_get_group(
    State(state): State<AppState>,
    Path(group_id): Path<String>,
    _auth: RbacViewer,
) -> Response {
    let coordinator = state.coordinator.clone();
    if let Some(resp) = forward_to_leader(
        &coordinator,
        "GET",
        &format!("/api/v1/cluster/pipeline-groups/{group_id}"),
        None,
    )
    .await
    {
        return resp;
    }
    let coord = coordinator.read().await;
    match coord.pipeline_groups.get(&group_id) {
        Some(group) => {
            let info = PipelineGroupInfo::from(group);
            reply_json_status(&info, StatusCode::OK)
        }
        None => error_response(StatusCode::NOT_FOUND, "Pipeline group not found"),
    }
}

async fn handle_delete_group(
    State(state): State<AppState>,
    Path(group_id): Path<String>,
    _auth: RbacAdmin,
) -> Response {
    let coordinator = state.coordinator.clone();
    if let Some(resp) = forward_to_leader(
        &coordinator,
        "DELETE",
        &format!("/api/v1/cluster/pipeline-groups/{group_id}"),
        None,
    )
    .await
    {
        return resp;
    }

    // Phase 1 (plan teardown, read lock) + Phase 2 (execute, no lock).
    // Transport is NATS when a client is configured, else HTTP (default).
    #[cfg(feature = "nats-transport")]
    let plan = {
        let (plan, http_client, nats_client) = {
            let coord = coordinator.read().await;
            match coord.plan_teardown_group(&group_id) {
                Ok(plan) => (plan, coord.http_client.clone(), coord.nats_client.clone()),
                Err(e) => return cluster_error_response(e),
            }
        };
        // Read lock released here
        crate::coordinator::Coordinator::execute_teardown_plan_dispatch(
            nats_client.as_ref(),
            &http_client,
            &plan,
        )
        .await;
        plan
    };
    #[cfg(not(feature = "nats-transport"))]
    let plan = {
        let (plan, http_client) = {
            let coord = coordinator.read().await;
            match coord.plan_teardown_group(&group_id) {
                Ok(plan) => (plan, coord.http_client.clone()),
                Err(e) => return cluster_error_response(e),
            }
        };
        // Read lock released here
        crate::coordinator::Coordinator::execute_teardown_plan(&http_client, &plan).await;
        plan
    };

    // Phase 3: Commit state changes under write lock
    let mut coord = coordinator.write().await;
    coord.commit_teardown_group(&plan);

    // Replicate the group removal through Raft
    #[cfg(feature = "raft")]
    if let Some(ref handle) = coord.raft_handle {
        let cmd = crate::raft::ClusterCommand::GroupRemoved {
            name: group_id.clone(),
        };
        if let Err(e) = handle.raft.client_write(cmd).await {
            tracing::error!("Raft replication failed for teardown_group: {e}");
            return reply_with_status(
                Json(&serde_json::json!({
                    "error": format!("Operation applied locally but Raft replication failed: {e}")
                })),
                StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
    }

    reply_with_status(
        Json(&serde_json::json!({"torn_down": true})),
        StatusCode::OK,
    )
}

async fn handle_inject_event(
    State(state): State<AppState>,
    Path(group_id): Path<String>,
    _auth: RbacOperator,
    RequestId(request_id): RequestId,
    Json(body): Json<InjectEventRequest>,
) -> Response {
    let coordinator = state.coordinator.clone();
    tracing::info!(request_id = %request_id, group_id = %group_id, "inject_event");
    if let Some(resp) = forward_to_leader(
        &coordinator,
        "POST",
        &format!("/api/v1/cluster/pipeline-groups/{group_id}/inject"),
        serde_json::to_value(&body).ok(),
    )
    .await
    {
        return resp;
    }
    // Phase 1 (resolve target, read lock) + Phase 2 (execute, no lock).
    // Transport is NATS when a client is configured, else HTTP (default).
    #[cfg(feature = "nats-transport")]
    let inject_result = {
        let (target, http_client, nats_client) = {
            let coord = coordinator.read().await;
            match coord.resolve_inject_target(&group_id, &body) {
                Ok(target) => (target, coord.http_client.clone(), coord.nats_client.clone()),
                Err(e) => return cluster_error_response(e),
            }
        };
        // Read lock released here
        crate::coordinator::Coordinator::execute_inject_event_dispatch(
            nats_client.as_ref(),
            &http_client,
            &target,
            &body,
        )
        .await
    };
    #[cfg(not(feature = "nats-transport"))]
    let inject_result = {
        let (target, http_client) = {
            let coord = coordinator.read().await;
            match coord.resolve_inject_target(&group_id, &body) {
                Ok(target) => (target, coord.http_client.clone()),
                Err(e) => return cluster_error_response(e),
            }
        };
        // Read lock released here
        crate::coordinator::Coordinator::execute_inject_event(&http_client, &target, &body).await
    };

    match inject_result {
        Ok(resp) => reply_json_status(&resp, StatusCode::OK),
        Err(e) => cluster_error_response(e),
    }
}

async fn handle_inject_batch(
    State(state): State<AppState>,
    Path(group_id): Path<String>,
    _auth: RbacOperator,
    RequestId(request_id): RequestId,
    Json(body): Json<InjectBatchRequest>,
) -> Response {
    let coordinator = state.coordinator.clone();
    tracing::info!(request_id = %request_id, group_id = %group_id, "inject_batch");
    if let Some(resp) = forward_to_leader(
        &coordinator,
        "POST",
        &format!("/api/v1/cluster/pipeline-groups/{group_id}/inject-batch"),
        serde_json::to_value(&body).ok(),
    )
    .await
    {
        return resp;
    }
    let coord = coordinator.read().await;
    match coord.inject_batch(&group_id, body).await {
        Ok(resp) => reply_json_status(&resp, StatusCode::OK),
        Err(e) => cluster_error_response(e),
    }
}

// =============================================================================
// DLQ proxy handlers
// =============================================================================

/// Resolve the first placement for a pipeline group, returning (worker_address, pipeline_id, api_key).
fn resolve_first_placement(
    coord: &Coordinator,
    group_id: &str,
) -> Result<(String, String, String), ClusterError> {
    let group = coord
        .pipeline_groups
        .get(group_id)
        .ok_or_else(|| ClusterError::GroupNotFound(group_id.to_string()))?;

    let deployment = group.placements.values().next().ok_or_else(|| {
        ClusterError::RoutingFailed(format!("No deployments found for group '{}'", group_id))
    })?;

    Ok((
        deployment.worker_address.clone(),
        deployment.pipeline_id.clone(),
        deployment.worker_api_key.clone(),
    ))
}

async fn handle_dlq_get(
    State(state): State<AppState>,
    Path(group_id): Path<String>,
    _auth: RbacViewer,
    Query(params): Query<DlqQueryParams>,
) -> Response {
    let coordinator = state.coordinator.clone();
    if let Some(resp) = forward_to_leader(
        &coordinator,
        "GET",
        &format!("/api/v1/cluster/pipeline-groups/{group_id}/dlq"),
        None,
    )
    .await
    {
        return resp;
    }

    let (worker_address, pipeline_id, api_key, http_client) = {
        let coord = coordinator.read().await;
        match resolve_first_placement(&coord, &group_id) {
            Ok((addr, pid, key)) => (addr, pid, key, coord.http_client().clone()),
            Err(e) => return cluster_error_response(e),
        }
    };

    let offset = params.offset.unwrap_or(0);
    let limit = params.limit.unwrap_or(100);
    let url = format!(
        "{}/api/v1/pipelines/{}/dlq?offset={}&limit={}",
        worker_address, pipeline_id, offset, limit
    );

    match http_client
        .get(&url)
        .header("x-api-key", &api_key)
        .send()
        .await
    {
        Ok(resp) => {
            let status =
                StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::json!({}));
            reply_json_status(&body, status)
        }
        Err(e) => cluster_error_response(ClusterError::RoutingFailed(e.to_string())),
    }
}

async fn handle_dlq_replay(
    State(state): State<AppState>,
    Path(group_id): Path<String>,
    _auth: RbacOperator,
) -> Response {
    let coordinator = state.coordinator.clone();
    if let Some(resp) = forward_to_leader(
        &coordinator,
        "POST",
        &format!("/api/v1/cluster/pipeline-groups/{group_id}/dlq/replay"),
        None,
    )
    .await
    {
        return resp;
    }

    let (worker_address, pipeline_id, api_key, http_client) = {
        let coord = coordinator.read().await;
        match resolve_first_placement(&coord, &group_id) {
            Ok((addr, pid, key)) => (addr, pid, key, coord.http_client().clone()),
            Err(e) => return cluster_error_response(e),
        }
    };

    let url = format!(
        "{}/api/v1/pipelines/{}/dlq/replay",
        worker_address, pipeline_id
    );

    match http_client
        .post(&url)
        .header("x-api-key", &api_key)
        .send()
        .await
    {
        Ok(resp) => {
            let status =
                StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::json!({}));
            reply_json_status(&body, status)
        }
        Err(e) => cluster_error_response(ClusterError::RoutingFailed(e.to_string())),
    }
}

async fn handle_dlq_clear(
    State(state): State<AppState>,
    Path(group_id): Path<String>,
    _auth: RbacOperator,
) -> Response {
    let coordinator = state.coordinator.clone();
    if let Some(resp) = forward_to_leader(
        &coordinator,
        "DELETE",
        &format!("/api/v1/cluster/pipeline-groups/{group_id}/dlq"),
        None,
    )
    .await
    {
        return resp;
    }

    let (worker_address, pipeline_id, api_key, http_client) = {
        let coord = coordinator.read().await;
        match resolve_first_placement(&coord, &group_id) {
            Ok((addr, pid, key)) => (addr, pid, key, coord.http_client().clone()),
            Err(e) => return cluster_error_response(e),
        }
    };

    let url = format!("{}/api/v1/pipelines/{}/dlq", worker_address, pipeline_id);

    match http_client
        .delete(&url)
        .header("x-api-key", &api_key)
        .send()
        .await
    {
        Ok(resp) => {
            let status =
                StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::json!({}));
            reply_json_status(&body, status)
        }
        Err(e) => cluster_error_response(ClusterError::RoutingFailed(e.to_string())),
    }
}

/// Request body for the validate endpoint.
#[derive(Debug, Deserialize)]
struct ValidateRequest {
    source: String,
}

/// A single diagnostic from validation.
#[derive(Debug, Serialize)]
struct ValidateDiagnostic {
    severity: &'static str,
    line: usize,
    column: usize,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    hint: Option<String>,
}

/// Response from the validate endpoint.
#[derive(Debug, Serialize)]
struct ValidateResponse {
    valid: bool,
    diagnostics: Vec<ValidateDiagnostic>,
}

// =============================================================================
// Migration / Drain / Rebalance types
// =============================================================================

/// Request body for the drain endpoint.
#[derive(Debug, Serialize, Deserialize)]
struct DrainRequest {
    pub timeout_secs: Option<u64>,
}

/// Response from the drain endpoint.
#[derive(Debug, Serialize)]
struct DrainResponse {
    pub worker_id: String,
    pub pipelines_migrated: usize,
    pub status: String,
}

/// Info about a single migration (for API responses).
#[derive(Debug, Serialize)]
struct MigrationInfo {
    pub id: String,
    pub pipeline_name: String,
    pub group_id: String,
    pub source_worker: String,
    pub target_worker: String,
    pub status: String,
    pub reason: String,
    pub elapsed_ms: u128,
}

/// Request body for manual migration.
#[derive(Debug, Serialize, Deserialize)]
struct ManualMigrateRequest {
    pub target_worker_id: String,
}

/// Response from the rebalance endpoint.
#[derive(Debug, Serialize)]
struct RebalanceResponse {
    pub migrations_started: usize,
    pub migration_ids: Vec<String>,
}

async fn handle_validate(
    State(state): State<AppState>,
    _auth: RbacViewer,
    Json(body): Json<ValidateRequest>,
) -> Response {
    let coordinator = state.coordinator.clone();
    // Inject cluster connectors so .from(mqtt_market) doesn't produce "undefined connector"
    let coord = coordinator.read().await;
    let (effective_source, preamble_lines) =
        connector_config::inject_connectors(&body.source, &coord.connectors);
    drop(coord);

    match varpulis_parser::parse(&effective_source) {
        Ok(program) => {
            // Run semantic validation after successful parse
            let validation = varpulis_core::validate::validate(&effective_source, &program);
            let diagnostics: Vec<ValidateDiagnostic> = validation
                .diagnostics
                .iter()
                .filter_map(|d| {
                    let (line, column) = position_to_line_col(&effective_source, d.span.start);
                    // Skip diagnostics in the injected preamble
                    if line <= preamble_lines {
                        return None;
                    }
                    Some(ValidateDiagnostic {
                        severity: match d.severity {
                            varpulis_core::validate::Severity::Error => "error",
                            varpulis_core::validate::Severity::Warning => "warning",
                        },
                        line: line - preamble_lines,
                        column,
                        message: d.message.clone(),
                        hint: d.hint.clone(),
                    })
                })
                .collect();
            let valid = !validation.has_errors();
            let resp = ValidateResponse { valid, diagnostics };
            reply_json_status(&resp, StatusCode::OK)
        }
        Err(e) => {
            let diagnostic = match e {
                ParseError::Located {
                    line,
                    column,
                    message,
                    hint,
                    ..
                } => ValidateDiagnostic {
                    severity: "error",
                    line: line.saturating_sub(preamble_lines).max(1),
                    column,
                    message,
                    hint,
                },
                ParseError::UnexpectedToken {
                    position,
                    expected,
                    found,
                } => {
                    let (line, column) = position_to_line_col(&effective_source, position);
                    ValidateDiagnostic {
                        severity: "error",
                        line: line.saturating_sub(preamble_lines).max(1),
                        column,
                        message: format!("Expected {}, found {}", expected, found),
                        hint: None,
                    }
                }
                ParseError::UnexpectedEof => ValidateDiagnostic {
                    severity: "error",
                    line: body.source.lines().count().max(1),
                    column: 1,
                    message: "Unexpected end of input".to_string(),
                    hint: Some(
                        "Check for missing closing brackets or incomplete statements".to_string(),
                    ),
                },
                ParseError::InvalidToken { position, message } => {
                    let (line, column) = position_to_line_col(&effective_source, position);
                    ValidateDiagnostic {
                        severity: "error",
                        line: line.saturating_sub(preamble_lines).max(1),
                        column,
                        message,
                        hint: None,
                    }
                }
                ParseError::InvalidNumber(msg) => ValidateDiagnostic {
                    severity: "error",
                    line: 1,
                    column: 1,
                    message: format!("Invalid number: {}", msg),
                    hint: None,
                },
                ParseError::InvalidDuration(msg) => ValidateDiagnostic {
                    severity: "error",
                    line: 1,
                    column: 1,
                    message: format!("Invalid duration: {}", msg),
                    hint: Some("Use format like 5s, 10m, 1h".to_string()),
                },
                ParseError::InvalidTimestamp(msg) => ValidateDiagnostic {
                    severity: "error",
                    line: 1,
                    column: 1,
                    message: format!("Invalid timestamp: {}", msg),
                    hint: None,
                },
                ParseError::UnterminatedString(position) => {
                    let (line, column) = position_to_line_col(&effective_source, position);
                    ValidateDiagnostic {
                        severity: "error",
                        line: line.saturating_sub(preamble_lines).max(1),
                        column,
                        message: "Unterminated string".to_string(),
                        hint: Some("Add a closing quote".to_string()),
                    }
                }
                ParseError::InvalidEscape(msg) => ValidateDiagnostic {
                    severity: "error",
                    line: 1,
                    column: 1,
                    message: format!("Invalid escape sequence: {}", msg),
                    hint: None,
                },
                ParseError::Custom { span, message } => {
                    let (line, column) = position_to_line_col(&effective_source, span.start);
                    ValidateDiagnostic {
                        severity: "error",
                        line: line.saturating_sub(preamble_lines).max(1),
                        column,
                        message,
                        hint: None,
                    }
                }
            };
            let resp = ValidateResponse {
                valid: false,
                diagnostics: vec![diagnostic],
            };
            reply_json_status(&resp, StatusCode::OK)
        }
    }
}

/// Convert byte position to line and column numbers (1-indexed).
fn position_to_line_col(source: &str, position: usize) -> (usize, usize) {
    let mut line = 1;
    let mut column = 1;
    for (i, ch) in source.chars().enumerate() {
        if i >= position {
            break;
        }
        if ch == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }
    (line, column)
}

async fn handle_topology(State(state): State<AppState>, _auth: RbacViewer) -> Response {
    let coordinator = state.coordinator.clone();
    if let Some(resp) =
        forward_to_leader(&coordinator, "GET", "/api/v1/cluster/topology", None).await
    {
        return resp;
    }
    let coord = coordinator.read().await;

    // Build worker→pipeline_groups mapping
    let mut worker_groups: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    for g in coord.pipeline_groups.values() {
        for dep in g.placements.values() {
            worker_groups
                .entry(dep.worker_id.0.clone())
                .or_default()
                .push(g.name.clone());
        }
    }

    // Build flat workers list
    let workers: Vec<TopologyWorkerEntry> = coord
        .workers
        .values()
        .map(|w| TopologyWorkerEntry {
            id: w.id.0.clone(),
            address: w.address.clone(),
            status: w.status.to_string(),
            pipeline_groups: worker_groups.get(&w.id.0).cloned().unwrap_or_default(),
        })
        .collect();

    // Build flat routes by cross-referencing spec routes with placements
    let mut routes: Vec<TopologyRouteEntry> = Vec::new();
    for g in coord.pipeline_groups.values() {
        for r in &g.spec.routes {
            let source_worker = g
                .placements
                .get(&r.from_pipeline)
                .map(|d| d.worker_id.0.clone())
                .unwrap_or_default();
            let target_worker = g
                .placements
                .get(&r.to_pipeline)
                .map(|d| d.worker_id.0.clone())
                .unwrap_or_default();
            // Skip routes where either end is unplaced (e.g. _external)
            if source_worker.is_empty() || target_worker.is_empty() {
                continue;
            }
            routes.push(TopologyRouteEntry {
                source_worker,
                source_pipeline: r.from_pipeline.clone(),
                target_worker,
                target_pipeline: r.to_pipeline.clone(),
                route_type: "inter-pipeline".to_string(),
            });
        }
    }

    // Build groups (kept for MCP explain_alert and backwards compat)
    let groups: Vec<GroupTopology> = coord
        .pipeline_groups
        .values()
        .map(|g| {
            let pipelines = g
                .placements
                .iter()
                .map(|(name, dep)| PipelineTopologyEntry {
                    name: name.clone(),
                    worker_id: dep.worker_id.0.clone(),
                    worker_address: dep.worker_address.clone(),
                })
                .collect();
            let grp_routes = g
                .spec
                .routes
                .iter()
                .map(|r| RouteTopologyEntry {
                    from_pipeline: r.from_pipeline.clone(),
                    to_pipeline: r.to_pipeline.clone(),
                    event_types: r.event_types.clone(),
                })
                .collect();
            GroupTopology {
                group_id: g.id.clone(),
                group_name: g.name.clone(),
                pipelines,
                routes: grp_routes,
            }
        })
        .collect();

    let topology = TopologyInfo {
        workers,
        routes,
        groups,
    };
    reply_json_status(&topology, StatusCode::OK)
}

/// Proxy a pipeline topology request to the worker hosting it.
async fn handle_pipeline_topology(
    State(state): State<AppState>,
    Path(group_id): Path<String>,
    _auth: RbacViewer,
) -> Response {
    let coord = state.coordinator.read().await;

    let group = match coord.pipeline_groups.values().find(|g| g.id == group_id) {
        Some(g) => g,
        None => {
            return cluster_error_response(ClusterError::GroupNotFound(group_id));
        }
    };

    // Find first running deployment
    let deployment = match group
        .placements
        .values()
        .find(|d| d.status == crate::pipeline_group::PipelineDeploymentStatus::Running)
    {
        Some(d) => d.clone(),
        None => {
            return reply_json_status(
                &serde_json::json!({"nodes": [], "edges": [], "sources": [], "sinks": []}),
                StatusCode::OK,
            );
        }
    };

    let client = coord.http_client.clone();
    drop(coord);

    let url = format!(
        "{}/api/v1/pipelines/{}/topology",
        deployment.worker_address, deployment.pipeline_id
    );

    match client
        .get(&url)
        .header("x-api-key", &deployment.worker_api_key)
        .send()
        .await
    {
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            let axum_status =
                StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            (
                axum_status,
                [("content-type", "application/json".to_string())],
                body,
            )
                .into_response()
        }
        Err(e) => {
            tracing::warn!("Cannot reach worker for topology: {e}");
            reply_json_status(
                &serde_json::json!({"nodes": [], "edges": [], "sources": [], "sinks": []}),
                StatusCode::OK,
            )
        }
    }
}

// =============================================================================
// Model Registry handlers
// =============================================================================

async fn handle_list_models(
    State(state): State<AppState>,
    _auth: RbacViewer,
    Query(pagination): Query<PaginationParams>,
) -> Response {
    let coordinator = state.coordinator.clone();
    if pagination.exceeds_max() {
        return error_response(
            StatusCode::BAD_REQUEST,
            &format!("limit must not exceed {MAX_LIMIT}"),
        );
    }
    if let Some(resp) = forward_to_leader(&coordinator, "GET", "/api/v1/cluster/models", None).await
    {
        return resp;
    }
    let coord = coordinator.read().await;
    let all_models: Vec<crate::model_registry::ModelRegistryEntry> =
        coord.model_registry.values().cloned().collect();
    let (models, meta) = pagination.paginate(all_models);
    let resp = serde_json::json!({ "models": models, "total": meta.total, "pagination": meta });
    reply_json_status(&resp, StatusCode::OK)
}

#[derive(Debug, Serialize, Deserialize)]
struct UploadModelRequest {
    name: String,
    format: Option<String>,
    inputs: Vec<String>,
    outputs: Vec<String>,
    #[serde(default)]
    description: String,
    /// Base64-encoded model data (for JSON upload without multipart)
    #[serde(default)]
    data_base64: String,
}

async fn handle_upload_model(
    State(state): State<AppState>,
    _auth: RbacOperator,
    Json(body): Json<UploadModelRequest>,
) -> Response {
    let coordinator = state.coordinator.clone();
    if let Some(resp) = forward_to_leader(
        &coordinator,
        "POST",
        "/api/v1/cluster/models",
        serde_json::to_value(&body).ok(),
    )
    .await
    {
        return resp;
    }

    let name = body.name.clone();

    // Validate model name to prevent path traversal (e.g. "../../etc/evil")
    if !connector_config::is_valid_connector_name(&name) {
        let resp = serde_json::json!({
            "error": "Invalid model name: must start with a letter or underscore and contain only ASCII alphanumeric characters or underscores"
        });
        return reply_json_status(&resp, StatusCode::BAD_REQUEST);
    }

    let s3_key = format!("models/{}.onnx", &name);

    // Decode and persist model file if data_base64 is provided
    let size_bytes = if !body.data_base64.is_empty() {
        use base64::Engine as _;
        match base64::engine::general_purpose::STANDARD.decode(&body.data_base64) {
            Ok(data) => {
                let model_dir = std::path::Path::new("/app/models");
                if !model_dir.exists() {
                    let _ = std::fs::create_dir_all(model_dir);
                }
                let model_path = model_dir.join(format!("{}.onnx", &name));
                if let Err(e) = std::fs::write(&model_path, &data) {
                    let resp = serde_json::json!({
                        "error": format!("Failed to write model file: {}", e)
                    });
                    return reply_with_status(Json(&resp), StatusCode::INTERNAL_SERVER_ERROR);
                }
                data.len() as u64
            }
            Err(e) => {
                let resp = serde_json::json!({
                    "error": format!("Invalid base64 data: {}", e)
                });
                return reply_with_status(Json(&resp), StatusCode::BAD_REQUEST);
            }
        }
    } else {
        0
    };

    let entry = crate::model_registry::ModelRegistryEntry {
        name: name.clone(),
        s3_key,
        format: body.format.unwrap_or_else(|| "onnx".to_string()),
        inputs: body.inputs,
        outputs: body.outputs,
        size_bytes,
        uploaded_at: chrono::Utc::now().to_rfc3339(),
        description: body.description,
    };

    let mut coord = coordinator.write().await;

    // Replicate via Raft if available
    #[cfg(feature = "raft")]
    if let Some(ref handle) = coord.raft_handle {
        let cmd = crate::raft::ClusterCommand::ModelRegistered {
            name: name.clone(),
            entry: entry.clone(),
        };
        if let Err(e) = handle.raft.client_write(cmd).await {
            let resp = serde_json::json!({ "error": format!("Raft replication failed: {}", e) });
            return reply_with_status(Json(&resp), StatusCode::INTERNAL_SERVER_ERROR);
        }
    }

    coord.model_registry.insert(name, entry.clone());
    reply_json_status(&entry, StatusCode::CREATED)
}

async fn handle_delete_model(
    State(state): State<AppState>,
    Path(name): Path<String>,
    _auth: RbacAdmin,
) -> Response {
    let coordinator = state.coordinator.clone();
    if let Some(resp) = forward_to_leader(
        &coordinator,
        "DELETE",
        &format!("/api/v1/cluster/models/{name}"),
        None,
    )
    .await
    {
        return resp;
    }

    let mut coord = coordinator.write().await;

    if !coord.model_registry.contains_key(&name) {
        let resp = serde_json::json!({ "error": format!("Model '{}' not found", name) });
        return reply_json_status(&resp, StatusCode::NOT_FOUND);
    }

    #[cfg(feature = "raft")]
    if let Some(ref handle) = coord.raft_handle {
        let cmd = crate::raft::ClusterCommand::ModelRemoved { name: name.clone() };
        if let Err(e) = handle.raft.client_write(cmd).await {
            let resp = serde_json::json!({ "error": format!("Raft replication failed: {}", e) });
            return reply_with_status(Json(&resp), StatusCode::INTERNAL_SERVER_ERROR);
        }
    }

    coord.model_registry.remove(&name);
    let resp = serde_json::json!({ "deleted": name });
    reply_json_status(&resp, StatusCode::OK)
}

async fn handle_download_model(
    State(state): State<AppState>,
    Path(name): Path<String>,
    _auth: RbacViewer,
) -> Response {
    let coordinator = state.coordinator.clone();
    if let Some(resp) = forward_to_leader(
        &coordinator,
        "GET",
        &format!("/api/v1/cluster/models/{name}/download"),
        None,
    )
    .await
    {
        return resp;
    }
    let coord = coordinator.read().await;
    match coord.model_registry.get(&name) {
        Some(entry) => {
            let resp = serde_json::json!(entry);
            reply_json_status(&resp, StatusCode::OK)
        }
        None => {
            let resp = serde_json::json!({ "error": format!("Model '{}' not found", name) });
            reply_json_status(&resp, StatusCode::NOT_FOUND)
        }
    }
}

// =============================================================================
// Chat handlers
// =============================================================================

async fn handle_chat(
    State(state): State<AppState>,
    _auth: RbacViewer,
    Json(body): Json<crate::chat::ChatRequest>,
) -> Response {
    let coordinator = state.coordinator.clone();
    if let Some(resp) = forward_to_leader(
        &coordinator,
        "POST",
        "/api/v1/cluster/chat",
        serde_json::to_value(&body).ok(),
    )
    .await
    {
        return resp;
    }
    let config = {
        let coord = coordinator.read().await;
        coord.llm_config.clone()
    };

    let config = match config {
        Some(c) => c,
        None => {
            let resp = serde_json::json!({
                "error": "No LLM provider configured. Set VARPULIS_LLM_ENDPOINT or configure in Settings."
            });
            return reply_with_status(Json(&resp), StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    match crate::chat::chat_completion(&config, &body.messages, &coordinator).await {
        Ok(response) => reply_json_status(&response, StatusCode::OK),
        Err(e) => {
            let status = if e.contains("timed out") || e.contains("timeout") {
                StatusCode::GATEWAY_TIMEOUT
            } else if e.contains("connect") || e.contains("Connection refused") {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::BAD_GATEWAY
            };
            let resp = serde_json::json!({ "error": e });
            reply_json_status(&resp, status)
        }
    }
}

async fn handle_get_chat_config(State(state): State<AppState>, _auth: RbacViewer) -> Response {
    let coordinator = state.coordinator.clone();
    if let Some(resp) =
        forward_to_leader(&coordinator, "GET", "/api/v1/cluster/chat/config", None).await
    {
        return resp;
    }
    let coord = coordinator.read().await;
    let resp = match &coord.llm_config {
        Some(config) => crate::chat::LlmConfigResponse {
            provider: format!("{:?}", config.provider)
                .to_lowercase()
                .replace("openaicompatible", "openai-compatible"),
            model: config.model.clone(),
            endpoint: config.endpoint.clone(),
            has_api_key: config.api_key.is_some(),
            configured: true,
        },
        None => crate::chat::LlmConfigResponse {
            provider: String::new(),
            model: String::new(),
            endpoint: String::new(),
            has_api_key: false,
            configured: false,
        },
    };
    reply_json_status(&resp, StatusCode::OK)
}

async fn handle_update_chat_config(
    State(state): State<AppState>,
    _auth: RbacOperator,
    Json(body): Json<crate::chat::LlmConfig>,
) -> Response {
    let coordinator = state.coordinator.clone();
    if let Some(resp) = forward_to_leader(
        &coordinator,
        "PUT",
        "/api/v1/cluster/chat/config",
        serde_json::to_value(&body).ok(),
    )
    .await
    {
        return resp;
    }

    let mut coord = coordinator.write().await;
    coord.llm_config = Some(body);
    let resp = serde_json::json!({ "status": "ok" });
    reply_json_status(&resp, StatusCode::OK)
}

// =============================================================================
// Migration / Drain / Rebalance handlers
// =============================================================================

async fn handle_drain_worker(
    State(state): State<AppState>,
    Path(worker_id): Path<String>,
    _auth: RbacOperator,
    Json(body): Json<DrainRequest>,
) -> Response {
    let coordinator = state.coordinator.clone();
    if let Some(resp) = forward_to_leader(
        &coordinator,
        "POST",
        &format!("/api/v1/cluster/workers/{worker_id}/drain"),
        serde_json::to_value(&body).ok(),
    )
    .await
    {
        return resp;
    }

    let mut coord = coordinator.write().await;

    let timeout = body.timeout_secs.map(std::time::Duration::from_secs);
    match coord
        .drain_worker(&WorkerId(worker_id.clone()), timeout)
        .await
    {
        Ok(migration_ids) => {
            let resp = DrainResponse {
                worker_id,
                pipelines_migrated: migration_ids.len(),
                status: "drained".into(),
            };
            reply_json_status(&resp, StatusCode::OK)
        }
        Err(e) => cluster_error_response(e),
    }
}

async fn handle_rebalance(State(state): State<AppState>, _auth: RbacOperator) -> Response {
    let coordinator = state.coordinator.clone();
    if let Some(resp) =
        forward_to_leader(&coordinator, "POST", "/api/v1/cluster/rebalance", None).await
    {
        return resp;
    }

    let mut coord = coordinator.write().await;

    match coord.rebalance().await {
        Ok(migration_ids) => {
            // Replicate updated group states through Raft
            #[cfg(feature = "raft")]
            if !migration_ids.is_empty() {
                if let Some(ref handle) = coord.raft_handle {
                    for (name, group) in &coord.pipeline_groups {
                        let group_json = serde_json::to_value(group).unwrap_or_default();
                        let cmd = crate::raft::ClusterCommand::GroupUpdated {
                            name: name.clone(),
                            group: group_json,
                        };
                        if let Err(e) = handle.raft.client_write(cmd).await {
                            tracing::error!("Raft replication failed for rebalance: {e}");
                            return reply_with_status(
                                Json(&serde_json::json!({
                                    "error": format!("Operation applied locally but Raft replication failed: {e}")
                                })),
                                StatusCode::INTERNAL_SERVER_ERROR,
                            );
                        }
                    }
                }
            }

            let resp = RebalanceResponse {
                migrations_started: migration_ids.len(),
                migration_ids,
            };
            reply_json_status(&resp, StatusCode::OK)
        }
        Err(e) => cluster_error_response(e),
    }
}

/// Request body for the rescale endpoint.
#[derive(Debug, serde::Deserialize)]
struct RescaleRequest {
    target_workers: usize,
}

async fn handle_rescale(
    State(state): State<AppState>,
    _auth: RbacAdmin,
    Json(body): Json<RescaleRequest>,
) -> Response {
    let coordinator = state.coordinator.clone();
    if let Some(resp) =
        forward_to_leader(&coordinator, "POST", "/api/v1/cluster/rescale", None).await
    {
        return resp;
    }

    let mut coord = coordinator.write().await;

    match coord.rescale(body.target_workers).await {
        Ok(result) => reply_json_status(&result, StatusCode::OK),
        Err(e) => cluster_error_response(e),
    }
}

async fn handle_list_migrations(
    State(state): State<AppState>,
    _auth: RbacViewer,
    Query(pagination): Query<PaginationParams>,
) -> Response {
    let coordinator = state.coordinator.clone();
    if pagination.exceeds_max() {
        return error_response(
            StatusCode::BAD_REQUEST,
            &format!("limit must not exceed {MAX_LIMIT}"),
        );
    }
    if let Some(resp) =
        forward_to_leader(&coordinator, "GET", "/api/v1/cluster/migrations", None).await
    {
        return resp;
    }
    let coord = coordinator.read().await;
    let all_migrations: Vec<MigrationInfo> = coord
        .active_migrations
        .values()
        .map(|m| MigrationInfo {
            id: m.id.clone(),
            pipeline_name: m.pipeline_name.clone(),
            group_id: m.group_id.clone(),
            source_worker: m.source_worker.0.clone(),
            target_worker: m.target_worker.0.clone(),
            status: m.status.to_string(),
            reason: m.reason.to_string(),
            elapsed_ms: m.started_at.elapsed().as_millis(),
        })
        .collect();
    let (migrations, meta) = pagination.paginate(all_migrations);
    let resp = serde_json::json!({
        "migrations": migrations,
        "total": meta.total,
        "pagination": meta,
    });
    reply_json_status(&resp, StatusCode::OK)
}

async fn handle_get_migration(
    State(state): State<AppState>,
    Path(migration_id): Path<String>,
    _auth: RbacViewer,
) -> Response {
    let coordinator = state.coordinator.clone();
    if let Some(resp) = forward_to_leader(
        &coordinator,
        "GET",
        &format!("/api/v1/cluster/migrations/{migration_id}"),
        None,
    )
    .await
    {
        return resp;
    }
    let coord = coordinator.read().await;
    match coord.active_migrations.get(&migration_id) {
        Some(m) => {
            let info = MigrationInfo {
                id: m.id.clone(),
                pipeline_name: m.pipeline_name.clone(),
                group_id: m.group_id.clone(),
                source_worker: m.source_worker.0.clone(),
                target_worker: m.target_worker.0.clone(),
                status: m.status.to_string(),
                reason: m.reason.to_string(),
                elapsed_ms: m.started_at.elapsed().as_millis(),
            };
            reply_json_status(&info, StatusCode::OK)
        }
        None => error_response(StatusCode::NOT_FOUND, "Migration not found"),
    }
}

async fn handle_manual_migrate(
    State(state): State<AppState>,
    Path((group_id, pipeline_name)): Path<(String, String)>,
    _auth: RbacOperator,
    Json(body): Json<ManualMigrateRequest>,
) -> Response {
    let coordinator = state.coordinator.clone();
    if let Some(resp) = forward_to_leader(
        &coordinator,
        "POST",
        &format!("/api/v1/cluster/pipelines/{group_id}/{pipeline_name}/migrate"),
        serde_json::to_value(&body).ok(),
    )
    .await
    {
        return resp;
    }

    // Phase 1 (plan, read lock) + Phase 2 (execute, no lock).
    // Transport is NATS when a client is configured, else HTTP (default).
    #[cfg(feature = "nats-transport")]
    let (plan, result) = {
        let (plan, http_client, source_alive, connectors, nats_client) = {
            let coord = coordinator.read().await;

            match coord.plan_migrate_pipeline(
                &pipeline_name,
                &group_id,
                &WorkerId(body.target_worker_id),
                MigrationReason::Manual,
            ) {
                Ok(plan) => {
                    let source_alive = coord
                        .workers
                        .get(&plan.source_worker_id)
                        .map(|w| w.status != crate::worker::WorkerStatus::Unhealthy)
                        .unwrap_or(false);
                    let connectors = coord.connectors.clone();
                    (
                        plan,
                        coord.http_client.clone(),
                        source_alive,
                        connectors,
                        coord.nats_client.clone(),
                    )
                }
                Err(e) => return cluster_error_response(e),
            }
        };
        // Read lock released here
        let result = crate::coordinator::Coordinator::execute_migrate_plan_dispatch(
            nats_client.as_ref(),
            &http_client,
            &plan,
            source_alive,
            &connectors,
            None,
        )
        .await;
        (plan, result)
    };
    #[cfg(not(feature = "nats-transport"))]
    let (plan, result) = {
        let (plan, http_client, source_alive, connectors) = {
            let coord = coordinator.read().await;

            match coord.plan_migrate_pipeline(
                &pipeline_name,
                &group_id,
                &WorkerId(body.target_worker_id),
                MigrationReason::Manual,
            ) {
                Ok(plan) => {
                    let source_alive = coord
                        .workers
                        .get(&plan.source_worker_id)
                        .map(|w| w.status != crate::worker::WorkerStatus::Unhealthy)
                        .unwrap_or(false);
                    let connectors = coord.connectors.clone();
                    (plan, coord.http_client.clone(), source_alive, connectors)
                }
                Err(e) => return cluster_error_response(e),
            }
        };
        // Read lock released here
        let result = crate::coordinator::Coordinator::execute_migrate_plan(
            &http_client,
            &plan,
            source_alive,
            &connectors,
        )
        .await;
        (plan, result)
    };

    // Phase 3: Commit results (write lock)
    let mut coord = coordinator.write().await;
    match result {
        Ok(new_pipeline_id) => {
            let migration_id = coord.commit_migrate_pipeline(&plan, &new_pipeline_id, true, None);

            // Replicate updated group state through Raft
            #[cfg(feature = "raft")]
            if let Some(ref handle) = coord.raft_handle {
                if let Some(group) = coord.pipeline_groups.get(&group_id) {
                    let group_json = serde_json::to_value(group).unwrap_or_default();
                    let cmd = crate::raft::ClusterCommand::GroupUpdated {
                        name: group_id.clone(),
                        group: group_json,
                    };
                    if let Err(e) = handle.raft.client_write(cmd).await {
                        tracing::error!("Raft replication failed for migrate_pipeline: {e}");
                        return reply_with_status(
                            Json(&serde_json::json!({
                                "error": format!("Operation applied locally but Raft replication failed: {e}")
                            })),
                            StatusCode::INTERNAL_SERVER_ERROR,
                        );
                    }
                }
            }

            let resp = serde_json::json!({
                "migration_id": migration_id,
                "pipeline": pipeline_name,
                "group_id": group_id,
                "status": "started",
            });
            reply_json_status(&resp, StatusCode::ACCEPTED)
        }
        Err(reason) => {
            coord.commit_migrate_pipeline(&plan, "", false, Some(reason.clone()));
            cluster_error_response(ClusterError::MigrationFailed(reason))
        }
    }
}

// =============================================================================
// Connector handlers
// =============================================================================

async fn handle_list_connectors(
    State(state): State<AppState>,
    _auth: RbacViewer,
    Query(pagination): Query<PaginationParams>,
) -> Response {
    let coordinator = state.coordinator.clone();
    if pagination.exceeds_max() {
        return error_response(
            StatusCode::BAD_REQUEST,
            &format!("limit must not exceed {MAX_LIMIT}"),
        );
    }
    if let Some(resp) =
        forward_to_leader(&coordinator, "GET", "/api/v1/cluster/connectors", None).await
    {
        return resp;
    }
    let coord = coordinator.read().await;
    let all_connectors: Vec<ClusterConnector> =
        coord.list_connectors().into_iter().cloned().collect();
    let (connectors, meta) = pagination.paginate(all_connectors);
    let resp = serde_json::json!({
        "connectors": connectors,
        "total": meta.total,
        "pagination": meta,
    });
    reply_json_status(&resp, StatusCode::OK)
}

async fn handle_get_connector(
    State(state): State<AppState>,
    Path(name): Path<String>,
    _auth: RbacViewer,
) -> Response {
    let coordinator = state.coordinator.clone();
    if let Some(resp) = forward_to_leader(
        &coordinator,
        "GET",
        &format!("/api/v1/cluster/connectors/{name}"),
        None,
    )
    .await
    {
        return resp;
    }
    let coord = coordinator.read().await;
    match coord.get_connector(&name) {
        Ok(connector) => reply_json_status(connector, StatusCode::OK),
        Err(e) => cluster_error_response(e),
    }
}

async fn handle_create_connector(
    State(state): State<AppState>,
    _auth: RbacOperator,
    Json(body): Json<ClusterConnector>,
) -> Response {
    let coordinator = state.coordinator.clone();
    if let Some(resp) = forward_to_leader(
        &coordinator,
        "POST",
        "/api/v1/cluster/connectors",
        serde_json::to_value(&body).ok(),
    )
    .await
    {
        return resp;
    }

    let mut coord = coordinator.write().await;

    // Replicate through Raft if enabled
    #[cfg(feature = "raft")]
    if let Some(ref handle) = coord.raft_handle {
        let cmd = crate::raft::ClusterCommand::ConnectorCreated {
            name: body.name.clone(),
            connector: body.clone(),
        };
        if let Err(e) = handle.raft.client_write(cmd).await {
            return cluster_error_response(ClusterError::NotLeader(e.to_string()));
        }
    }

    match coord.create_connector(body) {
        Ok(connector) => reply_with_status(Json(connector), StatusCode::CREATED),
        Err(e) => cluster_error_response(e),
    }
}

async fn handle_update_connector(
    State(state): State<AppState>,
    Path(name): Path<String>,
    _auth: RbacOperator,
    Json(body): Json<ClusterConnector>,
) -> Response {
    let coordinator = state.coordinator.clone();
    if let Some(resp) = forward_to_leader(
        &coordinator,
        "PUT",
        &format!("/api/v1/cluster/connectors/{name}"),
        serde_json::to_value(&body).ok(),
    )
    .await
    {
        return resp;
    }

    let mut coord = coordinator.write().await;

    #[cfg(feature = "raft")]
    if let Some(ref handle) = coord.raft_handle {
        let cmd = crate::raft::ClusterCommand::ConnectorUpdated {
            name: name.clone(),
            connector: body.clone(),
        };
        if let Err(e) = handle.raft.client_write(cmd).await {
            return cluster_error_response(ClusterError::NotLeader(e.to_string()));
        }
    }

    match coord.update_connector(&name, body) {
        Ok(connector) => reply_json_status(connector, StatusCode::OK),
        Err(e) => cluster_error_response(e),
    }
}

async fn handle_delete_connector(
    State(state): State<AppState>,
    Path(name): Path<String>,
    _auth: RbacAdmin,
) -> Response {
    let coordinator = state.coordinator.clone();
    if let Some(resp) = forward_to_leader(
        &coordinator,
        "DELETE",
        &format!("/api/v1/cluster/connectors/{name}"),
        None,
    )
    .await
    {
        return resp;
    }

    let mut coord = coordinator.write().await;

    #[cfg(feature = "raft")]
    if let Some(ref handle) = coord.raft_handle {
        let cmd = crate::raft::ClusterCommand::ConnectorRemoved { name: name.clone() };
        if let Err(e) = handle.raft.client_write(cmd).await {
            return cluster_error_response(ClusterError::NotLeader(e.to_string()));
        }
    }

    match coord.delete_connector(&name) {
        Ok(()) => reply_with_status(Json(&serde_json::json!({"deleted": true})), StatusCode::OK),
        Err(e) => cluster_error_response(e),
    }
}

// =============================================================================
// Metrics handler
// =============================================================================

async fn handle_metrics(State(state): State<AppState>, _auth: RbacViewer) -> Response {
    let coordinator = state.coordinator.clone();
    if let Some(resp) =
        forward_to_leader(&coordinator, "GET", "/api/v1/cluster/metrics", None).await
    {
        return resp;
    }
    let coord = coordinator.read().await;
    let metrics = coord.get_cluster_metrics();
    reply_json_status(&metrics, StatusCode::OK)
}

async fn handle_prometheus_metrics(State(state): State<AppState>, _auth: RbacViewer) -> Response {
    let coordinator = state.coordinator.clone();
    if let Some(resp) =
        forward_to_leader(&coordinator, "GET", "/api/v1/cluster/prometheus", None).await
    {
        return resp;
    }
    let coord = coordinator.read().await;
    let text = coord.cluster_metrics.gather();
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        text,
    )
        .into_response()
}

async fn handle_scaling(State(state): State<AppState>, _auth: RbacViewer) -> Response {
    let coordinator = state.coordinator.clone();
    if let Some(resp) =
        forward_to_leader(&coordinator, "GET", "/api/v1/cluster/scaling", None).await
    {
        return resp;
    }
    let coord = coordinator.read().await;
    match &coord.last_scaling_recommendation {
        Some(rec) => reply_json_status(rec, StatusCode::OK),
        None => {
            let resp = serde_json::json!({
                "action": "stable",
                "current_workers": coord.workers.values().filter(|w| w.status == crate::worker::WorkerStatus::Ready).count(),
                "target_workers": coord.workers.values().filter(|w| w.status == crate::worker::WorkerStatus::Ready).count(),
                "reason": "No scaling policy configured",
                "avg_pipelines_per_worker": 0.0,
                "total_pipelines": 0,
                "timestamp": chrono::Utc::now().to_rfc3339(),
            });
            reply_json_status(&resp, StatusCode::OK)
        }
    }
}

async fn handle_cluster_summary(State(state): State<AppState>, _auth: RbacViewer) -> Response {
    let coordinator = state.coordinator.clone();
    if let Some(resp) =
        forward_to_leader(&coordinator, "GET", "/api/v1/cluster/summary", None).await
    {
        return resp;
    }
    let coord = coordinator.read().await;

    let total_workers = coord.workers.len();
    let healthy_workers = coord
        .workers
        .values()
        .filter(|w| w.status == crate::worker::WorkerStatus::Ready)
        .count();
    let unhealthy_workers = coord
        .workers
        .values()
        .filter(|w| w.status == crate::worker::WorkerStatus::Unhealthy)
        .count();
    let draining_workers = coord
        .workers
        .values()
        .filter(|w| w.status == crate::worker::WorkerStatus::Draining)
        .count();

    let total_pipeline_groups = coord.pipeline_groups.len();
    let active_pipeline_groups = coord
        .pipeline_groups
        .values()
        .filter(|g| {
            g.status == crate::pipeline_group::GroupStatus::Running
                || g.status == crate::pipeline_group::GroupStatus::PartiallyRunning
        })
        .count();

    let metrics = coord.get_cluster_metrics();
    let total_events_processed: u64 = metrics.pipelines.iter().map(|p| p.events_in).sum();
    let events_per_second: f64 = 0.0; // instantaneous rate requires two samples; frontend computes delta

    let resp = serde_json::json!({
        "total_workers": total_workers,
        "healthy_workers": healthy_workers,
        "unhealthy_workers": unhealthy_workers,
        "draining_workers": draining_workers,
        "total_pipeline_groups": total_pipeline_groups,
        "active_pipeline_groups": active_pipeline_groups,
        "total_events_processed": total_events_processed,
        "events_per_second": events_per_second,
    });

    reply_json_status(&resp, StatusCode::OK)
}

// =============================================================================
// Raft cluster status handler
// =============================================================================

async fn handle_raft_status(State(state): State<AppState>, _auth: RbacViewer) -> Response {
    let coordinator = state.coordinator.clone();
    let _coord = coordinator.read().await;

    #[cfg(feature = "raft")]
    {
        if let Some(ref handle) = _coord.raft_handle {
            let metrics = handle.raft.metrics().borrow().clone();
            let current_leader = metrics.current_leader;
            let this_node_id = metrics.id;

            let mut nodes = Vec::new();
            for (nid, addr) in &handle.peer_addrs {
                let role = match current_leader {
                    Some(leader_id) if *nid == leader_id => "leader",
                    Some(_) => "follower",
                    None => "unknown",
                };
                nodes.push(serde_json::json!({
                    "id": nid,
                    "address": addr,
                    "role": role,
                    "is_current": *nid == this_node_id,
                }));
            }

            let resp = serde_json::json!({
                "enabled": true,
                "this_node_id": this_node_id,
                "leader_id": current_leader,
                "term": metrics.current_term,
                "commit_index": metrics.last_applied.as_ref().map(|l| l.index).unwrap_or(0),
                "nodes": nodes,
            });
            return reply_json_status(&resp, StatusCode::OK);
        }
    }

    // Raft not enabled or no handle — standalone mode
    let resp = serde_json::json!({
        "enabled": false,
        "this_node_id": 0,
        "leader_id": null,
        "term": 0,
        "commit_index": 0,
        "nodes": [],
    });
    reply_json_status(&resp, StatusCode::OK)
}

// =============================================================================
// Pipeline Graph Handlers (Visual Builder)
// =============================================================================

#[derive(Debug, Deserialize)]
struct PipelineGraphRequest {
    vpl: String,
}

#[derive(Debug, Serialize)]
struct GenerateResponse {
    vpl: String,
}

async fn handle_pipeline_to_graph(
    State(_state): State<AppState>,
    _auth: RbacViewer,
    Json(body): Json<PipelineGraphRequest>,
) -> Response {
    match varpulis_parser::parse(&body.vpl) {
        Ok(program) => {
            let graph = varpulis_runtime::engine::graph::program_to_graph(&program);
            reply_json_status(&graph, StatusCode::OK)
        }
        Err(e) => error_response(
            StatusCode::BAD_REQUEST,
            &format!("Failed to parse VPL: {e}"),
        ),
    }
}

async fn handle_graph_to_pipeline(
    State(_state): State<AppState>,
    _auth: RbacViewer,
    Json(graph): Json<varpulis_runtime::engine::graph::PipelineGraph>,
) -> Response {
    let vpl = varpulis_runtime::engine::graph::graph_to_vpl(&graph);
    let resp = GenerateResponse { vpl };
    reply_json_status(&resp, StatusCode::OK)
}

// =============================================================================
// Error handling
// =============================================================================

#[derive(Debug, Serialize)]
struct ApiError {
    error: String,
    code: String,
}

/// Build a JSON error response with the given status code and message.
fn error_response(status: StatusCode, message: &str) -> Response {
    let body = ApiError {
        error: message.to_string(),
        code: status.as_str().to_string(),
    };
    (status, Json(body)).into_response()
}

fn cluster_error_response(err: ClusterError) -> Response {
    let (status, code) = match &err {
        ClusterError::WorkerNotFound(_) => (StatusCode::NOT_FOUND, "worker_not_found"),
        ClusterError::GroupNotFound(_) => (StatusCode::NOT_FOUND, "group_not_found"),
        ClusterError::NoWorkersAvailable => {
            (StatusCode::SERVICE_UNAVAILABLE, "no_workers_available")
        }
        ClusterError::DeployFailed(_) => (StatusCode::INTERNAL_SERVER_ERROR, "deploy_failed"),
        ClusterError::RoutingFailed(_) => (StatusCode::BAD_GATEWAY, "routing_failed"),
        ClusterError::ConnectorNotFound(_) => (StatusCode::NOT_FOUND, "connector_not_found"),
        ClusterError::ConnectorValidation(_) => (StatusCode::BAD_REQUEST, "connector_validation"),
        ClusterError::MigrationFailed(_) => (StatusCode::INTERNAL_SERVER_ERROR, "migration_failed"),
        ClusterError::WorkerDraining(_) => (StatusCode::CONFLICT, "worker_draining"),
        ClusterError::NotLeader(_) => (StatusCode::MISDIRECTED_REQUEST, "not_leader"),
        ClusterError::InvalidOperation(_) => (StatusCode::BAD_REQUEST, "invalid_operation"),
    };
    let body = ApiError {
        error: err.to_string(),
        code: code.to_string(),
    };
    (status, Json(body)).into_response()
}

/// Helper: build a JSON response with a status code.
/// Replaces the common `(status, Json(data)).into_response()` pattern.
fn reply_json_status<T: Serialize>(data: &T, status: StatusCode) -> Response {
    (status, Json(serde_json::to_value(data).unwrap_or_default())).into_response()
}

/// Helper: combine a pre-built body and status code into a Response.
/// Accepts `(body, StatusCode)` where `body` is anything `IntoResponse`.
fn reply_with_status<B: IntoResponse>(body: B, status: StatusCode) -> Response {
    (status, body).into_response()
}

// =============================================================================
// Federation Handlers
// =============================================================================

#[cfg(feature = "federation")]
async fn handle_federation_status(State(state): State<AppState>, _auth: RbacViewer) -> Response {
    let coordinator = state.coordinator.clone();
    let coord = coordinator.read().await;
    match &coord.federation {
        Some(fed) => reply_with_status(Json(&fed.status()), StatusCode::OK),
        None => reply_with_status(
            Json(&serde_json::json!({
                "error": "Federation not enabled"
            })),
            StatusCode::NOT_FOUND,
        ),
    }
}

#[cfg(feature = "federation")]
async fn handle_federation_regions(State(state): State<AppState>, _auth: RbacViewer) -> Response {
    let coordinator = state.coordinator.clone();
    let coord = coordinator.read().await;
    match &coord.federation {
        Some(fed) => {
            let regions: Vec<_> = fed.get_regions().values().collect();
            reply_with_status(Json(&regions), StatusCode::OK)
        }
        None => reply_with_status(
            Json(&serde_json::json!({
                "error": "Federation not enabled"
            })),
            StatusCode::NOT_FOUND,
        ),
    }
}

#[cfg(feature = "federation")]
async fn handle_federation_add_region(
    State(state): State<AppState>,
    _auth: RbacAdmin,
    Json(config): Json<crate::federation::RegionConfig>,
) -> Response {
    let coordinator = state.coordinator.clone();
    let mut coord = coordinator.write().await;
    if coord.federation.is_none() {
        coord.federation = Some(crate::federation::FederationCoordinator::new(
            crate::federation::FederationConfig::default(),
        ));
    }
    if let Some(ref mut fed) = coord.federation {
        fed.register_region(&config);
        reply_with_status(
            Json(&serde_json::json!({
                "status": "registered",
                "region": config.name
            })),
            StatusCode::CREATED,
        )
    } else {
        reply_with_status(
            Json(&serde_json::json!({
                "error": "Federation initialization failed"
            })),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
    }
}

#[cfg(feature = "federation")]
async fn handle_federation_remove_region(
    State(state): State<AppState>,
    Path(region_name): Path<String>,
    _auth: RbacAdmin,
) -> Response {
    let coordinator = state.coordinator.clone();
    let mut coord = coordinator.write().await;
    match &mut coord.federation {
        Some(fed) => {
            if fed.deregister_region(&region_name) {
                reply_with_status(
                    Json(&serde_json::json!({
                        "status": "deregistered",
                        "region": region_name
                    })),
                    StatusCode::OK,
                )
            } else {
                reply_with_status(
                    Json(&serde_json::json!({
                        "error": "Region not found",
                        "region": region_name
                    })),
                    StatusCode::NOT_FOUND,
                )
            }
        }
        None => reply_with_status(
            Json(&serde_json::json!({
                "error": "Federation not enabled"
            })),
            StatusCode::NOT_FOUND,
        ),
    }
}

#[cfg(feature = "federation")]
async fn handle_federation_catalog(State(state): State<AppState>, _auth: RbacViewer) -> Response {
    let coordinator = state.coordinator.clone();
    let coord = coordinator.read().await;
    match &coord.federation {
        Some(fed) => {
            let catalog: Vec<serde_json::Value> = fed
                .get_global_catalog()
                .into_iter()
                .map(|(region, entry)| {
                    serde_json::json!({
                        "region": region,
                        "pipeline": entry.pipeline_name,
                        "group": entry.group_name,
                        "event_types": entry.event_types,
                    })
                })
                .collect();
            reply_with_status(Json(&catalog), StatusCode::OK)
        }
        None => reply_with_status(
            Json(&serde_json::json!({
                "error": "Federation not enabled"
            })),
            StatusCode::NOT_FOUND,
        ),
    }
}

// =============================================================================
// Health Probe Handlers
// =============================================================================

/// Liveness probe — always returns 200.
async fn handle_health_live() -> Response {
    reply_json_status(&serde_json::json!({"status": "ok"}), StatusCode::OK)
}

/// Readiness probe — returns 200 when the coordinator has at least one ready worker.
async fn handle_health_ready(State(state): State<AppState>) -> Response {
    let coordinator = state.coordinator.read().await;
    let has_ready = coordinator
        .workers
        .values()
        .any(|w| w.status == crate::worker::WorkerStatus::Ready);
    if has_ready {
        reply_json_status(&serde_json::json!({"status": "ready"}), StatusCode::OK)
    } else {
        reply_json_status(
            &serde_json::json!({"status": "not_ready", "reason": "no ready workers"}),
            StatusCode::SERVICE_UNAVAILABLE,
        )
    }
}

/// Startup probe — returns 200 once the coordinator exists (server is up).
async fn handle_health_started() -> Response {
    reply_json_status(&serde_json::json!({"status": "started"}), StatusCode::OK)
}

/// Detailed health endpoint — returns per-component health as JSON.
async fn handle_health_detailed(State(state): State<AppState>) -> Response {
    let coordinator = state.coordinator.read().await;

    let worker_health: Vec<serde_json::Value> = coordinator
        .workers
        .values()
        .map(|w| {
            serde_json::json!({
                "id": w.id.0,
                "status": format!("{:?}", w.status),
                "address": w.address,
                "seconds_since_heartbeat": w.last_heartbeat.elapsed().as_secs(),
            })
        })
        .collect();

    let all_ready = coordinator
        .workers
        .values()
        .all(|w| w.status == crate::worker::WorkerStatus::Ready);
    let overall = if coordinator.workers.is_empty() {
        "down"
    } else if all_ready {
        "up"
    } else {
        "degraded"
    };

    let pipeline_count: usize = coordinator
        .pipeline_groups
        .values()
        .map(|g| g.placements.len())
        .sum();

    reply_json_status(
        &serde_json::json!({
            "status": overall,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "workers": worker_health,
            "worker_count": coordinator.workers.len(),
            "pipeline_count": pipeline_count,
        }),
        StatusCode::OK,
    )
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    use super::*;
    use crate::worker::WorkerCapacity;

    fn setup_routes() -> (SharedCoordinator, Router) {
        let coord = shared_coordinator();

        let routes = cluster_routes(
            coord.clone(),
            Arc::new(RbacConfig::single_key("admin-key".to_string())),
            None,
            None,
        );
        (coord, routes)
    }

    /// Helper: send a request to the router and return the response.
    async fn send_request(router: &Router, req: Request<Body>) -> axum::response::Response {
        router.clone().oneshot(req).await.unwrap()
    }

    /// Helper: read response body bytes.
    async fn body_bytes(resp: axum::response::Response) -> Vec<u8> {
        axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec()
    }

    /// Helper: build a GET request with an API key header.
    fn get_req(path: &str, api_key: &str) -> Request<Body> {
        Request::builder()
            .method("GET")
            .uri(path)
            .header("x-api-key", api_key)
            .body(Body::empty())
            .unwrap()
    }

    /// Helper: build a GET request without an API key header.
    fn get_req_no_key(path: &str) -> Request<Body> {
        Request::builder()
            .method("GET")
            .uri(path)
            .body(Body::empty())
            .unwrap()
    }

    /// Helper: build a POST request with a JSON body and API key.
    fn post_json_req(path: &str, api_key: &str, body: &impl serde::Serialize) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(path)
            .header("x-api-key", api_key)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(body).unwrap()))
            .unwrap()
    }

    /// Helper: build a POST request with a JSON body and no API key.
    fn post_json_req_no_key(path: &str, body: &impl serde::Serialize) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(body).unwrap()))
            .unwrap()
    }

    /// Helper: build a DELETE request with an API key.
    fn delete_req(path: &str, api_key: &str) -> Request<Body> {
        Request::builder()
            .method("DELETE")
            .uri(path)
            .header("x-api-key", api_key)
            .body(Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn test_register_worker() {
        let (_coord, router) = setup_routes();

        let req = post_json_req(
            "/api/v1/cluster/workers/register",
            "admin-key",
            &RegisterWorkerRequest {
                worker_id: "w1".into(),
                address: "http://localhost:9000".into(),
                api_key: "worker-key".into(),
                capacity: WorkerCapacity::default(),
            },
        );
        let resp = send_request(&router, req).await;

        assert_eq!(resp.status(), StatusCode::CREATED);
        let bytes = body_bytes(resp).await;
        let body: RegisterWorkerResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body.worker_id, "w1");
    }

    #[tokio::test]
    async fn test_list_workers() {
        let (coord, router) = setup_routes();

        // Register a worker directly
        {
            let mut c = coord.write().await;
            c.register_worker(WorkerNode::new(
                WorkerId("w1".into()),
                "http://localhost:9000".into(),
                "key".into(),
            ));
        }

        let resp = send_request(&router, get_req("/api/v1/cluster/workers", "admin-key")).await;

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = body_bytes(resp).await;
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["total"], 1);
    }

    #[tokio::test]
    async fn test_heartbeat() {
        let (coord, router) = setup_routes();

        // Register worker
        {
            let mut c = coord.write().await;
            c.register_worker(WorkerNode::new(
                WorkerId("w1".into()),
                "http://localhost:9000".into(),
                "key".into(),
            ));
        }

        let req = post_json_req(
            "/api/v1/cluster/workers/w1/heartbeat",
            "admin-key",
            &HeartbeatRequest {
                events_processed: 42,
                pipelines_running: 1,
                pipeline_metrics: vec![],
            },
        );
        let resp = send_request(&router, req).await;

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = body_bytes(resp).await;
        let body: HeartbeatResponse = serde_json::from_slice(&bytes).unwrap();
        assert!(body.acknowledged);
    }

    #[tokio::test]
    async fn test_unauthorized_without_key() {
        let (_coord, router) = setup_routes();

        let resp = send_request(&router, get_req_no_key("/api/v1/cluster/workers")).await;

        // Should reject
        assert_ne!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_raft_status_requires_auth() {
        // /api/v1/cluster/raft leaks cluster topology (node ids, current leader,
        // peer addresses). Every sibling read route requires RbacViewer; this one
        // was missing the extractor, so it answered unauthenticated callers.
        let (_coord, router) = setup_routes();

        // No key → 401 (before the fix this returned 200 with the topology).
        let resp = send_request(&router, get_req_no_key("/api/v1/cluster/raft")).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        // Valid key → auth passes (not a 401).
        let resp = send_request(&router, get_req("/api/v1/cluster/raft", "admin-key")).await;
        assert_ne!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_topology() {
        let (_coord, router) = setup_routes();

        let resp = send_request(&router, get_req("/api/v1/cluster/topology", "admin-key")).await;

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = body_bytes(resp).await;
        let body: TopologyInfo = serde_json::from_slice(&bytes).unwrap();
        assert!(body.groups.is_empty());
    }

    #[tokio::test]
    async fn test_delete_worker() {
        let (coord, router) = setup_routes();

        {
            let mut c = coord.write().await;
            c.register_worker(WorkerNode::new(
                WorkerId("w1".into()),
                "http://localhost:9000".into(),
                "key".into(),
            ));
        }

        let resp = send_request(
            &router,
            delete_req("/api/v1/cluster/workers/w1", "admin-key"),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::OK);

        // Verify worker is gone
        let coord = coord.read().await;
        assert!(coord.workers.is_empty());
    }

    #[tokio::test]
    async fn test_list_pipeline_groups_empty() {
        let (_coord, router) = setup_routes();

        let resp = send_request(
            &router,
            get_req("/api/v1/cluster/pipeline-groups", "admin-key"),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = body_bytes(resp).await;
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["total"], 0);
    }

    #[tokio::test]
    async fn test_get_worker_found() {
        let (coord, router) = setup_routes();

        {
            let mut c = coord.write().await;
            c.register_worker(WorkerNode::new(
                WorkerId("w1".into()),
                "http://localhost:9000".into(),
                "key".into(),
            ));
        }

        let resp = send_request(&router, get_req("/api/v1/cluster/workers/w1", "admin-key")).await;

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = body_bytes(resp).await;
        let body: WorkerInfo = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body.id, "w1");
        assert_eq!(body.address, "http://localhost:9000");
        assert_eq!(body.status, "ready");
    }

    #[tokio::test]
    async fn test_get_worker_not_found() {
        let (_coord, router) = setup_routes();

        let resp = send_request(
            &router,
            get_req("/api/v1/cluster/workers/nonexistent", "admin-key"),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_worker_not_found() {
        let (_coord, router) = setup_routes();

        let resp = send_request(
            &router,
            delete_req("/api/v1/cluster/workers/nonexistent", "admin-key"),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_heartbeat_unknown_worker() {
        let (_coord, router) = setup_routes();

        let req = post_json_req(
            "/api/v1/cluster/workers/nonexistent/heartbeat",
            "admin-key",
            &HeartbeatRequest {
                events_processed: 0,
                pipelines_running: 0,
                pipeline_metrics: vec![],
            },
        );
        let resp = send_request(&router, req).await;

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_unauthorized_wrong_key() {
        let (_coord, router) = setup_routes();

        let resp = send_request(&router, get_req("/api/v1/cluster/workers", "wrong-key")).await;

        assert_ne!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_no_auth_mode() {
        // When RBAC is disabled, no auth required
        let coord = shared_coordinator();

        let router = cluster_routes(coord.clone(), Arc::new(RbacConfig::disabled()), None, None);

        // Register without API key
        let req = post_json_req_no_key(
            "/api/v1/cluster/workers/register",
            &RegisterWorkerRequest {
                worker_id: "w1".into(),
                address: "http://localhost:9000".into(),
                api_key: "key".into(),
                capacity: WorkerCapacity::default(),
            },
        );
        let resp = send_request(&router, req).await;
        assert_eq!(resp.status(), StatusCode::CREATED);

        // List without API key
        let resp = send_request(&router, get_req_no_key("/api/v1/cluster/workers")).await;

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = body_bytes(resp).await;
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["total"], 1);
    }

    #[tokio::test]
    async fn test_get_group_not_found() {
        let (_coord, router) = setup_routes();

        let resp = send_request(
            &router,
            get_req("/api/v1/cluster/pipeline-groups/nonexistent", "admin-key"),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let bytes = body_bytes(resp).await;
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["code"], "404");
    }

    #[tokio::test]
    async fn test_delete_group_not_found() {
        let (_coord, router) = setup_routes();

        let resp = send_request(
            &router,
            delete_req("/api/v1/cluster/pipeline-groups/nonexistent", "admin-key"),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let bytes = body_bytes(resp).await;
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["code"], "group_not_found");
    }

    #[tokio::test]
    async fn test_inject_event_group_not_found() {
        let (_coord, router) = setup_routes();

        let req = post_json_req(
            "/api/v1/cluster/pipeline-groups/nonexistent/inject",
            "admin-key",
            &serde_json::json!({
                "event_type": "TestEvent",
                "fields": {}
            }),
        );
        let resp = send_request(&router, req).await;

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let bytes = body_bytes(resp).await;
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["code"], "group_not_found");
    }

    #[tokio::test]
    async fn test_deploy_group_no_workers() {
        let (_coord, router) = setup_routes();

        let req = post_json_req(
            "/api/v1/cluster/pipeline-groups",
            "admin-key",
            &serde_json::json!({
                "name": "test-group",
                "pipelines": [
                    {"name": "p1", "source": "stream A = X"}
                ]
            }),
        );
        let resp = send_request(&router, req).await;

        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let bytes = body_bytes(resp).await;
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["code"], "no_workers_available");
    }

    #[tokio::test]
    async fn test_register_multiple_workers_list() {
        let (coord, router) = setup_routes();

        // Register 3 workers directly
        {
            let mut c = coord.write().await;
            for i in 0..3 {
                c.register_worker(WorkerNode::new(
                    WorkerId(format!("w{}", i)),
                    format!("http://localhost:900{}", i),
                    "key".into(),
                ));
            }
        }

        let resp = send_request(&router, get_req("/api/v1/cluster/workers", "admin-key")).await;

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = body_bytes(resp).await;
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["total"], 3);
        assert_eq!(body["workers"].as_array().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn test_topology_with_groups() {
        let (coord, router) = setup_routes();

        // Manually insert a pipeline group
        {
            let mut c = coord.write().await;
            use crate::pipeline_group::*;
            let spec = PipelineGroupSpec {
                name: "test-group".into(),
                pipelines: vec![PipelinePlacement {
                    name: "p1".into(),
                    source: "stream A = X".into(),
                    worker_affinity: None,
                    replicas: 1,
                    partition_key: None,
                }],
                routes: vec![InterPipelineRoute {
                    from_pipeline: "_external".into(),
                    to_pipeline: "p1".into(),
                    event_types: vec!["*".into()],
                    nats_subject: None,
                }],
                region_affinity: None,
                cross_region_routes: vec![],
            };

            let mut group = DeployedPipelineGroup::new("g1".into(), "test-group".into(), spec);
            group.placements.insert(
                "p1".into(),
                PipelineDeployment {
                    worker_id: WorkerId("w1".into()),
                    worker_address: "http://localhost:9000".into(),
                    worker_api_key: "key".into(),
                    pipeline_id: "pid1".into(),
                    status: PipelineDeploymentStatus::Running,
                    epoch: 0,
                },
            );
            c.pipeline_groups.insert("g1".into(), group);
        }

        let resp = send_request(&router, get_req("/api/v1/cluster/topology", "admin-key")).await;

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = body_bytes(resp).await;
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let groups = body["groups"].as_array().unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0]["group_name"], "test-group");
        assert_eq!(groups[0]["pipelines"].as_array().unwrap().len(), 1);
        assert_eq!(groups[0]["routes"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_get_group_found() {
        let (coord, router) = setup_routes();

        // Insert a pipeline group manually
        {
            let mut c = coord.write().await;
            use crate::pipeline_group::*;
            let spec = PipelineGroupSpec {
                name: "my-group".into(),
                pipelines: vec![PipelinePlacement {
                    name: "p1".into(),
                    source: "stream A = X".into(),
                    worker_affinity: None,
                    replicas: 1,
                    partition_key: None,
                }],
                routes: vec![],
                region_affinity: None,
                cross_region_routes: vec![],
            };

            let group = DeployedPipelineGroup::new("g1".into(), "my-group".into(), spec);
            c.pipeline_groups.insert("g1".into(), group);
        }

        let resp = send_request(
            &router,
            get_req("/api/v1/cluster/pipeline-groups/g1", "admin-key"),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = body_bytes(resp).await;
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["name"], "my-group");
        assert_eq!(body["id"], "g1");
    }

    #[tokio::test]
    async fn test_list_groups_with_entries() {
        let (coord, router) = setup_routes();

        {
            let mut c = coord.write().await;
            use crate::pipeline_group::*;
            for i in 0..3 {
                let spec = PipelineGroupSpec {
                    name: format!("group-{}", i),
                    pipelines: vec![],
                    routes: vec![],
                    region_affinity: None,
                    cross_region_routes: vec![],
                };
                let group =
                    DeployedPipelineGroup::new(format!("g{}", i), format!("group-{}", i), spec);
                c.pipeline_groups.insert(format!("g{}", i), group);
            }
        }

        let resp = send_request(
            &router,
            get_req("/api/v1/cluster/pipeline-groups", "admin-key"),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = body_bytes(resp).await;
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["total"], 3);
    }

    #[tokio::test]
    async fn test_register_worker_via_api_then_get() {
        let (_coord, router) = setup_routes();

        // Register
        let req = post_json_req(
            "/api/v1/cluster/workers/register",
            "admin-key",
            &RegisterWorkerRequest {
                worker_id: "api-worker".into(),
                address: "http://localhost:8000".into(),
                api_key: "worker-secret".into(),
                capacity: WorkerCapacity {
                    cpu_cores: 4,
                    pipelines_running: 0,
                    max_pipelines: 50,
                },
            },
        );
        let resp = send_request(&router, req).await;
        assert_eq!(resp.status(), StatusCode::CREATED);

        // Get the worker
        let resp = send_request(
            &router,
            get_req("/api/v1/cluster/workers/api-worker", "admin-key"),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = body_bytes(resp).await;
        let body: WorkerInfo = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body.id, "api-worker");
        assert_eq!(body.address, "http://localhost:8000");
        assert_eq!(body.max_pipelines, 50);
    }

    #[tokio::test]
    async fn test_register_delete_register_cycle() {
        let (_coord, router) = setup_routes();

        // Register
        let req = post_json_req(
            "/api/v1/cluster/workers/register",
            "admin-key",
            &RegisterWorkerRequest {
                worker_id: "w1".into(),
                address: "http://localhost:9000".into(),
                api_key: "key".into(),
                capacity: WorkerCapacity::default(),
            },
        );
        let resp = send_request(&router, req).await;
        assert_eq!(resp.status(), StatusCode::CREATED);

        // Delete
        let resp = send_request(
            &router,
            delete_req("/api/v1/cluster/workers/w1", "admin-key"),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        // Verify gone
        let resp = send_request(&router, get_req("/api/v1/cluster/workers/w1", "admin-key")).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        // Re-register
        let req = post_json_req(
            "/api/v1/cluster/workers/register",
            "admin-key",
            &RegisterWorkerRequest {
                worker_id: "w1".into(),
                address: "http://localhost:9001".into(),
                api_key: "new-key".into(),
                capacity: WorkerCapacity::default(),
            },
        );
        let resp = send_request(&router, req).await;
        assert_eq!(resp.status(), StatusCode::CREATED);

        // Verify re-registered with new address
        let resp = send_request(&router, get_req("/api/v1/cluster/workers/w1", "admin-key")).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = body_bytes(resp).await;
        let body: WorkerInfo = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body.address, "http://localhost:9001");
    }

    #[tokio::test]
    async fn test_heartbeat_then_get_worker_updates() {
        let (coord, router) = setup_routes();

        {
            let mut c = coord.write().await;
            c.register_worker(WorkerNode::new(
                WorkerId("w1".into()),
                "http://localhost:9000".into(),
                "key".into(),
            ));
        }

        // Send heartbeat with updated pipeline count
        let req = post_json_req(
            "/api/v1/cluster/workers/w1/heartbeat",
            "admin-key",
            &HeartbeatRequest {
                events_processed: 500,
                pipelines_running: 3,
                pipeline_metrics: vec![],
            },
        );
        let resp = send_request(&router, req).await;
        assert_eq!(resp.status(), StatusCode::OK);

        // Get worker should reflect new pipeline count
        let resp = send_request(&router, get_req("/api/v1/cluster/workers/w1", "admin-key")).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = body_bytes(resp).await;
        let body: WorkerInfo = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body.pipelines_running, 3);
    }

    fn rbac_viewer() -> Arc<RbacConfig> {
        use std::collections::HashMap;
        let mut keys = HashMap::new();
        keys.insert(
            "viewer-key".to_string(),
            crate::rbac::ApiKeyEntry {
                role: Role::Viewer,
                name: Some("grafana".to_string()),
            },
        );
        Arc::new(RbacConfig::multi_key(keys))
    }

    #[tokio::test]
    async fn test_rbac_viewer_can_read() {
        let coord = shared_coordinator();

        let router = cluster_routes(coord, rbac_viewer(), None, None);

        // Viewer can list workers (GET)
        let resp = send_request(&router, get_req("/api/v1/cluster/workers", "viewer-key")).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_rbac_viewer_cannot_deploy() {
        let coord = shared_coordinator();

        let router = cluster_routes(coord, rbac_viewer(), None, None);

        // Viewer cannot deploy (POST = Operator required)
        let req = post_json_req(
            "/api/v1/cluster/pipeline-groups",
            "viewer-key",
            &serde_json::json!({"name": "g1", "pipelines": []}),
        );
        let resp = send_request(&router, req).await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn test_rbac_operator_cannot_delete() {
        use std::collections::HashMap;
        let mut keys = HashMap::new();
        keys.insert(
            "op-key".to_string(),
            crate::rbac::ApiKeyEntry {
                role: Role::Operator,
                name: Some("ci".to_string()),
            },
        );
        let rbac = Arc::new(RbacConfig::multi_key(keys));
        let coord = shared_coordinator();

        let router = cluster_routes(coord, rbac, None, None);

        // Operator cannot delete workers (DELETE = Admin required)
        let resp = send_request(&router, delete_req("/api/v1/cluster/workers/w1", "op-key")).await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn test_rbac_multi_key_hierarchy() {
        use std::collections::HashMap;
        let mut keys = HashMap::new();
        keys.insert(
            "admin-key".to_string(),
            crate::rbac::ApiKeyEntry {
                role: Role::Admin,
                name: Some("admin".to_string()),
            },
        );
        keys.insert(
            "viewer-key".to_string(),
            crate::rbac::ApiKeyEntry {
                role: Role::Viewer,
                name: Some("grafana".to_string()),
            },
        );
        let rbac = Arc::new(RbacConfig::multi_key(keys));
        let coord = shared_coordinator();

        let router = cluster_routes(coord, rbac, None, None);

        // Admin can delete (Admin required)
        let resp = send_request(
            &router,
            delete_req("/api/v1/cluster/workers/w1", "admin-key"),
        )
        .await;
        // 404 because w1 doesn't exist, but NOT 403 -- auth passed
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        // Viewer gets 403 on delete
        let resp = send_request(
            &router,
            delete_req("/api/v1/cluster/workers/w1", "viewer-key"),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        // No key gets 401
        let resp = send_request(&router, get_req_no_key("/api/v1/cluster/workers")).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // =========================================================================
    // Rate limiting tests
    // =========================================================================

    #[tokio::test]
    async fn test_rate_limit_returns_429() {
        use crate::rate_limit::{RateLimitConfig, RateLimiter};

        // Allow burst of 2 requests, then rate limit
        let limiter = Arc::new(RateLimiter::new(RateLimitConfig::with_burst(10, 2)));

        let coord = shared_coordinator();
        let router = cluster_routes(
            coord,
            Arc::new(RbacConfig::single_key("admin-key".to_string())),
            Some(limiter),
            None,
        );

        // First 2 requests should succeed (burst)
        for _ in 0..2 {
            let req = post_json_req(
                "/api/v1/cluster/workers/register",
                "admin-key",
                &serde_json::json!({
                    "worker_id": "w1",
                    "address": "http://localhost:9000",
                    "api_key": "key",
                    "capacity": {
                        "cpu_cores": 4,
                        "memory_mb": 8192,
                        "max_pipelines": 10,
                        "pipelines_running": 0
                    }
                }),
            );
            let resp = send_request(&router, req).await;
            assert!(
                resp.status().is_success() || resp.status() == StatusCode::CREATED,
                "Expected success, got {}",
                resp.status()
            );
        }

        // 3rd request should get 429 Too Many Requests
        let req = post_json_req(
            "/api/v1/cluster/workers/register",
            "admin-key",
            &serde_json::json!({
                "worker_id": "w2",
                "address": "http://localhost:9001",
                "api_key": "key",
                "capacity": {
                    "cpu_cores": 4,
                    "memory_mb": 8192,
                    "max_pipelines": 10,
                    "pipelines_running": 0
                }
            }),
        );
        let resp = send_request(&router, req).await;
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);

        // Verify JSON body
        let bytes = body_bytes(resp).await;
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"], "rate_limited");
        assert_eq!(body["message"], "Too many requests");
        assert!(body["retry_after_seconds"].is_number());
    }

    #[tokio::test]
    async fn test_rate_limit_does_not_affect_get_routes() {
        use crate::rate_limit::{RateLimitConfig, RateLimiter};

        // Very restrictive: 1 request burst
        let limiter = Arc::new(RateLimiter::new(RateLimitConfig::with_burst(1, 1)));

        let coord = shared_coordinator();
        let router = cluster_routes(
            coord,
            Arc::new(RbacConfig::single_key("admin-key".to_string())),
            Some(limiter),
            None,
        );

        // Exhaust the rate limit with a POST
        let req = post_json_req(
            "/api/v1/cluster/workers/register",
            "admin-key",
            &serde_json::json!({
                "worker_id": "w1",
                "address": "http://localhost:9000",
                "api_key": "key",
                "capacity": {
                    "cpu_cores": 4,
                    "memory_mb": 8192,
                    "max_pipelines": 10,
                    "pipelines_running": 0
                }
            }),
        );
        let resp = send_request(&router, req).await;
        assert!(resp.status().is_success() || resp.status() == StatusCode::CREATED);

        // Verify POST is now rate limited
        let req = post_json_req(
            "/api/v1/cluster/workers/register",
            "admin-key",
            &serde_json::json!({
                "worker_id": "w2",
                "address": "http://localhost:9001",
                "api_key": "key",
                "capacity": {
                    "cpu_cores": 4,
                    "memory_mb": 8192,
                    "max_pipelines": 10,
                    "pipelines_running": 0
                }
            }),
        );
        let resp = send_request(&router, req).await;
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);

        // GET requests should still work (they are exempt from rate limiting)
        let resp = send_request(&router, get_req("/api/v1/cluster/workers", "admin-key")).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_rate_limit_disabled_allows_all() {
        use crate::rate_limit::{RateLimitConfig, RateLimiter};

        // Disabled rate limiter
        let limiter = Arc::new(RateLimiter::new(RateLimitConfig::disabled()));

        let coord = shared_coordinator();
        let router = cluster_routes(
            coord,
            Arc::new(RbacConfig::single_key("admin-key".to_string())),
            Some(limiter),
            None,
        );

        // Many POST requests should all succeed
        for i in 0..10 {
            let req = post_json_req(
                "/api/v1/cluster/workers/register",
                "admin-key",
                &serde_json::json!({
                    "worker_id": format!("w{}", i),
                    "address": format!("http://localhost:{}", 9000 + i),
                    "api_key": "key",
                    "capacity": {
                        "cpu_cores": 4,
                        "memory_mb": 8192,
                        "max_pipelines": 10,
                        "pipelines_running": 0
                    }
                }),
            );
            let resp = send_request(&router, req).await;
            assert!(
                resp.status().is_success() || resp.status() == StatusCode::CREATED,
                "Request {} failed with {}",
                i,
                resp.status()
            );
        }
    }

    #[tokio::test]
    async fn test_rate_limit_none_allows_all() {
        let coord = shared_coordinator();
        let router = cluster_routes(
            coord,
            Arc::new(RbacConfig::single_key("admin-key".to_string())),
            None, // No rate limiter
            None, // Default CORS
        );

        // Many POST requests should all succeed
        for i in 0..10 {
            let req = post_json_req(
                "/api/v1/cluster/workers/register",
                "admin-key",
                &serde_json::json!({
                    "worker_id": format!("w{}", i),
                    "address": format!("http://localhost:{}", 9000 + i),
                    "api_key": "key",
                    "capacity": {
                        "cpu_cores": 4,
                        "memory_mb": 8192,
                        "max_pipelines": 10,
                        "pipelines_running": 0
                    }
                }),
            );
            let resp = send_request(&router, req).await;
            assert!(
                resp.status().is_success() || resp.status() == StatusCode::CREATED,
                "Request {} failed with {}",
                i,
                resp.status()
            );
        }
    }
}
