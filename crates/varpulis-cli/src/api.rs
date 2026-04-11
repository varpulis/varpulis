//! REST API for SaaS pipeline management
//!
//! Provides RESTful endpoints for deploying and managing CEP pipelines
//! in a multi-tenant environment.

use std::convert::Infallible;

use axum::extract::{Json, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use futures_util::stream;
use indexmap::IndexMap;
use rustc_hash::FxBuildHasher;
use serde::{Deserialize, Serialize};
use tower_http::cors::{AllowHeaders, AllowMethods, AllowOrigin, CorsLayer};
use varpulis_core::pagination::{PaginationMeta, PaginationParams, MAX_LIMIT};
use varpulis_runtime::tenant::{SharedTenantManager, TenantError, TenantQuota};
use varpulis_runtime::Event;

use crate::auth::constant_time_compare;
use crate::billing::SharedBillingState;

// =============================================================================
// Request/Response types
// =============================================================================

#[derive(Debug, Deserialize, Serialize)]
pub struct DeployPipelineRequest {
    pub name: String,
    pub source: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeployPipelineResponse {
    pub id: String,
    pub name: String,
    pub status: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PipelineInfo {
    pub id: String,
    pub name: String,
    pub status: String,
    pub source: String,
    pub uptime_secs: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub global_template_id: Option<String>,
    /// Pipeline scope: "global", "tenant", or "own".
    #[serde(default = "default_scope")]
    pub scope_level: String,
    /// Source org for inherited pipelines (None = belongs to current org).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inherited_from_org_id: Option<String>,
    /// Whether the pipeline is read-only (inherited from parent/global).
    #[serde(default)]
    pub read_only: bool,
}

fn default_scope() -> String {
    "own".to_string()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PipelineListResponse {
    pub pipelines: Vec<PipelineInfo>,
    pub total: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<PaginationMeta>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PipelineMetricsResponse {
    pub pipeline_id: String,
    pub events_processed: u64,
    pub output_events_emitted: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct InjectEventRequest {
    pub event_type: String,
    pub fields: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct InjectBatchRequest {
    pub events: Vec<InjectEventRequest>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InjectBatchResponse {
    pub accepted: usize,
    pub output_events: Vec<serde_json::Value>,
    pub processing_time_us: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ReloadPipelineRequest {
    pub source: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CheckpointResponse {
    pub pipeline_id: String,
    pub checkpoint: varpulis_runtime::persistence::EngineCheckpoint,
    pub events_processed: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RestoreRequest {
    pub checkpoint: varpulis_runtime::persistence::EngineCheckpoint,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RestoreResponse {
    pub pipeline_id: String,
    pub restored: bool,
    pub events_restored: u64,
}

#[derive(Debug, Serialize)]
pub struct ApiError {
    pub error: String,
    pub code: String,
}

#[derive(Debug, Deserialize)]
pub struct DlqQueryParams {
    #[serde(default)]
    pub offset: Option<usize>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct DlqEntriesResponse {
    pub entries: Vec<varpulis_runtime::dead_letter::DlqEntryOwned>,
    pub total: u64,
}

#[derive(Debug, Serialize)]
pub struct DlqReplayResponse {
    pub replayed: usize,
}

#[derive(Debug, Serialize)]
pub struct DlqClearResponse {
    pub cleared: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UsageResponse {
    pub tenant_id: String,
    pub events_processed: u64,
    pub output_events_emitted: u64,
    pub active_pipelines: usize,
    pub quota: QuotaInfo,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QuotaInfo {
    pub max_pipelines: usize,
    pub max_events_per_second: u64,
    pub max_streams_per_pipeline: usize,
}

// =============================================================================
// Pipeline Graph (Visual Builder) Request/Response types
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct PipelineGraphRequest {
    pub vpl: String,
}

#[derive(Debug, Serialize)]
pub struct GenerateResponse {
    pub vpl: String,
}

// =============================================================================
// Tenant Admin Request/Response types
// =============================================================================

#[derive(Debug, Deserialize, Serialize)]
pub struct CreateTenantRequest {
    pub name: String,
    #[serde(default)]
    pub quota_tier: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TenantResponse {
    pub id: String,
    pub name: String,
    pub api_key: String,
    pub quota: QuotaInfo,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TenantListResponse {
    pub tenants: Vec<TenantResponse>,
    pub total: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<PaginationMeta>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TenantDetailResponse {
    pub id: String,
    pub name: String,
    pub api_key: String,
    pub quota: QuotaInfo,
    pub usage: TenantUsageInfo,
    pub pipeline_count: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TenantUsageInfo {
    pub events_processed: u64,
    pub output_events_emitted: u64,
    pub active_pipelines: usize,
}

// =============================================================================
// API Routes
// =============================================================================

/// Build a tower-http CORS layer from an optional list of allowed origins.
///
/// - Explicit list of origins: restrict to those origins.
/// - A list containing `"*"`: allow any origin (must be explicitly opted into).
/// - `None` (default): allow only localhost origins for safety.
fn build_cors(origins: Option<Vec<String>>) -> CorsLayer {
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
        _ => AllowOrigin::list([
            "http://localhost:5173".parse().unwrap(),
            "http://localhost:8080".parse().unwrap(),
            "http://127.0.0.1:5173".parse().unwrap(),
            "http://127.0.0.1:8080".parse().unwrap(),
        ]),
    };

    CorsLayer::new()
        .allow_methods(methods)
        .allow_headers(headers)
        .allow_origin(origin)
}

/// Shared state for the API router.
#[derive(Debug, Clone)]
pub struct ApiState {
    pub manager: SharedTenantManager,
    pub admin_key: Option<String>,
    pub billing_state: Option<SharedBillingState>,
    #[cfg(feature = "saas")]
    pub db_pool: Option<varpulis_db::PgPool>,
}

/// Axum extractor for X-API-Key header.
#[derive(Debug)]
pub struct ApiKey(pub String);

impl<S> axum::extract::FromRequestParts<S> for ApiKey
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        parts
            .headers
            .get("x-api-key")
            .and_then(|v| v.to_str().ok())
            .map(|s| Self(s.to_string()))
            .ok_or_else(|| {
                (
                    StatusCode::UNAUTHORIZED,
                    axum::Json(serde_json::json!({"error": "Missing X-API-Key header"})),
                )
                    .into_response()
            })
    }
}

/// Axum extractor for X-Admin-Key header.
#[derive(Debug)]
pub struct AdminKey(pub String);

impl<S> axum::extract::FromRequestParts<S> for AdminKey
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        parts
            .headers
            .get("x-admin-key")
            .and_then(|v| v.to_str().ok())
            .map(|s| Self(s.to_string()))
            .ok_or_else(|| {
                (
                    StatusCode::UNAUTHORIZED,
                    axum::Json(serde_json::json!({"error": "Missing X-Admin-Key header"})),
                )
                    .into_response()
            })
    }
}

/// Build the complete API route tree
pub fn api_routes(
    manager: SharedTenantManager,
    admin_key: Option<String>,
    cors_origins: Option<Vec<String>>,
    billing_state: Option<SharedBillingState>,
    #[cfg(feature = "saas")] db_pool: Option<varpulis_db::PgPool>,
) -> Router {
    let state = ApiState {
        manager,
        admin_key,
        billing_state,
        #[cfg(feature = "saas")]
        db_pool,
    };

    let cors = build_cors(cors_origins);

    Router::new()
        // Pipeline CRUD
        .route("/api/v1/pipelines", post(handle_deploy).get(handle_list))
        .route(
            "/api/v1/pipelines/{pipeline_id}",
            get(handle_get).delete(handle_delete),
        )
        // Pipeline actions
        .route(
            "/api/v1/pipelines/{pipeline_id}/events",
            post(handle_inject),
        )
        .route(
            "/api/v1/pipelines/{pipeline_id}/events-batch",
            post(handle_inject_batch),
        )
        .route(
            "/api/v1/pipelines/{pipeline_id}/checkpoint",
            post(handle_checkpoint),
        )
        .route(
            "/api/v1/pipelines/{pipeline_id}/restore",
            post(handle_restore),
        )
        .route(
            "/api/v1/pipelines/{pipeline_id}/metrics",
            get(handle_metrics),
        )
        .route(
            "/api/v1/pipelines/{pipeline_id}/topology",
            get(handle_topology),
        )
        .route(
            "/api/v1/pipelines/{pipeline_id}/reload",
            post(handle_reload),
        )
        .route("/api/v1/usage", get(handle_usage))
        .route("/api/v1/pipelines/{pipeline_id}/logs", get(handle_logs))
        // DLQ routes
        .route(
            "/api/v1/pipelines/{pipeline_id}/dlq",
            get(handle_dlq_get).delete(handle_dlq_clear),
        )
        .route(
            "/api/v1/pipelines/{pipeline_id}/dlq/replay",
            post(handle_dlq_replay),
        )
        // Pipeline graph (visual builder)
        .route("/api/v1/pipeline/graph", post(handle_pipeline_to_graph))
        .route("/api/v1/pipeline/generate", post(handle_graph_to_pipeline))
        // Tenant admin routes
        .route(
            "/api/v1/tenants",
            post(handle_create_tenant).get(handle_list_tenants),
        )
        .route(
            "/api/v1/tenants/{tenant_id}",
            get(handle_get_tenant).delete(handle_delete_tenant),
        )
        .layer(cors)
        .with_state(state)
}

// =============================================================================
// Handlers
// =============================================================================

async fn handle_deploy(
    State(state): State<ApiState>,
    ApiKey(api_key): ApiKey,
    Json(body): Json<DeployPipelineRequest>,
) -> Response {
    let manager = &state.manager;
    let mut mgr = manager.write().await;

    let tenant_id = match mgr.get_tenant_by_api_key(&api_key) {
        Some(id) => id.clone(),
        None => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                "invalid_api_key",
                "Invalid API key",
            )
        }
    };

    let pipeline_name = body.name.clone();
    #[cfg(feature = "saas")]
    let vpl_source = body.source.clone();

    let result = mgr
        .deploy_pipeline_on_tenant(&tenant_id, body.name, body.source)
        .await;

    match result {
        Ok(id) => {
            mgr.persist_if_needed(&tenant_id);

            // Sync pipeline to DB for hierarchy-aware views
            #[cfg(feature = "saas")]
            if let Some(ref pool) = state.db_pool {
                if let Ok(org_uuid) = tenant_id.0.parse::<uuid::Uuid>() {
                    if let Err(e) = varpulis_db::repo::create_scoped_pipeline(
                        pool,
                        org_uuid,
                        &pipeline_name,
                        &vpl_source,
                        "own",
                    )
                    .await
                    {
                        tracing::warn!("Failed to sync pipeline to DB: {}", e);
                    }
                }
            }

            let resp = DeployPipelineResponse {
                id,
                name: pipeline_name,
                status: "running".to_string(),
            };
            (StatusCode::CREATED, axum::Json(&resp)).into_response()
        }
        Err(e) => tenant_error_response(e),
    }
}

async fn handle_list(
    State(state): State<ApiState>,
    ApiKey(api_key): ApiKey,
    Query(pagination): Query<PaginationParams>,
) -> Response {
    let manager = &state.manager;
    if pagination.exceeds_max() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid_limit",
            &format!("limit must not exceed {MAX_LIMIT}"),
        );
    }

    let mgr = manager.read().await;

    let tenant_id = match mgr.get_tenant_by_api_key(&api_key) {
        Some(id) => id.clone(),
        None => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                "invalid_api_key",
                "Invalid API key",
            )
        }
    };

    let tenant = match mgr.get_tenant(&tenant_id) {
        Some(t) => t,
        None => {
            return error_response(
                StatusCode::NOT_FOUND,
                "tenant_not_found",
                "Tenant not found",
            )
        }
    };

    let all_pipelines: Vec<PipelineInfo> = tenant
        .pipelines
        .values()
        .map(|p| {
            let is_global = p.global_template_id.is_some();
            PipelineInfo {
                id: p.id.clone(),
                name: p.name.clone(),
                status: p.status.to_string(),
                source: p.source.clone(),
                uptime_secs: p.created_at.elapsed().as_secs(),
                global_template_id: p.global_template_id.clone(),
                scope_level: if is_global {
                    "global".to_string()
                } else {
                    "own".to_string()
                },
                inherited_from_org_id: None,
                read_only: is_global,
            }
        })
        .collect();

    let (pipelines, meta) = pagination.paginate(all_pipelines);
    let total = meta.total;
    let resp = PipelineListResponse {
        pipelines,
        total,
        pagination: Some(meta),
    };
    axum::Json(&resp).into_response()
}

async fn handle_get(
    State(state): State<ApiState>,
    Path(pipeline_id): Path<String>,
    ApiKey(api_key): ApiKey,
) -> Response {
    let manager = &state.manager;
    let mgr = manager.read().await;

    let tenant_id = match mgr.get_tenant_by_api_key(&api_key) {
        Some(id) => id.clone(),
        None => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                "invalid_api_key",
                "Invalid API key",
            )
        }
    };

    let tenant = match mgr.get_tenant(&tenant_id) {
        Some(t) => t,
        None => {
            return error_response(
                StatusCode::NOT_FOUND,
                "tenant_not_found",
                "Tenant not found",
            )
        }
    };

    match tenant.pipelines.get(&pipeline_id) {
        Some(p) => {
            let is_global = p.global_template_id.is_some();
            let info = PipelineInfo {
                id: p.id.clone(),
                name: p.name.clone(),
                status: p.status.to_string(),
                source: p.source.clone(),
                uptime_secs: p.created_at.elapsed().as_secs(),
                global_template_id: p.global_template_id.clone(),
                scope_level: if is_global {
                    "global".to_string()
                } else {
                    "own".to_string()
                },
                inherited_from_org_id: None,
                read_only: is_global,
            };
            axum::Json(&info).into_response()
        }
        None => error_response(
            StatusCode::NOT_FOUND,
            "pipeline_not_found",
            "Pipeline not found",
        ),
    }
}

async fn handle_delete(
    State(state): State<ApiState>,
    Path(pipeline_id): Path<String>,
    ApiKey(api_key): ApiKey,
) -> Response {
    let manager = &state.manager;
    let mut mgr = manager.write().await;

    let tenant_id = match mgr.get_tenant_by_api_key(&api_key) {
        Some(id) => id.clone(),
        None => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                "invalid_api_key",
                "Invalid API key",
            )
        }
    };

    #[cfg(feature = "saas")]
    let mut pipeline_name_for_db = None;

    let result = {
        let tenant = match mgr.get_tenant_mut(&tenant_id) {
            Some(t) => t,
            None => {
                return error_response(
                    StatusCode::NOT_FOUND,
                    "tenant_not_found",
                    "Tenant not found",
                )
            }
        };

        // Protect global pipelines from tenant deletion
        if let Some(pipeline) = tenant.pipelines.get(&pipeline_id) {
            if pipeline.global_template_id.is_some() {
                return error_response(
                    StatusCode::FORBIDDEN,
                    "global_pipeline_protected",
                    "Global pipelines can only be managed by admin",
                );
            }
            #[cfg(feature = "saas")]
            {
                pipeline_name_for_db = Some(pipeline.name.clone());
            }
        }

        tenant.remove_pipeline(&pipeline_id)
    };

    match result {
        Ok(()) => {
            mgr.persist_if_needed(&tenant_id);

            // Sync deletion to DB
            #[cfg(feature = "saas")]
            if let Some(ref pool) = state.db_pool {
                if let (Ok(org_uuid), Some(name)) =
                    (tenant_id.0.parse::<uuid::Uuid>(), &pipeline_name_for_db)
                {
                    let _ = varpulis_db::repo::delete_pipeline_by_name(pool, org_uuid, name).await;
                }
            }

            axum::Json(serde_json::json!({"deleted": true})).into_response()
        }
        Err(e) => tenant_error_response(e),
    }
}

async fn handle_inject(
    State(state): State<ApiState>,
    Path(pipeline_id): Path<String>,
    ApiKey(api_key): ApiKey,
    Json(body): Json<InjectEventRequest>,
) -> Response {
    let manager = &state.manager;
    let billing_state = &state.billing_state;
    // Check usage limit (SaaS mode only)
    #[cfg(feature = "saas")]
    let mut usage_warning: Option<f64> = None;
    #[cfg(feature = "saas")]
    if let Some(ref bs) = billing_state {
        if let Some(org_id) = bs.org_id_for_api_key(&api_key).await {
            match bs.check_usage_limit(org_id, 1).await {
                crate::billing::UsageCheckResult::Exceeded(err) => {
                    return crate::billing::usage_limit_response(&err);
                }
                crate::billing::UsageCheckResult::ApproachingLimit { usage_percent } => {
                    usage_warning = Some(usage_percent);
                }
                crate::billing::UsageCheckResult::Ok => {}
            }
            // Record the event for usage tracking
            bs.usage.write().await.record_events(org_id, 1);
        }
    }
    #[cfg(not(feature = "saas"))]
    let _ = &billing_state;

    let mut mgr = manager.write().await;

    let tenant_id = match mgr.get_tenant_by_api_key(&api_key) {
        Some(id) => id.clone(),
        None => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                "invalid_api_key",
                "Invalid API key",
            )
        }
    };

    // Check backpressure before processing
    if let Err(e) = mgr.check_backpressure() {
        return tenant_error_response(e);
    }

    let mut event = Event::new(body.event_type.clone());
    for (key, value) in &body.fields {
        let v = json_to_runtime_value(value);
        event = event.with_field(key.as_str(), v);
    }

    match mgr
        .process_event_with_backpressure(&tenant_id, &pipeline_id, event)
        .await
    {
        Ok(output_events) => {
            let events_json: Vec<serde_json::Value> = output_events
                .iter()
                .map(|e| {
                    let mut fields = serde_json::Map::new();
                    for (k, v) in &e.data {
                        fields.insert(k.to_string(), crate::websocket::value_to_json(v));
                    }
                    serde_json::json!({
                        "event_type": e.event_type.to_string(),
                        "fields": serde_json::Value::Object(fields),
                    })
                })
                .collect();
            let response = serde_json::json!({
                "accepted": true,
                "output_events": events_json,
            });
            #[cfg(feature = "saas")]
            if let Some(pct) = usage_warning {
                return (
                    StatusCode::OK,
                    [("X-Usage-Warning", format!("approaching_limit ({pct:.0}%)"))],
                    axum::Json(response),
                )
                    .into_response();
            }
            axum::Json(response).into_response()
        }
        Err(e) => tenant_error_response(e),
    }
}

async fn handle_inject_batch(
    State(state): State<ApiState>,
    Path(pipeline_id): Path<String>,
    ApiKey(api_key): ApiKey,
    Json(body): Json<InjectBatchRequest>,
) -> Response {
    let manager = &state.manager;
    let billing_state = &state.billing_state;
    let event_count = body.events.len() as i64;

    // Check usage limit for the entire batch (SaaS mode only)
    #[cfg(feature = "saas")]
    if let Some(ref bs) = billing_state {
        if let Some(org_id) = bs.org_id_for_api_key(&api_key).await {
            match bs.check_usage_limit(org_id, event_count).await {
                crate::billing::UsageCheckResult::Exceeded(err) => {
                    return crate::billing::usage_limit_response(&err);
                }
                crate::billing::UsageCheckResult::ApproachingLimit { .. }
                | crate::billing::UsageCheckResult::Ok => {}
            }
            // Record the batch for usage tracking
            bs.usage.write().await.record_events(org_id, event_count);
        }
    }
    #[cfg(not(feature = "saas"))]
    let _ = (&billing_state, event_count);

    let mut mgr = manager.write().await;

    let tenant_id = match mgr.get_tenant_by_api_key(&api_key) {
        Some(id) => id.clone(),
        None => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                "invalid_api_key",
                "Invalid API key",
            )
        }
    };

    // Check backpressure before processing the batch
    if let Err(e) = mgr.check_backpressure() {
        return tenant_error_response(e);
    }

    let start = std::time::Instant::now();
    let mut accepted = 0usize;
    let mut output_events = Vec::new();

    for req in body.events {
        let mut event = Event::new(req.event_type.clone());
        for (key, value) in &req.fields {
            let v = json_to_runtime_value(value);
            event = event.with_field(key.as_str(), v);
        }

        match mgr
            .process_event_with_backpressure(&tenant_id, &pipeline_id, event)
            .await
        {
            Ok(outputs) => {
                accepted += 1;
                for e in &outputs {
                    let mut flat = serde_json::Map::new();
                    flat.insert(
                        "event_type".to_string(),
                        serde_json::Value::String(e.event_type.to_string()),
                    );
                    for (k, v) in &e.data {
                        flat.insert(k.to_string(), crate::websocket::value_to_json(v));
                    }
                    output_events.push(serde_json::Value::Object(flat));
                }
            }
            Err(TenantError::BackpressureExceeded { .. }) => {
                // Stop processing the rest of the batch on backpressure
                break;
            }
            Err(_) => {
                // Skip other failed events silently in batch mode
            }
        }
    }

    let processing_time_us = start.elapsed().as_micros() as u64;

    let resp = InjectBatchResponse {
        accepted,
        output_events,
        processing_time_us,
    };
    axum::Json(&resp).into_response()
}

async fn handle_checkpoint(
    State(state): State<ApiState>,
    Path(pipeline_id): Path<String>,
    ApiKey(api_key): ApiKey,
) -> Response {
    let manager = &state.manager;
    let mgr = manager.read().await;

    let tenant_id = match mgr.get_tenant_by_api_key(&api_key) {
        Some(id) => id.clone(),
        None => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                "invalid_api_key",
                "Invalid API key",
            )
        }
    };

    let tenant = match mgr.get_tenant(&tenant_id) {
        Some(t) => t,
        None => {
            return error_response(
                StatusCode::NOT_FOUND,
                "tenant_not_found",
                "Tenant not found",
            )
        }
    };

    match tenant.checkpoint_pipeline(&pipeline_id).await {
        Ok(checkpoint) => {
            let resp = CheckpointResponse {
                pipeline_id,
                events_processed: checkpoint.events_processed,
                checkpoint,
            };
            axum::Json(&resp).into_response()
        }
        Err(e) => tenant_error_response(e),
    }
}

async fn handle_restore(
    State(state): State<ApiState>,
    Path(pipeline_id): Path<String>,
    ApiKey(api_key): ApiKey,
    Json(body): Json<RestoreRequest>,
) -> Response {
    let manager = &state.manager;
    let mut mgr = manager.write().await;

    let tenant_id = match mgr.get_tenant_by_api_key(&api_key) {
        Some(id) => id.clone(),
        None => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                "invalid_api_key",
                "Invalid API key",
            )
        }
    };

    let tenant = match mgr.get_tenant_mut(&tenant_id) {
        Some(t) => t,
        None => {
            return error_response(
                StatusCode::NOT_FOUND,
                "tenant_not_found",
                "Tenant not found",
            )
        }
    };

    match tenant
        .restore_pipeline(&pipeline_id, &body.checkpoint)
        .await
    {
        Ok(()) => {
            let resp = RestoreResponse {
                pipeline_id,
                restored: true,
                events_restored: body.checkpoint.events_processed,
            };
            axum::Json(&resp).into_response()
        }
        Err(e) => tenant_error_response(e),
    }
}

async fn handle_metrics(
    State(state): State<ApiState>,
    Path(pipeline_id): Path<String>,
    ApiKey(api_key): ApiKey,
) -> Response {
    let manager = &state.manager;
    let mgr = manager.read().await;

    let tenant_id = match mgr.get_tenant_by_api_key(&api_key) {
        Some(id) => id.clone(),
        None => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                "invalid_api_key",
                "Invalid API key",
            )
        }
    };

    let tenant = match mgr.get_tenant(&tenant_id) {
        Some(t) => t,
        None => {
            return error_response(
                StatusCode::NOT_FOUND,
                "tenant_not_found",
                "Tenant not found",
            )
        }
    };

    if !tenant.pipelines.contains_key(&pipeline_id) {
        return error_response(
            StatusCode::NOT_FOUND,
            "pipeline_not_found",
            "Pipeline not found",
        );
    }

    let resp = PipelineMetricsResponse {
        pipeline_id,
        events_processed: tenant.usage.events_processed,
        output_events_emitted: tenant.usage.output_events_emitted,
    };
    axum::Json(&resp).into_response()
}

async fn handle_topology(
    State(state): State<ApiState>,
    Path(pipeline_id): Path<String>,
    ApiKey(api_key): ApiKey,
) -> Response {
    let manager = &state.manager;
    let mgr = manager.read().await;

    let tenant_id = match mgr.get_tenant_by_api_key(&api_key) {
        Some(id) => id.clone(),
        None => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                "invalid_api_key",
                "Invalid API key",
            )
        }
    };

    let tenant = match mgr.get_tenant(&tenant_id) {
        Some(t) => t,
        None => {
            return error_response(
                StatusCode::NOT_FOUND,
                "tenant_not_found",
                "Tenant not found",
            )
        }
    };

    let pipeline = match tenant.pipelines.get(&pipeline_id) {
        Some(p) => p,
        None => {
            return error_response(
                StatusCode::NOT_FOUND,
                "pipeline_not_found",
                "Pipeline not found",
            )
        }
    };

    let engine = pipeline.engine.lock().await;
    let topology = engine.topology();
    axum::Json(&topology).into_response()
}

async fn handle_reload(
    State(state): State<ApiState>,
    Path(pipeline_id): Path<String>,
    ApiKey(api_key): ApiKey,
    Json(body): Json<ReloadPipelineRequest>,
) -> Response {
    let manager = &state.manager;
    let mut mgr = manager.write().await;

    let tenant_id = match mgr.get_tenant_by_api_key(&api_key) {
        Some(id) => id.clone(),
        None => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                "invalid_api_key",
                "Invalid API key",
            )
        }
    };

    let result = {
        let tenant = match mgr.get_tenant_mut(&tenant_id) {
            Some(t) => t,
            None => {
                return error_response(
                    StatusCode::NOT_FOUND,
                    "tenant_not_found",
                    "Tenant not found",
                )
            }
        };

        // Protect global pipelines from tenant reload
        if let Some(pipeline) = tenant.pipelines.get(&pipeline_id) {
            if pipeline.global_template_id.is_some() {
                return error_response(
                    StatusCode::FORBIDDEN,
                    "global_pipeline_protected",
                    "Global pipelines can only be managed by admin",
                );
            }
        }

        tenant.reload_pipeline(&pipeline_id, body.source).await
    };

    match result {
        Ok(()) => {
            mgr.persist_if_needed(&tenant_id);
            axum::Json(serde_json::json!({"reloaded": true})).into_response()
        }
        Err(e) => tenant_error_response(e),
    }
}

async fn handle_usage(State(state): State<ApiState>, ApiKey(api_key): ApiKey) -> Response {
    let manager = &state.manager;
    let mgr = manager.read().await;

    let tenant_id = match mgr.get_tenant_by_api_key(&api_key) {
        Some(id) => id.clone(),
        None => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                "invalid_api_key",
                "Invalid API key",
            )
        }
    };

    let tenant = match mgr.get_tenant(&tenant_id) {
        Some(t) => t,
        None => {
            return error_response(
                StatusCode::NOT_FOUND,
                "tenant_not_found",
                "Tenant not found",
            )
        }
    };

    let resp = UsageResponse {
        tenant_id: tenant.id.to_string(),
        events_processed: tenant.usage.events_processed,
        output_events_emitted: tenant.usage.output_events_emitted,
        active_pipelines: tenant.usage.active_pipelines,
        quota: QuotaInfo {
            max_pipelines: tenant.quota.max_pipelines,
            max_events_per_second: tenant.quota.max_events_per_second,
            max_streams_per_pipeline: tenant.quota.max_streams_per_pipeline,
        },
    };
    axum::Json(&resp).into_response()
}

/// Handle SSE log streaming for a pipeline
async fn handle_logs(
    State(state): State<ApiState>,
    Path(pipeline_id): Path<String>,
    ApiKey(api_key): ApiKey,
) -> Response {
    let manager = &state.manager;
    let mgr = manager.read().await;

    let tenant_id = match mgr.get_tenant_by_api_key(&api_key) {
        Some(id) => id.clone(),
        None => return error_response(StatusCode::UNAUTHORIZED, "invalid_key", "Invalid API key"),
    };

    // Verify tenant owns this pipeline
    let tenant = match mgr.get_tenant(&tenant_id) {
        Some(t) => t,
        None => {
            return error_response(
                StatusCode::NOT_FOUND,
                "tenant_not_found",
                "Tenant not found",
            )
        }
    };

    let rx: tokio::sync::broadcast::Receiver<Event> =
        match tenant.subscribe_pipeline_logs(&pipeline_id) {
            Ok(rx) => rx,
            Err(_) => {
                return error_response(
                    StatusCode::NOT_FOUND,
                    "pipeline_not_found",
                    &format!("Pipeline {pipeline_id} not found"),
                )
            }
        };

    drop(mgr); // Release the read lock before streaming

    // Create SSE stream from broadcast receiver using futures unfold
    let stream = stream::unfold(rx, |mut rx| async move {
        match rx.recv().await {
            Ok(event) => {
                let data: serde_json::Map<String, serde_json::Value> = event
                    .data
                    .iter()
                    .map(|(k, v): (&std::sync::Arc<str>, &varpulis_core::Value)| {
                        (k.to_string(), json_from_value(v))
                    })
                    .collect();
                let json = serde_json::to_string(&LogEvent {
                    event_type: event.event_type.to_string(),
                    timestamp: event.timestamp.to_rfc3339(),
                    data,
                })
                .unwrap_or_default();
                let sse = axum::response::sse::Event::default().data(json);
                Some((Ok::<_, Infallible>(sse), rx))
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                let msg = format!("{{\"warning\":\"skipped {n} events\"}}");
                let sse = axum::response::sse::Event::default()
                    .event("warning")
                    .data(msg);
                Some((Ok(sse), rx))
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => None,
        }
    });

    axum::response::sse::Sse::new(stream)
        .keep_alive(axum::response::sse::KeepAlive::default())
        .into_response()
}

#[derive(Serialize)]
struct LogEvent {
    event_type: String,
    timestamp: String,
    data: serde_json::Map<String, serde_json::Value>,
}

fn json_from_value(v: &varpulis_core::Value) -> serde_json::Value {
    match v {
        varpulis_core::Value::Null => serde_json::Value::Null,
        varpulis_core::Value::Bool(b) => serde_json::Value::Bool(*b),
        varpulis_core::Value::Int(i) => serde_json::json!(*i),
        varpulis_core::Value::Float(f) => serde_json::json!(*f),
        varpulis_core::Value::Str(s) => serde_json::Value::String(s.to_string()),
        varpulis_core::Value::Timestamp(ns) => serde_json::json!(*ns),
        varpulis_core::Value::Duration(ns) => serde_json::json!(*ns),
        varpulis_core::Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(json_from_value).collect())
        }
        varpulis_core::Value::Map(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.to_string(), json_from_value(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
    }
}

// =============================================================================
// DLQ Handlers
// =============================================================================

async fn handle_dlq_get(
    State(state): State<ApiState>,
    Path(pipeline_id): Path<String>,
    ApiKey(api_key): ApiKey,
    Query(params): Query<DlqQueryParams>,
) -> Response {
    let manager = &state.manager;
    let mgr = manager.read().await;

    let tenant_id = match mgr.get_tenant_by_api_key(&api_key) {
        Some(id) => id.clone(),
        None => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                "invalid_api_key",
                "Invalid API key",
            )
        }
    };

    let tenant = match mgr.get_tenant(&tenant_id) {
        Some(t) => t,
        None => {
            return error_response(
                StatusCode::NOT_FOUND,
                "tenant_not_found",
                "Tenant not found",
            )
        }
    };

    let pipeline = match tenant.pipelines.get(&pipeline_id) {
        Some(p) => p,
        None => {
            return error_response(
                StatusCode::NOT_FOUND,
                "pipeline_not_found",
                "Pipeline not found",
            )
        }
    };

    let engine = pipeline.engine.lock().await;
    let dlq = match engine.dlq() {
        Some(d) => d,
        None => {
            let resp = DlqEntriesResponse {
                entries: Vec::new(),
                total: 0,
            };
            return axum::Json(&resp).into_response();
        }
    };

    let offset = params.offset.unwrap_or(0);
    let limit = params.limit.unwrap_or(100).min(1000);

    match dlq.read_entries(offset, limit) {
        Ok(entries) => {
            let resp = DlqEntriesResponse {
                total: dlq.line_count(),
                entries,
            };
            axum::Json(&resp).into_response()
        }
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "dlq_read_error",
            &format!("Failed to read DLQ: {e}"),
        ),
    }
}

async fn handle_dlq_replay(
    State(state): State<ApiState>,
    Path(pipeline_id): Path<String>,
    ApiKey(api_key): ApiKey,
) -> Response {
    let manager = &state.manager;
    let mgr = manager.read().await;

    let tenant_id = match mgr.get_tenant_by_api_key(&api_key) {
        Some(id) => id.clone(),
        None => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                "invalid_api_key",
                "Invalid API key",
            )
        }
    };

    let tenant = match mgr.get_tenant(&tenant_id) {
        Some(t) => t,
        None => {
            return error_response(
                StatusCode::NOT_FOUND,
                "tenant_not_found",
                "Tenant not found",
            )
        }
    };

    let pipeline = match tenant.pipelines.get(&pipeline_id) {
        Some(p) => p,
        None => {
            return error_response(
                StatusCode::NOT_FOUND,
                "pipeline_not_found",
                "Pipeline not found",
            )
        }
    };

    // Read all DLQ entries
    let entries = {
        let engine = pipeline.engine.lock().await;
        let dlq = match engine.dlq() {
            Some(d) => d,
            None => {
                let resp = DlqReplayResponse { replayed: 0 };
                return axum::Json(&resp).into_response();
            }
        };
        // Read all entries (up to a reasonable limit)
        match dlq.read_entries(0, 100_000) {
            Ok(entries) => entries,
            Err(e) => {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "dlq_read_error",
                    &format!("Failed to read DLQ: {e}"),
                )
            }
        }
    };

    // Replay each entry as an event into the pipeline engine
    let mut replayed = 0usize;
    {
        let mut engine = pipeline.engine.lock().await;
        for entry in &entries {
            // Reconstruct event from the DLQ entry
            let event_type = entry
                .event
                .get("event_type")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let mut event = Event::new(event_type);
            if let Some(data) = entry.event.get("data").and_then(|v| v.as_object()) {
                for (k, v) in data {
                    let rv = json_to_runtime_value(v);
                    event = event.with_field(k.as_str(), rv);
                }
            }
            if engine.process(event).await.is_ok() {
                replayed += 1;
            }
        }
    }

    let resp = DlqReplayResponse { replayed };
    axum::Json(&resp).into_response()
}

async fn handle_dlq_clear(
    State(state): State<ApiState>,
    Path(pipeline_id): Path<String>,
    ApiKey(api_key): ApiKey,
) -> Response {
    let manager = &state.manager;
    let mgr = manager.read().await;

    let tenant_id = match mgr.get_tenant_by_api_key(&api_key) {
        Some(id) => id.clone(),
        None => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                "invalid_api_key",
                "Invalid API key",
            )
        }
    };

    let tenant = match mgr.get_tenant(&tenant_id) {
        Some(t) => t,
        None => {
            return error_response(
                StatusCode::NOT_FOUND,
                "tenant_not_found",
                "Tenant not found",
            )
        }
    };

    let pipeline = match tenant.pipelines.get(&pipeline_id) {
        Some(p) => p,
        None => {
            return error_response(
                StatusCode::NOT_FOUND,
                "pipeline_not_found",
                "Pipeline not found",
            )
        }
    };

    let engine = pipeline.engine.lock().await;
    match engine.dlq() {
        Some(dlq) => match dlq.clear() {
            Ok(()) => {
                let resp = DlqClearResponse { cleared: true };
                axum::Json(&resp).into_response()
            }
            Err(e) => error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "dlq_clear_error",
                &format!("Failed to clear DLQ: {e}"),
            ),
        },
        None => {
            let resp = DlqClearResponse { cleared: true };
            axum::Json(&resp).into_response()
        }
    }
}

// =============================================================================
// Tenant Admin Routes
// =============================================================================

// Tenant admin routes are now part of the main Router in api_routes()

#[allow(clippy::result_large_err)]
fn validate_admin_key(provided: &str, configured: &Option<String>) -> Result<(), Response> {
    match configured {
        None => Err(error_response(
            StatusCode::FORBIDDEN,
            "admin_disabled",
            "Admin API is disabled (no --api-key configured)",
        )),
        Some(key) => {
            if constant_time_compare(key, provided) {
                Ok(())
            } else {
                Err(error_response(
                    StatusCode::UNAUTHORIZED,
                    "invalid_admin_key",
                    "Invalid admin key",
                ))
            }
        }
    }
}

fn quota_from_tier(tier: Option<&str>) -> TenantQuota {
    match tier {
        Some("free") => TenantQuota::free(),
        Some("pro") => TenantQuota::pro(),
        Some("enterprise") => TenantQuota::enterprise(),
        _ => TenantQuota::default(),
    }
}

async fn handle_create_tenant(
    State(state): State<ApiState>,
    AdminKey(admin_key): AdminKey,
    Json(body): Json<CreateTenantRequest>,
) -> Response {
    let manager = &state.manager;
    let configured_key = &state.admin_key;
    if let Err(resp) = validate_admin_key(&admin_key, configured_key) {
        return resp;
    }

    let api_key = uuid::Uuid::new_v4().to_string();
    let quota = quota_from_tier(body.quota_tier.as_deref());

    let mut mgr = manager.write().await;
    match mgr.create_tenant(body.name.clone(), api_key.clone(), quota.clone()) {
        Ok(tenant_id) => {
            let resp = TenantResponse {
                id: tenant_id.as_str().to_string(),
                name: body.name,
                api_key,
                quota: QuotaInfo {
                    max_pipelines: quota.max_pipelines,
                    max_events_per_second: quota.max_events_per_second,
                    max_streams_per_pipeline: quota.max_streams_per_pipeline,
                },
            };
            (StatusCode::CREATED, axum::Json(&resp)).into_response()
        }
        Err(e) => tenant_error_response(e),
    }
}

async fn handle_list_tenants(
    State(state): State<ApiState>,
    AdminKey(admin_key): AdminKey,
    Query(pagination): Query<PaginationParams>,
) -> Response {
    let manager = &state.manager;
    let configured_key = &state.admin_key;
    if let Err(resp) = validate_admin_key(&admin_key, configured_key) {
        return resp;
    }

    if pagination.exceeds_max() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid_limit",
            &format!("limit must not exceed {MAX_LIMIT}"),
        );
    }

    let mgr = manager.read().await;
    let all_tenants: Vec<TenantResponse> = mgr
        .list_tenants()
        .iter()
        .map(|t| TenantResponse {
            id: t.id.as_str().to_string(),
            name: t.name.clone(),
            api_key: format!("{}...", &t.api_key_hash[..8]),
            quota: QuotaInfo {
                max_pipelines: t.quota.max_pipelines,
                max_events_per_second: t.quota.max_events_per_second,
                max_streams_per_pipeline: t.quota.max_streams_per_pipeline,
            },
        })
        .collect();
    let (tenants, meta) = pagination.paginate(all_tenants);
    let total = meta.total;
    let resp = TenantListResponse {
        tenants,
        total,
        pagination: Some(meta),
    };
    axum::Json(&resp).into_response()
}

async fn handle_get_tenant(
    State(state): State<ApiState>,
    Path(tenant_id_str): Path<String>,
    AdminKey(admin_key): AdminKey,
) -> Response {
    let manager = &state.manager;
    let configured_key = &state.admin_key;
    if let Err(resp) = validate_admin_key(&admin_key, configured_key) {
        return resp;
    }

    let mgr = manager.read().await;
    let tenant_id = varpulis_runtime::TenantId::new(&tenant_id_str);
    match mgr.get_tenant(&tenant_id) {
        Some(t) => {
            let resp = TenantDetailResponse {
                id: t.id.as_str().to_string(),
                name: t.name.clone(),
                api_key: format!("{}...", &t.api_key_hash[..8]),
                quota: QuotaInfo {
                    max_pipelines: t.quota.max_pipelines,
                    max_events_per_second: t.quota.max_events_per_second,
                    max_streams_per_pipeline: t.quota.max_streams_per_pipeline,
                },
                usage: TenantUsageInfo {
                    events_processed: t.usage.events_processed,
                    output_events_emitted: t.usage.output_events_emitted,
                    active_pipelines: t.usage.active_pipelines,
                },
                pipeline_count: t.pipelines.len(),
            };
            axum::Json(&resp).into_response()
        }
        None => error_response(
            StatusCode::NOT_FOUND,
            "tenant_not_found",
            "Tenant not found",
        ),
    }
}

async fn handle_delete_tenant(
    State(state): State<ApiState>,
    Path(tenant_id_str): Path<String>,
    AdminKey(admin_key): AdminKey,
) -> Response {
    let manager = &state.manager;
    let configured_key = &state.admin_key;
    if let Err(resp) = validate_admin_key(&admin_key, configured_key) {
        return resp;
    }

    let mut mgr = manager.write().await;
    let tenant_id = varpulis_runtime::TenantId::new(&tenant_id_str);
    match mgr.remove_tenant(&tenant_id) {
        Ok(()) => axum::Json(serde_json::json!({"deleted": true})).into_response(),
        Err(e) => tenant_error_response(e),
    }
}

// =============================================================================
// Pipeline Graph Handlers (Visual Builder)
// =============================================================================

async fn handle_pipeline_to_graph(Json(body): Json<PipelineGraphRequest>) -> Response {
    match varpulis_parser::parse(&body.vpl) {
        Ok(program) => {
            let graph = varpulis_runtime::engine::graph::program_to_graph(&program);
            (StatusCode::OK, axum::Json(graph)).into_response()
        }
        Err(e) => error_response(
            StatusCode::BAD_REQUEST,
            "parse_error",
            &format!("Failed to parse VPL: {e}"),
        ),
    }
}

async fn handle_graph_to_pipeline(
    Json(graph): Json<varpulis_runtime::engine::graph::PipelineGraph>,
) -> Response {
    let vpl = varpulis_runtime::engine::graph::graph_to_vpl(&graph);
    (StatusCode::OK, axum::Json(GenerateResponse { vpl })).into_response()
}

// =============================================================================
// Helpers
// =============================================================================

fn error_response(status: StatusCode, code: &str, message: &str) -> Response {
    let body = ApiError {
        error: message.to_string(),
        code: code.to_string(),
    };
    (status, axum::Json(body)).into_response()
}

fn tenant_error_response(err: TenantError) -> Response {
    // BackpressureExceeded needs a Retry-After header, handle it specially
    if let TenantError::BackpressureExceeded { current, max } = &err {
        let body = serde_json::json!({
            "error": format!("queue depth {current} exceeds maximum {max}"),
            "code": "queue_depth_exceeded",
            "retry_after": 1,
        });
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [("Retry-After", "1"), ("Content-Type", "application/json")],
            serde_json::to_string(&body).unwrap_or_default(),
        )
            .into_response();
    }

    let (status, code) = match &err {
        TenantError::NotFound(_) => (StatusCode::NOT_FOUND, "not_found"),
        TenantError::PipelineNotFound(_) => (StatusCode::NOT_FOUND, "pipeline_not_found"),
        TenantError::QuotaExceeded(_) => (StatusCode::TOO_MANY_REQUESTS, "quota_exceeded"),
        TenantError::RateLimitExceeded => (StatusCode::TOO_MANY_REQUESTS, "rate_limited"),
        TenantError::BackpressureExceeded { .. } => unreachable!(),
        TenantError::ParseError(_) => (StatusCode::BAD_REQUEST, "parse_error"),
        TenantError::EngineError(_) => (StatusCode::INTERNAL_SERVER_ERROR, "engine_error"),
        TenantError::AlreadyExists(_) => (StatusCode::CONFLICT, "already_exists"),
    };
    error_response(status, code, &err.to_string())
}

fn json_to_runtime_value(v: &serde_json::Value) -> varpulis_core::Value {
    match v {
        serde_json::Value::Null => varpulis_core::Value::Null,
        serde_json::Value::Bool(b) => varpulis_core::Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                varpulis_core::Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                varpulis_core::Value::Float(f)
            } else {
                varpulis_core::Value::Null
            }
        }
        serde_json::Value::String(s) => varpulis_core::Value::Str(s.clone().into()),
        serde_json::Value::Array(arr) => {
            varpulis_core::Value::array(arr.iter().map(json_to_runtime_value).collect())
        }
        serde_json::Value::Object(map) => {
            let mut m: IndexMap<std::sync::Arc<str>, varpulis_core::Value, FxBuildHasher> =
                IndexMap::with_hasher(FxBuildHasher);
            for (k, v) in map {
                m.insert(k.as_str().into(), json_to_runtime_value(v));
            }
            varpulis_core::Value::map(m)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::Request;
    use tokio::sync::RwLock;
    use tower::ServiceExt;
    use varpulis_runtime::tenant::{TenantManager, TenantQuota};

    use super::*;

    /// Test response wrapper for axum integration tests.
    struct TestResponse {
        status: StatusCode,
        body: bytes::Bytes,
        headers: axum::http::HeaderMap,
    }

    impl TestResponse {
        fn status(&self) -> StatusCode {
            self.status
        }
        fn body(&self) -> &[u8] {
            &self.body
        }
        fn headers(&self) -> &axum::http::HeaderMap {
            &self.headers
        }
    }

    /// Test request builder for axum integration tests.
    struct TestRequestBuilder {
        method: String,
        path: String,
        headers: Vec<(String, String)>,
        body: Option<String>,
    }

    impl TestRequestBuilder {
        fn new() -> Self {
            Self {
                method: "GET".to_string(),
                path: "/".to_string(),
                headers: Vec::new(),
                body: None,
            }
        }
        fn method(mut self, m: &str) -> Self {
            self.method = m.to_string();
            self
        }
        fn path(mut self, p: &str) -> Self {
            self.path = p.to_string();
            self
        }
        fn header(mut self, k: &str, v: &str) -> Self {
            self.headers.push((k.to_string(), v.to_string()));
            self
        }
        fn json<T: serde::Serialize>(mut self, body: &T) -> Self {
            self.body = Some(serde_json::to_string(body).unwrap());
            self.headers
                .push(("content-type".to_string(), "application/json".to_string()));
            self
        }
        async fn reply(self, app: &Router) -> TestResponse {
            let mut builder = Request::builder()
                .method(self.method.as_str())
                .uri(&self.path);
            for (k, v) in &self.headers {
                builder = builder.header(k.as_str(), v.as_str());
            }
            let body = match self.body {
                Some(b) => Body::from(b),
                None => Body::empty(),
            };
            let req = builder.body(body).unwrap();
            let resp = app.clone().oneshot(req).await.unwrap();
            let status = resp.status();
            let headers = resp.headers().clone();
            let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            TestResponse {
                status,
                body,
                headers,
            }
        }
    }

    /// Mimics `test_request()`.
    fn test_request() -> TestRequestBuilder {
        TestRequestBuilder::new()
    }

    async fn setup_test_manager() -> SharedTenantManager {
        let mut mgr = TenantManager::new();
        let id = mgr
            .create_tenant(
                "Test Corp".into(),
                "test-key-123".into(),
                TenantQuota::default(),
            )
            .unwrap();

        // Deploy a pipeline
        let tenant = mgr.get_tenant_mut(&id).unwrap();
        tenant
            .deploy_pipeline(
                "Test Pipeline".into(),
                "stream A = SensorReading .where(x > 1)".into(),
            )
            .await
            .unwrap();

        Arc::new(RwLock::new(mgr))
    }

    #[tokio::test]
    async fn test_deploy_pipeline() {
        let mgr = setup_test_manager().await;
        let routes = api_routes(mgr, None, None, None);

        let resp = test_request()
            .method("POST")
            .path("/api/v1/pipelines")
            .header("x-api-key", "test-key-123")
            .json(&DeployPipelineRequest {
                name: "New Pipeline".into(),
                source: "stream B = Events .where(y > 10)".into(),
            })
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::CREATED);
        let body: DeployPipelineResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body.name, "New Pipeline");
        assert_eq!(body.status, "running");
    }

    #[tokio::test]
    async fn test_deploy_invalid_api_key() {
        let mgr = setup_test_manager().await;
        let routes = api_routes(mgr, None, None, None);

        let resp = test_request()
            .method("POST")
            .path("/api/v1/pipelines")
            .header("x-api-key", "wrong-key")
            .json(&DeployPipelineRequest {
                name: "Bad".into(),
                source: "stream X = Y .where(z > 1)".into(),
            })
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_deploy_invalid_vpl() {
        let mgr = setup_test_manager().await;
        let routes = api_routes(mgr, None, None, None);

        let resp = test_request()
            .method("POST")
            .path("/api/v1/pipelines")
            .header("x-api-key", "test-key-123")
            .json(&DeployPipelineRequest {
                name: "Bad VPL".into(),
                source: "this is not valid {{{".into(),
            })
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_list_pipelines() {
        let mgr = setup_test_manager().await;
        let routes = api_routes(mgr, None, None, None);

        let resp = test_request()
            .method("GET")
            .path("/api/v1/pipelines")
            .header("x-api-key", "test-key-123")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let body: PipelineListResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body.total, 1);
        assert_eq!(body.pipelines[0].name, "Test Pipeline");
    }

    #[tokio::test]
    async fn test_usage_endpoint() {
        let mgr = setup_test_manager().await;
        let routes = api_routes(mgr, None, None, None);

        let resp = test_request()
            .method("GET")
            .path("/api/v1/usage")
            .header("x-api-key", "test-key-123")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let body: UsageResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body.active_pipelines, 1);
    }

    #[tokio::test]
    async fn test_inject_event() {
        let mgr = setup_test_manager().await;

        // Get pipeline ID
        let pipeline_id = {
            let m = mgr.read().await;
            let tid = m.get_tenant_by_api_key("test-key-123").unwrap().clone();
            let tenant = m.get_tenant(&tid).unwrap();
            tenant.pipelines.keys().next().unwrap().clone()
        };

        let routes = api_routes(mgr, None, None, None);

        let resp = test_request()
            .method("POST")
            .path(&format!("/api/v1/pipelines/{pipeline_id}/events"))
            .header("x-api-key", "test-key-123")
            .json(&InjectEventRequest {
                event_type: "SensorReading".into(),
                fields: {
                    let mut m = serde_json::Map::new();
                    m.insert(
                        "x".into(),
                        serde_json::Value::Number(serde_json::Number::from(42)),
                    );
                    m
                },
            })
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_json_to_runtime_value() {
        assert_eq!(
            json_to_runtime_value(&serde_json::json!(null)),
            varpulis_core::Value::Null
        );
        assert_eq!(
            json_to_runtime_value(&serde_json::json!(true)),
            varpulis_core::Value::Bool(true)
        );
        assert_eq!(
            json_to_runtime_value(&serde_json::json!(42)),
            varpulis_core::Value::Int(42)
        );
        assert_eq!(
            json_to_runtime_value(&serde_json::json!(1.23)),
            varpulis_core::Value::Float(1.23)
        );
        assert_eq!(
            json_to_runtime_value(&serde_json::json!("hello")),
            varpulis_core::Value::Str("hello".into())
        );
    }

    #[test]
    fn test_error_response_format() {
        let resp = error_response(StatusCode::BAD_REQUEST, "test_error", "Something failed");
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_tenant_error_mapping() {
        let resp = tenant_error_response(TenantError::NotFound("t1".into()));
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let resp = tenant_error_response(TenantError::RateLimitExceeded);
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);

        let parse_err = varpulis_parser::parse("INVALID{{{").unwrap_err();
        let resp = tenant_error_response(TenantError::ParseError(parse_err));
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // =========================================================================
    // Tenant Admin API tests
    // =========================================================================

    fn setup_admin_routes(admin_key: Option<&str>) -> (SharedTenantManager, Router) {
        let mgr = Arc::new(RwLock::new(TenantManager::new()));
        let key = admin_key.map(|k| k.to_string());
        let routes = api_routes(mgr.clone(), key, None, None);
        (mgr, routes)
    }

    #[tokio::test]
    async fn test_create_tenant() {
        let (_mgr, routes) = setup_admin_routes(Some("admin-secret"));

        let resp = test_request()
            .method("POST")
            .path("/api/v1/tenants")
            .header("x-admin-key", "admin-secret")
            .json(&CreateTenantRequest {
                name: "Acme Corp".into(),
                quota_tier: None,
            })
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::CREATED);
        let body: TenantResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body.name, "Acme Corp");
        assert!(!body.api_key.is_empty());
        assert!(!body.id.is_empty());
    }

    #[tokio::test]
    async fn test_list_tenants_admin() {
        let (_mgr, routes) = setup_admin_routes(Some("admin-secret"));

        // Create two tenants
        for name in &["Tenant A", "Tenant B"] {
            test_request()
                .method("POST")
                .path("/api/v1/tenants")
                .header("x-admin-key", "admin-secret")
                .json(&CreateTenantRequest {
                    name: name.to_string(),
                    quota_tier: None,
                })
                .reply(&routes)
                .await;
        }

        let resp = test_request()
            .method("GET")
            .path("/api/v1/tenants")
            .header("x-admin-key", "admin-secret")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let body: TenantListResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body.total, 2);
    }

    #[tokio::test]
    async fn test_get_tenant_admin() {
        let (_mgr, routes) = setup_admin_routes(Some("admin-secret"));

        // Create a tenant
        let create_resp = test_request()
            .method("POST")
            .path("/api/v1/tenants")
            .header("x-admin-key", "admin-secret")
            .json(&CreateTenantRequest {
                name: "Detail Corp".into(),
                quota_tier: Some("pro".into()),
            })
            .reply(&routes)
            .await;

        let created: TenantResponse = serde_json::from_slice(create_resp.body()).unwrap();

        let resp = test_request()
            .method("GET")
            .path(&format!("/api/v1/tenants/{}", created.id))
            .header("x-admin-key", "admin-secret")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let body: TenantDetailResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body.name, "Detail Corp");
        assert_eq!(body.pipeline_count, 0);
        // Pro tier quotas
        assert_eq!(body.quota.max_pipelines, 20);
    }

    #[tokio::test]
    async fn test_delete_tenant_admin() {
        let (_mgr, routes) = setup_admin_routes(Some("admin-secret"));

        // Create then delete
        let create_resp = test_request()
            .method("POST")
            .path("/api/v1/tenants")
            .header("x-admin-key", "admin-secret")
            .json(&CreateTenantRequest {
                name: "Doomed".into(),
                quota_tier: None,
            })
            .reply(&routes)
            .await;
        let created: TenantResponse = serde_json::from_slice(create_resp.body()).unwrap();

        let resp = test_request()
            .method("DELETE")
            .path(&format!("/api/v1/tenants/{}", created.id))
            .header("x-admin-key", "admin-secret")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);

        // Verify tenant is gone
        let list_resp = test_request()
            .method("GET")
            .path("/api/v1/tenants")
            .header("x-admin-key", "admin-secret")
            .reply(&routes)
            .await;
        let body: TenantListResponse = serde_json::from_slice(list_resp.body()).unwrap();
        assert_eq!(body.total, 0);
    }

    #[tokio::test]
    async fn test_invalid_admin_key() {
        let (_mgr, routes) = setup_admin_routes(Some("admin-secret"));

        let resp = test_request()
            .method("GET")
            .path("/api/v1/tenants")
            .header("x-admin-key", "wrong-key")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_no_admin_key_configured() {
        let (_mgr, routes) = setup_admin_routes(None);

        let resp = test_request()
            .method("GET")
            .path("/api/v1/tenants")
            .header("x-admin-key", "anything")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn test_create_tenant_tier_selection() {
        let (_mgr, routes) = setup_admin_routes(Some("admin-secret"));

        // Free tier
        let resp = test_request()
            .method("POST")
            .path("/api/v1/tenants")
            .header("x-admin-key", "admin-secret")
            .json(&CreateTenantRequest {
                name: "Free User".into(),
                quota_tier: Some("free".into()),
            })
            .reply(&routes)
            .await;
        let body: TenantResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body.quota.max_pipelines, 5); // free tier

        // Enterprise tier
        let resp = test_request()
            .method("POST")
            .path("/api/v1/tenants")
            .header("x-admin-key", "admin-secret")
            .json(&CreateTenantRequest {
                name: "Enterprise User".into(),
                quota_tier: Some("enterprise".into()),
            })
            .reply(&routes)
            .await;
        let body: TenantResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body.quota.max_pipelines, 1000); // enterprise tier
    }

    // =========================================================================
    // Pipeline CRUD handler tests
    // =========================================================================

    /// Helper: get the first pipeline ID from the test manager
    async fn get_first_pipeline_id(mgr: &SharedTenantManager) -> String {
        let m = mgr.read().await;
        let tid = m.get_tenant_by_api_key("test-key-123").unwrap().clone();
        let tenant = m.get_tenant(&tid).unwrap();
        tenant.pipelines.keys().next().unwrap().clone()
    }

    #[tokio::test]
    async fn test_get_single_pipeline() {
        let mgr = setup_test_manager().await;
        let pipeline_id = get_first_pipeline_id(&mgr).await;
        let routes = api_routes(mgr, None, None, None);

        let resp = test_request()
            .method("GET")
            .path(&format!("/api/v1/pipelines/{pipeline_id}"))
            .header("x-api-key", "test-key-123")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let body: PipelineInfo = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body.id, pipeline_id);
        assert_eq!(body.name, "Test Pipeline");
        assert_eq!(body.status, "running");
        assert!(body.source.contains("SensorReading"));
    }

    #[tokio::test]
    async fn test_get_pipeline_not_found() {
        let mgr = setup_test_manager().await;
        let routes = api_routes(mgr, None, None, None);

        let resp = test_request()
            .method("GET")
            .path("/api/v1/pipelines/nonexistent-id")
            .header("x-api-key", "test-key-123")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_pipeline_api() {
        let mgr = setup_test_manager().await;
        let pipeline_id = get_first_pipeline_id(&mgr).await;
        let routes = api_routes(mgr.clone(), None, None, None);

        let resp = test_request()
            .method("DELETE")
            .path(&format!("/api/v1/pipelines/{pipeline_id}"))
            .header("x-api-key", "test-key-123")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body["deleted"], true);

        // Verify it's gone
        let list_resp = test_request()
            .method("GET")
            .path("/api/v1/pipelines")
            .header("x-api-key", "test-key-123")
            .reply(&routes)
            .await;
        let list: PipelineListResponse = serde_json::from_slice(list_resp.body()).unwrap();
        assert_eq!(list.total, 0);
    }

    #[tokio::test]
    async fn test_delete_pipeline_not_found() {
        let mgr = setup_test_manager().await;
        let routes = api_routes(mgr, None, None, None);

        let resp = test_request()
            .method("DELETE")
            .path("/api/v1/pipelines/nonexistent-id")
            .header("x-api-key", "test-key-123")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // =========================================================================
    // Batch inject handler tests
    // =========================================================================

    #[tokio::test]
    async fn test_inject_batch() {
        let mgr = setup_test_manager().await;
        let pipeline_id = get_first_pipeline_id(&mgr).await;
        let routes = api_routes(mgr, None, None, None);

        let resp = test_request()
            .method("POST")
            .path(&format!("/api/v1/pipelines/{pipeline_id}/events-batch"))
            .header("x-api-key", "test-key-123")
            .json(&InjectBatchRequest {
                events: vec![
                    InjectEventRequest {
                        event_type: "SensorReading".into(),
                        fields: {
                            let mut m = serde_json::Map::new();
                            m.insert("x".into(), serde_json::json!(5));
                            m
                        },
                    },
                    InjectEventRequest {
                        event_type: "SensorReading".into(),
                        fields: {
                            let mut m = serde_json::Map::new();
                            m.insert("x".into(), serde_json::json!(10));
                            m
                        },
                    },
                ],
            })
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let body: InjectBatchResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body.accepted, 2);
        assert!(body.processing_time_us > 0);
    }

    #[tokio::test]
    async fn test_inject_batch_invalid_pipeline() {
        let mgr = setup_test_manager().await;
        let routes = api_routes(mgr, None, None, None);

        // Batch mode silently skips failed events (including nonexistent pipeline)
        let resp = test_request()
            .method("POST")
            .path("/api/v1/pipelines/nonexistent/events-batch")
            .header("x-api-key", "test-key-123")
            .json(&InjectBatchRequest {
                events: vec![InjectEventRequest {
                    event_type: "Test".into(),
                    fields: serde_json::Map::new(),
                }],
            })
            .reply(&routes)
            .await;

        // Returns 200 but accepted=0 since pipeline doesn't exist
        assert_eq!(resp.status(), StatusCode::OK);
        let body: InjectBatchResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body.accepted, 0);
    }

    // =========================================================================
    // Checkpoint/Restore handler tests
    // =========================================================================

    #[tokio::test]
    async fn test_checkpoint_pipeline() {
        let mgr = setup_test_manager().await;
        let pipeline_id = get_first_pipeline_id(&mgr).await;
        let routes = api_routes(mgr, None, None, None);

        let resp = test_request()
            .method("POST")
            .path(&format!("/api/v1/pipelines/{pipeline_id}/checkpoint"))
            .header("x-api-key", "test-key-123")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let body: CheckpointResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body.pipeline_id, pipeline_id);
    }

    #[tokio::test]
    async fn test_checkpoint_not_found() {
        let mgr = setup_test_manager().await;
        let routes = api_routes(mgr, None, None, None);

        let resp = test_request()
            .method("POST")
            .path("/api/v1/pipelines/nonexistent/checkpoint")
            .header("x-api-key", "test-key-123")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_restore_pipeline() {
        let mgr = setup_test_manager().await;
        let pipeline_id = get_first_pipeline_id(&mgr).await;
        let routes = api_routes(mgr, None, None, None);

        // First checkpoint
        let cp_resp = test_request()
            .method("POST")
            .path(&format!("/api/v1/pipelines/{pipeline_id}/checkpoint"))
            .header("x-api-key", "test-key-123")
            .reply(&routes)
            .await;
        let cp: CheckpointResponse = serde_json::from_slice(cp_resp.body()).unwrap();

        // Then restore
        let resp = test_request()
            .method("POST")
            .path(&format!("/api/v1/pipelines/{pipeline_id}/restore"))
            .header("x-api-key", "test-key-123")
            .json(&RestoreRequest {
                checkpoint: cp.checkpoint,
            })
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let body: RestoreResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body.pipeline_id, pipeline_id);
        assert!(body.restored);
    }

    #[tokio::test]
    async fn test_restore_not_found() {
        let mgr = setup_test_manager().await;
        let routes = api_routes(mgr, None, None, None);

        let checkpoint = varpulis_runtime::persistence::EngineCheckpoint {
            version: varpulis_runtime::persistence::CHECKPOINT_VERSION,
            window_states: std::collections::HashMap::new(),
            sase_states: std::collections::HashMap::new(),
            join_states: std::collections::HashMap::new(),
            variables: std::collections::HashMap::new(),
            events_processed: 0,
            output_events_emitted: 0,
            watermark_state: None,
            distinct_states: std::collections::HashMap::new(),
            limit_states: std::collections::HashMap::new(),
            source_offsets: std::collections::HashMap::new(),
        };

        let resp = test_request()
            .method("POST")
            .path("/api/v1/pipelines/nonexistent/restore")
            .header("x-api-key", "test-key-123")
            .json(&RestoreRequest { checkpoint })
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // =========================================================================
    // Metrics handler tests
    // =========================================================================

    #[tokio::test]
    async fn test_metrics_endpoint() {
        let mgr = setup_test_manager().await;
        let pipeline_id = get_first_pipeline_id(&mgr).await;
        let routes = api_routes(mgr, None, None, None);

        let resp = test_request()
            .method("GET")
            .path(&format!("/api/v1/pipelines/{pipeline_id}/metrics"))
            .header("x-api-key", "test-key-123")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let body: PipelineMetricsResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body.pipeline_id, pipeline_id);
    }

    #[tokio::test]
    async fn test_metrics_not_found() {
        let mgr = setup_test_manager().await;
        let routes = api_routes(mgr, None, None, None);

        let resp = test_request()
            .method("GET")
            .path("/api/v1/pipelines/nonexistent/metrics")
            .header("x-api-key", "test-key-123")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // =========================================================================
    // Reload handler tests
    // =========================================================================

    #[tokio::test]
    async fn test_reload_pipeline() {
        let mgr = setup_test_manager().await;
        let pipeline_id = get_first_pipeline_id(&mgr).await;
        let routes = api_routes(mgr, None, None, None);

        let resp = test_request()
            .method("POST")
            .path(&format!("/api/v1/pipelines/{pipeline_id}/reload"))
            .header("x-api-key", "test-key-123")
            .json(&ReloadPipelineRequest {
                source: "stream B = Events .where(y > 10)".into(),
            })
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body["reloaded"], true);
    }

    #[tokio::test]
    async fn test_reload_invalid_vpl() {
        let mgr = setup_test_manager().await;
        let pipeline_id = get_first_pipeline_id(&mgr).await;
        let routes = api_routes(mgr, None, None, None);

        let resp = test_request()
            .method("POST")
            .path(&format!("/api/v1/pipelines/{pipeline_id}/reload"))
            .header("x-api-key", "test-key-123")
            .json(&ReloadPipelineRequest {
                source: "not valid {{{".into(),
            })
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_reload_not_found() {
        let mgr = setup_test_manager().await;
        let routes = api_routes(mgr, None, None, None);

        let resp = test_request()
            .method("POST")
            .path("/api/v1/pipelines/nonexistent/reload")
            .header("x-api-key", "test-key-123")
            .json(&ReloadPipelineRequest {
                source: "stream B = Events .where(y > 10)".into(),
            })
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // =========================================================================
    // Logs (SSE) handler tests
    // =========================================================================

    #[tokio::test]
    async fn test_logs_invalid_pipeline() {
        let mgr = setup_test_manager().await;
        let routes = api_routes(mgr, None, None, None);

        let resp = test_request()
            .method("GET")
            .path("/api/v1/pipelines/nonexistent/logs")
            .header("x-api-key", "test-key-123")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_logs_invalid_api_key() {
        let mgr = setup_test_manager().await;
        let pipeline_id = get_first_pipeline_id(&mgr).await;
        let routes = api_routes(mgr, None, None, None);

        let resp = test_request()
            .method("GET")
            .path(&format!("/api/v1/pipelines/{pipeline_id}/logs"))
            .header("x-api-key", "wrong-key")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // =========================================================================
    // json_to_runtime_value extended tests
    // =========================================================================

    #[test]
    fn test_json_to_runtime_value_array() {
        let arr = serde_json::json!([1, "hello", true]);
        let val = json_to_runtime_value(&arr);
        match val {
            varpulis_core::Value::Array(a) => {
                assert_eq!(a.len(), 3);
                assert_eq!(a[0], varpulis_core::Value::Int(1));
                assert_eq!(a[1], varpulis_core::Value::Str("hello".into()));
                assert_eq!(a[2], varpulis_core::Value::Bool(true));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_json_to_runtime_value_object() {
        let obj = serde_json::json!({"key": "val", "num": 42});
        let val = json_to_runtime_value(&obj);
        match val {
            varpulis_core::Value::Map(m) => {
                assert_eq!(m.len(), 2);
            }
            _ => panic!("Expected Map"),
        }
    }

    #[test]
    fn test_json_from_value_roundtrip() {
        use varpulis_core::Value;
        assert_eq!(json_from_value(&Value::Null), serde_json::json!(null));
        assert_eq!(json_from_value(&Value::Bool(true)), serde_json::json!(true));
        assert_eq!(json_from_value(&Value::Int(42)), serde_json::json!(42));
        assert_eq!(
            json_from_value(&Value::Float(2.71)),
            serde_json::json!(2.71)
        );
        assert_eq!(
            json_from_value(&Value::Str("hi".into())),
            serde_json::json!("hi")
        );
        assert_eq!(
            json_from_value(&Value::Timestamp(1000000)),
            serde_json::json!(1000000)
        );
        assert_eq!(
            json_from_value(&Value::Duration(5000)),
            serde_json::json!(5000)
        );
    }

    // =========================================================================
    // Additional tenant_error_response coverage
    // =========================================================================

    #[test]
    fn test_tenant_error_all_variants() {
        let resp = tenant_error_response(TenantError::PipelineNotFound("p1".into()));
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let resp = tenant_error_response(TenantError::QuotaExceeded("max pipelines".into()));
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);

        let resp = tenant_error_response(TenantError::EngineError(
            varpulis_runtime::EngineError::Pipeline("boom".into()),
        ));
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let resp = tenant_error_response(TenantError::AlreadyExists("t1".into()));
        assert_eq!(resp.status(), StatusCode::CONFLICT);

        let resp = tenant_error_response(TenantError::BackpressureExceeded {
            current: 50000,
            max: 50000,
        });
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(resp.headers().get("Retry-After").unwrap(), "1");
    }

    // =========================================================================
    // Pagination tests
    // =========================================================================

    #[tokio::test]
    async fn test_list_pipelines_default_pagination() {
        let mgr = setup_test_manager().await;
        let routes = api_routes(mgr, None, None, None);

        let resp = test_request()
            .method("GET")
            .path("/api/v1/pipelines")
            .header("x-api-key", "test-key-123")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let body: PipelineListResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body.total, 1);
        let pagination = body.pagination.unwrap();
        assert_eq!(pagination.total, 1);
        assert_eq!(pagination.offset, 0);
        assert_eq!(pagination.limit, 50);
        assert!(!pagination.has_more);
    }

    #[tokio::test]
    async fn test_list_pipelines_with_pagination_params() {
        let mgr = setup_test_manager().await;

        // Deploy two more pipelines
        {
            let mut m = mgr.write().await;
            let tid = m.get_tenant_by_api_key("test-key-123").unwrap().clone();
            let tenant = m.get_tenant_mut(&tid).unwrap();
            tenant
                .deploy_pipeline(
                    "Pipeline B".into(),
                    "stream B = Events .where(y > 2)".into(),
                )
                .await
                .unwrap();
            tenant
                .deploy_pipeline(
                    "Pipeline C".into(),
                    "stream C = Events .where(z > 3)".into(),
                )
                .await
                .unwrap();
        }

        let routes = api_routes(mgr, None, None, None);

        // First page: limit=1, offset=0
        let resp = test_request()
            .method("GET")
            .path("/api/v1/pipelines?limit=1&offset=0")
            .header("x-api-key", "test-key-123")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let body: PipelineListResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body.pipelines.len(), 1);
        assert_eq!(body.total, 3);
        let pagination = body.pagination.unwrap();
        assert!(pagination.has_more);
        assert_eq!(pagination.limit, 1);

        // Second page: limit=1, offset=2
        let resp = test_request()
            .method("GET")
            .path("/api/v1/pipelines?limit=1&offset=2")
            .header("x-api-key", "test-key-123")
            .reply(&routes)
            .await;

        let body: PipelineListResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body.pipelines.len(), 1);
        assert_eq!(body.total, 3);
        assert!(!body.pagination.unwrap().has_more);
    }

    #[tokio::test]
    async fn test_list_pipelines_limit_exceeds_max() {
        let mgr = setup_test_manager().await;
        let routes = api_routes(mgr, None, None, None);

        let resp = test_request()
            .method("GET")
            .path("/api/v1/pipelines?limit=1001")
            .header("x-api-key", "test-key-123")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_list_tenants_with_pagination() {
        let (_mgr, routes) = setup_admin_routes(Some("admin-secret"));

        // Create 3 tenants
        for name in &["T1", "T2", "T3"] {
            test_request()
                .method("POST")
                .path("/api/v1/tenants")
                .header("x-admin-key", "admin-secret")
                .json(&CreateTenantRequest {
                    name: name.to_string(),
                    quota_tier: None,
                })
                .reply(&routes)
                .await;
        }

        // Page through with limit=2
        let resp = test_request()
            .method("GET")
            .path("/api/v1/tenants?limit=2&offset=0")
            .header("x-admin-key", "admin-secret")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let body: TenantListResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body.tenants.len(), 2);
        assert_eq!(body.total, 3);
        assert!(body.pagination.unwrap().has_more);

        // Last page
        let resp = test_request()
            .method("GET")
            .path("/api/v1/tenants?limit=2&offset=2")
            .header("x-admin-key", "admin-secret")
            .reply(&routes)
            .await;

        let body: TenantListResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body.tenants.len(), 1);
        assert!(!body.pagination.unwrap().has_more);
    }

    #[tokio::test]
    async fn test_inject_backpressure_429() {
        use std::sync::atomic::Ordering;

        let mut mgr = TenantManager::new();
        mgr.set_max_queue_depth(5);
        let id = mgr
            .create_tenant(
                "BP Corp".into(),
                "bp-key-123".into(),
                TenantQuota::default(),
            )
            .unwrap();

        let tenant = mgr.get_tenant_mut(&id).unwrap();
        let pid = tenant
            .deploy_pipeline(
                "BP Pipeline".into(),
                "stream A = SensorReading .where(x > 1)".into(),
            )
            .await
            .unwrap();

        // Simulate queue being full
        mgr.pending_events_counter().store(5, Ordering::Relaxed);

        let shared = Arc::new(RwLock::new(mgr));
        let routes = api_routes(shared, None, None, None);

        let resp = test_request()
            .method("POST")
            .path(&format!("/api/v1/pipelines/{pid}/events"))
            .header("x-api-key", "bp-key-123")
            .json(&InjectEventRequest {
                event_type: "SensorReading".into(),
                fields: serde_json::Map::new(),
            })
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        // Check Retry-After header
        assert_eq!(resp.headers().get("Retry-After").unwrap(), "1");
        // Check response body
        let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body["code"], "queue_depth_exceeded");
    }

    #[tokio::test]
    async fn test_inject_batch_backpressure_429() {
        use std::sync::atomic::Ordering;

        let mut mgr = TenantManager::new();
        mgr.set_max_queue_depth(5);
        let id = mgr
            .create_tenant(
                "BP Batch Corp".into(),
                "bp-batch-key".into(),
                TenantQuota::default(),
            )
            .unwrap();

        let tenant = mgr.get_tenant_mut(&id).unwrap();
        let pid = tenant
            .deploy_pipeline(
                "BP Batch Pipeline".into(),
                "stream A = SensorReading .where(x > 1)".into(),
            )
            .await
            .unwrap();

        // Simulate queue being full
        mgr.pending_events_counter().store(5, Ordering::Relaxed);

        let shared = Arc::new(RwLock::new(mgr));
        let routes = api_routes(shared, None, None, None);

        let resp = test_request()
            .method("POST")
            .path(&format!("/api/v1/pipelines/{pid}/events-batch"))
            .header("x-api-key", "bp-batch-key")
            .json(&InjectBatchRequest {
                events: vec![InjectEventRequest {
                    event_type: "SensorReading".into(),
                    fields: serde_json::Map::new(),
                }],
            })
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(resp.headers().get("Retry-After").unwrap(), "1");
    }
}
