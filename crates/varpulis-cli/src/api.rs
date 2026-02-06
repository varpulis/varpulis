//! REST API for SaaS pipeline management
//!
//! Provides RESTful endpoints for deploying and managing CEP pipelines
//! in a multi-tenant environment.

use crate::auth::constant_time_compare;
use indexmap::IndexMap;
use rustc_hash::FxBuildHasher;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use varpulis_runtime::tenant::{SharedTenantManager, TenantError, TenantQuota};
use varpulis_runtime::Event;
use warp::http::StatusCode;
use warp::{Filter, Rejection, Reply};

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
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PipelineListResponse {
    pub pipelines: Vec<PipelineInfo>,
    pub total: usize,
}

#[derive(Debug, Serialize)]
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
pub struct ReloadPipelineRequest {
    pub source: String,
}

#[derive(Debug, Serialize)]
pub struct ApiError {
    pub error: String,
    pub code: String,
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

/// Build the complete API route tree
pub fn api_routes(
    manager: SharedTenantManager,
    admin_key: Option<String>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let api = warp::path("api").and(warp::path("v1"));

    let admin_routes = tenant_admin_routes(manager.clone(), admin_key);

    let deploy = api
        .and(warp::path("pipelines"))
        .and(warp::path::end())
        .and(warp::post())
        .and(with_api_key())
        .and(warp::body::json())
        .and(with_manager(manager.clone()))
        .and_then(handle_deploy);

    let list = api
        .and(warp::path("pipelines"))
        .and(warp::path::end())
        .and(warp::get())
        .and(with_api_key())
        .and(with_manager(manager.clone()))
        .and_then(handle_list);

    let get_pipeline = api
        .and(warp::path("pipelines"))
        .and(warp::path::param::<String>())
        .and(warp::path::end())
        .and(warp::get())
        .and(with_api_key())
        .and(with_manager(manager.clone()))
        .and_then(handle_get);

    let delete = api
        .and(warp::path("pipelines"))
        .and(warp::path::param::<String>())
        .and(warp::path::end())
        .and(warp::delete())
        .and(with_api_key())
        .and(with_manager(manager.clone()))
        .and_then(handle_delete);

    let inject = api
        .and(warp::path("pipelines"))
        .and(warp::path::param::<String>())
        .and(warp::path("events"))
        .and(warp::path::end())
        .and(warp::post())
        .and(with_api_key())
        .and(warp::body::json())
        .and(with_manager(manager.clone()))
        .and_then(handle_inject);

    let metrics = api
        .and(warp::path("pipelines"))
        .and(warp::path::param::<String>())
        .and(warp::path("metrics"))
        .and(warp::path::end())
        .and(warp::get())
        .and(with_api_key())
        .and(with_manager(manager.clone()))
        .and_then(handle_metrics);

    let reload = api
        .and(warp::path("pipelines"))
        .and(warp::path::param::<String>())
        .and(warp::path("reload"))
        .and(warp::path::end())
        .and(warp::post())
        .and(with_api_key())
        .and(warp::body::json())
        .and(with_manager(manager.clone()))
        .and_then(handle_reload);

    let usage = api
        .and(warp::path("usage"))
        .and(warp::path::end())
        .and(warp::get())
        .and(with_api_key())
        .and(with_manager(manager.clone()))
        .and_then(handle_usage);

    // CORS configuration for browser-based clients
    let cors = warp::cors()
        .allow_any_origin()
        .allow_methods(vec!["GET", "POST", "DELETE", "OPTIONS"])
        .allow_headers(vec!["content-type", "x-api-key", "authorization"]);

    deploy
        .or(list)
        .or(get_pipeline)
        .or(delete)
        .or(inject)
        .or(metrics)
        .or(reload)
        .or(usage)
        .or(admin_routes)
        .with(cors)
}

// =============================================================================
// Filters
// =============================================================================

fn with_manager(
    manager: SharedTenantManager,
) -> impl Filter<Extract = (SharedTenantManager,), Error = Infallible> + Clone {
    warp::any().map(move || manager.clone())
}

fn with_api_key() -> impl Filter<Extract = (String,), Error = Rejection> + Clone {
    warp::header::<String>("x-api-key")
}

// =============================================================================
// Handlers
// =============================================================================

async fn handle_deploy(
    api_key: String,
    body: DeployPipelineRequest,
    manager: SharedTenantManager,
) -> Result<impl Reply, Infallible> {
    let mut mgr = manager.write().await;

    let tenant_id = match mgr.get_tenant_by_api_key(&api_key) {
        Some(id) => id.clone(),
        None => {
            return Ok(error_response(
                StatusCode::UNAUTHORIZED,
                "invalid_api_key",
                "Invalid API key",
            ))
        }
    };

    let tenant = match mgr.get_tenant_mut(&tenant_id) {
        Some(t) => t,
        None => {
            return Ok(error_response(
                StatusCode::NOT_FOUND,
                "tenant_not_found",
                "Tenant not found",
            ))
        }
    };
    let result = tenant.deploy_pipeline(body.name.clone(), body.source).await;

    match result {
        Ok(id) => {
            mgr.persist_if_needed(&tenant_id);
            let resp = DeployPipelineResponse {
                id,
                name: body.name,
                status: "running".to_string(),
            };
            Ok(
                warp::reply::with_status(warp::reply::json(&resp), StatusCode::CREATED)
                    .into_response(),
            )
        }
        Err(e) => Ok(tenant_error_response(e)),
    }
}

async fn handle_list(
    api_key: String,
    manager: SharedTenantManager,
) -> Result<impl Reply, Infallible> {
    let mgr = manager.read().await;

    let tenant_id = match mgr.get_tenant_by_api_key(&api_key) {
        Some(id) => id.clone(),
        None => {
            return Ok(error_response(
                StatusCode::UNAUTHORIZED,
                "invalid_api_key",
                "Invalid API key",
            ))
        }
    };

    let tenant = match mgr.get_tenant(&tenant_id) {
        Some(t) => t,
        None => {
            return Ok(error_response(
                StatusCode::NOT_FOUND,
                "tenant_not_found",
                "Tenant not found",
            ))
        }
    };

    let pipelines: Vec<PipelineInfo> = tenant
        .pipelines
        .values()
        .map(|p| PipelineInfo {
            id: p.id.clone(),
            name: p.name.clone(),
            status: p.status.to_string(),
            source: p.source.clone(),
            uptime_secs: p.created_at.elapsed().as_secs(),
        })
        .collect();

    let total = pipelines.len();
    let resp = PipelineListResponse { pipelines, total };
    Ok(warp::reply::with_status(warp::reply::json(&resp), StatusCode::OK).into_response())
}

async fn handle_get(
    pipeline_id: String,
    api_key: String,
    manager: SharedTenantManager,
) -> Result<impl Reply, Infallible> {
    let mgr = manager.read().await;

    let tenant_id = match mgr.get_tenant_by_api_key(&api_key) {
        Some(id) => id.clone(),
        None => {
            return Ok(error_response(
                StatusCode::UNAUTHORIZED,
                "invalid_api_key",
                "Invalid API key",
            ))
        }
    };

    let tenant = match mgr.get_tenant(&tenant_id) {
        Some(t) => t,
        None => {
            return Ok(error_response(
                StatusCode::NOT_FOUND,
                "tenant_not_found",
                "Tenant not found",
            ))
        }
    };

    match tenant.pipelines.get(&pipeline_id) {
        Some(p) => {
            let info = PipelineInfo {
                id: p.id.clone(),
                name: p.name.clone(),
                status: p.status.to_string(),
                source: p.source.clone(),
                uptime_secs: p.created_at.elapsed().as_secs(),
            };
            Ok(warp::reply::with_status(warp::reply::json(&info), StatusCode::OK).into_response())
        }
        None => Ok(error_response(
            StatusCode::NOT_FOUND,
            "pipeline_not_found",
            "Pipeline not found",
        )),
    }
}

async fn handle_delete(
    pipeline_id: String,
    api_key: String,
    manager: SharedTenantManager,
) -> Result<impl Reply, Infallible> {
    let mut mgr = manager.write().await;

    let tenant_id = match mgr.get_tenant_by_api_key(&api_key) {
        Some(id) => id.clone(),
        None => {
            return Ok(error_response(
                StatusCode::UNAUTHORIZED,
                "invalid_api_key",
                "Invalid API key",
            ))
        }
    };

    let result = {
        let tenant = match mgr.get_tenant_mut(&tenant_id) {
            Some(t) => t,
            None => {
                return Ok(error_response(
                    StatusCode::NOT_FOUND,
                    "tenant_not_found",
                    "Tenant not found",
                ))
            }
        };
        tenant.remove_pipeline(&pipeline_id)
    };

    match result {
        Ok(()) => {
            mgr.persist_if_needed(&tenant_id);
            Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({"deleted": true})),
                StatusCode::OK,
            )
            .into_response())
        }
        Err(e) => Ok(tenant_error_response(e)),
    }
}

async fn handle_inject(
    pipeline_id: String,
    api_key: String,
    body: InjectEventRequest,
    manager: SharedTenantManager,
) -> Result<impl Reply, Infallible> {
    let mut mgr = manager.write().await;

    let tenant_id = match mgr.get_tenant_by_api_key(&api_key) {
        Some(id) => id.clone(),
        None => {
            return Ok(error_response(
                StatusCode::UNAUTHORIZED,
                "invalid_api_key",
                "Invalid API key",
            ))
        }
    };

    let tenant = match mgr.get_tenant_mut(&tenant_id) {
        Some(t) => t,
        None => {
            return Ok(error_response(
                StatusCode::NOT_FOUND,
                "tenant_not_found",
                "Tenant not found",
            ))
        }
    };

    let mut event = Event::new(body.event_type.clone());
    for (key, value) in &body.fields {
        let v = json_to_runtime_value(value);
        event = event.with_field(key.as_str(), v);
    }

    match tenant.process_event(&pipeline_id, event).await {
        Ok(output_events) => {
            let events_json: Vec<serde_json::Value> = output_events
                .iter()
                .map(|e| {
                    let mut map = serde_json::Map::new();
                    map.insert(
                        "event_type".into(),
                        serde_json::Value::String(e.event_type.to_string()),
                    );
                    for (k, v) in &e.data {
                        map.insert(k.to_string(), crate::websocket::value_to_json(v));
                    }
                    serde_json::Value::Object(map)
                })
                .collect();
            let response = serde_json::json!({
                "accepted": true,
                "output_events": events_json,
            });
            Ok(
                warp::reply::with_status(warp::reply::json(&response), StatusCode::OK)
                    .into_response(),
            )
        }
        Err(e) => Ok(tenant_error_response(e)),
    }
}

async fn handle_metrics(
    pipeline_id: String,
    api_key: String,
    manager: SharedTenantManager,
) -> Result<impl Reply, Infallible> {
    let mgr = manager.read().await;

    let tenant_id = match mgr.get_tenant_by_api_key(&api_key) {
        Some(id) => id.clone(),
        None => {
            return Ok(error_response(
                StatusCode::UNAUTHORIZED,
                "invalid_api_key",
                "Invalid API key",
            ))
        }
    };

    let tenant = match mgr.get_tenant(&tenant_id) {
        Some(t) => t,
        None => {
            return Ok(error_response(
                StatusCode::NOT_FOUND,
                "tenant_not_found",
                "Tenant not found",
            ))
        }
    };

    if !tenant.pipelines.contains_key(&pipeline_id) {
        return Ok(error_response(
            StatusCode::NOT_FOUND,
            "pipeline_not_found",
            "Pipeline not found",
        ));
    }

    let resp = PipelineMetricsResponse {
        pipeline_id,
        events_processed: tenant.usage.events_processed,
        output_events_emitted: tenant.usage.output_events_emitted,
    };
    Ok(warp::reply::with_status(warp::reply::json(&resp), StatusCode::OK).into_response())
}

async fn handle_reload(
    pipeline_id: String,
    api_key: String,
    body: ReloadPipelineRequest,
    manager: SharedTenantManager,
) -> Result<impl Reply, Infallible> {
    let mut mgr = manager.write().await;

    let tenant_id = match mgr.get_tenant_by_api_key(&api_key) {
        Some(id) => id.clone(),
        None => {
            return Ok(error_response(
                StatusCode::UNAUTHORIZED,
                "invalid_api_key",
                "Invalid API key",
            ))
        }
    };

    let result = {
        let tenant = match mgr.get_tenant_mut(&tenant_id) {
            Some(t) => t,
            None => {
                return Ok(error_response(
                    StatusCode::NOT_FOUND,
                    "tenant_not_found",
                    "Tenant not found",
                ))
            }
        };
        tenant.reload_pipeline(&pipeline_id, body.source)
    };

    match result {
        Ok(()) => {
            mgr.persist_if_needed(&tenant_id);
            Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({"reloaded": true})),
                StatusCode::OK,
            )
            .into_response())
        }
        Err(e) => Ok(tenant_error_response(e)),
    }
}

async fn handle_usage(
    api_key: String,
    manager: SharedTenantManager,
) -> Result<impl Reply, Infallible> {
    let mgr = manager.read().await;

    let tenant_id = match mgr.get_tenant_by_api_key(&api_key) {
        Some(id) => id.clone(),
        None => {
            return Ok(error_response(
                StatusCode::UNAUTHORIZED,
                "invalid_api_key",
                "Invalid API key",
            ))
        }
    };

    let tenant = match mgr.get_tenant(&tenant_id) {
        Some(t) => t,
        None => {
            return Ok(error_response(
                StatusCode::NOT_FOUND,
                "tenant_not_found",
                "Tenant not found",
            ))
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
    Ok(warp::reply::with_status(warp::reply::json(&resp), StatusCode::OK).into_response())
}

// =============================================================================
// Tenant Admin Routes
// =============================================================================

fn with_admin_key() -> impl Filter<Extract = (String,), Error = Rejection> + Clone {
    warp::header::<String>("x-admin-key")
}

fn with_admin_key_config(
    admin_key: Option<String>,
) -> impl Filter<Extract = (Option<String>,), Error = Infallible> + Clone {
    warp::any().map(move || admin_key.clone())
}

/// Build tenant admin route tree (CRUD for tenants)
pub fn tenant_admin_routes(
    manager: SharedTenantManager,
    admin_key: Option<String>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let api = warp::path("api")
        .and(warp::path("v1"))
        .and(warp::path("tenants"));

    let create = api
        .and(warp::path::end())
        .and(warp::post())
        .and(with_admin_key())
        .and(warp::body::json())
        .and(with_manager(manager.clone()))
        .and(with_admin_key_config(admin_key.clone()))
        .and_then(handle_create_tenant);

    let list_tenants = api
        .and(warp::path::end())
        .and(warp::get())
        .and(with_admin_key())
        .and(with_manager(manager.clone()))
        .and(with_admin_key_config(admin_key.clone()))
        .and_then(handle_list_tenants);

    let get_tenant = api
        .and(warp::path::param::<String>())
        .and(warp::path::end())
        .and(warp::get())
        .and(with_admin_key())
        .and(with_manager(manager.clone()))
        .and(with_admin_key_config(admin_key.clone()))
        .and_then(handle_get_tenant);

    let delete_tenant = api
        .and(warp::path::param::<String>())
        .and(warp::path::end())
        .and(warp::delete())
        .and(with_admin_key())
        .and(with_manager(manager.clone()))
        .and(with_admin_key_config(admin_key))
        .and_then(handle_delete_tenant);

    create.or(list_tenants).or(get_tenant).or(delete_tenant)
}

#[allow(clippy::result_large_err)]
fn validate_admin_key(
    provided: &str,
    configured: &Option<String>,
) -> Result<(), warp::reply::Response> {
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
    admin_key: String,
    body: CreateTenantRequest,
    manager: SharedTenantManager,
    configured_key: Option<String>,
) -> Result<impl Reply, Infallible> {
    if let Err(resp) = validate_admin_key(&admin_key, &configured_key) {
        return Ok(resp);
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
            Ok(
                warp::reply::with_status(warp::reply::json(&resp), StatusCode::CREATED)
                    .into_response(),
            )
        }
        Err(e) => Ok(tenant_error_response(e)),
    }
}

async fn handle_list_tenants(
    admin_key: String,
    manager: SharedTenantManager,
    configured_key: Option<String>,
) -> Result<impl Reply, Infallible> {
    if let Err(resp) = validate_admin_key(&admin_key, &configured_key) {
        return Ok(resp);
    }

    let mgr = manager.read().await;
    let tenants: Vec<TenantResponse> = mgr
        .list_tenants()
        .iter()
        .map(|t| TenantResponse {
            id: t.id.as_str().to_string(),
            name: t.name.clone(),
            api_key: t.api_key.clone(),
            quota: QuotaInfo {
                max_pipelines: t.quota.max_pipelines,
                max_events_per_second: t.quota.max_events_per_second,
                max_streams_per_pipeline: t.quota.max_streams_per_pipeline,
            },
        })
        .collect();
    let total = tenants.len();
    let resp = TenantListResponse { tenants, total };
    Ok(warp::reply::with_status(warp::reply::json(&resp), StatusCode::OK).into_response())
}

async fn handle_get_tenant(
    tenant_id_str: String,
    admin_key: String,
    manager: SharedTenantManager,
    configured_key: Option<String>,
) -> Result<impl Reply, Infallible> {
    if let Err(resp) = validate_admin_key(&admin_key, &configured_key) {
        return Ok(resp);
    }

    let mgr = manager.read().await;
    let tenant_id = varpulis_runtime::TenantId::new(&tenant_id_str);
    match mgr.get_tenant(&tenant_id) {
        Some(t) => {
            let resp = TenantDetailResponse {
                id: t.id.as_str().to_string(),
                name: t.name.clone(),
                api_key: t.api_key.clone(),
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
            Ok(warp::reply::with_status(warp::reply::json(&resp), StatusCode::OK).into_response())
        }
        None => Ok(error_response(
            StatusCode::NOT_FOUND,
            "tenant_not_found",
            "Tenant not found",
        )),
    }
}

async fn handle_delete_tenant(
    tenant_id_str: String,
    admin_key: String,
    manager: SharedTenantManager,
    configured_key: Option<String>,
) -> Result<impl Reply, Infallible> {
    if let Err(resp) = validate_admin_key(&admin_key, &configured_key) {
        return Ok(resp);
    }

    let mut mgr = manager.write().await;
    let tenant_id = varpulis_runtime::TenantId::new(&tenant_id_str);
    match mgr.remove_tenant(&tenant_id) {
        Ok(()) => Ok(warp::reply::with_status(
            warp::reply::json(&serde_json::json!({"deleted": true})),
            StatusCode::OK,
        )
        .into_response()),
        Err(e) => Ok(tenant_error_response(e)),
    }
}

// =============================================================================
// Helpers
// =============================================================================

fn error_response(status: StatusCode, code: &str, message: &str) -> warp::reply::Response {
    let body = ApiError {
        error: message.to_string(),
        code: code.to_string(),
    };
    warp::reply::with_status(warp::reply::json(&body), status).into_response()
}

fn tenant_error_response(err: TenantError) -> warp::reply::Response {
    let (status, code) = match &err {
        TenantError::NotFound(_) => (StatusCode::NOT_FOUND, "not_found"),
        TenantError::PipelineNotFound(_) => (StatusCode::NOT_FOUND, "pipeline_not_found"),
        TenantError::QuotaExceeded(_) => (StatusCode::TOO_MANY_REQUESTS, "quota_exceeded"),
        TenantError::RateLimitExceeded => (StatusCode::TOO_MANY_REQUESTS, "rate_limited"),
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
    use super::*;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use varpulis_runtime::tenant::{TenantManager, TenantQuota};

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
        let routes = api_routes(mgr, None);

        let resp = warp::test::request()
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
        let routes = api_routes(mgr, None);

        let resp = warp::test::request()
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
        let routes = api_routes(mgr, None);

        let resp = warp::test::request()
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
        let routes = api_routes(mgr, None);

        let resp = warp::test::request()
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
        let routes = api_routes(mgr, None);

        let resp = warp::test::request()
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

        let routes = api_routes(mgr, None);

        let resp = warp::test::request()
            .method("POST")
            .path(&format!("/api/v1/pipelines/{}/events", pipeline_id))
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

        let resp = tenant_error_response(TenantError::ParseError("bad".into()));
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // =========================================================================
    // Tenant Admin API tests
    // =========================================================================

    fn setup_admin_routes(
        admin_key: Option<&str>,
    ) -> (
        SharedTenantManager,
        impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone,
    ) {
        let mgr = Arc::new(RwLock::new(TenantManager::new()));
        let key = admin_key.map(|k| k.to_string());
        let routes = api_routes(mgr.clone(), key);
        (mgr, routes)
    }

    #[tokio::test]
    async fn test_create_tenant() {
        let (_mgr, routes) = setup_admin_routes(Some("admin-secret"));

        let resp = warp::test::request()
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
            warp::test::request()
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

        let resp = warp::test::request()
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
        let create_resp = warp::test::request()
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

        let resp = warp::test::request()
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
        let create_resp = warp::test::request()
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

        let resp = warp::test::request()
            .method("DELETE")
            .path(&format!("/api/v1/tenants/{}", created.id))
            .header("x-admin-key", "admin-secret")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);

        // Verify tenant is gone
        let list_resp = warp::test::request()
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

        let resp = warp::test::request()
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

        let resp = warp::test::request()
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
        let resp = warp::test::request()
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
        assert_eq!(body.quota.max_pipelines, 2); // free tier

        // Enterprise tier
        let resp = warp::test::request()
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
}
