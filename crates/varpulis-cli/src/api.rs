//! REST API for SaaS pipeline management
//!
//! Provides RESTful endpoints for deploying and managing CEP pipelines
//! in a multi-tenant environment.

use crate::auth::constant_time_compare;
use futures_util::stream;
use indexmap::IndexMap;
use rustc_hash::FxBuildHasher;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use varpulis_runtime::tenant::{SharedTenantManager, TenantError, TenantQuota};
use varpulis_runtime::Event;
use warp::http::StatusCode;
use warp::{Filter, Rejection, Reply};

use varpulis_core::pagination::{PaginationMeta, PaginationParams, MAX_LIMIT};
use varpulis_core::security::{JSON_BODY_LIMIT, LARGE_BODY_LIMIT};

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
        .and(warp::body::content_length_limit(JSON_BODY_LIMIT))
        .and(warp::body::json())
        .and(with_manager(manager.clone()))
        .and_then(handle_deploy);

    let list = api
        .and(warp::path("pipelines"))
        .and(warp::path::end())
        .and(warp::get())
        .and(with_api_key())
        .and(warp::query::<PaginationParams>())
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
        .and(warp::body::content_length_limit(JSON_BODY_LIMIT))
        .and(warp::body::json())
        .and(with_manager(manager.clone()))
        .and_then(handle_inject);

    let inject_batch = api
        .and(warp::path("pipelines"))
        .and(warp::path::param::<String>())
        .and(warp::path("events-batch"))
        .and(warp::path::end())
        .and(warp::post())
        .and(with_api_key())
        .and(warp::body::content_length_limit(LARGE_BODY_LIMIT))
        .and(warp::body::json())
        .and(with_manager(manager.clone()))
        .and_then(handle_inject_batch);

    let checkpoint = api
        .and(warp::path("pipelines"))
        .and(warp::path::param::<String>())
        .and(warp::path("checkpoint"))
        .and(warp::path::end())
        .and(warp::post())
        .and(with_api_key())
        .and(with_manager(manager.clone()))
        .and_then(handle_checkpoint);

    let restore = api
        .and(warp::path("pipelines"))
        .and(warp::path::param::<String>())
        .and(warp::path("restore"))
        .and(warp::path::end())
        .and(warp::post())
        .and(with_api_key())
        .and(warp::body::content_length_limit(LARGE_BODY_LIMIT))
        .and(warp::body::json())
        .and(with_manager(manager.clone()))
        .and_then(handle_restore);

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
        .and(warp::body::content_length_limit(JSON_BODY_LIMIT))
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

    let logs = api
        .and(warp::path("pipelines"))
        .and(warp::path::param::<String>())
        .and(warp::path("logs"))
        .and(warp::path::end())
        .and(warp::get())
        .and(with_api_key())
        .and(with_manager(manager.clone()))
        .and_then(handle_logs);

    // SECURITY: allow_any_origin() is acceptable here because production
    // deployments sit behind nginx which enforces origin restrictions.
    // Per-tenant API keys provide the actual access-control boundary.
    let cors = warp::cors()
        .allow_any_origin()
        .allow_methods(vec!["GET", "POST", "DELETE", "OPTIONS"])
        .allow_headers(vec!["content-type", "x-api-key", "authorization"]);

    deploy
        .or(list)
        .or(get_pipeline)
        .or(delete)
        .or(inject)
        .or(inject_batch)
        .or(checkpoint)
        .or(restore)
        .or(metrics)
        .or(reload)
        .or(usage)
        .or(logs)
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

    let result = mgr
        .deploy_pipeline_on_tenant(&tenant_id, body.name.clone(), body.source)
        .await;

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
    pagination: PaginationParams,
    manager: SharedTenantManager,
) -> Result<impl Reply, Infallible> {
    if pagination.exceeds_max() {
        return Ok(error_response(
            StatusCode::BAD_REQUEST,
            "invalid_limit",
            &format!("limit must not exceed {MAX_LIMIT}"),
        ));
    }

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

    let all_pipelines: Vec<PipelineInfo> = tenant
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

    let (pipelines, meta) = pagination.paginate(all_pipelines);
    let total = meta.total;
    let resp = PipelineListResponse {
        pipelines,
        total,
        pagination: Some(meta),
    };
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
            Ok(
                warp::reply::with_status(warp::reply::json(&response), StatusCode::OK)
                    .into_response(),
            )
        }
        Err(e) => Ok(tenant_error_response(e)),
    }
}

async fn handle_inject_batch(
    pipeline_id: String,
    api_key: String,
    body: InjectBatchRequest,
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

    let start = std::time::Instant::now();
    let mut accepted = 0usize;
    let mut output_events = Vec::new();

    for req in body.events {
        let mut event = Event::new(req.event_type.clone());
        for (key, value) in &req.fields {
            let v = json_to_runtime_value(value);
            event = event.with_field(key.as_str(), v);
        }

        match tenant.process_event(&pipeline_id, event).await {
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
            Err(_) => {
                // Skip failed events silently in batch mode
            }
        }
    }

    let processing_time_us = start.elapsed().as_micros() as u64;

    let resp = InjectBatchResponse {
        accepted,
        output_events,
        processing_time_us,
    };
    Ok(warp::reply::with_status(warp::reply::json(&resp), StatusCode::OK).into_response())
}

async fn handle_checkpoint(
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

    match tenant.checkpoint_pipeline(&pipeline_id).await {
        Ok(checkpoint) => {
            let resp = CheckpointResponse {
                pipeline_id,
                events_processed: checkpoint.events_processed,
                checkpoint,
            };
            Ok(warp::reply::with_status(warp::reply::json(&resp), StatusCode::OK).into_response())
        }
        Err(e) => Ok(tenant_error_response(e)),
    }
}

async fn handle_restore(
    pipeline_id: String,
    api_key: String,
    body: RestoreRequest,
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
            Ok(warp::reply::with_status(warp::reply::json(&resp), StatusCode::OK).into_response())
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
        tenant.reload_pipeline(&pipeline_id, body.source).await
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

/// Handle SSE log streaming for a pipeline
async fn handle_logs(
    pipeline_id: String,
    api_key: String,
    manager: SharedTenantManager,
) -> Result<warp::reply::Response, Rejection> {
    let mgr = manager.read().await;

    let tenant_id = match mgr.get_tenant_by_api_key(&api_key) {
        Some(id) => id.clone(),
        None => {
            return Ok(error_response(
                StatusCode::UNAUTHORIZED,
                "invalid_key",
                "Invalid API key",
            ))
        }
    };

    // Verify tenant owns this pipeline
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

    let rx: tokio::sync::broadcast::Receiver<Event> =
        match tenant.subscribe_pipeline_logs(&pipeline_id) {
            Ok(rx) => rx,
            Err(_) => {
                return Ok(error_response(
                    StatusCode::NOT_FOUND,
                    "pipeline_not_found",
                    &format!("Pipeline {} not found", pipeline_id),
                ))
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
                let sse = warp::sse::Event::default().data(json);
                Some((Ok::<_, Infallible>(sse), rx))
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                let msg = format!("{{\"warning\":\"skipped {} events\"}}", n);
                let sse = warp::sse::Event::default().event("warning").data(msg);
                Some((Ok(sse), rx))
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => None,
        }
    });

    Ok(warp::sse::reply(warp::sse::keep_alive().stream(stream)).into_response())
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
        .and(warp::body::content_length_limit(JSON_BODY_LIMIT))
        .and(warp::body::json())
        .and(with_manager(manager.clone()))
        .and(with_admin_key_config(admin_key.clone()))
        .and_then(handle_create_tenant);

    let list_tenants = api
        .and(warp::path::end())
        .and(warp::get())
        .and(with_admin_key())
        .and(warp::query::<PaginationParams>())
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
    pagination: PaginationParams,
    manager: SharedTenantManager,
    configured_key: Option<String>,
) -> Result<impl Reply, Infallible> {
    if let Err(resp) = validate_admin_key(&admin_key, &configured_key) {
        return Ok(resp);
    }

    if pagination.exceeds_max() {
        return Ok(error_response(
            StatusCode::BAD_REQUEST,
            "invalid_limit",
            &format!("limit must not exceed {MAX_LIMIT}"),
        ));
    }

    let mgr = manager.read().await;
    let all_tenants: Vec<TenantResponse> = mgr
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
    let (tenants, meta) = pagination.paginate(all_tenants);
    let total = meta.total;
    let resp = TenantListResponse {
        tenants,
        total,
        pagination: Some(meta),
    };
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
        let routes = api_routes(mgr, None);

        let resp = warp::test::request()
            .method("GET")
            .path(&format!("/api/v1/pipelines/{}", pipeline_id))
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
        let routes = api_routes(mgr, None);

        let resp = warp::test::request()
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
        let routes = api_routes(mgr.clone(), None);

        let resp = warp::test::request()
            .method("DELETE")
            .path(&format!("/api/v1/pipelines/{}", pipeline_id))
            .header("x-api-key", "test-key-123")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body["deleted"], true);

        // Verify it's gone
        let list_resp = warp::test::request()
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
        let routes = api_routes(mgr, None);

        let resp = warp::test::request()
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
        let routes = api_routes(mgr, None);

        let resp = warp::test::request()
            .method("POST")
            .path(&format!("/api/v1/pipelines/{}/events-batch", pipeline_id))
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
        let routes = api_routes(mgr, None);

        // Batch mode silently skips failed events (including nonexistent pipeline)
        let resp = warp::test::request()
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
        let routes = api_routes(mgr, None);

        let resp = warp::test::request()
            .method("POST")
            .path(&format!("/api/v1/pipelines/{}/checkpoint", pipeline_id))
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
        let routes = api_routes(mgr, None);

        let resp = warp::test::request()
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
        let routes = api_routes(mgr, None);

        // First checkpoint
        let cp_resp = warp::test::request()
            .method("POST")
            .path(&format!("/api/v1/pipelines/{}/checkpoint", pipeline_id))
            .header("x-api-key", "test-key-123")
            .reply(&routes)
            .await;
        let cp: CheckpointResponse = serde_json::from_slice(cp_resp.body()).unwrap();

        // Then restore
        let resp = warp::test::request()
            .method("POST")
            .path(&format!("/api/v1/pipelines/{}/restore", pipeline_id))
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
        let routes = api_routes(mgr, None);

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
        };

        let resp = warp::test::request()
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
        let routes = api_routes(mgr, None);

        let resp = warp::test::request()
            .method("GET")
            .path(&format!("/api/v1/pipelines/{}/metrics", pipeline_id))
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
        let routes = api_routes(mgr, None);

        let resp = warp::test::request()
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
        let routes = api_routes(mgr, None);

        let resp = warp::test::request()
            .method("POST")
            .path(&format!("/api/v1/pipelines/{}/reload", pipeline_id))
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
        let routes = api_routes(mgr, None);

        let resp = warp::test::request()
            .method("POST")
            .path(&format!("/api/v1/pipelines/{}/reload", pipeline_id))
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
        let routes = api_routes(mgr, None);

        let resp = warp::test::request()
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
        let routes = api_routes(mgr, None);

        let resp = warp::test::request()
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
        let routes = api_routes(mgr, None);

        let resp = warp::test::request()
            .method("GET")
            .path(&format!("/api/v1/pipelines/{}/logs", pipeline_id))
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

        let resp = tenant_error_response(TenantError::EngineError("boom".into()));
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let resp = tenant_error_response(TenantError::AlreadyExists("t1".into()));
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    // =========================================================================
    // Pagination tests
    // =========================================================================

    #[tokio::test]
    async fn test_list_pipelines_default_pagination() {
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

        let routes = api_routes(mgr, None);

        // First page: limit=1, offset=0
        let resp = warp::test::request()
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
        let resp = warp::test::request()
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
        let routes = api_routes(mgr, None);

        let resp = warp::test::request()
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

        // Page through with limit=2
        let resp = warp::test::request()
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
        let resp = warp::test::request()
            .method("GET")
            .path("/api/v1/tenants?limit=2&offset=2")
            .header("x-admin-key", "admin-secret")
            .reply(&routes)
            .await;

        let body: TenantListResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body.tenants.len(), 1);
        assert!(!body.pagination.unwrap().has_more);
    }
}
