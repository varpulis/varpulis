//! Coordinator REST API routes (warp-based).

use crate::connector_config::{self, ClusterConnector};
use crate::coordinator::{Coordinator, InjectBatchRequest, InjectEventRequest};
use crate::migration::MigrationReason;
use crate::pipeline_group::{PipelineGroupInfo, PipelineGroupSpec};
use crate::routing::{GroupTopology, PipelineTopologyEntry, RouteTopologyEntry, TopologyInfo};
use crate::worker::{
    HeartbeatRequest, HeartbeatResponse, RegisterWorkerRequest, RegisterWorkerResponse, WorkerId,
    WorkerInfo, WorkerNode,
};
use crate::ClusterError;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::RwLock;
use varpulis_parser::ParseError;
use warp::http::StatusCode;
use warp::{Filter, Rejection, Reply};

/// Shared coordinator state.
pub type SharedCoordinator = Arc<RwLock<Coordinator>>;

/// Create a new shared coordinator.
pub fn shared_coordinator() -> SharedCoordinator {
    Arc::new(RwLock::new(Coordinator::new()))
}

/// Build all coordinator API routes under `/api/v1/cluster/`.
pub fn cluster_routes(
    coordinator: SharedCoordinator,
    admin_key: Option<String>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let api = warp::path("api")
        .and(warp::path("v1"))
        .and(warp::path("cluster"));

    let register_worker = api
        .and(warp::path("workers"))
        .and(warp::path("register"))
        .and(warp::path::end())
        .and(warp::post())
        .and(with_optional_auth(admin_key.clone()))
        .and(warp::body::json())
        .and(with_coordinator(coordinator.clone()))
        .and_then(handle_register_worker);

    let heartbeat = api
        .and(warp::path("workers"))
        .and(warp::path::param::<String>())
        .and(warp::path("heartbeat"))
        .and(warp::path::end())
        .and(warp::post())
        .and(warp::body::json())
        .and(with_coordinator(coordinator.clone()))
        .and_then(handle_heartbeat);

    let list_workers = api
        .and(warp::path("workers"))
        .and(warp::path::end())
        .and(warp::get())
        .and(with_optional_auth(admin_key.clone()))
        .and(with_coordinator(coordinator.clone()))
        .and_then(handle_list_workers);

    let get_worker = api
        .and(warp::path("workers"))
        .and(warp::path::param::<String>())
        .and(warp::path::end())
        .and(warp::get())
        .and(with_optional_auth(admin_key.clone()))
        .and(with_coordinator(coordinator.clone()))
        .and_then(handle_get_worker);

    let delete_worker = api
        .and(warp::path("workers"))
        .and(warp::path::param::<String>())
        .and(warp::path::end())
        .and(warp::delete())
        .and(with_optional_auth(admin_key.clone()))
        .and(with_coordinator(coordinator.clone()))
        .and_then(handle_delete_worker);

    let deploy_group = api
        .and(warp::path("pipeline-groups"))
        .and(warp::path::end())
        .and(warp::post())
        .and(with_optional_auth(admin_key.clone()))
        .and(warp::body::json())
        .and(with_coordinator(coordinator.clone()))
        .and_then(handle_deploy_group);

    let list_groups = api
        .and(warp::path("pipeline-groups"))
        .and(warp::path::end())
        .and(warp::get())
        .and(with_optional_auth(admin_key.clone()))
        .and(with_coordinator(coordinator.clone()))
        .and_then(handle_list_groups);

    let get_group = api
        .and(warp::path("pipeline-groups"))
        .and(warp::path::param::<String>())
        .and(warp::path::end())
        .and(warp::get())
        .and(with_optional_auth(admin_key.clone()))
        .and(with_coordinator(coordinator.clone()))
        .and_then(handle_get_group);

    let delete_group = api
        .and(warp::path("pipeline-groups"))
        .and(warp::path::param::<String>())
        .and(warp::path::end())
        .and(warp::delete())
        .and(with_optional_auth(admin_key.clone()))
        .and(with_coordinator(coordinator.clone()))
        .and_then(handle_delete_group);

    let inject_event = api
        .and(warp::path("pipeline-groups"))
        .and(warp::path::param::<String>())
        .and(warp::path("inject"))
        .and(warp::path::end())
        .and(warp::post())
        .and(with_optional_auth(admin_key.clone()))
        .and(warp::body::json())
        .and(with_coordinator(coordinator.clone()))
        .and_then(handle_inject_event);

    let inject_batch = api
        .and(warp::path("pipeline-groups"))
        .and(warp::path::param::<String>())
        .and(warp::path("inject-batch"))
        .and(warp::path::end())
        .and(warp::post())
        .and(with_optional_auth(admin_key.clone()))
        .and(warp::body::json())
        .and(with_coordinator(coordinator.clone()))
        .and_then(handle_inject_batch);

    let topology = api
        .and(warp::path("topology"))
        .and(warp::path::end())
        .and(warp::get())
        .and(with_optional_auth(admin_key.clone()))
        .and(with_coordinator(coordinator.clone()))
        .and_then(handle_topology);

    let validate = api
        .and(warp::path("validate"))
        .and(warp::path::end())
        .and(warp::post())
        .and(with_optional_auth(admin_key.clone()))
        .and(warp::body::json())
        .and(with_coordinator(coordinator.clone()))
        .and_then(handle_validate);

    // --- Migration / Drain / Rebalance endpoints ---

    let drain_worker = api
        .and(warp::path("workers"))
        .and(warp::path::param::<String>())
        .and(warp::path("drain"))
        .and(warp::path::end())
        .and(warp::post())
        .and(with_optional_auth(admin_key.clone()))
        .and(warp::body::json())
        .and(with_coordinator(coordinator.clone()))
        .and_then(handle_drain_worker);

    let rebalance = api
        .and(warp::path("rebalance"))
        .and(warp::path::end())
        .and(warp::post())
        .and(with_optional_auth(admin_key.clone()))
        .and(with_coordinator(coordinator.clone()))
        .and_then(handle_rebalance);

    let list_migrations = api
        .and(warp::path("migrations"))
        .and(warp::path::end())
        .and(warp::get())
        .and(with_optional_auth(admin_key.clone()))
        .and(with_coordinator(coordinator.clone()))
        .and_then(handle_list_migrations);

    let get_migration = api
        .and(warp::path("migrations"))
        .and(warp::path::param::<String>())
        .and(warp::path::end())
        .and(warp::get())
        .and(with_optional_auth(admin_key.clone()))
        .and(with_coordinator(coordinator.clone()))
        .and_then(handle_get_migration);

    let manual_migrate = api
        .and(warp::path("pipelines"))
        .and(warp::path::param::<String>())
        .and(warp::path::param::<String>())
        .and(warp::path("migrate"))
        .and(warp::path::end())
        .and(warp::post())
        .and(with_optional_auth(admin_key.clone()))
        .and(warp::body::json())
        .and(with_coordinator(coordinator.clone()))
        .and_then(handle_manual_migrate);

    // --- Connector CRUD endpoints ---

    let list_connectors = api
        .and(warp::path("connectors"))
        .and(warp::path::end())
        .and(warp::get())
        .and(with_optional_auth(admin_key.clone()))
        .and(with_coordinator(coordinator.clone()))
        .and_then(handle_list_connectors);

    let get_connector = api
        .and(warp::path("connectors"))
        .and(warp::path::param::<String>())
        .and(warp::path::end())
        .and(warp::get())
        .and(with_optional_auth(admin_key.clone()))
        .and(with_coordinator(coordinator.clone()))
        .and_then(handle_get_connector);

    let create_connector = api
        .and(warp::path("connectors"))
        .and(warp::path::end())
        .and(warp::post())
        .and(with_optional_auth(admin_key.clone()))
        .and(warp::body::json())
        .and(with_coordinator(coordinator.clone()))
        .and_then(handle_create_connector);

    let update_connector = api
        .and(warp::path("connectors"))
        .and(warp::path::param::<String>())
        .and(warp::path::end())
        .and(warp::put())
        .and(with_optional_auth(admin_key.clone()))
        .and(warp::body::json())
        .and(with_coordinator(coordinator.clone()))
        .and_then(handle_update_connector);

    let delete_connector = api
        .and(warp::path("connectors"))
        .and(warp::path::param::<String>())
        .and(warp::path::end())
        .and(warp::delete())
        .and(with_optional_auth(admin_key.clone()))
        .and(with_coordinator(coordinator.clone()))
        .and_then(handle_delete_connector);

    // --- Metrics endpoint ---

    let metrics = api
        .and(warp::path("metrics"))
        .and(warp::path::end())
        .and(warp::get())
        .and(with_optional_auth(admin_key))
        .and(with_coordinator(coordinator.clone()))
        .and_then(handle_metrics);

    let cors = warp::cors()
        .allow_any_origin()
        .allow_methods(vec!["GET", "POST", "PUT", "DELETE", "OPTIONS"])
        .allow_headers(vec!["content-type", "x-api-key", "authorization"]);

    // Group routes to avoid warp recursive type overflow
    let worker_routes = register_worker
        .or(heartbeat)
        .or(list_workers)
        .or(get_worker)
        .or(delete_worker)
        .or(drain_worker)
        .boxed();

    let pipeline_routes = deploy_group
        .or(list_groups)
        .or(get_group)
        .or(delete_group)
        .or(inject_event)
        .or(inject_batch)
        .boxed();

    let migration_routes = rebalance
        .or(list_migrations)
        .or(get_migration)
        .or(manual_migrate)
        .boxed();

    let connector_routes = list_connectors
        .or(get_connector)
        .or(create_connector)
        .or(update_connector)
        .or(delete_connector)
        .boxed();

    worker_routes
        .or(pipeline_routes)
        .or(topology)
        .or(validate)
        .or(migration_routes)
        .or(connector_routes)
        .or(metrics)
        .with(cors)
}

// =============================================================================
// Filters
// =============================================================================

fn with_coordinator(
    coordinator: SharedCoordinator,
) -> impl Filter<Extract = (SharedCoordinator,), Error = Infallible> + Clone {
    warp::any().map(move || coordinator.clone())
}

fn with_optional_auth(
    admin_key: Option<String>,
) -> impl Filter<Extract = ((),), Error = Rejection> + Clone {
    let key = admin_key.clone();
    warp::any()
        .and(warp::header::optional::<String>("x-api-key"))
        .and_then(move |provided: Option<String>| {
            let key = key.clone();
            async move {
                match &key {
                    None => Ok::<(), Rejection>(()), // no auth required
                    Some(expected) => match provided {
                        Some(ref p) if p == expected => Ok(()),
                        _ => Err(warp::reject::custom(Unauthorized)),
                    },
                }
            }
        })
}

#[derive(Debug)]
struct Unauthorized;
impl warp::reject::Reject for Unauthorized {}

// =============================================================================
// Handlers
// =============================================================================

async fn handle_register_worker(
    _auth: (),
    body: RegisterWorkerRequest,
    coordinator: SharedCoordinator,
) -> Result<impl Reply, Infallible> {
    let mut coord = coordinator.write().await;
    let node = WorkerNode {
        id: WorkerId(body.worker_id.clone()),
        address: body.address,
        api_key: body.api_key,
        status: crate::worker::WorkerStatus::Registering,
        capacity: body.capacity,
        last_heartbeat: std::time::Instant::now(),
        assigned_pipelines: Vec::new(),
    };
    let id = coord.register_worker(node);
    let resp = RegisterWorkerResponse {
        worker_id: id.0,
        status: "registered".into(),
    };
    Ok(warp::reply::with_status(warp::reply::json(&resp), StatusCode::CREATED).into_response())
}

async fn handle_heartbeat(
    worker_id: String,
    body: HeartbeatRequest,
    coordinator: SharedCoordinator,
) -> Result<impl Reply, Infallible> {
    let mut coord = coordinator.write().await;
    match coord.heartbeat(&WorkerId(worker_id), &body) {
        Ok(()) => Ok(warp::reply::with_status(
            warp::reply::json(&HeartbeatResponse { acknowledged: true }),
            StatusCode::OK,
        )
        .into_response()),
        Err(e) => Ok(error_response(StatusCode::NOT_FOUND, &e.to_string())),
    }
}

async fn handle_list_workers(
    _auth: (),
    coordinator: SharedCoordinator,
) -> Result<impl Reply, Infallible> {
    let coord = coordinator.read().await;
    let workers: Vec<WorkerInfo> = coord.workers.values().map(WorkerInfo::from).collect();
    let resp = serde_json::json!({
        "workers": workers,
        "total": workers.len(),
    });
    Ok(warp::reply::with_status(warp::reply::json(&resp), StatusCode::OK).into_response())
}

async fn handle_get_worker(
    worker_id: String,
    _auth: (),
    coordinator: SharedCoordinator,
) -> Result<impl Reply, Infallible> {
    let coord = coordinator.read().await;
    match coord.workers.get(&WorkerId(worker_id)) {
        Some(node) => {
            let info = WorkerInfo::from(node);
            Ok(warp::reply::with_status(warp::reply::json(&info), StatusCode::OK).into_response())
        }
        None => Ok(error_response(StatusCode::NOT_FOUND, "Worker not found")),
    }
}

async fn handle_delete_worker(
    worker_id: String,
    _auth: (),
    coordinator: SharedCoordinator,
) -> Result<impl Reply, Infallible> {
    let mut coord = coordinator.write().await;
    match coord.deregister_worker(&WorkerId(worker_id)) {
        Ok(()) => Ok(warp::reply::with_status(
            warp::reply::json(&serde_json::json!({"deleted": true})),
            StatusCode::OK,
        )
        .into_response()),
        Err(e) => Ok(error_response(StatusCode::NOT_FOUND, &e.to_string())),
    }
}

async fn handle_deploy_group(
    _auth: (),
    body: PipelineGroupSpec,
    coordinator: SharedCoordinator,
) -> Result<impl Reply, Infallible> {
    let mut coord = coordinator.write().await;
    match coord.deploy_group(body).await {
        Ok(group_id) => {
            let group = coord.pipeline_groups.get(&group_id).unwrap();
            let info = PipelineGroupInfo::from(group);
            Ok(
                warp::reply::with_status(warp::reply::json(&info), StatusCode::CREATED)
                    .into_response(),
            )
        }
        Err(e) => Ok(cluster_error_response(e)),
    }
}

async fn handle_list_groups(
    _auth: (),
    coordinator: SharedCoordinator,
) -> Result<impl Reply, Infallible> {
    let coord = coordinator.read().await;
    let groups: Vec<PipelineGroupInfo> = coord
        .pipeline_groups
        .values()
        .map(PipelineGroupInfo::from)
        .collect();
    let resp = serde_json::json!({
        "pipeline_groups": groups,
        "total": groups.len(),
    });
    Ok(warp::reply::with_status(warp::reply::json(&resp), StatusCode::OK).into_response())
}

async fn handle_get_group(
    group_id: String,
    _auth: (),
    coordinator: SharedCoordinator,
) -> Result<impl Reply, Infallible> {
    let coord = coordinator.read().await;
    match coord.pipeline_groups.get(&group_id) {
        Some(group) => {
            let info = PipelineGroupInfo::from(group);
            Ok(warp::reply::with_status(warp::reply::json(&info), StatusCode::OK).into_response())
        }
        None => Ok(error_response(
            StatusCode::NOT_FOUND,
            "Pipeline group not found",
        )),
    }
}

async fn handle_delete_group(
    group_id: String,
    _auth: (),
    coordinator: SharedCoordinator,
) -> Result<impl Reply, Infallible> {
    let mut coord = coordinator.write().await;
    match coord.teardown_group(&group_id).await {
        Ok(()) => Ok(warp::reply::with_status(
            warp::reply::json(&serde_json::json!({"torn_down": true})),
            StatusCode::OK,
        )
        .into_response()),
        Err(e) => Ok(cluster_error_response(e)),
    }
}

async fn handle_inject_event(
    group_id: String,
    _auth: (),
    body: InjectEventRequest,
    coordinator: SharedCoordinator,
) -> Result<impl Reply, Infallible> {
    let coord = coordinator.read().await;
    match coord.inject_event(&group_id, body).await {
        Ok(resp) => {
            Ok(warp::reply::with_status(warp::reply::json(&resp), StatusCode::OK).into_response())
        }
        Err(e) => Ok(cluster_error_response(e)),
    }
}

async fn handle_inject_batch(
    group_id: String,
    _auth: (),
    body: InjectBatchRequest,
    coordinator: SharedCoordinator,
) -> Result<impl Reply, Infallible> {
    let coord = coordinator.read().await;
    match coord.inject_batch(&group_id, body).await {
        Ok(resp) => {
            Ok(warp::reply::with_status(warp::reply::json(&resp), StatusCode::OK).into_response())
        }
        Err(e) => Ok(cluster_error_response(e)),
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
#[derive(Debug, Deserialize)]
struct DrainRequest {
    #[allow(dead_code)]
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
#[derive(Debug, Deserialize)]
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
    _auth: (),
    body: ValidateRequest,
    coordinator: SharedCoordinator,
) -> Result<impl Reply, Infallible> {
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
            Ok(warp::reply::with_status(warp::reply::json(&resp), StatusCode::OK).into_response())
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
            Ok(warp::reply::with_status(warp::reply::json(&resp), StatusCode::OK).into_response())
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

async fn handle_topology(
    _auth: (),
    coordinator: SharedCoordinator,
) -> Result<impl Reply, Infallible> {
    let coord = coordinator.read().await;
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
            let routes = g
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
                routes,
            }
        })
        .collect();

    let topology = TopologyInfo { groups };
    Ok(warp::reply::with_status(warp::reply::json(&topology), StatusCode::OK).into_response())
}

// =============================================================================
// Migration / Drain / Rebalance handlers
// =============================================================================

async fn handle_drain_worker(
    worker_id: String,
    _auth: (),
    _body: DrainRequest,
    coordinator: SharedCoordinator,
) -> Result<impl Reply, Infallible> {
    let mut coord = coordinator.write().await;
    match coord.drain_worker(&WorkerId(worker_id.clone())).await {
        Ok(migration_ids) => {
            let resp = DrainResponse {
                worker_id,
                pipelines_migrated: migration_ids.len(),
                status: "drained".into(),
            };
            Ok(warp::reply::with_status(warp::reply::json(&resp), StatusCode::OK).into_response())
        }
        Err(e) => Ok(cluster_error_response(e)),
    }
}

async fn handle_rebalance(
    _auth: (),
    coordinator: SharedCoordinator,
) -> Result<impl Reply, Infallible> {
    let mut coord = coordinator.write().await;
    match coord.rebalance().await {
        Ok(migration_ids) => {
            let resp = RebalanceResponse {
                migrations_started: migration_ids.len(),
                migration_ids,
            };
            Ok(warp::reply::with_status(warp::reply::json(&resp), StatusCode::OK).into_response())
        }
        Err(e) => Ok(cluster_error_response(e)),
    }
}

async fn handle_list_migrations(
    _auth: (),
    coordinator: SharedCoordinator,
) -> Result<impl Reply, Infallible> {
    let coord = coordinator.read().await;
    let migrations: Vec<MigrationInfo> = coord
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
    let resp = serde_json::json!({
        "migrations": migrations,
        "total": migrations.len(),
    });
    Ok(warp::reply::with_status(warp::reply::json(&resp), StatusCode::OK).into_response())
}

async fn handle_get_migration(
    migration_id: String,
    _auth: (),
    coordinator: SharedCoordinator,
) -> Result<impl Reply, Infallible> {
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
            Ok(warp::reply::with_status(warp::reply::json(&info), StatusCode::OK).into_response())
        }
        None => Ok(error_response(StatusCode::NOT_FOUND, "Migration not found")),
    }
}

async fn handle_manual_migrate(
    group_id: String,
    pipeline_name: String,
    _auth: (),
    body: ManualMigrateRequest,
    coordinator: SharedCoordinator,
) -> Result<impl Reply, Infallible> {
    let mut coord = coordinator.write().await;
    match coord
        .migrate_pipeline(
            &pipeline_name,
            &group_id,
            &WorkerId(body.target_worker_id),
            MigrationReason::Manual,
        )
        .await
    {
        Ok(migration_id) => {
            let resp = serde_json::json!({
                "migration_id": migration_id,
                "pipeline": pipeline_name,
                "group_id": group_id,
                "status": "started",
            });
            Ok(
                warp::reply::with_status(warp::reply::json(&resp), StatusCode::ACCEPTED)
                    .into_response(),
            )
        }
        Err(e) => Ok(cluster_error_response(e)),
    }
}

// =============================================================================
// Connector handlers
// =============================================================================

async fn handle_list_connectors(
    _auth: (),
    coordinator: SharedCoordinator,
) -> Result<impl Reply, Infallible> {
    let coord = coordinator.read().await;
    let connectors: Vec<&ClusterConnector> = coord.list_connectors();
    let resp = serde_json::json!({
        "connectors": connectors,
        "total": connectors.len(),
    });
    Ok(warp::reply::with_status(warp::reply::json(&resp), StatusCode::OK).into_response())
}

async fn handle_get_connector(
    name: String,
    _auth: (),
    coordinator: SharedCoordinator,
) -> Result<impl Reply, Infallible> {
    let coord = coordinator.read().await;
    match coord.get_connector(&name) {
        Ok(connector) => Ok(
            warp::reply::with_status(warp::reply::json(connector), StatusCode::OK).into_response(),
        ),
        Err(e) => Ok(cluster_error_response(e)),
    }
}

async fn handle_create_connector(
    _auth: (),
    body: ClusterConnector,
    coordinator: SharedCoordinator,
) -> Result<impl Reply, Infallible> {
    let mut coord = coordinator.write().await;
    match coord.create_connector(body) {
        Ok(connector) => Ok(warp::reply::with_status(
            warp::reply::json(connector),
            StatusCode::CREATED,
        )
        .into_response()),
        Err(e) => Ok(cluster_error_response(e)),
    }
}

async fn handle_update_connector(
    name: String,
    _auth: (),
    body: ClusterConnector,
    coordinator: SharedCoordinator,
) -> Result<impl Reply, Infallible> {
    let mut coord = coordinator.write().await;
    match coord.update_connector(&name, body) {
        Ok(connector) => Ok(
            warp::reply::with_status(warp::reply::json(connector), StatusCode::OK).into_response(),
        ),
        Err(e) => Ok(cluster_error_response(e)),
    }
}

async fn handle_delete_connector(
    name: String,
    _auth: (),
    coordinator: SharedCoordinator,
) -> Result<impl Reply, Infallible> {
    let mut coord = coordinator.write().await;
    match coord.delete_connector(&name) {
        Ok(()) => Ok(warp::reply::with_status(
            warp::reply::json(&serde_json::json!({"deleted": true})),
            StatusCode::OK,
        )
        .into_response()),
        Err(e) => Ok(cluster_error_response(e)),
    }
}

// =============================================================================
// Metrics handler
// =============================================================================

async fn handle_metrics(
    _auth: (),
    coordinator: SharedCoordinator,
) -> Result<impl Reply, Infallible> {
    let coord = coordinator.read().await;
    let metrics = coord.get_cluster_metrics();
    Ok(warp::reply::with_status(warp::reply::json(&metrics), StatusCode::OK).into_response())
}

// =============================================================================
// Error handling
// =============================================================================

#[derive(Debug, Serialize)]
struct ApiError {
    error: String,
    code: String,
}

fn error_response(status: StatusCode, message: &str) -> warp::reply::Response {
    let body = ApiError {
        error: message.to_string(),
        code: status.as_str().to_string(),
    };
    warp::reply::with_status(warp::reply::json(&body), status).into_response()
}

fn cluster_error_response(err: ClusterError) -> warp::reply::Response {
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
    };
    let body = ApiError {
        error: err.to_string(),
        code: code.to_string(),
    };
    warp::reply::with_status(warp::reply::json(&body), status).into_response()
}

/// Handle warp rejections with specific HTTP status codes and messages.
pub async fn handle_rejection(err: Rejection) -> Result<impl Reply, Infallible> {
    if err.find::<Unauthorized>().is_some() {
        Ok(error_response(
            StatusCode::UNAUTHORIZED,
            "Invalid or missing API key",
        ))
    } else if err.find::<warp::reject::MissingHeader>().is_some() {
        Ok(error_response(
            StatusCode::UNAUTHORIZED,
            "Missing API key header",
        ))
    } else if let Some(e) = err.find::<warp::filters::body::BodyDeserializeError>() {
        Ok(error_response(
            StatusCode::BAD_REQUEST,
            &format!("Invalid request body: {}", e),
        ))
    } else if err.find::<warp::reject::InvalidQuery>().is_some() {
        Ok(error_response(
            StatusCode::BAD_REQUEST,
            "Invalid query parameters",
        ))
    } else if err.find::<warp::reject::PayloadTooLarge>().is_some() {
        Ok(error_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "Request payload too large",
        ))
    } else if err.find::<warp::reject::UnsupportedMediaType>().is_some() {
        Ok(error_response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Unsupported media type",
        ))
    } else if err.find::<warp::reject::MethodNotAllowed>().is_some() {
        Ok(error_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "Method not allowed",
        ))
    } else if err.is_not_found() {
        Ok(error_response(StatusCode::NOT_FOUND, "Not found"))
    } else {
        tracing::error!("Unhandled rejection: {:?}", err);
        Ok(error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::WorkerCapacity;

    fn setup_routes() -> (
        SharedCoordinator,
        impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone,
    ) {
        let coord = shared_coordinator();
        let routes = cluster_routes(coord.clone(), Some("admin-key".to_string()));
        (coord, routes)
    }

    #[tokio::test]
    async fn test_register_worker() {
        let (_coord, routes) = setup_routes();

        let resp = warp::test::request()
            .method("POST")
            .path("/api/v1/cluster/workers/register")
            .header("x-api-key", "admin-key")
            .json(&RegisterWorkerRequest {
                worker_id: "w1".into(),
                address: "http://localhost:9000".into(),
                api_key: "worker-key".into(),
                capacity: WorkerCapacity::default(),
            })
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::CREATED);
        let body: RegisterWorkerResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body.worker_id, "w1");
    }

    #[tokio::test]
    async fn test_list_workers() {
        let (coord, routes) = setup_routes();

        // Register a worker directly
        {
            let mut c = coord.write().await;
            c.register_worker(WorkerNode::new(
                WorkerId("w1".into()),
                "http://localhost:9000".into(),
                "key".into(),
            ));
        }

        let resp = warp::test::request()
            .method("GET")
            .path("/api/v1/cluster/workers")
            .header("x-api-key", "admin-key")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body["total"], 1);
    }

    #[tokio::test]
    async fn test_heartbeat() {
        let (coord, routes) = setup_routes();

        // Register worker
        {
            let mut c = coord.write().await;
            c.register_worker(WorkerNode::new(
                WorkerId("w1".into()),
                "http://localhost:9000".into(),
                "key".into(),
            ));
        }

        let resp = warp::test::request()
            .method("POST")
            .path("/api/v1/cluster/workers/w1/heartbeat")
            .json(&HeartbeatRequest {
                events_processed: 42,
                pipelines_running: 1,
                pipeline_metrics: vec![],
            })
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let body: HeartbeatResponse = serde_json::from_slice(resp.body()).unwrap();
        assert!(body.acknowledged);
    }

    #[tokio::test]
    async fn test_unauthorized_without_key() {
        let (_coord, routes) = setup_routes();

        let resp = warp::test::request()
            .method("GET")
            .path("/api/v1/cluster/workers")
            // no x-api-key header
            .reply(&routes)
            .await;

        // Should reject
        assert_ne!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_topology() {
        let (_coord, routes) = setup_routes();

        let resp = warp::test::request()
            .method("GET")
            .path("/api/v1/cluster/topology")
            .header("x-api-key", "admin-key")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let body: TopologyInfo = serde_json::from_slice(resp.body()).unwrap();
        assert!(body.groups.is_empty());
    }

    #[tokio::test]
    async fn test_delete_worker() {
        let (coord, routes) = setup_routes();

        {
            let mut c = coord.write().await;
            c.register_worker(WorkerNode::new(
                WorkerId("w1".into()),
                "http://localhost:9000".into(),
                "key".into(),
            ));
        }

        let resp = warp::test::request()
            .method("DELETE")
            .path("/api/v1/cluster/workers/w1")
            .header("x-api-key", "admin-key")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);

        // Verify worker is gone
        let coord = coord.read().await;
        assert!(coord.workers.is_empty());
    }

    #[tokio::test]
    async fn test_list_pipeline_groups_empty() {
        let (_coord, routes) = setup_routes();

        let resp = warp::test::request()
            .method("GET")
            .path("/api/v1/cluster/pipeline-groups")
            .header("x-api-key", "admin-key")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body["total"], 0);
    }

    #[tokio::test]
    async fn test_get_worker_found() {
        let (coord, routes) = setup_routes();

        {
            let mut c = coord.write().await;
            c.register_worker(WorkerNode::new(
                WorkerId("w1".into()),
                "http://localhost:9000".into(),
                "key".into(),
            ));
        }

        let resp = warp::test::request()
            .method("GET")
            .path("/api/v1/cluster/workers/w1")
            .header("x-api-key", "admin-key")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let body: WorkerInfo = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body.id, "w1");
        assert_eq!(body.address, "http://localhost:9000");
        assert_eq!(body.status, "ready");
    }

    #[tokio::test]
    async fn test_get_worker_not_found() {
        let (_coord, routes) = setup_routes();

        let resp = warp::test::request()
            .method("GET")
            .path("/api/v1/cluster/workers/nonexistent")
            .header("x-api-key", "admin-key")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_worker_not_found() {
        let (_coord, routes) = setup_routes();

        let resp = warp::test::request()
            .method("DELETE")
            .path("/api/v1/cluster/workers/nonexistent")
            .header("x-api-key", "admin-key")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_heartbeat_unknown_worker() {
        let (_coord, routes) = setup_routes();

        let resp = warp::test::request()
            .method("POST")
            .path("/api/v1/cluster/workers/nonexistent/heartbeat")
            .json(&HeartbeatRequest {
                events_processed: 0,
                pipelines_running: 0,
                pipeline_metrics: vec![],
            })
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_unauthorized_wrong_key() {
        let (_coord, routes) = setup_routes();

        let resp = warp::test::request()
            .method("GET")
            .path("/api/v1/cluster/workers")
            .header("x-api-key", "wrong-key")
            .reply(&routes)
            .await;

        assert_ne!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_no_auth_mode() {
        // When admin_key is None, no auth required
        let coord = shared_coordinator();
        let routes = cluster_routes(coord.clone(), None);

        // Register without API key
        let resp = warp::test::request()
            .method("POST")
            .path("/api/v1/cluster/workers/register")
            .json(&RegisterWorkerRequest {
                worker_id: "w1".into(),
                address: "http://localhost:9000".into(),
                api_key: "key".into(),
                capacity: WorkerCapacity::default(),
            })
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::CREATED);

        // List without API key
        let resp = warp::test::request()
            .method("GET")
            .path("/api/v1/cluster/workers")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body["total"], 1);
    }

    #[tokio::test]
    async fn test_get_group_not_found() {
        let (_coord, routes) = setup_routes();

        let resp = warp::test::request()
            .method("GET")
            .path("/api/v1/cluster/pipeline-groups/nonexistent")
            .header("x-api-key", "admin-key")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body["code"], "404");
    }

    #[tokio::test]
    async fn test_delete_group_not_found() {
        let (_coord, routes) = setup_routes();

        let resp = warp::test::request()
            .method("DELETE")
            .path("/api/v1/cluster/pipeline-groups/nonexistent")
            .header("x-api-key", "admin-key")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body["code"], "group_not_found");
    }

    #[tokio::test]
    async fn test_inject_event_group_not_found() {
        let (_coord, routes) = setup_routes();

        let resp = warp::test::request()
            .method("POST")
            .path("/api/v1/cluster/pipeline-groups/nonexistent/inject")
            .header("x-api-key", "admin-key")
            .json(&serde_json::json!({
                "event_type": "TestEvent",
                "fields": {}
            }))
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body["code"], "group_not_found");
    }

    #[tokio::test]
    async fn test_deploy_group_no_workers() {
        let (_coord, routes) = setup_routes();

        let resp = warp::test::request()
            .method("POST")
            .path("/api/v1/cluster/pipeline-groups")
            .header("x-api-key", "admin-key")
            .json(&serde_json::json!({
                "name": "test-group",
                "pipelines": [
                    {"name": "p1", "source": "stream A = X"}
                ]
            }))
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body["code"], "no_workers_available");
    }

    #[tokio::test]
    async fn test_register_multiple_workers_list() {
        let (coord, routes) = setup_routes();

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

        let resp = warp::test::request()
            .method("GET")
            .path("/api/v1/cluster/workers")
            .header("x-api-key", "admin-key")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body["total"], 3);
        assert_eq!(body["workers"].as_array().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn test_topology_with_groups() {
        let (coord, routes) = setup_routes();

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
                    mqtt_topic: None,
                }],
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
                },
            );
            c.pipeline_groups.insert("g1".into(), group);
        }

        let resp = warp::test::request()
            .method("GET")
            .path("/api/v1/cluster/topology")
            .header("x-api-key", "admin-key")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        let groups = body["groups"].as_array().unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0]["group_name"], "test-group");
        assert_eq!(groups[0]["pipelines"].as_array().unwrap().len(), 1);
        assert_eq!(groups[0]["routes"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_get_group_found() {
        let (coord, routes) = setup_routes();

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
            };

            let group = DeployedPipelineGroup::new("g1".into(), "my-group".into(), spec);
            c.pipeline_groups.insert("g1".into(), group);
        }

        let resp = warp::test::request()
            .method("GET")
            .path("/api/v1/cluster/pipeline-groups/g1")
            .header("x-api-key", "admin-key")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body["name"], "my-group");
        assert_eq!(body["id"], "g1");
    }

    #[tokio::test]
    async fn test_list_groups_with_entries() {
        let (coord, routes) = setup_routes();

        {
            let mut c = coord.write().await;
            use crate::pipeline_group::*;
            for i in 0..3 {
                let spec = PipelineGroupSpec {
                    name: format!("group-{}", i),
                    pipelines: vec![],
                    routes: vec![],
                };
                let group =
                    DeployedPipelineGroup::new(format!("g{}", i), format!("group-{}", i), spec);
                c.pipeline_groups.insert(format!("g{}", i), group);
            }
        }

        let resp = warp::test::request()
            .method("GET")
            .path("/api/v1/cluster/pipeline-groups")
            .header("x-api-key", "admin-key")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body["total"], 3);
    }

    #[tokio::test]
    async fn test_register_worker_via_api_then_get() {
        let (_coord, routes) = setup_routes();

        // Register
        let resp = warp::test::request()
            .method("POST")
            .path("/api/v1/cluster/workers/register")
            .header("x-api-key", "admin-key")
            .json(&RegisterWorkerRequest {
                worker_id: "api-worker".into(),
                address: "http://localhost:8000".into(),
                api_key: "worker-secret".into(),
                capacity: WorkerCapacity {
                    cpu_cores: 4,
                    pipelines_running: 0,
                    max_pipelines: 50,
                },
            })
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::CREATED);

        // Get the worker
        let resp = warp::test::request()
            .method("GET")
            .path("/api/v1/cluster/workers/api-worker")
            .header("x-api-key", "admin-key")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let body: WorkerInfo = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body.id, "api-worker");
        assert_eq!(body.address, "http://localhost:8000");
        assert_eq!(body.max_pipelines, 50);
    }

    #[tokio::test]
    async fn test_register_delete_register_cycle() {
        let (_coord, routes) = setup_routes();

        // Register
        let resp = warp::test::request()
            .method("POST")
            .path("/api/v1/cluster/workers/register")
            .header("x-api-key", "admin-key")
            .json(&RegisterWorkerRequest {
                worker_id: "w1".into(),
                address: "http://localhost:9000".into(),
                api_key: "key".into(),
                capacity: WorkerCapacity::default(),
            })
            .reply(&routes)
            .await;
        assert_eq!(resp.status(), StatusCode::CREATED);

        // Delete
        let resp = warp::test::request()
            .method("DELETE")
            .path("/api/v1/cluster/workers/w1")
            .header("x-api-key", "admin-key")
            .reply(&routes)
            .await;
        assert_eq!(resp.status(), StatusCode::OK);

        // Verify gone
        let resp = warp::test::request()
            .method("GET")
            .path("/api/v1/cluster/workers/w1")
            .header("x-api-key", "admin-key")
            .reply(&routes)
            .await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        // Re-register
        let resp = warp::test::request()
            .method("POST")
            .path("/api/v1/cluster/workers/register")
            .header("x-api-key", "admin-key")
            .json(&RegisterWorkerRequest {
                worker_id: "w1".into(),
                address: "http://localhost:9001".into(),
                api_key: "new-key".into(),
                capacity: WorkerCapacity::default(),
            })
            .reply(&routes)
            .await;
        assert_eq!(resp.status(), StatusCode::CREATED);

        // Verify re-registered with new address
        let resp = warp::test::request()
            .method("GET")
            .path("/api/v1/cluster/workers/w1")
            .header("x-api-key", "admin-key")
            .reply(&routes)
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: WorkerInfo = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body.address, "http://localhost:9001");
    }

    #[tokio::test]
    async fn test_heartbeat_then_get_worker_updates() {
        let (coord, routes) = setup_routes();

        {
            let mut c = coord.write().await;
            c.register_worker(WorkerNode::new(
                WorkerId("w1".into()),
                "http://localhost:9000".into(),
                "key".into(),
            ));
        }

        // Send heartbeat with updated pipeline count
        let resp = warp::test::request()
            .method("POST")
            .path("/api/v1/cluster/workers/w1/heartbeat")
            .json(&HeartbeatRequest {
                events_processed: 500,
                pipelines_running: 3,
                pipeline_metrics: vec![],
            })
            .reply(&routes)
            .await;
        assert_eq!(resp.status(), StatusCode::OK);

        // Get worker should reflect new pipeline count
        let resp = warp::test::request()
            .method("GET")
            .path("/api/v1/cluster/workers/w1")
            .header("x-api-key", "admin-key")
            .reply(&routes)
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: WorkerInfo = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(body.pipelines_running, 3);
    }
}
