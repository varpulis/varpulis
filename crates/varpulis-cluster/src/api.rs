//! Coordinator REST API routes (warp-based).

use crate::coordinator::{Coordinator, InjectEventRequest};
use crate::pipeline_group::{PipelineGroupInfo, PipelineGroupSpec};
use crate::routing::{GroupTopology, PipelineTopologyEntry, RouteTopologyEntry, TopologyInfo};
use crate::worker::{
    HeartbeatRequest, HeartbeatResponse, RegisterWorkerRequest, RegisterWorkerResponse, WorkerId,
    WorkerInfo, WorkerNode,
};
use crate::ClusterError;
use serde::Serialize;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::RwLock;
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

    let topology = api
        .and(warp::path("topology"))
        .and(warp::path::end())
        .and(warp::get())
        .and(with_optional_auth(admin_key))
        .and(with_coordinator(coordinator.clone()))
        .and_then(handle_topology);

    let cors = warp::cors()
        .allow_any_origin()
        .allow_methods(vec!["GET", "POST", "DELETE", "OPTIONS"])
        .allow_headers(vec!["content-type", "x-api-key", "authorization"]);

    register_worker
        .or(heartbeat)
        .or(list_workers)
        .or(get_worker)
        .or(delete_worker)
        .or(deploy_group)
        .or(list_groups)
        .or(get_group)
        .or(delete_group)
        .or(inject_event)
        .or(topology)
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
    };
    let body = ApiError {
        error: err.to_string(),
        code: code.to_string(),
    };
    warp::reply::with_status(warp::reply::json(&body), status).into_response()
}

/// Handle warp rejections (auth failures, etc.).
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
    } else {
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
}
