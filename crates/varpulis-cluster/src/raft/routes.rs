//! Warp routes for Raft inter-coordinator RPCs and management.

use std::collections::BTreeMap;
use std::sync::Arc;

use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use warp::Filter;

use super::{NodeId, RaftNode, TypeConfig, VarpulisRaft};

/// Maximum body size for normal JSON endpoints (1 MB).
const JSON_BODY_LIMIT: u64 = 1024 * 1024;
/// Maximum body size for large payloads: append entries, snapshots (16 MB).
const LARGE_BODY_LIMIT: u64 = 16 * 1024 * 1024;

/// Shared Raft handle type.
pub type SharedRaft = Arc<VarpulisRaft>;

/// Build all `/raft/*` warp routes.
///
/// When `admin_key` is Some, all mutating Raft endpoints require the
/// `x-api-key` header. The `/raft/metrics` endpoint stays unauthenticated
/// (read-only, useful for monitoring).
pub fn raft_routes(
    raft: SharedRaft,
    admin_key: Option<String>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    let raft_prefix = warp::path("raft");

    let vote = raft_prefix
        .and(warp::path("vote"))
        .and(warp::path::end())
        .and(warp::post())
        .and(with_optional_raft_auth(admin_key.clone()))
        .and(warp::body::content_length_limit(JSON_BODY_LIMIT))
        .and(warp::body::json())
        .and(with_raft(raft.clone()))
        .and_then(handle_vote);

    let append = raft_prefix
        .and(warp::path("append"))
        .and(warp::path::end())
        .and(warp::post())
        .and(with_optional_raft_auth(admin_key.clone()))
        .and(warp::body::content_length_limit(LARGE_BODY_LIMIT))
        .and(warp::body::json())
        .and(with_raft(raft.clone()))
        .and_then(handle_append_entries);

    let snapshot = raft_prefix
        .and(warp::path("snapshot"))
        .and(warp::path::end())
        .and(warp::post())
        .and(with_optional_raft_auth(admin_key.clone()))
        .and(warp::body::content_length_limit(LARGE_BODY_LIMIT))
        .and(warp::body::json())
        .and(with_raft(raft.clone()))
        .and_then(handle_snapshot);

    let init = raft_prefix
        .and(warp::path("init"))
        .and(warp::path::end())
        .and(warp::post())
        .and(with_optional_raft_auth(admin_key.clone()))
        .and(warp::body::content_length_limit(JSON_BODY_LIMIT))
        .and(warp::body::json())
        .and(with_raft(raft.clone()))
        .and_then(handle_init);

    let add_learner = raft_prefix
        .and(warp::path("add-learner"))
        .and(warp::path::end())
        .and(warp::post())
        .and(with_optional_raft_auth(admin_key.clone()))
        .and(warp::body::content_length_limit(JSON_BODY_LIMIT))
        .and(warp::body::json())
        .and(with_raft(raft.clone()))
        .and_then(handle_add_learner);

    let change_membership = raft_prefix
        .and(warp::path("change-membership"))
        .and(warp::path::end())
        .and(warp::post())
        .and(with_optional_raft_auth(admin_key))
        .and(warp::body::content_length_limit(JSON_BODY_LIMIT))
        .and(warp::body::json())
        .and(with_raft(raft.clone()))
        .and_then(handle_change_membership);

    // Metrics stays unauthenticated (read-only, useful for monitoring)
    let metrics = raft_prefix
        .and(warp::path("metrics"))
        .and(warp::path::end())
        .and(warp::get())
        .and(with_raft(raft))
        .and_then(handle_metrics);

    vote.or(append)
        .or(snapshot)
        .or(init)
        .or(add_learner)
        .or(change_membership)
        .or(metrics)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn with_raft(
    raft: SharedRaft,
) -> impl Filter<Extract = (SharedRaft,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || raft.clone())
}

/// Optional authentication filter for Raft RPC endpoints.
/// Reuses the same `x-api-key` header and `Unauthorized` rejection as the
/// cluster API routes.
fn with_optional_raft_auth(
    admin_key: Option<String>,
) -> impl Filter<Extract = ((),), Error = warp::Rejection> + Clone {
    let key = admin_key.clone();
    warp::any()
        .and(warp::header::optional::<String>("x-api-key"))
        .and_then(move |provided: Option<String>| {
            let key = key.clone();
            async move {
                match &key {
                    None => Ok::<(), warp::Rejection>(()),
                    Some(expected) => match provided {
                        Some(ref p) if p == expected => Ok(()),
                        _ => Err(warp::reject::custom(RaftUnauthorized)),
                    },
                }
            }
        })
}

#[derive(Debug)]
struct RaftUnauthorized;
impl warp::reject::Reject for RaftUnauthorized {}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn handle_vote(
    _auth: (),
    req: VoteRequest<NodeId>,
    raft: SharedRaft,
) -> Result<impl warp::Reply, warp::Rejection> {
    match raft.vote(req).await {
        Ok(resp) => Ok(warp::reply::with_status(
            warp::reply::json(&resp),
            warp::http::StatusCode::OK,
        )),
        Err(e) => Ok(warp::reply::with_status(
            warp::reply::json(&serde_json::json!({"error": e.to_string()})),
            warp::http::StatusCode::INTERNAL_SERVER_ERROR,
        )),
    }
}

async fn handle_append_entries(
    _auth: (),
    req: AppendEntriesRequest<TypeConfig>,
    raft: SharedRaft,
) -> Result<impl warp::Reply, warp::Rejection> {
    match raft.append_entries(req).await {
        Ok(resp) => Ok(warp::reply::with_status(
            warp::reply::json(&resp),
            warp::http::StatusCode::OK,
        )),
        Err(e) => Ok(warp::reply::with_status(
            warp::reply::json(&serde_json::json!({"error": e.to_string()})),
            warp::http::StatusCode::INTERNAL_SERVER_ERROR,
        )),
    }
}

async fn handle_snapshot(
    _auth: (),
    req: InstallSnapshotRequest<TypeConfig>,
    raft: SharedRaft,
) -> Result<impl warp::Reply, warp::Rejection> {
    match raft.install_snapshot(req).await {
        Ok(resp) => Ok(warp::reply::with_status(
            warp::reply::json(&resp),
            warp::http::StatusCode::OK,
        )),
        Err(e) => Ok(warp::reply::with_status(
            warp::reply::json(&serde_json::json!({"error": e.to_string()})),
            warp::http::StatusCode::INTERNAL_SERVER_ERROR,
        )),
    }
}

/// Initialize request: map of node_id -> address.
#[derive(serde::Deserialize)]
struct InitRequest {
    members: BTreeMap<NodeId, String>,
}

async fn handle_init(
    _auth: (),
    req: InitRequest,
    raft: SharedRaft,
) -> Result<impl warp::Reply, warp::Rejection> {
    let members: BTreeMap<NodeId, RaftNode> = req
        .members
        .into_iter()
        .map(|(id, addr)| (id, RaftNode { addr }))
        .collect();

    let resp = raft.initialize(members).await;
    match resp {
        Ok(_) => Ok(warp::reply::json(&serde_json::json!({"status": "ok"}))),
        Err(e) => Ok(warp::reply::json(
            &serde_json::json!({"status": "error", "message": e.to_string()}),
        )),
    }
}

/// Add learner request.
#[derive(serde::Deserialize)]
struct AddLearnerRequest {
    node_id: NodeId,
    addr: String,
}

async fn handle_add_learner(
    _auth: (),
    req: AddLearnerRequest,
    raft: SharedRaft,
) -> Result<impl warp::Reply, warp::Rejection> {
    let node = RaftNode { addr: req.addr };
    let resp = raft.add_learner(req.node_id, node, true).await;
    match resp {
        Ok(r) => Ok(warp::reply::json(
            &serde_json::json!({"status": "ok", "response": format!("{:?}", r)}),
        )),
        Err(e) => Ok(warp::reply::json(
            &serde_json::json!({"status": "error", "message": e.to_string()}),
        )),
    }
}

/// Change membership request: list of voter node IDs.
#[derive(serde::Deserialize)]
struct ChangeMembershipRequest {
    members: Vec<NodeId>,
}

async fn handle_change_membership(
    _auth: (),
    req: ChangeMembershipRequest,
    raft: SharedRaft,
) -> Result<impl warp::Reply, warp::Rejection> {
    let members: std::collections::BTreeSet<NodeId> = req.members.into_iter().collect();
    let resp = raft.change_membership(members, false).await;
    match resp {
        Ok(r) => Ok(warp::reply::json(
            &serde_json::json!({"status": "ok", "response": format!("{:?}", r)}),
        )),
        Err(e) => Ok(warp::reply::json(
            &serde_json::json!({"status": "error", "message": e.to_string()}),
        )),
    }
}

async fn handle_metrics(raft: SharedRaft) -> Result<impl warp::Reply, warp::Rejection> {
    let metrics = raft.metrics().borrow().clone();
    Ok(warp::reply::json(&serde_json::json!({
        "id": metrics.id,
        "state": format!("{:?}", metrics.state),
        "current_term": metrics.current_term,
        "current_leader": metrics.current_leader,
        "last_applied": metrics.last_applied,
        "last_log_index": metrics.last_log_index,
        "membership": format!("{:?}", metrics.membership_config),
    })))
}
