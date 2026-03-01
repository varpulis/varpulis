//! Axum routes for Raft inter-coordinator RPCs and management.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};

use super::{NodeId, RaftNode, TypeConfig, VarpulisRaft};

use varpulis_core::security::{constant_time_compare, JSON_BODY_LIMIT, LARGE_BODY_LIMIT};

/// Shared Raft handle type.
pub type SharedRaft = Arc<VarpulisRaft>;

/// Shared state for raft routes (Raft handle + optional admin key).
#[derive(Clone)]
pub struct RaftState {
    pub raft: SharedRaft,
    pub admin_key: Option<String>,
}

impl std::fmt::Debug for RaftState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftState")
            .field("admin_key", &self.admin_key.as_deref().map(|_| "***"))
            .finish_non_exhaustive()
    }
}

/// Build all `/raft/*` axum routes.
///
/// When `admin_key` is Some, all mutating Raft endpoints require the
/// `x-api-key` header. The `/raft/metrics` endpoint stays unauthenticated
/// (read-only, useful for monitoring).
pub fn raft_routes(raft: SharedRaft, admin_key: Option<String>) -> Router {
    let state = RaftState { raft, admin_key };

    Router::new()
        .route(
            "/raft/vote",
            post(handle_vote).layer(tower_http::limit::RequestBodyLimitLayer::new(
                JSON_BODY_LIMIT as usize,
            )),
        )
        .route(
            "/raft/append",
            post(handle_append_entries).layer(tower_http::limit::RequestBodyLimitLayer::new(
                LARGE_BODY_LIMIT as usize,
            )),
        )
        .route(
            "/raft/snapshot",
            post(handle_snapshot).layer(tower_http::limit::RequestBodyLimitLayer::new(
                LARGE_BODY_LIMIT as usize,
            )),
        )
        .route(
            "/raft/init",
            post(handle_init).layer(tower_http::limit::RequestBodyLimitLayer::new(
                JSON_BODY_LIMIT as usize,
            )),
        )
        .route(
            "/raft/add-learner",
            post(handle_add_learner).layer(tower_http::limit::RequestBodyLimitLayer::new(
                JSON_BODY_LIMIT as usize,
            )),
        )
        .route(
            "/raft/change-membership",
            post(handle_change_membership).layer(tower_http::limit::RequestBodyLimitLayer::new(
                JSON_BODY_LIMIT as usize,
            )),
        )
        .route(
            "/raft/write",
            post(handle_write).layer(tower_http::limit::RequestBodyLimitLayer::new(
                JSON_BODY_LIMIT as usize,
            )),
        )
        .route("/raft/metrics", get(handle_metrics))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Auth middleware extractor
// ---------------------------------------------------------------------------

/// Extractor that validates the optional raft auth key.
/// Used on mutating endpoints. Metrics stays unauthenticated.
struct RaftAuth;

impl axum::extract::FromRequestParts<RaftState> for RaftAuth {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &RaftState,
    ) -> Result<Self, Self::Rejection> {
        match &state.admin_key {
            None => Ok(RaftAuth),
            Some(expected) => {
                let provided = parts
                    .headers
                    .get("x-api-key")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());
                match provided {
                    Some(ref p) if constant_time_compare(p, expected) => Ok(RaftAuth),
                    _ => Err((
                        StatusCode::UNAUTHORIZED,
                        Json(serde_json::json!({"error": "Unauthorized"})),
                    )
                        .into_response()),
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn handle_vote(
    State(state): State<RaftState>,
    _auth: RaftAuth,
    Json(req): Json<VoteRequest<NodeId>>,
) -> Response {
    match state.raft.vote(req).await {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn handle_append_entries(
    State(state): State<RaftState>,
    _auth: RaftAuth,
    Json(req): Json<AppendEntriesRequest<TypeConfig>>,
) -> Response {
    match state.raft.append_entries(req).await {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn handle_snapshot(
    State(state): State<RaftState>,
    _auth: RaftAuth,
    Json(req): Json<InstallSnapshotRequest<TypeConfig>>,
) -> Response {
    match state.raft.install_snapshot(req).await {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// Initialize request: map of node_id -> address.
#[derive(serde::Deserialize)]
struct InitRequest {
    members: BTreeMap<NodeId, String>,
}

async fn handle_init(
    State(state): State<RaftState>,
    _auth: RaftAuth,
    Json(req): Json<InitRequest>,
) -> Response {
    let members: BTreeMap<NodeId, RaftNode> = req
        .members
        .into_iter()
        .map(|(id, addr)| (id, RaftNode { addr }))
        .collect();

    match state.raft.initialize(members).await {
        Ok(()) => Json(serde_json::json!({"status": "ok"})).into_response(),
        Err(e) => {
            Json(serde_json::json!({"status": "error", "message": e.to_string()})).into_response()
        }
    }
}

/// Add learner request.
#[derive(serde::Deserialize)]
struct AddLearnerRequest {
    node_id: NodeId,
    addr: String,
}

async fn handle_add_learner(
    State(state): State<RaftState>,
    _auth: RaftAuth,
    Json(req): Json<AddLearnerRequest>,
) -> Response {
    let node = RaftNode { addr: req.addr };
    match state.raft.add_learner(req.node_id, node, true).await {
        Ok(r) => Json(serde_json::json!({"status": "ok", "response": format!("{:?}", r)}))
            .into_response(),
        Err(e) => {
            Json(serde_json::json!({"status": "error", "message": e.to_string()})).into_response()
        }
    }
}

/// Change membership request: list of voter node IDs.
#[derive(serde::Deserialize)]
struct ChangeMembershipRequest {
    members: Vec<NodeId>,
}

async fn handle_change_membership(
    State(state): State<RaftState>,
    _auth: RaftAuth,
    Json(req): Json<ChangeMembershipRequest>,
) -> Response {
    let members: std::collections::BTreeSet<NodeId> = req.members.into_iter().collect();
    match state.raft.change_membership(members, false).await {
        Ok(r) => Json(serde_json::json!({"status": "ok", "response": format!("{:?}", r)}))
            .into_response(),
        Err(e) => {
            Json(serde_json::json!({"status": "error", "message": e.to_string()})).into_response()
        }
    }
}

/// Internal write endpoint: accept a [`ClusterCommand`] and apply it through Raft.
/// Used by follower coordinators to forward heartbeat metrics to the leader.
async fn handle_write(
    State(state): State<RaftState>,
    _auth: RaftAuth,
    Json(cmd): Json<super::ClusterCommand>,
) -> Response {
    match state.raft.client_write(cmd).await {
        Ok(_) => (StatusCode::OK, Json(serde_json::json!({"status": "ok"}))).into_response(),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn handle_metrics(State(state): State<RaftState>) -> Response {
    let metrics = state.raft.metrics().borrow().clone();
    Json(serde_json::json!({
        "id": metrics.id,
        "state": format!("{:?}", metrics.state),
        "current_term": metrics.current_term,
        "current_leader": metrics.current_leader,
        "last_applied": metrics.last_applied,
        "last_log_index": metrics.last_log_index,
        "membership": format!("{:?}", metrics.membership_config),
    }))
    .into_response()
}
