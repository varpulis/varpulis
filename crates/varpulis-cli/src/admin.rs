//! Admin panel API endpoints for Varpulis Cloud (saas feature).
//!
//! All endpoints require JWT with `role: "admin"`.

use axum::extract::{Json, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::Router;
use serde::Deserialize;

use crate::oauth::{self, SharedOAuthState};

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct AdminState {
    pub db_pool: Option<varpulis_db::PgPool>,
    pub oauth_state: Option<SharedOAuthState>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract and verify JWT claims, then assert admin role.
async fn extract_admin_claims(
    auth_header: Option<&str>,
    oauth_state: &Option<SharedOAuthState>,
) -> Result<oauth::Claims, StatusCode> {
    let state = oauth_state
        .as_ref()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let token = auth_header
        .and_then(|h| h.strip_prefix("Bearer ").map(|t| t.trim().to_string()))
        .ok_or(StatusCode::UNAUTHORIZED)?;

    if token.is_empty() {
        return Err(StatusCode::UNAUTHORIZED);
    }

    // Check revocation
    let hash = {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        token.hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    };
    if state.sessions.read().await.is_revoked(&hash) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    use jsonwebtoken::{decode, DecodingKey, Validation};
    let token_data = decode::<oauth::Claims>(
        &token,
        &DecodingKey::from_secret(state.config.jwt_secret.as_bytes()),
        &Validation::default(),
    )
    .map_err(|_| StatusCode::UNAUTHORIZED)?;

    // Require admin role
    if token_data.claims.role != "admin" {
        return Err(StatusCode::FORBIDDEN);
    }

    Ok(token_data.claims)
}

#[allow(clippy::result_large_err)]
fn require_pool(state: &AdminState) -> Result<varpulis_db::PgPool, Response> {
    state.db_pool.clone().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "Database not configured"})),
        )
            .into_response()
    })
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

/// GET /api/v1/admin/tenants — list all organizations.
async fn handle_list_tenants(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());

    if let Err(status) = extract_admin_claims(auth_header, &state.oauth_state).await {
        return (status, Json(serde_json::json!({"error": "Unauthorized"}))).into_response();
    }

    let pool = match require_pool(&state) {
        Ok(p) => p,
        Err(r) => return r,
    };

    match varpulis_db::repo::list_all_organizations(&pool).await {
        Ok(orgs) => {
            let mut tenants = Vec::with_capacity(orgs.len());
            for o in &orgs {
                let usage = varpulis_db::repo::get_org_usage_summary(&pool, o.id)
                    .await
                    .unwrap_or(0);
                tenants.push(serde_json::json!({
                    "id": o.id.to_string(),
                    "name": o.name,
                    "tier": o.tier,
                    "status": o.status,
                    "trial_expires_at": o.trial_expires_at.map(|t| t.to_rfc3339()),
                    "pipeline_limit": o.pipeline_limit,
                    "events_per_second_limit": o.events_per_second_limit,
                    "monthly_event_limit": o.monthly_event_limit,
                    "events_this_month": usage,
                    "notes": o.notes,
                    "created_at": o.created_at.to_rfc3339(),
                    "updated_at": o.updated_at.to_rfc3339(),
                }));
            }
            (
                StatusCode::OK,
                Json(serde_json::json!({"tenants": tenants})),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!("Failed to list tenants: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "Internal error"})),
            )
                .into_response()
        }
    }
}

/// GET /api/v1/admin/tenants/{org_id} — detailed tenant info.
async fn handle_get_tenant(
    State(state): State<AdminState>,
    Path(org_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());

    if let Err(status) = extract_admin_claims(auth_header, &state.oauth_state).await {
        return (status, Json(serde_json::json!({"error": "Unauthorized"}))).into_response();
    }

    let pool = match require_pool(&state) {
        Ok(p) => p,
        Err(r) => return r,
    };

    let org_uuid: uuid::Uuid = match org_id.parse() {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "Invalid org_id"})),
            )
                .into_response();
        }
    };

    let org = match varpulis_db::repo::get_organization(&pool, org_uuid).await {
        Ok(Some(o)) => o,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "Tenant not found"})),
            )
                .into_response();
        }
        Err(e) => {
            tracing::error!("Failed to get tenant: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "Internal error"})),
            )
                .into_response();
        }
    };

    let usage = varpulis_db::repo::get_org_usage_summary(&pool, org_uuid)
        .await
        .unwrap_or(0);

    let pipelines = varpulis_db::repo::list_pipelines(&pool, org_uuid)
        .await
        .unwrap_or_default();

    let api_keys = varpulis_db::repo::list_api_keys(&pool, org_uuid)
        .await
        .unwrap_or_default();

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "id": org.id.to_string(),
            "name": org.name,
            "tier": org.tier,
            "status": org.status,
            "stripe_customer_id": org.stripe_customer_id,
            "trial_expires_at": org.trial_expires_at.map(|t| t.to_rfc3339()),
            "pipeline_limit": org.pipeline_limit,
            "events_per_second_limit": org.events_per_second_limit,
            "monthly_event_limit": org.monthly_event_limit,
            "events_this_month": usage,
            "notes": org.notes,
            "created_at": org.created_at.to_rfc3339(),
            "updated_at": org.updated_at.to_rfc3339(),
            "pipelines": pipelines.iter().map(|p| serde_json::json!({
                "id": p.id.to_string(),
                "name": p.name,
                "status": p.status,
                "created_at": p.created_at.to_rfc3339(),
            })).collect::<Vec<_>>(),
            "api_keys": api_keys.iter().map(|k| serde_json::json!({
                "id": k.id.to_string(),
                "name": k.name,
                "created_at": k.created_at.to_rfc3339(),
                "last_used_at": k.last_used_at.map(|t| t.to_rfc3339()),
            })).collect::<Vec<_>>(),
        })),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
struct ChangeTierRequest {
    tier: String,
}

/// PUT /api/v1/admin/tenants/{org_id}/tier — change tier.
async fn handle_change_tier(
    State(state): State<AdminState>,
    Path(org_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<ChangeTierRequest>,
) -> Response {
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());

    if let Err(status) = extract_admin_claims(auth_header, &state.oauth_state).await {
        return (status, Json(serde_json::json!({"error": "Unauthorized"}))).into_response();
    }

    let pool = match require_pool(&state) {
        Ok(p) => p,
        Err(r) => return r,
    };

    let org_uuid: uuid::Uuid = match org_id.parse() {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "Invalid org_id"})),
            )
                .into_response();
        }
    };

    // Validate tier
    let valid_tiers = ["free", "pro", "business", "enterprise"];
    if !valid_tiers.contains(&body.tier.as_str()) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "Invalid tier", "valid": valid_tiers})),
        )
            .into_response();
    }

    if let Err(e) = varpulis_db::repo::update_org_tier(&pool, org_uuid, &body.tier).await {
        tracing::error!("Failed to change tier: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": "Internal error"})),
        )
            .into_response();
    }

    tracing::info!("Admin changed org {} tier to {}", org_id, body.tier);
    (StatusCode::OK, Json(serde_json::json!({"ok": true}))).into_response()
}

#[derive(Debug, Deserialize)]
struct ChangeStatusRequest {
    status: String,
}

/// PUT /api/v1/admin/tenants/{org_id}/status — set status.
async fn handle_change_status(
    State(state): State<AdminState>,
    Path(org_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<ChangeStatusRequest>,
) -> Response {
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());

    if let Err(status) = extract_admin_claims(auth_header, &state.oauth_state).await {
        return (status, Json(serde_json::json!({"error": "Unauthorized"}))).into_response();
    }

    let pool = match require_pool(&state) {
        Ok(p) => p,
        Err(r) => return r,
    };

    let org_uuid: uuid::Uuid = match org_id.parse() {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "Invalid org_id"})),
            )
                .into_response();
        }
    };

    let valid_statuses = ["active", "trial", "suspended", "revoked"];
    if !valid_statuses.contains(&body.status.as_str()) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "Invalid status", "valid": valid_statuses})),
        )
            .into_response();
    }

    if let Err(e) = varpulis_db::repo::update_org_status(&pool, org_uuid, &body.status).await {
        tracing::error!("Failed to change status: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": "Internal error"})),
        )
            .into_response();
    }

    tracing::info!("Admin changed org {} status to {}", org_id, body.status);
    (StatusCode::OK, Json(serde_json::json!({"ok": true}))).into_response()
}

#[derive(Debug, Deserialize)]
struct ExtendTrialRequest {
    expires_at: String,
}

/// PUT /api/v1/admin/tenants/{org_id}/trial — extend trial.
async fn handle_extend_trial(
    State(state): State<AdminState>,
    Path(org_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<ExtendTrialRequest>,
) -> Response {
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());

    if let Err(status) = extract_admin_claims(auth_header, &state.oauth_state).await {
        return (status, Json(serde_json::json!({"error": "Unauthorized"}))).into_response();
    }

    let pool = match require_pool(&state) {
        Ok(p) => p,
        Err(r) => return r,
    };

    let org_uuid: uuid::Uuid = match org_id.parse() {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "Invalid org_id"})),
            )
                .into_response();
        }
    };

    let new_expiry: chrono::DateTime<chrono::Utc> = match body.expires_at.parse() {
        Ok(dt) => dt,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "Invalid expires_at (expected RFC 3339)"})),
            )
                .into_response();
        }
    };

    if let Err(e) = varpulis_db::repo::extend_trial(&pool, org_uuid, new_expiry).await {
        tracing::error!("Failed to extend trial: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": "Internal error"})),
        )
            .into_response();
    }

    tracing::info!("Admin extended trial for org {} to {}", org_id, new_expiry);
    (StatusCode::OK, Json(serde_json::json!({"ok": true}))).into_response()
}

#[derive(Debug, Deserialize)]
#[allow(clippy::struct_field_names)]
struct UpdateLimitsRequest {
    pipeline_limit: Option<i32>,
    events_per_second_limit: Option<i32>,
    monthly_event_limit: Option<i64>,
}

/// PUT /api/v1/admin/tenants/{org_id}/limits — override per-tenant limits.
async fn handle_update_limits(
    State(state): State<AdminState>,
    Path(org_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<UpdateLimitsRequest>,
) -> Response {
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());

    if let Err(status) = extract_admin_claims(auth_header, &state.oauth_state).await {
        return (status, Json(serde_json::json!({"error": "Unauthorized"}))).into_response();
    }

    let pool = match require_pool(&state) {
        Ok(p) => p,
        Err(r) => return r,
    };

    let org_uuid: uuid::Uuid = match org_id.parse() {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "Invalid org_id"})),
            )
                .into_response();
        }
    };

    // Get current org to fill in any unspecified limits
    let org = match varpulis_db::repo::get_organization(&pool, org_uuid).await {
        Ok(Some(o)) => o,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "Tenant not found"})),
            )
                .into_response();
        }
        Err(e) => {
            tracing::error!("Failed to get org: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "Internal error"})),
            )
                .into_response();
        }
    };

    let pipeline_limit = body.pipeline_limit.unwrap_or(org.pipeline_limit);
    let eps_limit = body
        .events_per_second_limit
        .unwrap_or(org.events_per_second_limit);
    let monthly_limit = body.monthly_event_limit.unwrap_or(org.monthly_event_limit);

    if let Err(e) = varpulis_db::repo::update_org_limits(
        &pool,
        org_uuid,
        pipeline_limit,
        eps_limit,
        monthly_limit,
    )
    .await
    {
        tracing::error!("Failed to update limits: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": "Internal error"})),
        )
            .into_response();
    }

    tracing::info!(
        "Admin updated limits for org {}: pipelines={}, eps={}, monthly={}",
        org_id,
        pipeline_limit,
        eps_limit,
        monthly_limit
    );
    (StatusCode::OK, Json(serde_json::json!({"ok": true}))).into_response()
}

/// GET /api/v1/admin/usage — aggregate usage across all tenants.
async fn handle_aggregate_usage(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());

    if let Err(status) = extract_admin_claims(auth_header, &state.oauth_state).await {
        return (status, Json(serde_json::json!({"error": "Unauthorized"}))).into_response();
    }

    let pool = match require_pool(&state) {
        Ok(p) => p,
        Err(r) => return r,
    };

    let orgs = match varpulis_db::repo::list_all_organizations(&pool).await {
        Ok(o) => o,
        Err(e) => {
            tracing::error!("Failed to list orgs: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "Internal error"})),
            )
                .into_response();
        }
    };

    let mut total_events: i64 = 0;
    let mut active_trials = 0u32;
    let mut paid_customers = 0u32;
    let mut suspended = 0u32;

    for o in &orgs {
        let usage = varpulis_db::repo::get_org_usage_summary(&pool, o.id)
            .await
            .unwrap_or(0);
        total_events += usage;
        match o.status.as_str() {
            "trial" => active_trials += 1,
            "suspended" | "revoked" => suspended += 1,
            _ => {}
        }
        if o.tier != "free" {
            paid_customers += 1;
        }
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "total_tenants": orgs.len(),
            "active_trials": active_trials,
            "paid_customers": paid_customers,
            "suspended": suspended,
            "total_events_this_month": total_events,
        })),
    )
        .into_response()
}

/// POST /api/v1/admin/tenants/{org_id}/revoke — hard revoke: mark revoked.
async fn handle_revoke_tenant(
    State(state): State<AdminState>,
    Path(org_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());

    if let Err(status) = extract_admin_claims(auth_header, &state.oauth_state).await {
        return (status, Json(serde_json::json!({"error": "Unauthorized"}))).into_response();
    }

    let pool = match require_pool(&state) {
        Ok(p) => p,
        Err(r) => return r,
    };

    let org_uuid: uuid::Uuid = match org_id.parse() {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "Invalid org_id"})),
            )
                .into_response();
        }
    };

    if let Err(e) = varpulis_db::repo::update_org_status(&pool, org_uuid, "revoked").await {
        tracing::error!("Failed to revoke org: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": "Internal error"})),
        )
            .into_response();
    }

    tracing::warn!("Admin revoked org {}", org_id);
    (StatusCode::OK, Json(serde_json::json!({"ok": true}))).into_response()
}

// ---------------------------------------------------------------------------
// Trial expiration background task
// ---------------------------------------------------------------------------

/// Spawn a background task that checks for expired trials every hour.
pub fn spawn_trial_expiry_checker(pool: varpulis_db::PgPool) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(3600));
        loop {
            interval.tick().await;
            let now = chrono::Utc::now();
            match varpulis_db::repo::get_expiring_trials(&pool, now).await {
                Ok(expired) => {
                    for org in expired {
                        tracing::info!(
                            "Trial expired for org {} ({}), suspending",
                            org.id,
                            org.name
                        );
                        if let Err(e) =
                            varpulis_db::repo::update_org_status(&pool, org.id, "suspended").await
                        {
                            tracing::error!("Failed to suspend expired org {}: {}", org.id, e);
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Trial expiry check failed: {}", e);
                }
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Route assembly
// ---------------------------------------------------------------------------

pub fn admin_routes(
    db_pool: Option<varpulis_db::PgPool>,
    oauth_state: Option<SharedOAuthState>,
) -> Router {
    let state = AdminState {
        db_pool,
        oauth_state,
    };

    Router::new()
        .route("/api/v1/admin/tenants", get(handle_list_tenants))
        .route("/api/v1/admin/tenants/{org_id}", get(handle_get_tenant))
        .route(
            "/api/v1/admin/tenants/{org_id}/tier",
            put(handle_change_tier),
        )
        .route(
            "/api/v1/admin/tenants/{org_id}/status",
            put(handle_change_status),
        )
        .route(
            "/api/v1/admin/tenants/{org_id}/trial",
            put(handle_extend_trial),
        )
        .route(
            "/api/v1/admin/tenants/{org_id}/limits",
            put(handle_update_limits),
        )
        .route("/api/v1/admin/usage", get(handle_aggregate_usage))
        .route(
            "/api/v1/admin/tenants/{org_id}/revoke",
            post(handle_revoke_tenant),
        )
        .with_state(state)
}
