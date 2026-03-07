//! Admin panel API endpoints for Varpulis Cloud (saas feature).
//!
//! All endpoints require JWT with `role: "admin"`.

use axum::extract::{Json, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::Router;
use serde::Deserialize;

use crate::auth;
use crate::oauth::{self, SharedOAuthState};

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct AdminState {
    pub db_pool: Option<varpulis_db::PgPool>,
    pub oauth_state: Option<SharedOAuthState>,
    pub tenant_manager: Option<varpulis_runtime::SharedTenantManager>,
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
    let hash = oauth::token_hash(&token);
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
                    "slug": o.slug,
                    "tier": o.tier,
                    "status": o.status,
                    "org_type": o.org_type,
                    "parent_org_id": o.parent_org_id.map(|id| id.to_string()),
                    "db_schema": o.db_schema,
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

    let sub_tenants = varpulis_db::repo::list_child_organizations(&pool, org_uuid)
        .await
        .unwrap_or_default();

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "id": org.id.to_string(),
            "name": org.name,
            "slug": org.slug,
            "tier": org.tier,
            "status": org.status,
            "org_type": org.org_type,
            "parent_org_id": org.parent_org_id.map(|id| id.to_string()),
            "db_schema": org.db_schema,
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
                "scope_level": p.scope_level,
                "inherited_from_org_id": p.inherited_from_org_id.map(|id| id.to_string()),
                "created_at": p.created_at.to_rfc3339(),
            })).collect::<Vec<_>>(),
            "api_keys": api_keys.iter().map(|k| serde_json::json!({
                "id": k.id.to_string(),
                "name": k.name,
                "created_at": k.created_at.to_rfc3339(),
                "last_used_at": k.last_used_at.map(|t| t.to_rfc3339()),
            })).collect::<Vec<_>>(),
            "sub_tenants": sub_tenants.iter().map(|s| serde_json::json!({
                "id": s.id.to_string(),
                "name": s.name,
                "slug": s.slug,
                "status": s.status,
                "org_type": s.org_type,
                "db_schema": s.db_schema,
                "created_at": s.created_at.to_rfc3339(),
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

    // Sync runtime quota
    if let Some(ref tm) = state.tenant_manager {
        let tid = varpulis_runtime::TenantId::new(org_id.clone());
        let quota = varpulis_runtime::TenantQuota::for_tier(&body.tier);
        let mut mgr = tm.write().await;
        mgr.update_tenant_quota(&tid, quota);
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

    // When suspending/revoking, remove tenant from runtime (stops all pipelines)
    if body.status == "suspended" || body.status == "revoked" {
        if let Some(ref tm) = state.tenant_manager {
            let tid = varpulis_runtime::TenantId::new(org_id.clone());
            let mut mgr = tm.write().await;
            if let Err(e) = mgr.remove_tenant(&tid) {
                tracing::warn!("Failed to remove runtime tenant on status change: {}", e);
            }
        }
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

    // Sync runtime quota
    if let Some(ref tm) = state.tenant_manager {
        let tid = varpulis_runtime::TenantId::new(org_id.clone());
        let quota = varpulis_runtime::TenantQuota {
            max_pipelines: pipeline_limit as usize,
            max_events_per_second: eps_limit as u64,
            max_streams_per_pipeline: 100, // keep default
        };
        let mut mgr = tm.write().await;
        mgr.update_tenant_quota(&tid, quota);
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

#[derive(Debug, Deserialize)]
struct CreateTenantRequest {
    name: String,
    admin_username: String,
    admin_password: String,
    admin_email: String,
    tier: Option<String>,
}

/// POST /api/v1/admin/tenants — create a new tenant (org + user + API key).
async fn handle_create_tenant(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(body): Json<CreateTenantRequest>,
) -> Response {
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());

    if let Err(status) = extract_admin_claims(auth_header, &state.oauth_state).await {
        return (status, Json(serde_json::json!({"error": "Unauthorized"}))).into_response();
    }

    let pool = match require_pool(&state) {
        Ok(p) => p,
        Err(r) => return r,
    };

    let tier = body.tier.as_deref().unwrap_or("free");
    let valid_tiers = ["free", "pro", "business", "enterprise"];
    if !valid_tiers.contains(&tier) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "Invalid tier", "valid": valid_tiers})),
        )
            .into_response();
    }

    // Validate password
    if body.admin_password.len() < 8 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "Password must be at least 8 characters"})),
        )
            .into_response();
    }

    // 1. Hash password and create local user in DB
    let password_hash = match crate::users::hash_password(&body.admin_password) {
        Ok(h) => h,
        Err(e) => {
            tracing::error!("Password hashing failed: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "Password hashing failed"})),
            )
                .into_response();
        }
    };

    let db_user = match varpulis_db::repo::create_local_user(
        &pool,
        &body.admin_username,
        &password_hash,
        &body.name,
        &body.admin_email,
        "user",
    )
    .await
    {
        Ok(u) => u,
        Err(e) => {
            let msg = e.to_string();
            let status = if msg.contains("duplicate") || msg.contains("unique") {
                StatusCode::CONFLICT
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            tracing::error!("Failed to create DB user: {}", e);
            return (
                status,
                Json(serde_json::json!({"error": format!("Failed to create user: {}", e)})),
            )
                .into_response();
        }
    };

    let owner_uuid = db_user.id;

    let org = if tier == "free" {
        varpulis_db::repo::create_trial_organization(&pool, owner_uuid, &body.name).await
    } else {
        varpulis_db::repo::create_organization(&pool, owner_uuid, &body.name).await
    };

    let org = match org {
        Ok(o) => o,
        Err(e) => {
            tracing::error!("Failed to create organization: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": format!("Failed to create organization: {}", e)})),
            )
                .into_response();
        }
    };

    // Set tier if not default
    if tier != "free" {
        if let Err(e) = varpulis_db::repo::update_org_tier(&pool, org.id, tier).await {
            tracing::error!("Failed to set tier: {}", e);
        }
        // Set status to active for paid tiers
        if let Err(e) = varpulis_db::repo::update_org_status(&pool, org.id, "active").await {
            tracing::error!("Failed to set status: {}", e);
        }
    }

    // 3. Generate API key and store in DB
    let api_key = auth::generate_api_key();
    let key_hash = oauth::token_hash(&api_key);

    if let Err(e) = varpulis_db::repo::create_api_key(&pool, org.id, &key_hash, "default").await {
        tracing::error!("Failed to create API key: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("Failed to create API key: {}", e)})),
        )
            .into_response();
    }

    // 4. Create runtime tenant so the API key works immediately
    if let Some(ref tm) = state.tenant_manager {
        let quota = varpulis_runtime::TenantQuota::for_tier(tier);
        let tid = varpulis_runtime::TenantId::new(org.id.to_string());
        let mut mgr = tm.write().await;
        if let Err(e) = mgr.create_tenant_with_id(tid, body.name.clone(), key_hash.clone(), quota) {
            tracing::warn!("Failed to create runtime tenant (non-fatal): {}", e);
        }
    }

    // 5. Auto-copy deployed global pipeline templates to the new tenant
    if let Ok(templates) = varpulis_db::repo::list_deployed_global_templates(&pool).await {
        for t in &templates {
            if let Err(e) = varpulis_db::repo::create_global_pipeline_copy(
                &pool,
                org.id,
                t.id,
                &t.name,
                &t.vpl_source,
            )
            .await
            {
                tracing::warn!(
                    "Failed to copy global pipeline '{}' to new tenant {}: {}",
                    t.name,
                    org.id,
                    e
                );
            }

            // Deploy in runtime
            if let Some(ref tm) = state.tenant_manager {
                let tid = varpulis_runtime::TenantId::new(org.id.to_string());
                let mut mgr = tm.write().await;
                if mgr.get_tenant(&tid).is_some() {
                    if let Err(e) = mgr
                        .deploy_global_pipeline_on_tenant(
                            &tid,
                            t.name.clone(),
                            t.vpl_source.clone(),
                            t.id.to_string(),
                        )
                        .await
                    {
                        tracing::warn!("Failed to deploy global pipeline to runtime: {}", e);
                    }
                }
            }
        }
    }

    tracing::info!(
        "Admin created tenant '{}' (org={}, tier={})",
        body.name,
        org.id,
        tier
    );

    (
        StatusCode::CREATED,
        Json(serde_json::json!({
            "tenant": {
                "id": org.id.to_string(),
                "name": body.name,
                "tier": tier,
                "status": if tier == "free" { "trial" } else { "active" },
            },
            "admin_user": {
                "id": owner_uuid.to_string(),
                "username": body.admin_username,
            },
            "api_key": api_key,
        })),
    )
        .into_response()
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
// Global Pipeline handlers
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct DeployGlobalPipelineRequest {
    name: String,
    vpl_source: String,
}

/// POST /api/v1/admin/global-pipelines — deploy a global pipeline to all tenants.
async fn handle_deploy_global_pipeline(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(body): Json<DeployGlobalPipelineRequest>,
) -> Response {
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());

    let claims = match extract_admin_claims(auth_header, &state.oauth_state).await {
        Ok(c) => c,
        Err(status) => {
            return (status, Json(serde_json::json!({"error": "Unauthorized"}))).into_response()
        }
    };

    let pool = match require_pool(&state) {
        Ok(p) => p,
        Err(r) => return r,
    };

    // Validate VPL source parses
    if let Err(e) = varpulis_parser::parse(&body.vpl_source) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": format!("VPL parse error: {e}")})),
        )
            .into_response();
    }

    // Parse deployer UUID from claims
    let deployed_by = claims.user_id.parse::<uuid::Uuid>().ok();

    // Create global template
    let template = match varpulis_db::repo::create_global_template(
        &pool,
        &body.name,
        &body.vpl_source,
        deployed_by,
    )
    .await
    {
        Ok(t) => t,
        Err(e) => {
            tracing::error!("Failed to create global template: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "Failed to create template"})),
            )
                .into_response();
        }
    };

    // Copy to all active orgs
    let orgs = varpulis_db::repo::list_all_organizations(&pool)
        .await
        .unwrap_or_default();

    let mut copies_created = 0u32;
    for org in &orgs {
        if org.status == "revoked" {
            continue;
        }
        match varpulis_db::repo::create_global_pipeline_copy(
            &pool,
            org.id,
            template.id,
            &body.name,
            &body.vpl_source,
        )
        .await
        {
            Ok(_) => copies_created += 1,
            Err(e) => tracing::warn!("Failed to copy global pipeline to org {}: {}", org.id, e),
        }

        // Deploy in runtime
        if let Some(ref tm) = state.tenant_manager {
            let tid = varpulis_runtime::TenantId::new(org.id.to_string());
            let mut mgr = tm.write().await;
            if mgr.get_tenant(&tid).is_some() {
                if let Err(e) = mgr
                    .deploy_global_pipeline_on_tenant(
                        &tid,
                        body.name.clone(),
                        body.vpl_source.clone(),
                        template.id.to_string(),
                    )
                    .await
                {
                    tracing::warn!(
                        "Failed to deploy global pipeline to runtime tenant {}: {}",
                        org.id,
                        e
                    );
                }
            }
        }
    }

    tracing::info!(
        "Admin deployed global pipeline '{}' (template={}, copies={})",
        body.name,
        template.id,
        copies_created
    );

    (
        StatusCode::CREATED,
        Json(serde_json::json!({
            "template_id": template.id.to_string(),
            "name": body.name,
            "copies_created": copies_created,
        })),
    )
        .into_response()
}

/// GET /api/v1/admin/global-pipelines — list all global pipeline templates.
async fn handle_list_global_pipelines(
    State(state): State<AdminState>,
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

    let templates = match varpulis_db::repo::list_global_templates(&pool).await {
        Ok(t) => t,
        Err(e) => {
            tracing::error!("Failed to list global templates: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "Internal error"})),
            )
                .into_response();
        }
    };

    let mut items = Vec::with_capacity(templates.len());
    for t in &templates {
        let copies = varpulis_db::repo::list_global_template_copies(&pool, t.id)
            .await
            .unwrap_or_default();
        items.push(serde_json::json!({
            "id": t.id.to_string(),
            "name": t.name,
            "vpl_source": t.vpl_source,
            "status": t.status,
            "tenant_count": copies.len(),
            "deployed_by": t.deployed_by.map(|u| u.to_string()),
            "created_at": t.created_at.to_rfc3339(),
            "updated_at": t.updated_at.to_rfc3339(),
        }));
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({"global_pipelines": items})),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
struct UpdateGlobalPipelineRequest {
    vpl_source: String,
}

/// PUT /api/v1/admin/global-pipelines/{id} — update VPL source for all copies.
async fn handle_update_global_pipeline(
    State(state): State<AdminState>,
    Path(template_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<UpdateGlobalPipelineRequest>,
) -> Response {
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());

    if let Err(status) = extract_admin_claims(auth_header, &state.oauth_state).await {
        return (status, Json(serde_json::json!({"error": "Unauthorized"}))).into_response();
    }

    let pool = match require_pool(&state) {
        Ok(p) => p,
        Err(r) => return r,
    };

    let template_uuid: uuid::Uuid = match template_id.parse() {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "Invalid template ID"})),
            )
                .into_response();
        }
    };

    // Validate VPL
    if let Err(e) = varpulis_parser::parse(&body.vpl_source) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": format!("VPL parse error: {e}")})),
        )
            .into_response();
    }

    // Check template exists
    match varpulis_db::repo::get_global_template(&pool, template_uuid).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "Template not found"})),
            )
                .into_response();
        }
        Err(e) => {
            tracing::error!("Failed to get template: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "Internal error"})),
            )
                .into_response();
        }
    }

    // Update template
    if let Err(e) =
        varpulis_db::repo::update_global_template_source(&pool, template_uuid, &body.vpl_source)
            .await
    {
        tracing::error!("Failed to update template: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": "Failed to update template"})),
        )
            .into_response();
    }

    // Update all copies
    let copies_updated = varpulis_db::repo::update_global_template_copies_source(
        &pool,
        template_uuid,
        &body.vpl_source,
    )
    .await
    .unwrap_or(0);

    // Reload in runtime tenants
    if let Some(ref tm) = state.tenant_manager {
        let copies = varpulis_db::repo::list_global_template_copies(&pool, template_uuid)
            .await
            .unwrap_or_default();
        for copy in &copies {
            let tid = varpulis_runtime::TenantId::new(copy.org_id.to_string());
            let mut mgr = tm.write().await;
            if let Some(tenant) = mgr.get_tenant_mut(&tid) {
                // Find the pipeline by global_template_id
                let pipeline_id = tenant
                    .pipelines
                    .values()
                    .find(|p| {
                        p.global_template_id.as_deref() == Some(template_uuid.to_string().as_str())
                    })
                    .map(|p| p.id.clone());
                if let Some(pid) = pipeline_id {
                    if let Err(e) = tenant.reload_pipeline(&pid, body.vpl_source.clone()).await {
                        tracing::warn!(
                            "Failed to reload global pipeline {} on tenant {}: {}",
                            pid,
                            copy.org_id,
                            e
                        );
                    }
                }
            }
        }
    }

    tracing::info!(
        "Admin updated global pipeline template {} ({} copies updated)",
        template_id,
        copies_updated
    );

    (
        StatusCode::OK,
        Json(serde_json::json!({"ok": true, "copies_updated": copies_updated})),
    )
        .into_response()
}

/// DELETE /api/v1/admin/global-pipelines/{id} — undeploy from all tenants.
async fn handle_undeploy_global_pipeline(
    State(state): State<AdminState>,
    Path(template_id): Path<String>,
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

    let template_uuid: uuid::Uuid = match template_id.parse() {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "Invalid template ID"})),
            )
                .into_response();
        }
    };

    // Get copies before deletion (for runtime cleanup)
    let copies = varpulis_db::repo::list_global_template_copies(&pool, template_uuid)
        .await
        .unwrap_or_default();

    // Remove from runtime tenants
    if let Some(ref tm) = state.tenant_manager {
        for copy in &copies {
            let tid = varpulis_runtime::TenantId::new(copy.org_id.to_string());
            let mut mgr = tm.write().await;
            if let Some(tenant) = mgr.get_tenant_mut(&tid) {
                let pipeline_id = tenant
                    .pipelines
                    .values()
                    .find(|p| {
                        p.global_template_id.as_deref() == Some(template_uuid.to_string().as_str())
                    })
                    .map(|p| p.id.clone());
                if let Some(pid) = pipeline_id {
                    let _ = tenant.remove_pipeline(&pid);
                }
            }
        }
    }

    // Delete template (CASCADE removes all DB copies)
    if let Err(e) = varpulis_db::repo::delete_global_template(&pool, template_uuid).await {
        tracing::error!("Failed to delete global template: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": "Failed to delete template"})),
        )
            .into_response();
    }

    tracing::info!(
        "Admin undeployed global pipeline template {} ({} copies removed)",
        template_id,
        copies.len()
    );

    (
        StatusCode::OK,
        Json(serde_json::json!({"ok": true, "copies_removed": copies.len()})),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// Route assembly
// ---------------------------------------------------------------------------

pub fn admin_routes(
    db_pool: Option<varpulis_db::PgPool>,
    oauth_state: Option<SharedOAuthState>,
    tenant_manager: Option<varpulis_runtime::SharedTenantManager>,
) -> Router {
    let state = AdminState {
        db_pool,
        oauth_state,
        tenant_manager,
    };

    Router::new()
        .route(
            "/api/v1/admin/tenants",
            get(handle_list_tenants).post(handle_create_tenant),
        )
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
        .route(
            "/api/v1/admin/global-pipelines",
            post(handle_deploy_global_pipeline).get(handle_list_global_pipelines),
        )
        .route(
            "/api/v1/admin/global-pipelines/{id}",
            put(handle_update_global_pipeline).delete(handle_undeploy_global_pipeline),
        )
        .with_state(state)
}
