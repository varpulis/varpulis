//! Organization and API key management endpoints for Varpulis Cloud (saas feature).

use axum::extract::{Json, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::Router;
use serde::Deserialize;

use crate::oauth::{self, SharedOAuthState};

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct OrgState {
    pub db_pool: Option<varpulis_db::PgPool>,
    pub oauth_state: Option<SharedOAuthState>,
    pub tenant_manager: Option<varpulis_runtime::SharedTenantManager>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract and verify JWT claims from the Authorization header using the OAuth state.
async fn extract_claims(
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
    let hash = crate::oauth::token_hash(&token);
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

    Ok(token_data.claims)
}

/// Verify the authenticated user has admin access to the given org.
///
/// Access is granted if any of:
/// 1. User has global admin role
/// 2. User is a member of the org with owner/admin role
/// 3. User owns the org (legacy owner_id check)
/// 4. **Hierarchy**: the org is a sub-tenant and the user is admin of its parent tenant
async fn verify_org_access(
    pool: &varpulis_db::PgPool,
    claims: &oauth::Claims,
    org_uuid: uuid::Uuid,
) -> Result<(), Response> {
    // Global admins can access any org
    if claims.role == "admin" {
        return Ok(());
    }

    let user_uuid: uuid::Uuid = claims.user_id.parse().map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "Invalid user_id in token"})),
        )
            .into_response()
    })?;

    let org = varpulis_db::repo::get_organization(pool, org_uuid)
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "Internal error"})),
            )
                .into_response()
        })?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "Organization not found"})),
            )
                .into_response()
        })?;

    // Check direct membership (owner/admin role)
    if let Ok(Some(member)) =
        varpulis_db::repo::get_user_org_membership(pool, user_uuid, org_uuid).await
    {
        if member.status == "active" && (member.role == "owner" || member.role == "admin") {
            return Ok(());
        }
    }

    // Legacy: direct owner_id check
    if org.owner_id == user_uuid {
        return Ok(());
    }

    // Hierarchy check: if this is a sub-tenant, check if user is admin of the parent tenant
    if org.org_type == "sub_tenant" {
        if let Some(parent_id) = org.parent_org_id {
            // Check parent org membership
            if let Ok(Some(parent_member)) =
                varpulis_db::repo::get_user_org_membership(pool, user_uuid, parent_id).await
            {
                if parent_member.status == "active"
                    && (parent_member.role == "owner" || parent_member.role == "admin")
                {
                    return Ok(());
                }
            }
            // Legacy: parent org owner_id check
            if let Ok(Some(parent_org)) = varpulis_db::repo::get_organization(pool, parent_id).await
            {
                if parent_org.owner_id == user_uuid {
                    return Ok(());
                }
            }
        }
    }

    Err((
        StatusCode::FORBIDDEN,
        Json(serde_json::json!({"error": "Forbidden"})),
    )
        .into_response())
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

/// GET /api/v1/orgs — list user's organizations.
async fn handle_list_orgs(State(state): State<OrgState>, headers: HeaderMap) -> Response {
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());

    let claims = match extract_claims(auth_header, &state.oauth_state).await {
        Ok(c) => c,
        Err(status) => {
            return (status, Json(serde_json::json!({"error": "Unauthorized"}))).into_response();
        }
    };

    let pool = match state.db_pool {
        Some(p) => p,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "Database not configured"})),
            )
                .into_response();
        }
    };

    let user_id: uuid::Uuid = match claims.user_id.parse() {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "Invalid user_id in token"})),
            )
                .into_response();
        }
    };

    // Get orgs with membership roles
    let memberships = varpulis_db::repo::get_user_memberships(&pool, user_id).await;
    match memberships {
        Ok(membership_list) => {
            let orgs_json: Vec<serde_json::Value> = membership_list
                .iter()
                .map(|(member, org)| {
                    serde_json::json!({
                        "id": org.id.to_string(),
                        "name": org.name,
                        "tier": org.tier,
                        "role": member.role,
                        "slug": org.slug,
                        "org_type": org.org_type,
                        "parent_org_id": org.parent_org_id.map(|id| id.to_string()),
                        "db_schema": org.db_schema,
                        "created_at": org.created_at.to_rfc3339(),
                    })
                })
                .collect();
            (
                StatusCode::OK,
                Json(serde_json::json!({"organizations": orgs_json})),
            )
                .into_response()
        }
        Err(_) => {
            // Fallback to legacy endpoint without roles
            match varpulis_db::repo::get_user_organizations(&pool, user_id).await {
                Ok(orgs) => {
                    let orgs_json: Vec<serde_json::Value> = orgs
                        .iter()
                        .map(|o| {
                            serde_json::json!({
                                "id": o.id.to_string(),
                                "name": o.name,
                                "tier": o.tier,
                                "created_at": o.created_at.to_rfc3339(),
                            })
                        })
                        .collect();
                    (
                        StatusCode::OK,
                        Json(serde_json::json!({"organizations": orgs_json})),
                    )
                        .into_response()
                }
                Err(e) => {
                    tracing::error!("Failed to list orgs: {}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({"error": "Internal error"})),
                    )
                        .into_response()
                }
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct CreateApiKeyRequest {
    name: Option<String>,
    scopes: Option<String>,
    expires_in: Option<String>,
}

/// POST /api/v1/orgs/{org_id}/api-keys — generate a new API key.
async fn handle_create_api_key(
    State(state): State<OrgState>,
    Path(org_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<CreateApiKeyRequest>,
) -> Response {
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());

    let claims = match extract_claims(auth_header, &state.oauth_state).await {
        Ok(c) => c,
        Err(status) => {
            return (status, Json(serde_json::json!({"error": "Unauthorized"}))).into_response();
        }
    };

    let pool = match state.db_pool {
        Some(ref p) => p.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "Database not configured"})),
            )
                .into_response();
        }
    };

    // Verify the org belongs to the user
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

    // Always verify ownership via DB lookup
    if let Err(resp) = verify_org_access(&pool, &claims, org_uuid).await {
        return resp;
    }

    // Generate 32 random bytes, hex encode with vpl_ prefix
    let raw_bytes: [u8; 32] = rand::random();
    let raw_key = format!("vpl_{}", hex::encode(raw_bytes));
    let key_prefix = raw_key[..12].to_string(); // "vpl_" + 8 hex chars

    // SHA-256 hash for storage
    use sha2::Digest;
    let hash = hex::encode(sha2::Sha256::digest(raw_key.as_bytes()));

    let key_name = body.name.unwrap_or_else(|| "default".to_string());
    let scopes = body.scopes.unwrap_or_else(|| "*".to_string());

    // Parse expiry duration
    let expires_at = body.expires_in.as_deref().and_then(|dur| {
        let days: i64 = dur.strip_suffix('d').and_then(|n| n.parse().ok())?;
        Some(chrono::Utc::now() + chrono::Duration::days(days))
    });

    // Parse user_id for created_by
    let created_by: Option<uuid::Uuid> = claims.user_id.parse().ok();

    match varpulis_db::repo::create_api_key_extended(
        &pool,
        org_uuid,
        &hash,
        &key_name,
        &key_prefix,
        &scopes,
        expires_at,
        created_by,
    )
    .await
    {
        Ok(api_key) => {
            // Auto-provision runtime tenant if not already registered
            if let Some(ref tm) = state.tenant_manager {
                let tid = varpulis_runtime::TenantId::new(org_uuid.to_string());
                let mut mgr = tm.write().await;
                if mgr.get_tenant(&tid).is_none() {
                    // Look up org tier for quota
                    let tier = match varpulis_db::repo::get_organization(&pool, org_uuid).await {
                        Ok(Some(org)) => org.tier,
                        _ => "free".to_string(),
                    };
                    let org_name = match varpulis_db::repo::get_organization(&pool, org_uuid).await
                    {
                        Ok(Some(org)) => org.name,
                        _ => "unknown".to_string(),
                    };
                    let quota = varpulis_runtime::TenantQuota::for_tier(&tier);
                    if let Err(e) = mgr.create_tenant_with_id(tid, org_name, hash.clone(), quota) {
                        tracing::warn!("Failed to auto-provision runtime tenant: {}", e);
                    } else {
                        tracing::info!("Auto-provisioned runtime tenant for org {}", org_uuid);
                    }
                }
            }

            (
                StatusCode::CREATED,
                Json(serde_json::json!({
                    "id": api_key.id.to_string(),
                    "api_key": raw_key,
                    "name": api_key.name,
                    "key_prefix": key_prefix,
                    "scopes": scopes,
                    "expires_at": expires_at.map(|t| t.to_rfc3339()),
                    "created_at": api_key.created_at.to_rfc3339(),
                })),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!("Failed to create API key: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "Internal error"})),
            )
                .into_response()
        }
    }
}

/// GET /api/v1/orgs/{org_id}/api-keys — list API keys (no secrets).
async fn handle_list_api_keys(
    State(state): State<OrgState>,
    Path(org_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());

    let claims = match extract_claims(auth_header, &state.oauth_state).await {
        Ok(c) => c,
        Err(status) => {
            return (status, Json(serde_json::json!({"error": "Unauthorized"}))).into_response();
        }
    };

    let pool = match state.db_pool {
        Some(ref p) => p.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "Database not configured"})),
            )
                .into_response();
        }
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

    // Verify the authenticated user owns this org
    if let Err(resp) = verify_org_access(&pool, &claims, org_uuid).await {
        return resp;
    }

    match varpulis_db::repo::list_api_keys(&pool, org_uuid).await {
        Ok(keys) => {
            let keys_json: Vec<serde_json::Value> = keys
                .iter()
                .map(|k| {
                    serde_json::json!({
                        "id": k.id.to_string(),
                        "name": k.name,
                        "key_prefix": k.key_prefix,
                        "scopes": k.scopes,
                        "created_at": k.created_at.to_rfc3339(),
                        "expires_at": k.expires_at.map(|t| t.to_rfc3339()),
                        "last_used_at": k.last_used_at.map(|t| t.to_rfc3339()),
                    })
                })
                .collect();
            (
                StatusCode::OK,
                Json(serde_json::json!({"api_keys": keys_json})),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!("Failed to list API keys: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "Internal error"})),
            )
                .into_response()
        }
    }
}

/// DELETE /api/v1/orgs/{org_id}/api-keys/{key_id} — revoke an API key.
async fn handle_delete_api_key(
    State(state): State<OrgState>,
    Path((org_id, key_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());

    let claims = match extract_claims(auth_header, &state.oauth_state).await {
        Ok(c) => c,
        Err(status) => {
            return (status, Json(serde_json::json!({"error": "Unauthorized"}))).into_response();
        }
    };

    let pool = match state.db_pool {
        Some(ref p) => p.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "Database not configured"})),
            )
                .into_response();
        }
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

    // Verify the authenticated user owns this org
    if let Err(resp) = verify_org_access(&pool, &claims, org_uuid).await {
        return resp;
    }

    let key_uuid: uuid::Uuid = match key_id.parse() {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "Invalid key_id"})),
            )
                .into_response();
        }
    };

    match varpulis_db::repo::delete_api_key(&pool, key_uuid, org_uuid).await {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({"ok": true}))).into_response(),
        Err(e) => {
            tracing::error!("Failed to delete API key: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "Internal error"})),
            )
                .into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// Member management endpoints
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct InviteMemberRequest {
    email: String,
    role: Option<String>,
}

/// POST /api/v1/orgs/{org_id}/members — invite a member.
async fn handle_invite_member(
    State(state): State<OrgState>,
    Path(org_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<InviteMemberRequest>,
) -> Response {
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());
    let claims = match extract_claims(auth_header, &state.oauth_state).await {
        Ok(c) => c,
        Err(status) => {
            return (status, Json(serde_json::json!({"error": "Unauthorized"}))).into_response();
        }
    };

    let pool = match state.db_pool {
        Some(ref p) => p.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "Database not configured"})),
            )
                .into_response();
        }
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

    if let Err(resp) = verify_org_access(&pool, &claims, org_uuid).await {
        return resp;
    }

    // Lookup user by email
    let user = match varpulis_db::repo::get_user_by_email(&pool, &body.email).await {
        Ok(Some(u)) => u,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "User not found with that email"})),
            )
                .into_response();
        }
        Err(e) => {
            tracing::error!("Failed to lookup user: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "Internal error"})),
            )
                .into_response();
        }
    };

    let role = body.role.unwrap_or_else(|| "member".to_string());
    match varpulis_db::repo::add_org_member(&pool, org_uuid, user.id, &role).await {
        Ok(_) => (
            StatusCode::CREATED,
            Json(serde_json::json!({"ok": true, "user_id": user.id.to_string()})),
        )
            .into_response(),
        Err(e) => {
            tracing::error!("Failed to add org member: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "Internal error"})),
            )
                .into_response()
        }
    }
}

/// GET /api/v1/orgs/{org_id}/members — list members.
async fn handle_list_members(
    State(state): State<OrgState>,
    Path(org_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());
    let claims = match extract_claims(auth_header, &state.oauth_state).await {
        Ok(c) => c,
        Err(status) => {
            return (status, Json(serde_json::json!({"error": "Unauthorized"}))).into_response();
        }
    };

    let pool = match state.db_pool {
        Some(ref p) => p.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "Database not configured"})),
            )
                .into_response();
        }
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

    if let Err(resp) = verify_org_access(&pool, &claims, org_uuid).await {
        return resp;
    }

    match varpulis_db::repo::list_org_members(&pool, org_uuid).await {
        Ok(members) => {
            let members_json: Vec<serde_json::Value> = members
                .iter()
                .map(|(member, user)| {
                    serde_json::json!({
                        "user_id": user.id.to_string(),
                        "name": user.name,
                        "email": user.email,
                        "role": member.role,
                        "status": member.status,
                        "accepted_at": member.accepted_at.map(|t| t.to_rfc3339()),
                    })
                })
                .collect();
            (
                StatusCode::OK,
                Json(serde_json::json!({"members": members_json})),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!("Failed to list org members: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "Internal error"})),
            )
                .into_response()
        }
    }
}

/// DELETE /api/v1/orgs/{org_id}/members/{user_id} — remove a member.
async fn handle_remove_member(
    State(state): State<OrgState>,
    Path((org_id, user_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());
    let claims = match extract_claims(auth_header, &state.oauth_state).await {
        Ok(c) => c,
        Err(status) => {
            return (status, Json(serde_json::json!({"error": "Unauthorized"}))).into_response();
        }
    };

    let pool = match state.db_pool {
        Some(ref p) => p.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "Database not configured"})),
            )
                .into_response();
        }
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

    if let Err(resp) = verify_org_access(&pool, &claims, org_uuid).await {
        return resp;
    }

    let member_uuid: uuid::Uuid = match user_id.parse() {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "Invalid user_id"})),
            )
                .into_response();
        }
    };

    match varpulis_db::repo::remove_org_member(&pool, org_uuid, member_uuid).await {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({"ok": true}))).into_response(),
        Err(e) => {
            tracing::error!("Failed to remove org member: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "Internal error"})),
            )
                .into_response()
        }
    }
}

#[derive(Debug, Deserialize)]
struct UpdateMemberRoleRequest {
    role: String,
}

/// PUT /api/v1/orgs/{org_id}/members/{user_id} — change member role.
async fn handle_update_member_role(
    State(state): State<OrgState>,
    Path((org_id, user_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(body): Json<UpdateMemberRoleRequest>,
) -> Response {
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());
    let claims = match extract_claims(auth_header, &state.oauth_state).await {
        Ok(c) => c,
        Err(status) => {
            return (status, Json(serde_json::json!({"error": "Unauthorized"}))).into_response();
        }
    };

    let pool = match state.db_pool {
        Some(ref p) => p.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "Database not configured"})),
            )
                .into_response();
        }
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

    if let Err(resp) = verify_org_access(&pool, &claims, org_uuid).await {
        return resp;
    }

    let member_uuid: uuid::Uuid = match user_id.parse() {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "Invalid user_id"})),
            )
                .into_response();
        }
    };

    // Prevent removing the last owner
    if body.role != "owner" {
        if let Ok(Some(current)) =
            varpulis_db::repo::get_user_org_membership(&pool, member_uuid, org_uuid).await
        {
            if current.role == "owner" {
                // Check if there's another owner
                if let Ok(members) = varpulis_db::repo::list_org_members(&pool, org_uuid).await {
                    let owner_count = members.iter().filter(|(m, _)| m.role == "owner").count();
                    if owner_count <= 1 {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(serde_json::json!({
                                "error": "Cannot remove the last owner. Transfer ownership first."
                            })),
                        )
                            .into_response();
                    }
                }
            }
        }
    }

    // Update via remove + re-add (simple approach)
    let _ = varpulis_db::repo::remove_org_member(&pool, org_uuid, member_uuid).await;
    match varpulis_db::repo::add_org_member(&pool, org_uuid, member_uuid, &body.role).await {
        Ok(_) => (StatusCode::OK, Json(serde_json::json!({"ok": true}))).into_response(),
        Err(e) => {
            tracing::error!("Failed to update member role: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "Internal error"})),
            )
                .into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// Sub-tenant management endpoints
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct CreateSubTenantRequest {
    name: String,
}

/// POST /api/v1/orgs/{org_id}/sub-tenants — create a sub-tenant under a tenant.
async fn handle_create_sub_tenant(
    State(state): State<OrgState>,
    Path(org_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<CreateSubTenantRequest>,
) -> Response {
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());
    let claims = match extract_claims(auth_header, &state.oauth_state).await {
        Ok(c) => c,
        Err(status) => {
            return (status, Json(serde_json::json!({"error": "Unauthorized"}))).into_response();
        }
    };

    let pool = match state.db_pool {
        Some(ref p) => p.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "Database not configured"})),
            )
                .into_response();
        }
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

    // Verify the authenticated user is an admin of this org
    if let Err(resp) = verify_org_access(&pool, &claims, org_uuid).await {
        return resp;
    }

    // Verify the parent org is a tenant (not global, not sub_tenant)
    let parent_org = match varpulis_db::repo::get_organization(&pool, org_uuid).await {
        Ok(Some(org)) => org,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "Organization not found"})),
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

    if parent_org.org_type != "tenant" {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "Sub-tenants can only be created under tenant-type organizations"})),
        )
            .into_response();
    }

    let owner_uuid: uuid::Uuid = match claims.user_id.parse() {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "Invalid user_id in token"})),
            )
                .into_response();
        }
    };

    match varpulis_db::repo::create_sub_tenant(&pool, org_uuid, owner_uuid, &body.name).await {
        Ok(sub_tenant) => (
            StatusCode::CREATED,
            Json(serde_json::json!({
                "id": sub_tenant.id.to_string(),
                "name": sub_tenant.name,
                "slug": sub_tenant.slug,
                "org_type": sub_tenant.org_type,
                "parent_org_id": sub_tenant.parent_org_id.map(|id| id.to_string()),
                "db_schema": sub_tenant.db_schema,
                "status": sub_tenant.status,
                "created_at": sub_tenant.created_at.to_rfc3339(),
            })),
        )
            .into_response(),
        Err(e) => {
            tracing::error!("Failed to create sub-tenant: {}", e);
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response()
        }
    }
}

/// GET /api/v1/orgs/{org_id}/sub-tenants — list sub-tenants of a tenant.
async fn handle_list_sub_tenants(
    State(state): State<OrgState>,
    Path(org_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());
    let claims = match extract_claims(auth_header, &state.oauth_state).await {
        Ok(c) => c,
        Err(status) => {
            return (status, Json(serde_json::json!({"error": "Unauthorized"}))).into_response();
        }
    };

    let pool = match state.db_pool {
        Some(ref p) => p.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "Database not configured"})),
            )
                .into_response();
        }
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

    if let Err(resp) = verify_org_access(&pool, &claims, org_uuid).await {
        return resp;
    }

    match varpulis_db::repo::list_sub_tenants(&pool, org_uuid).await {
        Ok(subs) => {
            let subs_json: Vec<serde_json::Value> = subs
                .iter()
                .map(|s| {
                    serde_json::json!({
                        "id": s.id.to_string(),
                        "name": s.name,
                        "slug": s.slug,
                        "org_type": s.org_type,
                        "parent_org_id": s.parent_org_id.map(|id| id.to_string()),
                        "db_schema": s.db_schema,
                        "status": s.status,
                        "tier": s.tier,
                        "created_at": s.created_at.to_rfc3339(),
                    })
                })
                .collect();
            (
                StatusCode::OK,
                Json(serde_json::json!({"sub_tenants": subs_json})),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!("Failed to list sub-tenants: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "Internal error"})),
            )
                .into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// Schema info endpoint
// ---------------------------------------------------------------------------

/// GET /api/v1/orgs/{org_id}/schema — get schema isolation info for an org.
async fn handle_get_schema_info(
    State(state): State<OrgState>,
    Path(org_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());
    let claims = match extract_claims(auth_header, &state.oauth_state).await {
        Ok(c) => c,
        Err(status) => {
            return (status, Json(serde_json::json!({"error": "Unauthorized"}))).into_response();
        }
    };

    let pool = match state.db_pool {
        Some(ref p) => p.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "Database not configured"})),
            )
                .into_response();
        }
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

    if let Err(resp) = verify_org_access(&pool, &claims, org_uuid).await {
        return resp;
    }

    let org = match varpulis_db::repo::get_organization(&pool, org_uuid).await {
        Ok(Some(o)) => o,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "Organization not found"})),
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

    // Get effective schema (own or inherited from parent)
    let effective_schema = match varpulis_db::repo::get_effective_schema(&pool, org_uuid).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("Failed to get effective schema: {}", e);
            None
        }
    };

    // Verify schema exists in PostgreSQL
    let schema_exists = if let Some(ref schema_name) = effective_schema {
        varpulis_db::repo::verify_schema_exists(&pool, schema_name)
            .await
            .unwrap_or(false)
    } else {
        false
    };

    // List tables in schema
    let tables = if let Some(ref schema_name) = effective_schema {
        varpulis_db::repo::list_schema_tables(&pool, schema_name)
            .await
            .unwrap_or_default()
    } else {
        vec![]
    };

    let is_inherited = org.db_schema.is_none() && effective_schema.is_some();

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "org_id": org_uuid.to_string(),
            "slug": org.slug,
            "db_schema": org.db_schema,
            "effective_schema": effective_schema,
            "schema_exists": schema_exists,
            "is_inherited": is_inherited,
            "tables": tables,
        })),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// Pipeline visibility (hierarchy-aware)
// ---------------------------------------------------------------------------

/// GET /api/v1/orgs/{org_id}/pipelines — list visible pipelines (own + inherited).
async fn handle_list_org_pipelines(
    State(state): State<OrgState>,
    Path(org_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());
    let claims = match extract_claims(auth_header, &state.oauth_state).await {
        Ok(c) => c,
        Err(status) => {
            return (status, Json(serde_json::json!({"error": "Unauthorized"}))).into_response();
        }
    };

    let pool = match state.db_pool {
        Some(ref p) => p.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "Database not configured"})),
            )
                .into_response();
        }
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

    if let Err(resp) = verify_org_access(&pool, &claims, org_uuid).await {
        return resp;
    }

    match varpulis_db::repo::list_visible_pipelines(&pool, org_uuid).await {
        Ok(pipelines) => {
            let pipelines_json: Vec<serde_json::Value> = pipelines
                .iter()
                .map(|p| {
                    let is_inherited = p.inherited_from_org_id.is_some() || p.org_id != org_uuid;
                    serde_json::json!({
                        "id": p.id.to_string(),
                        "name": p.name,
                        "status": p.status,
                        "vpl_source": p.vpl_source,
                        "scope_level": p.scope_level,
                        "inherited_from_org_id": p.inherited_from_org_id.map(|id| id.to_string()),
                        "read_only": is_inherited,
                        "created_at": p.created_at.to_rfc3339(),
                    })
                })
                .collect();
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "pipelines": pipelines_json,
                    "total": pipelines_json.len(),
                })),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!("Failed to list pipelines: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "Internal error"})),
            )
                .into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// Route assembly
// ---------------------------------------------------------------------------

pub fn org_routes(
    db_pool: Option<varpulis_db::PgPool>,
    oauth_state: Option<SharedOAuthState>,
    tenant_manager: Option<varpulis_runtime::SharedTenantManager>,
) -> Router {
    let state = OrgState {
        db_pool,
        oauth_state,
        tenant_manager,
    };

    Router::new()
        // GET /api/v1/orgs
        .route("/api/v1/orgs", get(handle_list_orgs))
        // POST /api/v1/orgs/{org_id}/api-keys
        .route(
            "/api/v1/orgs/{org_id}/api-keys",
            post(handle_create_api_key).get(handle_list_api_keys),
        )
        // DELETE /api/v1/orgs/{org_id}/api-keys/{key_id}
        .route(
            "/api/v1/orgs/{org_id}/api-keys/{key_id}",
            delete(handle_delete_api_key),
        )
        // Member management
        .route(
            "/api/v1/orgs/{org_id}/members",
            post(handle_invite_member).get(handle_list_members),
        )
        .route(
            "/api/v1/orgs/{org_id}/members/{user_id}",
            delete(handle_remove_member).put(handle_update_member_role),
        )
        // Sub-tenant management
        .route(
            "/api/v1/orgs/{org_id}/sub-tenants",
            post(handle_create_sub_tenant).get(handle_list_sub_tenants),
        )
        // Schema info
        .route("/api/v1/orgs/{org_id}/schema", get(handle_get_schema_info))
        // Pipeline visibility (hierarchy-aware)
        .route(
            "/api/v1/orgs/{org_id}/pipelines",
            get(handle_list_org_pipelines),
        )
        .with_state(state)
}
