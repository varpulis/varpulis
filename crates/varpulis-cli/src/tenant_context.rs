//! Unified tenant context middleware for Varpulis Cloud.
//!
//! Replaces ad-hoc `extract_claims()` / `extract_admin_claims()` with a single
//! axum extractor that resolves user identity, org membership, and org status
//! from JWT or API key authentication.

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use uuid::Uuid;

use crate::oauth::{self, SharedOAuthState};

/// Resolved tenant context available to all handlers.
///
/// Extraction order:
/// 1. `Authorization: Bearer <jwt>` — decode JWT, get user_id + org_id
/// 2. `X-API-Key` or `Authorization: ApiKey <key>` — SHA-256 hash, lookup → org_id
/// 3. `X-Org-Id` header override — if present AND user has membership, use it
#[derive(Debug, Clone)]
pub struct TenantContext {
    /// Database user UUID.
    pub user_id: Uuid,
    /// Active organization UUID.
    pub org_id: Uuid,
    /// Per-org role from `org_members` (owner, admin, member, viewer).
    pub org_role: String,
    /// Global role from `users.role` (admin, user).
    pub global_role: String,
    /// Organization status (active, trial, suspended, revoked).
    pub org_status: String,
    /// How the request was authenticated (jwt, api_key).
    pub auth_method: String,
}

impl TenantContext {
    /// Returns true if the user has global admin privileges.
    pub fn is_global_admin(&self) -> bool {
        self.global_role == "admin"
    }

    /// Returns true if the user has at least org-admin level access.
    pub fn is_org_admin(&self) -> bool {
        self.is_global_admin() || self.org_role == "owner" || self.org_role == "admin"
    }

    /// Returns true if the org is in a usable state (active or trial).
    pub fn is_org_active(&self) -> bool {
        self.org_status == "active" || self.org_status == "trial"
    }
}

/// Shared state required for `TenantContext` extraction.
/// Must be present in the axum Router state via extension.
#[derive(Clone, Debug)]
pub struct TenantContextDeps {
    pub db_pool: varpulis_db::PgPool,
    pub oauth_state: SharedOAuthState,
}

impl<S> FromRequestParts<S> for TenantContext
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // Get dependencies from extensions
        let deps = parts
            .extensions
            .get::<TenantContextDeps>()
            .cloned()
            .ok_or_else(|| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(serde_json::json!({"error": "TenantContext not configured"})),
                )
                    .into_response()
            })?;

        let pool = &deps.db_pool;
        let oauth_state = &deps.oauth_state;

        // --- Step 1: Try JWT from Authorization header or cookie ---
        let auth_header = parts
            .headers
            .get("authorization")
            .and_then(|v| v.to_str().ok());

        let cookie_header = parts.headers.get("cookie").and_then(|v| v.to_str().ok());

        // Try API key first (X-API-Key header or Authorization: ApiKey <key>)
        let api_key_value = parts
            .headers
            .get("x-api-key")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .or_else(|| {
                auth_header.and_then(|h| h.strip_prefix("ApiKey ").map(|k| k.trim().to_string()))
            });

        if let Some(raw_key) = api_key_value {
            return resolve_from_api_key(pool, &raw_key).await;
        }

        // Try JWT from Bearer token or cookie
        let jwt_token = auth_header
            .and_then(|h| h.strip_prefix("Bearer ").map(|t| t.trim().to_string()))
            .or_else(|| cookie_header.and_then(oauth::extract_jwt_from_cookie));

        let token = jwt_token.ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                axum::Json(serde_json::json!({"error": "Missing authentication"})),
            )
                .into_response()
        })?;

        if token.is_empty() {
            return Err((
                StatusCode::UNAUTHORIZED,
                axum::Json(serde_json::json!({"error": "Empty token"})),
            )
                .into_response());
        }

        // Check session revocation
        let hash = oauth::token_hash(&token);
        if oauth_state.sessions.read().await.is_revoked(&hash) {
            return Err((
                StatusCode::UNAUTHORIZED,
                axum::Json(serde_json::json!({"error": "Session revoked"})),
            )
                .into_response());
        }

        // Decode JWT
        let claims = {
            use jsonwebtoken::{decode, DecodingKey, Validation};
            decode::<oauth::Claims>(
                &token,
                &DecodingKey::from_secret(oauth_state.config.jwt_secret.as_bytes()),
                &Validation::default(),
            )
            .map_err(|_| {
                (
                    StatusCode::UNAUTHORIZED,
                    axum::Json(serde_json::json!({"error": "Invalid token"})),
                )
                    .into_response()
            })?
            .claims
        };

        let user_id: Uuid = claims.user_id.parse().map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                axum::Json(serde_json::json!({"error": "Invalid user_id in token"})),
            )
                .into_response()
        })?;

        // Determine org_id: prefer X-Org-Id header override, fall back to JWT claim
        let org_id_header = parts
            .headers
            .get("x-org-id")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<Uuid>().ok());

        let jwt_org_id: Option<Uuid> = if claims.org_id.is_empty() {
            None
        } else {
            claims.org_id.parse().ok()
        };

        let org_id = org_id_header.or(jwt_org_id).ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                axum::Json(serde_json::json!({"error": "No organization context"})),
            )
                .into_response()
        })?;

        // Look up org membership
        let org_role = if claims.role == "admin" {
            // Global admins get implicit owner access to all orgs
            "owner".to_string()
        } else {
            match varpulis_db::repo::get_user_org_membership(pool, user_id, org_id).await {
                Ok(Some(member)) if member.status == "active" => member.role,
                _ => {
                    // Fall back to legacy owner_id check
                    match varpulis_db::repo::get_organization(pool, org_id).await {
                        Ok(Some(org)) if org.owner_id == user_id => "owner".to_string(),
                        _ => {
                            return Err((
                                StatusCode::FORBIDDEN,
                                axum::Json(serde_json::json!({"error": "Not a member of this organization"})),
                            )
                                .into_response());
                        }
                    }
                }
            }
        };

        // Get org status
        let org_status = match varpulis_db::repo::get_organization(pool, org_id).await {
            Ok(Some(org)) => org.status,
            _ => "unknown".to_string(),
        };

        Ok(TenantContext {
            user_id,
            org_id,
            org_role,
            global_role: claims.role,
            org_status,
            auth_method: "jwt".to_string(),
        })
    }
}

/// Resolve tenant context from a raw API key.
async fn resolve_from_api_key(
    pool: &varpulis_db::PgPool,
    raw_key: &str,
) -> Result<TenantContext, Response> {
    use sha2::Digest;
    let key_hash = hex::encode(sha2::Sha256::digest(raw_key.as_bytes()));

    let api_key = varpulis_db::repo::get_api_key_by_hash(pool, &key_hash)
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(serde_json::json!({"error": "Internal error"})),
            )
                .into_response()
        })?
        .ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                axum::Json(serde_json::json!({"error": "Invalid API key"})),
            )
                .into_response()
        })?;

    // Touch last_used_at (best-effort, don't fail the request)
    let _ = varpulis_db::repo::touch_api_key(pool, api_key.id).await;

    // Get org status
    let org = varpulis_db::repo::get_organization(pool, api_key.org_id)
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(serde_json::json!({"error": "Internal error"})),
            )
                .into_response()
        })?
        .ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                axum::Json(serde_json::json!({"error": "Organization not found"})),
            )
                .into_response()
        })?;

    // API key auth uses the org owner as the user identity
    let user_id = api_key.created_by.unwrap_or(org.owner_id);

    Ok(TenantContext {
        user_id,
        org_id: api_key.org_id,
        org_role: "owner".to_string(), // API keys imply full org access
        global_role: "user".to_string(),
        org_status: org.status,
        auth_method: "api_key".to_string(),
    })
}
