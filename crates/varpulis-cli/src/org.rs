//! Organization and API key management endpoints for Varpulis Cloud (saas feature).

use std::sync::Arc;

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

#[derive(Clone)]
pub struct OrgState {
    pub db_pool: Option<varpulis_db::PgPool>,
    pub oauth_state: Option<SharedOAuthState>,
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

    Ok(token_data.claims)
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

#[derive(Debug, Deserialize)]
struct CreateApiKeyRequest {
    name: Option<String>,
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

    if claims.org_id != org_id && !claims.org_id.is_empty() {
        // Verify ownership
        if let Ok(Some(org)) = varpulis_db::repo::get_organization(&pool, org_uuid).await {
            let user_uuid: uuid::Uuid = match claims.user_id.parse() {
                Ok(id) => id,
                Err(_) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(serde_json::json!({"error": "Invalid user_id"})),
                    )
                        .into_response();
                }
            };
            if org.owner_id != user_uuid {
                return (
                    StatusCode::FORBIDDEN,
                    Json(serde_json::json!({"error": "Forbidden"})),
                )
                    .into_response();
            }
        }
    }

    // Generate 32 random bytes, hex encode with vpl_ prefix
    let raw_bytes: [u8; 32] = rand::random();
    let raw_key = format!("vpl_{}", hex::encode(raw_bytes));

    // SHA-256 hash for storage
    use sha2::Digest;
    let hash = hex::encode(sha2::Sha256::digest(raw_key.as_bytes()));

    let key_name = body.name.unwrap_or_else(|| "default".to_string());
    match varpulis_db::repo::create_api_key(&pool, org_uuid, &hash, &key_name).await {
        Ok(api_key) => (
            StatusCode::CREATED,
            Json(serde_json::json!({
                "id": api_key.id.to_string(),
                "key": raw_key,
                "name": api_key.name,
                "created_at": api_key.created_at.to_rfc3339(),
            })),
        )
            .into_response(),
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

    let _claims = match extract_claims(auth_header, &state.oauth_state).await {
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

    match varpulis_db::repo::list_api_keys(&pool, org_uuid).await {
        Ok(keys) => {
            let keys_json: Vec<serde_json::Value> = keys
                .iter()
                .map(|k| {
                    serde_json::json!({
                        "id": k.id.to_string(),
                        "name": k.name,
                        "created_at": k.created_at.to_rfc3339(),
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

    let _claims = match extract_claims(auth_header, &state.oauth_state).await {
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

    let _org_uuid: uuid::Uuid = match org_id.parse() {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "Invalid org_id"})),
            )
                .into_response();
        }
    };

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

    match varpulis_db::repo::delete_api_key(&pool, key_uuid).await {
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
// Route assembly
// ---------------------------------------------------------------------------

pub fn org_routes(
    db_pool: Option<varpulis_db::PgPool>,
    oauth_state: Option<SharedOAuthState>,
) -> Router {
    let state = OrgState {
        db_pool,
        oauth_state,
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
        .with_state(state)
}
