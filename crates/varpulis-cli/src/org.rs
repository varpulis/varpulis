//! Organization and API key management endpoints for Varpulis Cloud (saas feature).

use serde::Deserialize;
use warp::Filter;

use crate::oauth::{self, SharedOAuthState};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract and verify JWT claims from the Authorization header using the OAuth state.
async fn extract_claims(
    auth_header: Option<String>,
    oauth_state: &Option<SharedOAuthState>,
) -> Result<oauth::Claims, warp::http::StatusCode> {
    let state = oauth_state
        .as_ref()
        .ok_or(warp::http::StatusCode::SERVICE_UNAVAILABLE)?;

    let token = auth_header
        .and_then(|h| h.strip_prefix("Bearer ").map(|t| t.trim().to_string()))
        .ok_or(warp::http::StatusCode::UNAUTHORIZED)?;

    if token.is_empty() {
        return Err(warp::http::StatusCode::UNAUTHORIZED);
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
        return Err(warp::http::StatusCode::UNAUTHORIZED);
    }

    use jsonwebtoken::{decode, DecodingKey, Validation};
    let token_data = decode::<oauth::Claims>(
        &token,
        &DecodingKey::from_secret(state.config.jwt_secret.as_bytes()),
        &Validation::default(),
    )
    .map_err(|_| warp::http::StatusCode::UNAUTHORIZED)?;

    Ok(token_data.claims)
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

/// GET /api/v1/orgs — list user's organizations.
async fn handle_list_orgs(
    auth_header: Option<String>,
    db_pool: Option<varpulis_db::PgPool>,
    oauth_state: Option<SharedOAuthState>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let claims = match extract_claims(auth_header, &oauth_state).await {
        Ok(c) => c,
        Err(status) => {
            return Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({"error": "Unauthorized"})),
                status,
            ));
        }
    };

    let pool = match db_pool {
        Some(p) => p,
        None => {
            return Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({"error": "Database not configured"})),
                warp::http::StatusCode::SERVICE_UNAVAILABLE,
            ));
        }
    };

    let user_id: uuid::Uuid = match claims.user_id.parse() {
        Ok(id) => id,
        Err(_) => {
            return Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({"error": "Invalid user_id in token"})),
                warp::http::StatusCode::BAD_REQUEST,
            ));
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
            Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({"organizations": orgs_json})),
                warp::http::StatusCode::OK,
            ))
        }
        Err(e) => {
            tracing::error!("Failed to list orgs: {}", e);
            Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({"error": "Internal error"})),
                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
            ))
        }
    }
}

#[derive(Debug, Deserialize)]
struct CreateApiKeyRequest {
    name: Option<String>,
}

/// POST /api/v1/orgs/{org_id}/api-keys — generate a new API key.
async fn handle_create_api_key(
    org_id: String,
    body: CreateApiKeyRequest,
    auth_header: Option<String>,
    db_pool: Option<varpulis_db::PgPool>,
    oauth_state: Option<SharedOAuthState>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let claims = match extract_claims(auth_header, &oauth_state).await {
        Ok(c) => c,
        Err(status) => {
            return Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({"error": "Unauthorized"})),
                status,
            ));
        }
    };

    let pool = match db_pool {
        Some(p) => p,
        None => {
            return Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({"error": "Database not configured"})),
                warp::http::StatusCode::SERVICE_UNAVAILABLE,
            ));
        }
    };

    // Verify the org belongs to the user
    let org_uuid: uuid::Uuid = match org_id.parse() {
        Ok(id) => id,
        Err(_) => {
            return Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({"error": "Invalid org_id"})),
                warp::http::StatusCode::BAD_REQUEST,
            ));
        }
    };

    if claims.org_id != org_id && !claims.org_id.is_empty() {
        // Verify ownership
        if let Ok(Some(org)) = varpulis_db::repo::get_organization(&pool, org_uuid).await {
            let user_uuid: uuid::Uuid = match claims.user_id.parse() {
                Ok(id) => id,
                Err(_) => {
                    return Ok(warp::reply::with_status(
                        warp::reply::json(&serde_json::json!({"error": "Invalid user_id"})),
                        warp::http::StatusCode::BAD_REQUEST,
                    ));
                }
            };
            if org.owner_id != user_uuid {
                return Ok(warp::reply::with_status(
                    warp::reply::json(&serde_json::json!({"error": "Forbidden"})),
                    warp::http::StatusCode::FORBIDDEN,
                ));
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
        Ok(api_key) => Ok(warp::reply::with_status(
            warp::reply::json(&serde_json::json!({
                "id": api_key.id.to_string(),
                "key": raw_key,
                "name": api_key.name,
                "created_at": api_key.created_at.to_rfc3339(),
            })),
            warp::http::StatusCode::CREATED,
        )),
        Err(e) => {
            tracing::error!("Failed to create API key: {}", e);
            Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({"error": "Internal error"})),
                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
            ))
        }
    }
}

/// GET /api/v1/orgs/{org_id}/api-keys — list API keys (no secrets).
async fn handle_list_api_keys(
    org_id: String,
    auth_header: Option<String>,
    db_pool: Option<varpulis_db::PgPool>,
    oauth_state: Option<SharedOAuthState>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let _claims = match extract_claims(auth_header, &oauth_state).await {
        Ok(c) => c,
        Err(status) => {
            return Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({"error": "Unauthorized"})),
                status,
            ));
        }
    };

    let pool = match db_pool {
        Some(p) => p,
        None => {
            return Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({"error": "Database not configured"})),
                warp::http::StatusCode::SERVICE_UNAVAILABLE,
            ));
        }
    };

    let org_uuid: uuid::Uuid = match org_id.parse() {
        Ok(id) => id,
        Err(_) => {
            return Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({"error": "Invalid org_id"})),
                warp::http::StatusCode::BAD_REQUEST,
            ));
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
            Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({"api_keys": keys_json})),
                warp::http::StatusCode::OK,
            ))
        }
        Err(e) => {
            tracing::error!("Failed to list API keys: {}", e);
            Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({"error": "Internal error"})),
                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
            ))
        }
    }
}

/// DELETE /api/v1/orgs/{org_id}/api-keys/{key_id} — revoke an API key.
async fn handle_delete_api_key(
    org_id: String,
    key_id: String,
    auth_header: Option<String>,
    db_pool: Option<varpulis_db::PgPool>,
    oauth_state: Option<SharedOAuthState>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let _claims = match extract_claims(auth_header, &oauth_state).await {
        Ok(c) => c,
        Err(status) => {
            return Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({"error": "Unauthorized"})),
                status,
            ));
        }
    };

    let pool = match db_pool {
        Some(p) => p,
        None => {
            return Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({"error": "Database not configured"})),
                warp::http::StatusCode::SERVICE_UNAVAILABLE,
            ));
        }
    };

    let _org_uuid: uuid::Uuid = match org_id.parse() {
        Ok(id) => id,
        Err(_) => {
            return Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({"error": "Invalid org_id"})),
                warp::http::StatusCode::BAD_REQUEST,
            ));
        }
    };

    let key_uuid: uuid::Uuid = match key_id.parse() {
        Ok(id) => id,
        Err(_) => {
            return Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({"error": "Invalid key_id"})),
                warp::http::StatusCode::BAD_REQUEST,
            ));
        }
    };

    match varpulis_db::repo::delete_api_key(&pool, key_uuid).await {
        Ok(()) => Ok(warp::reply::with_status(
            warp::reply::json(&serde_json::json!({"ok": true})),
            warp::http::StatusCode::OK,
        )),
        Err(e) => {
            tracing::error!("Failed to delete API key: {}", e);
            Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({"error": "Internal error"})),
                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// Route assembly
// ---------------------------------------------------------------------------

pub fn org_routes(
    db_pool: Option<varpulis_db::PgPool>,
    oauth_state: Option<SharedOAuthState>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    let pool1 = db_pool.clone();
    let pool2 = db_pool.clone();
    let pool3 = db_pool.clone();
    let pool4 = db_pool;
    let oauth1 = oauth_state.clone();
    let oauth2 = oauth_state.clone();
    let oauth3 = oauth_state.clone();
    let oauth4 = oauth_state;

    // GET /api/v1/orgs
    let list_orgs = warp::path!("api" / "v1" / "orgs")
        .and(warp::get())
        .and(warp::header::optional::<String>("authorization"))
        .and(warp::any().map(move || pool1.clone()))
        .and(warp::any().map(move || oauth1.clone()))
        .and_then(handle_list_orgs);

    // POST /api/v1/orgs/{org_id}/api-keys
    let create_key = warp::path!("api" / "v1" / "orgs" / String / "api-keys")
        .and(warp::post())
        .and(warp::body::json())
        .and(warp::header::optional::<String>("authorization"))
        .and(warp::any().map(move || pool2.clone()))
        .and(warp::any().map(move || oauth2.clone()))
        .and_then(handle_create_api_key);

    // GET /api/v1/orgs/{org_id}/api-keys
    let list_keys = warp::path!("api" / "v1" / "orgs" / String / "api-keys")
        .and(warp::get())
        .and(warp::header::optional::<String>("authorization"))
        .and(warp::any().map(move || pool3.clone()))
        .and(warp::any().map(move || oauth3.clone()))
        .and_then(handle_list_api_keys);

    // DELETE /api/v1/orgs/{org_id}/api-keys/{key_id}
    let delete_key = warp::path!("api" / "v1" / "orgs" / String / "api-keys" / String)
        .and(warp::delete())
        .and(warp::header::optional::<String>("authorization"))
        .and(warp::any().map(move || pool4.clone()))
        .and(warp::any().map(move || oauth4.clone()))
        .and_then(handle_delete_api_key);

    list_orgs.or(create_key).or(list_keys).or(delete_key)
}
