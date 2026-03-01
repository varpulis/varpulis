//! OAuth/OIDC authentication module for Varpulis Cloud.
//!
//! Provides OAuth 2.0 flow with GitHub as the identity provider,
//! optional generic OIDC support, JWT session management, and axum route handlers.

use axum::extract::{Json, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::Router;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::audit::{AuditAction, AuditEntry, SharedAuditLogger};
use crate::users::SharedUserStore;

// ---------------------------------------------------------------------------
// Auth Provider trait
// ---------------------------------------------------------------------------

/// Standardized user info returned by any auth provider.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserInfo {
    /// Unique provider-side user identifier
    pub provider_id: String,
    /// Display name
    pub name: String,
    /// Login/username (provider-specific)
    pub login: String,
    /// Email address (may be empty)
    pub email: String,
    /// Avatar URL (may be empty)
    pub avatar: String,
}

/// Error type for OAuth provider operations.
///
/// Distinct from [`crate::auth::AuthError`] which covers API key/header authentication.
#[derive(Debug)]
pub struct OAuthError(pub String);

impl std::fmt::Display for OAuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OAuth error: {}", self.0)
    }
}

impl std::error::Error for OAuthError {}

/// Trait for pluggable authentication providers.
///
/// Implementations handle the OAuth/OIDC flow for a specific identity provider.
/// The engine uses this to abstract over GitHub OAuth, generic OIDC (Okta, Auth0,
/// Azure AD, Keycloak, etc.), and future providers.
#[async_trait::async_trait]
pub trait AuthProvider: Send + Sync {
    /// Provider name (e.g., "github", "oidc")
    fn name(&self) -> &str;

    /// Generate the authorization URL to redirect the user to.
    fn authorize_url(&self, redirect_uri: &str) -> String;

    /// Exchange an authorization code for user info.
    async fn exchange_code(&self, code: &str, redirect_uri: &str) -> Result<UserInfo, OAuthError>;
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// OAuth configuration loaded from environment variables.
#[derive(Debug, Clone)]
pub struct OAuthConfig {
    pub github_client_id: String,
    pub github_client_secret: String,
    pub jwt_secret: String,
    /// Where to redirect after successful OAuth callback (e.g. "http://localhost:5173")
    pub frontend_url: String,
    /// The base URL of this server for the callback (e.g. "http://localhost:9000")
    pub server_url: String,
}

impl OAuthConfig {
    /// Build config from environment variables.
    /// Returns None if required vars are not set (OAuth disabled).
    pub fn from_env() -> Option<Self> {
        let client_id = std::env::var("GITHUB_CLIENT_ID").ok()?;
        let client_secret = std::env::var("GITHUB_CLIENT_SECRET").ok()?;
        let jwt_secret =
            std::env::var("JWT_SECRET").unwrap_or_else(|_| crate::auth::generate_api_key());
        let frontend_url =
            std::env::var("FRONTEND_URL").unwrap_or_else(|_| "http://localhost:5173".to_string());
        let server_url =
            std::env::var("SERVER_URL").unwrap_or_else(|_| "http://localhost:9000".to_string());

        Some(Self {
            github_client_id: client_id,
            github_client_secret: client_secret,
            jwt_secret,
            frontend_url,
            server_url,
        })
    }
}

// ---------------------------------------------------------------------------
// JWT Claims
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,    // GitHub user ID or local user ID
    pub name: String,   // Display name
    pub login: String,  // GitHub username or local username
    pub avatar: String, // Avatar URL
    pub email: String,  // Email (may be empty)
    pub exp: usize,     // Expiration (Unix timestamp)
    pub iat: usize,     // Issued at
    #[serde(default)]
    pub user_id: String, // DB user UUID (empty when saas not enabled)
    #[serde(default)]
    pub org_id: String, // DB organization UUID (empty when saas not enabled)
    #[serde(default)]
    pub role: String, // "admin" | "operator" | "viewer"
    #[serde(default)]
    pub session_id: String, // For session revocation
    #[serde(default)]
    pub auth_method: String, // "local" | "github" | "oidc" | "apikey"
}

// ---------------------------------------------------------------------------
// GitHub OAuth Provider
// ---------------------------------------------------------------------------

/// GitHub OAuth 2.0 auth provider.
#[derive(Debug)]
pub struct GitHubOAuth {
    pub client_id: String,
    pub client_secret: String,
    http_client: reqwest::Client,
}

impl GitHubOAuth {
    pub fn new(client_id: String, client_secret: String) -> Self {
        Self {
            client_id,
            client_secret,
            http_client: reqwest::Client::new(),
        }
    }
}

#[async_trait::async_trait]
impl AuthProvider for GitHubOAuth {
    fn name(&self) -> &'static str {
        "github"
    }

    fn authorize_url(&self, redirect_uri: &str) -> String {
        format!(
            "https://github.com/login/oauth/authorize?client_id={}&redirect_uri={}&scope=read:user%20user:email",
            self.client_id,
            urlencoding::encode(redirect_uri),
        )
    }

    async fn exchange_code(&self, code: &str, redirect_uri: &str) -> Result<UserInfo, OAuthError> {
        // Exchange authorization code for access token
        let token_resp = self
            .http_client
            .post("https://github.com/login/oauth/access_token")
            .header("Accept", "application/json")
            .form(&[
                ("client_id", self.client_id.as_str()),
                ("client_secret", self.client_secret.as_str()),
                ("code", code),
                ("redirect_uri", redirect_uri),
            ])
            .send()
            .await
            .map_err(|e| OAuthError(format!("GitHub token exchange failed: {e}")))?;

        let token_data: GitHubTokenResponse = token_resp
            .json()
            .await
            .map_err(|e| OAuthError(format!("Failed to parse GitHub token response: {e}")))?;

        // Fetch user profile
        let user: GitHubUser = self
            .http_client
            .get("https://api.github.com/user")
            .header(
                "Authorization",
                format!("Bearer {}", token_data.access_token),
            )
            .header("User-Agent", "Varpulis")
            .send()
            .await
            .map_err(|e| OAuthError(format!("GitHub user fetch failed: {e}")))?
            .json()
            .await
            .map_err(|e| OAuthError(format!("Failed to parse GitHub user: {e}")))?;

        Ok(UserInfo {
            provider_id: user.id.to_string(),
            name: user.name.clone().unwrap_or_else(|| user.login.clone()),
            login: user.login,
            email: user.email.unwrap_or_default(),
            avatar: user.avatar_url,
        })
    }
}

// ---------------------------------------------------------------------------
// GitHub API response types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct GitHubTokenResponse {
    access_token: String,
    #[allow(dead_code)]
    token_type: String,
}

#[derive(Debug, Deserialize)]
struct GitHubUser {
    id: u64,
    login: String,
    name: Option<String>,
    avatar_url: String,
    email: Option<String>,
}

// ---------------------------------------------------------------------------
// Session store (invalidated tokens)
// ---------------------------------------------------------------------------

/// Tracks invalidated JWT tokens (logout).
/// In production this would be backed by Redis/DB, but for MVP an in-memory
/// set is sufficient.
#[derive(Debug)]
pub struct SessionStore {
    /// Set of invalidated JTIs (JWT IDs) or raw token hashes.
    revoked: HashMap<String, std::time::Instant>,
}

impl Default for SessionStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionStore {
    pub fn new() -> Self {
        Self {
            revoked: HashMap::new(),
        }
    }

    pub fn revoke(&mut self, token_hash: String) {
        self.revoked.insert(token_hash, std::time::Instant::now());
    }

    pub fn is_revoked(&self, token_hash: &str) -> bool {
        self.revoked.contains_key(token_hash)
    }

    /// Remove entries older than 24 hours (tokens expire anyway).
    pub fn cleanup(&mut self) {
        let cutoff = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_secs(86400))
            .unwrap();
        self.revoked.retain(|_, instant| *instant > cutoff);
    }
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

pub type SharedOAuthState = Arc<OAuthState>;

#[derive(Debug)]
pub struct OAuthState {
    pub config: OAuthConfig,
    pub sessions: RwLock<SessionStore>,
    pub http_client: reqwest::Client,
    #[cfg(feature = "saas")]
    pub db_pool: Option<varpulis_db::PgPool>,
    pub audit_logger: Option<SharedAuditLogger>,
    pub user_store: Option<SharedUserStore>,
}

impl OAuthState {
    pub fn new(config: OAuthConfig) -> Self {
        Self {
            config,
            sessions: RwLock::new(SessionStore::new()),
            http_client: reqwest::Client::new(),
            #[cfg(feature = "saas")]
            db_pool: None,
            audit_logger: None,
            user_store: None,
        }
    }

    pub fn with_audit_logger(mut self, logger: Option<SharedAuditLogger>) -> Self {
        self.audit_logger = logger;
        self
    }

    pub fn with_user_store(mut self, store: SharedUserStore) -> Self {
        self.user_store = Some(store);
        self
    }

    #[cfg(feature = "saas")]
    pub fn with_db_pool(mut self, pool: varpulis_db::PgPool) -> Self {
        self.db_pool = Some(pool);
        self
    }
}

// ---------------------------------------------------------------------------
// JWT helpers
// ---------------------------------------------------------------------------

fn create_jwt(
    config: &OAuthConfig,
    user: &GitHubUser,
    user_id: &str,
    org_id: &str,
) -> Result<String, jsonwebtoken::errors::Error> {
    use jsonwebtoken::{encode, EncodingKey, Header};

    let now = chrono::Utc::now().timestamp() as usize;
    let claims = Claims {
        sub: user.id.to_string(),
        name: user.name.clone().unwrap_or_else(|| user.login.clone()),
        login: user.login.clone(),
        avatar: user.avatar_url.clone(),
        email: user.email.clone().unwrap_or_default(),
        exp: now + 86400 * 7, // 7 days
        iat: now,
        user_id: user_id.to_string(),
        org_id: org_id.to_string(),
        role: String::new(),
        session_id: String::new(),
        auth_method: "github".to_string(),
    };

    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(config.jwt_secret.as_bytes()),
    )
}

/// Create a JWT for a local (username/password) user with session tracking.
#[allow(clippy::too_many_arguments)]
pub fn create_jwt_for_local_user(
    config: &OAuthConfig,
    user_id: &str,
    username: &str,
    display_name: &str,
    email: &str,
    role: &str,
    session_id: &str,
    ttl_secs: usize,
) -> Result<String, jsonwebtoken::errors::Error> {
    use jsonwebtoken::{encode, EncodingKey, Header};

    let now = chrono::Utc::now().timestamp() as usize;
    let claims = Claims {
        sub: user_id.to_string(),
        name: display_name.to_string(),
        login: username.to_string(),
        avatar: String::new(),
        email: email.to_string(),
        exp: now + ttl_secs,
        iat: now,
        user_id: user_id.to_string(),
        org_id: String::new(),
        role: role.to_string(),
        session_id: session_id.to_string(),
        auth_method: "local".to_string(),
    };

    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(config.jwt_secret.as_bytes()),
    )
}

pub fn verify_jwt(
    config: &OAuthConfig,
    token: &str,
) -> Result<Claims, jsonwebtoken::errors::Error> {
    use jsonwebtoken::{decode, DecodingKey, Validation};

    let token_data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(config.jwt_secret.as_bytes()),
        &Validation::default(),
    )?;

    Ok(token_data.claims)
}

/// Simple hash for token revocation tracking (not cryptographic, just for lookup).
pub fn token_hash(token: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    token.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

// ---------------------------------------------------------------------------
// Cookie helpers
// ---------------------------------------------------------------------------

const COOKIE_NAME: &str = "varpulis_session";

/// Create a Set-Cookie header value for the session JWT.
fn create_session_cookie(jwt: &str, max_age_secs: u64) -> String {
    format!(
        "{COOKIE_NAME}={jwt}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age={max_age_secs}"
    )
}

/// Create a Set-Cookie header value that clears the session cookie.
fn clear_session_cookie() -> String {
    format!("{COOKIE_NAME}=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0")
}

/// Extract the session JWT from a Cookie header value.
pub fn extract_jwt_from_cookie(cookie_header: &str) -> Option<String> {
    for cookie in cookie_header.split(';') {
        let cookie = cookie.trim();
        if let Some(value) = cookie.strip_prefix("varpulis_session=") {
            let value = value.trim();
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

/// GET /auth/github — redirect user to GitHub OAuth authorization page.
async fn handle_github_redirect(State(state): State<Option<SharedOAuthState>>) -> Response {
    let state = match state {
        Some(s) => s,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "OAuth not configured"})),
            )
                .into_response();
        }
    };

    let redirect_uri = format!("{}/auth/github/callback", state.config.server_url);
    let url = format!(
        "https://github.com/login/oauth/authorize?client_id={}&redirect_uri={}&scope=read:user%20user:email",
        state.config.github_client_id,
        urlencoding::encode(&redirect_uri),
    );

    Redirect::temporary(&url).into_response()
}

/// Query params for the OAuth callback.
#[derive(Debug, Deserialize)]
struct CallbackQuery {
    code: String,
}

/// GET /auth/github/callback?code=... — exchange code for token, fetch user, issue JWT.
async fn handle_github_callback(
    State(state): State<Option<SharedOAuthState>>,
    Query(query): Query<CallbackQuery>,
) -> Response {
    let state = match state {
        Some(s) => s,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "OAuth not configured"})),
            )
                .into_response();
        }
    };

    let redirect_uri = format!("{}/auth/github/callback", state.config.server_url);

    // Exchange authorization code for access token
    let token_resp = match state
        .http_client
        .post("https://github.com/login/oauth/access_token")
        .header("Accept", "application/json")
        .form(&[
            ("client_id", state.config.github_client_id.as_str()),
            ("client_secret", state.config.github_client_secret.as_str()),
            ("code", query.code.as_str()),
            ("redirect_uri", redirect_uri.as_str()),
        ])
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(e) => {
            tracing::error!("GitHub token exchange failed: {}", e);
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({"error": "GitHub token exchange failed"})),
            )
                .into_response();
        }
    };

    let token_data: GitHubTokenResponse = match token_resp.json().await {
        Ok(data) => data,
        Err(e) => {
            tracing::error!("Failed to parse GitHub token response: {}", e);
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({"error": "Failed to parse GitHub token response"})),
            )
                .into_response();
        }
    };

    // Fetch user profile
    let user: GitHubUser = match state
        .http_client
        .get("https://api.github.com/user")
        .header(
            "Authorization",
            format!("Bearer {}", token_data.access_token),
        )
        .header("User-Agent", "Varpulis")
        .send()
        .await
    {
        Ok(resp) => match resp.json().await {
            Ok(user) => user,
            Err(e) => {
                tracing::error!("Failed to parse GitHub user: {}", e);
                return (
                    StatusCode::BAD_GATEWAY,
                    Json(serde_json::json!({"error": "Failed to parse GitHub user"})),
                )
                    .into_response();
            }
        },
        Err(e) => {
            tracing::error!("GitHub user fetch failed: {}", e);
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({"error": "GitHub user fetch failed"})),
            )
                .into_response();
        }
    };

    // DB integration: upsert user and auto-create org
    let (db_user_id, db_org_id) = {
        #[cfg(feature = "saas")]
        {
            if let Some(ref pool) = state.db_pool {
                match upsert_user_and_org(pool, &user).await {
                    Ok((uid, oid)) => (uid, oid),
                    Err(e) => {
                        tracing::error!("DB user/org upsert failed: {}", e);
                        (String::new(), String::new())
                    }
                }
            } else {
                (String::new(), String::new())
            }
        }
        #[cfg(not(feature = "saas"))]
        {
            (String::new(), String::new())
        }
    };

    // Create JWT
    let jwt = match create_jwt(&state.config, &user, &db_user_id, &db_org_id) {
        Ok(token) => token,
        Err(e) => {
            tracing::error!("JWT creation failed: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "JWT creation failed"})),
            )
                .into_response();
        }
    };

    tracing::info!("OAuth login: {} ({})", user.login, user.id);

    // Audit log: successful login
    if let Some(ref logger) = state.audit_logger {
        logger
            .log(
                AuditEntry::new(&user.login, AuditAction::Login, "/auth/github/callback")
                    .with_detail(format!("GitHub user ID: {}", user.id)),
            )
            .await;
    }

    // Redirect to frontend with JWT as query parameter
    let redirect_url = format!("{}/?token={}", state.config.frontend_url, jwt);
    Redirect::temporary(&redirect_url).into_response()
}

/// Upsert user in DB and auto-create a default org if none exist.
#[cfg(feature = "saas")]
async fn upsert_user_and_org(
    pool: &varpulis_db::PgPool,
    github_user: &GitHubUser,
) -> Result<(String, String), String> {
    let db_user = varpulis_db::repo::create_or_update_user(
        pool,
        &github_user.id.to_string(),
        github_user.email.as_deref().unwrap_or(""),
        github_user.name.as_deref().unwrap_or(&github_user.login),
        &github_user.avatar_url,
    )
    .await
    .map_err(|e| e.to_string())?;

    let orgs = varpulis_db::repo::get_user_organizations(pool, db_user.id)
        .await
        .map_err(|e| e.to_string())?;

    let org = if orgs.is_empty() {
        let org_name = format!("{}'s org", github_user.login);
        varpulis_db::repo::create_organization(pool, db_user.id, &org_name)
            .await
            .map_err(|e| e.to_string())?
    } else {
        orgs.into_iter().next().unwrap()
    };

    tracing::info!(
        "DB upsert: user={} org={} ({})",
        db_user.id,
        org.id,
        org.name
    );

    Ok((db_user.id.to_string(), org.id.to_string()))
}

/// POST /auth/logout — invalidate JWT and clear session cookie.
async fn handle_logout(
    State(state): State<Option<SharedOAuthState>>,
    headers: HeaderMap,
) -> Response {
    let state = match state {
        Some(s) => s,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "OAuth not configured"})),
            )
                .into_response();
        }
    };

    let auth_header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let cookie_header = headers
        .get("cookie")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Extract token from cookie or Authorization header
    let token = cookie_header
        .as_deref()
        .and_then(extract_jwt_from_cookie)
        .or_else(|| {
            auth_header
                .as_ref()
                .map(|h| h.strip_prefix("Bearer ").unwrap_or(h).trim().to_string())
        });

    if let Some(token) = token {
        if !token.is_empty() {
            // Revoke session in user store if it's a local auth session
            if let Ok(claims) = verify_jwt(&state.config, &token) {
                if claims.auth_method == "local" && !claims.session_id.is_empty() {
                    if let Some(ref user_store) = state.user_store {
                        user_store.write().await.revoke_session(&claims.session_id);
                    }
                }
            }

            let hash = token_hash(&token);
            state.sessions.write().await.revoke(hash);

            // Audit log: logout
            if let Some(ref logger) = state.audit_logger {
                logger
                    .log(AuditEntry::new(
                        "session",
                        AuditAction::Logout,
                        "/auth/logout",
                    ))
                    .await;
            }
        }
    }

    (
        StatusCode::OK,
        [("set-cookie", clear_session_cookie())],
        Json(serde_json::json!({ "ok": true })),
    )
        .into_response()
}

/// GET /api/v1/me — return current user from JWT (cookie or Bearer header).
async fn handle_me(State(state): State<Option<SharedOAuthState>>, headers: HeaderMap) -> Response {
    let state = match state {
        Some(s) => s,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "OAuth not configured"})),
            )
                .into_response();
        }
    };

    let auth_header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let cookie_header = headers
        .get("cookie")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Extract token from cookie or Authorization header
    let token = cookie_header
        .as_deref()
        .and_then(extract_jwt_from_cookie)
        .or_else(|| {
            auth_header
                .as_ref()
                .map(|h| h.strip_prefix("Bearer ").unwrap_or(h).trim().to_string())
        });

    let token = match token {
        Some(t) if !t.is_empty() => t,
        _ => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "No token provided" })),
            )
                .into_response();
        }
    };

    // Check revocation
    let hash = token_hash(&token);
    if state.sessions.read().await.is_revoked(&hash) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "Token revoked" })),
        )
            .into_response();
    }

    // Verify JWT
    match verify_jwt(&state.config, &token) {
        Ok(claims) => {
            #[allow(unused_mut)]
            let mut response = serde_json::json!({
                "id": claims.sub,
                "name": claims.name,
                "login": claims.login,
                "avatar": claims.avatar,
                "email": claims.email,
                "user_id": claims.user_id,
                "org_id": claims.org_id,
                "role": claims.role,
                "auth_method": claims.auth_method,
            });

            // Enrich with DB data when saas is enabled
            #[cfg(feature = "saas")]
            if let Some(ref pool) = state.db_pool {
                if !claims.user_id.is_empty() {
                    if let Ok(user_uuid) = claims.user_id.parse::<uuid::Uuid>() {
                        if let Ok(orgs) =
                            varpulis_db::repo::get_user_organizations(pool, user_uuid).await
                        {
                            let orgs_json: Vec<serde_json::Value> = orgs
                                .iter()
                                .map(|o| {
                                    serde_json::json!({
                                        "id": o.id.to_string(),
                                        "name": o.name,
                                        "tier": o.tier,
                                    })
                                })
                                .collect();
                            response["organizations"] = serde_json::json!(orgs_json);
                        }
                    }
                }
            }

            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            tracing::debug!("JWT verification failed: {}", e);
            (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "Invalid token" })),
            )
                .into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// Local auth route handlers
// ---------------------------------------------------------------------------

/// Login request body.
#[derive(Debug, Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

/// POST /auth/login — authenticate with username/password, return JWT in cookie.
async fn handle_login(
    State(state): State<Option<SharedOAuthState>>,
    Json(body): Json<LoginRequest>,
) -> Response {
    let state = match state {
        Some(s) => s,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({ "error": "OAuth not configured" })),
            )
                .into_response();
        }
    };

    let user_store = match &state.user_store {
        Some(store) => store.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({ "error": "Local auth not configured" })),
            )
                .into_response();
        }
    };

    let mut store = user_store.write().await;

    let user = match store.verify_password(&body.username, &body.password) {
        Ok(u) => u,
        Err(e) => {
            // Audit: failed login
            if let Some(ref logger) = state.audit_logger {
                logger
                    .log(
                        AuditEntry::new(&body.username, AuditAction::Login, "/auth/login")
                            .with_outcome(crate::audit::AuditOutcome::Failure)
                            .with_detail(e.clone()),
                    )
                    .await;
            }
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "Invalid username or password" })),
            )
                .into_response();
        }
    };

    let session = store.create_session(&user);
    let ttl_secs = store.session_config().absolute_timeout.as_secs() as usize;
    drop(store);

    let jwt = match create_jwt_for_local_user(
        &state.config,
        &user.id,
        &user.username,
        &user.display_name,
        &user.email,
        &user.role,
        &session.session_id,
        ttl_secs,
    ) {
        Ok(token) => token,
        Err(e) => {
            tracing::error!("JWT creation failed: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "Internal server error" })),
            )
                .into_response();
        }
    };

    // Audit: successful login
    if let Some(ref logger) = state.audit_logger {
        logger
            .log(
                AuditEntry::new(&user.username, AuditAction::Login, "/auth/login")
                    .with_detail(format!("session: {}", session.session_id)),
            )
            .await;
    }

    let cookie = create_session_cookie(&jwt, ttl_secs as u64);
    let response = serde_json::json!({
        "ok": true,
        "user": {
            "id": user.id,
            "username": user.username,
            "display_name": user.display_name,
            "email": user.email,
            "role": user.role,
        },
        "token": jwt,
    });

    (StatusCode::OK, [("set-cookie", cookie)], Json(response)).into_response()
}

/// POST /auth/renew — renew session, issue new JWT in cookie.
async fn handle_renew(
    State(state): State<Option<SharedOAuthState>>,
    headers: HeaderMap,
) -> Response {
    let state = match state {
        Some(s) => s,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "OAuth not configured"})),
            )
                .into_response();
        }
    };

    let auth_header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let cookie_header = headers
        .get("cookie")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Extract JWT from cookie or Authorization header
    let token = cookie_header
        .as_deref()
        .and_then(extract_jwt_from_cookie)
        .or_else(|| {
            auth_header
                .as_ref()
                .map(|h| h.strip_prefix("Bearer ").unwrap_or(h).trim().to_string())
        });

    let token = match token {
        Some(t) if !t.is_empty() => t,
        _ => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "No session token" })),
            )
                .into_response();
        }
    };

    // Verify existing JWT
    let claims = match verify_jwt(&state.config, &token) {
        Ok(c) => c,
        Err(_) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "Invalid or expired token" })),
            )
                .into_response();
        }
    };

    // Only renew local auth sessions
    if claims.auth_method != "local" || claims.session_id.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "Session renewal not applicable" })),
        )
            .into_response();
    }

    let user_store = match &state.user_store {
        Some(store) => store.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({ "error": "Local auth not configured" })),
            )
                .into_response();
        }
    };

    let mut store = user_store.write().await;

    // Validate existing session
    if store.validate_session(&claims.session_id).is_none() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "Session expired or revoked" })),
        )
            .into_response();
    }

    // Look up user to get current role (may have been updated)
    let user = match store.get_user_by_id(&claims.sub) {
        Some(u) => u.clone(),
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "User not found" })),
            )
                .into_response();
        }
    };

    let ttl_secs = store.session_config().absolute_timeout.as_secs() as usize;
    drop(store);

    // Revoke old token and issue new one with same session
    let hash = token_hash(&token);
    state.sessions.write().await.revoke(hash);

    let jwt = match create_jwt_for_local_user(
        &state.config,
        &user.id,
        &user.username,
        &user.display_name,
        &user.email,
        &user.role,
        &claims.session_id,
        ttl_secs,
    ) {
        Ok(t) => t,
        Err(e) => {
            tracing::error!("JWT renewal failed: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "Internal server error" })),
            )
                .into_response();
        }
    };

    if let Some(ref logger) = state.audit_logger {
        logger
            .log(AuditEntry::new(
                &user.username,
                AuditAction::SessionRenew,
                "/auth/renew",
            ))
            .await;
    }

    let cookie = create_session_cookie(&jwt, ttl_secs as u64);

    (
        StatusCode::OK,
        [("set-cookie", cookie)],
        Json(serde_json::json!({
            "ok": true,
            "token": jwt,
        })),
    )
        .into_response()
}

/// Request body for creating a user.
#[derive(Debug, Deserialize)]
struct CreateUserRequest {
    username: String,
    password: String,
    display_name: String,
    #[serde(default)]
    email: String,
    #[serde(default = "default_role")]
    role: String,
}

fn default_role() -> String {
    "viewer".to_string()
}

/// POST /auth/users — create a new user (admin only).
async fn handle_create_user(
    State(state): State<Option<SharedOAuthState>>,
    headers: HeaderMap,
    Json(body): Json<CreateUserRequest>,
) -> Response {
    let state = match state {
        Some(s) => s,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "OAuth not configured"})),
            )
                .into_response();
        }
    };

    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());
    let cookie_header = headers.get("cookie").and_then(|v| v.to_str().ok());

    // Verify admin access
    let claims = match extract_and_verify_claims(&state, auth_header, cookie_header).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };

    if claims.role != "admin" {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "Admin access required" })),
        )
            .into_response();
    }

    let user_store = match &state.user_store {
        Some(s) => s.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({ "error": "Local auth not configured" })),
            )
                .into_response();
        }
    };

    let mut store = user_store.write().await;
    match store.create_user(
        &body.username,
        &body.password,
        &body.display_name,
        &body.email,
        &body.role,
    ) {
        Ok(user) => {
            if let Some(ref logger) = state.audit_logger {
                logger
                    .log(
                        AuditEntry::new(&claims.login, AuditAction::UserCreate, "/auth/users")
                            .with_detail(format!(
                                "Created user: {} ({})",
                                user.username, user.role
                            )),
                    )
                    .await;
            }

            (
                StatusCode::CREATED,
                Json(serde_json::json!({
                    "id": user.id,
                    "username": user.username,
                    "display_name": user.display_name,
                    "email": user.email,
                    "role": user.role,
                })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": e })),
        )
            .into_response(),
    }
}

/// GET /auth/users — list all users (admin only).
async fn handle_list_users(
    State(state): State<Option<SharedOAuthState>>,
    headers: HeaderMap,
) -> Response {
    let state = match state {
        Some(s) => s,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "OAuth not configured"})),
            )
                .into_response();
        }
    };

    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());
    let cookie_header = headers.get("cookie").and_then(|v| v.to_str().ok());

    let claims = match extract_and_verify_claims(&state, auth_header, cookie_header).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };

    if claims.role != "admin" {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "Admin access required" })),
        )
            .into_response();
    }

    let user_store = match &state.user_store {
        Some(s) => s.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({ "error": "Local auth not configured" })),
            )
                .into_response();
        }
    };

    let store = user_store.read().await;
    let users = store.list_users();

    (StatusCode::OK, Json(serde_json::json!({ "users": users }))).into_response()
}

/// Helper: extract JWT from cookie or Authorization header, verify it, check revocation.
async fn extract_and_verify_claims(
    state: &SharedOAuthState,
    auth_header: Option<&str>,
    cookie_header: Option<&str>,
) -> Result<Claims, Response> {
    let token = cookie_header
        .and_then(extract_jwt_from_cookie)
        .or_else(|| auth_header.map(|h| h.strip_prefix("Bearer ").unwrap_or(h).trim().to_string()));

    let token = match token {
        Some(t) if !t.is_empty() => t,
        _ => {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "Authentication required" })),
            )
                .into_response());
        }
    };

    // Check revocation
    let hash = token_hash(&token);
    if state.sessions.read().await.is_revoked(&hash) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "Token revoked" })),
        )
            .into_response());
    }

    verify_jwt(&state.config, &token).map_err(|_| {
        (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "Invalid or expired token" })),
        )
            .into_response()
    })
}

// ---------------------------------------------------------------------------
// Route assembly
// ---------------------------------------------------------------------------

/// Build OAuth/auth routes. When `state` is None, endpoints return 503.
pub fn oauth_routes(state: Option<SharedOAuthState>) -> Router {
    Router::new()
        // GET /auth/github
        .route("/auth/github", get(handle_github_redirect))
        // GET /auth/github/callback?code=...
        .route("/auth/github/callback", get(handle_github_callback))
        // POST /auth/login
        .route("/auth/login", post(handle_login))
        // POST /auth/renew
        .route("/auth/renew", post(handle_renew))
        // POST /auth/logout
        .route("/auth/logout", post(handle_logout))
        // GET /api/v1/me
        .route("/api/v1/me", get(handle_me))
        // POST /auth/users (admin only)
        // GET /auth/users (admin only)
        .route(
            "/auth/users",
            post(handle_create_user).get(handle_list_users),
        )
        .with_state(state)
}

/// Spawn a background task to periodically clean up revoked tokens.
pub fn spawn_session_cleanup(state: SharedOAuthState) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(3600));
        loop {
            interval.tick().await;
            state.sessions.write().await.cleanup();
        }
    });
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn get_req(uri: &str) -> Request<Body> {
        Request::builder()
            .method("GET")
            .uri(uri)
            .body(Body::empty())
            .unwrap()
    }

    #[test]
    fn test_jwt_roundtrip() {
        let config = OAuthConfig {
            github_client_id: "test".to_string(),
            github_client_secret: "test".to_string(),
            jwt_secret: "super-secret-key-for-testing".to_string(),
            frontend_url: "http://localhost:5173".to_string(),
            server_url: "http://localhost:9000".to_string(),
        };

        let user = GitHubUser {
            id: 12345,
            login: "testuser".to_string(),
            name: Some("Test User".to_string()),
            avatar_url: "https://example.com/avatar.png".to_string(),
            email: Some("test@example.com".to_string()),
        };

        let token = create_jwt(&config, &user, "", "").expect("JWT creation should succeed");
        let claims = verify_jwt(&config, &token).expect("JWT verification should succeed");

        assert_eq!(claims.sub, "12345");
        assert_eq!(claims.login, "testuser");
        assert_eq!(claims.name, "Test User");
        assert_eq!(claims.email, "test@example.com");
    }

    #[test]
    fn test_jwt_invalid_secret() {
        let config = OAuthConfig {
            github_client_id: "test".to_string(),
            github_client_secret: "test".to_string(),
            jwt_secret: "secret-1".to_string(),
            frontend_url: "http://localhost:5173".to_string(),
            server_url: "http://localhost:9000".to_string(),
        };

        let user = GitHubUser {
            id: 1,
            login: "u".to_string(),
            name: None,
            avatar_url: String::new(),
            email: None,
        };

        let token = create_jwt(&config, &user, "", "").unwrap();

        // Verify with different secret should fail
        let config2 = OAuthConfig {
            jwt_secret: "secret-2".to_string(),
            ..config
        };
        assert!(verify_jwt(&config2, &token).is_err());
    }

    #[test]
    fn test_session_store_revoke() {
        let mut store = SessionStore::new();
        let hash = "abc123".to_string();

        assert!(!store.is_revoked(&hash));
        store.revoke(hash.clone());
        assert!(store.is_revoked(&hash));
    }

    #[test]
    fn test_token_hash_deterministic() {
        let h1 = token_hash("my-token");
        let h2 = token_hash("my-token");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_token_hash_different_for_different_tokens() {
        let h1 = token_hash("token-a");
        let h2 = token_hash("token-b");
        assert_ne!(h1, h2);
    }

    #[tokio::test]
    async fn test_me_endpoint_no_token() {
        let config = OAuthConfig {
            github_client_id: "test".to_string(),
            github_client_secret: "test".to_string(),
            jwt_secret: "test-secret".to_string(),
            frontend_url: "http://localhost:5173".to_string(),
            server_url: "http://localhost:9000".to_string(),
        };
        let state = Arc::new(OAuthState::new(config));
        let app = oauth_routes(Some(state));

        let res = app.oneshot(get_req("/api/v1/me")).await.unwrap();

        assert_eq!(res.status(), 401);
    }

    #[tokio::test]
    async fn test_me_endpoint_valid_token() {
        let config = OAuthConfig {
            github_client_id: "test".to_string(),
            github_client_secret: "test".to_string(),
            jwt_secret: "test-secret".to_string(),
            frontend_url: "http://localhost:5173".to_string(),
            server_url: "http://localhost:9000".to_string(),
        };

        let user = GitHubUser {
            id: 42,
            login: "octocat".to_string(),
            name: Some("Octocat".to_string()),
            avatar_url: "https://github.com/octocat.png".to_string(),
            email: Some("octocat@github.com".to_string()),
        };

        let token = create_jwt(&config, &user, "", "").unwrap();
        let state = Arc::new(OAuthState::new(config));
        let app = oauth_routes(Some(state));

        let req: Request<Body> = Request::builder()
            .method("GET")
            .uri("/api/v1/me")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), 200);
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["login"], "octocat");
        assert_eq!(body["name"], "Octocat");
    }

    #[tokio::test]
    async fn test_me_endpoint_revoked_token() {
        let config = OAuthConfig {
            github_client_id: "test".to_string(),
            github_client_secret: "test".to_string(),
            jwt_secret: "test-secret".to_string(),
            frontend_url: "http://localhost:5173".to_string(),
            server_url: "http://localhost:9000".to_string(),
        };

        let user = GitHubUser {
            id: 42,
            login: "octocat".to_string(),
            name: Some("Octocat".to_string()),
            avatar_url: "https://github.com/octocat.png".to_string(),
            email: Some("octocat@github.com".to_string()),
        };

        let token = create_jwt(&config, &user, "", "").unwrap();
        let state = Arc::new(OAuthState::new(config));

        // Revoke the token
        let hash = token_hash(&token);
        state.sessions.write().await.revoke(hash);

        let app = oauth_routes(Some(state));

        let req: Request<Body> = Request::builder()
            .method("GET")
            .uri("/api/v1/me")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), 401);
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"], "Token revoked");
    }

    #[tokio::test]
    async fn test_logout_endpoint() {
        let config = OAuthConfig {
            github_client_id: "test".to_string(),
            github_client_secret: "test".to_string(),
            jwt_secret: "test-secret".to_string(),
            frontend_url: "http://localhost:5173".to_string(),
            server_url: "http://localhost:9000".to_string(),
        };
        let state = Arc::new(OAuthState::new(config));
        let app = oauth_routes(Some(state));

        let req: Request<Body> = Request::builder()
            .method("POST")
            .uri("/auth/logout")
            .header("authorization", "Bearer some-token")
            .body(Body::empty())
            .unwrap();
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), 200);
        let set_cookie = res.headers().get("set-cookie").unwrap().to_str().unwrap();
        assert!(set_cookie.contains("Max-Age=0"));
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["ok"], true);
    }

    #[test]
    fn test_extract_jwt_from_cookie() {
        assert_eq!(
            extract_jwt_from_cookie("varpulis_session=abc123"),
            Some("abc123".to_string())
        );
        assert_eq!(
            extract_jwt_from_cookie("other=foo; varpulis_session=abc123; more=bar"),
            Some("abc123".to_string())
        );
        assert_eq!(extract_jwt_from_cookie("other=foo"), None);
        assert_eq!(extract_jwt_from_cookie("varpulis_session="), None);
    }

    #[test]
    fn test_local_jwt_roundtrip() {
        let config = OAuthConfig {
            github_client_id: "test".to_string(),
            github_client_secret: "test".to_string(),
            jwt_secret: "test-secret-key-32chars-minimum!!".to_string(),
            frontend_url: "http://localhost:5173".to_string(),
            server_url: "http://localhost:9000".to_string(),
        };

        let token = create_jwt_for_local_user(
            &config,
            "user-123",
            "alice",
            "Alice Smith",
            "alice@example.com",
            "admin",
            "session-456",
            3600,
        )
        .unwrap();

        let claims = verify_jwt(&config, &token).unwrap();
        assert_eq!(claims.sub, "user-123");
        assert_eq!(claims.login, "alice");
        assert_eq!(claims.name, "Alice Smith");
        assert_eq!(claims.role, "admin");
        assert_eq!(claims.session_id, "session-456");
        assert_eq!(claims.auth_method, "local");
    }

    #[tokio::test]
    async fn test_login_endpoint() {
        let config = OAuthConfig {
            github_client_id: "test".to_string(),
            github_client_secret: "test".to_string(),
            jwt_secret: "test-secret".to_string(),
            frontend_url: "http://localhost:5173".to_string(),
            server_url: "http://localhost:9000".to_string(),
        };

        let dir = tempfile::tempdir().unwrap();
        let user_store = Arc::new(RwLock::new(crate::users::UserStore::new(
            dir.path().join("users.json"),
            crate::users::SessionConfig::default(),
        )));

        // Create a test user
        user_store
            .write()
            .await
            .create_user(
                "testuser",
                "testpass123",
                "Test User",
                "test@test.com",
                "admin",
            )
            .unwrap();

        let state = Arc::new(OAuthState::new(config).with_user_store(user_store));
        let app = oauth_routes(Some(state));

        // Test successful login
        let req: Request<Body> = Request::builder()
            .method("POST")
            .uri("/auth/login")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"username":"testuser","password":"testpass123"}"#,
            ))
            .unwrap();
        let res = app.clone().oneshot(req).await.unwrap();

        assert_eq!(res.status(), 200);
        let set_cookie = res.headers().get("set-cookie").unwrap().to_str().unwrap();
        assert!(set_cookie.contains("varpulis_session="));
        assert!(set_cookie.contains("HttpOnly"));
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["ok"], true);
        assert_eq!(body["user"]["username"], "testuser");
        assert!(body["token"].is_string());

        // Test failed login
        let req: Request<Body> = Request::builder()
            .method("POST")
            .uri("/auth/login")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"username":"testuser","password":"wrongpass"}"#,
            ))
            .unwrap();
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), 401);
    }

    #[tokio::test]
    async fn test_me_endpoint_with_cookie() {
        let config = OAuthConfig {
            github_client_id: "test".to_string(),
            github_client_secret: "test".to_string(),
            jwt_secret: "test-secret".to_string(),
            frontend_url: "http://localhost:5173".to_string(),
            server_url: "http://localhost:9000".to_string(),
        };

        let token = create_jwt_for_local_user(
            &config,
            "user-1",
            "alice",
            "Alice",
            "alice@test.com",
            "admin",
            "sess-1",
            3600,
        )
        .unwrap();

        let state = Arc::new(OAuthState::new(config));
        let app = oauth_routes(Some(state));

        let req: Request<Body> = Request::builder()
            .method("GET")
            .uri("/api/v1/me")
            .header("cookie", format!("varpulis_session={token}"))
            .body(Body::empty())
            .unwrap();
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), 200);
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["login"], "alice");
        assert_eq!(body["role"], "admin");
        assert_eq!(body["auth_method"], "local");
    }
}
