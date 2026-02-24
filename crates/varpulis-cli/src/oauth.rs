//! GitHub OAuth authentication module for Varpulis Cloud.
//!
//! Provides OAuth 2.0 flow with GitHub as the identity provider,
//! JWT session management, and warp route filters.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use warp::Filter;

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
    pub sub: String,    // GitHub user ID
    pub name: String,   // Display name
    pub login: String,  // GitHub username
    pub avatar: String, // Avatar URL
    pub email: String,  // Email (may be empty)
    pub exp: usize,     // Expiration (Unix timestamp)
    pub iat: usize,     // Issued at
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
        let cutoff = std::time::Instant::now() - std::time::Duration::from_secs(86400);
        self.revoked.retain(|_, instant| *instant > cutoff);
    }
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

pub type SharedOAuthState = Arc<OAuthState>;

pub struct OAuthState {
    pub config: OAuthConfig,
    pub sessions: RwLock<SessionStore>,
    pub http_client: reqwest::Client,
}

impl OAuthState {
    pub fn new(config: OAuthConfig) -> Self {
        Self {
            config,
            sessions: RwLock::new(SessionStore::new()),
            http_client: reqwest::Client::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// JWT helpers
// ---------------------------------------------------------------------------

fn create_jwt(
    config: &OAuthConfig,
    user: &GitHubUser,
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
    };

    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(config.jwt_secret.as_bytes()),
    )
}

fn verify_jwt(config: &OAuthConfig, token: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
    use jsonwebtoken::{decode, DecodingKey, Validation};

    let token_data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(config.jwt_secret.as_bytes()),
        &Validation::default(),
    )?;

    Ok(token_data.claims)
}

/// Simple hash for token revocation tracking (not cryptographic, just for lookup).
fn token_hash(token: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    token.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

/// GET /auth/github — redirect user to GitHub OAuth authorization page.
async fn handle_github_redirect(
    state: SharedOAuthState,
) -> Result<impl warp::Reply, warp::Rejection> {
    let redirect_uri = format!("{}/auth/github/callback", state.config.server_url);
    let url = format!(
        "https://github.com/login/oauth/authorize?client_id={}&redirect_uri={}&scope=read:user%20user:email",
        state.config.github_client_id,
        urlencoding::encode(&redirect_uri),
    );

    Ok(warp::redirect::temporary(
        url.parse::<warp::http::Uri>().unwrap(),
    ))
}

/// Query params for the OAuth callback.
#[derive(Debug, Deserialize)]
struct CallbackQuery {
    code: String,
}

/// GET /auth/github/callback — exchange code for token, fetch user, issue JWT.
async fn handle_github_callback(
    query: CallbackQuery,
    state: SharedOAuthState,
) -> Result<impl warp::Reply, warp::Rejection> {
    let redirect_uri = format!("{}/auth/github/callback", state.config.server_url);

    // Exchange authorization code for access token
    let token_resp = state
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
        .map_err(|e| {
            tracing::error!("GitHub token exchange failed: {}", e);
            warp::reject::reject()
        })?;

    let token_data: GitHubTokenResponse = token_resp.json().await.map_err(|e| {
        tracing::error!("Failed to parse GitHub token response: {}", e);
        warp::reject::reject()
    })?;

    // Fetch user profile
    let user: GitHubUser = state
        .http_client
        .get("https://api.github.com/user")
        .header(
            "Authorization",
            format!("Bearer {}", token_data.access_token),
        )
        .header("User-Agent", "Varpulis")
        .send()
        .await
        .map_err(|e| {
            tracing::error!("GitHub user fetch failed: {}", e);
            warp::reject::reject()
        })?
        .json()
        .await
        .map_err(|e| {
            tracing::error!("Failed to parse GitHub user: {}", e);
            warp::reject::reject()
        })?;

    // Create JWT
    let jwt = create_jwt(&state.config, &user).map_err(|e| {
        tracing::error!("JWT creation failed: {}", e);
        warp::reject::reject()
    })?;

    tracing::info!("OAuth login: {} ({})", user.login, user.id);

    // Redirect to frontend with JWT as query parameter
    let redirect_url = format!("{}/?token={}", state.config.frontend_url, jwt);
    Ok(warp::redirect::temporary(
        redirect_url.parse::<warp::http::Uri>().unwrap(),
    ))
}

/// POST /auth/logout — invalidate JWT.
async fn handle_logout(
    auth_header: Option<String>,
    state: SharedOAuthState,
) -> Result<impl warp::Reply, warp::Rejection> {
    if let Some(header) = auth_header {
        let token = header.strip_prefix("Bearer ").unwrap_or(&header).trim();
        if !token.is_empty() {
            let hash = token_hash(token);
            state.sessions.write().await.revoke(hash);
        }
    }

    Ok(warp::reply::json(&serde_json::json!({ "ok": true })))
}

/// GET /api/v1/me — return current user from JWT.
async fn handle_me(
    auth_header: Option<String>,
    state: SharedOAuthState,
) -> Result<impl warp::Reply, warp::Rejection> {
    let token = match auth_header {
        Some(header) => {
            let t = header
                .strip_prefix("Bearer ")
                .unwrap_or(&header)
                .trim()
                .to_string();
            if t.is_empty() {
                return Ok(warp::reply::with_status(
                    warp::reply::json(&serde_json::json!({ "error": "No token provided" })),
                    warp::http::StatusCode::UNAUTHORIZED,
                ));
            }
            t
        }
        None => {
            return Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({ "error": "No token provided" })),
                warp::http::StatusCode::UNAUTHORIZED,
            ));
        }
    };

    // Check revocation
    let hash = token_hash(&token);
    if state.sessions.read().await.is_revoked(&hash) {
        return Ok(warp::reply::with_status(
            warp::reply::json(&serde_json::json!({ "error": "Token revoked" })),
            warp::http::StatusCode::UNAUTHORIZED,
        ));
    }

    // Verify JWT
    match verify_jwt(&state.config, &token) {
        Ok(claims) => Ok(warp::reply::with_status(
            warp::reply::json(&serde_json::json!({
                "id": claims.sub,
                "name": claims.name,
                "login": claims.login,
                "avatar": claims.avatar,
                "email": claims.email,
            })),
            warp::http::StatusCode::OK,
        )),
        Err(e) => {
            tracing::debug!("JWT verification failed: {}", e);
            Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({ "error": "Invalid token" })),
                warp::http::StatusCode::UNAUTHORIZED,
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// Route assembly
// ---------------------------------------------------------------------------

/// Build OAuth/auth routes. When `state` is None, endpoints return 503.
/// Always returns the same concrete filter type for warp route composition.
pub fn oauth_routes(
    state: Option<SharedOAuthState>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    let state1 = state.clone();
    let state2 = state.clone();
    let state3 = state.clone();
    let state4 = state;

    // GET /auth/github
    let github_redirect = warp::path!("auth" / "github")
        .and(warp::get())
        .and(warp::any().map(move || state1.clone()))
        .and_then(|state: Option<SharedOAuthState>| async move {
            match state {
                Some(s) => handle_github_redirect(s).await,
                None => Err(warp::reject::reject()),
            }
        });

    // GET /auth/github/callback?code=...
    let github_callback = warp::path!("auth" / "github" / "callback")
        .and(warp::get())
        .and(warp::query::<CallbackQuery>())
        .and(warp::any().map(move || state2.clone()))
        .and_then(
            |query: CallbackQuery, state: Option<SharedOAuthState>| async move {
                match state {
                    Some(s) => handle_github_callback(query, s).await,
                    None => Err(warp::reject::reject()),
                }
            },
        );

    // POST /auth/logout
    let logout = warp::path!("auth" / "logout")
        .and(warp::post())
        .and(warp::header::optional::<String>("authorization"))
        .and(warp::any().map(move || state3.clone()))
        .and_then(
            |auth_header: Option<String>, state: Option<SharedOAuthState>| async move {
                match state {
                    Some(s) => handle_logout(auth_header, s).await,
                    None => Err(warp::reject::reject()),
                }
            },
        );

    // GET /api/v1/me
    let me = warp::path!("api" / "v1" / "me")
        .and(warp::get())
        .and(warp::header::optional::<String>("authorization"))
        .and(warp::any().map(move || state4.clone()))
        .and_then(
            |auth_header: Option<String>, state: Option<SharedOAuthState>| async move {
                match state {
                    Some(s) => handle_me(auth_header, s).await,
                    None => Err(warp::reject::reject()),
                }
            },
        );

    github_redirect.or(github_callback).or(logout).or(me)
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

        let token = create_jwt(&config, &user).expect("JWT creation should succeed");
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
            avatar_url: "".to_string(),
            email: None,
        };

        let token = create_jwt(&config, &user).unwrap();

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
        let routes = oauth_routes(Some(state));

        let res = warp::test::request()
            .method("GET")
            .path("/api/v1/me")
            .reply(&routes)
            .await;

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

        let token = create_jwt(&config, &user).unwrap();
        let state = Arc::new(OAuthState::new(config));
        let routes = oauth_routes(Some(state));

        let res = warp::test::request()
            .method("GET")
            .path("/api/v1/me")
            .header("authorization", format!("Bearer {}", token))
            .reply(&routes)
            .await;

        assert_eq!(res.status(), 200);
        let body: serde_json::Value = serde_json::from_slice(res.body()).unwrap();
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

        let token = create_jwt(&config, &user).unwrap();
        let state = Arc::new(OAuthState::new(config));

        // Revoke the token
        let hash = token_hash(&token);
        state.sessions.write().await.revoke(hash);

        let routes = oauth_routes(Some(state));

        let res = warp::test::request()
            .method("GET")
            .path("/api/v1/me")
            .header("authorization", format!("Bearer {}", token))
            .reply(&routes)
            .await;

        assert_eq!(res.status(), 401);
        let body: serde_json::Value = serde_json::from_slice(res.body()).unwrap();
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
        let routes = oauth_routes(Some(state));

        let res = warp::test::request()
            .method("POST")
            .path("/auth/logout")
            .header("authorization", "Bearer some-token")
            .reply(&routes)
            .await;

        assert_eq!(res.status(), 200);
        let body: serde_json::Value = serde_json::from_slice(res.body()).unwrap();
        assert_eq!(body["ok"], true);
    }
}
