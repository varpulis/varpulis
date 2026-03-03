//! Authentication module for Varpulis CLI
//!
//! Provides API key authentication for WebSocket connections.

use std::sync::Arc;

use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// Authentication configuration
#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// Whether authentication is enabled
    pub enabled: bool,
    /// The API key (if authentication is enabled)
    api_key: Option<String>,
}

impl AuthConfig {
    /// Create a new AuthConfig with authentication disabled
    pub const fn disabled() -> Self {
        Self {
            enabled: false,
            api_key: None,
        }
    }

    /// Create a new AuthConfig with the given API key
    pub const fn with_api_key(api_key: String) -> Self {
        Self {
            enabled: true,
            api_key: Some(api_key),
        }
    }

    /// Check if the provided key matches the configured API key
    pub fn validate_key(&self, provided_key: &str) -> bool {
        if !self.enabled {
            return true;
        }

        match &self.api_key {
            Some(key) => constant_time_compare(key, provided_key),
            None => false,
        }
    }

    /// Check if authentication is required
    pub const fn is_required(&self) -> bool {
        self.enabled
    }

    /// Get the configured API key
    pub fn api_key(&self) -> Option<&str> {
        self.api_key.as_deref()
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self::disabled()
    }
}

/// Authentication error types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    /// No credentials provided
    MissingCredentials,
    /// Invalid credentials
    InvalidCredentials,
    /// Malformed authorization header
    MalformedHeader,
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingCredentials => write!(f, "Authentication required"),
            Self::InvalidCredentials => write!(f, "Invalid API key"),
            Self::MalformedHeader => write!(f, "Malformed authorization header"),
        }
    }
}

impl std::error::Error for AuthError {}

/// Result type for authentication operations
pub type AuthResult<T> = Result<T, AuthError>;

/// Extract API key from Authorization header
///
/// Supports formats:
/// - `Bearer <api-key>`
/// - `ApiKey <api-key>`
/// - `<api-key>` (raw key)
pub fn extract_from_header(header_value: &str) -> AuthResult<String> {
    let header = header_value.trim();

    if header.is_empty() {
        return Err(AuthError::MissingCredentials);
    }

    // Try "Bearer " prefix with space
    if let Some(rest) = header.strip_prefix("Bearer ") {
        let key = rest.trim();
        if key.is_empty() {
            return Err(AuthError::MalformedHeader);
        }
        return Ok(key.to_string());
    }

    // Try "Bearer\t" prefix with tab
    if let Some(rest) = header.strip_prefix("Bearer\t") {
        let key = rest.trim();
        if key.is_empty() {
            return Err(AuthError::MalformedHeader);
        }
        return Ok(key.to_string());
    }

    // "Bearer" alone (no space, no key) is malformed
    if header == "Bearer" {
        return Err(AuthError::MalformedHeader);
    }

    // Try "ApiKey " prefix with space
    if let Some(rest) = header.strip_prefix("ApiKey ") {
        let key = rest.trim();
        if key.is_empty() {
            return Err(AuthError::MalformedHeader);
        }
        return Ok(key.to_string());
    }

    // Try "ApiKey\t" prefix with tab
    if let Some(rest) = header.strip_prefix("ApiKey\t") {
        let key = rest.trim();
        if key.is_empty() {
            return Err(AuthError::MalformedHeader);
        }
        return Ok(key.to_string());
    }

    // "ApiKey" alone (no space, no key) is malformed
    if header == "ApiKey" {
        return Err(AuthError::MalformedHeader);
    }

    // Treat as raw key if no recognized prefix
    Ok(header.to_string())
}

/// Extract API key from `Sec-WebSocket-Protocol` header.
///
/// Looks for a subprotocol prefixed with `varpulis-auth.` and extracts the API key.
/// This avoids exposing the API key in URL query parameters (which are logged in
/// server access logs, browser history, and proxy logs).
pub fn extract_from_ws_protocol(header: &str) -> AuthResult<String> {
    for protocol in header.split(',') {
        let protocol = protocol.trim();
        if let Some(key) = protocol.strip_prefix("varpulis-auth.") {
            if !key.is_empty() {
                return Ok(key.to_string());
            }
        }
    }
    Err(AuthError::MissingCredentials)
}

/// Extract API key from query parameters
///
/// Looks for `api_key` or `token` parameter
pub fn extract_from_query(query: &str) -> AuthResult<String> {
    if query.is_empty() {
        return Err(AuthError::MissingCredentials);
    }

    // Parse query string manually to avoid dependencies
    for pair in query.split('&') {
        let mut parts = pair.splitn(2, '=');
        let key = parts.next().unwrap_or("");
        let value = parts.next().unwrap_or("");

        if (key == "api_key" || key == "token") && !value.is_empty() {
            // URL decode the value (basic decoding)
            let decoded = url_decode(value);
            return Ok(decoded);
        }
    }

    Err(AuthError::MissingCredentials)
}

/// Basic URL decoding for API keys
fn url_decode(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars();

    while let Some(c) = chars.next() {
        if c == '%' {
            // Try to decode hex sequence
            let hex: String = chars.by_ref().take(2).collect();
            if hex.len() == 2 {
                if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                    result.push(byte as char);
                    continue;
                }
            }
            // Invalid hex, keep original
            result.push('%');
            result.push_str(&hex);
        } else if c == '+' {
            result.push(' ');
        } else {
            result.push(c);
        }
    }

    result
}

/// Constant-time string comparison to prevent timing attacks.
///
/// Delegates to [`varpulis_core::security::constant_time_compare`] which
/// does **not** leak the expected key length via timing.
pub fn constant_time_compare(a: &str, b: &str) -> bool {
    varpulis_core::security::constant_time_compare(a, b)
}

/// Generate a cryptographically random API key.
///
/// Uses the OS CSPRNG (via `rand::rng`) to produce a 32-character
/// alphanumeric key with ~190 bits of entropy.
pub fn generate_api_key() -> String {
    use rand::Rng;

    let mut rng = rand::rng();
    let mut key = String::with_capacity(32);
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    for _ in 0..32 {
        let idx = rng.random_range(0..CHARSET.len());
        key.push(CHARSET[idx] as char);
    }

    key
}

/// Axum middleware for API key authentication.
///
/// Checks authentication in this order:
/// 1. X-API-Key header (backward-compatible)
/// 2. Authorization: Bearer/ApiKey header (API key validation)
/// 3. Cookie: varpulis_session=`<jwt>` (JWT session cookie)
/// 4. Sec-WebSocket-Protocol: varpulis-auth.`<key>` (WebSocket upgrade)
/// 5. Query parameter: api_key or token (last resort, kept for backward compatibility)
///
/// Pass `oauth_state` to enable JWT verification from cookies.
pub fn auth_middleware(config: Arc<AuthConfig>) -> impl tower::Layer<axum::routing::Route> + Clone {
    axum::middleware::from_fn_with_state::<_, _, ()>(config, auth_middleware_fn)
}

/// Authentication state that can carry optional OAuth state.
#[derive(Debug, Clone)]
pub struct AuthState {
    pub config: Arc<AuthConfig>,
    pub oauth_state: Option<crate::oauth::SharedOAuthState>,
}

/// Create an axum middleware layer for authentication with optional JWT cookie support.
pub fn auth_middleware_with_jwt(
    config: Arc<AuthConfig>,
    oauth_state: Option<crate::oauth::SharedOAuthState>,
) -> impl tower::Layer<axum::routing::Route> + Clone {
    let state = AuthState {
        config,
        oauth_state,
    };
    axum::middleware::from_fn_with_state::<_, _, ()>(state, auth_middleware_jwt_fn)
}

/// Axum middleware function for API key auth (no JWT).
pub async fn auth_middleware_fn(
    axum::extract::State(config): axum::extract::State<Arc<AuthConfig>>,
    req: Request,
    next: Next,
) -> Result<Response, AuthRejection> {
    let state = AuthState {
        config,
        oauth_state: None,
    };
    check_auth(&state, &req).await?;
    Ok(next.run(req).await)
}

/// Axum middleware function for auth with JWT cookie support.
async fn auth_middleware_jwt_fn(
    axum::extract::State(state): axum::extract::State<AuthState>,
    req: Request,
    next: Next,
) -> Result<Response, AuthRejection> {
    check_auth(&state, &req).await?;
    Ok(next.run(req).await)
}

/// Core authentication check, shared by both middleware variants.
pub async fn check_auth(state: &AuthState, req: &Request) -> Result<(), AuthRejection> {
    check_auth_from_parts(state, req.headers(), req.uri()).await
}

/// Authentication check from raw parts (headers + URI).
///
/// This avoids the need to hold a `Request<Body>` across await points,
/// which would require `Body: Sync` (it isn't).
pub async fn check_auth_from_parts(
    state: &AuthState,
    headers: &axum::http::HeaderMap,
    uri: &axum::http::Uri,
) -> Result<(), AuthRejection> {
    let config = &state.config;
    let oauth = &state.oauth_state;

    // If auth is disabled, allow all
    if !config.is_required() {
        return Ok(());
    }

    let auth_header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let cookie_header = headers
        .get("cookie")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let ws_protocol = headers
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let query = uri.query().unwrap_or("").to_string();

    // Try to extract API key from header first
    if let Some(header) = &auth_header {
        match extract_from_header(header) {
            Ok(key) if config.validate_key(&key) => return Ok(()),
            Ok(_) => return Err(AuthRejection::InvalidCredentials),
            Err(AuthError::MalformedHeader) => return Err(AuthRejection::MalformedHeader),
            Err(_) => {} // Try other methods
        }
    }

    // Try JWT from cookie
    if let Some(ref cookie) = cookie_header {
        if let Some(jwt) = crate::oauth::extract_jwt_from_cookie(cookie) {
            if let Some(ref state) = oauth {
                // Verify JWT is valid and not revoked
                let hash = crate::oauth::token_hash(&jwt);
                if !state.sessions.read().await.is_revoked(&hash)
                    && crate::oauth::verify_jwt(&state.config, &jwt).is_ok()
                {
                    return Ok(());
                }
            }
        }
    }

    // Try Authorization header as Bearer JWT (when OAuth is configured)
    if let Some(ref header) = auth_header {
        if let Some(token) = header.strip_prefix("Bearer ") {
            let token = token.trim();
            if !token.is_empty() {
                if let Some(ref state) = oauth {
                    let hash = crate::oauth::token_hash(token);
                    if !state.sessions.read().await.is_revoked(&hash)
                        && crate::oauth::verify_jwt(&state.config, token).is_ok()
                    {
                        return Ok(());
                    }
                }
            }
        }
    }

    // Try Sec-WebSocket-Protocol header (avoids API key in URL query params)
    if let Some(ref protocol) = ws_protocol {
        match extract_from_ws_protocol(protocol) {
            Ok(key) if config.validate_key(&key) => return Ok(()),
            Ok(_) => return Err(AuthRejection::InvalidCredentials),
            Err(_) => {} // Try query params as last resort
        }
    }

    // Try query params (last resort, kept for backward compatibility)
    match extract_from_query(&query) {
        Ok(key) if config.validate_key(&key) => Ok(()),
        Ok(_) => Err(AuthRejection::InvalidCredentials),
        Err(_) => Err(AuthRejection::MissingCredentials),
    }
}

/// Authentication rejection type (implements IntoResponse for axum)
#[derive(Debug)]
pub enum AuthRejection {
    MissingCredentials,
    InvalidCredentials,
    MalformedHeader,
}

impl IntoResponse for AuthRejection {
    fn into_response(self) -> Response {
        let (code, message) = match self {
            Self::MissingCredentials => (StatusCode::UNAUTHORIZED, "Authentication required"),
            Self::InvalidCredentials => (StatusCode::UNAUTHORIZED, "Invalid API key"),
            Self::MalformedHeader => (StatusCode::BAD_REQUEST, "Malformed authorization header"),
        };
        (code, axum::Json(serde_json::json!({ "error": message }))).into_response()
    }
}

// =============================================================================
// Tests - TDD approach
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------------------------------------------------------------
    // AuthConfig tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_auth_config_disabled() {
        let config = AuthConfig::disabled();
        assert!(!config.enabled);
        assert!(!config.is_required());
    }

    #[test]
    fn test_auth_config_with_api_key() {
        let config = AuthConfig::with_api_key("secret123".to_string());
        assert!(config.enabled);
        assert!(config.is_required());
    }

    #[test]
    fn test_auth_config_validate_key_disabled() {
        let config = AuthConfig::disabled();
        // When disabled, any key is valid
        assert!(config.validate_key("anything"));
        assert!(config.validate_key(""));
    }

    #[test]
    fn test_auth_config_validate_key_correct() {
        let config = AuthConfig::with_api_key("secret123".to_string());
        assert!(config.validate_key("secret123"));
    }

    #[test]
    fn test_auth_config_validate_key_incorrect() {
        let config = AuthConfig::with_api_key("secret123".to_string());
        assert!(!config.validate_key("wrong"));
        assert!(!config.validate_key(""));
        assert!(!config.validate_key("secret1234")); // Too long
        assert!(!config.validate_key("secret12")); // Too short
    }

    #[test]
    fn test_auth_config_default() {
        let config = AuthConfig::default();
        assert!(!config.enabled);
    }

    // -------------------------------------------------------------------------
    // extract_from_header tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_extract_from_header_bearer() {
        let result = extract_from_header("Bearer my-api-key");
        assert_eq!(result, Ok("my-api-key".to_string()));
    }

    #[test]
    fn test_extract_from_header_bearer_with_spaces() {
        let result = extract_from_header("  Bearer   my-api-key  ");
        assert_eq!(result, Ok("my-api-key".to_string()));
    }

    #[test]
    fn test_extract_from_header_apikey() {
        let result = extract_from_header("ApiKey secret-key");
        assert_eq!(result, Ok("secret-key".to_string()));
    }

    #[test]
    fn test_extract_from_header_raw() {
        let result = extract_from_header("raw-key-without-prefix");
        assert_eq!(result, Ok("raw-key-without-prefix".to_string()));
    }

    #[test]
    fn test_extract_from_header_empty() {
        let result = extract_from_header("");
        assert_eq!(result, Err(AuthError::MissingCredentials));
    }

    #[test]
    fn test_extract_from_header_bearer_empty_key() {
        let result = extract_from_header("Bearer ");
        assert_eq!(result, Err(AuthError::MalformedHeader));
    }

    #[test]
    fn test_extract_from_header_apikey_empty_key() {
        let result = extract_from_header("ApiKey ");
        assert_eq!(result, Err(AuthError::MalformedHeader));
    }

    // -------------------------------------------------------------------------
    // extract_from_query tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_extract_from_query_api_key() {
        let result = extract_from_query("api_key=my-secret");
        assert_eq!(result, Ok("my-secret".to_string()));
    }

    #[test]
    fn test_extract_from_query_token() {
        let result = extract_from_query("token=my-token");
        assert_eq!(result, Ok("my-token".to_string()));
    }

    #[test]
    fn test_extract_from_query_with_other_params() {
        let result = extract_from_query("foo=bar&api_key=secret&baz=qux");
        assert_eq!(result, Ok("secret".to_string()));
    }

    #[test]
    fn test_extract_from_query_empty() {
        let result = extract_from_query("");
        assert_eq!(result, Err(AuthError::MissingCredentials));
    }

    #[test]
    fn test_extract_from_query_no_key() {
        let result = extract_from_query("foo=bar&baz=qux");
        assert_eq!(result, Err(AuthError::MissingCredentials));
    }

    #[test]
    fn test_extract_from_query_empty_value() {
        let result = extract_from_query("api_key=");
        assert_eq!(result, Err(AuthError::MissingCredentials));
    }

    #[test]
    fn test_extract_from_query_url_encoded() {
        let result = extract_from_query("api_key=key%20with%20spaces");
        assert_eq!(result, Ok("key with spaces".to_string()));
    }

    #[test]
    fn test_extract_from_query_plus_sign() {
        let result = extract_from_query("api_key=key+with+plus");
        assert_eq!(result, Ok("key with plus".to_string()));
    }

    // -------------------------------------------------------------------------
    // extract_from_ws_protocol tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_extract_from_ws_protocol_valid() {
        let result = extract_from_ws_protocol("varpulis-v1, varpulis-auth.my-secret-key");
        assert_eq!(result, Ok("my-secret-key".to_string()));
    }

    #[test]
    fn test_extract_from_ws_protocol_only_auth() {
        let result = extract_from_ws_protocol("varpulis-auth.abc123");
        assert_eq!(result, Ok("abc123".to_string()));
    }

    #[test]
    fn test_extract_from_ws_protocol_no_auth() {
        let result = extract_from_ws_protocol("varpulis-v1");
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_from_ws_protocol_empty() {
        let result = extract_from_ws_protocol("");
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_from_ws_protocol_empty_key() {
        let result = extract_from_ws_protocol("varpulis-auth.");
        assert!(result.is_err());
    }

    // -------------------------------------------------------------------------
    // url_decode tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_url_decode_plain() {
        assert_eq!(url_decode("hello"), "hello");
    }

    #[test]
    fn test_url_decode_spaces() {
        assert_eq!(url_decode("hello%20world"), "hello world");
    }

    #[test]
    fn test_url_decode_plus() {
        assert_eq!(url_decode("hello+world"), "hello world");
    }

    #[test]
    fn test_url_decode_special_chars() {
        assert_eq!(url_decode("%21%40%23"), "!@#");
    }

    // -------------------------------------------------------------------------
    // constant_time_compare tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_constant_time_compare_equal() {
        assert!(constant_time_compare("abc", "abc"));
        assert!(constant_time_compare("", ""));
        assert!(constant_time_compare(
            "longer-string-123",
            "longer-string-123"
        ));
    }

    #[test]
    fn test_constant_time_compare_not_equal() {
        assert!(!constant_time_compare("abc", "abd"));
        assert!(!constant_time_compare("abc", "ab"));
        assert!(!constant_time_compare("abc", "abcd"));
        assert!(!constant_time_compare("", "a"));
    }

    // -------------------------------------------------------------------------
    // generate_api_key tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_generate_api_key_length() {
        let key = generate_api_key();
        assert_eq!(key.len(), 32);
    }

    #[test]
    fn test_generate_api_key_alphanumeric() {
        let key = generate_api_key();
        assert!(key.chars().all(|c| c.is_ascii_alphanumeric()));
    }

    #[test]
    fn test_generate_api_key_unique() {
        let key1 = generate_api_key();
        std::thread::sleep(std::time::Duration::from_millis(1));
        let key2 = generate_api_key();
        assert_ne!(key1, key2);
    }

    // -------------------------------------------------------------------------
    // AuthError Display tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_auth_error_display_missing() {
        let err = AuthError::MissingCredentials;
        assert_eq!(format!("{err}"), "Authentication required");
    }

    #[test]
    fn test_auth_error_display_invalid() {
        let err = AuthError::InvalidCredentials;
        assert_eq!(format!("{err}"), "Invalid API key");
    }

    #[test]
    fn test_auth_error_display_malformed() {
        let err = AuthError::MalformedHeader;
        assert_eq!(format!("{err}"), "Malformed authorization header");
    }

    // -------------------------------------------------------------------------
    // Integration tests with axum
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_with_auth_disabled() {
        let config = Arc::new(AuthConfig::disabled());
        let state = AuthState {
            config,
            oauth_state: None,
        };
        // Build a fake request with no auth
        let req = Request::builder()
            .uri("/")
            .body(axum::body::Body::empty())
            .unwrap();
        let result = check_auth(&state, &req).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_with_auth_valid_header() {
        let config = Arc::new(AuthConfig::with_api_key("secret".to_string()));
        let state = AuthState {
            config,
            oauth_state: None,
        };
        let req = Request::builder()
            .uri("/")
            .header("authorization", "Bearer secret")
            .body(axum::body::Body::empty())
            .unwrap();
        let result = check_auth(&state, &req).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_with_auth_valid_query() {
        let config = Arc::new(AuthConfig::with_api_key("secret".to_string()));
        let state = AuthState {
            config,
            oauth_state: None,
        };
        let req = Request::builder()
            .uri("/?api_key=secret")
            .body(axum::body::Body::empty())
            .unwrap();
        let result = check_auth(&state, &req).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_with_auth_invalid_key() {
        let config = Arc::new(AuthConfig::with_api_key("secret".to_string()));
        let state = AuthState {
            config,
            oauth_state: None,
        };
        let req = Request::builder()
            .uri("/")
            .header("authorization", "Bearer wrong")
            .body(axum::body::Body::empty())
            .unwrap();
        let result = check_auth(&state, &req).await;
        assert!(matches!(result, Err(AuthRejection::InvalidCredentials)));
    }

    #[tokio::test]
    async fn test_with_auth_missing_credentials() {
        let config = Arc::new(AuthConfig::with_api_key("secret".to_string()));
        let state = AuthState {
            config,
            oauth_state: None,
        };
        let req = Request::builder()
            .uri("/")
            .body(axum::body::Body::empty())
            .unwrap();
        let result = check_auth(&state, &req).await;
        assert!(matches!(result, Err(AuthRejection::MissingCredentials)));
    }
}
