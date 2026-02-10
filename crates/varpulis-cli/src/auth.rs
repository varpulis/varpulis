//! Authentication module for Varpulis CLI
//!
//! Provides API key authentication for WebSocket connections.

use std::sync::Arc;
use warp::Filter;

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
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            api_key: None,
        }
    }

    /// Create a new AuthConfig with the given API key
    pub fn with_api_key(api_key: String) -> Self {
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
    pub fn is_required(&self) -> bool {
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
#[derive(Debug, Clone, PartialEq)]
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
            AuthError::MissingCredentials => write!(f, "Authentication required"),
            AuthError::InvalidCredentials => write!(f, "Invalid API key"),
            AuthError::MalformedHeader => write!(f, "Malformed authorization header"),
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
    let mut chars = s.chars().peekable();

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

/// Constant-time string comparison to prevent timing attacks
pub fn constant_time_compare(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }

    let mut result = 0u8;
    for (x, y) in a.bytes().zip(b.bytes()) {
        result |= x ^ y;
    }

    result == 0
}

/// Generate a random API key
pub fn generate_api_key() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0));

    // Simple random generation using timestamp and random bytes
    let seed = timestamp.as_nanos();
    let mut state = seed;

    let mut key = String::with_capacity(32);
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    for _ in 0..32 {
        // LCG random number generator
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let idx = ((state >> 33) as usize) % CHARSET.len();
        key.push(CHARSET[idx] as char);
    }

    key
}

/// Warp filter for API key authentication
pub fn with_auth(
    config: Arc<AuthConfig>,
) -> impl warp::Filter<Extract = ((),), Error = warp::Rejection> + Clone {
    warp::any()
        .and(warp::header::optional::<String>("authorization"))
        .and(warp::query::raw().or(warp::any().map(String::new)).unify())
        .and_then(move |auth_header: Option<String>, query: String| {
            let config = config.clone();
            async move {
                // If auth is disabled, allow all
                if !config.is_required() {
                    return Ok(());
                }

                // Try to extract API key from header first
                if let Some(header) = auth_header {
                    match extract_from_header(&header) {
                        Ok(key) if config.validate_key(&key) => return Ok(()),
                        Ok(_) => {
                            return Err(warp::reject::custom(AuthRejection::InvalidCredentials))
                        }
                        Err(AuthError::MalformedHeader) => {
                            return Err(warp::reject::custom(AuthRejection::MalformedHeader))
                        }
                        Err(_) => {} // Try query params next
                    }
                }

                // Try query params
                match extract_from_query(&query) {
                    Ok(key) if config.validate_key(&key) => Ok(()),
                    Ok(_) => Err(warp::reject::custom(AuthRejection::InvalidCredentials)),
                    Err(_) => Err(warp::reject::custom(AuthRejection::MissingCredentials)),
                }
            }
        })
}

/// Warp rejection type for authentication errors
#[derive(Debug)]
pub enum AuthRejection {
    MissingCredentials,
    InvalidCredentials,
    MalformedHeader,
}

impl warp::reject::Reject for AuthRejection {}

/// Handle authentication and rate limit rejections
pub async fn handle_rejection(
    err: warp::Rejection,
) -> Result<impl warp::Reply, std::convert::Infallible> {
    // Check for rate limit rejection first
    if let Some(reply) = crate::rate_limit::handle_rate_limit_rejection(&err) {
        return Ok(reply);
    }

    let (code, message): (warp::http::StatusCode, String) =
        if let Some(auth_err) = err.find::<AuthRejection>() {
            match auth_err {
                AuthRejection::MissingCredentials => (
                    warp::http::StatusCode::UNAUTHORIZED,
                    "Authentication required".into(),
                ),
                AuthRejection::InvalidCredentials => (
                    warp::http::StatusCode::UNAUTHORIZED,
                    "Invalid API key".into(),
                ),
                AuthRejection::MalformedHeader => (
                    warp::http::StatusCode::BAD_REQUEST,
                    "Malformed authorization header".into(),
                ),
            }
        } else if let Some(e) = err.find::<warp::filters::body::BodyDeserializeError>() {
            (
                warp::http::StatusCode::BAD_REQUEST,
                format!("Invalid request body: {}", e),
            )
        } else if err.find::<warp::reject::InvalidQuery>().is_some() {
            (
                warp::http::StatusCode::BAD_REQUEST,
                "Invalid query parameters".into(),
            )
        } else if err.find::<warp::reject::PayloadTooLarge>().is_some() {
            (
                warp::http::StatusCode::PAYLOAD_TOO_LARGE,
                "Request payload too large".into(),
            )
        } else if err.find::<warp::reject::UnsupportedMediaType>().is_some() {
            (
                warp::http::StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "Unsupported media type".into(),
            )
        } else if err.find::<warp::reject::MethodNotAllowed>().is_some() {
            (
                warp::http::StatusCode::METHOD_NOT_ALLOWED,
                "Method not allowed".into(),
            )
        } else if err.is_not_found() {
            (warp::http::StatusCode::NOT_FOUND, "Not found".into())
        } else {
            tracing::error!("Unhandled rejection: {:?}", err);
            (
                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error".into(),
            )
        };

    Ok(warp::reply::with_status(
        warp::reply::json(&serde_json::json!({
            "error": message
        })),
        code,
    ))
}

// =============================================================================
// Tests - TDD approach
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use warp::Filter;

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
        assert_eq!(format!("{}", err), "Authentication required");
    }

    #[test]
    fn test_auth_error_display_invalid() {
        let err = AuthError::InvalidCredentials;
        assert_eq!(format!("{}", err), "Invalid API key");
    }

    #[test]
    fn test_auth_error_display_malformed() {
        let err = AuthError::MalformedHeader;
        assert_eq!(format!("{}", err), "Malformed authorization header");
    }

    // -------------------------------------------------------------------------
    // Integration tests with warp
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_with_auth_disabled() {
        let config = Arc::new(AuthConfig::disabled());
        let filter = with_auth(config)
            .untuple_one()
            .map(|| warp::reply::html("ok"));

        let res = warp::test::request().path("/").reply(&filter).await;
        assert_eq!(res.status(), 200);
    }

    #[tokio::test]
    async fn test_with_auth_valid_header() {
        let config = Arc::new(AuthConfig::with_api_key("secret".to_string()));
        let filter = with_auth(config)
            .untuple_one()
            .map(|| warp::reply::html("ok"))
            .recover(handle_rejection);

        let res = warp::test::request()
            .path("/")
            .header("authorization", "Bearer secret")
            .reply(&filter)
            .await;
        assert_eq!(res.status(), 200);
    }

    #[tokio::test]
    async fn test_with_auth_valid_query() {
        let config = Arc::new(AuthConfig::with_api_key("secret".to_string()));
        let filter = with_auth(config)
            .untuple_one()
            .map(|| warp::reply::html("ok"))
            .recover(handle_rejection);

        let res = warp::test::request()
            .path("/?api_key=secret")
            .reply(&filter)
            .await;
        assert_eq!(res.status(), 200);
    }

    #[tokio::test]
    async fn test_with_auth_invalid_key() {
        let config = Arc::new(AuthConfig::with_api_key("secret".to_string()));
        let filter = with_auth(config)
            .untuple_one()
            .map(|| warp::reply::html("ok"))
            .recover(handle_rejection);

        let res = warp::test::request()
            .path("/")
            .header("authorization", "Bearer wrong")
            .reply(&filter)
            .await;
        assert_eq!(res.status(), 401);
    }

    #[tokio::test]
    async fn test_with_auth_missing_credentials() {
        let config = Arc::new(AuthConfig::with_api_key("secret".to_string()));
        let filter = with_auth(config)
            .untuple_one()
            .map(|| warp::reply::html("ok"))
            .recover(handle_rejection);

        let res = warp::test::request().path("/").reply(&filter).await;
        assert_eq!(res.status(), 401);
    }
}
