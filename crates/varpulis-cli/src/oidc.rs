//! Generic OIDC authentication provider for Varpulis Cloud.
//!
//! Supports any OIDC-compliant identity provider (Okta, Auth0, Azure AD,
//! Keycloak, Google Workspace) via standard `.well-known/openid-configuration`
//! discovery.
//!
//! # Configuration
//!
//! Set these environment variables:
//! - `OIDC_ISSUER_URL`: The OIDC issuer URL (e.g., `https://accounts.google.com`)
//! - `OIDC_CLIENT_ID`: The client ID from your IdP
//! - `OIDC_CLIENT_SECRET`: The client secret from your IdP
//!
//! # Feature Gate
//!
//! This module requires the `oidc` feature flag:
//! ```toml
//! varpulis-cli = { features = ["oidc"] }
//! ```

#[cfg(feature = "oidc")]
mod inner {
    use crate::oauth::{AuthProvider, OAuthError, UserInfo};
    use openidconnect::core::CoreProviderMetadata;
    use openidconnect::{AuthorizationCode, ClientId, ClientSecret, IssuerUrl, Nonce, RedirectUrl};

    /// OIDC configuration loaded from environment variables.
    #[derive(Debug, Clone)]
    pub struct OidcConfig {
        pub issuer_url: String,
        pub client_id: String,
        pub client_secret: String,
    }

    impl OidcConfig {
        /// Build OIDC config from environment variables.
        /// Returns None if required vars are not set.
        pub fn from_env() -> Option<Self> {
            let issuer_url = std::env::var("OIDC_ISSUER_URL").ok()?;
            let client_id = std::env::var("OIDC_CLIENT_ID").ok()?;
            let client_secret = std::env::var("OIDC_CLIENT_SECRET").ok()?;

            Some(Self {
                issuer_url,
                client_id,
                client_secret,
            })
        }
    }

    /// Generic OIDC provider using standard OpenID Connect discovery.
    #[derive(Debug)]
    pub struct OidcProvider {
        config: OidcConfig,
        http_client: reqwest::Client,
    }

    impl OidcProvider {
        pub fn new(config: OidcConfig) -> Self {
            Self {
                config,
                http_client: reqwest::Client::new(),
            }
        }
    }

    #[async_trait::async_trait]
    impl AuthProvider for OidcProvider {
        fn name(&self) -> &str {
            "oidc"
        }

        fn authorize_url(&self, redirect_uri: &str) -> String {
            // Build the authorization URL manually from the issuer URL.
            // Most OIDC providers use {issuer}/authorize as the auth endpoint.
            // For full correctness, the caller should use async discovery, but
            // this sync method is used for the initial redirect.
            format!(
                "{}/authorize?client_id={}&redirect_uri={}&response_type=code&scope=openid%20email%20profile",
                self.config.issuer_url.trim_end_matches('/'),
                urlencoding::encode(&self.config.client_id),
                urlencoding::encode(redirect_uri),
            )
        }

        async fn exchange_code(
            &self,
            code: &str,
            redirect_uri: &str,
        ) -> Result<UserInfo, OAuthError> {
            // Step 1: Discover provider metadata
            let issuer = IssuerUrl::new(self.config.issuer_url.clone())
                .map_err(|e| OAuthError(format!("Invalid issuer URL: {}", e)))?;

            let provider_metadata = CoreProviderMetadata::discover_async(issuer, &self.http_client)
                .await
                .map_err(|e| OAuthError(format!("OIDC discovery failed: {}", e)))?;

            // Step 2: Build client from provider metadata
            let redirect_url = RedirectUrl::new(redirect_uri.to_string())
                .map_err(|e| OAuthError(format!("Invalid redirect URI: {}", e)))?;

            let client_id = ClientId::new(self.config.client_id.clone());
            let client_secret = ClientSecret::new(self.config.client_secret.clone());

            let client = openidconnect::core::CoreClient::from_provider_metadata(
                provider_metadata,
                client_id,
                Some(client_secret),
            )
            .set_redirect_uri(redirect_url);

            // Step 3: Exchange code for tokens
            let token_response = client
                .exchange_code(AuthorizationCode::new(code.to_string()))
                .map_err(|e| OAuthError(format!("Failed to build token request: {}", e)))?
                .request_async(&self.http_client)
                .await
                .map_err(|e| OAuthError(format!("OIDC token exchange failed: {}", e)))?;

            // Step 4: Extract ID token claims
            use openidconnect::TokenResponse;
            let id_token = token_response
                .id_token()
                .ok_or_else(|| OAuthError("No ID token in response".to_string()))?;

            // Use an empty nonce — in production, store/verify nonce from authorize_url
            let nonce = Nonce::new(String::new());
            let verifier = client.id_token_verifier();
            let claims = id_token
                .claims(&verifier, &nonce)
                .map_err(|e| OAuthError(format!("ID token verification failed: {}", e)))?;

            let subject = claims.subject().to_string();
            let email = claims.email().map(|e| e.to_string()).unwrap_or_default();
            let name = claims
                .name()
                .and_then(|n| n.get(None).map(|v| v.to_string()))
                .unwrap_or_else(|| email.clone());
            let login = claims
                .preferred_username()
                .map(|u| u.to_string())
                .unwrap_or_else(|| email.split('@').next().unwrap_or(&subject).to_string());
            let avatar = claims
                .picture()
                .and_then(|p| p.get(None).map(|v| v.to_string()))
                .unwrap_or_default();

            Ok(UserInfo {
                provider_id: subject,
                name,
                login,
                email,
                avatar,
            })
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_oidc_config_from_env_missing() {
            std::env::remove_var("OIDC_ISSUER_URL");
            std::env::remove_var("OIDC_CLIENT_ID");
            std::env::remove_var("OIDC_CLIENT_SECRET");
            assert!(OidcConfig::from_env().is_none());
        }

        #[test]
        fn test_oidc_provider_name() {
            let config = OidcConfig {
                issuer_url: "https://accounts.google.com".to_string(),
                client_id: "test-client-id".to_string(),
                client_secret: "test-secret".to_string(),
            };
            let provider = OidcProvider::new(config);
            assert_eq!(provider.name(), "oidc");
        }

        #[test]
        fn test_oidc_authorize_url_contains_client_id() {
            let config = OidcConfig {
                issuer_url: "https://accounts.google.com".to_string(),
                client_id: "my-client-id".to_string(),
                client_secret: "my-secret".to_string(),
            };
            let provider = OidcProvider::new(config);
            let url = provider.authorize_url("http://localhost:9000/auth/oidc/callback");
            assert!(url.contains("my-client-id"));
            assert!(url.contains("response_type=code"));
            assert!(url.contains("openid"));
        }

        #[test]
        fn test_user_info_serialization() {
            let info = UserInfo {
                provider_id: "12345".to_string(),
                name: "Test User".to_string(),
                login: "testuser".to_string(),
                email: "test@example.com".to_string(),
                avatar: "https://example.com/avatar.png".to_string(),
            };
            let json = serde_json::to_string(&info).unwrap();
            let restored: UserInfo = serde_json::from_str(&json).unwrap();
            assert_eq!(restored.provider_id, "12345");
            assert_eq!(restored.login, "testuser");
        }
    }
}

#[cfg(feature = "oidc")]
pub use inner::*;
