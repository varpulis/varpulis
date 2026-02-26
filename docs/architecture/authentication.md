# Authentication Architecture

Technical architecture of Varpulis's authentication system, covering the AuthProvider trait, GitHub OAuth, OIDC, and JWT session management.

## Overview

Varpulis supports multiple authentication methods through the `AuthProvider` trait abstraction:

```
                    ┌─────────────────┐
                    │  AuthProvider    │  (trait)
                    │  trait           │
                    └────┬────────┬───┘
                         │        │
              ┌──────────┘        └──────────┐
              ▼                              ▼
    ┌─────────────────┐            ┌─────────────────┐
    │  GitHubProvider  │            │  OidcProvider    │
    │  (default SaaS)  │            │  (--features     │
    │                  │            │     oidc)         │
    └────────┬────────┘            └────────┬────────┘
             │                              │
             ▼                              ▼
    ┌─────────────────────────────────────────────────┐
    │              JWT Session Layer                    │
    │  (7-day expiry, HMAC-SHA256, HttpOnly cookie)    │
    └─────────────────────────────────────────────────┘
```

## AuthProvider Trait

```rust
// crates/varpulis-cli/src/oauth.rs
#[async_trait]
pub trait AuthProvider: Send + Sync {
    /// Provider name (e.g., "github", "oidc")
    fn name(&self) -> &str;

    /// Generate the authorization URL for redirect
    fn authorize_url(&self) -> (String, String);  // (url, state)

    /// Exchange an authorization code for user info
    async fn exchange_code(&self, code: &str) -> Result<AuthUser, AuthError>;
}
```

The trait enables:
- Multiple providers active simultaneously
- Easy addition of new identity providers
- Consistent JWT session issuance regardless of provider

### AuthUser

```rust
pub struct AuthUser {
    pub provider: String,       // "github" or "oidc"
    pub provider_id: String,    // Provider-specific user ID
    pub email: Option<String>,
    pub name: Option<String>,
    pub avatar_url: Option<String>,
}
```

---

## GitHub OAuth Flow

The default authentication method in SaaS mode.

```
Browser                Varpulis               GitHub
  │                       │                      │
  │  GET /auth/github     │                      │
  │──────────────────────>│                      │
  │  302 → github.com     │                      │
  │<──────────────────────│                      │
  │                       │                      │
  │  User authenticates   │                      │
  │──────────────────────────────────────────────>│
  │                       │                      │
  │  302 → /auth/callback?code=xxx               │
  │<─────────────────────────────────────────────│
  │                       │                      │
  │  GET /auth/callback   │                      │
  │──────────────────────>│                      │
  │                       │  POST /access_token  │
  │                       │─────────────────────>│
  │                       │  { access_token }    │
  │                       │<─────────────────────│
  │                       │  GET /user           │
  │                       │─────────────────────>│
  │                       │  { id, email, ... }  │
  │                       │<─────────────────────│
  │                       │                      │
  │  Set-Cookie: jwt=...  │                      │
  │<──────────────────────│                      │
```

### Configuration

```bash
export GITHUB_CLIENT_ID="your-github-app-client-id"
export GITHUB_CLIENT_SECRET="your-github-app-client-secret"
```

---

## OIDC Flow

Enabled with `--features oidc`. Uses the standard Authorization Code flow with PKCE.

```
Browser                Varpulis               OIDC Provider
  │                       │                      │
  │  GET /auth/oidc/login │                      │
  │──────────────────────>│                      │
  │                       │  Discover endpoints  │
  │                       │  GET /.well-known/   │
  │                       │    openid-config     │
  │                       │─────────────────────>│
  │                       │  { authorization_    │
  │                       │    endpoint, ... }   │
  │                       │<─────────────────────│
  │  302 → provider       │                      │
  │<──────────────────────│                      │
  │                       │                      │
  │  User authenticates   │                      │
  │──────────────────────────────────────────────>│
  │                       │                      │
  │  302 → /auth/oidc/callback?code=xxx          │
  │<─────────────────────────────────────────────│
  │                       │                      │
  │  GET /auth/oidc/      │                      │
  │       callback        │                      │
  │──────────────────────>│                      │
  │                       │  POST /token         │
  │                       │─────────────────────>│
  │                       │  { id_token,         │
  │                       │    access_token }    │
  │                       │<─────────────────────│
  │                       │                      │
  │                       │  Validate id_token:  │
  │                       │  - Verify signature  │
  │                       │  - Check issuer      │
  │                       │  - Check audience    │
  │                       │  - Check expiry      │
  │                       │                      │
  │  Set-Cookie: jwt=...  │                      │
  │<──────────────────────│                      │
```

### OIDC Discovery

The `OidcProvider` fetches the provider's `.well-known/openid-configuration` at startup to discover:
- Authorization endpoint
- Token endpoint
- UserInfo endpoint
- JWKS URI (for ID token verification)

### Configuration

```bash
export OIDC_ISSUER_URL="https://provider.example.com/realms/varpulis"
export OIDC_CLIENT_ID="varpulis-app"
export OIDC_CLIENT_SECRET="secret"
```

---

## JWT Session Management

After successful authentication (via any provider), Varpulis issues a JWT session token.

### Token Structure

```json
{
  "sub": "github:12345",          // provider:provider_id
  "email": "user@example.com",
  "name": "User Name",
  "provider": "github",
  "iat": 1708900000,              // Issued at
  "exp": 1709504800               // Expires (7 days)
}
```

### Token Properties

| Property | Value |
|----------|-------|
| Algorithm | HMAC-SHA256 |
| Expiry | 7 days |
| Storage | HttpOnly, Secure, SameSite=Lax cookie |
| Signing key | Derived from `VARPULIS_API_KEY` or `JWT_SECRET` |

### Session Validation

On each API request:
1. Extract JWT from `Authorization: Bearer <token>` header or cookie
2. Verify HMAC-SHA256 signature
3. Check expiry (`exp` claim)
4. Extract user identity from claims
5. Look up organization membership and role from database

---

## Multi-Provider Configuration

Both providers can be active simultaneously:

```bash
# GitHub OAuth (SaaS default)
export GITHUB_CLIENT_ID="..."
export GITHUB_CLIENT_SECRET="..."

# OIDC (requires --features oidc)
export OIDC_ISSUER_URL="..."
export OIDC_CLIENT_ID="..."
export OIDC_CLIENT_SECRET="..."
```

The server registers routes for all configured providers:

| Route | Provider |
|-------|----------|
| `/auth/github` | GitHub OAuth |
| `/auth/github/callback` | GitHub OAuth callback |
| `/auth/oidc/login` | OIDC Authorization redirect |
| `/auth/oidc/callback` | OIDC Authorization callback |

---

## Security Considerations

- **CSRF Protection**: OAuth state parameter is validated on callback
- **Token Security**: JWT stored in HttpOnly cookie (not accessible to JavaScript)
- **Secret Zeroization**: Client secrets are held in `SecretString` (zeroized on drop)
- **HTTPS Required**: In production, all auth flows must use HTTPS
- **Scope Minimization**: Only `openid profile email` scopes requested from OIDC

---

## File Locations

| Component | File |
|-----------|------|
| AuthProvider trait | `crates/varpulis-cli/src/oauth.rs` |
| GitHub OAuth | `crates/varpulis-cli/src/oauth.rs` |
| OIDC Provider | `crates/varpulis-cli/src/oidc.rs` |
| JWT session | `crates/varpulis-cli/src/oauth.rs` |
| Auth routes | `crates/varpulis-cli/src/routes.rs` |

---

## See Also

- [SSO/OIDC Tutorial](../tutorials/sso-oidc-tutorial.md) -- Provider setup guides
- [Configuration Guide](../guides/configuration.md) -- OIDC configuration reference
- [Security Policy](../../SECURITY.md) -- Security measures and vulnerability reporting
