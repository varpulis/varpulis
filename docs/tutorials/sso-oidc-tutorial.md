# SSO / OIDC Tutorial

This tutorial walks you through configuring Single Sign-On (SSO) for Varpulis using OpenID Connect (OIDC). You'll learn to set up OIDC with Keycloak, Azure AD, Okta, and Auth0, and understand how the authentication flow integrates with Varpulis SaaS mode.

## Prerequisites

- Varpulis built with the `oidc` feature (`cargo build --release --features oidc`)
- An OIDC provider account (Keycloak, Azure AD, Okta, or Auth0)
- Basic understanding of OAuth 2.0 / OIDC flows

---

## Part 1: How OIDC Authentication Works

Varpulis uses the Authorization Code flow with PKCE:

```
User → Varpulis login page → Redirect to OIDC provider
                                    ↓
                           User authenticates
                                    ↓
Provider → Redirect back to Varpulis with auth code
                                    ↓
Varpulis → Exchange code for tokens → Validate ID token
                                    ↓
                           Issue JWT session (7-day expiry)
```

The `AuthProvider` trait abstracts the identity provider, allowing Varpulis to support multiple SSO backends (GitHub OAuth, OIDC, or custom providers) simultaneously.

---

## Part 2: Configuration

### Environment Variables

```bash
# Required
export OIDC_ISSUER_URL="https://your-provider.example.com/realms/varpulis"
export OIDC_CLIENT_ID="varpulis-app"
export OIDC_CLIENT_SECRET="your-client-secret"

# Optional
export OIDC_REDIRECT_URI="http://localhost:9000/auth/oidc/callback"
export OIDC_SCOPES="openid profile email"
```

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `OIDC_ISSUER_URL` | Yes | — | OIDC issuer URL (must serve `.well-known/openid-configuration`) |
| `OIDC_CLIENT_ID` | Yes | — | Client ID registered with the provider |
| `OIDC_CLIENT_SECRET` | Yes | — | Client secret for the confidential client |
| `OIDC_REDIRECT_URI` | No | `{server_url}/auth/oidc/callback` | OAuth callback URL |
| `OIDC_SCOPES` | No | `openid profile email` | Requested OIDC scopes |

### Build with OIDC

```bash
cargo build --release --features oidc
# Or combine with SaaS mode:
cargo build --release --features saas,oidc
```

### Start the Server

```bash
varpulis server \
    --port 9000 \
    --api-key "admin-key"
```

When the OIDC environment variables are set and the `oidc` feature is enabled, the OIDC login routes are automatically registered.

---

## Part 3: Testing with Keycloak

Keycloak is a popular open-source identity provider, ideal for testing OIDC integration locally.

### Start Keycloak

```bash
docker run -d --name keycloak \
    -p 8080:8080 \
    -e KC_BOOTSTRAP_ADMIN_USERNAME=admin \
    -e KC_BOOTSTRAP_ADMIN_PASSWORD=admin \
    quay.io/keycloak/keycloak:latest \
    start-dev
```

### Configure Keycloak

1. Open http://localhost:8080 and log in with `admin` / `admin`

2. **Create a realm** named `varpulis`

3. **Create a client**:
   - Client ID: `varpulis-app`
   - Client type: OpenID Connect
   - Client authentication: On (confidential)
   - Valid redirect URIs: `http://localhost:9000/auth/oidc/callback`
   - Web origins: `http://localhost:9000`

4. **Copy the client secret** from the Credentials tab

5. **Create a test user**:
   - Username: `testuser`
   - Email: `test@example.com`
   - Set a password (temporary: off)

### Configure Varpulis

```bash
export OIDC_ISSUER_URL="http://localhost:8080/realms/varpulis"
export OIDC_CLIENT_ID="varpulis-app"
export OIDC_CLIENT_SECRET="<paste-secret-here>"

varpulis server --port 9000 --api-key "admin-key"
```

### Test the Flow

1. Navigate to `http://localhost:9000/auth/oidc/login`
2. You'll be redirected to Keycloak's login page
3. Log in with `testuser`
4. After authentication, you'll be redirected back with a JWT session cookie

---

## Part 4: Azure AD (Entra ID)

### Register an Application

1. Go to Azure Portal > Azure Active Directory > App registrations
2. **New registration**:
   - Name: `Varpulis CEP`
   - Redirect URI: `http://localhost:9000/auth/oidc/callback` (Web)
3. Note the **Application (client) ID** and **Directory (tenant) ID**
4. Under Certificates & Secrets, create a new **Client secret**

### Configuration

```bash
export OIDC_ISSUER_URL="https://login.microsoftonline.com/<tenant-id>/v2.0"
export OIDC_CLIENT_ID="<application-client-id>"
export OIDC_CLIENT_SECRET="<client-secret-value>"
```

### Azure-Specific Notes

- Azure AD uses `v2.0` in the issuer URL for OIDC compliance
- Group claims can be included by configuring Token Configuration in the Azure Portal
- For multi-tenant apps, use `https://login.microsoftonline.com/common/v2.0` as the issuer

---

## Part 5: Okta

### Create an Application

1. Okta Admin Dashboard > Applications > Create App Integration
2. Sign-in method: OIDC - OpenID Connect
3. Application type: Web Application
4. Redirect URI: `http://localhost:9000/auth/oidc/callback`
5. Note the **Client ID** and **Client secret**

### Configuration

```bash
export OIDC_ISSUER_URL="https://your-org.okta.com"
export OIDC_CLIENT_ID="<client-id>"
export OIDC_CLIENT_SECRET="<client-secret>"
```

### Okta-Specific Notes

- The issuer URL is your Okta organization URL (no path suffix needed)
- Custom authorization servers use `https://your-org.okta.com/oauth2/<server-id>`
- Assign users or groups to the application for access

---

## Part 6: Auth0

### Create an Application

1. Auth0 Dashboard > Applications > Create Application
2. Type: Regular Web Application
3. Settings:
   - Allowed Callback URLs: `http://localhost:9000/auth/oidc/callback`
   - Allowed Logout URLs: `http://localhost:9000`
4. Note **Domain**, **Client ID**, and **Client Secret**

### Configuration

```bash
export OIDC_ISSUER_URL="https://your-tenant.auth0.com/"
export OIDC_CLIENT_ID="<client-id>"
export OIDC_CLIENT_SECRET="<client-secret>"
```

### Auth0-Specific Notes

- The issuer URL must include the trailing slash
- Auth0 supports social connections (Google, GitHub) as upstream IdPs
- Use Rules or Actions for custom claim mapping

---

## Multi-Provider Configuration

Varpulis supports running GitHub OAuth and OIDC simultaneously. Both authentication methods can be active at the same time:

```bash
# GitHub OAuth (always available in SaaS mode)
export GITHUB_CLIENT_ID="..."
export GITHUB_CLIENT_SECRET="..."

# OIDC (requires --features oidc)
export OIDC_ISSUER_URL="..."
export OIDC_CLIENT_ID="..."
export OIDC_CLIENT_SECRET="..."
```

The Web UI will show login buttons for all configured providers.

---

## Troubleshooting

### "OIDC discovery failed"

The issuer URL must serve a valid `/.well-known/openid-configuration` document. Verify:

```bash
curl -s "${OIDC_ISSUER_URL}/.well-known/openid-configuration" | jq .issuer
```

### "Invalid redirect URI"

The redirect URI configured in Varpulis must exactly match one registered with the OIDC provider, including protocol, host, port, and path.

### "Token validation failed"

Check that:
- The issuer in the ID token matches `OIDC_ISSUER_URL`
- The audience matches `OIDC_CLIENT_ID`
- The server clock is synchronized (JWT expiry is time-sensitive)

### CORS Issues with Web UI

If the Web UI can't complete the OIDC flow, ensure the provider has `http://localhost:9000` (or your deployment URL) in its allowed origins / CORS configuration.

---

## Quick Reference

| Setting | Value |
|---------|-------|
| Feature flag | `--features oidc` |
| Auth flow | Authorization Code + PKCE |
| Session | JWT, 7-day expiry |
| Login endpoint | `/auth/oidc/login` |
| Callback endpoint | `/auth/oidc/callback` |
| Required env vars | `OIDC_ISSUER_URL`, `OIDC_CLIENT_ID`, `OIDC_CLIENT_SECRET` |

---

## Next Steps

- [Authentication Architecture](../architecture/authentication.md) -- AuthProvider trait, JWT sessions, flow diagrams
- [Configuration Guide](../guides/configuration.md) -- OIDC configuration reference
- [Security Policy](../../SECURITY.md) -- Security measures and reporting
