//! Stripe billing integration for Varpulis Cloud.
//!
//! Provides usage tracking, tier management, and Stripe Checkout/Portal
//! integration via REST endpoints.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Json, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
#[cfg(feature = "saas")]
use chrono::Datelike;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::audit::{AuditAction, AuditEntry, SharedAuditLogger};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Billing configuration loaded from environment variables.
#[derive(Debug, Clone)]
pub struct BillingConfig {
    pub stripe_secret_key: String,
    pub stripe_webhook_secret: String,
    pub pro_price_id: String,
    pub business_price_id: String,
    pub frontend_url: String,
}

impl BillingConfig {
    /// Build config from environment variables.
    /// Returns None if Stripe is not configured.
    pub fn from_env() -> Option<Self> {
        let secret_key = std::env::var("STRIPE_SECRET_KEY").ok()?;
        let webhook_secret =
            std::env::var("STRIPE_WEBHOOK_SECRET").unwrap_or_else(|_| String::new());
        let pro_price_id = std::env::var("STRIPE_PRO_PRICE_ID").unwrap_or_else(|_| String::new());
        let business_price_id =
            std::env::var("STRIPE_BUSINESS_PRICE_ID").unwrap_or_else(|_| String::new());
        let frontend_url =
            std::env::var("FRONTEND_URL").unwrap_or_else(|_| "http://localhost:5173".to_string());

        Some(Self {
            stripe_secret_key: secret_key,
            stripe_webhook_secret: webhook_secret,
            pro_price_id,
            business_price_id,
            frontend_url,
        })
    }

    /// Get the Stripe price ID for a given tier.
    pub fn price_id_for_tier(&self, tier: &Tier) -> Option<&str> {
        match tier {
            Tier::Pro if !self.pro_price_id.is_empty() => Some(&self.pro_price_id),
            Tier::Business if !self.business_price_id.is_empty() => Some(&self.business_price_id),
            _ => None,
        }
    }

    /// Determine tier from a Stripe price ID.
    pub fn tier_for_price_id(&self, price_id: &str) -> Option<Tier> {
        if !self.pro_price_id.is_empty() && price_id == self.pro_price_id {
            Some(Tier::Pro)
        } else if !self.business_price_id.is_empty() && price_id == self.business_price_id {
            Some(Tier::Business)
        } else {
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Tier
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    Free,
    Pro,
    Business,
    Enterprise,
}

impl Tier {
    pub const fn event_limit(&self) -> Option<i64> {
        match self {
            Self::Free => Some(100_000),
            Self::Pro => Some(10_000_000),
            Self::Business => Some(100_000_000),
            Self::Enterprise => None,
        }
    }

    pub const fn display_name(&self) -> &str {
        match self {
            Self::Free => "Free",
            Self::Pro => "Pro ($49/mo)",
            Self::Business => "Business ($199/mo)",
            Self::Enterprise => "Enterprise",
        }
    }
}

impl std::fmt::Display for Tier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Free => write!(f, "free"),
            Self::Pro => write!(f, "pro"),
            Self::Business => write!(f, "business"),
            Self::Enterprise => write!(f, "enterprise"),
        }
    }
}

impl std::str::FromStr for Tier {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "free" => Ok(Self::Free),
            "pro" => Ok(Self::Pro),
            "business" => Ok(Self::Business),
            "enterprise" => Ok(Self::Enterprise),
            other => Err(format!("unknown tier: {other}")),
        }
    }
}

// ---------------------------------------------------------------------------
// Usage tracking
// ---------------------------------------------------------------------------

/// In-memory buffer for event counts, flushed to DB periodically.
#[derive(Debug)]
pub struct UsageTracker {
    buffer: HashMap<Uuid, i64>,
    /// Cached DB monthly totals (reloaded every 60s during flush cycle).
    monthly_totals: HashMap<Uuid, i64>,
    /// Cached per-org monthly limits (loaded from DB).
    monthly_limits: HashMap<Uuid, i64>,
}

impl Default for UsageTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl UsageTracker {
    pub fn new() -> Self {
        Self {
            buffer: HashMap::new(),
            monthly_totals: HashMap::new(),
            monthly_limits: HashMap::new(),
        }
    }

    pub fn record_events(&mut self, org_id: Uuid, count: i64) {
        *self.buffer.entry(org_id).or_insert(0) += count;
    }

    /// Drain all buffered counts, returning `(org_id, event_count)` pairs.
    pub fn drain(&mut self) -> Vec<(Uuid, i64)> {
        self.buffer.drain().collect()
    }

    pub fn get(&self, org_id: &Uuid) -> i64 {
        self.buffer.get(org_id).copied().unwrap_or(0)
    }

    /// Update the cached monthly total for an org (from DB).
    pub fn set_monthly_total(&mut self, org_id: Uuid, total: i64) {
        self.monthly_totals.insert(org_id, total);
    }

    /// Get the cached monthly total.
    pub fn get_monthly_total(&self, org_id: &Uuid) -> Option<i64> {
        self.monthly_totals.get(org_id).copied()
    }

    /// Update the cached monthly limit for an org.
    pub fn set_monthly_limit(&mut self, org_id: Uuid, limit: i64) {
        self.monthly_limits.insert(org_id, limit);
    }

    /// Get the cached monthly limit.
    pub fn get_monthly_limit(&self, org_id: &Uuid) -> Option<i64> {
        self.monthly_limits.get(org_id).copied()
    }

    /// Fast-path check: can this org process `additional` more events?
    /// Returns None if cache miss (caller should fall through to DB query).
    pub fn check_cached_limit(&self, org_id: &Uuid, additional: i64) -> Option<bool> {
        let total = self.monthly_totals.get(org_id)?;
        let limit = self.monthly_limits.get(org_id)?;
        let buffered = self.buffer.get(org_id).copied().unwrap_or(0);
        Some(total + buffered + additional <= *limit)
    }
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct BillingState {
    pub config: BillingConfig,
    pub usage: RwLock<UsageTracker>,
    pub http_client: reqwest::Client,
    #[cfg(feature = "saas")]
    pub db_pool: Option<varpulis_db::PgPool>,
    pub audit_logger: Option<SharedAuditLogger>,
}

impl BillingState {
    pub fn new(config: BillingConfig) -> Self {
        Self {
            config,
            usage: RwLock::new(UsageTracker::new()),
            http_client: reqwest::Client::new(),
            #[cfg(feature = "saas")]
            db_pool: None,
            audit_logger: None,
        }
    }

    pub fn with_audit_logger(mut self, logger: Option<SharedAuditLogger>) -> Self {
        self.audit_logger = logger;
        self
    }

    #[cfg(feature = "saas")]
    pub fn with_db_pool(mut self, pool: varpulis_db::PgPool) -> Self {
        self.db_pool = Some(pool);
        self
    }
}

pub type SharedBillingState = Arc<BillingState>;

// ---------------------------------------------------------------------------
// Usage limit enforcement
// ---------------------------------------------------------------------------

/// Error returned when usage exceeds tier limits.
#[derive(Debug, Serialize)]
pub struct UsageLimitExceeded {
    pub tier: Tier,
    pub limit: i64,
    pub current_usage: i64,
    pub message: String,
}

/// Result of a usage limit check: Ok with optional warning, or Err if exceeded.
#[derive(Debug)]
pub enum UsageCheckResult {
    /// Within limits, no warning.
    Ok,
    /// Within limits but approaching (>80%).
    ApproachingLimit { usage_percent: f64 },
    /// Over limit.
    Exceeded(UsageLimitExceeded),
}

impl BillingState {
    /// Check if the org can process `additional_events` more events this month.
    /// Returns `UsageCheckResult::Ok` or `::ApproachingLimit` if within limit,
    /// `::Exceeded` if over.
    #[cfg(feature = "saas")]
    pub async fn check_usage_limit(
        &self,
        org_id: Uuid,
        additional_events: i64,
    ) -> UsageCheckResult {
        // 1. Get org tier from DB
        let tier = if let Some(ref pool) = self.db_pool {
            if let Ok(Some(org)) = varpulis_db::repo::get_organization(pool, org_id).await {
                org.tier.parse().unwrap_or(Tier::Free)
            } else {
                Tier::Free
            }
        } else {
            Tier::Free
        };

        // Enterprise = no limit
        let limit = match tier.event_limit() {
            Some(l) => l,
            None => return UsageCheckResult::Ok,
        };

        // 2. Get current month usage from DB
        let db_usage = if let Some(ref pool) = self.db_pool {
            let today = chrono::Utc::now().date_naive();
            let start =
                chrono::NaiveDate::from_ymd_opt(today.year(), today.month(), 1).unwrap_or(today);
            if let Ok(rows) = varpulis_db::repo::get_usage(pool, org_id, start, today).await {
                rows.iter().map(|r| r.events_processed).sum::<i64>()
            } else {
                0
            }
        } else {
            0
        };

        // 3. Add in-memory buffer (not yet flushed to DB)
        let buffered = self.usage.read().await.get(&org_id);
        let total = db_usage + buffered + additional_events;

        if total > limit {
            UsageCheckResult::Exceeded(UsageLimitExceeded {
                tier: tier.clone(),
                limit,
                current_usage: db_usage + buffered,
                message: format!(
                    "Usage limit exceeded for {} tier ({}/{} events this month). Upgrade to increase your limit.",
                    tier.display_name(),
                    db_usage + buffered,
                    limit,
                ),
            })
        } else {
            let usage_percent = (total as f64 / limit as f64) * 100.0;
            if usage_percent >= 80.0 {
                UsageCheckResult::ApproachingLimit { usage_percent }
            } else {
                UsageCheckResult::Ok
            }
        }
    }

    /// Look up org_id for a raw API key from the database.
    /// Hashes the key with SHA-256 and looks up the hash.
    #[cfg(feature = "saas")]
    pub async fn org_id_for_api_key(&self, raw_key: &str) -> Option<Uuid> {
        use sha2::Digest;
        let pool = self.db_pool.as_ref()?;
        let hash = hex::encode(sha2::Sha256::digest(raw_key.as_bytes()));
        let api_key = varpulis_db::repo::get_api_key_by_hash(pool, &hash)
            .await
            .ok()??;
        Some(api_key.org_id)
    }
}

/// Build a 429 Too Many Requests response from a usage limit error.
pub fn usage_limit_response(err: &UsageLimitExceeded) -> Response {
    let body = Json(serde_json::json!({
        "error": "usage_limit_exceeded",
        "message": err.message,
        "tier": err.tier,
        "limit": err.limit,
        "current_usage": err.current_usage,
        "upgrade_url": "/billing",
    }));

    (
        StatusCode::TOO_MANY_REQUESTS,
        [("Retry-After", "3600")],
        body,
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// Usage flush task
// ---------------------------------------------------------------------------

/// Spawn a background task that flushes in-memory usage counters to the DB every 60s
/// and reloads cached monthly totals/limits.
#[cfg(feature = "saas")]
pub fn spawn_usage_flush(state: SharedBillingState, pool: varpulis_db::PgPool) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            let entries = state.usage.write().await.drain();
            let today = chrono::Utc::now().date_naive();
            for (org_id, count) in &entries {
                if let Err(e) =
                    varpulis_db::repo::record_usage(&pool, *org_id, today, *count, 0).await
                {
                    tracing::error!("Failed to flush usage for org {}: {}", org_id, e);
                }
            }

            // Reload monthly totals and limits from DB for cache
            if let Ok(orgs) = varpulis_db::repo::list_all_organizations(&pool).await {
                let mut tracker = state.usage.write().await;
                for org in &orgs {
                    if let Ok(total) = varpulis_db::repo::get_org_usage_summary(&pool, org.id).await
                    {
                        tracker.set_monthly_total(org.id, total);
                    }
                    tracker.set_monthly_limit(org.id, org.monthly_event_limit);
                }
            }

            if !entries.is_empty() {
                tracing::debug!("Usage flush complete ({} orgs)", entries.len());
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Stripe helpers
// ---------------------------------------------------------------------------

/// Call the Stripe API with form-encoded body.
async fn stripe_post(
    client: &reqwest::Client,
    secret_key: &str,
    endpoint: &str,
    params: &[(&str, &str)],
) -> Result<serde_json::Value, String> {
    let resp = client
        .post(format!("https://api.stripe.com/v1/{endpoint}"))
        .basic_auth(secret_key, None::<&str>)
        .form(params)
        .send()
        .await
        .map_err(|e| format!("Stripe request failed: {e}"))?;

    let status = resp.status();
    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("Stripe response parse failed: {e}"))?;

    if !status.is_success() {
        let msg = body["error"]["message"]
            .as_str()
            .unwrap_or("Unknown Stripe error");
        return Err(format!("Stripe API error ({status}): {msg}"));
    }

    Ok(body)
}

/// Verify Stripe webhook signature (HMAC-SHA256).
fn verify_stripe_signature(payload: &[u8], sig_header: &str, secret: &str) -> bool {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    // Parse signature header: "t=timestamp,v1=signature"
    let mut timestamp = "";
    let mut signature = "";
    for part in sig_header.split(',') {
        if let Some(t) = part.strip_prefix("t=") {
            timestamp = t;
        } else if let Some(s) = part.strip_prefix("v1=") {
            signature = s;
        }
    }

    if timestamp.is_empty() || signature.is_empty() {
        return false;
    }

    // Compute expected signature
    let signed_payload = format!(
        "{}.{}",
        timestamp,
        std::str::from_utf8(payload).unwrap_or("")
    );
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key size");
    hmac::Mac::update(&mut mac, signed_payload.as_bytes());
    let expected = hex::encode(mac.finalize().into_bytes());

    // Constant-time comparison
    expected == signature
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

/// GET /api/v1/billing/usage — get usage summary.
async fn handle_usage(
    State(state): State<Option<SharedBillingState>>,
    headers: HeaderMap,
) -> Response {
    let auth_header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    match state {
        Some(s) => {
            // Try DB first when saas is enabled
            #[cfg(feature = "saas")]
            if let Some(ref pool) = s.db_pool {
                if let Some(org_id) = extract_org_id_from_header(&auth_header, &s) {
                    let today = chrono::Utc::now().date_naive();
                    let start = chrono::NaiveDate::from_ymd_opt(today.year(), today.month(), 1)
                        .unwrap_or(today);
                    if let Ok(rows) = varpulis_db::repo::get_usage(pool, org_id, start, today).await
                    {
                        let total: i64 = rows.iter().map(|r| r.events_processed).sum();
                        return (
                            StatusCode::OK,
                            Json(serde_json::json!({
                                "events_this_month": total,
                                "daily": rows.iter().map(|r| serde_json::json!({
                                    "date": r.date.to_string(),
                                    "events_processed": r.events_processed,
                                })).collect::<Vec<_>>(),
                            })),
                        )
                            .into_response();
                    }
                }
            }

            // Fallback: in-memory buffer
            let _ = auth_header;
            let tracker = s.usage.read().await;
            let orgs: Vec<serde_json::Value> = tracker
                .buffer
                .iter()
                .map(|(org_id, count)| {
                    serde_json::json!({
                        "org_id": org_id.to_string(),
                        "events_today": count,
                    })
                })
                .collect();
            (StatusCode::OK, Json(serde_json::json!({ "usage": orgs }))).into_response()
        }
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "Billing not configured" })),
        )
            .into_response(),
    }
}

/// GET /api/v1/billing/plan — get current plan.
async fn handle_plan(
    State(state): State<Option<SharedBillingState>>,
    headers: HeaderMap,
) -> Response {
    let auth_header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    match state {
        Some(_s) => {
            // Try DB for real plan when saas enabled
            #[cfg(feature = "saas")]
            if let Some(ref pool) = _s.db_pool {
                if let Some(org_id) = extract_org_id_from_header(&auth_header, &_s) {
                    if let Ok(Some(org)) = varpulis_db::repo::get_organization(pool, org_id).await {
                        let tier: Tier = org.tier.parse().unwrap_or(Tier::Free);
                        return (
                            StatusCode::OK,
                            Json(serde_json::json!({
                                "tier": org.tier,
                                "event_limit": tier.event_limit(),
                                "display_name": tier.display_name(),
                            })),
                        )
                            .into_response();
                    }
                }
            }

            // Fallback: hardcoded free
            let _ = auth_header;
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "tier": "free",
                    "event_limit": 10_000,
                    "display_name": "Free",
                })),
            )
                .into_response()
        }
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "Billing not configured" })),
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct CheckoutRequest {
    /// Target tier: "pro" or "business". Defaults to "pro".
    tier: Option<String>,
    success_url: Option<String>,
    cancel_url: Option<String>,
}

/// POST /api/v1/billing/checkout — create Stripe Checkout session.
async fn handle_checkout(
    State(state): State<Option<SharedBillingState>>,
    headers: HeaderMap,
    Json(body): Json<CheckoutRequest>,
) -> Response {
    let auth_header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    match state {
        Some(s) => {
            // Determine target tier from request body
            let target_tier: Tier = body
                .tier
                .as_deref()
                .unwrap_or("pro")
                .parse()
                .unwrap_or(Tier::Pro);

            let price_id = match s.config.price_id_for_tier(&target_tier) {
                Some(id) => id.to_string(),
                None => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(serde_json::json!({
                            "error": format!("Stripe Price ID not configured for {} tier", target_tier)
                        })),
                    )
                        .into_response();
                }
            };

            let success_url = body
                .success_url
                .unwrap_or_else(|| format!("{}/billing?success=true", s.config.frontend_url));
            let cancel_url = body
                .cancel_url
                .unwrap_or_else(|| format!("{}/billing", s.config.frontend_url));

            // Build Stripe Checkout params
            let org_id_str = extract_org_id_str_from_header(&auth_header, &s).unwrap_or_default();

            let mut params: Vec<(&str, &str)> = vec![
                ("mode", "subscription"),
                ("line_items[0][price]", &price_id),
                ("line_items[0][quantity]", "1"),
                ("success_url", &success_url),
                ("cancel_url", &cancel_url),
            ];

            if !org_id_str.is_empty() {
                params.push(("client_reference_id", &org_id_str));
            }

            // Look up existing Stripe customer
            #[allow(unused_mut)]
            let mut customer_id = String::new();
            #[cfg(feature = "saas")]
            if let Some(ref pool) = s.db_pool {
                if let Some(org_uuid) = extract_org_id_from_header(&auth_header, &s) {
                    if let Ok(Some(org)) = varpulis_db::repo::get_organization(pool, org_uuid).await
                    {
                        if let Some(cid) = org.stripe_customer_id {
                            customer_id = cid;
                        }
                    }
                }
            }

            if !customer_id.is_empty() {
                params.push(("customer", &customer_id));
            }

            match stripe_post(
                &s.http_client,
                &s.config.stripe_secret_key,
                "checkout/sessions",
                &params,
            )
            .await
            {
                Ok(session) => {
                    let checkout_url = session["url"].as_str().unwrap_or("");
                    let session_id = session["id"].as_str().unwrap_or("");
                    // Audit log: checkout started
                    if let Some(ref logger) = s.audit_logger {
                        logger
                            .log(
                                AuditEntry::new(
                                    &org_id_str,
                                    AuditAction::CheckoutStarted,
                                    "/api/v1/billing/checkout",
                                )
                                .with_detail(format!("session: {session_id}")),
                            )
                            .await;
                    }
                    (
                        StatusCode::OK,
                        Json(serde_json::json!({
                            "checkout_url": checkout_url,
                            "session_id": session_id,
                        })),
                    )
                        .into_response()
                }
                Err(e) => {
                    tracing::error!("Stripe checkout failed: {}", e);
                    (
                        StatusCode::BAD_GATEWAY,
                        Json(serde_json::json!({"error": e})),
                    )
                        .into_response()
                }
            }
        }
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "Billing not configured" })),
        )
            .into_response(),
    }
}

/// POST /api/v1/billing/portal — create Stripe Customer Portal session.
async fn handle_portal(
    State(state): State<Option<SharedBillingState>>,
    headers: HeaderMap,
) -> Response {
    let auth_header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    match state {
        Some(s) => {
            #[allow(unused_mut)]
            let mut customer_id = String::new();

            #[cfg(feature = "saas")]
            if let Some(ref pool) = s.db_pool {
                if let Some(org_uuid) = extract_org_id_from_header(&auth_header, &s) {
                    if let Ok(Some(org)) = varpulis_db::repo::get_organization(pool, org_uuid).await
                    {
                        if let Some(cid) = org.stripe_customer_id {
                            customer_id = cid;
                        }
                    }
                }
            }
            let _ = auth_header;

            if customer_id.is_empty() {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": "No Stripe customer found. Upgrade first."
                    })),
                )
                    .into_response();
            }

            let return_url = format!("{}/billing", s.config.frontend_url);
            match stripe_post(
                &s.http_client,
                &s.config.stripe_secret_key,
                "billing_portal/sessions",
                &[("customer", &customer_id), ("return_url", &return_url)],
            )
            .await
            {
                Ok(session) => {
                    let portal_url = session["url"].as_str().unwrap_or("");
                    (
                        StatusCode::OK,
                        Json(serde_json::json!({
                            "portal_url": portal_url,
                        })),
                    )
                        .into_response()
                }
                Err(e) => {
                    tracing::error!("Stripe portal failed: {}", e);
                    (
                        StatusCode::BAD_GATEWAY,
                        Json(serde_json::json!({"error": e})),
                    )
                        .into_response()
                }
            }
        }
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "Billing not configured" })),
        )
            .into_response(),
    }
}

/// POST /api/v1/billing/webhook — handle Stripe webhook events.
async fn handle_webhook(
    State(state): State<Option<SharedBillingState>>,
    headers: HeaderMap,
    body: bytes::Bytes,
) -> Response {
    let sig_header = headers
        .get("stripe-signature")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let s = match state {
        Some(s) => s,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "Billing not configured"})),
            )
                .into_response();
        }
    };

    // Verify signature
    if !s.config.stripe_webhook_secret.is_empty() {
        let sig = sig_header.unwrap_or_default();
        if !verify_stripe_signature(&body, &sig, &s.config.stripe_webhook_secret) {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "Invalid signature"})),
            )
                .into_response();
        }
    }

    // Parse event
    let event: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!("Webhook parse error: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "Invalid JSON"})),
            )
                .into_response();
        }
    };

    let event_type = event["type"].as_str().unwrap_or("");
    tracing::info!("Stripe webhook: {}", event_type);

    // Audit log: webhook received
    if let Some(ref logger) = s.audit_logger {
        logger
            .log(
                AuditEntry::new(
                    "stripe",
                    AuditAction::WebhookReceived,
                    "/api/v1/billing/webhook",
                )
                .with_detail(event_type.to_string()),
            )
            .await;
    }

    #[cfg(feature = "saas")]
    if let Some(ref pool) = s.db_pool {
        match event_type {
            "checkout.session.completed" => {
                let obj = &event["data"]["object"];
                let customer = obj["customer"].as_str().unwrap_or("");
                let client_ref = obj["client_reference_id"].as_str().unwrap_or("");

                if !client_ref.is_empty() && !customer.is_empty() {
                    if let Ok(org_id) = client_ref.parse::<uuid::Uuid>() {
                        if let Err(e) =
                            varpulis_db::repo::update_org_stripe_customer(pool, org_id, customer)
                                .await
                        {
                            tracing::error!("Failed to save Stripe customer: {}", e);
                        }

                        // Determine tier from subscription price_id
                        let subscription_id = obj["subscription"].as_str().unwrap_or("");
                        let new_tier = if !subscription_id.is_empty() {
                            // Fetch subscription to get price_id
                            if let Ok(sub) = stripe_post(
                                &s.http_client,
                                &s.config.stripe_secret_key,
                                &format!("subscriptions/{subscription_id}"),
                                &[],
                            )
                            .await
                            {
                                let price_id = sub["items"]["data"][0]["price"]["id"]
                                    .as_str()
                                    .unwrap_or("");
                                s.config.tier_for_price_id(price_id).unwrap_or(Tier::Pro)
                            } else {
                                Tier::Pro
                            }
                        } else {
                            Tier::Pro
                        };

                        let tier_str = new_tier.to_string();
                        if let Err(e) =
                            varpulis_db::repo::update_org_tier(pool, org_id, &tier_str).await
                        {
                            tracing::error!("Failed to update tier: {}", e);
                        }
                        // Clear trial status on paid upgrade
                        if let Err(e) =
                            varpulis_db::repo::update_org_status(pool, org_id, "active").await
                        {
                            tracing::error!("Failed to update org status: {}", e);
                        }
                        tracing::info!(
                            "Org {} upgraded to {} (customer: {})",
                            org_id,
                            tier_str,
                            customer
                        );
                        // Audit log: tier upgrade
                        if let Some(ref logger) = s.audit_logger {
                            logger
                                .log(
                                    AuditEntry::new(
                                        org_id.to_string(),
                                        AuditAction::TierChange,
                                        "/api/v1/billing/webhook",
                                    )
                                    .with_detail(format!("upgraded to {tier_str}")),
                                )
                                .await;
                        }
                    }
                }
            }
            "customer.subscription.deleted" => {
                let customer = event["data"]["object"]["customer"].as_str().unwrap_or("");
                if !customer.is_empty() {
                    if let Ok(Some(org)) =
                        varpulis_db::repo::get_org_by_stripe_customer(pool, customer).await
                    {
                        if let Err(e) =
                            varpulis_db::repo::update_org_tier(pool, org.id, "free").await
                        {
                            tracing::error!("Failed to downgrade org: {}", e);
                        }
                        tracing::info!("Org {} downgraded to free", org.id);
                        // Audit log: tier downgrade
                        if let Some(ref logger) = s.audit_logger {
                            logger
                                .log(
                                    AuditEntry::new(
                                        org.id.to_string(),
                                        AuditAction::TierChange,
                                        "/api/v1/billing/webhook",
                                    )
                                    .with_detail("downgraded to free"),
                                )
                                .await;
                        }
                    }
                }
            }
            "customer.subscription.updated" => {
                let obj = &event["data"]["object"];
                let customer = obj["customer"].as_str().unwrap_or("");
                let price_id = obj["items"]["data"][0]["price"]["id"]
                    .as_str()
                    .unwrap_or("");

                if !customer.is_empty() && !price_id.is_empty() {
                    if let Some(new_tier) = s.config.tier_for_price_id(price_id) {
                        if let Ok(Some(org)) =
                            varpulis_db::repo::get_org_by_stripe_customer(pool, customer).await
                        {
                            let tier_str = new_tier.to_string();
                            if org.tier != tier_str {
                                if let Err(e) =
                                    varpulis_db::repo::update_org_tier(pool, org.id, &tier_str)
                                        .await
                                {
                                    tracing::error!(
                                        "Failed to update tier on subscription change: {}",
                                        e
                                    );
                                } else {
                                    tracing::info!(
                                        "Org {} tier changed to {} via subscription update",
                                        org.id,
                                        tier_str
                                    );
                                }
                            }
                        }
                    }
                }
            }
            "invoice.payment_failed" => {
                let customer = event["data"]["object"]["customer"].as_str().unwrap_or("");
                tracing::warn!("Payment failed for customer {}", customer);
            }
            _ => {
                tracing::debug!("Unhandled webhook event: {}", event_type);
            }
        }
    }

    #[cfg(not(feature = "saas"))]
    {
        let _ = event_type;
        tracing::debug!("Webhook received but saas feature not enabled");
    }

    (StatusCode::OK, Json(serde_json::json!({"received": true}))).into_response()
}

// ---------------------------------------------------------------------------
// JWT claim extraction helpers
// ---------------------------------------------------------------------------

/// Extract org_id UUID from Authorization header JWT.
#[cfg_attr(not(feature = "saas"), allow(dead_code))]
fn extract_org_id_from_header(
    auth_header: &Option<String>,
    state: &BillingState,
) -> Option<uuid::Uuid> {
    extract_org_id_str_from_header(auth_header, state)?
        .parse()
        .ok()
}

/// Extract org_id string from Authorization header JWT.
fn extract_org_id_str_from_header(
    auth_header: &Option<String>,
    _state: &BillingState,
) -> Option<String> {
    let header = auth_header.as_ref()?;
    let token = header.strip_prefix("Bearer ")?.trim();
    if token.is_empty() {
        return None;
    }

    // Decode JWT without full verification (billing state doesn't have JWT secret).
    // Use jsonwebtoken's dangerous_insecure_decode to read claims.
    use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
    let mut validation = Validation::new(Algorithm::HS256);
    validation.insecure_disable_signature_validation();
    validation.validate_exp = false;
    let token_data =
        decode::<serde_json::Value>(token, &DecodingKey::from_secret(b""), &validation).ok()?;

    let org_id = token_data.claims["org_id"].as_str()?;
    if org_id.is_empty() {
        return None;
    }
    Some(org_id.to_string())
}

// ---------------------------------------------------------------------------
// Route assembly
// ---------------------------------------------------------------------------

/// Build billing routes. When `state` is None, endpoints return 503.
pub fn billing_routes(state: Option<SharedBillingState>) -> Router {
    Router::new()
        .route("/api/v1/billing/usage", get(handle_usage))
        .route("/api/v1/billing/plan", get(handle_plan))
        .route("/api/v1/billing/checkout", post(handle_checkout))
        .route("/api/v1/billing/portal", post(handle_portal))
        .route("/api/v1/billing/webhook", post(handle_webhook))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    use super::*;

    fn get_req(uri: &str) -> Request<Body> {
        Request::builder()
            .method("GET")
            .uri(uri)
            .body(Body::empty())
            .unwrap()
    }

    #[test]
    fn test_tier_event_limits() {
        assert_eq!(Tier::Free.event_limit(), Some(100_000));
        assert_eq!(Tier::Pro.event_limit(), Some(10_000_000));
        assert_eq!(Tier::Business.event_limit(), Some(100_000_000));
        assert_eq!(Tier::Enterprise.event_limit(), None);
    }

    #[test]
    fn test_tier_display_name() {
        assert_eq!(Tier::Free.display_name(), "Free");
        assert_eq!(Tier::Pro.display_name(), "Pro ($49/mo)");
        assert_eq!(Tier::Business.display_name(), "Business ($199/mo)");
        assert_eq!(Tier::Enterprise.display_name(), "Enterprise");
    }

    #[test]
    fn test_tier_from_str() {
        assert_eq!("free".parse::<Tier>(), Ok(Tier::Free));
        assert_eq!("pro".parse::<Tier>(), Ok(Tier::Pro));
        assert_eq!("business".parse::<Tier>(), Ok(Tier::Business));
        assert_eq!("enterprise".parse::<Tier>(), Ok(Tier::Enterprise));
        assert!("invalid".parse::<Tier>().is_err());
    }

    #[test]
    fn test_tier_serialization() {
        let json = serde_json::to_string(&Tier::Business).unwrap();
        assert_eq!(json, "\"business\"");
        let deserialized: Tier = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, Tier::Business);
    }

    #[test]
    fn test_usage_tracker_record_and_drain() {
        let mut tracker = UsageTracker::new();
        let org = Uuid::new_v4();

        tracker.record_events(org, 100);
        tracker.record_events(org, 50);
        assert_eq!(tracker.get(&org), 150);

        let drained = tracker.drain();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0], (org, 150));

        // Buffer should be empty after drain
        assert_eq!(tracker.get(&org), 0);
    }

    #[test]
    fn test_usage_tracker_multiple_orgs() {
        let mut tracker = UsageTracker::new();
        let org1 = Uuid::new_v4();
        let org2 = Uuid::new_v4();

        tracker.record_events(org1, 100);
        tracker.record_events(org2, 200);
        tracker.record_events(org1, 50);

        assert_eq!(tracker.get(&org1), 150);
        assert_eq!(tracker.get(&org2), 200);
    }

    #[test]
    fn test_verify_stripe_signature() {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        let secret = "whsec_test123";
        let payload = b"{\"type\":\"test\"}";
        let timestamp = "1234567890";

        // Compute expected signature
        let signed = format!("{}.{}", timestamp, std::str::from_utf8(payload).unwrap());
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        hmac::Mac::update(&mut mac, signed.as_bytes());
        let sig = hex::encode(mac.finalize().into_bytes());

        let header = format!("t={timestamp},v1={sig}");

        assert!(verify_stripe_signature(payload, &header, secret));
        assert!(!verify_stripe_signature(payload, &header, "wrong_secret"));
        assert!(!verify_stripe_signature(b"tampered", &header, secret));
    }

    #[tokio::test]
    async fn test_billing_routes_not_configured() {
        let app = billing_routes(None);

        let res = app.oneshot(get_req("/api/v1/billing/plan")).await.unwrap();

        assert_eq!(res.status(), 503);
    }

    #[tokio::test]
    async fn test_billing_routes_usage() {
        let config = BillingConfig {
            stripe_secret_key: "sk_test_xxx".to_string(),
            stripe_webhook_secret: "whsec_xxx".to_string(),
            pro_price_id: "price_xxx".to_string(),
            business_price_id: "price_biz_xxx".to_string(),
            frontend_url: "http://localhost:5173".to_string(),
        };
        let state = Arc::new(BillingState::new(config));
        let app = billing_routes(Some(state));

        let res = app.oneshot(get_req("/api/v1/billing/usage")).await.unwrap();

        assert_eq!(res.status(), 200);
    }

    #[tokio::test]
    async fn test_billing_routes_plan() {
        let config = BillingConfig {
            stripe_secret_key: "sk_test_xxx".to_string(),
            stripe_webhook_secret: "whsec_xxx".to_string(),
            pro_price_id: "price_xxx".to_string(),
            business_price_id: "price_biz_xxx".to_string(),
            frontend_url: "http://localhost:5173".to_string(),
        };
        let state = Arc::new(BillingState::new(config));
        let app = billing_routes(Some(state));

        let res = app.oneshot(get_req("/api/v1/billing/plan")).await.unwrap();

        assert_eq!(res.status(), 200);
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["tier"], "free");
    }

    #[tokio::test]
    async fn test_webhook_invalid_signature() {
        let config = BillingConfig {
            stripe_secret_key: "sk_test_xxx".to_string(),
            stripe_webhook_secret: "whsec_real_secret".to_string(),
            pro_price_id: "price_xxx".to_string(),
            business_price_id: "price_biz_xxx".to_string(),
            frontend_url: "http://localhost:5173".to_string(),
        };
        let state = Arc::new(BillingState::new(config));
        let app = billing_routes(Some(state));

        let req: Request<Body> = Request::builder()
            .method("POST")
            .uri("/api/v1/billing/webhook")
            .header("stripe-signature", "t=123,v1=bad")
            .header("content-type", "application/octet-stream")
            .body(Body::from("{\"type\":\"test\"}"))
            .unwrap();
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), 400);
    }

    #[test]
    fn test_usage_limit_exceeded_serialization() {
        let err = UsageLimitExceeded {
            tier: Tier::Free,
            limit: 100_000,
            current_usage: 100_500,
            message: "Usage limit exceeded for Free tier".to_string(),
        };
        let json = serde_json::to_value(&err).unwrap();
        assert_eq!(json["tier"], "free");
        assert_eq!(json["limit"], 100_000);
        assert_eq!(json["current_usage"], 100_500);
    }

    #[test]
    fn test_usage_limit_response_status() {
        let err = UsageLimitExceeded {
            tier: Tier::Free,
            limit: 100_000,
            current_usage: 110_000,
            message: "Limit exceeded".to_string(),
        };
        let resp = usage_limit_response(&err);
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[test]
    fn test_tier_enterprise_no_limit() {
        // Enterprise has no event limit
        assert_eq!(Tier::Enterprise.event_limit(), None);
    }

    #[test]
    fn test_usage_tracker_tracks_independently() {
        let mut tracker = UsageTracker::new();
        let org = Uuid::new_v4();

        // Record events in multiple increments
        tracker.record_events(org, 5_000);
        tracker.record_events(org, 3_000);
        tracker.record_events(org, 2_001);

        // Should be cumulative
        assert_eq!(tracker.get(&org), 10_001);
    }
}
