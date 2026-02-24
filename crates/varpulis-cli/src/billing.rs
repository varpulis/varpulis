//! Stripe billing integration for Varpulis Cloud.
//!
//! Provides usage tracking, tier management, and Stripe Checkout/Portal
//! integration via REST endpoints.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;
use warp::Filter;

#[cfg(feature = "saas")]
use chrono::Datelike;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Billing configuration loaded from environment variables.
#[derive(Debug, Clone)]
pub struct BillingConfig {
    pub stripe_secret_key: String,
    pub stripe_webhook_secret: String,
    pub pro_price_id: String,
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
        let frontend_url =
            std::env::var("FRONTEND_URL").unwrap_or_else(|_| "http://localhost:5173".to_string());

        Some(Self {
            stripe_secret_key: secret_key,
            stripe_webhook_secret: webhook_secret,
            pro_price_id,
            frontend_url,
        })
    }
}

// ---------------------------------------------------------------------------
// Tier
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    Free,
    Pro,
    Enterprise,
}

impl Tier {
    pub fn event_limit(&self) -> Option<i64> {
        match self {
            Tier::Free => Some(10_000),
            Tier::Pro => Some(10_000_000),
            Tier::Enterprise => None,
        }
    }

    pub fn display_name(&self) -> &str {
        match self {
            Tier::Free => "Free",
            Tier::Pro => "Pro ($49/mo)",
            Tier::Enterprise => "Enterprise",
        }
    }
}

impl std::fmt::Display for Tier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Tier::Free => write!(f, "free"),
            Tier::Pro => write!(f, "pro"),
            Tier::Enterprise => write!(f, "enterprise"),
        }
    }
}

impl std::str::FromStr for Tier {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "free" => Ok(Tier::Free),
            "pro" => Ok(Tier::Pro),
            "enterprise" => Ok(Tier::Enterprise),
            other => Err(format!("unknown tier: {}", other)),
        }
    }
}

// ---------------------------------------------------------------------------
// Usage tracking
// ---------------------------------------------------------------------------

/// In-memory buffer for event counts, flushed to DB periodically.
pub struct UsageTracker {
    buffer: HashMap<Uuid, i64>,
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
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

pub struct BillingState {
    pub config: BillingConfig,
    pub usage: RwLock<UsageTracker>,
    pub http_client: reqwest::Client,
    #[cfg(feature = "saas")]
    pub db_pool: Option<varpulis_db::PgPool>,
}

impl BillingState {
    pub fn new(config: BillingConfig) -> Self {
        Self {
            config,
            usage: RwLock::new(UsageTracker::new()),
            http_client: reqwest::Client::new(),
            #[cfg(feature = "saas")]
            db_pool: None,
        }
    }

    #[cfg(feature = "saas")]
    pub fn with_db_pool(mut self, pool: varpulis_db::PgPool) -> Self {
        self.db_pool = Some(pool);
        self
    }
}

pub type SharedBillingState = Arc<BillingState>;

// ---------------------------------------------------------------------------
// Usage flush task
// ---------------------------------------------------------------------------

/// Spawn a background task that flushes in-memory usage counters to the DB every 60s.
#[cfg(feature = "saas")]
pub fn spawn_usage_flush(state: SharedBillingState, pool: varpulis_db::PgPool) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            let entries = state.usage.write().await.drain();
            if entries.is_empty() {
                continue;
            }
            let today = chrono::Utc::now().date_naive();
            for (org_id, count) in entries {
                if let Err(e) =
                    varpulis_db::repo::record_usage(&pool, org_id, today, count, 0).await
                {
                    tracing::error!("Failed to flush usage for org {}: {}", org_id, e);
                }
            }
            tracing::debug!("Usage flush complete");
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
        .post(format!("https://api.stripe.com/v1/{}", endpoint))
        .basic_auth(secret_key, None::<&str>)
        .form(params)
        .send()
        .await
        .map_err(|e| format!("Stripe request failed: {}", e))?;

    let status = resp.status();
    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("Stripe response parse failed: {}", e))?;

    if !status.is_success() {
        let msg = body["error"]["message"]
            .as_str()
            .unwrap_or("Unknown Stripe error");
        return Err(format!("Stripe API error ({}): {}", status, msg));
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
    auth_header: Option<String>,
    state: Option<SharedBillingState>,
) -> Result<impl warp::Reply, warp::Rejection> {
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
                        return Ok(warp::reply::with_status(
                            warp::reply::json(&serde_json::json!({
                                "events_this_month": total,
                                "daily": rows.iter().map(|r| serde_json::json!({
                                    "date": r.date.to_string(),
                                    "events_processed": r.events_processed,
                                })).collect::<Vec<_>>(),
                            })),
                            warp::http::StatusCode::OK,
                        ));
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
            Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({ "usage": orgs })),
                warp::http::StatusCode::OK,
            ))
        }
        None => Ok(warp::reply::with_status(
            warp::reply::json(&serde_json::json!({ "error": "Billing not configured" })),
            warp::http::StatusCode::SERVICE_UNAVAILABLE,
        )),
    }
}

/// GET /api/v1/billing/plan — get current plan.
async fn handle_plan(
    auth_header: Option<String>,
    state: Option<SharedBillingState>,
) -> Result<impl warp::Reply, warp::Rejection> {
    match state {
        Some(_s) => {
            // Try DB for real plan when saas enabled
            #[cfg(feature = "saas")]
            if let Some(ref pool) = _s.db_pool {
                if let Some(org_id) = extract_org_id_from_header(&auth_header, &_s) {
                    if let Ok(Some(org)) = varpulis_db::repo::get_organization(pool, org_id).await {
                        let tier: Tier = org.tier.parse().unwrap_or(Tier::Free);
                        return Ok(warp::reply::with_status(
                            warp::reply::json(&serde_json::json!({
                                "tier": org.tier,
                                "event_limit": tier.event_limit(),
                                "display_name": tier.display_name(),
                            })),
                            warp::http::StatusCode::OK,
                        ));
                    }
                }
            }

            // Fallback: hardcoded free
            let _ = auth_header;
            Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({
                    "tier": "free",
                    "event_limit": 10_000,
                    "display_name": "Free",
                })),
                warp::http::StatusCode::OK,
            ))
        }
        None => Ok(warp::reply::with_status(
            warp::reply::json(&serde_json::json!({ "error": "Billing not configured" })),
            warp::http::StatusCode::SERVICE_UNAVAILABLE,
        )),
    }
}

#[derive(Debug, Deserialize)]
struct CheckoutRequest {
    success_url: Option<String>,
    cancel_url: Option<String>,
}

/// POST /api/v1/billing/checkout — create Stripe Checkout session.
async fn handle_checkout(
    body: CheckoutRequest,
    auth_header: Option<String>,
    state: Option<SharedBillingState>,
) -> Result<impl warp::Reply, warp::Rejection> {
    match state {
        Some(s) => {
            if s.config.pro_price_id.is_empty() {
                return Ok(warp::reply::with_status(
                    warp::reply::json(&serde_json::json!({
                        "error": "Stripe Price ID not configured"
                    })),
                    warp::http::StatusCode::BAD_REQUEST,
                ));
            }

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
                ("line_items[0][price]", &s.config.pro_price_id),
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
                    Ok(warp::reply::with_status(
                        warp::reply::json(&serde_json::json!({
                            "checkout_url": checkout_url,
                            "session_id": session_id,
                        })),
                        warp::http::StatusCode::OK,
                    ))
                }
                Err(e) => {
                    tracing::error!("Stripe checkout failed: {}", e);
                    Ok(warp::reply::with_status(
                        warp::reply::json(&serde_json::json!({"error": e})),
                        warp::http::StatusCode::BAD_GATEWAY,
                    ))
                }
            }
        }
        None => Ok(warp::reply::with_status(
            warp::reply::json(&serde_json::json!({ "error": "Billing not configured" })),
            warp::http::StatusCode::SERVICE_UNAVAILABLE,
        )),
    }
}

/// POST /api/v1/billing/portal — create Stripe Customer Portal session.
async fn handle_portal(
    auth_header: Option<String>,
    state: Option<SharedBillingState>,
) -> Result<impl warp::Reply, warp::Rejection> {
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
                return Ok(warp::reply::with_status(
                    warp::reply::json(&serde_json::json!({
                        "error": "No Stripe customer found. Upgrade first."
                    })),
                    warp::http::StatusCode::BAD_REQUEST,
                ));
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
                    Ok(warp::reply::with_status(
                        warp::reply::json(&serde_json::json!({
                            "portal_url": portal_url,
                        })),
                        warp::http::StatusCode::OK,
                    ))
                }
                Err(e) => {
                    tracing::error!("Stripe portal failed: {}", e);
                    Ok(warp::reply::with_status(
                        warp::reply::json(&serde_json::json!({"error": e})),
                        warp::http::StatusCode::BAD_GATEWAY,
                    ))
                }
            }
        }
        None => Ok(warp::reply::with_status(
            warp::reply::json(&serde_json::json!({ "error": "Billing not configured" })),
            warp::http::StatusCode::SERVICE_UNAVAILABLE,
        )),
    }
}

/// POST /api/v1/billing/webhook — handle Stripe webhook events.
async fn handle_webhook(
    sig_header: Option<String>,
    body: bytes::Bytes,
    state: Option<SharedBillingState>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let s = match state {
        Some(s) => s,
        None => {
            return Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({"error": "Billing not configured"})),
                warp::http::StatusCode::SERVICE_UNAVAILABLE,
            ));
        }
    };

    // Verify signature
    if !s.config.stripe_webhook_secret.is_empty() {
        let sig = sig_header.unwrap_or_default();
        if !verify_stripe_signature(&body, &sig, &s.config.stripe_webhook_secret) {
            return Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({"error": "Invalid signature"})),
                warp::http::StatusCode::BAD_REQUEST,
            ));
        }
    }

    // Parse event
    let event: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!("Webhook parse error: {}", e);
            return Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({"error": "Invalid JSON"})),
                warp::http::StatusCode::BAD_REQUEST,
            ));
        }
    };

    let event_type = event["type"].as_str().unwrap_or("");
    tracing::info!("Stripe webhook: {}", event_type);

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
                        if let Err(e) =
                            varpulis_db::repo::update_org_tier(pool, org_id, "pro").await
                        {
                            tracing::error!("Failed to update tier: {}", e);
                        }
                        tracing::info!("Org {} upgraded to pro (customer: {})", org_id, customer);
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
                    }
                }
            }
            "customer.subscription.updated" => {
                // Could check for plan changes; for now just log
                tracing::info!("Subscription updated (no-op)");
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

    Ok(warp::reply::with_status(
        warp::reply::json(&serde_json::json!({"received": true})),
        warp::http::StatusCode::OK,
    ))
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
pub fn billing_routes(
    state: Option<SharedBillingState>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    let state1 = state.clone();
    let state2 = state.clone();
    let state3 = state.clone();
    let state4 = state.clone();
    let state5 = state;

    let usage = warp::path!("api" / "v1" / "billing" / "usage")
        .and(warp::get())
        .and(warp::header::optional::<String>("authorization"))
        .and(warp::any().map(move || state1.clone()))
        .and_then(handle_usage);

    let plan = warp::path!("api" / "v1" / "billing" / "plan")
        .and(warp::get())
        .and(warp::header::optional::<String>("authorization"))
        .and(warp::any().map(move || state2.clone()))
        .and_then(handle_plan);

    let checkout = warp::path!("api" / "v1" / "billing" / "checkout")
        .and(warp::post())
        .and(warp::body::json())
        .and(warp::header::optional::<String>("authorization"))
        .and(warp::any().map(move || state3.clone()))
        .and_then(handle_checkout);

    let portal = warp::path!("api" / "v1" / "billing" / "portal")
        .and(warp::post())
        .and(warp::header::optional::<String>("authorization"))
        .and(warp::any().map(move || state4.clone()))
        .and_then(handle_portal);

    let webhook = warp::path!("api" / "v1" / "billing" / "webhook")
        .and(warp::post())
        .and(warp::header::optional::<String>("stripe-signature"))
        .and(warp::body::bytes())
        .and(warp::any().map(move || state5.clone()))
        .and_then(handle_webhook);

    usage.or(plan).or(checkout).or(portal).or(webhook)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tier_event_limits() {
        assert_eq!(Tier::Free.event_limit(), Some(10_000));
        assert_eq!(Tier::Pro.event_limit(), Some(10_000_000));
        assert_eq!(Tier::Enterprise.event_limit(), None);
    }

    #[test]
    fn test_tier_display_name() {
        assert_eq!(Tier::Free.display_name(), "Free");
        assert_eq!(Tier::Pro.display_name(), "Pro ($49/mo)");
        assert_eq!(Tier::Enterprise.display_name(), "Enterprise");
    }

    #[test]
    fn test_tier_from_str() {
        assert_eq!("free".parse::<Tier>(), Ok(Tier::Free));
        assert_eq!("pro".parse::<Tier>(), Ok(Tier::Pro));
        assert_eq!("enterprise".parse::<Tier>(), Ok(Tier::Enterprise));
        assert!("invalid".parse::<Tier>().is_err());
    }

    #[test]
    fn test_tier_serialization() {
        let json = serde_json::to_string(&Tier::Pro).unwrap();
        assert_eq!(json, "\"pro\"");
        let deserialized: Tier = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, Tier::Pro);
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

        let header = format!("t={},v1={}", timestamp, sig);

        assert!(verify_stripe_signature(payload, &header, secret));
        assert!(!verify_stripe_signature(payload, &header, "wrong_secret"));
        assert!(!verify_stripe_signature(b"tampered", &header, secret));
    }

    #[tokio::test]
    async fn test_billing_routes_not_configured() {
        let routes = billing_routes(None);

        let res = warp::test::request()
            .method("GET")
            .path("/api/v1/billing/plan")
            .reply(&routes)
            .await;

        assert_eq!(res.status(), 503);
    }

    #[tokio::test]
    async fn test_billing_routes_usage() {
        let config = BillingConfig {
            stripe_secret_key: "sk_test_xxx".to_string(),
            stripe_webhook_secret: "whsec_xxx".to_string(),
            pro_price_id: "price_xxx".to_string(),
            frontend_url: "http://localhost:5173".to_string(),
        };
        let state = Arc::new(BillingState::new(config));
        let routes = billing_routes(Some(state));

        let res = warp::test::request()
            .method("GET")
            .path("/api/v1/billing/usage")
            .reply(&routes)
            .await;

        assert_eq!(res.status(), 200);
    }

    #[tokio::test]
    async fn test_billing_routes_plan() {
        let config = BillingConfig {
            stripe_secret_key: "sk_test_xxx".to_string(),
            stripe_webhook_secret: "whsec_xxx".to_string(),
            pro_price_id: "price_xxx".to_string(),
            frontend_url: "http://localhost:5173".to_string(),
        };
        let state = Arc::new(BillingState::new(config));
        let routes = billing_routes(Some(state));

        let res = warp::test::request()
            .method("GET")
            .path("/api/v1/billing/plan")
            .reply(&routes)
            .await;

        assert_eq!(res.status(), 200);
        let body: serde_json::Value = serde_json::from_slice(res.body()).unwrap();
        assert_eq!(body["tier"], "free");
    }

    #[tokio::test]
    async fn test_webhook_invalid_signature() {
        let config = BillingConfig {
            stripe_secret_key: "sk_test_xxx".to_string(),
            stripe_webhook_secret: "whsec_real_secret".to_string(),
            pro_price_id: "price_xxx".to_string(),
            frontend_url: "http://localhost:5173".to_string(),
        };
        let state = Arc::new(BillingState::new(config));
        let routes = billing_routes(Some(state));

        let res = warp::test::request()
            .method("POST")
            .path("/api/v1/billing/webhook")
            .header("stripe-signature", "t=123,v1=bad")
            .body("{\"type\":\"test\"}")
            .reply(&routes)
            .await;

        assert_eq!(res.status(), 400);
    }
}
