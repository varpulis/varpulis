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

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Billing configuration loaded from environment variables.
#[derive(Debug, Clone)]
pub struct BillingConfig {
    pub stripe_secret_key: String,
    pub stripe_webhook_secret: String,
    pub pro_price_id: String,
}

impl BillingConfig {
    /// Build config from environment variables.
    /// Returns None if Stripe is not configured.
    pub fn from_env() -> Option<Self> {
        let secret_key = std::env::var("STRIPE_SECRET_KEY").ok()?;
        let webhook_secret =
            std::env::var("STRIPE_WEBHOOK_SECRET").unwrap_or_else(|_| String::new());
        let pro_price_id = std::env::var("STRIPE_PRO_PRICE_ID").unwrap_or_else(|_| String::new());

        Some(Self {
            stripe_secret_key: secret_key,
            stripe_webhook_secret: webhook_secret,
            pro_price_id,
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
}

impl BillingState {
    pub fn new(config: BillingConfig) -> Self {
        Self {
            config,
            usage: RwLock::new(UsageTracker::new()),
            http_client: reqwest::Client::new(),
        }
    }
}

pub type SharedBillingState = Arc<BillingState>;

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

/// GET /api/v1/billing/usage — get usage summary.
async fn handle_usage(
    state: Option<SharedBillingState>,
) -> Result<impl warp::Reply, warp::Rejection> {
    match state {
        Some(s) => {
            let tracker = s.usage.read().await;
            // Return a summary (in production, this would query the DB)
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
    state: Option<SharedBillingState>,
) -> Result<impl warp::Reply, warp::Rejection> {
    match state {
        Some(_) => Ok(warp::reply::with_status(
            warp::reply::json(&serde_json::json!({
                "tier": "free",
                "event_limit": 10_000,
                "display_name": "Free",
            })),
            warp::http::StatusCode::OK,
        )),
        None => Ok(warp::reply::with_status(
            warp::reply::json(&serde_json::json!({ "error": "Billing not configured" })),
            warp::http::StatusCode::SERVICE_UNAVAILABLE,
        )),
    }
}

#[derive(Debug, Deserialize)]
struct CheckoutRequest {
    #[allow(dead_code)]
    success_url: Option<String>,
    #[allow(dead_code)]
    cancel_url: Option<String>,
}

/// POST /api/v1/billing/checkout — create Stripe Checkout session.
async fn handle_checkout(
    _body: CheckoutRequest,
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
            // In production: call Stripe API to create a Checkout Session
            // For now, return a placeholder
            Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({
                    "checkout_url": "https://checkout.stripe.com/placeholder",
                    "session_id": uuid::Uuid::new_v4().to_string(),
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

/// POST /api/v1/billing/portal — create Stripe Customer Portal session.
async fn handle_portal(
    state: Option<SharedBillingState>,
) -> Result<impl warp::Reply, warp::Rejection> {
    match state {
        Some(_) => Ok(warp::reply::with_status(
            warp::reply::json(&serde_json::json!({
                "portal_url": "https://billing.stripe.com/placeholder",
            })),
            warp::http::StatusCode::OK,
        )),
        None => Ok(warp::reply::with_status(
            warp::reply::json(&serde_json::json!({ "error": "Billing not configured" })),
            warp::http::StatusCode::SERVICE_UNAVAILABLE,
        )),
    }
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
    let state4 = state;

    let usage = warp::path!("api" / "v1" / "billing" / "usage")
        .and(warp::get())
        .and(warp::any().map(move || state1.clone()))
        .and_then(handle_usage);

    let plan = warp::path!("api" / "v1" / "billing" / "plan")
        .and(warp::get())
        .and(warp::any().map(move || state2.clone()))
        .and_then(handle_plan);

    let checkout = warp::path!("api" / "v1" / "billing" / "checkout")
        .and(warp::post())
        .and(warp::body::json())
        .and(warp::any().map(move || state3.clone()))
        .and_then(handle_checkout);

    let portal = warp::path!("api" / "v1" / "billing" / "portal")
        .and(warp::post())
        .and(warp::any().map(move || state4.clone()))
        .and_then(handle_portal);

    usage.or(plan).or(checkout).or(portal)
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
}
