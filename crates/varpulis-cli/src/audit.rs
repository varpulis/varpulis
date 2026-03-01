//! Structured audit logging for Varpulis.
//!
//! Records security-relevant events as JSON-lines to a dedicated audit log file
//! and exposes a read-only API endpoint for Admin users.

use axum::extract::{Json, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, RwLock};

// ---------------------------------------------------------------------------
// Audit entry
// ---------------------------------------------------------------------------

/// A single audit log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    pub timestamp: DateTime<Utc>,
    pub actor: String,
    pub action: AuditAction,
    pub target: String,
    pub outcome: AuditOutcome,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,
}

/// The kind of action being audited.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditAction {
    Login,
    Logout,
    TokenRefresh,
    PipelineDeploy,
    PipelineDelete,
    PipelineUpdate,
    StreamStart,
    StreamStop,
    ApiKeyCreate,
    ApiKeyDelete,
    TierChange,
    CheckoutStarted,
    WebhookReceived,
    SettingsChange,
    AdminAccess,
    UserCreate,
    UserUpdate,
    UserDelete,
    UserDisable,
    PasswordChange,
    SessionRenew,
    MaxSessionsExceeded,
}

/// Whether the action succeeded or failed.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditOutcome {
    Success,
    Failure,
    Denied,
}

// ---------------------------------------------------------------------------
// Audit logger
// ---------------------------------------------------------------------------

/// Thread-safe audit logger that writes JSON-lines to a file.
pub struct AuditLogger {
    writer: Mutex<tokio::io::BufWriter<tokio::fs::File>>,
    recent: RwLock<Vec<AuditEntry>>,
    max_recent: usize,
}

pub type SharedAuditLogger = Arc<AuditLogger>;

impl AuditLogger {
    /// Open (or create) the audit log file and return a shared logger.
    pub async fn open(path: PathBuf) -> Result<SharedAuditLogger, String> {
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|e| format!("Failed to open audit log {}: {}", path.display(), e))?;

        tracing::info!("Audit log: {}", path.display());

        Ok(Arc::new(Self {
            writer: Mutex::new(tokio::io::BufWriter::new(file)),
            recent: RwLock::new(Vec::new()),
            max_recent: 1000,
        }))
    }

    /// Record an audit event.
    pub async fn log(&self, entry: AuditEntry) {
        // Write to file
        if let Ok(mut line) = serde_json::to_string(&entry) {
            line.push('\n');
            let mut writer = self.writer.lock().await;
            if let Err(e) = writer.write_all(line.as_bytes()).await {
                tracing::error!("Audit write failed: {}", e);
            }
            let _ = writer.flush().await;
        }

        // Keep in recent buffer
        let mut recent = self.recent.write().await;
        if recent.len() >= self.max_recent {
            recent.remove(0);
        }
        recent.push(entry);
    }

    /// Get recent audit entries (most recent first).
    pub async fn recent(&self, limit: usize) -> Vec<AuditEntry> {
        let recent = self.recent.read().await;
        recent.iter().rev().take(limit).cloned().collect()
    }
}

// ---------------------------------------------------------------------------
// Convenience constructors
// ---------------------------------------------------------------------------

impl AuditEntry {
    pub fn new(actor: impl Into<String>, action: AuditAction, target: impl Into<String>) -> Self {
        Self {
            timestamp: Utc::now(),
            actor: actor.into(),
            action,
            target: target.into(),
            outcome: AuditOutcome::Success,
            detail: None,
            ip: None,
        }
    }

    pub const fn with_outcome(mut self, outcome: AuditOutcome) -> Self {
        self.outcome = outcome;
        self
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    pub fn with_ip(mut self, ip: impl Into<String>) -> Self {
        self.ip = Some(ip.into());
        self
    }
}

// ---------------------------------------------------------------------------
// API routes
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct AuditQuery {
    #[serde(default = "default_limit")]
    limit: usize,
    #[serde(default)]
    action: Option<String>,
    #[serde(default)]
    actor: Option<String>,
}

const fn default_limit() -> usize {
    100
}

/// GET /api/v1/audit — returns recent audit entries (Admin only).
async fn handle_audit_list(
    Query(query): Query<AuditQuery>,
    State(logger): State<Option<SharedAuditLogger>>,
) -> impl IntoResponse {
    let logger = match logger {
        Some(l) => l,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "Audit logging not enabled"})),
            )
                .into_response();
        }
    };

    let limit = query.limit.min(1000);
    let mut entries = logger.recent(limit).await;

    // Optional filters
    if let Some(ref action_filter) = query.action {
        entries.retain(|e| {
            let action_str = serde_json::to_string(&e.action).unwrap_or_default();
            action_str.contains(action_filter)
        });
    }
    if let Some(ref actor_filter) = query.actor {
        entries.retain(|e| e.actor.contains(actor_filter.as_str()));
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "entries": entries,
            "count": entries.len(),
        })),
    )
        .into_response()
}

/// Build audit log routes.
pub fn audit_routes(logger: Option<SharedAuditLogger>) -> Router {
    Router::new()
        .route("/api/v1/audit", get(handle_audit_list))
        .with_state(logger)
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

    #[test]
    fn test_audit_entry_serialization() {
        let entry = AuditEntry::new("user@example.com", AuditAction::Login, "/auth/github")
            .with_detail("GitHub OAuth")
            .with_ip("10.0.0.1");

        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("\"action\":\"login\""));
        assert!(json.contains("\"outcome\":\"success\""));
        assert!(json.contains("\"ip\":\"10.0.0.1\""));

        // Roundtrip
        let parsed: AuditEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.actor, "user@example.com");
    }

    #[test]
    fn test_audit_entry_failure() {
        let entry = AuditEntry::new("anonymous", AuditAction::Login, "/auth/github")
            .with_outcome(AuditOutcome::Failure)
            .with_detail("Invalid OAuth code");

        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("\"outcome\":\"failure\""));
    }

    #[tokio::test]
    async fn test_audit_logger_recent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        let logger = AuditLogger::open(path).await.unwrap();

        for i in 0..5 {
            logger
                .log(AuditEntry::new(
                    format!("user_{i}"),
                    AuditAction::Login,
                    "/auth",
                ))
                .await;
        }

        let recent = logger.recent(3).await;
        assert_eq!(recent.len(), 3);
        // Most recent first
        assert_eq!(recent[0].actor, "user_4");
        assert_eq!(recent[2].actor, "user_2");
    }

    #[tokio::test]
    async fn test_audit_routes_not_configured() {
        let app = audit_routes(None);

        let req: Request<Body> = Request::builder()
            .method("GET")
            .uri("/api/v1/audit")
            .body(Body::empty())
            .unwrap();
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), 503);
    }

    #[tokio::test]
    async fn test_audit_routes_returns_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        let logger = AuditLogger::open(path).await.unwrap();

        logger
            .log(AuditEntry::new("admin", AuditAction::Login, "/auth"))
            .await;

        let app = audit_routes(Some(logger));
        let req: Request<Body> = Request::builder()
            .method("GET")
            .uri("/api/v1/audit?limit=10")
            .body(Body::empty())
            .unwrap();
        let res = app.oneshot(req).await.unwrap();

        assert_eq!(res.status(), 200);
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["count"], 1);
    }
}
