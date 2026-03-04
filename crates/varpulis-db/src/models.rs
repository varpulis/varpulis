use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

/// A registered user (GitHub OAuth, local username/password, or both).
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct User {
    pub id: Uuid,
    pub github_id: Option<String>,
    pub email: String,
    pub name: String,
    pub avatar_url: String,
    pub created_at: DateTime<Utc>,
    pub username: Option<String>,
    pub password_hash: Option<String>,
    pub display_name: String,
    pub role: String,
    pub disabled: bool,
    pub updated_at: DateTime<Utc>,
}

/// An organization that owns pipelines and API keys.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Organization {
    pub id: Uuid,
    pub owner_id: Uuid,
    pub name: String,
    /// One of "free", "pro", "business", "enterprise".
    pub tier: String,
    pub stripe_customer_id: Option<String>,
    /// NULL for paid orgs, set to now()+30d for free trial signups.
    pub trial_expires_at: Option<DateTime<Utc>>,
    /// One of "active", "trial", "suspended", "revoked".
    pub status: String,
    pub pipeline_limit: i32,
    pub events_per_second_limit: i32,
    pub monthly_event_limit: i64,
    pub notes: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A hashed API key belonging to an organization.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ApiKey {
    pub id: Uuid,
    pub org_id: Uuid,
    pub key_hash: String,
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
}

/// A deployed VPL pipeline belonging to an organization.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Pipeline {
    pub id: Uuid,
    pub org_id: Uuid,
    pub name: String,
    pub vpl_source: String,
    /// One of "deployed", "stopped", "error".
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Daily aggregated usage metrics for an organization.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct UsageDaily {
    pub org_id: Uuid,
    pub date: NaiveDate,
    pub events_processed: i64,
    pub output_events: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_fields() {
        let user = User {
            id: Uuid::new_v4(),
            github_id: Some("12345".to_string()),
            email: "test@example.com".to_string(),
            name: "Test User".to_string(),
            avatar_url: "https://example.com/avatar.png".to_string(),
            created_at: Utc::now(),
            username: None,
            password_hash: None,
            display_name: String::new(),
            role: "viewer".to_string(),
            disabled: false,
            updated_at: Utc::now(),
        };
        assert_eq!(user.github_id.as_deref(), Some("12345"));
        assert_eq!(user.email, "test@example.com");
    }

    #[test]
    fn test_organization_tiers() {
        for tier in &["free", "pro", "business", "enterprise"] {
            let org = Organization {
                id: Uuid::new_v4(),
                owner_id: Uuid::new_v4(),
                name: "Test Org".to_string(),
                tier: tier.to_string(),
                stripe_customer_id: None,
                trial_expires_at: None,
                status: "active".to_string(),
                pipeline_limit: 5,
                events_per_second_limit: 500,
                monthly_event_limit: 100_000,
                notes: String::new(),
                created_at: Utc::now(),
                updated_at: Utc::now(),
            };
            assert_eq!(&org.tier, tier);
        }
    }

    #[test]
    fn test_pipeline_statuses() {
        for status in &["deployed", "stopped", "error"] {
            let pipeline = Pipeline {
                id: Uuid::new_v4(),
                org_id: Uuid::new_v4(),
                name: "pipeline-1".to_string(),
                vpl_source: "stream S = Events .emit()".to_string(),
                status: status.to_string(),
                created_at: Utc::now(),
                updated_at: Utc::now(),
            };
            assert_eq!(&pipeline.status, status);
        }
    }

    #[test]
    fn test_api_key_last_used_optional() {
        let key = ApiKey {
            id: Uuid::new_v4(),
            org_id: Uuid::new_v4(),
            key_hash: "sha256:abc123".to_string(),
            name: "production".to_string(),
            created_at: Utc::now(),
            last_used_at: None,
        };
        assert!(key.last_used_at.is_none());
    }

    #[test]
    fn test_usage_daily_creation() {
        let usage = UsageDaily {
            org_id: Uuid::new_v4(),
            date: NaiveDate::from_ymd_opt(2026, 2, 24).unwrap(),
            events_processed: 1_000_000,
            output_events: 50_000,
        };
        assert_eq!(usage.events_processed, 1_000_000);
        assert_eq!(usage.output_events, 50_000);
    }

    #[test]
    fn test_model_serialization() {
        let user = User {
            id: Uuid::new_v4(),
            github_id: Some("99999".to_string()),
            email: "ser@test.com".to_string(),
            name: "Serialize Test".to_string(),
            avatar_url: String::new(),
            created_at: Utc::now(),
            username: None,
            password_hash: None,
            display_name: String::new(),
            role: "viewer".to_string(),
            disabled: false,
            updated_at: Utc::now(),
        };
        let json = serde_json::to_string(&user).unwrap();
        let deserialized: User = serde_json::from_str(&json).unwrap();
        assert_eq!(user.id, deserialized.id);
        assert_eq!(user.github_id, deserialized.github_id);
    }
}
