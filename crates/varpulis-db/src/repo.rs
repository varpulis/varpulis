use chrono::{Datelike, NaiveDate, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::models::{ApiKey, GlobalPipelineTemplate, Organization, Pipeline, UsageDaily, User};
use crate::DbError;

// ---------------------------------------------------------------------------
// Users
// ---------------------------------------------------------------------------

const USER_COLUMNS: &str = "id, github_id, email, name, avatar_url, created_at, \
                            username, password_hash, display_name, role, disabled, updated_at, \
                            email_verified, verification_token, verification_expires_at";

/// Create a new user or update an existing one (upsert on `github_id`).
pub async fn create_or_update_user(
    pool: &PgPool,
    github_id: &str,
    email: &str,
    name: &str,
    avatar_url: &str,
) -> Result<User, DbError> {
    let query = format!(
        "INSERT INTO users (github_id, email, name, avatar_url)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (github_id) DO UPDATE
             SET email      = EXCLUDED.email,
                 name       = EXCLUDED.name,
                 avatar_url = EXCLUDED.avatar_url
         RETURNING {USER_COLUMNS}"
    );
    let user = sqlx::query_as::<_, User>(&query)
        .bind(github_id)
        .bind(email)
        .bind(name)
        .bind(avatar_url)
        .fetch_one(pool)
        .await?;

    Ok(user)
}

/// Look up a user by their GitHub ID.
pub async fn get_user_by_github_id(
    pool: &PgPool,
    github_id: &str,
) -> Result<Option<User>, DbError> {
    let query = format!("SELECT {USER_COLUMNS} FROM users WHERE github_id = $1");
    let user = sqlx::query_as::<_, User>(&query)
        .bind(github_id)
        .fetch_optional(pool)
        .await?;

    Ok(user)
}

/// Create a local user with username and password hash.
pub async fn create_local_user(
    pool: &PgPool,
    username: &str,
    password_hash: &str,
    display_name: &str,
    email: &str,
    role: &str,
) -> Result<User, DbError> {
    let query = format!(
        "INSERT INTO users (username, password_hash, display_name, email, role, name, avatar_url, email_verified)
         VALUES ($1, $2, $3, $4, $5, $3, '', true)
         RETURNING {USER_COLUMNS}"
    );
    let user = sqlx::query_as::<_, User>(&query)
        .bind(username)
        .bind(password_hash)
        .bind(display_name)
        .bind(email)
        .bind(role)
        .fetch_one(pool)
        .await?;

    Ok(user)
}

/// Look up a user by username (for login).
pub async fn get_user_by_username(pool: &PgPool, username: &str) -> Result<Option<User>, DbError> {
    let query = format!("SELECT {USER_COLUMNS} FROM users WHERE username = $1");
    let user = sqlx::query_as::<_, User>(&query)
        .bind(username)
        .fetch_optional(pool)
        .await?;

    Ok(user)
}

/// Look up a user by ID.
pub async fn get_user_by_id(pool: &PgPool, id: Uuid) -> Result<Option<User>, DbError> {
    let query = format!("SELECT {USER_COLUMNS} FROM users WHERE id = $1");
    let user = sqlx::query_as::<_, User>(&query)
        .bind(id)
        .fetch_optional(pool)
        .await?;

    Ok(user)
}

/// Update a user's details (admin operation). Only non-None fields are updated.
pub async fn update_user(
    pool: &PgPool,
    id: Uuid,
    display_name: Option<&str>,
    email: Option<&str>,
    role: Option<&str>,
    disabled: Option<bool>,
) -> Result<Option<User>, DbError> {
    // Build dynamic SET clause
    let mut sets = Vec::new();
    let mut bind_idx = 1u32;
    if display_name.is_some() {
        sets.push(format!("display_name = ${bind_idx}"));
        bind_idx += 1;
    }
    if email.is_some() {
        sets.push(format!("email = ${bind_idx}"));
        bind_idx += 1;
    }
    if role.is_some() {
        sets.push(format!("role = ${bind_idx}"));
        bind_idx += 1;
    }
    if disabled.is_some() {
        sets.push(format!("disabled = ${bind_idx}"));
        bind_idx += 1;
    }
    if sets.is_empty() {
        return get_user_by_id(pool, id).await;
    }
    sets.push("updated_at = now()".to_string());
    let query = format!(
        "UPDATE users SET {} WHERE id = ${bind_idx} RETURNING {USER_COLUMNS}",
        sets.join(", "),
    );

    let mut q = sqlx::query_as::<_, User>(&query);
    if let Some(v) = display_name {
        q = q.bind(v);
    }
    if let Some(v) = email {
        q = q.bind(v);
    }
    if let Some(v) = role {
        q = q.bind(v);
    }
    if let Some(v) = disabled {
        q = q.bind(v);
    }
    q = q.bind(id);

    let user = q.fetch_optional(pool).await?;
    Ok(user)
}

/// Update a user's password hash.
pub async fn update_password_hash(pool: &PgPool, id: Uuid, new_hash: &str) -> Result<(), DbError> {
    sqlx::query("UPDATE users SET password_hash = $1, updated_at = now() WHERE id = $2")
        .bind(new_hash)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

/// List all users (for admin panel).
pub async fn list_users(pool: &PgPool) -> Result<Vec<User>, DbError> {
    let query = format!("SELECT {USER_COLUMNS} FROM users ORDER BY created_at");
    let users = sqlx::query_as::<_, User>(&query).fetch_all(pool).await?;
    Ok(users)
}

/// Delete a user by ID.
pub async fn delete_user(pool: &PgPool, id: Uuid) -> Result<(), DbError> {
    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Create a local user with email verification required (self-service signup).
#[allow(clippy::too_many_arguments)]
pub async fn create_local_user_with_verification(
    pool: &PgPool,
    username: &str,
    password_hash: &str,
    display_name: &str,
    email: &str,
    role: &str,
    token: &str,
    expires_at: chrono::DateTime<Utc>,
) -> Result<User, DbError> {
    let query = format!(
        "INSERT INTO users (username, password_hash, display_name, email, role, name, avatar_url, \
         email_verified, verification_token, verification_expires_at)
         VALUES ($1, $2, $3, $4, $5, $3, '', false, $6, $7)
         RETURNING {USER_COLUMNS}"
    );
    let user = sqlx::query_as::<_, User>(&query)
        .bind(username)
        .bind(password_hash)
        .bind(display_name)
        .bind(email)
        .bind(role)
        .bind(token)
        .bind(expires_at)
        .fetch_one(pool)
        .await?;

    Ok(user)
}

/// Look up a user by their verification token.
pub async fn get_user_by_verification_token(
    pool: &PgPool,
    token: &str,
) -> Result<Option<User>, DbError> {
    let query = format!("SELECT {USER_COLUMNS} FROM users WHERE verification_token = $1");
    let user = sqlx::query_as::<_, User>(&query)
        .bind(token)
        .fetch_optional(pool)
        .await?;

    Ok(user)
}

/// Mark a user's email as verified and clear the verification token.
pub async fn verify_user_email(pool: &PgPool, user_id: Uuid) -> Result<(), DbError> {
    sqlx::query(
        "UPDATE users SET email_verified = true, verification_token = NULL, \
         verification_expires_at = NULL, updated_at = now() WHERE id = $1",
    )
    .bind(user_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Look up a user by email address.
pub async fn get_user_by_email(pool: &PgPool, email: &str) -> Result<Option<User>, DbError> {
    let query = format!("SELECT {USER_COLUMNS} FROM users WHERE email = $1");
    let user = sqlx::query_as::<_, User>(&query)
        .bind(email)
        .fetch_optional(pool)
        .await?;

    Ok(user)
}

/// Check if any user with admin role exists.
pub async fn has_admin_user(pool: &PgPool) -> Result<bool, DbError> {
    let row: Option<(i64,)> =
        sqlx::query_as("SELECT COUNT(*) FROM users WHERE role = 'admin' AND username IS NOT NULL")
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|r| r.0).unwrap_or(0) > 0)
}

// ---------------------------------------------------------------------------
// Organizations
// ---------------------------------------------------------------------------

const ORG_COLUMNS: &str = "id, owner_id, name, tier, stripe_customer_id, trial_expires_at, status, pipeline_limit, events_per_second_limit, monthly_event_limit, notes, created_at, updated_at, slug";

/// Create a new organization owned by the given user.
/// Also inserts an `org_members` row with role `owner`.
pub async fn create_organization(
    pool: &PgPool,
    owner_id: Uuid,
    name: &str,
) -> Result<Organization, DbError> {
    let query = format!(
        "INSERT INTO organizations (owner_id, name) VALUES ($1, $2) RETURNING {ORG_COLUMNS}"
    );
    let org = sqlx::query_as::<_, Organization>(&query)
        .bind(owner_id)
        .bind(name)
        .fetch_one(pool)
        .await?;

    // Ensure the owner is also a member
    let _ = add_org_member(pool, org.id, owner_id, "owner").await;

    Ok(org)
}

/// Create a new organization with trial status (30-day free trial).
/// Also inserts an `org_members` row with role `owner`.
pub async fn create_trial_organization(
    pool: &PgPool,
    owner_id: Uuid,
    name: &str,
) -> Result<Organization, DbError> {
    let query = format!(
        "INSERT INTO organizations (owner_id, name, status, trial_expires_at) \
         VALUES ($1, $2, 'trial', now() + interval '30 days') \
         RETURNING {ORG_COLUMNS}"
    );
    let org = sqlx::query_as::<_, Organization>(&query)
        .bind(owner_id)
        .bind(name)
        .fetch_one(pool)
        .await?;

    // Ensure the owner is also a member
    let _ = add_org_member(pool, org.id, owner_id, "owner").await;

    Ok(org)
}

/// Get an organization by its ID.
pub async fn get_organization(pool: &PgPool, id: Uuid) -> Result<Option<Organization>, DbError> {
    let query = format!("SELECT {ORG_COLUMNS} FROM organizations WHERE id = $1");
    let org = sqlx::query_as::<_, Organization>(&query)
        .bind(id)
        .fetch_optional(pool)
        .await?;

    Ok(org)
}

/// List all organizations a user is a member of (via `org_members` JOIN).
/// Falls back to `owner_id` match for backward compatibility.
pub async fn get_user_organizations(
    pool: &PgPool,
    user_id: Uuid,
) -> Result<Vec<Organization>, DbError> {
    let query = format!(
        "SELECT {ORG_COLUMNS} FROM organizations \
         WHERE id IN (\
             SELECT org_id FROM org_members WHERE user_id = $1 AND status = 'active' \
             UNION \
             SELECT id FROM organizations WHERE owner_id = $1\
         ) ORDER BY created_at"
    );
    let orgs = sqlx::query_as::<_, Organization>(&query)
        .bind(user_id)
        .fetch_all(pool)
        .await?;

    Ok(orgs)
}

/// Update the Stripe customer ID for an organization.
pub async fn update_org_stripe_customer(
    pool: &PgPool,
    org_id: Uuid,
    customer_id: &str,
) -> Result<(), DbError> {
    sqlx::query("UPDATE organizations SET stripe_customer_id = $1 WHERE id = $2")
        .bind(customer_id)
        .bind(org_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Update the tier of an organization.
pub async fn update_org_tier(pool: &PgPool, org_id: Uuid, tier: &str) -> Result<(), DbError> {
    sqlx::query("UPDATE organizations SET tier = $1, updated_at = now() WHERE id = $2")
        .bind(tier)
        .bind(org_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Get an organization by its Stripe customer ID.
pub async fn get_org_by_stripe_customer(
    pool: &PgPool,
    customer_id: &str,
) -> Result<Option<Organization>, DbError> {
    let query = format!("SELECT {ORG_COLUMNS} FROM organizations WHERE stripe_customer_id = $1");
    let org = sqlx::query_as::<_, Organization>(&query)
        .bind(customer_id)
        .fetch_optional(pool)
        .await?;
    Ok(org)
}

// ---------------------------------------------------------------------------
// Organization Members
// ---------------------------------------------------------------------------

use crate::models::OrgMember;

/// Add a user as a member of an organization.
pub async fn add_org_member(
    pool: &PgPool,
    org_id: Uuid,
    user_id: Uuid,
    role: &str,
) -> Result<OrgMember, DbError> {
    let member = sqlx::query_as::<_, OrgMember>(
        "INSERT INTO org_members (org_id, user_id, role, status, accepted_at) \
         VALUES ($1, $2, $3, 'active', now()) \
         ON CONFLICT (org_id, user_id) DO UPDATE SET role = EXCLUDED.role \
         RETURNING id, org_id, user_id, role, status, invited_at, accepted_at",
    )
    .bind(org_id)
    .bind(user_id)
    .bind(role)
    .fetch_one(pool)
    .await?;

    Ok(member)
}

/// Get a user's membership in a specific organization.
pub async fn get_user_org_membership(
    pool: &PgPool,
    user_id: Uuid,
    org_id: Uuid,
) -> Result<Option<OrgMember>, DbError> {
    let member = sqlx::query_as::<_, OrgMember>(
        "SELECT id, org_id, user_id, role, status, invited_at, accepted_at \
         FROM org_members WHERE user_id = $1 AND org_id = $2",
    )
    .bind(user_id)
    .bind(org_id)
    .fetch_optional(pool)
    .await?;

    Ok(member)
}

/// List all memberships for a user (with the associated organization).
pub async fn get_user_memberships(
    pool: &PgPool,
    user_id: Uuid,
) -> Result<Vec<(OrgMember, Organization)>, DbError> {
    let members = sqlx::query_as::<_, OrgMember>(
        "SELECT id, org_id, user_id, role, status, invited_at, accepted_at \
         FROM org_members WHERE user_id = $1 AND status = 'active' ORDER BY invited_at",
    )
    .bind(user_id)
    .fetch_all(pool)
    .await?;

    let mut results = Vec::with_capacity(members.len());
    for member in members {
        if let Ok(Some(org)) = get_organization(pool, member.org_id).await {
            results.push((member, org));
        }
    }

    Ok(results)
}

/// List all members of an organization (with associated user info).
pub async fn list_org_members(
    pool: &PgPool,
    org_id: Uuid,
) -> Result<Vec<(OrgMember, User)>, DbError> {
    let members = sqlx::query_as::<_, OrgMember>(
        "SELECT id, org_id, user_id, role, status, invited_at, accepted_at \
         FROM org_members WHERE org_id = $1 ORDER BY invited_at",
    )
    .bind(org_id)
    .fetch_all(pool)
    .await?;

    let mut results = Vec::with_capacity(members.len());
    for member in members {
        if let Ok(Some(user)) = get_user_by_id(pool, member.user_id).await {
            results.push((member, user));
        }
    }

    Ok(results)
}

/// Remove a user from an organization.
pub async fn remove_org_member(pool: &PgPool, org_id: Uuid, user_id: Uuid) -> Result<(), DbError> {
    sqlx::query("DELETE FROM org_members WHERE org_id = $1 AND user_id = $2")
        .bind(org_id)
        .bind(user_id)
        .execute(pool)
        .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Admin organization management
// ---------------------------------------------------------------------------

/// List all organizations (admin query).
pub async fn list_all_organizations(pool: &PgPool) -> Result<Vec<Organization>, DbError> {
    let query = format!("SELECT {ORG_COLUMNS} FROM organizations ORDER BY created_at");
    let orgs = sqlx::query_as::<_, Organization>(&query)
        .fetch_all(pool)
        .await?;
    Ok(orgs)
}

/// Update the status of an organization (active, trial, suspended, revoked).
pub async fn update_org_status(pool: &PgPool, org_id: Uuid, status: &str) -> Result<(), DbError> {
    sqlx::query("UPDATE organizations SET status = $1, updated_at = now() WHERE id = $2")
        .bind(status)
        .bind(org_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Update per-tenant resource limits.
pub async fn update_org_limits(
    pool: &PgPool,
    org_id: Uuid,
    pipeline_limit: i32,
    eps_limit: i32,
    monthly_limit: i64,
) -> Result<(), DbError> {
    sqlx::query(
        "UPDATE organizations SET pipeline_limit = $1, events_per_second_limit = $2, \
         monthly_event_limit = $3, updated_at = now() WHERE id = $4",
    )
    .bind(pipeline_limit)
    .bind(eps_limit)
    .bind(monthly_limit)
    .bind(org_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Extend a trial expiration date.
pub async fn extend_trial(
    pool: &PgPool,
    org_id: Uuid,
    new_expiry: chrono::DateTime<Utc>,
) -> Result<(), DbError> {
    sqlx::query(
        "UPDATE organizations SET trial_expires_at = $1, status = 'trial', updated_at = now() WHERE id = $2",
    )
    .bind(new_expiry)
    .bind(org_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Get all trial orgs whose trial has expired (for background expiry task).
pub async fn get_expiring_trials(
    pool: &PgPool,
    before: chrono::DateTime<Utc>,
) -> Result<Vec<Organization>, DbError> {
    let query = format!(
        "SELECT {ORG_COLUMNS} FROM organizations \
         WHERE status = 'trial' AND trial_expires_at IS NOT NULL AND trial_expires_at < $1"
    );
    let orgs = sqlx::query_as::<_, Organization>(&query)
        .bind(before)
        .fetch_all(pool)
        .await?;
    Ok(orgs)
}

/// Aggregated usage summary for an organization (current month).
pub async fn get_org_usage_summary(pool: &PgPool, org_id: Uuid) -> Result<i64, DbError> {
    let today = Utc::now().date_naive();
    let start = chrono::NaiveDate::from_ymd_opt(today.year(), today.month(), 1).unwrap_or(today);
    let row: Option<(i64,)> = sqlx::query_as(
        "SELECT COALESCE(SUM(events_processed), 0) FROM usage_daily \
         WHERE org_id = $1 AND date >= $2 AND date <= $3",
    )
    .bind(org_id)
    .bind(start)
    .bind(today)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.0).unwrap_or(0))
}

// ---------------------------------------------------------------------------
// API Keys
// ---------------------------------------------------------------------------

const API_KEY_COLUMNS: &str = "id, org_id, key_hash, name, created_at, last_used_at, \
                               key_prefix, scopes, expires_at, revoked_at, created_by";

/// Create a new API key for an organization.
pub async fn create_api_key(
    pool: &PgPool,
    org_id: Uuid,
    key_hash: &str,
    name: &str,
) -> Result<ApiKey, DbError> {
    let query = format!(
        "INSERT INTO api_keys (org_id, key_hash, name) VALUES ($1, $2, $3) RETURNING {API_KEY_COLUMNS}"
    );
    let key = sqlx::query_as::<_, ApiKey>(&query)
        .bind(org_id)
        .bind(key_hash)
        .bind(name)
        .fetch_one(pool)
        .await?;

    Ok(key)
}

/// Create an API key with extended attributes (prefix, scopes, expiry, creator).
#[allow(clippy::too_many_arguments)]
pub async fn create_api_key_extended(
    pool: &PgPool,
    org_id: Uuid,
    key_hash: &str,
    name: &str,
    key_prefix: &str,
    scopes: &str,
    expires_at: Option<chrono::DateTime<Utc>>,
    created_by: Option<Uuid>,
) -> Result<ApiKey, DbError> {
    let query = format!(
        "INSERT INTO api_keys (org_id, key_hash, name, key_prefix, scopes, expires_at, created_by) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING {API_KEY_COLUMNS}"
    );
    let key = sqlx::query_as::<_, ApiKey>(&query)
        .bind(org_id)
        .bind(key_hash)
        .bind(name)
        .bind(key_prefix)
        .bind(scopes)
        .bind(expires_at)
        .bind(created_by)
        .fetch_one(pool)
        .await?;

    Ok(key)
}

/// Look up an active API key by its hash (excludes revoked and expired keys).
pub async fn get_api_key_by_hash(pool: &PgPool, hash: &str) -> Result<Option<ApiKey>, DbError> {
    let query = format!(
        "SELECT {API_KEY_COLUMNS} FROM api_keys \
         WHERE key_hash = $1 AND revoked_at IS NULL \
         AND (expires_at IS NULL OR expires_at > now())"
    );
    let key = sqlx::query_as::<_, ApiKey>(&query)
        .bind(hash)
        .fetch_optional(pool)
        .await?;

    Ok(key)
}

/// List all active API keys for an organization (excludes revoked).
pub async fn list_api_keys(pool: &PgPool, org_id: Uuid) -> Result<Vec<ApiKey>, DbError> {
    let query = format!(
        "SELECT {API_KEY_COLUMNS} FROM api_keys \
         WHERE org_id = $1 AND revoked_at IS NULL ORDER BY created_at"
    );
    let keys = sqlx::query_as::<_, ApiKey>(&query)
        .bind(org_id)
        .fetch_all(pool)
        .await?;

    Ok(keys)
}

/// Delete an API key by its ID, scoped to an organization.
pub async fn delete_api_key(pool: &PgPool, id: Uuid, org_id: Uuid) -> Result<(), DbError> {
    let result = sqlx::query("DELETE FROM api_keys WHERE id = $1 AND org_id = $2")
        .bind(id)
        .bind(org_id)
        .execute(pool)
        .await?;

    if result.rows_affected() == 0 {
        return Err(sqlx::Error::RowNotFound.into());
    }

    Ok(())
}

/// Soft-delete (revoke) an API key by setting `revoked_at`, scoped to an org.
pub async fn revoke_api_key(pool: &PgPool, id: Uuid, org_id: Uuid) -> Result<(), DbError> {
    let result = sqlx::query(
        "UPDATE api_keys SET revoked_at = now() WHERE id = $1 AND org_id = $2 AND revoked_at IS NULL",
    )
    .bind(id)
    .bind(org_id)
    .execute(pool)
    .await?;

    if result.rows_affected() == 0 {
        return Err(sqlx::Error::RowNotFound.into());
    }

    Ok(())
}

/// Update the `last_used_at` timestamp of an API key to now.
pub async fn touch_api_key(pool: &PgPool, id: Uuid) -> Result<(), DbError> {
    sqlx::query("UPDATE api_keys SET last_used_at = $1 WHERE id = $2")
        .bind(Utc::now())
        .bind(id)
        .execute(pool)
        .await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Pipelines
// ---------------------------------------------------------------------------

const PIPELINE_COLUMNS: &str =
    "id, org_id, name, vpl_source, status, created_at, updated_at, global_template_id";

/// Create a new pipeline for an organization.
pub async fn create_pipeline(
    pool: &PgPool,
    org_id: Uuid,
    name: &str,
    vpl_source: &str,
) -> Result<Pipeline, DbError> {
    let query = format!(
        "INSERT INTO pipelines (org_id, name, vpl_source) \
         VALUES ($1, $2, $3) RETURNING {PIPELINE_COLUMNS}"
    );
    let pipeline = sqlx::query_as::<_, Pipeline>(&query)
        .bind(org_id)
        .bind(name)
        .bind(vpl_source)
        .fetch_one(pool)
        .await?;

    Ok(pipeline)
}

/// Get a pipeline by its ID, scoped to an organization.
pub async fn get_pipeline(
    pool: &PgPool,
    id: Uuid,
    org_id: Uuid,
) -> Result<Option<Pipeline>, DbError> {
    let query = format!("SELECT {PIPELINE_COLUMNS} FROM pipelines WHERE id = $1 AND org_id = $2");
    let pipeline = sqlx::query_as::<_, Pipeline>(&query)
        .bind(id)
        .bind(org_id)
        .fetch_optional(pool)
        .await?;

    Ok(pipeline)
}

/// List all pipelines belonging to an organization.
pub async fn list_pipelines(pool: &PgPool, org_id: Uuid) -> Result<Vec<Pipeline>, DbError> {
    let query =
        format!("SELECT {PIPELINE_COLUMNS} FROM pipelines WHERE org_id = $1 ORDER BY created_at");
    let pipelines = sqlx::query_as::<_, Pipeline>(&query)
        .bind(org_id)
        .fetch_all(pool)
        .await?;

    Ok(pipelines)
}

/// Update the status of a pipeline, scoped to an organization.
pub async fn update_pipeline_status(
    pool: &PgPool,
    id: Uuid,
    org_id: Uuid,
    status: &str,
) -> Result<(), DbError> {
    sqlx::query("UPDATE pipelines SET status = $1, updated_at = $2 WHERE id = $3 AND org_id = $4")
        .bind(status)
        .bind(Utc::now())
        .bind(id)
        .bind(org_id)
        .execute(pool)
        .await?;

    Ok(())
}

/// Update the VPL source of a pipeline, scoped to an organization.
pub async fn update_pipeline_source(
    pool: &PgPool,
    id: Uuid,
    org_id: Uuid,
    vpl_source: &str,
) -> Result<(), DbError> {
    sqlx::query(
        "UPDATE pipelines SET vpl_source = $1, updated_at = $2 WHERE id = $3 AND org_id = $4",
    )
    .bind(vpl_source)
    .bind(Utc::now())
    .bind(id)
    .bind(org_id)
    .execute(pool)
    .await?;

    Ok(())
}

/// Delete a pipeline by its ID, scoped to an organization.
pub async fn delete_pipeline(pool: &PgPool, id: Uuid, org_id: Uuid) -> Result<(), DbError> {
    let result = sqlx::query("DELETE FROM pipelines WHERE id = $1 AND org_id = $2")
        .bind(id)
        .bind(org_id)
        .execute(pool)
        .await?;

    if result.rows_affected() == 0 {
        return Err(sqlx::Error::RowNotFound.into());
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Global Pipeline Templates
// ---------------------------------------------------------------------------

const GLOBAL_TEMPLATE_COLUMNS: &str =
    "id, name, vpl_source, status, deployed_by, created_at, updated_at";

/// Create a new global pipeline template.
pub async fn create_global_template(
    pool: &PgPool,
    name: &str,
    vpl_source: &str,
    deployed_by: Option<Uuid>,
) -> Result<GlobalPipelineTemplate, DbError> {
    let query = format!(
        "INSERT INTO global_pipeline_templates (name, vpl_source, deployed_by) \
         VALUES ($1, $2, $3) RETURNING {GLOBAL_TEMPLATE_COLUMNS}"
    );
    let template = sqlx::query_as::<_, GlobalPipelineTemplate>(&query)
        .bind(name)
        .bind(vpl_source)
        .bind(deployed_by)
        .fetch_one(pool)
        .await?;

    Ok(template)
}

/// List all global pipeline templates.
pub async fn list_global_templates(pool: &PgPool) -> Result<Vec<GlobalPipelineTemplate>, DbError> {
    let query = format!(
        "SELECT {GLOBAL_TEMPLATE_COLUMNS} FROM global_pipeline_templates ORDER BY created_at"
    );
    let templates = sqlx::query_as::<_, GlobalPipelineTemplate>(&query)
        .fetch_all(pool)
        .await?;

    Ok(templates)
}

/// Get a single global template by ID.
pub async fn get_global_template(
    pool: &PgPool,
    id: Uuid,
) -> Result<Option<GlobalPipelineTemplate>, DbError> {
    let query =
        format!("SELECT {GLOBAL_TEMPLATE_COLUMNS} FROM global_pipeline_templates WHERE id = $1");
    let template = sqlx::query_as::<_, GlobalPipelineTemplate>(&query)
        .bind(id)
        .fetch_optional(pool)
        .await?;

    Ok(template)
}

/// Update a global template's VPL source.
pub async fn update_global_template_source(
    pool: &PgPool,
    id: Uuid,
    vpl_source: &str,
) -> Result<(), DbError> {
    sqlx::query(
        "UPDATE global_pipeline_templates SET vpl_source = $1, updated_at = now() WHERE id = $2",
    )
    .bind(vpl_source)
    .bind(id)
    .execute(pool)
    .await?;

    Ok(())
}

/// Delete a global template (CASCADE removes all tenant copies).
pub async fn delete_global_template(pool: &PgPool, id: Uuid) -> Result<(), DbError> {
    sqlx::query("DELETE FROM global_pipeline_templates WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;

    Ok(())
}

/// List only deployed global templates.
pub async fn list_deployed_global_templates(
    pool: &PgPool,
) -> Result<Vec<GlobalPipelineTemplate>, DbError> {
    let query = format!(
        "SELECT {GLOBAL_TEMPLATE_COLUMNS} FROM global_pipeline_templates \
         WHERE status = 'deployed' ORDER BY created_at"
    );
    let templates = sqlx::query_as::<_, GlobalPipelineTemplate>(&query)
        .fetch_all(pool)
        .await?;

    Ok(templates)
}

/// Create a pipeline copy linked to a global template.
pub async fn create_global_pipeline_copy(
    pool: &PgPool,
    org_id: Uuid,
    template_id: Uuid,
    name: &str,
    vpl_source: &str,
) -> Result<Pipeline, DbError> {
    let query = format!(
        "INSERT INTO pipelines (org_id, name, vpl_source, global_template_id) \
         VALUES ($1, $2, $3, $4) RETURNING {PIPELINE_COLUMNS}"
    );
    let pipeline = sqlx::query_as::<_, Pipeline>(&query)
        .bind(org_id)
        .bind(name)
        .bind(vpl_source)
        .bind(template_id)
        .fetch_one(pool)
        .await?;

    Ok(pipeline)
}

/// List all pipeline copies of a given global template.
pub async fn list_global_template_copies(
    pool: &PgPool,
    template_id: Uuid,
) -> Result<Vec<Pipeline>, DbError> {
    let query = format!(
        "SELECT {PIPELINE_COLUMNS} FROM pipelines WHERE global_template_id = $1 ORDER BY created_at"
    );
    let pipelines = sqlx::query_as::<_, Pipeline>(&query)
        .bind(template_id)
        .fetch_all(pool)
        .await?;

    Ok(pipelines)
}

/// Update all pipeline copies of a global template with new VPL source.
pub async fn update_global_template_copies_source(
    pool: &PgPool,
    template_id: Uuid,
    vpl_source: &str,
) -> Result<u64, DbError> {
    let result = sqlx::query(
        "UPDATE pipelines SET vpl_source = $1, updated_at = now() WHERE global_template_id = $2",
    )
    .bind(vpl_source)
    .bind(template_id)
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

// ---------------------------------------------------------------------------
// Usage
// ---------------------------------------------------------------------------

/// Record (upsert) daily usage metrics for an organization.
pub async fn record_usage(
    pool: &PgPool,
    org_id: Uuid,
    date: NaiveDate,
    events_processed: i64,
    output_events: i64,
) -> Result<(), DbError> {
    sqlx::query(
        r"
        INSERT INTO usage_daily (org_id, date, events_processed, output_events)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (org_id, date) DO UPDATE
            SET events_processed = usage_daily.events_processed + EXCLUDED.events_processed,
                output_events    = usage_daily.output_events    + EXCLUDED.output_events
        ",
    )
    .bind(org_id)
    .bind(date)
    .bind(events_processed)
    .bind(output_events)
    .execute(pool)
    .await?;

    Ok(())
}

/// Query daily usage metrics for an organization over a date range (inclusive).
pub async fn get_usage(
    pool: &PgPool,
    org_id: Uuid,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<Vec<UsageDaily>, DbError> {
    let rows = sqlx::query_as::<_, UsageDaily>(
        r"
        SELECT org_id, date, events_processed, output_events
        FROM usage_daily
        WHERE org_id = $1 AND date >= $2 AND date <= $3
        ORDER BY date
        ",
    )
    .bind(org_id)
    .bind(start_date)
    .bind(end_date)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}
