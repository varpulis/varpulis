use chrono::{NaiveDate, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::models::{ApiKey, Organization, Pipeline, UsageDaily, User};
use crate::DbError;

// ---------------------------------------------------------------------------
// Users
// ---------------------------------------------------------------------------

/// Create a new user or update an existing one (upsert on `github_id`).
pub async fn create_or_update_user(
    pool: &PgPool,
    github_id: &str,
    email: &str,
    name: &str,
    avatar_url: &str,
) -> Result<User, DbError> {
    let user = sqlx::query_as::<_, User>(
        r"
        INSERT INTO users (github_id, email, name, avatar_url)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (github_id) DO UPDATE
            SET email      = EXCLUDED.email,
                name       = EXCLUDED.name,
                avatar_url = EXCLUDED.avatar_url
        RETURNING id, github_id, email, name, avatar_url, created_at
        ",
    )
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
    let user = sqlx::query_as::<_, User>(
        "SELECT id, github_id, email, name, avatar_url, created_at FROM users WHERE github_id = $1",
    )
    .bind(github_id)
    .fetch_optional(pool)
    .await?;

    Ok(user)
}

// ---------------------------------------------------------------------------
// Organizations
// ---------------------------------------------------------------------------

/// Create a new organization owned by the given user.
pub async fn create_organization(
    pool: &PgPool,
    owner_id: Uuid,
    name: &str,
) -> Result<Organization, DbError> {
    let org = sqlx::query_as::<_, Organization>(
        r"
        INSERT INTO organizations (owner_id, name)
        VALUES ($1, $2)
        RETURNING id, owner_id, name, tier, stripe_customer_id, created_at
        ",
    )
    .bind(owner_id)
    .bind(name)
    .fetch_one(pool)
    .await?;

    Ok(org)
}

/// Get an organization by its ID.
pub async fn get_organization(pool: &PgPool, id: Uuid) -> Result<Option<Organization>, DbError> {
    let org = sqlx::query_as::<_, Organization>(
        "SELECT id, owner_id, name, tier, stripe_customer_id, created_at FROM organizations WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    Ok(org)
}

/// List all organizations that a user owns.
pub async fn get_user_organizations(
    pool: &PgPool,
    user_id: Uuid,
) -> Result<Vec<Organization>, DbError> {
    let orgs = sqlx::query_as::<_, Organization>(
        "SELECT id, owner_id, name, tier, stripe_customer_id, created_at FROM organizations WHERE owner_id = $1 ORDER BY created_at",
    )
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
    sqlx::query("UPDATE organizations SET tier = $1 WHERE id = $2")
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
    let org = sqlx::query_as::<_, Organization>(
        "SELECT id, owner_id, name, tier, stripe_customer_id, created_at FROM organizations WHERE stripe_customer_id = $1",
    )
    .bind(customer_id)
    .fetch_optional(pool)
    .await?;
    Ok(org)
}

// ---------------------------------------------------------------------------
// API Keys
// ---------------------------------------------------------------------------

/// Create a new API key for an organization.
pub async fn create_api_key(
    pool: &PgPool,
    org_id: Uuid,
    key_hash: &str,
    name: &str,
) -> Result<ApiKey, DbError> {
    let key = sqlx::query_as::<_, ApiKey>(
        r"
        INSERT INTO api_keys (org_id, key_hash, name)
        VALUES ($1, $2, $3)
        RETURNING id, org_id, key_hash, name, created_at, last_used_at
        ",
    )
    .bind(org_id)
    .bind(key_hash)
    .bind(name)
    .fetch_one(pool)
    .await?;

    Ok(key)
}

/// Look up an API key by its hash.
pub async fn get_api_key_by_hash(pool: &PgPool, hash: &str) -> Result<Option<ApiKey>, DbError> {
    let key = sqlx::query_as::<_, ApiKey>(
        "SELECT id, org_id, key_hash, name, created_at, last_used_at FROM api_keys WHERE key_hash = $1",
    )
    .bind(hash)
    .fetch_optional(pool)
    .await?;

    Ok(key)
}

/// List all API keys for an organization (without exposing the hash).
pub async fn list_api_keys(pool: &PgPool, org_id: Uuid) -> Result<Vec<ApiKey>, DbError> {
    let keys = sqlx::query_as::<_, ApiKey>(
        "SELECT id, org_id, key_hash, name, created_at, last_used_at FROM api_keys WHERE org_id = $1 ORDER BY created_at",
    )
    .bind(org_id)
    .fetch_all(pool)
    .await?;

    Ok(keys)
}

/// Delete an API key by its ID.
pub async fn delete_api_key(pool: &PgPool, id: Uuid) -> Result<(), DbError> {
    sqlx::query("DELETE FROM api_keys WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;

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

/// Create a new pipeline for an organization.
pub async fn create_pipeline(
    pool: &PgPool,
    org_id: Uuid,
    name: &str,
    vpl_source: &str,
) -> Result<Pipeline, DbError> {
    let pipeline = sqlx::query_as::<_, Pipeline>(
        r"
        INSERT INTO pipelines (org_id, name, vpl_source)
        VALUES ($1, $2, $3)
        RETURNING id, org_id, name, vpl_source, status, created_at, updated_at
        ",
    )
    .bind(org_id)
    .bind(name)
    .bind(vpl_source)
    .fetch_one(pool)
    .await?;

    Ok(pipeline)
}

/// Get a pipeline by its ID.
pub async fn get_pipeline(pool: &PgPool, id: Uuid) -> Result<Option<Pipeline>, DbError> {
    let pipeline = sqlx::query_as::<_, Pipeline>(
        "SELECT id, org_id, name, vpl_source, status, created_at, updated_at FROM pipelines WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    Ok(pipeline)
}

/// List all pipelines belonging to an organization.
pub async fn list_pipelines(pool: &PgPool, org_id: Uuid) -> Result<Vec<Pipeline>, DbError> {
    let pipelines = sqlx::query_as::<_, Pipeline>(
        "SELECT id, org_id, name, vpl_source, status, created_at, updated_at FROM pipelines WHERE org_id = $1 ORDER BY created_at",
    )
    .bind(org_id)
    .fetch_all(pool)
    .await?;

    Ok(pipelines)
}

/// Update the status of a pipeline.
pub async fn update_pipeline_status(pool: &PgPool, id: Uuid, status: &str) -> Result<(), DbError> {
    sqlx::query("UPDATE pipelines SET status = $1, updated_at = $2 WHERE id = $3")
        .bind(status)
        .bind(Utc::now())
        .bind(id)
        .execute(pool)
        .await?;

    Ok(())
}

/// Update the VPL source of a pipeline.
pub async fn update_pipeline_source(
    pool: &PgPool,
    id: Uuid,
    vpl_source: &str,
) -> Result<(), DbError> {
    sqlx::query("UPDATE pipelines SET vpl_source = $1, updated_at = $2 WHERE id = $3")
        .bind(vpl_source)
        .bind(Utc::now())
        .bind(id)
        .execute(pool)
        .await?;

    Ok(())
}

/// Delete a pipeline by its ID.
pub async fn delete_pipeline(pool: &PgPool, id: Uuid) -> Result<(), DbError> {
    sqlx::query("DELETE FROM pipelines WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;

    Ok(())
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
