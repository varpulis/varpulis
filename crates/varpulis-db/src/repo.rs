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

const ORG_COLUMNS: &str = "id, owner_id, name, tier, stripe_customer_id, trial_expires_at, status, pipeline_limit, events_per_second_limit, monthly_event_limit, notes, created_at, updated_at, slug, org_type, parent_org_id, db_schema, k8s_namespace, kafka_topic_prefix";

/// Create a new organization owned by the given user.
/// Also inserts an `org_members` row with role `owner`.
/// Auto-generates a slug and provisions a tenant schema.
pub async fn create_organization(
    pool: &PgPool,
    owner_id: Uuid,
    name: &str,
) -> Result<Organization, DbError> {
    let base_slug = generate_slug(name);
    let slug = ensure_unique_slug(pool, &base_slug).await?;

    let query = format!(
        "INSERT INTO organizations (owner_id, name, slug) \
         VALUES ($1, $2, $3) RETURNING {ORG_COLUMNS}"
    );
    let org = sqlx::query_as::<_, Organization>(&query)
        .bind(owner_id)
        .bind(name)
        .bind(&slug)
        .fetch_one(pool)
        .await?;

    // Ensure the owner is also a member
    let _ = add_org_member(pool, org.id, owner_id, "owner").await;

    // Provision tenant schema (best-effort, log error)
    if let Err(e) = provision_tenant_schema(pool, org.id).await {
        tracing::error!("Failed to provision schema for org {}: {}", org.id, e);
    }

    // Re-read to get updated db_schema
    let org = get_organization(pool, org.id).await?.unwrap_or(org);

    Ok(org)
}

/// Create a new organization with trial status (30-day free trial).
/// Also inserts an `org_members` row with role `owner`.
/// Auto-generates a slug and provisions a tenant schema.
pub async fn create_trial_organization(
    pool: &PgPool,
    owner_id: Uuid,
    name: &str,
) -> Result<Organization, DbError> {
    let base_slug = generate_slug(name);
    let slug = ensure_unique_slug(pool, &base_slug).await?;

    let query = format!(
        "INSERT INTO organizations (owner_id, name, slug, status, trial_expires_at) \
         VALUES ($1, $2, $3, 'trial', now() + interval '30 days') \
         RETURNING {ORG_COLUMNS}"
    );
    let org = sqlx::query_as::<_, Organization>(&query)
        .bind(owner_id)
        .bind(name)
        .bind(&slug)
        .fetch_one(pool)
        .await?;

    // Ensure the owner is also a member
    let _ = add_org_member(pool, org.id, owner_id, "owner").await;

    // Provision tenant schema (best-effort, log error)
    if let Err(e) = provision_tenant_schema(pool, org.id).await {
        tracing::error!("Failed to provision schema for org {}: {}", org.id, e);
    }

    // Re-read to get updated db_schema
    let org = get_organization(pool, org.id).await?.unwrap_or(org);

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
// Tenant Hierarchy
// ---------------------------------------------------------------------------

/// Create a sub-tenant under a parent tenant.
/// The parent must be org_type 'tenant' (no sub-sub-tenants allowed).
/// Sub-tenants inherit the parent's db_schema (shared schema, isolated by org_id).
pub async fn create_sub_tenant(
    pool: &PgPool,
    parent_org_id: Uuid,
    owner_id: Uuid,
    name: &str,
) -> Result<Organization, DbError> {
    // Verify parent is a tenant (not global, not sub_tenant)
    let parent = get_organization(pool, parent_org_id)
        .await?
        .ok_or_else(|| DbError::Pool("Parent organization not found".to_string()))?;
    if parent.org_type != "tenant" {
        return Err(DbError::Pool(
            "Sub-tenants can only be created under tenant-type organizations".to_string(),
        ));
    }

    let base_slug = generate_slug(name);
    let slug = ensure_unique_slug(pool, &base_slug).await?;

    // Sub-tenant inherits parent's db_schema (shared schema, RLS isolation)
    let inherited_schema = parent.db_schema.as_deref();

    let query = format!(
        "INSERT INTO organizations (owner_id, name, slug, org_type, parent_org_id, db_schema, status) \
         VALUES ($1, $2, $3, 'sub_tenant', $4, $5, 'active') \
         RETURNING {ORG_COLUMNS}"
    );
    let org = sqlx::query_as::<_, Organization>(&query)
        .bind(owner_id)
        .bind(name)
        .bind(&slug)
        .bind(parent_org_id)
        .bind(inherited_schema)
        .fetch_one(pool)
        .await?;

    // Ensure the owner is also a member
    let _ = add_org_member(pool, org.id, owner_id, "owner").await;

    Ok(org)
}

/// Get or create the singleton global organization.
pub async fn get_or_create_global_org(
    pool: &PgPool,
    owner_id: Uuid,
) -> Result<Organization, DbError> {
    let query =
        format!("SELECT {ORG_COLUMNS} FROM organizations WHERE org_type = 'global' LIMIT 1");
    if let Some(org) = sqlx::query_as::<_, Organization>(&query)
        .fetch_optional(pool)
        .await?
    {
        return Ok(org);
    }

    let query = format!(
        "INSERT INTO organizations (owner_id, name, slug, org_type, tier, status, db_schema) \
         VALUES ($1, 'Global', 'global', 'global', 'enterprise', 'active', 'tenant_global') \
         RETURNING {ORG_COLUMNS}"
    );
    let org = sqlx::query_as::<_, Organization>(&query)
        .bind(owner_id)
        .fetch_one(pool)
        .await?;

    let _ = add_org_member(pool, org.id, owner_id, "owner").await;

    // Provision global schema (best-effort)
    if let Err(e) = provision_tenant_schema(pool, org.id).await {
        tracing::error!("Failed to provision global schema: {}", e);
    }

    Ok(org)
}

/// Get the global organization (if it exists).
pub async fn get_global_org(pool: &PgPool) -> Result<Option<Organization>, DbError> {
    let query =
        format!("SELECT {ORG_COLUMNS} FROM organizations WHERE org_type = 'global' LIMIT 1");
    let org = sqlx::query_as::<_, Organization>(&query)
        .fetch_optional(pool)
        .await?;
    Ok(org)
}

/// List direct child organizations of a parent.
pub async fn list_child_organizations(
    pool: &PgPool,
    parent_id: Uuid,
) -> Result<Vec<Organization>, DbError> {
    let query = format!(
        "SELECT {ORG_COLUMNS} FROM organizations WHERE parent_org_id = $1 ORDER BY created_at"
    );
    let orgs = sqlx::query_as::<_, Organization>(&query)
        .bind(parent_id)
        .fetch_all(pool)
        .await?;
    Ok(orgs)
}

/// List all tenants (org_type = 'tenant'). For global admin view.
pub async fn list_tenants(pool: &PgPool) -> Result<Vec<Organization>, DbError> {
    let query = format!(
        "SELECT {ORG_COLUMNS} FROM organizations WHERE org_type = 'tenant' ORDER BY created_at"
    );
    let orgs = sqlx::query_as::<_, Organization>(&query)
        .fetch_all(pool)
        .await?;
    Ok(orgs)
}

/// List all sub-tenants of a given tenant.
pub async fn list_sub_tenants(
    pool: &PgPool,
    tenant_id: Uuid,
) -> Result<Vec<Organization>, DbError> {
    let query = format!(
        "SELECT {ORG_COLUMNS} FROM organizations \
         WHERE parent_org_id = $1 AND org_type = 'sub_tenant' ORDER BY created_at"
    );
    let orgs = sqlx::query_as::<_, Organization>(&query)
        .bind(tenant_id)
        .fetch_all(pool)
        .await?;
    Ok(orgs)
}

/// List pipelines visible to an org, including inherited ones from parent and global.
/// Returns own pipelines + inherited from parent tenant + inherited from global org.
pub async fn list_visible_pipelines(pool: &PgPool, org_id: Uuid) -> Result<Vec<Pipeline>, DbError> {
    // Get the org to determine its type and parent
    let org = get_organization(pool, org_id)
        .await?
        .ok_or_else(|| DbError::Pool("Organization not found".to_string()))?;

    match org.org_type.as_str() {
        "global" => {
            // Global sees only its own pipelines
            list_pipelines(pool, org_id).await
        }
        "tenant" => {
            // Tenant sees own pipelines (includes inherited global copies with org_id = tenant)
            let query = format!(
                "SELECT {PIPELINE_COLUMNS} FROM pipelines \
                 WHERE org_id = $1 \
                 ORDER BY scope_level DESC, created_at"
            );
            let pipelines = sqlx::query_as::<_, Pipeline>(&query)
                .bind(org_id)
                .fetch_all(pool)
                .await?;
            Ok(pipelines)
        }
        "sub_tenant" => {
            // Sub-tenant sees own pipelines (includes inherited copies with org_id = sub_tenant)
            // Inherited pipelines are copied to sub-tenant via propagate_pipeline_to_sub_tenants
            let query = format!(
                "SELECT {PIPELINE_COLUMNS} FROM pipelines \
                 WHERE org_id = $1 \
                 ORDER BY scope_level DESC, created_at"
            );
            let pipelines = sqlx::query_as::<_, Pipeline>(&query)
                .bind(org_id)
                .fetch_all(pool)
                .await?;
            Ok(pipelines)
        }
        _ => list_pipelines(pool, org_id).await,
    }
}

/// Create a pipeline with scope level and optional inheritance.
pub async fn create_scoped_pipeline(
    pool: &PgPool,
    org_id: Uuid,
    name: &str,
    vpl_source: &str,
    scope_level: &str,
) -> Result<Pipeline, DbError> {
    let query = format!(
        "INSERT INTO pipelines (org_id, name, vpl_source, scope_level) \
         VALUES ($1, $2, $3, $4) RETURNING {PIPELINE_COLUMNS}"
    );
    let pipeline = sqlx::query_as::<_, Pipeline>(&query)
        .bind(org_id)
        .bind(name)
        .bind(vpl_source)
        .bind(scope_level)
        .fetch_one(pool)
        .await?;
    Ok(pipeline)
}

/// Propagate a tenant-scoped pipeline to all sub-tenants as inherited.
pub async fn propagate_pipeline_to_sub_tenants(
    pool: &PgPool,
    pipeline: &Pipeline,
) -> Result<u64, DbError> {
    let sub_tenants = list_sub_tenants(pool, pipeline.org_id).await?;
    let mut count = 0u64;
    for sub in &sub_tenants {
        let query = format!(
            "INSERT INTO pipelines (org_id, name, vpl_source, scope_level, inherited_from_org_id) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT DO NOTHING \
             RETURNING {PIPELINE_COLUMNS}"
        );
        let result = sqlx::query_as::<_, Pipeline>(&query)
            .bind(sub.id)
            .bind(&pipeline.name)
            .bind(&pipeline.vpl_source)
            .bind(&pipeline.scope_level)
            .bind(pipeline.org_id)
            .fetch_optional(pool)
            .await?;
        if result.is_some() {
            count += 1;
        }
    }
    Ok(count)
}

// ---------------------------------------------------------------------------
// Slug Generation & Schema Provisioning
// ---------------------------------------------------------------------------

/// Generate a URL-safe slug from an org name.
/// Lowercases, replaces non-alphanumeric with underscore, collapses runs.
pub fn generate_slug(name: &str) -> String {
    let mut result = String::with_capacity(name.len());
    let mut prev_underscore = true; // start true to skip leading underscores
    for c in name.chars() {
        if c.is_ascii_alphanumeric() {
            result.push(c.to_ascii_lowercase());
            prev_underscore = false;
        } else if !prev_underscore {
            result.push('_');
            prev_underscore = true;
        }
    }
    result.trim_end_matches('_').to_string()
}

/// Ensure a slug is unique in the organizations table, appending a numeric suffix if needed.
pub async fn ensure_unique_slug(pool: &PgPool, base_slug: &str) -> Result<String, DbError> {
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM organizations WHERE slug = $1")
        .bind(base_slug)
        .fetch_one(pool)
        .await?;
    if count.0 == 0 {
        return Ok(base_slug.to_string());
    }
    for i in 1..1000 {
        let candidate = format!("{base_slug}_{i}");
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM organizations WHERE slug = $1")
            .bind(&candidate)
            .fetch_one(pool)
            .await?;
        if count.0 == 0 {
            return Ok(candidate);
        }
    }
    Err(DbError::Pool("Could not generate unique slug".to_string()))
}

/// Provision a PostgreSQL schema for a tenant organization.
/// Creates `tenant_{slug}` schema with data plane tables (pipelines, usage_daily).
/// Updates the org's `db_schema` column.
pub async fn provision_tenant_schema(pool: &PgPool, org_id: Uuid) -> Result<String, DbError> {
    let org = get_organization(pool, org_id)
        .await?
        .ok_or_else(|| DbError::Pool("Organization not found".to_string()))?;

    let slug = org
        .slug
        .as_ref()
        .ok_or_else(|| DbError::Pool("Organization has no slug".to_string()))?;

    let schema_name = format!("tenant_{slug}");

    // Validate schema name characters (prevent SQL injection)
    if !schema_name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(DbError::Pool("Invalid schema name".to_string()));
    }

    // Create schema
    sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema_name}\""))
        .execute(pool)
        .await?;

    // Create data plane tables in tenant schema
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS \"{schema_name}\".pipelines ( \
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(), \
            org_id UUID NOT NULL REFERENCES public.organizations(id) ON DELETE CASCADE, \
            name TEXT NOT NULL, \
            vpl_source TEXT NOT NULL DEFAULT '', \
            status TEXT NOT NULL DEFAULT 'stopped', \
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(), \
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), \
            global_template_id UUID, \
            scope_level TEXT NOT NULL DEFAULT 'own', \
            inherited_from_org_id UUID REFERENCES public.organizations(id) ON DELETE CASCADE \
        )"
    ))
    .execute(pool)
    .await?;

    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS \"{schema_name}\".usage_daily ( \
            org_id UUID NOT NULL REFERENCES public.organizations(id) ON DELETE CASCADE, \
            date DATE NOT NULL, \
            events_processed BIGINT NOT NULL DEFAULT 0, \
            output_events BIGINT NOT NULL DEFAULT 0, \
            PRIMARY KEY (org_id, date) \
        )"
    ))
    .execute(pool)
    .await?;

    // Create indexes
    sqlx::query(&format!(
        "CREATE INDEX IF NOT EXISTS idx_pipelines_org ON \"{schema_name}\".pipelines(org_id)"
    ))
    .execute(pool)
    .await?;

    sqlx::query(&format!(
        "CREATE INDEX IF NOT EXISTS idx_usage_daily_org_date \
         ON \"{schema_name}\".usage_daily(org_id, date)"
    ))
    .execute(pool)
    .await?;

    // Update org with schema name
    sqlx::query("UPDATE organizations SET db_schema = $1 WHERE id = $2")
        .bind(&schema_name)
        .bind(org_id)
        .execute(pool)
        .await?;

    tracing::info!(
        "Provisioned tenant schema '{}' for org {}",
        schema_name,
        org_id
    );
    Ok(schema_name)
}

/// Verify that a PostgreSQL schema exists.
pub async fn verify_schema_exists(pool: &PgPool, schema_name: &str) -> Result<bool, DbError> {
    let result: Option<(String,)> = sqlx::query_as(
        "SELECT schema_name FROM information_schema.schemata WHERE schema_name = $1",
    )
    .bind(schema_name)
    .fetch_optional(pool)
    .await?;
    Ok(result.is_some())
}

/// Get the effective schema for an org (own db_schema or parent's).
pub async fn get_effective_schema(pool: &PgPool, org_id: Uuid) -> Result<Option<String>, DbError> {
    let org = get_organization(pool, org_id)
        .await?
        .ok_or_else(|| DbError::Pool("Organization not found".to_string()))?;

    if let Some(ref schema) = org.db_schema {
        return Ok(Some(schema.clone()));
    }

    // Sub-tenant: inherit from parent
    if let Some(parent_id) = org.parent_org_id {
        let parent = get_organization(pool, parent_id)
            .await?
            .ok_or_else(|| DbError::Pool("Parent organization not found".to_string()))?;
        return Ok(parent.db_schema);
    }

    Ok(None)
}

/// List tables in a tenant schema.
pub async fn list_schema_tables(pool: &PgPool, schema_name: &str) -> Result<Vec<String>, DbError> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT table_name FROM information_schema.tables \
         WHERE table_schema = $1 ORDER BY table_name",
    )
    .bind(schema_name)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|r| r.0).collect())
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
    "id, org_id, name, vpl_source, status, created_at, updated_at, global_template_id, scope_level, inherited_from_org_id";

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

/// Delete a pipeline by name for a given org (used to sync runtime deletions to DB).
pub async fn delete_pipeline_by_name(
    pool: &PgPool,
    org_id: Uuid,
    name: &str,
) -> Result<(), DbError> {
    sqlx::query("DELETE FROM pipelines WHERE org_id = $1 AND name = $2")
        .bind(org_id)
        .bind(name)
        .execute(pool)
        .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Kubernetes Namespace Management
// ---------------------------------------------------------------------------

/// Set the k8s_namespace for a tenant org.
pub async fn set_k8s_namespace(
    pool: &PgPool,
    org_id: Uuid,
    namespace: &str,
) -> Result<(), DbError> {
    sqlx::query("UPDATE organizations SET k8s_namespace = $1 WHERE id = $2")
        .bind(namespace)
        .bind(org_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Clear the k8s_namespace for a tenant org.
pub async fn clear_k8s_namespace(pool: &PgPool, org_id: Uuid) -> Result<(), DbError> {
    sqlx::query("UPDATE organizations SET k8s_namespace = NULL WHERE id = $1")
        .bind(org_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Get the effective k8s namespace for an org (own or parent's).
pub async fn get_effective_namespace(
    pool: &PgPool,
    org_id: Uuid,
) -> Result<Option<String>, DbError> {
    let org = get_organization(pool, org_id)
        .await?
        .ok_or_else(|| DbError::Pool("Organization not found".to_string()))?;

    if let Some(ns) = org.k8s_namespace {
        return Ok(Some(ns));
    }

    // Sub-tenants inherit parent's namespace
    if let Some(parent_id) = org.parent_org_id {
        if let Some(parent) = get_organization(pool, parent_id).await? {
            return Ok(parent.k8s_namespace);
        }
    }

    Ok(None)
}

// ---------------------------------------------------------------------------
// Kafka Topic Prefix Management
// ---------------------------------------------------------------------------

/// Set the kafka_topic_prefix for a tenant org.
pub async fn set_kafka_topic_prefix(
    pool: &PgPool,
    org_id: Uuid,
    prefix: &str,
) -> Result<(), DbError> {
    sqlx::query("UPDATE organizations SET kafka_topic_prefix = $1 WHERE id = $2")
        .bind(prefix)
        .bind(org_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Clear the kafka_topic_prefix for a tenant org.
pub async fn clear_kafka_topic_prefix(pool: &PgPool, org_id: Uuid) -> Result<(), DbError> {
    sqlx::query("UPDATE organizations SET kafka_topic_prefix = NULL WHERE id = $1")
        .bind(org_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Get the effective Kafka topic prefix for an org (own or parent's).
pub async fn get_effective_topic_prefix(
    pool: &PgPool,
    org_id: Uuid,
) -> Result<Option<String>, DbError> {
    let org = get_organization(pool, org_id)
        .await?
        .ok_or_else(|| DbError::Pool("Organization not found".to_string()))?;

    if let Some(prefix) = org.kafka_topic_prefix {
        return Ok(Some(prefix));
    }

    // Sub-tenants inherit parent's topic prefix
    if let Some(parent_id) = org.parent_org_id {
        if let Some(parent) = get_organization(pool, parent_id).await? {
            return Ok(parent.kafka_topic_prefix);
        }
    }

    Ok(None)
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
    // Find the global org to set inherited_from_org_id
    let global_org_id = get_global_org(pool).await?.map(|o| o.id);
    let query = format!(
        "INSERT INTO pipelines (org_id, name, vpl_source, global_template_id, scope_level, inherited_from_org_id) \
         VALUES ($1, $2, $3, $4, 'global', $5) RETURNING {PIPELINE_COLUMNS}"
    );
    let pipeline = sqlx::query_as::<_, Pipeline>(&query)
        .bind(org_id)
        .bind(name)
        .bind(vpl_source)
        .bind(template_id)
        .bind(global_org_id)
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
