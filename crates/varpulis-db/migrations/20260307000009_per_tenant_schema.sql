-- Phase 2: Per-Tenant PostgreSQL Schema
-- Backfill slugs for existing orgs, add schema provisioning infrastructure.

-- 1. Backfill slugs from org name (lowercase, replace non-alnum with underscore)
-- Use id suffix to guarantee uniqueness
UPDATE organizations
SET slug = lower(regexp_replace(
    regexp_replace(
        regexp_replace(name, '[^a-zA-Z0-9]+', '_', 'g'),
        '^_+', ''),
    '_+$', ''))
    || '_' || left(id::text, 8)
WHERE slug IS NULL;

-- 2. Backfill db_schema for existing tenant orgs that don't have one
UPDATE organizations
SET db_schema = 'tenant_' || slug
WHERE org_type = 'tenant' AND db_schema IS NULL AND slug IS NOT NULL;

-- 3. Enable RLS on data plane tables for sub-tenant isolation
ALTER TABLE pipelines ENABLE ROW LEVEL SECURITY;
ALTER TABLE usage_daily ENABLE ROW LEVEL SECURITY;

-- Allow the table owner to bypass RLS (needed for migrations and admin operations)
ALTER TABLE pipelines FORCE ROW LEVEL SECURITY;
ALTER TABLE usage_daily FORCE ROW LEVEL SECURITY;

-- Default permissive policy (all rows visible)
-- Actual tenant isolation is enforced by the application via org_id WHERE clauses
-- and per-tenant schemas.
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname = 'pipelines_default' AND tablename = 'pipelines') THEN
        CREATE POLICY pipelines_default ON pipelines FOR ALL USING (true) WITH CHECK (true);
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname = 'usage_daily_default' AND tablename = 'usage_daily') THEN
        CREATE POLICY usage_daily_default ON usage_daily FOR ALL USING (true) WITH CHECK (true);
    END IF;
END $$;
