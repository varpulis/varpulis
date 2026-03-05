-- Org membership (many-to-many users <-> orgs, carrying per-org role)
CREATE TABLE org_members (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role TEXT NOT NULL DEFAULT 'member',   -- owner, admin, member, viewer
    status TEXT NOT NULL DEFAULT 'active', -- active, invited, suspended
    invited_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    accepted_at TIMESTAMPTZ,
    UNIQUE(org_id, user_id)
);
CREATE INDEX idx_org_members_user ON org_members(user_id);
CREATE INDEX idx_org_members_org ON org_members(org_id);

-- Backfill owner memberships from existing organizations.owner_id
INSERT INTO org_members (org_id, user_id, role, status, accepted_at)
SELECT id, owner_id, 'owner', 'active', created_at FROM organizations
ON CONFLICT (org_id, user_id) DO NOTHING;

-- Org slug for URL-safe identifiers
ALTER TABLE organizations ADD COLUMN IF NOT EXISTS slug TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS idx_organizations_slug
    ON organizations(slug) WHERE slug IS NOT NULL;

-- API key enhancements: prefix, scopes, expiry, soft-delete
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS key_prefix TEXT NOT NULL DEFAULT '';
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS scopes TEXT NOT NULL DEFAULT '*';
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ;
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS created_by UUID REFERENCES users(id);
