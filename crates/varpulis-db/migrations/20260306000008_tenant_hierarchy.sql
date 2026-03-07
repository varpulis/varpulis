-- Tenant hierarchy: Global -> Tenant -> Sub-Tenant (max 3 levels)

-- Organization type: 'global' (singleton), 'tenant', 'sub_tenant'
ALTER TABLE organizations ADD COLUMN IF NOT EXISTS org_type TEXT NOT NULL DEFAULT 'tenant';

-- Parent org reference (NULL for global, global_id for tenants, tenant_id for sub-tenants)
ALTER TABLE organizations ADD COLUMN IF NOT EXISTS parent_org_id UUID
    REFERENCES organizations(id) ON DELETE RESTRICT;

-- PostgreSQL schema name for data isolation (NULL for sub-tenants = inherit parent)
ALTER TABLE organizations ADD COLUMN IF NOT EXISTS db_schema TEXT;

CREATE INDEX IF NOT EXISTS idx_organizations_parent ON organizations(parent_org_id)
    WHERE parent_org_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_organizations_type ON organizations(org_type);

-- Pipeline scoping for inheritance visibility
-- 'global' = visible to all tenants/sub-tenants, editable only at global level
-- 'tenant' = visible to sub-tenants, editable only at tenant level
-- 'own'    = visible and editable only within the owning org
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS scope_level TEXT NOT NULL DEFAULT 'own';

-- Source org for inherited pipelines (NULL = pipeline belongs to its org_id)
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS inherited_from_org_id UUID
    REFERENCES organizations(id) ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS idx_pipelines_scope ON pipelines(scope_level)
    WHERE scope_level != 'own';
CREATE INDEX IF NOT EXISTS idx_pipelines_inherited ON pipelines(inherited_from_org_id)
    WHERE inherited_from_org_id IS NOT NULL;

-- Constraint: org_type must be one of the allowed values
ALTER TABLE organizations ADD CONSTRAINT chk_org_type
    CHECK (org_type IN ('global', 'tenant', 'sub_tenant'));

-- Constraint: scope_level must be one of the allowed values
ALTER TABLE pipelines ADD CONSTRAINT chk_scope_level
    CHECK (scope_level IN ('global', 'tenant', 'own'));
