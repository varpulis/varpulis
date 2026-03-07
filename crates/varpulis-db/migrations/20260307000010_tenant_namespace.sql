-- Phase 3: Per-tenant Kubernetes namespace tracking
ALTER TABLE organizations
    ADD COLUMN IF NOT EXISTS k8s_namespace TEXT;

-- Sub-tenants share parent namespace
COMMENT ON COLUMN organizations.k8s_namespace IS
    'Kubernetes namespace for this tenant; NULL for sub-tenants (inherit parent)';
