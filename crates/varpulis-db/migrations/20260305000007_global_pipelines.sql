-- Global pipeline templates: admin-deployed pipelines that auto-copy to every tenant
CREATE TABLE IF NOT EXISTS global_pipeline_templates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    vpl_source TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'deployed',
    deployed_by UUID REFERENCES users(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Link pipelines to the global template they were copied from
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS global_template_id UUID
    REFERENCES global_pipeline_templates(id) ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS idx_pipelines_global_template
    ON pipelines(global_template_id) WHERE global_template_id IS NOT NULL;
