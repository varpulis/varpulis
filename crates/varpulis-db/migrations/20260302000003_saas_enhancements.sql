-- SaaS enhancements: trial lifecycle, per-org limits, status tracking

ALTER TABLE organizations ADD COLUMN IF NOT EXISTS trial_expires_at TIMESTAMPTZ;
ALTER TABLE organizations ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active';
ALTER TABLE organizations ADD COLUMN IF NOT EXISTS pipeline_limit INTEGER NOT NULL DEFAULT 5;
ALTER TABLE organizations ADD COLUMN IF NOT EXISTS events_per_second_limit INTEGER NOT NULL DEFAULT 500;
ALTER TABLE organizations ADD COLUMN IF NOT EXISTS monthly_event_limit BIGINT NOT NULL DEFAULT 100000;
ALTER TABLE organizations ADD COLUMN IF NOT EXISTS notes TEXT NOT NULL DEFAULT '';
ALTER TABLE organizations ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS idx_organizations_trial_expires
    ON organizations(trial_expires_at)
    WHERE status = 'trial';

CREATE INDEX IF NOT EXISTS idx_organizations_status
    ON organizations(status);
