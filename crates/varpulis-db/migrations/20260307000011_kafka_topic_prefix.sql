-- Phase 7: Per-tenant Kafka topic prefix for isolation
ALTER TABLE organizations
    ADD COLUMN IF NOT EXISTS kafka_topic_prefix TEXT;

-- Sub-tenants share parent's topic prefix
COMMENT ON COLUMN organizations.kafka_topic_prefix IS
    'Kafka topic prefix for this tenant; NULL for sub-tenants (inherit parent)';
