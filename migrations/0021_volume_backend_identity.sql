-- No inferred backfill: an existing binding without a verified backend needs
-- operator reconciliation. Installation fails atomically for such history.
ALTER TABLE volumes ADD CONSTRAINT volumes_checked_backend CHECK (
    bound_instance IS NULL OR COALESCE(
        jsonb_typeof(bound_instance->'backendId') = 'string'
        AND octet_length(bound_instance->>'backendId') BETWEEN 1 AND 512
        AND bound_instance->>'backendId' !~ '^[[:space:]]|[[:space:]]$', FALSE)
);
