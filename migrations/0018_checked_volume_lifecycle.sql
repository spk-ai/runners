ALTER TABLE volumes
    ADD COLUMN lifecycle_revision BIGINT NOT NULL DEFAULT 1 CHECK (lifecycle_revision > 0),
    ADD COLUMN checked_lifecycle BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN bound_instance JSONB,
    ADD COLUMN removal_intent JSONB;

ALTER TABLE volumes ADD CONSTRAINT volumes_checked_state CHECK (
    (checked_lifecycle OR (bound_instance IS NULL AND removal_intent IS NULL))
    AND (bound_instance IS NULL OR COALESCE(
        jsonb_typeof(bound_instance) = 'object'
        AND jsonb_typeof(bound_instance->'instanceUid') = 'string'
        AND jsonb_typeof(bound_instance->'instanceId') = 'string'
        AND length(bound_instance->>'instanceUid') > 0
        AND length(bound_instance->>'instanceId') > 0
        AND bound_instance->>'instanceId' = instance_id
        AND bound_instance->>'volumeKey' = id::text
        AND bound_instance->'identityLabels'->>'volume_key' = id::text, FALSE))
    AND (removal_intent IS NULL OR COALESCE(
        jsonb_typeof(removal_intent) = 'object'
        AND length(removal_intent->>'id') > 0
        AND length(removal_intent->>'requestedAt') > 0
        AND removal_intent->'expected' = bound_instance, FALSE))
    AND (NOT checked_lifecycle OR (
        (status <> 'active' OR bound_instance IS NOT NULL)
        AND (status NOT IN ('deprovisioning', 'deleted') OR removal_intent IS NOT NULL)
        AND (status <> 'deleted' OR COALESCE(length(removal_intent->>'confirmedAt') > 0, FALSE))
        AND (removal_intent->>'confirmedAt' IS NULL OR status = 'deleted')
        AND (status NOT IN ('provisioning', 'active', 'failed') OR removal_intent IS NULL)
    ))
);

-- Old binaries do not know the revision column. Their lifecycle writes must
-- fail once a record has opted into checked operations, including after reopen.
-- Metering-only writes do not authorize deletion or invalidate a lifecycle CAS.
CREATE FUNCTION guard_checked_volume_lifecycle() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    lifecycle_changed BOOLEAN;
    reopening_deleted BOOLEAN;
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.checked_lifecycle THEN
            RAISE EXCEPTION 'checked volume records require explicit retention handling'
                USING ERRCODE = '55000', CONSTRAINT = 'volumes_checked_lifecycle';
        END IF;
        RETURN OLD;
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.checked_lifecycle AND (NEW.lifecycle_revision <> 1 OR NEW.status <> 'provisioning'
            OR NEW.bound_instance IS NOT NULL OR NEW.removal_intent IS NOT NULL
            OR NEW.instance_id IS NOT NULL OR NEW.removed_at IS NOT NULL) THEN
            RAISE EXCEPTION 'checked volumes must start unbound and provisioning'
                USING ERRCODE = '55000', CONSTRAINT = 'volumes_checked_lifecycle';
        END IF;
        RETURN NEW;
    END IF;

    lifecycle_changed := ROW(NEW.id, NEW.volume_id, NEW.thread_id, NEW.runner_id,
        NEW.agent_id, NEW.organization_id, NEW.owner_kind, NEW.owner_id,
        NEW.instance_id, NEW.size_gb, NEW.status, NEW.checked_lifecycle, NEW.bound_instance, NEW.removal_intent)
        IS DISTINCT FROM ROW(OLD.id, OLD.volume_id, OLD.thread_id, OLD.runner_id,
        OLD.agent_id, OLD.organization_id, OLD.owner_kind, OLD.owner_id,
        OLD.instance_id, OLD.size_gb, OLD.status, OLD.checked_lifecycle, OLD.bound_instance, OLD.removal_intent);

    IF lifecycle_changed AND NEW.lifecycle_revision = OLD.lifecycle_revision THEN
        IF OLD.checked_lifecycle OR NEW.checked_lifecycle THEN
            RAISE EXCEPTION 'checked volume lifecycle requires an explicit revision increment'
                USING ERRCODE = '55000', CONSTRAINT = 'volumes_checked_lifecycle';
        END IF;
        NEW.lifecycle_revision := OLD.lifecycle_revision + 1;
    END IF;
    IF NEW.lifecycle_revision <> OLD.lifecycle_revision AND NEW.lifecycle_revision <> OLD.lifecycle_revision + 1 THEN
        RAISE EXCEPTION 'invalid volume lifecycle revision'
            USING ERRCODE = '55000', CONSTRAINT = 'volumes_checked_lifecycle';
    END IF;

    IF OLD.checked_lifecycle OR NEW.checked_lifecycle THEN
        IF NOT NEW.checked_lifecycle OR ROW(NEW.id, NEW.volume_id, NEW.thread_id, NEW.runner_id,
            NEW.agent_id, NEW.organization_id, NEW.owner_kind, NEW.owner_id)
            IS DISTINCT FROM ROW(OLD.id, OLD.volume_id, OLD.thread_id, OLD.runner_id,
            OLD.agent_id, OLD.organization_id, OLD.owner_kind, OLD.owner_id) THEN
            RAISE EXCEPTION 'checked volume ownership is immutable'
                USING ERRCODE = '55000', CONSTRAINT = 'volumes_checked_lifecycle';
        END IF;
        reopening_deleted := OLD.checked_lifecycle AND OLD.status = 'deleted' AND NEW.status = 'provisioning'
            AND OLD.removal_intent->>'confirmedAt' IS NOT NULL;
        IF OLD.bound_instance IS NOT NULL AND NEW.bound_instance IS DISTINCT FROM OLD.bound_instance
            AND NOT (reopening_deleted AND NEW.bound_instance IS NULL) THEN
            RAISE EXCEPTION 'cannot replace a bound volume incarnation'
                USING ERRCODE = '55000', CONSTRAINT = 'volumes_checked_lifecycle';
        END IF;
        IF OLD.removal_intent IS NOT NULL AND NOT reopening_deleted THEN
            IF NEW.removal_intent IS NULL
                OR NEW.removal_intent - 'confirmedAt' IS DISTINCT FROM OLD.removal_intent - 'confirmedAt'
                OR (OLD.removal_intent->>'confirmedAt' IS NOT NULL
                    AND NEW.removal_intent->'confirmedAt' IS DISTINCT FROM OLD.removal_intent->'confirmedAt') THEN
                RAISE EXCEPTION 'cannot discard or retarget a removal intent'
                    USING ERRCODE = '55000', CONSTRAINT = 'volumes_checked_lifecycle';
            END IF;
        END IF;
        IF OLD.checked_lifecycle AND NEW.status <> OLD.status AND NOT (
            (OLD.status = 'provisioning' AND NEW.status IN ('active', 'failed', 'deprovisioning'))
            OR (OLD.status = 'active' AND NEW.status = 'deprovisioning')
            OR (OLD.status = 'deprovisioning' AND NEW.status = 'deleted')
            OR (OLD.status = 'failed' AND NEW.status = 'provisioning')
            OR reopening_deleted) THEN
            RAISE EXCEPTION 'invalid checked volume transition'
                USING ERRCODE = '55000', CONSTRAINT = 'volumes_checked_lifecycle';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER volumes_checked_lifecycle
    BEFORE INSERT OR UPDATE OR DELETE ON volumes
    FOR EACH ROW EXECUTE FUNCTION guard_checked_volume_lifecycle();
