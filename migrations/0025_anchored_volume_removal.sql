-- Retirement is separate from idle compute release. Retain the exact original
-- ownership, binding and native absence receipt; this does not authorize reuse.
LOCK TABLE workloads, volumes, runtime_volume_admission_guards IN SHARE ROW EXCLUSIVE MODE;

ALTER TABLE volumes ADD COLUMN anchored_removal_observation JSONB;
ALTER TABLE volumes ADD CONSTRAINT volumes_anchored_removal_state CHECK (
    anchored_removal_observation IS NULL OR COALESCE(
        checked_lifecycle AND resource_anchor IS NOT NULL AND status = 'deleted'
        AND removal_intent->'anchored' = 'true'::jsonb
        AND removal_intent->>'confirmedAt' IS NOT NULL
        AND anchored_removal_observation = jsonb_build_object(
            'state', 'VOLUME_REMOVAL_STATE_ABSENT', 'backendId', bound_instance->>'backendId',
            'anchor', resource_anchor), FALSE));

CREATE OR REPLACE FUNCTION guard_volume_resource_anchor() RETURNS trigger
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    pin runtime_volume_admission_guards%ROWTYPE;
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.resource_anchor IS NOT NULL THEN
            RAISE EXCEPTION 'anchored volume history must be retained'
                USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
        END IF;
        RETURN OLD;
    END IF;
    PERFORM lock_runtime_volume_owner(NEW.owner_kind, NEW.owner_id);
    SELECT * INTO pin FROM runtime_volume_admission_guards WHERE owner_kind = NEW.owner_kind AND owner_id = NEW.owner_id;
    IF TG_OP = 'UPDATE' THEN
        IF OLD.resource_anchor IS NOT NULL AND (
            ROW(NEW.resource_anchor, NEW.anchor_reservation) IS DISTINCT FROM ROW(OLD.resource_anchor, OLD.anchor_reservation)
            OR OLD.bound_instance IS NOT NULL AND NEW.bound_instance IS DISTINCT FROM OLD.bound_instance
            OR OLD.removal_intent IS NOT NULL AND NEW.removal_intent IS NULL) THEN
            RAISE EXCEPTION 'retirement does not authorize replacing persistent ownership or history'
                USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
        END IF;
        IF NEW.anchored_removal_observation IS DISTINCT FROM OLD.anchored_removal_observation AND (
            OLD.anchored_removal_observation IS NOT NULL OR NEW.lifecycle_revision <> OLD.lifecycle_revision + 1) THEN
            RAISE EXCEPTION 'native absence requires a checked immutable receipt'
                USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
        END IF;
    END IF;
    IF NEW.resource_anchor IS NULL THEN
        IF NEW.anchored_removal_observation IS NOT NULL
            OR COALESCE(NEW.removal_intent->'anchored', 'false'::jsonb) <> 'false'::jsonb
            OR pin.resource_anchors_required AND (NEW.bound_instance IS NOT NULL OR NEW.removal_intent IS NOT NULL OR NEW.status IN ('active', 'deprovisioning', 'deleted')) THEN
            RAISE EXCEPTION 'anchored owner cannot bind or retire an unanchored workspace'
                USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
        END IF;
        RETURN NEW;
    END IF;
    IF NOT pin.resource_anchors_required OR NOT NEW.checked_lifecycle OR
        NOT valid_registry_resource_anchor(NEW.resource_anchor, 'RESOURCE_ANCHOR_KIND_VOLUME', NEW.id, pin.prepared_backend_id,
            NEW.owner_kind, NEW.owner_id, NEW.agent_id, NEW.thread_id) THEN
        RAISE EXCEPTION 'invalid persistent volume anchor'
            USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
    END IF;
    IF TG_OP = 'INSERT' THEN
        RAISE EXCEPTION 'volume anchors require a checked existing reservation'
            USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
    END IF;
    IF OLD.resource_anchor IS NULL THEN
        IF (jsonb_typeof(NEW.anchor_reservation) = 'object'
            AND NEW.anchor_reservation - ARRAY['workloadId', 'preparationRevision', 'resourceRevision'] = '{}'::jsonb
            AND NEW.anchor_reservation->>'workloadId' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            AND NEW.anchor_reservation->>'workloadId' <> '00000000-0000-0000-0000-000000000000'
            AND jsonb_typeof(NEW.anchor_reservation->'preparationRevision') = 'string'
            AND jsonb_typeof(NEW.anchor_reservation->'resourceRevision') = 'string'
            AND NEW.anchor_reservation->>'preparationRevision' ~ '^[1-9][0-9]{0,18}$'
            AND NEW.anchor_reservation->>'resourceRevision' ~ '^[1-9][0-9]{0,18}$') IS NOT TRUE THEN
            RAISE EXCEPTION 'exact volume anchor reservation receipt required'
                USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
        END IF;
        IF NOT OLD.checked_lifecycle OR OLD.status <> 'provisioning' OR OLD.bound_instance IS NOT NULL
            OR OLD.instance_id IS NOT NULL OR OLD.removal_intent IS NOT NULL
            OR NEW.lifecycle_revision <> OLD.lifecycle_revision + 1 OR NEW.status <> 'provisioning'
            OR NEW.bound_instance IS NOT NULL OR NEW.instance_id IS NOT NULL
            OR NOT EXISTS (SELECT 1 FROM workloads w WHERE w.owner_kind = NEW.owner_kind AND w.owner_id = NEW.owner_id
                AND w.id::text = NEW.anchor_reservation->>'workloadId'
                AND w.preparation_revision = (NEW.anchor_reservation->>'preparationRevision')::bigint
                AND w.resource_anchors->>'revision' = NEW.anchor_reservation->>'resourceRevision'
                AND w.preparation_phase = 'reserved' AND w.status = 'starting' AND w.resource_anchors IS NOT NULL
                AND w.resource_anchors->'workload' IS NULL AND NEW.id = ANY(w.prepared_volume_ids)) THEN
            RAISE EXCEPTION 'volume anchor binding requires an unused anchored workload'
                USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
        END IF;
    END IF;
    IF NEW.size_gb IS DISTINCT FROM OLD.size_gb
        OR NEW.bound_instance IS NOT NULL AND (
            NEW.bound_instance->'anchor' IS DISTINCT FROM NEW.resource_anchor
            OR NEW.bound_instance->'identityLabels' IS DISTINCT FROM NEW.resource_anchor->'identityLabels'
            OR NEW.bound_instance->>'backendId' IS DISTINCT FROM NEW.resource_anchor->>'backendId') THEN
        RAISE EXCEPTION 'anchored volume requires its exact original binding'
            USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
    END IF;
    IF NEW.status IN ('provisioning', 'active') THEN
        IF NEW.removal_intent IS NOT NULL OR NEW.anchored_removal_observation IS NOT NULL THEN
            RAISE EXCEPTION 'open anchored volume cannot carry retirement authority'
                USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.status NOT IN ('deprovisioning', 'deleted') OR (
        NEW.bound_instance IS NOT NULL AND NEW.resource_anchor = OLD.resource_anchor
        AND jsonb_typeof(NEW.removal_intent) = 'object'
        AND NEW.removal_intent - ARRAY['id', 'expected', 'requestedAt', 'confirmedAt', 'anchored'] = '{}'::jsonb
        AND NEW.removal_intent->>'id' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        AND NEW.removal_intent->>'id' <> '00000000-0000-0000-0000-000000000000'
        AND jsonb_typeof(NEW.removal_intent->'requestedAt') = 'string'
        AND length(NEW.removal_intent->>'requestedAt') > 0
        AND NEW.removal_intent->'anchored' = 'true'::jsonb
        AND NEW.removal_intent->'expected' = NEW.bound_instance) IS NOT TRUE THEN
        RAISE EXCEPTION 'bound anchored retirement intent required'
            USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
    END IF;
    IF NEW.status = 'deprovisioning' THEN
        IF NEW.removal_intent ? 'confirmedAt' OR NEW.anchored_removal_observation IS NOT NULL THEN
            RAISE EXCEPTION 'pending anchored retirement cannot be confirmed'
                USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
        END IF;
    ELSIF (jsonb_typeof(NEW.removal_intent->'confirmedAt') = 'string'
        AND length(NEW.removal_intent->>'confirmedAt') > 0
        AND NEW.anchored_removal_observation = jsonb_build_object(
            'state', 'VOLUME_REMOVAL_STATE_ABSENT', 'backendId', NEW.bound_instance->>'backendId',
            'anchor', NEW.resource_anchor)) IS NOT TRUE THEN
        RAISE EXCEPTION 'matching native anchored absence receipt required'
            USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
    END IF;
    RETURN NEW;
END;
$$;
