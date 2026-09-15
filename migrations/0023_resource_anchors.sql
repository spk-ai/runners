-- Existing records retain their original lifecycle. No implicit owner adoption.
LOCK TABLE workloads, volumes, runtime_volume_admission_guards IN SHARE ROW EXCLUSIVE MODE;

ALTER TABLE workloads ADD COLUMN resource_anchors JSONB;
ALTER TABLE volumes ADD COLUMN resource_anchor JSONB, ADD COLUMN anchor_reservation JSONB;
ALTER TABLE runtime_volume_admission_guards
    ADD COLUMN resource_anchors_required BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE workloads ADD CONSTRAINT workloads_resource_anchors_shape CHECK (
    resource_anchors IS NULL OR (preparation_phase IS NOT NULL AND jsonb_typeof(resource_anchors) = 'object'));
ALTER TABLE volumes ADD CONSTRAINT volumes_resource_anchor_shape CHECK (
    (resource_anchor IS NULL AND anchor_reservation IS NULL) OR
    (resource_anchor IS NOT NULL AND anchor_reservation IS NOT NULL AND checked_lifecycle
        AND jsonb_typeof(resource_anchor) = 'object' AND jsonb_typeof(anchor_reservation) = 'object'));
ALTER TABLE runtime_volume_admission_guards ADD CONSTRAINT runtime_resource_anchor_pin CHECK (
    NOT resource_anchors_required OR prepared_backend_id IS NOT NULL);

CREATE FUNCTION valid_registry_resource_anchor(a JSONB, kind TEXT, resource_id UUID,
    backend TEXT, owner_kind TEXT, owner_id UUID, agent_id UUID, thread_id UUID)
RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    labels JSONB;
    human TEXT;
BEGIN
    IF a IS NULL OR jsonb_typeof(a) <> 'object' OR resource_id IS NULL OR owner_id IS NULL
        OR resource_id = '00000000-0000-0000-0000-000000000000'::uuid
        OR owner_id = '00000000-0000-0000-0000-000000000000'::uuid
        OR kind NOT IN ('RESOURCE_ANCHOR_KIND_WORKLOAD', 'RESOURCE_ANCHOR_KIND_VOLUME')
        OR (a->>'instanceUid' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            AND a->>'instanceUid' <> '00000000-0000-0000-0000-000000000000') IS NOT TRUE THEN
        RETURN FALSE;
    END IF;
    labels := jsonb_build_object('app.kubernetes.io/managed-by', 'k8s-runner',
        'agyn.dev/managed-by', 'agents-orchestrator', 'managed-by', 'agents-orchestrator');
    IF owner_kind = 'agent_instance' THEN
        IF agent_id IS NULL OR agent_id = '00000000-0000-0000-0000-000000000000'::uuid THEN RETURN FALSE; END IF;
        labels := labels || jsonb_build_object('agent-instance-id', owner_id::text, 'agent-id', agent_id::text);
        IF kind = 'RESOURCE_ANCHOR_KIND_WORKLOAD' THEN
            IF thread_id IS NULL OR thread_id = '00000000-0000-0000-0000-000000000000'::uuid THEN RETURN FALSE; END IF;
            labels := labels || jsonb_build_object('thread-id', thread_id::text);
        END IF;
    ELSIF owner_kind = 'sandbox' THEN
        human := a->'identityLabels'->>'sandbox-owner-id';
        IF (human ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            AND human <> '00000000-0000-0000-0000-000000000000') IS NOT TRUE THEN RETURN FALSE; END IF;
        labels := labels || jsonb_build_object('sandbox-id', owner_id::text, 'sandbox-owner-id', human);
    ELSE
        RETURN FALSE;
    END IF;
    IF kind = 'RESOURCE_ANCHOR_KIND_VOLUME' THEN
        labels := labels || jsonb_build_object('volume_key', resource_id::text);
    END IF;
    RETURN (a = jsonb_build_object('kind', kind, 'resourceId', resource_id::text,
        'backendId', backend, 'instanceUid', a->>'instanceUid', 'identityLabels', labels)) IS TRUE;
END;
$$;

CREATE FUNCTION guard_resource_anchor_owner() RETURNS trigger
LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    IF OLD.resource_anchors_required AND (TG_OP = 'DELETE' OR NOT NEW.resource_anchors_required) THEN
        RAISE EXCEPTION 'resource anchor owner pin is immutable'
            USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER runtime_resource_anchor_owner BEFORE UPDATE OR DELETE ON runtime_volume_admission_guards
    FOR EACH ROW EXECUTE FUNCTION guard_resource_anchor_owner();

CREATE FUNCTION guard_workload_resource_anchors() RETURNS trigger
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    pin runtime_volume_admission_guards%ROWTYPE;
    resources JSONB;
    anchor JSONB;
    volume_anchors JSONB;
    anchor_revision BIGINT;
    old_revision BIGINT;
    preparation_changed BOOLEAN;
    check_volume_records BOOLEAN;
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.resource_anchors IS NOT NULL THEN
            RAISE EXCEPTION 'anchored workload history requires explicit retirement evidence'
                USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
        END IF;
        RETURN OLD;
    END IF;
    PERFORM lock_runtime_volume_owner(NEW.owner_kind, NEW.owner_id);
    SELECT * INTO pin FROM runtime_volume_admission_guards WHERE owner_kind = NEW.owner_kind AND owner_id = NEW.owner_id;
    IF NEW.resource_anchors IS NULL THEN
        IF TG_OP = 'UPDATE' AND OLD.resource_anchors IS NOT NULL OR pin.resource_anchors_required
            AND (TG_OP = 'INSERT' OR NEW.status IN ('starting', 'running') OR NEW.removal_confirmed_at IS NULL) THEN
            RAISE EXCEPTION 'unanchored admission is disabled for this owner'
                USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
        END IF;
        RETURN NEW;
    END IF;
    resources := NEW.resource_anchors;
    IF (jsonb_typeof(resources) = 'object' AND resources - ARRAY['revision', 'workload', 'volumes'] = '{}'::jsonb
        AND resources->>'revision' ~ '^[1-9][0-9]{0,18}$'
        AND jsonb_typeof(resources->'revision') = 'string') IS NOT TRUE THEN
        RAISE EXCEPTION 'invalid workload resource revision'
            USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
    END IF;
    anchor_revision := (resources->>'revision')::bigint;
    check_volume_records := FALSE;
    IF TG_OP = 'INSERT' THEN
        IF resources <> '{"revision":"1"}'::jsonb OR NEW.preparation_phase <> 'reserved'
            OR NEW.preparation_revision <> 1 OR NEW.status <> 'starting' THEN
            RAISE EXCEPTION 'anchored workloads must start as unused reservations'
                USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
        END IF;
        IF EXISTS (SELECT 1 FROM volumes WHERE owner_kind = NEW.owner_kind AND owner_id = NEW.owner_id
            AND bound_instance IS NOT NULL AND resource_anchor IS NULL) THEN
            RAISE EXCEPTION 'existing volumes need explicit anchor adoption'
                USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
        END IF;
        IF NOT pin.resource_anchors_required THEN
            UPDATE runtime_volume_admission_guards SET resource_anchors_required = TRUE
                WHERE owner_kind = NEW.owner_kind AND owner_id = NEW.owner_id;
        END IF;
    ELSE
        IF OLD.resource_anchors IS NULL THEN
            RAISE EXCEPTION 'existing workloads cannot acquire anchor semantics'
                USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
        END IF;
        old_revision := (OLD.resource_anchors->>'revision')::bigint;
        preparation_changed := ROW(NEW.preparation_phase, NEW.prepared_binding, NEW.prepared_removal_observation)
            IS DISTINCT FROM ROW(OLD.preparation_phase, OLD.prepared_binding, OLD.prepared_removal_observation);
        IF preparation_changed THEN
            IF anchor_revision <> old_revision + 1 OR resources - 'revision' IS DISTINCT FROM OLD.resource_anchors - 'revision' THEN
                RAISE EXCEPTION 'anchored transitions require both checked revisions'
                    USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
            END IF;
            check_volume_records := NEW.preparation_phase IN ('preparing', 'bound', 'activating', 'active');
        ELSIF resources IS DISTINCT FROM OLD.resource_anchors THEN
            IF OLD.resource_anchors->'workload' IS NOT NULL OR resources->'workload' IS NULL
                OR NEW.preparation_phase <> 'reserved' OR NEW.status <> 'starting'
                OR NEW.preparation_revision <> OLD.preparation_revision OR anchor_revision <> old_revision + 1 THEN
                RAISE EXCEPTION 'anchor binding requires an unused reservation and exact revision'
                    USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
            END IF;
            check_volume_records := TRUE;
        END IF;
    END IF;
    anchor := resources->'workload';
    volume_anchors := COALESCE(resources->'volumes', '[]'::jsonb);
    IF anchor IS NULL THEN
        IF volume_anchors <> '[]'::jsonb OR NEW.preparation_phase NOT IN ('reserved', 'removed') OR NEW.prepared_binding IS NOT NULL THEN
            RAISE EXCEPTION 'native creation requires previously bound resource anchors'
                USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
        END IF;
    ELSE
        IF NOT valid_registry_resource_anchor(anchor, 'RESOURCE_ANCHOR_KIND_WORKLOAD', NEW.id, NEW.prepared_backend_id,
            NEW.owner_kind, NEW.owner_id, NEW.agent_id, NEW.thread_id) OR jsonb_typeof(volume_anchors) <> 'array' THEN
            RAISE EXCEPTION 'invalid workload resource anchor'
                USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
        END IF;
        IF jsonb_array_length(volume_anchors) <> cardinality(NEW.prepared_volume_ids)
            OR (SELECT count(DISTINCT a->>'resourceId') FROM jsonb_array_elements(volume_anchors) a) <> cardinality(NEW.prepared_volume_ids)
            OR EXISTS (SELECT 1 FROM jsonb_array_elements(volume_anchors) a WHERE NOT EXISTS (
                SELECT 1 FROM unnest(NEW.prepared_volume_ids) id WHERE a->>'resourceId' = id::text
                AND valid_registry_resource_anchor(a, 'RESOURCE_ANCHOR_KIND_VOLUME', id, NEW.prepared_backend_id,
                    NEW.owner_kind, NEW.owner_id, NEW.agent_id, NEW.thread_id)
                AND a->'identityLabels'->>'sandbox-owner-id' IS NOT DISTINCT FROM anchor->'identityLabels'->>'sandbox-owner-id')) THEN
            RAISE EXCEPTION 'invalid complete volume anchor set'
                USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
        END IF;
        IF check_volume_records AND EXISTS (SELECT 1 FROM jsonb_array_elements(volume_anchors) a WHERE NOT EXISTS (
            SELECT 1 FROM volumes v WHERE v.id::text = a->>'resourceId' AND v.id = ANY(NEW.prepared_volume_ids)
                AND v.checked_lifecycle AND v.status IN ('provisioning', 'active') AND v.removal_intent IS NULL
                AND v.resource_anchor = a AND v.owner_kind = NEW.owner_kind AND v.owner_id = NEW.owner_id
                AND ROW(v.runner_id, v.organization_id, v.thread_id, v.agent_id)
                    IS NOT DISTINCT FROM ROW(NEW.runner_id, NEW.organization_id, NEW.thread_id, NEW.agent_id))) THEN
            RAISE EXCEPTION 'volume anchors must be recorded before creation authority'
                USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
        END IF;
    END IF;
    IF NEW.prepared_binding IS NOT NULL AND (NEW.prepared_binding->'anchor' IS DISTINCT FROM anchor
        OR EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(NEW.prepared_binding->'volumes', '[]'::jsonb)) b
            WHERE NOT EXISTS (SELECT 1 FROM jsonb_array_elements(volume_anchors) a
                WHERE a->>'resourceId' = b->>'volumeKey' AND b->'anchor' = a))) THEN
        RAISE EXCEPTION 'native binding changed a previously recorded owner UID'
            USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER workloads_resource_anchors BEFORE INSERT OR UPDATE OR DELETE ON workloads
    FOR EACH ROW EXECUTE FUNCTION guard_workload_resource_anchors();

CREATE FUNCTION guard_volume_resource_anchor() RETURNS trigger
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
    IF TG_OP = 'UPDATE' AND OLD.resource_anchor IS NOT NULL AND
        ROW(NEW.resource_anchor, NEW.anchor_reservation) IS DISTINCT FROM ROW(OLD.resource_anchor, OLD.anchor_reservation) THEN
        RAISE EXCEPTION 'volume owner UID is immutable until checked retirement'
            USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
    END IF;
    IF NEW.resource_anchor IS NULL THEN
        IF pin.resource_anchors_required AND (NEW.bound_instance IS NOT NULL OR NEW.removal_intent IS NOT NULL OR NEW.status IN ('active', 'deprovisioning', 'deleted')) THEN
            RAISE EXCEPTION 'anchored owner cannot bind an unanchored workspace'
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
    IF NEW.status NOT IN ('provisioning', 'active') OR NEW.removal_intent IS NOT NULL
        OR NEW.size_gb IS DISTINCT FROM OLD.size_gb
        OR NEW.bound_instance IS NOT NULL AND (
            NEW.bound_instance->'anchor' IS DISTINCT FROM NEW.resource_anchor
            OR NEW.bound_instance->'identityLabels' IS DISTINCT FROM NEW.resource_anchor->'identityLabels'
            OR NEW.bound_instance->>'backendId' IS DISTINCT FROM NEW.resource_anchor->>'backendId') THEN
        RAISE EXCEPTION 'anchored volume requires exact binding or a separate retirement contract'
            USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER volumes_resource_anchor BEFORE INSERT OR UPDATE OR DELETE ON volumes
    FOR EACH ROW EXECUTE FUNCTION guard_volume_resource_anchor();
