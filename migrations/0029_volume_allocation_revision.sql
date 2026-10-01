-- Preserve the exact first anchor-binding revision after an unused checked
-- volume fails/reopens. Historical receipts retain their revision-2 meaning.
-- No existing rows, identities, binding receipts or applied migrations change.
LOCK TABLE workloads, volumes, runtime_volume_admission_guards IN SHARE ROW EXCLUSIVE MODE;

CREATE FUNCTION volume_anchor_allocation_revision(receipt JSONB) RETURNS BIGINT
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
      WHEN NOT (receipt ? 'allocationRevision') THEN 2
      WHEN jsonb_typeof(receipt->'allocationRevision') = 'string'
        AND receipt->>'allocationRevision' ~ '^[1-9][0-9]{0,18}$'
      THEN CASE WHEN (receipt->>'allocationRevision')::numeric BETWEEN 2 AND 9223372036854775807
        THEN (receipt->>'allocationRevision')::bigint ELSE NULL END
      ELSE NULL END
$$;

CREATE OR REPLACE FUNCTION guard_volume_resource_anchor() RETURNS trigger
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    pin runtime_volume_admission_guards%ROWTYPE;
    adoption_pending BOOLEAN;
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
        IF volume_anchor_migration_binding(OLD,NEW) THEN RETURN NEW; END IF;
        IF OLD.resource_anchor IS NOT NULL AND (
            ROW(NEW.resource_anchor, NEW.anchor_reservation, NEW.anchor_adoption) IS DISTINCT FROM ROW(OLD.resource_anchor, OLD.anchor_reservation, OLD.anchor_adoption)
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
        IF NEW.anchor_adoption IS NOT NULL OR NEW.anchored_removal_observation IS NOT NULL
            OR COALESCE(NEW.removal_intent->'anchored', 'false'::jsonb) <> 'false'::jsonb
            OR pin.resource_anchors_required AND (NEW.bound_instance IS NOT NULL OR NEW.removal_intent IS NOT NULL OR NEW.status IN ('active', 'deprovisioning', 'deleted')) THEN
            RAISE EXCEPTION 'anchored owner cannot bind or retire an unanchored workspace'
                USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
        END IF;
        RETURN NEW;
    END IF;
    adoption_pending := NEW.anchor_adoption IS NOT NULL AND pin.volume_anchor_migration IS NOT NULL
        AND NOT COALESCE((pin.volume_anchor_migration->>'complete')::boolean,FALSE)
        AND EXISTS(SELECT 1 FROM jsonb_array_elements(pin.volume_anchor_migration->'entries') e
            WHERE e->'adoption'=NEW.anchor_adoption AND e->'applied'->'volume'=NEW.bound_instance);
    IF (NOT pin.resource_anchors_required AND NOT adoption_pending) OR NOT NEW.checked_lifecycle OR
        NOT valid_registry_resource_anchor(NEW.resource_anchor, 'RESOURCE_ANCHOR_KIND_VOLUME', NEW.id,
            COALESCE(pin.prepared_backend_id,pin.volume_anchor_migration->>'backendId'),
            NEW.owner_kind, NEW.owner_id, NEW.agent_id, NEW.thread_id) THEN
        RAISE EXCEPTION 'invalid persistent volume anchor'
            USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
    END IF;
    IF TG_OP = 'INSERT' THEN
        RAISE EXCEPTION 'volume anchors require a checked existing reservation'
            USING ERRCODE = '55000', CONSTRAINT = 'resource_anchor_lifecycle';
    END IF;
    IF OLD.resource_anchor IS NULL THEN
        IF NEW.anchor_adoption IS NOT NULL OR (jsonb_typeof(NEW.anchor_reservation) = 'object'
            AND NEW.anchor_reservation - ARRAY['workloadId', 'preparationRevision', 'resourceRevision', 'allocationRevision'] = '{}'::jsonb
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
            OR NEW.lifecycle_revision IS DISTINCT FROM volume_anchor_allocation_revision(NEW.anchor_reservation)
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

CREATE OR REPLACE FUNCTION guard_workload_resource_anchors() RETURNS trigger
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    pin runtime_volume_admission_guards%ROWTYPE;
    resources JSONB;
    anchor JSONB;
    volume_anchors JSONB;
    anchor_revision BIGINT;
    old_revision BIGINT;
    preparation_changed BOOLEAN;
    revocation_changed BOOLEAN;
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
    IF (jsonb_typeof(resources) = 'object' AND resources - ARRAY['revision', 'workload', 'volumes', 'preparationRevocation', 'revocationObservation'] = '{}'::jsonb
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
        revocation_changed := ROW(resources->'preparationRevocation', resources->'revocationObservation')
            IS DISTINCT FROM ROW(OLD.resource_anchors->'preparationRevocation', OLD.resource_anchors->'revocationObservation');
        IF revocation_changed THEN
            IF anchor_revision <> old_revision + 1 OR NEW.preparation_revision <> OLD.preparation_revision + 1
                OR resources - ARRAY['revision', 'preparationRevocation', 'revocationObservation']
                    IS DISTINCT FROM OLD.resource_anchors - ARRAY['revision', 'preparationRevocation', 'revocationObservation']
                OR OLD.preparation_phase <> 'removing' OR OLD.prepared_binding IS NOT NULL OR NEW.prepared_binding IS NOT NULL
                OR OLD.prepared_removal_observation IS NOT NULL OR NEW.prepared_removal_observation IS NOT NULL
                OR NOT (
                    OLD.resource_anchors->'preparationRevocation' IS NULL
                        AND OLD.resource_anchors->'revocationObservation' IS NULL
                        AND resources->'preparationRevocation' IS NOT NULL AND resources->'revocationObservation' IS NULL
                        AND NEW.preparation_phase = 'removing'
                    OR OLD.resource_anchors->'preparationRevocation' IS NOT NULL
                        AND resources->'preparationRevocation' = OLD.resource_anchors->'preparationRevocation'
                        AND OLD.resource_anchors->'revocationObservation' IS NULL AND resources->'revocationObservation' IS NOT NULL
                        AND NEW.preparation_phase = 'removed') THEN
                RAISE EXCEPTION 'revocation requires separate immutable proof and cleanup transitions'
                    USING ERRCODE = '55000', CONSTRAINT = 'preparation_revocation_lifecycle';
            END IF;
            check_volume_records := TRUE;
        ELSIF preparation_changed THEN
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
    IF resources ? 'preparationRevocation' OR resources ? 'revocationObservation' THEN
        IF NOT valid_registry_preparation_revocation(resources->'preparationRevocation', resources)
            OR NEW.prepared_binding IS NOT NULL OR NEW.prepared_removal_observation IS NOT NULL
            OR NEW.preparation_phase NOT IN ('removing', 'removed')
            OR (NEW.preparation_phase = 'removed') IS DISTINCT FROM (resources->'revocationObservation' IS NOT NULL) THEN
            RAISE EXCEPTION 'invalid persisted unactivated preparation evidence'
                USING ERRCODE = '55000', CONSTRAINT = 'preparation_revocation_lifecycle';
        END IF;
        IF resources->'revocationObservation' IS NOT NULL AND NOT valid_registry_revocation_observation(
            resources->'revocationObservation', resources->'preparationRevocation', resources) THEN
            RAISE EXCEPTION 'revoked preparation requires a complete disjoint native inventory'
                USING ERRCODE = '55000', CONSTRAINT = 'preparation_revocation_lifecycle';
        END IF;
        -- Recheck checked volume state after the owner-row write, not just at
        -- the preceding Go read. Immutable history may outlive later retirement.
        IF resources->'revocationObservation' IS NOT NULL
            AND (TG_OP = 'INSERT' OR resources->'revocationObservation' IS DISTINCT FROM OLD.resource_anchors->'revocationObservation') THEN
            IF EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(resources->'revocationObservation'->'volumes', '[]'::jsonb)) b
                WHERE NOT EXISTS (SELECT 1 FROM volumes v
                    WHERE v.id::text = b->>'volumeKey' AND v.id = ANY(NEW.prepared_volume_ids)
                        AND v.checked_lifecycle AND v.status = 'active' AND v.removal_intent IS NULL
                        AND v.bound_instance = b AND v.instance_id = b->>'instanceId' AND v.resource_anchor = b->'anchor'
                        AND v.owner_kind = NEW.owner_kind AND v.owner_id = NEW.owner_id
                        AND ROW(v.runner_id, v.organization_id, v.thread_id, v.agent_id)
                            IS NOT DISTINCT FROM ROW(NEW.runner_id, NEW.organization_id, NEW.thread_id, NEW.agent_id)))
                OR EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(resources->'revocationObservation'->'absentVolumeIds', '[]'::jsonb)) missing
                    WHERE NOT EXISTS (SELECT 1 FROM volumes v
                        WHERE to_jsonb(v.id::text) = missing AND v.id = ANY(NEW.prepared_volume_ids)
                            AND v.checked_lifecycle AND v.status = 'provisioning' AND v.lifecycle_revision = volume_anchor_allocation_revision(v.anchor_reservation)
                            AND v.removal_intent IS NULL AND v.bound_instance IS NULL AND v.instance_id IS NULL
                            AND v.anchor_reservation->>'preparationRevision' = '1'
                            AND v.anchor_reservation->>'resourceRevision' = '1'
                            AND v.anchor_reservation->>'workloadId' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                            AND v.anchor_reservation->>'workloadId' <> '00000000-0000-0000-0000-000000000000'
                            AND EXISTS (SELECT 1 FROM jsonb_array_elements(volume_anchors) a
                                WHERE a->>'resourceId' = v.id::text AND a = v.resource_anchor)
                            AND v.owner_kind = NEW.owner_kind AND v.owner_id = NEW.owner_id
                            AND ROW(v.runner_id, v.organization_id, v.thread_id, v.agent_id)
                                IS NOT DISTINCT FROM ROW(NEW.runner_id, NEW.organization_id, NEW.thread_id, NEW.agent_id))) THEN
                RAISE EXCEPTION 'revoked preparation inventory changed checked workspace identity'
                    USING ERRCODE = '55000', CONSTRAINT = 'preparation_revocation_lifecycle';
            END IF;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
