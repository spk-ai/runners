-- No inferred adoption or cleanup. Existing identities/history are unchanged.
LOCK TABLE workloads, volumes, runtime_volume_admission_guards IN SHARE ROW EXCLUSIVE MODE;

CREATE FUNCTION valid_registry_preparation_revocation(proof JSONB, resources JSONB) RETURNS BOOLEAN
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    anchors JSONB := COALESCE(resources->'volumes', '[]'::jsonb);
    claimed JSONB := COALESCE(proof->'volumeAnchors', '[]'::jsonb);
BEGIN
    IF (jsonb_typeof(proof) = 'object' AND octet_length(proof::text) <= 131072
        AND proof - ARRAY['workloadAnchor', 'volumeAnchors', 'instanceUid', 'selectedPodUid'] = '{}'::jsonb
        AND jsonb_typeof(proof->'instanceUid') = 'string'
        AND proof->>'instanceUid' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        AND proof->>'instanceUid' <> '00000000-0000-0000-0000-000000000000'
        AND proof->'workloadAnchor' = resources->'workload'
        AND jsonb_typeof(claimed) = 'array' AND jsonb_typeof(anchors) = 'array') IS NOT TRUE THEN
        RETURN FALSE;
    END IF;
    IF proof ? 'selectedPodUid' AND (jsonb_typeof(proof->'selectedPodUid') = 'string'
        AND proof->>'selectedPodUid' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        AND proof->>'selectedPodUid' <> '00000000-0000-0000-0000-000000000000') IS NOT TRUE THEN
        RETURN FALSE;
    END IF;
    RETURN jsonb_array_length(claimed) <= 64 AND jsonb_array_length(claimed) = jsonb_array_length(anchors)
        AND (SELECT count(DISTINCT a->>'resourceId') FROM jsonb_array_elements(claimed) a) = jsonb_array_length(anchors)
        AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(claimed) a
            WHERE NOT EXISTS (SELECT 1 FROM jsonb_array_elements(anchors) expected WHERE expected = a));
END;
$$;

CREATE FUNCTION valid_registry_revocation_observation(observation JSONB, proof JSONB, resources JSONB) RETURNS BOOLEAN
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    found JSONB := COALESCE(observation->'volumes', '[]'::jsonb);
    missing JSONB := COALESCE(observation->'absentVolumeIds', '[]'::jsonb);
    anchors JSONB := COALESCE(resources->'volumes', '[]'::jsonb);
    item JSONB;
    anchor JSONB;
BEGIN
    IF (jsonb_typeof(observation) = 'object' AND octet_length(observation::text) <= 262144
        AND observation - ARRAY['state', 'revocation', 'volumes', 'absentVolumeIds'] = '{}'::jsonb
        AND observation->>'state' = 'REVOKED_PREPARATION_STATE_POD_ABSENT'
        AND observation->'revocation' = proof
        AND jsonb_typeof(found) = 'array' AND jsonb_typeof(missing) = 'array'
        AND jsonb_typeof(anchors) = 'array') IS NOT TRUE THEN
        RETURN FALSE;
    END IF;
    IF jsonb_array_length(found) + jsonb_array_length(missing) <> jsonb_array_length(anchors)
        OR jsonb_array_length(anchors) > 64
        OR (SELECT count(DISTINCT b->>'instanceId') FROM jsonb_array_elements(found) b) <> jsonb_array_length(found)
        OR (SELECT count(DISTINCT b->>'instanceUid') FROM jsonb_array_elements(found) b) <> jsonb_array_length(found)
        OR (SELECT count(DISTINCT id) FROM (
            SELECT b->'volumeKey' id FROM jsonb_array_elements(found) b
            UNION ALL SELECT id FROM jsonb_array_elements(missing) id) ids) <> jsonb_array_length(anchors) THEN
        RETURN FALSE;
    END IF;
    FOR item IN SELECT value FROM jsonb_array_elements(found) LOOP
        SELECT value INTO anchor FROM jsonb_array_elements(anchors) WHERE value->>'resourceId' = item->>'volumeKey';
        IF (jsonb_typeof(item) = 'object'
            AND item - ARRAY['instanceId', 'volumeKey', 'instanceUid', 'identityLabels', 'backendId', 'anchor'] = '{}'::jsonb
            AND anchor IS NOT NULL AND item->'anchor' = anchor AND item->'identityLabels' = anchor->'identityLabels'
            AND item->'volumeKey' = anchor->'resourceId' AND item->'backendId' = anchor->'backendId'
            AND jsonb_typeof(item->'instanceId') = 'string' AND octet_length(item->>'instanceId') BETWEEN 1 AND 253
            AND item->>'instanceId' ~ '^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$'
            AND jsonb_typeof(item->'instanceUid') = 'string'
            AND item->>'instanceUid' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            AND item->>'instanceUid' <> '00000000-0000-0000-0000-000000000000') IS NOT TRUE THEN
            RETURN FALSE;
        END IF;
    END LOOP;
    RETURN NOT EXISTS (SELECT 1 FROM jsonb_array_elements(missing) id
        WHERE NOT EXISTS (SELECT 1 FROM jsonb_array_elements(anchors) a WHERE a->'resourceId' = id));
END;
$$;

CREATE OR REPLACE FUNCTION guard_prepared_workload() RETURNS trigger
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    pin runtime_volume_admission_guards%ROWTYPE;
    changed BOOLEAN;
    allowed BOOLEAN;
    bound_volumes JSONB;
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.preparation_phase IS NOT NULL AND OLD.removal_confirmed_at IS NULL THEN
            RAISE EXCEPTION 'unconfirmed preparation must be retained'
                USING ERRCODE = '55000', CONSTRAINT = 'prepared_workload_lifecycle';
        END IF;
        RETURN OLD;
    END IF;
    IF TG_OP = 'UPDATE' AND OLD.preparation_phase IS NOT NULL AND
        (NEW.preparation_phase IS NULL OR ROW(NEW.id, NEW.owner_kind, NEW.owner_id, NEW.runner_id,
            NEW.organization_id, NEW.thread_id, NEW.agent_id, NEW.prepared_backend_id, NEW.prepared_volume_ids)
            IS DISTINCT FROM ROW(OLD.id, OLD.owner_kind, OLD.owner_id, OLD.runner_id,
            OLD.organization_id, OLD.thread_id, OLD.agent_id, OLD.prepared_backend_id, OLD.prepared_volume_ids)) THEN
        RAISE EXCEPTION 'prepared workload identity is immutable'
            USING ERRCODE = '55000', CONSTRAINT = 'prepared_workload_lifecycle';
    END IF;

    IF TG_OP = 'UPDATE' AND ROW(NEW.owner_kind, NEW.owner_id) IS DISTINCT FROM ROW(OLD.owner_kind, OLD.owner_id) THEN
        IF ROW(OLD.owner_kind, OLD.owner_id) < ROW(NEW.owner_kind, NEW.owner_id) THEN
            PERFORM lock_runtime_volume_owner(OLD.owner_kind, OLD.owner_id);
            PERFORM lock_runtime_volume_owner(NEW.owner_kind, NEW.owner_id);
        ELSE
            PERFORM lock_runtime_volume_owner(NEW.owner_kind, NEW.owner_id);
            PERFORM lock_runtime_volume_owner(OLD.owner_kind, OLD.owner_id);
        END IF;
        IF EXISTS (SELECT 1 FROM runtime_volume_admission_guards WHERE owner_kind = OLD.owner_kind
            AND owner_id = OLD.owner_id AND prepared_backend_id IS NOT NULL) THEN
            RAISE EXCEPTION 'cannot move history out of a prepared owner'
                USING ERRCODE = '55000', CONSTRAINT = 'prepared_workload_lifecycle';
        END IF;
    ELSE
        -- A real owner-row write ensures stale RR/serializable snapshots abort.
        PERFORM lock_runtime_volume_owner(NEW.owner_kind, NEW.owner_id);
    END IF;
    SELECT * INTO pin FROM runtime_volume_admission_guards
        WHERE owner_kind = NEW.owner_kind AND owner_id = NEW.owner_id;
    IF NEW.preparation_phase IS NULL THEN
        IF pin.prepared_backend_id IS NOT NULL AND (TG_OP = 'INSERT'
            OR NEW.status IN ('starting', 'running') OR NEW.removal_confirmed_at IS NULL) THEN
            RAISE EXCEPTION 'legacy admission is disabled for prepared owners'
                USING ERRCODE = '55000', CONSTRAINT = 'prepared_workload_lifecycle';
        END IF;
        RETURN NEW;
    END IF;

    IF TG_OP = 'INSERT' THEN
        IF NEW.preparation_phase <> 'reserved' OR NEW.preparation_revision <> 1 OR NEW.status <> 'starting'
            OR NEW.prepared_binding IS NOT NULL OR NEW.removal_confirmed_at IS NOT NULL
            OR NEW.prepared_removal_observation IS NOT NULL THEN
            RAISE EXCEPTION 'prepared workloads must start as unused reservations'
                USING ERRCODE = '55000', CONSTRAINT = 'prepared_workload_lifecycle';
        END IF;
        IF EXISTS (SELECT 1 FROM workloads w WHERE w.owner_kind = NEW.owner_kind AND w.owner_id = NEW.owner_id
            AND w.id <> NEW.id AND w.removal_confirmed_at IS NULL) OR EXISTS (SELECT 1 FROM volumes v
            WHERE v.owner_kind = NEW.owner_kind AND v.owner_id = NEW.owner_id
            AND (NOT v.checked_lifecycle OR v.status NOT IN ('provisioning', 'active')
                OR ROW(v.runner_id, v.organization_id, v.thread_id, v.agent_id)
                    IS DISTINCT FROM ROW(NEW.runner_id, NEW.organization_id, NEW.thread_id, NEW.agent_id)
                OR v.bound_instance IS NOT NULL AND v.bound_instance->>'backendId' IS DISTINCT FROM NEW.prepared_backend_id)) THEN
            RAISE EXCEPTION 'prepared owner requires reconciled volumes and confirmed predecessors'
                USING ERRCODE = '55000', CONSTRAINT = 'prepared_workload_lifecycle';
        END IF;
        IF cardinality(NEW.prepared_volume_ids) <> (SELECT count(DISTINCT id) FROM unnest(NEW.prepared_volume_ids) id)
            OR EXISTS (SELECT 1 FROM unnest(NEW.prepared_volume_ids) AS requested(id)
                WHERE NOT EXISTS (SELECT 1 FROM volumes v WHERE v.id = requested.id AND v.owner_kind = NEW.owner_kind AND v.owner_id = NEW.owner_id)) THEN
            RAISE EXCEPTION 'prepared volume set is incomplete or has another owner'
                USING ERRCODE = '55000', CONSTRAINT = 'prepared_workload_lifecycle';
        END IF;
        IF pin.prepared_backend_id IS NULL THEN
            UPDATE runtime_volume_admission_guards SET prepared_backend_id = NEW.prepared_backend_id,
                prepared_runner_id = NEW.runner_id, prepared_organization_id = NEW.organization_id,
                prepared_thread_id = NEW.thread_id, prepared_agent_id = NEW.agent_id
                WHERE owner_kind = NEW.owner_kind AND owner_id = NEW.owner_id;
        END IF;
    ELSE
        IF OLD.preparation_phase IS NULL THEN
            RAISE EXCEPTION 'legacy workloads cannot be adopted as prepared'
                USING ERRCODE = '55000', CONSTRAINT = 'prepared_workload_lifecycle';
        END IF;
        IF OLD.prepared_binding IS NOT NULL AND NEW.prepared_binding IS DISTINCT FROM OLD.prepared_binding
            OR OLD.prepared_removal_observation IS NOT NULL AND NEW.prepared_removal_observation IS DISTINCT FROM OLD.prepared_removal_observation
            OR OLD.removal_confirmed_at IS NOT NULL AND NEW.removal_confirmed_at IS DISTINCT FROM OLD.removal_confirmed_at THEN
            RAISE EXCEPTION 'prepared binding and removal evidence are immutable'
                USING ERRCODE = '55000', CONSTRAINT = 'prepared_workload_lifecycle';
        END IF;
        changed := ROW(NEW.preparation_phase, NEW.prepared_binding, NEW.prepared_removal_observation,
            NEW.resource_anchors->'preparationRevocation', NEW.resource_anchors->'revocationObservation')
            IS DISTINCT FROM ROW(OLD.preparation_phase, OLD.prepared_binding, OLD.prepared_removal_observation,
            OLD.resource_anchors->'preparationRevocation', OLD.resource_anchors->'revocationObservation');
        IF changed THEN
            allowed := CASE OLD.preparation_phase
                WHEN 'reserved' THEN NEW.preparation_phase IN ('preparing', 'removed')
                WHEN 'preparing' THEN NEW.preparation_phase IN ('bound', 'removing')
                WHEN 'bound' THEN NEW.preparation_phase IN ('activating', 'removing')
                WHEN 'activating' THEN NEW.preparation_phase IN ('active', 'removing')
                WHEN 'active' THEN NEW.preparation_phase = 'removing'
                WHEN 'removing' THEN NEW.preparation_phase = 'removed' OR
                    (NEW.preparation_phase = 'removing' AND OLD.prepared_binding IS NULL AND NEW.prepared_binding IS NOT NULL) OR
                    (NEW.preparation_phase = 'removing' AND OLD.resource_anchors->'preparationRevocation' IS NULL
                        AND NEW.resource_anchors->'preparationRevocation' IS NOT NULL)
                ELSE FALSE END;
            IF NOT allowed OR NEW.preparation_revision <> OLD.preparation_revision + 1
                OR NEW.preparation_phase IN ('preparing', 'activating') AND NEW.status <> 'starting'
                OR OLD.preparation_phase = 'reserved' AND NEW.preparation_phase = 'removed' AND NEW.prepared_binding IS NOT NULL
                OR OLD.preparation_phase = 'removing' AND NEW.preparation_phase = 'removed' AND OLD.prepared_binding IS NULL
                    AND (OLD.resource_anchors->'preparationRevocation' IS NULL
                        OR NEW.resource_anchors->'revocationObservation' IS NULL)
                OR OLD.prepared_binding IS NULL AND NEW.prepared_binding IS NOT NULL
                    AND NOT (OLD.preparation_phase = 'preparing' AND NEW.preparation_phase = 'bound'
                        OR OLD.preparation_phase = 'removing' AND NEW.preparation_phase = 'removing') THEN
                RAISE EXCEPTION 'invalid preparation transition or revision'
                    USING ERRCODE = '55000', CONSTRAINT = 'prepared_workload_lifecycle';
            END IF;
        ELSIF NEW.preparation_revision <> OLD.preparation_revision OR NEW.removal_confirmed_at IS DISTINCT FROM OLD.removal_confirmed_at THEN
            RAISE EXCEPTION 'legacy updates cannot change preparation evidence'
                USING ERRCODE = '55000', CONSTRAINT = 'prepared_workload_lifecycle';
        END IF;
    END IF;
    IF pin.prepared_backend_id IS NOT NULL AND ROW(NEW.prepared_backend_id, NEW.runner_id, NEW.organization_id, NEW.thread_id, NEW.agent_id)
        IS DISTINCT FROM ROW(pin.prepared_backend_id, pin.prepared_runner_id, pin.prepared_organization_id, pin.prepared_thread_id, pin.prepared_agent_id) THEN
        RAISE EXCEPTION 'prepared owner backend or identity changed'
            USING ERRCODE = '55000', CONSTRAINT = 'prepared_workload_lifecycle';
    END IF;
    IF NEW.status = 'running' AND NEW.preparation_phase NOT IN ('activating', 'active', 'removing') THEN
        RAISE EXCEPTION 'running report precedes activation authorization'
            USING ERRCODE = '55000', CONSTRAINT = 'prepared_workload_lifecycle';
    END IF;
    IF NEW.prepared_binding IS NOT NULL THEN
        bound_volumes := COALESCE(NEW.prepared_binding->'volumes', '[]'::jsonb);
        IF (NEW.prepared_binding->>'workloadId' = NEW.id::text
            AND NEW.prepared_binding->>'backendId' = NEW.prepared_backend_id
            AND NEW.prepared_binding->>'instanceUid' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            AND NEW.prepared_binding->>'instanceUid' <> '00000000-0000-0000-0000-000000000000'
            AND jsonb_typeof(bound_volumes) = 'array') IS NOT TRUE THEN
            RAISE EXCEPTION 'invalid prepared workload binding'
                USING ERRCODE = '55000', CONSTRAINT = 'prepared_workload_lifecycle';
        END IF;
        IF TG_OP = 'INSERT' OR OLD.prepared_binding IS DISTINCT FROM NEW.prepared_binding
            OR OLD.preparation_phase IS DISTINCT FROM NEW.preparation_phase AND NEW.preparation_phase IN ('activating', 'active') THEN
            IF jsonb_array_length(bound_volumes) <> cardinality(NEW.prepared_volume_ids)
                OR (SELECT count(DISTINCT v->>'volumeKey') FROM jsonb_array_elements(bound_volumes) v) <> cardinality(NEW.prepared_volume_ids)
                OR (SELECT count(DISTINCT v->>'instanceId') FROM jsonb_array_elements(bound_volumes) v) <> cardinality(NEW.prepared_volume_ids)
                OR EXISTS (SELECT 1 FROM jsonb_array_elements(bound_volumes) b WHERE NOT EXISTS (
                    SELECT 1 FROM volumes v WHERE v.id = ANY(NEW.prepared_volume_ids) AND v.id::text = b->>'volumeKey'
                    AND v.checked_lifecycle AND v.status = 'active' AND v.removal_intent IS NULL AND v.bound_instance = b
                    AND v.owner_kind = NEW.owner_kind AND v.owner_id = NEW.owner_id
                    AND b->>'backendId' = NEW.prepared_backend_id
                    AND ROW(v.runner_id, v.organization_id, v.thread_id, v.agent_id)
                        IS NOT DISTINCT FROM ROW(NEW.runner_id, NEW.organization_id, NEW.thread_id, NEW.agent_id))) THEN
                RAISE EXCEPTION 'prepared binding does not match checked volume set'
                    USING ERRCODE = '55000', CONSTRAINT = 'prepared_workload_lifecycle';
            END IF;
        END IF;
    END IF;
    IF NEW.preparation_phase = 'removed' AND NEW.prepared_binding IS NOT NULL THEN
        IF NEW.prepared_removal_observation IS DISTINCT FROM jsonb_build_object(
            'state', 'PREPARED_WORKLOAD_REMOVAL_STATE_ABSENT', 'binding', NEW.prepared_binding) THEN
            RAISE EXCEPTION 'exact-binding absence observation required'
                USING ERRCODE = '55000', CONSTRAINT = 'prepared_workload_lifecycle';
        END IF;
    ELSIF NEW.prepared_removal_observation IS NOT NULL THEN
        RAISE EXCEPTION 'unexpected preparation removal observation'
            USING ERRCODE = '55000', CONSTRAINT = 'prepared_workload_lifecycle';
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
                            AND v.checked_lifecycle AND v.status = 'provisioning' AND v.lifecycle_revision = 2
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
