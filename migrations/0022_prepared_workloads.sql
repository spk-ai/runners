-- No adoption or inferred confirmation: all existing records stay legacy.
LOCK TABLE workloads, volumes, runtime_volume_admission_guards IN SHARE ROW EXCLUSIVE MODE;

ALTER TABLE workloads
    ADD COLUMN preparation_phase TEXT,
    ADD COLUMN preparation_revision BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN prepared_backend_id TEXT,
    ADD COLUMN prepared_volume_ids UUID[],
    ADD COLUMN prepared_binding JSONB,
    ADD COLUMN prepared_removal_observation JSONB;

ALTER TABLE workloads ADD CONSTRAINT workloads_preparation_shape CHECK ((
    CASE WHEN preparation_phase IS NULL THEN
        preparation_revision = 0 AND prepared_backend_id IS NULL AND prepared_volume_ids IS NULL
        AND prepared_binding IS NULL AND prepared_removal_observation IS NULL
    ELSE
        preparation_phase IN ('reserved', 'preparing', 'bound', 'activating', 'active', 'removing', 'removed')
        AND preparation_revision > 0 AND prepared_backend_id IS NOT NULL
        AND octet_length(prepared_backend_id) BETWEEN 1 AND 512
        AND prepared_backend_id = btrim(prepared_backend_id, E' \t\n\r\f\013')
        AND prepared_volume_ids IS NOT NULL AND cardinality(prepared_volume_ids) <= 64
        AND array_position(prepared_volume_ids, NULL) IS NULL
        AND ((prepared_binding IS NULL AND instance_id IS NULL
                AND preparation_phase IN ('reserved', 'preparing', 'removing', 'removed'))
            OR (prepared_binding IS NOT NULL AND jsonb_typeof(prepared_binding) = 'object'
                AND preparation_phase IN ('bound', 'activating', 'active', 'removing', 'removed')
                AND instance_id = id::text))
        AND ((preparation_phase = 'removed' AND removal_confirmed_at IS NOT NULL AND status IN ('stopped', 'failed'))
            OR (preparation_phase <> 'removed' AND removal_confirmed_at IS NULL))
        AND (prepared_removal_observation IS NULL OR preparation_phase = 'removed')
    END
) IS TRUE);

-- A pin survives workload-history GC. It is never silently cleared, even for
-- owners without volumes. Backend migration needs a separate fenced protocol.
ALTER TABLE runtime_volume_admission_guards
    ADD COLUMN prepared_backend_id TEXT,
    ADD COLUMN prepared_runner_id UUID,
    ADD COLUMN prepared_organization_id UUID,
    ADD COLUMN prepared_thread_id UUID,
    ADD COLUMN prepared_agent_id UUID;

ALTER TABLE runtime_volume_admission_guards ADD CONSTRAINT runtime_prepared_pin CHECK ((
    CASE WHEN prepared_backend_id IS NULL THEN
        prepared_runner_id IS NULL AND prepared_organization_id IS NULL
        AND prepared_thread_id IS NULL AND prepared_agent_id IS NULL
    ELSE
        octet_length(prepared_backend_id) BETWEEN 1 AND 512
        AND prepared_backend_id = btrim(prepared_backend_id, E' \t\n\r\f\013')
        AND prepared_runner_id IS NOT NULL AND prepared_organization_id IS NOT NULL
        AND (owner_kind = 'sandbox' OR (prepared_thread_id IS NOT NULL AND prepared_agent_id IS NOT NULL))
    END
) IS TRUE);

CREATE FUNCTION guard_prepared_owner_pin() RETURNS trigger
LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    IF OLD.prepared_backend_id IS NOT NULL THEN
        IF TG_OP = 'DELETE' OR ROW(NEW.owner_kind, NEW.owner_id, NEW.prepared_backend_id,
            NEW.prepared_runner_id, NEW.prepared_organization_id, NEW.prepared_thread_id, NEW.prepared_agent_id)
            IS DISTINCT FROM ROW(OLD.owner_kind, OLD.owner_id, OLD.prepared_backend_id,
            OLD.prepared_runner_id, OLD.prepared_organization_id, OLD.prepared_thread_id, OLD.prepared_agent_id) THEN
            RAISE EXCEPTION 'prepared owner pin is immutable'
                USING ERRCODE = '55000', CONSTRAINT = 'prepared_workload_lifecycle';
        END IF;
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER runtime_prepared_pin BEFORE UPDATE OR DELETE ON runtime_volume_admission_guards
    FOR EACH ROW EXECUTE FUNCTION guard_prepared_owner_pin();

CREATE FUNCTION guard_prepared_workload() RETURNS trigger
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
        changed := ROW(NEW.preparation_phase, NEW.prepared_binding, NEW.prepared_removal_observation)
            IS DISTINCT FROM ROW(OLD.preparation_phase, OLD.prepared_binding, OLD.prepared_removal_observation);
        IF changed THEN
            allowed := CASE OLD.preparation_phase
                WHEN 'reserved' THEN NEW.preparation_phase IN ('preparing', 'removed')
                WHEN 'preparing' THEN NEW.preparation_phase IN ('bound', 'removing')
                WHEN 'bound' THEN NEW.preparation_phase IN ('activating', 'removing')
                WHEN 'activating' THEN NEW.preparation_phase IN ('active', 'removing')
                WHEN 'active' THEN NEW.preparation_phase = 'removing'
                WHEN 'removing' THEN NEW.preparation_phase = 'removed' OR
                    (NEW.preparation_phase = 'removing' AND OLD.prepared_binding IS NULL AND NEW.prepared_binding IS NOT NULL)
                ELSE FALSE END;
            IF NOT allowed OR NEW.preparation_revision <> OLD.preparation_revision + 1
                OR NEW.preparation_phase IN ('preparing', 'activating') AND NEW.status <> 'starting'
                OR OLD.preparation_phase = 'reserved' AND NEW.preparation_phase = 'removed' AND NEW.prepared_binding IS NOT NULL
                OR OLD.preparation_phase = 'removing' AND NEW.preparation_phase = 'removed' AND OLD.prepared_binding IS NULL
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

CREATE TRIGGER workloads_preparation BEFORE INSERT OR UPDATE OR DELETE ON workloads
    FOR EACH ROW EXECUTE FUNCTION guard_prepared_workload();

CREATE FUNCTION guard_prepared_owner_volume() RETURNS trigger
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    pin runtime_volume_admission_guards%ROWTYPE;
BEGIN
    IF TG_OP = 'DELETE' THEN
        PERFORM lock_runtime_volume_owner(OLD.owner_kind, OLD.owner_id);
        IF EXISTS (SELECT 1 FROM runtime_volume_admission_guards WHERE owner_kind = OLD.owner_kind
            AND owner_id = OLD.owner_id AND prepared_backend_id IS NOT NULL) AND
            (OLD.status <> 'deleted' OR OLD.removal_intent->>'confirmedAt' IS NULL
                OR EXISTS (SELECT 1 FROM workloads w WHERE w.owner_kind = OLD.owner_kind
                    AND w.owner_id = OLD.owner_id AND w.removal_confirmed_at IS NULL)) THEN
            RAISE EXCEPTION 'cannot discard unconfirmed prepared-owner volume'
                USING ERRCODE = '55000', CONSTRAINT = 'prepared_workload_lifecycle';
        END IF;
        RETURN OLD;
    END IF;
    -- Existing checked guards reject owner mutation; a legacy row cannot leave
    -- an opted-in owner because opt-in itself rejects all unchecked history.
    PERFORM lock_runtime_volume_owner(NEW.owner_kind, NEW.owner_id);
    SELECT * INTO pin FROM runtime_volume_admission_guards WHERE owner_kind = NEW.owner_kind AND owner_id = NEW.owner_id;
    IF pin.prepared_backend_id IS NULL THEN RETURN NEW; END IF;
    IF NOT NEW.checked_lifecycle OR ROW(NEW.runner_id, NEW.organization_id, NEW.thread_id, NEW.agent_id)
        IS DISTINCT FROM ROW(pin.prepared_runner_id, pin.prepared_organization_id, pin.prepared_thread_id, pin.prepared_agent_id)
        OR NEW.bound_instance IS NOT NULL AND NEW.bound_instance->>'backendId' IS DISTINCT FROM pin.prepared_backend_id
        OR NEW.status = 'failed' AND EXISTS (SELECT 1 FROM workloads w
            WHERE w.owner_kind = NEW.owner_kind AND w.owner_id = NEW.owner_id AND w.preparation_phase IS NOT NULL
            AND w.removal_confirmed_at IS NULL AND NEW.id = ANY(w.prepared_volume_ids)) THEN
        RAISE EXCEPTION 'volume conflicts with pinned prepared owner'
            USING ERRCODE = '55000', CONSTRAINT = 'prepared_workload_lifecycle';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER volumes_prepared_owner BEFORE INSERT OR UPDATE OR DELETE ON volumes
    FOR EACH ROW EXECUTE FUNCTION guard_prepared_owner_volume();
