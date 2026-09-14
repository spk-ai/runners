-- Install both sides atomically. Existing contradictions require an explicit
-- lifecycle audit, not inferred absence or automatic ownership repair.
LOCK TABLE workloads, volumes IN SHARE ROW EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM volumes v JOIN workloads w USING (owner_kind, owner_id)
        WHERE v.checked_lifecycle AND w.removal_confirmed_at IS NULL
        AND (v.status IN ('deprovisioning', 'deleted')
            OR ROW(v.organization_id, v.runner_id, v.thread_id, v.agent_id)
                IS DISTINCT FROM ROW(w.organization_id, w.runner_id, w.thread_id, w.agent_id))
    ) OR EXISTS (
        SELECT 1 FROM volumes v JOIN workloads w USING (owner_kind, owner_id)
        WHERE v.checked_lifecycle AND w.removal_confirmed_at IS NULL
        GROUP BY v.owner_kind, v.owner_id HAVING count(DISTINCT w.id) > 1
    ) OR EXISTS (
        SELECT 1 FROM volumes a JOIN volumes b USING (owner_kind, owner_id)
        WHERE a.checked_lifecycle AND b.checked_lifecycle
        AND ROW(a.organization_id, a.runner_id, a.thread_id, a.agent_id)
            IS DISTINCT FROM ROW(b.organization_id, b.runner_id, b.thread_id, b.agent_id)
    ) THEN
        RAISE EXCEPTION 'checked runtime owners require an admission audit before migration'
            USING ERRCODE = '55000', CONSTRAINT = 'runtime_volume_admission';
    END IF;
END;
$$;

CREATE TABLE runtime_volume_admission_guards (
    owner_kind TEXT NOT NULL CHECK (owner_kind IN ('agent_instance', 'sandbox')),
    owner_id UUID NOT NULL,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    PRIMARY KEY (owner_kind, owner_id)
);

-- A real write, not just an advisory/row lock: a stale repeatable-read or
-- serializable snapshot must fail rather than miss a newly committed workload.
CREATE FUNCTION lock_runtime_volume_owner(kind TEXT, owner UUID) RETURNS void
LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    INSERT INTO runtime_volume_admission_guards (owner_kind, owner_id)
        VALUES (kind, owner)
        ON CONFLICT (owner_kind, owner_id) DO UPDATE
        SET revision = runtime_volume_admission_guards.revision + 1;
END;
$$;

CREATE FUNCTION guard_volume_workload_admission() RETURNS trigger
LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    IF NOT NEW.checked_lifecycle THEN
        RETURN NEW;
    END IF;
    IF TG_OP = 'UPDATE' AND ROW(NEW.id, NEW.owner_kind, NEW.owner_id, NEW.organization_id,
        NEW.runner_id, NEW.thread_id, NEW.agent_id, NEW.status, NEW.checked_lifecycle,
        NEW.lifecycle_revision, NEW.bound_instance, NEW.removal_intent)
        IS NOT DISTINCT FROM ROW(OLD.id, OLD.owner_kind, OLD.owner_id, OLD.organization_id,
        OLD.runner_id, OLD.thread_id, OLD.agent_id, OLD.status, OLD.checked_lifecycle,
        OLD.lifecycle_revision, OLD.bound_instance, OLD.removal_intent) THEN
        RETURN NEW;
    END IF;
    PERFORM lock_runtime_volume_owner(NEW.owner_kind, NEW.owner_id);

    -- These reads intentionally follow the guard write in separate statements.
    -- Do not lock the other table's rows: its writers already hold their own
    -- row before entering the same owner guard.
    IF EXISTS (
        SELECT 1 FROM volumes v
        WHERE v.owner_kind = NEW.owner_kind AND v.owner_id = NEW.owner_id
        AND v.checked_lifecycle AND v.id <> NEW.id
        AND ROW(v.organization_id, v.runner_id, v.thread_id, v.agent_id)
            IS DISTINCT FROM ROW(NEW.organization_id, NEW.runner_id, NEW.thread_id, NEW.agent_id)
    ) OR EXISTS (
        SELECT 1 FROM workloads w
        WHERE w.owner_kind = NEW.owner_kind AND w.owner_id = NEW.owner_id
        AND w.removal_confirmed_at IS NULL
        AND ROW(w.organization_id, w.runner_id, w.thread_id, w.agent_id)
            IS DISTINCT FROM ROW(NEW.organization_id, NEW.runner_id, NEW.thread_id, NEW.agent_id)
    ) OR EXISTS (
        SELECT 1 FROM workloads w
        WHERE w.owner_kind = NEW.owner_kind AND w.owner_id = NEW.owner_id
        AND w.removal_confirmed_at IS NULL OFFSET 1
    ) THEN
        RAISE EXCEPTION 'checked volume conflicts with runtime owner identity or admission'
            USING ERRCODE = '55000', CONSTRAINT = 'runtime_volume_admission';
    END IF;

    IF (NEW.status IN ('deprovisioning', 'deleted')
        OR (TG_OP = 'UPDATE' AND NEW.status = 'provisioning' AND OLD.status IN ('failed', 'deleted')))
        AND EXISTS (SELECT 1 FROM workloads w
            WHERE w.owner_kind = NEW.owner_kind AND w.owner_id = NEW.owner_id
            AND w.removal_confirmed_at IS NULL) THEN
        RAISE EXCEPTION 'runtime owner still has an unconfirmed workload'
            USING ERRCODE = '55000', CONSTRAINT = 'runtime_volume_admission';
    END IF;
    IF NEW.status = 'failed' AND EXISTS (SELECT 1 FROM workloads w
        WHERE w.owner_kind = NEW.owner_kind AND w.owner_id = NEW.owner_id
        AND w.status IN ('starting', 'running')) THEN
        RAISE EXCEPTION 'cannot fail provisioning while its runtime owner is executing'
            USING ERRCODE = '55000', CONSTRAINT = 'runtime_volume_admission';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION guard_workload_volume_admission() RETURNS trigger
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    protected BOOLEAN;
    admitting BOOLEAN;
BEGIN
    IF TG_OP = 'UPDATE' AND ROW(NEW.id, NEW.owner_kind, NEW.owner_id, NEW.organization_id,
        NEW.runner_id, NEW.thread_id, NEW.agent_id, NEW.status, NEW.removal_confirmed_at)
        IS NOT DISTINCT FROM ROW(OLD.id, OLD.owner_kind, OLD.owner_id, OLD.organization_id,
        OLD.runner_id, OLD.thread_id, OLD.agent_id, OLD.status, OLD.removal_confirmed_at) THEN
        RETURN NEW;
    END IF;

    IF TG_OP = 'UPDATE' AND ROW(NEW.owner_kind, NEW.owner_id)
        IS DISTINCT FROM ROW(OLD.owner_kind, OLD.owner_id) THEN
        IF ROW(OLD.owner_kind, OLD.owner_id) < ROW(NEW.owner_kind, NEW.owner_id) THEN
            PERFORM lock_runtime_volume_owner(OLD.owner_kind, OLD.owner_id);
            PERFORM lock_runtime_volume_owner(NEW.owner_kind, NEW.owner_id);
        ELSE
            PERFORM lock_runtime_volume_owner(NEW.owner_kind, NEW.owner_id);
            PERFORM lock_runtime_volume_owner(OLD.owner_kind, OLD.owner_id);
        END IF;
    ELSIF TG_OP = 'DELETE' THEN
        PERFORM lock_runtime_volume_owner(OLD.owner_kind, OLD.owner_id);
    ELSE
        PERFORM lock_runtime_volume_owner(NEW.owner_kind, NEW.owner_id);
    END IF;

    IF TG_OP = 'DELETE' THEN
        IF OLD.removal_confirmed_at IS NULL AND EXISTS (SELECT 1 FROM volumes v
            WHERE v.owner_kind = OLD.owner_kind AND v.owner_id = OLD.owner_id AND v.checked_lifecycle) THEN
            RAISE EXCEPTION 'cannot discard an unconfirmed checked-owner workload'
                USING ERRCODE = '55000', CONSTRAINT = 'runtime_volume_admission';
        END IF;
        RETURN OLD;
    END IF;

    SELECT EXISTS (SELECT 1 FROM volumes v
        WHERE v.owner_kind = NEW.owner_kind AND v.owner_id = NEW.owner_id AND v.checked_lifecycle)
        INTO protected;
    IF TG_OP = 'UPDATE' THEN
        protected := protected OR EXISTS (SELECT 1 FROM volumes v
            WHERE v.owner_kind = OLD.owner_kind AND v.owner_id = OLD.owner_id AND v.checked_lifecycle);
        IF protected AND ROW(NEW.id, NEW.owner_kind, NEW.owner_id, NEW.organization_id,
            NEW.runner_id, NEW.thread_id, NEW.agent_id)
            IS DISTINCT FROM ROW(OLD.id, OLD.owner_kind, OLD.owner_id, OLD.organization_id,
            OLD.runner_id, OLD.thread_id, OLD.agent_id) THEN
            RAISE EXCEPTION 'checked-owner workload identity is immutable'
                USING ERRCODE = '55000', CONSTRAINT = 'runtime_volume_admission';
        END IF;
        IF protected AND OLD.removal_confirmed_at IS NOT NULL
            AND NEW.removal_confirmed_at IS DISTINCT FROM OLD.removal_confirmed_at THEN
            RAISE EXCEPTION 'cannot discard or change workload removal confirmation'
                USING ERRCODE = '55000', CONSTRAINT = 'runtime_volume_admission';
        END IF;
        admitting := NEW.status IN ('starting', 'running') AND OLD.status NOT IN ('starting', 'running');
    ELSE
        admitting := TRUE;
    END IF;

    IF protected AND admitting THEN
        IF EXISTS (SELECT 1 FROM volumes v
            WHERE v.owner_kind = NEW.owner_kind AND v.owner_id = NEW.owner_id AND v.checked_lifecycle
            AND (v.status NOT IN ('provisioning', 'active')
                OR ROW(v.organization_id, v.runner_id, v.thread_id, v.agent_id)
                    IS DISTINCT FROM ROW(NEW.organization_id, NEW.runner_id, NEW.thread_id, NEW.agent_id)))
            OR EXISTS (SELECT 1 FROM workloads w
                WHERE w.owner_kind = NEW.owner_kind AND w.owner_id = NEW.owner_id
                AND w.id <> NEW.id AND w.removal_confirmed_at IS NULL) THEN
            RAISE EXCEPTION 'checked-owner workload admission conflicts with volume or predecessor'
                USING ERRCODE = '55000', CONSTRAINT = 'runtime_volume_admission';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER volumes_workload_admission
    BEFORE INSERT OR UPDATE ON volumes
    FOR EACH ROW EXECUTE FUNCTION guard_volume_workload_admission();
CREATE TRIGGER workloads_volume_admission
    BEFORE INSERT OR UPDATE OR DELETE ON workloads
    FOR EACH ROW EXECUTE FUNCTION guard_workload_volume_admission();
