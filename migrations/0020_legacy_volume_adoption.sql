-- This installs an adoption precondition, not an audit or an adoption backfill.
-- Existing writers and in-flight backend operations must be drained separately.
LOCK TABLE workloads, volumes IN SHARE ROW EXCLUSIVE MODE;

CREATE FUNCTION guard_legacy_volume_adoption() RETURNS trigger
LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    IF OLD.status NOT IN ('provisioning', 'active') OR NEW.status <> 'active'
        OR OLD.instance_id IS NULL OR length(OLD.instance_id) = 0
        OR NEW.instance_id IS DISTINCT FROM OLD.instance_id
        OR NEW.bound_instance IS NULL
        OR NEW.bound_instance->>'instanceId' IS DISTINCT FROM OLD.instance_id
        OR NEW.removal_intent IS NOT NULL
        OR NEW.size_gb IS DISTINCT FROM OLD.size_gb
        OR NEW.removed_at IS DISTINCT FROM OLD.removed_at
        OR NEW.last_metering_sampled_at IS DISTINCT FROM OLD.last_metering_sampled_at THEN
        RAISE EXCEPTION 'legacy volume generation requires explicit reconciliation before adoption'
            USING ERRCODE = '55000', CONSTRAINT = 'volumes_legacy_adoption';
    END IF;

    -- Share the workload writer's real guard-row write, including protection
    -- against stale repeatable-read/serializable snapshots. Read only after it.
    PERFORM lock_runtime_volume_owner(NEW.owner_kind, NEW.owner_id);
    IF EXISTS (SELECT 1 FROM workloads w
        WHERE w.owner_kind = NEW.owner_kind AND w.owner_id = NEW.owner_id
        AND w.removal_confirmed_at IS NULL) THEN
        RAISE EXCEPTION 'legacy volume adoption requires confirmed predecessor removal'
            USING ERRCODE = '55000', CONSTRAINT = 'runtime_volume_admission';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER volumes_legacy_adoption
    BEFORE UPDATE ON volumes
    FOR EACH ROW WHEN (NOT OLD.checked_lifecycle AND NEW.checked_lifecycle)
    EXECUTE FUNCTION guard_legacy_volume_adoption();
