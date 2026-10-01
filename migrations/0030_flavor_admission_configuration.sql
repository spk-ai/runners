-- Idle policies share the existing runner lifecycle. Busy/retained workloads
-- still block deletion through their native guards; no history is reset.
ALTER TABLE workload_flavor_admission DROP CONSTRAINT workload_flavor_admission_runner_id_fkey;
ALTER TABLE workload_flavor_admission ADD CONSTRAINT workload_flavor_admission_runner_id_fkey
    FOREIGN KEY (runner_id) REFERENCES runners(id) ON DELETE CASCADE;

-- Policy and workload insertion contend on a durable per-flavor row. This
-- produces a serialization failure under a stale fixed snapshot instead of
-- overlooking a policy or workload that committed before the table lock.
LOCK TABLE workloads, workload_flavor_admission IN SHARE ROW EXCLUSIVE MODE;
CREATE TABLE workload_flavor_policy_guards (
    runner_id UUID NOT NULL REFERENCES runners(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    flavor TEXT NOT NULL,
    revision BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (runner_id, flavor)
);
INSERT INTO workload_flavor_policy_guards (runner_id, flavor)
    SELECT runner_id, flavor FROM workloads
    UNION SELECT runner_id, flavor FROM workload_flavor_admission;
CREATE TRIGGER flavor_policy_guard_writer BEFORE INSERT OR UPDATE OR DELETE
    ON workload_flavor_policy_guards FOR EACH ROW EXECUTE FUNCTION guard_flavor_reservation_writer();

CREATE OR REPLACE FUNCTION guard_flavor_admission_configuration() RETURNS trigger
LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    -- Only the nested workload trigger maintains counters. Policy writes are
    -- serialized with workload insertion, including the first opt-in policy.
    IF TG_OP = 'UPDATE' AND pg_trigger_depth() > 1 AND
        ROW(NEW.runner_id, NEW.flavor, NEW.capacity) IS NOT DISTINCT FROM
        ROW(OLD.runner_id, OLD.flavor, OLD.capacity) THEN
        RETURN NEW;
    END IF;
    -- A table lock does not refresh an existing REPEATABLE READ snapshot.
    -- Workload readers share the guard, so installing a new policy must use a
    -- fresh READ COMMITTED query to establish that all workloads are drained.
    -- This applies only to operator policy installation, never workload writes.
    IF TG_OP = 'INSERT' AND current_setting('transaction_isolation') NOT IN ('read committed', 'read uncommitted') THEN
        RAISE EXCEPTION 'admission policy installation requires read committed isolation'
            USING ERRCODE = '55000', CONSTRAINT = 'workload_flavor_admission';
    END IF;
    LOCK TABLE workloads IN SHARE ROW EXCLUSIVE MODE;
    IF TG_OP = 'DELETE' THEN
        -- Cascaded runner deletion also removes its guard. There is no new
        -- admission once that runner row is locked for deletion.
        IF pg_trigger_depth() = 1 THEN
            INSERT INTO workload_flavor_policy_guards (runner_id, flavor, revision) VALUES (OLD.runner_id, OLD.flavor, 1)
                ON CONFLICT (runner_id, flavor) DO UPDATE SET revision = workload_flavor_policy_guards.revision + 1;
        END IF;
    ELSE
        INSERT INTO workload_flavor_policy_guards (runner_id, flavor, revision) VALUES (NEW.runner_id, NEW.flavor, 1)
            ON CONFLICT (runner_id, flavor) DO UPDATE SET revision = workload_flavor_policy_guards.revision + 1;
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.occupied <> 0 OR EXISTS (SELECT 1 FROM workloads
            WHERE runner_id = NEW.runner_id AND flavor = NEW.flavor AND removal_confirmed_at IS NULL) THEN
            RAISE EXCEPTION 'drain flavor before enabling admission'
                USING ERRCODE = '55000', CONSTRAINT = 'workload_flavor_admission';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        IF OLD.occupied <> 0 THEN
            RAISE EXCEPTION 'unconfirmed workloads retain admission policy'
                USING ERRCODE = '55000', CONSTRAINT = 'workload_flavor_admission';
        END IF;
        RETURN OLD;
    END IF;
    IF ROW(NEW.runner_id, NEW.flavor, NEW.occupied) IS DISTINCT FROM
        ROW(OLD.runner_id, OLD.flavor, OLD.occupied) THEN
        RAISE EXCEPTION 'admission identity and counters are immutable to policy writers'
            USING ERRCODE = '55000', CONSTRAINT = 'workload_flavor_admission';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION guard_workload_flavor_admission() RETURNS trigger
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    policy workload_flavor_admission%ROWTYPE;
    reserved BOOLEAN;
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO workload_flavor_policy_guards (runner_id, flavor) VALUES (NEW.runner_id, NEW.flavor)
            ON CONFLICT (runner_id, flavor) DO NOTHING;
        -- Shared readers preserve unrelated-owner concurrency. Configuration
        -- changes update this row; fixed snapshots then serialize or abort.
        PERFORM 1 FROM workload_flavor_policy_guards
            WHERE runner_id = NEW.runner_id AND flavor = NEW.flavor FOR SHARE;
        SELECT * INTO policy FROM workload_flavor_admission
            WHERE runner_id = NEW.runner_id AND flavor = NEW.flavor FOR UPDATE;
        IF FOUND THEN
            IF NEW.preparation_phase IS DISTINCT FROM 'reserved' OR NEW.resource_anchors IS NULL OR
                NEW.removal_confirmed_at IS NOT NULL THEN
                RAISE EXCEPTION 'limited flavors require anchored preparation'
                    USING ERRCODE = '55000', CONSTRAINT = 'workload_flavor_admission';
            END IF;
            UPDATE workload_flavor_admission SET occupied = occupied + 1
                WHERE runner_id = NEW.runner_id AND flavor = NEW.flavor AND occupied < capacity;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'flavor capacity exhausted'
                    USING ERRCODE = '55000', CONSTRAINT = 'workload_flavor_capacity';
            END IF;
            INSERT INTO workload_flavor_reservations (workload_id, runner_id, flavor)
                VALUES (NEW.id, NEW.runner_id, NEW.flavor);
        END IF;
        RETURN NEW;
    END IF;
    SELECT EXISTS (SELECT 1 FROM workload_flavor_reservations WHERE workload_id = OLD.id) INTO reserved;
    IF TG_OP = 'DELETE' THEN
        IF reserved AND OLD.removal_confirmed_at IS NULL THEN
            RAISE EXCEPTION 'unconfirmed workload retains its slot'
                USING ERRCODE = '55000', CONSTRAINT = 'workload_flavor_admission';
        END IF;
        RETURN OLD;
    END IF;
    IF (OLD.preparation_phase IS NOT NULL AND
            ROW(NEW.runner_id, NEW.flavor, NEW.allocated_cpu_millicores, NEW.allocated_ram_bytes, NEW.persistent_shells)
            IS DISTINCT FROM ROW(OLD.runner_id, OLD.flavor, OLD.allocated_cpu_millicores, OLD.allocated_ram_bytes, OLD.persistent_shells)) THEN
        RAISE EXCEPTION 'prepared workload admission metadata is immutable'
            USING ERRCODE = '55000', CONSTRAINT = 'workload_flavor_admission';
    END IF;
    IF reserved AND OLD.removal_confirmed_at IS NULL AND NEW.removal_confirmed_at IS NOT NULL THEN
        -- Existing anchored lifecycle triggers validate the exact absence proof.
        -- Any later trigger failure rolls this decrement back in the same write.
        UPDATE workload_flavor_admission SET occupied = occupied - 1
            WHERE runner_id = OLD.runner_id AND flavor = OLD.flavor AND occupied > 0;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'admission counter missing'
                USING ERRCODE = '55000', CONSTRAINT = 'workload_flavor_admission';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
