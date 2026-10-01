-- Operator-owned opt-in limits. An empty table preserves existing admission.
-- Configure only after draining the selected runner/flavor; application replicas
-- share the database counter rather than each maintaining a local semaphore.
CREATE TABLE workload_flavor_admission (
    runner_id UUID NOT NULL REFERENCES runners(id),
    flavor TEXT NOT NULL CHECK (flavor <> '' AND flavor = btrim(flavor)),
    capacity INTEGER NOT NULL CHECK (capacity BETWEEN 0 AND 10000),
    occupied INTEGER NOT NULL DEFAULT 0 CHECK (occupied >= 0 AND occupied <= capacity),
    PRIMARY KEY (runner_id, flavor)
);
-- Keep reservation bookkeeping out of historical workload rows.
CREATE TABLE workload_flavor_reservations (
    workload_id UUID PRIMARY KEY REFERENCES workloads(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    runner_id UUID NOT NULL,
    flavor TEXT NOT NULL
);

CREATE FUNCTION guard_flavor_reservation_writer() RETURNS trigger
LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    IF pg_trigger_depth() < 2 THEN
        RAISE EXCEPTION 'flavor reservations are database-owned'
            USING ERRCODE = '55000', CONSTRAINT = 'workload_flavor_admission';
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER flavor_reservation_writer BEFORE INSERT OR UPDATE OR DELETE
    ON workload_flavor_reservations FOR EACH ROW EXECUTE FUNCTION guard_flavor_reservation_writer();

CREATE FUNCTION guard_flavor_admission_configuration() RETURNS trigger
LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    -- Only the nested workload trigger maintains counters. Policy writes are
    -- serialized with workload insertion, including the first opt-in policy.
    IF TG_OP = 'UPDATE' AND pg_trigger_depth() > 1 AND
        ROW(NEW.runner_id, NEW.flavor, NEW.capacity) IS NOT DISTINCT FROM
        ROW(OLD.runner_id, OLD.flavor, OLD.capacity) THEN
        RETURN NEW;
    END IF;
    LOCK TABLE workloads IN SHARE ROW EXCLUSIVE MODE;
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
CREATE TRIGGER flavor_admission_configuration BEFORE INSERT OR UPDATE OR DELETE
    ON workload_flavor_admission FOR EACH ROW EXECUTE FUNCTION guard_flavor_admission_configuration();

CREATE FUNCTION guard_workload_flavor_admission() RETURNS trigger
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    policy workload_flavor_admission%ROWTYPE;
    reserved BOOLEAN;
BEGIN
    IF TG_OP = 'INSERT' THEN
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
CREATE TRIGGER workload_flavor_admission BEFORE INSERT OR UPDATE OR DELETE
    ON workloads FOR EACH ROW EXECUTE FUNCTION guard_workload_flavor_admission();
