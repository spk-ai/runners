-- Metadata adoption is distinct from allocating replacement storage. Every
-- owner stays closed until all exact native readiness receipts are durable.
LOCK TABLE workloads, volumes, runtime_volume_admission_guards IN SHARE ROW EXCLUSIVE MODE;

ALTER TABLE runtime_volume_admission_guards ADD COLUMN volume_anchor_migration JSONB;
ALTER TABLE volumes ADD COLUMN anchor_adoption JSONB;
ALTER TABLE volumes DROP CONSTRAINT volumes_resource_anchor_shape;
ALTER TABLE volumes ADD CONSTRAINT volumes_resource_anchor_shape CHECK (
    (resource_anchor IS NULL AND anchor_reservation IS NULL AND anchor_adoption IS NULL) OR
    (resource_anchor IS NOT NULL AND checked_lifecycle AND jsonb_typeof(resource_anchor) = 'object'
        AND ((anchor_reservation IS NOT NULL AND jsonb_typeof(anchor_reservation) = 'object' AND anchor_adoption IS NULL)
            OR (anchor_reservation IS NULL AND anchor_adoption IS NOT NULL AND jsonb_typeof(anchor_adoption) = 'object'))));

CREATE FUNCTION migration_uuid(value TEXT) RETURNS BOOLEAN LANGUAGE sql IMMUTABLE AS $$
    SELECT COALESCE(value ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        AND value <> '00000000-0000-0000-0000-000000000000', FALSE)
$$;

CREATE FUNCTION valid_volume_anchor_migration_entry(e JSONB, backend TEXT) RETURNS BOOLEAN
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    source JSONB := e->'source';
    previous JSONB := source->'previous';
    intent JSONB := e->'intent';
    adoption JSONB := e->'adoption';
    binding JSONB;
BEGIN
    IF (jsonb_typeof(e) = 'object' AND e - ARRAY['source','checkedRevision','adoptionId','intent','adoption','applied','ready','unresolvedReason'] = '{}'::jsonb
        AND jsonb_typeof(source) = 'object' AND source - ARRAY['volumeId','expectedRevision','previous'] = '{}'::jsonb
        AND migration_uuid(source->>'volumeId') AND jsonb_typeof(source->'expectedRevision') = 'string'
        AND source->>'expectedRevision' ~ '^[1-9][0-9]{0,17}$') IS NOT TRUE THEN RETURN FALSE; END IF;
    IF previous IS NULL THEN
        RETURN e = jsonb_build_object('source', source, 'unresolvedReason', 'unbound_failed_generation');
    END IF;
    IF (e->>'unresolvedReason' IS NULL AND jsonb_typeof(e->'checkedRevision') = 'string'
        AND e->>'checkedRevision' ~ '^[1-9][0-9]{0,17}$'
        AND (e->>'checkedRevision')::bigint BETWEEN (source->>'expectedRevision')::bigint AND (source->>'expectedRevision')::bigint + 1
        AND migration_uuid(e->>'adoptionId') AND previous->>'backendId' = backend
        AND previous->>'volumeKey' = source->>'volumeId' AND migration_uuid(previous->>'instanceUid')
        AND length(previous->>'instanceId') BETWEEN 1 AND 253
        AND previous - ARRAY['instanceId','volumeKey','instanceUid','identityLabels','backendId'] = '{}'::jsonb
        AND intent = jsonb_build_object('kind','RESOURCE_ANCHOR_KIND_VOLUME','resourceId',source->>'volumeId',
            'backendId',backend,'identityLabels',previous->'identityLabels')) IS NOT TRUE THEN RETURN FALSE; END IF;
    IF adoption IS NULL THEN RETURN NOT (e ? 'applied' OR e ? 'ready'); END IF;
    IF (migration_uuid(adoption->>'instanceUid') AND migration_uuid(adoption->'anchor'->>'instanceUid')
        AND adoption->>'pvcSpecSha256' ~ '^[0-9a-f]{64}$'
        AND adoption = jsonb_build_object('id',e->>'adoptionId','previous',previous,
            'anchor',intent || jsonb_build_object('instanceUid',adoption->'anchor'->>'instanceUid'),
            'instanceUid',adoption->>'instanceUid','pvcSpecSha256',adoption->>'pvcSpecSha256')) IS NOT TRUE THEN RETURN FALSE; END IF;
    IF NOT e ? 'applied' THEN RETURN NOT e ? 'ready'; END IF;
    binding := previous || jsonb_build_object('anchor',adoption->'anchor');
    IF e->'applied' IS DISTINCT FROM jsonb_build_object('adoption',adoption,'volume',binding,
        'state','VOLUME_ANCHOR_ADOPTION_STATE_APPLIED') THEN RETURN FALSE; END IF;
    RETURN NOT e ? 'ready' OR e->'ready' = jsonb_build_object('adoption',adoption,'volume',binding,
        'state','VOLUME_ANCHOR_ADOPTION_STATE_READY');
END;
$$;

CREATE FUNCTION guard_volume_anchor_migration() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    doc JSONB := NEW.volume_anchor_migration;
    old_doc JSONB := OLD.volume_anchor_migration;
    e JSONB;
    previous_e JSONB;
    v volumes%ROWTYPE;
    index INTEGER := 0;
    changed INTEGER := 0;
    finished BOOLEAN;
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.volume_anchor_migration IS NOT NULL THEN
            RAISE EXCEPTION 'migration evidence and owner admission block must be retained'
                USING ERRCODE='55000', CONSTRAINT='volume_anchor_migration';
        END IF;
        RETURN OLD;
    END IF;
    IF old_doc IS NOT NULL AND ROW(NEW.owner_kind,NEW.owner_id) IS DISTINCT FROM ROW(OLD.owner_kind,OLD.owner_id) THEN
        RAISE EXCEPTION 'migration admission owner is immutable' USING ERRCODE='55000', CONSTRAINT='volume_anchor_migration';
    END IF;
    IF doc IS NOT DISTINCT FROM old_doc THEN
        IF doc IS NOT NULL AND NOT COALESCE((doc->>'complete')::boolean,FALSE) AND
            ROW(NEW.prepared_backend_id,NEW.prepared_runner_id,NEW.prepared_organization_id,NEW.prepared_thread_id,NEW.prepared_agent_id,NEW.resource_anchors_required)
            IS DISTINCT FROM ROW(OLD.prepared_backend_id,OLD.prepared_runner_id,OLD.prepared_organization_id,OLD.prepared_thread_id,OLD.prepared_agent_id,OLD.resource_anchors_required) THEN
            RAISE EXCEPTION 'partial migration cannot change admission pins'
                USING ERRCODE='55000', CONSTRAINT='volume_anchor_migration';
        END IF;
        RETURN NEW;
    END IF;
    IF (jsonb_typeof(doc)='object' AND doc - ARRAY['id','ownerKind','ownerId','runnerId','organizationId','backendId','revision','entries','complete']='{}'::jsonb
        AND migration_uuid(doc->>'id') AND migration_uuid(doc->>'runnerId') AND migration_uuid(doc->>'organizationId')
        AND doc->>'ownerId'=NEW.owner_id::text
        AND doc->>'ownerKind'=CASE NEW.owner_kind WHEN 'agent_instance' THEN 'RUNTIME_OWNER_KIND_AGENT_INSTANCE' ELSE 'RUNTIME_OWNER_KIND_SANDBOX' END
        AND jsonb_typeof(doc->'revision')='string' AND doc->>'revision' ~ '^[1-9][0-9]{0,17}$'
        AND octet_length(doc->>'backendId') BETWEEN 1 AND 512 AND doc->>'backendId'=btrim(doc->>'backendId',E' \t\n\r\f\013')
        AND jsonb_typeof(doc->'entries')='array' AND jsonb_array_length(doc->'entries') BETWEEN 1 AND 64
        AND (NOT doc ? 'complete' OR doc->'complete'='true'::jsonb)) IS NOT TRUE THEN
        RAISE EXCEPTION 'invalid owner migration document' USING ERRCODE='55000', CONSTRAINT='volume_anchor_migration';
    END IF;
    finished := COALESCE((doc->>'complete')::boolean,FALSE);
    IF NOT finished AND ROW(NEW.prepared_backend_id,NEW.prepared_runner_id,NEW.prepared_organization_id,NEW.prepared_thread_id,NEW.prepared_agent_id,NEW.resource_anchors_required)
        IS DISTINCT FROM ROW(OLD.prepared_backend_id,OLD.prepared_runner_id,OLD.prepared_organization_id,OLD.prepared_thread_id,OLD.prepared_agent_id,OLD.resource_anchors_required) THEN
        RAISE EXCEPTION 'only completed migration may change permanent admission pins'
            USING ERRCODE='55000', CONSTRAINT='volume_anchor_migration';
    END IF;
    IF old_doc IS NULL THEN
        IF doc->>'revision'<>'1' OR finished OR NEW.resource_anchors_required OR
            EXISTS(SELECT 1 FROM workloads w WHERE w.owner_kind=NEW.owner_kind AND w.owner_id=NEW.owner_id AND w.removal_confirmed_at IS NULL) THEN
            RAISE EXCEPTION 'migration requires a drained unanchored owner' USING ERRCODE='55000', CONSTRAINT='volume_anchor_migration';
        END IF;
    ELSIF COALESCE((old_doc->>'complete')::boolean,FALSE) OR
        doc - ARRAY['revision','entries','complete'] IS DISTINCT FROM old_doc - ARRAY['revision','entries','complete'] OR
        (doc->>'revision')::bigint<>(old_doc->>'revision')::bigint+1 OR
        jsonb_array_length(doc->'entries')<>jsonb_array_length(old_doc->'entries') THEN
        RAISE EXCEPTION 'migration identity, revision and completed history are immutable' USING ERRCODE='55000', CONSTRAINT='volume_anchor_migration';
    END IF;
    IF (SELECT count(*) FROM volumes WHERE owner_kind=NEW.owner_kind AND owner_id=NEW.owner_id)<>jsonb_array_length(doc->'entries') OR
        (SELECT count(DISTINCT x->'source'->>'volumeId') FROM jsonb_array_elements(doc->'entries') x)<>jsonb_array_length(doc->'entries') THEN
        RAISE EXCEPTION 'complete original owner inventory required' USING ERRCODE='55000', CONSTRAINT='volume_anchor_migration';
    END IF;
    FOR e IN SELECT value FROM jsonb_array_elements(doc->'entries') LOOP
        IF NOT valid_volume_anchor_migration_entry(e,doc->>'backendId') THEN
            RAISE EXCEPTION 'invalid append-only native adoption evidence' USING ERRCODE='55000', CONSTRAINT='volume_anchor_migration';
        END IF;
        SELECT * INTO v FROM volumes WHERE id=(e->'source'->>'volumeId')::uuid AND owner_kind=NEW.owner_kind AND owner_id=NEW.owner_id;
        IF NOT FOUND OR ROW(v.runner_id,v.organization_id) IS DISTINCT FROM ROW((doc->>'runnerId')::uuid,(doc->>'organizationId')::uuid) THEN
            RAISE EXCEPTION 'migration volume owner mismatch' USING ERRCODE='55000', CONSTRAINT='volume_anchor_migration';
        END IF;
        IF e ? 'intent' AND NOT valid_registry_resource_anchor(e->'intent' || jsonb_build_object('instanceUid',e->>'adoptionId'),
            'RESOURCE_ANCHOR_KIND_VOLUME',v.id,doc->>'backendId',v.owner_kind,v.owner_id,v.agent_id,v.thread_id) THEN
            RAISE EXCEPTION 'migration native owner mismatch' USING ERRCODE='55000', CONSTRAINT='volume_anchor_migration';
        END IF;
        IF old_doc IS NULL THEN
            IF v.lifecycle_revision<>(e->'source'->>'expectedRevision')::bigint OR v.resource_anchor IS NOT NULL OR
                e ? 'adoption' OR e ? 'applied' OR e ? 'ready' OR v.removal_intent IS NOT NULL OR
                (e->'source'->'previous' IS NULL AND NOT (v.status='failed' AND NOT v.checked_lifecycle AND v.instance_id IS NULL AND v.bound_instance IS NULL)) OR
                (e->'source'->'previous' IS NOT NULL AND (
                    v.status NOT IN ('active','provisioning') OR v.instance_id IS NULL OR
                    e->'source'->'previous'->>'instanceId' IS DISTINCT FROM v.instance_id OR
                    v.checked_lifecycle AND v.bound_instance IS DISTINCT FROM e->'source'->'previous' OR
                    (e->>'checkedRevision')::bigint<>v.lifecycle_revision+CASE WHEN v.checked_lifecycle THEN 0 ELSE 1 END)) THEN
                RAISE EXCEPTION 'original volume cannot be adopted from this generation' USING ERRCODE='55000', CONSTRAINT='volume_anchor_migration';
            END IF;
        ELSE
            previous_e := old_doc->'entries'->index;
            IF e - ARRAY['adoption','applied','ready'] IS DISTINCT FROM previous_e - ARRAY['adoption','applied','ready'] THEN
                RAISE EXCEPTION 'migration plan is immutable' USING ERRCODE='55000', CONSTRAINT='volume_anchor_migration';
            END IF;
            IF e IS DISTINCT FROM previous_e THEN
                changed := changed+1;
                IF finished OR NOT ((NOT previous_e ? 'adoption' AND e ? 'adoption' AND e-'adoption'=previous_e)
                    OR (previous_e ? 'adoption' AND NOT previous_e ? 'applied' AND e ? 'applied' AND e-'applied'=previous_e)
                    OR (previous_e ? 'applied' AND NOT previous_e ? 'ready' AND e ? 'ready' AND e-'ready'=previous_e)) THEN
                    RAISE EXCEPTION 'native evidence may only advance one checked step' USING ERRCODE='55000', CONSTRAINT='volume_anchor_migration';
                END IF;
            END IF;
        END IF;
        IF finished AND (NOT e ? 'ready' OR v.anchor_adoption IS DISTINCT FROM e->'adoption' OR
            v.resource_anchor IS DISTINCT FROM e->'adoption'->'anchor' OR v.bound_instance IS DISTINCT FROM e->'ready'->'volume' OR
            v.status<>'active' OR NOT v.checked_lifecycle OR v.removal_intent IS NOT NULL OR
            v.lifecycle_revision<>(e->>'checkedRevision')::bigint+1) THEN
            RAISE EXCEPTION 'all original bindings need persisted independent native readiness' USING ERRCODE='55000', CONSTRAINT='volume_anchor_migration';
        END IF;
        index := index+1;
    END LOOP;
    IF old_doc IS NOT NULL AND changed<>(CASE WHEN finished THEN 0 ELSE 1 END) THEN
        RAISE EXCEPTION 'migration transition must advance exactly once' USING ERRCODE='55000', CONSTRAINT='volume_anchor_migration';
    END IF;
    IF finished AND (NOT NEW.resource_anchors_required OR NEW.prepared_backend_id IS DISTINCT FROM doc->>'backendId' OR
        ROW(NEW.prepared_runner_id,NEW.prepared_organization_id) IS DISTINCT FROM ROW((doc->>'runnerId')::uuid,(doc->>'organizationId')::uuid) OR
        EXISTS(SELECT 1 FROM volumes x WHERE x.owner_kind=NEW.owner_kind AND x.owner_id=NEW.owner_id AND
            ROW(x.thread_id,x.agent_id) IS DISTINCT FROM ROW(NEW.prepared_thread_id,NEW.prepared_agent_id))) THEN
        RAISE EXCEPTION 'migration completion requires exact permanent admission pins' USING ERRCODE='55000', CONSTRAINT='volume_anchor_migration';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER runtime_volume_anchor_migration BEFORE INSERT OR UPDATE OR DELETE ON runtime_volume_admission_guards
    FOR EACH ROW EXECUTE FUNCTION guard_volume_anchor_migration();

CREATE FUNCTION volume_anchor_migration_binding(previous volumes, next volumes) RETURNS BOOLEAN
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    doc JSONB;
    e JSONB;
BEGIN
    SELECT volume_anchor_migration INTO doc FROM runtime_volume_admission_guards
        WHERE owner_kind=previous.owner_kind AND owner_id=previous.owner_id;
    IF doc IS NULL OR COALESCE((doc->>'complete')::boolean,FALSE) THEN RETURN FALSE; END IF;
    SELECT value INTO e FROM jsonb_array_elements(doc->'entries') WHERE value->'source'->>'volumeId'=previous.id::text;
    IF NOT FOUND THEN RETURN FALSE; END IF;
    RETURN COALESCE(e ? 'applied' AND previous.checked_lifecycle AND next.checked_lifecycle AND
        previous.status='active' AND next.status='active' AND previous.bound_instance=e->'source'->'previous' AND
        previous.resource_anchor IS NULL AND previous.anchor_adoption IS NULL AND next.anchor_reservation IS NULL AND
        next.anchor_adoption=e->'adoption' AND next.resource_anchor=e->'adoption'->'anchor' AND next.bound_instance=e->'applied'->'volume' AND
        previous.lifecycle_revision=(e->>'checkedRevision')::bigint AND next.lifecycle_revision=previous.lifecycle_revision+1 AND
        to_jsonb(next)-ARRAY['updated_at','lifecycle_revision','bound_instance','resource_anchor','anchor_adoption'] =
        to_jsonb(previous)-ARRAY['updated_at','lifecycle_revision','bound_instance','resource_anchor','anchor_adoption'],FALSE);
END;
$$;

CREATE FUNCTION guard_migrating_volume() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    doc JSONB;
    e JSONB;
    kind TEXT;
    owner UUID;
BEGIN
    IF TG_OP='DELETE' THEN kind:=OLD.owner_kind; owner:=OLD.owner_id;
    ELSE kind:=NEW.owner_kind; owner:=NEW.owner_id; END IF;
    -- Existing checked guards forbid identity changes. Also fence legacy rows
    -- attempting to leave a blocked owner before they have a checked binding.
    IF TG_OP='UPDATE' AND ROW(NEW.owner_kind,NEW.owner_id) IS DISTINCT FROM ROW(OLD.owner_kind,OLD.owner_id) THEN
        PERFORM lock_runtime_volume_owner(OLD.owner_kind,OLD.owner_id);
        IF EXISTS(SELECT 1 FROM runtime_volume_admission_guards WHERE owner_kind=OLD.owner_kind AND owner_id=OLD.owner_id AND volume_anchor_migration IS NOT NULL) THEN
            RAISE EXCEPTION 'migration owner identity is immutable' USING ERRCODE='55000', CONSTRAINT='volume_anchor_migration';
        END IF;
    END IF;
    PERFORM lock_runtime_volume_owner(kind,owner);
    SELECT volume_anchor_migration INTO doc FROM runtime_volume_admission_guards WHERE owner_kind=kind AND owner_id=owner;
    IF doc IS NULL OR COALESCE((doc->>'complete')::boolean,FALSE) THEN
        IF TG_OP='DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP='UPDATE' THEN
        IF to_jsonb(NEW)-ARRAY['updated_at','last_metering_sampled_at'] = to_jsonb(OLD)-ARRAY['updated_at','last_metering_sampled_at'] THEN RETURN NEW; END IF;
        IF volume_anchor_migration_binding(OLD,NEW) THEN RETURN NEW; END IF;
        SELECT value INTO e FROM jsonb_array_elements(doc->'entries') WHERE value->'source'->>'volumeId'=OLD.id::text;
        IF FOUND AND NOT OLD.checked_lifecycle AND NEW.checked_lifecycle AND NOT e ? 'adoption' AND
            OLD.lifecycle_revision=(e->'source'->>'expectedRevision')::bigint AND NEW.lifecycle_revision=(e->>'checkedRevision')::bigint AND
            NEW.lifecycle_revision=OLD.lifecycle_revision+1 AND NEW.status='active' AND NEW.bound_instance=e->'source'->'previous' AND
            to_jsonb(NEW)-ARRAY['updated_at','lifecycle_revision','checked_lifecycle','status','bound_instance'] =
            to_jsonb(OLD)-ARRAY['updated_at','lifecycle_revision','checked_lifecycle','status','bound_instance'] THEN RETURN NEW; END IF;
    END IF;
    RAISE EXCEPTION 'owner migration blocks volume lifecycle changes' USING ERRCODE='55000', CONSTRAINT='volume_anchor_migration';
END;
$$;
CREATE TRIGGER volumes_anchor_migration BEFORE INSERT OR UPDATE OR DELETE ON volumes
    FOR EACH ROW EXECUTE FUNCTION guard_migrating_volume();

CREATE FUNCTION guard_migrating_workload() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    doc JSONB;
    kind TEXT;
    owner UUID;
BEGIN
    IF TG_OP='DELETE' THEN kind:=OLD.owner_kind; owner:=OLD.owner_id;
    ELSE kind:=NEW.owner_kind; owner:=NEW.owner_id; END IF;
    IF TG_OP='UPDATE' AND ROW(NEW.owner_kind,NEW.owner_id) IS DISTINCT FROM ROW(OLD.owner_kind,OLD.owner_id) THEN
        PERFORM lock_runtime_volume_owner(OLD.owner_kind,OLD.owner_id);
        IF EXISTS(SELECT 1 FROM runtime_volume_admission_guards WHERE owner_kind=OLD.owner_kind AND owner_id=OLD.owner_id AND volume_anchor_migration IS NOT NULL) THEN
            RAISE EXCEPTION 'migration workload owner identity is immutable' USING ERRCODE='55000', CONSTRAINT='volume_anchor_migration';
        END IF;
    END IF;
    PERFORM lock_runtime_volume_owner(kind,owner);
    SELECT volume_anchor_migration INTO doc FROM runtime_volume_admission_guards WHERE owner_kind=kind AND owner_id=owner;
    IF doc IS NULL OR COALESCE((doc->>'complete')::boolean,FALSE) THEN
        IF TG_OP='DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP='UPDATE' AND to_jsonb(NEW)-ARRAY['updated_at','last_metering_sampled_at','last_activity_at'] =
        to_jsonb(OLD)-ARRAY['updated_at','last_metering_sampled_at','last_activity_at'] THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'owner migration blocks workload admission and lifecycle changes' USING ERRCODE='55000', CONSTRAINT='volume_anchor_migration';
END;
$$;
CREATE TRIGGER workloads_anchor_migration BEFORE INSERT OR UPDATE OR DELETE ON workloads
    FOR EACH ROW EXECUTE FUNCTION guard_migrating_workload();

-- Retain the existing checked lifecycle, adding only the journal-checked
-- attachment of an owner to the same bound physical incarnation.
CREATE OR REPLACE FUNCTION guard_checked_volume_lifecycle() RETURNS trigger LANGUAGE plpgsql AS $$
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
            AND NOT (reopening_deleted AND NEW.bound_instance IS NULL)
            AND NOT volume_anchor_migration_binding(OLD,NEW) THEN
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
