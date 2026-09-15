-- Preserve registry thread_id (the legacy instance alias) and all stored rows.
-- The actual inbox thread is pinned by the immutable native workload anchor.
CREATE OR REPLACE FUNCTION valid_registry_resource_anchor(a JSONB, kind TEXT, resource_id UUID,
    backend TEXT, owner_kind TEXT, owner_id UUID, agent_id UUID, thread_id UUID)
RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    labels JSONB;
    human TEXT;
    inbox_thread TEXT;
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
            inbox_thread := a->'identityLabels'->>'thread-id';
            IF (inbox_thread ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                AND inbox_thread <> '00000000-0000-0000-0000-000000000000') IS NOT TRUE THEN RETURN FALSE; END IF;
            labels := labels || jsonb_build_object('thread-id', inbox_thread);
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
