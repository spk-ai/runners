# Existing Workspace Adoption Registry

Migration `0027_volume_anchor_migration.sql` adds a durable owner migration
document and a separate `volumes.anchor_adoption` receipt. The dependent API
branch is `feat/volume-anchor-migration`; the operator coordinator lives in the
matching agents-orchestrator branch. Native runner `0ed8c5c` supplies adoption.

`BeginVolumeAnchorMigration` requires the complete drained owner inventory and
exact lifecycle revisions. Its transaction serializes on the real owner guard,
installs the immutable admission block, and promotes only observed legacy
bindings to checked lifecycle. An unchecked unbound failed record remains
unchanged and quarantines the owner. No PVC or native receipt is invented.

`AdvanceVolumeAnchorMigration` appends one reserve/apply/ready receipt per CAS.
Applying persists the original PVC binding plus its new resource anchor and
native adoption receipt atomically. Completion requires every entry READY and
the exact current volume rows, then permanently pins prepared anchored admission.
`GetVolumeAnchorMigration` independently reads committed progress. The operator
must resume the same immutable plan after an uncertain response.

SQL guards reject old lifecycle/admission writes and owner identity escapes
while migration is pending. Metering-only updates remain permitted. Migration
documents cannot be cleared, deleted, rewritten, advanced across missing stages
or changed after completion. Existing allocation and retirement evidence remains
distinct; adoption never impersonates an allocation reservation.

Run `go test -race ./...` with the opt-in PostgreSQL environment documented in
`internal/server/volume_lifecycle_live_test.go`. The migration cases restart the
registry between commits and cover both owner kinds, mixed legacy/checked
volumes, stale CAS, malformed native evidence, failed-generation quarantine,
raw old writers under supported isolation levels, and anchored follow-up.

Schema application alone does not adopt anything and is not rollback authority.
Drain writers and verify a coordinated restore-tested backup first. Preserve the
whole migration/adoption JSON in backup fingerprints. Keep controllers stopped
until owners are fully migrated or explicitly retained under quarantine. There
is deliberately no force-unblock API. This trusted-local protocol is not node
fencing or proof that interrupted task side effects can be retried safely.
