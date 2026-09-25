# Existing Workspace Adoption Registry

Migration `0027_volume_anchor_migration.sql` adds a durable owner migration
document and a separate `volumes.anchor_adoption` receipt. The dependent API
branch is `feat/volume-anchor-migration`; the operator coordinator lives in the
matching agents-orchestrator branch. Native runner `0ed8c5c` supplies adoption.

The transaction, complete owner inventory, append-only CAS evidence and quarantine
contract live beside `BeginVolumeAnchorMigration` and
`AdvanceVolumeAnchorMigration` in
[volume_anchor_migration.go](internal/server/volume_anchor_migration.go).
Their comments explain the original
[migration 0027](migrations/0027_volume_anchor_migration.sql).
Adoption remains distinct from first-allocation and retirement evidence.

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
