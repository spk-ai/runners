# Existing Workspace Adoption Registry

The dependent API branch is `feat/volume-anchor-migration`; the operator
coordinator lives in the matching agents-orchestrator branch. Native runner
`0ed8c5c` supplies adoption.

See [volume_anchor_migration.go](internal/server/volume_anchor_migration.go)
for the migration contract and the caller's explanation of the original
[migration 0027](migrations/0027_volume_anchor_migration.sql).
Adoption remains distinct from first-allocation and retirement evidence.

Run `go test -race ./...` with the opt-in PostgreSQL environment documented in
[volume_lifecycle_live_test.go](internal/server/volume_lifecycle_live_test.go).
The [migration fixture](internal/server/volume_anchor_migration_live_test.go)
owns the executable acceptance cases.

Schema application alone does not adopt anything and is not rollback authority.
Drain writers and verify a coordinated restore-tested backup first. Preserve the
whole migration/adoption JSON in backup fingerprints. Keep controllers stopped
until owners are fully migrated or explicitly retained under quarantine. There
is deliberately no force-unblock API. This trusted-local protocol is not node
fencing or proof that interrupted task side effects can be retried safely.
