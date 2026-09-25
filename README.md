# Runners

The Runners service manages runner registrations and workload runtime state.

See [AGENTS.md](AGENTS.md) for source owners and contribution rules, and
[docs/catalog.json](docs/catalog.json) for operational and historical documents.

The dependent [preparation-revocation registry](PREPARATION-REVOCATION.md)
persists interrupted-provisioning recovery without replacing workspace identity.

## Explicit Workload Removal Confirmation

Billing versus physical-removal semantics live beside `updateWorkload` in
[workloads.go](internal/server/workloads.go), with the original
[confirmation migration](migrations/0017_workload_removal_confirmation.sql).
Confirmation is a trusted observation, not infrastructure fencing.

The [API contribution branch](https://github.com/spk-ai/api/tree/feat/workload-removal-confirmation)
supplies the additive protobuf fields. Until that contract is published, generate from the sibling
API checkout instead of the BSR default:

```sh
cd ../api
buf generate . --template ../runners/buf.gen.yaml --output ../runners \
  --include-imports --path proto/agynio/api/identity/v1 \
  --path proto/agynio/api/authorization/v1 --path proto/agynio/api/ziti_management/v1 \
  --path proto/agynio/api/notifications/v1 --path proto/agynio/api/agents/v1 \
  --path proto/agynio/api/runner/v1 --path proto/agynio/api/runners/v1
cd ../runners
go test ./...
go test -race ./...
```

`TestLiveWorkloadRemovalConfirmation` additionally uses a disposable PostgreSQL
database via `AGYN_RUNNERS_REMOVAL_TEST_DSN`. It refuses non-loopback hosts and
databases not named `a2a_removal_acceptance`; it creates and removes its own
schema. It verifies the real migration, authenticated runner failure reporting,
explicit confirmation, retry preservation and the reopening constraint. It
makes no agent/model calls and must not target the deployed platform database.

Rollout requires the updated orchestrator and regenerated Gateway as well as
this service. Historical workloads need inspection, not a timestamp backfill.

Architecture: [Runners](https://github.com/agynio/architecture/blob/main/architecture/runners.md)

## Local Development

Full setup: [Local Development](https://github.com/agynio/architecture/blob/main/architecture/operations/local-development.md)

### Prepare environment

```bash
git clone https://github.com/agynio/bootstrap.git
cd bootstrap
chmod +x apply.sh
./apply.sh -y
```

See [bootstrap](https://github.com/agynio/bootstrap) for details.

### Run from sources

```bash
# Deploy once (exit when healthy)
devspace dev

# Watch mode (streams logs, re-syncs on changes)
devspace dev -w
```

### Run tests

```bash
# From the agynio/e2e repo
devspace run test-e2e --tag svc_runners
```

E2E coverage is centralized in [agynio/e2e](https://github.com/agynio/e2e) under the go-core suite.
See [E2E Testing](https://github.com/agynio/architecture/blob/main/architecture/operations/e2e-testing.md).

### Closed volume reuse

Legacy same-owner reopening is documented beside `reopenClosedVolume` in
[volumes.go](internal/server/volumes.go). Checked lifecycle and explicit audited
binding belong to [volume_lifecycle.go](internal/server/volume_lifecycle.go).
An AlreadyExists response is not proof of ownership or native PVC validation.

CI runs `TestLiveVolumeReopen` against disposable PostgreSQL. To run it locally,
set `AGYN_RUNNERS_VOLUME_TEST_DSN` to a PostgreSQL URL for a disposable database
named `runners_volume_acceptance` on `127.0.0.1`, then run:

```sh
go test -race ./internal/server -run '^TestLiveVolumeReopen$' -count=1 -timeout=3m
```

The test applies the real migrations in its own uniquely named schema and drops
only that schema. It checks same-owner recovery, canonical/deprecated fields,
ownership mismatches, nullable sandbox identity, unchanged conflicts, and
overlapping PostgreSQL updates. It makes no agent/model calls. Never point it at
the deployed platform database.

### Checked volume lifecycle

The checked lifecycle contract lives beside `CreateVolumeChecked`,
`UpdateVolumeChecked` and `applyVolumeOperation` in
[volume_lifecycle.go](internal/server/volume_lifecycle.go). Its comments link the
original [lifecycle migration](migrations/0018_checked_volume_lifecycle.sql) and
later admission, adoption and backend guards. Apply reviewed additive migrations;
never edit an applied migration to explain or change the contract.

The existing disposable PostgreSQL test now includes agent/sandbox checked
lifecycles, independent-reader persistence, new-server-object intent recovery,
raw old-SQL rejection, stale retries, guarded reopen and eight simultaneously
blocked checked updates with exactly one successful CAS. It does not prove a
real runner's absence, process-level failover or a deployed coordinated rollout.

Drain/audit all writers before activation; legacy records are not automatically
adopted. Service authorization, late backend creates, partitioned nodes,
storage-level fencing and checked-record retention remain production work.

### Workload admission and volume removal

Migration `0019_volume_workload_admission.sql` closes the interval between a
controller's idle-workload scan and its checked begin-removal update. It requires
both the workload confirmation migration `0017` and checked-volume migration
`0018`; this branch is based on their combined integration `0492121`, not the
independent checked-volume contribution alone.

The owner guard and checked lifecycle invariants are documented at the Go
callers in [volume_lifecycle.go](internal/server/volume_lifecycle.go) and
[workloads.go](internal/server/workloads.go), with the original
[admission migration](migrations/0019_volume_workload_admission.sql).
The guard key is the runtime owner, not a global organization/class lock.

Migration locks both tables while installing the guards. It rejects existing
checked owners with contradictory deletion/workload state, identity mismatches
or multiple unconfirmed workloads. It neither modifies historical rows nor
infers absence. Audit/drain before rollout, preserve guard rows, and use new
workload IDs after confirmed removal. A database admission rejection maps to
`FailedPrecondition`; serialization/deadlock errors map to `Aborted`. Neither
permits automatically replaying a backend side effect.

`TestLiveVolumeReopen/workload-admission` uses the existing disposable database
gate and exercises real registry methods with a stub authorization writer, plus
raw SQL interleavings. Forty-eight blocked cases cover both owner kinds, all
four PostgreSQL isolation settings, either race winner, winner rollback and
competing starts. Each observes the actual blocker PID, admits a different owner
while blocked, joins the losing operation and independently reads committed
state. `admission-migration` tests valid upgrades and atomic refusal of four
historical contradictions. No runner, Kubernetes, model or deployed database is
used by these admission fixtures.

Generate from the combined API proposal `ec2bfed` (the generation command above
with that checkout), then run both gated database suites and the complete race
suite. The published BSR API and stock platform images do not contain these
proposals. Controller/sandbox integration, mixed-writer rollout and full A2A
acceptance remain required. The database reservation does not fence delayed
backend creates, a replayed Start RPC or a partitioned node, and it does not
authenticate the controller's removal evidence or the selected runner backend.

### Explicit legacy adoption

The dependent `feat/legacy-volume-adoption` branch adds migration
`0020_legacy_volume_adoption.sql` on top of `f05b479`. It reuses the proposed
`UpdateVolumeChecked(bind)` RPC; it does not add an adoption endpoint or
automatically import existing records.

Audited legacy bind, immutable backend/owner validation and explicit checked
reopen are owned by [volume_lifecycle.go](internal/server/volume_lifecycle.go)
and the original [adoption migration](migrations/0020_legacy_volume_adoption.sql).
This operation is not a lasting deployment drain or proof against late creates.
The current identity profile uses the native runner's Kubernetes validators;
other backends require an explicit identity contract.

`TestLiveVolumeReopen/legacy-adoption` covers both owner kinds, all five
unconfirmed workload states, known-name provenance, unchanged rejected writes,
revision retries and 32 actually blocked transaction interleavings across all
four isolation settings. Another owner's adoption progresses while the tested
owner is blocked. `adoption-migration` upgrades from `0019`, verifies repeated
migration leaves volume/workload/guard history unchanged, and exercises the new
raw-SQL guards. Full `go test -race ./...` passes with both disposable database
gates enabled (419 tests including subtests); `go build ./...` and `go vet ./...`
also pass. These fixtures use no deployed database, Kubernetes or model.

The migration is a local precondition, not an ownership audit or a deployment
permit. It does not authenticate callers, verify sandbox human ownership, prove
backend incarnation, retroactively validate existing checked bindings, stop old
writers or fence in-flight creates/deletes and partitioned nodes. Those checks
and a coordinated all-writer rollout remain mandatory before real adoption.

## Backend-Bound Volumes

Backend identity and matching confirmation requirements live beside
`validateVolumeBinding` and `applyVolumeOperation` in
[volume_lifecycle.go](internal/server/volume_lifecycle.go). The registry trusts
authenticated controller observations; it does not contact the native runner.

Migration `0021_volume_backend_identity.sql` also rejects new backend-less
bindings through old SQL writers. It validates existing history and fails
atomically if a prior checked binding lacks the required identity. It performs
no backfill, adoption or deletion. Such history needs explicit reconciliation;
do not disable the constraint or invent a namespace UID to force the upgrade.

All 423 race tests pass with both disposable PostgreSQL fixtures enabled.
The backend migration tests check two owner kinds, refused/repeated upgrades,
unchanged history, a validated constraint and rejected old SQL writes. Build
and vet pass. This is not a deployed database upgrade, independent backend
authentication, workload-start fencing or full A2A acceptance.

## Prepared Workloads

The dependent [prepared-workload registry proposal](PREPARED-WORKLOADS.md) adds
durable preparation states, exact bindings, revision CAS, immutable owner/backend
pins and old-writer guards. Native controller integration and coordinated rollout
are still required; the document records the database/RPC acceptance scope.

## Helm chart defaults

The chart ships with a DENY-based Istio AuthorizationPolicy. By default,
`authorizationPolicy.identityServiceAccounts` allows in-mesh callers that
forward `x-identity-id` (`gateway`, `expose`, `notifications`, `chat`).
Override the list if your deployment uses different service accounts (for
example, add `agents-orchestrator-e2e` for E2E runs).
