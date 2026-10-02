# Runners

The Runners service manages runner registrations and workload runtime state.

See [AGENTS.md](AGENTS.md) for source owners and contribution rules, and
[docs/catalog.json](docs/catalog.json) for operational and historical documents.

See [preparation revocation](PREPARATION-REVOCATION.md) for recovery rollout limits.

## Caller Authorization

Every control-plane RPC passes the [rpcauth](internal/rpcauth/interceptor.go)
unary and stream interceptors. Callers send an audience-bound projected
ServiceAccount token in `x-agyn-caller-token`; runners validates it with a
TokenReview and applies the deployment's grants to the method's class
([classification](internal/rpcauth/classification.json), pinned to the API).
Configuration is `RUNNERS_RPC_AUTH_MODE` (`enforce` by default, `permissive`
only logs), `RUNNERS_RPC_POLICY` or `RUNNERS_RPC_POLICY_FILE` (the
[Policy](internal/rpcauth/policy.go) document) and `RUNNERS_TOKENREVIEW_TIMEOUT`.
The ServiceAccount needs `create` on `tokenreviews.authentication.k8s.io`; the
chart's `rpcAuth` values render all of this. Enforcement starts only after every
caller sends tokens, and a NetworkPolicy must still limit who reaches the port.

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

The opt-in [database fixture](internal/server/workload_removal_live_test.go)
requires `AGYN_RUNNERS_REMOVAL_TEST_DSN` pointing to a disposable loopback
database named `a2a_removal_acceptance`. Never target the deployed platform database.

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

Use the repository's [DevSpace workflow](devspace.yaml) after bootstrap:

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

Assertions and fixture isolation are in
[volume_lifecycle_live_test.go](internal/server/volume_lifecycle_live_test.go).
Never point it at the deployed platform database.

### Checked volume lifecycle

The checked lifecycle contract lives in
[volume_lifecycle.go](internal/server/volume_lifecycle.go). Its comments link the
original [lifecycle migration](migrations/0018_checked_volume_lifecycle.sql) and
later admission, adoption and backend guards. Apply reviewed additive migrations;
never edit an applied migration to explain or change the contract.

The database fixture above does not prove a real runner's absence, process-level
failover or a deployed coordinated rollout.

Drain/audit all writers before activation; legacy records are not automatically
adopted. Service authorization, late backend creates, partitioned nodes,
storage-level fencing and checked-record retention remain production work.

### Workload admission and volume removal

The original admission contribution required migrations `0017` and `0018`,
based on combined integration `0492121`, not the independent checked-volume
contribution alone.

The owner guard and checked lifecycle invariants are documented at the Go
callers in [volume_lifecycle.go](internal/server/volume_lifecycle.go) and
[workloads.go](internal/server/workloads.go), with the original
[admission migration](migrations/0019_volume_workload_admission.sql).
Audit/drain before rollout and preserve guard rows. An admission rejection is
not permission to replay a possibly completed backend side effect.

Historical admission acceptance and reproduction instructions follow; they are
not a new run or a compatibility manifest for the current branch.

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

The original `feat/legacy-volume-adoption` contribution used migration
`0020_legacy_volume_adoption.sql` on top of `f05b479`.

Audited legacy bind, immutable backend/owner validation and explicit checked
reopen are owned by [volume_lifecycle.go](internal/server/volume_lifecycle.go)
and the original [adoption migration](migrations/0020_legacy_volume_adoption.sql).
This operation is not a lasting deployment drain or proof against late creates.
The current identity profile uses the native runner's Kubernetes validators;
other backends require an explicit identity contract.

Historical acceptance of that contribution:

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

Backend identity and confirmation contracts live in
[volume_lifecycle.go](internal/server/volume_lifecycle.go). The registry trusts
authenticated controller observations; it does not contact the native runner.

Backend-less history needs explicit reconciliation before migration `0021`;
do not disable the constraint or invent a namespace UID to force the upgrade.

Historical acceptance of the backend-identity contribution:

All 423 race tests pass with both disposable PostgreSQL fixtures enabled.
The backend migration tests check two owner kinds, refused/repeated upgrades,
unchanged history, a validated constraint and rejected old SQL writes. Build
and vet pass. This is not a deployed database upgrade, independent backend
authentication, workload-start fencing or full A2A acceptance.

## Prepared Workloads

The [prepared-workload guide](PREPARED-WORKLOADS.md) records cross-repository
rollout requirements and historical database/RPC acceptance limits.

## Helm chart defaults

Review [chart values](charts/runners/values.yaml) and the
[authorization policy](charts/runners/templates/authorizationpolicy.yaml) against
your deployment's service accounts before rollout, including E2E identities.
The policy assumes trusted mesh identity and trustworthy forwarded identity
headers; source compatibility does not establish that trust boundary.
