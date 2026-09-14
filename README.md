# Runners

The Runners service manages runner registrations and workload runtime state.

## Explicit Workload Removal Confirmation

`removed_at` retains its existing metering semantics: a failed/stopped status
ends billing even when the workload has not been removed. The additive
`removal_confirmed_at` field is written only by an explicit internal lifecycle
update after runner inspection. Failure reports and logical deletion do not
infer it. Migration `0017` deliberately leaves historical records unverified.

Confirmation requires a terminal workload, preserves the first timestamp on
retries, and does not overwrite billing end. A database constraint prevents a
concurrent or older writer from reopening a confirmed workload. This is a
record of a trusted controller's observation, not infrastructure fencing.

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

`CreateVolume` can reopen a failed or deleted record only for the same runtime
owner kind/ID, organization, runner, volume definition, thread and agent class.
Nullable sandbox thread/class fields must also match. The check is part of the
database update, not a separate read that could race with another create.
An identity mismatch or an already-open record returns `AlreadyExists` without
changing the stored row. Callers must validate existing records before reuse;
`AlreadyExists` alone is not proof that a volume belongs to the caller.

A successful reopen retains identity and creation time, clears the previous
backing instance and removal timestamp, and restarts metering. Existing size
and requested-status behavior is unchanged. This does not authorize RPC callers,
validate a runner's actual PVC, fence old workloads, or make SQL administrator
writes immutable. That legacy ownership check itself needs no new API or database
migration. Checked records are excluded from implicit create/reopen.

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

This branch additionally requires the proposed checked-volume API and migration
`0018_checked_volume_lifecycle.sql`. It adds a positive lifecycle revision, a
sticky checked flag, the bound backend incarnation and a durable removal intent.

Use `CreateVolumeChecked` for new provisioning, then revision-checked bind,
begin-removal, confirm-removal, fail-provisioning or explicit reopen operations.
Binding validates logical owner/class/key and cannot replace a live generation's
UID. Begin commits the immutable target before any backend deletion. Confirmation
requires that intent's ID; its authorized caller must first obtain matching
runner `ABSENT` evidence. This server does not independently contact the runner.

Database triggers reject legacy lifecycle mutations of checked rows, attempted
unprotection/retargeting, discarded pending intents and direct checked-record
deletion. Legacy lifecycle changes advance revisions; metering alone does not.
Checked SQL uses an atomic revision predicate and does not overwrite concurrent
metering updates. Reopen validates all persistent identity fields; deleted
generations require confirmation, while failed provisioning is not absence proof.

The existing disposable PostgreSQL test now includes agent/sandbox checked
lifecycles, independent-reader persistence, new-server-object intent recovery,
raw old-SQL rejection, stale retries, guarded reopen and eight simultaneously
blocked checked updates with exactly one successful CAS. It does not prove a
real runner's absence, process-level failover or a deployed coordinated rollout.

Drain/audit all writers before activation; legacy records are not automatically
adopted. Service authorization, late backend creates, partitioned nodes,
storage-level fencing and checked-record retention remain production work.

## Helm chart defaults

The chart ships with a DENY-based Istio AuthorizationPolicy. By default,
`authorizationPolicy.identityServiceAccounts` allows in-mesh callers that
forward `x-identity-id` (`gateway`, `expose`, `notifications`, `chat`).
Override the list if your deployment uses different service accounts (for
example, add `agents-orchestrator-e2e` for E2E runs).
