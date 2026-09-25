# Resource Anchor Registry

The dependent [anchored retirement proposal](ANCHORED-VOLUME-RETIREMENT.md)
adds migration `0025` and durable PVC-and-owner absence receipts. Historical
anchor verification below retains its original scope.

Registry source and disposable PostgreSQL acceptance verified on 2026-09-15.
Branch `feat/resource-anchor-registry` is based on registry `e7c42f4` and the
dependent API branch of the same name, revision `6fe4cab`, based on native API
`3b25d03`.
Nothing is installed or ready for coordinated rollout. Controllers are not
migrated by this contribution, and no upstream PR has been submitted.

## Contract Owners

Dual revisions and complete owner binding live beside
`BindWorkloadResourceAnchors` in
[resource_anchors.go](internal/server/resource_anchors.go). Exact reservation
rechecks live beside `checkVolumeAnchorReservation` and the checked update in
[volume_lifecycle.go](internal/server/volume_lifecycle.go).
These comments explain the original
[anchor migration](migrations/0023_resource_anchors.sql) without changing its bytes.

Native owner identities remain trusted backend/controller assertions, not
authentication. This work must not pretend that anchor absence alone proves
child absence, or permit anchored-volume removal through older APIs. Initially
absent recovery, anchor retirement receipts, orphan/credential reconciliation,
node/storage fencing and coordinated deployment remain required.

## Cancellation Race

The first real contention test reproduced a Read Committed bug: the UPDATE's
initial EXISTS could see the old reservation, wait for the owner lock, and then
bind using a replacement reservation belonging to that same owner. Matching
only the owner and volume set in the trigger was insufficient.

The persisted reservation receipt lets the trigger recheck the exact workload
and both revisions after its owner-row write. A canceled reservation cannot
borrow the replacement's authority. Read Committed rejects the stale request;
Repeatable Read and Serializable reject the stale writer with serialization
failure. Rollback preserves the original reservation, and another owner can
progress while this owner waits. There is no automatic native retry.

The ordering follows PostgreSQL's
[statement snapshot rules](https://www.postgresql.org/docs/16/transaction-iso.html)
and [trigger behavior](https://www.postgresql.org/docs/16/trigger-definition.html).

## Verification

- API lint and breaking checks against `3b25d03` pass.
- Full registry race suite: 620 passing test entries, zero failures/skips, with
  both real PostgreSQL gates enabled. Build and unfiltered vet pass.
- Real loopback registry RPCs cover both owner kinds, zero/one/two volumes,
  before-authority rejection, exact bindings, two turns with server replacement,
  immutable history, old-API/SQL rejection and persistent volume reuse.
- Twelve forced blocked interleavings cover both owners, all three isolation
  levels, commit/rollback and progress for another owner.
- Twenty focused race repetitions pass all 1,600 entries, including 240 forced
  database interleavings, with no failures/skips.
- Migration `0023` preserves 28 existing prepared workloads across all seven
  phases, with and without volumes, plus 14 bound volume records. Two migration
  applications preserve all prior row fields and invent no anchor identities.
  Earlier upgrade/rejection fixtures also pass using historical seed SQL.
- Regression tests reject missing persisted volume anchors and conflicting
  sandbox human-owner labels. Native receipts and authorization are fixtures,
  not real Kubernetes, A2A, authentication or provider-agent acceptance.

## Native Inbox Thread Identity

Controller integration exposed a distinction hidden by the original fixture:
the orchestrator writes the agent instance ID to the registry's legacy
`thread_id`, while the assembled runtime's `thread-id` label is the actual
inbox thread. These identifiers need not match. The original anchor validator
incorrectly equated them and would reject real agent starts.

The native inbox thread remains a required canonical UUID inside the immutable
workload anchor. It is checked against the request by the controller and native
runner, and against that same persisted anchor during recovery. It is not a
volume owner or an instance-lifetime placement pin. A subsequent workload may
use another inbox thread without changing the instance, volume anchor or
historical reservation receipt. Authorization of these identities remains an
external requirement, not something provided by this shape validator.

Additive migration `0024_resource_anchor_thread_identity.sql` corrects the
database validator without rewriting `0023` or changing any existing row.
The upgrade test seeds an anchored workload and volume under the real `0023`
schema, reproduces rejection of a distinct inbox thread, applies migrations
twice and compares every workload/volume/owner-guard field. It also rejects
noncanonical thread IDs and direct mutation of an already-bound thread.
Real RPC lifecycle fixtures now use different native thread IDs across their
two turns while preserving registry and volume identity.

The focused regression failed before the source correction. The first upgrade
assertion compared a mutation response with an enriched GET response; the final
test compares pre/post GETs and independent SQL snapshots instead. The final
full race run passes all 620 entries, with both disposable PostgreSQL gates
enabled. This dependent correction is not installed.

Generate the dependent API locally from the API checkout (the generated `.gen`
directory is ignored):

```sh
buf lint
buf breaking --against '.git#ref=3b25d03'
buf generate . --template ../runners-resource-anchors/buf.gen.yaml \
  --output ../runners-resource-anchors --include-imports \
  --path proto/agynio/api/runners/v1 --path proto/agynio/api/runner/v1 \
  --path proto/agynio/api/agents/v1 --path proto/agynio/api/authorization/v1 \
  --path proto/agynio/api/identity/v1 --path proto/agynio/api/notifications/v1 \
  --path proto/agynio/api/ziti_management/v1
```

From this registry checkout, run `go build ./...`, `go vet ./...` and
`GOMAXPROCS=4 go test -race ./... -count=1 -timeout=3m` with both fixture variables
set privately: `AGYN_RUNNERS_VOLUME_TEST_DSN` must identify loopback database
`runners_volume_acceptance`; `AGYN_RUNNERS_REMOVAL_TEST_DSN` must identify
loopback database `a2a_removal_acceptance`. Use disposable PostgreSQL, never the
platform database. The fixtures create and drop only their own schemas.

The acceptance used pinned PostgreSQL 16.6 image
`postgres@sha256:1d04b9ba1d4996401f2552b51beda8187f175c0645c091e4781134fc9c9a3eef`
in a bounded, loopback-only Docker container with tmpfs storage. This is fixture
reproducibility, not a production database-version recommendation.

## Checklist

- [x] Additive anchored registry APIs and distinct revision contract.
- [x] Immutable workload/volume anchor persistence before preparation authority.
- [x] Database old-writer/admission guards, including zero-volume owners.
- [x] Source tests, real PostgreSQL contention and historical migration checks.
- [ ] Native reservation lost replies and initially absent/late resource recovery.
- [ ] Both controller paths and combined native/process recovery acceptance.
- [ ] Anchored-volume retirement, durable cleanup and coordinated A2A rollout.
