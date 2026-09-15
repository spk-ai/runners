# Resource Anchor Registry

Registry source and disposable PostgreSQL acceptance verified on 2026-09-15.
Branch `feat/resource-anchor-registry` is based on registry `e7c42f4` and the
dependent API branch of the same name, revision `6fe4cab`, based on native API
`3b25d03`.
Nothing is installed or ready for coordinated rollout. Controllers are not
migrated by this contribution, and no upstream PR has been submitted.

The existing preparation revision cannot fence an older writer that already
knows that field. Anchored reservations therefore have an additional, durable
resource revision. New preparation transitions compare and advance both;
metadata-only anchor binding advances the resource revision while leaving
the preparation phase/revision unchanged. Database guards enforce this even
when the writer does not know the added columns. Existing records stay unchanged.

## Required Ordering

1. Create a distinct anchored workload reservation. Pin the owner to anchored
   operation so future old-client starts cannot bypass the contract.
2. Reserve native metadata only. Bind each volume anchor in its checked record
   under the exact, still-unused workload reservation. Persist that reservation's
   workload ID and both revisions in an immutable `VolumeAnchorReservation`.
3. Bind the complete workload/volume anchor set immutably to the reservation.
4. Advance both revisions before native anchored preparation. Bind exact PVCs
   and Pod afterward, preserving the previously recorded owner UIDs.
5. Continue checked activation/removal without changing the immutable anchors.
   Lost replies require reading committed state, never replaying preparation.

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
- Full registry race suite: 618 passing test entries, zero failures/skips, with
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
