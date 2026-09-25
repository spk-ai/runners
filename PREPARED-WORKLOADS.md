# Prepared Workload Registry

Dependent proposal on `feat/volume-backend-identity` (`c3b5338`). Generate against
the API's `feat/prepared-workload-registry`, which extends the native prepared
API (`53e0817`). Original repository licenses are unchanged. This is not a
published capability, a database rollout, or a complete A2A integration.

## Contract Owners

See [prepared_workloads.go](internal/server/prepared_workloads.go) for the
lifecycle contract.
Checked workspace identities belong to
[volume_lifecycle.go](internal/server/volume_lifecycle.go); dual revisions belong
to [resource_anchors.go](internal/server/resource_anchors.go).
Database enforcement is linked from those callers to the original
[prepared-workload migration](migrations/0022_prepared_workloads.sql).

Preserve historical billing and identity, audit/drain incompatible
writers and coordinate controller/native/API rollout. Physical workspace
retirement remains separate from compute release.

## Verification

Historical acceptance of the prepared-registry proposal above:

- Build and vet pass. All **538 tests including subtests** pass with `-race` and
  both disposable PostgreSQL fixtures enabled; no individual test is skipped.
  Packages without tests are reported separately by Go.
- The added tests cover the phase matrix, invalid/stale requests, a CAS lost
  after reading, exact/mismatched bindings, empty mount sets and owners without
  volumes, late prepare after cancellation/failure, and duplicate creation.
- Real loopback registry RPCs, a fresh registry server/connection, and independent
  SQL reads verify persisted responses and two generations for both owner kinds.
  Native Pod identities/removal observations and Agents metadata/authorization
  are fixtures. No Kubernetes, native runner, model or A2A controller participates.
- **48 forced blocked interleavings** cover activation/cancellation and
  prepared/legacy admission in both orders, commit/rollback, both owner kinds and
  READ COMMITTED, REPEATABLE READ and SERIALIZABLE. Another owner progresses while
  the tested owner is blocked; independent reads verify the final phase/pin.
- Raw SQL tests attempt binding substitution, invalid sets, removal shortcuts,
  identity changes and discarded pins. Rejected workload mutations leave the
  complete row unchanged. Upgrade from 0021 and a repeated migration preserve
  history and explicitly leave all new legacy fields empty/defaulted.
- A race-enabled repeat of the prepared unit/RPC/database selection passes
  **2,320 entries across 20 repetitions**, with no failures or skipped tests.

Reproduce after generating the matching API:

```sh
# From the matching API checkout:
buf generate . --template ../runners-prepared-workloads/buf.gen.yaml \
  --output ../runners-prepared-workloads --include-imports

# From this checkout; use disposable loopback databases only:
AGYN_RUNNERS_VOLUME_TEST_DSN='postgres://postgres@127.0.0.1:PORT/runners_volume_acceptance?sslmode=disable' \
AGYN_RUNNERS_REMOVAL_TEST_DSN='postgres://postgres@127.0.0.1:PORT/a2a_removal_acceptance?sslmode=disable' \
  go test -race ./... -count=1 -timeout=4m
```

The local acceptance used PostgreSQL 16.6 in a resource-bounded, loopback-only
disposable container with image
`postgres@sha256:1d04b9ba1d4996401f2552b51beda8187f175c0645c091e4781134fc9c9a3eef`.
Do not point these tests at the deployed database. They create/drop private test
schemas. Plaintext test RPCs, fixture authorization and local trust database
authentication are not production configuration.

## Remaining Integration

These are the original proposal's integration gates, not a current completion list.

- Migrate both agent and sandbox controllers, their generated clients and any
  Gateway routing/policy that must expose the distinct methods. Keep all prepared
  paths fail-closed on Unimplemented, never fallback to name-only methods.
- Reconcile late gated creates, delayed holds and interrupted startup Secret
  ownership across the controller/native boundary.
- Authenticate runner routes and authorize workload owners; enforce all native
  writers and admission mutations. The observation is a stored trusted caller
  assertion, not a signed receipt. A privileged direct native call or database
  administrator is not fenced by registry CAS.
- Separately fence partitioned nodes/storage, forced deletes and cloned backend
  identities; registry admission is not that fence.
- Audit/reconcile legacy data, drain incompatible writers, roll out compatible
  migrations/clients together, and rerun complete A2A/model acceptance. This
  source verification does not authorize automatic adoption or production use.
