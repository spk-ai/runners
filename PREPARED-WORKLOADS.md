# Prepared Workload Registry

Dependent proposal on `feat/volume-backend-identity` (`c3b5338`). Generate against
the API's `feat/prepared-workload-registry`, which extends the native prepared
API (`53e0817`). Original repository licenses are unchanged. This is not a
published capability, a database rollout, or a complete A2A integration.

## Contract

`CreatePreparedWorkload` creates a STARTING, RESERVED record with revision 1,
the backend identity and a complete set of registry volume IDs. It does not
authorize a native call. The owner must have only compatible checked volumes
and no unconfirmed predecessor; this applies even when the mount set is empty.
The same transaction permanently pins the owner's backend and persistent
identity. Confirmed workload history may be garbage collected without enabling
legacy admission or silently moving that owner to another backend.

`UpdatePreparedWorkload` requires the expected revision and exactly one command:

| Command | Required State | Result |
| --- | --- | --- |
| Begin preparation | RESERVED and STARTING | PREPARING; native preparation may now be attempted once |
| Bind | PREPARING, each checked volume already bound | BOUND with immutable native workload and complete volume identities |
| Begin activation | BOUND and STARTING | ACTIVATING; authorize activation of that exact binding |
| Confirm activation | ACTIVATING, identical binding | ACTIVE, without changing observed workload status |
| Begin removal | PREPARING, BOUND, ACTIVATING or ACTIVE | REMOVING, irreversibly excludes new activation authorization |
| Late bind | REMOVING without a binding | Remains REMOVING; capture exact identity for cleanup only |
| Confirm removal | REMOVING with a binding, exact-binding native ABSENT | REMOVED, persisted observation and server-time removal confirmation |
| Abort reservation | RESERVED only | REMOVED without a native binding; no preparation was authorized |

Each successful command advances revision once. A lost response requires a read
of the exact workload ID and validation of its owner/backend/state, not a new
native preparation or agent-message replay. Concurrent CAS failures return
Aborted. Duplicate creation retains the existing primary key; callers must not
infer ownership merely from AlreadyExists. Billing/status reports do not advance
the preparation revision, authorize activation, or confirm physical removal.
Removal preserves an existing failure status.

Bind uses the same volume validation as checked-volume binding and compares
every physical identity with the stored active checked volume. Volume IDs are
canonical and unique; binding volumes use the native runner's name ordering.
The database repeats set/identity checks under owner serialization before bind
and activation. It cannot prove the caller included every desired mount; the
controller must derive the complete set from the assembled native request and
reject a native response with any missing, added or substituted volume.

## Database Enforcement

Migration `0022_prepared_workloads.sql` atomically installs nullable preparation
fields, an immutable owner pin and both workload/volume guards. It neither
adopts legacy workloads nor invents bindings or absence. Existing records keep
their status, billing, removal evidence and all other historical values.

The owner guard uses the existing real row-write pattern, followed by separate
reads in VOLATILE trigger functions. A stale repeatable-read or serializable
transaction cannot authorize from an old snapshot. No new cross-table `FOR
UPDATE` acquisition is introduced. PostgreSQL documents the relevant
[snapshot/serialization behavior](https://www.postgresql.org/docs/current/transaction-iso.html),
[VOLATILE function snapshots](https://www.postgresql.org/docs/current/xfunc-volatility.html)
and [transactional trigger execution](https://www.postgresql.org/docs/current/trigger-definition.html).

Old SQL cannot replace a prepared binding, skip phases or bypass revision
checks, confirm removal using the billing field, discard an unconfirmed
workload, or clear/move a pinned owner. Old CreateWorkload is excluded for opted-in
owners. Unconfirmed preparations protect their volume records even after a
failure report; failing a still-needed provisioning record would otherwise make
late binding impossible. Physical workspace deletion remains a separate checked
operation after compute absence, not a side effect of task compute release.

## Verification

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

- Migrate both agent and sandbox controllers, their generated clients and any
  Gateway routing/policy that must expose the distinct methods. Keep all prepared
  paths fail-closed on Unimplemented, never fallback to name-only methods.
- Persist/controller-reconcile preparation intent, lost native prepare replies,
  late gated creates, delayed holds and interrupted startup Secret ownership.
  PREPARING with an unknown binding deliberately retains admission; there is no
  safe generic-error abort or native observation/recovery capability here yet.
- Authenticate runner routes and authorize workload owners; enforce all native
  writers and admission mutations. The observation is a stored trusted caller
  assertion, not a signed receipt. A privileged direct native call or database
  administrator is not fenced by registry CAS.
- Cancellation that wins before begin activation prevents new authorization.
  After ACTIVATING, native activation may already be in flight. Retain admission
  until exact physical absence, and separately fence partitioned nodes/storage,
  forced deletes and cloned backend identities.
- Audit/reconcile legacy data, drain incompatible writers, roll out compatible
  migrations/clients together, and rerun complete A2A/model acceptance. This
  source verification does not authorize automatic adoption or production use.
