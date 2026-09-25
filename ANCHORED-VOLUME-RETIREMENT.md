# Anchored Volume Retirement Registry

Dependent on `feat/resource-anchor-registry` and the matching API
`feat/anchored-volume-removal`. This proposal is not installed.

See [anchored_volume_removal.go](internal/server/anchored_volume_removal.go)
for the retirement contract.
The checked caller links the original
[retirement migration](migrations/0025_anchored_volume_removal.sql).
Ordinary idle compute release does not request workspace retirement.

## Verification

Historical acceptance of the anchored-retirement proposal above:

The full race suite passes 752 entries with both disposable PostgreSQL gates
enabled and no skips. Tests exercise real migrations, RPCs and independent SQL
reads, plus loss of acknowledged begin/confirm responses across service
replacement. Invalid, pending and foreign responses cannot commit confirmation.
Native observations in these registry tests are fixtures, not GC evidence.

Forced contention covers admission winning, retirement winning and rollback,
both owner kinds and all supported transaction isolation settings. Historical
fixtures seed the schema that actually existed, apply the migrations twice and
compare prior workload/volume/owner-guard fields. The new column remains NULL
until explicit native confirmation; historical projections do not backfill it.

Use two dedicated disposable databases, never the installed registry:

```sh
AGYN_RUNNERS_VOLUME_TEST_DSN="$VOLUME_FIXTURE_DSN" \
AGYN_RUNNERS_REMOVAL_TEST_DSN="$REMOVAL_FIXTURE_DSN" \
GOMAXPROCS=4 go test -race ./... -count=1 -timeout=4m
```

The matching orchestrator branch supplies separate combined native/process
acceptance. Authentication of native receipts, backend/node fencing, late-child
cleanup, unbound first-provision reconciliation, all-writer upgrades and a
DNS-compatible real-agent A2A rollout remain required. Current absence is not
permission to reopen a retired workspace. Fork licenses are unchanged.
