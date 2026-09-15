# Preparation Revocation Registry

Dependent proposal requiring the matching API, native runner and controller.
Migration `0026_preparation_revocation.sql` is additive: it replaces lifecycle
guards without rewriting existing workload, workspace or admission history.
Existing repository licensing is unchanged.

`RecordPreparationRevocation` is accepted only by `UpdateAnchoredWorkload` for
an unbound `REMOVING` workload, with both revisions checked. The exact native
workload/volume anchor set and revocation-record UID are stored in existing
`resource_anchors` JSONB. Recording proof alone does not release admission.

`ConfirmPreparationRevocation` requires that earlier persisted receipt and a
complete `POD_ABSENT` observation. Found volumes must already have matching
active checked bindings, names and UIDs. An absent volume must still be its
original unbound anchored provisioning generation (revision 2), with its
original reservation receipt and no physical identity or removal intent. That
reservation may belong to an earlier attempt; it is never replaced on retry.

Both Go validation and PostgreSQL guards enforce the transition. The SQL check
runs under the same owner-row lock as admission and competing checked-volume
writes. A PVC bound between the controller's read and its final CAS invalidates
the stale absence observation. Proof/observation require separate writes and
remain immutable after confirmation. The workload's ordinary Pod binding and
Pod-removal observation stay nil; no Pod UID is invented.

Old writers cannot erase proof, skip confirmation, overwrite a known workspace
UID, reopen/delete the history or use the unanchored update API. This does not
protect against a database administrator dropping the guards. Native receipts
are trusted authenticated-controller inputs, not cryptographically verified
statements inside PostgreSQL.

## Verification

On 2026-09-15 the full race suite, with both disposable PostgreSQL gates enabled,
passed 838 entries with no failures/skips. Build and vet passed.

The new real SQL/RPC matrix covers agent instances and sandboxes with zero,
absent, found and mixed volumes; stale dual revisions; service restart;
independent SQL reads; direct-SQL bypass attempts; and a real competing volume
bind between validation and workload UPDATE. Native receipts are synthetic in
these registry tests, not evidence of Kubernetes cleanup.

The upgrade fixture seeds schema 0025 with reserved, unbound-removing,
bound-removing and removed workloads for both owner kinds. It verifies complete
SQL row/guard snapshots and matching pre/post-upgrade reads across repeated
migration application, then completes an interrupted preparation using the new
distinct proof and confirmation operations.

Use the existing `TestLiveVolumeReopen` gate with an explicitly disposable
loopback database named `runners_volume_acceptance`. The other gate is
`AGYN_RUNNERS_REMOVAL_TEST_DSN` for `a2a_removal_acceptance`. Never target the
installed platform database. Generate from the matching sibling API until the
contract is published; the current BSR default is not this proposal.

This branch is not deployed. Coordinated all-writer adoption, authenticated
future-write/node fencing, receipt retention and hardened operations remain
production gates. A stored observation is not proof that no late native CREATE
can ever appear, and does not authorize retrying uncertain agent side effects.
