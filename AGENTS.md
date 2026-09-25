# Runners Contribution Guide

## Owners
- `internal/server/volume_anchor_migration.go`: owner-wide migration transaction.
- `internal/server/volume_lifecycle.go` and `workloads.go` in that directory:
  checked storage, admission and billing versus physical-removal evidence.
- `internal/server/{prepared_workloads,resource_anchors,preparation_revocation,anchored_volume_removal}.go`:
  phase/CAS validation, native identity and retained cleanup history.
- `migrations/`: immutable database enforcement; follow links from Go callers.

## Documentation
- Keep implementation invariants beside their handwritten Go or protobuf owner.
  Update those comments and focused tests when behavior changes; Markdown holds
  operations, cross-repository decisions, security boundaries and dated evidence.
- Start at `docs/catalog.json`. Maintain its version-1 document entries
  (`id`, `path`, `title`, `purpose`, `kind`) for meaningful Markdown and
  `AGENTS.md` only, using repository-relative paths. Do not index generated code.
- When a compatible structural navigator is available, discover repositories and
  components first, then batch-inspect selected owners and their related tests.
  Otherwise use native declarations, imports, RPC types and adjacent tests;
  `git diff upstream/main...HEAD` (or the reviewed base) identifies the changes.
  Do not add a navigator dependency or machine-specific paths to this repository.
  Navigator commands are `repos [--worktrees]`,
  `scan --repo REPO`, `inspect REPO::path` and `docs --repo REPO`.
  IDs here are `api`, `runners`, `orchestrator`, `k8s-runner` and `gateway`.
  Worktrees use Git-discovered basenames, optionally selected by `--worktree`.
  At genuine cross-repo owners, optional `@see repo::extensionless/component`
  references can aid navigation; same-repo `@see` paths retain the extension.
- Preserve dated verification, failures, skips and dependency revisions as
  historical evidence; do not silently turn them into current acceptance claims.
- Do not edit generated sources or applied SQL migrations, including comments.
  Migration bytes participate in recovery/backup checks. Explain SQL behavior
  beside the owning Go caller and link the original migration; schema changes
  require a separately reviewed additive migration. Preserve licensing/notices.

## Verification
Use existing matching API bindings; do not rewrite `.gen/` for documentation.
Run focused model-free `go test -mod=readonly -race ./internal/server -run REGEXP`
with database gates unset. `AGYN_RUNNERS_VOLUME_TEST_DSN` and
`AGYN_RUNNERS_REMOVAL_TEST_DSN` opt into disposable-database mutation, not ordinary
unit coverage. Never target an installed registry. Report excluded live tests
and missing generated dependencies, rather than weakening assertions.
