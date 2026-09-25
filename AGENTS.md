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
- Keep `docs/catalog.json` as the version-1 index of meaningful Markdown and
  `AGENTS.md`, not a source inventory. Preserve its stable document IDs.
- Use the workspace Navigator first when available: `repos [--worktrees]`,
  then `scan --repo runners`, then batch `inspect runners::owner runners::related-owner`.
  Use `docs --repo runners` and `doc runners::document-id` for guides. Worktrees use
  Git-discovered basenames, optionally selected by `--worktree`.
  Standalone contributors need no Navigator: use `git diff upstream/main...HEAD`
  (or the reviewed base), `rg`, Go/Buf tools and adjacent tests.
  Do not copy Navigator tooling, dependencies or machine-specific paths here.
  Optional cross-repo `@see repo::extensionless/component` links belong at genuine
  contract owners; same-repo `@see` paths retain the source extension.
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
