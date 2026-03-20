# PR8: CI Race Job and Docs Alignment

## Problem
CI does not run race detection; some docs/ADRs describe stale or mismatched architecture/API details.

## Goal
Improve regression detection and align docs with real implementation status.

## Scope
- `.github/workflows/ci.yml`
- `docs/architecture.md`
- `docs/adr/0003-logical-vs-physical-plan.md`
- roadmap/document links referenced from `README.md`

## Plan
1. Add a CI job for `go test -race ./...`.
2. Update architecture docs to match existing code and package boundaries.
3. Correct ADR/public API wording mismatches.
4. Mark stubbed areas explicitly as not implemented/experimental.

## Tests/Verification
- CI workflow syntax validation.
- Run local test matrix command set where feasible.
- Manual doc link and consistency pass.

## Risks
- Race job may initially fail and uncover existing concurrency issues.

## Rollout
- Enable race job; if duration is high, keep as separate matrix or dedicated job.

## Acceptance Criteria
- CI runs race detector on PRs.
- Core docs accurately reflect current code and API reality.
