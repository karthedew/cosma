# PR3: Context Propagation End-to-End

## Problem
Cancellation/timeout context is inconsistently propagated; some paths use `context.Background()`.

## Goal
Honor caller context from scan through pipeline and operators.

## Scope
- `internal/exec/pipeline.go`
- `operator/filter.go`
- `scan/parquet.go`
- related tests in `internal/exec/*_test.go`, `scan/*_test.go`

## Plan
1. Store and propagate pipeline context to all operator execution points.
2. Remove internal hardcoded background contexts where caller context should apply.
3. Thread context into filter compute and parquet read paths.
4. Wrap cancellation errors consistently for debugging.

## Tests
- Add cancellation test that aborts long-running pipeline work.
- Add timeout test for parquet scan/read path.
- Ensure no regressions in existing execution tests.

## Risks
- Signature changes may touch multiple callsites.

## Rollout
- Keep API-compatible where possible; update internal wiring first.

## Acceptance Criteria
- Caller cancellation reliably stops execution.
- No `context.Background()` in runtime-critical processing paths.
