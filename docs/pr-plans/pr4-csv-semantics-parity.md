# PR4: CSV Semantics Parity (Eager vs Streaming)

## Problem
CSV null parsing defaults differ between eager `ReadCSV` and streaming `ScanCSV`.

## Goal
Same CSV input yields consistent null semantics regardless of API path.

## Scope
- `dataframe/io_options.go`
- `scan/csv.go`
- `dataframe/io_test.go`
- `scan/csv_test.go`

## Plan
1. Define one canonical default null policy.
2. Update eager and streaming option builders to share that default behavior.
3. Add a shared fixture-based parity test strategy.
4. Document null behavior in package docs.

## Tests
- Parity tests for empty string/null token handling.
- Header/no-header combinations.
- Explicit `NullValues` override behavior tests.

## Risks
- Behavior change for users relying on current mismatch.

## Rollout
- Document as consistency fix; include migration note if needed.

## Acceptance Criteria
- Eager and streaming CSV APIs agree on null interpretation by default and with overrides.
