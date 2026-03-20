# PR6: Retain/Release Audit and Leak Hardening

## Problem
Multiple retain paths in IO/conversion code are not clearly paired with releases.

## Goal
Audit and correct retain/release pairing across ingestion and conversion paths.

## Scope
- `dataframe/arrow_schema.go`
- `dataframe/io.go`
- iter/pipeline interaction points as needed
- lifecycle-related tests

## Plan
1. Inventory all `Retain` and `Release` sites in dataframe ingestion paths.
2. Pair every retain with deterministic ownership transfer or release.
3. Remove unnecessary retains where ownership is already guaranteed.
4. Add test coverage for repeated read/convert/release cycles.

## Tests
- Stress-style loop tests for repeated read/convert/release.
- Ensure no use-after-release in iterator/pipeline handoffs.

## Risks
- Subtle refcount bugs if ownership assumptions are wrong.

## Rollout
- Land in small commits grouped by subsystem (CSV, Parquet, record batches).

## Acceptance Criteria
- No unpaired retain paths in audited modules.
- Lifecycle tests pass reliably.
