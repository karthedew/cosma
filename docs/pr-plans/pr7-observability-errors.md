# PR7: Observability and Error Surfaces

## Problem
Close-time and operator-context errors are under-reported, reducing diagnosability.

## Goal
Improve error fidelity with contextual wrapping and close-error handling.

## Scope
- `scan/reader.go`
- `dataframe/io.go`
- `internal/exec/pipeline.go`

## Plan
1. Capture and surface close errors where practical.
2. Wrap pipeline/operator errors with operator identity/context.
3. Standardize error messages for source and operation stage.
4. Keep error chains inspectable (`%w`).

## Tests
- Add tests for close failure propagation behavior.
- Add tests for wrapped operator error context.
- Validate no masking of original underlying errors.

## Risks
- Slightly different error strings may require test updates.

## Rollout
- Prioritize additive wrapping; avoid breaking public error contracts where possible.

## Acceptance Criteria
- Errors include actionable stage/operator context.
- Important close failures are not silently dropped.
