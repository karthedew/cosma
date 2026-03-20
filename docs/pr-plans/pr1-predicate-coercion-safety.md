# PR1: Predicate Coercion Safety

## Problem
Numeric literal coercion in predicate binding can silently overflow or truncate, producing incorrect filter results.

## Goal
Reject lossy or out-of-range coercions at bind time with clear errors.

## Scope
- `internal/expr/bind.go`
- `internal/expr/bind_test.go`

## Plan
1. Add explicit range checks for int/uint coercions (including `uint64 -> int64`).
2. Reject fractional float coercion for int/uint target kinds.
3. Reject values outside representable bounds.
4. Keep float coercion behavior explicit and tested.
5. Improve error messages to include source and target kind.

## Tests
- Add boundary tests for max/min signed and unsigned values.
- Add negative-to-uint failure tests.
- Add fractional float-to-int/uint failure tests.
- Add large `uint64` overflow test for int coercion.

## Risks
- Existing callers relying on permissive coercion may start failing.

## Rollout
- Land with tests first-class; no API surface change.

## Acceptance Criteria
- No silent truncation/overflow in predicate coercion paths.
- All new boundary tests pass.
