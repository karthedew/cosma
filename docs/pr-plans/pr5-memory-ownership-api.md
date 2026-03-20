# PR5: Memory Ownership API (Core)

## Problem
Arrow-backed memory ownership is not explicit in DataFrame/Series lifecycle.

## Goal
Introduce deterministic ownership semantics (retain/release contract) for core dataframe objects.

## Scope
- `dataframe/dataframe.go`
- `dataframe/series.go`
- relevant docs/ADR updates

## Plan
1. Define ownership model in code and docs.
2. Add explicit lifecycle methods (`Release`, and `Retain` if needed).
3. Clarify transfer/borrow rules in constructors and conversion helpers.
4. Ensure panic-safe and idempotent release behavior.

## Tests
- Add lifecycle tests validating release behavior and repeat safety.
- Add ownership contract tests around constructor/clone semantics.

## Risks
- API changes may require migration in internal callsites/tests.

## Rollout
- Introduce methods with clear docs; keep backward compatibility where possible.

## Acceptance Criteria
- Ownership rules are explicit, documented, and test-covered.
- Core objects can be deterministically released.
