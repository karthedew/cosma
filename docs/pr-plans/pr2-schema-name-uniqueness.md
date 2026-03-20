# PR2: Schema Name Uniqueness

## Problem
Duplicate field names are not rejected consistently, causing ambiguous or inconsistent column resolution.

## Goal
Enforce a single deterministic rule: duplicate schema field names are invalid.

## Scope
- `schema/schema.go`
- `schema/schema_test.go`
- any constructor/callsite tests affected

## Plan
1. Add duplicate-name validation in schema construction.
2. Return clear, actionable duplicate-name errors.
3. Ensure all schema creation paths use the validated constructor.
4. Update tests and any fixtures with duplicate fields.

## Tests
- Add duplicate-name rejection tests.
- Add case-sensitivity expectation tests (documented behavior).
- Verify existing valid schema tests remain green.

## Risks
- Might break tests/examples that accidentally depended on duplicates.

## Rollout
- Land as behavior hardening with release-note mention.

## Acceptance Criteria
- Duplicate schema names are rejected everywhere.
- Lookup behavior is consistent across planner/executor/dataframe.
