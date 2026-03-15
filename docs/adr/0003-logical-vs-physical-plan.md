# ADR 0003: Logical vs Physical Plan

## Status

Accepted

## Context

Cosma needs an eager DataFrame API today but must evolve into a lazy engine with
query planning and optimization. Without a plan abstraction, future lazy
execution would require breaking API changes.

## Decision

Cosma introduces internal logical and physical plan layers from the beginning.
The DataFrame API is eager by default but builds a logical plan internally. The
logical plan is translated to a physical plan for execution. Planning APIs are
internal until Phase 5 ships a user-facing lazy interface.

## Consequences

- The public API remains eager while preserving a path to lazy execution.
- Optimizer passes can be added without changing user-facing DataFrame methods.
- Execution configuration (batch size, parallelism) lives in physical planning.
