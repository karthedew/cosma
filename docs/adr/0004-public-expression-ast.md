# ADR 0004: Public Expression AST

## Status

Accepted

## Context

Cosma's expression tree was initially implemented in `internal/expr`, making all
node types (`ColumnNode`, `BinaryNode`, `AggNode`, etc.) module-private. This
prevented external users from calling `df.Filter` and `df.WithColumn` entirely,
since the parameter types were unreachable from outside the module.

Three design options were considered for Phase 1:

1. **Re-export via interface** — a public `expr.Expr` interface that internal
   nodes satisfy. External callers can build expressions but internal node types
   leak via type assertions.

2. **Opaque wrapper struct** — a public `expr.Expr` struct that holds an
   `internal/expr.Expr` privately, with an `Unwrap` escape hatch for the engine.
   Tree is hidden; serialization requires a separate layer.

3. **Fully public AST** (chosen) — promote all node types and op constants to
   `cosma/expr`. The tree is inspectable, serializable, and pattern-matchable
   directly.

## Decision

All expression AST node types (`ColumnNode`, `LiteralNode`, `BinaryNode`,
`UnaryNode`, `AggNode`, `AliasNode`, `CastNode`) and op types (`BinaryOp`,
`UnaryOp`, `AggOp`) are exported from `cosma/expr`. This is the Polars Rust
approach: `Expr` is a transparent public type.

The fluent builder API (`expr.Col("age").Gt(expr.Lit(30))`) remains the primary
construction surface. Direct struct construction is available for power users
(custom optimizer passes, plan deserializers).

Engine internals that are not part of the user-facing contract stay in
`internal/expr`: type promotion helpers (`isNumeric`, `promoteNumeric`),
coercion rules, and schema bind logic.

## Consequences

- External users can build, inspect, serialize, and deserialize expression trees.
- Distributed compute (plan serialization over Arrow Flight) is possible without
  a separate schema — the public node types are the wire format.
- Custom optimizer passes can pattern-match on public node types.
- `internal/compute` imports `cosma/expr` for node types; no `Unwrap` indirection.
- Import graph (no cycles): `cosma/expr → internal/expr` (helpers only);
  `plan → cosma/expr`; `dataframe → cosma/expr`; `internal/compute → cosma/expr`.
- Breaking change from the internal-only design: the expression tree is now part
  of the public API contract and cannot be restructured without a major version bump.
