---
name: cosma-context
description: Domain language and resolved design decisions for the Cosma project
metadata:
  type: project
---

# Cosma — Domain Context

Cosma is an Arrow-native dataframe and n-dimensional array engine for Go.

## Project Goals
Cosma is an Arrow-native **dataframe and n-dimensional array engine** for Go.
- **DataFrame** (`cosma/dataframe`): 2D tabular data — rows × typed columns backed by Arrow chunked arrays.
- **CArray** (`cosma/carray`): n-dimensional array for data beyond 2D (image batches, time series, embeddings). Parallel development track alongside the dataframe phases. Name chosen to avoid confusion with Apache Arrow's Tensor type (which targets GPU/ML compute and may be a future integration point).
- The two are designed to interop: a `carray.Array` should eventually be a valid column type in a DataFrame, and `expr.Col()` should be able to reference `carray`-typed columns. No implementation work needed now — this is a documented seam.

## Resolved Terms

### Public Expression Package
- **Name:** `expr`
- **Import path:** `github.com/karthedew/cosma/expr`
- **Usage:** `expr.Col("age").Gt(expr.Lit(30))`
- Internal engine stays in `internal/expr`; the public package is a thin builder surface only.

### Expr (public type)
- `expr.Expr` is a public wrapper struct with an exported `Node ExprNode` field. The AST node types are **fully public** in `cosma/expr` — inspectable, serializable, and pattern-matchable. This enables distributed compute (plan serialization over Arrow Flight), custom optimization passes, and caching.
- Public node types: `ColumnNode`, `LiteralNode`, `BinaryNode`, `UnaryNode`, `AggNode`, `AliasNode`, `CastNode`.
- Public op types: `BinaryOp`, `UnaryOp`, `AggOp` (with all constants).
- Users build trees via fluent builder functions and methods (`expr.Col("age").Gt(expr.Lit(30))`). The fluent API is the primary construction surface; direct struct construction is available for power users (optimizer passes, deserializers).
- `internal/expr` is largely promoted to `cosma/expr`. Only engine internals stay internal: coercion rules, type promotion helpers (`isNumeric`, `promoteNumeric`, etc.), and schema bind logic.
- No `Unwrap` function needed — `compute.Eval` type-switches on public node types directly.
- This is the Polars Rust approach: `Expr` is a transparent public type.

### WithColumn
- Signature: `df.WithColumn(e expr.Expr) (*DataFrame, error)`
- Output column name comes from `.As("name")` chained on the expression (Polars-style).
- The `name string` parameter is removed.
- Default output name rule: if the expression resolves to a single root column (e.g. `Col("age").Mul(Lit(2))`), the root column name is used and the column is replaced in place. If the expression involves multiple columns and no `.As()` is present, an error is returned with message directing the user to add `.As()`.

### Plan package / expression storage
- `plan.FilterNode.Predicate` stores `expr.Expr` (public type).
- `plan` imports `cosma/expr`. `internal/compute` also imports `cosma/expr` for node types.
- Import graph (no cycles): `cosma/expr → internal/expr` (for engine helpers only); `plan → cosma/expr`; `dataframe → plan`; `internal/compute → cosma/expr`.

### RecordReader (streaming interface)
- Public interface: `scan.RecordReader` — promoted from ADR 0002's internal shape.
- Methods: `Schema() *arrow.Schema`, `Next() bool`, `Record() arrow.Record`, `Err() error`, `Release()`.
- Both `scan.ScanCSV`/`scan.ScanParquet` and `LazyFrame.CollectStream()` return `scan.RecordReader`, making file scans and lazy plan outputs interchangeable at the type level.

### Sort
- Multi-column sort API: `df.Sort(keys ...expr.SortKey) (*DataFrame, error)`
- `expr.SortKey` is built via `expr.By("col")` with fluent `.Desc()` and `.NullsFirst()` methods.
- Single-column `Sort(col, desc)` is replaced outright (no external users yet — clean break is fine).
- `expr.SortKey` lives in the `cosma/expr` package alongside other expression types.

### Collect (lazy execution)
- `LazyFrame.Collect(ctx context.Context) (*DataFrame, error)`
- Execution model: logical plan → optimizer passes → lower to `PhysicalPlan` → execute physical nodes against `internal/compute` kernels.
- Physical nodes carry execution-specific config (batch size, allocator). Optimizer passes (predicate pushdown, projection pushdown, limit pushdown) operate on the logical plan before lowering.
- Physical plan skeleton is in `plan/physical.go`; real operator nodes replace the empty stub in Phase 4.

### Join
- API: `df.Join(other *DataFrame, on string, how string) (*DataFrame, error)`
- `how` values: `"inner"`, `"left"` (Phase 3); semi/anti deferred.
- Column name conflicts: non-key columns present in both DataFrames get a `_right` suffix on the right-side column (Polars default). The key column from the right side is dropped (redundant).
- Suffix is `"_right"` by default; no options struct until there is user demand for configurability.

### Boolean null semantics
- `And`/`Or` use **Kleene three-valued logic**: a null input is ignored when the other operand determines the result (`NULL AND FALSE` → `FALSE`; `NULL OR TRUE` → `TRUE`). All other null inputs propagate as null.
- `Not` propagates null (unary, no second operand to short-circuit with).
- Backed by Arrow's `compute.KleeneAnd` / `compute.KleeneOr`.

### GroupBy aggregation API
- `GroupBy().Agg()` accepts `...expr.AggExpr` (compile-time safe).
- Package-level constructors `dataframe.Sum("col")`, `dataframe.Count("col")`, etc. (the `dataframe.Agg` struct) are **removed**. Replaced by `expr.Col("col").Sum()` → `expr.AggExpr`.
- Scalar reduction methods on DataFrame (`df.Sum("col")`, `df.Mean("col")`, etc. → single scalar value) are **kept** as convenience methods. They are a different API from GroupBy aggregation.

### AggExpr (public type)
- Aggregate builder methods (`Sum()`, `Count()`, `Mean()`, `Min()`, `Max()`) return `AggNode` (a concrete public struct), not `Expr`.
- `GroupBy().Agg()` accepts `...AggNode`. Passing a non-aggregate expression to `Agg()` is a **compile-time** error (different type from `Expr`).
- `AggNode` fields: `Op AggOp`, `Inner Expr`, `Alias string`. `.As(name string) AggNode` sets the alias.

### compute.KernelHandler (custom type dispatch)
- `internal/compute` exposes a registration mechanism for custom Arrow DataTypes:
  ```go
  type BinaryKernel func(op BinaryOp, left, right arrow.Array, mem memory.Allocator) (arrow.Array, error)
  type UnaryKernel  func(op UnaryOp, input arrow.Array, mem memory.Allocator) (arrow.Array, error)
  func RegisterBinaryKernel(typeID arrow.Type, k BinaryKernel)
  func RegisterUnaryKernel(typeID arrow.Type, k UnaryKernel)
  ```
- `carray` calls these at `init()` time. `compute.evalBinary` / `evalUnary` check registered kernels before their built-in type switch.
- This is a Phase 2 deliverable — must be in place before `carray` work starts.

### Arrow type coverage (Phase 2 scope)
Phase 2 must cover ALL Arrow primitive and temporal types. The `comparable`, `isNumeric`, and `promoteNumeric` helpers must be extended to handle:
- **Numeric** (already): int8/16/32/64, uint8/16/32/64, float32/float64
- **String/binary** (new): utf8, large_utf8, binary, large_binary — comparable, Count/Min/Max supported
- **Temporal** (new): Timestamp (all timezone variants), Date32, Date64, Time32, Time64, Duration
  - Temporal comparisons: `timestamp_col < Lit(time.Now())`
  - Duration arithmetic: `timestamp - timestamp → duration`, `timestamp + duration → timestamp`
  - `LitTimestamp(t time.Time, tz string) Expr` constructor added to public `expr` package
- **Decimal** (new): Decimal128 (Decimal256 deferred)
- **Deferred**: List, LargeList, FixedSizeList, Struct, Map, Union — complex nesting semantics, separate phase
Phase 2 exit criteria: every expression the public builder can construct either evaluates correctly or is rejected at bind time — **for all types listed above**, never with a runtime "not yet implemented".
