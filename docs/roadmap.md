# Cosma Roadmap

_Last updated: 2026-06-12_

## Guiding Principles

1. Correctness before acceleration: no silent coercion bugs, clear null semantics, deterministic behavior.
2. Explicit memory ownership: Arrow retain/release rules are part of the API contract.
3. Streaming by design: avoid full materialization by default on large data paths.
4. Parallel where it matters: partitioned, measurable speedups on CPU-bound operators.
5. Small stable surface: ship thin, composable APIs and harden incrementally.

---

## v0.1.0 — Shipped

### Phase 1 — Public Expression API
Promoted `internal/expr` to the fully public `cosma/expr` package. All AST node types
(`ColumnNode`, `LiteralNode`, `BinaryNode`, `UnaryNode`, `AggNode`, `AliasNode`,
`CastNode`) and op constants are exported. External users can now call `df.Filter`,
`df.WithColumn`, `df.GroupBy().Agg`, and `df.Sort` using only public packages.
See [ADR 0004](adr/0004-public-expression-ast.md).

### Phase 2 — Expression Engine Completeness
Kleene three-valued boolean semantics for `And`/`Or`. Unary kernels (`Not`, `Neg`,
`IsNull`, `IsNotNull`). `Cast` and `Alias` evaluation. Full Arrow type coverage:
string/binary, temporal (`Timestamp`, `Date32/64`, `Time32/64`, `Duration`),
`Decimal128`. Custom kernel registration via `compute.RegisterBinaryKernel` /
`compute.RegisterUnaryKernel` (extension seam for the future `carray` package).

### Phase 3 — Eager Completeness and Performance Baseline
Hash join (`inner` + `left`, `_right` suffix on column conflicts, NaN-safe).
Multi-key stable sort with typed take/gather kernel (`compute.Take`). Benchmarks
for filter, sort, groupby, join, and IO recorded in [`docs/benchmarks.md`](benchmarks.md).
Context propagation on all long-running eager paths.

### Phase 4 — Lazy Execution
`LazyFrame.Collect(ctx)` compiles a logical plan onto `internal/compute` kernels and
returns a `*DataFrame`. Real physical plan nodes, logical→physical lowering, and an
injected `Executor` interface (breaks the `plan ↔ dataframe` import cycle). Optimizer:
predicate pushdown, projection pushdown, limit pushdown into scan nodes. Lazy API parity:
`WithColumn`, `Sort`, `GroupBy().Agg`, `Join` as plan nodes.

### Phase 5 — Streaming Execution
`LazyFrame.CollectStream(ctx)` returns a `dataframe.RecordReader` iterator rather
than a materialized `*DataFrame`. `scan.LazyScanCSV` / `scan.LazyScanParquet`
plug directly into the lazy plan as streaming sources. Filter/project/limit applied
per record batch; sort/groupby/join materialize as documented. Context cancellation
and bounded RSS verified.

### Phase 6 — Parallel Execution
`compute.SetParallelism(n)` controls fan-out. `EvalParallel` fans filter evaluation
across goroutines with errgroup; order-preserving. Two-phase parallel `GroupReduceParallel`
(local partials → merge). `OpMetrics` / `LastMetrics()` for per-operation observability.
Demonstrated 3.1× filter speedup at `p=8` on a 1M-row benchmark.

### Phase 7 — ADBC Connectivity
`internal/ingest.ADBC(ctx, db, cfg)` wraps any `adbc.Database` implementation
(DuckDB, PostgreSQL, FlightSQL, or a mock) as an `array.RecordReader` — the same
seam used by CSV and Parquet. Schema passthrough (ADBC results are already Arrow),
null bitmaps preserved, context cancellation wired. See [`examples/adbc/`](../examples/adbc/).

### Phase 8 — Gonum Integration
`gonum.ToMatrix(df, opts)` → `*mat.Dense` and `gonum.ToVector(s, opts)` →
`*mat.VecDense`. Explicit `NullPolicy` (Error / Drop / Fill) — no silent behavior.
Deterministic column ordering. Returns an error for empty results rather than
panicking into Gonum.

---

## Ongoing

- **Memory ownership**: `NewSeriesFromChunked` uses transfer semantics (caller must
  not Release after passing). `Column` has no `Release` — adding `Retain` without a
  corresponding teardown path would leak. Full retain/release on Column/Series is a
  future hardening item.
- **Observability**: `OpMetrics` / `LastMetrics` is internal-only today. Exposing a
  public metrics hook (or OpenTelemetry span) is unfinished.
- **Backward-compatibility**: the public `expr` AST is the first stable contract.
  All other public packages (`dataframe`, `scan`, `plan`, `gonum`) are evolving and
  may change before v1.0.

---

## What's Next

### Near-term

- **`carray` — n-dimensional array package** (see [ADR 0005](adr/0005-carray-package.md)):
  `carray.Array` as a Cosma-branded n-dimensional array type at import path
  `github.com/karthedew/cosma/carray`. Intended to become a valid column type in
  `DataFrame`, with custom kernels registered via `compute.RegisterBinaryKernel`.

- **Semi / anti joins**: `df.Join(other, on, "semi")` and `"anti"` — hash-based,
  follows the same `_right` suffix semantics as inner/left.

- **Window / rolling functions**: `df.Lazy().WithColumn(expr.Col("x").RollingMean(3))` —
  per-column, bounded window, streaming-compatible.

- **Public parallelism API**: expose `SetParallelism` / `Parallelism` from a public
  `cosma/compute` package so external users can tune fan-out without importing internal.

### Medium-term

- **SQL interface**: thin SQL-to-`LazyFrame` transpiler so users can write
  `cosma.SQL("SELECT * FROM df WHERE age > 30")`.

- **Arrow Flight / distributed execution**: serialize the public expression AST
  (ADR 0004 was designed for this) over Arrow Flight for multi-node fan-out.

- **Spill-to-disk for pipeline-breaking operators**: sort, groupby, and hash join
  currently materialize in memory; spill makes them safe on datasets larger than RAM.

### Distant

- **GPU Tensor integration**: `cosma/tensor` wrapping Apache Arrow's GPU Tensor
  work once the Go bindings stabilize. The `carray` naming (ADR 0005) was chosen
  specifically to leave `cosma/tensor` free for this.

- **Custom memory allocator**: replace `memory.NewGoAllocator()` with a pluggable
  allocator interface for arena / pool strategies.
