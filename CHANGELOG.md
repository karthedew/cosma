# Changelog

All notable changes to this project will be documented in this file.

## [v0.1.0] — 2026-06-12

First tagged pre-release. All 8 planned phases shipped and green under
`go test -race ./...`.

### Added

- **Public expression AST** (`cosma/expr`): fully public node types, op constants,
  and fluent builder. External modules can now call `df.Filter`, `df.WithColumn`,
  `df.GroupBy().Agg`, and `df.Sort` using only public packages. ([ADR 0004](docs/adr/0004-public-expression-ast.md))
- **Expression engine completeness**: Kleene boolean semantics, unary kernels (`Not`,
  `Neg`, `IsNull`, `IsNotNull`), `Cast`/`Alias` evaluation, full Arrow type coverage
  (string, temporal, `Decimal128`). Custom kernel registration via
  `compute.RegisterBinaryKernel` / `compute.RegisterUnaryKernel`.
- **Hash join** (`df.Join`): inner and left, `_right` suffix on column conflicts,
  NaN-safe. Multi-key stable sort with typed take/gather kernel. Benchmarks for
  filter, sort, groupby, join, and IO recorded in `docs/benchmarks.md`.
- **Lazy execution** (`LazyFrame.Collect(ctx)`): logical plan → optimizer
  (predicate/projection/limit pushdown) → physical plan → `DataFrameExecutor`.
  Injected `plan.Executor` interface breaks the `plan ↔ dataframe` import cycle.
  Full lazy API parity: `WithColumn`, `Sort`, `GroupBy().Agg`, `Join` as plan nodes.
- **Streaming execution** (`LazyFrame.CollectStream(ctx)`): per-batch iterator over
  large-than-memory datasets. `scan.LazyScanCSV` / `scan.LazyScanParquet` as
  streaming lazy sources. Context cancellation and bounded RSS.
- **Parallel execution**: `compute.EvalParallel` fans filter evaluation across
  goroutines; two-phase `GroupReduceParallel`. `OpMetrics` / `LastMetrics()`
  observability. 3.1× filter speedup at 8 workers on 1M-row benchmark.
- **ADBC connectivity** (`internal/ingest.ADBC`): driver-agnostic adapter wrapping
  any `adbc.Database` as an `array.RecordReader`. `github.com/apache/arrow-adbc/go/adbc v1.11.0`.
- **Gonum integration** (`cosma/gonum`): `ToMatrix` → `*mat.Dense`,
  `ToVector` → `*mat.VecDense`, explicit `NullPolicy` (Error/Drop/Fill).
- **Arrow module upgrade**: migrated from `github.com/apache/arrow/go/v18` to
  `github.com/apache/arrow-go/v18` v18.6.0 across all packages.

## [Unreleased]

- `carray` n-dimensional array package (see [ADR 0005](docs/adr/0005-carray-package.md))
- Public parallelism API
- Semi/anti joins
- Window/rolling functions
