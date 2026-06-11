# Cosma Roadmap

_Last updated: 2026-06-10_

This roadmap focuses on shipping a fast, reliable, Arrow-native dataframe engine
for Go. It is ordered by what blocks users today, not by ambition.

## Guiding Principles

1. Correctness before acceleration: no silent coercion bugs, clear null semantics, deterministic behavior.
2. Explicit memory ownership: Arrow retain/release rules are part of the API contract.
3. Streaming by design: avoid full materialization by default on large data paths.
4. Parallel where it matters: partitioned, measurable speedups on CPU-bound operators.
5. Small stable surface: ship thin, composable APIs and harden incrementally.

## Where the code is today

Implemented and tested (full suite green under `go test -race ./...`):

- **Core:** chunked Arrow-native `DataFrame`/`Series`/`Column` with append/flush
  lifecycle, rechunking (`Rechunk`, `RechunkToRows`, `RechunkIfNeeded`).
- **Eager ops:** `Select`/`Drop`/`Rename`, `Limit`/`Head`, `Filter(expr)`,
  `WithColumn(name, expr)`, `Sort(col, descending)` (stable, nulls-last),
  `Sum`/`Count`/`Mean`/`Min`/`Max`, `GroupBy(keys...).Agg(...)`,
  `Concat`/`HStack`.
- **Compute kernels** (`internal/compute`): expression evaluation over record
  batches (comparisons and `+ - * /` arithmetic), boolean-mask filter, single-pass
  and grouped numeric reductions, stable argsort.
- **Expressions** (`internal/expr`): fluent builder with typed literals,
  comparisons, boolean ops, arithmetic, null checks, and aggregate markers.
- **IO:** eager `ReadCSV`/`WriteCSV`/`ReadParquet`/`WriteParquet`; streaming
  `scan.ScanCSV`/`scan.ScanParquet` with batching options; `RecordBatchIter`.
- **Planning:** logical plan (Scan/Filter/Project/Limit) with schema-checked
  `Bind` and `Explain`; `df.Lazy().Filter/Select/Limit().Plan()` builds plans
  but **does not execute them** — `PhysicalPlan` is an empty skeleton.

Explicitly out of scope for v1 (revisit after Phase 5): SQL, window/rolling,
pivot, distributed execution, custom memory manager, GPU.

## Phase 1 — Public Expression API _(highest priority — blocks all external users)_

`df.Filter` and `df.WithColumn` take `internal/expr` types. Go forbids importing
`internal/` packages from outside the module, so **no external user can call the
expression-based API today**. Nothing else on this roadmap matters to users
until this ships.

Features to implement:

- A public `expr` (or `cosma`) package exposing the builder surface:
  `Col`, `Lit`, typed literals, comparisons, `And`/`Or`/`Not`, arithmetic,
  `IsNull`/`IsNotNull`, aggregate constructors, `.As(name)`.
- Keep evaluation internal: the public package builds trees; kernels stay in
  `internal/compute`.
- Update `dataframe` signatures (`Filter`, `WithColumn`, `Lazy().Filter`,
  `GroupBy().Agg`) to accept the public types.
- Reconcile ADR 0002/0003 with the published surface; document the API with
  runnable examples; fix README/docs references to the deleted public
  `compute` package.

Exit criteria: an external module can `go get` Cosma and run a
filter/with-column/groupby pipeline using only public packages.

## Phase 2 — Expression Engine Completeness

The builder accepts more than the engine can evaluate: `compute.Eval` handles
only Column/Literal/Binary nodes, and only comparison + arithmetic binary ops.
Everything else errors at runtime.

Features to implement:

- Boolean kernels: `And`/`Or` (Kleene null semantics, documented) and unary `Not`.
- Unary kernels: `Neg`, `IsNull`, `IsNotNull`.
- `Cast` and `Alias` node evaluation.
- Type-agnostic `Count` (currently routed through numeric-only `Reduce`/
  `GroupReduce`), plus `Min`/`Max` over strings and booleans.
- Defined coercion rules for mixed-type comparisons/arithmetic (no lossy or
  overflow coercion), with conformance tests per Arrow type.
- Clear error messages naming the unsupported op and column type.

Exit criteria: every expression the public builder can construct either
evaluates correctly or is rejected at bind time — never with a runtime
"not yet implemented".

## Phase 3 — Eager Completeness and Performance Baseline

Features to implement:

- **Joins:** eager `df.Join(other, on, how)` for inner and left joins (hash
  join over group-key machinery); semi/anti later.
- **Sort improvements:** multi-column sort with per-key direction; replace the
  boxed-value row reorder in `Sort` with a typed take/gather kernel
  (`compute.Take(chunked, indices)`); nulls-first option.
- **Take/gather kernel:** shared by sort, join, and future shuffles.
- **Benchmarks:** there are currently none. Add `go test -bench` suites for
  filter, sort, groupby, join, and CSV/Parquet scan on representative datasets;
  record baselines in `docs/` so regressions are visible.
- **Context propagation:** plumb `context.Context` through long-running eager
  paths (IO already accepts it for Parquet; extend to compute-heavy ops).

Exit criteria: the eager operator suite covers the common
select/filter/derive/sort/groupby/join workflow, and baseline performance
numbers are documented and repeatable.

## Phase 4 — Lazy Execution

`Lazy()` builds logical plans that nothing can run. Make the lazy API real.

Features to implement:

- `LazyFrame.Collect(ctx)` — compile the bound logical plan onto
  `internal/compute` kernels (not a revived exec stack) and return a
  `*DataFrame`.
- Physical plan: real operator nodes replacing the current empty
  `PhysicalNode` skeleton, with a logical→physical lowering step.
- Lazy coverage parity: `WithColumn`, `Sort`, `GroupBy().Agg`, `Join` as plan
  nodes, not just Filter/Select/Limit.
- Basic optimizer rules: projection pushdown, predicate pushdown into scans
  (wire into `scan` include-columns and row-group filtering for Parquet),
  limit pushdown.
- `Explain` output for both logical and optimized physical plans.

Exit criteria: `df.Lazy()...Collect()` produces results identical to the eager
path on a conformance suite, with pushdowns observable in `Explain`.

## Phase 5 — Streaming Execution

Goal: lazy plans over `scan` sources, processing datasets larger than memory.

Features to implement:

- `scan.ScanCSV/ScanParquet` as lazy sources: `scan → plan → Collect()` without
  materializing the full table (per ADR 0002 streaming boundary).
- Streaming operator execution: filter/project/limit applied per record batch
  as it arrives; pipeline-breaking ops (sort, groupby, join) documented as
  materializing with spill-awareness deferred.
- `CollectStream(ctx)` returning a record-batch iterator instead of a
  DataFrame.
- Cancellation and memory-bounded behavior tests on long scans.

Exit criteria: a filter/project/limit pipeline over a Parquet file larger than
available memory completes with bounded RSS and honors context cancellation.

## Phase 6 — Parallel Execution

Features to implement:

- Partitioned scan and per-chunk parallel kernels (filter/project/arithmetic
  are embarrassingly parallel over chunks).
- Two-phase parallel groupby (`GroupReduce` is already shaped for this) and
  parallel hash join build/probe.
- Merge/reduce stages with deterministic output ordering.
- Configurable parallelism (default `GOMAXPROCS`), operator-level metrics,
  and a documented profiling workflow.

Exit criteria: demonstrated speedups versus the single-thread baseline on the
Phase 3 benchmark suite; everything stays green under `-race`.

## Phase 7 — ADBC Connectivity

Features to implement:

- ADBC reader adapter producing Arrow record batches into the `internal/ingest`
  seam (so databases look like any other scan source).
- Schema mapping and null-semantics alignment; error/retry handling at the
  connector boundary.
- At least one production-ready driver (e.g. PostgreSQL or DuckDB) with an
  end-to-end scan → transform → collect example.

Exit criteria: ADBC ingestion is stable, tested, and documented.

## Phase 8 — Gonum Integration

Features to implement:

- Export helpers from DataFrame/record batches to Gonum matrices/vectors with
  deterministic column ordering.
- Explicit null-handling policy for numerical export (error, drop, or fill —
  caller's choice).
- Conversion benchmarks and shape/ordering/null-semantics tests.

Exit criteria: users can reliably build numerical pipelines from Cosma into
Gonum.

## Ongoing Workstreams

- **Docs accuracy:** README still lists the removed public `compute` package;
  keep `docs/architecture.md` and `docs/packages.md` in sync as the expression
  API goes public.
- **Repo hygiene:** `cmd/cosma-expr` is an uncommitted scratch binary that does
  not compile (`go build ./...` fails at the repo root) — fix or remove so the
  module always builds clean.
- **Memory ownership:** keep retain/release contracts documented and audited
  as new kernels land (see `docs/pr-plans/` pr5/pr6).
- **Observability:** error quality, metrics hooks, profiling support.
- **Compatibility:** backward-compatibility notes for pre-alpha users on every
  surface change.
