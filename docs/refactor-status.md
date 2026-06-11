# Cosma Refactor Status

_Last updated: 2026-06-10_

Refocusing Cosma onto a focused first version: a chunked Arrow DataFrame with
parallel expressions, eager `filter`/`select`/`with_column`, groupby
aggregation, rechunking, and CSV/Parquet IO. Work proceeds via `/tdd`
(red → green → refactor, one vertical slice at a time).

**Explicitly NOT yet in scope:** SQL, joins, window/rolling, pivot, streaming
engine, distributed execution, query optimizer, predicate pushdown, custom
memory manager, GPU.

## Status at a glance

All six approved deepening candidates are **done**. Full suite + `-race` +
`go vet` are green. One eager op (**Sort**) is mid-implementation (see below).

| # | Candidate | Status |
|---|-----------|--------|
| 1 | Deepen the chunked DataFrame module | ✅ Done |
| 2 | One source→chunk ingestion seam (`internal/ingest`) | ✅ Done |
| 3 | Eager DataFrame ops on top of the seam | ✅ Done (Sort in progress) |
| 4 | Collapse the dead pass-through exec stack | ✅ Done |
| 5 | Centralize Arrow↔Cosma schema translation | ✅ Done |
| 6 | Remove placeholder public `compute` | ✅ Done |

## What changed this session

### Eager DataFrame ops (candidate #3)
Each shipped as its own red→green TDD slice:

- **Select / Drop / Rename** — zero-copy, validated against unknown/duplicate columns.
- **Limit** — chunk-aware, delegates to `Head`.
- **Filter(expr)** — evaluates a boolean predicate per canonical chunk, gathers survivors.
- **WithColumn(name, expr)** — derives/replaces a column from an expression.
- **Sum / Count / Mean / Min / Max** — whole-column reductions (boxed scalars).
- **GroupBy(keys...).Agg(...)** — hash group-by with `Sum/Count/Mean/Min/Max(col).As(name)`,
  groups emitted in first-appearance order.
- **Concat / HStack** — vertical (zero-copy chunk concat) and horizontal combination.

These run on a new **`internal/compute`** package — the canonical chunk-kernel
home. It depends only on Arrow + `internal/expr` and **never imports
`dataframe`**, so eager ops call into it without an import cycle. It provides:

- `Eval(expr, record, mem)` — Column / Literal / Binary nodes; comparisons → bool,
  arithmetic `+ - * /` (integer divide-by-zero → null).
- `FilterRecord(rec, mask, mem)` — boolean-mask take kernel.
- `Reduce(chunked)` → `Aggregates{Count, Sum, Min, Max, Mean}`.
- `GroupReduce(groupIDs, numGroups, chunked)` — per-group fold (two-phase ready).
- `BoxedValues` / `BuildArray` — boxed row extraction and typed array construction
  for group keys and outputs.
- Generic per-type `gather` / `cmp` / `arith` / `reduce` kernels.

### Cleanup (candidates #4–#6)

- **#6:** Deleted the dead public `compute/` package (filter/groupby/join/project,
  all "not implemented" stubs, imported by nobody). Rewrote `cmd/cosma-dev` to
  demonstrate real `df.GroupBy(...).Agg(...)`.
- **#5:** Moved Arrow↔Cosma translation into `schema/translate.go`
  (`FromArrow`, `ToArrow`, `FieldFromArrow`, `DTypeFromArrow`); deleted the
  dataframe-local copies and pointed all call sites at `schema.*`.
- **#4:** Deleted `internal/exec/` (eval/compile/pipeline) and `operator/`
  (filter/limit/map/project) — both dead in the live import graph
  (exec had zero non-test importers; operator was imported only by exec),
  superseded by `internal/compute`. The two `scan` tests that were exec's only
  consumers were rewritten to test `ScanCSV`/`ScanParquet` batching directly.
  `plan/` is **kept** (still used by `dataframe/lazy.go` and `df.Plan()` for the
  lazy-API preview, per ADR 0003).

## Current package layout

```
dataframe/         core type + all eager ops
internal/compute/  the real chunk kernels (Arrow + internal/expr only)
internal/ingest/   source → RecordReader seam
internal/expr/     fluent expression tree (still internal)
internal/stream/   streaming helpers
scan/              public streaming scan faces
plan/              logical plan (internal; only the lazy preview uses it)
schema/            types + Arrow translation
memory/            allocator helpers
```

**Removed:** public `compute/`, `internal/exec/`, `operator/`.

## Public eager API surface

All ops are immutable (return a new `*DataFrame`, receiver unchanged) and
zero-copy where possible.

- **Shape / identity:** `NumRows`, `NumCols`, `Columns`, `Schema`, `Column`, `Head`, `Limit`
- **Chunking:** `NumChunks`, `ChunkSizes`, `Rechunk`, `RechunkToRows`, `ShouldRechunk`, `RechunkIfNeeded`
- **Reshaping:** `Select`, `Drop`, `Rename`
- **Compute:** `Filter(expr)`, `WithColumn(name, expr)` — expressions via the
  `internal/expr` builder (`Col`, `Lit`, `.Gt`, `.Add`, …)
- **Aggregation:** `Sum`, `Count`, `Mean`, `Min`, `Max`(col);
  `GroupBy(keys...).Agg(Sum/Count/Mean/Min/Max(col).As(name))`
- **Combination:** `Concat(dfs...)`, `df.HStack(others...)`
- **IO:** `ReadCSV`, `WriteCSV`, `ReadParquet`, `WriteParquet`; `RecordBatchIter` for streaming

## Where we're going next

1. **Sort** _(in progress)_ — the one remaining Phase-4 eager op. The
   `internal/compute.SortIndices(chunked, descending)` argsort kernel
   (stable, nulls-last) is written; `df.Sort(col, descending)` and its
   green tests are not yet finished.
2. **Compute type-coverage gaps** — wire boolean `And`/`Or`/`Not` and
   `IsNull`/`IsNotNull` into `compute.Eval` (only comparisons + arithmetic are
   wired today); make `Count` type-agnostic (it currently routes through the
   numeric-only `Reduce`/`GroupReduce`); handle `Cast`/`Alias`/`Agg` nodes.
3. **Phase 8 lazy API** — public lazy `.Select`/`.Filter`/`.Limit`/`.Collect()`
   that build a `plan.LogicalPlan` and compile onto `internal/compute`
   (not a revived exec). This is also where the public expression API
   (promoting `internal/expr`) and the ADR 0002/0003 reconciliation land.

## Conventions

- **TDD:** write the failing test → confirm RED → minimal GREEN → run
  `go test -race ./...` + `go vet ./...`.
- **Test helpers** (in `dataframe/*_test.go`): `multiChunkInt64(t, name, [][]int64)`,
  `int64Values` / `float64Values` / `stringValues(t, df, name)`.
- **Arrow refcounting gotcha:** `NewChunkedColumn` / `NewSeriesFromChunked` take
  ownership **without** retaining, so don't double-`Release` a `*arrow.Chunked`
  you've handed to a `Series` (this bit `WithColumn` once during development).
