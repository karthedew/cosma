# Package Guide

| Package | Audience | Stability | Notes |
| --- | --- | --- | --- |
| dataframe | Public | Evolving public API | Main user entry point |
| expr | Public | Stable public API | Public expression AST and fluent builders (ADR 0004) |
| scan | Public | Evolving public API | Streaming readers; lazy scan sources |
| schema | Public | Evolving public API | Schema helpers/types |
| plan | Public | Experimental public API | Logical planning surface |
| gonum | Public | Evolving public API | Export to Gonum mat.Dense / mat.VecDense |
| operator | Repo-visible | Unstable, not an extension API | May move/internalize later |
| internal/ingest | Internal | Internal only | CSV, Parquet, ADBC reader adapters |
| internal/compute | Internal | Internal only | Expression eval kernels, parallel execution |
| internal/expr | Internal | Internal only | Binding/coercion helpers over the public expr tree |
| internal/stream | Internal | Internal only | Reader adapters/stream glue |

## Public API Expectations

Cosma exposes a small set of public packages intended for end users. These
packages are evolving and may change as the execution engine and expression
system mature. The public API is centered on `dataframe` and `scan`, with
`plan` providing an experimental planning surface and `compute` focused on
numerical export and transformations. The `expr` package exposes the public
expression AST (node types, op constants, and fluent builders) used by
`dataframe.Filter`, `WithColumn`, `GroupBy().Agg`, and `Sort`; per ADR 0004 the
node types are a stable, inspectable, serializable contract.

## Internal Packages

The execution engine and expression system are internal-only. Packages under
`internal/` are not stable and should not be imported by external users. These
packages can change freely while the engine evolves.

## Packages Likely to Move

The `operator` package is repo-visible but not a stable extension point. It may
move under `internal/` once the execution model stabilizes or become a
documented public extension surface later.
