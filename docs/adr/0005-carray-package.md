# ADR 0005: N-Dimensional Array Package Named `carray`

## Status

Accepted

## Context

Cosma's stated goal is "a dataframe and n-dimensional array engine for Go."
A DataFrame is fundamentally 2D (rows × typed columns). Use cases beyond 2D —
image batches `(batch, H, W, C)`, time series `(batch, timesteps, features)`,
per-row embeddings, shaped numerical arrays — cannot be cleanly expressed as
DataFrame columns without shoehorning them into nested Arrow List arrays, which
have no kernel support and poor ergonomics.

A dedicated n-dimensional array type is needed. The naming decision is
non-obvious and hard to reverse once the package is public.

Candidates considered:

- **`tensor`** — familiar from ML frameworks, but Apache Arrow has an active
  `Tensor` type being developed for GPU/ML compute. Using the same name would
  create ambiguity for users who know the Arrow ecosystem, and would conflict
  if Cosma later integrates with Arrow's GPU Tensor work.
- **`ndarray`** — immediately recognizable to NumPy users, but implies a NumPy
  clone and the `nd` prefix doesn't tie it to Cosma.
- **`nd`** — short and generic; package would be `cosma/nd` → `nd.Array{...}`.
- **`carray`** (chosen) — "Cosma array." Distinctive, unambiguous, clearly
  Cosma-branded, and leaves `tensor` free for a future Arrow Tensor integration.

## Decision

The n-dimensional array package is named `carray`, at import path
`github.com/karthedew/cosma/carray`. The primary type is `carray.Array`.

Development is a parallel track alongside the dataframe phases, starting after
Phase 1 (public expression API) ships.

## Consequences

- No conflict with Apache Arrow's Tensor type. If Cosma later integrates with
  Arrow GPU Tensors, `cosma/tensor` is available as a distinct package.
- `carray.Array` is intended to become a valid column type in a DataFrame,
  enabling `expr.Col("embedding")` to reference `carray`-typed columns.
- `internal/compute` uses a kernel registration mechanism
  (`compute.RegisterBinaryKernel`, `compute.RegisterUnaryKernel`) so `carray`
  can provide its own kernels without modifying core compute code. This
  registration API is a Phase 2 deliverable.
- The `cosma/expr` type system must eventually resolve `carray` column types
  during `DataType()` calls. This is a documented seam; no implementation
  work is needed until `carray` development begins.
