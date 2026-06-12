# Zarr Implementation Plan

Status: Draft
Relates to: Hierarchical N-Dimensional Stores design, ADR 0002 (streaming
boundary), ADR 0003 (logical vs physical plan), ADR 0005 (carray package).

Nine PR-sized slices, ordered so every PR lands green and the first
end-to-end query (`LazyScanStoreManifest(...).Filter(...).Collect(ctx)`)
works by Z6. Codenames Z1–Z9; each section follows the pr-plans format.

Dependency graph:

```
Z1 (SourceScan) ──────────────┐
Z2 (carray geometry) ──┐      │
Z3 (store core) ───────┼──► Z6 (manifest scan)
Z4 (zarr metadata) ────┤      │
Z5 (chunk decode) ─────┴──► Z7 (cell scan + pushdown) ──► Z8 (blosc)
                                                       └─► Z9 (dataset)
```

Z1 and Z2 are independent of everything else and of each other — they can
land in either order or in parallel.

---

## Z1: Generalized scan source (`dataframe.SourceScan` + `ScanHints`)

### Problem

`DataFrameExecutor.ScanStream` hard-rejects any `ScanNode.Handle` that is
not a `FileScan` (`dataframe/executor.go:145`), and eager `Scan` falls
through to `asDF`. No non-file source can execute. Separately, pushdown
hints (`PushedFilters`/`PushedColumns`/`PushedLimit`) are computed at
optimize time, after the scan handle is built, so a plain `open(ctx)`
closure can never honor them — today even file scans ignore
`PushedFilters` entirely.

### Goal

Any package can construct a lazy scan over a custom streaming source, and
the source sees the optimizer's pushdown hints at open time.

### Scope

- `dataframe/stream.go` — `SourceScan`, `ScanHints`
- `dataframe/lazy.go` — `NewLazySourceScan`
- `dataframe/executor.go` — `Scan` / `ScanStream` handle dispatch
- `plan/logical.go` — `ScanSourceCustom` constant
- `dataframe/source_scan_test.go`

### Plan

1. Add `ScanHints{Filters []expr.Expr, Columns []string, Limit int64}`
   (Limit `-1` = none, matching `PushedLimit` zero-value convention).
2. Add `SourceScan{Schema *arrow.Schema, Open func(ctx, ScanHints)
   (array.RecordReader, error), AllowNullable bool}`. Schema is required
   up front (the binder needs it; array/manifest schemas are statically
   known, no probe read needed).
3. Add `NewLazySourceScan(ss SourceScan) *LazyFrame` mirroring
   `NewLazyFileScan` with `plan.ScanSourceCustom`.
4. In `ScanStream`, dispatch on `SourceScan` before erroring: build
   `ScanHints` from the node's pushed fields, call `Open`, keep the
   existing `PushedColumns` projection wrapper as the fallback when the
   source doesn't prune columns itself (project is idempotent).
5. In eager `Scan`, open the reader, drain via `collectReader`, apply
   `PushedLimit` as `FileScan` does.
6. Pushdown stays advisory: surviving Filter/Project/Limit nodes above the
   scan keep results correct regardless of what `Open` honors.

### Tests

- In-memory `SourceScan` (synthetic record batches): Collect and
  CollectStream parity with the equivalent eager DataFrame.
- Hints plumbing: an `Open` that records its `ScanHints` sees pushed
  filters/columns/limit from `Filter`/`Select`/`Limit` above the scan.
- A source that ignores all hints still returns correct results
  (advisory contract).
- Context cancellation mid-stream.

### Risks

- None to existing behavior: `FileScan` paths are untouched; this adds a
  dispatch arm.

### Acceptance Criteria

- A package outside `dataframe` can build a lazy scan over a closure
  source with zero `plan` changes beyond the new constant.
- `Open` observes pushdown hints; ignoring them never corrupts results.

---

## Z2: `cosma/carray` geometry types

### Problem

Selection→chunk pruning math (the n-dimensional analogue of Parquet
row-group pruning) has no home. ADR 0005 reserves `cosma/carray`; the
package does not exist yet.

### Goal

Pure-geometry types — no I/O, no store imports — that carry all indexing
math for chunked arrays, tested exhaustively.

### Scope

- `carray/dim.go` — `Dim`
- `carray/selection.go` — `Slice`, `AxisSel`, `Selection`
- `carray/chunkgrid.go` — `ChunkGrid`, `ChunkIter`
- tests alongside

### Plan

1. `Dim{Name string, Size int64}`; `Slice{Start, Stop, Step}` half-open,
   `Step >= 1`.
2. `Selection{Axes []AxisSel}` orthogonal per-axis; zero value selects
   all. `AxisSel` is one-of `All` / `Index *int64` (drops the axis) /
   `Slice *Slice` / `Indices []int64` (must be strictly monotonic;
   `Validate` rejects otherwise).
3. `Selection.Validate(shape)` and `Selection.ResultShape(shape)`.
4. `ChunkGrid{Shape, ChunkShape []int64}` with `NumChunks`,
   `ChunkCount(axis)`, `ChunkBounds(coord) (lo, hi)` for the `[lo,hi)`
   cell box (edge chunks clipped to Shape).
5. `Intersecting(sel) ChunkIter` — iterator over chunk coords touched by
   the selection, row-major order. `ChunkIter` is a concrete struct with
   `Next() ([]int64, bool)`; no allocation per step beyond the coord
   reuse contract (document: returned slice valid until next `Next`).
6. `Clip(coord, sel) Selection` — the selection intersected with one
   chunk, rebased to chunk-local coordinates. `Intersecting` + `Clip`
   together are the entire pushdown story for dense arrays.

### Tests

- Property-style exhaustive tests on small grids (≤ 4 axes, sizes ≤ 7):
  for every selection shape, brute-force-enumerate selected cells and
  assert `Intersecting`/`Clip`/`ResultShape` agree with the enumeration.
- Edge chunks (Shape not divisible by ChunkShape), Step > 1 crossing
  chunk boundaries, `Index` axis-drop, empty selections, 0-d arrays
  (scalar: Shape `[]`), single-chunk arrays.

### Risks

- This package carries all the indexing math for Z7; an off-by-one here
  is silent data corruption later. Mitigated by brute-force comparison
  tests, not hand-picked cases.

### Acceptance Criteria

- 100% agreement with brute-force enumeration on the property test grid.
- No imports beyond stdlib.

---

## Z3: `cosma/store` core + in-memory test store

### Problem

There is no format-agnostic hierarchy layer (Tree/Group/Array) for
chunked stores.

### Goal

Layer 1 per the design: a dumb, faithful catalog of a store. Drivers
implement a four-method interface; everything else is generic.

### Scope

- `store/store.go` — `Store`, `Entry`, `ObjectKind`, `ErrChunkMissing`
- `store/object.go` — `Object`, `Group`, `Array`, `Attrs`, `CodecSpec`,
  `MemoryOrder`, `Endianness`
- `store/chunk.go` — `ChunkKey`, `ChunkRef`, `ChunkResolver` (optional
  interface)
- `store/tree.go` — `Tree`, `Open`, `OpenOption`
- `store/memstore/memstore.go` — in-memory driver for tests
- tests alongside

### Plan

1. `Store` interface exactly per design: `List`, `Meta`, `ReadChunk`
   (raw compressed bytes; missing chunk → `ErrChunkMissing`), `Close`.
   All methods safe for concurrent use.
2. `Array` carries `Shape`, `ChunkShape`, `DType schema.DType`, `Codecs`,
   `FillValue`, `Order`, `DimNames`, **and `Endianness`** — Zarr v2
   encodes byte order in the dtype (`>f8`), v3 in the `bytes` codec; the
   field normalizes both so the decode pipeline has one place to look.
   (Endianness was missing from the design doc.)
3. `Attrs` typed accessors: `String`, `Strings`, `Int`, `Float`, `Bool`.
4. `ChunkKey{ArrayPath, Coord}` / `ChunkRef{Key, URL, Offset, Length,
   Codecs}`; `ChunkRef` reachable only via optional
   `ChunkResolver interface{ ResolveChunk(ChunkKey) (ChunkRef, error) }`.
5. `Tree`: immutable snapshot, one metadata walk at `Open`, `objects`
   map + deterministic `order` slice. Methods: `Root`, `Object`, `Group`,
   `Array`, `Walk`, `Arrays`, `Store`. Add `Print(w io.Writer)`
   (tree-formatted listing — referenced by the design's mode table but
   absent from its API sketch).
6. `OpenOption`: define the type now; MVP implements eager full walk
   only (`WithPrefix`/`WithDepth` are post-MVP).
7. `memstore`: map-backed driver with builder helpers
   (`memstore.New().AddGroup(path, attrs).AddArray(path, spec,
   chunks)`) so Z4–Z9 tests don't need disk fixtures.

### Tests

- Tree walk determinism, path lookup, group/array kind dispatch.
- Concurrent `Tree` reads under `-race`.
- `ErrChunkMissing` propagation.
- `Print` golden output.

### Risks

- API here is the public driver contract; renames after Z4 ships are
  breaking. Mitigated: signatures lifted verbatim from the reviewed
  design.

### Acceptance Criteria

- `store` imports neither `dataframe` nor `plan` (enforce with a test
  that parses the import list, matching how layering is policed
  elsewhere).
- memstore passes a reusable driver conformance test that `store/zarr`
  will also run in Z4.

---

## Z4: `store/zarr` — v2 + v3 metadata, local filesystem

### Problem

No Zarr driver exists. Zarr v2 and v3 differ in metadata layout
(`.zgroup`/`.zarray`/`.zattrs` vs consolidated `zarr.json`), chunk key
encoding, dtype encoding, and fill-value representation.

### Goal

`zarr.OpenFS(path)` returns a `store.Store` over a local-filesystem Zarr
v2 or v3 store, with all metadata parsed into `store` types. Raw chunk
bytes only — decode is Z5.

### Scope

- `store/zarr/zarr.go` — `OpenFS`, version sniffing
- `store/zarr/v2.go`, `store/zarr/v3.go` — metadata parsing
- `store/zarr/dtype.go` — dtype string → `schema.DType` + endianness
- `store/zarr/fillvalue.go`
- `store/zarr/testdata/` + `store/zarr/testdata/generate.py`
- tests alongside

### Plan

1. Version sniff at root: `zarr.json` → v3; `.zgroup`/`.zarray` → v2;
   both → error (ambiguous store).
2. v2: parse `.zgroup`, `.zarray`, `.zattrs`; honor `dimension_separator`
   (`.` default, `/` supported) for chunk keys; read `_ARRAY_DIMENSIONS`
   into `Array.DimNames` (pass-through, no interpretation).
3. v2 consolidated metadata: if `.zmetadata` exists, build the whole tree
   from it — one read instead of O(nodes). In MVP (design doc deferred
   this; it is cheap and is the difference between 1 and N reads on any
   real store).
4. v3: parse `zarr.json` per node (`node_type` group/array),
   `dimension_names`, `chunk_grid` (regular only; error otherwise),
   `chunk_key_encoding` (`default` and `v2` encodings), codec list
   pass-through into `[]CodecSpec`.
5. dtype mapping: v2 numpy strings (`<f8`, `>i4`, `|b1`, `|u1`, `<f2`,
   `|SN` → `FixedSizeBinary`, `<UN` → error in MVP with a clear message,
   `<M8[unit]` → `Timestamp`) carrying endianness into
   `Array.Endianness`; v3 names (`float64`, `int32`, `bool`, ...).
   Structured dtypes: explicit unsupported error.
6. Fill values: v2 JSON quirks — `"NaN"`, `"Infinity"`, `"-Infinity"`
   strings, base64 for `|SN`, `null` (= undefined: record as Go nil and
   let the cell scan's missing-chunk policy handle it, per design open
   question 3). v3 typed fill values.
7. `ReadChunk`: resolve `ChunkKey.Coord` → chunk file path per
   version/separator; missing file → `store.ErrChunkMissing`. Implement
   `ChunkResolver` (paths are known without reading bytes).
8. Fixtures: `generate.py` (zarr-python, both v2 and v3) committed next
   to its committed output, so fixtures are regenerable but tests don't
   need Python.

### Tests

- Run the Z3 driver conformance suite against fixture stores.
- Golden metadata tests per fixture: shapes, dtypes, endianness, fill
  values, dim names, codec specs.
- Consolidated vs unconsolidated v2 produce identical Trees.
- Both `dimension_separator` values; nested groups; v3 `chunk_key_encoding`
  variants.
- Missing chunk → `ErrChunkMissing`.

### Risks

- Zarr v3 spec surface is large; scope is pinned to regular chunk grids
  and the two standard key encodings, everything else errors loudly.
- Fixture drift: pin the zarr-python version in `generate.py`.

### Acceptance Criteria

- `store.Open(ctx, zarr.OpenFS(...))` yields correct Trees for all
  fixtures, v2 and v3, with zero chunk reads.

---

## Z5: `internal/ndingest` chunk decode (gzip/zstd/lz4, endianness)

### Problem

Raw chunk bytes need to become typed Arrow buffers: codec decompression,
byte-order normalization, and C/F-order handling. No such pipeline
exists. (Blosc — the v2 default — is deliberately split out to Z8; it is
the long pole and nothing structural depends on it.)

### Goal

`ndingest.DecodeChunk(arr *store.Array, raw []byte) (decoded []byte,
error)` plus typed-buffer accessors, mirroring `internal/ingest`'s role
for files.

### Scope

- `internal/ndingest/codec.go` — codec registry + gzip/zlib/zstd/lz4
- `internal/ndingest/decode.go` — pipeline: codecs → byteswap → validate
  length against chunk cell count × dtype width
- tests alongside

### Plan

1. Codec registry keyed by `CodecSpec.ID` so Z8 (blosc) and future codecs
   are drop-ins: `Register(id string, fn DecodeFunc)`.
2. gzip/zlib via stdlib or `klauspost/compress`, zstd via
   `klauspost/compress/zstd`, lz4 via `pierrec/lz4` — both already in the
   module graph as Arrow indirects; they become direct deps.
3. v3 `bytes` codec consumes `Array.Endianness`; v2 path byteswaps when
   `Endianness` is big and the dtype width > 1. Swap in place on the
   decoded buffer.
4. Decoded-length validation: hard error on mismatch (corrupt chunk),
   never silent truncation.
5. F-order chunks: MVP transposes to C-order at decode time (correct,
   simple); revisit only if profiling demands.
6. Take a `memory.Allocator` (from `cosma/memory`) for output buffers,
   matching engine convention.

### Tests

- Round-trip per codec against fixture chunks from Z4's stores.
- Big-endian fixture (one `>f8` array in testdata) decodes to correct
  values.
- Truncated/corrupt chunk → error, no panic.
- F-order fixture matches its C-order twin cell-for-cell.

### Risks

- Low; all formats have reference implementations to test against.

### Acceptance Criteria

- Every non-blosc fixture chunk decodes byte-identically to
  zarr-python's output (golden values embedded in tests).

---

## Z6: `scan.LazyScanStoreManifest`

### Problem

No way to query a Tree as a DataFrame. This is the first user-visible
end-to-end slice.

### Goal

`scan.LazyScanStoreManifest(tree)` returns a LazyFrame catalog of the
store; `Filter(expr.Col("kind").Eq(expr.Lit("array"))).Collect(ctx)`
works.

### Scope

- `scan/store.go` — `LazyScanStoreManifest`
- `scan/store_test.go`

### Plan

1. Schema — all flat columns, because `compute.filterArray` (and
   sort/take) do not support list types yet and `Filter` re-gathers every
   column:
   `path utf8 | kind utf8 | dtype utf8 | ndim int32 | shape utf8 |
   chunk_shape utf8 | dim_names utf8 | nattrs int32 | nchunks int64`,
   where `shape`/`chunk_shape`/`dim_names` are comma-joined (`"100,200"`).
   Revisit as `list<int64>` when list kernels land (tracked separately —
   it is a general engine gap, not a store gap).
2. Build batches from `Tree.Walk` order (deterministic) behind a
   `dataframe.SourceScan` from Z1; metadata is already in memory, so the
   reader is a simple batcher — no I/O at collect time.
3. `nchunks` from `carray.ChunkGrid.NumChunks`; groups get null
   dtype/shape/nchunks columns (`AllowNullable: true`).

### Tests

- Manifest of a memstore tree: golden rows, deterministic order.
- The design doc's target query verbatim:
  `Filter(kind == "array").Collect`.
- CollectStream parity with Collect.
- Manifest of a Z4 zarr fixture.

### Risks

- None; pure metadata.

### Acceptance Criteria

- Design §7's catalog example runs unmodified against a fixture store.

---

## Z7: `scan.LazyScanArray` — cell scan with selection pushdown

### Problem

No path from an n-dim array to RecordBatches. This is the core deliverable
and the only slice that touches plan-level pushdown semantics.

### Goal

Lazy cell-table scan of one array: index columns per kept dim + `value`
column, chunk-pruned by `carray.Selection`, with filter/limit pushdown.

### Scope

- `scan/array.go` — `ArrayScan`, `LazyScanArray`
- `scan/pushdown.go` — pushed filters → `Selection` translation
- `internal/ndingest/cells.go` — chunk → cell-batch expansion
- tests alongside; benchmark in `scan/array_bench_test.go`

### Plan

1. `ArrayScan{Tree, ArrayPath, Dims []string, Selection
   carray.Selection, BatchCells int64}` per design, minus `WithCoords`
   (fast-follow, post-MVP). Dims default to positional `dim_0..` when
   the array has no `DimNames`.
2. Output schema: one `int64` column per kept dim + `value` in the
   array's dtype. Statically computable → `SourceScan.Schema` needs no
   I/O.
3. Execution in `ndingest`: `Intersecting` → bounded-parallel
   `ReadChunk` + `DecodeChunk` (worker pattern from
   `internal/compute/parallel.go`; bound = GOMAXPROCS, order
   not guaranteed — documented like Parquet row groups) → `Clip` →
   unravel kept cells into index columns + value column.
4. Batch invariant: **a batch never spans chunks; a chunk may emit
   multiple batches** when its kept-cell count exceeds `BatchCells`
   (default: min(chunk cell count, 64k)). This replaces the design doc's
   self-contradictory "one chunk → at most one batch" wording.
5. Missing chunks: materialize fill value (default). If fill value is
   undefined (v2 `null`), error with a message pointing at the future
   `WithMissingAsNull` option. Option itself is post-MVP.
6. Pushdown translation (`scan/pushdown.go`), consumed via Z1's
   `ScanHints` at `Open` time:
   - Split conjunctions: walk `BinaryOpAnd` trees into conjuncts (no such
     helper exists yet; lives here, not in `expr`, until a second user
     appears).
   - Recognize `col <op> lit` / `lit <op> col` for
     `Eq/Lt/Lte/Gt/Gte` where col is an index column → tighten that
     axis's `AxisSel`. Everything else is ignored (advisory; the
     surviving Filter node keeps results exact).
   - `Limit` hint → stop the chunk iterator once emitted rows cover it.
   - Pushed columns → drop unreferenced index columns before unravel.
7. Constructor wires `dataframe.NewLazySourceScan`.

### Tests

- Parity oracle: for fixture arrays small enough to enumerate, compare
  scan output (sorted) against brute-force cell enumeration in the test —
  across selections, missing chunks, edge chunks, every supported dtype.
- Pushdown: `Filter(time < 24)` on a chunked axis reads exactly the
  expected chunk subset (memstore counts `ReadChunk` calls).
- Pushdown correctness under partially-honorable predicates (e.g.
  `time < 24 && value > 0`): chunk count drops, results stay exact.
- Limit stops chunk reads early.
- CollectStream memory stays O(batch) on a large fixture (no full
  materialization).
- `-race` over the parallel fetch path.

### Risks

- Highest-complexity slice. Contained by Z2's exhaustively-tested
  geometry (all index math) and the brute-force parity oracle here.
- Unordered output may surprise users; documented prominently on
  `LazyScanArray`.

### Acceptance Criteria

- Design §7's cell-scan example runs (modulo `WithCoords`).
- Chunk-read counts prove pruning; parity oracle proves correctness.

---

## Z8: Blosc codec

### Problem

Zarr v2's default compressor is blosc (typically blosc-lz4 with byte
shuffle). No maintained pure-Go blosc exists; without it most real-world
v2 stores are unreadable. Deliberately isolated: it is the largest single
work item in the driver and nothing else depends on its internals.

### Goal

Pure-Go blosc1 decode registered in the Z5 codec registry: container
header, inner codecs, byte-shuffle, bitshuffle.

### Scope

- `internal/ndingest/blosc/` — header, shuffle, bitshuffle, decode
- registry hookup + tests

### Plan

1. Parse the 16-byte blosc1 header (versions, flags, typesize, nbytes,
   blocksize, cbytes); reject blosc2 frames with a clear error.
2. Inner codecs by flag: lz4/lz4hc (`pierrec/lz4` block format), zstd,
   zlib, snappy (`klauspost`); blosclz is its own small LZ decoder —
   implement from the reference (it is ~200 lines of straightforward LZ).
3. Block loop: blosc splits into blocksize blocks, each independently
   compressed; honor the memcpy'd (uncompressed) flag.
4. Byte-shuffle and bitshuffle inverse transforms, by typesize.
   Straightforward scalar Go first; SIMD never (out of scope).
5. Register as `"blosc"` with config (`cname`, `shuffle`) from
   `CodecSpec.Config`.

### Tests

- Golden chunks: fixtures from Z4's `generate.py` extended with every
  (cname × shuffle) combination zarr-python emits.
- Fuzz the header parser and blocksize/block-count math (`go test -fuzz`)
  — this code eats untrusted bytes.
- Truncated frames error cleanly.

### Risks

- The long pole; that is why it is a standalone PR with golden-data
  tests rather than a bullet inside Z4. blosclz reference behavior is
  stable and small.

### Acceptance Criteria

- Default-config zarr-python v2 stores (blosc-lz4-shuffle) read
  end-to-end through `LazyScanArray`.

---

## Z9: `cosma/dataset` — semantic views and conventions

### Problem

Layer 2 is missing: nothing resolves dim names, roles, or alignment.
Interpretation must be derived, pluggable, and allowed to fail without
poisoning the Tree.

### Goal

`dataset.Interpret(tree, path)` returns an aligned `View` (or a clean
error); `InterpretTree` mirrors the hierarchy with nil Views where
interpretation fails. `scan.Var(view, name)` bridges to Z7.

### Scope

- `dataset/dataset.go` — `Role`, `ArrayRef`, `View`, `TreeView`,
  `Interpret`, `InterpretTree`, `Option`
- `dataset/convention.go` — `Convention`, `XarrayConvention`,
  `BareConvention`
- `scan/var.go` — `Var(v *dataset.View, name string) ArrayScan`
- tests alongside

### Plan

1. Types per design. One correction to the design doc: `ScanVar` cannot
   be a method on `dataset.View` defined in `scan` (no methods on foreign
   types) and `dataset` cannot import `scan` (scan → dataset would
   cycle). It becomes the free function `scan.Var`, keeping imports
   one-directional: `scan → dataset → carray/store`.
2. `XarrayConvention`: detects on `_ARRAY_DIMENSIONS` / v3
   `dimension_names` presence; a 1-D array whose name equals its own dim
   name → `RoleCoord`; others `RoleData`.
3. `BareConvention`: always detects, positional `dim_0..dim_n-1`, all
   `RoleData`. Always last in the chain.
4. `Interpret` consistency checks fail construction (dim name with two
   sizes, coord shape mismatch) — no silent coercion; the group simply
   stays a `Group`.
5. `InterpretTree` walks the tree, nil View on per-group failure
   (siblings need not align).
6. `Option`: `WithConventions(...)` to override the default chain —
   the seam a future `TensorH5Convention` drops into.

### Tests

- xarray-written fixture (in Z4 testdata): coords detected, dims
  resolved, roles correct.
- Bare fixture (no dim metadata) → positional dims via BareConvention.
- Inconsistent group (same dim name, two sizes) → `Interpret` errors;
  `InterpretTree` yields nil View there and valid Views for siblings.
- `scan.Var(...)` end-to-end: design §7's final example, minus
  `WithCoords`.

### Risks

- Convention detection order is observable behavior; document that
  detection is first-match-wins and the chain is overridable.

### Acceptance Criteria

- Both §7 target-experience examples run end-to-end against a real
  zarr-python fixture (with `WithCoords` noted as post-MVP).

---

## Deferred (deliberately, with the seams already in place)

| Item | Seam that accommodates it |
| --- | --- |
| `WithCoords` coord join | `ArrayScan` field exists in design; add after Z9 |
| List-typed manifest columns | list kernels in `internal/compute` (general engine work) |
| Object storage backends | `Store` is already path/byte oriented |
| kerchunk / HDF5 `store/href` driver | `ChunkKey`/`ChunkRef` split + `ChunkResolver` |
| Slices mode + `carray.Array` handoff | ADR 0005 track; `carray` geometry ships in Z2 |
| Lazy subtree `Open` (`WithPrefix`/`WithDepth`) | `OpenOption` type ships in Z3 |
| `WithMissingAsNull` | missing-chunk policy isolated in Z7 step 5 |
| blosc2, sharding codec | codec registry (Z5) |
