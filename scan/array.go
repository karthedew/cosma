package scan

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/karthedew/cosma/carray"
	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/internal/ndingest"
	"github.com/karthedew/cosma/store"
)

// defaultBatchCells is the per-batch kept-cell ceiling used when ArrayScan
// leaves BatchCells unset. The effective ceiling is min(this, chunk cell count)
// so a batch never spans chunks.
const defaultBatchCells int64 = 65536

// ArrayScan describes a lazy cell-table scan of one store array. The output is a
// flat table: one index column per KEPT dimension, plus a final "value" column
// in the array's dtype. An axis selected by a single Index is dropped from the
// output (its position is fixed, so it carries no information per row). By
// default an index column holds the int64 cell index; a dim listed in CoordPaths
// instead holds its coordinate's VALUES (WithCoords).
//
// WithMissingAsNull (mapping missing chunks to null rather than fill value) is
// post-MVP and intentionally absent.
type ArrayScan struct {
	// Tree is the open store hierarchy the array lives in.
	Tree *store.Tree
	// ArrayPath is the absolute path of the array within Tree.
	ArrayPath string
	// Dims names the output index columns, one per array axis (including axes a
	// single Index will drop — the name of a dropped axis is simply never
	// emitted). When empty, names default to the array's DimNames, or to
	// "dim_0".."dim_{n-1}" when the array carries no DimNames.
	Dims []string
	// Selection restricts which cells are scanned. The zero value scans every
	// cell. Single-Index axes are dropped from the output schema.
	Selection carray.Selection
	// BatchCells caps the kept-cell count of one output batch. <= 0 means the
	// default (min(defaultBatchCells, chunk cell count)). A batch never spans
	// chunks regardless of this value.
	BatchCells int64
	// CoordPaths maps a dimension name to the absolute store path of its 1-D
	// coordinate array (WithCoords). For each kept dim present here, the output
	// column holds the coordinate's VALUES (in the coordinate's dtype) instead of
	// the integer index; dims absent here keep their int64 index column. The
	// coordinate array must be 1-D with extent equal to the dim it labels.
	// scan.Var(v, name, scan.WithCoords()) populates this from a dataset.View;
	// it may also be set by hand.
	CoordPaths map[string]string
}

// LazyScanArray returns a LazyFrame that scans one store array as a cell table.
//
// Output schema (statically computable, no I/O):
//
//	<kept dim 0> int64 | ... | <kept dim k> int64 | value <array dtype>
//
// A dim selected by a single carray Index is dropped from the schema. Index
// column names come from ArrayScan.Dims, else the array's DimNames, else
// positional "dim_0"..; the value column is always named "value".
//
// OUTPUT IS UNORDERED. Chunks are fetched and decoded in bounded parallel
// (GOMAXPROCS workers) and emitted in completion order, exactly like Parquet row
// groups: row order across chunks is not guaranteed. Wrap the frame in a Sort if
// you need a deterministic order — that is the caller's choice, not paid for by
// every scan.
//
// A kept dim listed in ArrayScan.CoordPaths emits its coordinate's values (in
// the coordinate's dtype) instead of the int64 index — the schema column type
// reflects this. Coordinate values are loaded lazily per data chunk and each
// coordinate chunk is decoded at most once (cached for the scan).
//
// Behind the SourceScan, advisory ScanHints are honored to PRUNE work only:
// range predicates on kept index columns tighten the carray.Selection (fewer
// chunks read), a column projection drops unreferenced index columns from the
// output (never changing which cells are read), and a limit stops the chunk
// iterator early. Predicates on a coord-valued column are NOT pushed (their
// literals are coordinate values, not indices). The Filter/Project/Limit nodes
// above the scan keep results exact regardless of what was honored.
//
// Missing chunks (store.ErrChunkMissing) materialize the array's FillValue. If
// FillValue is nil (undefined), the scan errors — a future WithMissingAsNull
// option will map such chunks to null instead.
func LazyScanArray(s ArrayScan) *dataframe.LazyFrame {
	plan, err := planArrayScan(s)
	if err != nil {
		// Defer the error to Open time so the constructor stays non-failing and
		// matches the LazyScan* family: a schema is still required up front, so
		// fall back to an empty single-column schema the binder can accept.
		fallback := arrow.NewSchema([]arrow.Field{
			{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		}, nil)
		return dataframe.NewLazySourceScan(dataframe.SourceScan{
			Schema:        fallback,
			AllowNullable: true,
			Open: func(context.Context, dataframe.ScanHints) (array.RecordReader, error) {
				return nil, err
			},
		})
	}

	return dataframe.NewLazySourceScan(dataframe.SourceScan{
		Schema:        plan.outSchema,
		AllowNullable: true,
		Open: func(ctx context.Context, hints dataframe.ScanHints) (array.RecordReader, error) {
			return plan.open(ctx, hints, memory.DefaultAllocator)
		},
	})
}

// arrayPlan is the resolved, I/O-free shape of a scan: everything statically
// derivable from the array metadata and the base selection. open() turns it into
// a streaming reader at collect time.
type arrayPlan struct {
	tree       *store.Tree
	arr        *store.Array
	grid       carray.ChunkGrid
	baseSel    carray.Selection
	outSchema  *arrow.Schema
	valueType  arrow.DataType
	width      int
	batchCells int64

	// keptAxes are the array-axis indices that survive into the output (not
	// single-Index axes), in axis order. keptNames[i] names keptAxes[i].
	keptAxes  []int
	keptNames []string

	// Coordinate columns (WithCoords), one entry per kept axis. keptCoordPath[i]
	// is the coord array's path for kept axis i, or "" when that dim has no
	// coordinate (a plain int64 index column). keptCoordType[i] is the coord
	// column's Arrow type when coord'd, else nil. coordDims is the set of
	// coord-valued dim names, passed to applyPushdown so their predicates do not
	// tighten the selection.
	keptCoordPath []string
	keptCoordType []arrow.DataType
	coordDims     map[string]bool
}

// planArrayScan resolves the array and computes the static plan. It performs no
// chunk I/O.
func planArrayScan(s ArrayScan) (*arrayPlan, error) {
	if s.Tree == nil {
		return nil, fmt.Errorf("scan: ArrayScan.Tree is nil")
	}
	arr, ok := s.Tree.Array(s.ArrayPath)
	if !ok {
		return nil, fmt.Errorf("scan: no array at %q", s.ArrayPath)
	}
	if err := s.Selection.Validate(arr.Shape); err != nil {
		return nil, fmt.Errorf("scan: invalid selection for %q: %w", s.ArrayPath, err)
	}

	valueType, err := arrowTypeForDType(arr.DType)
	if err != nil {
		return nil, fmt.Errorf("scan: array %q: %w", s.ArrayPath, err)
	}
	width, err := dtypeWidth(arr.DType)
	if err != nil {
		return nil, fmt.Errorf("scan: array %q: %w", s.ArrayPath, err)
	}

	names := resolveDimNames(s.Dims, arr)

	// Determine which axes are kept (dropped iff selected by a single Index).
	var keptAxes []int
	var keptNames []string
	for axis := range arr.Shape {
		if selAxisIsIndex(s.Selection, axis) {
			continue
		}
		keptAxes = append(keptAxes, axis)
		keptNames = append(keptNames, names[axis])
	}

	// Resolve coordinate columns (WithCoords): for each kept dim named in
	// CoordPaths, the column takes the coordinate array's dtype instead of int64.
	keptCoordPath := make([]string, len(keptAxes))
	keptCoordType := make([]arrow.DataType, len(keptAxes))
	var coordDims map[string]bool
	for i, name := range keptNames {
		path, ok := s.CoordPaths[name]
		if !ok {
			continue
		}
		carr, ok := s.Tree.Array(path)
		if !ok {
			return nil, fmt.Errorf("scan: coordinate %q for dim %q: no array at %q", path, name, path)
		}
		if carr.Ndim() != 1 {
			return nil, fmt.Errorf("scan: coordinate %q for dim %q must be 1-D, got rank %d", path, name, carr.Ndim())
		}
		if carr.Shape[0] != arr.Shape[keptAxes[i]] {
			return nil, fmt.Errorf("scan: coordinate %q size %d != dim %q extent %d",
				path, carr.Shape[0], name, arr.Shape[keptAxes[i]])
		}
		ct, err := arrowTypeForDType(carr.DType)
		if err != nil {
			return nil, fmt.Errorf("scan: coordinate %q for dim %q: %w", path, name, err)
		}
		keptCoordPath[i] = path
		keptCoordType[i] = ct
		if coordDims == nil {
			coordDims = make(map[string]bool)
		}
		coordDims[name] = true
	}

	fields := make([]arrow.Field, 0, len(keptAxes)+1)
	for i, name := range keptNames {
		ct := keptCoordType[i]
		if ct == nil {
			ct = arrow.PrimitiveTypes.Int64
		}
		fields = append(fields, arrow.Field{Name: name, Type: ct})
	}
	fields = append(fields, arrow.Field{Name: "value", Type: valueType})
	outSchema := arrow.NewSchema(fields, nil)

	batch := s.BatchCells
	if batch <= 0 {
		batch = defaultBatchCells
		if cc := chunkCellCount(arr.ChunkShape); cc > 0 && cc < batch {
			batch = cc
		}
	}

	return &arrayPlan{
		tree:          s.Tree,
		arr:           arr,
		grid:          carray.ChunkGrid{Shape: arr.Shape, ChunkShape: arr.ChunkShape},
		baseSel:       s.Selection,
		outSchema:     outSchema,
		valueType:     valueType,
		width:         width,
		batchCells:    batch,
		keptAxes:      keptAxes,
		keptNames:     keptNames,
		keptCoordPath: keptCoordPath,
		keptCoordType: keptCoordType,
		coordDims:     coordDims,
	}, nil
}

// resolveDimNames returns one output name per array axis. Explicit Dims win;
// then the array's DimNames; then positional "dim_i". Index axes still get a
// name (it is simply never emitted), keeping the slice axis-aligned.
func resolveDimNames(dims []string, arr *store.Array) []string {
	n := arr.Ndim()
	out := make([]string, n)
	switch {
	case len(dims) == n:
		copy(out, dims)
	case len(arr.DimNames) == n:
		copy(out, arr.DimNames)
	default:
		for i := 0; i < n; i++ {
			out[i] = fmt.Sprintf("dim_%d", i)
		}
	}
	return out
}

// chunkCellCount returns prod(chunkShape); a 0-d chunk has one cell.
func chunkCellCount(chunkShape []int64) int64 {
	n := int64(1)
	for _, d := range chunkShape {
		n *= d
	}
	return n
}

// open builds the streaming reader for a collect. It applies pushdown, plans the
// touched chunks, and returns a reader that fetches+decodes chunks in bounded
// parallel and slices kept cells into batches that never span chunks.
func (p *arrayPlan) open(ctx context.Context, hints dataframe.ScanHints, mem memory.Allocator) (array.RecordReader, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	if ctx == nil {
		ctx = context.Background()
	}

	pd := applyPushdown(
		dataframeScanHints{Filters: hints.Filters, Columns: hints.Columns, Limit: hints.Limit},
		p.baseSel,
		p.keptNames,
		p.keptAxes,
		p.arr.Shape,
		p.coordDims,
	)

	// The output schema after projection drops unreferenced kept index columns.
	// outCols holds kept-column positions (indices into p.keptAxes / cellRows.idx)
	// that survive projection, in output order. A coord-valued kept column carries
	// the coordinate's dtype; otherwise an int64 index column.
	outFields := make([]arrow.Field, 0, len(p.keptAxes)+1)
	outCols := make([]int, 0, len(p.keptAxes))
	for i := range p.keptAxes {
		if pd.keepOutput[i] {
			ct := p.keptCoordType[i]
			if ct == nil {
				ct = arrow.PrimitiveTypes.Int64
			}
			outFields = append(outFields, arrow.Field{Name: p.keptNames[i], Type: ct})
			outCols = append(outCols, i)
		}
	}
	outFields = append(outFields, arrow.Field{Name: "value", Type: p.valueType})
	outSchema := arrow.NewSchema(outFields, nil)

	loader, err := newCoordLoader(p, mem)
	if err != nil {
		return nil, err
	}

	// Collect the touched chunk coordinates up front (cheap, metadata only) so
	// the parallel workers can pull from a shared queue.
	var coords [][]int64
	it := p.grid.Intersecting(pd.sel)
	for {
		c, ok := it.Next()
		if !ok {
			break
		}
		cp := make([]int64, len(c))
		copy(cp, c)
		coords = append(coords, cp)
	}

	r := &arrayReader{
		ctx:       ctx,
		mem:       mem,
		plan:      p,
		sel:       pd.sel,
		limit:     pd.limit,
		outSchema: outSchema,
		outCols:   outCols,
		coords:    coords,
		coordLoad: loader,
		results:   make(chan chunkResult, max1(runtime.GOMAXPROCS(0))),
	}
	r.start()
	return r, nil
}

func max1(n int) int {
	if n < 1 {
		return 1
	}
	return n
}

// chunkResult is one decoded chunk's kept cells, ready to be sliced into
// batches, or a fatal error from fetch/decode.
type chunkResult struct {
	rows cellRows
	err  error
}

// arrayReader is the array.RecordReader behind a LazyScanArray collect. Workers
// fetch+decode chunks in parallel and push cellRows to results; Next drains one
// chunk's rows at a time and slices them into batches of at most batchCells,
// never spanning chunks.
type arrayReader struct {
	ctx  context.Context
	mem  memory.Allocator
	plan *arrayPlan
	sel  carray.Selection

	limit     int64
	outSchema *arrow.Schema
	outCols   []int
	coordLoad *coordLoader

	coords  [][]int64
	results chan chunkResult

	once     sync.Once
	cancel   context.CancelFunc
	emitted  int64 // rows already emitted (for the limit budget)
	produced int64 // rows decoded so far across workers (atomic; limit gating)

	// pending holds the current chunk's rows being sliced into batches.
	pending    cellRows
	pendingOff int64

	cur arrow.Record
	err error
	eof bool

	refCount int64
}

// start launches the bounded worker pool. Workers pull chunk coords from a
// shared index and push results; a closer goroutine closes results once all
// workers finish.
func (r *arrayReader) start() {
	r.refCount = 1
	ctx, cancel := context.WithCancel(r.ctx)
	r.cancel = cancel

	if len(r.coords) == 0 {
		close(r.results)
		return
	}

	n := max1(runtime.GOMAXPROCS(0))
	if n > len(r.coords) {
		n = len(r.coords)
	}
	// When a limit is set, cap the worker count to the number of chunks that
	// could plausibly be needed (each chunk yields at least one kept cell, so
	// `limit` workers can over-read by at most worker-count-1 chunks). This keeps
	// the limit's chunk-read pruning effective without serializing the pool.
	if r.limit >= 0 {
		need := r.limit + 1 // +1 so a worker is available to find end-of-stream.
		if int64(n) > need {
			n = int(need)
		}
		n = max1(n)
	}

	var next int64 = -1
	var nextMu sync.Mutex
	grab := func() (int, bool) {
		// Stop dispatching once enough rows have been decoded to satisfy the
		// limit (best-effort; the exact PhysLimit above enforces the count).
		if r.limit >= 0 && atomic.LoadInt64(&r.produced) >= r.limit {
			return 0, false
		}
		nextMu.Lock()
		next++
		i := next
		nextMu.Unlock()
		if int(i) >= len(r.coords) {
			return 0, false
		}
		return int(i), true
	}

	var wg sync.WaitGroup
	for w := 0; w < n; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				i, ok := grab()
				if !ok {
					return
				}
				rows, err := r.fetchChunk(ctx, r.coords[i])
				if err == nil {
					atomic.AddInt64(&r.produced, int64(rows.len()))
				}
				select {
				case r.results <- chunkResult{rows: rows, err: err}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(r.results)
	}()
}

func (r *arrayReader) Schema() *arrow.Schema          { return r.outSchema }
func (r *arrayReader) Record() arrow.RecordBatch      { return r.cur }
func (r *arrayReader) RecordBatch() arrow.RecordBatch { return r.cur }
func (r *arrayReader) Retain()                        { r.refCount++ }

func (r *arrayReader) Err() error {
	if r.err != nil && errors.Is(r.err, errEOF) {
		return nil
	}
	return r.err
}

// errEOF is an internal sentinel distinguishing clean end-of-stream from a real
// error in r.err. It is never surfaced to callers (Err filters it out).
var errEOF = errors.New("scan: eof")

func (r *arrayReader) Release() {
	r.refCount--
	if r.refCount != 0 {
		return
	}
	r.once.Do(func() {
		if r.cancel != nil {
			r.cancel()
		}
		// Drain the channel so workers blocked on send can exit.
		go func() {
			for range r.results {
			}
		}()
		r.coordLoad.release()
	})
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
}

func (r *arrayReader) Next() bool {
	if r.eof || r.err != nil {
		return false
	}
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}

	for {
		if err := r.ctx.Err(); err != nil {
			r.err = err
			return false
		}
		if r.err != nil {
			return false // e.g. a coord-load error surfaced by nextBatch.
		}
		if r.limit >= 0 && r.emitted >= r.limit {
			r.finish()
			return false
		}

		// Slice the current pending chunk into the next batch.
		if r.pendingOff < int64(r.pending.len()) {
			rec := r.nextBatch()
			if rec == nil {
				continue
			}
			r.cur = rec
			return true
		}

		// Pull the next decoded chunk.
		res, ok := <-r.results
		if !ok {
			r.finish()
			return false
		}
		if res.err != nil {
			r.err = res.err
			if r.cancel != nil {
				r.cancel()
			}
			return false
		}
		r.pending = res.rows
		r.pendingOff = 0
	}
}

// finish marks clean end-of-stream and tears down the worker pool.
func (r *arrayReader) finish() {
	r.eof = true
	if r.cancel != nil {
		r.cancel()
	}
}

// nextBatch builds one record from the pending chunk's rows: at most batchCells
// rows, capped by the remaining limit budget. Returns nil only if the batch
// would be empty (which Next treats as advance).
func (r *arrayReader) nextBatch() arrow.Record {
	total := int64(r.pending.len())
	start := r.pendingOff
	count := r.plan.batchCells
	if start+count > total {
		count = total - start
	}
	if r.limit >= 0 {
		remaining := r.limit - r.emitted
		if remaining <= 0 {
			r.pendingOff = total
			return nil
		}
		if count > remaining {
			count = remaining
		}
	}
	if count <= 0 {
		r.pendingOff = total
		return nil
	}

	rec, err := r.pending.record(r.mem, r.outSchema, r.outCols, int(start), int(count), r.coordLoad)
	if err != nil {
		r.err = err
		if r.cancel != nil {
			r.cancel()
		}
		return nil
	}
	r.pendingOff += count
	r.emitted += count
	return rec
}

// fetchChunk reads, decodes, and unravels the kept cells of one chunk into
// cellRows. A missing chunk materializes the fill value (or errors if the fill
// value is undefined). The returned rows are chunk-global cell indices plus
// values; ordering within the chunk is C order over the kept cells.
func (r *arrayReader) fetchChunk(ctx context.Context, coord []int64) (cellRows, error) {
	a := r.plan.arr
	raw, err := r.plan.tree.Store().ReadChunk(ctx, store.ChunkKey{ArrayPath: a.ArrayPath, Coord: coord})

	local := r.plan.grid.Clip(coord, r.sel)
	lo, _ := r.plan.grid.ChunkBounds(coord)

	if err != nil {
		if errors.Is(err, store.ErrChunkMissing) {
			return r.fillChunk(coord, local, lo)
		}
		return cellRows{}, fmt.Errorf("scan: read chunk %v of %q: %w", coord, a.ArrayPath, err)
	}

	decoded, err := ndingest.DecodeChunk(a, raw, r.mem)
	if err != nil {
		return cellRows{}, fmt.Errorf("scan: decode chunk %v of %q: %w", coord, a.ArrayPath, err)
	}
	defer r.mem.Free(decoded)

	return r.expandChunk(decoded, local, lo, coord)
}

// fillChunk produces kept-cell rows for a missing chunk, materializing the
// array's FillValue. It errors when FillValue is undefined (nil).
func (r *arrayReader) fillChunk(coord []int64, local carray.Selection, lo []int64) (cellRows, error) {
	a := r.plan.arr
	if a.FillValue == nil {
		return cellRows{}, fmt.Errorf(
			"scan: chunk %v of %q is missing and the array has no fill value "+
				"(a future WithMissingAsNull option will map missing chunks to null)",
			coord, a.ArrayPath)
	}
	fv, err := coerceFillValue(a.FillValue, r.plan.valueType)
	if err != nil {
		return cellRows{}, fmt.Errorf("scan: array %q fill value: %w", a.ArrayPath, err)
	}
	fb, err := encodeFillValue(fv, r.plan.valueType)
	if err != nil {
		return cellRows{}, fmt.Errorf("scan: array %q fill value: %w", a.ArrayPath, err)
	}

	rows := newCellRows(r.plan.valueType, r.plan.width, r.plan.keptAxes)
	r.walkKept(local, lo, func(globalIdx []int64) {
		rows.appendFill(globalIdx, fb)
	})
	return rows, nil
}

// expandChunk unravels the decoded chunk buffer's kept cells into rows.
func (r *arrayReader) expandChunk(decoded []byte, local carray.Selection, lo []int64, coord []int64) (cellRows, error) {
	rows := newCellRows(r.plan.valueType, r.plan.width, r.plan.keptAxes)
	chunkShape := r.plan.arr.ChunkShape
	width := r.plan.width

	r.walkKept(local, lo, func(globalIdx []int64) {
		// Compute the C-order offset of this cell within the full (padded) chunk
		// buffer using its chunk-local index (globalIdx - lo).
		off := cOffset(globalIdx, lo, chunkShape)
		rows.appendCell(globalIdx, decoded, int(off)*width)
	})
	return rows, nil
}

// walkKept enumerates, in C order, every kept cell of one chunk. local is the
// chunk-local selection (from Clip); lo is the chunk's cell-space origin. fn is
// called with the GLOBAL multi-index of each kept cell (chunk-local + lo). The
// slice passed to fn is reused; callers must copy what they retain.
func (r *arrayReader) walkKept(local carray.Selection, lo []int64, fn func(globalIdx []int64)) {
	rank := len(r.plan.arr.Shape)
	if rank == 0 {
		// 0-d scalar: exactly one cell, no index.
		fn(nil)
		return
	}

	// Per-axis local positions (ascending). For an Index axis Clip yields a
	// single-element selector; positions() returns that one local position.
	// Clip selectors are already rebased to [0, extent), so enumerating against
	// the chunk's (edge-clipped) extent is correct.
	perAxis := make([][]int64, rank)
	for axis := 0; axis < rank; axis++ {
		extent := chunkAxisExtent(r.plan.arr.Shape[axis], r.plan.arr.ChunkShape[axis], lo[axis])
		perAxis[axis] = selAxisPositions(local, axis, extent)
		if len(perAxis[axis]) == 0 {
			return // No kept cells on this axis → no cells in the chunk.
		}
	}

	global := make([]int64, rank)
	idx := make([]int64, rank)
	for {
		// Materialize the global multi-index.
		for axis := 0; axis < rank; axis++ {
			global[axis] = lo[axis] + perAxis[axis][idx[axis]]
		}
		fn(global)

		// Advance in C order (last axis fastest).
		carry := true
		for axis := rank - 1; axis >= 0 && carry; axis-- {
			idx[axis]++
			if idx[axis] < int64(len(perAxis[axis])) {
				carry = false
			} else {
				idx[axis] = 0
			}
		}
		if carry {
			return
		}
	}
}

// chunkAxisExtent returns the cell extent of a chunk along one axis given the
// full size, chunk extent, and the chunk's origin lo on that axis (edge chunks
// are clipped to size).
func chunkAxisExtent(size, chunk, lo int64) int64 {
	hi := lo + chunk
	if hi > size {
		hi = size
	}
	return hi - lo
}

// cOffset returns the C-order linear cell offset of globalIdx within a chunk of
// the given FULL (padded) chunkShape, where lo is the chunk's cell origin. The
// chunk buffer from DecodeChunk is always full chunkShape, so strides use
// chunkShape (not the clipped edge extent).
func cOffset(globalIdx, lo, chunkShape []int64) int64 {
	rank := len(chunkShape)
	off := int64(0)
	stride := int64(1)
	for axis := rank - 1; axis >= 0; axis-- {
		localPos := globalIdx[axis] - lo[axis]
		off += localPos * stride
		stride *= chunkShape[axis]
	}
	return off
}
