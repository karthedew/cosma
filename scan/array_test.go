package scan_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/karthedew/cosma/carray"
	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/expr"
	"github.com/karthedew/cosma/scan"
	"github.com/karthedew/cosma/schema"
	"github.com/karthedew/cosma/store"
	"github.com/karthedew/cosma/store/memstore"
)

// --- fixtures ----------------------------------------------------------------

// fixtureArray describes an array fixture and supplies the canonical cell value
// for every cell (by full multi-index). The fixture builder writes full, padded,
// little-endian C-order chunk payloads so the scan exercises the real decode
// path (compressor null).
type fixtureArray struct {
	path       string
	shape      []int64
	chunkShape []int64
	dtype      schema.DType
	dimNames   []string
	fillValue  any
	// valueAt returns the canonical value of cell idx as a float64 (both int64
	// and float64 arrays use this; integer values round-trip exactly).
	valueAt func(idx []int64) float64
	// missing, when set, marks chunk coords (dotted) that are deliberately not
	// written to the store, so they exercise the missing-chunk path.
	missing map[string]bool
}

// build writes the fixture into a fresh memstore and returns the store plus the
// chunk payloads it wrote (for assertions). The bytes for each chunk are a full
// chunkShape buffer, padded for edge chunks, in C order.
func (f fixtureArray) build() *memstore.Store {
	s := memstore.New()
	chunks := map[string][]byte{}

	grid := carray.ChunkGrid{Shape: f.shape, ChunkShape: f.chunkShape}
	width := dtypeWidthTest(f.dtype)

	forEachChunkCoord(grid, func(coord []int64) {
		dotted := dotCoord(coord)
		if f.missing[dotted] {
			return
		}
		lo, _ := grid.ChunkBounds(coord)
		cells := chunkCells(f.chunkShape)
		buf := make([]byte, int(cells)*width)
		// Walk the full (padded) chunk in C order; pad cells (outside Shape) get
		// zero, which the scan never reads (it only enumerates in-bounds cells).
		writeChunk(buf, f.chunkShape, lo, f.shape, width, f.dtype, f.valueAt)
		chunks[dotted] = buf
	})

	s.AddArray(f.path, store.Array{
		Shape:      f.shape,
		ChunkShape: f.chunkShape,
		DType:      f.dtype,
		DimNames:   f.dimNames,
		FillValue:  f.fillValue,
	}, chunks)
	return s
}

// writeChunk fills buf (full chunkShape, C order) with the fixture's value for
// each in-bounds cell. Out-of-bounds (padding) cells stay zero.
func writeChunk(buf []byte, chunkShape, lo, shape []int64, width int, dt schema.DType, valueAt func([]int64) float64) {
	rank := len(chunkShape)
	if rank == 0 {
		putCell(buf, 0, width, dt, valueAt(nil))
		return
	}
	cells := chunkCells(chunkShape)
	local := make([]int64, rank)
	global := make([]int64, rank)
	for c := int64(0); c < cells; c++ {
		// Decompose c into the C-order multi-index over chunkShape.
		rem := c
		for axis := rank - 1; axis >= 0; axis-- {
			local[axis] = rem % chunkShape[axis]
			rem /= chunkShape[axis]
		}
		inBounds := true
		for axis := 0; axis < rank; axis++ {
			global[axis] = lo[axis] + local[axis]
			if global[axis] >= shape[axis] {
				inBounds = false
			}
		}
		if !inBounds {
			continue
		}
		putCell(buf, int(c), width, dt, valueAt(global))
	}
}

func putCell(buf []byte, cell, width int, dt schema.DType, v float64) {
	off := cell * width
	switch dt {
	case schema.Int64:
		binary.LittleEndian.PutUint64(buf[off:], uint64(int64(v)))
	case schema.Float64:
		binary.LittleEndian.PutUint64(buf[off:], math.Float64bits(v))
	default:
		panic("test fixture: unsupported dtype " + string(dt))
	}
}

func dtypeWidthTest(dt schema.DType) int {
	switch dt {
	case schema.Int64, schema.Float64:
		return 8
	default:
		panic("test fixture: unsupported dtype " + string(dt))
	}
}

func chunkCells(chunkShape []int64) int64 {
	n := int64(1)
	for _, d := range chunkShape {
		n *= d
	}
	return n
}

func forEachChunkCoord(g carray.ChunkGrid, fn func(coord []int64)) {
	rank := len(g.Shape)
	if rank == 0 {
		fn(nil)
		return
	}
	counts := make([]int64, rank)
	total := int64(1)
	for axis := 0; axis < rank; axis++ {
		counts[axis] = g.ChunkCount(axis)
		total *= counts[axis]
	}
	coord := make([]int64, rank)
	for i := int64(0); i < total; i++ {
		rem := i
		for axis := rank - 1; axis >= 0; axis-- {
			coord[axis] = rem % counts[axis]
			rem /= counts[axis]
		}
		cp := make([]int64, rank)
		copy(cp, coord)
		fn(cp)
	}
}

func dotCoord(coord []int64) string {
	if len(coord) == 0 {
		return ""
	}
	parts := make([]string, len(coord))
	for i, c := range coord {
		parts[i] = fmt.Sprintf("%d", c)
	}
	return joinDot(parts)
}

func joinDot(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += "."
		}
		out += p
	}
	return out
}

// --- counting store ----------------------------------------------------------

// countingStore wraps a store.Store and counts ReadChunk calls per dotted chunk
// coordinate, so tests can assert exactly which chunks a scan touched.
type countingStore struct {
	inner store.Store
	mu    sync.Mutex
	reads map[string]int
	total int64
}

func newCountingStore(inner store.Store) *countingStore {
	return &countingStore{inner: inner, reads: map[string]int{}}
}

func (c *countingStore) List(ctx context.Context, p string) ([]store.Entry, error) {
	return c.inner.List(ctx, p)
}
func (c *countingStore) Meta(ctx context.Context, p string) (store.Object, error) {
	return c.inner.Meta(ctx, p)
}
func (c *countingStore) ReadChunk(ctx context.Context, key store.ChunkKey) ([]byte, error) {
	c.mu.Lock()
	c.reads[dotCoord(key.Coord)]++
	c.mu.Unlock()
	atomic.AddInt64(&c.total, 1)
	return c.inner.ReadChunk(ctx, key)
}
func (c *countingStore) Close() error { return c.inner.Close() }

func (c *countingStore) touched() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.reads))
	for k := range c.reads {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func (c *countingStore) totalReads() int64 { return atomic.LoadInt64(&c.total) }

// --- output reading ----------------------------------------------------------

// scanRow is a normalized output row: the kept index columns and the value as a
// float64 (lossless for the int64/float64 fixtures used here).
type scanRow struct {
	idx []int64
	val float64
}

func (r scanRow) key() string {
	return fmt.Sprintf("%v|%v", r.idx, r.val)
}

// collectScanRows collects a LazyScanArray frame and normalizes every output
// row. idxCols is the count of leading int64 index columns (schema-dependent).
func collectScanRows(t *testing.T, df *dataframe.DataFrame) []scanRow {
	t.Helper()
	iter, err := dataframe.NewRecordBatchIter(df)
	if err != nil {
		t.Fatalf("NewRecordBatchIter: %v", err)
	}
	var rows []scanRow
	for {
		rec, ok, err := iter.Next()
		if err != nil {
			t.Fatalf("iter.Next: %v", err)
		}
		if !ok {
			break
		}
		rows = append(rows, recordToScanRows(rec)...)
		rec.Release()
	}
	return rows
}

func recordToScanRows(rec arrow.Record) []scanRow {
	ncol := int(rec.NumCols())
	idxCols := ncol - 1 // last is value
	idxArrs := make([]*array.Int64, idxCols)
	for c := 0; c < idxCols; c++ {
		idxArrs[c] = rec.Column(c).(*array.Int64)
	}
	valCol := rec.Column(idxCols)

	valAt := func(i int) float64 {
		switch v := valCol.(type) {
		case *array.Int64:
			return float64(v.Value(i))
		case *array.Float64:
			return v.Value(i)
		default:
			panic(fmt.Sprintf("unexpected value column type %T", valCol))
		}
	}

	out := make([]scanRow, rec.NumRows())
	for i := range out {
		idx := make([]int64, idxCols)
		for c := 0; c < idxCols; c++ {
			idx[c] = idxArrs[c].Value(i)
		}
		out[i] = scanRow{idx: idx, val: valAt(i)}
	}
	return out
}

func sortRows(rows []scanRow) {
	sort.Slice(rows, func(a, b int) bool { return rows[a].key() < rows[b].key() })
}

// --- brute-force oracle ------------------------------------------------------

// bruteForce enumerates the expected (kept index..., value) rows of a selection
// directly from the fixture, independent of the scan implementation. keptAxes
// are the array axes that survive into output (non-Index axes). For missing
// chunks the fill value is substituted.
func (f fixtureArray) bruteForce(sel carray.Selection, keptAxes []int) []scanRow {
	grid := carray.ChunkGrid{Shape: f.shape, ChunkShape: f.chunkShape}
	var rows []scanRow

	enumerateSelectedCells(f.shape, sel, func(idx []int64) {
		var val float64
		coord := cellChunkCoord(idx, f.chunkShape)
		if f.missing[dotCoord(coord)] {
			val = toFloat(f.fillValue)
		} else {
			val = f.valueAt(idx)
		}
		kept := make([]int64, len(keptAxes))
		for i, axis := range keptAxes {
			kept[i] = idx[axis]
		}
		rows = append(rows, scanRow{idx: kept, val: val})
	})
	_ = grid
	return rows
}

func toFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int64:
		return float64(x)
	case int:
		return float64(x)
	default:
		panic(fmt.Sprintf("fill value %T not supported in oracle", v))
	}
}

func cellChunkCoord(idx, chunkShape []int64) []int64 {
	coord := make([]int64, len(idx))
	for axis := range idx {
		coord[axis] = idx[axis] / chunkShape[axis]
	}
	return coord
}

// enumerateSelectedCells walks every cell the selection keeps, in C order,
// calling fn with the full array multi-index. It mirrors carray's per-axis
// position semantics over the public AxisSel fields.
func enumerateSelectedCells(shape []int64, sel carray.Selection, fn func(idx []int64)) {
	rank := len(shape)
	if rank == 0 {
		fn(nil)
		return
	}
	perAxis := make([][]int64, rank)
	for axis := 0; axis < rank; axis++ {
		perAxis[axis] = oracleAxisPositions(sel, axis, shape[axis])
		if len(perAxis[axis]) == 0 {
			return
		}
	}
	idx := make([]int64, rank)
	cur := make([]int64, rank)
	for {
		for axis := 0; axis < rank; axis++ {
			cur[axis] = perAxis[axis][idx[axis]]
		}
		fn(cur)
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

func oracleAxisPositions(sel carray.Selection, axis int, size int64) []int64 {
	var a carray.AxisSel
	if axis < len(sel.Axes) {
		a = sel.Axes[axis]
	}
	switch {
	case a.Index != nil:
		return []int64{*a.Index}
	case a.Slice != nil:
		s := *a.Slice
		var out []int64
		for p := s.Start; p < s.Stop; p += s.Step {
			out = append(out, p)
		}
		return out
	case a.Indices != nil:
		out := make([]int64, len(a.Indices))
		copy(out, a.Indices)
		return out
	default:
		out := make([]int64, size)
		for i := int64(0); i < size; i++ {
			out[i] = i
		}
		return out
	}
}

// keptAxesOf returns the array axes that survive into output for sel (every
// axis not selected by a single Index).
func keptAxesOf(shape []int64, sel carray.Selection) []int {
	var out []int
	for axis := range shape {
		if axis < len(sel.Axes) && sel.Axes[axis].Index != nil {
			continue
		}
		out = append(out, axis)
	}
	return out
}

func assertRowsEqual(t *testing.T, got, want []scanRow) {
	t.Helper()
	sortRows(got)
	sortRows(want)
	if len(got) != len(want) {
		t.Fatalf("row count: got %d want %d\n got=%v\nwant=%v", len(got), len(want), got, want)
	}
	for i := range got {
		if got[i].key() != want[i].key() {
			t.Fatalf("row %d: got %v want %v", i, got[i], want[i])
		}
	}
}

func openTree(t *testing.T, s store.Store) *store.Tree {
	t.Helper()
	tree, err := store.Open(context.Background(), s)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	return tree
}

func iptr(v int64) *int64 { return &v }

// --- tests -------------------------------------------------------------------

// twoDFloat is a 4x6 float64 array, chunks 4x3 (no edge), values = 10*r + c.
func twoDFloat() fixtureArray {
	return fixtureArray{
		path:       "/temp",
		shape:      []int64{4, 6},
		chunkShape: []int64{4, 3},
		dtype:      schema.Float64,
		dimNames:   []string{"time", "station"},
		fillValue:  float64(-1),
		valueAt:    func(idx []int64) float64 { return float64(10*idx[0] + idx[1]) },
	}
}

// edgeInt is a 5x5 int64 array, chunks 2x2 (edge chunks: shape % chunk != 0).
func edgeInt() fixtureArray {
	return fixtureArray{
		path:       "/edge",
		shape:      []int64{5, 5},
		chunkShape: []int64{2, 2},
		dtype:      schema.Int64,
		dimNames:   []string{"y", "x"},
		fillValue:  int64(-7),
		valueAt:    func(idx []int64) float64 { return float64(100*idx[0] + idx[1]) },
	}
}

func TestArrayScanParityFullAndSelections(t *testing.T) {
	cases := []struct {
		name string
		fix  fixtureArray
		sel  carray.Selection
	}{
		{"2d-full", twoDFloat(), carray.Selection{}},
		{
			"2d-slice",
			twoDFloat(),
			carray.Selection{Axes: []carray.AxisSel{
				{Slice: &carray.Slice{Start: 1, Stop: 4, Step: 1}},
				{Slice: &carray.Slice{Start: 0, Stop: 5, Step: 2}},
			}},
		},
		{
			"2d-index-drop",
			twoDFloat(),
			carray.Selection{Axes: []carray.AxisSel{
				{Index: iptr(2)},
				{},
			}},
		},
		{
			"2d-indices-gather",
			twoDFloat(),
			carray.Selection{Axes: []carray.AxisSel{
				{Indices: []int64{0, 3}},
				{Indices: []int64{1, 2, 5}},
			}},
		},
		{"edge-full", edgeInt(), carray.Selection{}},
		{
			"edge-slice",
			edgeInt(),
			carray.Selection{Axes: []carray.AxisSel{
				{Slice: &carray.Slice{Start: 0, Stop: 5, Step: 1}},
				{Slice: &carray.Slice{Start: 1, Stop: 5, Step: 1}},
			}},
		},
		{
			"edge-index-drop",
			edgeInt(),
			carray.Selection{Axes: []carray.AxisSel{
				{Index: iptr(4)},
				{},
			}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tree := openTree(t, tc.fix.build())
			df, err := scan.LazyScanArray(scan.ArrayScan{
				Tree:      tree,
				ArrayPath: tc.fix.path,
				Selection: tc.sel,
			}).Collect(context.Background())
			if err != nil {
				t.Fatalf("Collect: %v", err)
			}
			got := collectScanRows(t, df)
			want := tc.fix.bruteForce(tc.sel, keptAxesOf(tc.fix.shape, tc.sel))
			assertRowsEqual(t, got, want)
		})
	}
}

func TestArrayScanScalar(t *testing.T) {
	fix := fixtureArray{
		path:       "/scalar",
		shape:      nil,
		chunkShape: nil,
		dtype:      schema.Float64,
		fillValue:  float64(0),
		valueAt:    func([]int64) float64 { return 42.5 },
	}
	tree := openTree(t, fix.build())
	df, err := scan.LazyScanArray(scan.ArrayScan{Tree: tree, ArrayPath: fix.path}).
		Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	got := collectScanRows(t, df)
	if len(got) != 1 || got[0].val != 42.5 || len(got[0].idx) != 0 {
		t.Fatalf("scalar scan: got %v", got)
	}
	// Schema: value column only, no index columns.
	if names := fieldNames(df); len(names) != 1 || names[0] != "value" {
		t.Fatalf("scalar schema fields: %v want [value]", names)
	}
}

func fieldNames(df *dataframe.DataFrame) []string {
	fields := df.Schema().Fields()
	names := make([]string, len(fields))
	for i, f := range fields {
		names[i] = f.Name
	}
	return names
}

func TestArrayScanDefaultDimNames(t *testing.T) {
	fix := twoDFloat()
	fix.dimNames = nil // force positional dim_0, dim_1
	tree := openTree(t, fix.build())
	df, err := scan.LazyScanArray(scan.ArrayScan{Tree: tree, ArrayPath: fix.path}).
		Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	got := fieldNames(df)
	want := []string{"dim_0", "dim_1", "value"}
	if fmt.Sprintf("%v", got) != fmt.Sprintf("%v", want) {
		t.Fatalf("default dim names: got %v want %v", got, want)
	}
}

func TestArrayScanMissingChunkFill(t *testing.T) {
	fix := edgeInt()
	fix.missing = map[string]bool{"0.0": true} // top-left 2x2 chunk missing
	tree := openTree(t, fix.build())
	df, err := scan.LazyScanArray(scan.ArrayScan{Tree: tree, ArrayPath: fix.path}).
		Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	got := collectScanRows(t, df)
	want := fix.bruteForce(carray.Selection{}, keptAxesOf(fix.shape, carray.Selection{}))
	assertRowsEqual(t, got, want)
}

func TestArrayScanMissingChunkNilFillErrors(t *testing.T) {
	fix := edgeInt()
	fix.fillValue = nil
	fix.missing = map[string]bool{"1.1": true}
	tree := openTree(t, fix.build())
	_, err := scan.LazyScanArray(scan.ArrayScan{Tree: tree, ArrayPath: fix.path}).
		Collect(context.Background())
	if err == nil {
		t.Fatal("expected error for missing chunk with nil fill value")
	}
}

// TestArrayScanPushdownPrunesChunks asserts a range predicate on a chunked axis
// reads exactly the expected chunk subset.
func TestArrayScanPushdownPrunesChunks(t *testing.T) {
	// 1-D int64, shape 100, chunks of 10 -> 10 chunks. time < 24 keeps chunks
	// 0,1,2 (cells 0..23).
	fix := fixtureArray{
		path:       "/t",
		shape:      []int64{100},
		chunkShape: []int64{10},
		dtype:      schema.Int64,
		dimNames:   []string{"time"},
		fillValue:  int64(-1),
		valueAt:    func(idx []int64) float64 { return float64(idx[0]) },
	}
	cs := newCountingStore(fix.build())
	tree := openTree(t, cs)

	df, err := scan.LazyScanArray(scan.ArrayScan{Tree: tree, ArrayPath: fix.path}).
		Filter(expr.Col("time").Lt(expr.Lit(int64(24)))).
		Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}

	// Result exact: time in [0,24).
	got := collectScanRows(t, df)
	var want []scanRow
	for i := int64(0); i < 24; i++ {
		want = append(want, scanRow{idx: []int64{i}, val: float64(i)})
	}
	assertRowsEqual(t, got, want)

	// Pruning: only chunks 0,1,2 read.
	touched := cs.touched()
	wantTouched := []string{"0", "1", "2"}
	if fmt.Sprintf("%v", touched) != fmt.Sprintf("%v", wantTouched) {
		t.Fatalf("chunks read: got %v want %v", touched, wantTouched)
	}
}

// TestArrayScanPartiallyHonorablePredicate: time<24 && value>0 prunes on the
// honorable conjunct yet stays exact against a brute force of the FULL predicate.
func TestArrayScanPartiallyHonorablePredicate(t *testing.T) {
	fix := fixtureArray{
		path:       "/t",
		shape:      []int64{100},
		chunkShape: []int64{10},
		dtype:      schema.Int64,
		dimNames:   []string{"time"},
		fillValue:  int64(-1),
		valueAt:    func(idx []int64) float64 { return float64(idx[0]) },
	}
	cs := newCountingStore(fix.build())
	tree := openTree(t, cs)

	pred := expr.Col("time").Lt(expr.Lit(int64(24))).And(expr.Col("value").Gt(expr.Lit(int64(0))))
	df, err := scan.LazyScanArray(scan.ArrayScan{Tree: tree, ArrayPath: fix.path}).
		Filter(pred).
		Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}

	got := collectScanRows(t, df)
	var want []scanRow
	for i := int64(1); i < 24; i++ { // value>0 drops cell 0
		want = append(want, scanRow{idx: []int64{i}, val: float64(i)})
	}
	assertRowsEqual(t, got, want)

	// time<24 still prunes to chunks 0,1,2 despite the un-pushable value>0.
	if got := cs.touched(); fmt.Sprintf("%v", got) != "[0 1 2]" {
		t.Fatalf("chunks read: got %v want [0 1 2]", got)
	}
}

// TestArrayScanLimitStopsChunkReads: a small limit stops the chunk iterator
// before reading every chunk.
func TestArrayScanLimitStopsChunkReads(t *testing.T) {
	fix := fixtureArray{
		path:       "/t",
		shape:      []int64{100},
		chunkShape: []int64{10},
		dtype:      schema.Int64,
		dimNames:   []string{"time"},
		fillValue:  int64(-1),
		valueAt:    func(idx []int64) float64 { return float64(idx[0]) },
	}
	cs := newCountingStore(fix.build())
	tree := openTree(t, cs)

	df, err := scan.LazyScanArray(scan.ArrayScan{
		Tree:       tree,
		ArrayPath:  fix.path,
		BatchCells: 10,
	}).
		Limit(5).
		Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if n := df.NumRows(); n != 5 {
		t.Fatalf("limit rows: got %d want 5", n)
	}
	// With a single-worker pipeline a limit of 5 must not read all 10 chunks.
	if cs.totalReads() >= 10 {
		t.Fatalf("limit did not prune chunk reads: read %d of 10", cs.totalReads())
	}
}

// TestArrayScanBatchInvariant: BatchCells smaller than a chunk's kept cells
// yields >1 batch, none spanning chunks.
func TestArrayScanBatchInvariant(t *testing.T) {
	fix := fixtureArray{
		path:       "/big",
		shape:      []int64{40},
		chunkShape: []int64{40}, // one chunk, 40 cells
		dtype:      schema.Int64,
		dimNames:   []string{"i"},
		fillValue:  int64(0),
		valueAt:    func(idx []int64) float64 { return float64(idx[0]) },
	}
	tree := openTree(t, fix.build())

	rr, err := scan.LazyScanArray(scan.ArrayScan{
		Tree:       tree,
		ArrayPath:  fix.path,
		BatchCells: 16,
	}).CollectStream(context.Background())
	if err != nil {
		t.Fatalf("CollectStream: %v", err)
	}
	defer rr.Release()

	var batches int
	var total int64
	maxBatch := int64(0)
	for rr.Next() {
		rec := rr.Record()
		batches++
		total += rec.NumRows()
		if rec.NumRows() > maxBatch {
			maxBatch = rec.NumRows()
		}
	}
	if err := rr.Err(); err != nil {
		t.Fatalf("stream err: %v", err)
	}
	if total != 40 {
		t.Fatalf("streamed rows: got %d want 40", total)
	}
	if batches < 2 {
		t.Fatalf("expected >1 batch with BatchCells=16 over 40 cells, got %d", batches)
	}
	if maxBatch > 16 {
		t.Fatalf("batch exceeded BatchCells: %d > 16", maxBatch)
	}
}

// TestArrayScanColumnProjection: a column projection drops an index column from
// output but never changes which cells are read.
func TestArrayScanColumnProjection(t *testing.T) {
	fix := twoDFloat()
	tree := openTree(t, fix.build())

	// Select only "station" and "value": the "time" index column is dropped.
	df, err := scan.LazyScanArray(scan.ArrayScan{Tree: tree, ArrayPath: fix.path}).
		Select("station", "value").
		Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	got := fieldNames(df)
	want := []string{"station", "value"}
	if fmt.Sprintf("%v", got) != fmt.Sprintf("%v", want) {
		t.Fatalf("projected schema names: got %v want %v", got, want)
	}
	// Row count unchanged (all cells still read): 4x6 = 24.
	if df.NumRows() != 24 {
		t.Fatalf("projected row count: got %d want 24", df.NumRows())
	}
}
