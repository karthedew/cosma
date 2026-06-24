package scan_test

import (
	"context"
	"encoding/binary"
	"sort"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/karthedew/cosma/carray"
	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/dataset"
	"github.com/karthedew/cosma/expr"
	"github.com/karthedew/cosma/scan"
	"github.com/karthedew/cosma/schema"
	"github.com/karthedew/cosma/store"
	"github.com/karthedew/cosma/store/memstore"
	"github.com/karthedew/cosma/store/zarr"
)

func leI64(vals ...int64) []byte {
	b := make([]byte, len(vals)*8)
	for i, v := range vals {
		binary.LittleEndian.PutUint64(b[i*8:], uint64(v))
	}
	return b
}

// coordRow is one output row with coordinate-valued index columns read as int64.
type coordRow struct {
	idx []int64 // coord (or index) column values, in column order
	val int64
}

// readCoordRows collects a frame whose index columns and value column are all
// int64, returning rows in a canonical sorted order (the scan is unordered).
func readCoordRows(t *testing.T, df *dataframe.DataFrame) []coordRow {
	t.Helper()
	iter, err := dataframe.NewRecordBatchIter(df)
	if err != nil {
		t.Fatalf("NewRecordBatchIter: %v", err)
	}
	ncol := len(df.Schema().Fields())
	var rows []coordRow
	for {
		rec, ok, err := iter.Next()
		if err != nil {
			t.Fatalf("iter.Next: %v", err)
		}
		if !ok {
			break
		}
		for r := 0; int64(r) < rec.NumRows(); r++ {
			row := coordRow{}
			for c := 0; c < ncol-1; c++ {
				row.idx = append(row.idx, rec.Column(c).(*array.Int64).Value(r))
			}
			row.val = rec.Column(ncol - 1).(*array.Int64).Value(r)
			rows = append(rows, row)
		}
		rec.Release()
	}
	sortCoordRows(rows)
	return rows
}

func sortCoordRows(rows []coordRow) {
	sort.Slice(rows, func(i, j int) bool {
		for k := range rows[i].idx {
			if rows[i].idx[k] != rows[j].idx[k] {
				return rows[i].idx[k] < rows[j].idx[k]
			}
		}
		return rows[i].val < rows[j].val
	})
}

// perArrayCountingStore counts ReadChunk calls per array path.
type perArrayCountingStore struct {
	store.Store
	mu     sync.Mutex
	counts map[string]int
}

func newPerArrayCounting(s store.Store) *perArrayCountingStore {
	return &perArrayCountingStore{Store: s, counts: map[string]int{}}
}

func (c *perArrayCountingStore) ReadChunk(ctx context.Context, key store.ChunkKey) ([]byte, error) {
	c.mu.Lock()
	c.counts[key.ArrayPath]++
	c.mu.Unlock()
	return c.Store.ReadChunk(ctx, key)
}

func (c *perArrayCountingStore) count(path string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[path]
}

// --- end-to-end on the committed xarray fixture ------------------------------

// TestWithCoordsEndToEnd is the headline test: scan the v2_xarray data variable
// with scan.Var(..., scan.WithCoords()) and confirm the y/x columns hold the
// coordinate values, not integer indices.
func TestWithCoordsEndToEnd(t *testing.T) {
	st, err := zarr.OpenFS("../store/zarr/testdata/v2_xarray")
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	tree, err := store.Open(context.Background(), st)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	v, err := dataset.Interpret(tree, "/")
	if err != nil {
		t.Fatalf("Interpret: %v", err)
	}

	// Without coords: y/x are integer indices 0..1, 0..2.
	plain, err := scan.LazyScanArray(scan.Var(v, "data")).Collect(context.Background())
	if err != nil {
		t.Fatalf("plain collect: %v", err)
	}
	if got := fieldNames(plain); got[0] != "y" || got[1] != "x" {
		t.Fatalf("columns = %v, want [y x value]", got)
	}

	// With coords: y holds {10,20}, x holds {0,1,2}; value is data float64.
	df, err := scan.LazyScanArray(scan.Var(v, "data", scan.WithCoords())).
		Collect(context.Background())
	if err != nil {
		t.Fatalf("withcoords collect: %v", err)
	}
	// value column is float64, so read y,x (int64) and value (float64) by hand.
	iter, _ := dataframe.NewRecordBatchIter(df)
	type row struct {
		y, x int64
		v    float64
	}
	var rows []row
	for {
		rec, ok, err := iter.Next()
		if err != nil {
			t.Fatalf("iter: %v", err)
		}
		if !ok {
			break
		}
		yc := rec.Column(0).(*array.Int64)
		xc := rec.Column(1).(*array.Int64)
		vc := rec.Column(2).(*array.Float64)
		for r := 0; int64(r) < rec.NumRows(); r++ {
			rows = append(rows, row{yc.Value(r), xc.Value(r), vc.Value(r)})
		}
		rec.Release()
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].y != rows[j].y {
			return rows[i].y < rows[j].y
		}
		return rows[i].x < rows[j].x
	})
	want := []row{
		{10, 0, 0}, {10, 1, 1}, {10, 2, 2},
		{20, 0, 3}, {20, 1, 4}, {20, 2, 5},
	}
	if len(rows) != len(want) {
		t.Fatalf("got %d rows, want %d: %+v", len(rows), len(want), rows)
	}
	for i := range want {
		if rows[i] != want[i] {
			t.Errorf("row %d = %+v, want %+v", i, rows[i], want[i])
		}
	}
}

// --- memstore control tests --------------------------------------------------

// build2DStore builds a (4,4) int64 data array "/d" chunked (2,2) with value
// 10*y+x, plus 1-D int64 coords "/y" and "/x" (chunked 2) holding y*100 and
// x*1000. Returns the store.
func build2DStore() *memstore.Store {
	s := memstore.New()
	// data chunks: 4 chunks of (2,2), C-order within each chunk.
	dataChunk := func(y0, x0 int64) []byte {
		var vals []int64
		for dy := int64(0); dy < 2; dy++ {
			for dx := int64(0); dx < 2; dx++ {
				vals = append(vals, 10*(y0+dy)+(x0+dx))
			}
		}
		return leI64(vals...)
	}
	s.AddArray("/d", store.Array{
		Shape: []int64{4, 4}, ChunkShape: []int64{2, 2}, DType: schema.Int64,
		DimNames: []string{"y", "x"}, FillValue: int64(-1),
	}, map[string][]byte{
		"0.0": dataChunk(0, 0), "0.1": dataChunk(0, 2),
		"1.0": dataChunk(2, 0), "1.1": dataChunk(2, 2),
	})
	s.AddArray("/y", store.Array{
		Shape: []int64{4}, ChunkShape: []int64{2}, DType: schema.Int64,
		DimNames: []string{"y"}, FillValue: int64(-1),
	}, map[string][]byte{"0": leI64(0, 100), "1": leI64(200, 300)})
	s.AddArray("/x", store.Array{
		Shape: []int64{4}, ChunkShape: []int64{2}, DType: schema.Int64,
		DimNames: []string{"x"}, FillValue: int64(-1),
	}, map[string][]byte{"0": leI64(0, 1000), "1": leI64(2000, 3000)})
	return s
}

func TestWithCoordsReplaceParity(t *testing.T) {
	tree := openTree(t, build2DStore())
	df, err := scan.LazyScanArray(scan.ArrayScan{
		Tree: tree, ArrayPath: "/d",
		CoordPaths: map[string]string{"y": "/y", "x": "/x"},
	}).Collect(context.Background())
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	got := readCoordRows(t, df)
	// Brute force: for each cell (gy,gx), y-coord = gy*100 (0,100,200,300),
	// x-coord = gx*1000, value = 10*gy+gx.
	var want []coordRow
	ycoord := []int64{0, 100, 200, 300}
	xcoord := []int64{0, 1000, 2000, 3000}
	for gy := int64(0); gy < 4; gy++ {
		for gx := int64(0); gx < 4; gx++ {
			want = append(want, coordRow{idx: []int64{ycoord[gy], xcoord[gx]}, val: 10*gy + gx})
		}
	}
	sortCoordRows(want)
	if len(got) != len(want) {
		t.Fatalf("got %d rows want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].val != want[i].val || got[i].idx[0] != want[i].idx[0] || got[i].idx[1] != want[i].idx[1] {
			t.Fatalf("row %d = %+v want %+v", i, got[i], want[i])
		}
	}
}

func TestWithCoordsDimWithoutCoord(t *testing.T) {
	tree := openTree(t, build2DStore())
	// Only y has a coordinate; x stays an int64 index column.
	df, err := scan.LazyScanArray(scan.ArrayScan{
		Tree: tree, ArrayPath: "/d",
		CoordPaths: map[string]string{"y": "/y"},
	}).Collect(context.Background())
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	got := readCoordRows(t, df)
	// y is coord (0,100,200,300), x is raw index (0,1,2,3).
	seenY := map[int64]bool{}
	seenX := map[int64]bool{}
	for _, r := range got {
		seenY[r.idx[0]] = true
		seenX[r.idx[1]] = true
	}
	for _, y := range []int64{0, 100, 200, 300} {
		if !seenY[y] {
			t.Errorf("missing y coord %d", y)
		}
	}
	for _, x := range []int64{0, 1, 2, 3} {
		if !seenX[x] {
			t.Errorf("missing x index %d", x)
		}
	}
}

func TestWithCoordsCacheReadsEachChunkOnce(t *testing.T) {
	cs := newPerArrayCounting(build2DStore())
	tree := openTree(t, cs)
	_, err := scan.LazyScanArray(scan.ArrayScan{
		Tree: tree, ArrayPath: "/d",
		CoordPaths: map[string]string{"y": "/y", "x": "/x"},
	}).Collect(context.Background())
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	// /y and /x each have 2 chunks; all 4 data chunks share them. With the
	// per-scan cache each coord chunk is read exactly once -> 2 reads per coord,
	// not 4 (one per data chunk).
	if n := cs.count("/y"); n != 2 {
		t.Errorf("/y coord reads = %d, want 2 (each chunk once)", n)
	}
	if n := cs.count("/x"); n != 2 {
		t.Errorf("/x coord reads = %d, want 2 (each chunk once)", n)
	}
}

func TestWithCoordsLazyOnlySelected(t *testing.T) {
	cs := newPerArrayCounting(build2DStore())
	tree := openTree(t, cs)
	// Select y in [0,2) only -> only data chunks (0,0),(0,1) -> only y coord
	// chunk 0; x coord chunks 0 and 1 still both touched.
	sel := carray.Selection{Axes: []carray.AxisSel{
		{Slice: &carray.Slice{Start: 0, Stop: 2, Step: 1}},
		{},
	}}
	_, err := scan.LazyScanArray(scan.ArrayScan{
		Tree: tree, ArrayPath: "/d", Selection: sel,
		CoordPaths: map[string]string{"y": "/y", "x": "/x"},
	}).Collect(context.Background())
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if n := cs.count("/y"); n != 1 {
		t.Errorf("/y coord reads = %d, want 1 (only selected chunk)", n)
	}
}

// TestWithCoordsPushdownDisabled: a filter on a coord column returns correct
// rows even though coord pushdown is disabled (the surviving Filter applies it).
func TestWithCoordsPushdownDisabled(t *testing.T) {
	tree := openTree(t, build2DStore())
	df, err := scan.LazyScanArray(scan.ArrayScan{
		Tree: tree, ArrayPath: "/d",
		CoordPaths: map[string]string{"y": "/y", "x": "/x"},
	}).
		Filter(expr.Col("y").Gte(expr.Lit(int64(200)))).
		Collect(context.Background())
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	got := readCoordRows(t, df)
	for _, r := range got {
		if r.idx[0] < 200 {
			t.Errorf("row with y-coord %d survived filter y>=200", r.idx[0])
		}
	}
	// gy in {2,3} -> 2 rows * 4 x = 8 rows.
	if len(got) != 8 {
		t.Errorf("got %d rows, want 8 (y-coord >= 200)", len(got))
	}
}

func TestWithCoordsMissingCoordNilFillErrors(t *testing.T) {
	s := build2DStore()
	// Replace /x with a coord array whose chunk "1" is missing and fill is nil.
	s = memstore.New()
	s.AddArray("/d", store.Array{
		Shape: []int64{4}, ChunkShape: []int64{2}, DType: schema.Int64,
		DimNames: []string{"x"}, FillValue: int64(0),
	}, map[string][]byte{"0": leI64(1, 2), "1": leI64(3, 4)})
	s.AddArray("/x", store.Array{
		Shape: []int64{4}, ChunkShape: []int64{2}, DType: schema.Int64,
		DimNames: []string{"x"}, FillValue: nil, // undefined
	}, map[string][]byte{"0": leI64(0, 10)}) // chunk "1" missing
	tree := openTree(t, s)

	_, err := scan.LazyScanArray(scan.ArrayScan{
		Tree: tree, ArrayPath: "/d",
		CoordPaths: map[string]string{"x": "/x"},
	}).Collect(context.Background())
	if err == nil {
		t.Fatal("expected error for missing coord chunk with nil fill value")
	}
}

func TestWithCoordsRejectsBadCoord(t *testing.T) {
	tree := openTree(t, build2DStore())
	// Point a dim at a non-existent coord path.
	_, err := scan.LazyScanArray(scan.ArrayScan{
		Tree: tree, ArrayPath: "/d",
		CoordPaths: map[string]string{"y": "/nope"},
	}).Collect(context.Background())
	if err == nil {
		t.Fatal("expected error for missing coordinate array")
	}
}
