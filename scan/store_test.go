package scan_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/expr"
	"github.com/karthedew/cosma/scan"
	"github.com/karthedew/cosma/schema"
	"github.com/karthedew/cosma/store"
	"github.com/karthedew/cosma/store/memstore"
)

// newManifestTree builds a nested fixture tree whose Walk order is deterministic
// (depth-first, store-listing order):
//
//	/                       group
//	├── weather/            group, 1 attr
//	│   ├── temp            2-D float64, shape 4x6 / chunks 4x3 -> 2 chunks
//	│   └── time            1-D int64, shape 4 / chunk 4 -> 1 chunk, no DimNames
//	├── flags               1-D bool, shape 8 / chunk 8 -> 1 chunk
//	└── scalar              0-D float64 -> 1 chunk, shape ""
func newManifestTree(t *testing.T) *store.Tree {
	t.Helper()
	s := memstore.New().
		AddGroup("/weather", store.Attrs{"title": "station data"}).
		AddArray("/weather/temp", store.Array{
			Shape:      []int64{4, 6},
			ChunkShape: []int64{4, 3},
			DType:      schema.Float64,
			DimNames:   []string{"time", "station"},
		}, nil).
		AddArray("/weather/time", store.Array{
			Shape:      []int64{4},
			ChunkShape: []int64{4},
			DType:      schema.Int64,
		}, nil).
		AddArray("/flags", store.Array{
			Shape:      []int64{8},
			ChunkShape: []int64{8},
			DType:      schema.Bool,
			DimNames:   []string{"id"},
		}, nil).
		AddArray("/scalar", store.Array{
			Shape:      nil,
			ChunkShape: nil,
			DType:      schema.Float64,
		}, nil)

	tree, err := store.Open(context.Background(), s)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	return tree
}

// manifestRow is one decoded catalog row for golden comparison. Pointer fields
// are nil when the underlying Arrow value is null.
type manifestRow struct {
	path       string
	kind       string
	dtype      *string
	ndim       *int32
	shape      *string
	chunkShape *string
	dimNames   *string
	nattrs     int32
	nchunks    *int64
}

func sp(s string) *string { return &s }
func i32(v int32) *int32  { return &v }
func i64(v int64) *int64  { return &v }

// collectRows drains a DataFrame's single batch into manifestRow values.
func collectRows(t *testing.T, df *dataframe.DataFrame) []manifestRow {
	t.Helper()
	iter, err := dataframe.NewRecordBatchIter(df)
	if err != nil {
		t.Fatalf("NewRecordBatchIter: %v", err)
	}
	var rows []manifestRow
	for {
		rec, ok, err := iter.Next()
		if err != nil {
			t.Fatalf("iter.Next: %v", err)
		}
		if !ok {
			break
		}
		rows = append(rows, recordRows(rec)...)
		rec.Release()
	}
	return rows
}

func recordRows(rec arrow.Record) []manifestRow {
	col := func(name string) arrow.Array {
		idxs := rec.Schema().FieldIndices(name)
		return rec.Column(idxs[0])
	}
	pathCol := col("path").(*array.String)
	kindCol := col("kind").(*array.String)
	dtypeCol := col("dtype").(*array.String)
	ndimCol := col("ndim").(*array.Int32)
	shapeCol := col("shape").(*array.String)
	chunkShapeCol := col("chunk_shape").(*array.String)
	dimNamesCol := col("dim_names").(*array.String)
	nattrsCol := col("nattrs").(*array.Int32)
	nchunksCol := col("nchunks").(*array.Int64)

	strAt := func(c *array.String, i int) *string {
		if c.IsNull(i) {
			return nil
		}
		return sp(c.Value(i))
	}
	i32At := func(c *array.Int32, i int) *int32 {
		if c.IsNull(i) {
			return nil
		}
		return i32(c.Value(i))
	}
	i64At := func(c *array.Int64, i int) *int64 {
		if c.IsNull(i) {
			return nil
		}
		return i64(c.Value(i))
	}

	out := make([]manifestRow, rec.NumRows())
	for i := range out {
		out[i] = manifestRow{
			path:       pathCol.Value(i),
			kind:       kindCol.Value(i),
			dtype:      strAt(dtypeCol, i),
			ndim:       i32At(ndimCol, i),
			shape:      strAt(shapeCol, i),
			chunkShape: strAt(chunkShapeCol, i),
			dimNames:   strAt(dimNamesCol, i),
			nattrs:     nattrsCol.Value(i),
			nchunks:    i64At(nchunksCol, i),
		}
	}
	return out
}

func eqPtr[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func rowEq(a, b manifestRow) bool {
	return a.path == b.path && a.kind == b.kind &&
		eqPtr(a.dtype, b.dtype) && eqPtr(a.ndim, b.ndim) &&
		eqPtr(a.shape, b.shape) && eqPtr(a.chunkShape, b.chunkShape) &&
		eqPtr(a.dimNames, b.dimNames) && a.nattrs == b.nattrs &&
		eqPtr(a.nchunks, b.nchunks)
}

func assertRows(t *testing.T, got, want []manifestRow) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("row count = %d, want %d\n got=%+v", len(got), len(want), got)
	}
	for i := range want {
		if !rowEq(got[i], want[i]) {
			t.Errorf("row %d:\n got  %+v\n want %+v", i, got[i], want[i])
		}
	}
}

// goldenRows is the expected manifest for newManifestTree, in Walk order.
func goldenRows() []manifestRow {
	return []manifestRow{
		{path: "/", kind: "group", nattrs: 0},
		{path: "/weather", kind: "group", nattrs: 1},
		{
			path: "/weather/temp", kind: "array",
			dtype: sp("float64"), ndim: i32(2),
			shape: sp("4,6"), chunkShape: sp("4,3"),
			dimNames: sp("time,station"), nattrs: 0, nchunks: i64(2),
		},
		{
			path: "/weather/time", kind: "array",
			dtype: sp("int64"), ndim: i32(1),
			shape: sp("4"), chunkShape: sp("4"),
			dimNames: nil, nattrs: 0, nchunks: i64(1),
		},
		{
			path: "/flags", kind: "array",
			dtype: sp("bool"), ndim: i32(1),
			shape: sp("8"), chunkShape: sp("8"),
			dimNames: sp("id"), nattrs: 0, nchunks: i64(1),
		},
		{
			path: "/scalar", kind: "array",
			dtype: sp("float64"), ndim: i32(0),
			shape: sp(""), chunkShape: sp(""),
			dimNames: nil, nattrs: 0, nchunks: i64(1),
		},
	}
}

func TestManifestGoldenRows(t *testing.T) {
	tree := newManifestTree(t)
	df, err := scan.LazyScanStoreManifest(tree).Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	assertRows(t, collectRows(t, df), goldenRows())
}

// TestManifestTargetQuery runs the design's catalog query verbatim and asserts
// it returns exactly the array rows.
func TestManifestTargetQuery(t *testing.T) {
	tree := newManifestTree(t)
	df, err := scan.LazyScanStoreManifest(tree).
		Filter(expr.Col("kind").Eq(expr.Lit("array"))).
		Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}

	var want []manifestRow
	for _, r := range goldenRows() {
		if r.kind == "array" {
			want = append(want, r)
		}
	}
	assertRows(t, collectRows(t, df), want)
}

// TestManifestGroupRowsNull asserts group rows have null array-only columns.
func TestManifestGroupRowsNull(t *testing.T) {
	tree := newManifestTree(t)
	df, err := scan.LazyScanStoreManifest(tree).
		Filter(expr.Col("kind").Eq(expr.Lit("group"))).
		Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	rows := collectRows(t, df)
	if len(rows) != 2 {
		t.Fatalf("group rows = %d, want 2", len(rows))
	}
	for _, r := range rows {
		if r.dtype != nil || r.ndim != nil || r.shape != nil ||
			r.chunkShape != nil || r.dimNames != nil || r.nchunks != nil {
			t.Errorf("group row %q has non-null array-only column: %+v", r.path, r)
		}
	}
}

// TestManifestCollectStreamParity asserts CollectStream yields the same rows as
// Collect.
func TestManifestCollectStreamParity(t *testing.T) {
	tree := newManifestTree(t)

	df, err := scan.LazyScanStoreManifest(tree).Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	eager := collectRows(t, df)

	rr, err := scan.LazyScanStoreManifest(tree).CollectStream(context.Background())
	if err != nil {
		t.Fatalf("CollectStream: %v", err)
	}
	defer rr.Release()

	var streamed []manifestRow
	for rr.Next() {
		streamed = append(streamed, recordRows(rr.Record())...)
	}
	if err := rr.Err(); err != nil {
		t.Fatalf("stream Err: %v", err)
	}
	assertRows(t, streamed, eager)
}

// TestManifestNChunksEdges checks nchunks for edge chunks (shape not divisible
// by chunk shape) and a 0-d scalar (exactly one chunk).
func TestManifestNChunksEdges(t *testing.T) {
	s := memstore.New().
		// 10 / 3 -> ceil = 4 chunks along the single axis (edge chunk).
		AddArray("/edge1d", store.Array{
			Shape: []int64{10}, ChunkShape: []int64{3}, DType: schema.Int32,
		}, nil).
		// (10,10) / (3,4) -> ceil(10/3)=4 * ceil(10/4)=3 -> 12 chunks.
		AddArray("/edge2d", store.Array{
			Shape: []int64{10, 10}, ChunkShape: []int64{3, 4}, DType: schema.Int32,
		}, nil).
		// 0-d scalar -> 1 chunk.
		AddArray("/scalar", store.Array{
			Shape: nil, ChunkShape: nil, DType: schema.Float64,
		}, nil)

	tree, err := store.Open(context.Background(), s)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}

	df, err := scan.LazyScanStoreManifest(tree).Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	rows := collectRows(t, df)

	want := map[string]int64{"/edge1d": 4, "/edge2d": 12, "/scalar": 1}
	seen := map[string]bool{}
	for _, r := range rows {
		exp, ok := want[r.path]
		if !ok {
			continue
		}
		seen[r.path] = true
		if r.nchunks == nil {
			t.Errorf("%s: nchunks is null, want %d", r.path, exp)
			continue
		}
		if *r.nchunks != exp {
			t.Errorf("%s: nchunks = %d, want %d", r.path, *r.nchunks, exp)
		}
	}
	for p := range want {
		if !seen[p] {
			t.Errorf("missing row for %s", p)
		}
	}
}
