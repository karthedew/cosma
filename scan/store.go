package scan

import (
	"context"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/karthedew/cosma/carray"
	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/store"
)

// manifestSchema is the flat catalog schema produced by LazyScanStoreManifest.
//
// Every column is a scalar (no list types): compute.filterArray, sort, and take
// have no list-kernel support yet, and Filter re-gathers every column, so a
// list-typed column could not survive the target catalog query. shape,
// chunk_shape, and dim_names are therefore comma-joined utf8 ("100,200"). The
// array-only columns are nullable so group rows can leave them null.
var manifestSchema = arrow.NewSchema([]arrow.Field{
	{Name: "path", Type: arrow.BinaryTypes.String},
	{Name: "kind", Type: arrow.BinaryTypes.String},
	{Name: "dtype", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "ndim", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	{Name: "shape", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "chunk_shape", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "dim_names", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "nattrs", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	{Name: "nchunks", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
}, nil)

// LazyScanStoreManifest catalogs a store.Tree as a LazyFrame: one row per object
// in deterministic Tree.Walk order (the root group included), behind a flat
// schema
//
//	path utf8 | kind utf8 | dtype utf8 | ndim int32 | shape utf8 |
//	chunk_shape utf8 | dim_names utf8 | nattrs int32 | nchunks int64
//
// This is the first end-to-end tracer through the store stack: the design's
// catalog query
//
//	LazyScanStoreManifest(tree).
//	    Filter(expr.Col("kind").Eq(expr.Lit("array"))).
//	    Collect(ctx)
//
// runs store -> dataframe.SourceScan -> plan -> executor with no chunk reads.
//
// Column semantics:
//   - kind is store.ObjectKind.String() ("group" or "array").
//   - Group rows leave the array-only columns null: dtype, ndim, shape,
//     chunk_shape, dim_names, and nchunks. nattrs is populated for every row.
//   - shape / chunk_shape are comma-joined cell extents ("100,200"); a 0-d
//     (scalar) array has shape "" (the empty string, not null) — it is an array,
//     so the column is present, it just has no axes.
//   - dim_names is null when the array has no DimNames (DimNames == nil) and the
//     comma-joined names otherwise. (Choice: absent dim names read as null
//     rather than "" so a named-but-empty case would be distinguishable; arrays
//     never carry zero-length DimNames in practice.)
//   - nchunks is carray.ChunkGrid{Shape, ChunkShape}.NumChunks(); a 0-d array
//     has exactly one chunk.
//
// The tree's metadata is already in memory, so the catalog rows are built once,
// up front, and served from an in-memory record reader: there is no I/O at
// collect time and the schema is statically known. The SourceScan ignores all
// ScanHints — they are advisory, and the surviving Filter/Project/Limit nodes
// above the scan keep results correct regardless. Like scan.LazyScanCSV /
// LazyScanParquet, Collect drains the catalog into a DataFrame and CollectStream
// streams it; both observe the same rows.
func LazyScanStoreManifest(t *store.Tree) *dataframe.LazyFrame {
	rec := buildManifestRecord(t, memory.DefaultAllocator)

	return dataframe.NewLazySourceScan(dataframe.SourceScan{
		Schema:        manifestSchema,
		AllowNullable: true,
		Open: func(_ context.Context, _ dataframe.ScanHints) (array.RecordReader, error) {
			// Hints are advisory and ignored: the catalog is a small, fully
			// in-memory metadata table. NewRecordReader retains the batch, so
			// each returned reader owns its own reference independently of
			// this closure's.
			return array.NewRecordReader(manifestSchema, []arrow.Record{rec})
		},
	})
}

// buildManifestRecord materializes every Tree object into a single manifest
// record batch in Walk order. It performs no chunk I/O — only in-memory metadata
// is read.
func buildManifestRecord(t *store.Tree, mem memory.Allocator) arrow.Record {
	b := array.NewRecordBuilder(mem, manifestSchema)
	defer b.Release()

	path := b.Field(0).(*array.StringBuilder)
	kind := b.Field(1).(*array.StringBuilder)
	dtype := b.Field(2).(*array.StringBuilder)
	ndim := b.Field(3).(*array.Int32Builder)
	shape := b.Field(4).(*array.StringBuilder)
	chunkShape := b.Field(5).(*array.StringBuilder)
	dimNames := b.Field(6).(*array.StringBuilder)
	nattrs := b.Field(7).(*array.Int32Builder)
	nchunks := b.Field(8).(*array.Int64Builder)

	_ = t.Walk(func(obj store.Object) error {
		path.Append(obj.Path())
		kind.Append(obj.Kind().String())
		nattrs.Append(int32(len(obj.Attrs())))

		a, isArray := obj.(*store.Array)
		if !isArray {
			// Group row: every array-only column is null.
			dtype.AppendNull()
			ndim.AppendNull()
			shape.AppendNull()
			chunkShape.AppendNull()
			dimNames.AppendNull()
			nchunks.AppendNull()
			return nil
		}

		dtype.Append(string(a.DType))
		ndim.Append(int32(a.Ndim()))
		shape.Append(joinInts(a.Shape))
		chunkShape.Append(joinInts(a.ChunkShape))
		if a.DimNames == nil {
			dimNames.AppendNull()
		} else {
			dimNames.Append(strings.Join(a.DimNames, ","))
		}
		grid := carray.ChunkGrid{Shape: a.Shape, ChunkShape: a.ChunkShape}
		nchunks.Append(grid.NumChunks())
		return nil
	})

	return b.NewRecord()
}

// joinInts renders cell extents as a comma-joined string ("100,200"). A 0-d
// (scalar) array's empty shape renders as the empty string.
func joinInts(xs []int64) string {
	if len(xs) == 0 {
		return ""
	}
	parts := make([]string, len(xs))
	for i, x := range xs {
		parts[i] = strconv.FormatInt(x, 10)
	}
	return strings.Join(parts, ",")
}
