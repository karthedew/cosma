package scan

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// options_internal_test.go covers the CSV/Parquet option constructors and the
// toIngest mappers. Both option families and toIngest are package-internal.

func applyCSV(opts ...CSVOption) CSVOptions {
	cfg := DefaultCSVOptions()
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

func TestDefaultCSVOptions(t *testing.T) {
	t.Parallel()
	cfg := DefaultCSVOptions()
	assert.True(t, cfg.HasHeader)
	assert.Zero(t, cfg.ChunkSize)
	assert.Nil(t, cfg.NullValues)
}

func TestCSVOptionConstructors(t *testing.T) {
	t.Parallel()

	repl := strings.NewReplacer("a", "b")
	alloc := memory.NewGoAllocator()
	types := map[string]arrow.DataType{"x": arrow.PrimitiveTypes.Int32}

	cfg := applyCSV(
		WithCSVHeader(false),
		WithCSVChunkSize(128),
		WithCSVNullValues([]string{"NA", "null"}),
		WithCSVColumnTypes(types),
		WithCSVIncludeColumns([]string{"x", "y"}),
		WithCSVComma(';'),
		WithCSVComment('#'),
		WithCSVLazyQuotes(true),
		WithCSVStringsReplacer(repl),
		WithCSVAllocator(alloc),
	)

	assert.False(t, cfg.HasHeader)
	assert.Equal(t, 128, cfg.ChunkSize)
	assert.Equal(t, []string{"NA", "null"}, cfg.NullValues)
	assert.Equal(t, types, cfg.ColumnTypes)
	assert.Equal(t, []string{"x", "y"}, cfg.IncludeColumns)
	assert.Equal(t, ';', cfg.Comma)
	assert.Equal(t, '#', cfg.Comment)
	assert.True(t, cfg.LazyQuotes)
	assert.Same(t, repl, cfg.StringsReplacer)
	assert.Same(t, alloc, cfg.Allocator)
}

func TestCSVToIngest(t *testing.T) {
	t.Parallel()

	repl := strings.NewReplacer("x", "y")
	alloc := memory.NewGoAllocator()
	types := map[string]arrow.DataType{"a": arrow.PrimitiveTypes.Float64}

	cfg := applyCSV(
		WithCSVHeader(false),
		WithCSVChunkSize(64),
		WithCSVNullValues([]string{"-"}),
		WithCSVColumnTypes(types),
		WithCSVIncludeColumns([]string{"a"}),
		WithCSVComma('\t'),
		WithCSVComment('%'),
		WithCSVLazyQuotes(true),
		WithCSVStringsReplacer(repl),
		WithCSVAllocator(alloc),
	)
	ing := cfg.toIngest()

	assert.False(t, ing.HasHeader)
	assert.Equal(t, 64, ing.ChunkSize)
	assert.Equal(t, []string{"-"}, ing.NullValues)
	assert.Equal(t, types, ing.ColumnTypes)
	assert.Equal(t, []string{"a"}, ing.IncludeColumns)
	assert.Equal(t, '\t', ing.Comma)
	assert.Equal(t, '%', ing.Comment)
	assert.True(t, ing.LazyQuotes)
	assert.Same(t, repl, ing.StringsReplacer)
	assert.Same(t, alloc, ing.Allocator)
}

func applyParquet(opts ...ParquetOption) ParquetOptions {
	cfg := DefaultParquetOptions()
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

func TestDefaultParquetOptions(t *testing.T) {
	t.Parallel()
	cfg := DefaultParquetOptions()
	assert.Nil(t, cfg.Allocator)
	assert.Nil(t, cfg.ArrowReadProps)
	assert.Nil(t, cfg.Context)
	assert.Nil(t, cfg.ReadOptions)
}

func TestParquetOptionConstructors(t *testing.T) {
	t.Parallel()

	alloc := memory.NewGoAllocator()
	ctx := context.WithValue(context.Background(), struct{ k int }{1}, "v")
	ro := file.WithReadProps(nil)

	cfg := applyParquet(
		WithParquetAllocator(alloc),
		WithParquetReadOptions(ro),
		WithParquetBatchSize(1024),
		WithParquetParallel(true),
		WithParquetContext(ctx),
	)

	assert.Same(t, alloc, cfg.Allocator)
	require.Len(t, cfg.ReadOptions, 1)
	require.NotNil(t, cfg.ArrowReadProps)
	assert.Equal(t, int64(1024), cfg.ArrowReadProps.BatchSize)
	assert.True(t, cfg.ArrowReadProps.Parallel)
	assert.Equal(t, ctx, cfg.Context)
}

// TestParquetBatchSizeThenParallelShareProps confirms the two ArrowReadProps
// mutators lazily allocate once and accumulate onto the same struct.
func TestParquetBatchSizeThenParallelShareProps(t *testing.T) {
	t.Parallel()
	cfg := applyParquet(WithParquetBatchSize(7), WithParquetParallel(true))
	require.NotNil(t, cfg.ArrowReadProps)
	assert.Equal(t, int64(7), cfg.ArrowReadProps.BatchSize)
	assert.True(t, cfg.ArrowReadProps.Parallel)

	// Order-independent: Parallel first then BatchSize.
	cfg2 := applyParquet(WithParquetParallel(true), WithParquetBatchSize(9))
	require.NotNil(t, cfg2.ArrowReadProps)
	assert.Equal(t, int64(9), cfg2.ArrowReadProps.BatchSize)
	assert.True(t, cfg2.ArrowReadProps.Parallel)
}

func TestParquetToIngest(t *testing.T) {
	t.Parallel()

	alloc := memory.NewGoAllocator()
	ctx := context.Background()
	cfg := applyParquet(
		WithParquetAllocator(alloc),
		WithParquetBatchSize(256),
		WithParquetContext(ctx),
	)
	ing := cfg.toIngest()

	assert.Same(t, alloc, ing.Allocator)
	require.NotNil(t, ing.ArrowReadProps)
	assert.Equal(t, int64(256), ing.ArrowReadProps.BatchSize)
	assert.Equal(t, ctx, ing.Context)
}

// TestNilOptionIgnored confirms the scan constructors tolerate a nil option in
// the variadic list (the loop guards against it).
func TestScanCSVNilOptionTolerated(t *testing.T) {
	t.Parallel()
	// A non-existent path still errors, but a nil option must not panic before
	// the open attempt.
	_, err := ScanCSV(context.Background(), "/nonexistent/cosma-missing.csv", nil)
	require.Error(t, err)
}

func TestScanParquetNilOptionTolerated(t *testing.T) {
	t.Parallel()
	_, err := ScanParquet(context.Background(), "/nonexistent/cosma-missing.parquet", nil)
	require.Error(t, err)
}
