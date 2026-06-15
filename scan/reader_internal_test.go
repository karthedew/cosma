package scan

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// reader_internal_test.go covers WithContext / ctxReader and the lazy file-scan
// constructors, all package-internal.

// fakeReader is a minimal RecordReader counting Next calls and reporting a
// configurable end-of-stream and error.
type fakeReader struct {
	remaining int
	err       error
	released  bool
	schema    *arrow.Schema
}

func (f *fakeReader) Schema() *arrow.Schema { return f.schema }
func (f *fakeReader) Record() arrow.Record  { return nil }
func (f *fakeReader) Next() bool {
	if f.remaining <= 0 {
		return false
	}
	f.remaining--
	return true
}
func (f *fakeReader) Err() error { return f.err }
func (f *fakeReader) Release()   { f.released = true }

func TestWithContextNilPassthrough(t *testing.T) {
	t.Parallel()
	inner := &fakeReader{}
	// Nil context returns inner unchanged.
	assert.Same(t, inner, WithContext(nil, inner))
	// Nil inner returns nil.
	assert.Nil(t, WithContext(context.Background(), nil))
}

func TestCtxReaderForwards(t *testing.T) {
	t.Parallel()
	sc := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil)
	inner := &fakeReader{remaining: 2, schema: sc}
	r := WithContext(context.Background(), inner)

	assert.Same(t, sc, r.Schema())
	assert.True(t, r.Next())
	assert.True(t, r.Next())
	assert.False(t, r.Next())
	assert.NoError(t, r.Err())

	r.Release()
	assert.True(t, inner.released)
}

func TestCtxReaderCancellationStopsStream(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	inner := &fakeReader{remaining: 5}
	r := WithContext(ctx, inner)

	// Cancelled context: Next returns false and Err reports the ctx error.
	assert.False(t, r.Next())
	require.Error(t, r.Err())
	assert.ErrorIs(t, r.Err(), context.Canceled)

	// A subsequent Next still returns false (sticky error).
	assert.False(t, r.Next())
}

func TestCtxReaderInnerErrSurfaces(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("boom")
	inner := &fakeReader{remaining: 0, err: wantErr}
	r := WithContext(context.Background(), inner)
	assert.False(t, r.Next())
	assert.ErrorIs(t, r.Err(), wantErr)
}

func TestLazyScanConstructorsSchema(t *testing.T) {
	t.Parallel()
	// The constructors build a LazyFrame without touching the file. They are
	// non-failing; collecting a missing file is what errors. Here we just assert
	// the constructor returns a usable plan whose collect on a bad path errors.
	t.Run("csv", func(t *testing.T) {
		t.Parallel()
		lf := LazyScanCSV("/nonexistent/cosma-missing.csv", WithCSVChunkSize(8), nil)
		require.NotNil(t, lf)
		_, err := lf.Collect(context.Background())
		require.Error(t, err)
	})
	t.Run("parquet", func(t *testing.T) {
		t.Parallel()
		lf := LazyScanParquet("/nonexistent/cosma-missing.parquet", WithParquetBatchSize(8), nil)
		require.NotNil(t, lf)
		_, err := lf.Collect(context.Background())
		require.Error(t, err)
	})
}
