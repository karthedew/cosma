package scan

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cellrows_internal_test.go exercises cellRows.record / appendValues for every
// Arrow value-builder type against real little-endian buffers (no mocking). The
// array-scan parity tests only cover int64/float64 fixtures; this fills in the
// remaining primitive decode branches.

// buildCellRows assembles a one-axis cellRows from a typed slice of values,
// encoding each into width-sized little-endian bytes exactly as DecodeChunk
// would lay them out.
func buildCellRows(valueType arrow.DataType, width int, encode func(i int) []byte, n int) cellRows {
	cr := newCellRows(valueType, width, []int{0})
	for i := 0; i < n; i++ {
		cr.idx[0] = append(cr.idx[0], int64(i))
		cr.vals = append(cr.vals, encode(i)...)
	}
	return cr
}

func le16(v uint16) []byte { b := make([]byte, 2); binary.LittleEndian.PutUint16(b, v); return b }
func le32(v uint32) []byte { b := make([]byte, 4); binary.LittleEndian.PutUint32(b, v); return b }
func le64(v uint64) []byte { b := make([]byte, 8); binary.LittleEndian.PutUint64(b, v); return b }

func TestCellRowsRecordAllValueTypes(t *testing.T) {
	t.Parallel()

	mem := memory.NewGoAllocator()

	t.Run("int8", func(t *testing.T) {
		t.Parallel()
		cr := buildCellRows(arrow.PrimitiveTypes.Int8, 1, func(i int) []byte { return []byte{byte(int8(i - 1))} }, 3)
		rec := record1(mem, cr)
		defer rec.Release()
		col := rec.Column(1).(*array.Int8)
		assert.Equal(t, []int8{-1, 0, 1}, []int8{col.Value(0), col.Value(1), col.Value(2)})
	})

	t.Run("int16", func(t *testing.T) {
		t.Parallel()
		cr := buildCellRows(arrow.PrimitiveTypes.Int16, 2, func(i int) []byte { return le16(uint16(int16(1000 * (i - 1)))) }, 3)
		rec := record1(mem, cr)
		defer rec.Release()
		col := rec.Column(1).(*array.Int16)
		assert.Equal(t, int16(-1000), col.Value(0))
		assert.Equal(t, int16(1000), col.Value(2))
	})

	t.Run("int32", func(t *testing.T) {
		t.Parallel()
		cr := buildCellRows(arrow.PrimitiveTypes.Int32, 4, func(i int) []byte { return le32(uint32(int32(i - 1))) }, 3)
		rec := record1(mem, cr)
		defer rec.Release()
		col := rec.Column(1).(*array.Int32)
		assert.Equal(t, int32(-1), col.Value(0))
	})

	t.Run("int64", func(t *testing.T) {
		t.Parallel()
		cr := buildCellRows(arrow.PrimitiveTypes.Int64, 8, func(i int) []byte { return le64(uint64(int64(1 << 40))) }, 2)
		rec := record1(mem, cr)
		defer rec.Release()
		col := rec.Column(1).(*array.Int64)
		assert.Equal(t, int64(1<<40), col.Value(0))
	})

	t.Run("uint8", func(t *testing.T) {
		t.Parallel()
		cr := buildCellRows(arrow.PrimitiveTypes.Uint8, 1, func(i int) []byte { return []byte{byte(200 + i)} }, 2)
		rec := record1(mem, cr)
		defer rec.Release()
		col := rec.Column(1).(*array.Uint8)
		assert.Equal(t, uint8(200), col.Value(0))
		assert.Equal(t, uint8(201), col.Value(1))
	})

	t.Run("uint16", func(t *testing.T) {
		t.Parallel()
		cr := buildCellRows(arrow.PrimitiveTypes.Uint16, 2, func(i int) []byte { return le16(uint16(60000 + i)) }, 2)
		rec := record1(mem, cr)
		defer rec.Release()
		col := rec.Column(1).(*array.Uint16)
		assert.Equal(t, uint16(60000), col.Value(0))
	})

	t.Run("uint32", func(t *testing.T) {
		t.Parallel()
		cr := buildCellRows(arrow.PrimitiveTypes.Uint32, 4, func(i int) []byte { return le32(uint32(4000000000)) }, 1)
		rec := record1(mem, cr)
		defer rec.Release()
		col := rec.Column(1).(*array.Uint32)
		assert.Equal(t, uint32(4000000000), col.Value(0))
	})

	t.Run("uint64", func(t *testing.T) {
		t.Parallel()
		cr := buildCellRows(arrow.PrimitiveTypes.Uint64, 8, func(i int) []byte { return le64(uint64(1) << 50) }, 1)
		rec := record1(mem, cr)
		defer rec.Release()
		col := rec.Column(1).(*array.Uint64)
		assert.Equal(t, uint64(1)<<50, col.Value(0))
	})

	t.Run("float32", func(t *testing.T) {
		t.Parallel()
		cr := buildCellRows(arrow.PrimitiveTypes.Float32, 4, func(i int) []byte { return le32(math.Float32bits(1.5)) }, 1)
		rec := record1(mem, cr)
		defer rec.Release()
		col := rec.Column(1).(*array.Float32)
		assert.Equal(t, float32(1.5), col.Value(0))
	})

	t.Run("float64", func(t *testing.T) {
		t.Parallel()
		cr := buildCellRows(arrow.PrimitiveTypes.Float64, 8, func(i int) []byte { return le64(math.Float64bits(-3.25)) }, 1)
		rec := record1(mem, cr)
		defer rec.Release()
		col := rec.Column(1).(*array.Float64)
		assert.Equal(t, -3.25, col.Value(0))
	})

	t.Run("bool", func(t *testing.T) {
		t.Parallel()
		cr := buildCellRows(arrow.FixedWidthTypes.Boolean, 1, func(i int) []byte { return []byte{byte(i % 2)} }, 3)
		rec := record1(mem, cr)
		defer rec.Release()
		col := rec.Column(1).(*array.Boolean)
		assert.False(t, col.Value(0))
		assert.True(t, col.Value(1))
		assert.False(t, col.Value(2))
	})
}

// record1 builds a record of the full cellRows with a single index column and
// the value column, using the cellRows' own value type.
func record1(mem memory.Allocator, cr cellRows) arrow.Record {
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "i", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: cr.valueType},
	}, nil)
	rec, _ := cr.record(mem, sc, []int{0}, 0, cr.len(), nil)
	return rec
}

// TestCellRowsRecordSlice confirms record() honors the start/count window so a
// chunk slices into batches that never re-emit rows.
func TestCellRowsRecordSlice(t *testing.T) {
	t.Parallel()
	mem := memory.NewGoAllocator()
	cr := buildCellRows(arrow.PrimitiveTypes.Int64, 8, func(i int) []byte { return le64(uint64(int64(i * 10))) }, 5)

	sc := arrow.NewSchema([]arrow.Field{
		{Name: "i", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	rec, err := cr.record(mem, sc, []int{0}, 2, 2, nil)
	require.NoError(t, err)
	defer rec.Release()
	require.Equal(t, int64(2), rec.NumRows())
	idx := rec.Column(0).(*array.Int64)
	val := rec.Column(1).(*array.Int64)
	assert.Equal(t, []int64{2, 3}, []int64{idx.Value(0), idx.Value(1)})
	assert.Equal(t, []int64{20, 30}, []int64{val.Value(0), val.Value(1)})
}

// TestCellRowsValueOnlyLen exercises len() when there are no index columns
// (a 0-d scalar scan keeps no kept axes), deriving the row count from vals.
func TestCellRowsValueOnlyLen(t *testing.T) {
	t.Parallel()
	cr := newCellRows(arrow.PrimitiveTypes.Float64, 8, nil)
	cr.vals = append(cr.vals, le64(math.Float64bits(42.5))...)
	assert.Equal(t, 1, cr.len())

	mem := memory.NewGoAllocator()
	sc := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Float64}}, nil)
	rec, err := cr.record(mem, sc, nil, 0, 1, nil)
	require.NoError(t, err)
	defer rec.Release()
	require.Equal(t, int64(1), rec.NumRows())
	assert.Equal(t, 42.5, rec.Column(0).(*array.Float64).Value(0))
}
