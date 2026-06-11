package compute

import (
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
)

// FilterRecord returns a new record holding only the rows for which mask is
// true. mask must be a boolean array of length rec.NumRows(); a null mask entry
// is treated as false and drops the row.
//
// Ownership: the caller owns the returned record and must Release it. rec and
// mask are not released.
func FilterRecord(rec arrow.Record, mask arrow.Array, mem memory.Allocator) (arrow.Record, error) {
	if rec == nil {
		return nil, fmt.Errorf("compute.FilterRecord: nil record")
	}
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	b, ok := mask.(*array.Boolean)
	if !ok {
		return nil, fmt.Errorf("compute.FilterRecord: predicate is %s, want bool", mask.DataType())
	}
	if int64(b.Len()) != rec.NumRows() {
		return nil, fmt.Errorf("compute.FilterRecord: mask length %d != rows %d", b.Len(), rec.NumRows())
	}

	cols := make([]arrow.Array, rec.NumCols())
	for i := range cols {
		out, err := filterArray(rec.Column(i), b, mem)
		if err != nil {
			for _, c := range cols[:i] {
				c.Release()
			}
			return nil, err
		}
		cols[i] = out
	}

	out := array.NewRecord(rec.Schema(), cols, countSelected(b))
	for _, c := range cols {
		c.Release()
	}
	return out, nil
}

// countSelected counts the non-null true entries of mask.
func countSelected(mask *array.Boolean) int64 {
	var c int64
	for i := 0; i < mask.Len(); i++ {
		if !mask.IsNull(i) && mask.Value(i) {
			c++
		}
	}
	return c
}

// gather copies the rows of src selected by mask into a fresh array, preserving
// per-row nullity. Null or false mask entries drop the row.
func gather[T any](src valueArray[T], b primBuilder[T], mask *array.Boolean) arrow.Array {
	defer b.Release()
	for i := 0; i < src.Len(); i++ {
		if mask.IsNull(i) || !mask.Value(i) {
			continue
		}
		if src.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(src.Value(i))
	}
	return b.NewArray()
}

// filterArray dispatches the gather over the supported element types.
func filterArray(arr arrow.Array, mask *array.Boolean, mem memory.Allocator) (arrow.Array, error) {
	switch arr.DataType().ID() {
	case arrow.INT8:
		return gather[int8](arr.(*array.Int8), array.NewInt8Builder(mem), mask), nil
	case arrow.INT16:
		return gather[int16](arr.(*array.Int16), array.NewInt16Builder(mem), mask), nil
	case arrow.INT32:
		return gather[int32](arr.(*array.Int32), array.NewInt32Builder(mem), mask), nil
	case arrow.INT64:
		return gather[int64](arr.(*array.Int64), array.NewInt64Builder(mem), mask), nil
	case arrow.UINT8:
		return gather[uint8](arr.(*array.Uint8), array.NewUint8Builder(mem), mask), nil
	case arrow.UINT16:
		return gather[uint16](arr.(*array.Uint16), array.NewUint16Builder(mem), mask), nil
	case arrow.UINT32:
		return gather[uint32](arr.(*array.Uint32), array.NewUint32Builder(mem), mask), nil
	case arrow.UINT64:
		return gather[uint64](arr.(*array.Uint64), array.NewUint64Builder(mem), mask), nil
	case arrow.FLOAT32:
		return gather[float32](arr.(*array.Float32), array.NewFloat32Builder(mem), mask), nil
	case arrow.FLOAT64:
		return gather[float64](arr.(*array.Float64), array.NewFloat64Builder(mem), mask), nil
	case arrow.STRING:
		return gather[string](arr.(*array.String), array.NewStringBuilder(mem), mask), nil
	case arrow.BOOL:
		return gather[bool](arr.(*array.Boolean), array.NewBooleanBuilder(mem), mask), nil
	default:
		return nil, fmt.Errorf("compute.FilterRecord: type %s not supported", arr.DataType())
	}
}
