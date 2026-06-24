package scan

import (
	"encoding/binary"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// cellRows accumulates the kept cells of ONE chunk: for every kept array axis,
// the global cell index of each row; and the row's value bytes (little-endian,
// width-sized — the layout DecodeChunk guarantees). Rows are in C order over the
// kept cells. A chunk's cellRows is later sliced into one or more output records
// (a batch never spans chunks); projection chooses which axis index columns to
// emit at record-build time, so cellRows always retains every kept axis.
type cellRows struct {
	valueType arrow.DataType
	width     int
	keptAxes  []int // array-axis index for each kept column (axis order)
	// idx[k] is the column of global indices for kept axis keptAxes[k]. All
	// idx[k] share one length == row count.
	idx [][]int64
	// vals is the concatenation of every row's width-sized value bytes.
	vals []byte
}

// newCellRows allocates an empty accumulator. keptAxes lists the array axes that
// survive into output (one int64 column each); valueType/width describe the
// value column.
func newCellRows(valueType arrow.DataType, width int, keptAxes []int) cellRows {
	return cellRows{
		valueType: valueType,
		width:     width,
		keptAxes:  keptAxes,
		idx:       make([][]int64, len(keptAxes)),
	}
}

// len returns the row count.
func (r cellRows) len() int {
	if len(r.idx) == 0 {
		return len(r.vals) / max1(r.width)
	}
	return len(r.idx[0])
}

// appendCell appends one row. globalIdx is the cell's full array-axis multi-
// index (nil for a 0-d scalar); the kept axes are projected out of it. The value
// bytes are copied from src[off:off+width].
func (r *cellRows) appendCell(globalIdx []int64, src []byte, off int) {
	for k, axis := range r.keptAxes {
		r.idx[k] = append(r.idx[k], globalIdx[axis])
	}
	r.vals = append(r.vals, src[off:off+r.width]...)
}

// appendFill appends one row carrying the array's pre-encoded fill-value bytes.
func (r *cellRows) appendFill(globalIdx []int64, fill []byte) {
	for k, axis := range r.keptAxes {
		r.idx[k] = append(r.idx[k], globalIdx[axis])
	}
	r.vals = append(r.vals, fill...)
}

// record builds an Arrow record of `count` rows starting at `start`. outCols are
// kept-column positions (indices into r.idx / r.keptAxes) to emit, followed by
// the value column. A coord-valued kept column (loader.isCoord) emits the
// coordinate's values in the coord's dtype, looked up by the row's global index;
// every other kept column emits the int64 index. sc must match the resulting
// column types. loader may be nil when no column is coord-valued.
func (r cellRows) record(mem memory.Allocator, sc *arrow.Schema, outCols []int, start, count int, loader *coordLoader) (arrow.Record, error) {
	b := array.NewRecordBuilder(mem, sc)
	defer b.Release()

	for col, keptPos := range outCols {
		if loader != nil && loader.isCoord(keptPos) {
			fb := b.Field(col)
			for i := 0; i < count; i++ {
				if err := loader.appendValue(keptPos, fb, r.idx[keptPos][start+i]); err != nil {
					return nil, err
				}
			}
			continue
		}
		ib := b.Field(col).(*array.Int64Builder)
		ib.AppendValues(r.idx[keptPos][start:start+count], nil)
	}

	r.appendValues(b.Field(len(outCols)), start, count)
	return b.NewRecord(), nil
}

// appendValues decodes count value cells from the little-endian value bytes into
// the typed builder, starting at row `start`.
func (r cellRows) appendValues(fb array.Builder, start, count int) {
	w := r.width
	for i := 0; i < count; i++ {
		appendDecodedValue(fb, r.vals, (start+i)*w)
	}
}

// appendDecodedValue appends one little-endian value from src[off:] into the
// typed builder; the builder type (what arrowTypeForDType produces for the
// source dtype) determines how many bytes are read. It is the single decode
// point shared by the value column (cellRows.appendValues) and coordinate
// columns (coordDim.appendValue).
func appendDecodedValue(fb array.Builder, src []byte, off int) {
	switch vb := fb.(type) {
	case *array.Int8Builder:
		vb.Append(int8(src[off]))
	case *array.Int16Builder:
		vb.Append(int16(binary.LittleEndian.Uint16(src[off:])))
	case *array.Int32Builder:
		vb.Append(int32(binary.LittleEndian.Uint32(src[off:])))
	case *array.Int64Builder:
		vb.Append(int64(binary.LittleEndian.Uint64(src[off:])))
	case *array.Uint8Builder:
		vb.Append(src[off])
	case *array.Uint16Builder:
		vb.Append(binary.LittleEndian.Uint16(src[off:]))
	case *array.Uint32Builder:
		vb.Append(binary.LittleEndian.Uint32(src[off:]))
	case *array.Uint64Builder:
		vb.Append(binary.LittleEndian.Uint64(src[off:]))
	case *array.Float32Builder:
		vb.Append(math.Float32frombits(binary.LittleEndian.Uint32(src[off:])))
	case *array.Float64Builder:
		vb.Append(math.Float64frombits(binary.LittleEndian.Uint64(src[off:])))
	case *array.BooleanBuilder:
		vb.Append(src[off] != 0)
	default:
		// Unreachable: arrowTypeForDType only produces the cases above.
		panic("scan: unsupported value builder type")
	}
}

// encodeFillValue renders a coerced Go scalar fill value into its little-endian,
// width-sized byte form so appendFill can splice it like any decoded cell.
func encodeFillValue(v any, dt arrow.DataType) ([]byte, error) {
	switch dt.ID() {
	case arrow.BOOL:
		x, ok := v.(bool)
		if !ok {
			return nil, errFillType(v, dt)
		}
		if x {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	case arrow.INT8:
		x, ok := v.(int8)
		if !ok {
			return nil, errFillType(v, dt)
		}
		return []byte{byte(x)}, nil
	case arrow.UINT8:
		x, ok := v.(uint8)
		if !ok {
			return nil, errFillType(v, dt)
		}
		return []byte{x}, nil
	case arrow.INT16:
		x, ok := v.(int16)
		if !ok {
			return nil, errFillType(v, dt)
		}
		buf := make([]byte, 2)
		binary.LittleEndian.PutUint16(buf, uint16(x))
		return buf, nil
	case arrow.UINT16:
		x, ok := v.(uint16)
		if !ok {
			return nil, errFillType(v, dt)
		}
		buf := make([]byte, 2)
		binary.LittleEndian.PutUint16(buf, x)
		return buf, nil
	case arrow.INT32:
		x, ok := v.(int32)
		if !ok {
			return nil, errFillType(v, dt)
		}
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(x))
		return buf, nil
	case arrow.UINT32:
		x, ok := v.(uint32)
		if !ok {
			return nil, errFillType(v, dt)
		}
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, x)
		return buf, nil
	case arrow.FLOAT32:
		x, ok := v.(float32)
		if !ok {
			return nil, errFillType(v, dt)
		}
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(x))
		return buf, nil
	case arrow.INT64:
		x, ok := v.(int64)
		if !ok {
			return nil, errFillType(v, dt)
		}
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(x))
		return buf, nil
	case arrow.UINT64:
		x, ok := v.(uint64)
		if !ok {
			return nil, errFillType(v, dt)
		}
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, x)
		return buf, nil
	case arrow.FLOAT64:
		x, ok := v.(float64)
		if !ok {
			return nil, errFillType(v, dt)
		}
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, math.Float64bits(x))
		return buf, nil
	default:
		return nil, errFillType(v, dt)
	}
}
