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
// kept-column positions (indices into r.idx / r.keptAxes) to emit as int64 index
// columns, followed by the value column. sc must match: one int64 field per
// outCols entry, then "value".
func (r cellRows) record(mem memory.Allocator, sc *arrow.Schema, outCols []int, start, count int) arrow.Record {
	b := array.NewRecordBuilder(mem, sc)
	defer b.Release()

	for col, keptPos := range outCols {
		ib := b.Field(col).(*array.Int64Builder)
		ib.AppendValues(r.idx[keptPos][start:start+count], nil)
	}

	r.appendValues(b.Field(len(outCols)), start, count)
	return b.NewRecord()
}

// appendValues decodes count value cells from the little-endian value bytes into
// the typed builder, starting at row `start`.
func (r cellRows) appendValues(fb array.Builder, start, count int) {
	w := r.width
	base := start * w
	switch vb := fb.(type) {
	case *array.Int8Builder:
		for i := 0; i < count; i++ {
			vb.Append(int8(r.vals[base+i*w]))
		}
	case *array.Int16Builder:
		for i := 0; i < count; i++ {
			vb.Append(int16(binary.LittleEndian.Uint16(r.vals[base+i*w:])))
		}
	case *array.Int32Builder:
		for i := 0; i < count; i++ {
			vb.Append(int32(binary.LittleEndian.Uint32(r.vals[base+i*w:])))
		}
	case *array.Int64Builder:
		for i := 0; i < count; i++ {
			vb.Append(int64(binary.LittleEndian.Uint64(r.vals[base+i*w:])))
		}
	case *array.Uint8Builder:
		for i := 0; i < count; i++ {
			vb.Append(r.vals[base+i*w])
		}
	case *array.Uint16Builder:
		for i := 0; i < count; i++ {
			vb.Append(binary.LittleEndian.Uint16(r.vals[base+i*w:]))
		}
	case *array.Uint32Builder:
		for i := 0; i < count; i++ {
			vb.Append(binary.LittleEndian.Uint32(r.vals[base+i*w:]))
		}
	case *array.Uint64Builder:
		for i := 0; i < count; i++ {
			vb.Append(binary.LittleEndian.Uint64(r.vals[base+i*w:]))
		}
	case *array.Float32Builder:
		for i := 0; i < count; i++ {
			vb.Append(math.Float32frombits(binary.LittleEndian.Uint32(r.vals[base+i*w:])))
		}
	case *array.Float64Builder:
		for i := 0; i < count; i++ {
			vb.Append(math.Float64frombits(binary.LittleEndian.Uint64(r.vals[base+i*w:])))
		}
	case *array.BooleanBuilder:
		for i := 0; i < count; i++ {
			vb.Append(r.vals[base+i*w] != 0)
		}
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
