package dataframe

import (
	"fmt"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/decimal128"
	"github.com/apache/arrow/go/v18/arrow/decimal256"
	"github.com/apache/arrow/go/v18/arrow/float16"
	"github.com/apache/arrow/go/v18/arrow/memory"
)

func NewSeries(name string, values any) (*Series, error) {
	if name == "" {
		return nil, fmt.Errorf("series name is empty")
	}
	switch v := values.(type) {
	case *arrow.Chunked:
		return NewSeriesFromChunked(name, v), nil
	case arrow.Array:
		return NewSeriesFromArray(name, v)
	case []bool:
		return NewSeriesFromChunked(name, chunkedFromBool(v)), nil
	case []int8:
		return NewSeriesFromChunked(name, chunkedFromInt8(v)), nil
	case []int16:
		return NewSeriesFromChunked(name, chunkedFromInt16(v)), nil
	case []int32:
		return NewSeriesFromChunked(name, chunkedFromInt32(v)), nil
	case []int64:
		return NewSeriesFromChunked(name, chunkedFromInt64(v)), nil
	case []uint8:
		return NewSeriesFromChunked(name, chunkedFromUint8(v)), nil
	case []uint16:
		return NewSeriesFromChunked(name, chunkedFromUint16(v)), nil
	case []uint32:
		return NewSeriesFromChunked(name, chunkedFromUint32(v)), nil
	case []uint64:
		return NewSeriesFromChunked(name, chunkedFromUint64(v)), nil
	case []float16.Num:
		return NewSeriesFromChunked(name, chunkedFromFloat16(v)), nil
	case []float32:
		return NewSeriesFromChunked(name, chunkedFromFloat32(v)), nil
	case []float64:
		return NewSeriesFromChunked(name, chunkedFromFloat64(v)), nil
	case []string:
		return NewSeriesFromChunked(name, chunkedFromString(v)), nil
	case [][]byte:
		return NewSeriesFromChunked(name, chunkedFromBinary(v)), nil
	case []arrow.Date32:
		return NewSeriesFromChunked(name, chunkedFromDate32(v)), nil
	case []arrow.Date64:
		return NewSeriesFromChunked(name, chunkedFromDate64(v)), nil
	default:
		return nil, fmt.Errorf("unsupported series values type %T", values)
	}
}

func NewSeriesFromArray(name string, arr arrow.Array) (*Series, error) {
	if name == "" {
		return nil, fmt.Errorf("series name is empty")
	}
	if arr == nil {
		return nil, fmt.Errorf("series array is nil")
	}
	return NewSeriesFromChunked(name, arrow.NewChunked(arr.DataType(), []arrow.Array{arr})), nil
}

func NewSeriesNull(name string, length int) (*Series, error) {
	if length < 0 {
		return nil, fmt.Errorf("null length must be >= 0")
	}
	return NewSeriesFromArray(name, array.NewNull(length))
}

func NewSeriesLargeUtf8(name string, values []string) (*Series, error) {
	builder := array.NewLargeStringBuilder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	return NewSeriesFromChunked(name, arrow.NewChunked(arr.DataType(), []arrow.Array{arr})), nil
}

func NewSeriesBinary(name string, values [][]byte) (*Series, error) {
	return NewSeriesFromChunked(name, chunkedFromBinary(values)), nil
}

func NewSeriesLargeBinary(name string, values [][]byte) (*Series, error) {
	builder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.LargeBinary)
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	return NewSeriesFromChunked(name, arrow.NewChunked(arr.DataType(), []arrow.Array{arr})), nil
}

func NewSeriesFixedSizeBinary(name string, values [][]byte, byteWidth int) (*Series, error) {
	if byteWidth <= 0 {
		return nil, fmt.Errorf("byte width must be > 0")
	}
	builder := array.NewFixedSizeBinaryBuilder(memory.DefaultAllocator, &arrow.FixedSizeBinaryType{ByteWidth: byteWidth})
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	return NewSeriesFromChunked(name, arrow.NewChunked(arr.DataType(), []arrow.Array{arr})), nil
}

func NewSeriesTime32(name string, values []arrow.Time32, unit arrow.TimeUnit) (*Series, error) {
	builder := array.NewTime32Builder(memory.DefaultAllocator, &arrow.Time32Type{Unit: unit})
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	return NewSeriesFromChunked(name, arrow.NewChunked(arr.DataType(), []arrow.Array{arr})), nil
}

func NewSeriesTime64(name string, values []arrow.Time64, unit arrow.TimeUnit) (*Series, error) {
	builder := array.NewTime64Builder(memory.DefaultAllocator, &arrow.Time64Type{Unit: unit})
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	return NewSeriesFromChunked(name, arrow.NewChunked(arr.DataType(), []arrow.Array{arr})), nil
}

func NewSeriesTimestamp(name string, values []time.Time, unit arrow.TimeUnit, tz string) (*Series, error) {
	builder := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: unit, TimeZone: tz})
	defer builder.Release()
	for _, v := range values {
		builder.AppendTime(v)
	}
	arr := builder.NewArray()
	return NewSeriesFromChunked(name, arrow.NewChunked(arr.DataType(), []arrow.Array{arr})), nil
}

func NewSeriesTimestampValues(name string, values []arrow.Timestamp, unit arrow.TimeUnit, tz string) (*Series, error) {
	builder := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: unit, TimeZone: tz})
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	return NewSeriesFromChunked(name, arrow.NewChunked(arr.DataType(), []arrow.Array{arr})), nil
}

func NewSeriesDuration(name string, values []arrow.Duration, unit arrow.TimeUnit) (*Series, error) {
	builder := array.NewDurationBuilder(memory.DefaultAllocator, &arrow.DurationType{Unit: unit})
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	return NewSeriesFromChunked(name, arrow.NewChunked(arr.DataType(), []arrow.Array{arr})), nil
}

func NewSeriesMonthInterval(name string, values []arrow.MonthInterval) (*Series, error) {
	builder := array.NewMonthIntervalBuilder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	return NewSeriesFromChunked(name, arrow.NewChunked(arr.DataType(), []arrow.Array{arr})), nil
}

func NewSeriesDayTimeInterval(name string, values []arrow.DayTimeInterval) (*Series, error) {
	builder := array.NewDayTimeIntervalBuilder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	return NewSeriesFromChunked(name, arrow.NewChunked(arr.DataType(), []arrow.Array{arr})), nil
}

func NewSeriesMonthDayNanoInterval(name string, values []arrow.MonthDayNanoInterval) (*Series, error) {
	builder := array.NewMonthDayNanoIntervalBuilder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	return NewSeriesFromChunked(name, arrow.NewChunked(arr.DataType(), []arrow.Array{arr})), nil
}

func NewSeriesDecimal128(name string, values []decimal128.Num, precision, scale int32) (*Series, error) {
	builder := array.NewDecimal128Builder(memory.DefaultAllocator, &arrow.Decimal128Type{Precision: precision, Scale: scale})
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	return NewSeriesFromChunked(name, arrow.NewChunked(arr.DataType(), []arrow.Array{arr})), nil
}

func NewSeriesDecimal256(name string, values []decimal256.Num, precision, scale int32) (*Series, error) {
	builder := array.NewDecimal256Builder(memory.DefaultAllocator, &arrow.Decimal256Type{Precision: precision, Scale: scale})
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	return NewSeriesFromChunked(name, arrow.NewChunked(arr.DataType(), []arrow.Array{arr})), nil
}

func chunkedFromBool(values []bool) *arrow.Chunked {
	builder := array.NewBooleanBuilder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	return arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
}

func chunkedFromInt8(values []int8) *arrow.Chunked {
	builder := array.NewInt8Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	return arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
}

func chunkedFromInt16(values []int16) *arrow.Chunked {
	builder := array.NewInt16Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	return arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
}

func chunkedFromInt32(values []int32) *arrow.Chunked {
	builder := array.NewInt32Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	return arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
}

func chunkedFromInt64(values []int64) *arrow.Chunked {
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	return arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
}

func chunkedFromUint8(values []uint8) *arrow.Chunked {
	builder := array.NewUint8Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	return arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
}

func chunkedFromUint16(values []uint16) *arrow.Chunked {
	builder := array.NewUint16Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	return arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
}

func chunkedFromUint32(values []uint32) *arrow.Chunked {
	builder := array.NewUint32Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	return arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
}

func chunkedFromUint64(values []uint64) *arrow.Chunked {
	builder := array.NewUint64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	return arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
}

func chunkedFromFloat16(values []float16.Num) *arrow.Chunked {
	builder := array.NewFloat16Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	return arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
}

func chunkedFromFloat32(values []float32) *arrow.Chunked {
	builder := array.NewFloat32Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	return arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
}

func chunkedFromFloat64(values []float64) *arrow.Chunked {
	builder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	return arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
}

func chunkedFromString(values []string) *arrow.Chunked {
	builder := array.NewStringBuilder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	return arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
}

func chunkedFromBinary(values [][]byte) *arrow.Chunked {
	builder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	return arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
}

func chunkedFromDate32(values []arrow.Date32) *arrow.Chunked {
	builder := array.NewDate32Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	return arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
}

func chunkedFromDate64(values []arrow.Date64) *arrow.Chunked {
	builder := array.NewDate64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	return arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
}
