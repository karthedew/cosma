package dataframe

import (
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"

	"github.com/karthedew/cosma/schema"
)

type DataFrame struct {
	schema *schema.Schema
	cols   []Series
	height int64
}

func New(series []*Series) (*DataFrame, error) {
	fields := make([]schema.Field, len(series))
	cols := make([]Series, len(series))
	nameIndex := make(map[string]struct{}, len(series))
	var h int64 = -1

	for i, s := range series {
		if s == nil {
			return nil, fmt.Errorf("series %d is nil", i)
		}
		if s.Name() == "" {
			return nil, fmt.Errorf("series %d name is empty", i)
		}
		if _, ok := nameIndex[s.Name()]; ok {
			return nil, fmt.Errorf("duplicate series name %q", s.Name())
		}
		nameIndex[s.Name()] = struct{}{}

		field, err := schemaFieldFromArrow(s.Name(), s.DataType())
		if err != nil {
			return nil, fmt.Errorf("series %q dtype: %w", s.Name(), err)
		}

		fields[i] = field
		cols[i] = *s

		colLen := int64(s.Len())
		if h == -1 {
			h = colLen
		} else if colLen != h {
			return nil, fmt.Errorf("series %q len=%d != height=%d", s.Name(), colLen, h)
		}
	}

	s := schema.New(fields...)
	return NewDataFrame(s, cols)
}

func NewDataFrame(s *schema.Schema, cols []Series) (*DataFrame, error) {
	if s == nil {
		return nil, fmt.Errorf("schema is nil")
	}
	if len(cols) != s.Len() {
		return nil, fmt.Errorf("cols (%d) != schema fields (%d)", len(cols), s.Len())
	}

	fields := s.Fields()
	var h int64 = -1
	for i, f := range fields {
		// Height check
		colLen := int64(cols[i].Len())
		if h == -1 {
			h = colLen
		} else if colLen != h {
			return nil, fmt.Errorf("column %q len=%d != height=%d", f.Name, colLen, h)
		}
	}

	if h < 0 {
		h = 0
	}

	return &DataFrame{
		schema: s,
		cols:   cols,
		height: h,
	}, nil
}

func (df *DataFrame) Schema() *schema.Schema { return df.schema }
func (df *DataFrame) Height() int64          { return df.height }
func (df *DataFrame) Width() int             { return len(df.cols) }

func (df *DataFrame) Column(name string) (Series, bool) {
	for _, col := range df.cols {
		if col.Name() == name {
			return col, true
		}
	}
	return Series{}, false
}

func schemaFieldFromArrow(name string, dt arrow.DataType) (schema.Field, error) {
	if dt == nil {
		return schema.Field{}, fmt.Errorf("nil arrow type")
	}
	key, err := schemaDTypeFromArrow(dt)
	if err != nil {
		return schema.Field{}, err
	}
	return schema.Field{Name: name, Type: key, Nullable: false, ArrowType: dt}, nil
}

func schemaDTypeFromArrow(dt arrow.DataType) (schema.DType, error) {
	if dt == nil {
		return "", fmt.Errorf("nil arrow type")
	}
	switch dt.ID() {
	case arrow.NULL:
		return schema.Null, nil
	case arrow.BOOL:
		return schema.Bool, nil
	case arrow.INT8:
		return schema.Int8, nil
	case arrow.INT16:
		return schema.Int16, nil
	case arrow.INT32:
		return schema.Int32, nil
	case arrow.INT64:
		return schema.Int64, nil
	case arrow.UINT8:
		return schema.UInt8, nil
	case arrow.UINT16:
		return schema.UInt16, nil
	case arrow.UINT32:
		return schema.UInt32, nil
	case arrow.UINT64:
		return schema.UInt64, nil
	case arrow.FLOAT16:
		return schema.Float16, nil
	case arrow.FLOAT32:
		return schema.Float32, nil
	case arrow.FLOAT64:
		return schema.Float64, nil
	case arrow.STRING:
		return schema.Utf8, nil
	case arrow.LARGE_STRING:
		return schema.LargeUtf8, nil
	case arrow.STRING_VIEW:
		return schema.StringView, nil
	case arrow.BINARY:
		return schema.Binary, nil
	case arrow.LARGE_BINARY:
		return schema.LargeBinary, nil
	case arrow.FIXED_SIZE_BINARY:
		return schema.FixedSizeBinary, nil
	case arrow.BINARY_VIEW:
		return schema.BinaryView, nil
	case arrow.DATE32:
		return schema.Date32, nil
	case arrow.DATE64:
		return schema.Date64, nil
	case arrow.TIME32:
		return schema.Time32, nil
	case arrow.TIME64:
		return schema.Time64, nil
	case arrow.TIMESTAMP:
		return schema.Timestamp, nil
	case arrow.DURATION:
		return schema.Duration, nil
	case arrow.INTERVAL_MONTHS:
		return schema.IntervalMonth, nil
	case arrow.INTERVAL_DAY_TIME:
		return schema.IntervalDayTime, nil
	case arrow.INTERVAL_MONTH_DAY_NANO:
		return schema.IntervalMDN, nil
	case arrow.DECIMAL128:
		return schema.Decimal128, nil
	case arrow.DECIMAL256:
		return schema.Decimal256, nil
	case arrow.LIST:
		return schema.List, nil
	case arrow.LARGE_LIST:
		return schema.LargeList, nil
	case arrow.FIXED_SIZE_LIST:
		return schema.FixedSizeList, nil
	case arrow.LIST_VIEW:
		return schema.ListView, nil
	case arrow.LARGE_LIST_VIEW:
		return schema.LargeListView, nil
	case arrow.STRUCT:
		return schema.Struct, nil
	case arrow.MAP:
		return schema.Map, nil
	case arrow.SPARSE_UNION:
		return schema.SparseUnion, nil
	case arrow.DENSE_UNION:
		return schema.DenseUnion, nil
	case arrow.DICTIONARY:
		return schema.Dictionary, nil
	case arrow.RUN_END_ENCODED:
		return schema.RunEndEncoded, nil
	case arrow.EXTENSION:
		return schema.Extension, nil
	default:
		return "", fmt.Errorf("unsupported arrow type %q", dt.Name())
	}
}
