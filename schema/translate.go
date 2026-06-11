package schema

import (
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
)

// FromArrow builds a Cosma Schema from an Arrow schema, carrying each field's
// Arrow type through so it can be translated back losslessly.
func FromArrow(s *arrow.Schema) (*Schema, error) {
	if s == nil {
		return nil, fmt.Errorf("arrow schema is nil")
	}
	fields := s.Fields()
	cosmaFields := make([]Field, len(fields))
	for i, f := range fields {
		dtype, err := DTypeFromArrow(f.Type)
		if err != nil {
			return nil, fmt.Errorf("schema field %q dtype: %w", f.Name, err)
		}
		cosmaFields[i] = Field{
			Name:      f.Name,
			Type:      dtype,
			Nullable:  f.Nullable,
			ArrowType: f.Type,
		}
	}
	return New(cosmaFields...), nil
}

// ToArrow builds an Arrow schema from a Cosma Schema. Every field must carry a
// non-nil ArrowType (set by FromArrow or at construction).
func ToArrow(s *Schema) (*arrow.Schema, error) {
	if s == nil {
		return nil, fmt.Errorf("schema is nil")
	}
	fields := s.Fields()
	arrowFields := make([]arrow.Field, len(fields))
	for i, f := range fields {
		if f.ArrowType == nil {
			return nil, fmt.Errorf("schema field %q has nil arrow type", f.Name)
		}
		arrowFields[i] = arrow.Field{
			Name:     f.Name,
			Type:     f.ArrowType,
			Nullable: f.Nullable,
		}
	}
	return arrow.NewSchema(arrowFields, nil), nil
}

// FieldFromArrow builds a single non-nullable Field from a name and Arrow type.
func FieldFromArrow(name string, dt arrow.DataType) (Field, error) {
	dtype, err := DTypeFromArrow(dt)
	if err != nil {
		return Field{}, err
	}
	return Field{Name: name, Type: dtype, Nullable: false, ArrowType: dt}, nil
}

// DTypeFromArrow maps an Arrow data type to its Cosma logical DType.
func DTypeFromArrow(dt arrow.DataType) (DType, error) {
	if dt == nil {
		return "", fmt.Errorf("nil arrow type")
	}
	switch dt.ID() {
	case arrow.NULL:
		return Null, nil
	case arrow.BOOL:
		return Bool, nil
	case arrow.INT8:
		return Int8, nil
	case arrow.INT16:
		return Int16, nil
	case arrow.INT32:
		return Int32, nil
	case arrow.INT64:
		return Int64, nil
	case arrow.UINT8:
		return UInt8, nil
	case arrow.UINT16:
		return UInt16, nil
	case arrow.UINT32:
		return UInt32, nil
	case arrow.UINT64:
		return UInt64, nil
	case arrow.FLOAT16:
		return Float16, nil
	case arrow.FLOAT32:
		return Float32, nil
	case arrow.FLOAT64:
		return Float64, nil
	case arrow.STRING:
		return Utf8, nil
	case arrow.LARGE_STRING:
		return LargeUtf8, nil
	case arrow.STRING_VIEW:
		return StringView, nil
	case arrow.BINARY:
		return Binary, nil
	case arrow.LARGE_BINARY:
		return LargeBinary, nil
	case arrow.FIXED_SIZE_BINARY:
		return FixedSizeBinary, nil
	case arrow.BINARY_VIEW:
		return BinaryView, nil
	case arrow.DATE32:
		return Date32, nil
	case arrow.DATE64:
		return Date64, nil
	case arrow.TIME32:
		return Time32, nil
	case arrow.TIME64:
		return Time64, nil
	case arrow.TIMESTAMP:
		return Timestamp, nil
	case arrow.DURATION:
		return Duration, nil
	case arrow.INTERVAL_MONTHS:
		return IntervalMonth, nil
	case arrow.INTERVAL_DAY_TIME:
		return IntervalDayTime, nil
	case arrow.INTERVAL_MONTH_DAY_NANO:
		return IntervalMDN, nil
	case arrow.DECIMAL128:
		return Decimal128, nil
	case arrow.DECIMAL256:
		return Decimal256, nil
	case arrow.LIST:
		return List, nil
	case arrow.LARGE_LIST:
		return LargeList, nil
	case arrow.FIXED_SIZE_LIST:
		return FixedSizeList, nil
	case arrow.LIST_VIEW:
		return ListView, nil
	case arrow.LARGE_LIST_VIEW:
		return LargeListView, nil
	case arrow.STRUCT:
		return Struct, nil
	case arrow.MAP:
		return Map, nil
	case arrow.SPARSE_UNION:
		return SparseUnion, nil
	case arrow.DENSE_UNION:
		return DenseUnion, nil
	case arrow.DICTIONARY:
		return Dictionary, nil
	case arrow.RUN_END_ENCODED:
		return RunEndEncoded, nil
	case arrow.EXTENSION:
		return Extension, nil
	default:
		return "", fmt.Errorf("unsupported arrow type %q", dt.Name())
	}
}
