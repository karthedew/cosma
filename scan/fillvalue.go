package scan

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

// fillvalue.go coerces a store.Array.FillValue (free-form, typically the Go
// value JSON decoding produced — float64 for numbers, bool for booleans) into
// the concrete Go scalar that encodeFillValue expects for the value column's
// Arrow type. Coercion is exact: a float fill value with a fractional part on an
// integer column is rejected rather than truncated, surfacing a malformed store
// instead of silently corrupting fill cells.

// coerceFillValue narrows v to the Go scalar type matching dt.
func coerceFillValue(v any, dt arrow.DataType) (any, error) {
	switch dt.ID() {
	case arrow.BOOL:
		b, ok := v.(bool)
		if !ok {
			return nil, errFillType(v, dt)
		}
		return b, nil

	case arrow.INT8:
		n, ok := fillInt(v)
		if !ok {
			return nil, errFillType(v, dt)
		}
		return int8(n), nil
	case arrow.INT16:
		n, ok := fillInt(v)
		if !ok {
			return nil, errFillType(v, dt)
		}
		return int16(n), nil
	case arrow.INT32:
		n, ok := fillInt(v)
		if !ok {
			return nil, errFillType(v, dt)
		}
		return int32(n), nil
	case arrow.INT64:
		n, ok := fillInt(v)
		if !ok {
			return nil, errFillType(v, dt)
		}
		return n, nil
	case arrow.UINT8:
		n, ok := fillInt(v)
		if !ok {
			return nil, errFillType(v, dt)
		}
		return uint8(n), nil
	case arrow.UINT16:
		n, ok := fillInt(v)
		if !ok {
			return nil, errFillType(v, dt)
		}
		return uint16(n), nil
	case arrow.UINT32:
		n, ok := fillInt(v)
		if !ok {
			return nil, errFillType(v, dt)
		}
		return uint32(n), nil
	case arrow.UINT64:
		n, ok := fillInt(v)
		if !ok {
			return nil, errFillType(v, dt)
		}
		return uint64(n), nil

	case arrow.FLOAT32:
		f, ok := fillFloat(v)
		if !ok {
			return nil, errFillType(v, dt)
		}
		return float32(f), nil
	case arrow.FLOAT64:
		f, ok := fillFloat(v)
		if !ok {
			return nil, errFillType(v, dt)
		}
		return f, nil

	default:
		return nil, errFillType(v, dt)
	}
}

// fillInt coerces a numeric fill value to int64. Floats are accepted only when
// integral.
func fillInt(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int32:
		return int64(x), true
	case int16:
		return int64(x), true
	case int8:
		return int64(x), true
	case int:
		return int64(x), true
	case uint64:
		return int64(x), true
	case uint32:
		return int64(x), true
	case uint16:
		return int64(x), true
	case uint8:
		return int64(x), true
	case uint:
		return int64(x), true
	case float64:
		if x == float64(int64(x)) {
			return int64(x), true
		}
		return 0, false
	case float32:
		if x == float32(int64(x)) {
			return int64(x), true
		}
		return 0, false
	default:
		return 0, false
	}
}

// fillFloat coerces a numeric fill value to float64.
func fillFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int64:
		return float64(x), true
	case int32:
		return float64(x), true
	case int:
		return float64(x), true
	case uint64:
		return float64(x), true
	case uint:
		return float64(x), true
	default:
		return 0, false
	}
}

// errFillType reports a fill value that cannot be coerced to the value column's
// Arrow type.
func errFillType(v any, dt arrow.DataType) error {
	return fmt.Errorf("fill value %v (%T) is not assignable to value type %s", v, v, dt)
}
