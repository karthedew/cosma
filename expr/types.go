package expr

import (
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
)

func isNumeric(dt arrow.DataType) bool {
	if dt == nil {
		return false
	}
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT32, arrow.FLOAT64:
		return true
	}
	return false
}

func isFloat(dt arrow.DataType) bool {
	if dt == nil {
		return false
	}
	return dt.ID() == arrow.FLOAT32 || dt.ID() == arrow.FLOAT64
}

func isSignedInt(dt arrow.DataType) bool {
	if dt == nil {
		return false
	}
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
		return true
	}
	return false
}

func isUnsignedInt(dt arrow.DataType) bool {
	if dt == nil {
		return false
	}
	switch dt.ID() {
	case arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return true
	}
	return false
}

// numericRank returns a width tier for numeric arrow types: 1=8-bit, 2=16-bit,
// 3=32-bit, 4=64-bit. The boolean indicates whether the type is numeric.
func numericRank(dt arrow.DataType) (int, bool) {
	if dt == nil {
		return 0, false
	}
	switch dt.ID() {
	case arrow.INT8, arrow.UINT8:
		return 1, true
	case arrow.INT16, arrow.UINT16:
		return 2, true
	case arrow.INT32, arrow.UINT32, arrow.FLOAT32:
		return 3, true
	case arrow.INT64, arrow.UINT64, arrow.FLOAT64:
		return 4, true
	}
	return 0, false
}

// promoteNumeric returns the arithmetic result type for two numeric inputs.
// Rules:
//   - any float operand → float (float64 if any operand is 64-bit or integral, else float32)
//   - both signed → widest signed
//   - both unsigned → widest unsigned
//   - mixed signedness → int64 (lossy for uint64 but predictable)
func promoteNumeric(l, r arrow.DataType) (arrow.DataType, error) {
	if !isNumeric(l) || !isNumeric(r) {
		return nil, fmt.Errorf("cannot promote non-numeric types %s and %s", typeName(l), typeName(r))
	}

	if isFloat(l) || isFloat(r) {
		if l.ID() == arrow.FLOAT64 || r.ID() == arrow.FLOAT64 {
			return arrow.PrimitiveTypes.Float64, nil
		}
		if !isFloat(l) || !isFloat(r) {
			return arrow.PrimitiveTypes.Float64, nil
		}
		return arrow.PrimitiveTypes.Float32, nil
	}

	lSigned := isSignedInt(l)
	rSigned := isSignedInt(r)
	lRank, _ := numericRank(l)
	rRank, _ := numericRank(r)
	rank := lRank
	if rRank > rank {
		rank = rRank
	}

	if lSigned != rSigned {
		return arrow.PrimitiveTypes.Int64, nil
	}
	if lSigned {
		return signedIntForRank(rank), nil
	}
	return unsignedIntForRank(rank), nil
}

func signedIntForRank(rank int) arrow.DataType {
	switch rank {
	case 1:
		return arrow.PrimitiveTypes.Int8
	case 2:
		return arrow.PrimitiveTypes.Int16
	case 3:
		return arrow.PrimitiveTypes.Int32
	default:
		return arrow.PrimitiveTypes.Int64
	}
}

func unsignedIntForRank(rank int) arrow.DataType {
	switch rank {
	case 1:
		return arrow.PrimitiveTypes.Uint8
	case 2:
		return arrow.PrimitiveTypes.Uint16
	case 3:
		return arrow.PrimitiveTypes.Uint32
	default:
		return arrow.PrimitiveTypes.Uint64
	}
}

// inferLiteralType maps a Go scalar value to the canonical Arrow type used by
// LiteralNode. Untyped Go integers become int64 and untyped floats become
// float64 — typed Go scalars (int32, float32, ...) keep their width so the
// builder helpers can act as escape hatches.
func inferLiteralType(v any) (arrow.DataType, error) {
	switch v.(type) {
	case nil:
		return nil, fmt.Errorf("literal value is nil")
	case bool:
		return arrow.FixedWidthTypes.Boolean, nil
	case string:
		return arrow.BinaryTypes.String, nil
	case int8:
		return arrow.PrimitiveTypes.Int8, nil
	case int16:
		return arrow.PrimitiveTypes.Int16, nil
	case int32:
		return arrow.PrimitiveTypes.Int32, nil
	case int, int64:
		return arrow.PrimitiveTypes.Int64, nil
	case uint8:
		return arrow.PrimitiveTypes.Uint8, nil
	case uint16:
		return arrow.PrimitiveTypes.Uint16, nil
	case uint32:
		return arrow.PrimitiveTypes.Uint32, nil
	case uint, uint64:
		return arrow.PrimitiveTypes.Uint64, nil
	case float32:
		return arrow.PrimitiveTypes.Float32, nil
	case float64:
		return arrow.PrimitiveTypes.Float64, nil
	default:
		return nil, fmt.Errorf("unsupported literal type %T", v)
	}
}

func typeName(dt arrow.DataType) string {
	if dt == nil {
		return "<nil>"
	}
	return dt.Name()
}
