package expr

import (
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"

	"github.com/karthedew/cosma/schema"
)

// PromoteLiterals retypes default-inferred numeric literals so they match a
// sibling column's resolved type within BinaryNode operands. It exists so
// callers can write `Col("ids:int32").Gt(2)` without explicitly writing
// `Int32(2)` — the literal `2` parses as a Go int, infers to Int64, and
// without this pass would mismatch the int32 column at the kernel.
//
// The pass is conservative: it only retypes a LiteralNode when its sibling
// resolves to a single, distinct numeric type via the schema. Lit-vs-Lit
// and column-vs-column nodes are left alone — downstream type checking
// surfaces those.
func PromoteLiterals(e Expr, s *schema.Schema) (Expr, error) {
	if e == nil {
		return e, nil
	}
	var firstErr error
	out := Rewrite(e, func(node Expr) Expr {
		if firstErr != nil {
			return node
		}
		bn, ok := node.(BinaryNode)
		if !ok {
			return node
		}
		promoted, err := promoteSiblings(bn, s)
		if err != nil {
			firstErr = err
			return node
		}
		return promoted
	})
	if firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

func promoteSiblings(bn BinaryNode, s *schema.Schema) (BinaryNode, error) {
	leftLit, leftIsLit := bn.Left.(LiteralNode)
	rightLit, rightIsLit := bn.Right.(LiteralNode)

	if leftIsLit && !rightIsLit {
		target, err := bn.Right.DataType(s)
		if err != nil {
			return bn, nil
		}
		promoted, ok, err := promoteLiteralTo(leftLit, target)
		if err != nil {
			return bn, err
		}
		if ok {
			bn.Left = promoted
		}
		return bn, nil
	}
	if rightIsLit && !leftIsLit {
		target, err := bn.Left.DataType(s)
		if err != nil {
			return bn, nil
		}
		promoted, ok, err := promoteLiteralTo(rightLit, target)
		if err != nil {
			return bn, err
		}
		if ok {
			bn.Right = promoted
		}
		return bn, nil
	}
	return bn, nil
}

// promoteLiteralTo converts lit to target if target is numeric and the
// literal's current Go value can be represented losslessly. Returns the
// (possibly retyped) literal, an "ok" flag indicating whether a change
// happened, and an error for impossible conversions (e.g. negative value
// to unsigned).
func promoteLiteralTo(lit LiteralNode, target arrow.DataType) (LiteralNode, bool, error) {
	if target == nil {
		return lit, false, nil
	}
	if lit.Type != nil && lit.Type.ID() == target.ID() {
		return lit, false, nil
	}
	if !isNumeric(target) {
		return lit, false, nil
	}

	switch target.ID() {
	case arrow.INT8:
		v, err := toInt64(lit.Value)
		if err != nil {
			return lit, false, nil
		}
		return LiteralNode{Value: int8(v), Type: target}, true, nil
	case arrow.INT16:
		v, err := toInt64(lit.Value)
		if err != nil {
			return lit, false, nil
		}
		return LiteralNode{Value: int16(v), Type: target}, true, nil
	case arrow.INT32:
		v, err := toInt64(lit.Value)
		if err != nil {
			return lit, false, nil
		}
		return LiteralNode{Value: int32(v), Type: target}, true, nil
	case arrow.INT64:
		v, err := toInt64(lit.Value)
		if err != nil {
			return lit, false, nil
		}
		return LiteralNode{Value: v, Type: target}, true, nil
	case arrow.UINT8:
		v, err := toUint64(lit.Value)
		if err != nil {
			return lit, false, err
		}
		return LiteralNode{Value: uint8(v), Type: target}, true, nil
	case arrow.UINT16:
		v, err := toUint64(lit.Value)
		if err != nil {
			return lit, false, err
		}
		return LiteralNode{Value: uint16(v), Type: target}, true, nil
	case arrow.UINT32:
		v, err := toUint64(lit.Value)
		if err != nil {
			return lit, false, err
		}
		return LiteralNode{Value: uint32(v), Type: target}, true, nil
	case arrow.UINT64:
		v, err := toUint64(lit.Value)
		if err != nil {
			return lit, false, err
		}
		return LiteralNode{Value: v, Type: target}, true, nil
	case arrow.FLOAT32:
		v, err := toFloat64(lit.Value)
		if err != nil {
			return lit, false, nil
		}
		return LiteralNode{Value: float32(v), Type: target}, true, nil
	case arrow.FLOAT64:
		v, err := toFloat64(lit.Value)
		if err != nil {
			return lit, false, nil
		}
		return LiteralNode{Value: v, Type: target}, true, nil
	}
	return lit, false, nil
}

func toInt64(v any) (int64, error) {
	switch x := v.(type) {
	case int:
		return int64(x), nil
	case int8:
		return int64(x), nil
	case int16:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int64:
		return x, nil
	case uint:
		return int64(x), nil
	case uint8:
		return int64(x), nil
	case uint16:
		return int64(x), nil
	case uint32:
		return int64(x), nil
	case uint64:
		return int64(x), nil
	case float32:
		return int64(x), nil
	case float64:
		return int64(x), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}

func toUint64(v any) (uint64, error) {
	switch x := v.(type) {
	case int:
		if x < 0 {
			return 0, fmt.Errorf("negative literal cannot be unsigned")
		}
		return uint64(x), nil
	case int8:
		if x < 0 {
			return 0, fmt.Errorf("negative literal cannot be unsigned")
		}
		return uint64(x), nil
	case int16:
		if x < 0 {
			return 0, fmt.Errorf("negative literal cannot be unsigned")
		}
		return uint64(x), nil
	case int32:
		if x < 0 {
			return 0, fmt.Errorf("negative literal cannot be unsigned")
		}
		return uint64(x), nil
	case int64:
		if x < 0 {
			return 0, fmt.Errorf("negative literal cannot be unsigned")
		}
		return uint64(x), nil
	case uint:
		return uint64(x), nil
	case uint8:
		return uint64(x), nil
	case uint16:
		return uint64(x), nil
	case uint32:
		return uint64(x), nil
	case uint64:
		return x, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to uint64", v)
	}
}

func toFloat64(v any) (float64, error) {
	switch x := v.(type) {
	case int:
		return float64(x), nil
	case int8:
		return float64(x), nil
	case int16:
		return float64(x), nil
	case int32:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case uint:
		return float64(x), nil
	case uint8:
		return float64(x), nil
	case uint16:
		return float64(x), nil
	case uint32:
		return float64(x), nil
	case uint64:
		return float64(x), nil
	case float32:
		return float64(x), nil
	case float64:
		return x, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}
