package expr

import (
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v18/arrow"

	"github.com/karthedew/cosma/schema"
)

// ColumnNode references an input column by name. Its output type is resolved
// against the schema at bind time.
type ColumnNode struct {
	Name string
}

func (c ColumnNode) String() string   { return c.Name }
func (c ColumnNode) Children() []Expr { return nil }
func (ColumnNode) exprNode()          {}

func (c ColumnNode) DataType(s *schema.Schema) (arrow.DataType, error) {
	if s == nil {
		return nil, fmt.Errorf("schema is nil")
	}
	field, ok := s.Field(c.Name)
	if !ok {
		return nil, fmt.Errorf("column %q not in schema", c.Name)
	}
	if field.ArrowType == nil {
		return nil, fmt.Errorf("column %q has nil arrow type", c.Name)
	}
	return field.ArrowType, nil
}

// LiteralNode is a scalar constant. Type is resolved at construction so that
// later DataType() calls do not need to re-infer from the Go value.
type LiteralNode struct {
	Value any
	Type  arrow.DataType
}

func (l LiteralNode) String() string {
	if s, ok := l.Value.(string); ok {
		return fmt.Sprintf("%q", s)
	}
	return fmt.Sprintf("%v", l.Value)
}

func (l LiteralNode) Children() []Expr { return nil }
func (LiteralNode) exprNode()          {}

func (l LiteralNode) DataType(_ *schema.Schema) (arrow.DataType, error) {
	if l.Type != nil {
		return l.Type, nil
	}
	return inferLiteralType(l.Value)
}

// BinaryNode is the canonical comparison/logical/arithmetic node. The Op
// determines result-type rules: comparisons and logical ops produce bool;
// arithmetic ops use promoteNumeric on the operand types.
type BinaryNode struct {
	Op    BinaryOp
	Left  Expr
	Right Expr
}

func (b BinaryNode) String() string {
	return fmt.Sprintf("(%s %s %s)", b.Left, b.Op, b.Right)
}

func (b BinaryNode) Children() []Expr { return []Expr{b.Left, b.Right} }
func (BinaryNode) exprNode()          {}

func (b BinaryNode) DataType(s *schema.Schema) (arrow.DataType, error) {
	if b.Left == nil || b.Right == nil {
		return nil, fmt.Errorf("binary node has nil operand")
	}
	switch b.Op {
	case BinaryOpEq, BinaryOpNeq, BinaryOpLt, BinaryOpLte, BinaryOpGt, BinaryOpGte:
		lt, err := b.Left.DataType(s)
		if err != nil {
			return nil, err
		}
		rt, err := b.Right.DataType(s)
		if err != nil {
			return nil, err
		}
		if !comparable(lt, rt) {
			return nil, fmt.Errorf("cannot compare %s and %s", typeName(lt), typeName(rt))
		}
		return arrow.FixedWidthTypes.Boolean, nil

	case BinaryOpAnd, BinaryOpOr:
		lt, err := b.Left.DataType(s)
		if err != nil {
			return nil, err
		}
		rt, err := b.Right.DataType(s)
		if err != nil {
			return nil, err
		}
		if !isBool(lt) || !isBool(rt) {
			return nil, fmt.Errorf("logical %s requires bool operands, got %s and %s", b.Op, typeName(lt), typeName(rt))
		}
		return arrow.FixedWidthTypes.Boolean, nil

	case BinaryOpAdd, BinaryOpSub, BinaryOpMul, BinaryOpDiv:
		lt, err := b.Left.DataType(s)
		if err != nil {
			return nil, err
		}
		rt, err := b.Right.DataType(s)
		if err != nil {
			return nil, err
		}
		if b.Op == BinaryOpDiv {
			return arrow.PrimitiveTypes.Float64, nil
		}
		return promoteNumeric(lt, rt)

	default:
		return nil, fmt.Errorf("unsupported binary op %s", b.Op)
	}
}

// UnaryNode applies a single-operand operator. Logical and null-test ops
// return bool; arithmetic negation preserves the operand type.
type UnaryNode struct {
	Op    UnaryOp
	Inner Expr
}

func (u UnaryNode) String() string   { return fmt.Sprintf("%s(%s)", u.Op, u.Inner) }
func (u UnaryNode) Children() []Expr { return []Expr{u.Inner} }
func (UnaryNode) exprNode()          {}

func (u UnaryNode) DataType(s *schema.Schema) (arrow.DataType, error) {
	if u.Inner == nil {
		return nil, fmt.Errorf("unary node has nil operand")
	}
	switch u.Op {
	case UnaryOpNot:
		t, err := u.Inner.DataType(s)
		if err != nil {
			return nil, err
		}
		if !isBool(t) {
			return nil, fmt.Errorf("not requires bool operand, got %s", typeName(t))
		}
		return arrow.FixedWidthTypes.Boolean, nil
	case UnaryOpIsNull, UnaryOpIsNotNull:
		if _, err := u.Inner.DataType(s); err != nil {
			return nil, err
		}
		return arrow.FixedWidthTypes.Boolean, nil
	case UnaryOpNeg:
		t, err := u.Inner.DataType(s)
		if err != nil {
			return nil, err
		}
		if !isNumeric(t) {
			return nil, fmt.Errorf("neg requires numeric operand, got %s", typeName(t))
		}
		return t, nil
	default:
		return nil, fmt.Errorf("unsupported unary op %s", u.Op)
	}
}

// AggNode is a column-level aggregation: input is one expression, output is
// a single scalar value when reduced. Result types follow conventional rules
// (count → int64, mean → float64, min/max → input type, sum → int64/float64).
type AggNode struct {
	Op    AggOp
	Inner Expr
}

func (a AggNode) String() string   { return fmt.Sprintf("%s(%s)", a.Op, a.Inner) }
func (a AggNode) Children() []Expr { return []Expr{a.Inner} }
func (AggNode) exprNode()          {}

func (a AggNode) DataType(s *schema.Schema) (arrow.DataType, error) {
	if a.Inner == nil {
		return nil, fmt.Errorf("agg node has nil operand")
	}
	switch a.Op {
	case AggOpCount:
		if _, err := a.Inner.DataType(s); err != nil {
			return nil, err
		}
		return arrow.PrimitiveTypes.Int64, nil
	case AggOpMean:
		t, err := a.Inner.DataType(s)
		if err != nil {
			return nil, err
		}
		if !isNumeric(t) {
			return nil, fmt.Errorf("mean requires numeric operand, got %s", typeName(t))
		}
		return arrow.PrimitiveTypes.Float64, nil
	case AggOpSum:
		t, err := a.Inner.DataType(s)
		if err != nil {
			return nil, err
		}
		if !isNumeric(t) {
			return nil, fmt.Errorf("sum requires numeric operand, got %s", typeName(t))
		}
		if isFloat(t) {
			return arrow.PrimitiveTypes.Float64, nil
		}
		if isUnsignedInt(t) {
			return arrow.PrimitiveTypes.Uint64, nil
		}
		return arrow.PrimitiveTypes.Int64, nil
	case AggOpMin, AggOpMax:
		t, err := a.Inner.DataType(s)
		if err != nil {
			return nil, err
		}
		return t, nil
	default:
		return nil, fmt.Errorf("unsupported agg op %s", a.Op)
	}
}

// AliasNode renames the result of Inner. The output type and value are
// passed through unchanged; project operators are responsible for using
// Name when assembling the output schema.
type AliasNode struct {
	Name  string
	Inner Expr
}

func (a AliasNode) String() string {
	if a.Inner == nil {
		return fmt.Sprintf("alias(%q)", a.Name)
	}
	return fmt.Sprintf("%s as %s", a.Inner, a.Name)
}

func (a AliasNode) Children() []Expr { return []Expr{a.Inner} }
func (AliasNode) exprNode()          {}

func (a AliasNode) DataType(s *schema.Schema) (arrow.DataType, error) {
	if strings.TrimSpace(a.Name) == "" {
		return nil, fmt.Errorf("alias name is empty")
	}
	if a.Inner == nil {
		return nil, fmt.Errorf("alias %q has nil inner", a.Name)
	}
	return a.Inner.DataType(s)
}

// CastNode forces the output type of Inner to Type. Bind-time validation
// only checks that the inner expression resolves; runtime evaluation must
// verify the cast is supported and report failures.
type CastNode struct {
	Inner Expr
	Type  arrow.DataType
}

func (c CastNode) String() string   { return fmt.Sprintf("cast(%s as %s)", c.Inner, typeName(c.Type)) }
func (c CastNode) Children() []Expr { return []Expr{c.Inner} }
func (CastNode) exprNode()          {}

func (c CastNode) DataType(s *schema.Schema) (arrow.DataType, error) {
	if c.Type == nil {
		return nil, fmt.Errorf("cast target type is nil")
	}
	if c.Inner == nil {
		return nil, fmt.Errorf("cast has nil inner")
	}
	if _, err := c.Inner.DataType(s); err != nil {
		return nil, err
	}
	return c.Type, nil
}

// comparable reports whether two types support the standard comparison ops.
// Numeric types compare across widths via promotion; other types must match
// exactly (string vs string, bool vs bool, etc.).
func comparable(l, r arrow.DataType) bool {
	if l == nil || r == nil {
		return false
	}
	if isNumeric(l) && isNumeric(r) {
		return true
	}
	return l.ID() == r.ID()
}

func isBool(dt arrow.DataType) bool {
	if dt == nil {
		return false
	}
	return dt.ID() == arrow.BOOL
}
