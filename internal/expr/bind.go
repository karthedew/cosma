package expr

import (
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"

	"github.com/karthedew/cosma/schema"
)

type BoundPredicate interface {
	Expr
	Eval(rec arrow.Record) (arrow.Array, error)
}

type valueKind int

const (
	kindUnknown valueKind = iota
	kindInt
	kindUint
	kindFloat
	kindString
	kindBool
)

type boundValue interface {
	String() string
	Kind() valueKind
	ValueAt(rec arrow.Record, row int) (any, bool, error)
}

type boundColumn struct {
	name  string
	index int
	kind  valueKind
}

func (c boundColumn) String() string  { return c.name }
func (c boundColumn) Kind() valueKind { return c.kind }

func (c boundColumn) ValueAt(rec arrow.Record, row int) (any, bool, error) {
	if rec == nil {
		return nil, false, fmt.Errorf("record is nil")
	}
	if c.index < 0 || c.index >= int(rec.NumCols()) {
		return nil, false, fmt.Errorf("column index %d out of range", c.index)
	}
	arr := rec.Column(c.index)
	if arr == nil {
		return nil, false, fmt.Errorf("column %d is nil", c.index)
	}
	if row < 0 || row >= arr.Len() {
		return nil, false, fmt.Errorf("row %d out of range", row)
	}
	if arr.IsNull(row) {
		return nil, false, nil
	}

	switch col := arr.(type) {
	case *array.Int8:
		return int64(col.Value(row)), true, nil
	case *array.Int16:
		return int64(col.Value(row)), true, nil
	case *array.Int32:
		return int64(col.Value(row)), true, nil
	case *array.Int64:
		return col.Value(row), true, nil
	case *array.Uint8:
		return uint64(col.Value(row)), true, nil
	case *array.Uint16:
		return uint64(col.Value(row)), true, nil
	case *array.Uint32:
		return uint64(col.Value(row)), true, nil
	case *array.Uint64:
		return col.Value(row), true, nil
	case *array.Float32:
		return float64(col.Value(row)), true, nil
	case *array.Float64:
		return col.Value(row), true, nil
	case *array.String:
		return col.Value(row), true, nil
	case *array.LargeString:
		return col.Value(row), true, nil
	case *array.Boolean:
		return col.Value(row), true, nil
	default:
		return nil, false, fmt.Errorf("unsupported column type %T", arr)
	}
}

type boundLiteral struct {
	value any
	kind  valueKind
}

func (l boundLiteral) String() string  { return fmt.Sprintf("%v", l.value) }
func (l boundLiteral) Kind() valueKind { return l.kind }
func (l boundLiteral) ValueAt(_ arrow.Record, _ int) (any, bool, error) {
	return l.value, true, nil
}

type boundPredicate struct {
	op    string
	left  boundValue
	right boundValue
}

func (p boundPredicate) String() string {
	return fmt.Sprintf("%s %s %s", p.left, p.op, p.right)
}

func (p boundPredicate) Eval(rec arrow.Record) (arrow.Array, error) {
	if rec == nil {
		return nil, fmt.Errorf("record is nil")
	}
	rows := int(rec.NumRows())
	builder := array.NewBooleanBuilder(memory.DefaultAllocator)
	defer builder.Release()

	for i := 0; i < rows; i++ {
		lv, lok, err := p.left.ValueAt(rec, i)
		if err != nil {
			return nil, err
		}
		rv, rok, err := p.right.ValueAt(rec, i)
		if err != nil {
			return nil, err
		}
		if !lok || !rok {
			builder.Append(false)
			continue
		}

		ok, err := compareValues(p.op, p.left.Kind(), lv, rv)
		if err != nil {
			return nil, err
		}
		builder.Append(ok)
	}

	return builder.NewArray(), nil
}

func BindPredicate(e Expr, s *schema.Schema) (BoundPredicate, error) {
	if e == nil {
		return nil, fmt.Errorf("predicate is nil")
	}
	if s == nil {
		return nil, fmt.Errorf("schema is nil")
	}

	switch ex := e.(type) {
	case Eq:
		return bindComparison("==", ex.Left, ex.Right, s)
	case *Eq:
		if ex == nil {
			return nil, fmt.Errorf("predicate is nil")
		}
		return bindComparison("==", ex.Left, ex.Right, s)
	case Gt:
		return bindComparison(">", ex.Left, ex.Right, s)
	case *Gt:
		if ex == nil {
			return nil, fmt.Errorf("predicate is nil")
		}
		return bindComparison(">", ex.Left, ex.Right, s)
	default:
		return nil, fmt.Errorf("unsupported predicate %T", e)
	}
}

func bindComparison(op string, left Expr, right Expr, s *schema.Schema) (BoundPredicate, error) {
	if left == nil || right == nil {
		return nil, fmt.Errorf("comparison operands are nil")
	}

	leftCol, leftIsCol := asCol(left)
	rightCol, rightIsCol := asCol(right)
	leftLit, leftIsLit := asLit(left)
	rightLit, rightIsLit := asLit(right)

	switch {
	case leftIsCol && rightIsLit:
		bcol, kind, err := bindColumn(leftCol, s)
		if err != nil {
			return nil, err
		}
		blit, err := bindLiteral(rightLit, kind)
		if err != nil {
			return nil, err
		}
		return newPredicate(op, bcol, blit)
	case leftIsLit && rightIsCol:
		bcol, kind, err := bindColumn(rightCol, s)
		if err != nil {
			return nil, err
		}
		blit, err := bindLiteral(leftLit, kind)
		if err != nil {
			return nil, err
		}
		return newPredicate(op, blit, bcol)
	case leftIsCol && rightIsCol:
		lcol, lkind, err := bindColumn(leftCol, s)
		if err != nil {
			return nil, err
		}
		rcol, rkind, err := bindColumn(rightCol, s)
		if err != nil {
			return nil, err
		}
		if lkind != rkind {
			return nil, fmt.Errorf("mismatched column types for comparison")
		}
		return newPredicate(op, lcol, rcol)
	case leftIsLit && rightIsLit:
		return nil, fmt.Errorf("comparison requires at least one column")
	default:
		return nil, fmt.Errorf("unsupported comparison operands")
	}
}

func newPredicate(op string, left boundValue, right boundValue) (BoundPredicate, error) {
	if left.Kind() != right.Kind() {
		return nil, fmt.Errorf("comparison requires matching types")
	}
	if op == ">" && !isNumericKind(left.Kind()) {
		return nil, fmt.Errorf("gt comparison only supports numeric types")
	}
	if op == "==" && left.Kind() == kindUnknown {
		return nil, fmt.Errorf("eq comparison requires supported types")
	}
	return boundPredicate{op: op, left: left, right: right}, nil
}

func bindColumn(col Col, s *schema.Schema) (boundValue, valueKind, error) {
	if col.Name == "" {
		return nil, kindUnknown, fmt.Errorf("column name is empty")
	}
	field, ok := s.Field(col.Name)
	if !ok {
		return nil, kindUnknown, fmt.Errorf("column %q not in schema", col.Name)
	}
	idx, ok := s.FieldIndex(col.Name)
	if !ok {
		return nil, kindUnknown, fmt.Errorf("column %q index not found", col.Name)
	}
	kind, err := kindFromArrow(field.ArrowType)
	if err != nil {
		return nil, kindUnknown, err
	}
	return boundColumn{name: col.Name, index: idx, kind: kind}, kind, nil
}

func bindLiteral(lit Lit, kind valueKind) (boundValue, error) {
	val, err := coerceLiteral(lit.Value, kind)
	if err != nil {
		return nil, err
	}
	return boundLiteral{value: val, kind: kind}, nil
}

func kindFromArrow(dt arrow.DataType) (valueKind, error) {
	if dt == nil {
		return kindUnknown, fmt.Errorf("arrow type is nil")
	}
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
		return kindInt, nil
	case arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return kindUint, nil
	case arrow.FLOAT32, arrow.FLOAT64:
		return kindFloat, nil
	case arrow.STRING, arrow.LARGE_STRING:
		return kindString, nil
	case arrow.BOOL:
		return kindBool, nil
	default:
		return kindUnknown, fmt.Errorf("unsupported arrow type %s", dt.Name())
	}
}

func coerceLiteral(v any, kind valueKind) (any, error) {
	switch kind {
	case kindInt:
		return coerceInt(v)
	case kindUint:
		return coerceUint(v)
	case kindFloat:
		return coerceFloat(v)
	case kindString:
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("expected string literal")
		}
		return s, nil
	case kindBool:
		b, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("expected bool literal")
		}
		return b, nil
	default:
		return nil, fmt.Errorf("unsupported literal kind")
	}
}

func coerceInt(v any) (int64, error) {
	switch n := v.(type) {
	case int:
		return int64(n), nil
	case int8:
		return int64(n), nil
	case int16:
		return int64(n), nil
	case int32:
		return int64(n), nil
	case int64:
		return n, nil
	case uint:
		return int64(n), nil
	case uint8:
		return int64(n), nil
	case uint16:
		return int64(n), nil
	case uint32:
		return int64(n), nil
	case uint64:
		return int64(n), nil
	case float32:
		return int64(n), nil
	case float64:
		return int64(n), nil
	default:
		return 0, fmt.Errorf("expected numeric literal")
	}
}

func coerceUint(v any) (uint64, error) {
	switch n := v.(type) {
	case int:
		if n < 0 {
			return 0, fmt.Errorf("expected unsigned literal")
		}
		return uint64(n), nil
	case int8:
		if n < 0 {
			return 0, fmt.Errorf("expected unsigned literal")
		}
		return uint64(n), nil
	case int16:
		if n < 0 {
			return 0, fmt.Errorf("expected unsigned literal")
		}
		return uint64(n), nil
	case int32:
		if n < 0 {
			return 0, fmt.Errorf("expected unsigned literal")
		}
		return uint64(n), nil
	case int64:
		if n < 0 {
			return 0, fmt.Errorf("expected unsigned literal")
		}
		return uint64(n), nil
	case uint:
		return uint64(n), nil
	case uint8:
		return uint64(n), nil
	case uint16:
		return uint64(n), nil
	case uint32:
		return uint64(n), nil
	case uint64:
		return n, nil
	case float32:
		if n < 0 {
			return 0, fmt.Errorf("expected unsigned literal")
		}
		return uint64(n), nil
	case float64:
		if n < 0 {
			return 0, fmt.Errorf("expected unsigned literal")
		}
		return uint64(n), nil
	default:
		return 0, fmt.Errorf("expected numeric literal")
	}
}

func coerceFloat(v any) (float64, error) {
	switch n := v.(type) {
	case int:
		return float64(n), nil
	case int8:
		return float64(n), nil
	case int16:
		return float64(n), nil
	case int32:
		return float64(n), nil
	case int64:
		return float64(n), nil
	case uint:
		return float64(n), nil
	case uint8:
		return float64(n), nil
	case uint16:
		return float64(n), nil
	case uint32:
		return float64(n), nil
	case uint64:
		return float64(n), nil
	case float32:
		return float64(n), nil
	case float64:
		return n, nil
	default:
		return 0, fmt.Errorf("expected numeric literal")
	}
}

func compareValues(op string, kind valueKind, left any, right any) (bool, error) {
	switch kind {
	case kindInt:
		l, ok := left.(int64)
		if !ok {
			return false, fmt.Errorf("expected int64 left")
		}
		r, ok := right.(int64)
		if !ok {
			return false, fmt.Errorf("expected int64 right")
		}
		return compareInts(op, l, r)
	case kindUint:
		l, ok := left.(uint64)
		if !ok {
			return false, fmt.Errorf("expected uint64 left")
		}
		r, ok := right.(uint64)
		if !ok {
			return false, fmt.Errorf("expected uint64 right")
		}
		return compareUints(op, l, r)
	case kindFloat:
		l, ok := left.(float64)
		if !ok {
			return false, fmt.Errorf("expected float64 left")
		}
		r, ok := right.(float64)
		if !ok {
			return false, fmt.Errorf("expected float64 right")
		}
		return compareFloats(op, l, r)
	case kindString:
		l, ok := left.(string)
		if !ok {
			return false, fmt.Errorf("expected string left")
		}
		r, ok := right.(string)
		if !ok {
			return false, fmt.Errorf("expected string right")
		}
		if op != "==" {
			return false, fmt.Errorf("string comparisons only support ==")
		}
		return l == r, nil
	case kindBool:
		l, ok := left.(bool)
		if !ok {
			return false, fmt.Errorf("expected bool left")
		}
		r, ok := right.(bool)
		if !ok {
			return false, fmt.Errorf("expected bool right")
		}
		if op != "==" {
			return false, fmt.Errorf("bool comparisons only support ==")
		}
		return l == r, nil
	default:
		return false, fmt.Errorf("unsupported comparison")
	}
}

func compareInts(op string, l int64, r int64) (bool, error) {
	switch op {
	case "==":
		return l == r, nil
	case ">":
		return l > r, nil
	default:
		return false, fmt.Errorf("unsupported operator %q", op)
	}
}

func compareUints(op string, l uint64, r uint64) (bool, error) {
	switch op {
	case "==":
		return l == r, nil
	case ">":
		return l > r, nil
	default:
		return false, fmt.Errorf("unsupported operator %q", op)
	}
}

func compareFloats(op string, l float64, r float64) (bool, error) {
	switch op {
	case "==":
		return l == r, nil
	case ">":
		return l > r, nil
	default:
		return false, fmt.Errorf("unsupported operator %q", op)
	}
}

func isNumericKind(kind valueKind) bool {
	return kind == kindInt || kind == kindUint || kind == kindFloat
}

func asCol(e Expr) (Col, bool) {
	switch v := e.(type) {
	case Col:
		return v, true
	case *Col:
		if v != nil {
			return *v, true
		}
	}
	return Col{}, false
}

func asLit(e Expr) (Lit, bool) {
	switch v := e.(type) {
	case Lit:
		return v, true
	case *Lit:
		if v != nil {
			return *v, true
		}
	}
	return Lit{}, false
}
