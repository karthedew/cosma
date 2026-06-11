package expr

type BinaryOp uint8

const (
	BinaryOpInvalid BinaryOp = iota

	BinaryOpEq
	BinaryOpNeq
	BinaryOpLt
	BinaryOpLte
	BinaryOpGt
	BinaryOpGte

	BinaryOpAnd
	BinaryOpOr

	BinaryOpAdd
	BinaryOpSub
	BinaryOpMul
	BinaryOpDiv
)

func (op BinaryOp) String() string {
	switch op {
	case BinaryOpEq:
		return "=="
	case BinaryOpNeq:
		return "!="
	case BinaryOpLt:
		return "<"
	case BinaryOpLte:
		return "<="
	case BinaryOpGt:
		return ">"
	case BinaryOpGte:
		return ">="
	case BinaryOpAnd:
		return "and"
	case BinaryOpOr:
		return "or"
	case BinaryOpAdd:
		return "+"
	case BinaryOpSub:
		return "-"
	case BinaryOpMul:
		return "*"
	case BinaryOpDiv:
		return "/"
	default:
		return "<invalid binary op>"
	}
}

type UnaryOp uint8

const (
	UnaryOpInvalid UnaryOp = iota

	UnaryOpNot
	UnaryOpNeg
	UnaryOpIsNull
	UnaryOpIsNotNull
)

func (op UnaryOp) String() string {
	switch op {
	case UnaryOpNot:
		return "not"
	case UnaryOpNeg:
		return "-"
	case UnaryOpIsNull:
		return "is_null"
	case UnaryOpIsNotNull:
		return "is_not_null"
	default:
		return "<invalid unary op>"
	}
}

type AggOp uint8

const (
	AggOpInvalid AggOp = iota

	AggOpCount
	AggOpSum
	AggOpMean
	AggOpMin
	AggOpMax
)

func (op AggOp) String() string {
	switch op {
	case AggOpCount:
		return "count"
	case AggOpSum:
		return "sum"
	case AggOpMean:
		return "mean"
	case AggOpMin:
		return "min"
	case AggOpMax:
		return "max"
	default:
		return "<invalid agg op>"
	}
}
