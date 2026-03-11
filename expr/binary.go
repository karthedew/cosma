package expr

import "fmt"

type BinOp string

const (
	OpEq  BinOp = "=="
	OpNeq BinOp = "!="
	OpLt  BinOp = "<"
	OpLte BinOp = "<="
	OpGt  BinOp = ">"
	OpGte BinOp = ">="
	OpAnd BinOp = "AND"
	OpOr  BinOp = "OR"
	OpAdd BinOp = "+"
	OpSub BinOp = "-"
	OpMul BinOp = "*"
	OpDiv BinOp = "/"
)

type Binary struct {
	Op    BinOp
	Left  Expr
	Right Expr
}

func (Binary) isExpr() {}

func (b Binary) String() string {
	return fmt.Sprintf("(%s %s %s)", b.Left.String(), string(b.Op), b.Right.String())
}

func Bin(op BinOp, left, right Expr) Expr {
	return Binary{Op: op, Left: left, Right: right}
}
