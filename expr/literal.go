package expr

import "fmt"

type Literal struct {
	Value any
}

func (Literal) isExpr() {}

func (l Literal) String() string {
	return fmt.Sprintf("lit(%v)", l.Value)
}

func Lit(v any) Expr { return Literal{Value: v} }
