package expr

import "fmt"

type AggFn string

const (
	AggSum AggFn = "sum"
	AggMin AggFn = "min"
	AggMax AggFn = "max"
	AggAvg AggFn = "avg"
	AggCnt AggFn = "count"
)

type Agg struct {
	Fn  AggFn
	Arg Expr
	By  []Expr // group keys (optional)
}

func (Agg) isExpr() {}

func (a Agg) String() string {
	return fmt.Sprintf("%s(%s)", string(a.Fn), a.Arg.String())
}

func Sum(e Expr) Expr   { return Agg{Fn: AggSum, Arg: e} }
func Count(e Expr) Expr { return Agg{Fn: AggCnt, Arg: e} }
