package expr

// Expr is the logical expression tree used by lazy planning.
// Start minimal; expand as you implement projection/filter/groupby.
type Expr interface {
	isExpr()
	String() string
}
