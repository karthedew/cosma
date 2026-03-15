package dataframe

import (
	"fmt"

	"github.com/karthedew/cosma/internal/expr"
	"github.com/karthedew/cosma/plan"
)

type LazyFrame struct {
	root plan.LogicalNode
	err  error
}

func (df *DataFrame) Lazy() *LazyFrame {
	if df == nil {
		return &LazyFrame{err: fmt.Errorf("dataframe is nil")}
	}
	root := plan.NewScanNode(df.schema, plan.ScanSourceDataFrame)
	return &LazyFrame{root: root}
}

func (lf *LazyFrame) Filter(predicate expr.Expr) *LazyFrame {
	if lf.err != nil {
		return lf
	}
	if predicate == nil {
		lf.err = fmt.Errorf("filter predicate is nil")
		return lf
	}
	lf.root = plan.NewFilterNode(lf.root, predicate)
	return lf
}

func (lf *LazyFrame) Select(cols ...string) *LazyFrame {
	if lf.err != nil {
		return lf
	}
	if len(cols) == 0 {
		lf.err = fmt.Errorf("select columns are empty")
		return lf
	}
	lf.root = plan.NewProjectNode(lf.root, cols)
	return lf
}

func (lf *LazyFrame) Limit(n int64) *LazyFrame {
	if lf.err != nil {
		return lf
	}
	if n < 0 {
		lf.err = fmt.Errorf("limit must be >= 0")
		return lf
	}
	lf.root = plan.NewLimitNode(lf.root, n)
	return lf
}

func (lf *LazyFrame) Plan() (*plan.LogicalPlan, error) {
	if lf.err != nil {
		return nil, lf.err
	}
	if lf.root == nil {
		return nil, fmt.Errorf("lazy plan has no root")
	}
	return plan.NewLogicalPlan(lf.root), nil
}
