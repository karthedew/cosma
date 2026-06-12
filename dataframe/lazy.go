package dataframe

import (
	"context"
	"fmt"

	"github.com/karthedew/cosma/expr"
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
	root.Handle = df
	return &LazyFrame{root: root}
}

func (lf *LazyFrame) Filter(predicate expr.Expr) *LazyFrame {
	if lf.err != nil {
		return lf
	}
	if predicate.Node == nil {
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

func (lf *LazyFrame) WithColumn(e expr.Expr) *LazyFrame {
	if lf.err != nil {
		return lf
	}
	if e.Node == nil {
		lf.err = fmt.Errorf("withcolumn expression is nil")
		return lf
	}
	lf.root = plan.NewWithColumnNode(lf.root, e)
	return lf
}

func (lf *LazyFrame) Sort(keys ...expr.SortKey) *LazyFrame {
	if lf.err != nil {
		return lf
	}
	if len(keys) == 0 {
		lf.err = fmt.Errorf("sort keys are empty")
		return lf
	}
	lf.root = plan.NewSortNode(lf.root, keys)
	return lf
}

// LazyGroupBy is the intermediate handle returned by LazyFrame.GroupBy; call
// Agg to finish the grouped aggregation and return to a LazyFrame.
type LazyGroupBy struct {
	lf   *LazyFrame
	keys []string
}

func (lf *LazyFrame) GroupBy(keys ...string) *LazyGroupBy {
	return &LazyGroupBy{lf: lf, keys: keys}
}

func (g *LazyGroupBy) Agg(aggs ...expr.AggNode) *LazyFrame {
	lf := g.lf
	if lf.err != nil {
		return lf
	}
	if len(aggs) == 0 {
		lf.err = fmt.Errorf("agg requires at least one aggregation")
		return lf
	}
	lf.root = plan.NewAggregateNode(lf.root, g.keys, aggs)
	return lf
}

func (lf *LazyFrame) Join(other *LazyFrame, on string, how string) *LazyFrame {
	if lf.err != nil {
		return lf
	}
	if other == nil {
		lf.err = fmt.Errorf("join: other LazyFrame is nil")
		return lf
	}
	if other.err != nil {
		lf.err = other.err
		return lf
	}
	lf.root = plan.NewJoinNode(lf.root, other.root, on, how)
	return lf
}

// Collect executes the lazy plan: bind, optimize, lower to a physical plan, and
// run it against the eager dataframe operators.
func (lf *LazyFrame) Collect(ctx context.Context) (*DataFrame, error) {
	logical, err := lf.Plan()
	if err != nil {
		return nil, err
	}
	bound, err := plan.Bind(logical)
	if err != nil {
		return nil, err
	}
	optimized, err := plan.Optimize(bound)
	if err != nil {
		return nil, err
	}
	physical, err := plan.Lower(optimized)
	if err != nil {
		return nil, err
	}
	out, err := physical.Execute(ctx, DataFrameExecutor{})
	if err != nil {
		return nil, err
	}
	df, ok := out.(*DataFrame)
	if !ok {
		return nil, fmt.Errorf("collect: executor returned %T, want *DataFrame", out)
	}
	return df, nil
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
