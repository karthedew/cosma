package dataframe

import (
	"context"
	"fmt"

	"github.com/karthedew/cosma/expr"
	"github.com/karthedew/cosma/plan"
)

// DataFrameExecutor implements plan.Executor by delegating each physical
// operator to the eager DataFrame methods. It is the concrete bridge that lets
// the plan package drive execution without importing dataframe (which would be
// a cycle): plan defines the Executor interface, dataframe satisfies it, and
// LazyFrame.Collect injects an instance.
type DataFrameExecutor struct{}

var _ plan.Executor = DataFrameExecutor{}

func asDF(v any, op string) (*DataFrame, error) {
	df, ok := v.(*DataFrame)
	if !ok {
		return nil, fmt.Errorf("%s: expected *DataFrame, got %T", op, v)
	}
	return df, nil
}

// Scan materializes the ScanNode's source DataFrame and honors the PushedLimit
// annotation by truncating during the scan. Other pushdown hints (filters,
// columns) are advisory and applied by the surviving operator nodes above.
func (DataFrameExecutor) Scan(ctx context.Context, node *plan.ScanNode) (any, error) {
	if node == nil {
		return nil, fmt.Errorf("scan: node is nil")
	}
	df, err := asDF(node.Handle, "scan")
	if err != nil {
		return nil, err
	}
	if node.PushedLimit >= 0 {
		df = df.Limit(int(node.PushedLimit))
	}
	return df, nil
}

func (DataFrameExecutor) Filter(ctx context.Context, v any, predicate expr.Expr) (any, error) {
	df, err := asDF(v, "filter")
	if err != nil {
		return nil, err
	}
	return df.Filter(ctx, predicate)
}

func (DataFrameExecutor) Project(v any, cols []string) (any, error) {
	df, err := asDF(v, "project")
	if err != nil {
		return nil, err
	}
	return df.Select(cols...)
}

func (DataFrameExecutor) Limit(v any, n int64) (any, error) {
	df, err := asDF(v, "limit")
	if err != nil {
		return nil, err
	}
	return df.Limit(int(n)), nil
}

func (DataFrameExecutor) Sort(ctx context.Context, v any, keys []expr.SortKey) (any, error) {
	df, err := asDF(v, "sort")
	if err != nil {
		return nil, err
	}
	return df.Sort(ctx, keys...)
}

func (DataFrameExecutor) Aggregate(ctx context.Context, v any, groupKeys []string, aggs []expr.AggNode) (any, error) {
	df, err := asDF(v, "aggregate")
	if err != nil {
		return nil, err
	}
	return df.GroupBy(groupKeys...).Agg(ctx, aggs...)
}

func (DataFrameExecutor) Join(ctx context.Context, left, right any, on string, how string) (any, error) {
	l, err := asDF(left, "join left")
	if err != nil {
		return nil, err
	}
	r, err := asDF(right, "join right")
	if err != nil {
		return nil, err
	}
	return l.Join(ctx, r, on, how)
}

func (DataFrameExecutor) WithColumn(v any, e expr.Expr) (any, error) {
	df, err := asDF(v, "withcolumn")
	if err != nil {
		return nil, err
	}
	return df.WithColumn(e)
}
