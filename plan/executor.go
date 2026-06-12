package plan

import (
	"context"

	"github.com/karthedew/cosma/expr"
)

// Executor bridges the plan package to the dataframe package without an import
// cycle. The dataframe package already imports plan, so plan cannot import
// dataframe back. Instead, the dataframe package provides a concrete
// implementation of this interface and injects it into PhysicalPlan.Execute.
//
// All DataFrame values cross this boundary as any: each method receives and
// returns the opaque handle the implementation understands (a
// *dataframe.DataFrame), keeping plan free of a dataframe import.
type Executor interface {
	// Scan materializes the source handle into a DataFrame, optionally honoring
	// scan-level pushdown annotations (columns, limit, filters). source is the
	// ScanNode handle (e.g. a *dataframe.DataFrame).
	Scan(ctx context.Context, node *ScanNode) (any, error)
	Filter(ctx context.Context, df any, predicate expr.Expr) (any, error)
	Project(df any, cols []string) (any, error)
	Limit(df any, n int64) (any, error)
	Sort(ctx context.Context, df any, keys []expr.SortKey) (any, error)
	Aggregate(ctx context.Context, df any, groupKeys []string, aggs []expr.AggNode) (any, error)
	Join(ctx context.Context, left, right any, on string, how string) (any, error)
	WithColumn(df any, e expr.Expr) (any, error)
}
