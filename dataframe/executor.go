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
	// A file-backed scan under eager Collect: open the streaming reader and
	// drain it into a DataFrame, honoring a pushed limit so we stop reading
	// batches early.
	if fs, ok := node.Handle.(FileScan); ok {
		reader, err := fs.open(ctx)
		if err != nil {
			return nil, err
		}
		df, err := collectReader(reader, fs.AllowNullable)
		if err != nil {
			return nil, err
		}
		if node.PushedLimit >= 0 {
			df = df.Limit(int(node.PushedLimit))
		}
		return df, nil
	}
	// A generalized source scan under eager Collect: open the reader with the
	// pushdown hints, drain it into a DataFrame, and honor a pushed limit. The
	// hints are advisory, so the limit above is reapplied even if the source
	// already truncated.
	if ss, ok := node.Handle.(SourceScan); ok {
		reader, err := ss.Open(ctx, scanHints(node))
		if err != nil {
			return nil, err
		}
		df, err := collectReader(reader, ss.AllowNullable)
		if err != nil {
			return nil, err
		}
		if node.PushedLimit >= 0 {
			df = df.Limit(int(node.PushedLimit))
		}
		return df, nil
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

// --- Streaming execution (Phase 5) -------------------------------------------
//
// These methods carry the streaming reader as any (a RecordReader, which is the
// dataframe-local shape of scan.RecordReader). plan defines the streaming
// Executor methods over any for the same no-cycle reason DataFrames cross the
// boundary as any.

func asReader(v any, op string) (RecordReader, error) {
	r, ok := v.(RecordReader)
	if !ok {
		return nil, fmt.Errorf("%s: expected RecordReader, got %T", op, v)
	}
	return r, nil
}

// ScanStream opens a file-backed ScanNode as a streaming RecordReader, honoring
// the pushed column projection where the source supports it. PushedLimit is
// applied above by the streaming PhysLimit node and PushedFilters by the
// streaming PhysFilter node, so the scan itself only prunes columns.
func (DataFrameExecutor) ScanStream(ctx context.Context, node *plan.ScanNode) (any, error) {
	if node == nil {
		return nil, fmt.Errorf("scan stream: node is nil")
	}
	switch h := node.Handle.(type) {
	case FileScan:
		reader, err := h.open(ctx)
		if err != nil {
			return nil, err
		}
		var r RecordReader = reader
		if cols := node.PushedColumns; len(cols) > 0 {
			r = newProjectReader(r, cols)
		}
		return newCtxReader(ctx, r), nil
	case SourceScan:
		reader, err := h.Open(ctx, scanHints(node))
		if err != nil {
			return nil, err
		}
		var r RecordReader = reader
		// Keep the column projection as a fallback: if the source did not prune
		// to PushedColumns itself, this narrows the batches (project is
		// idempotent, so it is harmless when the source already pruned).
		if cols := node.PushedColumns; len(cols) > 0 {
			r = newProjectReader(r, cols)
		}
		return newCtxReader(ctx, r), nil
	default:
		return nil, fmt.Errorf("scan stream: unsupported scan handle %T", node.Handle)
	}
}

// scanHints translates a ScanNode's optimizer pushdown annotations into the
// ScanHints passed to a SourceScan at open time. The annotations are advisory
// (see ScanHints): the surviving Filter/Project/Limit nodes above the scan keep
// results correct regardless of which the source honors.
func scanHints(node *plan.ScanNode) ScanHints {
	return ScanHints{
		Filters: node.PushedFilters,
		Columns: node.PushedColumns,
		Limit:   node.PushedLimit,
	}
}

func (DataFrameExecutor) FilterStream(ctx context.Context, v any, predicate expr.Expr) (any, error) {
	r, err := asReader(v, "filter stream")
	if err != nil {
		return nil, err
	}
	if predicate.Node == nil {
		return nil, fmt.Errorf("filter stream: nil predicate")
	}
	return newFilterReader(ctx, r, predicate), nil
}

func (DataFrameExecutor) ProjectStream(v any, cols []string) (any, error) {
	r, err := asReader(v, "project stream")
	if err != nil {
		return nil, err
	}
	return newProjectReader(r, cols), nil
}

func (DataFrameExecutor) LimitStream(v any, n int64) any {
	r, ok := v.(RecordReader)
	if !ok {
		// Surface the type error through the reader's Err on first Next.
		return &errReader{err: fmt.Errorf("limit stream: expected RecordReader, got %T", v)}
	}
	return newLimitReader(r, n)
}

// StreamFromDataFrame adapts a materialized DataFrame into a RecordReader so a
// pipeline-breaking operator's output (Sort, Aggregate, Join, WithColumn) can be
// streamed downstream.
func (DataFrameExecutor) StreamFromDataFrame(v any) (any, error) {
	df, err := asDF(v, "stream from dataframe")
	if err != nil {
		return nil, err
	}
	return newDFReader(df)
}
