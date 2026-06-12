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

	// --- Streaming execution (Phase 5) ---------------------------------------
	//
	// The streaming methods carry a scan.RecordReader as an opaque any for the
	// same no-cycle reason DataFrames do: scan.LazyScanX returns a
	// *dataframe.LazyFrame, so scan imports dataframe, and dataframe imports
	// plan — a plan import of scan would close the cycle. The concrete executor
	// (in package dataframe) type-asserts these handles back to
	// scan.RecordReader. ScanStream returns a reader directly off a file-backed
	// ScanNode; Filter/Project/Limit Stream transform a reader per batch without
	// materializing the upstream. CollectStream returns the opaque reader; the
	// caller (dataframe) unwraps it to scan.RecordReader.

	// ScanStream opens a file-backed ScanNode as a streaming RecordReader,
	// honoring pushed column/limit annotations where the source supports them.
	ScanStream(ctx context.Context, node *ScanNode) (any, error)
	// FilterStream applies predicate to each batch of reader, yielding a reader
	// over the surviving rows.
	FilterStream(ctx context.Context, reader any, predicate expr.Expr) (any, error)
	// ProjectStream narrows each batch of reader to cols.
	ProjectStream(reader any, cols []string) (any, error)
	// LimitStream stops the stream once n rows have been emitted across batches.
	LimitStream(reader any, n int64) any
	// StreamFromDataFrame adapts a materialized DataFrame handle into a
	// RecordReader so a pipeline-breaking operator's output can be streamed.
	StreamFromDataFrame(df any) (any, error)
}
