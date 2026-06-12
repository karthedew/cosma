package dataframe

import (
	"context"
	"fmt"

	"github.com/karthedew/cosma/expr"
	"github.com/karthedew/cosma/plan"
	"github.com/karthedew/cosma/schema"
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

// NewLazyFileScan builds a LazyFrame whose source is a file-backed scan. The
// physical plan opens the file as a streaming RecordReader under CollectStream
// (no full materialization) and as an eager scan under Collect. It is the
// dataframe-side constructor behind scan.LazyScanCSV / scan.LazyScanParquet,
// which cannot build it directly because plan/schema wiring and the FileScan
// handle live here. The file's schema is read up front so the lazy plan can be
// bound; the data is not drained.
func NewLazyFileScan(fs FileScan) *LazyFrame {
	if fs.Path == "" {
		return &LazyFrame{err: fmt.Errorf("lazy file scan: path is empty")}
	}
	arrSchema, err := FileSchema(fs)
	if err != nil {
		return &LazyFrame{err: fmt.Errorf("lazy file scan: read schema: %w", err)}
	}
	s, err := schema.FromArrow(arrSchema)
	if err != nil {
		return &LazyFrame{err: fmt.Errorf("lazy file scan: schema: %w", err)}
	}
	root := plan.NewScanNode(s, plan.ScanSourceFile)
	root.Handle = fs
	return &LazyFrame{root: root}
}

// NewLazySourceScan builds a LazyFrame whose source is a generalized streaming
// source (a SourceScan). It mirrors NewLazyFileScan but for any caller-supplied
// source: the physical plan opens it via SourceScan.Open under both CollectStream
// (streaming, no full materialization) and Collect (drained into a DataFrame),
// passing the optimizer's pushdown hints so the source can prune at open time.
// The schema is required up front so the lazy plan can be bound without draining
// data; it is an error for SourceScan.Schema or SourceScan.Open to be nil.
func NewLazySourceScan(ss SourceScan) *LazyFrame {
	if ss.Schema == nil {
		return &LazyFrame{err: fmt.Errorf("lazy source scan: schema is nil")}
	}
	if ss.Open == nil {
		return &LazyFrame{err: fmt.Errorf("lazy source scan: open is nil")}
	}
	s, err := schema.FromArrow(ss.Schema)
	if err != nil {
		return &LazyFrame{err: fmt.Errorf("lazy source scan: schema: %w", err)}
	}
	root := plan.NewScanNode(s, plan.ScanSourceCustom)
	root.Handle = ss
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

// CollectStream executes the lazy plan in streaming mode: bind, optimize, lower,
// then run the physical plan batch-by-batch, returning a RecordReader instead of
// a materialized DataFrame. Streamable operators (file scan, Filter, Project,
// Limit) transform one batch at a time, so a filter/limit pipeline over a file
// scan never loads the whole file. Pipeline-breaking operators (Sort, GroupBy,
// Join, WithColumn) materialize their input into a DataFrame first and then
// stream the result downstream. The returned reader is also a scan.RecordReader
// (identical method set), so file scans and streamed plans are interchangeable.
// The caller owns the reader and must Release it.
func (lf *LazyFrame) CollectStream(ctx context.Context) (RecordReader, error) {
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
	out, err := physical.ExecuteStream(ctx, DataFrameExecutor{})
	if err != nil {
		return nil, err
	}
	reader, ok := out.(RecordReader)
	if !ok {
		return nil, fmt.Errorf("collect stream: executor returned %T, want RecordReader", out)
	}
	return reader, nil
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
