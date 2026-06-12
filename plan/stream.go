package plan

import (
	"context"
	"fmt"
)

// StreamingNode is a physical node that can produce its result as a streaming
// RecordReader handle (carried as any; see Executor) instead of a materialized
// DataFrame. Streamable operators (Scan over a file, Filter, Project, Limit)
// transform batches in flight. Pipeline-breaking operators (Sort, Aggregate,
// Join, WithColumn) cannot emit a correct batch until they have seen all input,
// so they materialize via Execute and then stream the result through the
// executor's StreamFromDataFrame — this is the fallback ExecuteStream below.
type StreamingNode interface {
	PhysicalNode
	// ExecuteStream runs the node and returns a streaming reader handle.
	ExecuteStream(ctx context.Context, exec Executor) (any, error)
}

// ExecuteStream runs the physical plan in streaming mode, returning a Record
// reader handle (any wrapping scan.RecordReader) instead of a DataFrame. Nodes
// that implement StreamingNode stream natively; every other node materializes
// (via its Execute) and is adapted into a reader, which is exactly the
// pipeline-breaking contract.
func (p *PhysicalPlan) ExecuteStream(ctx context.Context, exec Executor) (any, error) {
	if p == nil || p.Root == nil {
		return nil, fmt.Errorf("physical plan is empty")
	}
	if exec == nil {
		return nil, fmt.Errorf("executor is nil")
	}
	return execStream(ctx, p.Root, exec)
}

// execStream returns node's output as a streaming reader handle. A node that
// implements StreamingNode streams natively; otherwise the node is materialized
// through Execute and adapted into a reader (the pipeline-breaking path).
func execStream(ctx context.Context, node PhysicalNode, exec Executor) (any, error) {
	if sn, ok := node.(StreamingNode); ok {
		return sn.ExecuteStream(ctx, exec)
	}
	df, err := node.Execute(ctx, exec)
	if err != nil {
		return nil, err
	}
	return exec.StreamFromDataFrame(df)
}

// PhysScan streams: a file source opens a RecordReader directly; an in-memory
// DataFrame source materializes (cheaply, it is already in memory) and adapts.
func (n *PhysScan) ExecuteStream(ctx context.Context, exec Executor) (any, error) {
	if n.Node != nil && n.Node.Source() == ScanSourceFile {
		return exec.ScanStream(ctx, n.Node)
	}
	df, err := exec.Scan(ctx, n.Node)
	if err != nil {
		return nil, err
	}
	return exec.StreamFromDataFrame(df)
}

// PhysFilter streams: filter each batch of the upstream reader.
func (n *PhysFilter) ExecuteStream(ctx context.Context, exec Executor) (any, error) {
	in, err := execStream(ctx, n.Input, exec)
	if err != nil {
		return nil, err
	}
	return exec.FilterStream(ctx, in, n.Predicate)
}

// PhysProject streams: narrow each batch of the upstream reader.
func (n *PhysProject) ExecuteStream(ctx context.Context, exec Executor) (any, error) {
	in, err := execStream(ctx, n.Input, exec)
	if err != nil {
		return nil, err
	}
	return exec.ProjectStream(in, n.Columns)
}

// PhysLimit streams: stop after N rows across batches.
func (n *PhysLimit) ExecuteStream(ctx context.Context, exec Executor) (any, error) {
	in, err := execStream(ctx, n.Input, exec)
	if err != nil {
		return nil, err
	}
	return exec.LimitStream(in, n.N), nil
}

// Compile-time assertions that the streamable operators satisfy StreamingNode.
// Sort, Aggregate, Join, and WithColumn intentionally do NOT implement it: they
// fall through to the materialize-then-stream path in execStream.
var (
	_ StreamingNode = (*PhysScan)(nil)
	_ StreamingNode = (*PhysFilter)(nil)
	_ StreamingNode = (*PhysProject)(nil)
	_ StreamingNode = (*PhysLimit)(nil)
)
