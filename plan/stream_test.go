package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/expr"
)

// TestExecuteStreamGuards covers ExecuteStream's nil-plan and nil-executor guards.
func TestExecuteStreamGuards(t *testing.T) {
	t.Parallel()

	_, err := (*PhysicalPlan)(nil).ExecuteStream(context.Background(), &fakeExecutor{})
	require.Error(t, err)

	_, err = (&PhysicalPlan{Root: nil}).ExecuteStream(context.Background(), &fakeExecutor{})
	require.Error(t, err)

	phys := buildPhysical(t, NewScanNode(abcSchemaI(), ScanSourceDataFrame))
	_, err = phys.ExecuteStream(context.Background(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "executor is nil")
}

// TestStreamScanFileSource: a file-backed scan opens a streaming reader directly
// via ScanStream (no materialization).
func TestStreamScanFileSource(t *testing.T) {
	t.Parallel()

	phys := buildPhysical(t, NewScanNode(abcSchemaI(), ScanSourceFile))
	exec := &fakeExecutor{}
	_, err := phys.ExecuteStream(context.Background(), exec)
	require.NoError(t, err)
	assert.Equal(t, []string{"ScanStream"}, exec.ops())
}

// TestStreamScanCustomSource: a custom source also streams via ScanStream.
func TestStreamScanCustomSource(t *testing.T) {
	t.Parallel()

	phys := buildPhysical(t, NewScanNode(abcSchemaI(), ScanSourceCustom))
	exec := &fakeExecutor{}
	_, err := phys.ExecuteStream(context.Background(), exec)
	require.NoError(t, err)
	assert.Equal(t, []string{"ScanStream"}, exec.ops())
}

// TestStreamScanDataFrameSource: an in-memory DataFrame scan materializes via
// Scan and is adapted into a reader via StreamFromDataFrame.
func TestStreamScanDataFrameSource(t *testing.T) {
	t.Parallel()

	phys := buildPhysical(t, NewScanNode(abcSchemaI(), ScanSourceDataFrame))
	exec := &fakeExecutor{}
	_, err := phys.ExecuteStream(context.Background(), exec)
	require.NoError(t, err)
	assert.Equal(t, []string{"Scan", "StreamFromDataFrame"}, exec.ops())
}

// TestStreamFilterProjectLimitNative: a Filter/Project/Limit over a file scan
// streams natively through FilterStream/ProjectStream/LimitStream.
func TestStreamFilterProjectLimitNative(t *testing.T) {
	t.Parallel()

	root := NewLimitNode(
		NewProjectNode(
			NewFilterNode(
				NewScanNode(abcSchemaI(), ScanSourceFile),
				expr.Col("a").Gt(expr.Lit(int64(0))),
			),
			[]string{"a"},
		),
		3,
	)
	phys := buildPhysical(t, root)
	exec := &fakeExecutor{}
	_, err := phys.ExecuteStream(context.Background(), exec)
	require.NoError(t, err)
	assert.Equal(t, []string{"ScanStream", "FilterStream", "ProjectStream", "LimitStream"}, exec.ops())
}

// TestStreamPipelineBreakerFallback: Sort/Aggregate/Join/WithColumn do not
// implement StreamingNode, so execStream materializes them via Execute and adapts
// the result with StreamFromDataFrame. Each case asserts the breaker ran
// eagerly (its Execute method fired) and the result was streamed.
func TestStreamPipelineBreakerFallback(t *testing.T) {
	t.Parallel()

	s := abcSchemaI()
	scan := func() *ScanNode { return NewScanNode(s, ScanSourceFile) }

	tests := []struct {
		name string
		root func() LogicalNode
		want []string
	}{
		{
			"sort",
			func() LogicalNode { return NewSortNode(scan(), []expr.SortKey{expr.By("a")}) },
			// Sort.Execute streams its input (file scan) then materializes... but
			// PhysSort.Execute calls Input.Execute (not ExecuteStream), so the
			// upstream scan materializes via Scan, then Sort, then the breaker
			// output is adapted with StreamFromDataFrame.
			[]string{"Scan", "Sort", "StreamFromDataFrame"},
		},
		{
			"aggregate",
			func() LogicalNode {
				return NewAggregateNode(scan(), []string{"a"}, []expr.AggNode{expr.Col("b").Sum()})
			},
			[]string{"Scan", "Aggregate", "StreamFromDataFrame"},
		},
		{
			"withcolumn",
			func() LogicalNode {
				return NewWithColumnNode(scan(), expr.Col("a").Add(expr.Lit(int64(1))).As("d"))
			},
			[]string{"Scan", "WithColumn", "StreamFromDataFrame"},
		},
		{
			"join",
			func() LogicalNode { return NewJoinNode(scan(), scan(), "a", "inner") },
			[]string{"Scan", "Scan", "Join", "StreamFromDataFrame"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			phys := buildPhysical(t, tt.root())
			exec := &fakeExecutor{}
			_, err := phys.ExecuteStream(context.Background(), exec)
			require.NoError(t, err)
			assert.Equal(t, tt.want, exec.ops())
		})
	}
}

// TestStreamBreakerOverStreamableInput: a Filter above a pipeline-breaking Sort.
// The Filter streams natively, but its input (Sort) is a breaker, so the Sort is
// materialized (Scan + Sort) and adapted, then FilterStream runs on the reader.
func TestStreamBreakerOverStreamableInput(t *testing.T) {
	t.Parallel()

	root := NewFilterNode(
		NewSortNode(NewScanNode(abcSchemaI(), ScanSourceFile), []expr.SortKey{expr.By("a")}),
		expr.Col("a").Gt(expr.Lit(int64(0))),
	)
	phys := buildPhysical(t, root)
	exec := &fakeExecutor{}
	_, err := phys.ExecuteStream(context.Background(), exec)
	require.NoError(t, err)
	assert.Equal(t, []string{"Scan", "Sort", "StreamFromDataFrame", "FilterStream"}, exec.ops())
}

// TestStreamErrorPropagation exercises the error returns along streaming paths.
func TestStreamErrorPropagation(t *testing.T) {
	t.Parallel()

	s := abcSchemaI()
	fileScan := func() *ScanNode { return NewScanNode(s, ScanSourceFile) }
	dfScan := func() *ScanNode { return NewScanNode(s, ScanSourceDataFrame) }

	tests := []struct {
		name string
		root func() LogicalNode
		exec *fakeExecutor
	}{
		{"scanstream", func() LogicalNode { return fileScan() }, &fakeExecutor{failScanStream: true}},
		{"scan df materialize", func() LogicalNode { return dfScan() }, &fakeExecutor{failScan: true}},
		{"scan df adapt", func() LogicalNode { return dfScan() }, &fakeExecutor{failStreamFromDF: true}},
		{"filterstream", func() LogicalNode {
			return NewFilterNode(fileScan(), expr.Col("a").Gt(expr.Lit(int64(0))))
		}, &fakeExecutor{failFilterStream: true}},
		{"filterstream upstream", func() LogicalNode {
			return NewFilterNode(fileScan(), expr.Col("a").Gt(expr.Lit(int64(0))))
		}, &fakeExecutor{failScanStream: true}},
		{"projectstream", func() LogicalNode {
			return NewProjectNode(fileScan(), []string{"a"})
		}, &fakeExecutor{failProjectStream: true}},
		{"projectstream upstream", func() LogicalNode {
			return NewProjectNode(fileScan(), []string{"a"})
		}, &fakeExecutor{failScanStream: true}},
		{"limitstream upstream", func() LogicalNode {
			return NewLimitNode(fileScan(), 3)
		}, &fakeExecutor{failScanStream: true}},
		{"breaker execute error", func() LogicalNode {
			return NewSortNode(fileScan(), []expr.SortKey{expr.By("a")})
		}, &fakeExecutor{failScan: true}},
		{"breaker adapt error", func() LogicalNode {
			return NewSortNode(fileScan(), []expr.SortKey{expr.By("a")})
		}, &fakeExecutor{failStreamFromDF: true}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			phys := buildPhysical(t, tt.root())
			_, err := phys.ExecuteStream(context.Background(), tt.exec)
			require.Error(t, err)
		})
	}
}
