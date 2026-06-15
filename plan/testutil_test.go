package plan

import (
	"context"
	"errors"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/karthedew/cosma/expr"
	"github.com/karthedew/cosma/schema"
)

// abcSchemaI is a three-column int64 source schema for plan-internal tests. It
// mirrors abcSchema in optimizer_pushcolumns_test.go but lives in package plan
// so internal-package tests can share it without an import.
func abcSchemaI() *schema.Schema {
	return schema.New(
		schema.Field{Name: "a", Type: schema.Int64, ArrowType: arrow.PrimitiveTypes.Int64},
		schema.Field{Name: "b", Type: schema.Int64, ArrowType: arrow.PrimitiveTypes.Int64},
		schema.Field{Name: "c", Type: schema.Int64, ArrowType: arrow.PrimitiveTypes.Int64},
	)
}

// call records one Executor method invocation in the order it was made, so a
// test can assert the executor saw the operators its plan implies.
type call struct {
	op string
}

// fakeExecutor is a no-cycle stand-in for the dataframe-backed Executor. Every
// method returns an opaque sentinel handle (a *string naming the op) and
// appends to calls, so tests can drive Execute/ExecuteStream without importing
// dataframe. Set the matching failAt field to make a method return an error,
// exercising the error-return paths in the physical and streaming nodes.
type fakeExecutor struct {
	calls []call

	failScan       bool
	failFilter     bool
	failProject    bool
	failLimit      bool
	failSort       bool
	failAggregate  bool
	failJoin       bool
	failWithColumn bool

	failScanStream    bool
	failFilterStream  bool
	failProjectStream bool
	failStreamFromDF  bool
}

func errBoom(op string) error { return errors.New(op + ": boom") }

func handle(op string) any { s := op; return &s }

func (e *fakeExecutor) record(op string) { e.calls = append(e.calls, call{op: op}) }

func (e *fakeExecutor) ops() []string {
	out := make([]string, len(e.calls))
	for i, c := range e.calls {
		out[i] = c.op
	}
	return out
}

func (e *fakeExecutor) Scan(ctx context.Context, node *ScanNode) (any, error) {
	e.record("Scan")
	if e.failScan {
		return nil, errBoom("Scan")
	}
	return handle("Scan"), nil
}

func (e *fakeExecutor) Filter(ctx context.Context, df any, predicate expr.Expr) (any, error) {
	e.record("Filter")
	if e.failFilter {
		return nil, errBoom("Filter")
	}
	return handle("Filter"), nil
}

func (e *fakeExecutor) Project(df any, cols []string) (any, error) {
	e.record("Project")
	if e.failProject {
		return nil, errBoom("Project")
	}
	return handle("Project"), nil
}

func (e *fakeExecutor) Limit(df any, n int64) (any, error) {
	e.record("Limit")
	if e.failLimit {
		return nil, errBoom("Limit")
	}
	return handle("Limit"), nil
}

func (e *fakeExecutor) Sort(ctx context.Context, df any, keys []expr.SortKey) (any, error) {
	e.record("Sort")
	if e.failSort {
		return nil, errBoom("Sort")
	}
	return handle("Sort"), nil
}

func (e *fakeExecutor) Aggregate(ctx context.Context, df any, groupKeys []string, aggs []expr.AggNode) (any, error) {
	e.record("Aggregate")
	if e.failAggregate {
		return nil, errBoom("Aggregate")
	}
	return handle("Aggregate"), nil
}

func (e *fakeExecutor) Join(ctx context.Context, left, right any, on string, how string) (any, error) {
	e.record("Join")
	if e.failJoin {
		return nil, errBoom("Join")
	}
	return handle("Join"), nil
}

func (e *fakeExecutor) WithColumn(df any, ex expr.Expr) (any, error) {
	e.record("WithColumn")
	if e.failWithColumn {
		return nil, errBoom("WithColumn")
	}
	return handle("WithColumn"), nil
}

func (e *fakeExecutor) ScanStream(ctx context.Context, node *ScanNode) (any, error) {
	e.record("ScanStream")
	if e.failScanStream {
		return nil, errBoom("ScanStream")
	}
	return handle("ScanStream"), nil
}

func (e *fakeExecutor) FilterStream(ctx context.Context, reader any, predicate expr.Expr) (any, error) {
	e.record("FilterStream")
	if e.failFilterStream {
		return nil, errBoom("FilterStream")
	}
	return handle("FilterStream"), nil
}

func (e *fakeExecutor) ProjectStream(reader any, cols []string) (any, error) {
	e.record("ProjectStream")
	if e.failProjectStream {
		return nil, errBoom("ProjectStream")
	}
	return handle("ProjectStream"), nil
}

func (e *fakeExecutor) LimitStream(reader any, n int64) any {
	e.record("LimitStream")
	return handle("LimitStream")
}

func (e *fakeExecutor) StreamFromDataFrame(df any) (any, error) {
	e.record("StreamFromDataFrame")
	if e.failStreamFromDF {
		return nil, errBoom("StreamFromDataFrame")
	}
	return handle("StreamFromDataFrame"), nil
}

// compile-time check that the fake satisfies the real interface.
var _ Executor = (*fakeExecutor)(nil)
