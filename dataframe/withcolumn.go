package dataframe

import (
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/memory"

	"github.com/karthedew/cosma/expr"
	"github.com/karthedew/cosma/internal/compute"
)

// WithColumn returns a new DataFrame with a column derived from evaluating e
// over each row. The output column name is taken, in order of precedence, from:
//
//   - an .As("name") alias on the expression, or
//   - the single root column the expression references (replaced in place).
//
// If the expression references more than one column and carries no alias, an
// error is returned directing the caller to add .As(). The receiver is
// unchanged; e is evaluated one canonical chunk at a time and the per-segment
// results are reassembled into a single chunked column aligned to the frame's
// height.
func (df *DataFrame) WithColumn(e expr.Expr) (*DataFrame, error) {
	if e.Node == nil {
		return nil, fmt.Errorf("withcolumn: nil expression")
	}

	name, err := outputColumnName(e)
	if err != nil {
		return nil, err
	}

	iter, err := NewRecordBatchIter(df)
	if err != nil {
		return nil, err
	}
	mem := memory.DefaultAllocator

	var dtype arrow.DataType
	segments := make([]arrow.Array, 0, df.NumChunks())
	release := func() {
		for _, a := range segments {
			a.Release()
		}
	}

	for {
		rec, ok, err := iter.Next()
		if err != nil {
			release()
			return nil, err
		}
		if !ok {
			break
		}
		arr, err := compute.Eval(e.Node, rec, mem)
		rec.Release()
		if err != nil {
			release()
			return nil, fmt.Errorf("withcolumn: %w", err)
		}
		if dtype == nil {
			dtype = arr.DataType()
		}
		segments = append(segments, arr)
	}

	if dtype == nil {
		release()
		return nil, fmt.Errorf("withcolumn: cannot derive %q from an empty frame", name)
	}

	chunked := arrow.NewChunked(dtype, segments)
	release() // NewChunked retained the segments; the Series now owns chunked

	newCol := NewSeriesFromChunked(name, chunked)

	idx := df.columnIndex(name)
	series := make([]*Series, 0, len(df.cols)+1)
	for i := range df.cols {
		if i == idx {
			series = append(series, newCol)
			continue
		}
		c := df.cols[i]
		series = append(series, &c)
	}
	if idx < 0 {
		series = append(series, newCol)
	}

	return New(series)
}

// outputColumnName derives the result column name for WithColumn. An alias at
// the root wins outright. Otherwise the expression must reference exactly one
// distinct column, whose name is reused (in-place replacement). Zero or
// multiple distinct columns without an alias is an error.
func outputColumnName(e expr.Expr) (string, error) {
	if alias, ok := e.Node.(expr.AliasNode); ok {
		return alias.Name, nil
	}

	seen := make(map[string]struct{})
	order := make([]string, 0, 1)
	_ = expr.Walk(e.Node, func(node expr.ExprNode) error {
		if c, ok := node.(expr.ColumnNode); ok {
			if _, dup := seen[c.Name]; !dup {
				seen[c.Name] = struct{}{}
				order = append(order, c.Name)
			}
		}
		return nil
	})

	switch len(order) {
	case 1:
		return order[0], nil
	case 0:
		return "", fmt.Errorf("withcolumn: expression references no column; add .As(name)")
	default:
		return "", fmt.Errorf("withcolumn: expression references multiple columns %v; add .As(name)", order)
	}
}
