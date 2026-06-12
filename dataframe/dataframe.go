package dataframe

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/karthedew/cosma/plan"
	"github.com/karthedew/cosma/schema"
)

type DataFrame struct {
	schema *schema.Schema
	cols   []Series
	height int64
}

func New(series []*Series) (*DataFrame, error) {
	fields := make([]schema.Field, len(series))
	cols := make([]Series, len(series))
	nameIndex := make(map[string]struct{}, len(series))
	var h int64 = -1

	for i, s := range series {
		if s == nil {
			return nil, fmt.Errorf("series %d is nil", i)
		}
		if s.Name() == "" {
			return nil, fmt.Errorf("series %d name is empty", i)
		}
		if _, ok := nameIndex[s.Name()]; ok {
			return nil, fmt.Errorf("duplicate series name %q", s.Name())
		}
		nameIndex[s.Name()] = struct{}{}

		field, err := schema.FieldFromArrow(s.Name(), s.DataType())
		if err != nil {
			return nil, fmt.Errorf("series %q dtype: %w", s.Name(), err)
		}

		fields[i] = field
		cols[i] = *s

		colLen := int64(s.Len())
		if h == -1 {
			h = colLen
		} else if colLen != h {
			return nil, fmt.Errorf("series %q len=%d != height=%d", s.Name(), colLen, h)
		}
	}

	s := schema.New(fields...)
	return NewDataFrame(s, cols)
}

func NewDataFrame(s *schema.Schema, cols []Series) (*DataFrame, error) {
	if s == nil {
		return nil, fmt.Errorf("schema is nil")
	}
	if len(cols) != s.Len() {
		return nil, fmt.Errorf("cols (%d) != schema fields (%d)", len(cols), s.Len())
	}

	fields := s.Fields()
	var h int64 = -1
	for i, f := range fields {
		// Height check
		colLen := int64(cols[i].Len())
		if h == -1 {
			h = colLen
		} else if colLen != h {
			return nil, fmt.Errorf("column %q len=%d != height=%d", f.Name, colLen, h)
		}
	}

	if h < 0 {
		h = 0
	}

	return &DataFrame{
		schema: s,
		cols:   cols,
		height: h,
	}, nil
}

func (df *DataFrame) Schema() *schema.Schema { return df.schema }

// NumRows is the logical row count of the DataFrame across all chunks.
func (df *DataFrame) NumRows() int64 { return df.height }

// NumCols is the number of columns in the DataFrame.
func (df *DataFrame) NumCols() int { return len(df.cols) }

// Columns returns the column names in schema order.
func (df *DataFrame) Columns() []string {
	names := make([]string, len(df.cols))
	for i := range df.cols {
		names[i] = df.cols[i].Name()
	}
	return names
}

func (df *DataFrame) Plan() *plan.LogicalPlan {
	if df == nil {
		return nil
	}
	return plan.NewLogicalPlan(plan.NewScanNode(df.schema, plan.ScanSourceDataFrame))
}

// mapColumns builds a new DataFrame by replacing each column's chunked data with
// fn(name, chunked), preserving the schema. fn receives a nil *arrow.Chunked for
// columns without data and may return nil to keep it. The receiver is unchanged.
func (df *DataFrame) mapColumns(fn func(name string, chunked *arrow.Chunked) *arrow.Chunked) *DataFrame {
	cols := make([]Series, len(df.cols))
	for i := range df.cols {
		name := df.cols[i].Name()
		cols[i] = *NewSeriesFromChunked(name, fn(name, df.cols[i].Chunked()))
	}
	out, err := NewDataFrame(df.schema, cols)
	if err != nil {
		// Invariants are inherited from the receiver, so this is unreachable.
		return nil
	}
	return out
}

func (df *DataFrame) Column(name string) (Series, bool) {
	for _, col := range df.cols {
		if col.Name() == name {
			return col, true
		}
	}
	return Series{}, false
}
