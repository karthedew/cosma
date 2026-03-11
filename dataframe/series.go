package dataframe

import (
	"github.com/apache/arrow/go/v18/arrow"
)

// Series is the core column type in Cosma.
// Polars concept: Series ~= name + Column (chunked arrays).
type Series struct {
	name string
	col  Column
}

func NewSeriesFromColumn(name string, col Column) *Series {
	return &Series{name: name, col: col}
}

func NewSeriesFromChunked(name string, chunked *arrow.Chunked) *Series {
	return &Series{name: name, col: NewChunkedColumn(chunked)}
}

func (s *Series) Name() string { return s.name }
func (s *Series) DataType() arrow.DataType {
	if s == nil || s.col == nil {
		return nil
	}
	return s.col.DataType()
}

func (s *Series) Chunked() *arrow.Chunked {
	if s == nil || s.col == nil {
		return nil
	}
	return s.col.Chunked()
}

// Len is the logical length across chunks.
func (s *Series) Len() int {
	if s == nil || s.col == nil {
		return 0
	}
	return int(s.col.Len())
}
