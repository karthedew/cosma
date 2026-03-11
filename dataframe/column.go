package dataframe

import (
	"github.com/apache/arrow/go/v18/arrow"
)

type Column interface {
	Len() int64
	DataType() arrow.DataType
	Chunked() *arrow.Chunked
}

type ChunkedColumn struct {
	data *arrow.Chunked
}

func NewChunkedColumn(chunked *arrow.Chunked) *ChunkedColumn {
	return &ChunkedColumn{data: chunked}
}

func (c *ChunkedColumn) Len() int64 {
	if c == nil || c.data == nil {
		return 0
	}
	return int64(c.data.Len())
}

func (c *ChunkedColumn) DataType() arrow.DataType {
	if c == nil || c.data == nil {
		return nil
	}
	return c.data.DataType()
}

func (c *ChunkedColumn) Chunked() *arrow.Chunked {
	if c == nil {
		return nil
	}
	return c.data
}
