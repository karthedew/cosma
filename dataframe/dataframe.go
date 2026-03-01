package dataframe

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

// ColumnAppender owns mutable, in-flight builder state.
type ColumnAppender interface {
	Append(value any) error
	AppendNull()
	ShouldFlush() bool
	PendingLen() int
	Flush() (arr arrow.Array, ok bool)
	Release()
}

// Column is a logical Arrow column with immutable historical chunks and one mutable appender.
type Column struct {
	name     string
	dtype    arrow.DataType
	chunks   []arrow.Array
	chunked  *arrow.Chunked
	appender ColumnAppender
}

func NewInt64Column(name string, chunkSize int) *Column {
	dtype := arrow.PrimitiveTypes.Int64
	return &Column{
		name:     name,
		dtype:    dtype,
		chunks:   make([]arrow.Array, 0),
		chunked:  arrow.NewChunked(dtype, nil),
		appender: NewInt64Appender(chunkSize),
	}
}

func (c *Column) Name() string {
	return c.name
}

func (c *Column) DataType() arrow.DataType {
	return c.dtype
}

func (c *Column) Chunked() *arrow.Chunked {
	return c.chunked
}

func (c *Column) Append(value any) error {
	if err := c.appender.Append(value); err != nil {
		return err
	}
	if c.appender.ShouldFlush() {
		c.Flush()
	}
	return nil
}

func (c *Column) AppendNull() {
	c.appender.AppendNull()
	if c.appender.ShouldFlush() {
		c.Flush()
	}
}

// Flush seals the current builder into an immutable Arrow array.
// It is safe to call when no pending values exist.
func (c *Column) Flush() {
	arr, ok := c.appender.Flush()
	if !ok {
		return
	}

	c.chunks = append(c.chunks, arr)
	if c.chunked != nil {
		c.chunked.Release()
	}
	c.chunked = arrow.NewChunked(c.dtype, c.chunks)
}

// Release decrements references held by the column and its appender.
func (c *Column) Release() {
	if c.appender != nil {
		c.appender.Release()
	}
	for _, chunk := range c.chunks {
		chunk.Release()
	}
	c.chunks = nil
	if c.chunked != nil {
		c.chunked.Release()
		c.chunked = nil
	}
}

type DataFrame struct {
	schema  *arrow.Schema
	columns []*Column
	index   map[string]int
}

type Column struct {
	Name		string
	DType		arrow.DataType
	Builder		arrow.Builder
	ChunkSize	int
	Chunks		[]arrow.Array
}

func New(schema *arrow.Schema, chunkSize int) (*DataFrame, error) {
	cols := make([]*Column, 0, len(schema.Fields()))
	idx := make(map[string]int, len(schema.Fields()))

	for i, f := range schema.Fields() {
		if _, exists := idx[f.Name]; exists {
			return nil, fmt.Errorf("duplicate field name: %s", f.Name)
		}

		var col *Column
		switch f.Type.ID() {
		case arrow.INT64:
			col = NewInt64Column(f.Name, chunkSize)
		default:
			return nil, fmt.Errorf("unsupported type for field %q: %s", f.Name, f.Type)
		}

		cols = append(cols, col)
		idx[f.Name] = i
	}

	return &DataFrame{schema: schema, columns: cols, index: idx}, nil
}

func (df *DataFrame) Schema() *arrow.Schema {
	return df.schema
}

func (df *DataFrame) Columns() []*Column {
	return df.columns
}

func (df *DataFrame) AppendRow(values ...any) error {
	if len(values) != len(df.columns) {
		return fmt.Errorf("append row: got %d values, expected %d", len(values), len(df.columns))
	}

	for i, v := range values {
		if v == nil {
			df.columns[i].AppendNull()
			continue
		}
		if err := df.columns[i].Append(v); err != nil {
			return fmt.Errorf("column %q: %w", df.columns[i].Name(), err)
		}
	}
	return nil
}

func (df *DataFrame) AppendInt64(colName string, v int64) error {
	i, ok := df.index[colName]
	if !ok {
		return fmt.Errorf("unknown column %q", colName)
	}
	if df.columns[i].DataType().ID() != arrow.INT64 {
		return fmt.Errorf("column %q is %s, not int64", colName, df.columns[i].DataType())
	}
	return df.columns[i].Append(v)
}

func (df *DataFrame) Flush() {
	for _, c := range df.columns {
		c.Flush()
	}
}

func (df *DataFrame) Release() {
	for _, c := range df.columns {
		c.Release()
	}
}
