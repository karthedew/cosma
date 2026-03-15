package operator

import (
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
)

type Project struct {
	schema  *arrow.Schema
	indices []int
}

func NewProject(schema *arrow.Schema, indices []int) (*Project, error) {
	if schema == nil {
		return nil, fmt.Errorf("schema is nil")
	}
	if len(indices) == 0 {
		return nil, fmt.Errorf("projection indices are empty")
	}

	fields := make([]arrow.Field, len(indices))
	for i, idx := range indices {
		if idx < 0 || idx >= schema.NumFields() {
			return nil, fmt.Errorf("projection index %d out of range", idx)
		}
		fields[i] = schema.Field(idx)
	}
	metadata := schema.Metadata()
	return &Project{
		schema:  arrow.NewSchema(fields, &metadata),
		indices: append([]int(nil), indices...),
	}, nil
}

func (p *Project) Schema() *arrow.Schema { return p.schema }

func (p *Project) Process(rec arrow.Record) (arrow.Record, error) {
	if rec == nil {
		return nil, nil
	}
	cols := make([]arrow.Array, len(p.indices))
	for i, idx := range p.indices {
		cols[i] = rec.Column(idx)
	}
	return array.NewRecord(p.schema, cols, rec.NumRows()), nil
}

func (p *Project) Release() {}

var _ Operator = (*Project)(nil)
