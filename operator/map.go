package operator

import (
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
)

type MapFunc func(arrow.Record) (arrow.Record, error)

type Map struct {
	schema *arrow.Schema
	fn     MapFunc
}

func NewMap(schema *arrow.Schema, fn MapFunc) (*Map, error) {
	if schema == nil {
		return nil, fmt.Errorf("schema is nil")
	}
	if fn == nil {
		return nil, fmt.Errorf("map func is nil")
	}
	return &Map{schema: schema, fn: fn}, nil
}

func (m *Map) Schema() *arrow.Schema { return m.schema }

func (m *Map) Process(rec arrow.Record) (arrow.Record, error) {
	if rec == nil {
		return nil, nil
	}
	return m.fn(rec)
}

func (m *Map) Release() {}

var _ Operator = (*Map)(nil)
