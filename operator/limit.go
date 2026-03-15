package operator

import (
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
)

type Limit struct {
	schema    *arrow.Schema
	remaining int64
}

func NewLimit(schema *arrow.Schema, limit int64) (*Limit, error) {
	if schema == nil {
		return nil, fmt.Errorf("schema is nil")
	}
	if limit < 0 {
		return nil, fmt.Errorf("limit must be >= 0")
	}
	return &Limit{schema: schema, remaining: limit}, nil
}

func (l *Limit) Schema() *arrow.Schema { return l.schema }

func (l *Limit) Process(rec arrow.Record) (arrow.Record, error) {
	if rec == nil || l.remaining <= 0 {
		return nil, nil
	}
	rows := rec.NumRows()
	if rows <= l.remaining {
		l.remaining -= rows
		return rec.NewSlice(0, rows), nil
	}
	out := rec.NewSlice(0, l.remaining)
	l.remaining = 0
	return out, nil
}

func (l *Limit) Release() {}

var _ Operator = (*Limit)(nil)
