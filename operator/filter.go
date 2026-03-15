package operator

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/compute"
)

type FilterFunc func(arrow.Record) (arrow.Array, error)

type Filter struct {
	schema  *arrow.Schema
	fn      FilterFunc
	options *compute.FilterOptions
	ctx     context.Context
}

func NewFilter(schema *arrow.Schema, fn FilterFunc, options *compute.FilterOptions) (*Filter, error) {
	if schema == nil {
		return nil, fmt.Errorf("schema is nil")
	}
	if fn == nil {
		return nil, fmt.Errorf("filter func is nil")
	}
	if options == nil {
		options = compute.DefaultFilterOptions()
	}
	return &Filter{schema: schema, fn: fn, options: options, ctx: context.Background()}, nil
}

func (f *Filter) Schema() *arrow.Schema { return f.schema }

func (f *Filter) Process(rec arrow.Record) (arrow.Record, error) {
	if rec == nil {
		return nil, nil
	}
	mask, err := f.fn(rec)
	if err != nil {
		return nil, err
	}
	if mask == nil {
		return nil, fmt.Errorf("filter mask is nil")
	}
	defer mask.Release()
	if mask.Len() != int(rec.NumRows()) {
		return nil, fmt.Errorf("filter mask len=%d does not match rows=%d", mask.Len(), rec.NumRows())
	}
	return compute.FilterRecordBatch(f.ctx, rec, mask, f.options)
}

func (f *Filter) Release() {}

var _ Operator = (*Filter)(nil)
