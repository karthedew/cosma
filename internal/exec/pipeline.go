package exec

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"

	"github.com/karthedew/cosma/operator"
)

type Pipeline struct {
	refCount int64
	src      array.RecordReader
	ops      []operator.Operator
	schema   *arrow.Schema
	cur      arrow.Record
	err      error
}

func NewPipeline(ctx context.Context, src array.RecordReader, ops []operator.Operator) (*Pipeline, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}
	if src == nil {
		return nil, fmt.Errorf("source reader is nil")
	}
	schema := src.Schema()
	for _, op := range ops {
		if op == nil {
			return nil, fmt.Errorf("operator is nil")
		}
		schema = op.Schema()
	}

	return &Pipeline{
		refCount: 1,
		src:      src,
		ops:      ops,
		schema:   schema,
	}, nil
}

func (p *Pipeline) Retain() {
	atomic.AddInt64(&p.refCount, 1)
}

func (p *Pipeline) Release() {
	if atomic.AddInt64(&p.refCount, -1) != 0 {
		return
	}
	if p.cur != nil {
		p.cur.Release()
		p.cur = nil
	}
	if p.src != nil {
		p.src.Release()
		p.src = nil
	}
	for _, op := range p.ops {
		op.Release()
	}
}

func (p *Pipeline) Schema() *arrow.Schema { return p.schema }
func (p *Pipeline) Record() arrow.Record  { return p.cur }
func (p *Pipeline) Err() error            { return p.err }

func (p *Pipeline) Next() bool {
	if p.err != nil || p.src == nil {
		return false
	}
	if p.cur != nil {
		p.cur.Release()
		p.cur = nil
	}

	for p.src.Next() {
		rec := p.src.Record()
		if rec == nil {
			p.err = fmt.Errorf("source record is nil")
			return false
		}
		out, err := p.process(rec)
		rec.Release()
		if err != nil {
			p.err = err
			return false
		}
		if out == nil || out.NumRows() == 0 {
			if out != nil {
				out.Release()
			}
			continue
		}
		p.cur = out
		return true
	}

	if err := p.src.Err(); err != nil {
		p.err = err
	}
	return false
}

func (p *Pipeline) process(rec arrow.Record) (arrow.Record, error) {
	out := rec
	for _, op := range p.ops {
		next, err := op.Process(out)
		if out != rec && out != nil && out != next {
			out.Release()
		}
		if err != nil {
			return nil, err
		}
		if next == nil {
			return nil, nil
		}
		out = next
	}
	if out == rec {
		out.Retain()
	}
	return out, nil
}

var _ array.RecordReader = (*Pipeline)(nil)
