package scan

import (
	"sync/atomic"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
)

type readerWithClose struct {
	refCount int64
	reader   array.RecordReader
	closeFn  func() error
}

func newReaderWithClose(reader array.RecordReader, closeFn func() error) array.RecordReader {
	if reader == nil {
		return nil
	}
	return &readerWithClose{refCount: 1, reader: reader, closeFn: closeFn}
}

func (r *readerWithClose) Retain() {
	atomic.AddInt64(&r.refCount, 1)
}

func (r *readerWithClose) Release() {
	if atomic.AddInt64(&r.refCount, -1) != 0 {
		return
	}
	if r.reader != nil {
		r.reader.Release()
		r.reader = nil
	}
	if r.closeFn != nil {
		_ = r.closeFn()
		r.closeFn = nil
	}
}

func (r *readerWithClose) Schema() *arrow.Schema { return r.reader.Schema() }
func (r *readerWithClose) Record() arrow.Record  { return r.reader.Record() }
func (r *readerWithClose) Err() error            { return r.reader.Err() }
func (r *readerWithClose) Next() bool            { return r.reader.Next() }

var _ array.RecordReader = (*readerWithClose)(nil)
