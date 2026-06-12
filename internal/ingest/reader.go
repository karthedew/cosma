package ingest

import (
	"errors"
	"io"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// readerWithClose wraps a RecordReader so that the underlying source (file
// handle, etc.) is closed when the reader's last reference is released.
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

func (r *readerWithClose) Schema() *arrow.Schema        { return r.reader.Schema() }
func (r *readerWithClose) RecordBatch() arrow.RecordBatch { return r.reader.RecordBatch() }
func (r *readerWithClose) Record() arrow.Record           { return r.reader.RecordBatch() }
func (r *readerWithClose) Next() bool                     { return r.reader.Next() }

// Err normalizes clean end-of-stream to nil. Sources differ (the Parquet record
// reader reports io.EOF when exhausted, the CSV reader reports nil), so the seam
// gives every caller the same contract: a nil Err means the stream ended
// cleanly.
func (r *readerWithClose) Err() error {
	if err := r.reader.Err(); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

var _ array.RecordReader = (*readerWithClose)(nil)
