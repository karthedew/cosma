package scan

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// RecordReader is Cosma's public streaming contract: a forward-only iterator
// over Arrow record batches. It is the unit of work for execution that does not
// fit in memory (ADR 0002). Both file scans (scan.ScanCSV / scan.ScanParquet)
// and lazy plan outputs (LazyFrame.CollectStream) return a RecordReader, so a
// file on disk and the result of a streamed plan are interchangeable at the
// type level.
//
// Ownership follows Arrow conventions. Record() is valid only between a
// Next()==true and the following Next() call; the reader owns the batch and may
// release it on the next advance. A caller that needs to keep a batch past the
// next Next() must Retain it. Release frees the reader and any source it owns
// (e.g. a file handle).
type RecordReader interface {
	// Schema returns the schema of every batch produced by this reader.
	Schema() *arrow.Schema
	// Next advances to the next batch, returning false at end-of-stream or on
	// error (inspect Err after a false return).
	Next() bool
	// Record returns the current batch. Valid only between Next()==true and the
	// next Next() call.
	Record() arrow.Record
	// Err returns the first non-EOF error encountered, or nil if the stream
	// ended cleanly.
	Err() error
	// Release frees the reader and any source it owns.
	Release()
}

// array.RecordReader (Arrow's reader, which the ingestion seam returns) is a
// superset of RecordReader — it adds Retain — so every ingest reader already
// satisfies this interface structurally.
var _ RecordReader = array.RecordReader(nil)

// ctxReader wraps a RecordReader and checks a context between batches. A
// cancelled context stops the stream: Next returns false and Err reports the
// context error. It is the streaming counterpart to the ctx.Err() check the
// eager operators already perform between record batches.
type ctxReader struct {
	ctx   context.Context
	inner RecordReader
	err   error
}

// WithContext returns a RecordReader that honors ctx cancellation between
// batches. If inner is already context-bound or ctx is nil, inner is returned
// unchanged.
func WithContext(ctx context.Context, inner RecordReader) RecordReader {
	if ctx == nil || inner == nil {
		return inner
	}
	return &ctxReader{ctx: ctx, inner: inner}
}

func (r *ctxReader) Schema() *arrow.Schema { return r.inner.Schema() }
func (r *ctxReader) Record() arrow.Record  { return r.inner.Record() }

func (r *ctxReader) Next() bool {
	if r.err != nil {
		return false
	}
	if err := r.ctx.Err(); err != nil {
		r.err = err
		return false
	}
	return r.inner.Next()
}

func (r *ctxReader) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.inner.Err()
}

func (r *ctxReader) Release() { r.inner.Release() }
