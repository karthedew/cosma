package dataframe

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"

	"github.com/karthedew/cosma/expr"
	"github.com/karthedew/cosma/internal/compute"
	"github.com/karthedew/cosma/internal/ingest"
)

// FileFormat identifies the on-disk encoding of a file-backed scan.
type FileFormat int

const (
	// FileFormatCSV is a delimited text source read via the CSV ingestion seam.
	FileFormatCSV FileFormat = iota
	// FileFormatParquet is a Parquet source read via the Parquet ingestion seam.
	FileFormatParquet
)

// FileScan is the opaque ScanNode.Handle for a file-backed (ScanSourceFile)
// scan. It carries everything the executor needs to open a streaming
// RecordReader for the file without first materializing it into a DataFrame.
// scan.LazyScanCSV / scan.LazyScanParquet construct it; the DataFrameExecutor
// opens it. It lives in package dataframe (not scan) because the executor that
// consumes it lives here and dataframe cannot import scan (that would close the
// scan -> dataframe -> plan -> ... cycle).
type FileScan struct {
	Path    string
	Format  FileFormat
	CSV     ingest.CSVConfig
	Parquet ingest.ParquetConfig
	// AllowNullable controls schema nullability when (and only when) the scan is
	// materialized into a DataFrame; streaming readers pass batches through
	// unchanged.
	AllowNullable bool
}

// open returns a streaming Arrow RecordReader over the file. The returned
// reader owns the file handle and closes it on Release.
func (f FileScan) open(ctx context.Context) (array.RecordReader, error) {
	switch f.Format {
	case FileFormatCSV:
		return ingest.CSV(f.Path, f.CSV)
	case FileFormatParquet:
		cfg := f.Parquet
		if cfg.Context == nil {
			cfg.Context = ctx
		}
		return ingest.Parquet(f.Path, cfg)
	default:
		return nil, fmt.Errorf("file scan: unknown format %d", f.Format)
	}
}

// FileSchema opens the file briefly to read its inferred Arrow schema without
// draining the data. It is used at lazy-plan build time so a file-backed
// ScanNode can be bound (the binder needs a schema) without an eager full read.
//
// The CSV inferring reader only knows its schema after the first batch is read,
// so when Schema() is nil up front we advance once to trigger inference, then
// release. This reads at most one chunk — not the whole file.
func FileSchema(f FileScan) (*arrow.Schema, error) {
	reader, err := f.open(context.Background())
	if err != nil {
		return nil, err
	}
	defer reader.Release()
	if s := reader.Schema(); s != nil {
		return s, nil
	}
	reader.Next()
	if err := reader.Err(); err != nil {
		return nil, err
	}
	s := reader.Schema()
	if s == nil {
		return nil, fmt.Errorf("file scan: could not infer schema for %q", f.Path)
	}
	return s, nil
}

// --- DataFrame -> RecordReader adapter --------------------------------------

// dfReader streams the record batches of a materialized DataFrame. It is the
// sink that lets pipeline-breaking operators (Sort, GroupBy, Join) materialize
// once and then hand their result back to the streaming pipeline.
type dfReader struct {
	refCount int64
	iter     *RecordBatchIter
	schema   *arrow.Schema
	cur      arrow.Record
	err      error
}

func newDFReader(df *DataFrame) (*dfReader, error) {
	iter, err := NewRecordBatchIter(df)
	if err != nil {
		return nil, err
	}
	return &dfReader{refCount: 1, iter: iter, schema: iter.Schema()}, nil
}

func (r *dfReader) Retain() { atomic.AddInt64(&r.refCount, 1) }

func (r *dfReader) Release() {
	if atomic.AddInt64(&r.refCount, -1) != 0 {
		return
	}
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
}

func (r *dfReader) Schema() *arrow.Schema { return r.schema }
func (r *dfReader) Record() arrow.Record  { return r.cur }
func (r *dfReader) Err() error            { return r.err }

func (r *dfReader) Next() bool {
	if r.err != nil || r.iter == nil {
		return false
	}
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	rec, ok, err := r.iter.Next()
	if err != nil {
		r.err = err
		return false
	}
	if !ok {
		return false
	}
	r.cur = rec
	return true
}

// --- Filter stream -----------------------------------------------------------

// filterReader applies a predicate to each batch of an upstream reader, emitting
// the surviving rows. It never accumulates more than one input batch and its
// filtered output, so a filter over a file scan stays O(batch_size) in memory.
// Empty result batches are skipped so callers do not see zero-row records.
type filterReader struct {
	ctx       context.Context
	upstream  RecordReader
	predicate expr.Expr
	mem       memory.Allocator
	cur       arrow.Record
	err       error
}

func newFilterReader(ctx context.Context, upstream RecordReader, predicate expr.Expr) *filterReader {
	return &filterReader{
		ctx:       ctx,
		upstream:  upstream,
		predicate: predicate,
		mem:       memory.DefaultAllocator,
	}
}

func (r *filterReader) Retain()               {}
func (r *filterReader) Schema() *arrow.Schema { return r.upstream.Schema() }
func (r *filterReader) Record() arrow.Record  { return r.cur }
func (r *filterReader) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.upstream.Err()
}

func (r *filterReader) Release() {
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	r.upstream.Release()
}

func (r *filterReader) Next() bool {
	if r.err != nil {
		return false
	}
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	for r.upstream.Next() {
		if err := r.ctx.Err(); err != nil {
			r.err = err
			return false
		}
		rec := r.upstream.Record()
		mask, err := compute.Eval(r.predicate.Node, rec, r.mem)
		if err != nil {
			r.err = fmt.Errorf("filter stream: %w", err)
			return false
		}
		out, err := compute.FilterRecord(rec, mask, r.mem)
		mask.Release()
		if err != nil {
			r.err = fmt.Errorf("filter stream: %w", err)
			return false
		}
		if out.NumRows() == 0 {
			out.Release()
			continue
		}
		r.cur = out
		return true
	}
	return false
}

// --- Project stream ----------------------------------------------------------

// projectReader narrows each batch of an upstream reader to a column subset
// (zero-copy column selection). Schema is computed once from the first observed
// batch's schema.
type projectReader struct {
	upstream RecordReader
	cols     []string
	schema   *arrow.Schema
	cur      arrow.Record
	err      error
}

func newProjectReader(upstream RecordReader, cols []string) *projectReader {
	r := &projectReader{upstream: upstream, cols: append([]string(nil), cols...)}
	r.schema = projectSchema(upstream.Schema(), r.cols)
	return r
}

// projectSchema builds the projected schema, or nil if a column is missing (the
// per-batch projection surfaces the error at iteration time).
func projectSchema(in *arrow.Schema, cols []string) *arrow.Schema {
	if in == nil {
		return nil
	}
	fields := make([]arrow.Field, 0, len(cols))
	for _, name := range cols {
		idxs := in.FieldIndices(name)
		if len(idxs) == 0 {
			return nil
		}
		fields = append(fields, in.Field(idxs[0]))
	}
	return arrow.NewSchema(fields, nil)
}

func (r *projectReader) Retain()               {}
func (r *projectReader) Schema() *arrow.Schema { return r.schema }
func (r *projectReader) Record() arrow.Record  { return r.cur }
func (r *projectReader) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.upstream.Err()
}

func (r *projectReader) Release() {
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	r.upstream.Release()
}

func (r *projectReader) Next() bool {
	if r.err != nil {
		return false
	}
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	if !r.upstream.Next() {
		return false
	}
	rec := r.upstream.Record()
	cols := make([]arrow.Array, len(r.cols))
	for i, name := range r.cols {
		idxs := rec.Schema().FieldIndices(name)
		if len(idxs) == 0 {
			r.err = fmt.Errorf("project stream: column %q not found", name)
			return false
		}
		cols[i] = rec.Column(idxs[0])
	}
	out := array.NewRecord(r.schema, cols, rec.NumRows())
	r.cur = out
	return true
}

// --- Limit stream ------------------------------------------------------------

// limitReader stops the stream once n rows have been emitted across batches,
// slicing the final batch to the exact remainder. Once the budget is reached it
// stops pulling from upstream, so a Limit over a file scan reads only as many
// batches as needed.
type limitReader struct {
	upstream  RecordReader
	remaining int64
	cur       arrow.Record
}

func newLimitReader(upstream RecordReader, n int64) *limitReader {
	return &limitReader{upstream: upstream, remaining: n}
}

func (r *limitReader) Retain()               {}
func (r *limitReader) Schema() *arrow.Schema { return r.upstream.Schema() }
func (r *limitReader) Record() arrow.Record  { return r.cur }
func (r *limitReader) Err() error            { return r.upstream.Err() }

func (r *limitReader) Release() {
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	r.upstream.Release()
}

func (r *limitReader) Next() bool {
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	if r.remaining <= 0 {
		return false
	}
	if !r.upstream.Next() {
		return false
	}
	rec := r.upstream.Record()
	if rec.NumRows() <= r.remaining {
		rec.Retain()
		r.cur = rec
		r.remaining -= rec.NumRows()
		return true
	}
	// Final partial batch: take only the rows that fit the budget.
	r.cur = rec.NewSlice(0, r.remaining)
	r.remaining = 0
	return true
}

// --- Context-checking reader -------------------------------------------------

// ctxReader checks a context between batches so a streaming scan with no filter
// above it (e.g. scan -> limit) still honors cancellation. The streaming filter
// reader performs its own per-batch check, but a bare scan stream needs this
// wrapper to make ctx.Done() observable mid-stream.
type ctxReader struct {
	ctx      context.Context
	upstream RecordReader
	err      error
}

func newCtxReader(ctx context.Context, upstream RecordReader) RecordReader {
	if ctx == nil {
		return upstream
	}
	return &ctxReader{ctx: ctx, upstream: upstream}
}

func (r *ctxReader) Retain()               {}
func (r *ctxReader) Schema() *arrow.Schema { return r.upstream.Schema() }
func (r *ctxReader) Record() arrow.Record  { return r.upstream.Record() }
func (r *ctxReader) Release()              { r.upstream.Release() }

func (r *ctxReader) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.upstream.Err()
}

func (r *ctxReader) Next() bool {
	if r.err != nil {
		return false
	}
	if err := r.ctx.Err(); err != nil {
		r.err = err
		return false
	}
	return r.upstream.Next()
}

// errReader is a reader that yields no rows and reports a fixed error. It lets a
// non-error-returning method (LimitStream) defer a type error to iteration time.
type errReader struct {
	err    error
	schema *arrow.Schema
}

func (r *errReader) Retain()               {}
func (r *errReader) Schema() *arrow.Schema { return r.schema }
func (r *errReader) Record() arrow.Record  { return nil }
func (r *errReader) Err() error            { return r.err }
func (r *errReader) Release()              {}
func (r *errReader) Next() bool            { return false }

// RecordReader is the streaming contract for dataframe streaming output. It is
// structurally identical to scan.RecordReader, so a value of either type
// satisfies the other (both are interfaces with the same method set) — this is
// what makes file scans and lazy stream outputs interchangeable at the type
// level (CONTEXT.md) without dataframe importing scan, which would close the
// scan -> dataframe -> plan cycle. CollectStream returns this type; a caller
// that imports scan may treat it as a scan.RecordReader directly.
type RecordReader interface {
	Schema() *arrow.Schema
	Next() bool
	Record() arrow.Record
	Err() error
	Release()
}
