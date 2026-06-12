package ingest

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ADBCConfig holds connection and query parameters for a database source.
type ADBCConfig struct {
	DSN       string
	Query     string
	ChunkSize int
	Allocator memory.Allocator
}

// ADBC opens a connection on db, executes cfg.Query, and returns a RecordReader
// over the result batches. The returned reader owns the connection and
// statement and releases them (statement first, then connection) on Release.
//
// Schema mapping: ADBC result schemas are already Arrow schemas, so they pass
// through directly with no translation. Null bitmaps on the returned records
// are preserved as-is.
func ADBC(ctx context.Context, db adbc.Database, cfg ADBCConfig) (array.RecordReader, error) {
	if db == nil {
		return nil, fmt.Errorf("adbc: database is nil")
	}
	if cfg.Query == "" {
		return nil, fmt.Errorf("adbc: query is empty")
	}

	conn, err := db.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("adbc: open connection: %w", err)
	}

	stmt, err := conn.NewStatement()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("adbc: new statement: %w", err)
	}

	if err := stmt.SetSqlQuery(cfg.Query); err != nil {
		_ = stmt.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("adbc: set query: %w", err)
	}

	rr, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		_ = stmt.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("adbc: execute query: %w", err)
	}

	return &adbcReader{
		ctx:     ctx,
		refCount: 1,
		reader:   rr,
		stmt:     stmt,
		conn:     conn,
	}, nil
}

// adbcReader wraps an ADBC result RecordReader so that the statement and
// connection are torn down when the reader's last reference is released, and so
// that context cancellation is observed between batches.
type adbcReader struct {
	ctx      context.Context
	refCount int64
	reader   array.RecordReader
	stmt     adbc.Statement
	conn     adbc.Connection
	err      error
}

func (r *adbcReader) Retain() {
	r.refCount++
}

func (r *adbcReader) Release() {
	r.refCount--
	if r.refCount != 0 {
		return
	}
	if r.reader != nil {
		r.reader.Release()
		r.reader = nil
	}
	if r.stmt != nil {
		_ = r.stmt.Close()
		r.stmt = nil
	}
	if r.conn != nil {
		_ = r.conn.Close()
		r.conn = nil
	}
}

func (r *adbcReader) Schema() *arrow.Schema        { return r.reader.Schema() }
func (r *adbcReader) RecordBatch() arrow.RecordBatch { return r.reader.RecordBatch() }
func (r *adbcReader) Record() arrow.Record           { return r.reader.RecordBatch() }

func (r *adbcReader) Next() bool {
	if r.err != nil {
		return false
	}
	if err := r.ctx.Err(); err != nil {
		r.err = fmt.Errorf("adbc: %w", err)
		return false
	}
	return r.reader.Next()
}

func (r *adbcReader) Err() error {
	if r.err != nil {
		return r.err
	}
	if err := r.reader.Err(); err != nil {
		return fmt.Errorf("adbc: %w", err)
	}
	return nil
}

var _ array.RecordReader = (*adbcReader)(nil)
