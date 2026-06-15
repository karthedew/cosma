package ingest

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

type fakeRecordReader struct {
	schema   *arrow.Schema
	records  []arrow.Record
	idx      int
	err      error
	released bool
}

func newFakeRecordReader(t *testing.T) *fakeRecordReader {
	t.Helper()
	s := arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewInt64Builder(memory.DefaultAllocator)
	b.AppendValues([]int64{1, 2}, nil)
	a := b.NewArray()
	b.Release()
	rec := array.NewRecord(s, []arrow.Array{a}, 2)
	a.Release()
	t.Cleanup(rec.Release)
	return &fakeRecordReader{schema: s, records: []arrow.Record{rec}, idx: -1}
}

func (f *fakeRecordReader) Retain()                        {}
func (f *fakeRecordReader) Release()                       { f.released = true }
func (f *fakeRecordReader) Schema() *arrow.Schema          { return f.schema }
func (f *fakeRecordReader) RecordBatch() arrow.RecordBatch { return f.Record() }
func (f *fakeRecordReader) Record() arrow.Record {
	if f.idx < 0 || f.idx >= len(f.records) {
		return nil
	}
	return f.records[f.idx]
}
func (f *fakeRecordReader) Next() bool {
	f.idx++
	return f.idx < len(f.records)
}
func (f *fakeRecordReader) Err() error { return f.err }

func TestReaderWithCloseLifecycleAndErr(t *testing.T) {
	fake := newFakeRecordReader(t)
	closed := 0
	rr := newReaderWithClose(fake, func() error { closed++; return nil })
	require.NotNil(t, rr)
	require.Same(t, fake.schema, rr.Schema())
	require.True(t, rr.Next())
	require.NotNil(t, rr.RecordBatch())
	require.NotNil(t, rr.Record())

	rr.Retain()
	rr.Release()
	require.False(t, fake.released)
	require.Zero(t, closed)
	rr.Release()
	require.True(t, fake.released)
	require.Equal(t, 1, closed)

	require.Nil(t, newReaderWithClose(nil, nil))
	fake = newFakeRecordReader(t)
	rr = newReaderWithClose(fake, nil)
	rr.Release()
	require.True(t, fake.released)

	fake = newFakeRecordReader(t)
	fake.err = io.EOF
	rr = newReaderWithClose(fake, nil)
	require.NoError(t, rr.Err())
	rr.Release()

	fake = newFakeRecordReader(t)
	fake.err = errors.New("boom")
	rr = newReaderWithClose(fake, nil)
	require.ErrorContains(t, rr.Err(), "boom")
	rr.Release()
}

func TestCSVReaderOptionsAllBranches(t *testing.T) {
	opts := csvReaderOptions(CSVConfig{
		HasHeader:       true,
		ChunkSize:       10,
		NullValues:      []string{"NA"},
		ColumnTypes:     map[string]arrow.DataType{"a": arrow.PrimitiveTypes.Int64},
		IncludeColumns:  []string{"a"},
		Comma:           ';',
		Comment:         '#',
		LazyQuotes:      true,
		StringsReplacer: strings.NewReplacer("x", "y"),
		Allocator:       memory.DefaultAllocator,
	})
	require.Len(t, opts, 10)
	require.Len(t, csvReaderOptions(CSVConfig{}), 2)

	_, err := CSV("/definitely/missing.csv", CSVConfig{})
	require.ErrorContains(t, err, "open csv")
}

type errorDB struct {
	adbc.Database
	openErr error
	conn    *errorConn
}

func (d *errorDB) Open(context.Context) (adbc.Connection, error) {
	if d.openErr != nil {
		return nil, d.openErr
	}
	if d.conn == nil {
		d.conn = &errorConn{}
	}
	return d.conn, nil
}

type errorConn struct {
	adbc.Connection
	stmtErr error
	stmt    *errorStmt
	closed  bool
}

func (c *errorConn) NewStatement() (adbc.Statement, error) {
	if c.stmtErr != nil {
		return nil, c.stmtErr
	}
	if c.stmt == nil {
		c.stmt = &errorStmt{}
	}
	return c.stmt, nil
}
func (c *errorConn) Close() error { c.closed = true; return nil }

type errorStmt struct {
	adbc.Statement
	setErr  error
	execErr error
	closed  bool
}

func (s *errorStmt) SetSqlQuery(string) error { return s.setErr }
func (s *errorStmt) ExecuteQuery(context.Context) (array.RecordReader, int64, error) {
	if s.execErr != nil {
		return nil, 0, s.execErr
	}
	return nil, 0, errors.New("not configured")
}
func (s *errorStmt) Close() error { s.closed = true; return nil }

func TestADBCErrorBranchesAndLifecycle(t *testing.T) {
	_, err := ADBC(context.Background(), nil, ADBCConfig{Query: "select 1"})
	require.ErrorContains(t, err, "database is nil")

	_, err = ADBC(context.Background(), &errorDB{openErr: errors.New("open")}, ADBCConfig{Query: "select 1"})
	require.ErrorContains(t, err, "open connection")

	conn := &errorConn{stmtErr: errors.New("stmt")}
	_, err = ADBC(context.Background(), &errorDB{conn: conn}, ADBCConfig{Query: "select 1"})
	require.ErrorContains(t, err, "new statement")
	require.True(t, conn.closed)

	stmt := &errorStmt{setErr: errors.New("set")}
	conn = &errorConn{stmt: stmt}
	_, err = ADBC(context.Background(), &errorDB{conn: conn}, ADBCConfig{Query: "select 1"})
	require.ErrorContains(t, err, "set query")
	require.True(t, stmt.closed)
	require.True(t, conn.closed)

	stmt = &errorStmt{execErr: errors.New("exec")}
	conn = &errorConn{stmt: stmt}
	_, err = ADBC(context.Background(), &errorDB{conn: conn}, ADBCConfig{Query: "select 1"})
	require.ErrorContains(t, err, "execute query")
	require.True(t, stmt.closed)
	require.True(t, conn.closed)

	db := newNumericDB(t, 2, 2)
	rr, err := ADBC(context.Background(), db, ADBCConfig{Query: "select * from t"})
	require.NoError(t, err)
	ar := rr.(*adbcReader)
	ar.Retain()
	require.True(t, ar.Next())
	require.NotNil(t, ar.RecordBatch())
	require.NotNil(t, ar.Record())
	ar.Release()
	require.NotNil(t, ar.reader)
	ar.Release()
	require.Nil(t, ar.reader)
}
