package ingest

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// mockDB is an in-memory adbc.Database backed by prebuilt Arrow records. It
// stands in for a real driver (e.g. DuckDB) so the adapter is testable without
// CGo. Embedding the ADBC interfaces means any method the adapter does not use
// is a nil call that panics loudly if exercised.
type mockDB struct {
	adbc.Database
	records []arrow.Record
	schema  *arrow.Schema

	openCalls int
}

func (m *mockDB) Open(ctx context.Context) (adbc.Connection, error) {
	m.openCalls++
	return &mockConn{db: m}, nil
}

func (m *mockDB) Close() error { return nil }

type mockConn struct {
	adbc.Connection
	db     *mockDB
	closed bool
}

func (c *mockConn) NewStatement() (adbc.Statement, error) {
	return &mockStmt{db: c.db}, nil
}

func (c *mockConn) Close() error {
	c.closed = true
	return nil
}

type mockStmt struct {
	adbc.Statement
	db     *mockDB
	query  string
	closed bool
}

func (s *mockStmt) SetSqlQuery(query string) error {
	s.query = query
	return nil
}

func (s *mockStmt) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	rr, err := array.NewRecordReader(s.db.schema, s.db.records)
	if err != nil {
		return nil, 0, err
	}
	return rr, -1, nil
}

func (s *mockStmt) Close() error {
	s.closed = true
	return nil
}

// newNumericDB builds a mock database with 3 int32 columns (a, b, c) and
// rowCount rows, split across batches of batchRows. Row i holds (i, i*10, i*100).
func newNumericDB(t *testing.T, rowCount, batchRows int) *mockDB {
	t.Helper()
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int32},
		{Name: "b", Type: arrow.PrimitiveTypes.Int32},
		{Name: "c", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	var records []arrow.Record
	for start := 0; start < rowCount; start += batchRows {
		end := start + batchRows
		if end > rowCount {
			end = rowCount
		}
		b := array.NewRecordBuilder(alloc, schema)
		for i := start; i < end; i++ {
			b.Field(0).(*array.Int32Builder).Append(int32(i))
			b.Field(1).(*array.Int32Builder).Append(int32(i * 10))
			b.Field(2).(*array.Int32Builder).Append(int32(i * 100))
		}
		rec := b.NewRecord()
		records = append(records, rec)
		b.Release()
	}
	t.Cleanup(func() {
		for _, r := range records {
			r.Release()
		}
	})
	return &mockDB{records: records, schema: schema}
}

func TestADBC_Schema(t *testing.T) {
	db := newNumericDB(t, 100, 25)
	reader, err := ADBC(context.Background(), db, ADBCConfig{Query: "SELECT * FROM t"})
	if err != nil {
		t.Fatalf("ADBC: %v", err)
	}
	defer reader.Release()

	got := reader.Schema()
	if got.NumFields() != 3 {
		t.Fatalf("NumFields = %d, want 3", got.NumFields())
	}
	wantNames := []string{"a", "b", "c"}
	for i, name := range wantNames {
		if got.Field(i).Name != name {
			t.Fatalf("field %d = %q, want %q", i, got.Field(i).Name, name)
		}
		if got.Field(i).Type.ID() != arrow.INT32 {
			t.Fatalf("field %d type = %s, want int32", i, got.Field(i).Type)
		}
	}
}

func TestADBC_RowCount(t *testing.T) {
	db := newNumericDB(t, 100, 25)
	reader, err := ADBC(context.Background(), db, ADBCConfig{Query: "SELECT * FROM t"})
	if err != nil {
		t.Fatalf("ADBC: %v", err)
	}
	defer reader.Release()

	var total int64
	var sawValue bool
	for reader.Next() {
		rec := reader.Record()
		total += rec.NumRows()
		col := rec.Column(1).(*array.Int32)
		// Spot-check: row global index 0 has b == 0; find row whose a==42.
		aCol := rec.Column(0).(*array.Int32)
		for i := 0; i < aCol.Len(); i++ {
			if aCol.Value(i) == 42 {
				if col.Value(i) != 420 {
					t.Fatalf("row a=42: b = %d, want 420", col.Value(i))
				}
				sawValue = true
			}
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reader err: %v", err)
	}
	if total != 100 {
		t.Fatalf("total rows = %d, want 100", total)
	}
	if !sawValue {
		t.Fatalf("never saw row a=42 for spot check")
	}
}

func TestADBC_CtxCancel(t *testing.T) {
	db := newNumericDB(t, 100, 10)
	ctx, cancel := context.WithCancel(context.Background())
	reader, err := ADBC(ctx, db, ADBCConfig{Query: "SELECT * FROM t"})
	if err != nil {
		t.Fatalf("ADBC: %v", err)
	}
	defer reader.Release()

	// Consume one batch, then cancel mid-stream.
	if !reader.Next() {
		t.Fatalf("expected at least one batch before cancel")
	}
	cancel()

	for reader.Next() {
		// drain until cancellation is observed
	}
	if err := reader.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Err() = %v, want wrapped context.Canceled", err)
	}
}

func TestADBC_EmptyQueryError(t *testing.T) {
	db := newNumericDB(t, 1, 1)
	if _, err := ADBC(context.Background(), db, ADBCConfig{}); err == nil {
		t.Fatalf("expected error for empty query")
	}
}
