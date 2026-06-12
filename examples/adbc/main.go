// Command adbc shows how a database, exposed through ADBC, feeds into Cosma as
// just another scan source. ADBC result batches are already Arrow, so the
// internal/ingest seam returns an array.RecordReader exactly like CSV and
// Parquet do, and the rest of the pipeline (DataFrame, Filter) is unchanged.
//
// The real DuckDB ADBC driver is CGo and was not available in this module
// version, so this example uses an in-memory mock adbc.Database. Swapping in a
// real driver is a one-line change: build the driver's adbc.Database and pass
// it to ingest.ADBC.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/karthedew/cosma/dataframe"
	cosmaexpr "github.com/karthedew/cosma/expr"
	"github.com/karthedew/cosma/internal/ingest"
)

func main() {
	ctx := context.Background()

	// db := duckdb.NewDriver(...).NewDatabase(...) // real driver
	db := newMockDatabase()
	defer db.Close()

	reader, err := ingest.ADBC(ctx, db, ingest.ADBCConfig{
		DSN:   "memory://",
		Query: "SELECT id, value FROM events",
	})
	if err != nil {
		log.Fatalf("adbc: %v", err)
	}

	df, err := collect(reader)
	if err != nil {
		log.Fatalf("collect: %v", err)
	}

	result, err := df.Filter(ctx, cosmaexpr.Col("value").Gt(cosmaexpr.Lit(int32(10))))
	if err != nil {
		log.Fatalf("filter: %v", err)
	}

	fmt.Println(result.String())
}

// collect drains an array.RecordReader into an eager DataFrame.
func collect(reader array.RecordReader) (*dataframe.DataFrame, error) {
	defer reader.Release()

	var records []arrow.Record
	for reader.Next() {
		rec := reader.Record()
		rec.Retain()
		records = append(records, rec)
	}
	if err := reader.Err(); err != nil {
		return nil, err
	}
	df, err := dataframe.FromRecordBatches(reader.Schema(), records)
	for _, r := range records {
		r.Release()
	}
	return df, err
}

// --- in-memory mock ADBC database ---

type mockDatabase struct {
	adbc.Database
	schema  *arrow.Schema
	records []arrow.Record
}

func newMockDatabase() *mockDatabase {
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "value", Type: arrow.PrimitiveTypes.Int32},
	}, nil)
	b := array.NewRecordBuilder(alloc, schema)
	defer b.Release()
	for i := int32(0); i < 20; i++ {
		b.Field(0).(*array.Int32Builder).Append(i)
		b.Field(1).(*array.Int32Builder).Append(i)
	}
	return &mockDatabase{schema: schema, records: []arrow.Record{b.NewRecord()}}
}

func (m *mockDatabase) Open(ctx context.Context) (adbc.Connection, error) {
	return &mockConnection{db: m}, nil
}

func (m *mockDatabase) Close() error {
	for _, r := range m.records {
		r.Release()
	}
	return nil
}

type mockConnection struct {
	adbc.Connection
	db *mockDatabase
}

func (c *mockConnection) NewStatement() (adbc.Statement, error) {
	return &mockStatement{db: c.db}, nil
}

func (c *mockConnection) Close() error { return nil }

type mockStatement struct {
	adbc.Statement
	db *mockDatabase
}

func (s *mockStatement) SetSqlQuery(string) error { return nil }

func (s *mockStatement) ExecuteQuery(context.Context) (array.RecordReader, int64, error) {
	rr, err := array.NewRecordReader(s.db.schema, s.db.records)
	return rr, -1, err
}

func (s *mockStatement) Close() error { return nil }
