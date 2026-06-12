package dataframe

import (
	"fmt"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/csv"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/karthedew/cosma/internal/ingest"
	"github.com/karthedew/cosma/schema"
)

func ReadCSV(path string, opts ...CSVOption) (*DataFrame, error) {
	cfg := applyCSVOptions(opts)
	reader, err := ingest.CSV(path, cfg.ingestConfig())
	if err != nil {
		return nil, err
	}
	return collectReader(reader, cfg.AllowNullable)
}

// collectReader drains a RecordReader into an eager DataFrame, retaining each
// batch and releasing the reader (and its source) when done. It is the single
// eager sink shared by every ReadX entry point.
func collectReader(reader array.RecordReader, allowNullable bool) (*DataFrame, error) {
	defer reader.Release()

	records := make([]arrow.Record, 0, 8)
	for reader.Next() {
		rec := reader.Record()
		if rec == nil {
			releaseRecords(records)
			return nil, fmt.Errorf("record batch is nil")
		}
		rec.Retain()
		records = append(records, rec)
	}
	if err := reader.Err(); err != nil {
		releaseRecords(records)
		return nil, err
	}

	df, err := FromRecordBatchesWithOptions(reader.Schema(), records, RecordBatchOptions{AllowNullable: allowNullable})
	releaseRecords(records)
	if err != nil {
		return nil, err
	}
	return df, nil
}

func WriteCSV(df *DataFrame, path string, opts ...CSVOption) error {
	if df == nil {
		return fmt.Errorf("dataframe is nil")
	}
	if path == "" {
		return fmt.Errorf("csv path is empty")
	}

	cfg := applyCSVOptions(opts)

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create csv: %w", err)
	}
	defer f.Close()

	schemaForWrite := df.schema
	if cfg.AllowNullable {
		schemaForWrite = schemaWithNullable(schemaForWrite)
	}
	arrSchema, err := schema.ToArrow(schemaForWrite)
	if err != nil {
		return fmt.Errorf("arrow schema: %w", err)
	}

	writer := csv.NewWriter(f, arrSchema, csvWriterOptions(cfg)...)
	iter, err := NewRecordBatchIterWithSchema(df, arrSchema)
	if err != nil {
		return err
	}

	for {
		rec, ok, err := iter.Next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		if err := writer.Write(rec); err != nil {
			rec.Release()
			return fmt.Errorf("write csv: %w", err)
		}
		rec.Release()
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush csv: %w", err)
	}
	if err := writer.Error(); err != nil {
		return fmt.Errorf("csv writer: %w", err)
	}
	return nil
}

func ReadParquet(path string, opts ...ParquetOption) (*DataFrame, error) {
	cfg := applyParquetOptions(opts)
	reader, err := ingest.Parquet(path, cfg.ingestConfig())
	if err != nil {
		return nil, err
	}
	return collectReader(reader, cfg.AllowNullable)
}

func WriteParquet(df *DataFrame, path string, opts ...ParquetOption) error {
	if df == nil {
		return fmt.Errorf("dataframe is nil")
	}
	if path == "" {
		return fmt.Errorf("parquet path is empty")
	}

	cfg := applyParquetOptions(opts)

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create parquet: %w", err)
	}
	defer f.Close()

	schemaForWrite := df.schema
	if cfg.AllowNullable {
		schemaForWrite = schemaWithNullable(schemaForWrite)
	}
	arrSchema, err := schema.ToArrow(schemaForWrite)
	if err != nil {
		return fmt.Errorf("arrow schema: %w", err)
	}

	writerProps := cfg.WriterProps
	if writerProps == nil {
		writerProps = parquet.NewWriterProperties()
	}
	arrowProps := pqarrow.DefaultWriterProps()
	if cfg.ArrowWriterProps != nil {
		arrowProps = *cfg.ArrowWriterProps
	}
	writer, err := pqarrow.NewFileWriter(arrSchema, f, writerProps, arrowProps)
	if err != nil {
		return fmt.Errorf("parquet writer: %w", err)
	}

	iter, err := NewRecordBatchIterWithSchema(df, arrSchema)
	if err != nil {
		return err
	}

	for {
		rec, ok, err := iter.Next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		if err := writer.Write(rec); err != nil {
			rec.Release()
			return fmt.Errorf("write parquet: %w", err)
		}
		rec.Release()
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("close parquet writer: %w", err)
	}
	return nil
}

func releaseRecords(records []arrow.Record) {
	for _, rec := range records {
		if rec != nil {
			rec.Release()
		}
	}
}
