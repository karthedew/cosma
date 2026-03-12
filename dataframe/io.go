package dataframe

import (
	"context"
	"fmt"
	"os"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/csv"
	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/file"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
)

func ReadCSV(path string, opts ...CSVOption) (*DataFrame, error) {
	if path == "" {
		return nil, fmt.Errorf("csv path is empty")
	}

	cfg := applyCSVOptions(opts)

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open csv: %w", err)
	}
	defer f.Close()

	reader := csv.NewInferringReader(f, csvReaderOptions(cfg)...)
	defer reader.Release()

	records := make([]arrow.Record, 0, 8)
	for reader.Next() {
		rec := reader.Record()
		if rec == nil {
			return nil, fmt.Errorf("csv record is nil")
		}
		rec.Retain()
		records = append(records, rec)
	}
	if err := reader.Err(); err != nil {
		releaseRecords(records)
		return nil, fmt.Errorf("read csv: %w", err)
	}

	df, err := FromRecordBatchesWithOptions(reader.Schema(), records, RecordBatchOptions{AllowNullable: cfg.AllowNullable})
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
	arrSchema, err := arrowSchemaFromSchema(schemaForWrite)
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
	if path == "" {
		return nil, fmt.Errorf("parquet path is empty")
	}

	cfg := applyParquetOptions(opts)

	reader, err := file.OpenParquetFile(path, false, cfg.ReadOptions...)
	if err != nil {
		return nil, fmt.Errorf("open parquet: %w", err)
	}
	defer reader.Close()

	props := pqarrow.ArrowReadProperties{}
	if cfg.ArrowReadProps != nil {
		props = *cfg.ArrowReadProps
	}
	fr, err := pqarrow.NewFileReader(reader, props, cfg.Allocator)
	if err != nil {
		return nil, fmt.Errorf("parquet reader: %w", err)
	}

	table, err := fr.ReadTable(context.Background())
	if err != nil {
		return nil, fmt.Errorf("read parquet: %w", err)
	}

	return dataFrameFromTable(table, cfg.AllowNullable)
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
	arrSchema, err := arrowSchemaFromSchema(schemaForWrite)
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

func dataFrameFromTable(table arrow.Table, allowNullable bool) (*DataFrame, error) {
	if table == nil {
		return nil, fmt.Errorf("table is nil")
	}
	defer table.Release()

	arrSchema := table.Schema()
	cosmaSchema, err := schemaFromArrow(arrSchema)
	if err != nil {
		return nil, err
	}
	if allowNullable {
		cosmaSchema = schemaWithNullable(cosmaSchema)
	}

	cols := make([]Series, int(table.NumCols()))
	for i := 0; i < int(table.NumCols()); i++ {
		col := table.Column(i)
		if col == nil {
			return nil, fmt.Errorf("table column %d is nil", i)
		}
		chunked := col.Data()
		if chunked == nil {
			return nil, fmt.Errorf("table column %q chunked is nil", col.Name())
		}
		chunked.Retain()
		cols[i] = *NewSeriesFromChunked(col.Name(), chunked)
	}

	return NewDataFrame(cosmaSchema, cols)
}

func releaseRecords(records []arrow.Record) {
	for _, rec := range records {
		if rec != nil {
			rec.Release()
		}
	}
}
