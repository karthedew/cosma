package ingest

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// defaultParquetBatchRows is the record-batch size used when a caller does not
// specify one. The pqarrow record reader emits no rows when BatchSize is zero.
const defaultParquetBatchRows = 1 << 16

// ParquetConfig holds the read-relevant Parquet settings shared by every
// ingestion caller.
type ParquetConfig struct {
	Allocator      memory.Allocator
	ReadOptions    []file.ReadOption
	ArrowReadProps *pqarrow.ArrowReadProperties
	Context        context.Context
}

// Parquet opens path and returns a RecordReader over its record batches. The
// returned reader owns the underlying file and closes it on Release.
func Parquet(path string, cfg ParquetConfig) (array.RecordReader, error) {
	if path == "" {
		return nil, fmt.Errorf("parquet path is empty")
	}

	reader, err := file.OpenParquetFile(path, false, cfg.ReadOptions...)
	if err != nil {
		return nil, fmt.Errorf("open parquet: %w", err)
	}

	props := pqarrow.ArrowReadProperties{}
	if cfg.ArrowReadProps != nil {
		props = *cfg.ArrowReadProps
	}
	// GetRecordReader yields nothing when BatchSize is unset, so the seam owns a
	// sane default rather than forcing every caller to set one.
	if props.BatchSize <= 0 {
		props.BatchSize = defaultParquetBatchRows
	}
	alloc := cfg.Allocator
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}

	fr, err := pqarrow.NewFileReader(reader, props, alloc)
	if err != nil {
		_ = reader.Close()
		return nil, fmt.Errorf("parquet reader: %w", err)
	}

	ctx := cfg.Context
	if ctx == nil {
		ctx = context.Background()
	}
	recReader, err := fr.GetRecordReader(ctx, nil, nil)
	if err != nil {
		_ = reader.Close()
		return nil, fmt.Errorf("parquet record reader: %w", err)
	}

	return newReaderWithClose(recReader, reader.Close), nil
}
