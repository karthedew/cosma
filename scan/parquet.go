package scan

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/parquet/file"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
)

func ScanParquet(path string, opts ...ParquetOption) (array.RecordReader, error) {
	if path == "" {
		return nil, fmt.Errorf("parquet path is empty")
	}

	cfg := DefaultParquetOptions()
	for _, opt := range opts {
		opt(&cfg)
	}

	reader, err := file.OpenParquetFile(path, false, cfg.ReadOptions...)
	if err != nil {
		return nil, fmt.Errorf("open parquet: %w", err)
	}

	props := pqarrow.ArrowReadProperties{}
	if cfg.ArrowReadProps != nil {
		props = *cfg.ArrowReadProps
	}
	fr, err := pqarrow.NewFileReader(reader, props, cfg.Allocator)
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
