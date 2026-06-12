package scan

import (
	"context"

	"github.com/karthedew/cosma/internal/ingest"
)

// ScanParquet opens a Parquet file and returns a streaming RecordReader over its
// batches. It is a thin adapter over the shared ingestion seam. The returned
// reader honors ctx cancellation between batches and owns the underlying file,
// closing it on Release.
func ScanParquet(ctx context.Context, path string, opts ...ParquetOption) (RecordReader, error) {
	cfg := DefaultParquetOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	// The parquet record reader is itself driven by a context for row-group
	// fetches; thread ctx through so cancellation reaches the Arrow reader, and
	// also wrap for the between-batch check.
	if cfg.Context == nil {
		cfg.Context = ctx
	}
	reader, err := ingest.Parquet(path, cfg.toIngest())
	if err != nil {
		return nil, err
	}
	return WithContext(ctx, reader), nil
}
