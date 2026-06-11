package scan

import (
	"github.com/apache/arrow/go/v18/arrow/array"

	"github.com/karthedew/cosma/internal/ingest"
)

// ScanParquet opens a Parquet file and returns a streaming RecordReader over its
// batches. It is a thin adapter over the shared ingestion seam.
func ScanParquet(path string, opts ...ParquetOption) (array.RecordReader, error) {
	cfg := DefaultParquetOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return ingest.Parquet(path, cfg.toIngest())
}
