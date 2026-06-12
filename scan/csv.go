package scan

import (
	"context"

	"github.com/karthedew/cosma/internal/ingest"
)

// ScanCSV opens a CSV file and returns a streaming RecordReader over its
// batches. It is a thin adapter over the shared ingestion seam. The returned
// reader honors ctx cancellation between batches and owns the file handle,
// closing it on Release.
func ScanCSV(ctx context.Context, path string, opts ...CSVOption) (RecordReader, error) {
	cfg := DefaultCSVOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	reader, err := ingest.CSV(path, cfg.toIngest())
	if err != nil {
		return nil, err
	}
	return WithContext(ctx, reader), nil
}
