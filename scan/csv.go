package scan

import (
	"github.com/apache/arrow/go/v18/arrow/array"

	"github.com/karthedew/cosma/internal/ingest"
)

// ScanCSV opens a CSV file and returns a streaming RecordReader over its
// batches. It is a thin adapter over the shared ingestion seam.
func ScanCSV(path string, opts ...CSVOption) (array.RecordReader, error) {
	cfg := DefaultCSVOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return ingest.CSV(path, cfg.toIngest())
}
