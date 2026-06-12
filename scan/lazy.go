package scan

import (
	"github.com/karthedew/cosma/dataframe"
)

// LazyScanCSV builds a LazyFrame whose source is a streaming CSV file scan.
// Unlike df.Lazy(), which starts from an in-memory DataFrame, the returned plan
// reads the file batch-by-batch when collected: LazyScanCSV(path).Filter(...).
// CollectStream(ctx) never materializes the whole file. Calling Collect (eager)
// instead drains the file into a DataFrame. Options mirror scan.ScanCSV.
func LazyScanCSV(path string, opts ...CSVOption) *dataframe.LazyFrame {
	cfg := DefaultCSVOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return dataframe.NewLazyFileScan(dataframe.FileScan{
		Path:          path,
		Format:        dataframe.FileFormatCSV,
		CSV:           cfg.toIngest(),
		AllowNullable: true,
	})
}

// LazyScanParquet builds a LazyFrame whose source is a streaming Parquet file
// scan. See LazyScanCSV for the streaming-vs-eager semantics. Options mirror
// scan.ScanParquet.
func LazyScanParquet(path string, opts ...ParquetOption) *dataframe.LazyFrame {
	cfg := DefaultParquetOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return dataframe.NewLazyFileScan(dataframe.FileScan{
		Path:          path,
		Format:        dataframe.FileFormatParquet,
		Parquet:       cfg.toIngest(),
		AllowNullable: true,
	})
}
