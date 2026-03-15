package scan

import (
	"fmt"
	"os"

	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/csv"
)

func ScanCSV(path string, opts ...CSVOption) (array.RecordReader, error) {
	if path == "" {
		return nil, fmt.Errorf("csv path is empty")
	}

	cfg := DefaultCSVOptions()
	for _, opt := range opts {
		opt(&cfg)
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open csv: %w", err)
	}

	reader := csv.NewInferringReader(f, csvReaderOptions(cfg)...)
	return newReaderWithClose(reader, f.Close), nil
}

func csvReaderOptions(cfg CSVOptions) []csv.Option {
	options := []csv.Option{csv.WithHeader(cfg.HasHeader)}
	if cfg.ChunkSize != 0 {
		options = append(options, csv.WithChunk(cfg.ChunkSize))
	}
	if len(cfg.NullValues) > 0 {
		options = append(options, csv.WithNullReader(true, cfg.NullValues...))
	}
	if len(cfg.ColumnTypes) > 0 {
		options = append(options, csv.WithColumnTypes(cfg.ColumnTypes))
	}
	if len(cfg.IncludeColumns) > 0 {
		options = append(options, csv.WithIncludeColumns(cfg.IncludeColumns))
	}
	if cfg.Comma != 0 {
		options = append(options, csv.WithComma(cfg.Comma))
	}
	if cfg.Comment != 0 {
		options = append(options, csv.WithComment(cfg.Comment))
	}
	if cfg.LazyQuotes {
		options = append(options, csv.WithLazyQuotes(cfg.LazyQuotes))
	}
	if cfg.StringsReplacer != nil {
		options = append(options, csv.WithStringsReplacer(cfg.StringsReplacer))
	}
	if cfg.Allocator != nil {
		options = append(options, csv.WithAllocator(cfg.Allocator))
	}

	return options
}
