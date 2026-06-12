// Package ingest is Cosma's single source-to-record-batch ingestion seam. It
// owns Arrow reader setup, null semantics, chunk sizing, and file-handle
// cleanup for each supported source, returning an array.RecordReader. Eager
// callers (dataframe.ReadCSV) and streaming callers (scan.ScanCSV) are thin
// adapters over this package, so ingestion behavior lives in exactly one place.
package ingest

import (
	"fmt"
	"os"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/csv"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// CSVConfig holds the read-relevant CSV settings shared by every ingestion
// caller. Materialization concerns (e.g. nullability of the resulting schema)
// and write-only options are deliberately not part of this seam.
type CSVConfig struct {
	HasHeader       bool
	ChunkSize       int
	NullValues      []string
	ColumnTypes     map[string]arrow.DataType
	IncludeColumns  []string
	Comma           rune
	Comment         rune
	LazyQuotes      bool
	StringsReplacer *strings.Replacer
	Allocator       memory.Allocator
}

// CSV opens path and returns a RecordReader over its inferred record batches.
// The returned reader owns the file handle and closes it on Release.
func CSV(path string, cfg CSVConfig) (array.RecordReader, error) {
	if path == "" {
		return nil, fmt.Errorf("csv path is empty")
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open csv: %w", err)
	}

	reader := csv.NewInferringReader(f, csvReaderOptions(cfg)...)
	return newReaderWithClose(reader, f.Close), nil
}

func csvReaderOptions(cfg CSVConfig) []csv.Option {
	options := []csv.Option{csv.WithHeader(cfg.HasHeader)}
	if cfg.ChunkSize > 0 {
		options = append(options, csv.WithChunk(cfg.ChunkSize))
	}
	if len(cfg.NullValues) > 0 {
		options = append(options, csv.WithNullReader(true, cfg.NullValues...))
	} else {
		options = append(options, csv.WithNullReader(true))
	}
	if cfg.ColumnTypes != nil {
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
