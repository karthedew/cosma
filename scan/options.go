package scan

import (
	"context"
	"strings"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet/file"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
)

type CSVOptions struct {
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

type CSVOption func(*CSVOptions)

func DefaultCSVOptions() CSVOptions {
	return CSVOptions{
		HasHeader: true,
	}
}

func WithCSVHeader(value bool) CSVOption {
	return func(cfg *CSVOptions) {
		cfg.HasHeader = value
	}
}

func WithCSVChunkSize(value int) CSVOption {
	return func(cfg *CSVOptions) {
		cfg.ChunkSize = value
	}
}

func WithCSVNullValues(values []string) CSVOption {
	return func(cfg *CSVOptions) {
		cfg.NullValues = values
	}
}

func WithCSVColumnTypes(types map[string]arrow.DataType) CSVOption {
	return func(cfg *CSVOptions) {
		cfg.ColumnTypes = types
	}
}

func WithCSVIncludeColumns(columns []string) CSVOption {
	return func(cfg *CSVOptions) {
		cfg.IncludeColumns = columns
	}
}

func WithCSVComma(comma rune) CSVOption {
	return func(cfg *CSVOptions) {
		cfg.Comma = comma
	}
}

func WithCSVComment(comment rune) CSVOption {
	return func(cfg *CSVOptions) {
		cfg.Comment = comment
	}
}

func WithCSVLazyQuotes(value bool) CSVOption {
	return func(cfg *CSVOptions) {
		cfg.LazyQuotes = value
	}
}

func WithCSVStringsReplacer(replacer *strings.Replacer) CSVOption {
	return func(cfg *CSVOptions) {
		cfg.StringsReplacer = replacer
	}
}

func WithCSVAllocator(alloc memory.Allocator) CSVOption {
	return func(cfg *CSVOptions) {
		cfg.Allocator = alloc
	}
}

type ParquetOptions struct {
	Allocator      memory.Allocator
	ReadOptions    []file.ReadOption
	ArrowReadProps *pqarrow.ArrowReadProperties
	Context        context.Context
}

type ParquetOption func(*ParquetOptions)

func DefaultParquetOptions() ParquetOptions {
	return ParquetOptions{}
}

func WithParquetAllocator(alloc memory.Allocator) ParquetOption {
	return func(cfg *ParquetOptions) {
		cfg.Allocator = alloc
	}
}

func WithParquetReadOptions(opts ...file.ReadOption) ParquetOption {
	return func(cfg *ParquetOptions) {
		cfg.ReadOptions = opts
	}
}

func WithParquetBatchSize(size int64) ParquetOption {
	return func(cfg *ParquetOptions) {
		if cfg.ArrowReadProps == nil {
			cfg.ArrowReadProps = &pqarrow.ArrowReadProperties{}
		}
		cfg.ArrowReadProps.BatchSize = size
	}
}

func WithParquetParallel(value bool) ParquetOption {
	return func(cfg *ParquetOptions) {
		if cfg.ArrowReadProps == nil {
			cfg.ArrowReadProps = &pqarrow.ArrowReadProperties{}
		}
		cfg.ArrowReadProps.Parallel = value
	}
}

func WithParquetContext(ctx context.Context) ParquetOption {
	return func(cfg *ParquetOptions) {
		cfg.Context = ctx
	}
}
