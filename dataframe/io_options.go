package dataframe

import (
	"strings"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/csv"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/file"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"

	"github.com/karthedew/cosma/internal/ingest"
)

type CSVOptions struct {
	HasHeader       bool
	ChunkSize       int
	NullValues      []string
	NullValue       string
	ColumnTypes     map[string]arrow.DataType
	IncludeColumns  []string
	Comma           rune
	Comment         rune
	LazyQuotes      bool
	StringsReplacer *strings.Replacer
	UseCRLF         bool
	BoolWriter      func(bool) string
	AllowNullable   bool
}

type CSVOption func(*CSVOptions)

func DefaultCSVOptions() CSVOptions {
	return CSVOptions{
		HasHeader:     true,
		AllowNullable: true,
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

func WithCSVNullValue(value string) CSVOption {
	return func(cfg *CSVOptions) {
		cfg.NullValue = value
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

func WithCSVCRLF(value bool) CSVOption {
	return func(cfg *CSVOptions) {
		cfg.UseCRLF = value
	}
}

func WithCSVBoolWriter(writer func(bool) string) CSVOption {
	return func(cfg *CSVOptions) {
		cfg.BoolWriter = writer
	}
}

func WithCSVAllowNullable(value bool) CSVOption {
	return func(cfg *CSVOptions) {
		cfg.AllowNullable = value
	}
}

type ParquetOptions struct {
	AllowNullable    bool
	Allocator        memory.Allocator
	ReadOptions      []file.ReadOption
	ArrowReadProps   *pqarrow.ArrowReadProperties
	WriterProps      *parquet.WriterProperties
	ArrowWriterProps *pqarrow.ArrowWriterProperties
}

type ParquetOption func(*ParquetOptions)

func DefaultParquetOptions() ParquetOptions {
	return ParquetOptions{
		AllowNullable: true,
		Allocator:     memory.DefaultAllocator,
	}
}

func WithParquetAllowNullable(value bool) ParquetOption {
	return func(cfg *ParquetOptions) {
		cfg.AllowNullable = value
	}
}

func WithParquetAllocator(allocator memory.Allocator) ParquetOption {
	return func(cfg *ParquetOptions) {
		cfg.Allocator = allocator
	}
}

func WithParquetReadOptions(options ...file.ReadOption) ParquetOption {
	return func(cfg *ParquetOptions) {
		cfg.ReadOptions = options
	}
}

func WithParquetArrowReadProps(props pqarrow.ArrowReadProperties) ParquetOption {
	return func(cfg *ParquetOptions) {
		cfg.ArrowReadProps = &props
	}
}

func WithParquetWriterProps(props *parquet.WriterProperties) ParquetOption {
	return func(cfg *ParquetOptions) {
		cfg.WriterProps = props
	}
}

func WithParquetArrowWriterProps(props pqarrow.ArrowWriterProperties) ParquetOption {
	return func(cfg *ParquetOptions) {
		cfg.ArrowWriterProps = &props
	}
}

func applyCSVOptions(options []CSVOption) CSVOptions {
	cfg := DefaultCSVOptions()
	for _, opt := range options {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

func applyParquetOptions(options []ParquetOption) ParquetOptions {
	cfg := DefaultParquetOptions()
	for _, opt := range options {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.Allocator == nil {
		cfg.Allocator = memory.DefaultAllocator
	}
	return cfg
}

// ingestConfig projects the read-relevant CSV options onto the shared ingestion
// seam. Write-only options (UseCRLF, BoolWriter, NullValue) and the
// materialization concern AllowNullable are intentionally excluded.
func (cfg CSVOptions) ingestConfig() ingest.CSVConfig {
	return ingest.CSVConfig{
		HasHeader:       cfg.HasHeader,
		ChunkSize:       cfg.ChunkSize,
		NullValues:      cfg.NullValues,
		ColumnTypes:     cfg.ColumnTypes,
		IncludeColumns:  cfg.IncludeColumns,
		Comma:           cfg.Comma,
		Comment:         cfg.Comment,
		LazyQuotes:      cfg.LazyQuotes,
		StringsReplacer: cfg.StringsReplacer,
	}
}

// ingestConfig projects the read-relevant Parquet options onto the shared
// ingestion seam. Write-only options are intentionally excluded.
func (cfg ParquetOptions) ingestConfig() ingest.ParquetConfig {
	return ingest.ParquetConfig{
		Allocator:      cfg.Allocator,
		ReadOptions:    cfg.ReadOptions,
		ArrowReadProps: cfg.ArrowReadProps,
	}
}

func csvWriterOptions(cfg CSVOptions) []csv.Option {
	options := []csv.Option{csv.WithHeader(cfg.HasHeader)}
	if cfg.ChunkSize > 0 {
		options = append(options, csv.WithChunk(cfg.ChunkSize))
	}
	if cfg.NullValue != "" {
		options = append(options, csv.WithNullWriter(cfg.NullValue))
	}
	if cfg.Comma != 0 {
		options = append(options, csv.WithComma(cfg.Comma))
	}
	if cfg.Comment != 0 {
		options = append(options, csv.WithComment(cfg.Comment))
	}
	if cfg.StringsReplacer != nil {
		options = append(options, csv.WithStringsReplacer(cfg.StringsReplacer))
	}
	if cfg.UseCRLF {
		options = append(options, csv.WithCRLF(cfg.UseCRLF))
	}
	if cfg.BoolWriter != nil {
		options = append(options, csv.WithBoolWriter(cfg.BoolWriter))
	}
	return options
}
