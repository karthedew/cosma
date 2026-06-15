package dataframe

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/expr"
	"github.com/karthedew/cosma/internal/ingest"
	"github.com/karthedew/cosma/plan"
	"github.com/karthedew/cosma/schema"
)

func TestNewSeriesPrimitiveAndSpecialConstructors(t *testing.T) {
	now := time.Unix(1, 0).UTC()
	for _, tc := range []struct {
		name string
		vals any
		id   arrow.Type
	}{
		{"bool", []bool{true}, arrow.BOOL},
		{"int8", []int8{1}, arrow.INT8},
		{"int16", []int16{1}, arrow.INT16},
		{"int32", []int32{1}, arrow.INT32},
		{"int64", []int64{1}, arrow.INT64},
		{"uint8", []uint8{1}, arrow.UINT8},
		{"uint16", []uint16{1}, arrow.UINT16},
		{"uint32", []uint32{1}, arrow.UINT32},
		{"uint64", []uint64{1}, arrow.UINT64},
		{"float16", []float16.Num{float16.New(1)}, arrow.FLOAT16},
		{"float32", []float32{1}, arrow.FLOAT32},
		{"float64", []float64{1}, arrow.FLOAT64},
		{"string", []string{"x"}, arrow.STRING},
		{"binary", [][]byte{[]byte("x")}, arrow.BINARY},
		{"date32", []arrow.Date32{1}, arrow.DATE32},
		{"date64", []arrow.Date64{24 * 60 * 60 * 1000}, arrow.DATE64},
		{"time", []time.Time{now}, arrow.TIMESTAMP},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := NewSeries(tc.name, tc.vals)
			require.NoError(t, err)
			require.Equal(t, 1, s.Len())
			require.Equal(t, tc.id, s.DataType().ID())
		})
	}

	_, err := NewSeries("", []int64{1})
	require.ErrorContains(t, err, "name is empty")
	_, err = NewSeries("bad", struct{}{})
	require.ErrorContains(t, err, "unsupported")
	_, err = NewSeriesFromArray("bad", nil)
	require.ErrorContains(t, err, "array is nil")
	_, err = NewSeriesNull("n", -1)
	require.ErrorContains(t, err, ">= 0")

	constructors := []struct {
		name string
		fn   func() (*Series, error)
		id   arrow.Type
	}{
		{"large_utf8", func() (*Series, error) { return NewSeriesLargeUtf8("x", []string{"a"}) }, arrow.LARGE_STRING},
		{"binary", func() (*Series, error) { return NewSeriesBinary("x", [][]byte{[]byte("a")}) }, arrow.BINARY},
		{"large_binary", func() (*Series, error) { return NewSeriesLargeBinary("x", [][]byte{[]byte("a")}) }, arrow.LARGE_BINARY},
		{"fixed_binary", func() (*Series, error) { return NewSeriesFixedSizeBinary("x", [][]byte{[]byte("ab")}, 2) }, arrow.FIXED_SIZE_BINARY},
		{"time32", func() (*Series, error) { return NewSeriesTime32("x", []arrow.Time32{1}, arrow.Second) }, arrow.TIME32},
		{"time64", func() (*Series, error) { return NewSeriesTime64("x", []arrow.Time64{1}, arrow.Nanosecond) }, arrow.TIME64},
		{"timestamp_values", func() (*Series, error) {
			return NewSeriesTimestampValues("x", []arrow.Timestamp{1}, arrow.Nanosecond, "UTC")
		}, arrow.TIMESTAMP},
		{"duration", func() (*Series, error) { return NewSeriesDuration("x", []arrow.Duration{1}, arrow.Nanosecond) }, arrow.DURATION},
		{"month_interval", func() (*Series, error) { return NewSeriesMonthInterval("x", []arrow.MonthInterval{1}) }, arrow.INTERVAL_MONTHS},
		{"day_time_interval", func() (*Series, error) { return NewSeriesDayTimeInterval("x", []arrow.DayTimeInterval{{Days: 1}}) }, arrow.INTERVAL_DAY_TIME},
		{"month_day_nano_interval", func() (*Series, error) {
			return NewSeriesMonthDayNanoInterval("x", []arrow.MonthDayNanoInterval{{Months: 1}})
		}, arrow.INTERVAL_MONTH_DAY_NANO},
		{"decimal128", func() (*Series, error) {
			return NewSeriesDecimal128("x", []decimal128.Num{decimal128.FromI64(1)}, 10, 2)
		}, arrow.DECIMAL128},
		{"decimal256", func() (*Series, error) {
			return NewSeriesDecimal256("x", []decimal256.Num{decimal256.FromI64(1)}, 10, 2)
		}, arrow.DECIMAL256},
	}
	for _, tc := range constructors {
		t.Run(tc.name, func(t *testing.T) {
			s, err := tc.fn()
			require.NoError(t, err)
			require.Equal(t, tc.id, s.DataType().ID())
		})
	}
	_, err = NewSeriesFixedSizeBinary("x", [][]byte{[]byte("a")}, 0)
	require.ErrorContains(t, err, "byte width")

	var nilSeries *Series
	require.Zero(t, nilSeries.Len())
	require.Nil(t, nilSeries.DataType())
	require.Nil(t, nilSeries.Chunked())
	colSeries := NewSeriesFromColumn("c", NewChunkedColumn(chunkedFromInt64([]int64{1})))
	require.Equal(t, "c", colSeries.Name())
}

func TestDataFrameOptionsCoverage(t *testing.T) {
	replacer := strings.NewReplacer("x", "y")
	csvCfg := applyCSVOptions([]CSVOption{
		nil,
		WithCSVHeader(false),
		WithCSVChunkSize(10),
		WithCSVNullValues([]string{"NA"}),
		WithCSVNullValue("NULL"),
		WithCSVColumnTypes(map[string]arrow.DataType{"a": arrow.PrimitiveTypes.Int64}),
		WithCSVIncludeColumns([]string{"a"}),
		WithCSVComma(';'),
		WithCSVComment('#'),
		WithCSVLazyQuotes(true),
		WithCSVStringsReplacer(replacer),
		WithCSVCRLF(true),
		WithCSVBoolWriter(func(v bool) string { return "b" }),
		WithCSVAllowNullable(false),
	})
	require.False(t, csvCfg.HasHeader)
	require.Equal(t, 10, csvCfg.ChunkSize)
	require.Equal(t, "NULL", csvCfg.NullValue)
	require.False(t, csvCfg.AllowNullable)
	require.Len(t, csvWriterOptions(csvCfg), 8)
	ingestCfg := csvCfg.ingestConfig()
	require.Equal(t, []string{"NA"}, ingestCfg.NullValues)
	require.Equal(t, []string{"a"}, ingestCfg.IncludeColumns)

	readProps := pqarrow.ArrowReadProperties{}
	writeProps := parquet.NewWriterProperties()
	arrowWriteProps := pqarrow.DefaultWriterProps()
	pqCfg := applyParquetOptions([]ParquetOption{
		nil,
		WithParquetAllowNullable(false),
		WithParquetAllocator(nil),
		WithParquetReadOptions(nil),
		WithParquetArrowReadProps(readProps),
		WithParquetWriterProps(writeProps),
		WithParquetArrowWriterProps(arrowWriteProps),
	})
	require.False(t, pqCfg.AllowNullable)
	require.NotNil(t, pqCfg.Allocator)
	require.NotNil(t, pqCfg.ArrowReadProps)
	require.Same(t, writeProps, pqCfg.WriterProps)
	require.NotNil(t, pqCfg.ArrowWriterProps)
	require.NotNil(t, pqCfg.ingestConfig().Allocator)
}

func TestArrowSchemaAndAggregateEdgeCoverage(t *testing.T) {
	_, err := FromRecordBatchesWithOptions(nil, nil, RecordBatchOptions{})
	require.ErrorContains(t, err, "schema is nil")

	goodSchema := arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int64}}, nil)
	df, err := FromRecordBatchesWithOptions(goodSchema, nil, RecordBatchOptions{AllowNullable: true})
	require.NoError(t, err)
	require.Equal(t, int64(0), df.NumRows())
	require.True(t, df.Schema().Fields()[0].Nullable)

	b := array.NewInt64Builder(memory.DefaultAllocator)
	b.AppendValues([]int64{1}, nil)
	a := b.NewArray()
	b.Release()
	rec := array.NewRecord(goodSchema, []arrow.Array{a}, 1)
	a.Release()
	defer rec.Release()
	mismatch := arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int64}, {Name: "b", Type: arrow.PrimitiveTypes.Int64}}, nil)
	_, err = FromRecordBatchesWithOptions(mismatch, []arrow.Record{rec}, RecordBatchOptions{})
	require.ErrorContains(t, err, "record columns")

	df = mustSmallDF(t)
	_, err = df.Count("missing")
	require.ErrorContains(t, err, "not found")
	_, err = df.Mean("missing")
	require.ErrorContains(t, err, "not found")
	_, err = df.Min("missing")
	require.ErrorContains(t, err, "not found")
	_, err = df.Max("missing")
	require.ErrorContains(t, err, "not found")
	nullB := array.NewInt64Builder(memory.DefaultAllocator)
	nullB.AppendNulls(2)
	nullArr := nullB.NewArray()
	nullB.Release()
	empty, err := NewSeriesFromArray("n", nullArr)
	nullArr.Release()
	require.NoError(t, err)
	emptyDF, err := New([]*Series{empty})
	require.NoError(t, err)
	_, err = emptyDF.Mean("n")
	require.ErrorContains(t, err, "no non-null")
}

func TestFormattingHelperCoverage(t *testing.T) {
	require.Equal(t, "i64", dtypeLabel(schema.Field{Type: schema.Int64}))
	require.Equal(t, "f32", dtypeLabel(schema.Field{Type: schema.Float32}))
	require.Equal(t, "f64", dtypeLabel(schema.Field{Type: schema.Float64}))
	require.Equal(t, "bool", dtypeLabel(schema.Field{Type: schema.Bool}))
	require.Equal(t, "custom", dtypeLabel(schema.Field{Type: schema.DType("custom")}))
	require.Equal(t, "int8", dtypeLabel(schema.Field{ArrowType: arrow.PrimitiveTypes.Int8}))
	require.Equal(t, "unknown", dtypeLabel(schema.Field{}))
	require.Equal(t, schema.Field{}, seriesField(nil))
	require.Equal(t, "", valueAt(Series{}, -1))
	require.Equal(t, "", valueAt(Series{}, 0))
	require.Equal(t, "", valueFromArray(nil, 0))

	mem := memory.DefaultAllocator
	ib := array.NewInt64Builder(mem)
	ib.Append(42)
	intArr := ib.NewArray()
	ib.Release()
	require.Equal(t, "42", valueFromArray(intArr, 0))
	intArr.Release()

	fb := array.NewFloat32Builder(mem)
	fb.Append(1.5)
	f32 := fb.NewArray()
	fb.Release()
	require.Equal(t, "1.5", valueFromArray(f32, 0))
	f32.Release()

	bb := array.NewBooleanBuilder(mem)
	bb.Append(false)
	boolArr := bb.NewArray()
	bb.Release()
	require.Equal(t, "false", valueFromArray(boolArr, 0))
	boolArr.Release()

	tsType := &arrow.TimestampType{Unit: arrow.Second, TimeZone: "UTC"}
	tsb := array.NewTimestampBuilder(mem, tsType)
	tsb.Append(arrow.Timestamp(1))
	ts := tsb.NewArray()
	tsb.Release()
	require.Equal(t, "1970-01-01T00:00:01Z", valueFromArray(ts, 0))
	ts.Release()
	require.Equal(t, "<unsupported>", formatTimestamp(1, nil))
	require.Equal(t, time.Unix(0, int64(time.Millisecond)).Format(time.RFC3339), formatTimestamp(1, &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "Mars/Olympus"}))
	require.Equal(t, time.Unix(0, int64(time.Microsecond)).Format(time.RFC3339), formatTimestamp(1, &arrow.TimestampType{Unit: arrow.Microsecond}))
	require.Equal(t, time.Unix(0, 1).Format(time.RFC3339), formatTimestamp(1, &arrow.TimestampType{Unit: arrow.Nanosecond}))
	require.Equal(t, "1970-01-01", formatDate32(0))
	require.Equal(t, "1970-01-02", formatDate64(24*60*60*1000))

	db := array.NewFloat64Builder(memory.DefaultAllocator)
	db.Append(2.25)
	f64 := db.NewArray()
	db.Release()
	require.Equal(t, "2.25", valueFromArray(f64, 0))
	f64.Release()
	nb := array.NewStringBuilder(memory.DefaultAllocator)
	nb.AppendNull()
	nullString := nb.NewArray()
	nb.Release()
	require.Equal(t, "null", valueFromArray(nullString, 0))
	nullString.Release()
	ub := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	ub.Append([]byte("x"))
	unsupported := ub.NewArray()
	ub.Release()
	require.Equal(t, "<unsupported>", valueFromArray(unsupported, 0))
	unsupported.Release()
}

func TestStreamReaderBranchCoverage(t *testing.T) {
	df := mustSmallDF(t)
	dfr, err := newDFReader(df)
	require.NoError(t, err)
	dfr.Retain()
	require.True(t, dfr.Next())
	dfr.Release()
	require.NotNil(t, dfr.Record())
	dfr.Release()
	require.Nil(t, dfr.Record())
	dfr = &dfReader{err: errors.New("boom")}
	require.False(t, dfr.Next())
	dfr = &dfReader{}
	require.False(t, dfr.Next())

	up := newFakeRecordReaderForDataFrame(t)
	fr := newFilterReader(context.Background(), up, exprColGtZero())
	fr.Retain()
	require.True(t, fr.Next())
	require.NotNil(t, fr.Record())
	fr.Release()
	require.True(t, up.released)

	up = newFakeRecordReaderForDataFrame(t)
	pr := newProjectReader(up, []string{"missing"})
	pr.Retain()
	require.Nil(t, pr.Schema())
	require.False(t, pr.Next())
	require.ErrorContains(t, pr.Err(), "not found")
	pr.Release()

	up = newFakeRecordReaderForDataFrame(t)
	lr := newLimitReader(up, 1)
	lr.Retain()
	require.True(t, lr.Next())
	require.Equal(t, int64(1), lr.Record().NumRows())
	lr.Release()
	require.True(t, up.released)
	require.False(t, newLimitReader(newFakeRecordReaderForDataFrame(t), 0).Next())

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	cr := newCtxReader(canceled, newFakeRecordReaderForDataFrame(t))
	cr.(*ctxReader).Retain()
	require.False(t, cr.Next())
	require.ErrorIs(t, cr.Err(), context.Canceled)
	cr.Release()

	er := &errReader{err: errors.New("fixed"), schema: up.Schema()}
	er.Retain()
	require.NotNil(t, er.Schema())
	require.Nil(t, er.Record())
	require.False(t, er.Next())
	require.ErrorContains(t, er.Err(), "fixed")
	er.Release()

	_, err = (FileScan{Format: FileFormat(99)}).open(context.Background())
	require.ErrorContains(t, err, "unknown format")
}

func mustSmallDF(t *testing.T) *DataFrame {
	t.Helper()
	s, err := NewSeries("a", []int64{1, 2, 3})
	require.NoError(t, err)
	df, err := New([]*Series{s})
	require.NoError(t, err)
	return df
}

func exprColGtZero() expr.Expr {
	return expr.Col("a").Gt(expr.Lit(int64(0)))
}

type fakeRecordReaderForDataFrame struct {
	schema   *arrow.Schema
	record   arrow.Record
	seen     bool
	released bool
	err      error
}

func newFakeRecordReaderForDataFrame(t *testing.T) *fakeRecordReaderForDataFrame {
	t.Helper()
	s := arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewInt64Builder(memory.DefaultAllocator)
	b.AppendValues([]int64{1, -1, 2}, nil)
	a := b.NewArray()
	b.Release()
	rec := array.NewRecord(s, []arrow.Array{a}, 3)
	a.Release()
	t.Cleanup(rec.Release)
	return &fakeRecordReaderForDataFrame{schema: s, record: rec}
}

func (f *fakeRecordReaderForDataFrame) Schema() *arrow.Schema { return f.schema }
func (f *fakeRecordReaderForDataFrame) Retain()               {}
func (f *fakeRecordReaderForDataFrame) Next() bool {
	if f.seen {
		return false
	}
	f.seen = true
	return true
}
func (f *fakeRecordReaderForDataFrame) Record() arrow.Record           { return f.record }
func (f *fakeRecordReaderForDataFrame) RecordBatch() arrow.RecordBatch { return f.record }
func (f *fakeRecordReaderForDataFrame) Err() error                     { return f.err }
func (f *fakeRecordReaderForDataFrame) Release()                       { f.released = true }

func TestExecutorAndLazyBranchCoverage(t *testing.T) {
	df := mustSmallDF(t)
	ex := DataFrameExecutor{}
	ctx := context.Background()

	require.NotNil(t, df.Plan())
	_, err := ex.Scan(ctx, nil)
	require.ErrorContains(t, err, "node is nil")
	badScan := plan.NewScanNode(df.Schema(), plan.ScanSourceDataFrame)
	badScan.Handle = "bad"
	_, err = ex.Scan(ctx, badScan)
	require.ErrorContains(t, err, "expected *DataFrame")
	goodScan := plan.NewScanNode(df.Schema(), plan.ScanSourceDataFrame)
	goodScan.Handle = df
	goodScan.PushedLimit = 1
	out, err := ex.Scan(ctx, goodScan)
	require.NoError(t, err)
	require.Equal(t, int64(1), out.(*DataFrame).NumRows())
	sourceScan := plan.NewScanNode(df.Schema(), plan.ScanSourceCustom)
	sourceScan.PushedLimit = 1
	sourceScan.Handle = SourceScan{Schema: newFakeRecordReaderForDataFrame(t).Schema(), Open: func(context.Context, ScanHints) (array.RecordReader, error) {
		return newFakeRecordReaderForDataFrame(t), nil
	}, AllowNullable: true}
	out, err = ex.Scan(ctx, sourceScan)
	require.NoError(t, err)
	require.Equal(t, int64(1), out.(*DataFrame).NumRows())
	sourceScan.Handle = SourceScan{Schema: newFakeRecordReaderForDataFrame(t).Schema(), Open: func(context.Context, ScanHints) (array.RecordReader, error) {
		return nil, errors.New("open source")
	}}
	_, err = ex.Scan(ctx, sourceScan)
	require.ErrorContains(t, err, "open source")

	_, err = ex.Filter(ctx, "bad", exprColGtZero())
	require.ErrorContains(t, err, "filter")
	_, err = ex.Project("bad", []string{"a"})
	require.ErrorContains(t, err, "project")
	_, err = ex.Limit("bad", 1)
	require.ErrorContains(t, err, "limit")
	_, err = ex.Sort(ctx, "bad", []expr.SortKey{expr.By("a")})
	require.ErrorContains(t, err, "sort")
	_, err = ex.Aggregate(ctx, "bad", []string{"a"}, []expr.AggNode{expr.Col("a").Count()})
	require.ErrorContains(t, err, "aggregate")
	_, err = ex.Join(ctx, "bad", df, "a", "inner")
	require.ErrorContains(t, err, "join left")
	_, err = ex.Join(ctx, df, "bad", "a", "inner")
	require.ErrorContains(t, err, "join right")
	_, err = ex.WithColumn("bad", expr.Col("a"))
	require.ErrorContains(t, err, "withcolumn")
	_, err = ex.StreamFromDataFrame("bad")
	require.ErrorContains(t, err, "stream from dataframe")

	_, err = ex.ScanStream(ctx, nil)
	require.ErrorContains(t, err, "node is nil")
	_, err = ex.ScanStream(ctx, badScan)
	require.ErrorContains(t, err, "unsupported")
	_, err = ex.FilterStream(ctx, "bad", exprColGtZero())
	require.ErrorContains(t, err, "filter stream")
	_, err = ex.FilterStream(ctx, newFakeRecordReaderForDataFrame(t), expr.Expr{})
	require.ErrorContains(t, err, "nil predicate")
	_, err = ex.ProjectStream("bad", []string{"a"})
	require.ErrorContains(t, err, "project stream")
	er := ex.LimitStream("bad", 1).(*errReader)
	require.False(t, er.Next())
	require.ErrorContains(t, er.Err(), "limit stream")

	ssNode := plan.NewScanNode(df.Schema(), plan.ScanSourceCustom)
	ssNode.PushedColumns = []string{"a"}
	ssNode.PushedLimit = 2
	ssNode.Handle = SourceScan{Schema: newFakeRecordReaderForDataFrame(t).Schema(), Open: func(ctx context.Context, hints ScanHints) (array.RecordReader, error) {
		require.Equal(t, []string{"a"}, hints.Columns)
		require.Equal(t, int64(2), hints.Limit)
		return newFakeRecordReaderForDataFrame(t), nil
	}}
	streamOut, err := ex.ScanStream(ctx, ssNode)
	require.NoError(t, err)
	r := streamOut.(RecordReader)
	require.NotNil(t, r.Schema())
	r.Release()
	ssNode.Handle = SourceScan{Schema: newFakeRecordReaderForDataFrame(t).Schema(), Open: func(context.Context, ScanHints) (array.RecordReader, error) {
		return nil, errors.New("open")
	}}
	_, err = ex.ScanStream(ctx, ssNode)
	require.ErrorContains(t, err, "open")

	lf := df.Lazy()
	lp, err := lf.Plan()
	require.NoError(t, err)
	require.NotNil(t, lp)
	_, err = ((*DataFrame)(nil)).Lazy().Plan()
	require.ErrorContains(t, err, "dataframe is nil")
	_, err = (&LazyFrame{}).Plan()
	require.ErrorContains(t, err, "no root")
	_, err = NewLazyFileScan(FileScan{}).Plan()
	require.ErrorContains(t, err, "path is empty")
	_, err = NewLazySourceScan(SourceScan{}).Plan()
	require.ErrorContains(t, err, "schema is nil")
	_, err = NewLazySourceScan(SourceScan{Schema: newFakeRecordReaderForDataFrame(t).Schema()}).Plan()
	require.ErrorContains(t, err, "open is nil")
	sourceLF := NewLazySourceScan(SourceScan{Schema: newFakeRecordReaderForDataFrame(t).Schema(), Open: func(context.Context, ScanHints) (array.RecordReader, error) {
		return newFakeRecordReaderForDataFrame(t), nil
	}})
	require.NotNil(t, sourceLF.Filter(exprColGtZero()).Select("a").Limit(1).WithColumn(expr.Col("a").As("b")).Sort(expr.By("a")))
	_, err = lf.Filter(expr.Expr{}).Collect(ctx)
	require.Error(t, err)
	_, err = lf.Select().Collect(ctx)
	require.Error(t, err)
	_, err = lf.Limit(-1).Collect(ctx)
	require.Error(t, err)
	_, err = lf.WithColumn(expr.Expr{}).Collect(ctx)
	require.Error(t, err)
	_, err = lf.Sort().Collect(ctx)
	require.Error(t, err)
	_, err = lf.GroupBy("a").Agg().Collect(ctx)
	require.Error(t, err)
	_, err = lf.Join(nil, "a", "inner").Collect(ctx)
	require.Error(t, err)
}

func TestFileSchemaCSVBranchCoverage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.csv")
	require.NoError(t, os.WriteFile(path, []byte("a\n1\n2\n"), 0o600))
	s, err := FileSchema(FileScan{Path: path, Format: FileFormatCSV, CSV: ingestCSVConfigForTest()})
	require.NoError(t, err)
	require.Equal(t, "a", s.Field(0).Name)
	_, err = FileSchema(FileScan{Path: filepath.Join(t.TempDir(), "missing.csv"), Format: FileFormatCSV})
	require.Error(t, err)
}

func ingestCSVConfigForTest() ingest.CSVConfig {
	return ingest.CSVConfig{HasHeader: true, ChunkSize: 1}
}

func TestIOBranchCoverage(t *testing.T) {
	df := mustSmallDF(t)
	require.ErrorContains(t, WriteCSV(nil, filepath.Join(t.TempDir(), "x.csv")), "dataframe is nil")
	require.ErrorContains(t, WriteCSV(df, ""), "path is empty")
	require.ErrorContains(t, WriteParquet(nil, filepath.Join(t.TempDir(), "x.parquet")), "dataframe is nil")
	require.ErrorContains(t, WriteParquet(df, ""), "path is empty")
	_, err := ReadCSV(filepath.Join(t.TempDir(), "missing.csv"))
	require.Error(t, err)
	_, err = ReadParquet(filepath.Join(t.TempDir(), "missing.parquet"))
	require.Error(t, err)

	nilRR := newFakeRecordReaderForDataFrame(t)
	nilRR.record = nil
	_, err = collectReader(nilRR, true)
	require.ErrorContains(t, err, "record batch is nil")
	errRR := newFakeRecordReaderForDataFrame(t)
	errRR.err = errors.New("reader failed")
	errRR.seen = true
	_, err = collectReader(errRR, false)
	require.ErrorContains(t, err, "reader failed")

	csvPath := filepath.Join(t.TempDir(), "out.csv")
	require.NoError(t, WriteCSV(df, csvPath, WithCSVHeader(false), WithCSVComma(';'), WithCSVNullValue("NA"), WithCSVCRLF(true)))
	parquetPath := filepath.Join(t.TempDir(), "out.parquet")
	require.NoError(t, WriteParquet(df, parquetPath, WithParquetAllowNullable(false)))
	readBack, err := ReadParquet(parquetPath)
	require.NoError(t, err)
	require.Equal(t, df.NumRows(), readBack.NumRows())
}

func TestConstructorColumnShapeAndRechunkBranches(t *testing.T) {
	require.Nil(t, ((*DataFrame)(nil)).Plan())
	require.Nil(t, ((*DataFrame)(nil)).Head(1))
	require.Nil(t, ((*DataFrame)(nil)).Rechunk())
	require.Nil(t, ((*DataFrame)(nil)).RechunkToRows(1))
	require.False(t, ((*DataFrame)(nil)).ShouldRechunk())
	var nilCol *ChunkedColumn
	require.Zero(t, nilCol.Len())
	require.Nil(t, nilCol.DataType())
	require.Nil(t, nilCol.Chunked())
	require.Zero(t, NewChunkedColumn(nil).Len())
	require.Nil(t, NewChunkedColumn(nil).DataType())

	_, err := New([]*Series{nil})
	require.ErrorContains(t, err, "series 0 is nil")
	emptyName := NewSeriesFromColumn("", NewChunkedColumn(chunkedFromInt64([]int64{1})))
	_, err = New([]*Series{emptyName})
	require.ErrorContains(t, err, "name is empty")
	a, err := NewSeries("a", []int64{1})
	require.NoError(t, err)
	a2, err := NewSeries("a", []int64{1})
	require.NoError(t, err)
	_, err = New([]*Series{a, a2})
	require.ErrorContains(t, err, "duplicate")
	b, err := NewSeries("b", []int64{1, 2})
	require.NoError(t, err)
	_, err = New([]*Series{a, b})
	require.ErrorContains(t, err, "height")
	_, err = NewDataFrame(nil, nil)
	require.ErrorContains(t, err, "schema is nil")
	_, err = NewDataFrame(schema.New(schema.Field{Name: "a", ArrowType: arrow.PrimitiveTypes.Int64}), nil)
	require.ErrorContains(t, err, "schema fields")

	df := mustSmallDF(t)
	_, ok := df.Column("missing")
	require.False(t, ok)
	require.Equal(t, int64(0), df.Head(-1).NumRows())
	require.Equal(t, df.NumRows(), df.Head(999).NumRows())
	require.False(t, df.RechunkToRows(0).ShouldRechunk())
	rechunked := df.RechunkToRows(2)
	require.False(t, rechunked.ShouldRechunk())
}
