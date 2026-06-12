package dataframe_test

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/expr"
	"github.com/karthedew/cosma/internal/compute"
)

// benchCSVPath is the synthetic 1M-row CSV generated once in TestMain and read by
// BenchmarkScanCSV_1M. It is removed on exit.
var benchCSVPath string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "cosma-bench")
	if err != nil {
		panic(err)
	}
	benchCSVPath = filepath.Join(dir, "bench_1m.csv")
	if err := writeSyntheticCSV(benchCSVPath, 1_000_000); err != nil {
		panic(err)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// writeSyntheticCSV emits an n-row CSV with an int64 key column and an int64
// value column. Keys cycle through 10 groups so the file doubles as input for a
// groupby-shaped workload.
func writeSyntheticCSV(path string, n int) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 1<<20)
	if _, err := w.WriteString("key,value\n"); err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		if _, err := w.WriteString(strconv.Itoa(i % 10)); err != nil {
			return err
		}
		if err := w.WriteByte(','); err != nil {
			return err
		}
		if _, err := w.WriteString(strconv.Itoa(i)); err != nil {
			return err
		}
		if err := w.WriteByte('\n'); err != nil {
			return err
		}
	}
	return w.Flush()
}

// buildInt64DF builds an in-memory DataFrame with a key column (group = i%groups)
// and a sequential value column, both int64.
func buildInt64DF(b *testing.B, n, groups int) *dataframe.DataFrame {
	b.Helper()
	keys := make([]int64, n)
	vals := make([]int64, n)
	for i := 0; i < n; i++ {
		keys[i] = int64(i % groups)
		vals[i] = int64(i)
	}
	key, err := dataframe.NewSeries("key", keys)
	if err != nil {
		b.Fatal(err)
	}
	val, err := dataframe.NewSeries("value", vals)
	if err != nil {
		b.Fatal(err)
	}
	df, err := dataframe.New([]*dataframe.Series{key, val})
	if err != nil {
		b.Fatal(err)
	}
	return df
}

func BenchmarkFilter_1M(b *testing.B) {
	const n = 1_000_000
	df := buildInt64DF(b, n, n) // unique keys; value used for predicate
	ctx := context.Background()
	pred := expr.Col("value").Gt(expr.Lit(int64(n / 2)))

	b.ReportAllocs()
	b.SetBytes(int64(n) * 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := df.Filter(ctx, pred)
		if err != nil {
			b.Fatal(err)
		}
		_ = out
	}
}

func BenchmarkSort_1M(b *testing.B) {
	const n = 1_000_000
	// Reverse-ordered values so the sort does real work.
	keys := make([]int64, n)
	vals := make([]int64, n)
	for i := 0; i < n; i++ {
		keys[i] = int64(i)
		vals[i] = int64(n - i)
	}
	key, err := dataframe.NewSeries("key", keys)
	if err != nil {
		b.Fatal(err)
	}
	val, err := dataframe.NewSeries("value", vals)
	if err != nil {
		b.Fatal(err)
	}
	df, err := dataframe.New([]*dataframe.Series{key, val})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.SetBytes(int64(n) * 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := df.Sort(ctx, expr.By("value"))
		if err != nil {
			b.Fatal(err)
		}
		_ = out
	}
}

func BenchmarkGroupBy_1M(b *testing.B) {
	const n = 1_000_000
	df := buildInt64DF(b, n, 10)
	ctx := context.Background()

	b.ReportAllocs()
	b.SetBytes(int64(n) * 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := df.GroupBy("key").Agg(ctx, expr.Col("value").Sum())
		if err != nil {
			b.Fatal(err)
		}
		_ = out
	}
}

func BenchmarkJoin_100K(b *testing.B) {
	const n = 100_000
	left := buildInt64DF(b, n, n)  // unique int64 keys
	right := buildInt64DF(b, n, n) // unique int64 keys, full overlap
	ctx := context.Background()

	b.ReportAllocs()
	b.SetBytes(int64(n) * 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := left.Join(ctx, right, "key", "inner")
		if err != nil {
			b.Fatal(err)
		}
		_ = out
	}
}

func BenchmarkScanCSV_1M(b *testing.B) {
	const n = 1_000_000
	info, err := os.Stat(benchCSVPath)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.SetBytes(info.Size())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		df, err := dataframe.ReadCSV(benchCSVPath)
		if err != nil {
			b.Fatal(err)
		}
		if df.NumRows() != n {
			b.Fatalf("rows = %d, want %d", df.NumRows(), n)
		}
	}
}

// buildMultiChunkInt64DF builds an in-memory multi-chunk DataFrame with a key
// column (group = i%groups) and a sequential value column. It splits the rows
// into numChunks equal-sized chunks via Concat so the parallel paths actually
// fan out across multiple goroutines.
func buildMultiChunkInt64DF(b *testing.B, n, groups, numChunks int) *dataframe.DataFrame {
	b.Helper()
	size := n / numChunks
	dfs := make([]*dataframe.DataFrame, numChunks)
	for c := 0; c < numChunks; c++ {
		lo := c * size
		hi := lo + size
		if c == numChunks-1 {
			hi = n // absorb rounding remainder into last chunk
		}
		chunk := hi - lo
		keys := make([]int64, chunk)
		vals := make([]int64, chunk)
		for i := 0; i < chunk; i++ {
			row := lo + i
			keys[i] = int64(row % groups)
			vals[i] = int64(row)
		}
		ks, err := dataframe.NewSeries("key", keys)
		if err != nil {
			b.Fatal(err)
		}
		vs, err := dataframe.NewSeries("value", vals)
		if err != nil {
			b.Fatal(err)
		}
		df, err := dataframe.New([]*dataframe.Series{ks, vs})
		if err != nil {
			b.Fatal(err)
		}
		dfs[c] = df
	}
	out, err := dataframe.Concat(dfs...)
	if err != nil {
		b.Fatal(err)
	}
	return out
}

// BenchmarkFilterParallel_1M benchmarks df.Filter with the parallel path
// (GOMAXPROCS workers) over a 1M-row, 8-chunk DataFrame.
func BenchmarkFilterParallel_1M(b *testing.B) {
	const n = 1_000_000
	const numChunks = 8
	df := buildMultiChunkInt64DF(b, n, n, numChunks)
	ctx := context.Background()
	pred := expr.Col("value").Gt(expr.Lit(int64(n / 2)))

	compute.SetParallelism(0) // GOMAXPROCS
	defer compute.SetParallelism(0)

	b.ReportAllocs()
	b.SetBytes(int64(n) * 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := df.Filter(ctx, pred)
		if err != nil {
			b.Fatal(err)
		}
		_ = out
	}
}

// BenchmarkGroupByParallel_1M benchmarks df.GroupBy.Agg with the parallel
// path over a 1M-row, 8-chunk DataFrame.
func BenchmarkGroupByParallel_1M(b *testing.B) {
	const n = 1_000_000
	const numChunks = 8
	df := buildMultiChunkInt64DF(b, n, 10, numChunks)
	ctx := context.Background()

	compute.SetParallelism(0) // GOMAXPROCS
	defer compute.SetParallelism(0)

	b.ReportAllocs()
	b.SetBytes(int64(n) * 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := df.GroupBy("key").Agg(ctx, expr.Col("value").Sum())
		if err != nil {
			b.Fatal(err)
		}
		_ = out
	}
}
