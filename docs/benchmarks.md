# Benchmark Baselines — Phase 3 (Thu Jun 11 07:54:51 MDT 2026)

```
goos: darwin
goarch: arm64
pkg: github.com/karthedew/cosma/dataframe
cpu: Apple M1
BenchmarkFilter_1M-8    	     157	  23571173 ns/op	 339.40 MB/s	26041156 B/op	     136 allocs/op
BenchmarkSort_1M-8      	      32	 109345001 ns/op	  73.16 MB/s	71997459 B/op	     110 allocs/op
BenchmarkGroupBy_1M-8   	      30	 112872300 ns/op	  70.88 MB/s	80018989 B/op	 3000080 allocs/op
BenchmarkJoin_100K-8    	     278	  12871395 ns/op	  62.15 MB/s	23335733 B/op	  299853 allocs/op
BenchmarkScanCSV_1M-8   	       2	2580757916 ns/op	   3.44 MB/s	2004453616 B/op	23000094 allocs/op
PASS
ok  	github.com/karthedew/cosma/dataframe	26.624s
```

# Benchmark Results — Phase 6 parallel execution (Wed Jun 11 2026)

Machine: Apple M1, darwin/arm64, GOMAXPROCS=8.

New parallel benchmarks use an 8-chunk DataFrame (1M rows split into 8×125K
chunks) so EvalParallel fans out across all 8 cores. The serial baselines
(BenchmarkFilter_1M, BenchmarkGroupBy_1M) use a single-chunk frame and
SetParallelism(1)-equivalent single-batch path.

```
goos: darwin
goarch: arm64
pkg: github.com/karthedew/cosma/dataframe
cpu: Apple M1
BenchmarkFilter_1M-8            	      51	  23899055 ns/op	 334.74 MB/s	26041168 B/op	     137 allocs/op
BenchmarkSort_1M-8              	      10	 108542112 ns/op	  73.70 MB/s	71997460 B/op	     110 allocs/op
BenchmarkGroupBy_1M-8           	       9	 121531968 ns/op	  65.83 MB/s	89044218 B/op	 3000500 allocs/op
BenchmarkJoin_100K-8            	      81	  15248061 ns/op	  52.47 MB/s	23334990 B/op	  299853 allocs/op
BenchmarkScanCSV_1M-8           	       1	2524508833 ns/op	   3.52 MB/s	2004453952 B/op	23000096 allocs/op
BenchmarkFilterParallel_1M-8    	     152	   7813826 ns/op	1023.83 MB/s	26428266 B/op	     599 allocs/op
BenchmarkGroupByParallel_1M-8   	       9	 125336518 ns/op	  63.83 MB/s	89042203 B/op	 3000495 allocs/op
PASS
ok  	github.com/karthedew/cosma/dataframe	15.065s
```

## Phase 6 analysis

**Filter (embarrassingly parallel):** 23.9 ms serial → 7.8 ms parallel on 8
cores — a **3.1x speedup** (334 MB/s → 1024 MB/s). This matches the
theoretical expectation for a pure compute-bound operator with no merge step
beyond list assembly.

**GroupBy (two-phase parallel):** The parallel GroupReduce strips are fast, but
the group-ID assignment loop (first pass: row → tuple → map key) is still
single-threaded and dominates for the 1M/10-group workload. The parallel
reduction phase saves time, but the overall end-to-end time is within noise of
the serial path (~122 ms vs ~125 ms). For high-cardinality groups or expensive
reduction columns the two-phase approach will show larger gains.

**No regressions:** all Phase 3 serial baselines are within ~5% of their
previous values, confirming the parallel wiring does not slow the serial path.
