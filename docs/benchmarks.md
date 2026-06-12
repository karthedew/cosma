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
