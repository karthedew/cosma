# Cosma

Cosma is an Arrow-native dataframe engine for Go.

Fast columnar data workflows with a public expression API, lazy+streaming execution,
parallel kernels, ADBC database connectivity, and Gonum integration.

## Status

v0.1.0 — APIs are evolving. Core packages (`dataframe`, `expr`, `scan`, `gonum`) are
stable enough for experimentation; breaking changes are possible before v1.0.

## Install

```bash
go get github.com/karthedew/cosma@latest
```

## Quickstart

```go
package main

import (
    "fmt"

    "github.com/karthedew/cosma/dataframe"
)

func main() {
    ids, _ := dataframe.NewSeries("ids", []int64{1, 2, 3})
    names, _ := dataframe.NewSeries("names", []string{"alpha", "beta", "gamma"})

    df, _ := dataframe.New([]*dataframe.Series{ids, names})
    fmt.Println(df.String())
}
```

## Filter & Expressions

Expressions are built with the public `cosma/expr` package and evaluated lazily or
eagerly. The expression tree is fully public — inspectable, serializable, and safe
for custom optimizer passes.

```go
import (
    "context"
    "github.com/karthedew/cosma/dataframe"
    "github.com/karthedew/cosma/expr"
)

result, err := df.Filter(context.Background(),
    expr.Col("age").Gt(expr.Lit(int64(30))).And(expr.Col("active").Eq(expr.Lit(true))),
)
```

Supported: comparisons, arithmetic, boolean (Kleene semantics), `IsNull`/`IsNotNull`,
`Cast`, aggregates (`Sum`, `Count`, `Mean`, `Min`, `Max`). Full Arrow type coverage
including string, temporal, and `Decimal128`.

## IO (CSV and Parquet)

```go
df, err := dataframe.ReadCSV("data.csv")
err = dataframe.WriteParquet(df, "data.parquet")
```

## Streaming Scan

```go
import "github.com/karthedew/cosma/scan"

reader, err := scan.ScanCSV("data.csv", scan.WithCSVChunkSize(2048))
defer reader.Release()

for reader.Next() {
    rec := reader.Record()
    _ = rec
}
if err := reader.Err(); err != nil {
    panic(err)
}
```

## Lazy Execution

`Lazy()` builds a logical plan. `Collect` executes it with predicate/projection/limit
pushdown into scan nodes.

```go
import (
    "context"
    "github.com/karthedew/cosma/dataframe"
    "github.com/karthedew/cosma/expr"
)

result, err := df.Lazy().
    Filter(expr.Col("age").Gt(expr.Lit(int64(30)))).
    Select("name", "age").
    Limit(100).
    Collect(context.Background())
```

For larger-than-memory datasets, use `CollectStream` to get a per-batch iterator
instead of a materialized `*DataFrame`:

```go
reader, err := df.Lazy().Filter(...).CollectStream(ctx)
defer reader.Release()
for reader.Next() { ... }
```

## Sort, GroupBy, Join

```go
// Multi-key sort
sorted, err := df.Sort(ctx,
    expr.By("age").Desc(),
    expr.By("name").WithNullsFirst(),
)

// GroupBy aggregation
grouped, err := df.GroupBy("department").Agg(ctx,
    expr.Col("salary").Sum().As("total_salary"),
    expr.Col("id").Count().As("headcount"),
)

// Hash join (inner or left)
joined, err := df.Join(ctx, other, "user_id", "inner")
```

## Parallel Execution

Filter and groupby operations automatically fan out across CPU cores. The engine uses
`GOMAXPROCS` by default; parallelism can be tuned internally via `compute.SetParallelism`.
Demonstrated 3.1× filter speedup at 8 workers on a 1M-row benchmark.

## ADBC Connectivity

Any ADBC-compatible database (DuckDB, PostgreSQL, FlightSQL) feeds into the same
`internal/ingest` seam as CSV and Parquet — the result is an `array.RecordReader` that
plugs directly into `dataframe.FromRecordBatches`. See [`examples/adbc/`](examples/adbc/)
for a complete end-to-end example.

## Gonum Integration

```go
import (
    cosmagonum "github.com/karthedew/cosma/gonum"
    "gonum.org/v1/gonum/mat"
)

// Export numeric columns to a Gonum matrix
m, err := cosmagonum.ToMatrix(df, cosmagonum.MatrixOptions{
    Cols:       []string{"x", "y", "z"},
    NullPolicy: cosmagonum.NullDrop,
})

// Export a single Series to a vector
v, err := cosmagonum.ToVector(series, cosmagonum.VectorOptions{
    NullPolicy: cosmagonum.NullFill,
    FillValue:  0,
})
```

`NullPolicy` must be chosen explicitly — `NullError` (default), `NullDrop`, or `NullFill`.
No silent behavior.

## Development

```bash
go test -race ./...   # full suite
go test -bench=. ./dataframe/   # benchmarks
golangci-lint run
```

## Docs

- [Roadmap](docs/roadmap.md)
- [Architecture](docs/architecture.md)
- [Package guide](docs/packages.md)
- [ADRs](docs/adr/README.md)
- [Benchmarks](docs/benchmarks.md)
- [Contributing](CONTRIBUTING.md)

## License

Apache-2.0. See `LICENSE`.
