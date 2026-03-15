# Cosma

Cosma is a pre-alpha, Arrow-backed dataframe engine for Go. It focuses on a
small core of columnar primitives today, with a path toward a Polars-like API
and query planner tomorrow.

Status
- Pre-alpha prototype. APIs will change and many features are stubs.
- Targeting a minimal, composable core first: DataFrame, scan, plan, compute.

Goals
- Arrow-first columnar data model.
- Clear, minimal API surface that scales into lazy execution.
- Friendly developer experience with strong tests and docs.

Install
```bash
go get github.com/karthedew/cosma@latest
```

Quickstart
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

Lazy Planning (Preview)
```go
lp, err := df.Lazy().
    Select("names").
    Limit(10).
    Plan()
if err != nil {
    panic(err)
}

bound, err := plan.Bind(lp)
if err != nil {
    panic(err)
}
```

Note: predicate expressions are still internal-only and will be surfaced when
the expression API stabilizes.

IO (CSV/Parquet)
```go
df, err := dataframe.ReadCSV("data.csv")
if err != nil {
    panic(err)
}

if err := dataframe.WriteParquet(df, "data.parquet"); err != nil {
    panic(err)
}
```

Streaming Scan (CSV/Parquet)
```go
reader, err := scan.ScanCSV("data.csv", scan.WithCSVChunkSize(2048))
if err != nil {
    panic(err)
}
defer reader.Release()

for reader.Next() {
    rec := reader.Record()
    _ = rec
}
if err := reader.Err(); err != nil {
    panic(err)
}
```

IO Options
```go
df, err := dataframe.ReadCSV(
    "data.csv",
    dataframe.WithCSVChunkSize(4096),
    dataframe.WithCSVNullValues([]string{"", "NA"}),
)
if err != nil {
    panic(err)
}

err = dataframe.WriteParquet(
    df,
    "data.parquet",
    dataframe.WithParquetAllowNullable(true),
)
if err != nil {
    panic(err)
}
```

Development
- Run tests: `go test ./...`
- Lint: `golangci-lint run`

Roadmap (early)
- See `docs/roadmap.md` for the full multi-phase plan.
- See `docs/architecture.md` and `docs/packages.md` for package intent.

Contributing
See `CONTRIBUTING.md` for local setup and workflow guidelines.

License
Apache-2.0. See `LICENSE`.
