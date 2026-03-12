# Cosma

Cosma is a pre-alpha, Arrow-backed dataframe engine for Go. It focuses on a
small core of columnar primitives today, with a path toward a Polars-like API
and query planner tomorrow.

Status
- Pre-alpha prototype. APIs will change and many features are stubs.
- Targeting a minimal, composable core first: Series, DataFrame, Expr, Compute.

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
- Arrow schema construction for RecordBatchIter.
- Expression compilation and evaluation.
- Compute operators: filter, project, groupby, join.
- IO: CSV/Parquet scanning and pushdown.

Contributing
See `CONTRIBUTING.md` for local setup and workflow guidelines.

License
Apache-2.0. See `LICENSE`.
