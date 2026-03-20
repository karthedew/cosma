# Cosma

Cosma is a pre-alpha, Arrow-native dataframe engine for Go.

It is focused on fast columnar data workflows with a practical eager API today,
and a lazy + streaming execution path for larger-than-memory datasets.

## Status

- Pre-alpha: APIs are still evolving.
- Current core packages: `dataframe`, `scan`, `plan`, `compute`.
- Short-term focus: correctness, memory ownership, streaming execution, and speed.

## Project Focus

- Useful eager dataframe operations first.
- Lazy planning and chunked streaming execution.
- Parallel execution for high throughput.
- Arrow-native interoperability, including ADBC connectivity.
- Numerical handoff into Gonum.

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
	ids, err := dataframe.NewSeries("ids", []int64{1, 2, 3})
	if err != nil {
		panic(err)
	}
	names, err := dataframe.NewSeries("names", []string{"alpha", "beta", "gamma"})
	if err != nil {
		panic(err)
	}

	df, err := dataframe.New([]*dataframe.Series{ids, names})
	if err != nil {
		panic(err)
	}

	fmt.Println(df.String())
}
```

## IO (CSV and Parquet)

```go
package main

import (
	"github.com/karthedew/cosma/dataframe"
)

func main() {
	df, err := dataframe.ReadCSV("data.csv")
	if err != nil {
		panic(err)
	}

	if err := dataframe.WriteParquet(df, "data.parquet"); err != nil {
		panic(err)
	}

}
```

## Streaming Scan (Chunked)

```go
package main

import (
	"github.com/karthedew/cosma/scan"
)

func main() {
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

}
```

## Lazy Planning (Preview)

```go
package main

import (
	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/plan"
)

func main() {
	ids, err := dataframe.NewSeries("ids", []int64{1, 2, 3})
	if err != nil {
		panic(err)
	}
	names, err := dataframe.NewSeries("names", []string{"alpha", "beta", "gamma"})
	if err != nil {
		panic(err)
	}

	df, err := dataframe.New([]*dataframe.Series{ids, names})
	if err != nil {
		panic(err)
	}

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
	_ = bound

}
```

Note: expression APIs are still maturing.

## Development

- Run tests: `go test ./...`
- Lint: `golangci-lint run`

## Docs

- Roadmap: `ROADMAP.md`
- Architecture: `docs/architecture.md`
- Package guide: `docs/packages.md`
- Contributing: `CONTRIBUTING.md`

## License

Apache-2.0. See `LICENSE`.
