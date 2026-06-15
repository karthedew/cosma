# Test Coverage Report

Generated on 2026-06-15 from branch `main`.

Command:

```sh
go test ./... -coverprofile=coverage.out -covermode=atomic
go tool cover -func=coverage.out
```

All tests passed.

## Overall Coverage

Total statement coverage across `./...`: **87.3%**.

This total includes command packages, examples, and helper packages that currently have no tests and report `0.0%` coverage.

## Package Coverage

| Package | Coverage |
| --- | ---: |
| `github.com/karthedew/cosma/carray` | 83.6% |
| `github.com/karthedew/cosma/cmd/cosma-csv` | 0.0% |
| `github.com/karthedew/cosma/cmd/cosma-dev` | 0.0% |
| `github.com/karthedew/cosma/cmd/cosma-self` | 0.0% |
| `github.com/karthedew/cosma/dataframe` | 90.0% |
| `github.com/karthedew/cosma/dataset` | 90.4% |
| `github.com/karthedew/cosma/examples/adbc` | 0.0% |
| `github.com/karthedew/cosma/examples/basic` | 0.0% |
| `github.com/karthedew/cosma/expr` | 93.7% |
| `github.com/karthedew/cosma/gonum` | 85.0% |
| `github.com/karthedew/cosma/internal/compute` | 90.5% |
| `github.com/karthedew/cosma/internal/expr` | 93.0% |
| `github.com/karthedew/cosma/internal/ingest` | 93.3% |
| `github.com/karthedew/cosma/internal/ndingest` | 88.0% |
| `github.com/karthedew/cosma/internal/ndingest/blosc` | 86.3% |
| `github.com/karthedew/cosma/internal/stream` | 92.6% |
| `github.com/karthedew/cosma/memory` | 100.0% |
| `github.com/karthedew/cosma/plan` | 94.2% |
| `github.com/karthedew/cosma/scan` | 95.1% |
| `github.com/karthedew/cosma/schema` | 98.8% |
| `github.com/karthedew/cosma/store` | 88.5% |
| `github.com/karthedew/cosma/store/conformance` | 0.0% |
| `github.com/karthedew/cosma/store/memstore` | 92.3% |
| `github.com/karthedew/cosma/store/zarr` | 92.9% |

## Review Targets

Packages still below 90% coverage:

| Area | Coverage | Notes |
| --- | ---: | --- |
| `carray` | 83.6% | Close to target; remaining geometry/edge paths should be reviewed. |
| `gonum` | 85.0% | Numeric conversion edge cases, especially `float64At`, remain below target. |
| `internal/ndingest/blosc` | 86.3% | Decode edge branches and registration helper remain below target. |
| `store` | 88.5% | Close to target; object kind/attrs and tree/store edge branches are visible gaps. |
| `internal/ndingest` | 88.0% | Close to target; codec and dtype-width branches are the main remaining gaps. |

Packages currently at or above 90%: `dataframe`, `dataset`, `expr`, `internal/compute`, `internal/expr`, `internal/ingest`, `internal/stream`, `memory`, `plan`, `scan`, `schema`, `store/memstore`, and `store/zarr`.

Command, example, and conformance helper packages report `0.0%`; decide separately whether they should get smoke tests or be excluded from future coverage gates.
