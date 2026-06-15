# Test Coverage Report

Snapshot of statement coverage, regenerated from `main`.

Command:

```sh
go test ./... -coverprofile=coverage.out -covermode=atomic
go tool cover -func=coverage.out
```

All tests pass.

## Overall Coverage

Total statement coverage across `./...`: **65.3%**.

This total includes command and example packages that have no tests and report
`0.0%`; excluding those, library coverage is substantially higher.

## Package Coverage

| Package | Coverage |
| --- | ---: |
| `github.com/karthedew/cosma/carray` | 83.6% |
| `github.com/karthedew/cosma/cmd/cosma-csv` | 0.0% |
| `github.com/karthedew/cosma/cmd/cosma-dev` | 0.0% |
| `github.com/karthedew/cosma/cmd/cosma-self` | 0.0% |
| `github.com/karthedew/cosma/dataframe` | 70.1% |
| `github.com/karthedew/cosma/dataset` | 90.4% |
| `github.com/karthedew/cosma/examples/adbc` | 0.0% |
| `github.com/karthedew/cosma/examples/basic` | 0.0% |
| `github.com/karthedew/cosma/expr` | 93.7% |
| `github.com/karthedew/cosma/gonum` | 85.0% |
| `github.com/karthedew/cosma/internal/compute` | 49.7% |
| `github.com/karthedew/cosma/internal/expr` | 93.0% |
| `github.com/karthedew/cosma/internal/ingest` | 70.0% |
| `github.com/karthedew/cosma/internal/ndingest` | 88.0% |
| `github.com/karthedew/cosma/internal/ndingest/blosc` | 86.3% |
| `github.com/karthedew/cosma/internal/stream` | 66.7% |
| `github.com/karthedew/cosma/memory` | 100.0% |
| `github.com/karthedew/cosma/plan` | 50.1% |
| `github.com/karthedew/cosma/scan` | 61.7% |
| `github.com/karthedew/cosma/schema` | 48.2% |
| `github.com/karthedew/cosma/store` | 88.5% |
| `github.com/karthedew/cosma/store/conformance` | 0.0% |
| `github.com/karthedew/cosma/store/memstore` | 92.3% |
| `github.com/karthedew/cosma/store/zarr` | 74.4% |

## Review Targets

Highest-impact library packages for additional tests, lowest coverage first:

| Area | Coverage | Notes |
| --- | ---: | --- |
| `schema` | 48.2% | Arrow dtype translation branches (`translate.go`) only partially covered; field/schema constructors and lookups need edge cases. |
| `internal/compute` | 49.7% | Aggregate dispatch, grouped reductions, binary/string comparisons, numeric literal variants, multi-key sort dispatch, take/filter error paths, temporal arithmetic. |
| `plan` | 50.1% | Logical/physical node methods, lower/execute paths, optimizer branches, stream execution, bind edge cases. |
| `scan` | 61.7% | Fill-value coercion, scan options, parquet lazy path, pushdown range conversions, cell-row encoding. |
| `internal/stream` | 66.7% | DataFrame-reader adapter retain/release and error paths. |
| `internal/ingest` | 70.0% | CSV/Parquet/ADBC reader option and error branches. |
| `dataframe` | 70.1% | IO options, temporal formatting, Series constructors for many Arrow dtypes, stream retain/release, lazy join/sort/group paths. |
| `store/zarr` | 74.4% | Remaining dtype/fill-value/codec-spec branches and error paths. |

`store/conformance` reports `0.0%` because it is a test-helper package exercised
through other packages' tests, not in-package. Command and example packages
(`cmd/*`, `examples/*`) report `0.0%`; decide separately whether they get smoke
tests or are excluded from any future coverage gate.
