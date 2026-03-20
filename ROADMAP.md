# Cosma Roadmap

This roadmap focuses on shipping a fast, reliable, Arrow-native dataframe engine for Go.

Current priorities:
- Make eager dataframe workflows immediately useful.
- Build lazy + streaming execution that can process datasets larger than memory.
- Make performance a first-class concern, including parallel execution.
- Integrate with ADBC drivers for database connectivity.
- Integrate with Gonum for numerical computing handoff.

## Guiding Principles

1. Correctness before acceleration: no silent coercion bugs, clear null semantics, deterministic behavior.
2. Explicit memory ownership: Arrow retain/release rules are part of the API contract.
3. Streaming by design: avoid full materialization by default on large data paths.
4. Parallel where it matters: partitioned, measurable speedups on CPU-bound operators.
5. Small stable surface: ship thin, composable APIs and harden incrementally.

## Phase 1 - Core Reliability and Eager UX

Goal: make the eager dataframe API trustworthy and useful for day-to-day use.

Focus:
- DataFrame/Series memory lifecycle and ownership contract.
- Predicate coercion safety and schema consistency rules.
- Context propagation, cancellation, and error quality.
- CSV/Parquet behavior parity and practical eager defaults.
- CI hardening (`go test -race ./...`).

Deliverables:
- Deterministic retain/release behavior and lifecycle docs.
- Correct predicate binding for numeric literals (no lossy or overflow coercion).
- Duplicate schema name policy enforced consistently.
- Improved eager IO semantics and guardrails.

Exit criteria:
- Core correctness and lifecycle tests are green.
- No known high-severity correctness issues in eager paths.
- CI includes race detection.

## Phase 2 - Useful Eager DataFrame Operations

Goal: provide practical, fast eager transformations that users expect.

Focus:
- Complete and optimize core eager operations (project/filter/limit/sort/select/add or replace column).
- Implement missing compute functionality (project/filter/groupby/join) in incremental slices.
- Strengthen expression support where needed for eager use.

Deliverables:
- Stable eager operator behavior with conformance tests.
- Benchmarks for key operations on representative datasets.
- Clear API docs for eager workflows.

Exit criteria:
- Core eager operator suite is implemented and tested.
- Baseline performance targets are documented and repeatable.

## Phase 3 - Lazy Planning + Streaming Execution

Goal: support lazy queries executed over chunked streams, including datasets larger than memory.

Focus:
- Expand logical planning and binding.
- Compile lazy plans into streaming pipelines.
- Use chunked record processing end-to-end.
- Avoid unnecessary full-table materialization in execution paths.

Deliverables:
- End-to-end lazy API path (`Lazy() ... Plan()`) into streaming execution.
- Chunked DataFrame and stream interoperability with predictable semantics.
- Tests for memory-bounded behavior and cancellation on long scans.

Exit criteria:
- Lazy plans execute correctly through streaming pipelines.
- Large-data workflows run without requiring full in-memory materialization.

## Phase 4 - Parallel Execution Engine

Goal: make Cosma lightning fast on multi-core workloads.

Focus:
- Partitioned scan and operator execution.
- Parallel implementations for filter/project/map/groupby/join where applicable.
- Merge/reduce stages with deterministic output semantics.
- Profiling-driven optimization and allocator tuning.

Deliverables:
- Configurable execution parallelism.
- Operator-level metrics/benchmarks and profiling workflow.
- Documented performance characteristics by workload shape.

Exit criteria:
- Demonstrated speedups versus single-thread baseline on benchmark suite.
- Parallel execution remains correct and race-free.

## Phase 5 - ADBC Connectivity

Goal: connect Cosma to ADBC-compliant data sources with Arrow-native transfer.

Focus:
- ADBC reader adapters to produce Arrow record batches/streams.
- Schema mapping and null semantics alignment.
- Error and retry handling for connector boundaries.

Deliverables:
- Initial ADBC connector support for at least one production-ready driver.
- End-to-end examples of scan -> transform -> dataframe/stream output.

Exit criteria:
- ADBC ingestion path is stable, tested, and documented.

## Phase 6 - Gonum Integration

Goal: make Cosma a strong data-prep layer for numerical workflows.

Focus:
- Efficient conversion/export paths from dataframe/record batches to Gonum-friendly structures.
- Deterministic column ordering and null handling policy for numerical export.
- Benchmarks for conversion overhead and throughput.

Deliverables:
- Gonum integration helpers with clear constraints and examples.
- Tests validating shape, ordering, and null semantics.

Exit criteria:
- Users can reliably build numerical pipelines from Cosma into Gonum.

## Ongoing Workstreams (Across All Phases)

- Documentation accuracy and API examples.
- Developer ergonomics and package stability.
- Observability (errors, metrics hooks, profiling support).
- Backward compatibility notes for pre-alpha users.
