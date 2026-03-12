# Cosma Roadmap Checklist

Use this as the execution checklist for building Cosma into a Go-native Arrow feature engine + Flight server for Ray workloads.

## Phase 0 - Architectural Decisions

- [ ] Write ADR: Arrow is the in-memory format everywhere.
- [ ] Write ADR: Flight is the cross-process protocol.
- [ ] Write ADR: Python is the Ray boundary.
- [ ] Write ADR: Cosma focuses on feature engineering, not GPU training.
- [ ] Document explicit non-goals (no custom DL framework, scheduler, or object store).
- [ ] Define version compatibility policy for Arrow Go/Python.
- [ ] Define schema evolution policy (strict vs additive).
- [ ] Define MVP acceptance test: Go -> Flight -> Python -> Ray actor.

## Phase 1 - Arrow-Native Core Foundation

### 1.1 Column Abstraction

- [ ] Implement `Column` with `arrow.Field` and Arrow-backed data.
- [ ] Support primitive types.
- [ ] Support dictionary arrays.
- [ ] Add list array support behind a feature flag (optional early).
- [ ] Add tests for null handling and type safety.
- [ ] Add tests for memory lifecycle (`Release`, refcount expectations).

### 1.2 Minimal DataFrame

- [ ] Implement `DataFrame` schema + column container.
- [ ] Implement projection/select.
- [ ] Implement vectorized filter.
- [ ] Implement add/replace column.
- [ ] Implement zero-copy slicing.
- [ ] Add correctness tests for each operation.
- [ ] Add baseline benchmarks (select/filter/slice on 1M rows).

### 1.3 RecordBatch as Unit of Work

- [ ] Add `ToRecordBatch` / batch materialization API.
- [ ] Add batch iterator API for chunked execution.
- [ ] Ensure internal pipelines operate on Arrow record batches.
- [ ] Verify no row-oriented JSON/protobuf path exists in core operations.

### 1.4 IO (CSV/Parquet)

- [ ] Ship `ReadCSV`/`WriteCSV` with option structs and nullable defaults.
- [ ] Ship `ReadParquet`/`WriteParquet` with Arrow properties passthrough.
- [ ] Add CSV/Parquet round-trip tests for nulls and missing values.
- [ ] Add IO examples + docs for option usage.

## Phase 2 - Feature Engineering Layer

### 2.1 Expression Engine

- [ ] Add minimal expression AST (`col`, literal, arithmetic, comparison, boolean).
- [ ] Back expression execution with Arrow compute kernels where available.
- [ ] Add expression conformance tests.

### 2.2 GroupBy (Phase 2.5)

- [ ] Implement hash groupby with `sum`, `mean`, `count`, `min`, `max`.
- [ ] Add deterministic output schema and naming conventions.
- [ ] Validate results against a reference implementation on sample datasets.

### 2.3 ML Batch Materialization

- [ ] Implement `ToMLBatch(cols []string) arrow.Record`.
- [ ] Guarantee deterministic column ordering.
- [ ] Define and test null semantics for ML-boundary outputs.
- [ ] Benchmark batch materialization throughput and memory.

## Phase 3 - Arrow Flight Server

- [ ] Implement `GetSchema(dataset_id)`.
- [ ] Implement `GetBatch(dataset_id)`.
- [ ] Implement `StreamBatches(dataset_id)`.
- [ ] Build in-memory dataset registry abstraction.
- [ ] Enforce Arrow IPC-only transport (no JSON row encoding).
- [ ] Add integration test with Python `pyarrow.flight` client.
- [ ] Add cancellation/backpressure tests for streaming.

## Phase 4 - Python Integration Layer

- [ ] Build thin Python client package (`get_schema`, `get_batch`, `stream_batches`).
- [ ] Add conversion helpers to `pyarrow.Table`.
- [ ] Add conversion helpers to `polars.from_arrow()`.
- [ ] Add conversion helpers for Torch/Numpy handoff.
- [ ] Publish examples showing end-to-end usage.

## Phase 5 - Ray Integration Patterns

### 5.1 Pull Model (Preferred)

- [ ] Build reference Ray actor that pulls batches from Cosma.
- [ ] Demonstrate training loop consuming Flight batches.
- [ ] Add retry/error-handling pattern for transient network failures.

### 5.2 Streaming Dataset Pattern

- [ ] Add batch iteration protocol for long-running consumers.
- [ ] Add backpressure behavior tests.
- [ ] Add partition-awareness metadata in stream responses.

## Phase 6 - Optional Tensor Support

- [ ] Add minimal `Tensor` type (`Data []float32`, `Shape []int64`) if needed.
- [ ] Use only for ML extraction and dense block paths.
- [ ] Keep Arrow buffers as source-of-truth storage.
- [ ] Gate by benchmark-proven benefit.

## Phase 7 - Performance and Advanced Execution

- [ ] Tune Arrow memory allocator usage and pooling.
- [ ] Add partitioned parallel scan engine with goroutines.
- [ ] Add operator-level profiling and flamegraph workflow.
- [ ] Prototype lazy execution DAG only after eager path is stable.

## Phase 8 - Distributed Awareness

- [ ] Add dataset partition metadata model.
- [ ] Add shard registration API.
- [ ] Add node-aware routing strategy.
- [ ] Add consistency model documentation for shard metadata.

## Phase 9 - Production Hardening

- [ ] Add Prometheus metrics (latency, throughput, memory, errors).
- [ ] Add memory pressure controls.
- [ ] Add disk spill policy for oversized workloads.
- [ ] Add schema validation and compatibility checks.
- [ ] Add upgrade/backward-compatibility test matrix.
- [ ] Add operational runbook and SLO definitions.

## MVP Fast Path (Ship First)

- [ ] Primitive-only Arrow DataFrame.
- [ ] RecordBatch export API.
- [ ] Flight server (`GetSchema`, `GetBatch`, `StreamBatches`).
- [ ] Python Flight client.
- [ ] Ray actor consuming batches from Cosma.
- [ ] End-to-end demo and benchmark report.

## Do Not Build (Scope Guard)

- [ ] Do not replace Ray object store.
- [ ] Do not build a custom distributed object storage layer.
- [ ] Do not build a custom GPU execution engine.
- [ ] Do not overbuild general tensor algebra.
