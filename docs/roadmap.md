# Cosma Roadmap

A development roadmap for Cosma, a Go-based Arrow-native dataframe and data processing
engine designed for high-performance ingestion, transformation, and analytics.

Cosma in the ecosystem:

Storage -> Cosma -> Numerical Compute / ML -> Applications

Example stack:

Cassandra / Parquet / Zarr
-> Cosma (DataFrame + Query Engine)
-> Gonum / ML frameworks
-> APIs / Analytics / ML

Cosma focuses on:

- Columnar data processing
- Parallel execution
- Streaming transformations
- Arrow interoperability
- Data preparation for numerical computing

## Phase 1 - Core DataFrame Foundation (Current Stage)

Goal: Establish a stable in-memory dataframe model built on Apache Arrow.

Core data structures:

```go
type DataFrame struct {
    Schema  *arrow.Schema
    Columns []*Series
    Height  int
}

type Series struct {
    Name string
    Data *arrow.Chunked
}
```

Core components:

- DataFrame
- Series
- ChunkedColumn
- Schema
- Column interface abstraction

Key features:

- Arrow schema support
- Chunked column arrays
- Dtype mapping to Arrow
- Dataframe construction from series
- Metadata support

Core API:

- NewDataFrame
- NewSeries
- Select
- AddColumn
- Shape
- Schema

Supported input formats:

- CSV
- Parquet
- Arrow record batches

Outcome: Cosma can construct and manipulate columnar tables.

## Phase 2 - Record Batch Streaming Engine

Goal: Move from in-memory tables to a streaming batch engine.

Core abstraction:

```go
type RecordBatchStream interface {
    Next() (*arrow.Record, error)
}
```

Pipeline architecture:

Scan -> RecordBatch -> Operator -> RecordBatch -> Sink

Implement scan operators:

- ScanCSV
- ScanParquet
- ScanArrow

Streaming operators:

- Filter
- Projection
- Limit
- Map

Benefits:

- Datasets larger than memory
- Pipeline execution
- Foundation for parallelism

Outcome: Cosma becomes a streaming dataframe engine.

## Phase 3 - Parallel Execution Engine

Goal: Enable multi-core execution.

Execution model:

Logical Plan
  -> Physical Plan
  -> Parallel Operators

Data partitioning:

Dataset
  -> Partitions
  -> Worker pipelines
  -> Merge results

Parallelizable operators:

- Filter
- Projection
- Map
- Aggregation

Outcome: Cosma achieves high-throughput data processing.

## Phase 4 - Query Operators

Goal: Support dataframe transformations similar to analytical engines.

Core operations:

- Select
- Filter
- Sort
- Limit

Group operations:

- GroupBy
- Aggregate
- Window functions

Join operations:

- Hash Join
- Merge Join

Aggregation functions:

- Sum
- Mean
- Count
- Min
- Max
- Variance

Example API:

```go
df.
  Filter("region = 'US'").
  GroupBy("day").
  Agg(mean("price"), sum("volume"))
```

Outcome: Cosma becomes a functional dataframe query engine.

## Phase 5 - Lazy Query Engine

Goal: Introduce query planning and optimization.

Architecture:

Logical Plan
  -> Optimizer
  -> Physical Plan
  -> Execution

Optimization techniques:

- Predicate pushdown
- Projection pushdown
- Scan pruning
- Operator fusion

Example optimization:

Before:

Scan -> Filter -> Select -> Aggregate

After:

Scan(filtered_columns)
  -> Aggregate

Outcome: Cosma supports query planning and optimization.

## Phase 6 - Numerical Computing Integration

Integration with Gonum for mathematical kernels.

Purpose:

Cosma processes large datasets while Gonum performs dense numerical computation.

Pipeline:

Large Dataset
  -> Cosma Query
  -> Feature Matrix
  -> Gonum Algorithms

Example API:

```go
matrix := df.ToDenseMatrix()
```

Possible algorithms:

- Regression
- PCA
- Covariance
- Clustering

Outcome: Cosma becomes a data preparation layer for numerical computing.

## Phase 7 - Data Connectivity Layer

Goal: Enable Cosma to read from multiple storage systems.

Supported sources:

- Cassandra
- DuckDB
- ClickHouse
- InfluxDB
- Parquet
- Zarr
- HDF5

Database interoperability via Apache Arrow Database Connectivity (ADBC).

Architecture:

Database
  -> Arrow Record Batches
  -> Cosma

Benefits:

- Zero-copy data transfer
- Efficient columnar ingestion

Outcome: Cosma becomes a universal Arrow ingestion layer.

## Phase 8 - Vector Data Ecosystem

Integration with vector databases such as Weaviate.

Cosma roles:

Embedding ETL
Documents
  -> Cosma preprocessing
  -> Embedding model
  -> Vector database

Vector Analytics
Vector Search Results
  -> Cosma DataFrame
  -> Analysis / Clustering

Potential features:

- Vector column datatype
- Embedding pipelines
- Batch vector ingestion

Outcome: Cosma supports vector data workflows.

## Phase 9 - Distributed Compute (Long Term)

Future expansion into distributed execution.

Possible integrations:

- Ray
- Arrow Flight
- Distributed worker nodes

Architecture:

Scan Node
  -> Compute Nodes
  -> Aggregation Node

Outcome: Cosma evolves into a distributed analytics engine.

## Phase 10 - Developer Ecosystem

Improve developer experience and adoption.

Deliverables:

- Go SDK
- Python bindings
- CLI tools
- Query DSL
- Documentation
- Performance benchmarks

Example CLI:

cosma query file.parquet "SELECT avg(price) GROUP BY day"

Outcome: Cosma becomes a production-ready data platform component.

## Final Vision

Cosma becomes a Go-native Arrow data processing engine positioned between
storage systems and compute frameworks.

Architecture:

Storage (Cassandra / Parquet / Zarr)
  -> Cosma (DataFrame + Query Engine)
  -> Math / ML (Gonum / ML frameworks)
  -> Applications (APIs / analytics / ML)

Simple mental model:

- Cosma: Data Processing Engine
- Gonum: Numerical Computing Engine
- Vector DB: Semantic Search Layer

Long-term role: parallel Arrow-based ingestion and transformation engine for the
Go ecosystem.
