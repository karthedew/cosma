# ADR 0002: Streaming Execution Boundary

## Status

Accepted

## Context

Cosma must support datasets larger than memory and enable pipeline execution
without requiring full materialization into a DataFrame.

## Decision

Cosma adopts Arrow record batches as the unit of work for execution. Streaming
interfaces will mirror Arrow's RecordReader shape:

- Schema() *arrow.Schema
- Next() bool
- Record() arrow.Record
- Err() error
- Release()

This interface is internal until the streaming engine is shipped in Phase 2.

## Consequences

- Data ingestion and operators can execute batch-by-batch without full loads.
- Ownership rules follow Arrow conventions: creators release records they own.
- DataFrame materialization becomes a sink over record batch streams.
