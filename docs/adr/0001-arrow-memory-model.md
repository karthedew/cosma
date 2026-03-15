# ADR 0001: Arrow Memory Model

## Status

Accepted

## Context

Cosma is intended to be Arrow-native. The in-memory representation must be
compatible with Arrow buffers to enable zero-copy interoperability with Arrow
tools, readers, and downstream numerical systems.

## Decision

Cosma uses Apache Arrow as the canonical in-memory format. All core data
structures (DataFrame, Series, Column) are backed by Arrow arrays or chunked
arrays. Arrow schema is the single source of truth for types, nullability, and
metadata.

## Consequences

- All execution paths operate on Arrow arrays or Arrow record batches.
- No row-oriented core representation will be introduced.
- Interop with Arrow readers/writers and ADBC can be zero-copy when possible.
- Memory lifecycle must follow Arrow reference counting and Release semantics.
