# Package Guide

| Package | Audience | Stability | Notes |
| --- | --- | --- | --- |
| dataframe | Public | Evolving public API | Main user entry point |
| scan | Public | Evolving public API | Streaming readers |
| schema | Public | Evolving public API | Schema helpers/types |
| plan | Public | Experimental public API | Logical planning surface |
| compute | Public | Evolving public API | Numeric export/transform |
| operator | Repo-visible | Unstable, not an extension API | May move/internalize later |
| internal/exec | Internal | Internal only | Runtime/compiler/executor |
| internal/expr | Internal | Internal only | Canonical expression engine |
| internal/stream | Internal | Internal only | Reader adapters/stream glue |
| expr | Deprecated | Legacy / remove | Replace with internal/expr |

## Public API Expectations

Cosma exposes a small set of public packages intended for end users. These
packages are evolving and may change as the execution engine and expression
system mature. The public API is centered on `dataframe` and `scan`, with
`plan` providing an experimental planning surface and `compute` focused on
numerical export and transformations.

## Internal Packages

The execution engine and expression system are internal-only. Packages under
`internal/` are not stable and should not be imported by external users. These
packages can change freely while the engine evolves.

## Packages Likely to Move

The `operator` package is repo-visible but not a stable extension point. It may
move under `internal/` once the execution model stabilizes or become a
documented public extension surface later. The legacy `expr` package is
deprecated and will be removed after migrations complete.
