## Context

`mongo2pg export` currently accumulates large in-memory row structures before writing final CSV outputs. On high-volume collections in Kubernetes, memory can exceed 8GiB and trigger OOMKilled restarts. Grouped exports can worsen peak usage because multiple source collections feed shared target tables.

## Goals / Non-Goals

**Goals:**

- Bound memory usage during export by processing and writing rows in chunks.
- Release chunk memory after each flush while preserving output correctness.
- Keep grouped and non-grouped export semantics unchanged.
- Provide configurable chunk size with safe defaults.

**Non-Goals:**

- Redesign mapping format or SQL schema generation.
- Change exported CSV format or import contract.
- Introduce external streaming systems.

## Decisions

1. Add chunked write pipeline in export path

- Decision: Replace global all-rows accumulation with chunk-sized table buffers that flush to CSV incrementally.
- Rationale: Peak memory becomes proportional to chunk size instead of full collection size.
- Alternative considered: Keep current accumulation and increase pod memory. Rejected because it is costly and unstable for very large datasets.

1. Use append-capable CSV.gz writer lifecycle per table

- Decision: Open/maintain per-table writer handles and append chunks as they are produced.
- Rationale: Avoid reloading full data structures and avoid repeated recompression churn.
- Alternative considered: Write temporary uncompressed files then gzip at end. Rejected due to disk overhead and extra post-processing.

1. Preserve grouped export correctness with chunking

- Decision: Keep grouped SQL/table resolution logic unchanged and apply chunk flush over the same resolved target table outputs.
- Rationale: Maintains existing grouped behavior and compatibility guarantees.
- Alternative considered: Separate grouped path with unique buffering logic. Rejected to reduce divergence and bugs.

1. Introduce configurable chunk size

- Decision: Add export chunk-size setting/flag with validated minimum/maximum bounds and a conservative default.
- Rationale: Operators can tune for pod memory and throughput constraints.
- Alternative considered: Hardcoded chunk size only. Rejected because workloads vary significantly.

## Risks / Trade-offs

- Risk: More frequent flushes may reduce throughput. -> Mitigation: tune chunk size and batch writes per table.
- Risk: Append writer lifecycle bugs can corrupt output if not finalized. -> Mitigation: explicit writer finalization and integration tests.
- Risk: Grouped multi-source ordering differences under chunking. -> Mitigation: preserve deterministic processing order and verify merged row counts.
- Risk: Small chunk size increases I/O overhead. -> Mitigation: safe default plus user override.

## Migration Plan

- Implement chunk buffer + flush mechanism behind export flow.
- Integrate chunking with grouped and non-grouped export branches.
- Add tests for chunked grouped outputs and legacy behavior parity.
- Roll out with default chunk size tuned for low memory pressure.
- Rollback: disable chunked path and revert to legacy accumulator if critical regressions occur.

## Open Questions

- Best default chunk size balancing memory safety and throughput for common clusters.
- Whether chunk size should be exposed only as CLI option or also persisted in config.
