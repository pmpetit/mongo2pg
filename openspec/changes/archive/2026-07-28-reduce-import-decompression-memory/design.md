## Context

`mongo2pg import` currently discovers CSV artifacts, begins a transaction, truncates target tables, then loads data via PostgreSQL COPY. For `.csv.gz` artifacts, decompression occurs in a memory-expensive way that can hold large intermediate payloads, causing OOM kills for large datasets.

## Goals / Non-Goals

**Goals:**

- Keep import memory bounded independently of compressed file size.
- Preserve current import compatibility with existing export artifacts and table mapping.
- Preserve import transactional semantics and observable stage transitions.

**Non-Goals:**

- Changing CSV schema, table naming, or export file conventions.
- Introducing a new user-facing CLI flag for decompression strategy.
- Reworking full import orchestration beyond decompression/COPY ingestion path.

## Decisions

1. Use streaming gzip decompression for `.csv.gz` inputs.

- Decision: Replace full materialization with `GzDecoder`-backed streaming reader and feed chunks directly into COPY writer.
- Rationale: Removes file-size-correlated memory spikes while keeping file format support unchanged.
- Alternative considered: Temporary decompressed files on disk; rejected because it adds I/O overhead and storage pressure.

1. Enforce bounded read/write buffers.

- Decision: Read compressed input in fixed-size chunks and forward uncompressed bytes incrementally.
- Rationale: Predictable RSS and easier operational sizing.
- Alternative considered: Adaptive dynamic buffers; rejected due to complexity with limited benefit.

1. Keep same import behavior contract.

- Decision: No changes to truncate order, table resolution, or transaction boundaries.
- Rationale: Minimizes regression risk and preserves compatibility with existing grouped/non-grouped exports.

1. Add decompression-phase debug telemetry.

- Decision: Emit stage markers for `copy_decompress_begin`, stream progress checkpoints, and stream completion.
- Rationale: Improves diagnosis of memory/performance issues without changing user-facing semantics.

## Risks / Trade-offs

- [Risk] Streaming path may reduce throughput in some environments. -> Mitigation: tune chunk size and keep efficient buffered I/O.
- [Risk] COPY stream interruption can leave partial table state inside transaction. -> Mitigation: keep existing transaction rollback-on-error behavior.
- [Risk] Edge-case gzip corruption handling differs from previous path. -> Mitigation: preserve categorized error propagation and add corrupt-file test.
- [Risk] Additional debug logs may be noisy at debug level. -> Mitigation: keep logs scoped to import debug namespace and phase boundaries.

## Migration Plan

1. Implement streaming decompression in import COPY ingestion code path.
2. Keep fallback for plain `.csv` files unchanged.
3. Run import regression tests (grouped and non-grouped tables).
4. Validate large `.csv.gz` import on constrained memory runner.
5. Rollback strategy: revert to previous import ingestion implementation if critical regressions emerge.

## Open Questions

- Should chunk size be internal constant or configurable via existing import config?
- Do we need explicit decompression throughput metrics in report output, or are debug logs sufficient?
