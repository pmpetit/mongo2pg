## Context

Infer currently attempts one large `$sample` operation, then falls back to one very large sequential `find().limit(total)` when sampling fails on huge collections. For collections with tens of millions of documents, this can exceed server execution limits, produce repeated timeout failures, and create poor operational control even when `max_time_ms` is raised.

## Goals / Non-Goals

**Goals:**

- Process huge infer workloads in bounded chunks to avoid single long-running MongoDB operations.
- Introduce configurable chunk size for infer fallback reads with a practical default (for example 1,000,000 docs).
- Preserve existing inference output semantics while improving reliability and progress observability.

**Non-Goals:**

- Replace schema inference algorithm or analyzer internals.
- Guarantee fixed memory footprint across all collection/document shapes.
- Add distributed parallel infer execution in this change.

## Decisions

- Decision: Add chunked fallback read loop for infer after `$sample` failure.
  - Rationale: current single large fallback query is fragile on huge collections.
  - Alternative considered: only increase max_time_ms further; rejected because long single operations still fail unpredictably.
- Decision: Expose chunk size as configurable infer setting (CLI and/or config), with default around 1M docs.
  - Rationale: operators need tuning based on cluster limits and collection shape.
  - Alternative considered: fixed hardcoded chunk size; rejected due to differing deployment constraints.
- Decision: Keep max-time semantics per chunk operation.
  - Rationale: chunking should bound both workload size and query duration.
  - Alternative considered: disable max_time_ms on chunked reads; rejected due to potential runaway queries.
- Decision: Emit chunk progress logs with chunk index, processed count, and fallback reason.
  - Rationale: large runs need visibility and easier troubleshooting.

## Risks / Trade-offs

- [Risk] Chunking can increase total query overhead due to repeated round trips → Mitigation: configurable chunk size with sensible default and documented tuning.
- [Risk] Ordering of fallback reads may affect representativeness compared to random sample → Mitigation: preserve current fallback semantics and document this as degraded-but-reliable path.
- [Risk] Very small chunk sizes can slow inference significantly → Mitigation: enforce minimum/validated chunk size range.

## Migration Plan

- Add chunk-size configuration parsing and validation.
- Implement chunked fallback read loop in infer path while preserving analyzer behavior.
- Add progress/warning log lines for chunked mode and timeout context.
- Add tests for chunk configuration precedence and chunked fallback control flow.
- Rollback by disabling chunked loop and reverting to existing fallback path if needed.

## Open Questions

- Should chunking be used only for fallback mode, or also optionally for normal infer reads when `percent=100` on huge collections?
- Should future work include adaptive chunk sizing based on observed latency per chunk?
