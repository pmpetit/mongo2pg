## Context

The current report command supports --post-import usage during checkmd5-oriented workflows, but there is no explicit trace envelope indicating when post-import reporting starts, key milestones, and completion/failure boundaries. Operators troubleshooting checksum mismatches or report latency cannot quickly correlate report activity with checkmd5 execution.

Existing telemetry/reporting capability already captures collection-level read-operation data. This change adds operational trace visibility for the report --post-import execution path without changing report data model contracts.

## Goals / Non-Goals

**Goals:**

- Emit explicit trace/log events when report runs with --post-import in checkmd5 workflows.
- Provide stable stage markers (start, data load, checkmd5 stage, summary, finish/error) for easier troubleshooting.
- Keep behavior backward-compatible for report runs without --post-import.

**Non-Goals:**

- No redesign of report output formats.
- No change to checkmd5 algorithms or checksum semantics.
- No new external dependencies or tracing backends.

## Decisions

1. Add a lightweight trace context for report --post-import flow using existing logging/tracing primitives.

- Rationale: Reuses current runtime/logging model and avoids introducing new observability infrastructure.
- Alternative considered: Dedicated structured telemetry file for post-import runs. Rejected as excessive for current scope.

1. Gate detailed post-import trace messages behind existing runtime verbosity controls while keeping key lifecycle markers always visible.

- Rationale: Preserves operator signal while preventing noisy default logs.
- Alternative considered: Always-on detailed logs. Rejected due to verbosity risk.

1. Place trace markers at command-level orchestration boundaries instead of deep per-record internals.

- Rationale: Keeps traces stable and meaningful across implementation changes.
- Alternative considered: Per-collection granular tracing only. Rejected because it misses command lifecycle insight.

## Risks / Trade-offs

- [Risk] Added log events may still increase output volume in large runs.
  → Mitigation: Keep mandatory markers concise and route verbose details to debug/trace levels.

- [Risk] Inconsistent stage naming across code paths can reduce usefulness.
  → Mitigation: Define canonical stage names and reuse constants/helpers.

- [Risk] Error-path trace omissions can leave partial observability.
  → Mitigation: Add explicit failure marker in all early-return/error branches for --post-import flow.

## Migration Plan

- Implement trace markers in report command orchestration around --post-import branch.
- Run existing report/checkmd5 tests and targeted post-import scenarios.
- Validate that non---post-import runs produce unchanged functional outputs.
- Rollback strategy: remove new trace markers if unexpected noise or regressions occur (no data migration required).

## Open Questions

- Should stage markers include elapsed duration per stage or only command total duration in this change?
- Should a final post-import trace summary be duplicated into generated report artifacts, or remain runtime logs only?
