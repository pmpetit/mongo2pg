## Context

Inference currently attempts high-volume sampling (`$sample`) when users set `percent = 100.0` on large collections. For very large namespaces (example: ~44M docs), server-side aggregation can exceed MongoDB operation time limits and return `MaxTimeMSExpired`, which triggers fallback behavior after expensive work. Config already includes `source.max_time_ms`, but infer paths do not reliably apply it to all sample/fallback reads.

## Goals / Non-Goals

**Goals:**

- Apply one consistent timeout budget from `source.max_time_ms` across infer sampling reads.
- Ensure timeout handling is explicit in logs/warnings and still preserves current fallback path.
- Keep behavior backward-compatible when `max_time_ms` is not set.

**Non-Goals:**

- Redesign infer strategy (sampling algorithm, chunking, or adaptive sizing).
- Introduce retry backoff policy changes beyond existing fallback.
- Add new config keys beyond existing `max_time_ms`.

## Decisions

- Decision: Treat `source.max_time_ms` as infer read timeout source of truth.
  - Rationale: existing user-facing config already expresses intended MongoDB query budget.
  - Alternative considered: introduce dedicated infer timeout key; rejected to avoid config sprawl.
- Decision: Apply timeout to both `$sample` aggregate and sequential fallback `find().limit(...)` paths.
  - Rationale: both operations are part of same infer stage and should follow one SLA.
  - Alternative considered: limit timeout to `$sample` only; rejected because fallback can still run unbounded on large collections.
- Decision: Keep fallback-on-sample-failure behavior, but include timeout reason context.
  - Rationale: preserves resilience while improving operator observability.
  - Alternative considered: hard-fail infer on timeout; rejected due to poorer usability on large datasets.

## Risks / Trade-offs

- Timeout too low may increase fallback frequency or partial inference quality → Mitigation: clear warning text references configured value and collection scope.
- Applying timeout to fallback may cause earlier termination in constrained clusters → Mitigation: default remains unchanged unless user sets `max_time_ms`.
- Different MongoDB commands can surface timeout variants (`MaxTimeMSExpired`, command code 50) → Mitigation: handle by command code/class, not fragile message text.

## Migration Plan

- No data migration required.
- Rollout by releasing code update; existing configs with `max_time_ms` immediately gain infer-time enforcement.
- Rollback by reverting change; behavior returns to current infer timeout handling.

## Open Questions

- Should future versions expose separate infer timeout override (only if users report need for distinct extraction vs infer budgets)?
