## Context

Chunked infer fallback currently processes large samples through repeated `find().skip().limit()` windows. In some environments, the initial `find` command succeeds but later `getMore` fails with `Unauthorized` (MongoDB code 13), which aborts chunk processing and produces incomplete inference. The implementation needs deterministic behavior when this transient/session-related auth failure appears mid-stream, while preserving safety for genuine credential misconfiguration.

## Goals / Non-Goals

**Goals:**

- Make chunked infer fallback resilient to `Unauthorized` errors that occur during cursor iteration (`getMore`).
- Keep progression deterministic by retrying from a stable boundary (`processed` docs), never double-counting documents in the sampled target.
- Bound retries to avoid infinite loops and provide explicit terminal errors.
- Emit parseable diagnostics for each auth retry and terminal outcome.

**Non-Goals:**

- Changing global authentication setup, connection-string semantics, or MongoDB driver internals.
- Guaranteeing exactly-once document processing across concurrent source mutations.
- Adding new user-facing commands beyond minimal config/CLI knobs needed for retry policy.

## Decisions

1. Retry only at chunk boundaries

- Decision: If `Unauthorized` occurs while reading a chunk cursor, discard that cursor and retry the chunk using a new `find` from the same `processed` offset.
- Rationale: chunk boundary retry is simple, bounded, and avoids partial-cursor continuation risks.
- Alternative considered: retry from in-chunk document index. Rejected due to increased bookkeeping complexity and unstable ordering without explicit sort.

1. Add bounded auth-retry budget for infer chunk fallback

- Decision: Introduce configurable `auth_retry_max` (default small value like 2 or 3) and enforce fail-fast when exhausted.
- Rationale: distinguishes transient auth/session glitches from persistent credential errors and prevents endless looping.
- Alternative considered: unlimited retries with backoff. Rejected because it can hide persistent misconfiguration and stall runs.

1. Keep retry classification strict

- Decision: Retry only when error matches MongoDB code 13 or canonical unauthorized markers during cursor iteration; all other errors preserve existing behavior.
- Rationale: prevents accidental retry of unrelated failures and keeps semantics predictable.
- Alternative considered: retry all chunk cursor errors. Rejected because it may mask data or network issues requiring immediate visibility.

1. Structured operational logs

- Decision: Emit stable log keys for namespace, chunk index, processed count, retry attempt, and final disposition.
- Rationale: makes incident diagnosis and automation parsing reliable.
- Alternative considered: free-form human logs only. Rejected due to poor machine parsing and troubleshooting latency.

## Risks / Trade-offs

- [Risk] Retrying a chunk with skip/limit on mutable collections can read slightly different documents. → Mitigation: keep behavior best-effort, document non-goal of strict snapshot consistency, and preserve bounded sample target.
- [Risk] Repeated unauthorized errors may still fail inference for some collections. → Mitigation: explicit terminal error with actionable context and retry counts.
- [Risk] Additional retry knobs increase configuration surface. → Mitigation: sensible defaults and strict validation.

## Migration Plan

- Implement retry policy in infer chunk fallback path.
- Add tests for unauthorized getMore retry success and retry exhaustion.
- Release with defaults that preserve current behavior unless unauthorized is observed.
- Rollback strategy: disable retry by setting retry max to 0 (if exposed) or revert change; no data migration needed.

## Open Questions

- Should `auth_retry_max` be exposed in CLI immediately or start as config-only to reduce CLI surface?
- Should retry include small jitter/backoff for environments with short token refresh windows?
- Do we want a per-collection fail mode (`skip` vs `hard-fail`) when auth retries are exhausted?
