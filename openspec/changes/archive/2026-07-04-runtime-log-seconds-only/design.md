## Context

The CLI runtime logger currently emits timestamps with fractional seconds and timezone offset, and elapsed time with millisecond precision. For routine operator usage, this precision is unnecessary and makes log scanning noisier.

## Goals / Non-Goals

**Goals:**
- Define a stable runtime log formatting contract with second precision timestamp.
- Define elapsed output contract as whole seconds only.
- Keep change localized to logger formatting and its tests.

**Non-Goals:**
- No change to log level routing, filtering, or log destinations.
- No structured logging migration.
- No change to unrelated report/export/import output.

## Decisions

- Keep existing log line shape `<timestamp> +<elapsed>s [<LEVEL>] <message>` and only change precision.
Rationale: Minimizes downstream impact while improving readability.
Alternative considered: introducing a fully new log format. Rejected due to compatibility churn.

- Format timestamp with `%Y-%m-%dT%H:%M:%S` in UTC context already used by runtime logger.
Rationale: Explicit second precision and deterministic width.
Alternative considered: retaining RFC3339 with suppressed subseconds. Rejected because offset suffix still adds unnecessary noise.

- Render elapsed via integer seconds (`Duration::as_secs()`).
Rationale: Matches requirement to keep only seconds and avoids fractional drift in assertions.
Alternative considered: rounded floating-point seconds. Rejected because output still implies fractional precision.

## Risks / Trade-offs

- [Risk] Existing log-parsing scripts may expect fractional elapsed values.
  - Mitigation: Keep token positions unchanged and update docs/tests to codify the new contract.
- [Trade-off] Loss of sub-second diagnostic precision.
  - Mitigation: Debug-level tracing remains available via message content when needed.
