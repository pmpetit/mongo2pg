## Context

mongo2pg currently initializes and uses MongoDB, PostgreSQL, and Kafka in different command paths (`infer`, `export`, `import`, `report --post-import`, `kafka-import`). Failures are often propagated with low-level driver messages that do not consistently identify which backend connection failed first. Users running mixed workflows can misdiagnose incidents and spend time checking the wrong dependency.

## Goals / Non-Goals

**Goals:**

- Ensure connection failures clearly identify backend (`mongo`, `pg`, `kafka`) and operation context.
- Keep message format consistent across command paths.
- Preserve existing control flow and exit behavior while adding diagnostic context.
- Cover both initial connect failures and first-use failures where connect is lazy.

**Non-Goals:**

- Rework retry policy or circuit-breaker behavior.
- Redesign all runtime logging format beyond needed backend attribution.
- Normalize every non-connection error category.

## Decisions

1. Introduce backend-specific error context wrappers at connection boundaries.

- Decision: add/standardize `anyhow::Context` messages at each backend acquisition/use boundary with explicit backend tag and operation.
- Rationale: minimal invasive change, keeps root cause chain from driver while adding deterministic top-level attribution.
- Alternative: central error enum refactor. Rejected for larger scope and slower delivery.

1. Standardize message shape for operator clarity.

- Decision: use a stable phrasing pattern such as `connection_failed backend=<mongo|pg|kafka> operation=<...>` in user-visible top-level errors/log lines.
- Rationale: easy grep/alerting and consistent troubleshooting guidance.
- Alternative: free-form text per call site. Rejected due to drift and ambiguity.

1. Apply attribution across command entrypoints, not only helper internals.

- Decision: verify each command path that touches backend connectivity emits attributed errors (infer/export/import/report/kafka-import).
- Rationale: avoids partial coverage where some commands still emit ambiguous failures.

## Risks / Trade-offs

- [Risk] Over-wrapping may duplicate context and create noisy chains. -> Mitigation: add one wrapper at command/backend boundary only; avoid stacking equivalent messages.
- [Risk] Some driver errors occur after successful connect and may be misread as connect-only. -> Mitigation: use `operation=<connect|query|copy|produce|consume>` field to disambiguate.
- [Risk] Existing tests may assert exact old strings. -> Mitigation: update assertions to match new stable prefixes and preserve error source chain.

## Migration Plan

- Implement wrappers and message standardization in targeted paths.
- Add/adjust tests for mongo/pg/kafka failure attribution.
- Validate CLI output manually with unreachable endpoints for each backend.
- Rollback strategy: revert wrapper additions if unexpected side effects appear; core behavior remains unchanged.

## Open Questions

- Should we include actionable hints per backend in message text (for example TLS/auth/host checks), or keep that for docs only?
- Do we want a machine-readable error code field in addition to message text in a later change?
