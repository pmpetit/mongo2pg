## Context

`run_kafka_import` in `src/bin/mongo2pg.rs` currently performs message polling, decode/apply, and transaction/offset progress handling inside one large control loop. This structure preserves behavior but makes stage-level performance analysis difficult. Operators need a low-overhead way to inspect where time is spent (read vs write vs commit) during kafka-import tuning and incident debugging.

The change introduces stage-oriented function boundaries and tracing spans that can be exported in Chrome trace format. Existing counters, retry behavior, and import semantics must remain intact.

## Goals / Non-Goals

**Goals:**

- Introduce explicit stage functions for kafka-import message read, PostgreSQL write/apply, and offset commit.
- Add optional `tracing` + `tracing-chrome` instrumentation around these stages.
- Preserve existing kafka-import behavior, including transaction batching and summary counters.
- Provide trace metadata sufficient for bottleneck diagnosis (topic, op kind, batch size, stage outcome, duration).

**Non-Goals:**

- Redesign kafka-import business logic, mapping semantics, or Debezium payload handling.
- Change default logging format for all commands.
- Guarantee zero-overhead tracing when enabled.
- Introduce distributed tracing backends (OTLP/Jaeger) in this change.

## Decisions

1. Add a dedicated kafka-import trace session initializer.

- Decision: initialize a tracing subscriber only when kafka-import tracing is enabled by configuration/flag.
- Rationale: avoids behavior changes and overhead for default runs.
- Alternatives considered:
- Always-on tracing: rejected due to unnecessary runtime overhead and larger outputs.
- Ad-hoc timing logs only: rejected because Chrome trace visualization is the explicit requirement.

1. Refactor loop into stage functions with stable interfaces.

- Decision: extract function boundaries for `read_message`, `write_to_pg`, and `commit_offset` responsibilities while keeping orchestration in `run_kafka_import`.
- Rationale: creates deterministic instrumentation points and improves testability without broad architecture churn.
- Alternatives considered:
- Keep monolith and add inline spans: rejected because stage ownership remains unclear and hard to maintain.
- Full state-machine rewrite: rejected as too large for this scoped observability change.

1. Instrument stages with structured spans.

- Decision: use `tracing::instrument` or explicit spans per stage, recording key fields (`worker`, `topic`, `op`, `batch_size`, `rows`, `result`).
- Rationale: enables consistent timeline analysis in trace viewer and stable dimensions for later alerts/metrics mapping.
- Alternatives considered:
- Text-only log markers: rejected because timing correlation across concurrent activity is weaker.

1. Preserve transaction batching semantics.

- Decision: stage extraction will not alter current commit/rollback thresholds; only encapsulate the existing logic.
- Rationale: keeps operational risk low and avoids regressions in throughput behavior.

## Risks / Trade-offs

- Trace output growth under high throughput -> Mitigation: make tracing opt-in and document expected artifact size.
- Additional function boundaries may complicate ownership of mutable counters -> Mitigation: define small context structs for mutable stage state and keep update points explicit.
- Subscriber lifecycle conflicts if global subscriber already set -> Mitigation: use scoped initialization strategy compatible with current command runtime and fail safely with clear warning.
- Minor runtime overhead when tracing enabled -> Mitigation: keep spans focused on stage boundaries, avoid per-field deep serialization.

## Migration Plan

- Add dependencies and minimal tracing bootstrap for kafka-import command path.
- Introduce stage functions and wire orchestration to call them without changing behavior.
- Add/adjust tests for stage-level behavior and no-regression flow.
- Document how to enable tracing and where Chrome trace artifacts are written.
- Rollback strategy: disable tracing switch and run legacy behavior path (same command flow, tracing disabled).

## Open Questions

- What is the preferred config surface for enabling trace output (new kafka-import flag vs config key)?
- Should trace files rotate automatically for long-running stream mode?
- Do we need a default trace output path convention under `results/` or temp directory?
