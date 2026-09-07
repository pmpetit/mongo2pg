## Context

`run_kafka_import` in `src/bin/mongo2pg.rs` currently combines configuration normalization, Kafka consumer setup, PostgreSQL writer flow, stage tracing, snapshot COPY/fallback behavior, and operational counters in a single, large function body. Recent feature growth (stage tracing and COPY/fallback logic) increased complexity and review risk for any future kafka-import changes.

The requested change is structural: move kafka-import runtime logic to a dedicated module (`src/bin/mongo2pg/kafka_import.rs`) while preserving command behavior and existing runtime semantics.

## Goals / Non-Goals

**Goals:**

- Extract kafka-import runtime implementation from `mongo2pg.rs` into a dedicated module.
- Keep CLI dispatch, options, logging contract, and behavior parity intact.
- Keep tracing stage behavior and snapshot COPY/fallback/DLQ semantics unchanged.
- Improve internal boundaries to simplify maintenance and testing.

**Non-Goals:**

- Redesign kafka-import runtime behavior.
- Change user-facing CLI arguments or config schema.
- Introduce new runtime dependencies.

## Decisions

1. Extract by module boundary, not by behavior rewrite.

- Decision: Move existing logic into a module-oriented structure with minimal semantic change.
- Rationale: Reduces migration risk and preserves current operational behavior.
- Alternative considered: Full rewrite into a new architecture. Rejected due to risk and timeline.

1. Keep `run_kafka_import` as thin orchestration in `mongo2pg.rs`.

- Decision: `mongo2pg.rs` retains command parsing/dispatch and delegates to module entrypoint.
- Rationale: Improves readability of main CLI file and isolates kafka domain concerns.

1. Preserve internal stage boundaries and helper functions.

- Decision: Keep extracted helpers for read/write/commit/tracing/fallback flows in module scope.
- Rationale: Existing stage separations map naturally to maintainable module APIs.

1. Validate behavior parity with focused tests.

- Decision: Re-run and extend kafka-import-targeted tests after extraction.
- Rationale: Structural refactors need explicit parity verification to avoid subtle regressions.

## Risks / Trade-offs

- [Accidental semantic drift during extraction] -> Mitigation: keep refactor mechanical, preserve function signatures/logic where possible, and validate with targeted tests.
- [Import/path churn in binary module] -> Mitigation: stage extraction with incremental compile checks and explicit module exports.
- [Trace or COPY fallback regressions] -> Mitigation: preserve dedicated tests and inspect key counters/log fields in parity checks.

## Migration Plan

1. Create kafka-import module file and move kafka-import specific types/helpers/functions.
2. Update `mongo2pg.rs` to import module and delegate `run_kafka_import` handling.
3. Resolve compile/link issues from moved symbols by adjusting visibility and imports.
4. Run targeted builds/tests and kafka-import-focused tests.
5. Rollback plan: if regressions appear, revert to previous `mongo2pg.rs` placement and reattempt extraction in smaller slices.

## Open Questions

- Should extracted module remain a sibling file under `src/bin/` or move into a shared library module under `src/` for reuse?
- Do we want a strict internal API boundary (private module internals with one public entrypoint) now, or defer until later cleanups?
