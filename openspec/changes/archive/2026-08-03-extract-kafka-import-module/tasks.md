## 1. Module Extraction Setup

- [x] 1.1 Create dedicated kafka-import module file and wire it into binary module graph without changing CLI command parsing.
- [x] 1.2 Move kafka-import-local types/enums/helpers from `mongo2pg.rs` into the new module with minimal semantic change.
- [x] 1.3 Expose a single module entrypoint for kafka-import execution and delegate from command dispatch.

## 2. Core Runtime Refactor

- [x] 2.1 Move `run_kafka_import` orchestration flow into module while preserving existing consume/decode/apply/commit control flow.
- [x] 2.2 Keep snapshot COPY batching, fallback replay, and ad hoc DLQ handling behavior unchanged during extraction.
- [x] 2.3 Keep trace lifecycle and stage instrumentation behavior unchanged after extraction.

## 3. Visibility and Dependency Cleanup

- [x] 3.1 Resolve imports, visibility, and shared utility access between `mongo2pg.rs` and kafka-import module.
- [x] 3.2 Remove obsolete kafka-import code from `mongo2pg.rs` once delegation is complete.
- [x] 3.3 Ensure module boundaries are clear and avoid exporting unnecessary internal symbols.

## 4. Parity Validation

- [x] 4.1 Update/add tests to verify stage tracing parity after extraction.
- [x] 4.2 Update/add tests to verify COPY/fallback/DLQ behavior parity after extraction.
- [x] 4.3 Run targeted build and kafka-import-related tests; fix regressions until parity is restored.

## 5. Developer Guidance

- [x] 5.1 Document new kafka-import module location and ownership boundaries for maintainers.
- [x] 5.2 Document any test entrypoints/checklist used to validate extraction parity.
