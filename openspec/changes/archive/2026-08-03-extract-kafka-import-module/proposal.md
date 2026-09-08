## Why

`run_kafka_import` in `mongo2pg.rs` has grown large and mixes orchestration, transport, decode, write, tracing, and error-handling concerns in one place. Extracting kafka-import logic into a dedicated module improves maintainability, testability, and review velocity without changing user-facing behavior.

## What Changes

- Introduce a dedicated kafka-import module file (`src/bin/mongo2pg/kafka_import.rs`) and move kafka-import runtime logic out of `mongo2pg.rs`.
- Keep CLI behavior, flags, progress logging shape, trace behavior, and failure semantics unchanged.
- Define clear module boundaries for kafka-import subroutines (config prep, consume loop, write/commit stages, trace lifecycle, fallback/DLQ handling).
- Keep existing compatibility with snapshot-mode COPY batching, fallback replay, and ad hoc DLQ behavior.
- Add/adjust tests to verify behavior parity after extraction.

## Capabilities

### New Capabilities

- `kafka-import-module-extraction`: Defines structural requirements for isolating kafka-import runtime implementation into a dedicated module while preserving behavior.

### Modified Capabilities

- `kafka-import-stage-tracing`: Preserve and verify stage tracing semantics after module extraction.
- `kafka-import-copy-bulk-fallback`: Preserve and verify snapshot COPY/fallback/DLQ semantics after module extraction.

## Impact

- Affected code: `src/bin/mongo2pg.rs` plus extracted module `src/bin/mongo2pg/kafka_import.rs`.
- Affected tests: kafka-import focused tests and newly added extraction-parity checks.
- APIs: no user-facing CLI/API change expected.
- Operational impact: reduced implementation complexity and improved maintainability for future kafka-import changes.
