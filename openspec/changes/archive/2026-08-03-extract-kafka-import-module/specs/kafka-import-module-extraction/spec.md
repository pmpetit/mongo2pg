## ADDED Requirements

### Requirement: Kafka-import runtime is isolated in a dedicated module

The kafka-import runtime implementation SHALL be moved out of `mongo2pg.rs` into a dedicated Rust module file (`src/bin/mongo2pg/kafka_import.rs`), while preserving command entrypoint behavior.

#### Scenario: CLI invocation still routes to kafka-import runtime

- **WHEN** `mongo2pg kafka-import` is executed
- **THEN** command dispatch invokes the extracted kafka-import module entrypoint and returns equivalent success/failure outcomes

### Requirement: Extracted module preserves behavior parity

The extracted kafka-import module MUST preserve existing processing semantics for consume/decode/apply/commit flow, including existing counters and control-flow decisions.

#### Scenario: Message processing parity

- **WHEN** kafka-import processes comparable message streams before and after extraction
- **THEN** stage outcomes and summary counters remain behaviorally equivalent

### Requirement: Extracted module defines explicit internal boundaries

The extracted module SHALL define explicit internal functions/types for stage orchestration to keep `mongo2pg.rs` focused on CLI composition.

#### Scenario: Stage boundaries present after extraction

- **WHEN** reviewing extracted module code
- **THEN** read, write/apply, commit, trace lifecycle, and fallback/DLQ logic are organized as dedicated functions or subcomponents instead of one monolithic routine
