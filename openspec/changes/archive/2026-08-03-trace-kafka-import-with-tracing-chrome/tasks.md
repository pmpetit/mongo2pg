## 1. Tracing Setup

- [x] 1.1 Add `tracing` and `tracing-chrome` dependencies in `Cargo.toml` for kafka-import runtime instrumentation
- [x] 1.2 Add kafka-import trace configuration surface (enable flag and output path behavior)
- [x] 1.3 Implement scoped trace-session bootstrap/teardown for kafka-import command lifecycle

## 2. Kafka-Import Stage Refactor

- [x] 2.1 Extract message-read stage function from kafka-import loop (topic read + timeout handling)
- [x] 2.2 Extract PostgreSQL write/apply stage function from kafka-import loop (decode/apply + counters)
- [x] 2.3 Extract offset-commit stage function from kafka-import loop (transaction/commit boundaries)
- [x] 2.4 Keep existing batching and rollback semantics unchanged while routing through stage functions

## 3. Stage Instrumentation

- [x] 3.1 Add read stage span with worker/topic/batch metadata and success/failure outcome
- [x] 3.2 Add write stage span with op kind, affected rows, and failure context metadata
- [x] 3.3 Add commit stage span with batch size, commit result, and rollback correlation metadata

## 4. Validation and Regression Safety

- [x] 4.1 Add or update tests for stage-function behavior parity with current kafka-import flow
- [x] 4.2 Add validation for tracing-enabled run producing Chrome trace artifact
- [x] 4.3 Run targeted kafka-import related tests and fix regressions

## 5. Documentation and Operator Guidance

- [x] 5.1 Document how to enable kafka-import tracing and inspect output in Chrome trace viewer
- [x] 5.2 Document expected overhead and artifact size trade-offs for long-running imports
