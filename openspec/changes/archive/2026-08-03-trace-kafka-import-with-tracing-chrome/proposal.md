## Why

Kafka import performance debugging is hard because the current `run_kafka_import` path mixes message read, PostgreSQL write, and offset commit behavior inside one large loop. We need stable trace spans around each stage so operators can see where time is spent and tune throughput safely.

## What Changes

- Add structured tracing support for `kafka-import` using `tracing` and `tracing-chrome`.
- Refactor kafka-import flow into explicit stage functions for:
- reading messages from Kafka topics,
- writing decoded events to PostgreSQL,
- committing Kafka offsets.
- Emit trace spans and stage metadata (topic, op, batch sizes, timing, success/failure) per stage.
- Keep existing import behavior and counters while making stage boundaries observable.

## Capabilities

### New Capabilities

- `kafka-import-stage-tracing`: Adds optional trace capture for kafka-import stage execution and timing with Chrome trace output.

### Modified Capabilities

- `runtime-log-format`: Extend runtime observability behavior for kafka-import so per-stage execution data is captured through tracing instrumentation.

## Impact

- Affected code: `src/bin/mongo2pg.rs` kafka-import runtime path.
- Dependencies: add `tracing` and `tracing-chrome` runtime wiring for kafka-import execution.
- Ops workflow: enables opening generated trace output in Chrome trace viewer for bottleneck analysis.
- Backward compatibility: default kafka-import semantics remain unchanged; tracing is additive.
