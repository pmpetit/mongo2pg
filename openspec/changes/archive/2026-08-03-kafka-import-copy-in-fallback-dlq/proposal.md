## Why

Kafka snapshot backfill is currently bottlenecked by per-message PostgreSQL writes, and transaction behavior is coupled to `transaction_batch_size` in a way that limits throughput tuning. We need a higher-throughput write path that remains reliable under malformed rows or transient database failures.

## What Changes

- Add a bulk snapshot write path for `kafka-import` using PostgreSQL `COPY` as the primary write mechanism.
- Keep `transaction_batch_size` as the row-count control for bulk `COPY` batches, while removing its role as a transaction on/off switch.
- Always run `COPY` ingestion inside an explicit transaction.
- If `COPY` ingestion fails for a batch, fallback to per-message single-row apply logic for that batch.
- If a fallback single-row write fails, publish the original message payload to an ad hoc DLQ topic named `dlq_<collection_name>` and continue processing.
- Add logging and counters to report `COPY` attempts, `COPY` failures, fallback usage, and DLQ publish outcomes.

## Capabilities

### New Capabilities

- `kafka-import-copy-bulk-fallback`: High-throughput snapshot ingestion using `COPY` with automatic fallback to single-row writes and ad hoc DLQ routing on fallback write failure.

### Modified Capabilities

- `collection-read-ops-reporting`: Extend import reporting with counters for bulk-copy/fallback/DLQ outcomes.

## Impact

- Affected code: `src/bin/mongo2pg.rs` Kafka import consume/apply loop, transaction lifecycle, and write paths.
- Affected runtime behavior: Snapshot-mode writes prefer `COPY`; fallback path preserves row-level resilience.
- Affected observability: New metrics/log lines for batch write mode transitions and DLQ events.
- Risk areas: Correct column/value shaping for `COPY`, transaction rollback boundaries, DLQ publication reliability under failure.
