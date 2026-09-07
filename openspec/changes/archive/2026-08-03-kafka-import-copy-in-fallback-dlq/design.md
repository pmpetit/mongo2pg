## Context

`kafka-import` currently applies snapshot rows through per-message SQL execution paths. This is resilient but slow for large backfills, and transaction behavior has been implicitly tied to `transaction_batch_size > 1` in parts of the flow. The requested change introduces a high-throughput snapshot path based on PostgreSQL `COPY`, while preserving reliability through single-row fallback and DLQ publication.

Constraints:

- Throughput increase must not compromise correctness.
- Existing message decoding/mapping and per-row apply behavior should remain available as fallback.
- `transaction_batch_size` must remain meaningful as batching control, but not as transaction enable/disable toggle for the bulk path.

## Goals / Non-Goals

**Goals:**

- Use PostgreSQL `COPY` for snapshot backfill writes as the primary write path.
- Use `transaction_batch_size` as bulk batch size (rows per `COPY` batch).
- Always execute each `COPY` batch inside an explicit transaction (`BEGIN`/`COMMIT`).
- On `COPY` batch failure, rollback and replay that batch through single-row writes.
- On fallback single-row write failure, publish the original message to ad hoc topic `dlq_<collection_name>` and continue.
- Emit clear counters/logs for copy attempts/failures, fallback attempts/failures, and DLQ publish outcomes.

**Non-Goals:**

- Replacing all non-snapshot write paths with `COPY`.
- Redesigning topic naming conventions beyond the requested ad hoc DLQ pattern.
- Introducing a global, cross-collection transactional boundary.

## Decisions

1. Primary snapshot write mode: `COPY` by collection batch.

- Decision: Build per-collection in-memory batches and flush via `COPY` when batch size reaches `transaction_batch_size`.
- Rationale: `COPY` minimizes protocol round trips and parse overhead, giving largest write throughput gain.
- Alternative considered: Multi-row INSERT with large VALUES lists. Rejected due to weaker throughput and more SQL construction overhead.

1. Transaction semantics for bulk mode.

- Decision: Every `COPY` flush runs in explicit transaction regardless of batch size value.
- Rationale: Ensures atomicity at batch boundary and deterministic rollback behavior on copy failure.
- Alternative considered: Autocommit `COPY`. Rejected because partial-batch failure handling is less deterministic.

1. Fallback strategy after copy failure.

- Decision: If `COPY` fails for a batch, rollback, then replay messages one-by-one using existing single-row apply logic.
- Rationale: Reuses proven mapping/validation behavior and isolates malformed rows without dropping the entire stream.
- Alternative considered: Drop failed batch directly to DLQ. Rejected because it loses recoverable rows.

1. DLQ policy for fallback write failures.

- Decision: If single-row fallback write fails for a message, publish that message payload to `dlq_<collection_name>`.
- Rationale: Keeps pipeline moving while preserving failed messages for operator replay/inspection.
- Alternative considered: Fail-fast and stop consumer. Rejected because single bad rows can stall long backfills.

1. `transaction_batch_size` behavior.

- Decision: `transaction_batch_size` determines bulk `COPY` flush size; it no longer decides whether transactions are enabled.
- Rationale: Preserves user tuning knob while removing control-flow ambiguity.

## Risks / Trade-offs

- [COPY row-shaping mismatch] -> Mitigation: Keep fallback replay path and log copy failure details with collection/table context.
- [Memory growth from batching] -> Mitigation: Flush strictly at `transaction_batch_size` and at stream idle/end boundaries.
- [Fallback throughput degradation under repeated copy failure] -> Mitigation: Counter and warning telemetry so operators can detect sustained fallback mode.
- [DLQ producer failure] -> Mitigation: Count and log DLQ publish failures distinctly from apply failures.

## Migration Plan

1. Add bulk snapshot buffering and `COPY` execution path behind existing kafka-import flow.
2. Preserve existing single-row apply logic and wire it as fallback replay for failed copy batches.
3. Add/extend counters and progress logs for copy/fallback/DLQ outcomes.
4. Validate with snapshot-mode integration tests and failure-injection tests.
5. Rollback strategy: disable/short-circuit bulk path to always use legacy single-row writes if severe regression appears.

## Open Questions

- Should `COPY` fallback trigger immediately for every failure, or should retries (same batch) be attempted first?
- Should we add an option to cap consecutive fallback failures before pausing/stopping consumer?
- Do we need per-collection DLQ topic sanitization rules if collection names contain uncommon characters?

## Operator Verification Steps

1. Run `kafka-import` snapshot mode with `transaction_batch_size=1` and confirm logs show snapshot-copy counters increasing (`copy_attempts`, `copy_failed` as applicable) and no non-snapshot tx-batch logs.
2. Run with `transaction_batch_size` > 1 and confirm `buffered_messages` drains in batches at threshold and on idle/end flush.
3. Force a COPY failure (for example by introducing a temporary incompatible value) and confirm logs show `copy_failed` and `fallback_replay_attempts` increments.
4. Force fallback single-row failure and confirm publication attempts to `dlq_<collection_name>` with corresponding `fallback_replay_dlq_*` counters.
5. Confirm file `reports/kafka_import_write_mode.stats.yaml` is produced with counters matching final log summary.
