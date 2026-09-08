## 1. Snapshot Bulk COPY Path

- [x] 1.1 Add snapshot-mode buffering structures keyed by collection/table and flush boundary logic using `transaction_batch_size` as COPY batch size.
- [x] 1.2 Implement COPY writer for buffered rows with deterministic column order and encoding compatible with generated mapping/DDL.
- [x] 1.3 Trigger COPY flush on threshold, idle timeout, and stream end to ensure no buffered rows are left unapplied.

## 2. Transaction and Fallback Control Flow

- [x] 2.1 Refactor transaction lifecycle so COPY flushes always execute in explicit BEGIN/COMMIT transactions independent of `transaction_batch_size` transaction toggling.
- [x] 2.2 On COPY failure, perform rollback and replay failed batch through existing single-row apply path in original message order.
- [x] 2.3 Ensure fallback replay failures do not stop the stream and are surfaced via error counters/log fields.

## 3. Ad Hoc DLQ Handling

- [x] 3.1 Publish fallback single-row write failures to `dlq_<collection_name>` using original message key/payload.
- [x] 3.2 Add distinct counters and logs for DLQ publish success and DLQ publish failure outcomes.
- [x] 3.3 Sanitize/validate collection-derived DLQ topic names and keep behavior consistent with current DLQ producer configuration.

## 4. Observability and Reporting

- [x] 4.1 Add counters for COPY attempts, COPY failures, fallback replay attempts, fallback replay failures, DLQ publish successes, and DLQ publish failures.
- [x] 4.2 Include new counters in periodic progress logs and final kafka-import summary output.
- [x] 4.3 Update report/stat serialization pathways required by modified reporting capability to expose the new import counters without breaking existing readers.

## 5. Validation and Regression Coverage

- [x] 5.1 Add tests proving `transaction_batch_size` controls COPY batch row count while COPY remains transactional for size 1 and >1.
- [x] 5.2 Add failure-injection tests for COPY failure -> fallback replay and fallback replay failure -> ad hoc DLQ publish.
- [x] 5.3 Run targeted build/tests and document operator verification steps for throughput and resilience behavior.
