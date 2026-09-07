## ADDED Requirements

### Requirement: Snapshot bulk ingest uses COPY with batch-size control

The kafka-import snapshot workflow SHALL use PostgreSQL COPY as the primary write mechanism, and SHALL use `transaction_batch_size` as the number of messages per bulk flush.

#### Scenario: Flush triggered at configured batch size

- **WHEN** snapshot mode processes messages and buffered rows for a collection reach `transaction_batch_size`
- **THEN** kafka-import executes a COPY flush for that batch

#### Scenario: Flush triggered on idle or stream end

- **WHEN** snapshot mode has buffered rows below `transaction_batch_size` and the stream reaches idle timeout or end
- **THEN** kafka-import executes a COPY flush for remaining buffered rows

### Requirement: COPY batches are always transactional

Each COPY flush SHALL execute inside an explicit PostgreSQL transaction regardless of configured `transaction_batch_size` value.

#### Scenario: Batch size equals one

- **WHEN** `transaction_batch_size` is `1`
- **THEN** each COPY flush still runs within an explicit BEGIN/COMMIT transaction

#### Scenario: Batch size greater than one

- **WHEN** `transaction_batch_size` is greater than `1`
- **THEN** each COPY flush still runs within an explicit BEGIN/COMMIT transaction

### Requirement: COPY failure falls back to single-row replay

If a COPY flush fails, kafka-import SHALL rollback the failed COPY transaction and replay the same batch through existing single-row write logic.

#### Scenario: COPY batch failure

- **WHEN** COPY execution fails for a buffered batch
- **THEN** kafka-import rolls back the COPY transaction and replays each message in the batch with single-row apply behavior

### Requirement: Fallback write failures are published to ad hoc DLQ

If single-row replay fails for a message after COPY fallback, kafka-import SHALL publish the original message payload to topic `dlq_<collection_name>` and continue processing later messages.

#### Scenario: Single-row replay failure after COPY fallback

- **WHEN** a message fails during fallback single-row apply
- **THEN** kafka-import publishes that message to `dlq_<collection_name>` and records the failure in counters/logs

#### Scenario: DLQ publish failure during fallback

- **WHEN** DLQ publication for a failed replay message fails
- **THEN** kafka-import records DLQ publication failure counters/logs and continues processing

### Requirement: Bulk/fallback outcomes are observable

kafka-import SHALL report counters for COPY attempts, COPY failures, fallback replay attempts, fallback replay failures, DLQ publish successes, and DLQ publish failures.

#### Scenario: Progress and summary reporting

- **WHEN** kafka-import emits progress or final summary logs
- **THEN** the logs include bulk/fallback/DLQ counters with non-ambiguous field names
