## MODIFIED Requirements

### Requirement: Snapshot bulk ingest uses COPY with batch-size control

The kafka-import snapshot workflow SHALL use PostgreSQL COPY as the primary write mechanism, and SHALL use `transaction_batch_size` as the number of messages per bulk flush, and this behavior SHALL be preserved when kafka-import runtime code is extracted into a dedicated module.

#### Scenario: Flush triggered at configured batch size

- **WHEN** snapshot mode processes messages and buffered rows for a collection reach `transaction_batch_size`
- **THEN** kafka-import executes a COPY flush for that batch

#### Scenario: Flush triggered on idle or stream end

- **WHEN** snapshot mode has buffered rows below `transaction_batch_size` and the stream reaches idle timeout or end
- **THEN** kafka-import executes a COPY flush for remaining buffered rows

#### Scenario: COPY batching parity after module extraction

- **WHEN** kafka-import snapshot mode runs after extracting kafka-import logic out of `mongo2pg.rs`
- **THEN** COPY flush thresholds, idle/end draining behavior, and emitted bulk counters remain equivalent to pre-extraction behavior

### Requirement: COPY failure falls back to single-row replay

If a COPY flush fails, kafka-import SHALL rollback the failed COPY transaction and replay the same batch through existing single-row write logic, and this fallback behavior SHALL remain unchanged after module extraction.

#### Scenario: COPY batch failure

- **WHEN** COPY execution fails for a buffered batch
- **THEN** kafka-import rolls back the COPY transaction and replays each message in the batch with single-row apply behavior

#### Scenario: Fallback/DLQ parity after module extraction

- **WHEN** fallback replay and ad hoc DLQ handling are triggered after extraction
- **THEN** replay ordering, DLQ topic derivation, and failure counters remain equivalent to pre-extraction behavior
