## 1. Error Attribution Foundations

- [x] 1.1 Inventory MongoDB, PostgreSQL, and Kafka connection/first-use failure boundaries in CLI command paths
- [x] 1.2 Define and apply a stable attributed message format (`connection_failed backend=<...> operation=<...>`) for connection-related failures
- [x] 1.3 Add shared helper(s) or consistent `anyhow::Context` usage to attach backend attribution without losing root error context

## 2. Command Path Coverage

- [x] 2.1 Update MongoDB-dependent paths (`infer`, `export`, `report`) to surface explicit mongo attribution on connection-related failures
- [x] 2.2 Update PostgreSQL-dependent paths (`import`, `report --post-import`) to surface explicit pg attribution on connection-related failures
- [x] 2.3 Update Kafka-dependent paths (`kafka-import`) to surface explicit kafka attribution on connection-related failures

## 3. Logging and Output Consistency

- [x] 3.1 Ensure runtime failure log lines include backend attribution token for connection-related failures
- [x] 3.2 Keep existing timestamp/elapsed runtime log formatting behavior unchanged while adding attribution
- [x] 3.3 Validate that attributed messages remain user-visible in CLI failure output and preserve underlying driver error details

## 4. Verification

- [x] 4.1 Add/update tests for mongo/pg/kafka connection failure attribution and root-cause chain preservation
- [x] 4.2 Add/update tests for runtime log backend attribution on connection-related failures
- [x] 4.3 Run targeted command-level checks with unreachable backend endpoints to confirm explicit attribution in real output
