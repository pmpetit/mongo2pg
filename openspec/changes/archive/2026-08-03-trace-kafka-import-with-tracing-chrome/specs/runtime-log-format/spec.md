## MODIFIED Requirements

### Requirement: Runtime failures include backend attribution token

Runtime failure log lines MUST include a stable backend attribution token when the failure is connection-related. For kafka-import stage-instrumented execution, failure logs MUST also preserve enough stage context to correlate with trace spans.

#### Scenario: Connection-related runtime failure log

- **WHEN** a runtime log line is emitted for a connection-related failure
- **THEN** the log line includes backend attribution identifying one of `mongo`, `pg`, or `kafka`

#### Scenario: Kafka-import stage failure correlation

- **WHEN** kafka-import emits a failure log for read, write, or commit stage processing
- **THEN** the log line includes stage context fields that allow correlating the failure with stage trace output
