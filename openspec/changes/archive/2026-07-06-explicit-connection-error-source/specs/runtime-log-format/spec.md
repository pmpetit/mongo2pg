## ADDED Requirements

### Requirement: Runtime failures include backend attribution token

Runtime failure log lines MUST include a stable backend attribution token when the failure is connection-related.

#### Scenario: Connection-related runtime failure log

- **WHEN** a runtime log line is emitted for a connection-related failure
- **THEN** the log line includes backend attribution identifying one of `mongo`, `pg`, or `kafka`
