## ADDED Requirements

### Requirement: Kafka-import write-mode counters are included in migration telemetry outputs

Migration telemetry outputs SHALL include kafka-import write-mode counters for COPY attempts/failures, fallback single-row attempts/failures, and DLQ publish outcomes.

#### Scenario: Counters available after kafka-import run

- **WHEN** kafka-import completes with bulk-write mode enabled
- **THEN** migration telemetry outputs include counters for COPY, fallback replay, and DLQ outcomes

#### Scenario: Backward compatibility for runs without bulk mode

- **WHEN** telemetry output is generated for runs that did not use COPY bulk mode
- **THEN** outputs remain readable and represent bulk/fallback counters as zero or absent without parse errors
