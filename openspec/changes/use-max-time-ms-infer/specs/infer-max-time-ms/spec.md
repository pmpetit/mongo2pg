## ADDED Requirements

### Requirement: Infer operations honor configured max_time_ms

The system SHALL apply `source.max_time_ms` as `maxTimeMS` to infer read operations used for schema inference sampling.

#### Scenario: Sample aggregation uses configured timeout

- **WHEN** infer runs sampling with `$sample` for a collection and `source.max_time_ms` is configured
- **THEN** the MongoDB aggregation command MUST execute with `maxTimeMS` equal to configured `source.max_time_ms`

### Requirement: Fallback infer reads honor configured max_time_ms

When infer falls back from `$sample` to sequential read (`find().limit(...)`), the system SHALL apply the same `source.max_time_ms` timeout to fallback reads.

#### Scenario: Fallback find uses configured timeout

- **WHEN** `$sample` fails and infer switches to sequential `find().limit(...)` and `source.max_time_ms` is configured
- **THEN** fallback MongoDB read MUST execute with `maxTimeMS` equal to configured `source.max_time_ms`

### Requirement: Timeout-driven fallback is observable

The system SHALL emit infer warnings that identify timeout-triggered fallback conditions for operator troubleshooting.

#### Scenario: Warning includes timeout context on MaxTimeMSExpired

- **WHEN** infer sampling fails with MongoDB timeout (`MaxTimeMSExpired` / code 50)
- **THEN** warning output MUST indicate timeout cause and that fallback path was selected
