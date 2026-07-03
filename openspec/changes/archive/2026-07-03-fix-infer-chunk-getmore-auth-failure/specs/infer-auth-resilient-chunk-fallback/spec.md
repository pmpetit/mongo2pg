## ADDED Requirements

### Requirement: Retry Unauthorized getMore at chunk boundary

The infer chunk fallback SHALL detect unauthorized cursor iteration failures (including MongoDB code 13) and retry the same chunk by issuing a fresh query window from the current processed offset.

#### Scenario: Unauthorized occurs mid-chunk and retry succeeds

- **WHEN** chunk fallback is processing collection data and cursor iteration returns an unauthorized error
- **THEN** the system issues a new query for the same chunk boundary and resumes processing without resetting already processed chunk totals

### Requirement: Enforce bounded auth retry policy

The infer chunk fallback MUST apply a bounded retry budget for unauthorized chunk-cursor failures and terminate collection inference when the retry budget is exhausted.

#### Scenario: Unauthorized retries exceed budget

- **WHEN** unauthorized chunk-cursor errors continue until retry budget is consumed
- **THEN** the system stops collection inference for that namespace and emits a terminal failure that includes namespace, chunk index, and retry summary

### Requirement: Preserve chunk progress and observability under auth retries

The infer chunk fallback SHALL emit stable parseable logs for unauthorized retry events and final disposition, including namespace, chunk index, processed count, retry attempt, and outcome.

#### Scenario: Retry logging is emitted for each auth failure

- **WHEN** unauthorized is detected while iterating a chunk cursor
- **THEN** the system logs an auth-retry event with stable keys and logs either success-after-retry or retry-exhausted outcome
