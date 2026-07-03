## ADDED Requirements

### Requirement: Runtime log level is configurable

The system SHALL provide configurable runtime log filtering so operators can select log verbosity for command execution.

#### Scenario: CLI override selects log level

- **WHEN** operator runs a command with an explicit log-level option
- **THEN** runtime logging MUST use that level filter for the process

#### Scenario: Config value selects log level when CLI flag absent

- **WHEN** log-level option is not provided and config defines a log level
- **THEN** runtime logging MUST use the configured level

### Requirement: Log output includes timestamp and elapsed time

The system SHALL emit runtime logs with both wall-clock timestamp and elapsed duration since process start.

#### Scenario: Log line includes timing context

- **WHEN** runtime logger emits a message
- **THEN** output MUST include timestamp and elapsed-time fields in each log line

### Requirement: Log formatting is consistent across subcommands

The system SHALL initialize logger formatting and filtering through a shared startup path used by all supported subcommands.

#### Scenario: Infer and export share formatter behavior

- **WHEN** infer and export commands emit runtime logs in the same process configuration
- **THEN** both commands MUST follow the same formatter and level-filter rules
