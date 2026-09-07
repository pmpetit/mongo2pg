## ADDED Requirements

### Requirement: Report --post-import emits command trace markers during checkmd5 workflows

When the report command runs with --post-import in checkmd5-related workflows, the system SHALL emit explicit trace markers for command lifecycle and critical execution stages.

#### Scenario: Post-import report starts

- **WHEN** the report command is invoked with --post-import
- **THEN** the runtime emits a trace marker indicating post-import report processing has started

#### Scenario: Post-import report reaches processing stages

- **WHEN** the --post-import flow advances through defined processing stages
- **THEN** the runtime emits stage-specific trace markers using stable stage names

#### Scenario: Post-import report finishes successfully

- **WHEN** the --post-import flow completes without error
- **THEN** the runtime emits a completion trace marker with summary context

#### Scenario: Post-import report fails

- **WHEN** an error occurs in the --post-import flow
- **THEN** the runtime emits a failure trace marker before returning the error

### Requirement: Non-post-import report behavior remains unchanged

The trace enhancement SHALL NOT alter functional behavior of report executions that do not use --post-import.

#### Scenario: Report without post-import

- **WHEN** the report command runs without --post-import
- **THEN** report outputs and control flow remain backward-compatible with pre-change behavior
