## ADDED Requirements

### Requirement: Import preflight creates missing target database

The system SHALL perform a database preflight before import apply operations and SHALL attempt to create the target database when it does not exist.

#### Scenario: Target database missing and creation succeeds

- **WHEN** `import` or `kafka-import` starts and the configured target database does not exist
- **THEN** the system attempts database creation using the admin fallback strategy
- **THEN** execution continues with downstream schema/table bootstrap only after database creation succeeds

### Requirement: Import preflight creates missing target schema

The system SHALL ensure the configured target schema exists before executing destination table DDL.

#### Scenario: Target schema missing and creation succeeds

- **WHEN** database preflight succeeds and the configured target schema does not exist
- **THEN** the system creates the schema before any destination table DDL is executed
- **THEN** DDL/bootstrap continues in the ensured schema context

### Requirement: Preflight privilege failures are explicit and actionable

The system MUST fail fast when database or schema creation is denied and MUST include remediation guidance in the surfaced error.

#### Scenario: Database creation denied

- **WHEN** target database does not exist and creation fails due to insufficient privileges
- **THEN** command execution stops before table DDL or data apply begins
- **THEN** the surfaced error identifies database creation as the failing operation and includes remediation guidance

#### Scenario: Schema creation denied

- **WHEN** target schema does not exist and schema creation fails due to insufficient privileges
- **THEN** command execution stops before table DDL or data apply begins
- **THEN** the surfaced error identifies schema creation as the failing operation and includes remediation guidance

### Requirement: Existing destination tables stop import early

The system MUST validate destination table existence during preflight and MUST stop import when any destination table already exists.

#### Scenario: Existing destination table detected

- **WHEN** preflight finds one or more destination tables already present
- **THEN** command execution stops before data apply
- **THEN** the surfaced error instructs operators to drop or clean destination tables before retry
