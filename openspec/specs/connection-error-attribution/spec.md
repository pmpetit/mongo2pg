# connection-error-attribution Specification

## Purpose

Define requirements for backend-specific connection failure attribution and error root-cause preservation in user-visible command failures.

## Requirements

### Requirement: Backend-specific connection failure attribution

The CLI MUST report connection failures with explicit backend attribution for MongoDB, PostgreSQL, and Kafka.

#### Scenario: MongoDB connection failure

- **WHEN** a command requires MongoDB and connection establishment or first backend operation fails
- **THEN** the surfaced error includes explicit backend attribution indicating MongoDB as the failing backend

#### Scenario: PostgreSQL connection failure

- **WHEN** a command requires PostgreSQL and connection establishment or first backend operation fails
- **THEN** the surfaced error includes explicit backend attribution indicating PostgreSQL as the failing backend

#### Scenario: Kafka connection failure

- **WHEN** a command requires Kafka and connection establishment or first backend operation fails
- **THEN** the surfaced error includes explicit backend attribution indicating Kafka as the failing backend

### Requirement: Attributed errors preserve root cause chain

Attributed connection errors MUST preserve original driver/root-cause details while adding backend context.

#### Scenario: Root cause retained

- **WHEN** backend attribution is added to an error
- **THEN** the emitted error still contains the underlying driver error details for troubleshooting

### Requirement: Attribution appears in command-visible failures

Attribution MUST be visible in user-facing command failure output for all relevant commands that use each backend.

#### Scenario: Command fails with attributed backend

- **WHEN** `infer`, `export`, `import`, `report --post-import`, `kafka-import`, or `ping` fails due to backend connectivity
- **THEN** command output includes explicit backend attribution identifying which dependency failed

### Requirement: Privilege-denied preflight errors are operation-attributed

Preflight authorization failures MUST include explicit backend and operation attribution for database and schema creation paths.

#### Scenario: Import fails on database creation privilege

- **WHEN** `import` or `kafka-import` cannot create a missing target database due to insufficient privileges
- **THEN** command output includes backend attribution for PostgreSQL and operation attribution for database creation
- **THEN** the error includes the underlying driver cause details

#### Scenario: Import fails on schema creation privilege

- **WHEN** `import` or `kafka-import` cannot create a missing target schema due to insufficient privileges
- **THEN** command output includes backend attribution for PostgreSQL and operation attribution for schema creation
- **THEN** the error includes the underlying driver cause details
