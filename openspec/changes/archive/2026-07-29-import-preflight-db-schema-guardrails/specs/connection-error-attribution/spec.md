## ADDED Requirements

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
