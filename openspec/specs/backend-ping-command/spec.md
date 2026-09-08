# backend-ping-command Specification

## Purpose

Define requirements for a ping command that checks backend connectivity and reports per-backend results.

## Requirements

### Requirement: Ping command supports explicit backend selection

The CLI SHALL provide a `ping` subcommand that checks connectivity for selected backends using explicit flags.

#### Scenario: Source backend selected

- **WHEN** the user runs `mongo2pg ping -c <config> --source`
- **THEN** the command attempts a MongoDB connectivity check using configured source connection settings

#### Scenario: Target backend selected

- **WHEN** the user runs `mongo2pg ping -c <config> --target`
- **THEN** the command attempts a PostgreSQL connectivity check using configured target connection settings

#### Scenario: Kafka backend selected

- **WHEN** the user runs `mongo2pg ping -c <config> --kafka`
- **THEN** the command attempts a Kafka reachability check using configured Kafka settings

### Requirement: Ping command reports per-backend status

The ping command MUST print explicit per-backend success/failure results for each selected backend.

#### Scenario: Multiple backend checks

- **WHEN** the user selects multiple backend flags in one command
- **THEN** output includes one status line per selected backend indicating pass/fail

### Requirement: Ping command exit code reflects aggregate result

The command MUST exit with success only if all selected backend checks succeed.

#### Scenario: At least one backend fails

- **WHEN** one or more selected backend checks fail
- **THEN** the command exits with non-zero status

#### Scenario: All selected backends succeed

- **WHEN** all selected backend checks succeed
- **THEN** the command exits with status code 0