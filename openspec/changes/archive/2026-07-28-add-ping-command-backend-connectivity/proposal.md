## Why

Operators currently discover source/target/connectivity issues only after starting heavier commands like infer, import, or kafka-import. A lightweight ping command provides a fast preflight check for MongoDB, PostgreSQL, and Kafka reachability with clear backend-scoped feedback.

## What Changes

- Add a new CLI subcommand `mongo2pg ping` with backend selector flags.
- Support `--source` to validate MongoDB connectivity.
- Support `--target` to validate PostgreSQL connectivity.
- Support `--kafka` to validate Kafka reachability using configured bootstrap/auth settings.
- Return explicit success/failure output per selected backend and non-zero exit on failure.
- Reuse existing backend attribution patterns for command-visible connectivity errors.

## Capabilities

### New Capabilities

- `backend-ping-command`: Lightweight backend connectivity checks for MongoDB, PostgreSQL, and Kafka via `mongo2pg ping`.

### Modified Capabilities

- `connection-error-attribution`: Extend attribution visibility requirements to include ping command connectivity failures.

## Impact

- Affected code: CLI command/args wiring, runtime dispatch in `src/bin/mongo2pg.rs`, backend-specific connectivity check helpers.
- Affected docs: command reference and usage examples for ping workflows.
- Operational impact: faster diagnostics in CI/Kubernetes before running import/export/infer/kafka-import.
