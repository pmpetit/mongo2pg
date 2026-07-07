## Why

Connection setup and runtime failures can currently surface as generic errors, making it hard to tell whether MongoDB, PostgreSQL, or Kafka is the failing dependency. This slows triage and increases recovery time during migrations and streaming runs.

## What Changes

- Add explicit connection-failure attribution in user-facing errors for MongoDB, PostgreSQL, and Kafka paths.
- Standardize failure message shape so logs and CLI output consistently identify failing backend and operation context.
- Preserve existing command behavior and exit semantics while improving diagnostics.
- Keep non-connection errors unchanged unless they are wrapped to add backend context.

## Capabilities

### New Capabilities

- `connection-error-attribution`: Distinguish and report connection failures by backend (mongo, pg, kafka) with clear actionable messages.

### Modified Capabilities

- `runtime-log-format`: Extend runtime error logging expectations so backend attribution appears consistently in emitted failure lines.

## Impact

- Affected code: connection initialization and backend access points in `src/bin/mongo2pg.rs`, plus shared error/context helpers if needed.
- Affected behavior: user-visible error strings for connection failures in infer/export/import/kafka-import/report paths.
- Dependencies: no new external dependency expected; likely use existing anyhow context layering.
