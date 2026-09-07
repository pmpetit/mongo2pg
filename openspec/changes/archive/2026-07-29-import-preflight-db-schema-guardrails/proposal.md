## Why

Import and kafka-import currently rely on destination PostgreSQL objects being manually prepared or permissive credentials, which causes brittle runtime failures and operator confusion. We need deterministic preflight checks that create missing database/schema when allowed, and fail early with explicit remediation when privileges or pre-existing tables block safe execution.

## What Changes

- Add a preflight sequence before DDL/data apply for import workflows.
- Attempt target database creation when absent using an admin-connection fallback strategy.
- Attempt target schema creation before executing any table DDL.
- Fail fast with actionable privilege error messages when database/schema creation is denied.
- Stop import early when destination tables already exist, returning clear operator remediation to drop tables before retry.
- Keep behavior consistent across `import` and `kafka-import` where shared bootstrap/preflight logic is used.

## Capabilities

### New Capabilities

- `postgres-import-preflight-guardrails`: Preflight validation and bootstrap for target PostgreSQL database/schema with explicit failure handling and table-existence stop conditions.

### Modified Capabilities

- `connection-error-attribution`: Extend error attribution requirements to include permission-denied paths for database/schema creation and preflight failures.

## Impact

- Affected code: PostgreSQL connection/bootstrap logic in `src/bin/mongo2pg.rs`, plus shared error formatting/attribution utilities.
- Affected behavior: Import workflows will fail earlier with clearer remediation instead of progressing to late SQL errors.
- Operational impact: Requires operators to intentionally handle existing destination tables; improves safety and predictability in CI/Kubernetes jobs.
