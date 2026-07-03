## Why

Chunked infer fallback now reads in multiple cursor batches, but some deployments fail mid-stream with `Unauthorized` on `getMore` even after the initial `find` succeeds. This causes partial or empty inference on large collections and leaves users without a deterministic recovery path.

## What Changes

- Add explicit handling for `Unauthorized` errors raised during chunk cursor iteration in infer fallback.
- Define retry behavior that re-establishes a fresh query window (next chunk) instead of continuing a broken cursor.
- Add guardrails for repeated auth failures: bounded retries, clear warning/error escalation, and fail outcome when retry budget is exhausted.
- Add structured logs for auth-failure events (namespace, chunk index, retry attempt, final disposition).
- Add tests for unauthorized `getMore` scenarios and verify stop/continue semantics.

## Capabilities

### New Capabilities

- `infer-auth-resilient-chunk-fallback`: Makes chunked infer fallback resilient to mid-stream auth failures by retrying safely with bounded policy and clear diagnostics.

### Modified Capabilities

- None.

## Impact

- Affected code: infer sampling fallback and cursor iteration paths in [src/bin/mongo2pg.rs](src/bin/mongo2pg.rs).
- Affected tests: infer fallback control-flow and error-handling unit/integration tests.
- Operational impact: clearer warning/error messages for auth instability and reduced silent partial inference on huge collections.
