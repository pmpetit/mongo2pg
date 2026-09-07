## Context

`mongo2pg` currently validates backend connectivity only as a side effect of heavier commands (`infer`, `export`, `import`, `report --post-import`, `kafka-import`). Operators need a fast, explicit preflight command to check one or more backends before running longer workflows in CI/Kubernetes.

## Goals / Non-Goals

**Goals:**

- Add a new `ping` subcommand that checks backend reachability with explicit flags.
- Support `--source` (MongoDB), `--target` (PostgreSQL), and `--kafka` (Kafka).
- Produce clear pass/fail output per selected backend and return non-zero exit on failure.
- Reuse existing connection attribution style so ping failures identify backend and preserve root cause.

**Non-Goals:**

- Deep health checks beyond connectivity/reachability (no schema checks, no query latency SLO checks).
- Runtime state mutation (no DB/schema/table creation or Kafka offset operations).
- Auto-discovery of all backends without explicit selection flags.

## Decisions

1. Explicit backend flags

- Decision: require one or more of `--source`, `--target`, `--kafka`.
- Rationale: avoids ambiguity, keeps command intent explicit.
- Alternative: no flags means all backends. Rejected for now to avoid accidental credential use and reduce surprises.

1. Reuse config and shared connection helpers

- Decision: `ping` reads the same `-c <config>` and uses existing connection helpers and Kafka client configuration paths.
- Rationale: consistent auth/uri behavior across commands and minimal duplicate logic.
- Alternative: independent ping-only configuration. Rejected due to maintenance burden.

1. Backend-specific lightweight checks

- MongoDB: parse client options and perform minimal backend operation (list database names or ping command).
- PostgreSQL: connect and execute `SELECT 1`.
- Kafka: create consumer/admin client and fetch metadata with bounded timeout.
- Rationale: enough to verify practical reachability without side effects.

1. Exit and logging behavior

- Decision: print result per backend; exit code 0 only when all selected backends succeed.
- Rationale: simple CI contract for readiness gates.

## Risks / Trade-offs

- [Risk] Kafka metadata check may fail intermittently under transient broker conditions. -> Mitigation: bounded timeout and explicit error output.
- [Risk] Requiring explicit flags may feel strict for interactive use. -> Mitigation: clear CLI help and examples.
- [Risk] Different providers may emit noisy connection errors. -> Mitigation: preserve root cause while adding backend attribution.

## Migration Plan

1. Add CLI `Ping` command + args and validation.
2. Implement backend ping helpers and shared command runner.
3. Add tests for flag parsing, success/failure exit behavior, and attribution wiring.
4. Update docs with examples and expected outputs.

## Open Questions

- Should a future `ping --all` alias be added for convenience?
- Should ping support machine-readable output (JSON) for CI pipelines?
