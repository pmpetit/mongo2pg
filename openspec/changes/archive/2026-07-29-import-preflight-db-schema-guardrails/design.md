## Context

Import and kafka-import currently bootstrap PostgreSQL objects late in execution and rely on existing destination database/schema plus broad credentials. In production jobs this causes ambiguous failures (permission denied, relation already exists, wrong search_path) after staging and partial setup work has already occurred. The change introduces a deterministic preflight phase that runs before table DDL/data application and returns actionable failure messages.

## Goals / Non-Goals

**Goals:**

- Add a preflight sequence that checks/creates destination database before connecting to destination DB session.
- Add schema preflight to ensure target schema exists before executing table DDL.
- Fail fast with explicit remediation for permission-denied database/schema creation.
- Stop import when destination tables already exist and return a clear operator action.
- Keep preflight behavior consistent between `import` and `kafka-import` shared bootstrap paths.

**Non-Goals:**

- Automatic destructive cleanup (dropping existing tables or schema).
- Changes to mapping generation, table naming, or DDL shape.
- Changes to Kafka message decode/apply semantics.

## Decisions

1. Preflight-first execution order

- Decision: run destination checks before any DDL execution and before long-running import apply loops.
- Rationale: avoids late failures and partial progress ambiguity.
- Alternative considered: keep current lazy creation behavior and improve logs only. Rejected because operator feedback remains delayed and non-deterministic.

1. Admin connection fallback for database creation

- Decision: use `connect_pg_admin_client` style fallback strategy to attempt database creation in an admin-capable context when initial destination database connection fails due to missing DB.
- Rationale: supports least-privileged runtime user while still allowing controlled bootstrap.
- Alternative considered: require destination DB pre-created externally. Rejected because it increases runbook complexity and drift risk.

1. Explicit privilege-denied error attribution

- Decision: map database/schema creation permission failures to explicit, actionable command errors that name failing operation and remediation.
- Rationale: users need immediate distinction between connectivity failures and authorization failures.
- Alternative considered: pass through raw driver errors only. Rejected due to low operator clarity.

1. Existing-table hard stop

- Decision: detect existing destination tables and abort import/kafka-import before apply; return remediation requiring operator drop/cleanup and retry.
- Rationale: protects against accidental duplicate-load or mixed-state writes.
- Alternative considered: configurable overwrite/truncate in same change. Rejected to keep risk surface narrow.

## Risks / Trade-offs

- [Risk] More strict preflight may fail workflows that previously proceeded partially. -> Mitigation: clear remediation messaging and documentation updates.
- [Risk] Admin fallback behavior differs across managed PostgreSQL providers. -> Mitigation: preserve root driver details in errors and include provider-agnostic remediation text.
- [Risk] Additional metadata/preflight queries increase startup latency slightly. -> Mitigation: preflight is constant-time and occurs once per run, acceptable trade-off for determinism.

## Migration Plan

1. Implement preflight helpers and wire into import + kafka-import bootstrap sequence.
2. Add tests for: create missing DB/schema success, privilege-denied failure messaging, existing-table early stop.
3. Roll out in one release with release note callout: imports now fail early on preflight violations.
4. Rollback strategy: revert preflight gating commit; no data migration needed.

## Open Questions

- Should existing-table checks allow an explicit future override flag (`--allow-existing-tables`) or remain strict-only?
- Should schema creation attempts be skipped when schema omitted and search_path defaults are used?
