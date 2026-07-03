## Context

Some databases split one logical entity across many similarly named collections such as events_lmfr, events_lmza, and events_bict. Infer currently emits one schema/mapping/table per collection, which multiplies identical SQL tables and complicates downstream reporting and querying. The requested behavior is a post-infer consolidation step that inspects inferred outputs under source/collections and merges compatible suffixed collections into one PostgreSQL table, with optional source discriminator key.

## Goals / Non-Goals

**Goals:**

- Detect candidate collection groups by name pattern prefix_suffix (for example events_lmfr, events_lmza).
- Verify grouped collections are schema-compatible based on inferred JSON/mapping content.
- Generate one PostgreSQL target table per compatible group instead of one table per collection.
- Add config switch add_grouped_key (boolean) controlling optional _key column/value derived from suffix.
- Preserve deterministic and explainable grouping logs (grouped, skipped, reason).

**Non-Goals:**

- Merging collections with incompatible schemas by coercing or dropping fields.
- Changing raw MongoDB extraction semantics.
- Replacing existing per-collection behavior when grouping is disabled.

## Decisions

1. Grouping stage runs after infer file generation, before to-pg SQL generation

- Decision: Introduce a post-infer consolidation pass that reads source/collections outputs and writes normalized merged mapping metadata.
- Rationale: infer remains focused on schema discovery, while grouping is a deterministic transform on infer artifacts.
- Alternative: infer directly into grouped schema. Rejected due to higher runtime coupling and harder observability.

1. Candidate detection uses naming convention plus schema equivalence

- Decision: Candidate collections share identical prefix before final underscore and have at least one suffix token; grouping only applied when inferred schema signatures are equivalent.
- Rationale: avoids accidental merges from name-only heuristics.
- Alternative: name-only grouping. Rejected because it risks data corruption when structures differ.

1. Optional _key discriminator driven by config add_grouped_key

- Decision: when add_grouped_key=true, merged target table includes _key text column containing suffix; when false, no extra column is emitted.
- Rationale: gives users switchable lineage tracking.
- Alternative: always add _key. Rejected to keep backward-compatible schema surface when not needed.

1. Conservative fallback behavior

- Decision: if any collection in a candidate group fails compatibility checks, keep per-collection tables for that group and emit warning.
- Rationale: safety first, no lossy merge.

## Risks / Trade-offs

- [Risk] Prefix/suffix heuristic may over-group edge-case names. → Mitigation: require schema compatibility and log decisions.
- [Risk] Added post-infer transform increases pipeline complexity. → Mitigation: isolate logic and add focused tests.
- [Risk] Adding _key can change downstream SQL expectations. → Mitigation: config-gated behavior and clear docs/default false.

## Migration Plan

- Add config field parsing and default for add_grouped_key.
- Implement grouping planner over source/collections artifacts.
- Integrate planner into to-pg/export mapping path.
- Add tests for positive grouping, mismatch skip, and _key on/off.

## Open Questions

- Should grouping be globally enabled by default or remain opt-in via config (recommended opt-in)?
- Should grouping require minimum group size >1 to avoid unnecessary rewrites (recommended yes)?
- Should _key column name be configurable later beyond fixed_key?
