## Context

Grouped to-pg output now collapses multiple source collections into one SQL file/table (for example events.sql / events table). Export still selects collections by checking for per-collection SQL files named from sanitized collection names (for example events_bcit.sql), so grouped collections are skipped before row extraction starts. Even if SQL lookup were relaxed, the current export writer emits output per collection folder and recreates CSV files each run, which would overwrite grouped table output when several source collections map to the same target table.

## Goals / Non-Goals

**Goals:**

- Make export resolve grouped target SQL files when multiple MongoDB collections map to one table.
- Ensure grouped source collections produce one coherent CSV stream per grouped target table without overwrite loss.
- Keep import compatibility by loading grouped table CSV data exactly once per table.
- Preserve current behavior for non-grouped collections.

**Non-Goals:**

- Redesign infer grouping heuristics or grouping eligibility rules.
- Change mapping YAML schema format beyond fields already introduced for grouped mapping/literal support.
- Alter downstream PostgreSQL schema semantics outside grouped export/import correctness.

## Decisions

1. Introduce grouped export planning from mapping roots

- Decision: Build an explicit collection-to-target-table plan from root mapping files (`mongo_path: .`, `pg_mapping.table_name`) before collection filtering in `run_export`.
- Rationale: grouped truth already lives in mapping YAML per source collection; this avoids filename heuristics and keeps behavior deterministic.
- Alternative: infer group from SQL file names or fuzzy matching only. Rejected because it is brittle and loses explicit mapping intent.

1. Export grouped data by target table batch, not by per-collection SQL path lookup

- Decision: Resolve SQL schema by grouped target table name for grouped members, and batch grouped collections into one export unit.
- Rationale: grouped workflow needs one table definition and one output data stream.
- Alternative: keep per-collection export calls and add SQL fallback to closest match. Rejected because it can still overwrite grouped CSV output and duplicate table loads.

1. Emit grouped CSV files once per target table with additive row aggregation

- Decision: Aggregate rows from all grouped source collections for a target table, then write grouped table CSV once.
- Rationale: prevents last-writer-wins overwrite and aligns with single grouped SQL file behavior.
- Alternative: append to gz incrementally per collection. Rejected initially due to complexity/ordering fragility; one-pass grouped write is simpler and deterministic.

1. Keep import truncation/load cycle table-centric

- Decision: Ensure grouped export produces one CSV artifact per grouped table so existing import truncate+COPY per table remains valid.
- Rationale: minimizes import changes and preserves current transactional guarantees.
- Alternative: load many per-collection CSV files into same table with repeated truncate disabled. Rejected due to higher risk and control-flow churn.

## Risks / Trade-offs

- [Risk] Grouped plan extraction may miss malformed/missing mapping files. → Mitigation: emit explicit warnings and fall back to non-grouped path for unaffected collections.
- [Risk] Aggregating many grouped collections may increase memory footprint before CSV write. → Mitigation: keep table-by-table aggregation and reuse existing row buffers; add progress diagnostics.
- [Risk] Changing selection semantics could regress non-grouped export. → Mitigation: add tests for both grouped and non-grouped collection resolution paths.
- [Risk] Ordering differences across grouped collection ingestion could affect deterministic file diffs. → Mitigation: sort grouped collection members before extraction.
