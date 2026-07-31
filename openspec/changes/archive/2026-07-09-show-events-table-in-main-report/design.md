## Context

Grouped export already resolves many MongoDB collections into one PostgreSQL target table (for example `events`). The generated migration report currently emphasizes source collection rows, but it does not make the resolved PostgreSQL table obvious in `main.html` for grouped mappings. Operators cannot quickly confirm that grouped infer/export produced the expected PostgreSQL destination table.

Constraints:

- Keep current infer/export data model and file formats backward compatible.
- Keep non-grouped reporting behavior unchanged.
- Avoid requiring new user configuration.

## Goals / Non-Goals

**Goals:**

- Show resolved PostgreSQL target table in main report rows where grouped mapping applies.
- Preserve existing report generation for legacy stats files and non-grouped mappings.
- Make grouped target-table visibility deterministic and testable.

**Non-Goals:**

- Redesign full report layout or styling.
- Change grouped export/import file naming semantics.
- Introduce new CLI flags.

## Decisions

1. Surface resolved target table in report row model

- Decision: Extend report row rendering inputs to include resolved PostgreSQL table name derived from mapping/grouping resolution.
- Rationale: Reuses existing mapping resolution logic; avoids duplicating inference in HTML layer.
- Alternative considered: Infer grouped table directly from collection naming convention during render. Rejected because it is brittle and can drift from mapping truth.

1. Render explicit table value in `main.html`

- Decision: Add a dedicated visible field/column in the report view for PostgreSQL table so grouped rows consistently show `events` (or other grouped target).
- Rationale: Clear UX signal for migration validation.
- Alternative considered: Tooltip or footnote only. Rejected because it is easy to miss and harder to test.

1. Keep fallback behavior for missing mapping data

- Decision: If resolved table is unavailable (legacy artifacts), render existing output without failure and without synthetic guesses.
- Rationale: Maintains backward compatibility with historical infer outputs.
- Alternative considered: Fail report generation. Rejected because it breaks existing workflows.

## Risks / Trade-offs

- Risk: Report layout width increases with new table field. -> Mitigation: Keep concise label/value and reuse existing styling patterns.
- Risk: Grouped mapping resolution in reporting diverges from export path. -> Mitigation: Reuse shared resolver/helper and add regression tests for grouped plus non-grouped inputs.
- Risk: Legacy stats may not have all metadata. -> Mitigation: Graceful fallback path with optional field handling.

## Migration Plan

- Implement report data plumbing and template rendering for target table visibility.
- Add/update tests that cover grouped collections mapping to one table and non-grouped controls.
- Regenerate/validate report output in sample PST dataset.
- Rollback strategy: remove rendering of new field and keep old row format if regressions are found.

## Open Questions

- Whether to place PostgreSQL table field as a dedicated column or as inline metadata near collection name/documents in current layout.
- Whether to include table-level grouping summary section in a follow-up change.
