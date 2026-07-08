## Why

Infer grouped many MongoDB collections into one PostgreSQL table `events`, but report `main.html` does not surface that created target table clearly. This blocks quick validation of grouped exports and confuses migration review.

## What Changes

- Add report behavior so grouped collections explicitly show resolved PostgreSQL target table in the main report view.
- Ensure grouped table visibility works when many collections map to one table (for example all `events_*` collections mapping to `events`).
- Preserve existing behavior for non-grouped collections and existing stats files.

## Capabilities

### New Capabilities

- `main-report-target-table-visibility`: Display resolved PostgreSQL table names in the main migration report, including grouped mappings.

### Modified Capabilities

- `grouped-export-table-resolution`: Extend grouped table resolution behavior to require report-level visibility of grouped target table names.

## Impact

- Affected code: report generation and/or report rendering pipeline in Rust.
- Affected artifacts: generated `main.html` report output.
- No API or CLI breaking changes expected.
- Improves operator validation workflow after infer/export runs.
