## 1. Report Data Plumbing

- [x] 1.1 Locate report row model/build path and add resolved PostgreSQL target table field as optional data.
- [x] 1.2 Reuse grouped mapping/table resolution logic so report uses same target-table result as export path.
- [x] 1.3 Add compatibility fallback so missing target-table metadata keeps current behavior without errors.

## 2. Main Report Rendering

- [x] 2.1 Update `main.html` generation/template to render PostgreSQL table value per collection row.
- [x] 2.2 Ensure grouped collections mapped to one table (for example `events_*` -> `events`) visibly show shared table name.
- [x] 2.3 Keep non-grouped row rendering unchanged except for added table visibility field.

## 3. Validation And Regression Coverage

- [x] 3.1 Add/update tests for grouped mappings showing shared target table in report output.
- [x] 3.2 Add/update tests for non-grouped mappings and legacy inputs without target-table metadata.
- [x] 3.3 Regenerate and verify report output for CIAM sample data to confirm `events` table appears in report.
