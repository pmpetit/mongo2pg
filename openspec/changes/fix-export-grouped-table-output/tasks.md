## 1. Grouped Export Planning

- [x] 1.1 Add grouped export plan builder that maps source collections to grouped target tables using root mapping YAML (`mongo_path: .`, `pg_mapping.table_name`)
- [x] 1.2 Integrate grouped plan into `run_export` collection selection so grouped members do not require per-collection SQL filenames
- [x] 1.3 Keep existing per-collection SQL matching path for non-grouped collections unchanged

## 2. Grouped SQL Resolution and Emission

- [x] 2.1 Update export SQL lookup to resolve grouped members via grouped target table SQL file
- [x] 2.2 Refactor grouped export flow to aggregate rows from all grouped source collections into one target table output
- [x] 2.3 Ensure grouped table CSV files are written once per target table to prevent overwrite/loss across grouped members

## 3. Import Compatibility

- [ ] 3.1 Verify import table discovery works with grouped table-centric CSV output
- [ ] 3.2 Ensure grouped table load executes one truncate-and-copy cycle per grouped target table
- [ ] 3.3 Preserve existing non-grouped import behavior and filtering semantics

## 4. Verification

- [x] 4.1 Add tests for grouped export SQL resolution (grouped member -> shared SQL file)
- [ ] 4.2 Add tests for grouped multi-collection row aggregation into a single target CSV
- [ ] 4.3 Add regression tests confirming non-grouped export/import paths remain unchanged
- [x] 4.4 Run build and targeted export/import test suites to confirm no regressions
