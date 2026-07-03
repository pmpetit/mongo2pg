## 1. Config and Grouping Inputs

- [x] 1.1 Add add_grouped_key boolean to TOML config parsing and inferred runtime config model
- [x] 1.2 Define default value and validation behavior for add_grouped_key
- [x] 1.3 Add tests for config parsing/precedence of add_grouped_key

## 2. Post-Infer Group Detection

- [x] 2.1 Implement collection grouping candidate detector based on common prefix and suffix after last underscore
- [x] 2.2 Implement schema compatibility check across candidate collections using inferred artifact content
- [x] 2.3 Emit diagnostics for grouped and skipped candidates with explicit reasons

## 3. Mapping and SQL Consolidation

- [x] 3.1 Refactor mapping generation to produce one shared target table for each mergeable group
- [x] 3.2 Ensure non-mergeable groups preserve existing per-collection table behavior
- [x] 3.3 Update SQL generation path to avoid duplicate table emission for grouped collections

## 4. Data Path and Grouped Key

- [x] 4.1 Update export/import row shaping for grouped collections to target shared table
- [x] 4.2 When add_grouped_key=true, append _key with collection suffix value on grouped rows
- [x] 4.3 When add_grouped_key=false, keep grouped rows without _key column

## 5. Verification

- [x] 5.1 Add unit tests for grouping detection and schema-compatibility gating
- [ ] 5.2 Add integration-style tests for grouped table generation and _key on/off behavior
- [x] 5.3 Run targeted infer/to-pg/export/import tests and cargo build to confirm no regressions
