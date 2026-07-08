## ADDED Requirements

### Requirement: Grouped collections resolve export SQL by target table mapping

The export command MUST resolve SQL schema for grouped source collections using grouped target table mapping instead of requiring per-collection SQL filenames.

#### Scenario: Grouped collection resolves to shared SQL file

- **WHEN** a source collection mapping root (`mongo_path = "."`) declares `pg_mapping.table_name = events`
- **THEN** export MUST use the SQL schema for target table `events` even if `events_<suffix>.sql` does not exist

### Requirement: Grouped source collections produce one CSV output per grouped target table

When multiple source collections map to the same grouped target table, export MUST produce one consolidated CSV artifact per target table without row loss caused by overwrite, including when export writes data in chunks.

#### Scenario: Multiple grouped sources export into one table CSV

- **WHEN** collections `events_bcit` and `events_lmza` both map to grouped target table `events`
- **THEN** export MUST emit one `events.csv.gz` output containing rows from both source collections according to mapping rules

#### Scenario: Multiple grouped sources export into one table CSV with chunking

- **WHEN** collections `events_bcit` and `events_lmza` both map to grouped target table `events` and export runs with chunked processing enabled
- **THEN** export MUST emit one `events.csv.gz` output containing rows from both source collections according to mapping rules
- **AND** chunked flushes from multiple sources MUST append into the same grouped output without truncation or overwrite

### Requirement: Non-grouped export behavior remains unchanged

Collections that do not participate in grouped mapping MUST retain current SQL resolution and CSV emission behavior.

#### Scenario: Non-grouped collection uses per-collection SQL

- **WHEN** a collection has no grouped target-table mapping
- **THEN** export MUST continue resolving SQL using the existing per-collection filename convention and emit collection-scoped CSV output as before

### Requirement: Grouped export artifacts are import-compatible

Grouped export output MUST be structured so import performs one truncate-and-load cycle per grouped target table without requiring per-collection SQL files.

#### Scenario: Import loads grouped table once

- **WHEN** grouped export generates data for target table `events`
- **THEN** import MUST load grouped `events` data with one table load path and MUST NOT require separate `events_<suffix>.sql` files
