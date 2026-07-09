## MODIFIED Requirements

### Requirement: Grouped collections resolve export SQL by target table mapping

The export command MUST resolve SQL schema for grouped source collections using grouped target table mapping instead of requiring per-collection SQL filenames, and report generation MUST expose that same resolved target table for grouped collections.

#### Scenario: Grouped collection resolves to shared SQL file

- **WHEN** a source collection mapping root (`mongo_path = "."`) declares `pg_mapping.table_name = events`
- **THEN** export MUST use the SQL schema for target table `events` even if `events_<suffix>.sql` does not exist
- **AND** report generation MUST expose resolved target table `events` for that grouped collection
