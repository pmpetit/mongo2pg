## MODIFIED Requirements

### Requirement: Grouped source collections produce one CSV output per grouped target table

When multiple source collections map to the same grouped target table, export MUST produce one consolidated CSV artifact per target table without row loss caused by overwrite, including when export writes data in chunks, for both local filesystem and object-storage destinations.

#### Scenario: Multiple grouped sources export into one table CSV

- **WHEN** collections `events_bcit` and `events_lmza` both map to grouped target table `events`
- **THEN** export MUST emit one `events.csv.gz` output containing rows from both source collections according to mapping rules regardless of destination backend

#### Scenario: Multiple grouped sources export into one table CSV with chunking

- **WHEN** collections `events_bcit` and `events_lmza` both map to grouped target table `events` and export runs with chunked processing enabled
- **THEN** export MUST emit one `events.csv.gz` output containing rows from both source collections according to mapping rules
- **AND** chunked flushes from multiple sources MUST append into the same grouped output without truncation or overwrite across destination backends
