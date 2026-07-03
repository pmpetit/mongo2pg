## ADDED Requirements

### Requirement: Detect compatible suffixed collection groups

The system SHALL detect candidate groups of collections that share a common prefix and differ by suffix after the last underscore, and SHALL only mark a group mergeable when inferred schemas are equivalent.

#### Scenario: Group detected and compatible

- **WHEN** infer artifacts contain collections events_lmfr, events_lmza, and events_bict with equivalent inferred schema
- **THEN** the system creates one merge group for prefix events and marks it eligible for single-table target generation

#### Scenario: Group skipped due to schema mismatch

- **WHEN** candidate collections share prefix but at least one inferred schema differs
- **THEN** the system keeps per-collection targets and records a skip reason

### Requirement: Generate one PostgreSQL target table per mergeable group

For mergeable groups, the system MUST generate one PostgreSQL table shared by all grouped collections instead of one table per collection.

#### Scenario: Single-table target generated

- **WHEN** a mergeable group is identified
- **THEN** mapping and SQL generation emit one shared target table definition for the full group

### Requirement: Optional grouped key discriminator

The system SHALL support config flag add_grouped_key; when enabled it MUST include _key in grouped target rows with value equal to the source collection suffix.

#### Scenario: Grouped key enabled

- **WHEN** add_grouped_key is true and a grouped collection row is exported
- **THEN** the row contains _key with suffix value such as lmfr or bict

#### Scenario: Grouped key disabled

- **WHEN** add_grouped_key is false
- **THEN** grouped target rows do not include _key

### Requirement: Backward-compatible behavior when grouping not applicable

The system MUST preserve existing per-collection table behavior when no valid groups are found or grouping preconditions fail.

#### Scenario: No candidate groups

- **WHEN** collections do not match grouping pattern or no compatible groups exist
- **THEN** each collection continues to produce its own target table as before
