## ADDED Requirements

### Requirement: Main report displays resolved PostgreSQL target table per collection row

The main migration report MUST display the resolved PostgreSQL target table for each collection row when table mapping data is available.

#### Scenario: Grouped collections display shared target table

- **WHEN** collections `events_bcit` and `events_lmza` are mapped to grouped target table `events`
- **THEN** each corresponding row in the main report displays PostgreSQL table `events`

#### Scenario: Non-grouped collection displays own target table

- **WHEN** a collection maps to a non-grouped PostgreSQL table name
- **THEN** the main report row displays that collection-specific PostgreSQL table value

### Requirement: Main report remains backward compatible when target table is unavailable

The main migration report MUST render successfully when target-table metadata is missing from older artifacts.

#### Scenario: Legacy artifact without target table metadata

- **WHEN** report generation processes stats/mapping inputs that do not provide resolved target table
- **THEN** report generation completes without error and existing row content is still rendered
