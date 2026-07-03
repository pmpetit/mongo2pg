# collection-read-ops-reporting Specification

## Purpose

Define requirements for capturing collection read operation telemetry during infer and presenting it in stats artifacts and migration reports.

## Requirements

### Requirement: Infer captures collection read operations

The infer workflow SHALL query MongoDB collection statistics for each processed collection and extract read operation telemetry when available.

#### Scenario: Read telemetry available from MongoDB

- **WHEN** infer processes a collection and MongoDB returns collection stats with read operation fields
- **THEN** infer captures the read operation count and observation start timestamp for that collection

#### Scenario: Read telemetry unavailable

- **WHEN** infer processes a collection and MongoDB does not return read operation fields or collection stats query fails
- **THEN** infer continues successfully and marks read telemetry as absent for that collection

### Requirement: Stats YAML stores read operation telemetry

Per-collection stats YAML SHALL support an optional `read_ops` object containing read operation count and optional timestamp.

#### Scenario: Telemetry present

- **WHEN** infer has read telemetry for a collection
- **THEN** the generated stats YAML includes `read_ops.read_ops` and includes `read_ops.since` when available

#### Scenario: Telemetry absent

- **WHEN** infer has no read telemetry for a collection
- **THEN** the generated stats YAML omits the `read_ops` object

### Requirement: Main report displays read telemetry near Documents

The main migration report SHALL render available read operation telemetry adjacent to the collection Documents value.

#### Scenario: Render telemetry in report row

- **WHEN** report generation reads collection stats containing `read_ops`
- **THEN** the collection row displays the read operation count and since timestamp near the Documents column value

#### Scenario: Backward compatibility with existing stats files

- **WHEN** report generation reads collection stats without `read_ops`
- **THEN** the report renders without telemetry text and without errors
