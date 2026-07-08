## ADDED Requirements

### Requirement: Export processes rows in bounded chunks

The export command MUST process and write table rows in bounded chunks so memory usage does not scale linearly with total collection size.

#### Scenario: Large collection is exported without full in-memory accumulation

- **WHEN** export runs on a collection whose total rows exceed one chunk size
- **THEN** export writes rows incrementally in multiple chunk flushes
- **AND** export MUST NOT require retaining all rows in memory before output is written

### Requirement: Export chunk memory is released after flush

The export command MUST release chunk buffer memory after each successful flush cycle.

#### Scenario: Successive chunk processing keeps bounded memory profile

- **WHEN** export processes many chunks for one table
- **THEN** each chunk buffer is cleared or dropped after flush
- **AND** peak memory remains bounded by configured chunk size and active writer overhead

### Requirement: Chunk size is configurable with safe defaults

The export command MUST support configurable chunk size and validate values against safe bounds.

#### Scenario: User provides custom chunk size

- **WHEN** user passes a valid export chunk-size value
- **THEN** export uses that chunk size for row buffering and flush cadence

#### Scenario: Invalid chunk size is rejected

- **WHEN** user passes zero or out-of-range chunk-size value
- **THEN** export fails fast with a clear validation error

### Requirement: Output format remains import-compatible under chunking

Chunked export MUST preserve the same CSV schema and import compatibility as non-chunked export.

#### Scenario: Chunked and non-chunked exports have compatible output shape

- **WHEN** the same mapping is exported with chunking enabled
- **THEN** generated CSV files keep expected column ordering and table/file conventions used by import
