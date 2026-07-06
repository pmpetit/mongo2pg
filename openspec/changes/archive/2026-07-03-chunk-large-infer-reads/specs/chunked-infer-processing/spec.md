## ADDED Requirements

### Requirement: Infer fallback reads support chunked processing

When `$sample` fails during infer on a collection, the system SHALL process fallback reads in bounded chunks instead of one single large `find().limit(...)` operation.

#### Scenario: Fallback mode processes multiple chunks

- **WHEN** `$sample` fails and inferred sample size exceeds configured chunk size
- **THEN** infer MUST execute multiple chunked fallback reads until target sample count is reached or source is exhausted

### Requirement: Chunk size is configurable for infer

The system SHALL provide configurable chunk size for infer processing with validation and deterministic precedence.

#### Scenario: Configured chunk size is applied

- **WHEN** operator sets infer chunk size via CLI or config
- **THEN** fallback chunked infer MUST use that value for per-chunk read size

#### Scenario: Invalid chunk size is rejected

- **WHEN** operator provides non-positive or invalid chunk-size value
- **THEN** command MUST fail fast with clear validation error

### Requirement: Chunked infer honors max_time_ms per chunk

Chunked infer fallback reads SHALL apply configured `max_time_ms` timeout policy to each chunk operation.

#### Scenario: Per-chunk timeout uses configured max_time_ms

- **WHEN** infer executes each fallback chunk and `max_time_ms` is configured
- **THEN** each chunked MongoDB read MUST apply the configured max-time limit

### Requirement: Chunked infer emits progress diagnostics

The system SHALL emit runtime diagnostics indicating chunk progress and fallback reason for huge collection inference.

#### Scenario: Chunk progress logging is visible

- **WHEN** infer enters chunked fallback mode
- **THEN** logs MUST include at minimum chunk index, chunk size, and cumulative processed count for operator visibility
- **THEN** logs SHOULD use a stable machine-parseable pattern (for example `chunk 3/45 size=1000000 processed=3000000/44914995`)
