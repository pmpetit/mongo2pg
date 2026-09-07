## Why

Import of large `*.csv.gz` artifacts can trigger OOM kills because current flow decompresses each file in memory before COPY. This blocks reliable migrations for large collections and makes import unstable in constrained CI/runtime environments.

## What Changes

- Replace full-file gzip decompression during import with streaming decompression piped directly into PostgreSQL COPY.
- Add explicit bounded-buffer behavior for compressed and uncompressed reads so memory does not scale with file size.
- Keep import semantics unchanged: same truncate/copy flow, same table mapping and compatibility with existing export output.
- Add debug telemetry for import decompression mode and per-file byte progress/phase boundaries.

## Capabilities

### New Capabilities

- `import-streaming-decompression`: Import processes compressed CSV artifacts via streaming decompression with bounded memory while preserving existing import behavior.

### Modified Capabilities

- *(none)*

## Impact

- Affected code: import path in CLI command flow (`src/bin/mongo2pg.rs`) and any helper utilities used by COPY input streams.
- Affected behavior: memory profile during import of `*.csv.gz` files; expected to become bounded and predictable.
- External interfaces: no CLI breaking change; existing export artifacts remain compatible.
- Validation: add/extend tests for large compressed input handling and regression checks for import correctness.
