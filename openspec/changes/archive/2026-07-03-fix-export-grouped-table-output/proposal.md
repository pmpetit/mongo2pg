## Why

Grouped table generation now produces a single SQL file such as events.sql for multiple source collections, but export still resolves SQL files by per-collection name (for example events_bcit.sql). This makes export skip grouped collections entirely and blocks end-to-end grouped workflows.

## What Changes

- Add grouped-aware export planning so collection-to-SQL resolution can target shared grouped SQL files.
- Define deterministic row emission behavior when multiple source collections map to one target table.
- Ensure grouped export output is import-safe (no overwrite/loss between grouped source collections).
- Keep non-grouped export/import behavior unchanged.

## Capabilities

### New Capabilities

- `grouped-export-table-resolution`: Resolve export/import data flow correctly when many MongoDB collections map to one grouped SQL table.

### Modified Capabilities

- None.

## Impact

- Affected code: export collection selection in src/bin/mongo2pg.rs, grouped export row/file emission in src/export.rs, import CSV loading assumptions in src/bin/mongo2pg.rs.
- Affected systems: MongoDB-to-CSV export pipeline, CSV-to-PostgreSQL import pipeline.
- No external API change expected; behavior change is in grouped pipeline correctness.
