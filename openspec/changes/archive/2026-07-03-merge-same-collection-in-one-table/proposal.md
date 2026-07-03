## Why

Some MongoDB datasets split one logical event stream across multiple collections using suffixes such as events_lmfr, events_lmza, and events_bict. Current infer/to-pg flow creates one PostgreSQL table per collection, which duplicates schema and makes querying and maintenance harder.

## What Changes

- Add a post-infer grouping step that detects sibling collections sharing a common prefix and compatible inferred schema, then marks them for a single target PostgreSQL table.
- Add config switch add_grouped_key (true/false) to enable writing a discriminator column _key containing the source suffix (for example lmfr, bict).
- Extend mapping/table generation so grouped collections emit one shared table definition instead of one table per collection.
- Ensure export/import path can populate grouped table rows with optional _key when grouping is enabled.
- Add validation and diagnostics for grouping decisions (group created, skipped due to schema mismatch, skipped when disabled).

## Capabilities

### New Capabilities

- grouped-collection-table-merge: Detects suffixed sibling collections with equivalent schema and merges them into one PostgreSQL target table, optionally adding _key discriminator.

### Modified Capabilities

- None.

## Impact

- Affected code: infer output processing, mapping generation, to-pg SQL generation, export/import row shaping.
- Affected config: source-level option add_grouped_key (boolean).
- Affected outputs: source/collections mapping files and schema/tables SQL where grouped collections are represented as one table.
- Backward compatibility: default behavior remains unchanged when grouping conditions are not met or add_grouped_key is false.
