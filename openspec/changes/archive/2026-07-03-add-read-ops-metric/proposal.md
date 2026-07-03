## Why

Migration reports currently show structural complexity and document counts but do not show collection read activity context. Teams need to see how frequently each collection is read and the observation window to better prioritize migration and validation work.

## What Changes

- Capture per-collection MongoDB read operations during infer using MongoDB collection stats.
- Persist read operation count and read-ops start timestamp in each collection stats YAML.
- Render read operation metadata in the main HTML report near the Documents column for each collection.
- Keep behavior backward-compatible when MongoDB does not provide read-ops data by omitting the metric gracefully.

## Capabilities

### New Capabilities

- `collection-read-ops-reporting`: Collect, persist, and display per-collection read-ops metrics from MongoDB collStats in migration reporting outputs.

### Modified Capabilities

- None.

## Impact

- Affected code: infer pipeline, stats YAML model, and report rendering components.
- Affected outputs: `*.stats.yaml` under results source collection folders and `results/<db>/reports/main.html`.
- No breaking CLI/API changes.
- No new external dependencies required.
