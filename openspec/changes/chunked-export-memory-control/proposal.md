## Why

Large exports can hold too many rows in memory before writing CSV output, causing Kubernetes OOMKilled events when pod memory limits are around 8GiB. Export needs bounded-memory behavior so high-volume collections can complete reliably.

## What Changes

- Add chunked export processing so rows are flushed incrementally instead of keeping full collection/table row sets in memory.
- Introduce memory-safe export write flow that releases chunk buffers after each flush.
- Add configurable chunk sizing with safe defaults for large collections.
- Preserve output compatibility and behavior for grouped and non-grouped exports.

## Capabilities

### New Capabilities

- `chunked-export-memory-control`: Export writes table data in bounded chunks and frees memory between flushes to prevent OOM on large datasets.

### Modified Capabilities

- `grouped-export-table-resolution`: Ensure grouped table exports remain correct when data is emitted in chunks across multiple source collections.

## Impact

- Affected code: export pipeline in Rust, especially row accumulation and CSV write path.
- Affected behavior: runtime memory profile during `mongo2pg export`.
- Affected validation: export tests for grouped/non-grouped correctness plus large-volume behavior.
- No expected CLI breaking changes; optional tuning flags/config may be added.
