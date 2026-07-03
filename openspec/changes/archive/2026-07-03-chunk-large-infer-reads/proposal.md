## Why

On very large collections, infer can still hit MongoDB `MaxTimeMSExpired` on `$sample` even with increased `max_time_ms`, and fallback to one huge sequential `find().limit(...)` remains heavy and brittle. Chunked infer reads reduce single-query pressure, improve progress resilience, and keep schema inference moving on high-volume datasets.

## What Changes

- Add chunked infer read strategy for large collections so schema inference processes documents in bounded chunks instead of one very large request.
- Introduce configurable infer chunk size (for example `chunk_size = 1000000`) to control per-query document batch size.
- Apply chunking to fallback path after `$sample` timeout/failure and keep max-time behavior per chunk.
- Improve infer logs to show chunked progress and chunk-level retries/failures.

## Capabilities

### New Capabilities

- `chunked-infer-processing`: Infer processes huge collections in configurable chunked reads to avoid single long-running operations.

### Modified Capabilities

- None.

## Impact

- Affected code: infer collection processing, fallback read loop, and infer progress logging.
- Config/API surface: new infer chunk-size setting (CLI and/or config) with sensible default.
- Runtime behavior: better reliability on huge collections, reduced risk from single-query timeouts.
