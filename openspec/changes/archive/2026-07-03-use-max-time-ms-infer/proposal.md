## Why

Infer currently runs full-percent sampling on very large collections and can hit MongoDB `MaxTimeMSExpired` during `$sample` aggregation. Users already provide `max_time_ms` in config, but infer does not consistently apply this time budget to sampling operations, causing warnings, retries, and slow fallback paths.

## What Changes

- Wire `source.max_time_ms` into infer sampling operations so MongoDB reads honor configured query time limits.
- Ensure `$sample` and infer-related find/aggregate calls use the same configured timeout policy.
- Preserve existing fallback behavior when `$sample` fails, while preventing unbounded long-running infer queries.
- Improve infer warning context so timeout-driven fallback is explicit and actionable.

## Capabilities

### New Capabilities

- `infer-max-time-ms`: Infer sampling and fallback reads enforce configured `max_time_ms` from source config.

### Modified Capabilities

- None.

## Impact

- Affected code: infer pipeline/query construction and MongoDB read options in Rust source.
- Config behavior: `source.max_time_ms` becomes effective for infer sample reads and related fallback reads.
- Runtime behavior: fewer runaway infer operations; clearer timeout/fallback warnings during large collection inference.
