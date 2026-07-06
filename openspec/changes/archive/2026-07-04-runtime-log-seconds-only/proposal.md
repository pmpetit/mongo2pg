## Why

Runtime logs currently print high-precision timestamps and fractional elapsed values, which adds noise for operators scanning logs quickly. A seconds-only format improves readability and consistency across environments.

## What Changes

- Standardize runtime log timestamp output to second precision (`YYYY-MM-DDTHH:MM:SS`).
- Standardize elapsed duration output to whole seconds (`+<N>s`).
- Add/adjust tests to lock the formatting contract and prevent regressions.

## Capabilities

### New Capabilities
- `runtime-log-format`: Defines the CLI runtime log line format for timestamp precision and elapsed duration precision.

### Modified Capabilities
- None.

## Impact

- Affected code: runtime logger formatting in `src/bin/mongo2pg.rs`.
- Affected tests: unit tests that assert runtime log line output shape.
- No API surface changes and no external dependency changes.
