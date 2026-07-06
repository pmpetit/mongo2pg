## 1. Runtime Log Formatting

- [x] 1.1 Update runtime log timestamp formatting to second precision (`YYYY-MM-DDTHH:MM:SS`) in `src/bin/mongo2pg.rs`
- [x] 1.2 Update elapsed duration formatting to whole seconds (`+<N>s`) in `src/bin/mongo2pg.rs`

## 2. Verification

- [x] 2.1 Update/add unit assertions to enforce timestamp and elapsed second-only format
- [x] 2.2 Run focused tests for runtime log formatter and confirm pass
