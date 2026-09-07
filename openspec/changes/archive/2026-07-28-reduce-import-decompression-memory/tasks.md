## 1. Import Decompression Refactor

- [ ] 1.1 Locate current `.csv.gz` import path and replace full-materialization decompression with streaming decompression into COPY input.
- [ ] 1.2 Implement bounded chunked buffering for compressed read and uncompressed forwarding.
- [ ] 1.3 Preserve existing transaction/truncate/load orchestration and plain `.csv` behavior.

## 2. Observability and Error Handling

- [ ] 2.1 Add import debug markers for decompression stream begin/end per table/file.
- [ ] 2.2 Ensure gzip/copy failures preserve categorized error context and rollback behavior.

## 3. Validation

- [ ] 3.1 Add or update tests to cover large `.csv.gz` import without full in-memory expansion.
- [ ] 3.2 Add regression coverage that validates row correctness and compatibility with existing export artifacts.
- [ ] 3.3 Execute test suite and verify no behavior regressions for grouped and non-grouped imports.
