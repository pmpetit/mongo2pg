## 1. Infer Chunk Configuration

- [x] 1.1 Add infer chunk-size configuration field(s) in CLI and config parsing with deterministic precedence
- [x] 1.2 Validate chunk-size values and fail fast for invalid or non-positive values
- [x] 1.3 Define and document default chunk size (for example 1,000,000 docs)

## 2. Chunked Fallback Implementation

- [x] 2.1 Refactor infer fallback from one large `find().limit(total)` into iterative chunked reads
- [x] 2.2 Ensure chunk loop stops at requested sample target or when source is exhausted
- [x] 2.3 Apply configured `max_time_ms` to each chunked read operation

## 3. Logging and Observability

- [x] 3.1 Add chunked fallback activation log with reason (`$sample` timeout/failure)
- [x] 3.2 Add per-chunk progress diagnostics (chunk index, chunk size, cumulative processed) using stable format for parsing
- [x] 3.3 Keep warning/error behavior clear when individual chunks fail

## 4. Verification

- [x] 4.1 Add unit tests for chunk-size precedence and validation behavior
- [ ] 4.2 Add infer-path tests for chunked fallback control flow and stop conditions
- [x] 4.3 Run build and relevant test suites to confirm no regressions
