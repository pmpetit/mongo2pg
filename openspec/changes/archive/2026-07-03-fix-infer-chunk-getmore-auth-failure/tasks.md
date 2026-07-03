## 1. Retry Policy Surface

- [x] 1.1 Add infer auth-retry configuration field(s) and deterministic precedence (CLI over config over default)
- [x] 1.2 Validate auth-retry values and fail fast for invalid ranges
- [x] 1.3 Define and document default auth-retry budget for chunk fallback

## 2. Unauthorized Detection and Retry Flow

- [x] 2.1 Add helper to classify unauthorized cursor-iteration errors (code 13 / unauthorized markers)
- [x] 2.2 Refactor chunk cursor loop to retry failed chunk from same processed boundary on unauthorized
- [x] 2.3 Preserve existing behavior for non-unauthorized errors
- [x] 2.4 Stop and return terminal collection failure when retry budget is exhausted

## 3. Logging and Diagnostics

- [x] 3.1 Emit structured auth-retry warning log with namespace, chunk index, processed count, retry attempt
- [x] 3.2 Emit structured terminal outcome log for retry exhaustion
- [x] 3.3 Keep existing chunk progress logs consistent with retry events

## 4. Verification

- [x] 4.1 Add unit tests for unauthorized error classification
- [x] 4.2 Add infer fallback tests for retry-success path and retry-exhausted path
- [x] 4.3 Run build and targeted infer/logging tests to confirm no regressions
