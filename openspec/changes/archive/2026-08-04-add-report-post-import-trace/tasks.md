## 1. Trace Flow Design in Code

- [x] 1.1 Identify report command code paths executed with --post-import during checkmd5 workflows and define canonical stage marker names.
- [x] 1.2 Add a shared trace/log helper for post-import report lifecycle markers (start, stage, success, failure) using existing logging/tracing primitives.

## 2. Implement Post-Import Trace Markers

- [x] 2.1 Emit a start marker when report begins --post-import processing.
- [x] 2.2 Emit stage markers at key orchestration boundaries (data load/checkmd5 stage/summary) with stable labels.
- [x] 2.3 Emit a success completion marker with summary context when post-import flow completes.
- [x] 2.4 Emit a failure marker in all error/early-return paths of the post-import flow before returning errors.

## 3. Preserve Backward Compatibility and Noise Controls

- [x] 3.1 Ensure non---post-import report executions keep existing behavior and output unchanged.
- [x] 3.2 Route detailed trace details through existing verbosity controls while keeping concise lifecycle markers operator-visible.

## 4. Validate and Document

- [x] 4.1 Add or update tests for --post-import trace markers in success and failure scenarios.
- [x] 4.2 Run targeted report/checkmd5 tests and verify no regressions for non---post-import paths.
- [x] 4.3 Update relevant docs/changelog entries to describe new post-import trace visibility and expected operator signals.
