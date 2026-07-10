## 1. Destination And Backend Foundations

- [x] 1.1 Add destination parsing so `base_dir` starting with `gs://` selects GCS and all other `base_dir` values select local filesystem.
- [x] 1.2 Introduce export writer backend abstraction used by grouped and non-grouped artifact writers.
- [x] 1.3 Keep local filesystem writer as default backend with no behavior regression for existing CLI flows.

## 2. GCS Backend Implementation

- [x] 2.1 Add Google Cloud Storage client dependencies and feature/config wiring for cloud writes.
- [x] 2.2 Implement GCS writer backend for artifact upload to bucket/object-prefix targets.
- [x] 2.3 Implement credential discovery and destination preflight validation before export write phase.

## 3. Grouped Export Compatibility

- [x] 3.1 Route grouped export write path through backend abstraction while preserving one artifact per target table.
- [x] 3.2 Ensure chunked grouped exports append/finalize correctly on both filesystem and GCS backends.
- [x] 3.3 Keep grouped artifact naming and format consistent across local and GCS destinations.

## 4. Error Handling And Observability

- [x] 4.1 Add categorized cloud write errors for authentication, authorization, not found, and transient failures.
- [x] 4.2 Include destination context in user-facing error messages and runtime reporting.
- [x] 4.3 Add logging hooks for backend selection and write/finalize stages for troubleshooting.

## 5. Validation And Documentation

- [x] 5.1 Add tests for destination parsing, backend selection, and local backward compatibility.
- [x] 5.2 Add tests for grouped and chunked exports targeting GCS (including failure-path assertions).
- [x] 5.3 Update user documentation with GCS destination examples, credential setup, and known limitations.
