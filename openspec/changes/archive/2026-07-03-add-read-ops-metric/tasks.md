## 1. Infer Telemetry Collection

- [x] 1.1 Add a collection-level helper to run MongoDB `$collStats` with latency stats and parse read operation fields.
- [x] 1.2 Integrate telemetry lookup into infer collection processing with best-effort error handling.
- [x] 1.3 Extend infer write path to pass telemetry data into stats YAML generation.

## 2. Stats Model and Serialization

- [x] 2.1 Add an optional read-ops structure to stats YAML schema with `read_ops` count and optional `since` timestamp.
- [x] 2.2 Update stats serialization API and all call sites to include optional telemetry input.
- [x] 2.3 Update/adjust stats unit tests for the new optional field behavior.

## 3. Report Rendering

- [x] 3.1 Extend report stats deserialization model to read optional read-ops telemetry.
- [x] 3.2 Render read-ops text near the Documents value in single-database report rows.
- [x] 3.3 Render read-ops text near the Documents value in multi-database per-collection report rows.

## 4. Verification

- [x] 4.1 Add a report rendering test that asserts read-ops metadata appears when present.
- [x] 4.2 Run focused tests for infer/stats/report changes and ensure no regressions in existing report behavior.
- [ ] 4.3 Execute infer + report on sample data and verify `results/<db>/reports/main.html` shows read-ops metadata beside Documents.
