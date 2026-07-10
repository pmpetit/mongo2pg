## 1. Export Chunking Foundation

- [x] 1.1 Identify current in-memory row accumulation structures in export path and introduce chunk buffer abstraction per target table.
- [x] 1.2 Implement incremental flush flow that writes chunk buffers to CSV output without waiting for full collection completion.
- [x] 1.3 Ensure chunk buffers are cleared/dropped immediately after successful flush to release memory.

## 2. Configuration And Validation

- [x] 2.1 Add export chunk-size configuration surface (CLI and/or config) with conservative default value.
- [x] 2.2 Validate chunk-size bounds and return clear errors for invalid values.
- [x] 2.3 Document runtime behavior for default and custom chunk sizes in command help or usage docs.

## 3. Grouped Export Compatibility

- [x] 3.1 Integrate chunked writes with grouped SQL lookup flow so grouped sources still target one resolved table output.
- [x] 3.2 Guarantee grouped chunk flushes append into shared target CSV without truncation/overwrite across source collections.
- [x] 3.3 Keep non-grouped export behavior unchanged except for memory-bounded chunk processing.

## 4. Verification And Regression Coverage

- [x] 4.1 Add tests covering chunked export correctness for non-grouped tables (row counts, schema/order compatibility).
- [x] 4.2 Add tests covering grouped chunked exports to confirm one consolidated output file with rows from multiple sources.
- [x] 4.3 Add or update stress/integration validation to confirm bounded memory behavior and prevent OOM regressions.
