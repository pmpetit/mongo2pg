## 1. Infer Timeout Plumbing

- [x] 1.1 Locate infer sampling and fallback query builders in Rust source and identify all MongoDB read entry points used during infer
- [x] 1.2 Thread `source.max_time_ms` through infer execution context so both aggregate and fallback find paths can access one timeout value
- [x] 1.3 Keep behavior unchanged when `source.max_time_ms` is unset (no forced timeout)

## 2. Apply Timeout to Infer Reads

- [x] 2.1 Set `maxTimeMS` on `$sample` aggregate commands used by infer
- [x] 2.2 Set `maxTimeMS` on sequential fallback `find().limit(...)` commands used after sample failure
- [x] 2.3 Ensure timeout units/types match MongoDB driver expectations and existing config parsing semantics

## 3. Observability and Error Handling

- [x] 3.1 Update infer warning output to explicitly indicate timeout-driven fallback when command code 50 / `MaxTimeMSExpired` is encountered
- [x] 3.2 Preserve existing non-timeout fallback handling and avoid introducing hard-fail regressions

## 4. Validation

- [x] 4.1 Add or update tests to verify infer applies configured `max_time_ms` to sampling and fallback read paths
- [x] 4.2 Add or update tests to verify warning output includes timeout context for `MaxTimeMSExpired`
- [x] 4.3 Run relevant test/build commands and confirm no regressions in infer workflow
