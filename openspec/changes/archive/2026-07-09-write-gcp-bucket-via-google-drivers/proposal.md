## Why

Teams running mongo2pg in cloud environments need to write export outputs directly to object storage instead of local disks. Today the tool assumes local filesystem writes, which blocks managed pipelines that require writing into Google Cloud Storage buckets, including S3-compatible workflows.

## What Changes

- Add support for writing export artifacts directly to Google Cloud Storage buckets.
- Introduce destination parsing where `base_dir` values starting with `gs://` are treated as bucket targets and all other values remain local filesystem paths.
- Add driver abstraction so storage backends can plug into existing write stages without changing export semantics.
- Add validation and error reporting for cloud write failures (auth, bucket/path missing, permission denied, transient network errors).
- Add tests covering local writes, cloud writes, and fallback/error behavior.

## Capabilities

### New Capabilities

- `gcs-object-storage-exports`: Enable mongo2pg to write export outputs to Google Cloud Storage bucket paths, including S3-style interoperability expectations.

### Modified Capabilities

- `grouped-export-table-resolution`: Ensure grouped export pipeline resolves output targets correctly when destination is a cloud object path instead of local filesystem path.

## Impact

- Affected code: export writing pipeline, destination/path parsing, configuration and CLI validation, runtime error reporting.
- Affected systems: Google Cloud Storage client integration and credentials handling.
- Dependencies: Google Cloud Storage Rust client/SDK and related auth crates.
- Operational impact: users can run cloud-native export workflows without mounting local volumes.
