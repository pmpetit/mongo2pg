## ADDED Requirements

### Requirement: Export destination SHALL support GCS bucket URIs

The export command MUST route artifact writes to Google Cloud Storage when `base_dir` starts with `gs://`, using the remainder as bucket/object prefix.

#### Scenario: base_dir with gs:// selects GCS backend

- **WHEN** user sets `base_dir` to a valid `gs://` URI
- **THEN** export MUST initialize the GCS storage backend instead of local filesystem backend

#### Scenario: base_dir without gs:// uses local backend

- **WHEN** user sets `base_dir` to a path that does not start with `gs://`
- **THEN** export MUST initialize local filesystem backend

### Requirement: GCS writes SHALL preserve artifact naming and format

The system MUST preserve existing export artifact naming and compression conventions when writing to GCS so downstream import behavior remains consistent.

#### Scenario: Grouped artifact naming preserved in object key

- **WHEN** grouped export writes target table output to GCS
- **THEN** object key MUST use the same grouped artifact filename convention used for local exports
- **AND** artifact content format MUST match local export format

### Requirement: GCS export SHALL provide categorized write errors

The system MUST report write failures with actionable categories including authentication, authorization, not found, and transient network failures.

#### Scenario: Authentication failure is reported with category

- **WHEN** export cannot obtain valid credentials for GCS writes
- **THEN** command MUST fail with an authentication category error
- **AND** error output MUST include guidance to validate configured credentials

#### Scenario: Permission failure is reported with category

- **WHEN** credentials are valid but bucket/object write is denied
- **THEN** command MUST fail with an authorization category error
- **AND** error output MUST include bucket path context for troubleshooting
