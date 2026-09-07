## Why

The checkmd5 workflow currently lacks a clear trace in the report command when --post-import is used, which makes post-import validation progress and troubleshooting harder. We need explicit report-time tracing so operators can confirm when post-import checkmd5 reporting is running and what stage it reached.

## What Changes

- Add report command tracing for --post-import execution paths used during checkmd5 processing.
- Include stage-level trace messages that make post-import report activity visible in logs and generated reporting context.
- Ensure traces are emitted consistently for success and failure paths in the post-import checkmd5 flow.

## Capabilities

### New Capabilities

- None.

### Modified Capabilities

- `collection-read-ops-reporting`: extend reporting requirements to include explicit trace visibility for report --post-import behavior in checkmd5 workflows.

## Impact

- Affected code in report/checkmd5 command flow and related logging/reporting helpers.
- No external API contract changes expected.
- Improves operator observability for post-import validation runs.
