## Why

Current CLI logging is mostly plain eprintln output with fixed verbosity, which makes debugging noisy in normal runs and too limited in troubleshooting runs. Operators need configurable log levels plus timestamped and elapsed-time context to diagnose long-running stages such as infer, export, and import.

## What Changes

- Add configurable log level control for runtime logs (for example: error, warn, info, debug, trace).
- Add timestamp output for log lines and include elapsed duration from process start.
- Standardize logger initialization so subcommands share one formatting and filtering path.
- Keep user-facing progress messages usable while integrating them into structured log behavior.

## Capabilities

### New Capabilities

- `runtime-logging-controls`: Configure log verbosity and include timestamp plus elapsed time in runtime logs.

### Modified Capabilities

- None.

## Impact

- Affected code: CLI entrypoint initialization and runtime logging call sites.
- Configuration and CLI surface: new log-level option and/or config key wiring.
- Operations: easier troubleshooting and performance triage with timed log output.
