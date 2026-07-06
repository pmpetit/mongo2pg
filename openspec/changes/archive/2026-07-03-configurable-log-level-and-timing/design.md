## Context

mongo2pg currently emits many runtime messages through direct stderr prints, with no centralized level filtering and no unified timestamp/elapsed-time context. This limits observability in long-running operations and makes troubleshooting inconsistent across infer, export, import, and Kafka flows.

## Goals / Non-Goals

**Goals:**

- Provide configurable runtime log level control for operational noise reduction and deep debugging.
- Add log timestamps and elapsed time since process start to runtime logs.
- Centralize logger setup so all subcommands share consistent formatting and filtering.

**Non-Goals:**

- Replace every user-facing progress/status line with machine-structured logs in one change.
- Introduce distributed tracing, external log sinks, or telemetry backends.
- Change business logic in infer/export/import beyond logging behavior.

## Decisions

- Decision: Introduce centralized logger initialization early in CLI startup.
  - Rationale: one initialization point avoids drift between subcommands and test paths.
  - Alternative considered: leave per-module logging behavior; rejected due to inconsistent formatting and duplicated filtering logic.
- Decision: Support configurable log level via CLI/config with clear precedence.
  - Rationale: operators need temporary overrides without editing config files.
  - Alternative considered: environment-variable-only control; rejected because explicit project/CLI configuration is easier to document and repeat.
- Decision: Include both wall-clock timestamp and elapsed duration in log output.
  - Rationale: timestamp aids cross-system correlation, elapsed time aids stage performance diagnosis.
  - Alternative considered: elapsed-only or timestamp-only formatting; rejected because each serves different operational needs.

## Risks / Trade-offs

- [Risk] Mixed legacy eprintln and new logger output could create inconsistent formatting during transition → Mitigation: prioritize high-value runtime paths first and define consistent migration pattern.
- [Risk] Too-verbose debug/trace output can affect performance or readability → Mitigation: default to info-level behavior and expose lower-noise level options.
- [Risk] Config/CLI precedence confusion for log level → Mitigation: document deterministic precedence and add unit tests around resolution logic.

## Migration Plan

- Add logger configuration fields/options and default behavior equivalent to current user experience.
- Initialize logger once at startup and route key runtime logs through it.
- Verify output format includes timestamp and elapsed time and respects configured log level.
- Rollback by disabling new logger wiring and returning to prior stderr behavior if needed.

## Open Questions

- Which timestamp format should be default (RFC3339 local/UTC) for best compatibility with existing operator tooling?
- Should progress-only lines stay always visible regardless of configured log level, or map strictly to info level?
