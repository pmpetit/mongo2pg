## 1. Logging Configuration Surface

- [x] 1.1 Add log-level configuration fields/arguments with deterministic precedence between CLI and config
- [x] 1.2 Validate accepted log-level values and define default behavior that preserves current operator experience
- [x] 1.3 Add/update docs or help text for log-level configuration usage

## 2. Logger Initialization and Formatting

- [x] 2.1 Add centralized logger initialization in CLI startup path shared by subcommands
- [x] 2.2 Configure log formatter to include wall-clock timestamp and elapsed time since process start
- [x] 2.3 Ensure infer/export/import/Kafka runtime paths use the shared logger formatting and filtering behavior

## 3. Runtime Message Migration

- [x] 3.1 Migrate high-value runtime diagnostics from direct stderr prints to leveled logger calls
- [x] 3.2 Keep progress/status lines readable while aligning them with selected log-level policy
- [x] 3.3 Add explicit tests for timestamp/elapsed formatting and level filtering behavior

## 4. Verification

- [x] 4.1 Add unit tests for log-level precedence resolution (CLI over config, config over default)
- [x] 4.2 Add command-level smoke tests to verify consistent log formatting across key subcommands
- [x] 4.3 Run build and relevant test suites to confirm no regressions
