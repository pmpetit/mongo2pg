## 1. CLI and Command Wiring

- [x] 1.1 Add `Ping` variant to CLI command enum and define `PingArgs` with `-c/--config`, `--source`, `--target`, `--kafka`
- [x] 1.2 Add argument validation to require at least one backend selection flag for `ping`
- [x] 1.3 Wire command dispatch in `main` to `run_ping(args)`

## 2. Backend Connectivity Checks

- [x] 2.1 Implement MongoDB ping helper using configured source URI and minimal backend operation
- [x] 2.2 Implement PostgreSQL ping helper using configured target URI and `SELECT 1`
- [x] 2.3 Implement Kafka ping helper using configured Kafka section and bounded metadata fetch

## 3. Output, Exit Behavior, and Attribution

- [x] 3.1 Emit per-backend pass/fail output for each selected backend
- [x] 3.2 Return exit code 0 only when all selected backend checks succeed; non-zero when any fail
- [x] 3.3 Ensure failures use backend-attributed error context and preserve root-cause details

## 4. Testing

- [x] 4.1 Add tests for ping CLI parsing and validation (flag combinations, missing flags)
- [x] 4.2 Add tests for ping result aggregation behavior (all-pass vs any-fail)
- [x] 4.3 Add tests or assertions covering backend attribution in ping failure output paths

## 5. Documentation

- [x] 5.1 Update reference docs with `mongo2pg ping` usage and backend flags
- [x] 5.2 Add examples for `ping --source`, `ping --target`, and `ping --kafka`
