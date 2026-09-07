## MODIFIED Requirements

### Requirement: Attribution appears in command-visible failures

Attribution MUST be visible in user-facing command failure output for all relevant commands that use each backend.

#### Scenario: Command fails with attributed backend

- **WHEN** `infer`, `export`, `import`, `report --post-import`, `kafka-import`, or `ping` fails due to backend connectivity
- **THEN** command output includes explicit backend attribution identifying which dependency failed
