# runtime-log-format Specification

## Purpose

Define requirements for CLI runtime log line formatting precision so timestamp and elapsed values remain readable and consistent.

## Requirements

### Requirement: Runtime log timestamp uses second precision

The CLI runtime logger SHALL emit the timestamp field using second precision in `YYYY-MM-DDTHH:MM:SS` format.

#### Scenario: Log line timestamp rendering

- **WHEN** a runtime log line is formatted
- **THEN** the timestamp omits fractional seconds and timezone suffix

### Requirement: Runtime elapsed value uses whole seconds

The CLI runtime logger SHALL emit elapsed duration as whole seconds in `+<N>s` form.

#### Scenario: Elapsed rendering from non-integer duration

- **WHEN** elapsed duration includes fractional seconds
- **THEN** the formatted elapsed value contains only integer seconds with `s` suffix
