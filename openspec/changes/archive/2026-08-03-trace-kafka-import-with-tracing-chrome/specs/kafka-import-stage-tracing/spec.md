## ADDED Requirements

### Requirement: Kafka import supports stage trace capture

The kafka-import command SHALL support optional trace capture using `tracing` with `tracing-chrome` output so operators can inspect stage timing in Chrome trace viewer.

#### Scenario: Tracing enabled for kafka-import

- **WHEN** kafka-import is started with tracing enabled
- **THEN** the command writes a Chrome trace artifact that contains spans for kafka-import execution

#### Scenario: Tracing disabled for kafka-import

- **WHEN** kafka-import is started without tracing enabled
- **THEN** kafka-import runs with existing behavior and no trace artifact is required

### Requirement: Kafka import emits stage spans for read, write, and commit

The kafka-import runtime SHALL emit distinct spans for message read, PostgreSQL write/apply, and offset commit stages.

#### Scenario: Message batch is processed successfully

- **WHEN** kafka-import consumes and applies a message or batch
- **THEN** the trace includes ordered stage spans for read, write, and commit with timing and success outcome

#### Scenario: Stage fails during processing

- **WHEN** a read, write, or commit stage returns an error
- **THEN** the corresponding stage span records a failure outcome and error context

### Requirement: Stage spans include tuning metadata

Each stage span MUST include enough metadata to correlate performance behavior across workers and topics.

#### Scenario: Span metadata recorded

- **WHEN** kafka-import emits a stage span
- **THEN** the span includes worker identity and stage name, and includes topic/op/batch context when available
