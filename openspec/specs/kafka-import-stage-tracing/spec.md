## MODIFIED Requirements

### Requirement: Kafka import emits stage spans for read, write, and commit

The kafka-import runtime SHALL emit distinct spans for message read, PostgreSQL write/apply, and offset commit stages, and this behavior SHALL be preserved when runtime code is moved into an extracted module.

#### Scenario: Message batch is processed successfully

- **WHEN** kafka-import consumes and applies a message or batch
- **THEN** the trace includes ordered stage spans for read, write, and commit with timing and success outcome

#### Scenario: Stage fails during processing

- **WHEN** a read, write, or commit stage returns an error
- **THEN** the corresponding stage span records a failure outcome and error context

#### Scenario: Stage tracing parity after module extraction

- **WHEN** kafka-import is executed after extracting runtime code from `mongo2pg.rs`
- **THEN** trace span names and stage-level correlation metadata remain equivalent to pre-extraction behavior
