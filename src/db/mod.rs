//! Connector boundary: isolates external system interactions (MongoDB, PostgreSQL, Kafka)
//! behind dedicated modules so pure `engine` logic stays free of client/network dependencies.
//!
//! See `openspec/changes/refactor-cli-commands-db-engine-layout/design.md` for the migration plan.

pub mod kafka;
pub mod mongo;
pub mod pg;
