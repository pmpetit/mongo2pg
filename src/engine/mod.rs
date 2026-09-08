//! Pure, deterministic business logic (schema analysis, DDL generation, checksum
//! computation, stats, and mapping). Engine modules do not depend on connector client
//! crates (MongoDB/PostgreSQL/Kafka), so they can be unit tested without live systems.
//!
//! `checksum` is an exception: computing a checksum inherently requires reading data
//! from MongoDB/PostgreSQL, so it depends on `db::mongo`/`db::pg` for connections.
//!
//! See `openspec/changes/refactor-cli-commands-db-engine-layout/design.md` for the migration plan.

pub mod analyzer;
pub mod checksum;
pub mod ddl;
pub mod mapping;
pub mod stats;

