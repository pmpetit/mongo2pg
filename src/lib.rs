//! `mongo2pg` – Schema inference and conversion library.
//!
//! Samples documents from a MongoDB collection, infers a probabilistic schema,
//! and exports it in the **expanded** JSON Schema dialect:
//!
//! * `x-bsonType` – the internal BSON type name
//! * `x-metadata` – `{ "count", "prob" }` per field/type
//! * `x-sampleValues` – reservoir-sampled values when available

pub mod analyzer;
pub mod converters;
pub mod report;
pub mod schema_diagram;
pub mod stats;
pub mod to_pg;
