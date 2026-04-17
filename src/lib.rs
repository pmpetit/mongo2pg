//! `mongodb_to_pg` – Schema inference and conversion library.
//!
//! Samples documents from a MongoDB collection, infers a probabilistic schema,
//! and exports it in three JSON Schema dialects:
//!
//! * **mongodb** – MongoDB JSON Schema (`bsonType`, `properties`, `required`, `anyOf`)
//! * **standard** – JSON Schema draft 2020-12 (`$schema`, `$defs`)
//! * **expanded** – Extended schema with `x-bsonType`, `x-metadata`, `x-sampleValues`

pub mod analyzer;
pub mod converters;
pub mod semantic_types;
pub mod stats;
