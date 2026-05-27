//! Helper to count the number of PG tables for a schema (for scoring).

use crate::analyzer::CollectionSchema;
use crate::to_pg::{collect_tables, process_fields, Table};

pub fn pg_table_count(schema: &CollectionSchema) -> usize {
    // Use a generic name since CollectionSchema does not have a name field
    let mut root = Table::new("root".to_string());
    process_fields(&mut root, &schema.object, "", true, None);
    collect_tables(&root).len()
}
