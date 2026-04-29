//! Schema statistics helpers: width, depth, and branch count.

use crate::analyzer::{CollectionSchema, FieldSchema};

/// Summary statistics for a [`CollectionSchema`].
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaStats {
    /// Number of top-level fields.
    pub width: usize,
    /// Maximum nesting depth (top-level fields are depth 1).
    pub depth: usize,
    /// Total number of fields across all nesting levels.
    pub branch: usize,
}

impl SchemaStats {
    /// Compute stats for the given schema.
    pub fn compute(schema: &CollectionSchema) -> Self {
        let width = schema.object.len();
        let mut total_branch = 0;
        let mut max_depth = 0;

        for field in schema.object.values() {
            let (d, b) = field_depth_branch(field, 1);
            if d > max_depth {
                max_depth = d;
            }
            total_branch += b;
        }

        SchemaStats {
            width,
            depth: max_depth,
            branch: total_branch,
        }
    }
}

/// Returns `(max_depth, branch_count)` for a field schema starting at `current_depth`.
fn field_depth_branch(field: &FieldSchema, current_depth: usize) -> (usize, usize) {
    let mut max_depth = current_depth;
    let mut branch = 1; // this field itself

    for type_schema in field.types.values() {
        if let Some(nested_obj) = &type_schema.object {
            for nested_field in nested_obj.values() {
                let (d, b) = field_depth_branch(nested_field, current_depth + 1);
                if d > max_depth {
                    max_depth = d;
                }
                branch += b;
            }
        }
        if let Some(array_items) = &type_schema.array {
            let (d, b) = field_depth_branch(array_items, current_depth + 1);
            if d > max_depth {
                max_depth = d;
            }
            branch += b;
        }
    }

    (max_depth, branch)
}

/// Format stats as human-readable lines (intended for stderr).
pub fn format_stats(schema: &CollectionSchema) -> Vec<String> {
    let s = SchemaStats::compute(schema);
    let type_summary = top_level_type_summary(schema);
    vec![
        format!("Documents sampled : {}", schema.count),
        format!("Width (top-level fields): {}", s.width),
        format!("Depth (max nesting)     : {}", s.depth),
        format!("Branch (total fields)   : {}", s.branch),
        format!("Top-level types         : {}", type_summary),
    ]
}

fn top_level_type_summary(schema: &CollectionSchema) -> String {
    let parts: Vec<String> = schema
        .object
        .iter()
        .map(|(name, field)| {
            let dominant = field
                .types
                .iter()
                .filter(|(t, _)| t.as_str() != crate::analyzer::TYPE_UNDEFINED)
                .max_by(|(_, a), (_, b)| {
                    a.count.partial_cmp(&b.count).unwrap_or(std::cmp::Ordering::Equal)
                })
                .map(|(t, _)| t.as_str())
                .unwrap_or("?");
            format!("{name}:{dominant}")
        })
        .collect();
    parts.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyzer::{CollectionSchema, FieldSchema, TypeSchema};
    use indexmap::IndexMap;

    fn simple_schema() -> CollectionSchema {
        let mut types = IndexMap::new();
        types.insert(
            "String".to_owned(),
            TypeSchema {
                count: 3,
                prop_in_types: 1.0,
                object: None,
                array: None,
                values: None,
            },
        );
        let mut object = IndexMap::new();
        object.insert(
            "name".to_owned(),
            FieldSchema {
                count: 3,
                prop_in_object: 1.0,
                types,
            },
        );
        CollectionSchema { count: 3, object }
    }

    #[test]
    fn stats_flat_schema() {
        let schema = simple_schema();
        let stats = SchemaStats::compute(&schema);
        assert_eq!(stats.width, 1);
        assert_eq!(stats.depth, 1);
        assert_eq!(stats.branch, 1);
    }
}
