//! Schema statistics helpers: width, depth, and branch count.

use serde::Serialize;

use crate::analyzer::{CollectionSchema, FieldSchema};

/// Summary statistics for a [`CollectionSchema`].
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaStats {
    /// Number of top-level fields.
    pub width: usize,
    /// Maximum number of fields at any single nesting level.
    pub max_width: usize,
    /// The nesting level (1-based) where max_width was observed.
    pub max_width_level: usize,
    /// Maximum nesting depth (top-level fields are depth 1).
    pub depth: usize,
    /// Total number of fields across all nesting levels.
    pub branch: usize,
    /// Number of fields at each nesting level (index 0 → level 1, index 1 → level 2, …).
    pub branches_by_level: Vec<usize>,
}

impl SchemaStats {
    /// Compute stats for the given schema.
    pub fn compute(schema: &CollectionSchema) -> Self {
        let width = schema.object.len();
        let mut total_branch = 0;
        let mut max_depth = 0;
        let mut level_counts: Vec<usize> = Vec::new();

        for field in schema.object.values() {
            let (d, b) = field_depth_branch(field, 1, &mut level_counts);
            if d > max_depth {
                max_depth = d;
            }
            total_branch += b;
        }

        let (max_width, max_width_level) = level_counts
            .iter()
            .copied()
            .enumerate()
            .max_by_key(|&(_, c)| c)
            .map(|(i, c)| (c, i + 1))
            .unwrap_or((width, 1));

        SchemaStats {
            width,
            max_width,
            max_width_level,
            depth: max_depth,
            branch: total_branch,
            branches_by_level: level_counts,
        }
    }
}

/// Returns `(max_depth, branch_count)` for a field schema starting at `current_depth`,
/// and accumulates the per-level field count into `level_counts`.
fn field_depth_branch(
    field: &FieldSchema,
    current_depth: usize,
    level_counts: &mut Vec<usize>,
) -> (usize, usize) {
    let mut max_depth = current_depth;
    let mut branch = 1; // this field itself

    // Ensure the vec is large enough for this level (1-indexed, stored at index depth-1).
    let idx = current_depth - 1;
    if level_counts.len() <= idx {
        level_counts.resize(current_depth, 0);
    }
    level_counts[idx] += 1;

    for type_schema in field.types.values() {
        if let Some(nested_obj) = &type_schema.object {
            for nested_field in nested_obj.values() {
                let (d, b) = field_depth_branch(nested_field, current_depth + 1, level_counts);
                if d > max_depth {
                    max_depth = d;
                }
                branch += b;
            }
        }
        if let Some(array_items) = &type_schema.array {
            let (d, b) = field_depth_branch(array_items, current_depth + 1, level_counts);
            if d > max_depth {
                max_depth = d;
            }
            branch += b;
        }
    }

    (max_depth, branch)
}

/// Format stats as human-readable lines (intended for stderr).
///
/// `total_docs` is the actual collection size from MongoDB; pass `None` when unavailable.
pub fn format_stats(schema: &CollectionSchema, total_docs: Option<u64>) -> Vec<String> {
    let s = SchemaStats::compute(schema);
    let type_summary = top_level_type_summary(schema);
    let branch_by_level: String = s
        .branches_by_level
        .iter()
        .enumerate()
        .map(|(i, &c)| format!("L{}:{}", i + 1, c))
        .collect::<Vec<_>>()
        .join("  ");
    let total_line = match total_docs {
        Some(n) => format!("Documents in collection : {}", n),
        None => "Documents in collection : (unknown)".to_owned(),
    };
    vec![
        total_line,
        format!("Documents sampled       : {}", schema.sampled),
        format!(
            "Width (top-level / max)  : {} / {} (L{})",
            s.width, s.max_width, s.max_width_level
        ),
        format!("Depth (max nesting)     : {}", s.depth),
        format!("Branch (per level)      : {}", branch_by_level),
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
                    a.probability
                        .partial_cmp(&b.probability)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .map(|(t, _)| t.as_str())
                .unwrap_or("?");
            format!("{name}:{dominant}")
        })
        .collect();
    parts.join(", ")
}

/// Structured stats suitable for YAML serialisation.
#[derive(Debug, Serialize)]
pub struct StatsYaml {
    pub documents_in_collection: serde_yaml::Value,
    pub documents_sampled: u64,
    pub width_top_level: usize,
    pub width_max: usize,
    pub width_max_level: usize,
    pub depth_max: usize,
    pub branch_total: usize,
    pub branch_per_level: indexmap::IndexMap<String, usize>,
    pub top_level_types: indexmap::IndexMap<String, String>,
}

/// Build a [`StatsYaml`] from a schema.
pub fn stats_to_yaml(schema: &CollectionSchema, total_docs: Option<u64>) -> StatsYaml {
    let s = SchemaStats::compute(schema);

    let branch_per_level: indexmap::IndexMap<String, usize> = s
        .branches_by_level
        .iter()
        .enumerate()
        .map(|(i, &c)| (format!("L{}", i + 1), c))
        .collect();

    let top_level_types: indexmap::IndexMap<String, String> = schema
        .object
        .iter()
        .map(|(name, field)| {
            let dominant = field
                .types
                .iter()
                .filter(|(t, _)| t.as_str() != crate::analyzer::TYPE_UNDEFINED)
                .max_by(|(_, a), (_, b)| {
                    a.probability
                        .partial_cmp(&b.probability)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .map(|(t, _)| t.clone())
                .unwrap_or_else(|| "?".to_owned());
            (name.clone(), dominant)
        })
        .collect();

    StatsYaml {
        documents_in_collection: match total_docs {
            Some(n) => serde_yaml::Value::Number(n.into()),
            None => serde_yaml::Value::String("unknown".to_owned()),
        },
        documents_sampled: schema.sampled,
        width_top_level: s.width,
        width_max: s.max_width,
        width_max_level: s.max_width_level,
        depth_max: s.depth,
        branch_total: s.branch,
        branch_per_level,
        top_level_types,
    }
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
                probability: 1.0,
                sampled: 0,
                as_jsonb: false,
                ndistinct: None,
                object: None,
                array: None,
                values: None,
            },
        );
        let mut object = IndexMap::new();
        object.insert(
            "name".to_owned(),
            FieldSchema {
                probability: 1.0,
                types,
            },
        );
        CollectionSchema {
            count: 3,
            sampled: 3,
            object,
        }
    }

    #[test]
    fn stats_flat_schema() {
        let schema = simple_schema();
        let stats = SchemaStats::compute(&schema);
        assert_eq!(stats.width, 1);
        assert_eq!(stats.depth, 1);
        assert_eq!(stats.branch, 1);
        assert_eq!(stats.branches_by_level, vec![1]);
    }
}
