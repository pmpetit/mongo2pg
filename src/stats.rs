//! Schema statistics helpers: width, depth, and branch count.

use crate::pg_table_count::pg_table_count;
use serde::{Deserialize, Serialize};

use crate::analyzer::{CollectionSchema, FieldSchema, TYPE_ARRAY};

/// Summary statistics for a [`CollectionSchema`].
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaStats {
    /// Number of top-level fields.
    pub width: usize,
    /// Maximum expected fields per document at any single nesting level (probability-weighted).
    pub max_width: f64,
    /// The nesting level (1-based) where max_width was observed.
    pub max_width_level: usize,
    /// Maximum nesting depth (top-level fields are depth 1).
    pub depth: usize,
    /// Expected total fields per document across all nesting levels (probability-weighted sum).
    pub branch: f64,
    /// Expected fields per document at each nesting level (probability-weighted; index 0 → level 1, …).
    pub branches_by_level: Vec<f64>,
    /// Average number of fields present per document (sum of field probabilities).
    pub avg_fields_per_doc: f64,
    /// Number of top-level fields that have an Array type.
    pub array_field_count: usize,
    /// Maximum polymorphism ratio (`distinct_fields / avg_fields_per_doc`) observed at any
    /// nesting level (including the top level). 1.0 = every document has all fields (dense);
    /// higher values indicate sparsity. This is used as the `poly_term` in the score.
    pub max_poly_ratio: f64,
    /// The nesting level (1-based) where `max_poly_ratio` was observed.
    pub max_poly_level: usize,
}

impl SchemaStats {
    /// Compute stats for the given schema.
    pub fn compute(schema: &CollectionSchema) -> Self {
        let width = schema.object.len();
        let mut total_branch: f64 = 0.0;
        let mut max_depth = 0;
        let mut level_counts: Vec<f64> = Vec::new();

        for field in schema.object.iter() {
            let (d, b) = field_depth_branch(field.1, 1, &mut level_counts, field.1.probability);
            if d > max_depth {
                max_depth = d;
            }
            total_branch += b;
        }

        let (max_width, max_width_level) = level_counts
            .iter()
            .copied()
            .enumerate()
            .max_by(|&(_, a), &(_, b)| a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(i, c)| (c, i + 1))
            .unwrap_or((width as f64, 1));

        // avg_fields_per_doc = sum of field probabilities (each field contributes its
        // presence probability to the expected key count per document).
        let avg_fields_per_doc: f64 = schema.object.values().map(|f| f.probability).sum();

        // Count top-level fields that have at least one Array type observation.
        let array_field_count = schema
            .object
            .values()
            .filter(|f| f.types.contains_key(TYPE_ARRAY))
            .count();

        let (max_poly_ratio, max_poly_level) = compute_max_poly_ratio(schema);

        SchemaStats {
            width,
            max_width,
            max_width_level,
            depth: max_depth,
            branch: total_branch,
            branches_by_level: level_counts,
            avg_fields_per_doc,
            array_field_count,
            max_poly_ratio,
            max_poly_level,
        }
    }

    /// Per-collection migrability complexity score.
    ///
    /// ```text
    /// C = depth_max / 2  +  array_fields  +  max_poly_ratio
    /// ```
    ///
    /// * `depth_max / 2`   – penalises nesting (halved so it doesn't dominate)
    /// * `array_fields`    – each array field adds a child table
    /// * `max_poly_ratio`  – maximum `distinct_fields / avg_fields_per_doc` across
    ///                       all nesting levels. 1.0 = perfectly flat; higher values
    ///                       indicate sparsity/polymorphism. Previously only the
    ///                       top-level ratio was used, which severely underestimated
    ///                       collections with sparse nested objects (e.g. audit-log
    ///                       documents where `newValues`/`oldValues` can hold any
    ///                       subset of hundreds of possible sub-fields).
    pub fn migrability_score(&self, pg_tables: usize) -> f64 {
        let depth_term = self.depth as f64 / 2.0;
        let array_term = self.array_field_count as f64;
        let poly_term = if self.max_poly_ratio > 0.0 {
            pg_tables as f64 / self.max_poly_ratio
        } else {
            pg_tables as f64
        };
        depth_term + array_term + poly_term
    }
}

/// Traverse all nested Object schemas (skipping `as_jsonb` fields) and return the
/// maximum `distinct / avg` polymorphism ratio found, together with its nesting level.
///
/// The top-level ratio is included as the baseline.
fn compute_max_poly_ratio(schema: &CollectionSchema) -> (f64, usize) {
    let avg_top: f64 = schema.object.values().map(|f| f.probability).sum();
    let ratio_top = if avg_top > 0.0 {
        schema.object.len() as f64 / avg_top
    } else {
        0.0
    };
    let mut max_ratio = ratio_top;
    let mut max_level = 1usize;

    for field in schema.object.values() {
        visit_field_poly(field, 2, &mut max_ratio, &mut max_level);
    }
    (max_ratio, max_level)
}

/// Recursively visit a [`FieldSchema`] and update `max_ratio` / `max_level` whenever
/// a nested Object sub-schema has a higher `distinct / avg` ratio.
fn visit_field_poly(field: &FieldSchema, level: usize, max_ratio: &mut f64, max_level: &mut usize) {
    for type_schema in field.types.values() {
        // Skip objects that will be stored as JSONB – their internal structure is opaque.
        if !type_schema.as_jsonb {
            if let Some(obj) = &type_schema.object {
                if !obj.is_empty() {
                    let distinct = obj.len() as f64;
                    let avg: f64 = obj.values().map(|f| f.probability).sum();
                    if avg > 0.0 {
                        let ratio = distinct / avg;
                        if ratio > *max_ratio {
                            *max_ratio = ratio;
                            *max_level = level;
                        }
                    }
                    for nested_field in obj.values() {
                        visit_field_poly(nested_field, level + 1, max_ratio, max_level);
                    }
                }
            }
        }
    }
}

/// Returns `(max_depth, weighted_branch_count)` for a field schema starting at `current_depth`.
///
/// `weight` is the cumulative probability of this field being present in a document
/// (top-level field probability × parent type probability × …). Using probability weights
/// means `level_counts` and `branch` reflect *expected fields per document* rather than
/// *total distinct fields observed*, which avoids inflated counts from map-pattern fields
/// (e.g. UUID keys where each key appears in only a fraction of documents).
fn field_depth_branch(
    field: &FieldSchema,
    current_depth: usize,
    level_counts: &mut Vec<f64>,
    weight: f64,
) -> (usize, f64) {
    let mut max_depth = current_depth;
    let mut branch = weight; // this field contributes its probability weight

    // Ensure the vec is large enough for this level (1-indexed, stored at index depth-1).
    let idx = current_depth - 1;
    if level_counts.len() <= idx {
        level_counts.resize(current_depth, 0.0);
    }
    level_counts[idx] += weight;

    for type_schema in field.types.values() {
        // Object fields marked as_jsonb become a single opaque JSONB column —
        // their nested content is never traversed relationally, so don't count
        // that nesting toward depth or branch.
        if !type_schema.as_jsonb {
            if let Some(nested_obj) = &type_schema.object {
                for nested_field in nested_obj.values() {
                    // Weight = parent field weight × this type's probability × sub-field's probability
                    let nested_weight = weight * type_schema.probability * nested_field.probability;
                    let (d, b) = field_depth_branch(
                        nested_field,
                        current_depth + 1,
                        level_counts,
                        nested_weight,
                    );
                    if d > max_depth {
                        max_depth = d;
                    }
                    branch += b;
                }
            }
        }
        if let Some(array_items) = &type_schema.array {
            // Array items are counted as one logical entry; weight by the array type probability.
            let array_weight = weight * type_schema.probability;
            let (d, b) =
                field_depth_branch(array_items, current_depth + 1, level_counts, array_weight);
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
        .map(|(i, &c)| format!("L{}:{:.1}", i + 1, c))
        .collect::<Vec<_>>()
        .join("  ");
    let total_line = match total_docs {
        Some(n) => format!("Documents in collection : {}", n),
        None => "Documents in collection : (unknown)".to_owned(),
    };
    let distinct_over_avg = if s.avg_fields_per_doc > 0.0 {
        s.width as f64 / s.avg_fields_per_doc
    } else {
        0.0
    };
    let pg_tables = pg_table_count(schema);
    vec![
        total_line,
        format!("Documents sampled       : {}", schema.sampled),
        format!(
            "Width (top-level / max)  : {} / {:.1} (L{})",
            s.width, s.max_width, s.max_width_level
        ),
        format!("Depth (max nesting)     : {}", s.depth),
        format!("Branch (per level)      : {}", branch_by_level),
        format!("Avg fields / doc        : {:.4}", s.avg_fields_per_doc),
        format!("Distinct fields / avg    : {:.4}", distinct_over_avg),
        format!(
            "Max poly ratio           : {:.4} (L{})",
            s.max_poly_ratio, s.max_poly_level
        ),
        format!(
            "Migrability score       : {:.2}",
            s.migrability_score(pg_tables)
        ),
        format!("PG tables               : {}", pg_tables),
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InferWarningYaml {
    #[serde(default = "default_infer_warning_kind")]
    pub kind: String,
    pub field_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub renamed_to: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keyword: Option<String>,
    pub dominant_family: String,
    pub dominant_ratio: f64,
    pub minority_families: Vec<InferWarningMinorityYaml>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub observed_types: Vec<InferWarningTypeYaml>,
}

fn default_infer_warning_kind() -> String {
    "mixed_scalar_types".to_owned()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InferWarningMinorityYaml {
    pub family: String,
    pub ratio: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InferWarningTypeYaml {
    pub type_name: String,
    pub ratio: f64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub examples: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct StatsYaml {
    pub documents_in_collection: serde_yaml::Value,
    pub documents_sampled: u64,
    pub width_top_level: usize,
    pub width_max: f64,
    pub width_max_level: usize,
    pub depth_max: usize,
    pub branch_total: f64,
    pub branch_per_level: indexmap::IndexMap<String, f64>,
    pub top_level_types: indexmap::IndexMap<String, String>,
    /// Number of top-level fields that carry at least one Array value.
    pub array_field_count: usize,
    /// Average number of fields present per document (sum of field probabilities).
    pub avg_fields_per_doc: f64,
    /// Ratio of distinct top-level fields to average fields per document
    /// (`width / avg_fields_per_doc`; `0` when `avg_fields_per_doc == 0`).
    pub distinct_fields_over_avg_fields_per_doc: f64,
    /// Maximum polymorphism ratio (`distinct / avg`) across all nesting levels.
    pub max_poly_ratio: f64,
    /// The nesting level (1-based) where `max_poly_ratio` was observed.
    pub max_poly_level: usize,
    /// Per-collection migrability complexity score.
    pub migrability_score: f64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub infer_warnings: Vec<InferWarningYaml>,
}

/// Build a [`StatsYaml`] from a schema.
pub fn stats_to_yaml(
    schema: &CollectionSchema,
    total_docs: Option<u64>,
    infer_warnings: &[InferWarningYaml],
) -> StatsYaml {
    let s = SchemaStats::compute(schema);

    let branch_per_level: indexmap::IndexMap<String, f64> = s
        .branches_by_level
        .iter()
        .enumerate()
        .map(|(i, &c)| (format!("L{}", i + 1), (c * 100.0).round() / 100.0))
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

    let pg_tables = pg_table_count(schema);
    let score = (s.migrability_score(pg_tables) * 100.0).round() / 100.0;
    let distinct_over_avg = if s.avg_fields_per_doc > 0.0 {
        (s.width as f64 / s.avg_fields_per_doc * 10000.0).round() / 10000.0
    } else {
        0.0
    };

    StatsYaml {
        documents_in_collection: match total_docs {
            Some(n) => serde_yaml::Value::Number(n.into()),
            None => serde_yaml::Value::String("unknown".to_owned()),
        },
        documents_sampled: schema.sampled,
        width_top_level: s.width,
        width_max: (s.max_width * 100.0).round() / 100.0,
        width_max_level: s.max_width_level,
        depth_max: s.depth,
        branch_total: (s.branch * 100.0).round() / 100.0,
        branch_per_level,
        top_level_types,
        array_field_count: s.array_field_count,
        avg_fields_per_doc: (s.avg_fields_per_doc * 100.0).round() / 100.0,
        distinct_fields_over_avg_fields_per_doc: distinct_over_avg,
        max_poly_ratio: (s.max_poly_ratio * 10000.0).round() / 10000.0,
        max_poly_level: s.max_poly_level,
        migrability_score: score,
        infer_warnings: infer_warnings.to_vec(),
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
                max_length: None,
                varchar_length: None,
                type_name: "Undefined".to_owned(),
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
        assert_eq!(stats.branch, 1.0);
        assert_eq!(stats.branches_by_level, vec![1.0]);
    }

    // Build a schema where two fields are always present (probability 1.0).
    fn two_field_schema() -> CollectionSchema {
        let make_field = |p: f64| {
            let mut types = IndexMap::new();
            types.insert(
                "String".to_owned(),
                TypeSchema {
                    probability: p,
                    sampled: 0,
                    as_jsonb: false,
                    ndistinct: None,
                    object: None,
                    array: None,
                    values: None,
                    max_length: None,
                    varchar_length: None,
                    type_name: "Undefined".to_owned(),
                },
            );
            FieldSchema {
                probability: p,
                types,
            }
        };
        let mut object = IndexMap::new();
        object.insert("a".to_owned(), make_field(1.0));
        object.insert("b".to_owned(), make_field(0.5)); // expected to be present in 50% of docs (probability 0.5)
        CollectionSchema {
            count: 2,
            sampled: 2,
            object,
        }
    }

    #[test]
    fn stats_to_yaml_includes_distinct_over_avg() {
        let schema = two_field_schema();
        // width=2, avg_fields_per_doc = 1.0 + 0.5 = 1.5
        // distinct_over_avg = 2 / 1.5 ≈ 1.3333
        let yaml = stats_to_yaml(&schema, Some(2), &[]);
        let expected = (2.0_f64 / 1.5 * 10000.0).round() / 10000.0;
        assert!(
            (yaml.distinct_fields_over_avg_fields_per_doc - expected).abs() < 1e-9,
            "expected {expected}, got {}",
            yaml.distinct_fields_over_avg_fields_per_doc
        );
    }

    #[test]
    fn stats_to_yaml_distinct_over_avg_zero_when_no_avg() {
        // A schema with no fields → avg_fields_per_doc == 0 → field must be 0.
        let empty = CollectionSchema {
            count: 0,
            sampled: 0,
            object: IndexMap::new(),
        };
        let yaml = stats_to_yaml(&empty, None, &[]);
        assert_eq!(
            yaml.distinct_fields_over_avg_fields_per_doc, 0.0,
            "should be 0 when avg_fields_per_doc is 0"
        );
    }

    #[test]
    fn stats_to_yaml_preserves_infer_warnings() {
        let schema = two_field_schema();
        let warnings = vec![InferWarningYaml {
            kind: "mixed_scalar_types".to_owned(),
            field_path: "advices[].earnings.monthly_gain".to_owned(),
            renamed_to: None,
            keyword: None,
            dominant_family: "numeric".to_owned(),
            dominant_ratio: 0.95,
            minority_families: vec![InferWarningMinorityYaml {
                family: "string".to_owned(),
                ratio: 0.05,
            }],
            observed_types: vec![InferWarningTypeYaml {
                type_name: "String".to_owned(),
                ratio: 0.05,
                examples: vec!["\"N/A\"".to_owned()],
            }],
        }];

        let yaml = stats_to_yaml(&schema, Some(2), &warnings);

        assert_eq!(yaml.infer_warnings, warnings);
    }

    #[test]
    fn format_stats_includes_distinct_over_avg() {
        let schema = two_field_schema();
        let lines = format_stats(&schema, Some(2));
        let joined = lines.join("\n");
        assert!(
            joined.contains("Distinct fields / avg"),
            "format_stats output must contain 'Distinct fields / avg'; got:\n{joined}"
        );
    }

    #[test]
    fn format_stats_distinct_over_avg_zero_when_no_avg() {
        let empty = CollectionSchema {
            count: 0,
            sampled: 0,
            object: IndexMap::new(),
        };
        let lines = format_stats(&empty, None);
        let joined = lines.join("\n");
        // The value line must show 0.0000
        assert!(
            joined.contains("Distinct fields / avg    : 0.0000"),
            "expected '0.0000' in output; got:\n{joined}"
        );
    }
}
