//! Detection of collection groups that should map to a single partitioned PostgreSQL table.
//!
//! Given a set of `(collection_name, CollectionSchema)` pairs (already inferred),
//! this module identifies clusters of collections that are both:
//!
//! * **Name-similar**: Jaro-Winkler similarity ≥ `name_threshold` (default 0.85).
//! * **Schema-similar**: Jaccard overlap of top-level field names ≥ `schema_threshold` (default 0.80).
//!
//! Collections like `EntityHistory_2022` … `EntityHistory_2026` share a common
//! prefix and identical (or near-identical) field sets and will be grouped together.
//! The output can be serialised as TOML and later used by `to-pg --groups` to generate
//! partitioned DDL.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use serde::Deserialize;
use strsim::jaro_winkler;

use crate::analyzer::CollectionSchema;

// ─────────────────────────────────────────────────────────────────────────────
// Public data structures
// ─────────────────────────────────────────────────────────────────────────────

/// A detected group of collections that are candidates for a single partitioned table.
#[derive(Debug)]
pub struct SuggestedGroup {
    /// Suggested PostgreSQL root table name (sanitised common name prefix).
    pub table: String,
    /// Suggested synthetic partition-key column name.
    pub partition_key: String,
    /// Suggested PostgreSQL type for the partition-key column.
    pub partition_key_type: String,
    /// Minimum Jaro-Winkler name similarity among all pairs in the group.
    pub name_similarity_min: f64,
    /// Minimum Jaccard field-name overlap among all pairs in the group.
    pub schema_similarity_min: f64,
    /// Member collections ordered by collection name.
    pub members: Vec<GroupMember>,
}

/// One member of a [`SuggestedGroup`].
#[derive(Debug)]
pub struct GroupMember {
    /// Original MongoDB collection name.
    pub collection: String,
    /// The varying part of the name extracted between the common prefix and suffix
    /// (e.g. `"2022"` for `EntityHistory_2022`). Used as the partition key value.
    pub partition_value: String,
}

// ─────────────────────────────────────────────────────────────────────────────
// TOML deserialization (groups.toml produced by `detect-groups`)
// ─────────────────────────────────────────────────────────────────────────────

/// Top-level structure of a `groups.toml` file.
#[derive(Debug, Deserialize)]
pub struct GroupsFile {
    #[serde(rename = "group", default)]
    pub groups: Vec<GroupDef>,
}

/// One `[[group]]` entry in `groups.toml`.
#[derive(Debug, Deserialize)]
pub struct GroupDef {
    /// Target PostgreSQL root (partitioned) table name.
    pub table: String,
    /// Partition strategy: `"list"` or `"range"`. Default `"list"`.
    #[serde(default = "default_partition_by")]
    pub partition_by: String,
    /// Name of the synthetic partition-key column to add to the merged schema.
    pub partition_key: String,
    /// PostgreSQL type for the partition-key column (e.g. `"INTEGER"`, `"TEXT"`).
    pub partition_key_type: String,
    /// Member collections.
    #[serde(rename = "member", default)]
    pub members: Vec<GroupMemberDef>,
}

/// One `[[group.member]]` entry.
#[derive(Debug, Deserialize)]
pub struct GroupMemberDef {
    /// MongoDB collection name.
    pub collection: String,
    /// Value for the partition-key column for rows coming from this collection.
    pub partition_value: String,
}

fn default_partition_by() -> String {
    "list".to_owned()
}

/// Parse a `groups.toml` file from disk.
pub fn load_groups_toml(path: &Path) -> anyhow::Result<GroupsFile> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("Failed to read {}: {e}", path.display()))?;
    toml::from_str(&content).map_err(|e| anyhow::anyhow!("Failed to parse {}: {e}", path.display()))
}

// ─────────────────────────────────────────────────────────────────────────────
// Public entry point
// ─────────────────────────────────────────────────────────────────────────────

/// Detect collection groups from a list of `(name, schema)` pairs.
///
/// `name_threshold`   – minimum Jaro-Winkler score \[0..1], default 0.85.
/// `schema_threshold` – minimum Jaccard field-name score \[0..1], default 0.80.
///
/// Returns groups with ≥ 2 members, sorted by suggested table name.
pub fn detect_groups(
    schemas: &[(&str, &CollectionSchema)],
    name_threshold: f64,
    schema_threshold: f64,
) -> Vec<SuggestedGroup> {
    let n = schemas.len();
    if n < 2 {
        return Vec::new();
    }

    // ── Step 1: pairwise similarity scores ───────────────────────────────────
    let mut name_sc = vec![vec![0_f64; n]; n];
    let mut schema_sc = vec![vec![0_f64; n]; n];
    let mut adj = vec![vec![false; n]; n];

    for i in 0..n {
        for j in (i + 1)..n {
            let ns = jaro_winkler(schemas[i].0, schemas[j].0);
            let ss = field_jaccard(schemas[i].1, schemas[j].1);
            name_sc[i][j] = ns;
            name_sc[j][i] = ns;
            schema_sc[i][j] = ss;
            schema_sc[j][i] = ss;
            if ns >= name_threshold && ss >= schema_threshold {
                adj[i][j] = true;
                adj[j][i] = true;
            }
        }
    }

    // ── Step 2: union-find clustering ─────────────────────────────────────────
    let mut parent: Vec<usize> = (0..n).collect();
    for i in 0..n {
        for j in (i + 1)..n {
            if adj[i][j] {
                let pi = uf_find(&mut parent, i);
                let pj = uf_find(&mut parent, j);
                if pi != pj {
                    parent[pj] = pi;
                }
            }
        }
    }

    // ── Step 3: gather clusters ───────────────────────────────────────────────
    let mut clusters: HashMap<usize, Vec<usize>> = HashMap::new();
    for i in 0..n {
        let root = uf_find(&mut parent, i);
        clusters.entry(root).or_default().push(i);
    }

    // ── Step 4: build SuggestedGroup for each cluster with ≥ 2 members ───────
    let mut groups: Vec<SuggestedGroup> = clusters
        .into_values()
        .filter(|v| v.len() >= 2)
        .map(|mut indices| {
            indices.sort_by_key(|&i| schemas[i].0);
            let names: Vec<&str> = indices.iter().map(|&i| schemas[i].0).collect();

            // Minimum pairwise scores within the cluster
            let mut name_min = 1.0_f64;
            let mut schema_min = 1.0_f64;
            for a in 0..indices.len() {
                for b in (a + 1)..indices.len() {
                    name_min = name_min.min(name_sc[indices[a]][indices[b]]);
                    schema_min = schema_min.min(schema_sc[indices[a]][indices[b]]);
                }
            }

            // Derive prefix/suffix and partition values
            let (raw_prefix, suffix) = common_prefix_suffix(&names);
            let prefix = meaningful_prefix(&raw_prefix);
            let table = sanitize_table_name(prefix);

            let members: Vec<GroupMember> = names
                .iter()
                .map(|&name| {
                    let end = name.len() - suffix.len();
                    // Strip any leading separator character that was part of the
                    // common prefix but not included in `prefix` (e.g. the `_`
                    // in `B2BSalesOrder_2022` when prefix = `B2BSalesOrder`).
                    let raw_val = if prefix.len() <= end {
                        &name[prefix.len()..end]
                    } else {
                        ""
                    };
                    let partition_value = raw_val
                        .trim_start_matches(|c: char| matches!(c, '_' | '-' | '.' | ' '))
                        .to_owned();
                    GroupMember {
                        collection: name.to_owned(),
                        partition_value,
                    }
                })
                .collect();

            let values: Vec<&str> = members.iter().map(|m| m.partition_value.as_str()).collect();
            let (partition_key, partition_key_type) = infer_partition_key(&values);

            SuggestedGroup {
                table,
                partition_key,
                partition_key_type,
                name_similarity_min: name_min,
                schema_similarity_min: schema_min,
                members,
            }
        })
        .collect();

    groups.sort_by(|a, b| a.table.cmp(&b.table));
    groups
}

// ─────────────────────────────────────────────────────────────────────────────
// TOML rendering
// ─────────────────────────────────────────────────────────────────────────────

/// Render detected groups as a TOML string suitable for saving to `groups.toml`.
///
/// The resulting file is intended to be passed to `mongo2pg to-pg --groups`
/// (future feature) to generate partitioned DDL.
pub fn render_toml(groups: &[SuggestedGroup]) -> String {
    if groups.is_empty() {
        return "# No collection groups detected.\n".to_owned();
    }

    let mut out = String::from(
        "# Suggested partition groups – generated by `mongo2pg detect-groups`\n\
         # Review and adjust before using with `mongo2pg to-pg --groups`\n\n",
    );

    for g in groups {
        out.push_str("[[group]]\n");
        out.push_str(&format!("table              = {:?}\n", g.table));
        out.push_str("partition_by       = \"list\"\n");
        out.push_str(&format!("partition_key      = {:?}\n", g.partition_key));
        out.push_str(&format!(
            "partition_key_type = {:?}\n",
            g.partition_key_type
        ));
        out.push_str(&format!(
            "# name_similarity  = {:.3}   schema_similarity = {:.3}\n\n",
            g.name_similarity_min, g.schema_similarity_min
        ));
        for m in &g.members {
            if m.partition_value.is_empty() {
                continue;
            }
            out.push_str("  [[group.member]]\n");
            out.push_str(&format!("  collection      = {:?}\n", m.collection));
            out.push_str(&format!("  partition_value = {:?}\n", m.partition_value));
            out.push('\n');
        }
    }
    out
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Jaccard similarity on the top-level field names of two schemas.
fn field_jaccard(a: &CollectionSchema, b: &CollectionSchema) -> f64 {
    let set_a: HashSet<&str> = a.object.keys().map(String::as_str).collect();
    let set_b: HashSet<&str> = b.object.keys().map(String::as_str).collect();
    let intersection = set_a.intersection(&set_b).count();
    let union_size = set_a.union(&set_b).count();
    if union_size == 0 {
        1.0
    } else {
        intersection as f64 / union_size as f64
    }
}

/// Union-find with path halving (iterative).
fn uf_find(parent: &mut Vec<usize>, mut x: usize) -> usize {
    while parent[x] != x {
        parent[x] = parent[parent[x]]; // path halving
        x = parent[x];
    }
    x
}

/// Compute the longest common prefix and suffix shared by all `names` (byte-level, ASCII safe).
///
/// The suffix is compared only against the portion of each name that comes after the
/// prefix, so the two never overlap.
fn common_prefix_suffix(names: &[&str]) -> (String, String) {
    if names.is_empty() {
        return (String::new(), String::new());
    }
    let first = names[0];

    // Common prefix
    let mut prefix_len = first.len();
    for s in &names[1..] {
        prefix_len = first
            .bytes()
            .zip(s.bytes())
            .take_while(|(a, b)| a == b)
            .count()
            .min(prefix_len);
    }

    // Common suffix – only compare the tail after the prefix to avoid overlap
    let max_suffix = names
        .iter()
        .map(|s| s.len().saturating_sub(prefix_len))
        .min()
        .unwrap_or(0);
    let first_tail: Vec<u8> = first[prefix_len..].bytes().rev().collect();
    let mut suffix_len = max_suffix;
    for s in &names[1..] {
        let tail: Vec<u8> = s[prefix_len..].bytes().rev().collect();
        let m = first_tail
            .iter()
            .zip(tail.iter())
            .take_while(|(a, b)| a == b)
            .count();
        suffix_len = suffix_len.min(m);
    }

    let prefix = first[..prefix_len].to_owned();
    let suffix = if suffix_len > 0 {
        first[first.len() - suffix_len..].to_owned()
    } else {
        String::new()
    };
    (prefix, suffix)
}

/// Trim the raw common prefix back to the last separator character so that
/// leading digits of the varying part are not absorbed into the table name.
///
/// Examples:
/// * `"EntityHistory_202"` → `"EntityHistory_"`
/// * `"log_202"` → `"log_"`
/// * `"abc"` → `"abc"` (no separator found, keep as-is)
fn meaningful_prefix(raw: &str) -> &str {
    if let Some(pos) = raw.rfind(|c: char| matches!(c, '_' | '-' | '.' | ' ')) {
        &raw[..=pos] // include the separator itself
    } else {
        raw // no separator – the whole prefix is meaningful
    }
}

/// Sanitise `raw` into a lowercase snake_case PostgreSQL table name.
fn sanitize_table_name(raw: &str) -> String {
    let s: String = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_owned();
    if s.is_empty() {
        "group".to_owned()
    } else if s.starts_with(|c: char| c.is_ascii_digit()) {
        format!("_{s}")
    } else {
        s
    }
}

/// Infer a partition key name and PostgreSQL type from the varying values.
///
/// Heuristics (in priority order):
/// * All values are 4-digit years (1900–2100) → key `year`, type `INTEGER`
/// * All values are integers → key `partition_id`, type `INTEGER`
/// * Otherwise → key `partition_key`, type `TEXT`
fn infer_partition_key(values: &[&str]) -> (String, String) {
    let all_ints = !values.is_empty()
        && values
            .iter()
            .all(|v| !v.is_empty() && v.parse::<i64>().is_ok());
    let all_years = all_ints
        && values.iter().all(|v| {
            v.len() == 4
                && v.parse::<u32>()
                    .map(|y| (1900..=2100).contains(&y))
                    .unwrap_or(false)
        });
    if all_years {
        ("year".to_owned(), "INTEGER".to_owned())
    } else if all_ints {
        ("partition_id".to_owned(), "INTEGER".to_owned())
    } else {
        ("partition_key".to_owned(), "TEXT".to_owned())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_schema() -> CollectionSchema {
        CollectionSchema {
            count: 0,
            sampled: 0,
            object: indexmap::IndexMap::new(),
        }
    }

    #[test]
    fn prefix_suffix_year_suffix() {
        let names = [
            "EntityHistory_2022",
            "EntityHistory_2023",
            "EntityHistory_2024",
        ];
        let refs: Vec<&str> = names.iter().map(|s| s.as_ref()).collect();
        let (prefix, suffix) = common_prefix_suffix(&refs);
        // raw prefix = "EntityHistory_202", suffix = ""
        assert_eq!(suffix, "");
        let mp = meaningful_prefix(&prefix);
        assert_eq!(mp, "EntityHistory_");
    }

    #[test]
    fn prefix_suffix_middle_year() {
        let names = ["log_2022_archive", "log_2023_archive"];
        let refs: Vec<&str> = names.iter().map(|s| s.as_ref()).collect();
        let (prefix, suffix) = common_prefix_suffix(&refs);
        let mp = meaningful_prefix(&prefix);
        assert_eq!(mp, "log_");
        assert_eq!(suffix, "_archive");
    }

    #[test]
    fn infer_year_key() {
        let (key, ty) = infer_partition_key(&["2022", "2023", "2024"]);
        assert_eq!(key, "year");
        assert_eq!(ty, "INTEGER");
    }

    #[test]
    fn infer_int_key() {
        let (key, ty) = infer_partition_key(&["1", "2", "3"]);
        assert_eq!(key, "partition_id");
        assert_eq!(ty, "INTEGER");
    }

    #[test]
    fn infer_text_key() {
        let (key, ty) = infer_partition_key(&["jan", "feb", "mar"]);
        assert_eq!(key, "partition_key");
        assert_eq!(ty, "TEXT");
    }

    #[test]
    fn detect_year_group() {
        let s = empty_schema();
        let pairs: Vec<(&str, &CollectionSchema)> = vec![
            ("EntityHistory_2022", &s),
            ("EntityHistory_2023", &s),
            ("EntityHistory_2024", &s),
            ("unrelated_collection", &s),
        ];
        // Schema Jaccard is 1.0 (all empty); name similarity for year collections is high.
        let groups = detect_groups(&pairs, 0.85, 0.0);
        assert_eq!(groups.len(), 1);
        let g = &groups[0];
        assert_eq!(g.table, "entityhistory"); // trailing _ is trimmed by sanitize_table_name
        assert_eq!(g.partition_key, "year");
        assert_eq!(g.members.len(), 3);
        assert_eq!(g.members[0].partition_value, "2022");
        assert_eq!(g.members[1].partition_value, "2023");
        assert_eq!(g.members[2].partition_value, "2024");
    }

    #[test]
    fn no_group_for_dissimilar_names() {
        let s = empty_schema();
        let pairs: Vec<(&str, &CollectionSchema)> =
            vec![("customers", &s), ("orders", &s), ("products", &s)];
        let groups = detect_groups(&pairs, 0.85, 0.80);
        assert!(groups.is_empty());
    }
}
