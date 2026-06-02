//! Export MongoDB collection data to gzipped CSV files.
//!
//! For each collection, reads the generated SQL DDL from `schema/tables/<name>.sql`
//! to determine the table structure, then streams all documents from MongoDB and
//! writes one gzipped CSV file per SQL table into `data/<name>/`.
//!
//! Nested arrays and objects are expanded across child tables exactly as
//! `to_pg` generated them, so the CSV files can be loaded directly into
//! PostgreSQL with `\COPY`.

use std::collections::HashMap;
use std::collections::HashSet;
use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};
use bson::Bson;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use flate2::write::GzEncoder;
use flate2::Compression;
use futures::TryStreamExt;
use mongodb::Client;

use crate::analyzer::CollectionSchema;
use crate::schema_diagram::{parse_sql, Table as SqlTable};

use crate::util::{
    flatten_grouped_root_array_object_fields, flatten_root_array_object_field,
    flattened_root_parent_id_column, grouped_root_array_object_fields, sanitize,
};

// ──────────────────────────────────────────────────────────────────────────────
// BSON → CSV string
// ──────────────────────────────────────────────────────────────────────────────

fn bson_to_string(val: &Bson) -> Option<String> {
    match val {
        Bson::ObjectId(oid) => Some(oid.to_hex()),
        Bson::String(s) => Some(s.clone()),
        Bson::Int32(n) => Some(n.to_string()),
        Bson::Int64(n) => Some(n.to_string()),
        Bson::Double(d) => Some(d.to_string()),
        Bson::Boolean(b) => Some(b.to_string()),
        Bson::DateTime(dt) => Some(format_millis(dt.timestamp_millis())),
        Bson::Decimal128(d) => Some(d.to_string()),
        Bson::Timestamp(ts) => Some(ts.time.to_string()),
        Bson::Null | Bson::Undefined => None,
        // For complex / uncommon types fall back to BSON extended-JSON representation.
        other => Some(serde_json::to_string(other).unwrap_or_default()),
    }
}

fn numeric_timestamp_to_millis(value: f64) -> Option<i64> {
    if !value.is_finite() {
        return None;
    }

    let abs = value.abs();
    let millis = if abs >= 1_000_000_000_000_000.0 {
        value / 1000.0
    } else if abs >= 1_000_000_000_000.0 {
        value
    } else {
        value * 1000.0
    };

    Some(millis.round() as i64)
}

fn format_datetime(dt: DateTime<Utc>) -> String {
    dt.format("%Y-%m-%d %H:%M:%S%.f%:z").to_string()
}

fn normalize_datetime_string(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Ok(number) = trimmed.parse::<f64>() {
        return numeric_timestamp_to_millis(number)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .map(format_datetime);
    }

    if let Ok(parsed) = DateTime::parse_from_rfc3339(trimmed) {
        return Some(format_datetime(parsed.with_timezone(&Utc)));
    }

    const DATETIME_FORMATS: [&str; 4] = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ];

    for format in DATETIME_FORMATS {
        if let Ok(parsed) = NaiveDateTime::parse_from_str(trimmed, format) {
            return Some(format_datetime(parsed.and_utc()));
        }
    }

    if let Ok(parsed) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        return parsed
            .and_hms_opt(0, 0, 0)
            .map(|dt| format_datetime(dt.and_utc()));
    }

    Some(trimmed.to_owned())
}

fn bson_to_timestamp_string(val: &Bson) -> Option<String> {
    match val {
        Bson::DateTime(dt) => {
            DateTime::<Utc>::from_timestamp_millis(dt.timestamp_millis()).map(format_datetime)
        }
        Bson::Timestamp(ts) => {
            DateTime::<Utc>::from_timestamp(ts.time as i64, 0).map(format_datetime)
        }
        Bson::String(text) => normalize_datetime_string(text),
        Bson::Int32(value) => numeric_timestamp_to_millis(*value as f64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .map(format_datetime),
        Bson::Int64(value) => numeric_timestamp_to_millis(*value as f64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .map(format_datetime),
        Bson::Double(value) => numeric_timestamp_to_millis(*value)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .map(format_datetime),
        Bson::Decimal128(value) => value
            .to_string()
            .parse::<f64>()
            .ok()
            .and_then(numeric_timestamp_to_millis)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .map(format_datetime),
        Bson::Null | Bson::Undefined => None,
        other => bson_to_string(other),
    }
}

fn is_timestamp_col_type(col_type: &str) -> bool {
    col_type
        .trim()
        .to_ascii_uppercase()
        .starts_with("TIMESTAMP")
}

/// Format a Unix-millisecond timestamp as a PostgreSQL-ingestible UTC datetime.
fn format_millis(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .map(format_datetime)
        .unwrap_or_default()
}

// ──────────────────────────────────────────────────────────────────────────────
// CSV escaping
// ──────────────────────────────────────────────────────────────────────────────

fn csv_escape(s: &str) -> String {
    if s.is_empty() || s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_owned()
    }
}

fn csv_cell_text(cell: Option<&str>) -> String {
    match cell {
        Some(text) => csv_escape(text),
        None => String::new(),
    }
}

/// Convert a BSON value to a clean `serde_json::Value`, mapping BSON-specific
/// types (ObjectId, DateTime, …) to their natural JSON equivalents.
/// Used to serialise JSONB columns so PostgreSQL COPY can ingest them.
fn bson_to_json_value(val: &Bson) -> serde_json::Value {
    match val {
        Bson::Double(d) => serde_json::Number::from_f64(*d)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Bson::String(s) => serde_json::Value::String(s.clone()),
        Bson::Array(arr) => serde_json::Value::Array(arr.iter().map(bson_to_json_value).collect()),
        Bson::Document(doc) => {
            let map = doc
                .iter()
                .map(|(k, v)| (k.clone(), bson_to_json_value(v)))
                .collect();
            serde_json::Value::Object(map)
        }
        Bson::Boolean(b) => serde_json::Value::Bool(*b),
        Bson::Null | Bson::Undefined => serde_json::Value::Null,
        Bson::Int32(n) => serde_json::Value::Number((*n).into()),
        Bson::Int64(n) => serde_json::Value::Number((*n).into()),
        Bson::ObjectId(oid) => serde_json::Value::String(oid.to_hex()),
        Bson::DateTime(dt) => serde_json::Value::String(format_millis(dt.timestamp_millis())),
        Bson::Decimal128(d) => serde_json::Value::String(d.to_string()),
        Bson::Timestamp(ts) => serde_json::Value::Number(ts.time.into()),
        other => serde_json::to_value(other).unwrap_or(serde_json::Value::Null),
    }
}

struct TableNode {
    /// SQL table name
    sql_name: String,
    /// Column names in declaration order (from SQL)
    columns: Vec<String>,
    /// Primary-key column names in declaration order.
    pk_cols: Vec<String>,
    /// FK column pointing to the parent table (`None` for root tables)
    fk_col: Option<String>,
    /// MongoDB field name at the current document level that yields this table's data.
    /// Empty for the root table.
    mongo_field: String,
    /// Root-level sibling MongoDB fields that are exported through this same child table.
    grouped_root_fields: Option<Vec<String>>,
    /// `true` when this child table represents a scalar array
    /// (has exactly 3 columns: pk, fk, `value`).
    is_scalar_array: bool,
    /// Columns whose SQL type is JSONB – serialised as clean JSON rather than
    /// plain text so PostgreSQL COPY can ingest them.
    jsonb_cols: HashSet<String>,
    /// Columns whose SQL type is timestamp-like and should be exported in a
    /// PostgreSQL-ingestible timestamp text format.
    timestamp_cols: HashSet<String>,
    /// For a promoted root array-of-objects table, the MongoDB array field to iterate.
    root_array_field: Option<String>,
    /// For a promoted root array-of-objects table, the root `_id` column stored on each row.
    root_parent_id_col: Option<String>,
    children: Vec<TableNode>,
}

#[cfg(test)]
fn build_tree(
    sql_tables: &[SqlTable],
    flattened_root: Option<(String, String)>,
    grouped_root_sources: &HashMap<String, Vec<String>>,
) -> Vec<TableNode> {
    build_tree_with_grouped_root(sql_tables, flattened_root, None, grouped_root_sources)
}

fn build_tree_with_grouped_root(
    sql_tables: &[SqlTable],
    flattened_root: Option<(String, String)>,
    flattened_grouped_root: Option<(Vec<String>, String)>,
    grouped_root_sources: &HashMap<String, Vec<String>>,
) -> Vec<TableNode> {
    let names: std::collections::HashSet<&str> =
        sql_tables.iter().map(|t| t.name.as_str()).collect();

    // Map each child table to its parent (first FK that points to a table in this file)
    let mut parent_of: HashMap<&str, &str> = HashMap::new();
    for t in sql_tables {
        for fk in &t.foreign_keys {
            if names.contains(fk.to_table.as_str()) {
                parent_of.insert(&t.name, &fk.to_table);
                break;
            }
        }
    }

    // Invert: parent → children
    let mut children_of: HashMap<&str, Vec<&SqlTable>> = HashMap::new();
    for t in sql_tables {
        if let Some(&par) = parent_of.get(t.name.as_str()) {
            children_of.entry(par).or_default().push(t);
        }
    }

    // Root = no parent
    sql_tables
        .iter()
        .filter(|t| !parent_of.contains_key(t.name.as_str()))
        .map(|r| {
            build_node(
                r,
                None,
                &children_of,
                flattened_root.clone(),
                flattened_grouped_root.clone(),
                grouped_root_sources,
            )
        })
        .collect()
}

fn grouped_root_table_sources(
    schema: &CollectionSchema,
    coll_name: &str,
) -> HashMap<String, Vec<String>> {
    grouped_root_array_object_fields(&schema.object)
        .into_iter()
        .map(|group| {
            (
                format!(
                    "{}_{}",
                    sanitize(coll_name),
                    sanitize(&group.representative)
                ),
                group.members,
            )
        })
        .collect()
}

fn flattened_root_for_export(
    sql_tables: &[SqlTable],
    schema: &CollectionSchema,
    coll_name: &str,
) -> Option<(String, String)> {
    let (field_name, _) = flatten_root_array_object_field(schema)?;
    let sanitized_field = sanitize(field_name);
    let root_keeps_jsonb_column = sql_tables.iter().any(|table| {
        table.foreign_keys.is_empty()
            && table.columns.iter().any(|column| {
                column.name == sanitized_field && column.col_type.eq_ignore_ascii_case("JSONB")
            })
    });

    if root_keeps_jsonb_column {
        None
    } else {
        Some((
            field_name.to_owned(),
            flattened_root_parent_id_column(coll_name),
        ))
    }
}

fn flattened_grouped_root_for_export(
    sql_tables: &[SqlTable],
    schema: &CollectionSchema,
    coll_name: &str,
) -> Option<(Vec<String>, String)> {
    let group = flatten_grouped_root_array_object_fields(schema)?;
    let parent_id_col = flattened_root_parent_id_column(coll_name);
    let root_table = sql_tables
        .iter()
        .find(|table| table.foreign_keys.is_empty())?;

    if root_table
        .columns
        .iter()
        .any(|column| column.name == parent_id_col)
        && root_table.columns.iter().any(|column| column.name == "key")
    {
        Some((group.members, parent_id_col))
    } else {
        None
    }
}

fn build_node(
    sql_t: &SqlTable,
    parent_name: Option<&str>,
    children_of: &HashMap<&str, Vec<&SqlTable>>,
    flattened_root: Option<(String, String)>,
    flattened_grouped_root: Option<(Vec<String>, String)>,
    grouped_root_sources: &HashMap<String, Vec<String>>,
) -> TableNode {
    // The MongoDB field name is the suffix after stripping "<parent>_" from the table name.
    let mongo_field = match parent_name {
        Some(p) => sql_t
            .name
            .strip_prefix(&format!("{p}_"))
            .unwrap_or(&sql_t.name)
            .to_owned(),
        None => String::new(),
    };

    let pk_cols: Vec<String> = sql_t
        .columns
        .iter()
        .filter(|c| c.primary_key)
        .map(|c| c.name.clone())
        .collect();

    let fk_col = sql_t.foreign_keys.first().map(|fk| fk.from_col.clone());

    let columns: Vec<String> = sql_t.columns.iter().map(|c| c.name.clone()).collect();

    // A scalar-array child has exactly: pk, fk, value  (3 columns total)
    let is_scalar_array =
        fk_col.is_some() && columns.len() == 3 && columns.iter().any(|c| c == "value");

    let jsonb_cols: HashSet<String> = sql_t
        .columns
        .iter()
        .filter(|c| c.col_type.eq_ignore_ascii_case("JSONB"))
        .map(|c| c.name.clone())
        .collect();
    let timestamp_cols: HashSet<String> = sql_t
        .columns
        .iter()
        .filter(|c| is_timestamp_col_type(&c.col_type))
        .map(|c| c.name.clone())
        .collect();

    let children: Vec<TableNode> = children_of
        .get(sql_t.name.as_str())
        .map(|cs| {
            cs.iter()
                .map(|child_sql| {
                    build_node(
                        child_sql,
                        Some(&sql_t.name),
                        children_of,
                        None,
                        None,
                        grouped_root_sources,
                    )
                })
                .collect()
        })
        .unwrap_or_default();

    let (root_array_field, root_parent_id_col, root_grouped_fields) = if parent_name.is_none() {
        if let Some((field_name, parent_id_col)) = flattened_root {
            (Some(field_name), Some(parent_id_col), None)
        } else if let Some((grouped_fields, parent_id_col)) = flattened_grouped_root {
            (None, Some(parent_id_col), Some(grouped_fields))
        } else {
            (None, None, None)
        }
    } else {
        (None, None, None)
    };
    let grouped_root_fields = if parent_name.is_some() {
        grouped_root_sources.get(&sql_t.name).cloned()
    } else {
        root_grouped_fields
    };

    TableNode {
        sql_name: sql_t.name.clone(),
        columns,
        pk_cols,
        fk_col,
        mongo_field,
        grouped_root_fields,
        is_scalar_array,
        jsonb_cols,
        timestamp_cols,
        root_array_field,
        root_parent_id_col,
        children,
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// BSON field lookup
// ──────────────────────────────────────────────────────────────────────────────

/// Find `sql_col` in a BSON document.
///
/// Tries an exact-name match first, then falls back to comparing sanitized names
/// so that e.g. `"invoiceId"` in MongoDB matches the `"invoiceid"` SQL column.
fn find_mongo_field<'a>(doc: &'a bson::Document, sql_col: &str) -> Option<&'a Bson> {
    fn collect_nested_matches<'a>(
        doc: &'a bson::Document,
        sql_col: &str,
        matches: &mut Vec<&'a Bson>,
    ) {
        for (key, val) in doc.iter() {
            if let Bson::Document(child_doc) = val {
                if sanitize(key) == sql_col {
                    matches.push(val);
                }
                if let Some(value) = find_mongo_field(child_doc, sql_col) {
                    matches.push(value);
                }
            }
        }
    }

    if let Some(v) = doc.get(sql_col) {
        return Some(v);
    }
    for (key, val) in doc.iter() {
        if sanitize(key) == sql_col {
            return Some(val);
        }
        if let Bson::Document(child_doc) = val {
            let key_prefix = format!("{}_", sanitize(key));
            if let Some(remainder) = sql_col.strip_prefix(&key_prefix) {
                if let Some(value) = find_mongo_field(child_doc, remainder) {
                    return Some(value);
                }
            }
        }
    }

    let mut matches = Vec::new();
    collect_nested_matches(doc, sql_col, &mut matches);
    if matches.len() == 1 {
        return matches.into_iter().next();
    }

    None
}

fn find_root_mongo_field<'a>(doc: &'a bson::Document, sql_col: &str) -> Option<&'a Bson> {
    find_mongo_field(doc, sql_col).or_else(|| {
        doc.get_document("_id")
            .ok()
            .and_then(|id_doc| find_mongo_field(id_doc, sql_col))
    })
}

// ──────────────────────────────────────────────────────────────────────────────
// Row extraction  (recursive, depth-first)
// ──────────────────────────────────────────────────────────────────────────────

fn extract_rows(
    val: &Bson,
    node: &TableNode,
    parent_id: Option<&str>,
    is_root: bool,
    all_rows: &mut HashMap<String, Vec<Vec<Option<String>>>>,
    counters: &mut HashMap<String, u64>,
) {
    let doc = match val {
        Bson::Document(d) => d,
        _ => return,
    };

    if is_root {
        if let (Some(root_parent_id_col), Some(grouped_fields)) =
            (&node.root_parent_id_col, &node.grouped_root_fields)
        {
            let parent_source_id = doc.get("_id").and_then(bson_to_string).unwrap_or_default();
            for grouped_field in grouped_fields {
                if let Some(Bson::Array(items)) = find_mongo_field(doc, grouped_field) {
                    for item in items {
                        let item_doc = match item {
                            Bson::Document(item_doc) => item_doc,
                            _ => continue,
                        };
                        let c = counters.entry(node.sql_name.clone()).or_insert(0);
                        *c += 1;
                        let my_id = c.to_string();

                        let row: Vec<Option<String>> = node
                            .columns
                            .iter()
                            .map(|col| {
                                if node.pk_cols.iter().any(|pk| pk == col) {
                                    Some(my_id.clone())
                                } else if col == root_parent_id_col {
                                    Some(parent_source_id.clone())
                                } else if col == "key" {
                                    Some(grouped_field.clone())
                                } else {
                                    find_mongo_field(item_doc, col)
                                        .map(|v| {
                                            if node.jsonb_cols.contains(col) {
                                                Some(
                                                    serde_json::to_string(&bson_to_json_value(v))
                                                        .unwrap_or_default(),
                                                )
                                            } else if node.timestamp_cols.contains(col) {
                                                bson_to_timestamp_string(v)
                                            } else {
                                                bson_to_string(v)
                                            }
                                        })
                                        .unwrap_or(None)
                                }
                            })
                            .collect();

                        all_rows.entry(node.sql_name.clone()).or_default().push(row);

                        for child in &node.children {
                            match find_mongo_field(item_doc, &child.mongo_field) {
                                Some(Bson::Array(arr)) => {
                                    if child.is_scalar_array {
                                        for child_item in arr {
                                            let child_id = {
                                                let c = counters
                                                    .entry(child.sql_name.clone())
                                                    .or_insert(0);
                                                *c += 1;
                                                c.to_string()
                                            };
                                            let child_row: Vec<Option<String>> = child
                                                .columns
                                                .iter()
                                                .map(|col| {
                                                    if child.pk_cols.iter().any(|pk| pk == col) {
                                                        Some(child_id.clone())
                                                    } else if Some(col) == child.fk_col.as_ref() {
                                                        Some(my_id.clone())
                                                    } else if col == "value" {
                                                        if child.timestamp_cols.contains(col) {
                                                            bson_to_timestamp_string(child_item)
                                                        } else {
                                                            bson_to_string(child_item)
                                                        }
                                                    } else {
                                                        None
                                                    }
                                                })
                                                .collect();
                                            all_rows
                                                .entry(child.sql_name.clone())
                                                .or_default()
                                                .push(child_row);
                                        }
                                    } else {
                                        for child_item in arr {
                                            extract_rows(
                                                child_item,
                                                child,
                                                Some(&my_id),
                                                false,
                                                all_rows,
                                                counters,
                                            );
                                        }
                                    }
                                }
                                Some(doc_val @ Bson::Document(_)) => {
                                    extract_rows(
                                        doc_val,
                                        child,
                                        Some(&my_id),
                                        false,
                                        all_rows,
                                        counters,
                                    );
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            return;
        }

        if let (Some(root_array_field), Some(root_parent_id_col)) =
            (&node.root_array_field, &node.root_parent_id_col)
        {
            let parent_source_id = doc.get("_id").and_then(bson_to_string).unwrap_or_default();
            if let Some(Bson::Array(items)) = find_mongo_field(doc, root_array_field) {
                for item in items {
                    let item_doc = match item {
                        Bson::Document(item_doc) => item_doc,
                        _ => continue,
                    };
                    let c = counters.entry(node.sql_name.clone()).or_insert(0);
                    *c += 1;
                    let my_id = c.to_string();

                    let row: Vec<Option<String>> = node
                        .columns
                        .iter()
                        .map(|col| {
                            if node.pk_cols.iter().any(|pk| pk == col) {
                                Some(my_id.clone())
                            } else if col == root_parent_id_col {
                                Some(parent_source_id.clone())
                            } else {
                                find_mongo_field(item_doc, col)
                                    .map(|v| {
                                        if node.jsonb_cols.contains(col) {
                                            Some(
                                                serde_json::to_string(&bson_to_json_value(v))
                                                    .unwrap_or_default(),
                                            )
                                        } else if node.timestamp_cols.contains(col) {
                                            bson_to_timestamp_string(v)
                                        } else {
                                            bson_to_string(v)
                                        }
                                    })
                                    .unwrap_or(None)
                            }
                        })
                        .collect();

                    all_rows.entry(node.sql_name.clone()).or_default().push(row);

                    for child in &node.children {
                        match find_mongo_field(item_doc, &child.mongo_field) {
                            Some(Bson::Array(arr)) => {
                                if child.is_scalar_array {
                                    for child_item in arr {
                                        let child_id = {
                                            let c =
                                                counters.entry(child.sql_name.clone()).or_insert(0);
                                            *c += 1;
                                            c.to_string()
                                        };
                                        let child_row: Vec<Option<String>> = child
                                            .columns
                                            .iter()
                                            .map(|col| {
                                                if child.pk_cols.iter().any(|pk| pk == col) {
                                                    Some(child_id.clone())
                                                } else if Some(col) == child.fk_col.as_ref() {
                                                    Some(my_id.clone())
                                                } else if col == "value" {
                                                    if child.timestamp_cols.contains(col) {
                                                        bson_to_timestamp_string(child_item)
                                                    } else {
                                                        bson_to_string(child_item)
                                                    }
                                                } else {
                                                    None
                                                }
                                            })
                                            .collect();
                                        all_rows
                                            .entry(child.sql_name.clone())
                                            .or_default()
                                            .push(child_row);
                                    }
                                } else {
                                    for child_item in arr {
                                        extract_rows(
                                            child_item,
                                            child,
                                            Some(&my_id),
                                            false,
                                            all_rows,
                                            counters,
                                        );
                                    }
                                }
                            }
                            Some(doc_val @ Bson::Document(_)) => {
                                extract_rows(
                                    doc_val,
                                    child,
                                    Some(&my_id),
                                    false,
                                    all_rows,
                                    counters,
                                );
                            }
                            _ => {}
                        }
                    }
                }
            }
            return;
        }
    }

    // Assign an ID for this row.
    let my_id: String = if is_root {
        // Root table: _id → id
        doc.get("_id").and_then(bson_to_string).unwrap_or_default()
    } else {
        let c = counters.entry(node.sql_name.clone()).or_insert(0);
        *c += 1;
        c.to_string()
    };

    // Build the row by iterating SQL columns in order.
    let row: Vec<Option<String>> = node
        .columns
        .iter()
        .map(|col| {
            if (!is_root && node.pk_cols.iter().any(|pk| pk == col))
                || (is_root && node.pk_cols.len() == 1 && node.pk_cols[0] == "id" && col == "id")
            {
                Some(my_id.clone())
            } else if Some(col) == node.fk_col.as_ref() {
                Some(parent_id.unwrap_or("").to_owned())
            } else {
                let lookup = if is_root {
                    find_root_mongo_field(doc, col)
                } else {
                    find_mongo_field(doc, col)
                };
                lookup
                    .map(|v| {
                        if node.jsonb_cols.contains(col) {
                            Some(serde_json::to_string(&bson_to_json_value(v)).unwrap_or_default())
                        } else if node.timestamp_cols.contains(col) {
                            bson_to_timestamp_string(v)
                        } else {
                            bson_to_string(v)
                        }
                    })
                    .unwrap_or(None)
            }
        })
        .collect();

    all_rows.entry(node.sql_name.clone()).or_default().push(row);

    // Recurse into child tables.
    for child in &node.children {
        if let Some(grouped_fields) = &child.grouped_root_fields {
            for grouped_field in grouped_fields {
                match find_mongo_field(doc, grouped_field) {
                    Some(Bson::Array(arr)) => {
                        for item in arr {
                            let item_doc = match item {
                                Bson::Document(item_doc) => item_doc,
                                _ => continue,
                            };
                            let child_id = {
                                let c = counters.entry(child.sql_name.clone()).or_insert(0);
                                *c += 1;
                                c.to_string()
                            };
                            let child_row: Vec<Option<String>> = child
                                .columns
                                .iter()
                                .map(|col| {
                                    if child.pk_cols.iter().any(|pk| pk == col) {
                                        Some(child_id.clone())
                                    } else if Some(col) == child.fk_col.as_ref() {
                                        Some(my_id.clone())
                                    } else if col == "key" {
                                        Some(grouped_field.clone())
                                    } else {
                                        find_mongo_field(item_doc, col)
                                            .map(|v| {
                                                if child.jsonb_cols.contains(col) {
                                                    Some(
                                                        serde_json::to_string(&bson_to_json_value(
                                                            v,
                                                        ))
                                                        .unwrap_or_default(),
                                                    )
                                                } else if child.timestamp_cols.contains(col) {
                                                    bson_to_timestamp_string(v)
                                                } else {
                                                    bson_to_string(v)
                                                }
                                            })
                                            .unwrap_or(None)
                                    }
                                })
                                .collect();
                            all_rows
                                .entry(child.sql_name.clone())
                                .or_default()
                                .push(child_row);

                            for grandchild in &child.children {
                                match find_mongo_field(item_doc, &grandchild.mongo_field) {
                                    Some(Bson::Array(arr)) => {
                                        if grandchild.is_scalar_array {
                                            for item in arr {
                                                let grandchild_id = {
                                                    let c = counters
                                                        .entry(grandchild.sql_name.clone())
                                                        .or_insert(0);
                                                    *c += 1;
                                                    c.to_string()
                                                };
                                                let grandchild_row: Vec<Option<String>> =
                                                    grandchild
                                                        .columns
                                                        .iter()
                                                        .map(|col| {
                                                            if grandchild
                                                                .pk_cols
                                                                .iter()
                                                                .any(|pk| pk == col)
                                                            {
                                                                Some(grandchild_id.clone())
                                                            } else if Some(col)
                                                                == grandchild.fk_col.as_ref()
                                                            {
                                                                Some(child_id.clone())
                                                            } else if col == "value" {
                                                                if grandchild
                                                                    .timestamp_cols
                                                                    .contains(col)
                                                                {
                                                                    bson_to_timestamp_string(item)
                                                                } else {
                                                                    bson_to_string(item)
                                                                }
                                                            } else {
                                                                None
                                                            }
                                                        })
                                                        .collect();
                                                all_rows
                                                    .entry(grandchild.sql_name.clone())
                                                    .or_default()
                                                    .push(grandchild_row);
                                            }
                                        } else {
                                            for item in arr {
                                                extract_rows(
                                                    item,
                                                    grandchild,
                                                    Some(&child_id),
                                                    false,
                                                    all_rows,
                                                    counters,
                                                );
                                            }
                                        }
                                    }
                                    Some(doc_val @ Bson::Document(_)) => {
                                        extract_rows(
                                            doc_val,
                                            grandchild,
                                            Some(&child_id),
                                            false,
                                            all_rows,
                                            counters,
                                        );
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                    Some(doc_val @ Bson::Document(_)) => {
                        extract_rows(doc_val, child, Some(&my_id), false, all_rows, counters);
                    }
                    _ => {}
                }
            }
            continue;
        }

        match find_mongo_field(doc, &child.mongo_field) {
            Some(Bson::Array(arr)) => {
                if child.is_scalar_array {
                    // One row per scalar element.
                    for item in arr {
                        let child_id = {
                            let c = counters.entry(child.sql_name.clone()).or_insert(0);
                            *c += 1;
                            c.to_string()
                        };
                        let child_row: Vec<Option<String>> = child
                            .columns
                            .iter()
                            .map(|col| {
                                if child.pk_cols.iter().any(|pk| pk == col) {
                                    Some(child_id.clone())
                                } else if Some(col) == child.fk_col.as_ref() {
                                    Some(my_id.clone())
                                } else if col == "value" {
                                    if child.timestamp_cols.contains(col) {
                                        bson_to_timestamp_string(item)
                                    } else {
                                        bson_to_string(item)
                                    }
                                } else {
                                    None
                                }
                            })
                            .collect();
                        all_rows
                            .entry(child.sql_name.clone())
                            .or_default()
                            .push(child_row);
                    }
                } else {
                    // One row per document element.
                    for item in arr {
                        extract_rows(item, child, Some(&my_id), false, all_rows, counters);
                    }
                }
            }
            Some(doc_val @ Bson::Document(_)) => {
                // Embedded 1:1 object.
                extract_rows(doc_val, child, Some(&my_id), false, all_rows, counters);
            }
            _ => {} // field absent or unexpected type – skip
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Public API
// ──────────────────────────────────────────────────────────────────────────────

/// Export a single MongoDB collection to gzipped CSV files.
///
/// One `.csv.gz` file is written per SQL table (root + all child tables) into
/// `<data_dir>/<db_name>/<sanitize(coll_name)>/`.  The SQL schema is read from
/// `<tables_dir>/<sanitize(coll_name)>.sql`.
pub async fn export_collection(
    client: &Client,
    db_name: &str,
    coll_name: &str,
    tables_dir: &Path,
    collections_dir: &Path,
    data_dir: &Path,
) -> Result<()> {
    // Only the SQL filename is sanitized; the MongoDB collection name must stay raw.
    let sql_lookup_name = sanitize(coll_name);
    let sql_path = tables_dir.join(format!("{sql_lookup_name}.sql"));
    if !sql_path.exists() {
        return Err(anyhow::anyhow!(
            "SQL schema not found: {} – run `to-pg` first",
            sql_path.display()
        ));
    }

    let sql = std::fs::read_to_string(&sql_path)
        .with_context(|| format!("Cannot read {}", sql_path.display()))?;

    let sql_tables = parse_sql(&sql);
    if sql_tables.is_empty() {
        return Err(anyhow::anyhow!(
            "No CREATE TABLE statements found in {}",
            sql_path.display()
        ));
    }

    let safe_name = coll_name.replace('/', "_");
    let schema_path = collections_dir
        .join(&safe_name)
        .join(format!("{safe_name}.json"));
    let (flattened_root, flattened_grouped_root, grouped_root_sources) =
        std::fs::read_to_string(&schema_path)
            .ok()
            .and_then(|content| serde_json::from_str::<CollectionSchema>(&content).ok())
            .map(|schema| {
                (
                    flattened_root_for_export(&sql_tables, &schema, coll_name),
                    flattened_grouped_root_for_export(&sql_tables, &schema, coll_name),
                    grouped_root_table_sources(&schema, coll_name),
                )
            })
            .unwrap_or_else(|| (None, None, HashMap::new()));
    let roots = build_tree_with_grouped_root(
        &sql_tables,
        flattened_root,
        flattened_grouped_root,
        &grouped_root_sources,
    );

    // Query MongoDB using the original collection name.
    let db = client.database(db_name);
    let collection = db.collection::<bson::Document>(coll_name);
    let mut cursor = collection
        .find(bson::doc! {})
        .await
        .with_context(|| format!("Failed to query {db_name}.{coll_name}"))?;

    let mut all_rows: HashMap<String, Vec<Vec<Option<String>>>> = HashMap::new();
    let mut counters: HashMap<String, u64> = HashMap::new();

    while let Some(doc) = cursor.try_next().await.context("Cursor error")? {
        let bson_val = Bson::Document(doc);
        for root in &roots {
            extract_rows(&bson_val, root, None, true, &mut all_rows, &mut counters);
        }
    }

    // Keep the database name raw, but sanitize the collection folder for filesystem safety.
    let out_dir = data_dir.join(db_name).join(&sql_lookup_name);
    std::fs::create_dir_all(&out_dir)
        .with_context(|| format!("Cannot create {}", out_dir.display()))?;

    for sql_t in &sql_tables {
        let columns: Vec<String> = sql_t.columns.iter().map(|c| c.name.clone()).collect();
        let rows = all_rows.get(&sql_t.name).cloned().unwrap_or_default();

        let csv_path = out_dir.join(format!("{}.csv.gz", sql_t.name));
        let file = std::fs::File::create(&csv_path)
            .with_context(|| format!("Cannot create {}", csv_path.display()))?;
        let mut gz = GzEncoder::new(file, Compression::default());

        // Header row
        let header: Vec<String> = columns.iter().map(|c| csv_escape(c)).collect();
        writeln!(gz, "{}", header.join(","))
            .with_context(|| format!("Write error for {}", csv_path.display()))?;

        // Data rows
        for row in &rows {
            let line: Vec<String> = row.iter().map(|v| csv_cell_text(v.as_deref())).collect();
            writeln!(gz, "{}", line.join(","))
                .with_context(|| format!("Write error for {}", csv_path.display()))?;
        }

        gz.finish()
            .with_context(|| format!("GZ flush error for {}", csv_path.display()))?;

        eprintln!("  {} rows -> {}", rows.len(), csv_path.display());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        build_tree, build_tree_with_grouped_root, extract_rows, flattened_grouped_root_for_export,
        flattened_root_for_export, grouped_root_table_sources,
    };
    use crate::analyzer::Analyzer;
    use crate::schema_diagram::parse_sql;
    use bson::{doc, Bson};
    use std::collections::HashMap;

    #[test]
    fn export_root_rows_use_flattened_object_id_fields_and_skip_fake_primary_column() {
        let sql = r#"
CREATE TABLE security_logs (
    log_type TEXT NOT NULL,
    projectid TEXT NOT NULL,
    provider TEXT NOT NULL,
    last_execution TEXT NOT NULL,
    PRIMARY KEY (log_type, projectid, provider)
);
"#;

        let tables = parse_sql(sql);
        let roots = build_tree(&tables, None, &HashMap::new());
        let mut all_rows = HashMap::new();
        let mut counters = HashMap::new();
        let doc = doc! {
            "_id": {
                "projectid": "FRAS-P-SAM-FRTERR2",
                "provider": "atlas",
                "log_type": "dbAccessHistory"
            },
            "last_execution": "2023-07-13T09:02:15.833170"
        };

        extract_rows(
            &Bson::Document(doc),
            &roots[0],
            None,
            true,
            &mut all_rows,
            &mut counters,
        );

        let rows = all_rows.get("security_logs").expect("root rows missing");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 4);
        assert_eq!(rows[0][0].as_deref(), Some("dbAccessHistory"));
        assert_eq!(rows[0][1].as_deref(), Some("FRAS-P-SAM-FRTERR2"));
        assert_eq!(rows[0][2].as_deref(), Some("atlas"));
        assert_eq!(rows[0][3].as_deref(), Some("2023-07-13T09:02:15.833170"));
    }

    #[test]
    fn export_root_rows_write_scalar_id_column_from_mongo_id() {
        let sql = r#"
CREATE TABLE activity_feed (
    id TEXT PRIMARY KEY,
    activity TEXT NOT NULL,
    targetid TEXT NOT NULL,
    targettype TEXT NOT NULL,
    timestamp DOUBLE PRECISION NOT NULL,
    who TEXT NOT NULL
);
"#;

        let tables = parse_sql(sql);
        let roots = build_tree(&tables, None, &HashMap::new());
        let mut all_rows = HashMap::new();
        let mut counters = HashMap::new();
        let doc = doc! {
            "_id": "6267ddea9270b5e839a81ac4",
            "activity": "Project created",
            "targetid": "FRAS-D-NLX-FEATURE1",
            "targettype": "project",
            "timestamp": 1650468505.273496,
            "who": "me"
        };

        extract_rows(
            &Bson::Document(doc),
            &roots[0],
            None,
            true,
            &mut all_rows,
            &mut counters,
        );

        let rows = all_rows.get("activity_feed").expect("root rows missing");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].as_deref(), Some("6267ddea9270b5e839a81ac4"));
        assert_eq!(rows[0][1].as_deref(), Some("Project created"));
        assert_eq!(rows[0][2].as_deref(), Some("FRAS-D-NLX-FEATURE1"));
    }

    #[test]
    fn export_promoted_root_array_objects_write_one_row_per_item() {
        let sql = r#"
CREATE TABLE engine (
    id BIGSERIAL PRIMARY KEY,
    engine_id TEXT NOT NULL,
    eol_date TIMESTAMP WITH TIME ZONE NOT NULL,
    grace_date TIMESTAMP WITH TIME ZONE NOT NULL,
    major_version TEXT NOT NULL
);
"#;

        let tables = parse_sql(sql);
        let roots = build_tree(
            &tables,
            Some(("versions".to_owned(), "engine_id".to_owned())),
            &HashMap::new(),
        );
        let mut all_rows = HashMap::new();
        let mut counters = HashMap::new();
        let doc = doc! {
            "_id": "pg",
            "versions": [
                {
                    "major_version": "13",
                    "grace_date": "2025-05-13T00:00:00Z",
                    "eol_date": "2025-11-13T00:00:00Z"
                },
                {
                    "major_version": "14",
                    "grace_date": "2026-05-12T00:00:00Z",
                    "eol_date": "2026-11-12T00:00:00Z"
                }
            ]
        };

        extract_rows(
            &Bson::Document(doc),
            &roots[0],
            None,
            true,
            &mut all_rows,
            &mut counters,
        );

        let rows = all_rows.get("engine").expect("root rows missing");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1].as_deref(), Some("pg"));
        assert_eq!(rows[0][4].as_deref(), Some("13"));
        assert_eq!(rows[1][1].as_deref(), Some("pg"));
        assert_eq!(rows[1][4].as_deref(), Some("14"));
    }

    #[test]
    fn export_timestamp_columns_coerce_numeric_values_to_iso_strings() {
        let sql = r#"
CREATE TABLE scheduling_jobs (
    id TEXT PRIMARY KEY,
    last_update TIMESTAMP WITH TIME ZONE NOT NULL
);
"#;

        let tables = parse_sql(sql);
        let roots = build_tree(&tables, None, &HashMap::new());
        let mut all_rows = HashMap::new();
        let mut counters = HashMap::new();
        let doc = doc! {
            "_id": "job-1",
            "last_update": 1650468505.273496_f64
        };

        extract_rows(
            &Bson::Document(doc),
            &roots[0],
            None,
            true,
            &mut all_rows,
            &mut counters,
        );

        let rows = all_rows.get("scheduling_jobs").expect("root rows missing");
        assert_eq!(rows[0][0].as_deref(), Some("job-1"));
        assert_eq!(rows[0][1].as_deref(), Some("2022-04-20 15:28:25.273+00:00"));
    }

    #[test]
    fn export_groups_same_shape_root_arrays_into_one_keyed_child_table() {
        let sql = r#"
CREATE TABLE communities (
    id BIGSERIAL PRIMARY KEY,
    communities_id TEXT NOT NULL,
    key TEXT NOT NULL,
    cloud TEXT NOT NULL,
    network_exposition TEXT NOT NULL,
    provider TEXT NOT NULL
);

CREATE TABLE communities_available_localizations (
    id BIGSERIAL PRIMARY KEY,
    communities_id BIGINT NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY (communities_id) REFERENCES communities (id) DEFERRABLE INITIALLY DEFERRED
);
"#;

        let mut analyzer = Analyzer::new(true);
        analyzer.process_document(&doc! {
            "_id": "community-1",
            "dev": [{
                "provider": "aiven",
                "cloud": "gcp",
                "network_exposition": "private_platform"
            }],
            "prod": [{
                "provider": "atlas",
                "cloud": "azure",
                "network_exposition": "public"
            }]
        });
        let schema = analyzer.finish();

        let tables = parse_sql(sql);
        let flattened_grouped_root =
            flattened_grouped_root_for_export(&tables, &schema, "communities");
        let grouped_root_sources = grouped_root_table_sources(&schema, "communities");
        let roots = build_tree_with_grouped_root(
            &tables,
            None,
            flattened_grouped_root,
            &grouped_root_sources,
        );
        let mut all_rows = HashMap::new();
        let mut counters = HashMap::new();
        let doc = doc! {
            "_id": "community-1",
            "dev": [{
                "available_localizations": ["eu-west-1"],
                "provider": "aiven",
                "cloud": "gcp",
                "network_exposition": "private_platform"
            }],
            "prod": [{
                "available_localizations": ["eu-west-2"],
                "provider": "atlas",
                "cloud": "azure",
                "network_exposition": "public"
            }]
        };

        extract_rows(
            &Bson::Document(doc),
            &roots[0],
            None,
            true,
            &mut all_rows,
            &mut counters,
        );

        let rows = all_rows
            .get("communities")
            .expect("grouped root rows missing");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1].as_deref(), Some("community-1"));
        assert_eq!(rows[0][2].as_deref(), Some("dev"));
        assert_eq!(rows[1][2].as_deref(), Some("prod"));

        let child_rows = all_rows
            .get("communities_available_localizations")
            .expect("grouped nested rows missing");
        assert_eq!(child_rows.len(), 2);
    }

    #[test]
    fn export_restores_scalar_only_object_sibling_child_rows() {
        let sql = r#"
CREATE TABLE projects (
    id TEXT PRIMARY KEY
);

CREATE TABLE projects_providers (
    id BIGSERIAL PRIMARY KEY,
    projects_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    namespace_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    FOREIGN KEY (projects_id) REFERENCES projects (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE projects_providers_metadata (
    id BIGSERIAL PRIMARY KEY,
    projects_providers_id BIGINT NOT NULL,
    creation_date TIMESTAMP WITH TIME ZONE NOT NULL,
    status TEXT NOT NULL,
    FOREIGN KEY (projects_providers_id) REFERENCES projects_providers (id) DEFERRABLE INITIALLY DEFERRED
);
"#;

        let tables = parse_sql(sql);
        let roots = build_tree(&tables, None, &HashMap::new());
        let mut all_rows = HashMap::new();
        let mut counters = HashMap::new();
        let doc = doc! {
            "_id": "project-1",
            "providers": [{
                "namespace": "fras-t-dba-176c358",
                "namespace_id": "fras-t-dba-176c358",
                "provider": "aiven",
                "metadata": {
                    "creation_date": "2025-08-11T00:00:00Z",
                    "status": "created"
                }
            }]
        };

        extract_rows(
            &Bson::Document(doc),
            &roots[0],
            None,
            true,
            &mut all_rows,
            &mut counters,
        );

        let rows = all_rows
            .get("projects_providers")
            .expect("providers rows missing");
        assert_eq!(rows.len(), 1);

        let metadata_rows = all_rows
            .get("projects_providers_metadata")
            .expect("providers metadata rows missing");
        assert_eq!(metadata_rows.len(), 1);
        assert_eq!(
            metadata_rows[0][2].as_deref(),
            Some("2025-08-11 00:00:00+00:00")
        );
        assert_eq!(metadata_rows[0][3].as_deref(), Some("created"));
    }

    #[test]
    fn export_timestamp_columns_normalize_rfc3339_strings() {
        let sql = r#"
CREATE TABLE scheduling_jobs (
    id TEXT PRIMARY KEY,
    last_update TIMESTAMP WITH TIME ZONE NOT NULL
);
"#;

        let tables = parse_sql(sql);
        let roots = build_tree(&tables, None, &HashMap::new());
        let mut all_rows = HashMap::new();
        let mut counters = HashMap::new();
        let doc = doc! {
            "_id": "job-1",
            "last_update": "2022-04-20T15:28:25.273Z"
        };

        extract_rows(
            &Bson::Document(doc),
            &roots[0],
            None,
            true,
            &mut all_rows,
            &mut counters,
        );

        let rows = all_rows.get("scheduling_jobs").expect("root rows missing");
        assert_eq!(rows[0][1].as_deref(), Some("2022-04-20 15:28:25.273+00:00"));
    }

    #[test]
    fn export_jsonb_root_array_objects_stay_on_root_row() {
        let sql = r#"
CREATE TABLE engine (
    id TEXT PRIMARY KEY,
    versions JSONB NOT NULL
);
"#;

        let tables = parse_sql(sql);
        let mut analyzer = Analyzer::new(true);
        analyzer.process_document(&doc! {
            "_id": "pg",
            "versions": [
                {
                    "major_version": "13",
                    "grace_date": "2025-05-13T00:00:00Z",
                    "eol_date": "2025-11-13T00:00:00Z"
                },
                {
                    "major_version": "14",
                    "grace_date": "2026-05-12T00:00:00Z",
                    "eol_date": "2026-11-12T00:00:00Z"
                }
            ]
        });
        let mut schema = analyzer.finish();
        schema.mark_objects_as_jsonb();

        let flattened_root = flattened_root_for_export(&tables, &schema, "engine");
        assert!(
            flattened_root.is_none(),
            "jsonb root array should not be promoted into one row per item"
        );

        let roots = build_tree(&tables, flattened_root, &HashMap::new());
        let mut all_rows = HashMap::new();
        let mut counters = HashMap::new();
        let doc = doc! {
            "_id": "pg",
            "versions": [
                {
                    "major_version": "13",
                    "grace_date": "2025-05-13T00:00:00Z",
                    "eol_date": "2025-11-13T00:00:00Z"
                },
                {
                    "major_version": "14",
                    "grace_date": "2026-05-12T00:00:00Z",
                    "eol_date": "2026-11-12T00:00:00Z"
                }
            ]
        };

        extract_rows(
            &Bson::Document(doc),
            &roots[0],
            None,
            true,
            &mut all_rows,
            &mut counters,
        );

        let rows = all_rows.get("engine").expect("root rows missing");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].as_deref(), Some("pg"));
        assert_eq!(
            rows[0][1].as_deref(),
            Some(
                "[{\"major_version\":\"13\",\"grace_date\":\"2025-05-13T00:00:00Z\",\"eol_date\":\"2025-11-13T00:00:00Z\"},{\"major_version\":\"14\",\"grace_date\":\"2026-05-12T00:00:00Z\",\"eol_date\":\"2026-11-12T00:00:00Z\"}]"
            )
        );
    }
}
