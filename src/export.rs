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
use flate2::write::GzEncoder;
use flate2::Compression;
use futures::TryStreamExt;
use mongodb::Client;

use crate::schema_diagram::{parse_sql, Table as SqlTable};

// ──────────────────────────────────────────────────────────────────────────────
// Sanitize  (mirrors to_pg::sanitize so we can reverse-map SQL columns → Mongo fields)
// ──────────────────────────────────────────────────────────────────────────────

fn is_pg_reserved(s: &str) -> bool {
    matches!(
        s,
        "all"
            | "analyse"
            | "analyze"
            | "and"
            | "any"
            | "array"
            | "as"
            | "asc"
            | "asymmetric"
            | "authorization"
            | "binary"
            | "both"
            | "case"
            | "cast"
            | "check"
            | "collate"
            | "collation"
            | "column"
            | "concurrently"
            | "constraint"
            | "create"
            | "cross"
            | "current_catalog"
            | "current_date"
            | "current_role"
            | "current_schema"
            | "current_time"
            | "current_timestamp"
            | "current_user"
            | "default"
            | "deferrable"
            | "desc"
            | "distinct"
            | "do"
            | "else"
            | "end"
            | "except"
            | "false"
            | "fetch"
            | "for"
            | "foreign"
            | "freeze"
            | "from"
            | "full"
            | "grant"
            | "group"
            | "having"
            | "ilike"
            | "in"
            | "initially"
            | "inner"
            | "intersect"
            | "into"
            | "is"
            | "isnull"
            | "join"
            | "lateral"
            | "leading"
            | "left"
            | "like"
            | "limit"
            | "localtime"
            | "localtimestamp"
            | "natural"
            | "not"
            | "notnull"
            | "null"
            | "offset"
            | "on"
            | "only"
            | "or"
            | "order"
            | "outer"
            | "overlaps"
            | "placing"
            | "primary"
            | "references"
            | "returning"
            | "right"
            | "select"
            | "session_user"
            | "similar"
            | "some"
            | "symmetric"
            | "system_user"
            | "table"
            | "tablesample"
            | "then"
            | "to"
            | "trailing"
            | "true"
            | "union"
            | "unique"
            | "user"
            | "using"
            | "variadic"
            | "verbose"
            | "when"
            | "where"
            | "window"
            | "with"
    )
}

fn sanitize(name: &str) -> String {
    let s: String = name
        .to_lowercase()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if s.starts_with(|c: char| c.is_ascii_digit()) || is_pg_reserved(&s) {
        format!("_{s}")
    } else {
        s
    }
}

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

/// Format a Unix-millisecond timestamp as `YYYY-MM-DDTHH:MM:SS.mmmZ`.
///
/// Uses Howard Hinnant's civil-calendar algorithm (public domain) to avoid
/// pulling in extra feature flags from the `chrono` crate.
fn format_millis(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let ms_part = ms.rem_euclid(1000) as u32;

    let days = secs.div_euclid(86400) as i32;
    let time = secs.rem_euclid(86400) as u32;
    let (h, mi, s) = (time / 3600, (time % 3600) / 60, time % 60);

    // Gregorian date from days-since-epoch (Hinnant's civil.h)
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if mo <= 2 { y + 1 } else { y };

    format!(
        "{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{ms_part:03}Z",
        mo = mo as u32,
        d = d as u32,
    )
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
    /// Primary-key column name (always `"id"`)
    pk_col: String,
    /// FK column pointing to the parent table (`None` for root tables)
    fk_col: Option<String>,
    /// MongoDB field name at the current document level that yields this table's data.
    /// Empty for the root table.
    mongo_field: String,
    /// `true` when this child table represents a scalar array
    /// (has exactly 3 columns: pk, fk, `value`).
    is_scalar_array: bool,
    /// Columns whose SQL type is JSONB – serialised as clean JSON rather than
    /// plain text so PostgreSQL COPY can ingest them.
    jsonb_cols: HashSet<String>,
    children: Vec<TableNode>,
}

fn build_tree(sql_tables: &[SqlTable]) -> Vec<TableNode> {
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
        .map(|r| build_node(r, None, &children_of))
        .collect()
}

fn build_node(
    sql_t: &SqlTable,
    parent_name: Option<&str>,
    children_of: &HashMap<&str, Vec<&SqlTable>>,
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

    let pk_col = sql_t
        .columns
        .iter()
        .find(|c| c.primary_key)
        .map(|c| c.name.clone())
        .unwrap_or_else(|| "id".to_owned());

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

    let children: Vec<TableNode> = children_of
        .get(sql_t.name.as_str())
        .map(|cs| {
            cs.iter()
                .map(|child_sql| build_node(child_sql, Some(&sql_t.name), children_of))
                .collect()
        })
        .unwrap_or_default();

    TableNode {
        sql_name: sql_t.name.clone(),
        columns,
        pk_col,
        fk_col,
        mongo_field,
        is_scalar_array,
        jsonb_cols,
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
    if let Some(v) = doc.get(sql_col) {
        return Some(v);
    }
    for (key, val) in doc.iter() {
        if sanitize(key) == sql_col {
            return Some(val);
        }
    }
    None
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
            if col == &node.pk_col {
                Some(my_id.clone())
            } else if Some(col) == node.fk_col.as_ref() {
                Some(parent_id.unwrap_or("").to_owned())
            } else {
                find_mongo_field(doc, col)
                    .map(|v| {
                        if node.jsonb_cols.contains(col) {
                            Some(serde_json::to_string(&bson_to_json_value(v)).unwrap_or_default())
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
                                if col == &child.pk_col {
                                    Some(child_id.clone())
                                } else if Some(col) == child.fk_col.as_ref() {
                                    Some(my_id.clone())
                                } else if col == "value" {
                                    bson_to_string(item)
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

    let roots = build_tree(&sql_tables);

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
