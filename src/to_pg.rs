//! PostgreSQL DDL generation from [`CollectionSchema`].
//!
//! Converts the internal schema representation produced by [`crate::analyzer::Analyzer`]
//! into one or more `CREATE TABLE` statements:
//!
//! * Scalar fields → columns with the closest PostgreSQL type.
//! * `Object` fields → separate 1:1 child table with a FK back to the parent.
//! * `Array` of objects → child table with a FK (one row per array element).
//! * `Array` of scalars → child table with a `value` column.
//! * Mixed-type fields (Object/Array combined with scalars) → `JSONB`.
//! * Hex-keyed map documents → child table with an extra `key TEXT NOT NULL` column.
//!
//! The public entry point is [`schema_to_ddl`].

use indexmap::IndexMap;

use crate::analyzer::{
    CollectionSchema, FieldSchema, TypeSchema, TYPE_ARRAY, TYPE_BINARY, TYPE_BOOLEAN, TYPE_CODE,
    TYPE_CODE_W_SCOPE, TYPE_DATE, TYPE_DBPOINTER, TYPE_DECIMAL128, TYPE_MAXKEY, TYPE_MINKEY,
    TYPE_NULL, TYPE_NUMBER, TYPE_OBJECT, TYPE_OBJECTID, TYPE_REGEX, TYPE_STRING, TYPE_SYMBOL,
    TYPE_TIMESTAMP, TYPE_UNDEFINED,
};

// ──────────────────────────────────────────────────────────────────────────────
// BSON → PostgreSQL type mapping
// ──────────────────────────────────────────────────────────────────────────────

fn bson_type_to_pg(t: &str) -> &'static str {
    match t {
        TYPE_STRING => "TEXT",
        TYPE_NUMBER => "DOUBLE PRECISION",
        TYPE_BOOLEAN => "BOOLEAN",
        TYPE_DATE => "TIMESTAMP WITH TIME ZONE",
        TYPE_DECIMAL128 => "NUMERIC",
        TYPE_OBJECTID => "TEXT",
        TYPE_BINARY => "BYTEA",
        TYPE_TIMESTAMP => "BIGINT",
        TYPE_REGEX | TYPE_SYMBOL | TYPE_CODE | TYPE_CODE_W_SCOPE | TYPE_DBPOINTER | TYPE_MAXKEY
        | TYPE_MINKEY => "TEXT",
        _ => "TEXT",
    }
}

fn is_null_type(t: &str) -> bool {
    t == TYPE_NULL || t == TYPE_UNDEFINED
}

// ──────────────────────────────────────────────────────────────────────────────
// PostgreSQL identifier sanitization
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

/// Convert a MongoDB field name to a valid, lowercase PostgreSQL identifier.
///
/// Non-ASCII-alphanumeric characters are replaced with `_`. Names that start
/// with a digit or clash with a PostgreSQL reserved word are prefixed with `_`.
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

/// Return the plain scalar type to use for FK columns that reference serial PKs.
fn fk_scalar_type(pg_type: &str) -> &str {
    match pg_type {
        "SERIAL" => "INTEGER",
        "BIGSERIAL" => "BIGINT",
        _ => pg_type,
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Value-based type heuristics
// ──────────────────────────────────────────────────────────────────────────────

/// Returns `true` when every sampled value is a finite whole number.
fn all_values_are_whole(values: &[serde_json::Value]) -> bool {
    !values.is_empty()
        && values
            .iter()
            .all(|v| v.as_f64().is_some_and(|f| f.is_finite() && f == f.floor()))
}

/// Returns a narrow integer PG type when every sampled value is a numeric string.
fn numeric_string_pg_type(values: &[serde_json::Value]) -> Option<&'static str> {
    if values.is_empty() {
        return None;
    }
    if values
        .iter()
        .all(|v| v.as_str().is_some_and(|s| s.parse::<i64>().is_ok()))
    {
        let max = values
            .iter()
            .filter_map(|v| v.as_str().and_then(|s| s.parse::<i64>().ok()))
            .map(|n| n.unsigned_abs())
            .max()
            .unwrap_or(0);
        return Some(if max <= 2_147_483_647 {
            "INTEGER"
        } else {
            "BIGINT"
        });
    }
    if values
        .iter()
        .all(|v| v.as_str().is_some_and(|s| s.parse::<f64>().is_ok()))
    {
        return Some("DOUBLE PRECISION");
    }
    None
}

/// Returns `true` when every sampled Date value has a midnight UTC time component,
/// making it a good candidate for `DATE` rather than `TIMESTAMP WITH TIME ZONE`.
fn all_dates_are_date_only(values: &[serde_json::Value]) -> bool {
    !values.is_empty()
        && values.iter().all(|v| {
            v.as_str().is_some_and(|s| {
                let norm = s.replace(".000Z", "Z").replace(".000+00:00", "Z");
                norm.ends_with("T00:00:00Z")
            })
        })
}

/// Returns `VARCHAR(n)` when the field name contains "code" and all sampled
/// string values are shorter than 5 characters.
fn varchar_for_code_field(col_name: &str, values: &[serde_json::Value]) -> Option<String> {
    if !col_name.contains("code") || values.is_empty() {
        return None;
    }
    let max_len = values
        .iter()
        .filter_map(|v| v.as_str().map(str::len))
        .max()?;
    if max_len < 5 {
        Some(format!("VARCHAR({max_len})"))
    } else {
        None
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Type resolution
// ──────────────────────────────────────────────────────────────────────────────

/// Map a slice of non-null `(bson_type_name, TypeSchema)` pairs to a single PG type.
fn resolve_scalar_pg_type(non_null: &[(&str, &TypeSchema)], col_name: &str) -> String {
    if non_null.is_empty() {
        return "TEXT".to_owned();
    }

    if non_null.len() == 1 {
        let (name, ts) = non_null[0];
        let values = ts.values.as_deref().unwrap_or(&[]);

        if name == TYPE_NUMBER {
            if all_values_are_whole(values) {
                let max = values
                    .iter()
                    .filter_map(|v| v.as_f64())
                    .map(|f| f.abs() as u64)
                    .max()
                    .unwrap_or(0);
                return if max <= 2_147_483_647 {
                    "INTEGER".to_owned()
                } else {
                    "BIGINT".to_owned()
                };
            }
        }
        if name == TYPE_DATE && all_dates_are_date_only(values) {
            return "DATE".to_owned();
        }
        if name == TYPE_STRING {
            if let Some(pg) = numeric_string_pg_type(values) {
                return pg.to_owned();
            }
            if let Some(pg) = varchar_for_code_field(col_name, values) {
                return pg;
            }
        }
        return bson_type_to_pg(name).to_owned();
    }

    // Multiple non-null types – try to widen numeric types before falling back to TEXT.
    // In the Rust schema, all numeric BSON types collapse to TYPE_NUMBER or TYPE_DECIMAL128.
    let all_numeric = non_null
        .iter()
        .all(|(n, _)| *n == TYPE_NUMBER || *n == TYPE_DECIMAL128);
    if all_numeric {
        if non_null.iter().any(|(n, _)| *n == TYPE_DECIMAL128) {
            return "NUMERIC".to_owned();
        }
        return "DOUBLE PRECISION".to_owned();
    }

    "TEXT".to_owned()
}

/// Infer the best PostgreSQL PK column type from the `_id` field's BSON types.
fn pk_type_for_id(non_null: &[(&str, &TypeSchema)]) -> String {
    if non_null.len() != 1 {
        return "TEXT".to_owned();
    }
    let (name, ts) = non_null[0];
    if name == TYPE_OBJECTID {
        return "UUID".to_owned();
    }
    if name == TYPE_NUMBER {
        let values = ts.values.as_deref().unwrap_or(&[]);
        if all_values_are_whole(values) {
            let max = values
                .iter()
                .filter_map(|v| v.as_f64())
                .map(|f| f.abs() as u64)
                .max()
                .unwrap_or(0);
            return if max <= 2_147_483_647 {
                "SERIAL".to_owned()
            } else {
                "BIGSERIAL".to_owned()
            };
        }
        return "BIGSERIAL".to_owned();
    }
    if name == TYPE_STRING {
        let values = ts.values.as_deref().unwrap_or(&[]);
        if !values.is_empty()
            && values
                .iter()
                .all(|v| v.as_str().is_some_and(|s| s.parse::<i64>().is_ok()))
        {
            return "BIGSERIAL".to_owned();
        }
    }
    "TEXT".to_owned()
}

// ──────────────────────────────────────────────────────────────────────────────
// Internal table / column structs
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct Column {
    name: String,
    pg_type: String,
    nullable: bool,
    primary_key: bool,
}

#[derive(Debug)]
struct Table {
    name: String,
    columns: Vec<Column>,
    child_tables: Vec<Table>,
    /// `(fk_column_name, referenced_table_name, referenced_column_name)`
    parent_ref: Option<(String, String, String)>,
}

impl Table {
    fn new(name: String) -> Self {
        Self {
            name,
            columns: Vec::new(),
            child_tables: Vec::new(),
            parent_ref: None,
        }
    }
}

/// Returns `(pk_column_name, pk_pg_type)` for a table, falling back to `("id", "TEXT")`.
fn find_pk(table: &Table) -> (String, String) {
    for col in &table.columns {
        if col.primary_key {
            return (col.name.clone(), col.pg_type.clone());
        }
    }
    ("id".to_owned(), "TEXT".to_owned())
}

// ──────────────────────────────────────────────────────────────────────────────
// Hex-keyed map document detection
// ──────────────────────────────────────────────────────────────────────────────

/// Returns `true` when `name` looks like a MongoDB map key (8+ lowercase hex chars).
fn is_hex_keyed_name(name: &str) -> bool {
    name.len() >= 8 && name.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f'))
}

/// For a map document (all sub-fields are hex-keyed), try to find uniform inner
/// document fields. Returns the first non-empty inner `IndexMap` found, or `None`
/// if there is no uniform inner structure (caller should fall back to JSONB).
fn map_value_fields(doc_ts: &TypeSchema) -> Option<&IndexMap<String, FieldSchema>> {
    let sub_fields = doc_ts.object.as_ref()?;
    if sub_fields.is_empty() {
        return None;
    }
    for sf in sub_fields.values() {
        let nonnull: Vec<_> = sf
            .types
            .iter()
            .filter(|(t, _)| !is_null_type(t.as_str()))
            .collect();
        if nonnull.len() == 1 && nonnull[0].0 == TYPE_OBJECT {
            if let Some(inner) = &nonnull[0].1.object {
                if !inner.is_empty() {
                    return Some(inner);
                }
            }
        }
    }
    None
}

// ──────────────────────────────────────────────────────────────────────────────
// Child table builders
// ──────────────────────────────────────────────────────────────────────────────

fn add_child_table(
    parent: &mut Table,
    array_field_col: &str,
    child_fields: &IndexMap<String, FieldSchema>,
) {
    let child_name = format!("{}_{}", parent.name, array_field_col);
    let (pk_name, pk_type) = find_pk(parent);
    let fk_col = format!("{}_id", parent.name);

    let mut child = Table::new(child_name);
    child.columns.push(Column {
        name: "id".to_owned(),
        pg_type: "BIGSERIAL".to_owned(),
        nullable: false,
        primary_key: true,
    });
    child.columns.push(Column {
        name: fk_col.clone(),
        pg_type: fk_scalar_type(&pk_type).to_owned(),
        nullable: false,
        primary_key: false,
    });
    child.parent_ref = Some((fk_col, parent.name.clone(), pk_name));

    process_fields(&mut child, child_fields, "", true);
    parent.child_tables.push(child);
}

fn add_scalar_array_table(
    parent: &mut Table,
    array_field_col: &str,
    scalar_types: &[(&str, &TypeSchema)],
) {
    let child_name = format!("{}_{}", parent.name, array_field_col);
    let (pk_name, pk_type) = find_pk(parent);
    let fk_col = format!("{}_id", parent.name);

    let mut child = Table::new(child_name);
    child.columns.push(Column {
        name: "id".to_owned(),
        pg_type: "BIGSERIAL".to_owned(),
        nullable: false,
        primary_key: true,
    });
    child.columns.push(Column {
        name: fk_col.clone(),
        pg_type: fk_scalar_type(&pk_type).to_owned(),
        nullable: false,
        primary_key: false,
    });
    child.parent_ref = Some((fk_col, parent.name.clone(), pk_name));

    let non_null: Vec<(&str, &TypeSchema)> = scalar_types
        .iter()
        .filter(|(t, _)| !is_null_type(t))
        .copied()
        .collect();
    let value_type = if non_null.is_empty() {
        "TEXT".to_owned()
    } else {
        resolve_scalar_pg_type(&non_null, "value")
    };
    child.columns.push(Column {
        name: "value".to_owned(),
        pg_type: value_type,
        nullable: false,
        primary_key: false,
    });

    parent.child_tables.push(child);
}

/// Create a 1:1 child table for an embedded document field.
fn add_doc_table(
    parent: &mut Table,
    doc_field_col: &str,
    doc_fields: &IndexMap<String, FieldSchema>,
) {
    let child_name = format!("{}_{}", parent.name, doc_field_col);
    let (pk_name, pk_type) = find_pk(parent);
    let fk_col = format!("{}_id", parent.name);

    let mut child = Table::new(child_name);
    child.columns.push(Column {
        name: "id".to_owned(),
        pg_type: "BIGSERIAL".to_owned(),
        nullable: false,
        primary_key: true,
    });
    child.columns.push(Column {
        name: fk_col.clone(),
        pg_type: fk_scalar_type(&pk_type).to_owned(),
        nullable: false,
        primary_key: false,
    });
    child.parent_ref = Some((fk_col, parent.name.clone(), pk_name));

    process_fields(&mut child, doc_fields, "", false);
    parent.child_tables.push(child);
}

/// Create a child table for a MongoDB map document (dynamic hex-keyed sub-fields).
/// An extra `key TEXT NOT NULL` column holds the original dynamic key.
fn add_map_table(
    parent: &mut Table,
    map_field_col: &str,
    value_fields: &IndexMap<String, FieldSchema>,
) {
    let child_name = format!("{}_{}", parent.name, map_field_col);
    let (pk_name, pk_type) = find_pk(parent);
    let fk_col = format!("{}_id", parent.name);

    let mut child = Table::new(child_name);
    child.columns.push(Column {
        name: "id".to_owned(),
        pg_type: "BIGSERIAL".to_owned(),
        nullable: false,
        primary_key: true,
    });
    child.columns.push(Column {
        name: fk_col.clone(),
        pg_type: fk_scalar_type(&pk_type).to_owned(),
        nullable: false,
        primary_key: false,
    });
    child.parent_ref = Some((fk_col, parent.name.clone(), pk_name));
    child.columns.push(Column {
        name: "key".to_owned(),
        pg_type: "TEXT".to_owned(),
        nullable: false,
        primary_key: false,
    });

    process_fields(&mut child, value_fields, "", false);
    parent.child_tables.push(child);
}

fn handle_array_field(
    table: &mut Table,
    col_name: &str,
    items_field: &FieldSchema,
    nullable: bool,
) {
    let non_null_items: Vec<(&str, &TypeSchema)> = items_field
        .types
        .iter()
        .filter(|(t, _)| !is_null_type(t.as_str()))
        .map(|(t, ts)| (t.as_str(), ts))
        .collect();

    if let Some((_, doc_ts)) = non_null_items.iter().find(|(t, _)| *t == TYPE_OBJECT) {
        // Array of documents → child table (one row per array element)
        if let Some(child_fields) = &doc_ts.object {
            let cf = child_fields.clone();
            add_child_table(table, col_name, &cf);
        } else {
            // Object type but no inner fields schema
            table.columns.push(Column {
                name: col_name.to_owned(),
                pg_type: "JSONB".to_owned(),
                nullable,
                primary_key: false,
            });
        }
    } else {
        // Array of scalars → child table with `value` column
        add_scalar_array_table(table, col_name, &non_null_items);
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Main recursive field processor
// ──────────────────────────────────────────────────────────────────────────────

fn process_fields(
    table: &mut Table,
    fields: &IndexMap<String, FieldSchema>,
    col_prefix: &str,
    mark_pk: bool,
) {
    for (raw_name, field) in fields {
        let col_name = sanitize(&format!("{col_prefix}{raw_name}"));

        let non_null: Vec<(&str, &TypeSchema)> = field
            .types
            .iter()
            .filter(|(t, _)| !is_null_type(t.as_str()))
            .map(|(t, ts)| (t.as_str(), ts))
            .collect();

        // A field is nullable when it has a Null/Undefined type OR when it is
        // absent in some documents (prob < 1.0).
        let nullable = non_null.len() < field.types.len() || field.probability < 1.0;

        if non_null.is_empty() {
            // Field is always null/undefined – omit from DDL.
            continue;
        }

        // ── Single pure Object ────────────────────────────────────────────────
        if non_null.len() == 1 && non_null[0].0 == TYPE_OBJECT {
            let ts = non_null[0].1;
            let is_hex = ts
                .object
                .as_ref()
                .is_some_and(|sf| !sf.is_empty() && sf.keys().all(|k| is_hex_keyed_name(k)));

            if is_hex {
                // Map document (dynamic hex keys)
                let value_fields = map_value_fields(ts).map(|vf| vf.clone());
                if let Some(vf) = value_fields {
                    add_map_table(table, &col_name, &vf);
                } else {
                    table.columns.push(Column {
                        name: col_name,
                        pg_type: "JSONB".to_owned(),
                        nullable,
                        primary_key: false,
                    });
                }
            } else if let Some(sub_fields) = &ts.object {
                let sf = sub_fields.clone();
                add_doc_table(table, &col_name, &sf);
            } else {
                // Object type but no inner field schema (empty document)
                table.columns.push(Column {
                    name: col_name,
                    pg_type: "JSONB".to_owned(),
                    nullable,
                    primary_key: false,
                });
            }
            continue;
        }

        // ── Single pure Array ─────────────────────────────────────────────────
        if non_null.len() == 1 && non_null[0].0 == TYPE_ARRAY {
            let ts = non_null[0].1;
            if let Some(items_field) = &ts.array {
                let items = *items_field.clone();
                handle_array_field(table, &col_name, &items, nullable);
            } else {
                // Array with no items schema → JSONB
                table.columns.push(Column {
                    name: col_name,
                    pg_type: "JSONB".to_owned(),
                    nullable,
                    primary_key: false,
                });
            }
            continue;
        }

        // ── Object or Array mixed with other types → JSONB ────────────────────
        if non_null
            .iter()
            .any(|(t, _)| *t == TYPE_OBJECT || *t == TYPE_ARRAY)
        {
            table.columns.push(Column {
                name: col_name,
                pg_type: "JSONB".to_owned(),
                nullable,
                primary_key: false,
            });
            continue;
        }

        // ── Scalar field(s) ───────────────────────────────────────────────────
        let is_pk = raw_name == "_id" && col_prefix.is_empty() && mark_pk;
        let pg_type = if is_pk {
            pk_type_for_id(&non_null)
        } else {
            resolve_scalar_pg_type(&non_null, &col_name)
        };

        if is_pk {
            if table.columns.iter().any(|c| c.primary_key) {
                // A surrogate PK was already added (e.g. from add_child_table).
                continue;
            }
            table.columns.push(Column {
                name: "id".to_owned(),
                pg_type,
                nullable: false,
                primary_key: true,
            });
        } else {
            // Guard against a literal field named "id" clashing with a surrogate PK.
            let final_name = if col_name == "id" && table.columns.iter().any(|c| c.name == "id") {
                "field_id".to_owned()
            } else {
                col_name
            };
            table.columns.push(Column {
                name: final_name,
                pg_type,
                nullable,
                primary_key: false,
            });
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Tree traversal and DDL rendering
// ──────────────────────────────────────────────────────────────────────────────

fn collect_tables(root: &Table) -> Vec<&Table> {
    let mut result: Vec<&Table> = Vec::new();
    let mut queue: Vec<&Table> = vec![root];
    while !queue.is_empty() {
        let current = queue.remove(0);
        result.push(current);
        for child in &current.child_tables {
            queue.push(child);
        }
    }
    result
}

fn render_column(col: &Column) -> String {
    let constraint = if col.primary_key {
        " PRIMARY KEY"
    } else if !col.nullable {
        " NOT NULL"
    } else {
        ""
    };
    format!("    {}{} {}{constraint}", col.name, "", col.pg_type)
        .trim_end()
        .to_owned()
}

fn render_table(table: &Table) -> String {
    let mut defs: Vec<String> = table.columns.iter().map(render_column).collect();
    if let Some((fk_col, ref_table, ref_col)) = &table.parent_ref {
        defs.push(format!(
            "    FOREIGN KEY ({fk_col}) REFERENCES {ref_table} ({ref_col}) DEFERRABLE INITIALLY DEFERRED"
        ));
    }
    let body = defs.join(",\n");
    format!("CREATE TABLE {} (\n{}\n);", table.name, body)
}

// ──────────────────────────────────────────────────────────────────────────────
// Public API
// ──────────────────────────────────────────────────────────────────────────────

/// Convert a [`CollectionSchema`] to PostgreSQL DDL `CREATE TABLE` statements.
///
/// Nested objects become 1:1 child tables. Arrays of documents become 1:N child
/// tables. Arrays of scalars become child tables with a `value` column.
/// Mixed-type or empty-array fields fall back to `JSONB`.
///
/// Summary statistics (table and column counts) are printed to **stderr**.
///
/// # Arguments
/// * `schema` – The schema produced by [`crate::analyzer::Analyzer::finish`].
/// * `table_name` – Base name for the root table (will be sanitised).
///
/// # Returns
/// A string containing one or more `CREATE TABLE` statements separated by blank lines.
pub fn schema_to_ddl(schema: &CollectionSchema, table_name: &str) -> String {
    let mut root = Table::new(sanitize(table_name));
    process_fields(&mut root, &schema.object, "", true);

    let tables = collect_tables(&root);
    let total_cols: usize = tables.iter().map(|t| t.columns.len()).sum();

    eprintln!("tables : {}", tables.len());
    eprintln!("columns: {total_cols}");

    tables
        .iter()
        .map(|t| render_table(t))
        .collect::<Vec<_>>()
        .join("\n\n")
}

// ──────────────────────────────────────────────────────────────────────────────
// Unit tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyzer::Analyzer;
    use bson::doc;

    fn analyze(docs: &[bson::Document]) -> CollectionSchema {
        let mut a = Analyzer::new(true);
        for d in docs {
            a.process_document(d);
        }
        a.finish()
    }

    #[test]
    fn test_flat_collection() {
        let docs = vec![
            doc! { "_id": 1_i32, "name": "Alice", "score": 99_i32 },
            doc! { "_id": 2_i32, "name": "Bob",   "score": 88_i32 },
        ];
        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "users");
        assert!(ddl.contains("CREATE TABLE users ("), "root table missing");
        assert!(ddl.contains("name TEXT"), "name column missing");
        assert!(ddl.contains("score"), "score column missing");
    }

    #[test]
    fn test_nullable_field() {
        let docs = vec![
            doc! { "_id": 1_i32, "opt": "present" },
            doc! { "_id": 2_i32 },
        ];
        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "t");
        // opt must not be NOT NULL
        assert!(
            !ddl.contains("opt TEXT NOT NULL"),
            "optional field must not have NOT NULL"
        );
    }

    #[test]
    fn test_objectid_pk_becomes_uuid() {
        let docs = vec![doc! { "_id": bson::oid::ObjectId::new(), "x": 1_i32 }];
        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "t");
        assert!(
            ddl.contains("id UUID PRIMARY KEY"),
            "ObjectId PK should become UUID"
        );
    }

    #[test]
    fn test_nested_object_creates_child_table() {
        let docs = vec![doc! { "_id": 1_i32, "addr": { "city": "Paris" } }];
        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "orders");
        assert!(
            ddl.contains("CREATE TABLE orders_addr ("),
            "child table for addr missing"
        );
        assert!(ddl.contains("FOREIGN KEY"), "FK constraint missing");
    }

    #[test]
    fn test_array_of_scalars_creates_child_table() {
        let docs = vec![doc! { "_id": 1_i32, "tags": ["rust", "mongodb"] }];
        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "posts");
        assert!(
            ddl.contains("CREATE TABLE posts_tags ("),
            "scalar array child table missing"
        );
        assert!(ddl.contains("value TEXT"), "value column missing");
    }

    #[test]
    fn test_reserved_word_prefixed() {
        let docs = vec![doc! { "_id": 1_i32, "order": "asc" }];
        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "t");
        assert!(
            ddl.contains("_order"),
            "reserved word 'order' should be prefixed with _"
        );
    }

    #[test]
    fn test_sanitize() {
        assert_eq!(sanitize("myField"), "myfield");
        assert_eq!(sanitize("my-field"), "my_field");
        assert_eq!(sanitize("123abc"), "_123abc");
        assert_eq!(sanitize("order"), "_order");
        assert_eq!(sanitize("_id"), "_id");
    }
}
