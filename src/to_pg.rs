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
use std::collections::{HashMap, HashSet};

use crate::analyzer::{
    CollectionSchema, FieldSchema, TypeSchema, TYPE_ARRAY, TYPE_BINARY, TYPE_BOOLEAN, TYPE_CODE,
    TYPE_CODE_W_SCOPE, TYPE_DATE, TYPE_DBPOINTER, TYPE_DECIMAL128, TYPE_DOUBLE, TYPE_INT32,
    TYPE_INT64, TYPE_MAXKEY, TYPE_MINKEY, TYPE_NUMBER, TYPE_OBJECT, TYPE_OBJECTID, TYPE_REGEX,
    TYPE_STRING, TYPE_SYMBOL, TYPE_TIMESTAMP,
};

// ──────────────────────────────────────────────────────────────────────────────
// BSON → PostgreSQL type mapping
// ──────────────────────────────────────────────────────────────────────────────

use crate::util::{
    can_inline_object_fields, flatten_grouped_root_array_object_fields,
    flatten_root_array_object_field,
    flattened_root_parent_id_column, is_null_type,
    grouped_root_array_object_fields, inline_object_column_names_with_prefix,
    inline_object_leaf_fields_with_prefix, matches_timestamp_field, scalar_type_family, sanitize,
};

const FORCED_TIMESTAMP_PG_TYPE: &str = "TIMESTAMP WITH TIME ZONE";

fn bson_type_to_pg(t: &str) -> &'static str {
    match t {
        TYPE_STRING => "TEXT",
        TYPE_NUMBER | TYPE_DOUBLE => "DOUBLE PRECISION",
        TYPE_INT32 => "INTEGER",
        TYPE_INT64 => "BIGINT",
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

// ──────────────────────────────────────────────────────────────────────────────
// PostgreSQL identifier sanitization
// ──────────────────────────────────────────────────────────────────────────────

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

/// Returns the PostgreSQL string type for sampled MongoDB string values.
///
/// - max length `<= 5`  -> `VARCHAR(max_len)`
/// - max length `<= 20` -> `VARCHAR(20)`
/// - max length `> 20`  -> `TEXT`
fn pg_string_type(values: &[serde_json::Value]) -> Option<String> {
    let max_len = values
        .iter()
        .filter_map(|v| v.as_str().map(str::len))
        .max()?;
    let max_len = max_len.max(1);

    if max_len <= 5 {
        Some(format!("VARCHAR({max_len})"))
    } else if max_len <= 20 {
        Some("VARCHAR(20)".to_owned())
    } else {
        Some("TEXT".to_owned())
    }
}

fn pg_string_type_from_schema(ts: &TypeSchema) -> Option<String> {
    if let Some(width) = ts.varchar_length {
        return Some(format!("VARCHAR({})", width.max(1)));
    }
    if let Some(max_length) = ts.max_length {
        if max_length > 20 {
            return Some("TEXT".to_owned());
        }
        return Some(format!("VARCHAR({})", max_length.max(1)));
    }

    pg_string_type(ts.values.as_deref().unwrap_or(&[]))
}

// ──────────────────────────────────────────────────────────────────────────────
// Type resolution
// ──────────────────────────────────────────────────────────────────────────────

/// Map a slice of non-null `(bson_type_name, TypeSchema)` pairs to a single PG type.
fn resolve_scalar_pg_type(non_null: &[(&str, &TypeSchema)], _col_name: &str) -> String {
    if non_null.is_empty() {
        return "TEXT".to_owned();
    }

    if non_null.len() == 1 {
        let (name, ts) = non_null[0];
        let values = ts.values.as_deref().unwrap_or(&[]);

        // MongoDB Number collapses multiple BSON numeric subtypes into one bucket,
        // so keep regular scalar fields wide enough for later decimal values.
        if matches!(name, TYPE_NUMBER | TYPE_DOUBLE) {
            return "DOUBLE PRECISION".to_owned();
        }
        if name == TYPE_INT32 {
            return "INTEGER".to_owned();
        }
        if name == TYPE_INT64 {
            return "BIGINT".to_owned();
        }
        if name == TYPE_DATE && all_dates_are_date_only(values) {
            return "DATE".to_owned();
        }
        if name == TYPE_STRING {
            if let Some(pg) = pg_string_type_from_schema(ts) {
                return pg;
            }
        }
        return bson_type_to_pg(name).to_owned();
    }

    // Multiple non-null types – try to widen numeric types before falling back to TEXT.
    let all_numeric = non_null.iter().all(|(n, _)| {
        matches!(
            *n,
            TYPE_NUMBER | TYPE_DOUBLE | TYPE_INT32 | TYPE_INT64 | TYPE_DECIMAL128
        )
    });
    if all_numeric {
        if non_null.iter().any(|(n, _)| *n == TYPE_DECIMAL128) {
            return "NUMERIC".to_owned();
        }
        if non_null
            .iter()
            .any(|(n, _)| matches!(*n, TYPE_NUMBER | TYPE_DOUBLE))
        {
            return "DOUBLE PRECISION".to_owned();
        }
        if non_null.iter().any(|(n, _)| *n == TYPE_INT64) {
            return "BIGINT".to_owned();
        }
        return "INTEGER".to_owned();
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
        return "TEXT".to_owned();
    }
    if name == TYPE_DOUBLE || name == TYPE_NUMBER {
        return "BIGSERIAL".to_owned();
    }
    if name == TYPE_INT32 {
        return "SERIAL".to_owned();
    }
    if name == TYPE_INT64 {
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
        if let Some(pg) = pg_string_type_from_schema(ts) {
            return pg;
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
pub struct Table {
    name: String,
    columns: Vec<Column>,
    child_tables: Vec<Table>,
    /// `[(fk_column_name, referenced_table_name, referenced_column_name)]`
    parent_ref: Option<Vec<(String, String, String)>>,
}

impl Table {
    pub fn new(name: String) -> Self {
        Self {
            name,
            columns: Vec::new(),
            child_tables: Vec::new(),
            parent_ref: None,
        }
    }
}

/// Returns `[(pk_column_name, pk_pg_type)]` for a table, falling back to `[("id", "TEXT")]`.
fn find_pk_columns(table: &Table) -> Vec<(String, String)> {
    let pk_columns: Vec<(String, String)> = table
        .columns
        .iter()
        .filter(|col| col.primary_key)
        .map(|col| (col.name.clone(), col.pg_type.clone()))
        .collect();

    if pk_columns.is_empty() {
        vec![("id".to_owned(), "TEXT".to_owned())]
    } else {
        pk_columns
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Hex-keyed map document detection
// ──────────────────────────────────────────────────────────────────────────────

/// Returns `true` when `name` looks like a MongoDB map key (8+ lowercase hex chars).

fn is_hex_keyed_name(name: &str) -> bool {
    name.len() >= 8 && name.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f'))
}

/// Returns true if the string looks like a UUID (hex digits and dashes, 36 chars, 8-4-4-4-12)
fn is_uuid_keyed_name(name: &str) -> bool {
    let parts: Vec<&str> = name.split('-').collect();
    name.len() == 36
        && parts.len() == 5
        && parts[0].len() == 8
        && parts[1].len() == 4
        && parts[2].len() == 4
        && parts[3].len() == 4
        && parts[4].len() == 12
        && parts
            .iter()
            .all(|p| p.chars().all(|c| c.is_ascii_hexdigit()))
}

/// Returns true if all keys in the map look like UUIDs
fn all_keys_are_uuids(map: &IndexMap<String, FieldSchema>) -> bool {
    !map.is_empty() && map.keys().all(|k| is_uuid_keyed_name(k))
}

/// For a map document (all sub-fields are UUID-keyed), try to find uniform inner
/// document fields. Returns the first non-empty inner `IndexMap` found, or `None`
/// if there is no uniform inner structure (caller should fall back to JSONB).
fn map_uuid_value_fields(doc_ts: &TypeSchema) -> Option<&IndexMap<String, FieldSchema>> {
    let sub_fields = doc_ts.object.as_ref()?;
    if sub_fields.is_empty() || !all_keys_are_uuids(sub_fields) {
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
// Child table name helper
// ──────────────────────────────────────────────────────────────────────────────

/// Build a child table name, stripping the `{schema}_` prefix when `pg_schema`
/// is provided so that tables deployed inside a dedicated schema have shorter names.
fn child_table_name(parent_name: &str, field: &str, pg_schema: Option<&str>) -> String {
    let ancestor_segments = parent_name.split('_').collect::<Vec<_>>();
    let raw = if ancestor_segments.iter().any(|segment| *segment == field) {
        let parent_segment = ancestor_segments
            .last()
            .copied()
            .unwrap_or(parent_name);
        format!("{parent_segment}_{field}")
    } else {
        field.to_owned()
    };
    if let Some(schema) = pg_schema {
        let prefix = format!("{}_", sanitize(schema));
        raw.strip_prefix(&prefix).map(str::to_owned).unwrap_or(raw)
    } else {
        raw
    }
}

/// Optionally prepend `CREATE SCHEMA` + `SET search_path` preamble.
fn prepend_schema_preamble(ddl: String, pg_schema: Option<&str>) -> String {
    match pg_schema {
        None => ddl,
        Some(schema) => {
            let s = sanitize(schema);
            format!("CREATE SCHEMA IF NOT EXISTS {s};\nSET search_path = {s};\n\n{ddl}")
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Child table builders
// ──────────────────────────────────────────────────────────────────────────────

fn add_child_table(
    parent: &mut Table,
    array_field_col: &str,
    child_fields: &IndexMap<String, FieldSchema>,
    pg_schema: Option<&str>,
    timestamp_fields: &[String],
) {
    let child_name = child_table_name(&parent.name, array_field_col, pg_schema);
    let parent_pks = find_pk_columns(parent);

    let mut child = Table::new(child_name);
    child.columns.push(Column {
        name: "id".to_owned(),
        pg_type: "BIGSERIAL".to_owned(),
        nullable: false,
        primary_key: true,
    });

    let mut parent_ref = Vec::new();
    if parent_pks.len() == 1 {
        let (pk_name, pk_type) = &parent_pks[0];
        let fk_col = format!("{}_id", parent.name);
        child.columns.push(Column {
            name: fk_col.clone(),
            pg_type: fk_scalar_type(pk_type).to_owned(),
            nullable: false,
            primary_key: false,
        });
        parent_ref.push((fk_col, parent.name.clone(), pk_name.clone()));
    } else {
        for (pk_name, pk_type) in &parent_pks {
            let fk_col = format!("{}_{}", parent.name, pk_name);
            child.columns.push(Column {
                name: fk_col.clone(),
                pg_type: fk_scalar_type(pk_type).to_owned(),
                nullable: false,
                primary_key: false,
            });
            parent_ref.push((fk_col, parent.name.clone(), pk_name.clone()));
        }
    }
    child.parent_ref = Some(parent_ref);

    process_fields(
        &mut child,
        child_fields,
        "",
        true,
        false,
        pg_schema,
        timestamp_fields,
    );
    parent.child_tables.push(child);
}

fn add_grouped_root_child_table(
    parent: &mut Table,
    representative_field_col: &str,
    child_fields: &IndexMap<String, FieldSchema>,
    pg_schema: Option<&str>,
    timestamp_fields: &[String],
) {
    let child_name = child_table_name(&parent.name, representative_field_col, pg_schema);
    let parent_pks = find_pk_columns(parent);

    let mut child = Table::new(child_name);
    child.columns.push(Column {
        name: "id".to_owned(),
        pg_type: "BIGSERIAL".to_owned(),
        nullable: false,
        primary_key: true,
    });

    let mut parent_ref = Vec::new();
    if parent_pks.len() == 1 {
        let (pk_name, pk_type) = &parent_pks[0];
        let fk_col = format!("{}_id", parent.name);
        child.columns.push(Column {
            name: fk_col.clone(),
            pg_type: fk_scalar_type(pk_type).to_owned(),
            nullable: false,
            primary_key: false,
        });
        parent_ref.push((fk_col, parent.name.clone(), pk_name.clone()));
    } else {
        for (pk_name, pk_type) in &parent_pks {
            let fk_col = format!("{}_{}", parent.name, pk_name);
            child.columns.push(Column {
                name: fk_col.clone(),
                pg_type: fk_scalar_type(pk_type).to_owned(),
                nullable: false,
                primary_key: false,
            });
            parent_ref.push((fk_col, parent.name.clone(), pk_name.clone()));
        }
    }
    child.parent_ref = Some(parent_ref);
    child.columns.push(Column {
        name: "key".to_owned(),
        pg_type: "TEXT".to_owned(),
        nullable: false,
        primary_key: false,
    });

    process_fields(
        &mut child,
        child_fields,
        "",
        true,
        false,
        pg_schema,
        timestamp_fields,
    );
    parent.child_tables.push(child);
}

fn add_scalar_array_table(
    parent: &mut Table,
    array_field_col: &str,
    mongo_field_name: &str,
    scalar_types: &[(&str, &TypeSchema)],
    pg_schema: Option<&str>,
    timestamp_fields: &[String],
) {
    let child_name = child_table_name(&parent.name, array_field_col, pg_schema);
    let parent_pks = find_pk_columns(parent);

    let mut child = Table::new(child_name);
    child.columns.push(Column {
        name: "id".to_owned(),
        pg_type: "BIGSERIAL".to_owned(),
        nullable: false,
        primary_key: true,
    });

    let mut parent_ref = Vec::new();
    if parent_pks.len() == 1 {
        let (pk_name, pk_type) = &parent_pks[0];
        let fk_col = format!("{}_id", parent.name);
        child.columns.push(Column {
            name: fk_col.clone(),
            pg_type: fk_scalar_type(pk_type).to_owned(),
            nullable: false,
            primary_key: false,
        });
        parent_ref.push((fk_col, parent.name.clone(), pk_name.clone()));
    } else {
        for (pk_name, pk_type) in &parent_pks {
            let fk_col = format!("{}_{}", parent.name, pk_name);
            child.columns.push(Column {
                name: fk_col.clone(),
                pg_type: fk_scalar_type(pk_type).to_owned(),
                nullable: false,
                primary_key: false,
            });
            parent_ref.push((fk_col, parent.name.clone(), pk_name.clone()));
        }
    }
    child.parent_ref = Some(parent_ref);

    let non_null: Vec<(&str, &TypeSchema)> = scalar_types
        .iter()
        .filter(|(t, _)| !is_null_type(t))
        .copied()
        .collect();
    let value_type = if matches_timestamp_field(mongo_field_name, timestamp_fields) {
        FORCED_TIMESTAMP_PG_TYPE.to_owned()
    } else if non_null.is_empty() {
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
    pg_schema: Option<&str>,
    timestamp_fields: &[String],
) {
    let child_name = child_table_name(&parent.name, doc_field_col, pg_schema);
    let parent_pks = find_pk_columns(parent);

    let mut child = Table::new(child_name);
    child.columns.push(Column {
        name: "id".to_owned(),
        pg_type: "BIGSERIAL".to_owned(),
        nullable: false,
        primary_key: true,
    });

    let mut parent_ref = Vec::new();
    if parent_pks.len() == 1 {
        let (pk_name, pk_type) = &parent_pks[0];
        let fk_col = format!("{}_id", parent.name);
        child.columns.push(Column {
            name: fk_col.clone(),
            pg_type: fk_scalar_type(pk_type).to_owned(),
            nullable: false,
            primary_key: false,
        });
        parent_ref.push((fk_col, parent.name.clone(), pk_name.clone()));
    } else {
        for (pk_name, pk_type) in &parent_pks {
            let fk_col = format!("{}_{}", parent.name, pk_name);
            child.columns.push(Column {
                name: fk_col.clone(),
                pg_type: fk_scalar_type(pk_type).to_owned(),
                nullable: false,
                primary_key: false,
            });
            parent_ref.push((fk_col, parent.name.clone(), pk_name.clone()));
        }
    }
    child.parent_ref = Some(parent_ref);

    process_fields(
        &mut child,
        doc_fields,
        "",
        false,
        false,
        pg_schema,
        timestamp_fields,
    );
    parent.child_tables.push(child);
}

/// Create a child table for a MongoDB map document (dynamic hex-keyed sub-fields).
/// An extra `key TEXT NOT NULL` column holds the original dynamic key.
fn add_map_table(
    parent: &mut Table,
    map_field_col: &str,
    value_fields: &IndexMap<String, FieldSchema>,
    pg_schema: Option<&str>,
    timestamp_fields: &[String],
) {
    let child_name = child_table_name(&parent.name, map_field_col, pg_schema);
    let parent_pks = find_pk_columns(parent);

    let mut child = Table::new(child_name);
    child.columns.push(Column {
        name: "id".to_owned(),
        pg_type: "BIGSERIAL".to_owned(),
        nullable: false,
        primary_key: true,
    });

    let mut parent_ref = Vec::new();
    if parent_pks.len() == 1 {
        let (pk_name, pk_type) = &parent_pks[0];
        let fk_col = format!("{}_id", parent.name);
        child.columns.push(Column {
            name: fk_col.clone(),
            pg_type: fk_scalar_type(pk_type).to_owned(),
            nullable: false,
            primary_key: false,
        });
        parent_ref.push((fk_col, parent.name.clone(), pk_name.clone()));
    } else {
        for (pk_name, pk_type) in &parent_pks {
            let fk_col = format!("{}_{}", parent.name, pk_name);
            child.columns.push(Column {
                name: fk_col.clone(),
                pg_type: fk_scalar_type(pk_type).to_owned(),
                nullable: false,
                primary_key: false,
            });
            parent_ref.push((fk_col, parent.name.clone(), pk_name.clone()));
        }
    }
    child.parent_ref = Some(parent_ref);

    // Use UUID type if all keys are UUIDs, else TEXT
    let key_type = if value_fields.keys().all(|k| is_uuid_keyed_name(k)) {
        "UUID"
    } else {
        "TEXT"
    };
    child.columns.push(Column {
        name: "key".to_owned(),
        pg_type: key_type.to_owned(),
        nullable: false,
        primary_key: false,
    });

    process_fields(
        &mut child,
        value_fields,
        "",
        false,
        false,
        pg_schema,
        timestamp_fields,
    );
    parent.child_tables.push(child);
}

fn handle_array_field(
    table: &mut Table,
    col_name: &str,
    mongo_field_name: &str,
    items_field: &FieldSchema,
    nullable: bool,
    flatten_to_jsonb: bool,
    pg_schema: Option<&str>,
    timestamp_fields: &[String],
) {
    let non_null_items: Vec<(&str, &TypeSchema)> = items_field
        .types
        .iter()
        .filter(|(t, _)| !is_null_type(t.as_str()))
        .map(|(t, ts)| (t.as_str(), ts))
        .collect();

    if let Some((_, doc_ts)) = non_null_items.iter().find(|(t, _)| *t == TYPE_OBJECT) {
        if flatten_to_jsonb && doc_ts.as_jsonb {
            table.columns.push(Column {
                name: col_name.to_owned(),
                pg_type: "JSONB".to_owned(),
                nullable,
                primary_key: false,
            });
            return;
        }

        // Array of documents → child table (one row per array element)
        if let Some(child_fields) = &doc_ts.object {
            let cf = child_fields.clone();
            add_child_table(table, col_name, &cf, pg_schema, timestamp_fields);
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
        add_scalar_array_table(
            table,
            col_name,
            mongo_field_name,
            &non_null_items,
            pg_schema,
            timestamp_fields,
        );
    }
}

fn flatten_object_id_fields(
    table: &mut Table,
    fields: &IndexMap<String, FieldSchema>,
    col_prefix: &str,
) {
    for (raw_name, field) in fields {
        let col_name = sanitize(&format!("{col_prefix}{raw_name}"));
        let non_null: Vec<(&str, &TypeSchema)> = field
            .types
            .iter()
            .filter(|(t, _)| !is_null_type(t.as_str()))
            .map(|(t, ts)| (t.as_str(), ts))
            .collect();

        if non_null.is_empty() {
            continue;
        }

        if non_null.len() == 1 && non_null[0].0 == TYPE_OBJECT {
            if let Some(sub_fields) = &non_null[0].1.object {
                flatten_object_id_fields(table, sub_fields, &format!("{col_name}_"));
                continue;
            }
        }

        let pg_type = if non_null.len() == 1 && non_null[0].0 == TYPE_ARRAY {
            "JSONB".to_owned()
        } else if non_null
            .iter()
            .any(|(t, _)| *t == TYPE_OBJECT || *t == TYPE_ARRAY)
        {
            "JSONB".to_owned()
        } else {
            resolve_scalar_pg_type(&non_null, &col_name)
        };

        table.columns.push(Column {
            name: col_name,
            pg_type,
            nullable: false,
            primary_key: true,
        });
    }
}

fn flatten_inline_object_fields(
    table: &mut Table,
    fields: &IndexMap<String, FieldSchema>,
    prefix: &[String],
    reserved: &HashSet<String>,
    timestamp_fields: &[String],
) {
    let mut reserved_names = table
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<HashSet<_>>();
    reserved_names.extend(reserved.iter().cloned());
    let column_names = inline_object_column_names_with_prefix(fields, prefix, &reserved_names);

    for (path, field) in inline_object_leaf_fields_with_prefix(fields, prefix) {
        let source_path = path.join(".");
        let Some(col_name) = column_names.get(&source_path).cloned() else {
            continue;
        };
        let raw_name = path.last().map(String::as_str).unwrap_or_default();
        let non_null: Vec<(&str, &TypeSchema)> = field
            .types
            .iter()
            .filter(|(t, _)| !is_null_type(t.as_str()))
            .map(|(t, ts)| (t.as_str(), ts))
            .collect();

        if non_null.is_empty() {
            continue;
        }

        let nullable = non_null.len() < field.types.len() || field.probability < 1.0;
        let pg_type = if non_null.len() == 1 && non_null[0].0 == TYPE_ARRAY {
            "JSONB".to_owned()
        } else if non_null
            .iter()
            .any(|(t, _)| *t == TYPE_OBJECT || *t == TYPE_ARRAY)
        {
            "JSONB".to_owned()
        } else if matches_timestamp_field(raw_name, timestamp_fields) {
            FORCED_TIMESTAMP_PG_TYPE.to_owned()
        } else {
            resolve_scalar_pg_type(&non_null, &col_name)
        };

        table.columns.push(Column {
            name: col_name,
            pg_type,
            nullable,
            primary_key: false,
        });
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Main recursive field processor
// ──────────────────────────────────────────────────────────────────────────────

pub fn process_fields(
    table: &mut Table,
    fields: &IndexMap<String, FieldSchema>,
    col_prefix: &str,
    mark_pk: bool,
    allow_inline_objects: bool,
    pg_schema: Option<&str>,
    timestamp_fields: &[String],
) {
    fn reserved_inline_sibling_names(
        fields: &IndexMap<String, FieldSchema>,
        current_raw_name: &str,
        col_prefix: &str,
        mark_pk: bool,
        allow_inline_objects: bool,
    ) -> HashSet<String> {
        let mut reserved = HashSet::new();

        for (raw_name, field) in fields {
            if raw_name == current_raw_name {
                continue;
            }

            let non_null: Vec<(&str, &TypeSchema)> = field
                .types
                .iter()
                .filter(|(t, _)| !is_null_type(t.as_str()))
                .map(|(t, ts)| (t.as_str(), ts))
                .collect();

            if non_null.is_empty() {
                continue;
            }

            if raw_name == "_id"
                && col_prefix.is_empty()
                && mark_pk
                && non_null.len() == 1
                && non_null[0].0 == TYPE_OBJECT
            {
                if let Some(sub_fields) = &non_null[0].1.object {
                    for (path, _) in inline_object_leaf_fields_with_prefix(sub_fields, &[]) {
                        reserved.insert(sanitize(&path.join("_")));
                    }
                }
                continue;
            }

            if non_null.len() == 1 && non_null[0].0 == TYPE_OBJECT {
                let ts = non_null[0].1;
                if ts.as_jsonb {
                    reserved.insert(sanitize(&format!("{col_prefix}{raw_name}")));
                    continue;
                }
                if let Some(sub_fields) = &ts.object {
                    if allow_inline_objects && can_inline_object_fields(sub_fields) {
                        for (path, _) in inline_object_leaf_fields_with_prefix(sub_fields, &[]) {
                            if let Some(last) = path.last() {
                                reserved.insert(sanitize(last));
                            }
                        }
                    }
                }
                continue;
            }

            if non_null.len() == 1 && non_null[0].0 == TYPE_ARRAY {
                let ts = non_null[0].1;
                if ts.array.is_none() {
                    reserved.insert(sanitize(&format!("{col_prefix}{raw_name}")));
                }
                continue;
            }

            if non_null
                .iter()
                .any(|(t, _)| *t == TYPE_OBJECT || *t == TYPE_ARRAY)
            {
                reserved.insert(sanitize(&format!("{col_prefix}{raw_name}")));
                continue;
            }

            if raw_name == "_id" && col_prefix.is_empty() && mark_pk {
                reserved.insert("id".to_owned());
            } else {
                reserved.insert(sanitize(&format!("{col_prefix}{raw_name}")));
            }
        }

        reserved
    }

    let grouped_root_fields = if col_prefix.is_empty() && mark_pk {
        grouped_root_array_object_fields(fields)
    } else {
        Vec::new()
    };
    let grouped_representatives = grouped_root_fields
        .iter()
        .map(|group| (group.representative.clone(), group))
        .collect::<HashMap<_, _>>();
    let grouped_members = grouped_root_fields
        .iter()
        .flat_map(|group| group.members.iter().cloned())
        .collect::<HashSet<_>>();

    for (raw_name, field) in fields {
        if let Some(group) = grouped_representatives.get(raw_name) {
            add_grouped_root_child_table(
                table,
                &sanitize(raw_name),
                &group.child_fields,
                pg_schema,
                timestamp_fields,
            );
            continue;
        }
        if grouped_members.contains(raw_name) {
            continue;
        }

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

        // ── Top-level object _id => flattened composite PK ───────────────────

        if raw_name == "_id"
            && col_prefix.is_empty()
            && mark_pk
            && non_null.len() == 1
            && non_null[0].0 == TYPE_OBJECT
        {
            if let Some(sub_fields) = &non_null[0].1.object {
                flatten_object_id_fields(table, sub_fields, "");
                continue;
            }
        }

        // ── Single pure Object ────────────────────────────────────────────────

        if non_null.len() == 1 && non_null[0].0 == TYPE_OBJECT {
            let ts = non_null[0].1;

            // --jsonb flag: emit a JSONB column instead of a child table
            if ts.as_jsonb {
                table.columns.push(Column {
                    name: col_name,
                    pg_type: "JSONB".to_owned(),
                    nullable,
                    primary_key: false,
                });
                continue;
            }

            let is_hex = ts
                .object
                .as_ref()
                .is_some_and(|sf| !sf.is_empty() && sf.keys().all(|k| is_hex_keyed_name(k)));

            let is_uuid = ts
                .object
                .as_ref()
                .is_some_and(|sf| !sf.is_empty() && sf.keys().all(|k| is_uuid_keyed_name(k)));

            if is_hex {
                // Map document (dynamic hex keys)
                let value_fields = map_value_fields(ts).map(|vf| vf.clone());
                if let Some(vf) = value_fields {
                    add_map_table(table, &col_name, &vf, pg_schema, timestamp_fields);
                } else {
                    table.columns.push(Column {
                        name: col_name,
                        pg_type: "JSONB".to_owned(),
                        nullable,
                        primary_key: false,
                    });
                }
            } else if is_uuid {
                // Map document (dynamic UUID keys)
                let value_fields = map_uuid_value_fields(ts).map(|vf| vf.clone());
                if let Some(vf) = value_fields {
                    add_map_table(table, &col_name, &vf, pg_schema, timestamp_fields);
                } else {
                    table.columns.push(Column {
                        name: col_name,
                        pg_type: "JSONB".to_owned(),
                        nullable,
                        primary_key: false,
                    });
                }
            } else if let Some(sub_fields) = &ts.object {
                if allow_inline_objects && can_inline_object_fields(sub_fields) {
                    let sibling_reserved =
                        reserved_inline_sibling_names(
                            fields,
                            raw_name,
                            col_prefix,
                            mark_pk,
                            allow_inline_objects,
                        );
                    flatten_inline_object_fields(
                        table,
                        sub_fields,
                        std::slice::from_ref(raw_name),
                        &sibling_reserved,
                        timestamp_fields,
                    );
                } else {
                    let sf = sub_fields.clone();
                    add_doc_table(table, &col_name, &sf, pg_schema, timestamp_fields);
                }
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
                let flatten_to_jsonb = col_prefix.is_empty()
                    && mark_pk
                    && fields.len() == 2
                    && fields.contains_key("_id")
                    && raw_name != "_id";
                handle_array_field(
                    table,
                    &col_name,
                    raw_name,
                    &items,
                    nullable,
                    flatten_to_jsonb,
                    pg_schema,
                    timestamp_fields,
                );
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

        // ── Object or Array mixed with other types ────────────────────────────
        if non_null
            .iter()
            .any(|(t, _)| *t == TYPE_OBJECT || *t == TYPE_ARRAY)
        {
            // Check if there's a dominant scalar type (>90% of non-null probability)
            // that can be used instead of JSONB
            let total_non_null_prob: f64 = non_null.iter().map(|(_, ts)| ts.probability).sum();
            
            // Group non-null types by their scalar family
            let mut scalar_families: HashMap<String, f64> = HashMap::new();
            for (type_name, ts) in &non_null {
                if let Some(family) = scalar_type_family(type_name) {
                    *scalar_families.entry(family.to_string()).or_insert(0.0) += ts.probability;
                }
            }
            
            // Find the dominant scalar family
            if let Some((dominant_family, dominant_prob)) = scalar_families.into_iter().max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)) {
                // If dominant scalar family has >90% of non-null probability, use it
                if dominant_prob / total_non_null_prob > 0.9 {
                    // Map scalar family to a representative type and get PG type
                    let pg_type = match dominant_family.as_str() {
                        "string" => "TEXT",
                        "numeric" => "DOUBLE PRECISION",
                        "boolean" => "BOOLEAN",
                        "datetime" => "TIMESTAMP WITH TIME ZONE",
                        "objectid" => "TEXT",
                        _ => "TEXT",
                    };
                    table.columns.push(Column {
                        name: col_name,
                        pg_type: pg_type.to_owned(),
                        nullable,
                        primary_key: false,
                    });
                    continue;
                }
            }
            
            // No dominant scalar type, use JSONB
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
        } else if matches_timestamp_field(raw_name, timestamp_fields) {
            FORCED_TIMESTAMP_PG_TYPE.to_owned()
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
            // Child tables already get a surrogate `id` PK; drop nested scalar `id`
            // fields instead of duplicating them as `field_id`.
            if col_name == "id" && table.columns.iter().any(|c| c.name == "id") {
                continue;
            }
            table.columns.push(Column {
                name: col_name,
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

pub fn collect_tables(root: &Table) -> Vec<&Table> {
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

fn render_column(col: &Column, inline_primary_key: bool) -> String {
    let rendered_pg_type = if col.pg_type.eq_ignore_ascii_case("VARCHAR(0)") {
        "TEXT"
    } else {
        col.pg_type.as_str()
    };
    let constraint = if col.primary_key && inline_primary_key {
        " PRIMARY KEY"
    } else if !col.nullable || col.primary_key {
        " NOT NULL"
    } else {
        ""
    };
    format!("    {}{} {}{constraint}", col.name, "", rendered_pg_type)
        .trim_end()
        .to_owned()
}

fn render_table(table: &Table) -> String {
    let pk_columns: Vec<&str> = table
        .columns
        .iter()
        .filter(|col| col.primary_key)
        .map(|col| col.name.as_str())
        .collect();
    let mut defs: Vec<String> = table
        .columns
        .iter()
        .map(|col| render_column(col, pk_columns.len() == 1))
        .collect();
    if pk_columns.len() > 1 {
        defs.push(format!("    PRIMARY KEY ({})", pk_columns.join(", ")));
    }
    if let Some(refs) = &table.parent_ref {
        let fk_cols: Vec<&str> = refs.iter().map(|(fk_col, _, _)| fk_col.as_str()).collect();
        let ref_table = &refs[0].1;
        let ref_cols: Vec<&str> = refs
            .iter()
            .map(|(_, _, ref_col)| ref_col.as_str())
            .collect();
        defs.push(format!(
            "    FOREIGN KEY ({}) REFERENCES {} ({}) DEFERRABLE INITIALLY DEFERRED",
            fk_cols.join(", "),
            ref_table,
            ref_cols.join(", ")
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
pub fn schema_to_ddl_with_timestamp_fields(
    schema: &CollectionSchema,
    table_name: &str,
    pg_schema: Option<&str>,
    timestamp_fields: &[String],
) -> String {
    if let Some(group) = flatten_grouped_root_array_object_fields(schema) {
        let mut root = Table::new(sanitize(table_name));
        root.columns.push(Column {
            name: "id".to_owned(),
            pg_type: "BIGSERIAL".to_owned(),
            nullable: false,
            primary_key: true,
        });

        let id_field = schema.object.get("_id").expect("_id should exist");
        let id_non_null = id_field
            .types
            .iter()
            .filter(|(type_name, _)| !is_null_type(type_name.as_str()))
            .map(|(type_name, type_schema)| (type_name.as_str(), type_schema))
            .collect::<Vec<_>>();
        root.columns.push(Column {
            name: flattened_root_parent_id_column(table_name),
            pg_type: fk_scalar_type(&pk_type_for_id(&id_non_null)).to_owned(),
            nullable: false,
            primary_key: false,
        });
        root.columns.push(Column {
            name: "key".to_owned(),
            pg_type: "TEXT".to_owned(),
            nullable: false,
            primary_key: false,
        });

        process_fields(
            &mut root,
            &group.child_fields,
            "",
            false,
            false,
            pg_schema,
            timestamp_fields,
        );

        let tables = collect_tables(&root);
        let ddl = tables
            .iter()
            .map(|t| render_table(t))
            .collect::<Vec<_>>()
            .join("\n\n");
        return prepend_schema_preamble(ddl, pg_schema);
    }

    if let Some((_, array_field)) = flatten_root_array_object_field(schema) {
        let array_type = array_field
            .types
            .iter()
            .find(|(type_name, _)| {
                !is_null_type(type_name.as_str()) && type_name.as_str() == TYPE_ARRAY
            })
            .map(|(_, type_schema)| type_schema);
        let object_ts = array_type
            .and_then(|type_schema| type_schema.array.as_ref())
            .and_then(|items_field| items_field.types.get(TYPE_OBJECT));
        if let Some(object_ts) = object_ts {
            if !object_ts.as_jsonb {
                let mut root = Table::new(sanitize(table_name));
                root.columns.push(Column {
                    name: "id".to_owned(),
                    pg_type: "BIGSERIAL".to_owned(),
                    nullable: false,
                    primary_key: true,
                });

                let id_field = schema.object.get("_id").expect("_id should exist");
                let id_non_null = id_field
                    .types
                    .iter()
                    .filter(|(type_name, _)| !is_null_type(type_name.as_str()))
                    .map(|(type_name, type_schema)| (type_name.as_str(), type_schema))
                    .collect::<Vec<_>>();
                root.columns.push(Column {
                    name: flattened_root_parent_id_column(table_name),
                    pg_type: fk_scalar_type(&pk_type_for_id(&id_non_null)).to_owned(),
                    nullable: false,
                    primary_key: false,
                });

                if let Some(item_fields) = &object_ts.object {
                    process_fields(
                        &mut root,
                        item_fields,
                        "",
                        false,
                        false,
                        pg_schema,
                        timestamp_fields,
                    );
                }

                let tables = collect_tables(&root);
                let ddl = tables
                    .iter()
                    .map(|t| render_table(t))
                    .collect::<Vec<_>>()
                    .join("\n\n");
                return prepend_schema_preamble(ddl, pg_schema);
            }
        }
    }

    let mut root = Table::new(sanitize(table_name));
    process_fields(
        &mut root,
        &schema.object,
        "",
        true,
        false,
        pg_schema,
        timestamp_fields,
    );

    let tables = collect_tables(&root);

    // Suppressed debug output: tables and columns count

    let ddl = tables
        .iter()
        .map(|t| render_table(t))
        .collect::<Vec<_>>()
        .join("\n\n");
    prepend_schema_preamble(ddl, pg_schema)
}

pub fn schema_to_ddl(
    schema: &CollectionSchema,
    table_name: &str,
    pg_schema: Option<&str>,
) -> String {
    schema_to_ddl_with_timestamp_fields(schema, table_name, pg_schema, &[])
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
        let ddl = schema_to_ddl(&schema, "users", None);
        assert!(ddl.contains("CREATE TABLE users ("), "root table missing");
        assert!(ddl.contains("name VARCHAR(5) NOT NULL"), "name column missing");
        assert!(ddl.contains("score INTEGER NOT NULL"), "score column missing");
    }

    #[test]
    fn test_nullable_field() {
        let docs = vec![
            doc! { "_id": 1_i32, "opt": "present" },
            doc! { "_id": 2_i32 },
        ];
        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "t", None);
        // opt must not be NOT NULL
        assert!(
            !ddl.contains("opt TEXT NOT NULL"),
            "optional field must not have NOT NULL"
        );
    }

    #[test]
    fn test_explicit_null_scalar_field_stays_nullable() {
        let docs = vec![
            doc! { "_id": 1_i32, "last_update": bson::Bson::Null },
            doc! { "_id": 2_i32, "last_update": "2022-08-17 07:57:18.419539" },
        ];
        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "scheduling_jobs", None);
        assert!(
            ddl.contains("last_update TEXT"),
            "nullable scalar column should be emitted"
        );
        assert!(
            !ddl.contains("last_update TEXT NOT NULL"),
            "explicit null scalar field must not have NOT NULL"
        );
    }

    #[test]
    fn test_schema_json_with_null_and_string_stays_nullable() {
        let schema: CollectionSchema = serde_json::from_str(
            r#"{
    "count": 2,
    "sampled": 2,
    "object": {
        "_id": {
            "probability": 1.0,
            "types": {
                "Number": {
                    "probability": 1.0,
                    "sampled": 2,
                    "values": [1, 2]
                }
            }
        },
        "last_update": {
            "probability": 1.0,
            "types": {
                "Null": {
                    "probability": 0.5,
                    "sampled": 1,
                    "values": [null]
                },
                "String": {
                    "probability": 0.5,
                    "sampled": 1,
                    "values": ["2022-08-17 07:57:18.419539"]
                }
            }
        }
    }
}"#,
        )
        .expect("schema json should parse");
        let ddl = schema_to_ddl(&schema, "scheduling_jobs", None);
        assert!(
            !ddl.contains("last_update TEXT NOT NULL"),
            "JSON-backed null scalar field must not have NOT NULL"
        );
    }

    #[test]
    fn test_objectid_pk_becomes_text() {
        let docs = vec![doc! { "_id": bson::oid::ObjectId::new(), "x": 1_i32 }];
        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "t", None);
        assert!(
            ddl.contains("id TEXT PRIMARY KEY"),
            "ObjectId PK should become TEXT"
        );
    }

    #[test]
    fn test_short_strings_use_tight_varchar() {
        let docs = vec![
            doc! { "_id": 1_i32, "code": "abc" },
            doc! { "_id": 2_i32, "code": "abcde" },
        ];
        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "teams_members", None);
        assert!(ddl.contains("code VARCHAR(5) NOT NULL"));
    }

    #[test]
    fn test_empty_strings_use_varchar_1() {
        let docs = vec![
            doc! { "_id": 1_i32, "secondary_localization": "" },
            doc! { "_id": 2_i32, "secondary_localization": "" },
        ];
        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "archived_projects", None);
        assert!(ddl.contains("secondary_localization VARCHAR(1) NOT NULL"));
    }

    #[test]
    fn test_medium_strings_use_varchar_20() {
        let docs = vec![
            doc! { "_id": 1_i32, "ldap": "20014291" },
            doc! { "_id": 2_i32, "ldap": "20101496" },
        ];
        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "teams_members", None);
        assert!(
            ddl.contains("ldap VARCHAR(20) NOT NULL"),
            "Strings up to 20 chars should use VARCHAR(20)"
        );
    }

    #[test]
    fn test_long_strings_stay_text() {
        let docs = vec![
            doc! { "_id": 1_i32, "name": "Schedule of stop/start job(s) are cost optimized" },
            doc! { "_id": 2_i32, "name": "Another long descriptive string value" },
        ];
        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "advisors", None);
        assert!(ddl.contains("name TEXT NOT NULL"));
    }

    #[test]
    fn test_persisted_varchar_length_drives_ddl() {
        let docs = vec![
            doc! { "_id": 1_i32, "code": "abc" },
            doc! { "_id": 2_i32, "code": "abcdefgh" },
        ];
        let mut schema = analyze(&docs);
        let string_type = schema
            .object
            .get_mut("code")
            .and_then(|field| field.types.get_mut(TYPE_STRING))
            .expect("code should be inferred as String");
        string_type.values = None;
        string_type.max_length = Some(8);
        string_type.varchar_length = Some(20);

        let ddl = schema_to_ddl(&schema, "teams_members", None);
        assert!(ddl.contains("code VARCHAR(20) NOT NULL"));
    }

    #[test]
    fn test_number_field_stays_double_precision_even_for_whole_samples() {
        let docs = vec![
            doc! { "_id": 1_i32, "ram": 1.0_f64 },
            doc! { "_id": 2_i32, "ram": 4.0_f64 },
        ];
        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "sizings", None);
        assert!(
            ddl.contains("ram DOUBLE PRECISION NOT NULL"),
            "MongoDB Number fields should remain DOUBLE PRECISION for non-id columns"
        );
    }

    #[test]
    fn test_int32_field_emits_integer() {
        let docs = vec![
            doc! { "_id": 1_i32, "ram": 1_i32 },
            doc! { "_id": 2_i32, "ram": 4_i32 },
        ];
        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "sizings", None);
        assert!(ddl.contains("ram INTEGER NOT NULL"));
    }

    #[test]
    fn test_mixed_int_and_double_field_emits_double_precision() {
        let docs = vec![
            doc! { "_id": 1_i32, "ram": 1_i32 },
            doc! { "_id": 2_i32, "ram": 3.5_f64 },
        ];
        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "sizings", None);
        assert!(ddl.contains("ram DOUBLE PRECISION NOT NULL"));
    }

    #[test]
    fn test_child_tables_drop_redundant_nested_id_field() {
        let docs = vec![doc! {
            "_id": 1_i32,
            "advices": [{
                "id": "72a50747b3bfaac0cc2973bf0393ce63fe86c5ed",
                "advice": "Service has invalid ip range",
                "object_id": "aiven-pg-pgprepapr",
                "object_type": "SERVICE"
            }]
        }];
        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "advisors", None);
        assert!(
            ddl.contains("CREATE TABLE advices ("),
            "child table for advices missing"
        );
        assert!(
            !ddl.contains("field_id TEXT"),
            "nested id field should be dropped instead of renamed"
        );
    }

    #[test]
    fn test_object_id_flattens_into_composite_primary_key() {
        let docs = vec![doc! {
            "_id": {
                "projectid": "FRAS-P-SAM-FRTERR2",
                "provider": "atlas",
                "log_type": "dbAccessHistory"
            },
            "last_execution": "2023-07-13T09:02:15.833170"
        }];
        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "executions", None);

        assert!(
            ddl.contains("projectid "),
            "projectid PK column missing"
        );
        assert!(
            ddl.contains("provider "),
            "provider PK column missing"
        );
        assert!(
            ddl.contains("log_type "),
            "log_type PK column missing"
        );
        assert!(
            ddl.contains("last_execution "),
            "data field should stay on root table"
        );
        assert!(ddl.contains("PRIMARY KEY ("), "composite PK missing");
        assert!(
            ddl.contains("log_type") && ddl.contains("projectid") && ddl.contains("provider"),
            "composite PK should use flattened _id fields"
        );
        assert!(
            !ddl.contains("CREATE TABLE executions__id"),
            "_id should not become a child table"
        );
    }

    #[test]
    fn test_nested_object_creates_child_table() {
        let docs = vec![doc! { "_id": 1_i32, "addr": { "city": "Paris" } }];
        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "orders", None);
        assert!(
            ddl.contains("CREATE TABLE addr ("),
            "child table for addr missing"
        );
        assert!(ddl.contains("FOREIGN KEY"), "FK constraint missing");
    }

    #[test]
    fn test_array_of_scalars_creates_child_table() {
        let docs = vec![doc! { "_id": 1_i32, "tags": ["rust", "mongodb"] }];
        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "posts", None);
        assert!(
            ddl.contains("CREATE TABLE tags ("),
            "scalar array child table missing"
        );
        assert!(ddl.contains("value VARCHAR(20) NOT NULL"), "value column missing");
    }

    #[test]
    fn test_jsonb_array_of_objects_stays_on_root_table() {
        let docs = vec![doc! {
            "_id": "pg",
            "versions": [{
                "major_version": "13",
                "grace_date": bson::DateTime::now(),
                "eol_date": bson::DateTime::now()
            }]
        }];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let mut schema = analyzer.finish();
        schema.mark_objects_as_jsonb();

        let ddl = schema_to_ddl(&schema, "engine", None);

        assert!(ddl.contains("CREATE TABLE engine ("));
        assert!(ddl.contains("versions JSONB NOT NULL"));
        assert!(
            !ddl.contains("CREATE TABLE engine_versions ("),
            "array of objects marked as jsonb should stay on the root table"
        );
    }

    #[test]
    fn test_array_of_objects_without_jsonb_flag_flattens_into_root_table() {
        let docs = vec![doc! {
            "_id": "pg",
            "versions": [{
                "major_version": "13",
                "grace_date": bson::DateTime::now(),
                "eol_date": bson::DateTime::now()
            }]
        }];
        let schema = analyze(&docs);

        let ddl = schema_to_ddl(&schema, "engine", None);

        assert!(ddl.contains("CREATE TABLE engine ("));
        assert!(
            ddl.contains("engine_id VARCHAR(2) NOT NULL"),
            "flattened root table should keep the collection _id as engine_id"
        );
        assert!(
            ddl.contains("major_version VARCHAR(2) NOT NULL"),
            "flattened root table should include the array item fields"
        );
        assert!(
            !ddl.contains("CREATE TABLE engine_versions ("),
            "engine-style arrays should not generate a child table when jsonb=false"
        );
        assert!(
            !ddl.contains("versions JSONB NOT NULL"),
            "engine should not emit a jsonb column when jsonb=false"
        );
    }

    #[test]
    fn test_jsonb_array_of_objects_with_other_root_fields_keeps_child_table() {
        let docs = vec![doc! {
            "_id": 1_i32,
            "name": "advisor",
            "advices": [{
                "advice": "oversized",
                "object_id": "svc-1"
            }]
        }];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let mut schema = analyzer.finish();
        schema.mark_objects_as_jsonb();

        let ddl = schema_to_ddl(&schema, "advisors", None);

        assert!(ddl.contains("name VARCHAR(20) NOT NULL"));
        assert!(ddl.contains("CREATE TABLE advices ("));
        assert!(
            !ddl.contains("advices JSONB"),
            "advisors-style arrays should still generate child tables"
        );
    }

    #[test]
    fn test_reserved_word_prefixed() {
        let docs = vec![doc! { "_id": 1_i32, "order": "asc", "current_timestamp": 42_i32 }];
        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "t", None);
        assert!(
            ddl.contains("_order"),
            "reserved word 'order' should be prefixed with _"
        );
        assert!(
            ddl.contains("_current_timestamp"),
            "reserved word 'current_timestamp' should be prefixed with _"
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

    #[test]
    fn test_timestamp_field_patterns_force_timestamp_columns() {
        let docs = vec![
            doc! { "_id": 1_i32, "last_update": 1_i64 },
            doc! { "_id": 2_i32, "last_update": "2022-08-17T07:57:18Z" },
        ];
        let schema = analyze(&docs);
        let patterns = vec!["last_update".to_owned(), "*_date".to_owned()];

        let ddl = schema_to_ddl_with_timestamp_fields(&schema, "scheduling_jobs", None, &patterns);

        assert!(ddl.contains("last_update TIMESTAMP WITH TIME ZONE NOT NULL"));
    }

    #[test]
    fn groups_same_shape_root_array_fields_into_one_keyed_child_table() {
        let docs = vec![bson::doc! {
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
        }];
        let mut analyzer = crate::analyzer::Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let ddl = schema_to_ddl_with_timestamp_fields(&schema, "communities", None, &[]);

        assert!(ddl.contains("CREATE TABLE communities ("));
        assert!(ddl.contains("communities_id VARCHAR(20) NOT NULL"));
        assert!(ddl.contains("key TEXT NOT NULL"));
        assert!(ddl.contains("CREATE TABLE available_localizations ("));
        assert!(!ddl.contains("CREATE TABLE communities_dev ("));
        assert!(!ddl.contains("CREATE TABLE communities_prod ("));
    }

    #[test]
    fn restores_scalar_only_object_with_siblings_to_child_table() {
        let docs = vec![bson::doc! {
            "_id": "project-1",
            "environment": "T",
            "providers": [{
                "namespace": "fras-t-dba-176c358",
                "namespace_id": "fras-t-dba-176c358",
                "provider": "aiven",
                "metadata": {
                    "creation_date": "2025-08-11T00:00:00Z",
                    "status": "created"
                }
            }]
        }];
        let mut analyzer = crate::analyzer::Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let ddl = schema_to_ddl_with_timestamp_fields(
            &schema,
            "projects",
            None,
            &["*_date".to_owned()],
        );

        assert!(ddl.contains("CREATE TABLE providers ("));
        assert!(ddl.contains("CREATE TABLE metadata ("));
        assert!(ddl.contains("providers_id BIGINT NOT NULL"));
        assert!(ddl.contains("creation_date TIMESTAMP WITH TIME ZONE NOT NULL"));
        assert!(ddl.contains("status VARCHAR(20) NOT NULL"));
    }
}
