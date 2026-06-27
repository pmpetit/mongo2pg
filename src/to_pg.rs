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
//! * Hex-keyed map documents → child table from map values.
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
    flatten_root_array_object_field, flattened_root_parent_id_column,
    grouped_root_array_object_fields, inline_object_column_names_with_prefix,
    inline_object_leaf_fields_with_prefix, is_null_type, is_pg_reserved,
    matches_timestamp_field, sanitize,
    scalar_type_family,
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
        TYPE_OBJECTID => "UUID",
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

fn maybe_quote_ident(ident: &str) -> String {
    if is_pg_reserved(ident) {
        format!("\"{}\"", ident.replace('"', "\"\""))
    } else {
        ident.to_owned()
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
        return "UUID".to_owned();
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

fn is_geojson_point_field_schema(field: &FieldSchema) -> bool {
    let non_null: Vec<(&str, &TypeSchema)> = field
        .types
        .iter()
        .filter(|(t, _)| !is_null_type(t.as_str()))
        .map(|(t, ts)| (t.as_str(), ts))
        .collect();

    if non_null.len() != 1 || non_null[0].0 != TYPE_OBJECT {
        return false;
    }

    let Some(obj_fields) = non_null[0].1.object.as_ref() else {
        return false;
    };

    let Some(type_field) = obj_fields.get("type") else {
        return false;
    };
    let type_non_null: Vec<&str> = type_field
        .types
        .iter()
        .filter(|(t, _)| !is_null_type(t.as_str()))
        .map(|(t, _)| t.as_str())
        .collect();
    if !type_non_null.iter().any(|t| *t == TYPE_STRING) {
        return false;
    }

    let has_point_type_value = type_field
        .types
        .get(TYPE_STRING)
        .and_then(|type_schema| type_schema.values.as_ref())
        .map(|values| {
            values.iter().any(|value| {
                value
                    .as_str()
                    .map(|raw| raw.eq_ignore_ascii_case("point"))
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false);
    if !has_point_type_value {
        return false;
    }

    let Some(coords_field) = obj_fields.get("coordinates") else {
        return false;
    };
    coords_field
        .types
        .iter()
        .filter(|(t, _)| !is_null_type(t.as_str()))
        .any(|(t, _)| t.as_str() == TYPE_ARRAY)
}

fn non_null_types(field: &FieldSchema) -> Vec<(&str, &TypeSchema)> {
    field
        .types
        .iter()
        .filter(|(t, _)| !is_null_type(t.as_str()))
        .map(|(t, ts)| (t.as_str(), ts))
        .collect()
}

fn geo_merged_doc_parts(
    sub_fields: &IndexMap<String, FieldSchema>,
) -> Option<(&IndexMap<String, FieldSchema>, String, bool)> {
    let mut geo_field_name: Option<String> = None;
    let mut geo_nullable = false;
    let mut sibling_fields: Option<&IndexMap<String, FieldSchema>> = None;

    for (name, field) in sub_fields {
        let non_null = non_null_types(field);
        if non_null.is_empty() {
            continue;
        }

        if non_null.len() == 1
            && non_null[0].0 == TYPE_OBJECT
            && is_geojson_point_field_schema(field)
        {
            if geo_field_name.is_some() {
                return None;
            }
            geo_nullable = non_null.len() < field.types.len() || field.probability < 1.0;
            geo_field_name = Some(sanitize(name));
            continue;
        }

        if non_null.len() == 1 && non_null[0].0 == TYPE_OBJECT {
            if sibling_fields.is_some() {
                return None;
            }
            sibling_fields = non_null[0].1.object.as_ref();
            continue;
        }

        return None;
    }

    match (sibling_fields, geo_field_name) {
        (Some(sibling), Some(geo_col)) if !sibling.is_empty() => {
            Some((sibling, geo_col, geo_nullable))
        }
        _ => None,
    }
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
        let parent_segment = ancestor_segments.last().copied().unwrap_or(parent_name);
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

/// Prepend extension setup and optionally `CREATE SCHEMA` + `SET search_path` preamble.
fn prepend_schema_preamble(ddl: String, pg_schema: Option<&str>) -> String {
    let mut preamble = String::new();

    if ddl.contains("DEFAULT public.gen_random_uuid()") {
        preamble.push_str("CREATE EXTENSION IF NOT EXISTS \"pgcrypto\";\n");
    }

    if ddl.to_ascii_lowercase().contains("geometry(") {
        preamble.push_str("CREATE EXTENSION IF NOT EXISTS postgis;\n");
    }

    if !preamble.is_empty() {
        preamble.push('\n');
    }

    match pg_schema {
        None => format!("{preamble}{ddl}"),
        Some(schema) => {
            let s = sanitize(schema);
            format!(
                "{preamble}CREATE SCHEMA IF NOT EXISTS {s};\nSET search_path = {s}, public;\n\n{ddl}"
            )
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

// fn add_scalar_array_table(
//     parent: &mut Table,
//     array_field_col: &str,
//     mongo_field_name: &str,
//     scalar_types: &[(&str, &TypeSchema)],
//     pg_schema: Option<&str>,
//     timestamp_fields: &[String],
// ) {
//     let child_name = child_table_name(&parent.name, array_field_col, pg_schema);
//     let parent_pks = find_pk_columns(parent);

//     let mut child = Table::new(child_name);
//     child.columns.push(Column {
//         name: "id".to_owned(),
//         pg_type: "BIGSERIAL".to_owned(),
//         nullable: false,
//         primary_key: true,
//     });

//     let mut parent_ref = Vec::new();
//     if parent_pks.len() == 1 {
//         let (pk_name, pk_type) = &parent_pks[0];
//         let fk_col = format!("{}_id", parent.name);
//         child.columns.push(Column {
//             name: fk_col.clone(),
//             pg_type: fk_scalar_type(pk_type).to_owned(),
//             nullable: false,
//             primary_key: false,
//         });
//         parent_ref.push((fk_col, parent.name.clone(), pk_name.clone()));
//     } else {
//         for (pk_name, pk_type) in &parent_pks {
//             let fk_col = format!("{}_{}", parent.name, pk_name);
//             child.columns.push(Column {
//                 name: fk_col.clone(),
//                 pg_type: fk_scalar_type(pk_type).to_owned(),
//                 nullable: false,
//                 primary_key: false,
//             });
//             parent_ref.push((fk_col, parent.name.clone(), pk_name.clone()));
//         }
//     }
//     child.parent_ref = Some(parent_ref);

//     let non_null: Vec<(&str, &TypeSchema)> = scalar_types
//         .iter()
//         .filter(|(t, _)| !is_null_type(t))
//         .copied()
//         .collect();
//     let value_type = if matches_timestamp_field(mongo_field_name, timestamp_fields) {
//         FORCED_TIMESTAMP_PG_TYPE.to_owned()
//     } else if non_null.is_empty() {
//         "TEXT".to_owned()
//     } else {
//         resolve_scalar_pg_type(&non_null, "value")
//     };
//     child.columns.push(Column {
//         name: "value".to_owned(),
//         pg_type: value_type,
//         nullable: false,
//         primary_key: false,
//     });

//     parent.child_tables.push(child);
// }

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

fn add_geo_merged_doc_table(
    parent: &mut Table,
    doc_field_col: &str,
    sibling_fields: &IndexMap<String, FieldSchema>,
    geo_field_col: &str,
    geo_nullable: bool,
    pg_schema: Option<&str>,
    timestamp_fields: &[String],
) {
    let child_name = format!("{}_{}", parent.name, doc_field_col);
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
        sibling_fields,
        "",
        false,
        false,
        pg_schema,
        timestamp_fields,
    );
    child.columns.push(Column {
        name: geo_field_col.to_owned(),
        pg_type: "geometry(Point,4326)".to_owned(),
        nullable: geo_nullable,
        primary_key: false,
    });

    parent.child_tables.push(child);
}

/// Create a child table for a MongoDB map document (dynamic hex-keyed sub-fields).
/// Dynamic map keys are not materialized as dedicated SQL columns.
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
    raw_name: &str,
    items_field: &FieldSchema,
    nullable: bool,
    flatten_to_jsonb: bool,
    pg_schema: Option<&str>,
    timestamp_fields: &[String],
) {
    fn is_generic_wrapper_name(name: &str) -> bool {
        matches!(
            name,
            "metadata"
                | "details"
                | "detail"
                | "data"
                | "payload"
                | "attributes"
                | "attribute"
                | "config"
                | "configuration"
                | "info"
                | "value"
        )
    }

    fn looks_like_entity_fields(fields: &IndexMap<String, FieldSchema>) -> bool {
        fields.keys().any(|raw| {
            matches!(
                sanitize(raw).as_str(),
                "id" | "_id" | "name" | "permalink" | "slug" | "code" | "key"
            )
        })
    }

    fn passthrough_array_object_child<'a>(
        item_fields: &'a IndexMap<String, FieldSchema>,
    ) -> Option<(String, &'a IndexMap<String, FieldSchema>)> {
        let mut nested_name: Option<String> = None;
        let mut nested_fields: Option<&IndexMap<String, FieldSchema>> = None;

        for (raw_name, field) in item_fields {
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
                let Some(obj_fields) = non_null[0].1.object.as_ref() else {
                    return None;
                };
                if nested_name.is_some() {
                    return None;
                }
                nested_name = Some(sanitize(raw_name));
                nested_fields = Some(obj_fields);
                continue;
            }

            return None;
        }

        match (nested_name, nested_fields) {
            (Some(name), Some(fields))
                if !is_generic_wrapper_name(&name) && looks_like_entity_fields(fields) =>
            {
                Some((name, fields))
            }
            _ => None,
        }
    }

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
            // Collapse wrappers like competitions[].competitor into a direct
            // competitor child table linked to the real parent table.
            if let Some((nested_name, nested_fields)) = passthrough_array_object_child(child_fields)
            {
                add_child_table(
                    table,
                    &nested_name,
                    nested_fields,
                    pg_schema,
                    timestamp_fields,
                );
            } else {
                add_child_table(table, col_name, child_fields, pg_schema, timestamp_fields);
            }
        } else {
            // Object type but no inner fields schema
            table.columns.push(Column {
                name: col_name.to_owned(),
                pg_type: "JSONB".to_owned(),
                nullable,
                primary_key: false,
            });
        }
    }

    if non_null_items.iter().any(|(t, _)| *t == TYPE_STRING) {
        let pg_type = if matches_timestamp_field(raw_name, timestamp_fields) {
            "TIMESTAMP WITH TIME ZONE[]"
        } else {
            "TEXT[]"
        };
        {
            table.columns.push(Column {
                name: col_name.to_owned(),
                pg_type: pg_type.to_owned(),
                // pg_type: "JSONB".to_owned(),
                nullable,
                primary_key: false,
            });
        }
    } else if non_null_items
        .iter()
        .any(|(t, _)| *t == TYPE_NUMBER || *t == TYPE_DOUBLE)
    {
        table.columns.push(Column {
            name: col_name.to_owned(),
            pg_type: "DOUBLE PRECISION[]".to_owned(),
            nullable,
            primary_key: false,
        });
    } else if non_null_items.iter().any(|(t, _)| *t == TYPE_INT32) {
        table.columns.push(Column {
            name: col_name.to_owned(),
            pg_type: "INTEGER[]".to_owned(),
            nullable,
            primary_key: false,
        });
    } else if non_null_items.iter().any(|(t, _)| *t == TYPE_INT64) {
        table.columns.push(Column {
            name: col_name.to_owned(),
            pg_type: "BIGINT[]".to_owned(),
            nullable,
            primary_key: false,
        });
    } else if non_null_items.iter().any(|(t, _)| *t == TYPE_DATE) {
        table.columns.push(Column {
            name: col_name.to_owned(),
            pg_type: "TIMESTAMP WITH TIME ZONE[]".to_owned(),
            nullable,
            primary_key: false,
        });
    } else if non_null_items.iter().any(|(t, _)| *t == TYPE_DECIMAL128) {
        table.columns.push(Column {
            name: col_name.to_owned(),
            pg_type: "NUMERIC[]".to_owned(),
            nullable,
            primary_key: false,
        });
    };
    // else {
    //     // Mixed types or unrecognized types → JSONB
    //     table.columns.push(Column {
    //         name: col_name.to_owned(),
    //         pg_type: "JSONB".to_owned(),
    //         nullable,
    //         primary_key: false,
    //     })
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

            if is_geojson_point_field_schema(field) {
                table.columns.push(Column {
                    name: col_name,
                    pg_type: "geometry(Point,4326)".to_owned(),
                    nullable,
                    primary_key: false,
                });
                continue;
            }

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
                if let Some((sibling_fields, geo_col, geo_nullable)) =
                    geo_merged_doc_parts(sub_fields)
                {
                    add_geo_merged_doc_table(
                        table,
                        &col_name,
                        sibling_fields,
                        &geo_col,
                        geo_nullable,
                        pg_schema,
                        timestamp_fields,
                    );
                    continue;
                }

                if allow_inline_objects && can_inline_object_fields(sub_fields) {
                    let sibling_reserved = reserved_inline_sibling_names(
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
            if let Some((dominant_family, dominant_prob)) = scalar_families
                .into_iter()
                .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
            {
                // If dominant scalar family has >90% of non-null probability, use it
                if dominant_prob / total_non_null_prob > 0.9 {
                    // Map scalar family to a representative type and get PG type
                    let pg_type = match dominant_family.as_str() {
                        "string" => "TEXT",
                        "numeric" => "DOUBLE PRECISION",
                        "boolean" => "BOOLEAN",
                        "datetime" => "TIMESTAMP WITH TIME ZONE",
                        "objectid" => "UUID",
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

fn collapse_passthrough_root(mut root: Table) -> Table {
    if root.parent_ref.is_some() || root.child_tables.len() != 1 {
        return root;
    }

    // Only collapse when root is a true passthrough wrapper. If the root table
    // has real non-PK columns, keep it; collapsing would drop source fields.
    if root.columns.iter().any(|col| !col.primary_key) {
        return root;
    }

    let mut child = root.child_tables.remove(0);
    let synthetic_child_prefix = format!("{}_", root.name);
    if !child.name.starts_with(&synthetic_child_prefix) {
        root.child_tables.push(child);
        return root;
    }

    let Some(refs) = child.parent_ref.clone() else {
        root.child_tables.push(child);
        return root;
    };

    if refs.is_empty() || refs.iter().any(|(_, ref_table, _)| ref_table != &root.name) {
        root.child_tables.push(child);
        return root;
    }

    let fk_cols = refs
        .iter()
        .map(|(fk_col, _, _)| fk_col.clone())
        .collect::<HashSet<_>>();

    let mut merged_columns = Vec::new();
    let mut seen = HashSet::new();

    for column in &child.columns {
        if column.primary_key {
            seen.insert(column.name.clone());
            merged_columns.push(Column {
                name: column.name.clone(),
                pg_type: column.pg_type.clone(),
                nullable: column.nullable,
                primary_key: column.primary_key,
            });
        }
    }

    for column in &child.columns {
        if column.primary_key || fk_cols.contains(&column.name) {
            continue;
        }
        if seen.insert(column.name.clone()) {
            merged_columns.push(Column {
                name: column.name.clone(),
                pg_type: column.pg_type.clone(),
                nullable: column.nullable,
                primary_key: false,
            });
        }
    }

    child.columns = merged_columns;
    child.parent_ref = None;
    child
}

fn ensure_unique_table_names(root: &mut Table) {
    fn strip_numeric_suffix(name: &str) -> String {
        if let Some((prefix, tail)) = name.rsplit_once('_') {
            if !prefix.is_empty() && !tail.is_empty() && tail.chars().all(|c| c.is_ascii_digit()) {
                return prefix.to_owned();
            }
        }
        name.to_owned()
    }

    fn next_unique_name(base: &str, parent_path: &[String], used: &HashSet<String>) -> String {
        if !used.contains(base) {
            return base.to_owned();
        }

        for depth in 1..=parent_path.len() {
            let prefix = parent_path[parent_path.len() - depth..].join("_");
            let candidate = format!("{prefix}_{base}");
            if !used.contains(&candidate) {
                return candidate;
            }
        }

        let mut suffix = 2usize;
        loop {
            let candidate = format!("{base}_{suffix}");
            if !used.contains(&candidate) {
                return candidate;
            }
            suffix += 1;
        }
    }

    fn visit(node: &mut Table, parent_path: &[String], used: &mut HashSet<String>) {
        let canonical = strip_numeric_suffix(&node.name);
        let unique_name = next_unique_name(&canonical, parent_path, used);
        node.name = unique_name.clone();
        used.insert(unique_name.clone());

        let mut child_parent_path = parent_path.to_vec();
        child_parent_path.push(unique_name.clone());

        for child in &mut node.child_tables {
            if let Some(parent_refs) = child.parent_ref.as_mut() {
                for (_, ref_table, _) in parent_refs.iter_mut() {
                    *ref_table = unique_name.clone();
                }
            }
            visit(child, &child_parent_path, used);
        }
    }

    let mut used = HashSet::new();
    visit(root, &[], &mut used);
}

fn render_column(col: &Column, inline_primary_key: bool) -> String {
    let rendered_pg_type = if col.pg_type.eq_ignore_ascii_case("VARCHAR(0)") {
        "TEXT"
    } else {
        col.pg_type.as_str()
    };
    let default_clause = if col.primary_key && col.pg_type.eq_ignore_ascii_case("UUID") {
        " DEFAULT public.gen_random_uuid()"
    } else {
        ""
    };
    let constraint = if col.primary_key && inline_primary_key {
        " PRIMARY KEY"
    } else if !col.nullable || col.primary_key {
        " NOT NULL"
    } else {
        ""
    };
    format!(
        "    {}{} {}{}{}",
        maybe_quote_ident(&col.name),
        "",
        rendered_pg_type,
        default_clause,
        constraint
    )
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
        defs.push(format!(
            "    PRIMARY KEY ({})",
            pk_columns
                .iter()
                .map(|col| maybe_quote_ident(col))
                .collect::<Vec<_>>()
                .join(", ")
        ));
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
            fk_cols
                .iter()
                .map(|col| maybe_quote_ident(col))
                .collect::<Vec<_>>()
                .join(", "),
            maybe_quote_ident(ref_table),
            ref_cols
                .iter()
                .map(|col| maybe_quote_ident(col))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    let body = defs.join(",\n");
    format!("CREATE TABLE {} (\n{}\n);", maybe_quote_ident(&table.name), body)
}

fn render_fk_indexes(table: &Table) -> Vec<String> {
    let Some(refs) = &table.parent_ref else {
        return Vec::new();
    };

    let fk_cols: Vec<&str> = refs.iter().map(|(fk_col, _, _)| fk_col.as_str()).collect();
    if fk_cols.is_empty() {
        return Vec::new();
    }

    let index_name = format!("idx_{}_{}", table.name, fk_cols.join("_"));
    vec![format!(
        "CREATE INDEX IF NOT EXISTS {} ON {} ({});",
        maybe_quote_ident(&sanitize(&index_name)),
        maybe_quote_ident(&table.name),
        fk_cols
            .iter()
            .map(|col| maybe_quote_ident(col))
            .collect::<Vec<_>>()
            .join(", ")
    )]
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

        ensure_unique_table_names(&mut root);

        let tables = collect_tables(&root);
        let ddl = tables
            .iter()
            .flat_map(|t| {
                let mut stmts = vec![render_table(t)];
                stmts.extend(render_fk_indexes(t));
                stmts
            })
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

                ensure_unique_table_names(&mut root);

                let tables = collect_tables(&root);
                let ddl = tables
                    .iter()
                    .flat_map(|t| {
                        let mut stmts = vec![render_table(t)];
                        stmts.extend(render_fk_indexes(t));
                        stmts
                    })
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

    let mut root = collapse_passthrough_root(root);
    ensure_unique_table_names(&mut root);
    let tables = collect_tables(&root);

    // Suppressed debug output: tables and columns count

    let ddl = tables
        .iter()
        .flat_map(|t| {
            let mut stmts = vec![render_table(t)];
            stmts.extend(render_fk_indexes(t));
            stmts
        })
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
        assert!(
            ddl.contains("name VARCHAR(5) NOT NULL"),
            "name column missing"
        );
        assert!(
            ddl.contains("score INTEGER NOT NULL"),
            "score column missing"
        );
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
    fn test_objectid_pk_becomes_uuid() {
        let docs = vec![doc! { "_id": bson::oid::ObjectId::new(), "x": 1_i32 }];
        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "t", None);
        assert!(
            ddl.contains("id UUID DEFAULT public.gen_random_uuid() PRIMARY KEY"),
            "ObjectId PK should become UUID"
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
    fn test_geojson_and_sibling_object_merge_without_location_field_name() {
        let docs = vec![doc! {
            "_id": bson::oid::ObjectId::new(),
            "theaterId": 1000_i32,
            "venue": {
                "details": {
                    "street1": "340 W Market",
                    "city": "Bloomington",
                    "state": "MN",
                    "zipcode": "55425"
                },
                "point": {
                    "type": "Point",
                    "coordinates": [-93.24565_f64, 44.85466_f64]
                }
            }
        }];
        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "theaters", None);

        assert!(ddl.contains("CREATE TABLE theaters_venue ("));
        assert!(ddl.contains("street1 "));
        assert!(ddl.contains("city "));
        assert!(ddl.contains("state "));
        assert!(ddl.contains("zipcode "));
        assert!(ddl.contains("point geometry(Point,4326) NOT NULL"));
        assert!(!ddl.contains("CREATE TABLE theaters_venue_details ("));
    }

    #[test]
    fn test_single_child_root_is_collapsed_into_child_without_fk() {
        let docs = vec![doc! {
            "_id": bson::oid::ObjectId::new(),
            "venue": {
                "details": {
                    "city": "Bloomington"
                },
                "point": {
                    "type": "Point",
                    "coordinates": [-93.24565_f64, 44.85466_f64]
                }
            }
        }];
        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "theaters", None);

        assert!(!ddl.contains("CREATE TABLE theaters ("));
        assert!(ddl.contains("CREATE TABLE theaters_venue ("));
        assert!(!ddl.contains("theaters_id"));
        assert!(!ddl.contains("REFERENCES theaters (id)"));
    }

    #[test]
    fn test_single_child_root_with_scalar_fields_is_not_collapsed() {
        let docs = vec![doc! {
            "_id": bson::oid::ObjectId::new(),
            "account_id": 42_i32,
            "transactions": [{
                "amount": 10_i32,
                "symbol": "abc",
            }]
        }];
        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "transactions", None);

        assert!(ddl.contains("CREATE TABLE transactions ("));
        assert!(ddl.contains("id UUID DEFAULT public.gen_random_uuid() PRIMARY KEY"));
        assert!(ddl.contains("account_id INTEGER NOT NULL"));
        assert!(ddl.contains("CREATE TABLE transactions_transactions ("));
        assert!(ddl.contains("transactions_id UUID NOT NULL"));
        assert!(ddl.contains("FOREIGN KEY (transactions_id) REFERENCES transactions (id)"));
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

        assert!(ddl.contains("projectid "), "projectid PK column missing");
        assert!(ddl.contains("provider "), "provider PK column missing");
        assert!(ddl.contains("log_type "), "log_type PK column missing");
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
        assert!(
            ddl.contains("CREATE INDEX IF NOT EXISTS idx_addr_orders_id ON addr (orders_id);"),
            "FK index missing for child table"
        );
    }

    #[test]
    fn test_array_of_scalars_creates_child_table() {
        let docs = vec![doc! { "_id": 1_i32, "tags": ["rust", "mongodb"] }];
        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "posts", None);
        assert!(ddl.contains(
            "CREATE TABLE posts (\n    id SERIAL PRIMARY KEY,\n    tags TEXT[] NOT NULL\n);"
        ));
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
    fn test_array_passthrough_object_uses_inner_key_as_table_and_parent_fk() {
        let docs = vec![doc! {
            "_id": bson::oid::ObjectId::new(),
            "competitions": [{
                "competitor": {
                    "name": "Acme",
                    "permalink": "acme"
                }
            }]
        }];

        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "companies", None);

        assert!(ddl.contains("CREATE TABLE competitor ("));
        assert!(!ddl.contains("CREATE TABLE competitions ("));
        assert!(ddl.contains("companies_id UUID NOT NULL"));
        assert!(ddl.contains("FOREIGN KEY (companies_id) REFERENCES companies (id)"));
        assert!(!ddl.contains("competitions_id"));
    }

    #[test]
    fn test_reserved_words_keep_original_name() {
        let docs = vec![doc! { "_id": 1_i32, "order": "asc", "current_timestamp": 42_i32 }];
        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "t", None);
        assert!(ddl.contains("\"order\" "));
        assert!(ddl.contains("\"current_timestamp\" "));
        assert!(!ddl.contains("_order "));
        assert!(!ddl.contains("_current_timestamp "));
    }

    #[test]
    fn test_geojson_polygon_is_not_inferred_as_point_geometry() {
        let docs = vec![doc! {
            "_id": bson::oid::ObjectId::new(),
            "place": {
                "bounding_box": {
                    "type": "Polygon",
                    "coordinates": [[[-85.95_f64, 42.52_f64], [-85.93_f64, 42.52_f64], [-85.93_f64, 42.54_f64], [-85.95_f64, 42.54_f64], [-85.95_f64, 42.52_f64]]]
                },
                "country": "US",
                "name": "Allegan"
            }
        }];

        let schema = analyze(&docs);
        let ddl = schema_to_ddl(&schema, "tweets", None);

        assert!(!ddl.contains("bounding_box geometry(Point,4326)"));
    }

    #[test]
    fn test_sanitize() {
        assert_eq!(sanitize("myField"), "myfield");
        assert_eq!(sanitize("my-field"), "my_field");
        assert_eq!(sanitize("123abc"), "_123abc");
        assert_eq!(sanitize("order"), "order");
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
    fn test_timestamp_field_patterns_force_timestamp_array_columns_for_string_arrays() {
        let docs = vec![doc! {
            "_id": 1_i32,
            "release_date": ["2025-01-01T00:00:00Z", "2025-01-02T00:00:00Z"]
        }];
        let schema = analyze(&docs);

        let ddl =
            schema_to_ddl_with_timestamp_fields(&schema, "releases", None, &["*_date".to_owned()]);

        assert!(ddl.contains("release_date TIMESTAMP WITH TIME ZONE[] NOT NULL"));
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
        assert!(!ddl.contains("CREATE TABLE communities_dev ("));
        assert!(!ddl.contains("CREATE TABLE communities_prod ("));
    }

    #[test]
    fn flattens_scalar_only_object_with_siblings_into_array_child_table() {
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

        let ddl =
            schema_to_ddl_with_timestamp_fields(&schema, "projects", None, &["*_date".to_owned()]);

        assert!(ddl.contains("CREATE TABLE projects ("));
        assert!(ddl.contains("CREATE TABLE providers ("));
        assert!(ddl.contains("projects_id VARCHAR(20) NOT NULL"));
        assert!(ddl.contains("creation_date TIMESTAMP WITH TIME ZONE NOT NULL"));
        assert!(ddl.contains("status VARCHAR(20) NOT NULL"));
        assert!(!ddl.contains("CREATE TABLE metadata ("));
    }

    #[test]
    fn flattens_relationships_person_inside_relationships_array_table() {
        let docs = vec![bson::doc! {
            "_id": bson::oid::ObjectId::new(),
            "relationships": [{
                "is_past": false,
                "title": "CEO",
                "person": {
                    "first_name": "Ada",
                    "last_name": "Lovelace",
                    "permalink": "ada-lovelace"
                }
            }]
        }];

        let mut analyzer = crate::analyzer::Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let ddl = schema_to_ddl(&schema, "companies", None);

        assert!(ddl.contains("CREATE TABLE companies ("));
        assert!(ddl.contains("companies_id UUID NOT NULL"));
        assert!(ddl.contains("first_name "));
        assert!(ddl.contains("last_name "));
        assert!(ddl.contains("permalink "));
        assert!(!ddl.contains("CREATE TABLE relationships ("));
        assert!(!ddl.contains("CREATE TABLE relationships_person ("));
    }

    #[test]
    fn monitoring_with_items_skewed_item_between_string_and_object() {
        let json_str = std::fs::read_to_string("tests/fixtures/monitoring_test1.json")
            .expect("Failed to read fixture");

        let doc: bson::Document = serde_json::from_str(&json_str).expect("Failed to parse JSON");

        let mut analyzer = Analyzer::new(true);
        analyzer.process_document(&doc);
        let schema = analyzer.finish();

        let ddl = schema_to_ddl(&schema, "monitoring", None);

        assert!(ddl.contains("CREATE TABLE monitoring ("));
    }

    #[test]
    fn monitoring_with_array_in_object_object() {
        let json_str = std::fs::read_to_string("tests/fixtures/host_verification_light.json")
            .expect("Failed to read fixture");

        let doc: bson::Document = serde_json::from_str(&json_str).expect("Failed to parse JSON");

        let mut analyzer = Analyzer::new(true);
        analyzer.process_document(&doc);
        let schema = analyzer.finish();

        let ddl = schema_to_ddl(&schema, "host_verification", None);

        assert!(ddl.contains("CREATE TABLE host_verification ("));
    }
    #[test]
    fn communities_with_id_becoming_big_serial() {
        let json_str = std::fs::read_to_string("tests/fixtures/communities.json")
            .expect("Failed to read fixture");

        let doc: bson::Document = serde_json::from_str(&json_str).expect("Failed to parse JSON");

        let mut analyzer = Analyzer::new(true);
        analyzer.process_document(&doc);
        let schema = analyzer.finish();

        let ddl = schema_to_ddl(&schema, "communities", None);

        assert!(ddl.contains("CREATE TABLE communities ("));

        //assert!(!ddl.contains("CREATE TABLE communities ("));
    }
    #[test]
    fn monitoring_with_array_of_int_in_object() {
        let json_str = std::fs::read_to_string("tests/fixtures/host_verification_int.json")
            .expect("Failed to read fixture");

        let doc: bson::Document = serde_json::from_str(&json_str).expect("Failed to parse JSON");

        let mut analyzer = Analyzer::new(true);
        analyzer.process_document(&doc);
        let schema = analyzer.finish();

        let ddl = schema_to_ddl(&schema, "host_verification", None);

        assert!(ddl.contains("CREATE TABLE host_verification ("));
    }

    #[test]
    fn customers_fixture_generates_expected_tables() {
        let json_str = std::fs::read_to_string("tests/fixtures/customers.json")
            .expect("Failed to read fixture");

        let doc: bson::Document = serde_json::from_str(&json_str).expect("Failed to parse JSON");

        let mut analyzer = Analyzer::new(true);
        analyzer.process_document(&doc);
        let schema = analyzer.finish();

        let ddl = schema_to_ddl(&schema, "customers", None);

        assert!(ddl.contains("CREATE TABLE customers ("));
        assert!(ddl.contains("CREATE TABLE tier_and_details ("));
    }

    #[test]
    fn transactions_fixture_generates_expected_tables() {
        let json_str = std::fs::read_to_string("tests/fixtures/transactions.json")
            .expect("Failed to read fixture");

        let doc: bson::Document = serde_json::from_str(&json_str).expect("Failed to parse JSON");

        let mut analyzer = Analyzer::new(true);
        analyzer.process_document(&doc);
        let schema = analyzer.finish();

        let ddl = schema_to_ddl(&schema, "transactions", None);

        assert!(ddl.contains("CREATE TABLE transactions_transactions"));
    }
}
