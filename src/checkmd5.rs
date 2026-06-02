use crate::analyzer::{CollectionSchema, FieldSchema, TypeSchema};
use crate::util::{
    can_inline_object_fields, flatten_grouped_root_array_object_fields,
    flatten_root_array_object_field, flattened_root_parent_id_column,
    grouped_root_array_object_fields, inline_object_column_names_with_prefix,
    inline_object_leaf_fields_with_prefix, is_pg_reserved, read_conf,
};
use anyhow::{anyhow, Context, Result};
use bson::{doc, Bson, Document};
use futures::TryStreamExt;
use postgres_native_tls::MakeTlsConnector;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio_postgres::{Client, Row};

#[derive(Debug, Clone, Deserialize)]
struct MappingYaml {
    #[serde(default)]
    dbname: Option<String>,
    pg_mapping: PgMappingYaml,
}

#[derive(Debug, Clone, Deserialize)]
struct PgMappingYaml {
    #[serde(default)]
    dbname: Option<String>,
    #[serde(default)]
    schema_name: Option<String>,
    table_name: String,
    #[serde(default)]
    columns: Vec<MappingColumnYaml>,
    #[serde(default)]
    ddl: Option<DdlTableYaml>,
}

#[derive(Debug, Clone, Deserialize)]
struct MappingColumnYaml {
    source_field: String,
    target_field: String,
    #[serde(default)]
    data_type: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct DdlTableYaml {
    columns: Vec<DdlColumnYaml>,
    #[serde(default)]
    foreign_keys: Vec<DdlForeignKeyYaml>,
}

#[derive(Debug, Clone, Deserialize)]
struct DdlColumnYaml {
    name: String,
    sql_type: String,
}

#[derive(Debug, Clone, Deserialize)]
struct DdlForeignKeyYaml {
    from_col: String,
}

#[derive(Debug, Clone)]
pub struct Md5ColumnMapping {
    pub source_field: String,
    pub target_field: String,
}

#[derive(Debug, Clone)]
pub struct Md5Summary {
    pub mongo_md5: String,
    pub pg_md5: String,
    pub columns: Vec<Md5ColumnMapping>,
    pub mismatches: Vec<Md5MismatchRow>,
}

#[derive(Debug, Clone)]
pub struct Md5MismatchRow {
    pub row_index: usize,
    pub mongo_values: Option<Vec<String>>,
    pub pg_values: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct Md5TableSummary {
    pub table_name: String,
    pub summary: Md5Summary,
}

#[derive(Debug, Clone)]
struct MappingTarget {
    source_collection: String,
    mapping_path: PathBuf,
    mapping_yaml: MappingYaml,
    source_path: SourcePath,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SourcePath {
    path: Vec<String>,
    scalar_array_field: Option<String>,
    grouped_fields: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HashRecord {
    md5: String,
    values: Vec<String>,
}

fn md5_hex_from_fragments<I, S>(fragments: I) -> String
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut concat = String::new();
    for fragment in fragments {
        concat.push_str(fragment.as_ref());
    }
    format!("{:x}", md5::compute(concat.as_bytes()))
}

fn bson_to_comparable_json(value: &Bson) -> serde_json::Value {
    match value {
        Bson::Double(v) => serde_json::json!(v),
        Bson::String(v) => serde_json::Value::String(v.clone()),
        Bson::Array(values) => serde_json::Value::Array(
            values
                .iter()
                .map(bson_to_comparable_json)
                .collect::<Vec<_>>(),
        ),
        Bson::Document(doc) => serde_json::Value::Object(
            doc.iter()
                .map(|(key, value)| (key.clone(), bson_to_comparable_json(value)))
                .collect(),
        ),
        Bson::Boolean(v) => serde_json::json!(v),
        Bson::Null | Bson::Undefined => serde_json::Value::Null,
        Bson::Int32(v) => serde_json::json!(v),
        Bson::Int64(v) => serde_json::json!(v),
        Bson::ObjectId(v) => serde_json::Value::String(v.to_hex()),
        Bson::DateTime(v) => serde_json::Value::String(v.to_string()),
        Bson::Timestamp(v) => serde_json::Value::String(v.to_string()),
        Bson::Binary(v) => serde_json::Value::String(v.to_string()),
        Bson::RegularExpression(v) => serde_json::Value::String(v.to_string()),
        Bson::Decimal128(v) => {
            if let Ok(number) = v.to_string().parse::<f64>() {
                serde_json::json!(number)
            } else {
                serde_json::Value::String(v.to_string())
            }
        }
        other => serde_json::Value::String(other.to_string()),
    }
}

fn canonicalize_json_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_owned(),
        serde_json::Value::Bool(v) => v.to_string(),
        serde_json::Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                value.to_string()
            } else if let Some(value) = number.as_u64() {
                value.to_string()
            } else if let Some(value) = number.as_f64() {
                if value.is_finite() && value.fract() == 0.0 {
                    format!("{value:.0}")
                } else {
                    number.to_string()
                }
            } else {
                number.to_string()
            }
        }
        serde_json::Value::String(v) => {
            serde_json::to_string(v).expect("serializing canonical JSON string should succeed")
        }
        serde_json::Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(canonicalize_json_value)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        serde_json::Value::Object(map) => format!(
            "{{{}}}",
            map.iter()
                .map(|(key, value)| format!(
                    "{}: {}",
                    serde_json::to_string(key)
                        .expect("serializing canonical JSON object key should succeed"),
                    canonicalize_json_value(value)
                ))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

fn normalize_json_literal(literal: &str) -> String {
    serde_json::from_str::<serde_json::Value>(literal)
        .map(|value| canonicalize_json_value(&value))
        .unwrap_or_else(|_| literal.to_owned())
}

fn is_textual_pg_type(data_type: Option<&str>) -> bool {
    matches!(
        data_type.map(|value| value.trim().to_ascii_lowercase()),
        Some(data_type)
            if matches!(
                data_type.as_str(),
                "text"
                    | "varchar"
                    | "character varying"
                    | "char"
                    | "character"
                    | "bpchar"
                    | "citext"
            )
    )
}

#[cfg(test)]
fn mongo_field_literal(doc: &Document, field: &str) -> String {
    mongo_field_literal_for_type(doc, field, None)
}

fn mongo_field_literal_for_type(
    doc: &Document,
    field: &str,
    target_data_type: Option<&str>,
) -> String {
    fn find_doc_value<'a>(doc: &'a Document, field: &str) -> Option<&'a Bson> {
        let (head, tail) = match field.split_once('.') {
            Some((head, tail)) => (head, Some(tail)),
            None => (field, None),
        };
        let value = doc.get(head)?;
        match tail {
            Some(rest) => match value {
                Bson::Document(child_doc) => find_doc_value(child_doc, rest),
                _ => None,
            },
            None => Some(value),
        }
    }

    let value = find_doc_value(doc, field).unwrap_or(&Bson::Null);
    let comparable = bson_to_comparable_json(value);
    let normalized = normalize_json_literal(
        &serde_json::to_string(&comparable)
            .expect("serializing MongoDB value to JSON should succeed"),
    );

    if is_textual_pg_type(target_data_type) && !matches!(comparable, serde_json::Value::Null) {
        match comparable {
            serde_json::Value::String(_) => normalized,
            _ => serde_json::to_string(&normalized)
                .expect("serializing PostgreSQL text comparison literal should succeed"),
        }
    } else {
        normalized
    }
}

#[cfg(test)]
fn mongo_hash_record(doc: &Document, source_fields: &[String]) -> HashRecord {
    let typed_source_fields = source_fields
        .iter()
        .map(|field| (field.clone(), None))
        .collect::<Vec<_>>();
    mongo_hash_record_for_columns(doc, &typed_source_fields)
}

fn mongo_hash_record_for_columns(
    doc: &Document,
    source_fields: &[(String, Option<String>)],
) -> HashRecord {
    let values = source_fields
        .iter()
        .map(|(field, target_data_type)| {
            mongo_field_literal_for_type(doc, field, target_data_type.as_deref())
        })
        .collect::<Vec<_>>();
    HashRecord {
        md5: md5_hex_from_fragments(values.iter()),
        values,
    }
}

fn sanitize_pg_name(name: &str) -> String {
    let s: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c.to_ascii_lowercase()
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

fn is_null_type(type_name: &str) -> bool {
    matches!(type_name, "Null" | "Undefined")
}

fn child_mapping_table_name(parent_name: &str, field: &str) -> String {
    let field = sanitize_pg_name(field);
    let ancestor_segments = parent_name.split('_').collect::<Vec<_>>();
    if ancestor_segments.iter().any(|segment| *segment == field) {
        let parent_segment = ancestor_segments.last().copied().unwrap_or(parent_name);
        format!("{parent_segment}_{field}")
    } else {
        field
    }
}

fn build_mapping_source_paths(
    collection: &str,
    schema: &CollectionSchema,
) -> HashMap<String, SourcePath> {
    fn visit_fields(
        parent_table_name: &str,
        path_prefix: &[String],
        fields: &indexmap::IndexMap<String, FieldSchema>,
        active_grouped_fields: Option<&[String]>,
        out: &mut HashMap<String, SourcePath>,
    ) {
        let grouped_root_fields = if path_prefix.is_empty() && active_grouped_fields.is_none() {
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
            .collect::<std::collections::HashSet<_>>();

        for (raw_name, field) in fields {
            if let Some(group) = grouped_representatives.get(raw_name) {
                let child_table = child_mapping_table_name(parent_table_name, raw_name);
                out.insert(
                    child_table.clone(),
                    SourcePath {
                        path: path_prefix.to_vec(),
                        scalar_array_field: None,
                        grouped_fields: Some(group.members.clone()),
                    },
                );
                visit_fields(
                    &child_table,
                    &[],
                    &group.child_fields,
                    Some(group.members.as_slice()),
                    out,
                );
                continue;
            }
            if grouped_members.contains(raw_name) {
                continue;
            }

            let non_null: Vec<(&str, &TypeSchema)> = field
                .types
                .iter()
                .filter(|(type_name, _)| !is_null_type(type_name.as_str()))
                .map(|(type_name, type_schema)| (type_name.as_str(), type_schema))
                .collect();

            if non_null.len() == 1 && non_null[0].0 == "Object" {
                let type_schema = non_null[0].1;
                if type_schema.as_jsonb {
                    continue;
                }
                if let Some(sub_fields) = &type_schema.object {
                    let child_table = child_mapping_table_name(parent_table_name, raw_name);
                    let mut child_path = path_prefix.to_vec();
                    child_path.push(raw_name.clone());
                    out.insert(
                        child_table.clone(),
                        SourcePath {
                            path: child_path.clone(),
                            scalar_array_field: None,
                            grouped_fields: active_grouped_fields.map(|fields| fields.to_vec()),
                        },
                    );
                    visit_fields(
                        &child_table,
                        &child_path,
                        sub_fields,
                        active_grouped_fields,
                        out,
                    );
                }
                continue;
            }

            if non_null.len() == 1 && non_null[0].0 == "Array" {
                let type_schema = non_null[0].1;
                if let Some(items_field) = &type_schema.array {
                    if let Some(object_schema) = items_field.types.get("Object") {
                        if let Some(sub_fields) = &object_schema.object {
                            let child_table = child_mapping_table_name(parent_table_name, raw_name);
                            let mut child_path = path_prefix.to_vec();
                            child_path.push(raw_name.clone());
                            out.insert(
                                child_table.clone(),
                                SourcePath {
                                    path: child_path.clone(),
                                    scalar_array_field: None,
                                    grouped_fields: active_grouped_fields
                                        .map(|fields| fields.to_vec()),
                                },
                            );
                            visit_fields(
                                &child_table,
                                &child_path,
                                sub_fields,
                                active_grouped_fields,
                                out,
                            );
                        }
                    } else {
                        let child_table = child_mapping_table_name(parent_table_name, raw_name);
                        let mut child_path = path_prefix.to_vec();
                        child_path.push(raw_name.clone());
                        out.insert(
                            child_table,
                            SourcePath {
                                path: child_path,
                                scalar_array_field: Some(raw_name.clone()),
                                grouped_fields: active_grouped_fields.map(|fields| fields.to_vec()),
                            },
                        );
                    }
                }
            }
        }
    }

    let root_table = sanitize_pg_name(collection);
    let mut out = HashMap::new();
    if let Some(group) = flatten_grouped_root_array_object_fields(schema) {
        out.insert(
            root_table.clone(),
            SourcePath {
                path: Vec::new(),
                scalar_array_field: None,
                grouped_fields: Some(group.members.clone()),
            },
        );
        visit_fields(
            &root_table,
            &[],
            &group.child_fields,
            Some(group.members.as_slice()),
            &mut out,
        );
        return out;
    }

    if let Some((field_name, array_field)) = flatten_root_array_object_field(schema) {
        let item_fields = array_field
            .types
            .get("Array")
            .and_then(|type_schema| type_schema.array.as_ref())
            .and_then(|items_field| items_field.types.get("Object"))
            .and_then(|type_schema| type_schema.object.as_ref());
        if let Some(item_fields) = item_fields {
            let prefix = vec![field_name.to_owned()];
            out.insert(
                root_table.clone(),
                SourcePath {
                    path: prefix.clone(),
                    scalar_array_field: None,
                    grouped_fields: None,
                },
            );
            visit_fields(&root_table, &prefix, item_fields, None, &mut out);
            return out;
        }
    }

    out.insert(
        root_table.clone(),
        SourcePath {
            path: Vec::new(),
            scalar_array_field: None,
            grouped_fields: None,
        },
    );
    visit_fields(&root_table, &[], &schema.object, None, &mut out);
    out
}

fn extract_source_documents(doc: &Document, source_path: &SourcePath) -> Vec<Document> {
    fn walk(doc: &Document, source_path: &SourcePath, root_id: Option<&Bson>) -> Vec<Document> {
        if let Some(grouped_fields) = &source_path.grouped_fields {
            let grouped_docs = grouped_fields
                .iter()
                .flat_map(|field_name| match doc.get(field_name) {
                    Some(Bson::Array(items)) => items
                        .iter()
                        .filter_map(|item| match item {
                            Bson::Document(child_doc) => {
                                let mut cloned = child_doc.clone();
                                cloned.insert("key", field_name.clone());
                                if let Some(root_id) = root_id {
                                    cloned.insert("_id", root_id.clone());
                                }
                                Some(cloned)
                            }
                            _ => None,
                        })
                        .collect::<Vec<_>>(),
                    Some(Bson::Document(child_doc)) => {
                        let mut cloned = child_doc.clone();
                        cloned.insert("key", field_name.clone());
                        if let Some(root_id) = root_id {
                            cloned.insert("_id", root_id.clone());
                        }
                        vec![cloned]
                    }
                    _ => Vec::new(),
                })
                .collect::<Vec<_>>();

            if source_path.path.is_empty() {
                return grouped_docs;
            }

            return grouped_docs
                .iter()
                .flat_map(|grouped_doc| {
                    walk(
                        grouped_doc,
                        &SourcePath {
                            path: source_path.path.clone(),
                            scalar_array_field: source_path.scalar_array_field.clone(),
                            grouped_fields: None,
                        },
                        grouped_doc.get("_id").or(root_id),
                    )
                })
                .collect();
        }

        if source_path.path.is_empty() {
            let mut cloned = doc.clone();
            if !cloned.contains_key("_id") {
                if let Some(root_id) = root_id {
                    cloned.insert("_id", root_id.clone());
                }
            }
            return vec![cloned];
        }

        let field_name = &source_path.path[0];
        let remaining = source_path.path[1..].to_vec();
        match doc.get(field_name) {
            Some(Bson::Document(child_doc)) => walk(
                child_doc,
                &SourcePath {
                    path: remaining,
                    scalar_array_field: source_path.scalar_array_field.clone(),
                    grouped_fields: None,
                },
                root_id,
            ),
            Some(Bson::Array(items)) => items
                .iter()
                .filter_map(|item| match item {
                    Bson::Document(child_doc) => Some(walk(
                        child_doc,
                        &SourcePath {
                            path: remaining.clone(),
                            scalar_array_field: source_path.scalar_array_field.clone(),
                            grouped_fields: None,
                        },
                        root_id,
                    )),
                    _ if remaining.is_empty() => {
                        source_path.scalar_array_field.as_ref().map(|field| {
                            let mut synthetic = Document::new();
                            synthetic.insert(field.clone(), item.clone());
                            if let Some(root_id) = root_id {
                                synthetic.insert("_id", root_id.clone());
                            }
                            vec![synthetic]
                        })
                    }
                    _ => None,
                })
                .flatten()
                .collect(),
            _ => Vec::new(),
        }
    }

    walk(doc, source_path, doc.get("_id"))
}

fn sort_hash_records(records: &mut [HashRecord]) {
    records.sort_by(|left, right| {
        left.values
            .cmp(&right.values)
            .then(left.md5.cmp(&right.md5))
    });
}

fn read_mapping_yaml(path: &Path) -> Result<MappingYaml> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("Failed to open mapping YAML: {}", path.display()))?;
    serde_yaml::from_reader(file).with_context(|| format!("Failed to parse {}", path.display()))
}

fn fields_for_source_path(
    schema: &CollectionSchema,
    source_path: &SourcePath,
) -> Option<indexmap::IndexMap<String, FieldSchema>> {
    fn descend(
        fields: &indexmap::IndexMap<String, FieldSchema>,
        path: &[String],
    ) -> Option<indexmap::IndexMap<String, FieldSchema>> {
        if path.is_empty() {
            return Some(fields.clone());
        }

        let field = fields.get(&path[0])?;
        let type_schema = field
            .types
            .iter()
            .find(|(type_name, _)| !is_null_type(type_name.as_str()))?
            .1;

        if let Some(object_fields) = type_schema.object.as_ref() {
            return descend(object_fields, &path[1..]);
        }

        if let Some(array_items) = type_schema.array.as_ref() {
            if let Some(object_schema) = array_items.types.get("Object") {
                if let Some(object_fields) = object_schema.object.as_ref() {
                    return descend(object_fields, &path[1..]);
                }
            }
        }

        None
    }

    if let Some(grouped_fields) = source_path.grouped_fields.as_ref() {
        let group = grouped_root_array_object_fields(&schema.object)
            .into_iter()
            .find(|group| &group.members == grouped_fields)?;
        return descend(&group.child_fields, &source_path.path);
    }

    descend(&schema.object, &source_path.path)
}

fn find_source_field_for_column(
    fields: &indexmap::IndexMap<String, FieldSchema>,
    column_name: &str,
    is_root: bool,
) -> Option<String> {
    fn reserved_inline_sibling_names(
        fields: &indexmap::IndexMap<String, FieldSchema>,
        current_raw_name: &str,
        is_root: bool,
    ) -> std::collections::HashSet<String> {
        let mut reserved = std::collections::HashSet::new();

        for (raw_name, field) in fields {
            if raw_name == current_raw_name {
                continue;
            }

            let non_null = field
                .types
                .iter()
                .filter(|(type_name, _)| !is_null_type(type_name.as_str()))
                .collect::<Vec<_>>();
            if non_null.is_empty() {
                continue;
            }

            if raw_name == "_id"
                && is_root
                && non_null.len() == 1
                && non_null[0].0.as_str() == "Object"
            {
                if let Some(sub_fields) = non_null[0].1.object.as_ref() {
                    for (path, _) in inline_object_leaf_fields_with_prefix(sub_fields, &[]) {
                        reserved.insert(sanitize_pg_name(&path.join("_")));
                    }
                }
                continue;
            }

            if non_null.len() == 1 && non_null[0].0.as_str() == "Object" {
                if let Some(sub_fields) = non_null[0].1.object.as_ref() {
                    if can_inline_object_fields(sub_fields) {
                        for (path, _) in inline_object_leaf_fields_with_prefix(sub_fields, &[]) {
                            if let Some(last) = path.last() {
                                reserved.insert(sanitize_pg_name(last));
                            }
                        }
                        continue;
                    }
                }
            }

            if raw_name == "_id" && is_root {
                reserved.insert("id".to_owned());
            } else {
                reserved.insert(sanitize_pg_name(raw_name));
            }
        }

        reserved
    }

    fn find_nested_source_field(
        fields: &indexmap::IndexMap<String, FieldSchema>,
        column_name: &str,
        is_root: bool,
    ) -> Option<String> {
        for (raw_name, field) in fields {
            let non_null = field
                .types
                .iter()
                .filter(|(type_name, _)| !is_null_type(type_name.as_str()))
                .collect::<Vec<_>>();
            if non_null.len() != 1 || non_null[0].0.as_str() != "Object" {
                continue;
            }
            let Some(sub_fields) = non_null[0].1.object.as_ref() else {
                continue;
            };
            if !can_inline_object_fields(sub_fields) {
                continue;
            }

            let reserved = reserved_inline_sibling_names(fields, raw_name, is_root);
            let prefix = vec![raw_name.clone()];
            let column_names =
                inline_object_column_names_with_prefix(sub_fields, &prefix, &reserved);
            for (source_field, target_field) in column_names {
                if target_field == column_name {
                    return Some(source_field);
                }
            }
        }

        None
    }

    if is_root && column_name == "id" && fields.contains_key("_id") {
        return Some("_id".to_owned());
    }

    if let Some(raw_name) = fields
        .keys()
        .find(|raw_name| sanitize_pg_name(raw_name) == column_name)
    {
        return Some(raw_name.clone());
    }

    if is_root {
        if let Some(id_object) = fields
            .get("_id")
            .and_then(|field| field.types.get("Object"))
            .and_then(|type_schema| type_schema.object.as_ref())
        {
            if let Some(raw_name) = id_object
                .keys()
                .find(|raw_name| sanitize_pg_name(raw_name) == column_name)
            {
                return Some(raw_name.clone());
            }
        }
    }

    find_nested_source_field(fields, column_name, is_root)
}

fn backfill_mapping_columns_from_schema(
    collection: &str,
    schema: &CollectionSchema,
    source_path: &SourcePath,
    mapping_yaml: &mut MappingYaml,
) {
    if !mapping_yaml.pg_mapping.columns.is_empty() {
        return;
    }

    let Some(ddl) = mapping_yaml.pg_mapping.ddl.as_ref() else {
        return;
    };

    let foreign_key_columns = ddl
        .foreign_keys
        .iter()
        .map(|fk| fk.from_col.as_str())
        .collect::<Vec<_>>();

    let mut push_column = |source_field: String, ddl_column: &DdlColumnYaml| {
        mapping_yaml.pg_mapping.columns.push(MappingColumnYaml {
            source_field,
            target_field: ddl_column.name.clone(),
            data_type: Some(ddl_column.sql_type.to_ascii_lowercase()),
        });
    };

    if let Some(scalar_array_field) = source_path.scalar_array_field.as_ref() {
        for ddl_column in &ddl.columns {
            if ddl_column.name == "id"
                || foreign_key_columns.iter().any(|fk| *fk == ddl_column.name)
            {
                continue;
            }
            push_column(scalar_array_field.clone(), ddl_column);
        }
        return;
    }

    if let Some((array_field_name, array_field)) = flatten_root_array_object_field(schema) {
        if source_path.path == vec![array_field_name.to_owned()] {
            let parent_id_col = flattened_root_parent_id_column(collection);
            let item_fields = array_field
                .types
                .get("Array")
                .and_then(|type_schema| type_schema.array.as_ref())
                .and_then(|items_field| items_field.types.get("Object"))
                .and_then(|type_schema| type_schema.object.as_ref());
            if let Some(item_fields) = item_fields {
                for ddl_column in &ddl.columns {
                    if ddl_column.name == "id" {
                        continue;
                    }
                    if ddl_column.name == parent_id_col {
                        push_column("_id".to_owned(), ddl_column);
                        continue;
                    }
                    if let Some(source_field) =
                        find_source_field_for_column(item_fields, &ddl_column.name, false)
                    {
                        push_column(source_field, ddl_column);
                    }
                }
            }
            return;
        }
    }

    if let Some(group) = flatten_grouped_root_array_object_fields(schema) {
        if source_path.path.is_empty()
            && source_path.grouped_fields.as_ref() == Some(&group.members)
        {
            let parent_id_col = flattened_root_parent_id_column(collection);
            for ddl_column in &ddl.columns {
                if ddl_column.name == "id" {
                    continue;
                }
                if ddl_column.name == parent_id_col {
                    push_column("_id".to_owned(), ddl_column);
                    continue;
                }
                if ddl_column.name == "key" {
                    push_column("key".to_owned(), ddl_column);
                    continue;
                }
                if let Some(source_field) = group
                    .child_fields
                    .keys()
                    .find(|raw_name| sanitize_pg_name(raw_name) == ddl_column.name)
                {
                    push_column(source_field.clone(), ddl_column);
                }
            }
            return;
        }
    }

    let Some(fields) = fields_for_source_path(schema, source_path) else {
        return;
    };

    for ddl_column in &ddl.columns {
        if foreign_key_columns.iter().any(|fk| *fk == ddl_column.name) {
            continue;
        }

        if source_path.path.is_empty() && ddl_column.name == "id" && fields.contains_key("_id") {
            push_column("_id".to_owned(), ddl_column);
            continue;
        }

        if !source_path.path.is_empty() && ddl_column.name == "id" {
            continue;
        }

        if let Some(source_field) =
            find_source_field_for_column(&fields, &ddl_column.name, source_path.path.is_empty())
        {
            push_column(source_field.clone(), ddl_column);
        }
    }
}

fn collection_paths_from_conf(conf: &crate::util::ConfData) -> Result<(String, PathBuf)> {
    let namespace = conf
        .namespace
        .as_ref()
        .ok_or_else(|| anyhow!("NAMESPACE not found in config"))?;
    let db_name = namespace
        .split_once('.')
        .map(|(db_name, _)| db_name)
        .unwrap_or(namespace)
        .to_owned();
    let collections_root = conf
        .base_dir
        .join(&conf.project_dir)
        .join("source")
        .join("collections");
    let collections_dir = if collections_root.join(&db_name).is_dir() {
        collections_root.join(&db_name)
    } else {
        collections_root
    };
    Ok((db_name, collections_dir))
}

fn discover_mapping_targets_for_collection(
    collection: &str,
    conf: &crate::util::ConfData,
) -> Result<Vec<MappingTarget>> {
    let (_, collections_dir) = collection_paths_from_conf(conf)?;
    let safe_collection_name = collection.replace('/', "_");
    let coll_dir = collections_dir.join(&safe_collection_name);
    let schema_path = coll_dir.join(format!("{safe_collection_name}.json"));
    let schema: CollectionSchema = serde_json::from_str(
        &std::fs::read_to_string(&schema_path)
            .with_context(|| format!("Failed to read {}", schema_path.display()))?,
    )
    .with_context(|| format!("Failed to parse {}", schema_path.display()))?;
    let source_paths = build_mapping_source_paths(collection, &schema);

    let mut targets = std::fs::read_dir(&coll_dir)
        .with_context(|| format!("Cannot read mapping directory {}", coll_dir.display()))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_file())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.starts_with("mapping_") && name.ends_with(".yaml"))
                .unwrap_or(false)
        })
        .map(|mapping_path| {
            let mut mapping_yaml = read_mapping_yaml(&mapping_path)?;
            let table_name = mapping_yaml.pg_mapping.table_name.clone();
            let source_path = source_paths.get(&table_name).cloned().ok_or_else(|| {
                anyhow!(
                    "No MongoDB source path found for table {} in {}",
                    table_name,
                    mapping_path.display()
                )
            })?;
            backfill_mapping_columns_from_schema(
                collection,
                &schema,
                &source_path,
                &mut mapping_yaml,
            );
            Ok(MappingTarget {
                source_collection: collection.to_owned(),
                mapping_path,
                mapping_yaml,
                source_path,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    targets.sort_by(|left, right| {
        left.source_path
            .path
            .len()
            .cmp(&right.source_path.path.len())
            .then(
                left.mapping_yaml
                    .pg_mapping
                    .table_name
                    .cmp(&right.mapping_yaml.pg_mapping.table_name),
            )
    });
    Ok(targets)
}

fn aggregate_md5_hexes(md5_hexes: impl IntoIterator<Item = String>) -> String {
    md5_hex_from_fragments(md5_hexes)
}

fn pg_hash_record(row: &Row) -> HashRecord {
    let values = (0..row.len())
        .map(|index| normalize_json_literal(&row.get::<usize, String>(index)))
        .collect::<Vec<_>>();
    HashRecord {
        md5: md5_hex_from_fragments(values.iter()),
        values,
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn mongo_sort_doc(source_fields: &[String]) -> Document {
    source_fields
        .iter()
        .map(|field| (field.clone(), Bson::Int32(1)))
        .collect()
}

fn pg_uri_with_database(uri: &str, database: &str) -> String {
    let (base, query) = match uri.split_once('?') {
        Some((base, query)) => (base, format!("?{query}")),
        None => (uri, String::new()),
    };
    let authority_start = uri.find("://").map(|pos| pos + 3).unwrap_or(0);
    match base[authority_start..].find('/') {
        Some(offset) => {
            let slash = authority_start + offset;
            format!("{}{database}{}", &base[..=slash], query)
        }
        None => format!("{base}/{database}{query}"),
    }
}

fn pg_sslmode(uri: &str) -> Option<&str> {
    let query = uri.split_once('?')?.1;
    query.split('&').find_map(|part| {
        let (key, value) = part.split_once('=')?;
        if key.eq_ignore_ascii_case("sslmode") {
            Some(value)
        } else {
            None
        }
    })
}

async fn connect_pg_client(target_uri: &str) -> Result<Client> {
    let mut tls_builder = native_tls::TlsConnector::builder();
    if matches!(pg_sslmode(target_uri), Some(mode) if mode.eq_ignore_ascii_case("require")) {
        tls_builder.danger_accept_invalid_certs(true);
        tls_builder.danger_accept_invalid_hostnames(true);
    }
    let tls = tls_builder
        .build()
        .with_context(|| "Failed to initialize PostgreSQL TLS connector")?;
    let tls = MakeTlsConnector::new(tls);

    let (pg_client, pg_connection) = tokio_postgres::connect(target_uri, tls)
        .await
        .with_context(|| "Failed to connect to PostgreSQL using TARGET_URI")?;
    tokio::spawn(async move {
        if let Err(err) = pg_connection.await {
            eprintln!("warning: PostgreSQL connection error: {err}");
        }
    });

    Ok(pg_client)
}

fn pg_select_query(
    schema_name: Option<&str>,
    table_name: &str,
    target_fields: &[String],
) -> String {
    let select_list = target_fields
        .iter()
        .map(|field| {
            let quoted = quote_ident(field);
            format!("COALESCE(to_json({quoted})::text, 'null') AS {quoted}")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let qualified_table = match schema_name {
        Some(schema_name) => format!("{}.{}", quote_ident(schema_name), quote_ident(table_name)),
        None => quote_ident(table_name),
    };
    let order_by = (1..=target_fields.len())
        .map(|index| index.to_string())
        .collect::<Vec<_>>()
        .join(", ");

    format!("SELECT {select_list} FROM {qualified_table} ORDER BY {order_by}",)
}

fn format_record_values(values: &[String]) -> String {
    format!("[{}]", values.join(", "))
}

fn print_hash_output(label: &str, records: &[HashRecord], aggregate: bool) {
    if aggregate {
        println!(
            "{label}: {}",
            aggregate_md5_hexes(records.iter().map(|record| record.md5.clone()))
        );
    } else {
        println!("{label}:");
        for record in records {
            println!("{}", record.md5);
        }
    }
}

fn print_mismatched_records(mongo_records: &[HashRecord], pg_records: &[HashRecord]) {
    for mismatch in collect_mismatched_record_samples(mongo_records, pg_records, usize::MAX) {
        println!("Mismatch at row {}:", mismatch.row_index);
        match &mismatch.mongo_values {
            Some(values) => {
                println!("  MongoDB md5: {}", md5_hex_from_fragments(values.iter()));
                println!("  MongoDB values: {}", format_record_values(values));
            }
            None => println!("  MongoDB: missing row"),
        }
        match &mismatch.pg_values {
            Some(values) => {
                println!(
                    "  PostgreSQL md5: {}",
                    md5_hex_from_fragments(values.iter())
                );
                println!("  PostgreSQL values: {}", format_record_values(values));
            }
            None => println!("  PostgreSQL: missing row"),
        }
    }
}

fn collect_mismatched_record_samples(
    mongo_records: &[HashRecord],
    pg_records: &[HashRecord],
    limit: usize,
) -> Vec<Md5MismatchRow> {
    let total = mongo_records.len().max(pg_records.len());
    let mut mismatches = Vec::new();

    for index in 0..total {
        let mongo_record = mongo_records.get(index);
        let pg_record = pg_records.get(index);
        let hashes_match = mongo_record.map(|record| record.md5.as_str())
            == pg_record.map(|record| record.md5.as_str());
        if hashes_match {
            continue;
        }

        mismatches.push(Md5MismatchRow {
            row_index: index + 1,
            mongo_values: mongo_record.map(|record| record.values.clone()),
            pg_values: pg_record.map(|record| record.values.clone()),
        });
        if mismatches.len() == limit {
            break;
        }
    }

    mismatches
}

fn build_md5_summary(
    columns: Vec<Md5ColumnMapping>,
    mongo_records: Vec<HashRecord>,
    pg_records: Vec<HashRecord>,
) -> Md5Summary {
    let mismatches = collect_mismatched_record_samples(&mongo_records, &pg_records, 5);

    Md5Summary {
        mongo_md5: aggregate_md5_hexes(mongo_records.into_iter().map(|record| record.md5)),
        pg_md5: aggregate_md5_hexes(pg_records.into_iter().map(|record| record.md5)),
        columns,
        mismatches,
    }
}

async fn collect_hash_records_for_target(
    target: &MappingTarget,
    conf: &crate::util::ConfData,
) -> Result<(Vec<Md5ColumnMapping>, Vec<HashRecord>, Vec<HashRecord>)> {
    let source_fields: Vec<String> = target
        .mapping_yaml
        .pg_mapping
        .columns
        .iter()
        .map(|c| c.source_field.clone())
        .collect();
    let typed_source_fields: Vec<(String, Option<String>)> = target
        .mapping_yaml
        .pg_mapping
        .columns
        .iter()
        .map(|c| (c.source_field.clone(), c.data_type.clone()))
        .collect();
    let target_fields: Vec<String> = target
        .mapping_yaml
        .pg_mapping
        .columns
        .iter()
        .map(|c| c.target_field.clone())
        .collect();
    let columns = target
        .mapping_yaml
        .pg_mapping
        .columns
        .iter()
        .map(|column| Md5ColumnMapping {
            source_field: column.source_field.clone(),
            target_field: column.target_field.clone(),
        })
        .collect::<Vec<_>>();
    if source_fields.is_empty() {
        return Err(anyhow!(
            "No source fields found in mapping YAML: {}",
            target.mapping_path.display()
        ));
    }

    if target_fields.is_empty() {
        return Err(anyhow!(
            "No target fields found in mapping YAML: {}",
            target.mapping_path.display()
        ));
    }

    let mongo_uri = conf
        .source_uri
        .as_ref()
        .ok_or_else(|| anyhow!("SOURCE_URI not found in config"))?;
    let (db_name, _) = collection_paths_from_conf(conf)?;
    let client_options = mongodb::options::ClientOptions::parse(mongo_uri).await?;
    let client = mongodb::Client::with_options(client_options)?;
    let mongo_collection = client
        .database(&db_name)
        .collection::<bson::Document>(&target.source_collection);

    let mut find_action = mongo_collection.find(doc! {});
    if target.source_path.path.is_empty() {
        find_action = find_action.sort(mongo_sort_doc(&source_fields));
    }
    let mut cursor = find_action.await?;
    let mut mongo_records = Vec::new();
    while let Some(doc) = cursor.try_next().await? {
        if target.source_path.path.is_empty() {
            mongo_records.push(mongo_hash_record_for_columns(&doc, &typed_source_fields));
        } else {
            mongo_records.extend(
                extract_source_documents(&doc, &target.source_path)
                    .into_iter()
                    .map(|mut source_doc| {
                        if source_fields.iter().any(|field| field == "_id")
                            && !source_doc.contains_key("_id")
                        {
                            if let Some(root_id) = doc.get("_id") {
                                source_doc.insert("_id", root_id.clone());
                            }
                        }
                        source_doc
                    })
                    .map(|source_doc| {
                        mongo_hash_record_for_columns(&source_doc, &typed_source_fields)
                    }),
            );
        }
    }
    sort_hash_records(&mut mongo_records);

    let target_uri = conf
        .target_uri
        .as_ref()
        .ok_or_else(|| anyhow!("TARGET_URI not found in config"))?;
    let target_database_name = conf
        .target_database_name
        .as_deref()
        .or(target.mapping_yaml.pg_mapping.dbname.as_deref())
        .or(target.mapping_yaml.dbname.as_deref())
        .ok_or_else(|| anyhow!("TARGET_DATABASE_NAME not found in config or mapping"))?;
    let schema_name =
        conf.target_schema
            .as_deref()
            .or(target.mapping_yaml.pg_mapping.schema_name.as_deref());
    let pg_uri = pg_uri_with_database(target_uri, target_database_name);
    let pg_client = connect_pg_client(&pg_uri).await?;
    let select_query = pg_select_query(
        schema_name,
        &target.mapping_yaml.pg_mapping.table_name,
        &target_fields,
    );
    let pg_rows = pg_client
        .query(&select_query, &[])
        .await
        .with_context(|| format!("Failed to fetch PostgreSQL rows with query: {select_query}"))?;
    let mut pg_records = pg_rows.iter().map(pg_hash_record).collect::<Vec<_>>();
    sort_hash_records(&mut pg_records);

    Ok((columns, mongo_records, pg_records))
}

pub async fn compute_md5_summaries_for_collection(
    collection: &str,
    config_path: &Path,
) -> Result<Vec<Md5TableSummary>> {
    let conf = read_conf(config_path)?;
    let targets = discover_mapping_targets_for_collection(collection, &conf)?;
    let mut summaries = Vec::new();

    for target in targets {
        let table_name = target.mapping_yaml.pg_mapping.table_name.clone();
        let (columns, mongo_records, pg_records) =
            collect_hash_records_for_target(&target, &conf).await?;
        summaries.push(Md5TableSummary {
            table_name,
            summary: build_md5_summary(columns, mongo_records, pg_records),
        });
    }

    Ok(summaries)
}

pub async fn compute_md5_summary(collection: &str, config_path: &Path) -> Result<Md5Summary> {
    compute_md5_summaries_for_collection(collection, config_path)
        .await?
        .into_iter()
        .find(|summary| summary.table_name == sanitize_pg_name(collection))
        .map(|summary| summary.summary)
        .ok_or_else(|| anyhow!("No root mapping summary found for collection {collection}"))
}

pub async fn run_check_md5(
    collection: String,
    config: Option<PathBuf>,
    aggregate: bool,
) -> Result<()> {
    let config_path = config
        .as_ref()
        .ok_or_else(|| anyhow!("-c <config> is required"))?;
    let conf = read_conf(config_path)?;
    let targets = discover_mapping_targets_for_collection(&collection, &conf)?;
    let total_tables = targets.len();

    for (index, target) in targets.iter().enumerate() {
        if index > 0 {
            println!();
        }
        let source_path = if target.source_path.path.is_empty() {
            collection.as_str().to_owned()
        } else {
            format!("{}.{}", collection, target.source_path.path.join("."))
        };
        println!(
            "[{}/{}] Table {}",
            index + 1,
            total_tables,
            target.mapping_yaml.pg_mapping.table_name
        );
        if !target.source_path.path.is_empty() {
            println!("  MongoDB path: {source_path}");
        }
        let (_, mongo_records, pg_records) = collect_hash_records_for_target(target, &conf).await?;
        print_hash_output("MongoDB", &mongo_records, aggregate);
        print_hash_output("PostgreSQL", &pg_records, aggregate);
        if !aggregate {
            print_mismatched_records(&mongo_records, &pg_records);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        aggregate_md5_hexes, backfill_mapping_columns_from_schema, build_mapping_source_paths,
        collect_mismatched_record_samples, extract_source_documents, format_record_values,
        md5_hex_from_fragments, mongo_field_literal, mongo_field_literal_for_type,
        mongo_hash_record, mongo_sort_doc, normalize_json_literal, pg_select_query, HashRecord,
        MappingYaml, SourcePath,
    };
    use crate::analyzer::Analyzer;
    use bson::{doc, Bson};

    #[test]
    fn backfill_mapping_columns_recovers_legacy_root_jsonb_mapping() {
        let docs = vec![doc! {
            "_id": "pg",
            "versions": [
                {"major_version": "16", "eol_date": bson::DateTime::now()}
            ]
        }];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();
        let mut mapping_yaml: MappingYaml = serde_yaml::from_str(
            r#"
pg_mapping:
  table_name: engine
  columns: []
  ddl:
    columns:
            - { name: id, sql_type: "VARCHAR(20)" }
            - { name: versions, sql_type: JSONB }
    foreign_keys: []
"#,
        )
        .expect("mapping yaml should parse");

        backfill_mapping_columns_from_schema(
            "engine",
            &schema,
            &SourcePath {
                path: Vec::new(),
                scalar_array_field: None,
                grouped_fields: None,
            },
            &mut mapping_yaml,
        );

        let columns = mapping_yaml
            .pg_mapping
            .columns
            .iter()
            .map(|column| (column.source_field.as_str(), column.target_field.as_str()))
            .collect::<Vec<_>>();
        assert!(columns.contains(&("_id", "id")));
        assert!(columns.contains(&("versions", "versions")));
    }

    #[test]
    fn backfill_mapping_columns_restores_nested_object_child_table() {
        let docs = vec![doc! {
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
        }];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();
        let mut mapping_yaml: MappingYaml = serde_yaml::from_str(
            r#"
pg_mapping:
    table_name: projects_providers_metadata
    columns: []
    ddl:
        columns:
            - { name: id, sql_type: BIGSERIAL }
            - { name: projects_providers_id, sql_type: BIGINT }
            - { name: creation_date, sql_type: "TIMESTAMP WITH TIME ZONE" }
            - { name: status, sql_type: TEXT }
        foreign_keys:
            - { from_col: projects_providers_id }
"#,
        )
        .expect("mapping yaml should parse");
        backfill_mapping_columns_from_schema(
            "projects",
            &schema,
            &SourcePath {
                path: vec!["providers".to_owned(), "metadata".to_owned()],
                scalar_array_field: None,
                grouped_fields: None,
            },
            &mut mapping_yaml,
        );

        let columns = mapping_yaml
            .pg_mapping
            .columns
            .iter()
            .map(|column| (column.source_field.as_str(), column.target_field.as_str()))
            .collect::<Vec<_>>();
        assert!(columns.contains(&("creation_date", "creation_date")));
        assert!(columns.contains(&("status", "status")));
    }

    #[test]
    fn mongo_hash_record_uses_only_requested_fields() {
        let doc = doc! {
            "name": "alice",
            "age": 42,
            "ignored": "x"
        };
        let fields = vec!["name".to_owned(), "age".to_owned()];
        let expected_concat = format!(
            "{}{}",
            mongo_field_literal(&doc, "name"),
            mongo_field_literal(&doc, "age")
        );

        let record = mongo_hash_record(&doc, &fields);

        assert_eq!(
            record.md5,
            format!("{:x}", md5::compute(expected_concat.as_bytes()))
        );
        assert_eq!(record.values, vec!["\"alice\"", "42"]);
    }

    #[test]
    fn missing_mongo_field_hashes_as_null_literal() {
        let doc = doc! { "name": "alice" };

        assert_eq!(mongo_field_literal(&doc, "missing"), "null");
    }

    #[test]
    fn normalize_json_literal_coalesces_integral_numbers() {
        assert_eq!(normalize_json_literal("647.0"), "647");
        assert_eq!(normalize_json_literal("0.0"), "0");
        assert_eq!(
            normalize_json_literal("[4, 647.0, \"business-16\", 16, 1050, \"3 nodes cluster\"]"),
            "[4, 647, \"business-16\", 16, 1050, \"3 nodes cluster\"]"
        );
    }

    #[test]
    fn normalize_json_literal_preserves_json_strings() {
        assert_eq!(normalize_json_literal("\"647.0\""), "\"647.0\"");
    }

    #[test]
    fn mongo_field_literal_coerces_numeric_values_for_text_targets() {
        let doc = doc! { "monthly_gain": 0 };

        assert_eq!(
            mongo_field_literal_for_type(&doc, "monthly_gain", Some("text")),
            "\"0\""
        );
    }

    #[test]
    fn mongo_field_literal_does_not_double_encode_string_values_for_text_targets() {
        let doc = doc! { "status": "OK", "object_id": bson::oid::ObjectId::parse_str("638f6313d082904d34928256").unwrap() };

        assert_eq!(
            mongo_field_literal_for_type(&doc, "status", Some("text")),
            "\"OK\""
        );
        assert_eq!(
            mongo_field_literal_for_type(&doc, "object_id", Some("text")),
            "\"638f6313d082904d34928256\""
        );
    }

    #[test]
    fn aggregate_md5_combines_document_hashes_into_one_value() {
        let aggregate = aggregate_md5_hexes(vec![
            "aaaabbbbccccdddd".repeat(2),
            "1111222233334444".repeat(2),
        ]);

        assert_eq!(
            aggregate,
            format!(
                "{:x}",
                md5::compute(
                    "aaaabbbbccccddddaaaabbbbccccdddd11112222333344441111222233334444".as_bytes()
                )
            )
        );
    }

    #[test]
    fn md5_from_fragments_concatenates_in_order() {
        let digest = md5_hex_from_fragments(["\"alice\"", "42", "null"]);

        assert_eq!(
            digest,
            format!("{:x}", md5::compute("\"alice\"42null".as_bytes()))
        );
    }

    #[test]
    fn format_record_values_joins_values_in_brackets() {
        assert_eq!(
            format_record_values(&["\"alice\"".to_owned(), "42".to_owned(), "null".to_owned()]),
            "[\"alice\", 42, null]"
        );
    }

    #[test]
    fn mongo_sort_doc_uses_all_source_fields_in_order() {
        let sort = mongo_sort_doc(&[
            "id".to_owned(),
            "last_update".to_owned(),
            "region".to_owned(),
        ]);

        let entries = sort.into_iter().collect::<Vec<_>>();
        assert_eq!(
            entries,
            vec![
                ("id".to_owned(), Bson::Int32(1)),
                ("last_update".to_owned(), Bson::Int32(1)),
                ("region".to_owned(), Bson::Int32(1)),
            ]
        );
    }

    #[test]
    fn pg_select_query_orders_by_all_selected_columns() {
        let query = pg_select_query(
            Some("dbapi"),
            "scheduling_jobs",
            &[
                "id".to_owned(),
                "last_update".to_owned(),
                "region".to_owned(),
            ],
        );

        assert!(
            query.contains("ORDER BY 1, 2, 3"),
            "query should order by all selected columns in order"
        );
    }

    #[test]
    fn build_mapping_source_paths_tracks_nested_tables() {
        let docs = vec![doc! {
            "_id": 1,
            "advices": [{
                "advice": "oversized",
                "earnings": {
                    "monthly_gain": 12.5_f64
                }
            }]
        }];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let source_paths = build_mapping_source_paths("advisors", &schema);

        assert_eq!(
            source_paths.get("advisors"),
            Some(&SourcePath {
                path: vec!["advices".to_owned()],
                scalar_array_field: None,
                grouped_fields: None,
            })
        );
        assert_eq!(source_paths.get("advices"), None);
        assert_eq!(
            source_paths.get("earnings"),
            Some(&SourcePath {
                path: vec!["advices".to_owned(), "earnings".to_owned()],
                scalar_array_field: None,
                grouped_fields: None,
            })
        );
    }

    #[test]
    fn build_mapping_source_paths_tracks_scalar_array_tables() {
        let docs = vec![doc! {
            "_id": 1,
            "available_versions": ["2", "3.3"]
        }];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let source_paths = build_mapping_source_paths("sizings", &schema);

        assert_eq!(
            source_paths.get("available_versions"),
            Some(&SourcePath {
                path: vec!["available_versions".to_owned()],
                scalar_array_field: Some("available_versions".to_owned()),
                grouped_fields: None,
            })
        );
    }

    #[test]
    fn build_mapping_source_paths_groups_same_shape_root_arrays() {
        let docs = vec![doc! {
            "_id": 1,
            "dev": [{
                "provider": "aiven",
                "available_localizations": ["eu-west-1"]
            }],
            "prod": [{
                "provider": "atlas",
                "available_localizations": ["eu-west-2"]
            }]
        }];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let source_paths = build_mapping_source_paths("communities", &schema);

        assert_eq!(
            source_paths.get("communities"),
            Some(&SourcePath {
                path: Vec::new(),
                scalar_array_field: None,
                grouped_fields: Some(vec!["dev".to_owned(), "prod".to_owned()]),
            })
        );
        assert_eq!(
            source_paths.get("available_localizations"),
            Some(&SourcePath {
                path: vec!["available_localizations".to_owned()],
                scalar_array_field: Some("available_localizations".to_owned()),
                grouped_fields: Some(vec!["dev".to_owned(), "prod".to_owned()]),
            })
        );
        assert_eq!(source_paths.get("communities_dev"), None);
        assert_eq!(source_paths.get("communities_prod"), None);
    }

    #[test]
    fn extract_source_documents_flattens_nested_array_objects() {
        let doc = doc! {
            "advices": [
                {
                    "earnings": { "monthly_gain": 12.5_f64 }
                },
                {
                    "earnings": { "monthly_gain": 7.0_f64 }
                }
            ]
        };

        let nested = extract_source_documents(
            &doc,
            &SourcePath {
                path: vec!["advices".to_owned(), "earnings".to_owned()],
                scalar_array_field: None,
                grouped_fields: None,
            },
        );

        assert_eq!(nested.len(), 2);
        assert_eq!(nested[0].get_f64("monthly_gain"), Ok(12.5));
        assert_eq!(nested[1].get_f64("monthly_gain"), Ok(7.0));
    }

    #[test]
    fn extract_source_documents_expands_scalar_arrays() {
        let doc = doc! {
            "_id": 42,
            "available_versions": ["2", "3.3"]
        };

        let nested = extract_source_documents(
            &doc,
            &SourcePath {
                path: vec!["available_versions".to_owned()],
                scalar_array_field: Some("available_versions".to_owned()),
                grouped_fields: None,
            },
        );

        assert_eq!(nested.len(), 2);
        assert_eq!(nested[0].get_str("available_versions"), Ok("2"));
        assert_eq!(nested[1].get_str("available_versions"), Ok("3.3"));
        assert_eq!(nested[0].get_i32("_id"), Ok(42));
    }

    #[test]
    fn extract_source_documents_groups_root_array_siblings_with_key() {
        let doc = doc! {
            "_id": 42,
            "dev": [{"provider": "aiven"}],
            "prod": [{"provider": "atlas"}]
        };

        let nested = extract_source_documents(
            &doc,
            &SourcePath {
                path: Vec::new(),
                scalar_array_field: None,
                grouped_fields: Some(vec!["dev".to_owned(), "prod".to_owned()]),
            },
        );

        assert_eq!(nested.len(), 2);
        assert_eq!(nested[0].get_str("key"), Ok("dev"));
        assert_eq!(nested[1].get_str("key"), Ok("prod"));
    }

    #[test]
    fn mongo_field_literal_supports_dotted_paths_for_inlined_objects() {
        let doc = doc! {
            "metadata": {
                "creation_date": "2025-08-11T00:00:00Z"
            }
        };

        assert_eq!(
            mongo_field_literal(&doc, "metadata.creation_date"),
            "\"2025-08-11T00:00:00Z\""
        );
    }

    #[test]
    fn collect_mismatched_record_samples_limits_to_first_five_rows() {
        let mongo_records = (0..7)
            .map(|index| HashRecord {
                md5: format!("mongo-{index}"),
                values: vec![format!("m{index}")],
            })
            .collect::<Vec<_>>();
        let pg_records = (0..7)
            .map(|index| HashRecord {
                md5: format!("pg-{index}"),
                values: vec![format!("p{index}")],
            })
            .collect::<Vec<_>>();

        let mismatches = collect_mismatched_record_samples(&mongo_records, &pg_records, 5);

        assert_eq!(mismatches.len(), 5);
        assert_eq!(mismatches[0].row_index, 1);
        assert_eq!(mismatches[4].row_index, 5);
        assert_eq!(
            mismatches[0].mongo_values.as_deref(),
            Some(&["m0".to_owned()][..])
        );
        assert_eq!(
            mismatches[0].pg_values.as_deref(),
            Some(&["p0".to_owned()][..])
        );
    }
}
