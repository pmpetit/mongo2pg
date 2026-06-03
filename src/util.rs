use crate::analyzer::{CollectionSchema, FieldSchema, TypeSchema};
use anyhow::{anyhow, Context, Result};
use indexmap::IndexMap;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize)]
pub struct ConfData {
    pub base_dir: PathBuf,
    pub title: String,
    pub project_dir: String,
    pub source_uri: Option<String>,
    pub target_uri: Option<String>,
    pub target_database_name: Option<String>,
    pub target_schema: Option<String>,
    pub namespace: Option<String>,
    pub number: Option<u64>,
    pub percent: Option<f64>,
    pub jsonb: bool,
    pub timestamp_fields: Vec<String>,
    pub include: Vec<String>,
    pub exclude: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct TomlProjectConfig {
    project: TomlProjectSection,
    #[serde(default)]
    source: Option<TomlSourceSection>,
    #[serde(default)]
    target: Option<TomlTargetSection>,
}

#[derive(Debug, Deserialize)]
struct TomlProjectSection {
    title: String,
    base_dir: PathBuf,
    project_dir: String,
}

#[derive(Debug, Deserialize, Default)]
struct TomlSourceSection {
    uri: Option<String>,
    namespace: Option<String>,
    number: Option<u64>,
    percent: Option<f64>,
    jsonb: Option<bool>,
    #[serde(default = "default_timestamp_fields", alias = "timestamp_field")]
    datetime_field: Vec<String>,
    #[serde(default)]
    include: Vec<String>,
    #[serde(default)]
    exclude: Vec<String>,
}

pub fn default_timestamp_fields() -> Vec<String> {
    vec![
        "created_at".to_owned(),
        "last_update".to_owned(),
        "updated_at".to_owned(),
        "*_date".to_owned(),
        "date".to_owned(),
    ]
}

#[derive(Debug, Deserialize, Default)]
struct TomlTargetSection {
    uri: Option<String>,
    database_name: Option<String>,
    schema: Option<String>,
}

pub fn read_conf(path: &Path) -> Result<ConfData> {
    fn parse_toml_conf(path: &Path, content: &str) -> Result<ConfData> {
        let parsed: TomlProjectConfig = toml::from_str(content)
            .with_context(|| format!("Failed to parse TOML config {}", path.display()))?;
        let source = parsed.source.unwrap_or_default();
        let target = parsed.target.unwrap_or_default();

        Ok(ConfData {
            base_dir: parsed.project.base_dir,
            title: parsed.project.title,
            project_dir: parsed.project.project_dir,
            source_uri: source.uri,
            target_uri: target.uri,
            target_database_name: target.database_name,
            target_schema: target.schema,
            namespace: source.namespace,
            number: source.number,
            percent: source.percent,
            jsonb: source.jsonb.unwrap_or(false),
            timestamp_fields: source.datetime_field,
            include: source.include,
            exclude: source.exclude,
        })
    }

    fn parse_legacy_conf(path: &Path, content: &str) -> Result<ConfData> {
        fn parse_conf_value(raw: &str) -> String {
            let value = raw.trim();
            if value.len() >= 2 {
                let quoted = (value.starts_with('"') && value.ends_with('"'))
                    || (value.starts_with('\'') && value.ends_with('\''));
                if quoted {
                    return value[1..value.len() - 1].trim().to_owned();
                }
            }
            value.to_owned()
        }

        let mut base_dir: Option<PathBuf> = None;
        let mut title: String = "mongo2pg Project Title".to_owned();
        let mut project_dir: Option<String> = None;
        let mut source_uri: Option<String> = None;
        let mut target_uri: Option<String> = None;
        let mut target_database_name: Option<String> = None;
        let mut target_schema: Option<String> = None;
        let mut namespace: Option<String> = None;
        let mut number: Option<u64> = None;
        let mut percent: Option<f64> = None;
        let mut jsonb: bool = false;

        for line in content.lines() {
            if let Some((key, val)) = line.split_once('=') {
                let parsed = parse_conf_value(val);
                match key.trim() {
                    "BASE_DIR" => base_dir = Some(PathBuf::from(&parsed)),
                    "TITLE" => title = parsed,
                    "PROJECT_DIR" => project_dir = Some(parsed),
                    "SOURCE_URI" => source_uri = Some(parsed),
                    "TARGET_URI" => target_uri = Some(parsed),
                    "TARGET_DATABASE_NAME" => target_database_name = Some(parsed),
                    "TARGET_SCHEMA" => target_schema = Some(parsed),
                    "NAMESPACE" => namespace = Some(parsed),
                    "NUMBER" => number = parsed.parse().ok(),
                    "PERCENT" => percent = parsed.parse().ok(),
                    "JSONB" => {
                        jsonb = matches!(parsed.to_lowercase().as_str(), "true" | "1" | "yes")
                    }
                    _ => {}
                }
            }
        }

        let base_dir =
            base_dir.ok_or_else(|| anyhow!("BASE_DIR not found in {}", path.display()))?;
        let project_dir =
            project_dir.ok_or_else(|| anyhow!("PROJECT_DIR not found in {}", path.display()))?;

        Ok(ConfData {
            base_dir,
            title,
            project_dir,
            source_uri,
            target_uri,
            target_database_name,
            target_schema,
            namespace,
            number,
            percent,
            jsonb,
            timestamp_fields: default_timestamp_fields(),
            include: Vec::new(),
            exclude: Vec::new(),
        })
    }

    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read config file {}", path.display()))?;

    match parse_toml_conf(path, &content) {
        Ok(conf) => Ok(conf),
        Err(_) => parse_legacy_conf(path, &content),
    }
}

pub fn should_infer_collection(name: &str, include: &[String], exclude: &[String]) -> bool {
    if exclude.iter().any(|candidate| {
        split_collection_property_filter(candidate).0 == name && !candidate.contains('.')
    }) {
        return false;
    }

    if !include.is_empty() {
        include
            .iter()
            .any(|candidate| split_collection_property_filter(candidate).0 == name)
    } else {
        true
    }
}

fn split_collection_property_filter(entry: &str) -> (&str, Option<&str>) {
    match entry.split_once('.') {
        Some((collection, property)) if !collection.is_empty() && !property.is_empty() => {
            (collection, Some(property))
        }
        _ => (entry, None),
    }
}

pub fn property_filter_entries_for_collection<'a>(
    collection: &str,
    entries: &'a [String],
) -> Vec<&'a str> {
    entries
        .iter()
        .filter_map(|entry| {
            let (entry_collection, property) = split_collection_property_filter(entry);
            (entry_collection == collection)
                .then_some(property)
                .flatten()
        })
        .collect()
}

#[derive(Debug, Clone)]
pub struct GroupedRootArrayObjectFields {
    pub representative: String,
    pub members: Vec<String>,
    pub child_fields: IndexMap<String, FieldSchema>,
}

fn type_shape_signature(type_schema: &TypeSchema) -> String {
    let object_sig = type_schema.object.as_ref().map(|fields| {
        let mut field_sigs = fields
            .iter()
            .map(|(name, field)| format!("{name}:{}", field_shape_signature(field)))
            .collect::<Vec<_>>();
        field_sigs.sort();
        format!("{{{}}}", field_sigs.join(","))
    });
    let array_sig = type_schema
        .array
        .as_ref()
        .map(|field| format!("[{}]", field_shape_signature(field)));

    format!(
        "jsonb:{}{}{}",
        type_schema.as_jsonb,
        object_sig.unwrap_or_default(),
        array_sig.unwrap_or_default()
    )
}

fn field_shape_signature(field: &FieldSchema) -> String {
    let mut type_sigs = field
        .types
        .iter()
        .map(|(name, type_schema)| format!("{name}:{}", type_shape_signature(type_schema)))
        .collect::<Vec<_>>();
    type_sigs.sort();
    type_sigs.join("|")
}

pub fn grouped_root_array_object_fields(
    fields: &IndexMap<String, FieldSchema>,
) -> Vec<GroupedRootArrayObjectFields> {
    let mut groups: Vec<(String, Vec<(String, IndexMap<String, FieldSchema>)>)> = Vec::new();

    for (raw_name, field) in fields {
        let non_null: Vec<(&str, &TypeSchema)> = field
            .types
            .iter()
            .filter(|(type_name, _)| !is_null_type(type_name.as_str()))
            .map(|(type_name, type_schema)| (type_name.as_str(), type_schema))
            .collect();
        if non_null.len() != 1 || non_null[0].0 != "Array" {
            continue;
        }
        let Some(item_field) = non_null[0].1.array.as_ref() else {
            continue;
        };
        let item_non_null: Vec<(&str, &TypeSchema)> = item_field
            .types
            .iter()
            .filter(|(type_name, _)| !is_null_type(type_name.as_str()))
            .map(|(type_name, type_schema)| (type_name.as_str(), type_schema))
            .collect();
        if item_non_null.len() != 1 || item_non_null[0].0 != "Object" {
            continue;
        }
        let Some(child_fields) = item_non_null[0].1.object.as_ref() else {
            continue;
        };

        let signature = field_shape_signature(field);
        if let Some((_, members)) = groups.iter_mut().find(|(sig, _)| *sig == signature) {
            members.push((raw_name.clone(), child_fields.clone()));
        } else {
            groups.push((signature, vec![(raw_name.clone(), child_fields.clone())]));
        }
    }

    groups
        .into_iter()
        .filter(|(_, members)| members.len() >= 2)
        .map(|(_, members)| GroupedRootArrayObjectFields {
            representative: members[0].0.clone(),
            members: members.iter().map(|(name, _)| name.clone()).collect(),
            child_fields: members[0].1.clone(),
        })
        .collect()
}

pub fn can_inline_object_fields(fields: &IndexMap<String, FieldSchema>) -> bool {
    if fields.is_empty() {
        return false;
    }

    let leaf_count = inline_object_leaf_fields(fields).len();
    leaf_count >= 2 && fields.values().all(can_inline_object_field)
}

pub fn can_inline_object_field(field: &FieldSchema) -> bool {
    let non_null = field
        .types
        .iter()
        .filter(|(type_name, _)| !is_null_type(type_name.as_str()))
        .collect::<Vec<_>>();

    if non_null.is_empty() {
        return true;
    }

    if non_null.len() != 1 {
        return false;
    }

    let (type_name, type_schema) = non_null[0];
    match type_name.as_str() {
        "Object" => type_schema
            .object
            .as_ref()
            .is_some_and(can_inline_object_fields),
        "Array" => false,
        _ => !type_schema.as_jsonb,
    }
}

pub fn inline_object_leaf_fields_with_prefix<'a>(
    fields: &'a IndexMap<String, FieldSchema>,
    prefix: &[String],
) -> Vec<(Vec<String>, &'a FieldSchema)> {
    fn visit<'a>(
        fields: &'a IndexMap<String, FieldSchema>,
        prefix: &[String],
        out: &mut Vec<(Vec<String>, &'a FieldSchema)>,
    ) {
        for (raw_name, field) in fields {
            let mut path = prefix.to_vec();
            path.push(raw_name.clone());

            let non_null = field
                .types
                .iter()
                .filter(|(type_name, _)| !is_null_type(type_name.as_str()))
                .collect::<Vec<_>>();

            if non_null.len() == 1 && non_null[0].0.as_str() == "Object" {
                if let Some(sub_fields) = non_null[0].1.object.as_ref() {
                    if can_inline_object_fields(sub_fields) {
                        visit(sub_fields, &path, out);
                        continue;
                    }
                }
            }

            out.push((path, field));
        }
    }

    let mut out = Vec::new();
    visit(fields, prefix, &mut out);
    out
}

pub fn inline_object_leaf_fields<'a>(
    fields: &'a IndexMap<String, FieldSchema>,
) -> Vec<(Vec<String>, &'a FieldSchema)> {
    inline_object_leaf_fields_with_prefix(fields, &[])
}

pub fn inline_object_column_names_with_prefix(
    fields: &IndexMap<String, FieldSchema>,
    prefix: &[String],
    reserved: &HashSet<String>,
) -> HashMap<String, String> {
    fn candidate_name(path: &[String], depth: usize) -> String {
        let len = path.len();
        let start = len.saturating_sub(depth + 1);
        sanitize(&path[start..].join("_"))
    }

    let leaf_paths = inline_object_leaf_fields_with_prefix(fields, prefix)
        .into_iter()
        .map(|(path, _)| path)
        .collect::<Vec<_>>();
    let mut depths = vec![0usize; leaf_paths.len()];

    loop {
        let mut name_to_indexes: HashMap<String, Vec<usize>> = HashMap::new();
        for (index, path) in leaf_paths.iter().enumerate() {
            let name = candidate_name(path, depths[index]);
            name_to_indexes.entry(name).or_default().push(index);
        }

        let mut changed = false;
        for (name, indexes) in &name_to_indexes {
            let collides = reserved.contains(name) || indexes.len() > 1;
            if !collides {
                continue;
            }
            for index in indexes {
                if depths[*index] + 1 < leaf_paths[*index].len() {
                    depths[*index] += 1;
                    changed = true;
                }
            }
        }

        if !changed {
            break;
        }
    }

    let mut assigned_counts: HashMap<String, usize> = HashMap::new();
    let mut result = HashMap::new();
    for (index, path) in leaf_paths.iter().enumerate() {
        let base_name = candidate_name(path, depths[index]);
        let count = assigned_counts.entry(base_name.clone()).or_insert(0);
        *count += 1;
        let final_name = if *count == 1 && !reserved.contains(&base_name) {
            base_name
        } else {
            format!("{}_{}", base_name, *count)
        };
        result.insert(path.join("."), final_name);
    }

    result
}

pub fn inline_object_column_names(
    fields: &IndexMap<String, FieldSchema>,
    reserved: &HashSet<String>,
) -> HashMap<String, String> {
    inline_object_column_names_with_prefix(fields, &[], reserved)
}

pub fn flatten_grouped_root_array_object_fields(
    schema: &CollectionSchema,
) -> Option<GroupedRootArrayObjectFields> {
    if schema.object.len() < 3 {
        return None;
    }

    let id_field = schema.object.get("_id")?;
    let id_non_null = id_field
        .types
        .iter()
        .filter(|(type_name, _)| !is_null_type(type_name.as_str()))
        .map(|(type_name, _)| type_name.as_str())
        .collect::<Vec<_>>();
    if id_non_null.len() != 1 || matches!(id_non_null[0], "Object" | "Array") {
        return None;
    }

    let grouped = grouped_root_array_object_fields(&schema.object);
    if grouped.len() != 1 {
        return None;
    }

    let group = grouped.into_iter().next()?;
    let non_id_fields = schema
        .object
        .keys()
        .filter(|name| name.as_str() != "_id")
        .cloned()
        .collect::<Vec<_>>();

    if non_id_fields.len() != group.members.len() {
        return None;
    }

    if non_id_fields
        .iter()
        .any(|field_name| !group.members.iter().any(|member| member == field_name))
    {
        return None;
    }

    Some(group)
}

pub fn matches_timestamp_field(name: &str, patterns: &[String]) -> bool {
    let normalized_name = name.trim().to_ascii_lowercase();
    patterns.iter().any(|pattern| {
        let normalized_pattern = pattern.trim().to_ascii_lowercase();
        if normalized_pattern.is_empty() {
            return false;
        }
        if let Some(suffix) = normalized_pattern.strip_prefix('*') {
            normalized_name.ends_with(suffix)
        } else {
            normalized_name == normalized_pattern
        }
    })
}

/// Utility functions shared across modules.

/// Returns true if the string is a reserved PostgreSQL keyword.
pub fn is_pg_reserved(s: &str) -> bool {
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
/// Non-ASCII-alphanumeric characters are replaced with `_`. Names that start
/// with a digit or clash with a PostgreSQL reserved word are prefixed with `_`.
pub fn sanitize(name: &str) -> String {
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

pub fn flatten_root_array_object_field<'a>(
    schema: &'a CollectionSchema,
) -> Option<(&'a str, &'a FieldSchema)> {
    if schema.object.len() != 2 {
        return None;
    }

    let id_field = schema.object.get("_id")?;
    let id_non_null = id_field
        .types
        .iter()
        .filter(|(type_name, _)| !is_null_type(type_name.as_str()))
        .map(|(type_name, _)| type_name.as_str())
        .collect::<Vec<_>>();
    if id_non_null.len() != 1 || matches!(id_non_null[0], "Object" | "Array") {
        return None;
    }

    let (field_name, field_schema) = schema
        .object
        .iter()
        .find(|(name, _)| name.as_str() != "_id")?;
    let non_null = field_schema
        .types
        .iter()
        .filter(|(type_name, _)| !is_null_type(type_name.as_str()))
        .collect::<Vec<_>>();
    if non_null.len() != 1 || non_null[0].0.as_str() != "Array" {
        return None;
    }
    let items_field = non_null[0].1.array.as_ref()?;
    let item_non_null = items_field
        .types
        .iter()
        .filter(|(type_name, _)| !is_null_type(type_name.as_str()))
        .collect::<Vec<_>>();
    if item_non_null.len() != 1 || item_non_null[0].0.as_str() != "Object" {
        return None;
    }
    let item_fields = item_non_null[0].1.object.as_ref()?;
    if item_fields.contains_key("id") {
        return None;
    }

    Some((field_name.as_str(), field_schema))
}

pub fn flattened_root_parent_id_column(table_name: &str) -> String {
    format!("{}_id", sanitize(table_name))
}

/// Returns true if the type string is BSON null or undefined.
pub fn is_null_type(t: &str) -> bool {
    matches!(t, "Null" | "Undefined" | "null" | "undefined")
}

/// Returns the scalar type family for a BSON type name, or None for non-scalar types.
pub fn scalar_type_family(type_name: &str) -> Option<&'static str> {
    match type_name {
        "Null" | "Undefined" | "Object" | "Array" => None,
        "ObjectId" => Some("objectid"),
        "Double" | "Int32" | "Int64" | "Decimal128" | "Number" => Some("numeric"),
        "String" => Some("string"),
        "Boolean" => Some("boolean"),
        "Date" | "Timestamp" => Some("datetime"),
        "Binary" => Some("binary"),
        "RegularExpression" => Some("regex"),
        "JavaScriptCode" | "JavaScriptCodeWithScope" => Some("javascript"),
        "Symbol" => Some("symbol"),
        "DbPointer" => Some("dbpointer"),
        other if other.eq_ignore_ascii_case("undefined") || other.eq_ignore_ascii_case("null") => {
            None
        }
        _ => Some("other"),
    }
}

#[cfg(test)]
mod tests {
    use super::{property_filter_entries_for_collection, read_conf, should_infer_collection};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn read_conf_accepts_datetime_field() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("mongo2pg-util-test-{unique}"));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let config_path = dir.join("dbapi.toml");
        std::fs::write(
            &config_path,
            r#"
[project]
title = "Test Project"
base_dir = "/tmp"
project_dir = "dbapi"

[source]
uri = "mongodb://example"
datetime_field = ["last_update", "*_date"]
"#,
        )
        .expect("write config");

        let conf = read_conf(&config_path).expect("config should parse");
        assert_eq!(conf.timestamp_fields, vec!["last_update", "*_date"]);

        let _ = std::fs::remove_file(&config_path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn read_conf_accepts_legacy_timestamp_field_alias() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("mongo2pg-util-test-{unique}"));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let config_path = dir.join("dbapi.toml");
        std::fs::write(
            &config_path,
            r#"
[project]
title = "Test Project"
base_dir = "/tmp"
project_dir = "dbapi"

[source]
uri = "mongodb://example"
timestamp_field = ["updated_at"]
"#,
        )
        .expect("write config");

        let conf = read_conf(&config_path).expect("config should parse");
        assert_eq!(conf.timestamp_fields, vec!["updated_at"]);

        let _ = std::fs::remove_file(&config_path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn should_infer_collection_accepts_dotted_include_entries() {
        let include = vec!["projects.archived_services".to_owned()];

        assert!(should_infer_collection("projects", &include, &[]));
        assert!(!should_infer_collection("teams", &include, &[]));
    }

    #[test]
    fn should_infer_collection_ignores_dotted_exclude_entries_for_collection_selection() {
        let exclude = vec!["projects.archived_services".to_owned()];

        assert!(should_infer_collection("projects", &[], &exclude));
    }

    #[test]
    fn property_filter_entries_for_collection_returns_only_matching_properties() {
        let entries = vec![
            "projects.archived_services".to_owned(),
            "projects.tags".to_owned(),
            "teams.members".to_owned(),
            "projects".to_owned(),
        ];

        assert_eq!(
            property_filter_entries_for_collection("projects", &entries),
            vec!["archived_services", "tags"]
        );
    }
}
