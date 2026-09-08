//! Cross-cutting helpers shared by two or more command handlers (and, in some
//! cases, by the `kafka-import` binary submodule). This is an INTERIM module:
//! per `openspec/changes/refactor-cli-commands-db-engine-layout/design.md`,
//! this logic will be redistributed into `db/*` and `engine/*` in later
//! phases of the migration. For now it centralizes helpers that are called
//! from more than one `commands::*` handler module.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use bson::Bson;
use bytes::Bytes;
use futures::SinkExt;
use google_cloud_storage::client::{Storage, StorageControl};
use indexmap::IndexMap;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::AsyncReadExt;
use toml::{map::Map as TomlMap, Value as TomlValue};

use crate::db::pg::connect_client as connect_pg_client;
use crate::engine::analyzer::FieldSchema;
use crate::engine::checksum::compute_md5_summaries_for_collection_with_collections_root;
use crate::export::{
    ensure_gcs_authentication, resolve_export_write_backend, resolve_grouped_sql_lookup_name,
    ExportWriteBackend, DEFAULT_EXPORT_CHUNK_ROWS,
};
use crate::report::{
    cluster_from_uri, render_post_import_html, PostImportCollectionRow, PostImportCountDiffRow,
    PostImportMd5Column, PostImportMd5MismatchRow, PostImportMd5Summary, PostImportNode,
    PostImportSnapshotSkipSummary, PostImportTableRow,
};
use crate::schema_diagram::{parse_sql, Table};
use crate::util::{
    configured_project_root, connection_failed_context, is_pg_reserved, read_conf,
    should_infer_collection,
};

// ──────────────────────────────────────────────────────────────────────────────
// Post-import trace/log helpers (used by write_post_import_report + ping/import)
// ──────────────────────────────────────────────────────────────────────────────

pub fn format_post_import_trace_line(
    stage: &str,
    namespace: &str,
    include_md5: bool,
    detail: &str,
) -> String {
    if detail.trim().is_empty() {
        format!("post_import_report stage={stage} namespace={namespace} include_md5={include_md5}")
    } else {
        format!(
            "post_import_report stage={stage} namespace={namespace} include_md5={include_md5} {detail}"
        )
    }
}

pub fn log_post_import_trace(stage: &str, namespace: &str, include_md5: bool, detail: &str) {
    info!(
        "{}",
        format_post_import_trace_line(stage, namespace, include_md5, detail)
    );
}

pub fn debug_post_import_trace(stage: &str, namespace: &str, include_md5: bool, detail: &str) {
    debug!(
        "{}",
        format_post_import_trace_line(stage, namespace, include_md5, detail)
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Config-override helpers (used by to-pg/export/import)
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Default)]
pub struct ConfigOverrides {
    pub project_dir: Option<String>,
    pub source_uri: Option<String>,
    pub namespace: Option<String>,
    pub number: Option<u64>,
    pub percent: Option<f64>,
    pub max_time_ms: Option<u64>,
    pub chunk_size: Option<u64>,
    pub auth_retry_max: Option<u32>,
    pub jsonb: Option<bool>,
    pub target_database_name: Option<String>,
    pub target_schema_name: Option<String>,
    pub kafka_topics: Option<Vec<String>>,
    pub kafka_max_messages: Option<usize>,
    pub kafka_offset: Option<String>,
    pub kafka_group_id: Option<String>,
    pub kafka_topic_prefix: Option<String>,
}

pub fn ensure_toml_table<'a>(
    doc: &'a mut TomlValue,
    key: &str,
) -> Result<&'a mut TomlMap<String, TomlValue>> {
    if !doc.is_table() {
        *doc = TomlValue::Table(TomlMap::new());
    }

    let table = doc
        .as_table_mut()
        .ok_or_else(|| anyhow!("Config root is not a TOML table"))?;

    if !table.contains_key(key) || !table.get(key).is_some_and(TomlValue::is_table) {
        table.insert(key.to_owned(), TomlValue::Table(TomlMap::new()));
    }

    table
        .get_mut(key)
        .and_then(TomlValue::as_table_mut)
        .ok_or_else(|| anyhow!("Failed to access TOML section [{key}]"))
}

pub fn apply_config_overrides(conf_path: &Path, overrides: &ConfigOverrides) -> Result<()> {
    let has_overrides = overrides.source_uri.is_some()
        || overrides.project_dir.is_some()
        || overrides.namespace.is_some()
        || overrides.number.is_some()
        || overrides.percent.is_some()
        || overrides.max_time_ms.is_some()
        || overrides.chunk_size.is_some()
        || overrides.auth_retry_max.is_some()
        || overrides.jsonb.is_some()
        || overrides.target_database_name.is_some()
        || overrides.target_schema_name.is_some()
        || overrides.kafka_topics.is_some()
        || overrides.kafka_max_messages.is_some()
        || overrides.kafka_offset.is_some()
        || overrides.kafka_group_id.is_some()
        || overrides.kafka_topic_prefix.is_some();

    if !has_overrides {
        return Ok(());
    }

    let raw = std::fs::read_to_string(conf_path)
        .with_context(|| format!("Failed to read config file {}", conf_path.display()))?;
    let mut doc: TomlValue = toml::from_str(&raw)
        .with_context(|| format!("Failed to parse TOML config {}", conf_path.display()))?;

    {
        let project = ensure_toml_table(&mut doc, "project")?;
        if let Some(v) = &overrides.project_dir {
            project.insert("project_dir".to_owned(), TomlValue::String(v.clone()));
        }
    }

    {
        let source = ensure_toml_table(&mut doc, "source")?;
        if let Some(v) = &overrides.source_uri {
            source.insert("uri".to_owned(), TomlValue::String(v.clone()));
        }
        if let Some(v) = &overrides.namespace {
            source.insert("namespace".to_owned(), TomlValue::String(v.clone()));
        }
        if let Some(v) = overrides.number {
            source.insert("number".to_owned(), TomlValue::Integer(v as i64));
        }
        if let Some(v) = overrides.percent {
            source.insert("percent".to_owned(), TomlValue::Float(v));
        }
        if let Some(v) = overrides.max_time_ms {
            source.insert("max_time_ms".to_owned(), TomlValue::Integer(v as i64));
        }
        if let Some(v) = overrides.chunk_size {
            source.insert("chunk_size".to_owned(), TomlValue::Integer(v as i64));
        }
        if let Some(v) = overrides.auth_retry_max {
            source.insert("auth_retry_max".to_owned(), TomlValue::Integer(v as i64));
        }
        if let Some(v) = overrides.jsonb {
            source.insert("jsonb".to_owned(), TomlValue::Boolean(v));
        }
    }

    {
        let target = ensure_toml_table(&mut doc, "target")?;
        if let Some(v) = &overrides.target_database_name {
            target.insert("database_name".to_owned(), TomlValue::String(v.clone()));
        }
        if let Some(v) = &overrides.target_schema_name {
            target.insert("schema_name".to_owned(), TomlValue::String(v.clone()));
        }
    }

    {
        let kafka = ensure_toml_table(&mut doc, "kafka")?;

        if let Some(v) = &overrides.kafka_group_id {
            kafka.insert("group_id".to_owned(), TomlValue::String(v.clone()));
        }
        if let Some(v) = &overrides.kafka_topic_prefix {
            kafka.insert("topic_prefix".to_owned(), TomlValue::String(v.clone()));
        }

        if let Some(v) = &overrides.kafka_topics {
            kafka.insert(
                "topics".to_owned(),
                TomlValue::Array(v.iter().cloned().map(TomlValue::String).collect()),
            );
        }
        if let Some(v) = overrides.kafka_max_messages {
            kafka.insert("max_messages".to_owned(), TomlValue::Integer(v as i64));
        }
        if let Some(v) = &overrides.kafka_offset {
            kafka.insert("offset".to_owned(), TomlValue::String(v.clone()));
            kafka.insert("auto_offset_reset".to_owned(), TomlValue::String(v.clone()));
        }
    }

    let updated = toml::to_string_pretty(&doc)
        .with_context(|| format!("Failed to render TOML config {}", conf_path.display()))?;
    std::fs::write(conf_path, updated)
        .with_context(|| format!("Failed to write config file {}", conf_path.display()))?;

    Ok(())
}

pub fn resolve_export_chunk_size(chunk_size: Option<u64>) -> Result<u64> {
    let resolved = chunk_size.unwrap_or(DEFAULT_EXPORT_CHUNK_ROWS);
    if resolved == 0 {
        return Err(anyhow!("chunk_size must be greater than 0"));
    }
    if resolved > i64::MAX as u64 {
        return Err(anyhow!("chunk_size must be <= {}", i64::MAX));
    }
    Ok(resolved)
}

// ──────────────────────────────────────────────────────────────────────────────
// DDL/mapping type system (used by to-pg, infer, and kafka-import for YAML
// (de)serialization)
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MappingColumn {
    pub source_field: String,
    pub target_field: String,
    pub data_type: String,
    pub nullable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub literal_value: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DdlColumnMapping {
    pub name: String,
    #[serde(rename = "sql_type", alias = "pg_type")]
    pub sql_type: String,
    pub nullable: bool,
    pub primary_key: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DdlForeignKeyMapping {
    pub from_col: String,
    pub to_table: String,
    pub to_col: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DdlTableMapping {
    pub name: String,
    pub columns: Vec<DdlColumnMapping>,
    #[serde(default)]
    pub foreign_keys: Vec<DdlForeignKeyMapping>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DdlEditingGuidance {
    pub safe_to_edit: Vec<String>,
    pub notes: Vec<String>,
}

pub fn default_ddl_editing_guidance() -> DdlEditingGuidance {
    DdlEditingGuidance {
        safe_to_edit: vec![
            "pg_mapping.ddl.columns[].sql_type".to_owned(),
            "pg_mapping.ddl.columns[].nullable".to_owned(),
            "pg_mapping.ddl.columns[].primary_key".to_owned(),
            "pg_mapping.ddl.foreign_keys[]".to_owned(),
        ],
        notes: vec![
            "Edit the ddl section, then rerun to-pg to regenerate the SQL file.".to_owned(),
            "Keep pg_mapping.columns aligned with source-to-target mappings for export and md5."
                .to_owned(),
            "Export reads the generated SQL files, so mapping edits take effect after to-pg."
                .to_owned(),
        ],
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PgMapping {
    pub dbname: String,
    pub schema_name: String,
    pub table_name: String,
    pub columns: Vec<MappingColumn>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ddl: Option<DdlTableMapping>,
    #[serde(default = "default_ddl_editing_guidance")]
    pub ddl_editing: DdlEditingGuidance,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CollectionMapping {
    pub collection_name: String,
    #[serde(alias = "dbname")]
    pub mongo_dbname: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mongo_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traversal: Option<TraversalPlan>,
    pub pg_mapping: PgMapping,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraversalMode {
    Root,
    Object,
    ArrayObject,
    ArrayScalar,
    MapObject,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TraversalPlan {
    pub mode: TraversalMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_table: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_field: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fk_column: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_column: Option<String>,
}

pub fn sanitize_pg_name(name: &str) -> String {
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
    if s.starts_with(|c: char| c.is_ascii_digit()) {
        format!("_{s}")
    } else {
        s
    }
}

pub fn normalize_pg_identifier(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        trimmed[1..trimmed.len() - 1].replace("\"\"", "\"")
    } else {
        trimmed.to_owned()
    }
}

pub fn ddl_table_mapping_from_table(table: &Table) -> DdlTableMapping {
    DdlTableMapping {
        name: table.name.clone(),
        columns: table
            .columns
            .iter()
            .map(|column| DdlColumnMapping {
                name: column.name.clone(),
                sql_type: column.col_type.clone(),
                nullable: !column.not_null,
                primary_key: column.primary_key,
            })
            .collect(),
        foreign_keys: table
            .foreign_keys
            .iter()
            .map(|fk| DdlForeignKeyMapping {
                from_col: fk.from_col.clone(),
                to_table: fk.to_table.clone(),
                to_col: fk.to_col.clone(),
            })
            .collect(),
    }
}

pub fn render_ddl_from_mapping_tables(
    tables: &[DdlTableMapping],
    schema_name: Option<&str>,
) -> String {
    render_ddl_from_mapping_tables_with_owner(tables, schema_name, None)
}

pub fn render_ddl_from_mapping_tables_with_owner(
    tables: &[DdlTableMapping],
    schema_name: Option<&str>,
    schema_owner: Option<&str>,
) -> String {
    fn quote_ident_always(ident: &str) -> String {
        format!("\"{}\"", ident.replace('"', "\"\""))
    }

    fn is_standalone_index_pseudo_column(column: &DdlColumnMapping) -> bool {
        column.name.eq_ignore_ascii_case("create")
            && column
                .sql_type
                .trim_start()
                .to_ascii_uppercase()
                .starts_with("INDEX IF NOT EXISTS")
    }

    fn rendered_sql_type(sql_type: &str) -> &str {
        if sql_type.eq_ignore_ascii_case("VARCHAR(0)") {
            "TEXT"
        } else {
            sql_type
        }
    }

    fn maybe_quote_ident(ident: &str) -> String {
        if is_pg_reserved(ident) {
            quote_ident(ident)
        } else {
            ident.to_owned()
        }
    }

    fn ordered_tables(tables: &[DdlTableMapping]) -> Vec<&DdlTableMapping> {
        let by_name = tables
            .iter()
            .map(|table| (table.name.as_str(), table))
            .collect::<std::collections::HashMap<_, _>>();
        let mut visited = std::collections::HashSet::new();
        let mut visiting = std::collections::HashSet::new();
        let mut ordered = Vec::with_capacity(tables.len());

        fn visit<'a>(
            table: &'a DdlTableMapping,
            by_name: &std::collections::HashMap<&'a str, &'a DdlTableMapping>,
            visited: &mut std::collections::HashSet<&'a str>,
            visiting: &mut std::collections::HashSet<&'a str>,
            ordered: &mut Vec<&'a DdlTableMapping>,
        ) {
            if visited.contains(table.name.as_str()) {
                return;
            }
            if !visiting.insert(table.name.as_str()) {
                return;
            }

            for fk in &table.foreign_keys {
                if let Some(parent) = by_name.get(fk.to_table.as_str()) {
                    visit(parent, by_name, visited, visiting, ordered);
                }
            }

            visiting.remove(table.name.as_str());
            visited.insert(table.name.as_str());
            ordered.push(table);
        }

        for table in tables {
            visit(table, &by_name, &mut visited, &mut visiting, &mut ordered);
        }

        ordered
    }

    fn fk_columns(from_col: &str) -> Vec<String> {
        from_col
            .split(',')
            .map(str::trim)
            .filter(|col| !col.is_empty())
            .map(str::to_owned)
            .collect()
    }

    fn fk_index_statements(table: &DdlTableMapping, schema_name: Option<&str>) -> Vec<String> {
        let mut seen = std::collections::HashSet::new();
        let mut statements = Vec::new();

        for fk in &table.foreign_keys {
            let cols = fk_columns(&fk.from_col);
            if cols.is_empty() {
                continue;
            }
            let dedupe_key = cols.join("\0");
            if !seen.insert(dedupe_key) {
                continue;
            }

            let index_name = format!("idx_{}_{}", table.name, cols.join("_"));
            let qualified_table = match schema_name {
                Some(schema) => format!("{}.{}", quote_ident(schema), quote_ident(&table.name)),
                None => quote_ident(&table.name),
            };
            let quoted_cols = cols
                .iter()
                .map(|col| quote_ident(col))
                .collect::<Vec<_>>()
                .join(", ");

            statements.push(format!(
                "CREATE INDEX IF NOT EXISTS {} ON {} ({});",
                quote_ident(&index_name),
                qualified_table,
                quoted_cols
            ));
        }

        statements
    }

    fn grouped_key_index_statements(
        table: &DdlTableMapping,
        schema_name: Option<&str>,
    ) -> Vec<String> {
        let has_grouped_key_column = table
            .columns
            .iter()
            .any(|column| column.name.eq_ignore_ascii_case("_key"));
        if !has_grouped_key_column {
            return Vec::new();
        }

        let index_name = format!("idx_{}_key", table.name);
        let qualified_table = match schema_name {
            Some(schema) => format!("{}.{}", quote_ident(schema), quote_ident(&table.name)),
            None => quote_ident(&table.name),
        };

        vec![format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} ({});",
            quote_ident(&index_name),
            qualified_table,
            quote_ident("_key")
        )]
    }

    let mut ddl_body = String::new();

    if let Some(schema) = schema_name {
        ddl_body.push_str(&format!(
            "CREATE SCHEMA IF NOT EXISTS {};\n",
            quote_ident(schema)
        ));
        if let Some(owner) = schema_owner
            .map(str::trim)
            .filter(|owner| !owner.is_empty())
        {
            ddl_body.push_str(&format!(
                "ALTER SCHEMA {} OWNER TO {};\n",
                quote_ident_always(schema),
                quote_ident_always(owner)
            ));
        }
        ddl_body.push_str(&format!(
            "SET search_path = {}, public;\n\n",
            quote_ident(schema)
        ));
    }

    for table in ordered_tables(tables) {
        ddl_body.push_str(&format!(
            "CREATE TABLE {} (\n",
            maybe_quote_ident(&table.name)
        ));

        let rendered_columns = table
            .columns
            .iter()
            .filter(|column| !is_standalone_index_pseudo_column(column))
            .collect::<Vec<_>>();

        let primary_keys = rendered_columns
            .iter()
            .filter(|column| column.primary_key)
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();

        let mut lines = rendered_columns
            .iter()
            .map(|column| {
                let mut line = format!(
                    "    {} {}",
                    maybe_quote_ident(&column.name),
                    rendered_sql_type(&column.sql_type)
                );
                if column.primary_key && column.sql_type.eq_ignore_ascii_case("uuid") {
                    line.push_str(" DEFAULT public.gen_random_uuid()");
                }
                if primary_keys.len() == 1 && column.primary_key {
                    line.push_str(" PRIMARY KEY");
                }
                if !column.nullable && !(primary_keys.len() == 1 && column.primary_key) {
                    line.push_str(" NOT NULL");
                }
                line
            })
            .collect::<Vec<_>>();

        if primary_keys.len() > 1 {
            lines.push(format!(
                "    PRIMARY KEY ({})",
                primary_keys
                    .iter()
                    .map(|column| maybe_quote_ident(column))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }

        lines.extend(table.foreign_keys.iter().map(|fk| {
            let fk_from_cols = fk
                .from_col
                .split(',')
                .map(str::trim)
                .filter(|col| !col.is_empty())
                .map(maybe_quote_ident)
                .collect::<Vec<_>>()
                .join(", ");
            let fk_to_cols = fk
                .to_col
                .split(',')
                .map(str::trim)
                .filter(|col| !col.is_empty())
                .map(maybe_quote_ident)
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "    FOREIGN KEY ({}) REFERENCES {} ({}) DEFERRABLE INITIALLY DEFERRED",
                fk_from_cols,
                maybe_quote_ident(&fk.to_table),
                fk_to_cols
            )
        }));

        ddl_body.push_str(&lines.join(",\n"));
        ddl_body.push_str("\n);\n\n");

        let fk_indexes = fk_index_statements(table, schema_name);
        if !fk_indexes.is_empty() {
            ddl_body.push_str(&fk_indexes.join("\n"));
            ddl_body.push_str("\n\n");
        }

        let grouped_key_indexes = grouped_key_index_statements(table, schema_name);
        if !grouped_key_indexes.is_empty() {
            ddl_body.push_str(&grouped_key_indexes.join("\n"));
            ddl_body.push_str("\n\n");
        }
    }

    let needs_pgcrypto = ddl_body.contains("public.gen_random_uuid()");
    let needs_postgis = ddl_body.to_ascii_lowercase().contains("geometry(");

    let mut ddl = String::new();
    if needs_pgcrypto {
        ddl.push_str("CREATE EXTENSION IF NOT EXISTS \"pgcrypto\";\n");
    }
    if needs_postgis {
        ddl.push_str("CREATE EXTENSION IF NOT EXISTS postgis;\n");
    }
    if needs_pgcrypto || needs_postgis {
        ddl.push('\n');
    }

    ddl.push_str(&ddl_body);

    ddl.trim_end().to_owned() + "\n"
}

pub fn load_mapping_ddl_tables(collection_dir: &Path) -> Result<Option<Vec<DdlTableMapping>>> {
    fn is_standalone_index_pseudo_column(column: &DdlColumnMapping) -> bool {
        column.name.eq_ignore_ascii_case("create")
            && column
                .sql_type
                .trim_start()
                .to_ascii_uppercase()
                .starts_with("INDEX IF NOT EXISTS")
    }

    let mut mapping_paths = std::fs::read_dir(collection_dir)
        .with_context(|| format!("Cannot read {}", collection_dir.display()))?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("mapping_") && name.ends_with(".yaml"))
        })
        .collect::<Vec<_>>();

    if mapping_paths.is_empty() {
        return Ok(None);
    }

    mapping_paths.sort();
    let mut tables = Vec::new();
    for path in mapping_paths {
        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("Failed to read {}", path.display()))?;
        let mapping: CollectionMapping = serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse {}", path.display()))?;
        let Some(mut ddl) = mapping.pg_mapping.ddl else {
            return Ok(None);
        };
        ddl.columns
            .retain(|column| !is_standalone_index_pseudo_column(column));

        if mapping.pg_mapping.columns.is_empty() {
            let fk_columns = ddl
                .foreign_keys
                .iter()
                .flat_map(|fk| fk.from_col.split(','))
                .map(str::trim)
                .filter(|col| !col.is_empty())
                .collect::<std::collections::HashSet<_>>();

            for column in &mut ddl.columns {
                if !column.primary_key && !fk_columns.contains(column.name.as_str()) {
                    column.nullable = true;
                }
            }
        }
        tables.push(ddl);
    }

    Ok(Some(tables))
}

// ──────────────────────────────────────────────────────────────────────────────
// Output-path / GCS staging helpers (used by init/export/import/report/kafka-import)
// ──────────────────────────────────────────────────────────────────────────────

pub fn append_non_empty_segment(path: &mut String, segment: Option<&str>) {
    if let Some(value) = segment.map(str::trim).filter(|value| !value.is_empty()) {
        if !path.is_empty() {
            path.push('/');
        }
        path.push_str(value);
    }
}

pub fn ensure_output_prefix_segments(
    prefix: &str,
    cluster_name: Option<&str>,
    project_dir: &str,
) -> String {
    let mut normalized = prefix.trim_matches('/').to_owned();

    let project_segment = project_dir.trim_matches('/');
    if !project_segment.is_empty() {
        let already_has_project = normalized
            .rsplit('/')
            .next()
            .is_some_and(|segment| segment == project_segment);
        if !already_has_project {
            append_non_empty_segment(&mut normalized, Some(project_segment));
        }
    }

    let cluster_segment = cluster_name
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if let Some(cluster) = cluster_segment {
        let already_has_cluster = normalized
            .rsplit('/')
            .next()
            .is_some_and(|segment| segment == cluster);
        if !already_has_cluster {
            append_non_empty_segment(&mut normalized, Some(cluster));
        }
    }

    normalized
}

pub fn import_table_name_from_csv_path(path: &Path) -> Option<String> {
    let file_name = path.file_name()?.to_str()?;
    if let Some(table) = file_name.strip_suffix(".csv.gz") {
        return Some(table.to_owned());
    }
    file_name.strip_suffix(".csv").map(str::to_owned)
}

pub fn is_supported_import_csv_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(".csv.gz") || name.ends_with(".csv"))
}

pub fn build_missing_import_csv_error(
    data_db_dir: &Path,
    requested_collection_dir: Option<&str>,
    allowed_table_count: usize,
) -> anyhow::Error {
    let scope_hint = requested_collection_dir
        .map(|collection| format!(" for requested collection '{collection}'"))
        .unwrap_or_default();

    let table_hint = if allowed_table_count == 0 {
        "No importable SQL tables were discovered from schema files.".to_owned()
    } else {
        format!("Found {allowed_table_count} SQL table(s), but no matching data files.",)
    };

    anyhow!(
        "No .csv or .csv.gz files found in {}{} that match generated SQL tables. {} \
Expected layout: data/<db>/<collection>/<table>.csv.gz (or .csv). \
Run `mongo2pg export -c <config>` first, or verify --namespace / [COLLECTION] filters.",
        data_db_dir.display(),
        scope_hint,
        table_hint,
    )
}

pub fn resolve_collections_dir(project_root: &Path, db_name: &str) -> PathBuf {
    let collections_root = project_root.join("source").join("collections");
    if collections_root.join(db_name).is_dir() {
        collections_root.join(db_name)
    } else {
        collections_root
    }
}

pub fn resolve_local_project_root_from_config(
    conf_path: &Path,
    conf_data: &crate::util::ConfData,
) -> PathBuf {
    let storage_backend =
        resolve_export_write_backend(&conf_data.base_dir).unwrap_or(ExportWriteBackend::LocalFs);

    match storage_backend {
        ExportWriteBackend::LocalFs => configured_project_root(conf_data),
        ExportWriteBackend::Gcs { prefix, .. } => {
            let current_dir = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
            let conf_abs = std::fs::canonicalize(conf_path).unwrap_or_else(|_| {
                if conf_path.is_absolute() {
                    conf_path.to_path_buf()
                } else {
                    current_dir.join(conf_path)
                }
            });

            let from_config = conf_abs
                .parent()
                .and_then(|p| p.parent())
                .map(|p| p.to_path_buf());
            let from_cwd_project = if let Some(cluster_name) = conf_data
                .cluster_name
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                current_dir.join(&conf_data.project_dir).join(cluster_name)
            } else {
                current_dir.join(&conf_data.project_dir)
            };
            let from_config_parent_with_prefix = from_config.as_ref().and_then(|root| {
                root.parent().map(|p| {
                    let effective_prefix = ensure_output_prefix_segments(
                        &prefix,
                        conf_data.cluster_name.as_deref(),
                        &conf_data.project_dir,
                    );
                    p.join(effective_prefix)
                })
            });

            let has_project_layout = |root: &PathBuf| {
                root.join("source").is_dir()
                    || root.join("schema").is_dir()
                    || root.join("reports").is_dir()
            };

            from_config_parent_with_prefix
                .iter()
                .chain(from_config.iter())
                .chain(std::iter::once(&from_cwd_project))
                .find(|root| has_project_layout(root))
                .cloned()
                .or(from_config_parent_with_prefix)
                .or(from_config)
                .unwrap_or(from_cwd_project)
        }
    }
}

pub fn gcs_prefix_candidates_for_project_subdir(
    prefix: &str,
    cluster_name: Option<&str>,
    project_dir: &str,
    subdir: &str,
) -> Vec<String> {
    let trimmed_prefix = prefix.trim_matches('/');
    let cluster_segment = cluster_name
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let project_segment = project_dir.trim_matches('/');
    let subdir_segment = subdir.trim_matches('/');
    let mut candidates = Vec::new();

    let effective_prefix = ensure_output_prefix_segments(prefix, cluster_name, project_dir);
    if !effective_prefix.is_empty() {
        candidates.push(format!("{effective_prefix}/{subdir_segment}/"));
    }

    if trimmed_prefix.is_empty() {
        candidates.push(format!("{subdir_segment}/"));
        if let Some(cluster_name) = cluster_segment {
            candidates.push(format!("{cluster_name}/{subdir_segment}/"));
        }
        if !project_segment.is_empty() {
            candidates.push(format!("{project_segment}/{subdir_segment}/"));
            if let Some(cluster_name) = cluster_segment {
                candidates.push(format!(
                    "{project_segment}/{cluster_name}/{subdir_segment}/"
                ));
            }
        }
    } else {
        candidates.push(format!("{trimmed_prefix}/{subdir_segment}/"));
        if let Some(cluster_name) = cluster_segment {
            candidates.push(format!("{trimmed_prefix}/{cluster_name}/{subdir_segment}/"));
        }
        if !project_segment.is_empty() {
            let ends_with_project = trimmed_prefix
                .split('/')
                .next_back()
                .is_some_and(|last| last == project_segment);
            if !ends_with_project {
                candidates.push(format!(
                    "{trimmed_prefix}/{project_segment}/{subdir_segment}/"
                ));
            }
        }
    }

    candidates.sort();
    candidates.dedup();
    candidates
}

pub async fn download_gcs_prefix_to_local_dir(
    bucket: &str,
    object_prefix: &str,
    local_dir: &Path,
) -> Result<usize> {
    debug!(
        "[gcs] metadata stage start: gs://{}/{} -> {}",
        bucket,
        object_prefix,
        local_dir.display()
    );
    let storage = Storage::builder()
        .build()
        .await
        .with_context(|| format!("Failed to initialize GCS data client for gs://{bucket}"))?;
    let storage_control = StorageControl::builder()
        .build()
        .await
        .with_context(|| format!("Failed to initialize GCS control client for gs://{bucket}"))?;
    let bucket_resource = format!("projects/_/buckets/{bucket}");

    let mut downloaded = 0usize;
    let mut page_token = String::new();
    loop {
        let mut request = storage_control
            .list_objects()
            .set_parent(bucket_resource.clone())
            .set_prefix(object_prefix.to_owned());
        if !page_token.is_empty() {
            request = request.set_page_token(page_token.clone());
        }

        let page = request
            .send()
            .await
            .with_context(|| format!("Failed while listing gs://{}/{}", bucket, object_prefix))?;

        for object in page.objects {
            let Some(relative) = object
                .name
                .strip_prefix(object_prefix)
                .map(|path| path.trim_start_matches('/'))
            else {
                continue;
            };
            if relative.is_empty() || relative.ends_with('/') || object.name.ends_with('/') {
                continue;
            }

            let destination = local_dir.join(relative);
            if let Some(parent) = destination.parent() {
                tokio::fs::create_dir_all(parent).await.with_context(|| {
                    format!("Cannot create local staging directory {}", parent.display())
                })?;
            }

            let mut reader = storage
                .read_object(bucket_resource.clone(), object.name.clone())
                .send()
                .await
                .with_context(|| format!("Failed to open gs://{}/{}", bucket, object.name))?;
            let mut bytes = Vec::new();
            while let Some(chunk) = reader.next().await {
                let chunk = chunk
                    .with_context(|| format!("Failed reading gs://{}/{}", bucket, object.name))?;
                bytes.extend_from_slice(&chunk);
            }

            tokio::fs::write(&destination, bytes)
                .await
                .with_context(|| {
                    format!("Cannot write staged import file {}", destination.display())
                })?;
            downloaded += 1;
        }

        if page.next_page_token.is_empty() {
            break;
        }
        page_token = page.next_page_token;
    }

    debug!(
        "[gcs] metadata stage done: downloaded_files={} from gs://{}/{}",
        downloaded, bucket, object_prefix
    );

    Ok(downloaded)
}

pub fn parse_gcs_uri(uri: &str) -> Result<Option<(String, String)>> {
    let raw = uri.trim();
    let Some(remainder) = raw
        .strip_prefix("gs://")
        .or_else(|| raw.strip_prefix("gs:/"))
    else {
        return Ok(None);
    };

    let trimmed = remainder.trim_matches('/');
    let (bucket, object) = trimmed
        .split_once('/')
        .map_or((trimmed, ""), |(bucket, object)| (bucket, object));

    if bucket.is_empty() {
        return Err(anyhow!(
            "Invalid config URI '{}': missing bucket name after gs://",
            uri
        ));
    }
    if object.is_empty() {
        return Err(anyhow!(
            "Invalid config URI '{}': missing object path after bucket name",
            uri
        ));
    }

    Ok(Some((bucket.to_owned(), object.to_owned())))
}

pub async fn download_gcs_object_bytes(bucket: &str, object: &str) -> Result<Vec<u8>> {
    ensure_gcs_authentication().await?;

    let storage = Storage::builder()
        .build()
        .await
        .with_context(|| format!("Failed to initialize GCS client for gs://{bucket}/{object}"))?;

    let bucket_resource = format!("projects/_/buckets/{bucket}");
    let mut reader = storage
        .read_object(bucket_resource, object.to_owned())
        .send()
        .await
        .with_context(|| format!("Failed to open gs://{bucket}/{object}"))?;

    let mut bytes = Vec::new();
    while let Some(chunk) = reader.next().await {
        let chunk = chunk.with_context(|| format!("Failed reading gs://{bucket}/{object}"))?;
        bytes.extend_from_slice(&chunk);
    }

    Ok(bytes)
}

pub async fn stage_config_path_if_gcs(
    config_path: PathBuf,
) -> Result<(PathBuf, Option<tempfile::TempDir>)> {
    let raw = config_path.to_string_lossy().to_string();
    let Some((bucket, object)) = parse_gcs_uri(&raw)? else {
        return Ok((config_path, None));
    };

    // Guard against malformed values where a local path is accidentally appended
    // after the config object path (for example: gs://.../config.toml/tmp/... ).
    let object = [".toml/", ".yaml/", ".yml/"]
        .iter()
        .find_map(|marker| {
            object
                .find(marker)
                .map(|idx| object[..idx + marker.len() - 1].to_owned())
        })
        .unwrap_or(object);

    let bytes = download_gcs_object_bytes(&bucket, &object).await?;
    let stage = tempfile::Builder::new()
        .prefix("mongo2pg-gcs-config-")
        .tempdir()
        .context("Cannot create temporary config staging directory")?;

    let file_name = Path::new(&object)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.trim().is_empty())
        .unwrap_or("config.toml");
    let local_path = stage.path().join(file_name);

    tokio::fs::write(&local_path, bytes)
        .await
        .with_context(|| format!("Failed to stage config file {}", local_path.display()))?;

    info!(
        "staged config file from gs://{}/{} into {}",
        bucket,
        object,
        local_path.display()
    );

    Ok((local_path, Some(stage)))
}

pub async fn stage_export_metadata_from_gcs(
    bucket: &str,
    prefix: &str,
    cluster_name: Option<&str>,
    project_dir: &str,
    db_name: &str,
) -> Result<Option<tempfile::TempDir>> {
    ensure_gcs_authentication().await?;

    let stage = tempfile::Builder::new()
        .prefix("mongo2pg-gcs-export-meta-")
        .tempdir()
        .context("Cannot create temporary export metadata staging directory")?;

    let staged_tables_dir = stage.path().join("schema").join("tables").join(db_name);
    std::fs::create_dir_all(&staged_tables_dir).with_context(|| {
        format!(
            "Cannot create staged schema directory {}",
            staged_tables_dir.display()
        )
    })?;

    let mut staged_tables = 0usize;
    for candidate in gcs_prefix_candidates_for_project_subdir(
        prefix,
        cluster_name,
        project_dir,
        &format!("schema/tables/{db_name}"),
    ) {
        let count = download_gcs_prefix_to_local_dir(bucket, &candidate, &staged_tables_dir)
            .await
            .with_context(|| {
                format!(
                    "Failed to stage schema metadata from gs://{}/{}",
                    bucket, candidate
                )
            })?;
        if count > 0 {
            staged_tables = count;
            info!(
                "staged {} schema files from gs://{}/{} into {}",
                count,
                bucket,
                candidate,
                staged_tables_dir.display()
            );
            break;
        }
    }

    if staged_tables == 0 {
        return Ok(None);
    }

    let staged_collections_root = stage.path().join("source").join("collections");
    std::fs::create_dir_all(&staged_collections_root).with_context(|| {
        format!(
            "Cannot create staged collections directory {}",
            staged_collections_root.display()
        )
    })?;

    let collection_candidates = [
        format!("source/collections/{db_name}"),
        "source/collections".to_owned(),
    ];
    for subdir in collection_candidates {
        for candidate in
            gcs_prefix_candidates_for_project_subdir(prefix, cluster_name, project_dir, &subdir)
        {
            let target_dir = if subdir.ends_with(&format!("/{db_name}")) {
                staged_collections_root.join(db_name)
            } else {
                staged_collections_root.clone()
            };
            let count = download_gcs_prefix_to_local_dir(bucket, &candidate, &target_dir)
                .await
                .with_context(|| {
                    format!(
                        "Failed to stage collection mappings from gs://{}/{}",
                        bucket, candidate
                    )
                })?;
            if count > 0 {
                info!(
                    "staged {} collection mapping files from gs://{}/{} into {}",
                    count,
                    bucket,
                    candidate,
                    target_dir.display()
                );
                break;
            }
        }
    }

    Ok(Some(stage))
}

pub async fn stage_source_collections_from_gcs(
    bucket: &str,
    prefix: &str,
    cluster_name: Option<&str>,
    project_dir: &str,
) -> Result<tempfile::TempDir> {
    ensure_gcs_authentication().await?;
    let stage = tempfile::Builder::new()
        .prefix("mongo2pg-gcs-collections-")
        .tempdir()
        .context("Cannot create temporary source staging directory")?;
    let target_dir = stage.path().join("source").join("collections");
    std::fs::create_dir_all(&target_dir).with_context(|| {
        format!(
            "Cannot create staged collections directory {}",
            target_dir.display()
        )
    })?;

    for candidate in gcs_prefix_candidates_for_project_subdir(
        prefix,
        cluster_name,
        project_dir,
        "source/collections",
    ) {
        let downloaded = download_gcs_prefix_to_local_dir(bucket, &candidate, &target_dir).await?;
        if downloaded > 0 {
            info!(
                "staged {} collection files from gs://{}/{} into {}",
                downloaded,
                bucket,
                candidate,
                target_dir.display()
            );
            return Ok(stage);
        }
    }

    Err(anyhow!(
        "Cannot find source collections under gs://{}/{}",
        bucket,
        prefix.trim_matches('/')
    ))
}

pub async fn stage_report_metadata_from_gcs(
    bucket: &str,
    prefix: &str,
    cluster_name: Option<&str>,
    project_dir: &str,
    db_name: &str,
) -> Result<tempfile::TempDir> {
    let stage =
        stage_source_collections_from_gcs(bucket, prefix, cluster_name, project_dir).await?;
    let tables_dir = stage.path().join("schema").join("tables").join(db_name);
    std::fs::create_dir_all(&tables_dir).with_context(|| {
        format!(
            "Cannot create staged schema directory {}",
            tables_dir.display()
        )
    })?;

    for candidate in gcs_prefix_candidates_for_project_subdir(
        prefix,
        cluster_name,
        project_dir,
        &format!("schema/tables/{db_name}"),
    ) {
        let downloaded = download_gcs_prefix_to_local_dir(bucket, &candidate, &tables_dir).await?;
        if downloaded > 0 {
            info!(
                "staged {} schema files from gs://{}/{} into {}",
                downloaded,
                bucket,
                candidate,
                tables_dir.display()
            );
            break;
        }
    }

    Ok(stage)
}

// ──────────────────────────────────────────────────────────────────────────────
// PostgreSQL name/URI/connection helpers (used by to-pg/export/import/ping/
// report/cluster-report and kafka-import)
// ──────────────────────────────────────────────────────────────────────────────

pub fn parse_namespace(ns: &str) -> Result<(&str, &str)> {
    let dot = ns
        .find('.')
        .ok_or_else(|| anyhow!("Namespace must be in the form <db>.<collection>, got: {ns}"))?;
    Ok((&ns[..dot], &ns[dot + 1..]))
}

pub fn sanitize_name(name: &str) -> String {
    let mut out = String::new();
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    while out.contains("__") {
        out = out.replace("__", "_");
    }
    out.trim_matches('_').to_owned()
}

pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub fn split_namespace_scope(namespace: &str) -> (&str, Option<&str>) {
    if let Some((db_name, coll_name)) = namespace.split_once('.') {
        (db_name, Some(coll_name))
    } else {
        (namespace, None)
    }
}

pub fn resolve_target_database_name_from_conf(
    target_database_name: Option<&str>,
    namespace: Option<&str>,
) -> Option<String> {
    target_database_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .or_else(|| {
            namespace
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|ns| split_namespace_scope(ns).0.to_owned())
        })
}

pub fn resolve_preamble_database_name(
    config_db_name: Option<&str>,
    effective_rel_sql: &Path,
) -> Option<String> {
    config_db_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .or_else(|| {
            effective_rel_sql
                .parent()
                .and_then(|p| p.file_name())
                .and_then(|s| s.to_str())
                .map(str::to_owned)
        })
}

pub fn extract_postgres_uri_username(uri: &str) -> Option<String> {
    let authority = uri.split_once("://")?.1.split('/').next()?;
    if !authority.contains('@') {
        return None;
    }
    let userinfo = authority.split('@').next()?;
    let username = userinfo.split(':').next().unwrap_or("").trim();
    if username.is_empty() {
        None
    } else {
        Some(username.to_owned())
    }
}

pub fn extract_search_path(sql: &str) -> Option<String> {
    sql.lines().find_map(|line| {
        let trimmed = line.trim();
        trimmed.strip_prefix("SET search_path = ").map(|rest| {
            let first_entry = rest
                .trim()
                .trim_end_matches(';')
                .split(',')
                .next()
                .unwrap_or("")
                .trim();
            normalize_pg_identifier(first_entry)
        })
    })
}

pub fn extract_psql_database_name(sql: &str) -> Option<String> {
    for line in sql.lines() {
        let trimmed = line.trim();

        if let Some(rest) = trimmed.strip_prefix("CREATE DATABASE ") {
            let ident = rest.trim().trim_end_matches(';').trim();
            if !ident.is_empty() {
                return Some(normalize_pg_identifier(ident));
            }
        }

        if let Some(rest) = trimmed.strip_prefix("\\connect ") {
            let ident = rest
                .trim()
                .trim_end_matches(';')
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim();
            if !ident.is_empty() {
                return Some(normalize_pg_identifier(ident));
            }
        }
    }

    None
}

pub fn strip_psql_preamble(sql: &str) -> String {
    sql.lines()
        .filter(|line| {
            let trimmed = line.trim_start();
            !trimmed.starts_with("DROP DATABASE ")
                && !trimmed.starts_with("CREATE DATABASE ")
                && !trimmed.starts_with("\\connect ")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn strip_postgis_extension_statement(sql: &str) -> String {
    sql.lines()
        .filter(|line| {
            let normalized = line.trim().to_ascii_lowercase();
            normalized != "create extension if not exists postgis;"
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn is_missing_postgis_control_file(err: &tokio_postgres::Error) -> bool {
    format_postgres_error(err)
        .to_ascii_lowercase()
        .contains("postgis.control")
}

pub fn pg_uri_with_database(uri: &str, database: &str) -> String {
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

pub fn format_postgres_error(err: &tokio_postgres::Error) -> String {
    if let Some(db_err) = err.as_db_error() {
        let mut parts = vec![format!(
            "{} (SQLSTATE {})",
            db_err.message(),
            db_err.code().code()
        )];

        if let Some(detail) = db_err.detail() {
            parts.push(format!("DETAIL: {detail}"));
        }
        if let Some(context) = db_err.where_() {
            parts.push(format!("CONTEXT: {context}"));
        }
        if let Some(hint) = db_err.hint() {
            parts.push(format!("HINT: {hint}"));
        }
        if let Some(table) = db_err.table() {
            parts.push(format!("TABLE: {table}"));
        }
        if let Some(column) = db_err.column() {
            parts.push(format!("COLUMN: {column}"));
        }

        parts.join("\n")
    } else {
        err.to_string()
    }
}

pub async fn stream_reader_to_copy<R>(
    mut reader: R,
    sink: &mut std::pin::Pin<&mut impl futures::Sink<Bytes, Error = tokio_postgres::Error>>,
    chunk_size: usize,
) -> Result<usize>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut total = 0usize;
    let mut buffer = vec![0u8; chunk_size.max(8 * 1024)];

    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        total = total.saturating_add(read);
        sink.as_mut()
            .send(Bytes::copy_from_slice(&buffer[..read]))
            .await?;
    }

    Ok(total)
}

pub fn resolve_root_table_name(parsed_tables: &[Table], coll_name: &str) -> String {
    let expected = sanitize_name(coll_name);

    if let Some(table) = parsed_tables.iter().find(|table| {
        let name = table.name.trim();
        name.eq_ignore_ascii_case(&expected)
            || name
                .split('.')
                .last()
                .map(|leaf| leaf.eq_ignore_ascii_case(&expected))
                .unwrap_or(false)
    }) {
        return table.name.clone();
    }

    parsed_tables
        .first()
        .map(|table| table.name.clone())
        .unwrap_or_else(|| sanitize_name(coll_name))
}

pub fn resolve_post_import_table_row<'a>(
    table_name: &str,
    local_rows: &'a HashMap<String, PostImportTableRow>,
    global_rows: &'a HashMap<String, PostImportTableRow>,
) -> Option<&'a PostImportTableRow> {
    local_rows
        .get(table_name)
        .or_else(|| global_rows.get(table_name))
}

pub fn is_hex_keyed_name(name: &str) -> bool {
    name.len() >= 8 && name.chars().all(|ch| ch.is_ascii_hexdigit())
}

pub fn is_uuid_keyed_name(name: &str) -> bool {
    let parts = name.split('-').collect::<Vec<_>>();
    parts.len() == 5
        && [8_usize, 4, 4, 4, 12]
            .iter()
            .zip(parts.iter())
            .all(|(expected_len, part)| {
                part.len() == *expected_len && part.chars().all(|ch| ch.is_ascii_hexdigit())
            })
}

pub fn dynamic_map_value_fields(
    fields: &IndexMap<String, FieldSchema>,
) -> Option<&IndexMap<String, FieldSchema>> {
    if fields.is_empty() {
        return None;
    }
    if !fields
        .keys()
        .all(|name| is_hex_keyed_name(name) || is_uuid_keyed_name(name))
    {
        return None;
    }

    for field in fields.values() {
        let non_null = field
            .types
            .iter()
            .filter(|(type_name, _)| !matches!(type_name.as_str(), "Null" | "Undefined"))
            .collect::<Vec<_>>();
        if non_null.len() == 1 && non_null[0].0.as_str() == "Object" {
            if let Some(value_fields) = non_null[0].1.object.as_ref() {
                if !value_fields.is_empty() {
                    return Some(value_fields);
                }
            }
        }
    }

    None
}

pub fn count_dynamic_map_entries(doc: &bson::Document) -> u64 {
    doc.values()
        .filter(|value| matches!(value, Bson::Document(entry) if !entry.is_empty()))
        .count() as u64
}

pub fn child_row_objects_for_mapping(
    node: &Value,
    child_mapping: &CollectionMapping,
) -> Vec<serde_json::Map<String, Value>> {
    match node {
        Value::Array(items) => {
            let mut out = Vec::new();
            for item in items {
                out.extend(child_row_objects_for_mapping(item, child_mapping));
            }
            return out;
        }
        Value::String(raw) => {
            if let Ok(parsed) = serde_json::from_str::<Value>(raw) {
                return child_row_objects_for_mapping(&parsed, child_mapping);
            }
            return Vec::new();
        }
        _ => {}
    }

    let Value::Object(obj) = node else {
        return Vec::new();
    };

    // Empty embedded object means no child rows.
    if obj.is_empty() {
        return Vec::new();
    }

    let mapped_source_fields = child_mapping
        .pg_mapping
        .columns
        .iter()
        .map(|column| column.source_field.as_str())
        .collect::<Vec<_>>();

    let expects_key_column = child_mapping
        .pg_mapping
        .columns
        .iter()
        .any(|mapped| mapped.source_field == "key");

    // Structural parent mappings can intentionally have no payload columns
    // (only PK/FK in DDL). In that case we must keep the original object node
    // as the parent anchor for relative child-path traversal.
    if mapped_source_fields.is_empty() && !expects_key_column {
        return vec![obj.clone()];
    }

    let has_mapped_fields = |candidate: &serde_json::Map<String, Value>| {
        mapped_source_fields.iter().any(|field| {
            if candidate.contains_key(*field) {
                return true;
            }

            let mut current = candidate;
            let mut segments = field.split('.').peekable();
            while let Some(segment) = segments.next() {
                let Some(value) = current.get(segment) else {
                    return false;
                };
                if segments.peek().is_some() {
                    let Some(object) = value.as_object() else {
                        return false;
                    };
                    current = object;
                }
            }
            true
        })
    };

    let row_from_entry_value = |entry_value: &Value| -> Option<serde_json::Map<String, Value>> {
        match entry_value {
            Value::Object(entry_obj) => Some(entry_obj.clone()),
            Value::String(raw) => serde_json::from_str::<Value>(raw)
                .ok()
                .and_then(|parsed| parsed.as_object().cloned()),
            _ => None,
        }
    };

    // Some connectors wrap arrays/objects before the actual row payload.
    // If current object has none of mapped fields, recursively unwrap known
    // wrapper keys first, then any nested aggregate container.
    // For map-style objects that rely on dynamic keys, defer to dedicated
    // map expansion branch below.
    let likely_map_style_object = expects_key_column
        && !obj.contains_key("key")
        && obj
            .values()
            .all(|value| matches!(value, Value::Object(_) | Value::String(_)));

    // Empty object means there is no child row to write.
    if obj.is_empty() && !has_mapped_fields(obj) {
        return Vec::new();
    }

    if !has_mapped_fields(obj) && !likely_map_style_object {
        // Map-like object without dedicated key column: expand all entry payloads
        // only when each entry clearly resolves to mapped fields.
        let mut map_rows = Vec::new();
        let mut all_entries_mappable = true;
        if !obj.is_empty() {
            for entry_value in obj.values() {
                if let Some(row) = row_from_entry_value(entry_value) {
                    if has_mapped_fields(&row) {
                        map_rows.push(row);
                    } else {
                        all_entries_mappable = false;
                        break;
                    }
                } else {
                    all_entries_mappable = false;
                    break;
                }
            }
            if all_entries_mappable && !map_rows.is_empty() {
                return map_rows;
            }
        }

        for wrapper_key in [
            "items",
            "array",
            "values",
            "records",
            "value",
            "transactions",
        ] {
            if let Some(wrapped) = obj.get(wrapper_key) {
                let extracted = child_row_objects_for_mapping(wrapped, child_mapping);
                if !extracted.is_empty() {
                    return extracted;
                }
            }
        }

        for wrapped in obj.values() {
            match wrapped {
                Value::Array(_) | Value::Object(_) | Value::String(_) => {
                    let extracted = child_row_objects_for_mapping(wrapped, child_mapping);
                    if !extracted.is_empty() {
                        return extracted;
                    }
                }
                _ => {}
            }
        }
    }

    let key_from_row_or_entry = |row: &serde_json::Map<String, Value>, entry_key: &str| {
        row.values()
            .find_map(|value| match value {
                Value::String(text) if text == entry_key => Some(text.clone()),
                _ => None,
            })
            .unwrap_or_else(|| entry_key.to_owned())
    };

    // Some connectors encode map entries as {"key": "...", "value": {...}}.
    // Normalize that shape to plain row object.
    if let Some(value_node) = obj.get("value") {
        if let Some(mut row) = row_from_entry_value(value_node) {
            if expects_key_column {
                if let Some(Value::String(existing_key)) = obj.get("key") {
                    let key_value = key_from_row_or_entry(&row, existing_key);
                    row.insert("key".to_owned(), Value::String(key_value));
                }
            }
            return vec![row];
        }
    }

    // Map-style object branch: { "dynamic_key": { ...row... }, ... }
    // Expand into one row per entry; inject dynamic key only when mapping requires it.
    if expects_key_column && !obj.contains_key("key") {
        let mut expanded = Vec::new();
        for (entry_key, entry_value) in obj {
            if let Some(mut row) = row_from_entry_value(entry_value) {
                let key_value = key_from_row_or_entry(&row, entry_key);
                row.insert("key".to_owned(), Value::String(key_value));
                expanded.push(row);
            }
        }
        if !expanded.is_empty() {
            return expanded;
        }
    }

    vec![obj.clone()]
}

pub fn preflight_existing_tables_error(
    database_name: &str,
    tables: &[(String, String)],
) -> anyhow::Error {
    let mut sorted = tables
        .iter()
        .map(|(schema, table)| format!("{}.{}", quote_ident(schema), quote_ident(table)))
        .collect::<Vec<_>>();
    sorted.sort();
    anyhow!(
        "Preflight check failed for database '{}': destination table(s) already exist: {}. Drop the existing table(s) or use a clean target database/schema before retrying import.",
        database_name,
        sorted.join(", ")
    )
}

// ──────────────────────────────────────────────────────────────────────────────
// Post-import report generation (used by report + import + ping)
// ──────────────────────────────────────────────────────────────────────────────

pub fn collect_post_import_warning_entries(
    collection_name: &str,
    node: &PostImportNode,
    out: &mut Vec<String>,
) {
    let node_label = node
        .pg_table_name
        .as_deref()
        .filter(|label| !label.trim().is_empty())
        .unwrap_or(&node.name);

    let has_md5_mismatch = node.md5_summary.as_ref().is_some_and(|summary| {
        summary.mongo_md5 != summary.pg_md5 || !summary.mismatches.is_empty()
    });
    if has_md5_mismatch {
        out.push(format!("{collection_name}:{node_label}:md5_mismatch"));
    }

    if !node.count_diff_rows.is_empty() {
        out.push(format!("{collection_name}:{node_label}:rowcount_mismatch"));
    }

    for child in &node.children {
        collect_post_import_warning_entries(collection_name, child, out);
    }
}

pub fn summarize_post_import_warning_entries(rows: &[PostImportCollectionRow]) -> Vec<String> {
    let mut entries = Vec::new();
    for row in rows {
        collect_post_import_warning_entries(&row.name, &row.root, &mut entries);
    }
    entries.sort();
    entries.dedup();
    entries
}

pub fn should_fail_post_import_on_warnings(warning_count: usize) -> bool {
    warning_count > 0
}

pub async fn write_post_import_report(
    conf: &Path,
    namespace_override: &str,
    source_uri_override: &str,
    include_md5: bool,
) -> Result<()> {
    async fn upload_post_import_report_to_gcs(
        output_path: &Path,
        bucket: &str,
        prefix: &str,
        cluster_name: Option<&str>,
        project_dir: &str,
    ) -> Result<()> {
        ensure_gcs_authentication().await?;
        let storage = Storage::builder()
            .build()
            .await
            .context("Failed to initialize Google Cloud Storage client")?;
        let bucket_resource = format!("projects/_/buckets/{bucket}");

        let effective_prefix = ensure_output_prefix_segments(prefix, cluster_name, project_dir);
        let object_key = if effective_prefix.is_empty() {
            "reports/post_report.html".to_owned()
        } else {
            format!("{effective_prefix}/reports/post_report.html")
        };

        let bytes = tokio::fs::read(output_path).await.with_context(|| {
            format!(
                "Cannot read post-import report for upload: {}",
                output_path.display()
            )
        })?;

        storage
            .write_object(bucket_resource, object_key.clone(), Bytes::from(bytes))
            .send_buffered()
            .await
            .with_context(|| {
                format!(
                    "Failed to upload post-import report {} to gs://{}/{}",
                    output_path.display(),
                    bucket,
                    object_key
                )
            })?;

        info!(
            "Post-import report uploaded to gs://{}/{}",
            bucket, object_key
        );

        Ok(())
    }

    let trace_started_at = Instant::now();
    let default_trace_namespace = if namespace_override.is_empty() {
        "<from-config>"
    } else {
        namespace_override
    };
    log_post_import_trace(
        "start",
        default_trace_namespace,
        include_md5,
        "begin write_post_import_report",
    );

    let mut trace_namespace = default_trace_namespace.to_owned();
    let mut trace_output_path: Option<PathBuf> = None;
    let mut trace_collection_count = 0usize;
    let mut trace_warning_count = 0usize;

    let outcome: Result<()> = async {
        let c = read_conf(conf)?;
        let conf_include: Vec<String> = c.include.iter().map(|name| sanitize_name(name)).collect();
        let conf_exclude: Vec<String> = c.exclude.iter().map(|name| sanitize_name(name)).collect();
        let storage_backend =
            resolve_export_write_backend(&c.base_dir).unwrap_or(ExportWriteBackend::LocalFs);
        let mut metadata_root = match &storage_backend {
            ExportWriteBackend::LocalFs => configured_project_root(&c),
            ExportWriteBackend::Gcs { .. } => resolve_local_project_root_from_config(conf, &c),
        };
        let reports_root = resolve_local_project_root_from_config(conf, &c);
        let configured_reports_dir = reports_root.join("reports");
        let reports_dir = match std::fs::create_dir_all(&configured_reports_dir) {
            Ok(()) => configured_reports_dir,
            Err(err) => {
                let fallback_reports_dir = std::env::temp_dir()
                    .join("mongo2pg-reports")
                    .join(sanitize_name(&c.project_dir))
                    .join("reports");
                std::fs::create_dir_all(&fallback_reports_dir).with_context(|| {
                    format!(
                        "Can't create reports dir {} (original error: {}) and fallback dir {}",
                        configured_reports_dir.display(),
                        err,
                        fallback_reports_dir.display()
                    )
                })?;
                info!(
                    "Can't create reports dir {}; using fallback {}",
                    configured_reports_dir.display(),
                    fallback_reports_dir.display()
                );
                fallback_reports_dir
            }
        };

        let namespace = if namespace_override.is_empty() {
            c.namespace.clone().ok_or_else(|| {
                anyhow!(
                    "No NAMESPACE provided: pass --namespace or add NAMESPACE to the config file"
                )
            })?
        } else {
            namespace_override.to_owned()
        };
        trace_namespace = namespace.clone();
        let backend_label = match &storage_backend {
            ExportWriteBackend::LocalFs => "localfs",
            ExportWriteBackend::Gcs { .. } => "gcs",
        };
        log_post_import_trace(
            "config_loaded",
            &trace_namespace,
            include_md5,
            &format!(
                "project_dir={} storage_backend={}",
                c.project_dir, backend_label
            ),
        );
        debug_post_import_trace(
            "reports_dir_ready",
            &trace_namespace,
            include_md5,
            &format!("reports_dir={}", reports_dir.display()),
        );

        let source_uri = if source_uri_override.is_empty() {
            c.source_uri
                .as_deref()
                .ok_or_else(|| anyhow!("SOURCE_URI not found in the config file"))?
                .to_owned()
        } else {
            source_uri_override.to_owned()
        };
        let target_uri = c
            .target_uri
            .as_deref()
            .ok_or_else(|| anyhow!("TARGET_URI not found in the config file"))?;
        let target_database_name = c.target_database_name.as_deref();
        let (db_name, _) = split_namespace_scope(&namespace);

        if include_md5 {
            log_post_import_trace(
                "checkmd5_enabled",
                &trace_namespace,
                include_md5,
                &format!("db={db_name}"),
            );
        }

        let mut metadata_stage: Option<tempfile::TempDir> = None;
        let mut schema_tables_root = metadata_root.join("schema").join("tables");

        if !(schema_tables_root.join(db_name).is_dir() || schema_tables_root.is_dir()) {
            if let ExportWriteBackend::Gcs { bucket, prefix } = &storage_backend {
                if let Some(stage) = stage_export_metadata_from_gcs(
                    bucket,
                    prefix,
                    c.cluster_name.as_deref(),
                    &c.project_dir,
                    db_name,
                )
                .await?
                {
                    metadata_root = stage.path().to_path_buf();
                    schema_tables_root = metadata_root.join("schema").join("tables");
                    info!(
                        "post-import report metadata staged from GCS into temporary directory {}",
                        metadata_root.display()
                    );
                    debug_post_import_trace(
                        "metadata_staged",
                        &trace_namespace,
                        include_md5,
                        &format!("metadata_root={}", metadata_root.display()),
                    );
                    metadata_stage = Some(stage);
                }
            }
        }

        let collections_dir = resolve_collections_dir(&metadata_root, db_name);
        let output_path = reports_dir.join("post_report.html");

        log_post_import_trace(
            "rows_build_begin",
            &trace_namespace,
            include_md5,
            &format!("db={db_name}"),
        );
        let rows = build_post_import_rows(
            conf,
            &source_uri,
            &target_database_name
                .map(|db_name| pg_uri_with_database(target_uri, db_name))
                .unwrap_or_else(|| target_uri.to_owned()),
            &namespace,
            &conf_include,
            &conf_exclude,
            &reports_dir,
            &collections_dir,
            &schema_tables_root,
            include_md5,
        )
        .await?;
        trace_collection_count = rows.len();
        let warning_entries = summarize_post_import_warning_entries(&rows);
        trace_warning_count = warning_entries.len();
        log_post_import_trace(
            "rows_build_done",
            &trace_namespace,
            include_md5,
            &format!(
                "collections={} warnings={}",
                trace_collection_count, trace_warning_count
            ),
        );

        debug_post_import_trace(
            "render_begin",
            &trace_namespace,
            include_md5,
            "rendering post-import html",
        );
        let html = render_post_import_html(
            &rows,
            &namespace,
            &cluster_from_uri(&source_uri),
            &cluster_from_uri(
                &target_database_name
                    .map(|db_name| pg_uri_with_database(target_uri, db_name))
                    .unwrap_or_else(|| target_uri.to_owned()),
            ),
        );
        std::fs::write(&output_path, html)
            .with_context(|| format!("Failed to write {}", output_path.display()))?;
        info!("Post-import report written to {}", output_path.display());
        trace_output_path = Some(output_path.clone());
        log_post_import_trace(
            "write_done",
            &trace_namespace,
            include_md5,
            &format!("output={}", output_path.display()),
        );

        if let ExportWriteBackend::Gcs { bucket, prefix } = &storage_backend {
            log_post_import_trace(
                "gcs_upload_begin",
                &trace_namespace,
                include_md5,
                &format!("bucket={bucket}"),
            );
            upload_post_import_report_to_gcs(
                &output_path,
                bucket,
                prefix,
                c.cluster_name.as_deref(),
                &c.project_dir,
            )
            .await?;
            log_post_import_trace(
                "gcs_upload_done",
                &trace_namespace,
                include_md5,
                &format!("bucket={bucket}"),
            );
        }

        if should_fail_post_import_on_warnings(warning_entries.len()) {
            let preview = warning_entries
                .iter()
                .take(10)
                .cloned()
                .collect::<Vec<_>>()
                .join(", ");
            let suffix = if warning_entries.len() > 10 {
                format!(", ... (+{} more)", warning_entries.len() - 10)
            } else {
                String::new()
            };
            return Err(anyhow!(
                "post-import report contains validation warnings in {} node(s): {}{}",
                warning_entries.len(),
                preview,
                suffix
            ));
        }

        drop(metadata_stage);

        Ok(())
    }
    .await;

    match outcome {
        Ok(()) => {
            let output_text = trace_output_path
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<none>".to_owned());
            log_post_import_trace(
                "success",
                &trace_namespace,
                include_md5,
                &format!(
                    "elapsed_ms={} collections={} output={}",
                    trace_started_at.elapsed().as_millis(),
                    trace_collection_count,
                    output_text
                ),
            );
            Ok(())
        }
        Err(err) => {
            log_post_import_trace(
                "failure",
                &trace_namespace,
                include_md5,
                &format!(
                    "elapsed_ms={} collections={} warnings={} error={:#}",
                    trace_started_at.elapsed().as_millis(),
                    trace_collection_count,
                    trace_warning_count,
                    err
                ),
            );
            Err(err)
        }
    }
}

#[derive(Debug, Deserialize)]
struct KafkaImportWriteModeStatsReportYaml {
    #[serde(default)]
    skipped_missing_required_snapshot_by_table: HashMap<String, usize>,
    #[serde(default)]
    skipped_missing_required_snapshot_samples: Vec<String>,
    #[serde(default)]
    copy_skipped_by_table: HashMap<String, usize>,
    #[serde(default)]
    copy_skipped_samples: Vec<String>,
}

pub fn load_snapshot_skip_summaries_for_post_report(
    reports_dir: &Path,
) -> HashMap<String, PostImportSnapshotSkipSummary> {
    let stats_path = reports_dir.join("kafka_import_write_mode.stats.yaml");
    let Ok(content) = std::fs::read_to_string(&stats_path) else {
        return HashMap::new();
    };
    let Ok(parsed) = serde_yaml::from_str::<KafkaImportWriteModeStatsReportYaml>(&content) else {
        return HashMap::new();
    };

    let mut samples_by_table: HashMap<String, Vec<String>> = HashMap::new();
    let mut reasons_by_table: HashMap<String, Vec<String>> = HashMap::new();
    for sample in parsed.skipped_missing_required_snapshot_samples {
        let mut table_name: Option<String> = None;
        let mut reason_line: Option<String> = None;
        let mut source_field: Option<&str> = None;
        let mut target_field: Option<&str> = None;
        let mut reason_code: Option<&str> = None;

        for token in sample.split_whitespace() {
            if let Some(value) = token.strip_prefix("table=") {
                table_name = Some(value.to_owned());
            } else if let Some(value) = token.strip_prefix("source_field=") {
                source_field = Some(value);
            } else if let Some(value) = token.strip_prefix("target_field=") {
                target_field = Some(value);
            } else if let Some(value) = token.strip_prefix("reason=") {
                reason_code = Some(value);
            }
        }

        if let Some(code) = reason_code {
            reason_line = Some(match (code, source_field, target_field) {
                ("missing_required_mapped_field", Some(source), Some(target)) => {
                    format!("Required mapped field missing: mongodb.{source} -> pg.{target}")
                }
                ("missing_required_mapped_field", _, _) => {
                    "Required mapped field missing".to_owned()
                }
                (other, _, _) => format!("{other}"),
            });
        }

        if let Some(table_name) = table_name {
            samples_by_table
                .entry(table_name.clone())
                .or_default()
                .push(sample);
            if let Some(reason) = reason_line {
                let reasons = reasons_by_table.entry(table_name).or_default();
                if !reasons.contains(&reason) {
                    reasons.push(reason);
                }
            }
        }
    }

    let mut copy_samples_by_table: HashMap<String, Vec<String>> = HashMap::new();
    let mut copy_reasons_by_table: HashMap<String, Vec<String>> = HashMap::new();
    for sample in parsed.copy_skipped_samples {
        let mut table_name: Option<String> = None;
        let mut reason_code: Option<&str> = None;

        for token in sample.split_whitespace() {
            if let Some(value) = token.strip_prefix("table=") {
                table_name = Some(value.to_owned());
            } else if let Some(value) = token.strip_prefix("reason=") {
                reason_code = Some(value);
            }
        }

        let reason_line = reason_code.map(|code| match code {
            "copy_non_root_mapping" => {
                "COPY fast-path skipped row: mapping contains non-root fields".to_owned()
            }
            "copy_unconvertible_literal" => {
                "COPY fast-path skipped row: value could not be converted to COPY literal"
                    .to_owned()
            }
            "copy_empty_columns" => {
                "COPY fast-path skipped row: no mapped columns available".to_owned()
            }
            "copy_non_empty_target" => {
                "COPY fast-path skipped batch: destination table already has rows (fallback upsert used)"
                    .to_owned()
            }
            other => format!("{other}"),
        });

        if let Some(table_name) = table_name {
            copy_samples_by_table
                .entry(table_name.clone())
                .or_default()
                .push(sample);
            if let Some(reason) = reason_line {
                let reasons = copy_reasons_by_table.entry(table_name).or_default();
                if !reasons.contains(&reason) {
                    reasons.push(reason);
                }
            }
        }
    }

    let mut table_names: std::collections::HashSet<String> = std::collections::HashSet::new();
    table_names.extend(
        parsed
            .skipped_missing_required_snapshot_by_table
            .keys()
            .cloned(),
    );
    table_names.extend(parsed.copy_skipped_by_table.keys().cloned());

    table_names
        .into_iter()
        .map(|table_name| {
            let count = parsed
                .skipped_missing_required_snapshot_by_table
                .get(&table_name)
                .copied()
                .unwrap_or(0);
            let copy_count = parsed
                .copy_skipped_by_table
                .get(&table_name)
                .copied()
                .unwrap_or(0);
            let samples = samples_by_table.remove(&table_name).unwrap_or_default();
            (
                table_name.clone(),
                PostImportSnapshotSkipSummary {
                    count,
                    samples,
                    reasons: reasons_by_table.remove(&table_name).unwrap_or_default(),
                    copy_count,
                    copy_samples: copy_samples_by_table
                        .remove(&table_name)
                        .unwrap_or_default(),
                    copy_reasons: copy_reasons_by_table
                        .remove(&table_name)
                        .unwrap_or_default(),
                },
            )
        })
        .collect()
}

pub async fn build_post_import_rows(
    config_path: &Path,
    source_uri: &str,
    target_uri: &str,
    namespace: &str,
    include: &[String],
    exclude: &[String],
    reports_dir: &Path,
    collections_root: &Path,
    schema_tables_root: &Path,
    include_md5: bool,
) -> Result<Vec<PostImportCollectionRow>> {
    use crate::engine::analyzer::{CollectionSchema, TypeSchema};
    use futures::TryStreamExt;
    use mongodb::Client;

    #[derive(Clone)]
    enum CountNodeKind {
        Root,
        Object { field_name: String },
        MapObject { field_name: String },
        ArrayScalar { field_name: String },
        ArrayObject { field_name: String },
    }

    #[derive(Clone)]
    struct CountNode {
        name: String,
        is_array: bool,
        mongo_count: u64,
        pg_table_key: Option<String>,
        pg_table_name: Option<String>,
        pg_row_count: Option<i64>,
        md5_summary: Option<PostImportMd5Summary>,
        snapshot_skip_summary: Option<PostImportSnapshotSkipSummary>,
        count_diff_rows: Vec<PostImportCountDiffRow>,
        kind: CountNodeKind,
        children: Vec<CountNode>,
    }

    fn is_null_type(type_name: &str) -> bool {
        matches!(type_name, "Null" | "Undefined")
    }

    fn child_table_name(parent_name: &str, field: &str, pg_schema: Option<&str>) -> String {
        let ancestor_segments = parent_name.split('_').collect::<Vec<_>>();
        let raw = if ancestor_segments.iter().any(|segment| *segment == field) {
            let parent_segment = ancestor_segments.last().copied().unwrap_or(parent_name);
            format!("{parent_segment}_{field}")
        } else {
            field.to_owned()
        };
        if let Some(schema) = pg_schema {
            let prefix = format!("{}{}", sanitize_pg_name(schema), "_");
            raw.strip_prefix(&prefix).map(str::to_owned).unwrap_or(raw)
        } else {
            raw
        }
    }

    fn mongo_path_key_from_segments(segments: &[String]) -> String {
        if segments.is_empty() {
            ".".to_owned()
        } else {
            format!(".{}", segments.join("."))
        }
    }

    fn normalize_mongo_path_key(raw: &str) -> String {
        let trimmed = raw.trim();
        if trimmed.is_empty() || trimmed == "." {
            return ".".to_owned();
        }
        let without_prefix = trimmed.trim_start_matches('.');
        format!(".{}", without_prefix)
    }

    fn load_table_name_by_mongo_path(collection_dir: &Path) -> HashMap<String, String> {
        let mut by_path = HashMap::new();
        let Ok(entries) = std::fs::read_dir(collection_dir) else {
            return by_path;
        };

        for entry in entries.flatten() {
            let file_path = entry.path();
            if !file_path.is_file() {
                continue;
            }
            let Some(file_name) = file_path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if !file_name.starts_with("mapping_") || !file_name.ends_with(".yaml") {
                continue;
            }

            let Ok(content) = std::fs::read_to_string(&file_path) else {
                continue;
            };
            let Ok(mapping) = serde_yaml::from_str::<CollectionMapping>(&content) else {
                continue;
            };

            let key = normalize_mongo_path_key(mapping.mongo_path.as_deref().unwrap_or("."));
            by_path
                .entry(key)
                .or_insert_with(|| mapping.pg_mapping.table_name.clone());

            // Some flattened child mappings intentionally keep mongo_path='.'
            // while source fields are dotted (e.g. 'location.address.city').
            // Add a synthetic path alias from the first source segment so
            // post-import tree nodes can attach to the child table.
            let has_parent_fk = mapping
                .pg_mapping
                .ddl
                .as_ref()
                .is_some_and(|ddl| !ddl.foreign_keys.is_empty())
                || mapping
                    .traversal
                    .as_ref()
                    .and_then(|plan| plan.parent_table.as_deref())
                    .is_some();
            if normalize_mongo_path_key(mapping.mongo_path.as_deref().unwrap_or(".")) == "."
                && has_parent_fk
            {
                for column in &mapping.pg_mapping.columns {
                    let Some(first_segment) = column.source_field.split('.').next() else {
                        continue;
                    };
                    if first_segment.is_empty() || first_segment == "_id" || first_segment == "key"
                    {
                        continue;
                    }
                    let alias = format!(".{}", first_segment.trim_start_matches('.'));
                    by_path
                        .entry(alias)
                        .or_insert_with(|| mapping.pg_mapping.table_name.clone());
                }
            }
        }

        by_path
    }

    fn build_field_nodes(
        parent_table_name: &str,
        mongo_path_segments: &[String],
        fields: &IndexMap<String, FieldSchema>,
        pg_schema: Option<&str>,
        table_name_by_mongo_path: &HashMap<String, String>,
        table_counts: &HashMap<String, PostImportTableRow>,
        global_table_counts: &HashMap<String, PostImportTableRow>,
        md5_summaries: &HashMap<String, PostImportMd5Summary>,
        snapshot_skip_summaries: &HashMap<String, PostImportSnapshotSkipSummary>,
    ) -> Vec<CountNode> {
        let mut nodes = Vec::new();

        for (raw_name, field) in fields {
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
                    let child_fields = dynamic_map_value_fields(sub_fields).unwrap_or(sub_fields);
                    let mut child_segments = mongo_path_segments.to_vec();
                    child_segments.push(raw_name.to_string());
                    let mongo_path_key = mongo_path_key_from_segments(&child_segments);
                    let table_name = table_name_by_mongo_path
                        .get(&mongo_path_key)
                        .cloned()
                        .unwrap_or_else(|| {
                            child_table_name(
                                parent_table_name,
                                &sanitize_pg_name(raw_name),
                                pg_schema,
                            )
                        });
                    let table_ref = resolve_post_import_table_row(
                        &table_name,
                        table_counts,
                        global_table_counts,
                    );
                    nodes.push(CountNode {
                        name: raw_name.to_string(),
                        is_array: false,
                        mongo_count: 0,
                        pg_table_key: table_ref.map(|_| table_name.clone()),
                        pg_table_name: table_ref.and_then(|t| {
                            Some(match &t.schema_name {
                                Some(schema) => format!("{}.{}", schema, t.table_name),
                                None => t.table_name.clone(),
                            })
                        }),
                        pg_row_count: table_ref.map(|t| t.row_count),
                        md5_summary: md5_summaries.get(&table_name).cloned(),
                        snapshot_skip_summary: snapshot_skip_summaries.get(&table_name).cloned(),
                        count_diff_rows: Vec::new(),
                        kind: if std::ptr::eq(child_fields, sub_fields) {
                            CountNodeKind::Object {
                                field_name: raw_name.to_string(),
                            }
                        } else {
                            CountNodeKind::MapObject {
                                field_name: raw_name.to_string(),
                            }
                        },
                        children: build_field_nodes(
                            &table_name,
                            &child_segments,
                            child_fields,
                            pg_schema,
                            table_name_by_mongo_path,
                            table_counts,
                            global_table_counts,
                            md5_summaries,
                            snapshot_skip_summaries,
                        ),
                    });
                }
                continue;
            }

            if non_null.len() == 1 && non_null[0].0 == "Array" {
                let type_schema = non_null[0].1;
                if let Some(items_field) = &type_schema.array {
                    let mut child_segments = mongo_path_segments.to_vec();
                    child_segments.push(raw_name.to_string());
                    let mongo_path_key = mongo_path_key_from_segments(&child_segments);
                    let table_name = table_name_by_mongo_path
                        .get(&mongo_path_key)
                        .cloned()
                        .unwrap_or_else(|| {
                            child_table_name(
                                parent_table_name,
                                &sanitize_pg_name(raw_name),
                                pg_schema,
                            )
                        });
                    let table_ref = resolve_post_import_table_row(
                        &table_name,
                        table_counts,
                        global_table_counts,
                    );
                    let object_type = items_field.types.get("Object");
                    let (kind, children) = if let Some(object_ts) = object_type {
                        (
                            CountNodeKind::ArrayObject {
                                field_name: raw_name.to_string(),
                            },
                            object_ts
                                .object
                                .as_ref()
                                .map(|sub_fields| {
                                    build_field_nodes(
                                        &table_name,
                                        &child_segments,
                                        sub_fields,
                                        pg_schema,
                                        table_name_by_mongo_path,
                                        table_counts,
                                        global_table_counts,
                                        md5_summaries,
                                        snapshot_skip_summaries,
                                    )
                                })
                                .unwrap_or_default(),
                        )
                    } else {
                        (
                            CountNodeKind::ArrayScalar {
                                field_name: raw_name.to_string(),
                            },
                            Vec::new(),
                        )
                    };
                    nodes.push(CountNode {
                        name: raw_name.to_string(),
                        is_array: true,
                        mongo_count: 0,
                        pg_table_key: table_ref.map(|_| table_name.clone()),
                        pg_table_name: table_ref.and_then(|t| {
                            Some(match &t.schema_name {
                                Some(schema) => format!("{}.{}", schema, t.table_name),
                                None => t.table_name.clone(),
                            })
                        }),
                        pg_row_count: table_ref.map(|t| t.row_count),
                        md5_summary: md5_summaries.get(&table_name).cloned(),
                        snapshot_skip_summary: snapshot_skip_summaries.get(&table_name).cloned(),
                        count_diff_rows: Vec::new(),
                        kind,
                        children,
                    });
                }
            }
        }

        nodes
    }

    fn count_children(nodes: &mut [CountNode], doc: &bson::Document) {
        for node in nodes {
            match &node.kind {
                CountNodeKind::Root => {}
                CountNodeKind::Object { field_name } => {
                    if let Some(Bson::Document(child_doc)) = doc.get(field_name) {
                        if !child_doc.is_empty() {
                            node.mongo_count += 1;
                            count_children(&mut node.children, child_doc);
                        }
                    }
                }
                CountNodeKind::MapObject { field_name } => {
                    if let Some(Bson::Document(child_doc)) = doc.get(field_name) {
                        node.mongo_count += count_dynamic_map_entries(child_doc);
                        for value in child_doc.values() {
                            if let Bson::Document(entry_doc) = value {
                                if !entry_doc.is_empty() {
                                    count_children(&mut node.children, entry_doc);
                                }
                            }
                        }
                    }
                }
                CountNodeKind::ArrayScalar { field_name } => {
                    if let Some(Bson::Array(items)) = doc.get(field_name) {
                        node.mongo_count += items
                            .iter()
                            .filter(|item| !matches!(item, Bson::Null))
                            .count() as u64;
                    }
                }
                CountNodeKind::ArrayObject { field_name } => {
                    if let Some(Bson::Array(items)) = doc.get(field_name) {
                        for item in items {
                            if let Bson::Document(child_doc) = item {
                                if !child_doc.is_empty() {
                                    node.mongo_count += 1;
                                    count_children(&mut node.children, child_doc);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn into_post_import_node(node: CountNode) -> PostImportNode {
        let md5_confirms_match = node.md5_summary.as_ref().is_some_and(|summary| {
            summary.mongo_md5 == summary.pg_md5 && summary.mismatches.is_empty()
        });
        let display_mongo_count = if md5_confirms_match {
            node.pg_row_count
                .and_then(|count| u64::try_from(count).ok())
                .unwrap_or(node.mongo_count)
        } else {
            node.mongo_count
        };

        PostImportNode {
            name: node.name,
            is_array: node.is_array,
            mongo_count: display_mongo_count,
            pg_table_name: node.pg_table_name,
            pg_row_count: node.pg_row_count,
            md5_summary: node.md5_summary,
            snapshot_skip_summary: node.snapshot_skip_summary,
            count_diff_rows: node.count_diff_rows,
            children: node
                .children
                .into_iter()
                .map(into_post_import_node)
                .collect(),
        }
    }

    fn collect_rowcount_mismatch_tables(node: &CountNode, out: &mut Vec<String>) {
        if let (Some(table_name), Some(pg_rows)) = (&node.pg_table_key, node.pg_row_count) {
            if pg_rows != node.mongo_count as i64 {
                let md5_confirms_match = node.md5_summary.as_ref().is_some_and(|summary| {
                    summary.mongo_md5 == summary.pg_md5 && summary.mismatches.is_empty()
                });
                if !md5_confirms_match {
                    out.push(table_name.clone());
                }
            }
        }
        for child in &node.children {
            collect_rowcount_mismatch_tables(child, out);
        }
    }

    fn apply_count_diff_rows(
        node: &mut CountNode,
        rows_by_table: &HashMap<String, Vec<PostImportCountDiffRow>>,
    ) {
        if let Some(table_name) = &node.pg_table_key {
            if let Some(rows) = rows_by_table.get(table_name) {
                node.count_diff_rows = rows.clone();
            }
        }
        for child in &mut node.children {
            apply_count_diff_rows(child, rows_by_table);
        }
    }

    fn is_transient_pg_count_error(err: &tokio_postgres::Error) -> bool {
        let message = err.to_string().to_ascii_lowercase();
        message.contains("connection closed")
            || message.contains("error communicating with the server")
            || message.contains("server closed the connection")
            || message.contains("connection reset")
            || message.contains("broken pipe")
            || message.contains("timed out")
            || message.contains("timeout")
    }

    async fn query_pg_count_with_retry(
        target_uri: &str,
        qualified_name: &str,
        count_sql: &str,
    ) -> Result<i64> {
        const RETRY_MAX: u32 = 2;
        let mut attempt = 0_u32;
        loop {
            attempt += 1;
            let pg_client = connect_pg_client(target_uri).await.with_context(|| {
                format!(
                    "{}: failed during PostgreSQL count connection setup",
                    connection_failed_context("pg", "connect")
                )
            })?;

            match pg_client.query_one(count_sql, &[]).await {
                Ok(row) => {
                    let row_count: i64 = row.get(0);
                    return Ok(row_count);
                }
                Err(err) if attempt <= RETRY_MAX && is_transient_pg_count_error(&err) => {
                    warn!(
                        "retrying PostgreSQL row count for {} attempt={}/{} due to transient error: {}",
                        qualified_name,
                        attempt,
                        RETRY_MAX + 1,
                        err
                    );
                    continue;
                }
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!("Failed to count PostgreSQL rows in {qualified_name}")
                    });
                }
            }
        }
    }

    fn quote_sql_literal(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }

    fn grouped_table_key_filter_value(coll_name: &str, table_name: &str) -> Option<String> {
        coll_name
            .strip_prefix(table_name)
            .and_then(|rest| rest.strip_prefix('_'))
            .filter(|suffix| !suffix.is_empty())
            .map(str::to_owned)
    }

    let (db_name, only_collection) = split_namespace_scope(namespace);

    let mongo_client = Client::with_uri_str(source_uri).await.with_context(|| {
        format!(
            "{}: failed to connect to MongoDB using SOURCE_URI",
            connection_failed_context("mongo", "connect")
        )
    })?;
    let mongo_db = mongo_client.database(db_name);
    let mut collection_names = mongo_db.list_collection_names().await.with_context(|| {
        format!(
            "{}: failed to list collections for MongoDB database {db_name}",
            connection_failed_context("mongo", "query")
        )
    })?;
    collection_names.retain(|name| !name.starts_with("system."));
    collection_names.retain(|name| should_infer_collection(&sanitize_name(name), include, exclude));
    if let Some(coll_name) = only_collection {
        collection_names.retain(|name| name == coll_name);
    }
    collection_names.sort();

    let ddl_dir = if schema_tables_root.join(db_name).is_dir() {
        schema_tables_root.join(db_name)
    } else {
        schema_tables_root.to_path_buf()
    };
    let snapshot_skip_summaries = load_snapshot_skip_summaries_for_post_report(reports_dir);
    let collections_dir_for_lookup = if collections_root.join(db_name).is_dir() {
        collections_root.join(db_name)
    } else {
        collections_root.to_path_buf()
    };

    let total_collections = collection_names.len();
    let mut global_table_rows: HashMap<String, PostImportTableRow> = HashMap::new();
    let mut rows = Vec::new();
    for (index, coll_name) in collection_names.into_iter().enumerate() {
        let document_count = mongo_db
            .collection::<bson::Document>(&coll_name)
            .count_documents(bson::doc! {})
            .await
            .with_context(|| {
                format!("Failed to count MongoDB documents for {db_name}.{coll_name}")
            })?;

        let safe_coll_name = coll_name.replace('/', "_");
        let mut sql_path = ddl_dir.join(format!("{}.sql", sanitize_name(&coll_name)));
        let mut schema_path = collections_dir_for_lookup
            .join(&safe_coll_name)
            .join(format!("{}.json", safe_coll_name));

        if !sql_path.is_file() || !schema_path.is_file() {
            if let Some(grouped_lookup) =
                resolve_grouped_sql_lookup_name(&collections_dir_for_lookup, &coll_name)
            {
                if !sql_path.is_file() {
                    let grouped_sql_path = ddl_dir.join(format!("{}.sql", grouped_lookup));
                    if grouped_sql_path.is_file() {
                        sql_path = grouped_sql_path;
                    }
                }

                if !schema_path.is_file() {
                    let grouped_schema_path = collections_dir_for_lookup
                        .join(&grouped_lookup)
                        .join(format!("{}.json", grouped_lookup));
                    if grouped_schema_path.is_file() {
                        schema_path = grouped_schema_path;
                    }
                }
            }

            if !sql_path.is_file() || !schema_path.is_file() {
                if let Some((group_prefix, _)) = coll_name.split_once('_') {
                    let safe_group_prefix = group_prefix.replace('/', "_");

                    if !sql_path.is_file() {
                        let grouped_sql_path = ddl_dir.join(format!("{}.sql", safe_group_prefix));
                        if grouped_sql_path.is_file() {
                            sql_path = grouped_sql_path;
                        }
                    }

                    if !schema_path.is_file() {
                        let grouped_schema_path = collections_dir_for_lookup
                            .join(&safe_group_prefix)
                            .join(format!("{}.json", safe_group_prefix));
                        if grouped_schema_path.is_file() {
                            schema_path = grouped_schema_path;
                        }
                    }
                }
            }
        }

        let schema: CollectionSchema = serde_json::from_str(
            &std::fs::read_to_string(&schema_path)
                .with_context(|| format!("Failed to read {}", schema_path.display()))?,
        )
        .with_context(|| format!("Failed to parse {}", schema_path.display()))?;

        let root = if sql_path.is_file() {
            let sql = std::fs::read_to_string(&sql_path)
                .with_context(|| format!("Failed to read {}", sql_path.display()))?;
            let schema_name = extract_search_path(&sql);
            let parsed_tables = parse_sql(&sql);
            let root_table_name = resolve_root_table_name(&parsed_tables, &coll_name);
            let mut table_rows = HashMap::new();
            for table in parsed_tables {
                let qualified_name = match &schema_name {
                    Some(schema) => format!("{}.{}", quote_ident(schema), quote_ident(&table.name)),
                    None => quote_ident(&table.name),
                };
                let grouped_key_filter = if table
                    .columns
                    .iter()
                    .any(|column| column.name.eq_ignore_ascii_case("_key"))
                {
                    grouped_table_key_filter_value(&coll_name, &table.name)
                } else {
                    None
                };
                let count_sql = if let Some(grouped_key) = grouped_key_filter {
                    format!(
                        "SELECT COUNT(*)::BIGINT FROM {qualified_name} WHERE {} = {}",
                        quote_ident("_key"),
                        quote_sql_literal(&grouped_key)
                    )
                } else {
                    format!("SELECT COUNT(*)::BIGINT FROM {qualified_name}")
                };
                let row_count =
                    query_pg_count_with_retry(target_uri, &qualified_name, &count_sql).await?;
                table_rows.insert(
                    table.name.clone(),
                    PostImportTableRow {
                        schema_name: schema_name.clone(),
                        table_name: table.name,
                        row_count,
                    },
                );
            }
            for (name, row) in &table_rows {
                global_table_rows
                    .entry(name.clone())
                    .or_insert_with(|| PostImportTableRow {
                        schema_name: row.schema_name.clone(),
                        table_name: row.table_name.clone(),
                        row_count: row.row_count,
                    });
            }
            let root_ref = table_rows.get(&root_table_name);
            let md5_summaries = if include_md5 {
                info!(
                    "[{}/{}] ⚙️  compute hash (md5) for {}.{}",
                    index + 1,
                    total_collections,
                    db_name,
                    coll_name
                );
                match compute_md5_summaries_for_collection_with_collections_root(
                    &coll_name,
                    config_path,
                    Some(&collections_dir_for_lookup),
                )
                .await
                {
                    Ok(summaries) => summaries
                        .into_iter()
                        .map(|table_summary| {
                            (
                                table_summary.table_name,
                                PostImportMd5Summary {
                                    mongo_md5: table_summary.summary.mongo_md5,
                                    pg_md5: table_summary.summary.pg_md5,
                                    columns: table_summary
                                        .summary
                                        .columns
                                        .into_iter()
                                        .map(|column| PostImportMd5Column {
                                            source_field: column.source_field,
                                            source_type: column.source_type,
                                            target_field: column.target_field,
                                            target_type: column.target_type,
                                        })
                                        .collect(),
                                    mismatches: table_summary
                                        .summary
                                        .mismatches
                                        .into_iter()
                                        .map(|mismatch| PostImportMd5MismatchRow {
                                            row_index: mismatch.row_index,
                                            mongo_values: mismatch.mongo_values,
                                            pg_values: mismatch.pg_values,
                                        })
                                        .collect(),
                                },
                            )
                        })
                        .collect::<HashMap<_, _>>(),
                    Err(err) => {
                        warn!(
                            "failed to compute md5 summary for {}.{}: {:#} (non-fatal; rowcount-diff phase will retry md5 for mismatched tables)",
                            db_name,
                            coll_name,
                            err
                        );
                        HashMap::new()
                    }
                }
            } else {
                HashMap::new()
            };
            let mut root = CountNode {
                name: coll_name.clone(),
                is_array: false,
                mongo_count: document_count,
                pg_table_key: root_ref.map(|_| root_table_name.clone()),
                pg_table_name: root_ref.and_then(|t| {
                    Some(match &t.schema_name {
                        Some(schema) => format!("{}.{}", schema, t.table_name),
                        None => t.table_name.clone(),
                    })
                }),
                pg_row_count: root_ref.map(|t| t.row_count),
                md5_summary: md5_summaries.get(&root_table_name).cloned(),
                snapshot_skip_summary: snapshot_skip_summaries.get(&root_table_name).cloned(),
                count_diff_rows: Vec::new(),
                kind: CountNodeKind::Root,
                // Resolve child table names from mapping mongo_path first so
                // prefixed names like status_place remain attached to the
                // correct source path in post-import reports.
                children: build_field_nodes(
                    &root_table_name,
                    &Vec::new(),
                    &schema.object,
                    schema_name.as_deref(),
                    &load_table_name_by_mongo_path(
                        schema_path
                            .parent()
                            .unwrap_or(collections_dir_for_lookup.as_path()),
                    ),
                    &table_rows,
                    &global_table_rows,
                    &md5_summaries,
                    &snapshot_skip_summaries,
                ),
            };

            let mut cursor = mongo_db
                .collection::<bson::Document>(&coll_name)
                .find(bson::doc! {})
                .await
                .with_context(|| {
                    format!("Failed to scan MongoDB documents for {db_name}.{coll_name}")
                })?;
            info!(
                "[{}/{}] 🔎 scan nested MongoDB fields for {}.{} (docs={})",
                index + 1,
                total_collections,
                db_name,
                coll_name,
                document_count
            );
            let mut scanned_docs: usize = 0;
            let nested_scan_log_every: usize = 10_000;
            while let Some(doc) = cursor.try_next().await.with_context(|| {
                format!("Failed to iterate MongoDB documents for {db_name}.{coll_name}")
            })? {
                count_children(&mut root.children, &doc);
                scanned_docs += 1;
                if scanned_docs % nested_scan_log_every == 0 {
                    info!(
                        "[{}/{}]↳ scanned {} / {} MongoDB docs for {}.{} nested counts",
                        index + 1,
                        total_collections,
                        scanned_docs,
                        document_count,
                        db_name,
                        coll_name,
                    );
                }
            }
            info!(
                "[{}/{}] ✅ nested MongoDB scan complete for {}.{} (scanned={})",
                index + 1,
                total_collections,
                db_name,
                coll_name,
                scanned_docs
            );

            let mut mismatch_tables = Vec::new();
            collect_rowcount_mismatch_tables(&root, &mut mismatch_tables);
            mismatch_tables.sort();
            mismatch_tables.dedup();

            if !mismatch_tables.is_empty() {
                for table_name in &mismatch_tables {
                    info!(
                        "[{}/{}] Rowcount diff detected for {}.{} table {}: searching first 5 differences",
                        index + 1,
                        total_collections,
                        db_name,
                        coll_name,
                        table_name,
                    );
                }

                let mismatch_set = mismatch_tables.iter().cloned().collect::<HashSet<_>>();
                let mut rows_by_table = HashMap::new();

                if !md5_summaries.is_empty() {
                    for (table_name, summary) in &md5_summaries {
                        if !mismatch_set.contains(table_name) {
                            continue;
                        }
                        rows_by_table.insert(
                            table_name.clone(),
                            summary
                                .mismatches
                                .iter()
                                .map(|mismatch| PostImportCountDiffRow {
                                    row_index: mismatch.row_index,
                                    mongo_values: mismatch.mongo_values.clone(),
                                    pg_values: mismatch.pg_values.clone(),
                                })
                                .collect::<Vec<_>>(),
                        );
                    }
                } else {
                    match compute_md5_summaries_for_collection_with_collections_root(
                        &coll_name,
                        config_path,
                        Some(&collections_dir_for_lookup),
                    )
                    .await
                    {
                        Ok(summaries) => {
                            for summary in summaries {
                                if !mismatch_set.contains(&summary.table_name) {
                                    continue;
                                }
                                rows_by_table.insert(
                                    summary.table_name,
                                    summary
                                        .summary
                                        .mismatches
                                        .into_iter()
                                        .map(|mismatch| PostImportCountDiffRow {
                                            row_index: mismatch.row_index,
                                            mongo_values: mismatch.mongo_values,
                                            pg_values: mismatch.pg_values,
                                        })
                                        .collect::<Vec<_>>(),
                                );
                            }
                        }
                        Err(err) => {
                            warn!(
                                "failed to collect count differences for {}.{}: {:#}",
                                db_name, coll_name, err
                            );
                        }
                    }
                }

                apply_count_diff_rows(&mut root, &rows_by_table);
            }

            into_post_import_node(root)
        } else {
            PostImportNode {
                name: coll_name.clone(),
                is_array: false,
                mongo_count: document_count,
                pg_table_name: None,
                pg_row_count: None,
                md5_summary: None,
                snapshot_skip_summary: None,
                count_diff_rows: Vec::new(),
                children: Vec::new(),
            }
        };

        rows.push(PostImportCollectionRow {
            name: coll_name,
            document_count,
            root,
        });
        info!(
            "[{}/{}] ✅ post-import report collection completed",
            index + 1,
            total_collections
        );
    }

    Ok(rows)
}
