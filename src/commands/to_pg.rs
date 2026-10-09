//! Collection grouping (post-infer consolidation) and the `to-pg` subcommand:
//! converts inferred schema JSON files into PostgreSQL DDL.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use log::{info, warn};

use crate::cli::ToPgArgs;
use crate::commands::infer::{
    move_infer_artifacts_to_gcs_if_needed_with_project_root,
    should_regenerate_from_schema_when_objectid_pk,
};
use crate::commands::shared::{
    apply_config_overrides, extract_postgres_uri_username, load_mapping_ddl_tables,
    normalize_pg_identifier, quote_ident, render_ddl_from_mapping_tables,
    render_ddl_from_mapping_tables_with_owner, resolve_local_project_root_from_config,
    resolve_preamble_database_name, resolve_target_database_name_from_conf,
    sanitize_name, stage_source_collections_from_gcs, CollectionMapping, ConfigOverrides,
    DdlColumnMapping, DdlTableMapping, MappingColumn,
};
use crate::db::pg::connect_client as connect_pg_client;
use crate::engine::analyzer::CollectionSchema;
use crate::engine::ddl::schema_to_ddl_with_timestamp_fields_and_owner;
use crate::export::{resolve_export_write_backend, ExportWriteBackend};
use crate::util::{
    connection_failed_context, read_conf, resolve_target_mapping_for_namespace_index,
    should_infer_collection_for_database,
};

pub struct CollectionGroup {
    /// Shared table name = prefix (e.g. "events" for events_lmfr / events_lmza).
    pub prefix: String,
    /// All collection names in the group.
    pub members: Vec<String>,
    /// First alphabetical member used as the schema representative.
    pub representative: String,
}

/// Detect candidate groups from a list of collection names.
/// Groups by prefix (everything before the last `_`); only groups with ≥2 members qualify.
pub fn detect_candidate_groups(names: &[String]) -> Vec<CollectionGroup> {
    let mut by_prefix: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();

    for name in names {
        if let Some(pos) = name.rfind('_') {
            let prefix = name[..pos].to_owned();
            if !prefix.is_empty() {
                by_prefix.entry(prefix).or_default().push(name.clone());
            }
        }
    }

    by_prefix
        .into_iter()
        .filter(|(_, members)| members.len() >= 2)
        .map(|(prefix, mut members)| {
            members.sort();
            let representative = members[0].clone();
            CollectionGroup {
                prefix,
                members,
                representative,
            }
        })
        .collect()
}

/// Build a sorted set of top-level field names from a collection's inferred JSON schema.
pub fn collection_field_signature(
    collections_dir: &Path,
    coll_name: &str,
) -> Result<std::collections::BTreeSet<String>> {
    let safe_name = coll_name.replace('/', "_");
    let json_path = collections_dir
        .join(&safe_name)
        .join(format!("{safe_name}.json"));
    let content = std::fs::read_to_string(&json_path)
        .with_context(|| format!("Cannot read {}", json_path.display()))?;
    let schema: CollectionSchema = serde_json::from_str(&content)
        .with_context(|| format!("Cannot parse {}", json_path.display()))?;
    Ok(schema.object.keys().cloned().collect())
}

/// Returns true when all member schema artifacts are readable.
///
/// Grouped merge now tolerates sparse schemas (missing optional fields) and
/// normalizes grouped mappings to the union of observed fields.
pub fn validate_group_schema_compatibility(
    collections_dir: &Path,
    group: &CollectionGroup,
) -> bool {
    group
        .members
        .iter()
        .all(|member| collection_field_signature(collections_dir, member).is_ok())
}

/// Rewrite each group member's mapping YAML to use the shared `prefix` table name.
/// When `add_grouped_key` is true also inserts a `_key TEXT` column carrying the
/// collection suffix as a `literal_value`.
pub fn apply_grouping_to_mappings(
    collections_dir: &Path,
    group: &CollectionGroup,
    add_grouped_key: bool,
) -> Result<()> {
    #[derive(Clone)]
    struct CanonicalColumn {
        source_field: String,
        data_type: String,
        sql_type: String,
        nullable: bool,
        primary_key: bool,
    }

    let mut loaded_members: Vec<(String, PathBuf, CollectionMapping)> = Vec::new();

    for member in &group.members {
        let safe_name = member.replace('/', "_");
        let member_dir = collections_dir.join(&safe_name);
        let exact_mapping_path = member_dir.join(format!("mapping_{safe_name}.yaml"));
        let mapping_path = if exact_mapping_path.exists() {
            exact_mapping_path
        } else {
            let mut candidates = std::fs::read_dir(&member_dir)
                .with_context(|| format!("Cannot read {}", member_dir.display()))?
                .filter_map(|entry| entry.ok())
                .map(|entry| entry.path())
                .filter(|path| {
                    path.file_name()
                        .and_then(|name| name.to_str())
                        .map(|name| name.starts_with("mapping_") && name.ends_with(".yaml"))
                        .unwrap_or(false)
                })
                .collect::<Vec<_>>();
            candidates.sort();
            if let Some(path) = candidates.into_iter().next() {
                path
            } else {
                warn!(
                    "grouping skip member={} reason=mapping_not_found path={}",
                    member,
                    exact_mapping_path.display()
                );
                continue;
            }
        };

        let content = std::fs::read_to_string(&mapping_path)
            .with_context(|| format!("Cannot read {}", mapping_path.display()))?;
        let mapping: CollectionMapping = serde_yaml::from_str(&content)
            .with_context(|| format!("Cannot parse {}", mapping_path.display()))?;
        loaded_members.push((member.clone(), mapping_path, mapping));
    }

    if loaded_members.is_empty() {
        return Err(anyhow!(
            "No mapping files loaded for group prefix={} members=[{}]",
            group.prefix,
            group.members.join(", ")
        ));
    }

    let mut canonical: std::collections::BTreeMap<String, CanonicalColumn> =
        std::collections::BTreeMap::new();

    for (_, _, mapping) in &loaded_members {
        let ddl_by_name = mapping
            .pg_mapping
            .ddl
            .as_ref()
            .map(|ddl| {
                ddl.columns
                    .iter()
                    .map(|c| (normalize_pg_identifier(&c.name), c))
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default();

        for column in &mapping.pg_mapping.columns {
            if column.target_field == "_key" {
                continue;
            }

            let target = normalize_pg_identifier(&column.target_field);
            let ddl_col = ddl_by_name.get(&target);
            let sql_type = ddl_col
                .map(|c| c.sql_type.clone())
                .unwrap_or_else(|| column.data_type.to_uppercase());
            let primary_key = ddl_col.map(|c| c.primary_key).unwrap_or(target == "id");
            let nullable = ddl_col.map(|c| c.nullable).unwrap_or(column.nullable);

            let candidate = CanonicalColumn {
                source_field: if column.source_field.trim().is_empty() {
                    target.clone()
                } else {
                    column.source_field.clone()
                },
                data_type: column.data_type.clone(),
                sql_type,
                nullable,
                primary_key,
            };

            canonical
                .entry(target)
                .and_modify(|existing| {
                    if existing.source_field.is_empty() && !candidate.source_field.is_empty() {
                        existing.source_field = candidate.source_field.clone();
                    }
                    if !existing
                        .data_type
                        .eq_ignore_ascii_case(candidate.data_type.as_str())
                    {
                        existing.data_type = "text".to_owned();
                    }
                    if !existing
                        .sql_type
                        .eq_ignore_ascii_case(candidate.sql_type.as_str())
                    {
                        existing.sql_type = "TEXT".to_owned();
                    }
                    existing.nullable = existing.nullable || candidate.nullable;
                    existing.primary_key = existing.primary_key || candidate.primary_key;
                })
                .or_insert(candidate);
        }
    }

    let mut ordered_targets = canonical.keys().cloned().collect::<Vec<_>>();
    ordered_targets.sort();
    if let Some(id_pos) = ordered_targets.iter().position(|name| name == "id") {
        let id = ordered_targets.remove(id_pos);
        ordered_targets.insert(0, id);
    }

    for (member, mapping_path, mut mapping) in loaded_members {
        let suffix = member
            .strip_prefix(&group.prefix)
            .and_then(|s| s.strip_prefix('_'))
            .unwrap_or(member.as_str())
            .to_owned();

        let existing_by_target = mapping
            .pg_mapping
            .columns
            .iter()
            .map(|c| (normalize_pg_identifier(&c.target_field), c.clone()))
            .collect::<HashMap<_, _>>();

        let mut normalized_columns = ordered_targets
            .iter()
            .filter_map(|target| {
                let canonical_col = canonical.get(target)?;
                let existing = existing_by_target.get(target);
                Some(MappingColumn {
                    source_field: existing
                        .map(|c| c.source_field.clone())
                        .filter(|s| !s.trim().is_empty())
                        .unwrap_or_else(|| canonical_col.source_field.clone()),
                    target_field: target.clone(),
                    data_type: canonical_col.data_type.clone(),
                    nullable: canonical_col.nullable,
                    literal_value: None,
                })
            })
            .collect::<Vec<_>>();

        if add_grouped_key {
            normalized_columns.push(MappingColumn {
                source_field: String::new(),
                target_field: "_key".to_owned(),
                data_type: "text".to_owned(),
                nullable: true,
                literal_value: Some(suffix),
            });
        }

        let existing_foreign_keys = mapping
            .pg_mapping
            .ddl
            .as_ref()
            .map(|d| d.foreign_keys.clone())
            .unwrap_or_default();

        let mut normalized_ddl_columns = ordered_targets
            .iter()
            .filter_map(|target| {
                let canonical_col = canonical.get(target)?;
                Some(DdlColumnMapping {
                    name: target.clone(),
                    sql_type: canonical_col.sql_type.clone(),
                    nullable: canonical_col.nullable,
                    primary_key: canonical_col.primary_key,
                })
            })
            .collect::<Vec<_>>();

        if add_grouped_key {
            normalized_ddl_columns.push(DdlColumnMapping {
                name: "_key".to_owned(),
                sql_type: "TEXT".to_owned(),
                nullable: true,
                primary_key: false,
            });
        }

        // Update table and schema to shared prefix, then write normalized union mapping.
        mapping.pg_mapping.table_name = group.prefix.clone();
        mapping.pg_mapping.schema_name = group.prefix.clone();
        mapping.pg_mapping.columns = normalized_columns;
        mapping.pg_mapping.ddl = Some(DdlTableMapping {
            name: group.prefix.clone(),
            columns: normalized_ddl_columns,
            foreign_keys: existing_foreign_keys,
        });

        let updated = serde_yaml::to_string(&mapping)
            .with_context(|| format!("Cannot serialize {}", mapping_path.display()))?;
        std::fs::write(&mapping_path, updated)
            .with_context(|| format!("Cannot write {}", mapping_path.display()))?;
    }

    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────────
// `to-pg` subcommand
// ──────────────────────────────────────────────────────────────────────────────

struct RunToPgConfigValues {
    db_name: Option<String>,
    target_schema: Option<String>,
    schema_owner: Option<String>,
    timestamp_fields: Vec<String>,
}

fn prepend_database_preamble(ddl: String, db_name: Option<&str>) -> String {
    match db_name {
        None => ddl,
        Some(name) => {
            let quoted_name = quote_ident(name);
            format!("--CREATE DATABASE {quoted_name};\n\\connect {quoted_name}\n\n{ddl}")
        }
    }
}

async fn preflight_ping_target_if_configured(args: &ToPgArgs) -> Result<()> {
    let Some(conf) = args.config.as_deref() else {
        return Ok(());
    };

    apply_config_overrides(
        conf,
        &ConfigOverrides {
            project_dir: args.project_dir.clone(),
            target_schema_name: args.schema.clone(),
            ..ConfigOverrides::default()
        },
    )?;

    let c = read_conf(conf)?;
    let target_uri = c
        .target_uri
        .as_deref()
        .ok_or_else(|| anyhow!("No TARGET_URI provided: add TARGET_URI to the config file"))?;
    info!("preflight ping target: begin");
    let pg_client = connect_pg_client(target_uri).await?;
    pg_client
        .query_one("SELECT 1", &[])
        .await
        .with_context(|| {
            format!(
                "{}: failed PostgreSQL ping query",
                connection_failed_context("pg", "query")
            )
        })?;
    info!("preflight ping target: ok");

    Ok(())
}

fn read_run_to_pg_config_values(args: &ToPgArgs) -> Result<RunToPgConfigValues> {
    let Some(conf) = args.config.as_deref() else {
        return Ok(RunToPgConfigValues {
            db_name: None,
            target_schema: None,
            schema_owner: None,
            timestamp_fields: Vec::new(),
        });
    };

    let c = read_conf(conf)?;
    let db_name = resolve_target_database_name_from_conf(
        c.target_database_name.as_deref(),
        c.namespace.as_deref(),
    );
    let schema_owner = c
        .target_uri
        .as_deref()
        .and_then(extract_postgres_uri_username);

    Ok(RunToPgConfigValues {
        db_name,
        target_schema: c.target_schema,
        schema_owner,
        timestamp_fields: c.timestamp_fields,
    })
}

async fn resolve_collections_and_output_dirs(
    args: &ToPgArgs,
) -> Result<(PathBuf, PathBuf, Option<tempfile::TempDir>)> {
    if let Some(conf) = args.config.as_deref() {
        let c = read_conf(conf)?;
        let mut source_stage = None;
        let local_project_root = match resolve_export_write_backend(&c.base_dir)? {
            ExportWriteBackend::LocalFs => resolve_local_project_root_from_config(conf, &c),
            ExportWriteBackend::Gcs { bucket, prefix } => {
                let stage = stage_source_collections_from_gcs(
                    &bucket,
                    &prefix,
                    c.cluster_name.as_deref(),
                    &c.project_dir,
                )
                .await?;
                let root = stage.path().to_path_buf();
                source_stage = Some(stage);
                root
            }
        };
        let cols = local_project_root.join("source").join("collections");
        let sql_out = local_project_root.join("schema").join("tables");
        Ok((cols, sql_out, source_stage))
    } else {
        let dir = args
            .output_dir
            .clone()
            .ok_or_else(|| anyhow!("Provide -c <config> or -o <output-dir>"))?;
        Ok((dir.clone(), dir, None))
    }
}

fn collect_single_collection_json_file(
    collections_dir: &Path,
    name: &str,
    config_db_name: Option<&str>,
) -> Result<Vec<(PathBuf, PathBuf)>> {
    let flat = collections_dir.join(name).join(format!("{name}.json"));
    if flat.exists() {
        let rel_sql = if let Some(db_name) = config_db_name {
            PathBuf::from(db_name).join(format!("{}.sql", sanitize_name(name)))
        } else {
            PathBuf::from(format!("{}.sql", sanitize_name(name)))
        };
        return Ok(vec![(rel_sql, flat)]);
    }

    if name.contains('/') {
        let json = collections_dir.join(name).join({
            let coll = name.split('/').next_back().unwrap_or(name);
            format!("{coll}.json")
        });
        return Ok(vec![(
            PathBuf::from(format!("{}.sql", sanitize_name(name))),
            json,
        )]);
    }

    Err(anyhow!(
        "Collection '{}' not found under {}",
        name,
        collections_dir.display()
    ))
}

fn collect_all_collection_json_files(
    collections_dir: &Path,
    config_db_name: Option<&str>,
) -> Result<Vec<(PathBuf, PathBuf)>> {
    let mut entries: Vec<(PathBuf, PathBuf)> = Vec::new();

    let top_dirs = std::fs::read_dir(collections_dir)
        .with_context(|| format!("Cannot read {}", collections_dir.display()))?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir());

    for top in top_dirs {
        let top_path = top.path();
        let top_name = top.file_name().to_string_lossy().into_owned();
        let direct_json = top_path.join(format!("{top_name}.json"));

        if direct_json.exists() {
            let rel_sql = if let Some(db_name) = config_db_name {
                PathBuf::from(db_name).join(format!("{}.sql", sanitize_name(&top_name)))
            } else {
                PathBuf::from(format!("{}.sql", sanitize_name(&top_name)))
            };
            entries.push((rel_sql, direct_json));
            continue;
        }

        let mut sub_dirs: Vec<(PathBuf, PathBuf)> = std::fs::read_dir(&top_path)
            .with_context(|| format!("Cannot read {}", top_path.display()))?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .filter_map(|e| {
                let coll_name = e.file_name().to_string_lossy().into_owned();
                let json = e.path().join(format!("{coll_name}.json"));
                if json.exists() {
                    Some((
                        PathBuf::from(format!(
                            "{}/{}.sql",
                            sanitize_name(&top_name),
                            sanitize_name(&coll_name)
                        )),
                        json,
                    ))
                } else {
                    None
                }
            })
            .collect();
        sub_dirs.sort_by(|a, b| a.0.cmp(&b.0));
        entries.extend(sub_dirs);
    }

    entries.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(entries)
}

fn collect_json_files_to_process(
    args: &ToPgArgs,
    collections_dir: &Path,
    config_db_name: Option<&str>,
) -> Result<Vec<(PathBuf, PathBuf)>> {
    let mut json_files = if let Some(name) = args.collection.as_deref() {
        collect_single_collection_json_file(collections_dir, name, config_db_name)?
    } else {
        collect_all_collection_json_files(collections_dir, config_db_name)?
    };

    if let Some(config_path) = args.config.as_deref() {
        let config = read_conf(config_path)?;
        if let Some(database) = config.namespace.as_deref() {
            json_files.retain(|(_, path)| {
                let collection = path
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .unwrap_or_default();
                should_infer_collection_for_database(
                    database,
                    collection,
                    &config.include,
                    &config.exclude,
                )
            });
        }
    }

    Ok(json_files)
}

fn resolve_add_grouped_key_from_config(args: &ToPgArgs) -> bool {
    args.config
        .as_deref()
        .and_then(|conf| read_conf(Path::new(conf)).ok())
        .map(|c| c.add_grouped_key)
        .unwrap_or(false)
}

fn prepare_grouping_state(
    collections_dir: &Path,
    json_files: &[(PathBuf, PathBuf)],
    add_grouped_key: bool,
) -> (HashMap<String, String>, HashSet<String>) {
    let mut grouped_table_for: HashMap<String, String> = HashMap::new();
    let mut group_representatives: HashSet<String> = HashSet::new();

    if !add_grouped_key {
        info!("grouping disabled: add_grouped_key=false");
        return (grouped_table_for, group_representatives);
    }

    let collection_names_for_grouping: Vec<String> = json_files
        .iter()
        .filter_map(|(rel_sql, _)| {
            rel_sql
                .file_stem()
                .and_then(|s| s.to_str())
                .map(|s| s.to_owned())
        })
        .collect();

    for group in detect_candidate_groups(&collection_names_for_grouping) {
        if !validate_group_schema_compatibility(collections_dir, &group) {
            warn!(
                "grouping_skipped prefix='{}' members=[{}] reason=schema_mismatch",
                group.prefix,
                group.members.join(", ")
            );
            continue;
        }
        info!(
            "grouping prefix='{}' members=[{}]",
            group.prefix,
            group.members.join(", ")
        );
        match apply_grouping_to_mappings(collections_dir, &group, add_grouped_key) {
            Ok(()) => {
                group_representatives.insert(group.representative.clone());
                for member in &group.members {
                    grouped_table_for.insert(member.clone(), group.prefix.clone());
                }
            }
            Err(e) => {
                warn!(
                    "grouping_skipped prefix='{}' reason=mapping_update_failed error={:#}",
                    group.prefix, e
                );
            }
        }
    }

    (grouped_table_for, group_representatives)
}

fn resolve_effective_rel_sql(rel_sql: &Path, grouped_prefix: Option<&str>) -> PathBuf {
    if let Some(prefix) = grouped_prefix {
        if let Some(parent) = rel_sql.parent() {
            parent.join(format!("{prefix}.sql"))
        } else {
            PathBuf::from(format!("{prefix}.sql"))
        }
    } else {
        rel_sql.to_path_buf()
    }
}

#[allow(clippy::too_many_arguments)]
fn write_sql_for_collection(
    args: &ToPgArgs,
    quiet: bool,
    collections_dir: &Path,
    output_dir: &Path,
    json_path: &Path,
    effective_rel_sql: &Path,
    grouped_prefix: Option<&str>,
    config_db_name: Option<&str>,
    config_target_schema: Option<&str>,
    config_schema_owner: Option<&str>,
    config_timestamp_fields: &[String],
) -> Result<()> {
    let sql_path = output_dir.join(effective_rel_sql);
    if let Some(parent) = sql_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create directory {}", parent.display()))?;
    }

    let table_name = args.table.clone().unwrap_or_else(|| {
        effective_rel_sql
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("table")
            .to_owned()
    });
    let content = std::fs::read_to_string(json_path)
        .with_context(|| format!("Failed to read {}", json_path.display()))?;
    let schema: CollectionSchema = serde_json::from_str(&content)
        .with_context(|| format!("Failed to parse {}", json_path.display()))?;
    let target_schema = args
        .schema
        .as_deref()
        .or(config_target_schema)
        .or(grouped_prefix)
        .or(Some(table_name.as_str()));
    let mapping_tables = load_mapping_ddl_tables(json_path.parent().unwrap_or(collections_dir))?;

    let force_schema_inference_for_objectid = mapping_tables.as_ref().is_some_and(|tables| {
        should_regenerate_from_schema_when_objectid_pk(&schema, tables, &table_name)
    });

    if force_schema_inference_for_objectid && !quiet {
        warn!(
            "mapping DDL for '{}' uses surrogate BIGSERIAL id while source _id is ObjectId; using schema-inferred DDL to preserve UUID mapping",
            table_name
        );
    }

    let ddl = if let Some(mapping_tables) =
        mapping_tables.filter(|_| !force_schema_inference_for_objectid)
    {
        if config_schema_owner.is_some() {
            render_ddl_from_mapping_tables_with_owner(
                &mapping_tables,
                target_schema,
                config_schema_owner,
            )
        } else {
            render_ddl_from_mapping_tables(&mapping_tables, target_schema)
        }
    } else {
        schema_to_ddl_with_timestamp_fields_and_owner(
            &schema,
            &table_name,
            target_schema,
            config_schema_owner,
            config_timestamp_fields,
        )
    };

    let preamble_db_name = resolve_preamble_database_name(config_db_name, effective_rel_sql);
    let ddl = prepend_database_preamble(ddl, preamble_db_name.as_deref());

    std::fs::write(&sql_path, &ddl)
        .with_context(|| format!("Failed to write {}", sql_path.display()))?;
    if !quiet {
        info!("SQL written to {}", sql_path.display());
    }

    Ok(())
}

fn log_to_pg_completion(quiet: bool) {
    if quiet {
        return;
    }
    info!(
        "to-pg completed. Review the generated SQL files to confirm that schema names and table names suit your needs."
    );
    info!(
        "Also check that table and column names do not exceed PostgreSQL's 63-byte identifier limit."
    );
    info!(
        "If you modify those SQL files, the next export and report commands will use them as their source of truth."
    );
}

async fn sync_infer_artifacts_if_gcs(
    config_path: Option<&Path>,
    source_stage: Option<&tempfile::TempDir>,
) -> Result<()> {
    if let (Some(conf), Some(stage)) = (config_path, source_stage) {
        if matches!(
            resolve_export_write_backend(&read_conf(conf)?.base_dir)?,
            ExportWriteBackend::Gcs { .. }
        ) {
            move_infer_artifacts_to_gcs_if_needed_with_project_root(conf, Some(stage.path()))
                .await?;
        }
    }
    Ok(())
}

pub async fn run_to_pg(args: ToPgArgs, quiet: bool) -> Result<()> {
    preflight_ping_target_if_configured(&args).await?;

    if let Some(conf) = args.config.as_deref() {
        let c = read_conf(conf)?;
        if !c.namespace_databases.is_empty() && args.collection.is_none() {
            return run_to_pg_for_configured_databases(&args, quiet, conf, &c).await;
        }
    }

    let config_values = read_run_to_pg_config_values(&args)?;
    let (collections_dir, output_dir, source_stage) =
        resolve_collections_and_output_dirs(&args).await?;
    run_to_pg_for_dirs(&args, quiet, &collections_dir, &output_dir, &config_values, false)?;
    log_to_pg_completion(quiet);
    sync_infer_artifacts_if_gcs(args.config.as_deref(), source_stage.as_ref()).await?;
    Ok(())
}

/// Generates PostgreSQL DDL for each explicitly configured database
/// (`[source].namespace` entries), reading from and writing to
/// each database's isolated `<database_name>/source` and
/// `<database_name>/schema` directories under the project root.
async fn run_to_pg_for_configured_databases(
    args: &ToPgArgs,
    quiet: bool,
    conf: &Path,
    c: &crate::util::ConfData,
) -> Result<()> {
    let (project_root, _source_stage) = match resolve_export_write_backend(&c.base_dir)? {
        ExportWriteBackend::LocalFs => (resolve_local_project_root_from_config(conf, c), None),
        ExportWriteBackend::Gcs { bucket, prefix } => {
            let stage = stage_source_collections_from_gcs(
                &bucket,
                &prefix,
                c.cluster_name.as_deref(),
                &c.project_dir,
            )
            .await?;
            (stage.path().to_path_buf(), Some(stage))
        }
    };
    let schema_owner = c
        .target_uri
        .as_deref()
        .and_then(extract_postgres_uri_username);

    for (idx, db_name) in c.namespace_databases.iter().enumerate() {
        let collections_dir =
            crate::commands::shared::multi_db_source_collections_dir(&project_root, db_name);
        if !collections_dir.is_dir() {
            warn!(
                "Skipping to-pg for database '{db_name}': no source collections found at {}",
                collections_dir.display()
            );
            continue;
        }

        let (target_db_name, target_schema_name) =
            resolve_target_mapping_for_namespace_index(c, idx, db_name);
        let output_dir = crate::commands::shared::multi_db_schema_tables_dir(
            &project_root,
            &target_db_name,
        );

        let config_values = RunToPgConfigValues {
            db_name: Some(target_db_name),
            target_schema: Some(target_schema_name),
            schema_owner: schema_owner.clone(),
            timestamp_fields: c.timestamp_fields.clone(),
        };

        if let Err(err) =
            run_to_pg_for_dirs(args, quiet, &collections_dir, &output_dir, &config_values, true)
        {
            warn!("to-pg failed for database '{db_name}': {err:#}");
        }
    }

    log_to_pg_completion(quiet);
    if matches!(resolve_export_write_backend(&c.base_dir)?, ExportWriteBackend::Gcs { .. }) {
        move_infer_artifacts_to_gcs_if_needed_with_project_root(conf, Some(&project_root)).await?;
    }
    Ok(())
}

fn run_to_pg_for_dirs(
    args: &ToPgArgs,
    quiet: bool,
    collections_dir: &Path,
    output_dir: &Path,
    config_values: &RunToPgConfigValues,
    output_dir_is_database_scoped: bool,
) -> Result<()> {
    let json_files =
        collect_json_files_to_process(
            args,
            collections_dir,
            (!output_dir_is_database_scoped).then_some(config_values.db_name.as_deref()).flatten(),
        )?;
    if json_files.is_empty() {
        warn!(
            "No JSON schema files found in {}",
            collections_dir.display()
        );
        return Ok(());
    }

    let add_grouped_key = resolve_add_grouped_key_from_config(args);
    let (grouped_table_for, group_representatives) =
        prepare_grouping_state(collections_dir, &json_files, add_grouped_key);

    for (rel_sql, json_path) in &json_files {
        let coll_stem = rel_sql.file_stem().and_then(|s| s.to_str()).unwrap_or("");
        let grouped_prefix = grouped_table_for.get(coll_stem).map(String::as_str);
        let is_non_representative_group_member =
            grouped_prefix.is_some() && !group_representatives.contains(coll_stem);

        if is_non_representative_group_member {
            let stale_sql_path = output_dir.join(rel_sql);
            if stale_sql_path.exists() {
                std::fs::remove_file(&stale_sql_path).with_context(|| {
                    format!(
                        "Failed to remove stale grouped SQL {}",
                        stale_sql_path.display()
                    )
                })?;
            }
            continue;
        }

        let effective_rel_sql = resolve_effective_rel_sql(rel_sql, grouped_prefix);
        write_sql_for_collection(
            args,
            quiet,
            collections_dir,
            output_dir,
            json_path,
            &effective_rel_sql,
            grouped_prefix,
            config_values.db_name.as_deref(),
            config_values.target_schema.as_deref(),
            config_values.schema_owner.as_deref(),
            &config_values.timestamp_fields,
        )?;
    }

    Ok(())
}
