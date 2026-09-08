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
    stage_source_collections_from_gcs, CollectionMapping, ConfigOverrides, DdlColumnMapping,
    DdlTableMapping, MappingColumn,
};
use crate::db::pg::connect_client as connect_pg_client;
use crate::engine::analyzer::CollectionSchema;
use crate::engine::ddl::schema_to_ddl_with_timestamp_fields_and_owner;
use crate::export::{resolve_export_write_backend, ExportWriteBackend};
use crate::util::{connection_failed_context, read_conf};

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

pub async fn run_to_pg(args: ToPgArgs, quiet: bool) -> Result<()> {
    fn prepend_database_preamble(ddl: String, db_name: Option<&str>) -> String {
        match db_name {
            None => ddl,
            Some(name) => {
                let quoted_name = quote_ident(name);
                format!("--CREATE DATABASE {quoted_name};\n\\connect {quoted_name}\n\n{ddl}")
            }
        }
    }

    if let Some(conf) = args.config.as_deref() {
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
    }

    // Resolve collections source dir and SQL output dir
    let config_db_name: Option<String> = if let Some(ref conf) = args.config {
        let c = read_conf(conf)?;
        resolve_target_database_name_from_conf(
            c.target_database_name.as_deref(),
            c.namespace.as_deref(),
        )
    } else {
        None
    };

    let config_target_schema: Option<String> = if let Some(ref conf) = args.config {
        read_conf(conf)?.target_schema
    } else {
        None
    };

    let config_schema_owner: Option<String> = if let Some(ref conf) = args.config {
        let c = read_conf(conf)?;
        c.target_uri
            .as_deref()
            .and_then(extract_postgres_uri_username)
    } else {
        None
    };

    let config_timestamp_fields: Vec<String> = if let Some(ref conf) = args.config {
        read_conf(conf)?.timestamp_fields
    } else {
        Vec::new()
    };

    let mut source_stage = None;
    let (collections_dir, output_dir) = if let Some(ref conf) = args.config {
        let c = read_conf(conf)?;
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
        (cols, sql_out)
    } else {
        let dir = args
            .output_dir
            .clone()
            .ok_or_else(|| anyhow!("Provide -c <config> or -o <output-dir>"))?;
        (dir.clone(), dir)
    };
    // Collect (output_subpath, json_path) pairs to process.
    //
    // Two layouts are supported:
    //   Flat:    <collections_dir>/<name>/<name>.json          → SQL: <output_dir>/<name>.sql
    //   Per-db:  <collections_dir>/<db>/<coll>/<coll>.json     → SQL: <output_dir>/<db>/<coll>.sql
    //
    // A directory is treated as a db folder when it contains no direct .json file
    // but does contain subdirectories.
    let json_files: Vec<(PathBuf, PathBuf)> = if let Some(ref name) = args.collection {
        // Single collection specified – try flat layout first, then per-db.
        let flat = collections_dir.join(name).join(format!("{name}.json"));
        if flat.exists() {
            let rel_sql = if let Some(db_name) = config_db_name.as_deref() {
                PathBuf::from(db_name).join(format!("{}.sql", name.to_lowercase()))
            } else {
                PathBuf::from(format!("{}.sql", name.to_lowercase()))
            };
            vec![(rel_sql, flat)]
        } else if name.contains('/') {
            // Caller passed "db/collection"
            let json = collections_dir.join(name).join({
                let coll = name.split('/').next_back().unwrap_or(name);
                format!("{coll}.json")
            });
            vec![(PathBuf::from(format!("{}.sql", name.to_lowercase())), json)]
        } else {
            return Err(anyhow!(
                "Collection '{}' not found under {}",
                name,
                collections_dir.display()
            ));
        }
    } else {
        let mut entries: Vec<(PathBuf, PathBuf)> = Vec::new();

        let top_dirs = std::fs::read_dir(&collections_dir)
            .with_context(|| format!("Cannot read {}", collections_dir.display()))?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir());

        for top in top_dirs {
            let top_path = top.path();
            let top_name = top.file_name().to_string_lossy().into_owned();
            let direct_json = top_path.join(format!("{top_name}.json"));

            if direct_json.exists() {
                // Flat layout: <collections_dir>/<name>/<name>.json
                let rel_sql = if let Some(db_name) = config_db_name.as_deref() {
                    PathBuf::from(db_name).join(format!("{}.sql", top_name.to_lowercase()))
                } else {
                    PathBuf::from(format!("{}.sql", top_name.to_lowercase()))
                };
                entries.push((rel_sql, direct_json));
            } else {
                // Per-db layout: treat this dir as a database folder
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
                                    top_name.to_lowercase(),
                                    coll_name.to_lowercase()
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
        }

        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
    };

    if json_files.is_empty() {
        warn!(
            "No JSON schema files found in {}",
            collections_dir.display()
        );
        return Ok(());
    }

    // Post-infer grouping: detect collections sharing a prefix_suffix pattern and
    // consolidate them into a single shared PostgreSQL table.
    let config_add_grouped_key = if let Some(ref conf) = args.config {
        read_conf(Path::new(conf))
            .map(|c| c.add_grouped_key)
            .unwrap_or(false)
    } else {
        false
    };

    // Collect flat collection names (stem of rel_sql, no path prefix, no .sql).
    let collection_names_for_grouping: Vec<String> = json_files
        .iter()
        .filter_map(|(rel_sql, _)| {
            rel_sql
                .file_stem()
                .and_then(|s| s.to_str())
                .map(|s| s.to_owned())
        })
        .collect();

    // Track which collections belong to a merged group and which is representative.
    let mut grouped_table_for: HashMap<String, String> = HashMap::new(); // coll_name → shared_table
    let mut group_representatives: HashSet<String> = HashSet::new();

    if config_add_grouped_key {
        for group in detect_candidate_groups(&collection_names_for_grouping) {
            if !validate_group_schema_compatibility(&collections_dir, &group) {
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
            match apply_grouping_to_mappings(&collections_dir, &group, config_add_grouped_key) {
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
    } else {
        info!("grouping disabled: add_grouped_key=false");
    }

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

        let effective_rel_sql = if let Some(prefix) = grouped_prefix {
            if let Some(parent) = rel_sql.parent() {
                parent.join(format!("{prefix}.sql"))
            } else {
                PathBuf::from(format!("{prefix}.sql"))
            }
        } else {
            rel_sql.clone()
        };

        let sql_path = output_dir.join(&effective_rel_sql);
        if let Some(parent) = sql_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create directory {}", parent.display()))?;
        }
        let table_name = args.table.as_deref().unwrap_or_else(|| {
            effective_rel_sql
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("table")
        });
        let content = std::fs::read_to_string(json_path)
            .with_context(|| format!("Failed to read {}", json_path.display()))?;
        let schema: CollectionSchema = serde_json::from_str(&content)
            .with_context(|| format!("Failed to parse {}", json_path.display()))?;
        let grouped_target_schema = grouped_prefix;
        let target_schema = args
            .schema
            .as_deref()
            .or(config_target_schema.as_deref())
            .or(grouped_target_schema)
            .or(Some(table_name));
        let mapping_tables =
            load_mapping_ddl_tables(json_path.parent().unwrap_or(&collections_dir))?;

        let force_schema_inference_for_objectid = mapping_tables.as_ref().is_some_and(|tables| {
            should_regenerate_from_schema_when_objectid_pk(&schema, tables, table_name)
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
                    config_schema_owner.as_deref(),
                )
            } else {
                render_ddl_from_mapping_tables(&mapping_tables, target_schema)
            }
        } else {
            schema_to_ddl_with_timestamp_fields_and_owner(
                &schema,
                table_name,
                target_schema,
                config_schema_owner.as_deref(),
                &config_timestamp_fields,
            )
        };
        let preamble_db_name =
            resolve_preamble_database_name(config_db_name.as_deref(), &effective_rel_sql);
        let ddl = prepend_database_preamble(ddl, preamble_db_name.as_deref());

        std::fs::write(&sql_path, &ddl)
            .with_context(|| format!("Failed to write {}", sql_path.display()))?;
        if !quiet {
            info!("SQL written to {}", sql_path.display());
        }
    }

    if !quiet {
        info!("to-pg completed. Review the generated SQL files to confirm that schema names and table names suit your needs."
        );
        info!(
            "Also check that table and column names do not exceed PostgreSQL's 63-byte identifier limit."
        );
        info!(
            "If you modify those SQL files, the next export and report commands will use them as their source of truth."
        );
    }

    if let (Some(conf), Some(stage)) = (args.config.as_deref(), source_stage.as_ref()) {
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
