//! `infer` subcommand (also the default): samples MongoDB collections, infers
//! their schema, generates PostgreSQL DDL/mapping artifacts, and (when chained
//! with a config file) triggers `to-pg` and `report` afterwards.

use std::collections::{HashMap, HashSet};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use bson::{doc, Bson};
use bytes::Bytes;
use google_cloud_storage::client::Storage;
use indexmap::IndexMap;
use log::{debug, info, warn};
use mongodb::Client;

use crate::cli::{InferArgs, ReportArgs, ToPgArgs, UriArg};
use crate::commands::report::run_report;
use crate::commands::shared::{
    apply_config_overrides, ddl_table_mapping_from_table, default_ddl_editing_guidance,
    ensure_output_prefix_segments, normalize_pg_identifier, parse_namespace,
    resolve_local_project_root_from_config, sanitize_name, sanitize_pg_name, split_namespace_scope,
    CollectionMapping, ConfigOverrides, DdlTableMapping, MappingColumn, PgMapping, TraversalMode,
    TraversalPlan,
};
use crate::commands::to_pg::run_to_pg;
use crate::engine::analyzer::{
    Analyzer, CollectionSchema, FieldSchema, TypeSchema, TYPE_NULL, TYPE_UNDEFINED,
};
use crate::engine::ddl::schema_to_ddl_with_timestamp_fields;
use crate::engine::mapping::mapping_mongo_path_for_segments;
use crate::engine::stats::{
    format_stats, stats_to_yaml, CollectionReadOpsYaml, InferWarningMinorityYaml,
    InferWarningTypeYaml, InferWarningYaml,
};
use crate::export::{ensure_gcs_authentication, resolve_export_write_backend, ExportWriteBackend};
use crate::report::{collect_rows, compute_cluster_score, compute_db_score, SYSTEM_DATABASES};
use crate::schema_diagram::parse_sql;
use crate::util::{
    can_inline_object_fields, connection_failed_context, flatten_grouped_root_array_object_fields,
    flatten_root_array_object_field, flattened_root_parent_id_column,
    grouped_root_array_object_fields, inline_object_column_names_with_prefix,
    inline_object_leaf_fields_with_prefix, is_pg_reserved, property_filter_entries_for_collection,
    read_conf, sanitize, scalar_type_family, should_infer_collection,
};

use futures::TryStreamExt;

pub async fn run_infer(args: InferArgs) -> Result<()> {
    if args.config.is_none() {
        if let Some(output_dir) = args.output_dir.as_deref() {
            let output_raw = output_dir.to_string_lossy();
            if output_raw.starts_with("gs://") || output_raw.starts_with("gs:/") {
                return Err(anyhow!(
                    "infer does not support direct --output-dir to GCS ({output_raw}). Use -c <config> with [project].base_dir = \"gs://<bucket>/<prefix>\" to enable post-infer upload"
                ));
            }
        }
    }

    if let Some(conf) = args.config.as_deref() {
        apply_config_overrides(
            conf,
            &ConfigOverrides {
                project_dir: args.project_dir.clone(),
                source_uri: args.mongo.source_uri.clone(),
                namespace: args.namespace.clone(),
                number: args.number,
                percent: args.percent,
                max_time_ms: args.max_time_ms,
                chunk_size: args.chunk_size,
                auth_retry_max: args.auth_retry_max,
                jsonb: args.jsonb.then_some(true),
                target_database_name: args.database_name.clone(),
                target_schema_name: args.schema_name.clone(),
                ..ConfigOverrides::default()
            },
        )?;
    }

    let chained_config = args.config.clone();
    let chained_output_dir = args.output_dir.clone();
    let quiet_infer = chained_config.is_some();
    let mut infer_fallback_root: Option<PathBuf> = None;
    // Resolve SOURCE_URI, namespace, number, and percent – reading conf file if -c was given
    let (
        resolved_source_uri,
        effective_output_dir,
        conf_namespace,
        conf_number,
        conf_percent,
        conf_max_time_ms,
        conf_chunk_size,
        conf_auth_retry_max,
        conf_jsonb,
        conf_target_schema,
        conf_timestamp_fields,
        conf_include,
        conf_exclude,
    ) = if let Some(ref conf) = args.config {
        let c = read_conf(conf)?;
        let source_uri = args
            .mongo
            .source_uri
            .clone()
            .or(c.source_uri.clone())
            .ok_or_else(|| {
                anyhow!("No SOURCE_URI provided: pass --source-uri or add SOURCE_URI to the config file")
            })?;
        if args.output_dir.is_some() {
            warn!(
                "--output-dir is ignored when --config is set; infer outputs are written under base_dir/project_dir/source/collections"
            );
        }
        let configured_project_root = resolve_local_project_root_from_config(conf, &c);
        let configured_out_dir = configured_project_root.join("source").join("collections");

        let (local_project_root, out_dir) = match std::fs::create_dir_all(&configured_out_dir) {
            Ok(()) => (configured_project_root, configured_out_dir),
            Err(err) => {
                let fallback_project_root = std::env::temp_dir()
                    .join("mongo2pg-infer")
                    .join(sanitize_name(&c.project_dir));
                let fallback_out_dir = fallback_project_root.join("source").join("collections");
                std::fs::create_dir_all(&fallback_out_dir).with_context(|| {
                    format!(
                        "Failed to create configured infer output dir {} (original error: {}) and fallback dir {}",
                        configured_out_dir.display(),
                        err,
                        fallback_out_dir.display()
                    )
                })?;
                warn!(
                    "Configured infer output dir {} is not writable; using fallback {}",
                    configured_out_dir.display(),
                    fallback_out_dir.display()
                );
                infer_fallback_root = Some(fallback_project_root.clone());
                (fallback_project_root, fallback_out_dir)
            }
        };

        info!(
            "infer output root (local): {}",
            local_project_root.display()
        );
        (
            source_uri,
            Some(out_dir),
            c.namespace,
            c.number,
            c.percent,
            c.max_time_ms,
            c.chunk_size,
            c.auth_retry_max,
            c.jsonb,
            c.target_schema,
            c.timestamp_fields,
            c.include,
            c.exclude,
        )
    } else {
        let source_uri =
            args.mongo.source_uri.clone().ok_or_else(|| {
                anyhow!("No SOURCE_URI provided: pass --source-uri or -c <config>")
            })?;
        (
            source_uri,
            args.output_dir.clone(),
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
    };

    let namespace = args.namespace.clone().or(conf_namespace);

    let client_options = crate::db::mongo::parse_client_options(&resolved_source_uri)
        .await
        .with_context(|| {
            format!(
                "{}: failed to parse MongoDB SOURCE_URI",
                connection_failed_context("mongo", "connect")
            )
        })?;
    let client = crate::db::mongo::client_with_options(client_options).with_context(|| {
        format!(
            "{}: failed to create MongoDB client",
            connection_failed_context("mongo", "connect")
        )
    })?;

    info!("preflight ping source: begin");
    client
        .database("admin")
        .run_command(doc! { "ping": 1_i32 })
        .await
        .with_context(|| {
            format!(
                "{}: failed MongoDB ping command",
                connection_failed_context("mongo", "query")
            )
        })?;
    info!("preflight ping source: ok");

    // CLI takes priority over conf for number/percent/jsonb; then fall back to defaults
    let resolved_number = args.number.or(conf_number);
    let resolved_percent = args.percent.or(conf_percent);
    let resolved_max_time_ms = args.max_time_ms.or(conf_max_time_ms);
    let resolved_chunk_size = resolve_infer_chunk_size(args.chunk_size.or(conf_chunk_size))?;
    let resolved_auth_retry_max =
        resolve_infer_auth_retry_max(args.auth_retry_max.or(conf_auth_retry_max))?;
    let resolved_jsonb = args.jsonb || conf_jsonb;

    let args = InferArgs {
        mongo: UriArg {
            source_uri: Some(resolved_source_uri),
        },
        namespace: namespace.clone(),
        output_dir: effective_output_dir,
        number: resolved_number,
        percent: resolved_percent,
        max_time_ms: resolved_max_time_ms,
        chunk_size: Some(resolved_chunk_size),
        auth_retry_max: Some(resolved_auth_retry_max),
        jsonb: resolved_jsonb,
        config: None,
        ..args
    };

    match namespace {
        None => {
            // No namespace provided: enumerate all user databases and infer each.
            infer_all_databases(
                &client,
                &args,
                &conf_include,
                &conf_exclude,
                &conf_timestamp_fields,
                !quiet_infer,
            )
            .await?;
        }
        Some(ref ns) if ns.contains('.') => {
            // Single collection: <db>.<collection>
            let (db_name, coll_name) = parse_namespace(ns)?;
            let existing_dbs = client.list_database_names().await.with_context(|| {
                format!(
                    "{}: failed to list databases",
                    connection_failed_context("mongo", "query")
                )
            })?;
            if !existing_dbs.iter().any(|d| d == db_name) {
                warn!(
                    "database '{db_name}' does not exist on the server. Available databases: {}",
                    existing_dbs.join(", ")
                );
            }
            let existing_colls = client
                .database(db_name)
                .list_collection_names()
                .await
                .with_context(|| {
                    format!(
                        "{}: failed to list collections",
                        connection_failed_context("mongo", "query")
                    )
                })?;
            if !existing_colls.iter().any(|c| c == coll_name) {
                warn!(
                    "collection '{coll_name}' does not exist in database '{db_name}'. Available collections: {}",
                    existing_colls.join(", ")
                );
            }
            let inferred_root_table_names = existing_colls
                .iter()
                .filter(|name| !name.starts_with("system."))
                .filter(|name| should_infer_collection(name, &conf_include, &conf_exclude))
                .map(|name| sanitize(name))
                .collect::<HashSet<_>>();
            if !should_infer_collection(coll_name, &conf_include, &conf_exclude) {
                info!(
                    "Skipping {db_name}.{coll_name}: filtered out by source.include/source.exclude"
                );
                return Ok(());
            }
            let schema = infer_collection(
                &client,
                db_name,
                coll_name,
                coll_name,
                conf_target_schema.as_deref(),
                &conf_include,
                &conf_exclude,
                &conf_timestamp_fields,
                &args,
                None,
                Some(&inferred_root_table_names),
                Some((1, 1)),
                !quiet_infer,
            )
            .await?;
            if args.print_json && !args.no_output {
                info!("{}", serde_json::to_string_pretty(&schema)?);
            }
        }
        Some(ref ns) => {
            // Whole single database: infer every collection.
            let db_name = ns.as_str();
            let existing_dbs = client.list_database_names().await.with_context(|| {
                format!(
                    "{}: failed to list databases",
                    connection_failed_context("mongo", "query")
                )
            })?;
            if !existing_dbs.iter().any(|d| d == db_name) {
                warn!(
                    "database '{db_name}' does not exist on the server. Available databases: {}",
                    existing_dbs.join(", ")
                );
            }
            let db = client.database(db_name);
            let coll_names = db.list_collection_names().await.with_context(|| {
                format!(
                    "{}: failed to list collections",
                    connection_failed_context("mongo", "query")
                )
            })?;
            let filtered_coll_names: Vec<&String> = coll_names
                .iter()
                .filter(|n| !n.starts_with("system."))
                .filter(|n| should_infer_collection(n, &conf_include, &conf_exclude))
                .collect();
            let inferred_root_table_names = filtered_coll_names
                .iter()
                .map(|name| sanitize(name))
                .collect::<HashSet<_>>();
            let total_collections = filtered_coll_names.len();

            let mut all_schemas: IndexMap<String, CollectionSchema> = IndexMap::new();
            for (index, coll_name) in filtered_coll_names.iter().enumerate() {
                match infer_collection(
                    &client,
                    db_name,
                    coll_name,
                    coll_name,
                    conf_target_schema.as_deref(),
                    &conf_include,
                    &conf_exclude,
                    &conf_timestamp_fields,
                    &args,
                    None,
                    Some(&inferred_root_table_names),
                    Some((index + 1, total_collections)),
                    !quiet_infer,
                )
                .await
                {
                    Ok(schema) => {
                        all_schemas.insert((*coll_name).clone(), schema);
                    }
                    Err(e) => warn!(" skipping {db_name}.{coll_name}: {e:#}"),
                }
            }
            if args.print_json && !args.no_output {
                info!("{}", serde_json::to_string_pretty(&all_schemas)?);
            }
        }
    }

    if let Some(fallback_root) = infer_fallback_root.as_ref() {
        if chained_config.is_some() {
            if let Some(conf) = chained_config.as_ref() {
                warn!(
                    "Infer artifacts were written to fallback root {}; running chained to-pg/report on fallback project root",
                    fallback_root.display()
                );

                let fallback_config_dir = fallback_root.join("config");
                std::fs::create_dir_all(&fallback_config_dir).with_context(|| {
                    format!(
                        "Failed to create fallback config directory {}",
                        fallback_config_dir.display()
                    )
                })?;
                let fallback_config_path = fallback_config_dir.join(
                    conf.file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or("config.toml"),
                );
                std::fs::copy(conf, &fallback_config_path).with_context(|| {
                    format!(
                        "Failed to stage fallback config {} from {}",
                        fallback_config_path.display(),
                        conf.display()
                    )
                })?;

                move_infer_artifacts_to_gcs_if_needed_with_project_root(
                    conf,
                    Some(fallback_root.as_path()),
                )
                .await?;

                run_to_pg(
                    ToPgArgs {
                        collection: None,
                        table: None,
                        config: Some(fallback_config_path.clone()),
                        output_dir: None,
                        schema: None,
                        project_dir: None,
                    },
                    true,
                )
                .await?;

                run_report(
                    ReportArgs {
                        mongo: UriArg { source_uri: None },
                        config: Some(fallback_config_path),
                        collections_dir: None,
                        output: None,
                        namespace: String::new(),
                        project_dir: None,
                        post_import: false,
                        check_md5: true,
                        noaggregate: false,
                    },
                    true,
                )
                .await?;

                move_infer_artifacts_to_gcs_if_needed_with_project_root(
                    conf,
                    Some(fallback_root.as_path()),
                )
                .await?;
            }
            info!(
                "Fallback infer pipeline completed under {} and artifacts uploaded to configured bucket.",
                fallback_root.display()
            );
            return Ok(());
        }
    }

    if let Some(ref conf) = chained_config {
        move_infer_artifacts_to_gcs_if_needed(conf).await?;
        run_to_pg(
            ToPgArgs {
                collection: None,
                table: None,
                config: Some(conf.clone()),
                output_dir: None,
                schema: None,
                project_dir: None,
            },
            true,
        )
        .await?;
        run_report(
            ReportArgs {
                mongo: UriArg { source_uri: None },
                config: Some(conf.clone()),
                collections_dir: None,
                output: None,
                namespace: String::new(),
                project_dir: None,
                post_import: false,
                check_md5: true,
                noaggregate: false,
            },
            true,
        )
        .await?;
        validate_infer_artifacts_use_base_dir(conf)?;
        move_infer_artifacts_to_gcs_if_needed(conf).await?;
        print_infer_summary(conf)?;
    }

    if chained_config.is_none() {
        debug!("[gcs] infer upload hook skipped: infer ran without -c config");
        if let Some(output_dir) = args.output_dir.as_deref() {
            info!(
                "Inference completed. Collection schemas and statistics were written under {}.",
                output_dir.display()
            );
            if chained_output_dir.is_some() {
                info!(
                "If you need PostgreSQL DDL from this standalone output, run to-pg separately on the generated collection files."
            );
            }
        }
    }

    Ok(())
}

pub fn validate_infer_artifacts_use_base_dir(conf: &Path) -> Result<()> {
    let c = read_conf(conf)?;
    if matches!(
        resolve_export_write_backend(&c.base_dir)?,
        ExportWriteBackend::Gcs { .. }
    ) {
        info!(
            "Infer artifact validation deferred to GCS upload/staging for base_dir='{}'",
            c.base_dir.display()
        );
        return Ok(());
    }

    let project_root = resolve_local_project_root_from_config(conf, &c);
    let source_collections_dir = project_root.join("source").join("collections");
    let schema_tables_dir = project_root.join("schema").join("tables");
    let reports_main = project_root.join("reports").join("main.html");

    if !source_collections_dir.is_dir() {
        return Err(anyhow!(
            "Infer output validation failed: source collections directory not found at {} (derived from base_dir='{}', project_dir='{}')",
            source_collections_dir.display(),
            c.base_dir.display(),
            c.project_dir
        ));
    }
    if !schema_tables_dir.is_dir() {
        return Err(anyhow!(
            "Infer output validation failed: schema tables directory not found at {} (derived from base_dir='{}', project_dir='{}')",
            schema_tables_dir.display(),
            c.base_dir.display(),
            c.project_dir
        ));
    }
    if !reports_main.is_file() {
        return Err(anyhow!(
            "Infer output validation failed: report file not found at {} (derived from base_dir='{}', project_dir='{}')",
            reports_main.display(),
            c.base_dir.display(),
            c.project_dir
        ));
    }

    info!(
        "Infer artifact validation succeeded under base_dir/project_dir: source={}, schema={}, report={}",
        source_collections_dir.display(),
        schema_tables_dir.display(),
        reports_main.display()
    );
    Ok(())
}

pub fn infer_artifact_directories(project_root: &Path) -> Vec<PathBuf> {
    vec![
        project_root.join("source"),
        project_root.join("schema"),
        project_root.join("reports"),
    ]
}

pub fn collect_files_recursive(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if !root.exists() {
        return Ok(files);
    }

    for entry in std::fs::read_dir(root)
        .with_context(|| format!("Cannot read directory {}", root.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            files.extend(collect_files_recursive(&path)?);
        } else if path.is_file() {
            files.push(path);
        }
    }

    Ok(files)
}

pub fn infer_object_key(
    prefix: &str,
    cluster_name: Option<&str>,
    project_dir: &str,
    project_root: &Path,
    file_path: &Path,
) -> Result<String> {
    let relative = file_path
        .strip_prefix(project_root)
        .with_context(|| {
            format!(
                "Cannot build infer object key: {} is not under {}",
                file_path.display(),
                project_root.display()
            )
        })?
        .to_string_lossy()
        .replace('\\', "/");

    let effective_prefix = ensure_output_prefix_segments(prefix, cluster_name, project_dir);
    if effective_prefix.is_empty() {
        let project_segment = project_dir.trim_matches('/');
        if project_segment.is_empty() {
            Ok(relative)
        } else {
            Ok(format!("{project_segment}/{relative}"))
        }
    } else {
        Ok(format!("{effective_prefix}/{relative}"))
    }
}

pub fn infer_object_mime_type(file_path: &Path) -> &'static str {
    match file_path.extension().and_then(|ext| ext.to_str()) {
        Some("json") => "application/json",
        Some("yaml") | Some("yml") => "application/x-yaml",
        Some("sql") => "application/sql",
        Some("html") => "text/html",
        Some("txt") => "text/plain",
        _ => "application/octet-stream",
    }
}

pub async fn move_infer_artifacts_to_gcs_if_needed(conf: &Path) -> Result<()> {
    move_infer_artifacts_to_gcs_if_needed_with_project_root(conf, None).await
}

pub async fn move_infer_artifacts_to_gcs_if_needed_with_project_root(
    conf: &Path,
    project_root_override: Option<&Path>,
) -> Result<()> {
    debug!(
        "[gcs] infer upload hook entered: config='{}'",
        conf.display()
    );
    let c = read_conf(conf)?;
    let backend = resolve_export_write_backend(&c.base_dir)?;
    let (bucket, prefix) = match backend {
        ExportWriteBackend::Gcs { bucket, prefix } => (bucket, prefix),
        ExportWriteBackend::LocalFs => {
            debug!(
                "Infer GCS upload skipped: [project].base_dir is local filesystem ('{}'), not gs://",
                c.base_dir.display()
            );
            return Ok(());
        }
    };

    debug!(
        "[gcs] infer upload enabled: base_dir='{}' bucket='{}' prefix='{}'",
        c.base_dir.display(),
        bucket,
        prefix.trim_matches('/'),
    );

    ensure_gcs_authentication().await?;
    let storage = Storage::builder()
        .build()
        .await
        .context("Failed to initialize Google Cloud Storage client")?;
    let bucket_resource = format!("projects/_/buckets/{bucket}");

    let project_root = project_root_override
        .map(Path::to_path_buf)
        .unwrap_or_else(|| resolve_local_project_root_from_config(conf, &c));
    debug!(
        "[gcs] infer upload project root resolved: {}",
        project_root.display()
    );
    let artifact_dirs = infer_artifact_directories(&project_root);
    for dir in &artifact_dirs {
        debug!(
            "[gcs] infer upload scan dir: {} exists={} is_dir={}",
            dir.display(),
            dir.exists(),
            dir.is_dir()
        );
    }
    let mut files_to_move = Vec::new();
    for dir in &artifact_dirs {
        files_to_move.extend(collect_files_recursive(dir)?);
    }
    debug!(
        "[gcs] infer upload candidate files: {}",
        files_to_move.len()
    );

    if files_to_move.is_empty() {
        debug!(
            "No infer artifacts found to move to gs://{}/{}",
            bucket,
            prefix.trim_matches('/')
        );
        return Ok(());
    }

    info!(
        "Moving {} infer artifact files to gs://{}/{}",
        files_to_move.len(),
        bucket,
        prefix.trim_matches('/')
    );
    let mut uploaded_files = 0usize;

    for file_path in &files_to_move {
        let object_key = infer_object_key(
            &prefix,
            c.cluster_name.as_deref(),
            &c.project_dir,
            &project_root,
            file_path,
        )?;
        let mime_type = infer_object_mime_type(file_path);
        let bytes = tokio::fs::read(file_path)
            .await
            .with_context(|| format!("Cannot read infer artifact {}", file_path.display()))?;
        let _ = mime_type;
        debug!(
            "[gcs] infer upload object: {} -> gs://{}/{}",
            file_path.display(),
            bucket,
            object_key
        );
        storage
            .write_object(
                bucket_resource.clone(),
                object_key.clone(),
                Bytes::from(bytes),
            )
            .send_buffered()
            .await
            .with_context(|| {
                format!(
                    "Failed to upload infer artifact {} to gs://{}/{}",
                    file_path.display(),
                    bucket,
                    object_key
                )
            })?;
        uploaded_files += 1;
    }

    debug!(
        "[gcs] infer upload done: uploaded_files={} bucket='{}' prefix='{}'",
        uploaded_files,
        bucket,
        prefix.trim_matches('/'),
    );

    let purge_local = std::env::var("MONGO2PG_GCS_PURGE_LOCAL")
        .ok()
        .map(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false);

    if purge_local {
        for file_path in &files_to_move {
            std::fs::remove_file(file_path).with_context(|| {
                format!(
                    "Failed to remove local infer artifact {}",
                    file_path.display()
                )
            })?;
        }
        info!(
            "Uploaded infer artifacts to gs://{}/{} and removed local copies under {}",
            bucket,
            prefix.trim_matches('/'),
            project_root.display()
        );
    } else {
        info!(
            "Uploaded infer artifacts to gs://{}/{} and kept local copies under {} (set MONGO2PG_GCS_PURGE_LOCAL=1 to remove local files)",
            bucket,
            prefix.trim_matches('/'),
            project_root.display()
        );
    }
    Ok(())
}

pub fn print_infer_summary(conf: &Path) -> Result<()> {
    let c = read_conf(conf)?;
    let project_root = resolve_local_project_root_from_config(conf, &c);
    let collections_dir = project_root.join("source").join("collections");
    let tables_root = project_root.join("schema").join("tables");
    let report_path = project_root.join("reports").join("main.html");

    let is_multi_db = std::fs::read_dir(&collections_dir)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| e.path().is_dir())
                .any(|e| {
                    std::fs::read_dir(e.path())
                        .map(|sub| sub.filter_map(|s| s.ok()).any(|s| s.path().is_dir()))
                        .unwrap_or(false)
                })
        })
        .unwrap_or(false);

    let (score, collection_count, table_count) = if is_multi_db {
        let mut db_scores = Vec::new();
        let mut total_collections = 0usize;
        let mut total_tables = 0usize;

        let mut db_names: Vec<String> = std::fs::read_dir(&collections_dir)
            .with_context(|| format!("Cannot read {}", collections_dir.display()))?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        db_names.sort();

        for db_name in &db_names {
            let db_dir = collections_dir.join(db_name);
            let tables_dir = tables_root.join(db_name);
            let rows = collect_rows(&db_dir, tables_dir.is_dir().then_some(tables_dir.as_path()))?;
            total_collections += rows.len();
            total_tables += rows
                .iter()
                .map(|row| row.tables_count().unwrap_or(0))
                .sum::<usize>();
            db_scores.push(compute_db_score(db_name, &rows));
        }

        (
            compute_cluster_score(&db_scores).score_total,
            total_collections,
            total_tables,
        )
    } else {
        let tables_dir_for_summary = {
            let db_name = c
                .namespace
                .as_deref()
                .map(|namespace| split_namespace_scope(namespace).0)
                .filter(|db_name| !db_name.is_empty())
                .map(|db_name| tables_root.join(db_name));
            db_name
                .filter(|dir| dir.is_dir())
                .unwrap_or_else(|| tables_root.clone())
        };
        let rows = collect_rows(
            &collections_dir,
            tables_dir_for_summary
                .is_dir()
                .then_some(tables_dir_for_summary.as_path()),
        )?;
        let score = compute_db_score(&c.project_dir, &rows).score_db;
        let table_count = rows
            .iter()
            .map(|row| row.tables_count().unwrap_or(0))
            .sum::<usize>();
        (score, rows.len(), table_count)
    };

    info!("Inference summary");
    info!("  Score: {:.2}", score);
    info!("  Collections: {}", collection_count);
    info!("  PostgreSQL tables: {}", table_count);
    info!("  Detailed HTML report: {}", report_path.display());
    info!(
        "  Next step: review the generated DDL files under {} and then run `mongo2pg export -c {}`",
        tables_root.display(),
        conf.display()
    );

    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub struct InferTypeWarning {
    pub field_path: String,
    pub dominant_family: String,
    pub dominant_ratio: f64,
    pub minority_families: Vec<(String, f64)>,
    pub observed_types: Vec<InferWarningTypeYaml>,
}

pub fn warning_examples(type_schema: &TypeSchema) -> Vec<String> {
    let mut examples = Vec::new();

    if let Some(values) = &type_schema.values {
        for value in values {
            let rendered = serde_json::to_string(value)
                .expect("serializing infer warning example should succeed");
            if !examples.iter().any(|existing| existing == &rendered) {
                examples.push(rendered);
            }
            if examples.len() == 5 {
                break;
            }
        }
    }

    examples
}

pub fn collect_infer_type_warnings(schema: &CollectionSchema) -> Vec<InferTypeWarning> {
    fn visit_field(path: &str, field: &FieldSchema, warnings: &mut Vec<InferTypeWarning>) {
        let mut family_probs: HashMap<&str, f64> = HashMap::new();
        let mut total_scalar_prob = 0.0;
        let mut total_non_scalar_prob = 0.0;
        let mut non_scalar_types: Vec<(&str, f64)> = Vec::new();
        let mut observed_types = Vec::new();

        for (type_name, type_schema) in &field.types {
            if let Some(family) = scalar_type_family(type_name) {
                *family_probs.entry(family).or_insert(0.0) += type_schema.probability;
                total_scalar_prob += type_schema.probability;
                observed_types.push(InferWarningTypeYaml {
                    type_name: type_name.clone(),
                    ratio: type_schema.probability,
                    examples: warning_examples(type_schema),
                });
            } else {
                // Non-scalar type (Object, Array, Null, Undefined)
                total_non_scalar_prob += type_schema.probability;
                non_scalar_types.push((type_name.as_str(), type_schema.probability));
                observed_types.push(InferWarningTypeYaml {
                    type_name: type_name.clone(),
                    ratio: type_schema.probability,
                    examples: warning_examples(type_schema),
                });
            }
        }

        observed_types.sort_by(|left, right| {
            right
                .ratio
                .total_cmp(&left.ratio)
                .then_with(|| left.type_name.cmp(&right.type_name))
        });

        // Check for scalar-scalar mixing (multiple scalar families)
        if family_probs.len() > 1 && total_scalar_prob > 0.0 {
            let mut family_ratios = family_probs
                .into_iter()
                .map(|(family, prob)| (family.to_owned(), prob / total_scalar_prob))
                .collect::<Vec<_>>();
            family_ratios.sort_by(|left, right| {
                right
                    .1
                    .total_cmp(&left.1)
                    .then_with(|| left.0.cmp(&right.0))
            });

            let dominant_ratio = family_ratios[0].1;
            if dominant_ratio > 0.5 {
                warnings.push(InferTypeWarning {
                    field_path: path.to_owned(),
                    dominant_family: family_ratios[0].0.clone(),
                    dominant_ratio,
                    minority_families: family_ratios[1..]
                        .iter()
                        .map(|(family, ratio)| (family.clone(), *ratio))
                        .collect(),
                    observed_types: observed_types
                        .into_iter()
                        .map(|mut observed_type| {
                            observed_type.ratio /= total_scalar_prob;
                            observed_type
                        })
                        .collect(),
                });
            }
        } else if !family_probs.is_empty() && total_non_scalar_prob > 0.0 {
            // Check for scalar + non-scalar mixing (e.g., String + Object)
            let mut family_ratios: Vec<(String, f64)> = family_probs
                .into_iter()
                .map(|(family, prob)| (family.to_owned(), prob))
                .collect();
            family_ratios.sort_by(|left, right| {
                right
                    .1
                    .total_cmp(&left.1)
                    .then_with(|| left.0.cmp(&right.0))
            });

            let total_prob = total_scalar_prob + total_non_scalar_prob;
            let dominant_ratio = family_ratios[0].1 / total_prob;

            // Warn if dominant scalar family has > 90% of the total probability
            if dominant_ratio > 0.9 {
                // Map non-scalar type names to a displayable family name
                let minority_families: Vec<(String, f64)> = non_scalar_types
                    .into_iter()
                    .map(|(type_name, prob)| {
                        // For non-scalar types, use the type name itself as the "family"
                        // but lowercase it for consistency
                        let family = match type_name {
                            "Object" => "object",
                            "Array" => "array",
                            "Null" => "null",
                            "Undefined" => "undefined",
                            _ => type_name,
                        };
                        (family.to_string(), prob / total_prob)
                    })
                    .collect();

                warnings.push(InferTypeWarning {
                    field_path: path.to_owned(),
                    dominant_family: family_ratios[0].0.clone(),
                    dominant_ratio,
                    minority_families,
                    observed_types: observed_types
                        .into_iter()
                        .map(|mut observed_type| {
                            observed_type.ratio /= total_prob;
                            observed_type
                        })
                        .collect(),
                });
            }
        }

        for type_schema in field.types.values() {
            if let Some(object_fields) = &type_schema.object {
                for (child_name, child_field) in object_fields {
                    let child_path = if path.is_empty() {
                        child_name.clone()
                    } else {
                        format!("{path}.{child_name}")
                    };
                    visit_field(&child_path, child_field, warnings);
                }
            }

            if let Some(array_items) = &type_schema.array {
                let array_path = if path.is_empty() {
                    "[]".to_owned()
                } else {
                    format!("{path}[]")
                };
                visit_field(&array_path, array_items, warnings);
            }
        }
    }

    let mut warnings = Vec::new();
    for (field_name, field) in &schema.object {
        visit_field(field_name, field, &mut warnings);
    }
    warnings
}

/// Collect warnings for scalar fields that can be null or undefined.
/// These fields should be normalized or the PG column should be nullable.
pub fn collect_nullable_scalar_warnings(schema: &CollectionSchema) -> Vec<InferWarningYaml> {
    fn visit_field(path: &str, field: &FieldSchema, warnings: &mut Vec<InferWarningYaml>) {
        let mut has_scalar = false;
        let mut scalar_types = Vec::new();
        let mut has_null = false;
        let mut has_undefined = false;
        let mut null_ratio = 0.0;
        let mut undefined_ratio = 0.0;

        for (type_name, type_schema) in &field.types {
            match type_name.as_str() {
                TYPE_NULL => {
                    has_null = true;
                    null_ratio = type_schema.probability;
                }
                TYPE_UNDEFINED => {
                    has_undefined = true;
                    undefined_ratio = type_schema.probability;
                }
                _ => {
                    if scalar_type_family(type_name).is_some() {
                        has_scalar = true;
                        scalar_types.push((type_name.clone(), type_schema.probability));
                    }
                }
            }
        }

        // Warn if we have scalar types and null/undefined
        if has_scalar && (has_null || has_undefined) {
            // Get the dominant scalar type
            scalar_types.sort_by(|a, b| b.1.total_cmp(&a.1));
            if let Some((dominant_scalar, _)) = scalar_types.first() {
                let total_nullish = if has_null && has_undefined {
                    null_ratio + undefined_ratio
                } else if has_null {
                    null_ratio
                } else {
                    undefined_ratio
                };

                // Only warn if the nullish probability is significant
                if total_nullish > 0.0 {
                    warnings.push(InferWarningYaml {
                        kind: "nullable_scalar".to_owned(),
                        field_path: path.to_owned(),
                        renamed_to: None,
                        keyword: None,
                        dominant_family: dominant_scalar.clone(),
                        dominant_ratio: scalar_types[0].1,
                        minority_families: Vec::new(),
                        observed_types: Vec::new(),
                    });
                }
            }
        }

        // Recurse into nested structures
        for type_schema in field.types.values() {
            if let Some(object_fields) = &type_schema.object {
                for (child_name, child_field) in object_fields {
                    let child_path = if path.is_empty() {
                        child_name.clone()
                    } else {
                        format!("{path}.{child_name}")
                    };
                    visit_field(&child_path, child_field, warnings);
                }
            }

            if let Some(array_items) = &type_schema.array {
                let array_path = if path.is_empty() {
                    "[]".to_owned()
                } else {
                    format!("{path}[]")
                };
                visit_field(&array_path, array_items, warnings);
            }
        }
    }

    let mut warnings = Vec::new();
    for (field_name, field) in &schema.object {
        visit_field(field_name, field, &mut warnings);
    }
    warnings
}

pub fn collect_identifier_warnings(schema: &CollectionSchema) -> Vec<InferWarningYaml> {
    fn normalized_pg_identifier(name: &str) -> String {
        name.to_ascii_lowercase()
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect()
    }

    fn looks_like_type_name(name: &str) -> bool {
        matches!(
            name,
            "array"
                | "bigint"
                | "binary"
                | "bool"
                | "boolean"
                | "bytea"
                | "char"
                | "character"
                | "character_varying"
                | "citext"
                | "date"
                | "datetime"
                | "decimal"
                | "decimal128"
                | "double"
                | "double_precision"
                | "float4"
                | "float8"
                | "int"
                | "int2"
                | "int4"
                | "int8"
                | "int32"
                | "int64"
                | "integer"
                | "json"
                | "jsonb"
                | "null"
                | "number"
                | "numeric"
                | "object"
                | "objectid"
                | "real"
                | "smallint"
                | "string"
                | "text"
                | "time"
                | "timestamp"
                | "timestamptz"
                | "undefined"
                | "uuid"
                | "varchar"
        )
    }

    fn warning_examples(type_schema: &TypeSchema) -> Vec<String> {
        type_schema
            .values
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .take(5)
            .map(|value| match value {
                serde_json::Value::String(text) => format!("\"{text}\""),
                _ => value.to_string(),
            })
            .collect()
    }

    fn observed_types_for_field(field: &FieldSchema) -> Vec<InferWarningTypeYaml> {
        let mut observed_types = field
            .types
            .iter()
            .filter(|(type_name, _)| !matches!(type_name.as_str(), "Null" | "Undefined"))
            .map(|(type_name, type_schema)| InferWarningTypeYaml {
                type_name: type_name.clone(),
                ratio: type_schema.probability,
                examples: warning_examples(type_schema),
            })
            .collect::<Vec<_>>();

        observed_types.sort_by(|left, right| {
            right
                .ratio
                .total_cmp(&left.ratio)
                .then_with(|| left.type_name.cmp(&right.type_name))
        });
        observed_types
    }

    fn visit_field(path: &str, field: &FieldSchema, warnings: &mut Vec<InferWarningYaml>) {
        let raw_name = path
            .rsplit('.')
            .next()
            .unwrap_or(path)
            .trim_end_matches("[]");
        let normalized = normalized_pg_identifier(raw_name);
        let observed_types = observed_types_for_field(field);
        if !normalized.is_empty() && is_pg_reserved(&normalized) {
            warnings.push(InferWarningYaml {
                kind: "pg_keyword".to_owned(),
                field_path: path.to_owned(),
                renamed_to: Some(sanitize(raw_name)),
                keyword: Some(normalized),
                dominant_family: String::new(),
                dominant_ratio: 0.0,
                minority_families: Vec::new(),
                observed_types,
            });
        } else if !normalized.is_empty() && looks_like_type_name(&normalized) {
            warnings.push(InferWarningYaml {
                kind: "type_name".to_owned(),
                field_path: path.to_owned(),
                renamed_to: None,
                keyword: Some(normalized),
                dominant_family: String::new(),
                dominant_ratio: 0.0,
                minority_families: Vec::new(),
                observed_types,
            });
        }

        for type_schema in field.types.values() {
            if let Some(object_fields) = &type_schema.object {
                for (child_name, child_field) in object_fields {
                    let child_path = if path.is_empty() {
                        child_name.clone()
                    } else {
                        format!("{path}.{child_name}")
                    };
                    visit_field(&child_path, child_field, warnings);
                }
            }

            if let Some(array_items) = &type_schema.array {
                let array_path = if path.is_empty() {
                    "[]".to_owned()
                } else {
                    format!("{path}[]")
                };
                visit_field(&array_path, array_items, warnings);
            }
        }
    }

    let mut warnings = Vec::new();
    for (field_name, field) in &schema.object {
        visit_field(field_name, field, &mut warnings);
    }
    warnings
}

pub fn emit_infer_type_warnings(db_name: &str, coll_name: &str, schema: &CollectionSchema) {
    for warning in collect_infer_type_warnings(schema) {
        let non_null_minorities: Vec<_> = warning
            .minority_families
            .iter()
            .filter(|(family, _ratio)| family.to_string() != "null")
            .collect();
        if !non_null_minorities.is_empty() {
            let minority_details = non_null_minorities
                .iter()
                .map(|(family, ratio)| format!("{} ({:.1}%)", family, ratio * 100.0))
                .collect::<Vec<_>>()
                .join(", ");

            info!(
                " source {}.{} field {} mixes incompatible scalar types: dominant {} ({:.1}% of non-null values), minority {}. Normalize source values before import.",
                db_name,
                coll_name,
                warning.field_path,
                warning.dominant_family,
                warning.dominant_ratio * 100.0,
                minority_details,
            );
        };
    }

    for warning in collect_identifier_warnings(schema) {
        if warning.kind == "pg_keyword" {
            info!(
                "source {}.{} field {} uses PostgreSQL keyword '{}'. Consider renaming it.",
                db_name,
                coll_name,
                warning.field_path,
                warning.keyword.as_deref().unwrap_or(""),
            );
        } else {
            info!(
                "source {}.{} field {} matches type name '{}'. Consider renaming it.",
                db_name,
                coll_name,
                warning.field_path,
                warning.keyword.as_deref().unwrap_or(""),
            );
        }
    }
}

pub fn infer_warnings_to_yaml(schema: &CollectionSchema) -> Vec<InferWarningYaml> {
    let mut warnings = collect_infer_type_warnings(schema)
        .into_iter()
        .map(|warning| InferWarningYaml {
            kind: "mixed_scalar_types".to_owned(),
            field_path: warning.field_path,
            renamed_to: None,
            keyword: None,
            dominant_family: warning.dominant_family,
            dominant_ratio: warning.dominant_ratio,
            minority_families: warning
                .minority_families
                .into_iter()
                .map(|(family, ratio)| InferWarningMinorityYaml { family, ratio })
                .collect(),
            observed_types: warning.observed_types,
        })
        .collect::<Vec<_>>();
    warnings.extend(collect_nullable_scalar_warnings(schema));
    warnings.extend(collect_identifier_warnings(schema));
    warnings
}

/// Infer schemas for all user databases on the server (skipping system databases).
///
/// Output files are written as `<output_dir>/<dbname>/<collname>/`.
/// Report generation is handled separately by the `report` command.
pub async fn infer_all_databases(
    client: &Client,
    args: &InferArgs,
    include: &[String],
    exclude: &[String],
    timestamp_fields: &[String],
    emit_stats: bool,
) -> Result<()> {
    let all_dbs = client.list_database_names().await.with_context(|| {
        format!(
            "{}: failed to list databases",
            connection_failed_context("mongo", "query")
        )
    })?;

    let user_dbs: Vec<String> = all_dbs
        .into_iter()
        .filter(|db| !SYSTEM_DATABASES.contains(&db.as_str()))
        .collect();

    if user_dbs.is_empty() {
        warn!("No user databases found on the server.");
        return Ok(());
    }

    info!(
        "Inferring {} database(s): {}",
        user_dbs.len(),
        user_dbs.join(", ")
    );

    let mut databases_with_collections: Vec<(String, Vec<String>)> = Vec::new();

    for db_name in &user_dbs {
        let db = client.database(db_name);
        let coll_names = match db.list_collection_names().await {
            Ok(n) => n,
            Err(e) => {
                warn!("skipping database '{db_name}' (cannot list collections): {e:#}");
                continue;
            }
        };

        let filtered_coll_names: Vec<String> = coll_names
            .into_iter()
            .filter(|n| !n.starts_with("system."))
            .filter(|n| should_infer_collection(n, include, exclude))
            .collect();

        databases_with_collections.push((db_name.clone(), filtered_coll_names));
    }

    let total_collections: usize = databases_with_collections
        .iter()
        .map(|(_, coll_names)| coll_names.len())
        .sum();

    let mut current_collection = 0usize;

    for (db_name, coll_names) in &databases_with_collections {
        let inferred_root_table_names = coll_names
            .iter()
            .map(|name| sanitize(name))
            .collect::<HashSet<_>>();
        let mut db_schemas: IndexMap<String, CollectionSchema> = IndexMap::new();

        for coll_name in coll_names {
            current_collection += 1;
            let db_out_dir = args.output_dir.as_deref().map(|d| d.join(db_name));
            match infer_collection(
                client,
                db_name,
                coll_name,
                coll_name,
                None,
                include,
                exclude,
                timestamp_fields,
                args,
                db_out_dir.as_deref(),
                Some(&inferred_root_table_names),
                Some((current_collection, total_collections)),
                emit_stats,
            )
            .await
            {
                Ok(schema) => {
                    db_schemas.insert(coll_name.clone(), schema);
                }
                Err(e) => warn!("skipping {db_name}.{coll_name}: {e:#}"),
            }
        }

        if args.print_json && !args.no_output && args.output_dir.is_none() {
            info!(
                "{}",
                serde_json::to_string_pretty(&IndexMap::from([(db_name.clone(), &db_schemas)]))?
            );
        }
    }

    Ok(())
}

/// Default maximum time we allow a single infer sampling query to run on the server.
pub const DEFAULT_SAMPLE_MAX_TIME: Duration = Duration::from_secs(120);
pub const DEFAULT_INFER_CHUNK_SIZE: u64 = 1_000_000;
pub const DEFAULT_INFER_AUTH_RETRY_MAX: u32 = 3;

pub fn infer_query_max_time(max_time_ms: Option<u64>) -> Duration {
    max_time_ms
        .filter(|value| *value > 0)
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_SAMPLE_MAX_TIME)
}

pub fn resolve_infer_chunk_size(chunk_size: Option<u64>) -> Result<u64> {
    let resolved = chunk_size.unwrap_or(DEFAULT_INFER_CHUNK_SIZE);
    if resolved == 0 {
        return Err(anyhow!("chunk_size must be greater than 0"));
    }
    if resolved > i64::MAX as u64 {
        return Err(anyhow!("chunk_size must be <= {}", i64::MAX));
    }
    Ok(resolved)
}

pub fn resolve_infer_auth_retry_max(auth_retry_max: Option<u32>) -> Result<u32> {
    let resolved = auth_retry_max.unwrap_or(DEFAULT_INFER_AUTH_RETRY_MAX);
    if resolved > 100 {
        return Err(anyhow!("auth_retry_max must be between 0 and 100"));
    }
    Ok(resolved)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnauthorizedRetryDecision {
    Retry,
    Exhausted,
}

pub fn is_unauthorized_cursor_error(error_text: &str) -> bool {
    let lower = error_text.to_ascii_lowercase();
    lower.contains("error code 13")
        || lower.contains("unauthorized")
        || lower.contains("requires authentication")
}

pub fn classify_unauthorized_retry(
    error_text: &str,
    retry_attempt: u32,
    retry_max: u32,
) -> Option<UnauthorizedRetryDecision> {
    if !is_unauthorized_cursor_error(error_text) {
        return None;
    }
    if retry_attempt < retry_max {
        Some(UnauthorizedRetryDecision::Retry)
    } else {
        Some(UnauthorizedRetryDecision::Exhausted)
    }
}

pub fn timeout_fallback_hint(error_text: &str, max_time_ms: Option<u64>) -> String {
    if error_text.contains("MaxTimeMSExpired") || error_text.contains("Error code 50") {
        match max_time_ms {
            Some(value) if value > 0 => {
                format!("; timeout hit (source.max_time_ms={value}ms)")
            }
            _ => "; timeout hit".to_owned(),
        }
    } else {
        String::new()
    }
}

/// Infer the schema for a single collection, print stats to stderr, and optionally write output files.
///
/// `output_name` controls the directory and file names under `output_dir`.
/// `output_dir_override`, when provided, is used instead of `args.output_dir`.
pub async fn infer_collection(
    client: &Client,
    db_name: &str,
    coll_name: &str,
    output_name: &str,
    target_schema: Option<&str>,
    include: &[String],
    exclude: &[String],
    timestamp_fields: &[String],
    args: &InferArgs,
    output_dir_override: Option<&Path>,
    known_root_table_names: Option<&HashSet<String>>,
    progress: Option<(usize, usize)>,
    emit_stats: bool,
) -> Result<CollectionSchema> {
    let collection_label = format!("{db_name}.{coll_name}");
    let progress_prefix = progress.map(|(current, total)| format!("[{current}/{total}] "));

    let output_dir = output_dir_override.or(args.output_dir.as_deref());
    let db = client.database(db_name);
    let collection = db.collection::<bson::Document>(coll_name);

    let (sample_size, known_total, sample_basis) = if let Some(pct) = args.percent {
        if pct <= 0.0 || pct > 100.0 {
            return Err(anyhow!(
                "--percent must be between 0 (exclusive) and 100 (inclusive), got {pct}"
            ));
        }
        let total = collection
            .estimated_document_count()
            .await
            .context("Failed to get document count for --percent calculation")?;
        let n = ((total as f64 * pct / 100.0).ceil() as u64).max(1);
        (
            n,
            Some(total),
            format!("sample: --percent {pct}% => {n}/{total} docs"),
        )
    } else {
        let n = args.number.unwrap_or(1000);
        (n, None, format!("sample: --number {n} docs"))
    };

    if let Some(prefix) = &progress_prefix {
        info!("{prefix}Inferring {collection_label} ({sample_basis})");
    } else {
        info!("Inferring {collection_label} ({sample_basis})");
    };

    let mut analyzer = Analyzer::new(true);
    let sample_max_time = infer_query_max_time(args.max_time_ms);
    let fallback_chunk_size = resolve_infer_chunk_size(args.chunk_size)?;
    let fallback_auth_retry_max = resolve_infer_auth_retry_max(args.auth_retry_max)?;

    // Try $sample; on any error fall back to a sequential find().limit().
    // $sample internally sorts documents, which can fail on provider2 shared tiers
    // (error 292 – sort memory limit) or emit deserialization errors on some
    // server/driver combinations.  find().limit() has no sort stage and works
    // on those tiers.  If find() also fails (e.g. error 241 in a broken view
    // pipeline), infer_collection returns that error and batch callers skip.
    let pipeline = vec![doc! { "$sample": { "size": sample_size as i64 } }];
    // Errors from $sample (sort memory limit, deserialization, etc.) surface during
    // cursor iteration, not at this .await.  The cursor loop below handles all of them
    // and falls back to find().limit() as needed.
    let sample_result = collection
        .aggregate(pipeline)
        .allow_disk_use(true)
        .max_time(sample_max_time)
        .await;

    /// Run chunked `find().skip().limit()` into `analyzer`, logging any error without propagating.
    async fn find_fallback(
        collection: &mongodb::Collection<bson::Document>,
        analyzer: &mut Analyzer,
        sample_size: u64,
        chunk_size: u64,
        auth_retry_max: u32,
        sample_max_time: Duration,
        db_name: &str,
        coll_name: &str,
    ) -> Result<()> {
        let total_chunks = sample_size.div_ceil(chunk_size).max(1);
        let mut processed = 0_u64;
        let mut chunk_index = 0_u64;
        let mut last_processed_id: Option<bson::Bson> = None;

        while processed < sample_size {
            chunk_index += 1;
            let remaining = sample_size - processed;
            let this_chunk = remaining.min(chunk_size);
            let chunk_start_id = last_processed_id.clone();
            info!(
                "chunk {}/{} size={} processed={}/{} collection={}.{} start_after_id={}",
                chunk_index,
                total_chunks,
                this_chunk,
                processed,
                sample_size,
                db_name,
                coll_name,
                chunk_start_id
                    .as_ref()
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "<begin>".to_owned())
            );

            let mut auth_retry_attempt = 0_u32;

            'retry_chunk: loop {
                let filter = match chunk_start_id.as_ref() {
                    Some(last_id) => doc! { "_id": { "$gt": last_id.clone() } },
                    None => doc! {},
                };
                let cursor_result = collection
                    .find(filter)
                    .sort(doc! { "_id": 1 })
                    .limit(this_chunk as i64)
                    .max_time(sample_max_time)
                    .await;

                let mut chunk_docs = 0_u64;
                let mut chunk_last_id: Option<bson::Bson> = None;
                let mut cur = match cursor_result {
                    Ok(cur) => cur,
                    Err(e) => {
                        warn!(
                            "find() chunk failed for {}.{} at chunk {}/{} (start_after_id={}, limit={}): {:#}",
                            db_name,
                            coll_name,
                            chunk_index,
                            total_chunks,
                            chunk_start_id
                                .as_ref()
                                .map(|id| id.to_string())
                                .unwrap_or_else(|| "<begin>".to_owned()),
                            this_chunk,
                            e
                        );
                        break;
                    }
                };

                loop {
                    match cur.try_next().await {
                        Ok(Some(d)) => {
                            chunk_last_id = d.get("_id").cloned();
                            analyzer.process_document(&d);
                            chunk_docs += 1;
                        }
                        Ok(None) => break,
                        Err(e) => {
                            let error_text = e.to_string();
                            match classify_unauthorized_retry(
                                &error_text,
                                auth_retry_attempt,
                                auth_retry_max,
                            ) {
                                Some(UnauthorizedRetryDecision::Retry) => {
                                    auth_retry_attempt += 1;
                                    warn!(
                                        "auth_retry namespace={}.{} chunk={}/{} processed={}/{} retry_attempt={}/{} reason={}",
                                        db_name,
                                        coll_name,
                                        chunk_index,
                                        total_chunks,
                                        processed,
                                        sample_size,
                                        auth_retry_attempt,
                                        auth_retry_max,
                                        error_text
                                    );
                                    continue 'retry_chunk;
                                }
                                Some(UnauthorizedRetryDecision::Exhausted) => {
                                    warn!(
                                        "auth_retry_exhausted namespace={}.{} chunk={}/{} processed={}/{} retries={} reason={}",
                                        db_name,
                                        coll_name,
                                        chunk_index,
                                        total_chunks,
                                        processed,
                                        sample_size,
                                        auth_retry_max,
                                        error_text
                                    );
                                    return Err(anyhow!(
                                        "Unauthorized cursor iteration persists for {}.{} at chunk {}/{} after {} retries",
                                        db_name,
                                        coll_name,
                                        chunk_index,
                                        total_chunks,
                                        auth_retry_max
                                    ));
                                }
                                None => {
                                    warn!(
                                        "find() chunk cursor error for {}.{} at chunk {}/{}: {:#}",
                                        db_name, coll_name, chunk_index, total_chunks, e
                                    );
                                    break;
                                }
                            }
                        }
                    }
                }

                if chunk_docs == 0 {
                    break;
                }

                last_processed_id = chunk_last_id.or(chunk_start_id);
                processed = processed.saturating_add(chunk_docs);
                if chunk_docs < this_chunk {
                    break;
                }
                break;
            }
        }

        Ok(())
    }

    match sample_result {
        Err(e) => {
            let timeout_hint = timeout_fallback_hint(&e.to_string(), args.max_time_ms);
            warn!(
                "$sample failed for {db_name}.{coll_name} \
                 ({e}){timeout_hint}; falling back to chunked sequential find() with chunk_size={fallback_chunk_size} target={sample_size}"
            );
            find_fallback(
                &collection,
                &mut analyzer,
                sample_size,
                fallback_chunk_size,
                fallback_auth_retry_max,
                sample_max_time,
                db_name,
                coll_name,
            )
            .await?;
        }
        Ok(mut cursor) => loop {
            match cursor.try_next().await {
                Ok(Some(doc)) => analyzer.process_document(&doc),
                Ok(None) => break,
                Err(e) => {
                    analyzer = Analyzer::new(true);
                    let timeout_hint = timeout_fallback_hint(&e.to_string(), args.max_time_ms);
                    warn!(
                        "$sample cursor error for {db_name}.{coll_name} \
                            ({e}){timeout_hint}; falling back to chunked sequential find() with chunk_size={fallback_chunk_size} target={sample_size}"
                    );
                    find_fallback(
                        &collection,
                        &mut analyzer,
                        sample_size,
                        fallback_chunk_size,
                        fallback_auth_retry_max,
                        sample_max_time,
                        db_name,
                        coll_name,
                    )
                    .await?;
                    break;
                }
            }
        },
    }

    let mut schema = analyzer.finish();
    apply_collection_property_filters(&mut schema, coll_name, include, exclude);
    let total_docs = if let Some(t) = known_total {
        t
    } else {
        collection
            .estimated_document_count()
            .await
            .unwrap_or(schema.sampled)
    };
    schema.count = total_docs;
    if args.jsonb {
        schema.mark_objects_as_jsonb();
    }
    let infer_warnings = infer_warnings_to_yaml(&schema);
    let read_ops = fetch_collection_read_ops_stats(&db, &collection).await;
    let has_search_node = detect_search_node_capability(&collection).await;
    emit_infer_type_warnings(db_name, coll_name, &schema);
    let output_dir = output_dir; // rebind to keep borrow checker happy

    let stats_lines = format_stats(&schema, Some(total_docs));

    if emit_stats {
        let stderr = io::stderr();
        let mut handle = stderr.lock();
        if let Some(prefix) = &progress_prefix {
            writeln!(handle, "{prefix}{collection_label} ({sample_basis})")?;
        } else {
            writeln!(handle, "[{collection_label}] ({sample_basis})")?;
        }
        for line in &stats_lines {
            writeln!(handle, "{line}")?;
        }
    }

    if let Some(out_dir) = output_dir {
        write_collection_files(
            out_dir,
            db_name,
            output_name,
            target_schema,
            timestamp_fields,
            known_root_table_names,
            &schema,
            &stats_lines,
            &infer_warnings,
            read_ops,
            has_search_node,
        )
        .with_context(|| format!("Failed to write output files for {output_name}"))?;
    }

    Ok(schema)
}

pub fn bson_as_u64(value: &Bson) -> Option<u64> {
    match value {
        Bson::Int32(v) if *v >= 0 => Some(*v as u64),
        Bson::Int64(v) if *v >= 0 => Some(*v as u64),
        Bson::Double(v) if v.is_finite() && *v >= 0.0 => Some(*v as u64),
        Bson::Decimal128(v) => v.to_string().parse::<u64>().ok(),
        _ => None,
    }
}

pub fn bson_as_timestamp_string(value: &Bson) -> Option<String> {
    match value {
        Bson::DateTime(dt) => {
            chrono::DateTime::<chrono::Utc>::from_timestamp_millis(dt.timestamp_millis())
                .map(|ts| ts.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        }
        Bson::String(text) if !text.trim().is_empty() => Some(text.trim().to_owned()),
        _ => None,
    }
}

pub fn bson_as_f64(value: &Bson) -> Option<f64> {
    match value {
        Bson::Int32(v) => Some(*v as f64),
        Bson::Int64(v) => Some(*v as f64),
        Bson::Double(v) if v.is_finite() => Some(*v),
        Bson::Decimal128(v) => v.to_string().parse::<f64>().ok(),
        _ => None,
    }
}

pub fn since_from_uptime_seconds(uptime_seconds: f64) -> Option<String> {
    if !uptime_seconds.is_finite() || uptime_seconds < 0.0 {
        return None;
    }

    let uptime_millis = (uptime_seconds * 1000.0).round() as i64;
    let since = chrono::Utc::now() - chrono::Duration::milliseconds(uptime_millis);
    Some(since.format("%Y-%m-%d %H:%M:%S UTC").to_string())
}

pub async fn fetch_server_uptime_since(db: &mongodb::Database) -> Option<String> {
    let server_status = db.run_command(doc! { "serverStatus": 1 }).await.ok()?;
    let uptime_seconds = server_status.get("uptime").and_then(bson_as_f64)?;
    since_from_uptime_seconds(uptime_seconds)
}

pub async fn fetch_collection_read_ops_stats(
    db: &mongodb::Database,
    collection: &mongodb::Collection<bson::Document>,
) -> Option<CollectionReadOpsYaml> {
    let mut cursor = collection
        .aggregate(vec![doc! {
            "$collStats": { "latencyStats": { "histograms": false } }
        }])
        .await
        .ok()?;

    let stats_doc = cursor.try_next().await.ok().flatten()?;
    let latency_stats = stats_doc.get_document("latencyStats").ok()?;
    let reads = latency_stats.get_document("reads").ok()?;
    let read_ops = reads.get("ops").and_then(bson_as_u64)?;
    let mut since = reads.get("since").and_then(bson_as_timestamp_string);
    if since.is_none() {
        since = fetch_server_uptime_since(db).await;
    }

    Some(CollectionReadOpsYaml { read_ops, since })
}

pub async fn detect_search_node_capability(
    collection: &mongodb::Collection<bson::Document>,
) -> bool {
    collection
        .aggregate(vec![
            doc! { "$listSearchIndexes": {} },
            doc! { "$limit": 1 },
        ])
        .await
        .is_ok()
}

pub fn apply_collection_property_filters(
    schema: &mut CollectionSchema,
    coll_name: &str,
    include: &[String],
    exclude: &[String],
) {
    let has_collection_wide_include = include.iter().any(|entry| entry == coll_name);
    let included_properties = property_filter_entries_for_collection(coll_name, include);
    let excluded_properties = property_filter_entries_for_collection(coll_name, exclude);

    if !has_collection_wide_include && !included_properties.is_empty() {
        schema.object.retain(|field_name, _| {
            field_name == "_id"
                || included_properties
                    .iter()
                    .any(|property| *property == field_name)
        });
    }

    if !excluded_properties.is_empty() {
        schema.object.retain(|field_name, _| {
            !excluded_properties
                .iter()
                .any(|property| *property == field_name)
        });
    }
}

pub fn schema_root_id_is_objectid(schema: &CollectionSchema) -> bool {
    let Some(id_field) = schema.object.get("_id") else {
        return false;
    };

    let non_null_types = id_field
        .types
        .iter()
        .filter(|(type_name, _)| !matches!(type_name.as_str(), TYPE_NULL | TYPE_UNDEFINED))
        .map(|(type_name, _)| type_name.as_str())
        .collect::<Vec<_>>();

    non_null_types.len() == 1 && non_null_types[0] == "ObjectId"
}

pub fn mapping_has_surrogate_bigserial_primary_id(tables: &[DdlTableMapping]) -> bool {
    tables.iter().any(|table| {
        table.columns.iter().any(|column| {
            let sql_type = column.sql_type.trim().to_ascii_lowercase();
            column.primary_key
                && column.name == "id"
                && (sql_type == "bigserial" || sql_type == "serial8")
        })
    })
}

pub fn mapping_has_flattened_parent_uuid_column(
    tables: &[DdlTableMapping],
    table_name: &str,
) -> bool {
    let expected_parent_id = flattened_root_parent_id_column(table_name);
    tables.iter().any(|table| {
        table.columns.iter().any(|column| {
            column.name == expected_parent_id && column.sql_type.eq_ignore_ascii_case("uuid")
        })
    })
}

pub fn should_regenerate_from_schema_when_objectid_pk(
    schema: &CollectionSchema,
    mapping_tables: &[DdlTableMapping],
    table_name: &str,
) -> bool {
    schema_root_id_is_objectid(schema)
        && mapping_has_surrogate_bigserial_primary_id(mapping_tables)
        && !mapping_has_flattened_parent_uuid_column(mapping_tables, table_name)
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn build_collection_mappings(
    db_name: &str,
    coll_name: &str,
    schema_name: Option<&str>,
    schema: &CollectionSchema,
) -> Vec<(String, CollectionMapping)> {
    build_collection_mappings_with_timestamp_fields(
        db_name,
        coll_name,
        schema_name,
        schema,
        &[],
        &std::collections::HashSet::new(),
    )
}

pub fn build_collection_mappings_with_timestamp_fields(
    db_name: &str,
    coll_name: &str,
    schema_name: Option<&str>,
    schema: &CollectionSchema,
    timestamp_fields: &[String],
    reserved_table_names: &std::collections::HashSet<String>,
) -> Vec<(String, CollectionMapping)> {
    fn is_uuid_like_key(name: &str) -> bool {
        let parts = name.split('-').collect::<Vec<_>>();
        name.len() == 36
            && parts.len() == 5
            && parts[0].len() == 8
            && parts[1].len() == 4
            && parts[2].len() == 4
            && parts[3].len() == 4
            && parts[4].len() == 12
            && parts
                .iter()
                .all(|part| part.chars().all(|ch| ch.is_ascii_hexdigit()))
    }

    fn map_document_value_fields<'a>(
        sub_fields: &'a IndexMap<String, FieldSchema>,
    ) -> Option<&'a IndexMap<String, FieldSchema>> {
        if sub_fields.is_empty()
            || !sub_fields.keys().all(|key| {
                !key.is_empty()
                    && key.chars().all(|ch| ch.is_ascii_hexdigit())
                    && (key.len() >= 8 || is_uuid_like_key(key))
            })
        {
            return None;
        }

        for field in sub_fields.values() {
            let non_null = field
                .types
                .iter()
                .filter(|(type_name, _)| !matches!(type_name.as_str(), "Null" | "Undefined"))
                .collect::<Vec<_>>();
            if non_null.len() == 1
                && non_null[0].0.as_str() == "Object"
                && non_null[0]
                    .1
                    .object
                    .as_ref()
                    .is_some_and(|obj| !obj.is_empty())
            {
                return non_null[0].1.object.as_ref();
            }
        }

        None
    }

    fn is_geojson_point_field(field: &FieldSchema) -> bool {
        let non_null: Vec<(&str, &TypeSchema)> = field
            .types
            .iter()
            .filter(|(type_name, _)| !matches!(type_name.as_str(), "Null" | "Undefined"))
            .map(|(type_name, type_schema)| (type_name.as_str(), type_schema))
            .collect();
        if non_null.len() != 1 || non_null[0].0 != "Object" {
            return false;
        }

        let Some(obj_fields) = non_null[0].1.object.as_ref() else {
            return false;
        };

        let Some(type_field) = obj_fields.get("type") else {
            return false;
        };
        let type_has_string = type_field
            .types
            .iter()
            .filter(|(type_name, _)| !matches!(type_name.as_str(), "Null" | "Undefined"))
            .any(|(type_name, _)| type_name == "String");
        if !type_has_string {
            return false;
        }

        let has_point_type_value = type_field
            .types
            .get("String")
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
            .filter(|(type_name, _)| !matches!(type_name.as_str(), "Null" | "Undefined"))
            .any(|(type_name, _)| type_name == "Array")
    }

    fn field_has_geo_merged_doc_shape(field: &FieldSchema) -> bool {
        let non_null: Vec<(&str, &TypeSchema)> = field
            .types
            .iter()
            .filter(|(type_name, _)| !matches!(type_name.as_str(), "Null" | "Undefined"))
            .map(|(type_name, type_schema)| (type_name.as_str(), type_schema))
            .collect();
        if non_null.len() != 1 || non_null[0].0 != "Object" {
            return false;
        }

        let Some(sub_fields) = non_null[0].1.object.as_ref() else {
            return false;
        };

        let mut geo_count = 0_usize;
        let mut sibling_object_count = 0_usize;
        for sub_field in sub_fields.values() {
            let sub_non_null: Vec<(&str, &TypeSchema)> = sub_field
                .types
                .iter()
                .filter(|(type_name, _)| !matches!(type_name.as_str(), "Null" | "Undefined"))
                .map(|(type_name, type_schema)| (type_name.as_str(), type_schema))
                .collect();
            if sub_non_null.is_empty() {
                continue;
            }

            if sub_non_null.len() == 1
                && sub_non_null[0].0 == "Object"
                && is_geojson_point_field(sub_field)
            {
                geo_count += 1;
                continue;
            }

            if sub_non_null.len() == 1 && sub_non_null[0].0 == "Object" {
                sibling_object_count += 1;
                continue;
            }

            return false;
        }

        geo_count == 1 && sibling_object_count == 1
    }

    fn preferred_child_mapping_table_name(
        parent_name: &str,
        field: &str,
        force_parent_prefix: bool,
    ) -> String {
        let field = sanitize_pg_name(field);
        if force_parent_prefix {
            return format!("{parent_name}_{field}");
        }
        let ancestor_segments = parent_name.split('_').collect::<Vec<_>>();
        if ancestor_segments.iter().any(|segment| *segment == field) {
            let parent_segment = ancestor_segments.last().copied().unwrap_or(parent_name);
            format!("{parent_segment}_{field}")
        } else {
            field
        }
    }

    fn child_name_lookup_key(parent_name: &str, field: &str) -> String {
        format!("{parent_name}\0{}", sanitize_pg_name(field))
    }

    fn unique_child_mapping_table_name(
        parent_name: &str,
        field: &str,
        force_parent_prefix: bool,
        reserved_table_names: &std::collections::HashSet<String>,
        assigned_table_names: &std::collections::HashSet<String>,
    ) -> String {
        let base = preferred_child_mapping_table_name(parent_name, field, force_parent_prefix);
        let is_taken =
            |name: &str| reserved_table_names.contains(name) || assigned_table_names.contains(name);

        if !is_taken(&base) {
            return base;
        }

        let parent_segments = parent_name
            .split('_')
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>();

        for depth in 1..=parent_segments.len() {
            let prefix = parent_segments[parent_segments.len() - depth..].join("_");
            let candidate = format!("{prefix}_{base}");
            if !is_taken(&candidate) {
                return candidate;
            }
        }

        let mut suffix = 2usize;
        loop {
            let candidate = format!("{parent_name}_{base}_{suffix}");
            if !is_taken(&candidate) {
                return candidate;
            }
            suffix += 1;
        }
    }

    fn resolved_child_mapping_table_name(
        parent_name: &str,
        field: &str,
        resolved_child_table_names: &HashMap<String, String>,
    ) -> String {
        resolved_child_table_names
            .get(&child_name_lookup_key(parent_name, field))
            .cloned()
            .unwrap_or_else(|| preferred_child_mapping_table_name(parent_name, field, false))
    }

    fn collect_resolved_child_table_names(
        old_parent_name: &str,
        new_parent_name: &str,
        fields: &IndexMap<String, FieldSchema>,
        is_root: bool,
        reserved_table_names: &std::collections::HashSet<String>,
        assigned_table_names: &mut std::collections::HashSet<String>,
        table_renames: &mut HashMap<String, String>,
        resolved_child_table_names: &mut HashMap<String, String>,
    ) {
        let grouped_root_fields = if is_root {
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
                let old_child =
                    preferred_child_mapping_table_name(old_parent_name, raw_name, false);
                let new_child = unique_child_mapping_table_name(
                    new_parent_name,
                    raw_name,
                    false,
                    reserved_table_names,
                    assigned_table_names,
                );
                assigned_table_names.insert(new_child.clone());
                table_renames.insert(old_child.clone(), new_child.clone());
                resolved_child_table_names.insert(
                    child_name_lookup_key(new_parent_name, raw_name),
                    new_child.clone(),
                );
                collect_resolved_child_table_names(
                    &old_child,
                    &new_child,
                    &group.child_fields,
                    false,
                    reserved_table_names,
                    assigned_table_names,
                    table_renames,
                    resolved_child_table_names,
                );
                continue;
            }
            if grouped_members.contains(raw_name) {
                continue;
            }

            let non_null: Vec<(&str, &TypeSchema)> = field
                .types
                .iter()
                .filter(|(type_name, _)| !matches!(type_name.as_str(), "Null" | "Undefined"))
                .map(|(type_name, type_schema)| (type_name.as_str(), type_schema))
                .collect();

            let child_fields = if non_null.len() == 1 && non_null[0].0 == "Object" {
                let type_schema = non_null[0].1;
                if type_schema.as_jsonb {
                    None
                } else {
                    type_schema.object.as_ref()
                }
            } else if non_null.len() == 1 && non_null[0].0 == "Array" {
                non_null[0]
                    .1
                    .array
                    .as_ref()
                    .and_then(|items_field| items_field.types.get("Object"))
                    .and_then(|type_schema| type_schema.object.as_ref())
            } else {
                None
            };

            if child_fields.is_some() || (non_null.len() == 1 && non_null[0].0 == "Array") {
                let force_parent_prefix = field_has_geo_merged_doc_shape(field);
                let old_child = preferred_child_mapping_table_name(
                    old_parent_name,
                    raw_name,
                    force_parent_prefix,
                );
                let new_child = unique_child_mapping_table_name(
                    new_parent_name,
                    raw_name,
                    force_parent_prefix,
                    reserved_table_names,
                    assigned_table_names,
                );
                assigned_table_names.insert(new_child.clone());
                table_renames.insert(old_child.clone(), new_child.clone());
                resolved_child_table_names.insert(
                    child_name_lookup_key(new_parent_name, raw_name),
                    new_child.clone(),
                );

                if let Some(child_fields) = child_fields {
                    collect_resolved_child_table_names(
                        &old_child,
                        &new_child,
                        child_fields,
                        false,
                        reserved_table_names,
                        assigned_table_names,
                        table_renames,
                        resolved_child_table_names,
                    );
                }
            }
        }
    }

    fn find_source_field_for_column(
        fields: &IndexMap<String, FieldSchema>,
        column_name: &str,
        is_root: bool,
    ) -> Option<String> {
        fn find_geo_merged_source_field(
            fields: &IndexMap<String, FieldSchema>,
            column_name: &str,
        ) -> Option<String> {
            for (raw_name, field) in fields {
                let non_null = field
                    .types
                    .iter()
                    .filter(|(type_name, _)| !matches!(type_name.as_str(), "Null" | "Undefined"))
                    .collect::<Vec<_>>();
                if non_null.len() != 1 || non_null[0].0.as_str() != "Object" {
                    continue;
                }
                let Some(sub_fields) = non_null[0].1.object.as_ref() else {
                    continue;
                };

                let mut geo_field_name: Option<&str> = None;
                let mut sibling_obj_name: Option<&str> = None;
                let mut sibling_obj_fields: Option<&IndexMap<String, FieldSchema>> = None;
                let mut valid = true;

                for (sub_name, sub_field) in sub_fields {
                    let sub_non_null = sub_field
                        .types
                        .iter()
                        .filter(|(type_name, _)| {
                            !matches!(type_name.as_str(), "Null" | "Undefined")
                        })
                        .collect::<Vec<_>>();
                    if sub_non_null.is_empty() {
                        continue;
                    }
                    if sub_non_null.len() != 1 || sub_non_null[0].0.as_str() != "Object" {
                        valid = false;
                        break;
                    }

                    if is_geojson_point_field(sub_field) {
                        if geo_field_name.is_some() {
                            valid = false;
                            break;
                        }
                        geo_field_name = Some(sub_name.as_str());
                    } else {
                        if sibling_obj_name.is_some() {
                            valid = false;
                            break;
                        }
                        sibling_obj_name = Some(sub_name.as_str());
                        sibling_obj_fields = sub_non_null[0].1.object.as_ref();
                    }
                }

                if !valid {
                    continue;
                }

                if let Some(geo_name) = geo_field_name {
                    if sanitize_pg_name(geo_name) == column_name {
                        return Some(format!("{raw_name}.{geo_name}"));
                    }
                }

                let (Some(sibling_name), Some(sibling_fields)) =
                    (sibling_obj_name, sibling_obj_fields)
                else {
                    continue;
                };

                for (path, _) in inline_object_leaf_fields_with_prefix(sibling_fields, &[]) {
                    if let Some(last) = path.last() {
                        if sanitize_pg_name(last) == column_name {
                            return Some(format!("{raw_name}.{sibling_name}.{}", path.join(".")));
                        }
                    }
                }
            }

            None
        }

        fn reserved_inline_sibling_names(
            fields: &IndexMap<String, FieldSchema>,
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
                    .filter(|(type_name, _)| !matches!(type_name.as_str(), "Null" | "Undefined"))
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
                            reserved.insert(sanitize(&path.join("_")));
                        }
                    }
                    continue;
                }

                if non_null.len() == 1 && non_null[0].0.as_str() == "Object" {
                    if let Some(sub_fields) = non_null[0].1.object.as_ref() {
                        if can_inline_object_fields(sub_fields) {
                            for (path, _) in inline_object_leaf_fields_with_prefix(sub_fields, &[])
                            {
                                if let Some(last) = path.last() {
                                    reserved.insert(sanitize(last));
                                }
                            }
                            continue;
                        }
                    }
                }

                if raw_name == "_id" && is_root {
                    reserved.insert("id".to_owned());
                } else {
                    reserved.insert(sanitize(raw_name));
                }
            }

            reserved
        }

        fn find_nested_source_field(
            fields: &IndexMap<String, FieldSchema>,
            column_name: &str,
            is_root: bool,
        ) -> Option<String> {
            for (raw_name, field) in fields {
                let non_null = field
                    .types
                    .iter()
                    .filter(|(type_name, _)| !matches!(type_name.as_str(), "Null" | "Undefined"))
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

        if let Some(raw_name) = fields.keys().find(|raw_name| {
            sanitize_pg_name(raw_name) == column_name
                || normalize_pg_identifier(raw_name) == column_name
                || raw_name.as_str() == column_name
        }) {
            return Some(raw_name.clone());
        }

        if is_root {
            if let Some(id_object) = fields
                .get("_id")
                .and_then(|field| field.types.get("Object"))
                .and_then(|type_schema| type_schema.object.as_ref())
            {
                if let Some(raw_name) = id_object.keys().find(|raw_name| {
                    sanitize_pg_name(raw_name) == column_name
                        || normalize_pg_identifier(raw_name) == column_name
                        || raw_name.as_str() == column_name
                }) {
                    return Some(raw_name.clone());
                }
            }

            if let Some(source_field) = find_geo_merged_source_field(fields, column_name) {
                return Some(source_field);
            }
        }

        find_nested_source_field(fields, column_name, is_root)
    }

    fn build_mapping_columns(
        table: &crate::schema_diagram::Table,
        fields: &IndexMap<String, FieldSchema>,
        is_root: bool,
    ) -> Vec<MappingColumn> {
        let foreign_key_columns = table
            .foreign_keys
            .iter()
            .map(|fk| fk.from_col.as_str())
            .collect::<Vec<_>>();

        table
            .columns
            .iter()
            .filter(|column| foreign_key_columns.iter().all(|fk| *fk != column.name))
            .filter_map(|column| {
                let target_field = normalize_pg_identifier(&column.name);
                if !is_root && target_field == "id" {
                    return None;
                }

                let source_field = find_source_field_for_column(fields, &target_field, is_root)?;
                Some(MappingColumn {
                    source_field,
                    target_field,
                    data_type: column.col_type.to_lowercase(),
                    nullable: !column.not_null,
                    literal_value: None,
                })
            })
            .collect()
    }

    fn collect_table_mappings(
        db_name: &str,
        root_collection_name: &str,
        schema_name: &str,
        table_name: &str,
        file_stem: &str,
        mongo_path_segments: &[String],
        fields: &IndexMap<String, FieldSchema>,
        is_root: bool,
        emit_current: bool,
        tables_by_name: &HashMap<String, crate::schema_diagram::Table>,
        resolved_child_table_names: &HashMap<String, String>,
        out: &mut Vec<(String, CollectionMapping)>,
    ) {
        fn table_has_child_references(
            table_name: &str,
            tables_by_name: &HashMap<String, crate::schema_diagram::Table>,
        ) -> bool {
            tables_by_name.values().any(|candidate| {
                candidate
                    .foreign_keys
                    .iter()
                    .any(|fk| fk.to_table == table_name)
            })
        }

        let grouped_root_fields = if is_root {
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

        if emit_current {
            if let Some(table) = tables_by_name.get(table_name) {
                let columns = build_mapping_columns(table, fields, is_root);
                if !columns.is_empty() || table_has_child_references(&table.name, tables_by_name) {
                    let mapping_collection_name = mongo_path_segments
                        .last()
                        .cloned()
                        .unwrap_or_else(|| root_collection_name.to_owned());
                    out.push((
                        file_stem.to_owned(),
                        CollectionMapping {
                            collection_name: mapping_collection_name,
                            mongo_dbname: db_name.to_owned(),
                            mongo_path: mapping_mongo_path_for_segments(
                                root_collection_name,
                                mongo_path_segments,
                            ),
                            traversal: None,
                            pg_mapping: PgMapping {
                                dbname: db_name.to_owned(),
                                schema_name: schema_name.to_owned(),
                                table_name: table.name.clone(),
                                columns,
                                ddl: Some(ddl_table_mapping_from_table(table)),
                                ddl_editing: default_ddl_editing_guidance(),
                            },
                        },
                    ));
                }
            }
        }

        for (raw_name, field) in fields {
            if let Some(group) = grouped_representatives.get(raw_name) {
                let child_table = resolved_child_mapping_table_name(
                    table_name,
                    raw_name,
                    resolved_child_table_names,
                );
                if let Some(table) = tables_by_name.get(&child_table) {
                    let foreign_key_columns = table
                        .foreign_keys
                        .iter()
                        .map(|fk| fk.from_col.as_str())
                        .collect::<Vec<_>>();
                    let columns = table
                        .columns
                        .iter()
                        .filter(|column| {
                            column.name != "id"
                                && foreign_key_columns.iter().all(|fk| *fk != column.name)
                        })
                        .filter_map(|column| {
                            let target_field = normalize_pg_identifier(&column.name);
                            if target_field == "key" {
                                Some(MappingColumn {
                                    source_field: "key".to_owned(),
                                    target_field,
                                    data_type: column.col_type.to_lowercase(),
                                    nullable: !column.not_null,
                                    literal_value: None,
                                })
                            } else {
                                find_source_field_for_column(
                                    &group.child_fields,
                                    &target_field,
                                    false,
                                )
                                .map(|source_field| {
                                    MappingColumn {
                                        source_field,
                                        target_field,
                                        data_type: column.col_type.to_lowercase(),
                                        nullable: !column.not_null,
                                        literal_value: None,
                                    }
                                })
                            }
                        })
                        .collect::<Vec<_>>();
                    if !columns.is_empty()
                        || table_has_child_references(&table.name, tables_by_name)
                    {
                        let mut child_mongo_path_segments = mongo_path_segments.to_vec();
                        child_mongo_path_segments.push(raw_name.clone());
                        out.push((
                            child_table.clone(),
                            CollectionMapping {
                                collection_name: raw_name.clone(),
                                mongo_dbname: db_name.to_owned(),
                                mongo_path: mapping_mongo_path_for_segments(
                                    root_collection_name,
                                    &child_mongo_path_segments,
                                ),
                                traversal: None,
                                pg_mapping: PgMapping {
                                    dbname: db_name.to_owned(),
                                    schema_name: schema_name.to_owned(),
                                    table_name: table.name.clone(),
                                    columns,
                                    ddl: Some(ddl_table_mapping_from_table(table)),
                                    ddl_editing: default_ddl_editing_guidance(),
                                },
                            },
                        ));
                    }
                }

                collect_table_mappings(
                    db_name,
                    root_collection_name,
                    schema_name,
                    &child_table,
                    &child_table,
                    &{
                        let mut child_mongo_path_segments = mongo_path_segments.to_vec();
                        child_mongo_path_segments.push(raw_name.clone());
                        child_mongo_path_segments
                    },
                    &group.child_fields,
                    false,
                    false,
                    tables_by_name,
                    resolved_child_table_names,
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
                .filter(|(type_name, _)| !matches!(type_name.as_str(), "Null" | "Undefined"))
                .map(|(type_name, type_schema)| (type_name.as_str(), type_schema))
                .collect();

            if non_null.len() == 1 && non_null[0].0 == "Object" {
                let type_schema = non_null[0].1;
                if type_schema.as_jsonb {
                    continue;
                }
                if let Some(sub_fields) = &type_schema.object {
                    let child_table = resolved_child_mapping_table_name(
                        table_name,
                        raw_name,
                        resolved_child_table_names,
                    );

                    if field_has_geo_merged_doc_shape(field) {
                        if let Some(table) = tables_by_name.get(&child_table) {
                            let foreign_key_columns = table
                                .foreign_keys
                                .iter()
                                .map(|fk| fk.from_col.as_str())
                                .collect::<Vec<_>>();
                            let mut geo_merged_lookup_fields = IndexMap::new();
                            geo_merged_lookup_fields.insert(raw_name.clone(), field.clone());
                            let columns = table
                                .columns
                                .iter()
                                .filter(|column| {
                                    column.name != "id"
                                        && foreign_key_columns.iter().all(|fk| *fk != column.name)
                                })
                                .filter_map(|column| {
                                    let target_field = normalize_pg_identifier(&column.name);
                                    find_source_field_for_column(
                                        &geo_merged_lookup_fields,
                                        &target_field,
                                        true,
                                    )
                                    .map(|source_field| {
                                        MappingColumn {
                                            source_field,
                                            target_field,
                                            data_type: column.col_type.to_lowercase(),
                                            nullable: !column.not_null,
                                            literal_value: None,
                                        }
                                    })
                                })
                                .collect::<Vec<_>>();

                            if !columns.is_empty()
                                || table_has_child_references(&table.name, tables_by_name)
                            {
                                out.push((
                                    child_table.clone(),
                                    CollectionMapping {
                                        collection_name: raw_name.clone(),
                                        mongo_dbname: db_name.to_owned(),
                                        mongo_path: mapping_mongo_path_for_segments(
                                            root_collection_name,
                                            mongo_path_segments,
                                        ),
                                        traversal: None,
                                        pg_mapping: PgMapping {
                                            dbname: db_name.to_owned(),
                                            schema_name: schema_name.to_owned(),
                                            table_name: table.name.clone(),
                                            columns,
                                            ddl: Some(ddl_table_mapping_from_table(table)),
                                            ddl_editing: default_ddl_editing_guidance(),
                                        },
                                    },
                                ));
                            }
                        }

                        continue;
                    }

                    if let Some(value_fields) = map_document_value_fields(sub_fields) {
                        if let Some(table) = tables_by_name.get(&child_table) {
                            let foreign_key_columns = table
                                .foreign_keys
                                .iter()
                                .map(|fk| fk.from_col.as_str())
                                .collect::<Vec<_>>();
                            let columns = table
                                .columns
                                .iter()
                                .filter(|column| {
                                    column.name != "id"
                                        && foreign_key_columns.iter().all(|fk| *fk != column.name)
                                })
                                .filter_map(|column| {
                                    let target_field = normalize_pg_identifier(&column.name);
                                    if target_field == "key" {
                                        Some(MappingColumn {
                                            source_field: "key".to_owned(),
                                            target_field,
                                            data_type: column.col_type.to_lowercase(),
                                            nullable: !column.not_null,
                                            literal_value: None,
                                        })
                                    } else {
                                        find_source_field_for_column(
                                            value_fields,
                                            &target_field,
                                            false,
                                        )
                                        .map(
                                            |source_field| MappingColumn {
                                                source_field,
                                                target_field,
                                                data_type: column.col_type.to_lowercase(),
                                                nullable: !column.not_null,
                                                literal_value: None,
                                            },
                                        )
                                    }
                                })
                                .collect::<Vec<_>>();

                            if !columns.is_empty()
                                || table_has_child_references(&table.name, tables_by_name)
                            {
                                let mut child_mongo_path_segments = mongo_path_segments.to_vec();
                                child_mongo_path_segments.push(raw_name.clone());
                                out.push((
                                    child_table.clone(),
                                    CollectionMapping {
                                        collection_name: raw_name.clone(),
                                        mongo_dbname: db_name.to_owned(),
                                        mongo_path: mapping_mongo_path_for_segments(
                                            root_collection_name,
                                            &child_mongo_path_segments,
                                        ),
                                        traversal: None,
                                        pg_mapping: PgMapping {
                                            dbname: db_name.to_owned(),
                                            schema_name: schema_name.to_owned(),
                                            table_name: table.name.clone(),
                                            columns,
                                            ddl: Some(ddl_table_mapping_from_table(table)),
                                            ddl_editing: default_ddl_editing_guidance(),
                                        },
                                    },
                                ));
                            }
                        }

                        collect_table_mappings(
                            db_name,
                            root_collection_name,
                            schema_name,
                            &child_table,
                            &child_table,
                            &{
                                let mut child_mongo_path_segments = mongo_path_segments.to_vec();
                                child_mongo_path_segments.push(raw_name.clone());
                                child_mongo_path_segments
                            },
                            value_fields,
                            false,
                            false,
                            tables_by_name,
                            resolved_child_table_names,
                            out,
                        );
                        continue;
                    }

                    collect_table_mappings(
                        db_name,
                        root_collection_name,
                        schema_name,
                        &child_table,
                        &child_table,
                        &{
                            let mut child_mongo_path_segments = mongo_path_segments.to_vec();
                            child_mongo_path_segments.push(raw_name.clone());
                            child_mongo_path_segments
                        },
                        sub_fields,
                        false,
                        true,
                        tables_by_name,
                        resolved_child_table_names,
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
                            let child_table = resolved_child_mapping_table_name(
                                table_name,
                                raw_name,
                                resolved_child_table_names,
                            );
                            collect_table_mappings(
                                db_name,
                                root_collection_name,
                                schema_name,
                                &child_table,
                                &child_table,
                                &{
                                    let mut child_mongo_path_segments =
                                        mongo_path_segments.to_vec();
                                    child_mongo_path_segments.push(raw_name.clone());
                                    child_mongo_path_segments
                                },
                                sub_fields,
                                false,
                                true,
                                tables_by_name,
                                resolved_child_table_names,
                                out,
                            );
                        }
                    } else {
                        let child_table = resolved_child_mapping_table_name(
                            table_name,
                            raw_name,
                            resolved_child_table_names,
                        );
                        if let Some(table) = tables_by_name.get(&child_table) {
                            let foreign_key_columns = table
                                .foreign_keys
                                .iter()
                                .map(|fk| fk.from_col.as_str())
                                .collect::<Vec<_>>();
                            let columns = table
                                .columns
                                .iter()
                                .filter(|column| {
                                    column.name != "id"
                                        && foreign_key_columns.iter().all(|fk| *fk != column.name)
                                })
                                .map(|column| MappingColumn {
                                    source_field: raw_name.clone(),
                                    target_field: normalize_pg_identifier(&column.name),
                                    data_type: column.col_type.to_lowercase(),
                                    nullable: !column.not_null,
                                    literal_value: None,
                                })
                                .collect::<Vec<_>>();
                            if !columns.is_empty()
                                || table_has_child_references(&table.name, tables_by_name)
                            {
                                let mut child_mongo_path_segments = mongo_path_segments.to_vec();
                                child_mongo_path_segments.push(raw_name.clone());
                                out.push((
                                    child_table.clone(),
                                    CollectionMapping {
                                        collection_name: raw_name.clone(),
                                        mongo_dbname: db_name.to_owned(),
                                        mongo_path: mapping_mongo_path_for_segments(
                                            root_collection_name,
                                            &child_mongo_path_segments,
                                        ),
                                        traversal: None,
                                        pg_mapping: PgMapping {
                                            dbname: db_name.to_owned(),
                                            schema_name: schema_name.to_owned(),
                                            table_name: table.name.clone(),
                                            columns,
                                            ddl: Some(ddl_table_mapping_from_table(table)),
                                            ddl_editing: default_ddl_editing_guidance(),
                                        },
                                    },
                                ));
                            }
                        }
                    }
                }
            }
        }
    }

    let ddl = schema_to_ddl_with_timestamp_fields(schema, coll_name, None, timestamp_fields);
    let mut tables = parse_sql(&ddl);
    let Some(root_table_name) = tables.first().map(|table| table.name.clone()) else {
        return Vec::new();
    };
    let mapping_schema_name = schema_name.unwrap_or(root_table_name.as_str()).to_owned();
    let mut assigned_table_names = reserved_table_names.clone();
    assigned_table_names.insert(root_table_name.clone());
    let mut table_renames = HashMap::new();
    let mut resolved_child_table_names = HashMap::new();

    if let Some(group) = flatten_grouped_root_array_object_fields(schema) {
        collect_resolved_child_table_names(
            &root_table_name,
            &root_table_name,
            &group.child_fields,
            false,
            reserved_table_names,
            &mut assigned_table_names,
            &mut table_renames,
            &mut resolved_child_table_names,
        );
    } else if let Some((_, array_field)) = flatten_root_array_object_field(schema) {
        if let Some(item_fields) = array_field
            .types
            .get("Array")
            .and_then(|type_schema| type_schema.array.as_ref())
            .and_then(|items_field| items_field.types.get("Object"))
            .and_then(|type_schema| type_schema.object.as_ref())
        {
            collect_resolved_child_table_names(
                &root_table_name,
                &root_table_name,
                item_fields,
                false,
                reserved_table_names,
                &mut assigned_table_names,
                &mut table_renames,
                &mut resolved_child_table_names,
            );
        }
    } else {
        collect_resolved_child_table_names(
            &root_table_name,
            &root_table_name,
            &schema.object,
            true,
            reserved_table_names,
            &mut assigned_table_names,
            &mut table_renames,
            &mut resolved_child_table_names,
        );
    }

    for table in &mut tables {
        if let Some(new_name) = table_renames.get(&table.name) {
            table.name = new_name.clone();
        }
        for foreign_key in &mut table.foreign_keys {
            if let Some(new_name) = table_renames.get(&foreign_key.to_table) {
                foreign_key.to_table = new_name.clone();
            }
        }
    }

    let tables_by_name = tables
        .into_iter()
        .map(|table| (table.name.clone(), table))
        .collect::<HashMap<_, _>>();

    if let Some(group) = flatten_grouped_root_array_object_fields(schema) {
        let Some(root_table) = tables_by_name.get(&root_table_name) else {
            return Vec::new();
        };

        let parent_id_col = flattened_root_parent_id_column(coll_name);
        let root_columns = root_table
            .columns
            .iter()
            .filter_map(|column| {
                let target_field = normalize_pg_identifier(&column.name);
                if target_field == "id" {
                    return None;
                }
                let source_field = if target_field == parent_id_col {
                    Some("_id".to_owned())
                } else if target_field == "key" {
                    Some("key".to_owned())
                } else {
                    group
                        .child_fields
                        .keys()
                        .find(|raw_name| sanitize(raw_name) == target_field)
                        .cloned()
                }?;
                Some(MappingColumn {
                    source_field,
                    target_field,
                    data_type: column.col_type.to_lowercase(),
                    nullable: !column.not_null,
                    literal_value: None,
                })
            })
            .collect::<Vec<_>>();

        let root_file_stem = sanitize(coll_name);
        let mut mappings = vec![(
            root_file_stem.clone(),
            CollectionMapping {
                collection_name: coll_name.to_owned(),
                mongo_dbname: db_name.to_owned(),
                mongo_path: Some(".".to_owned()),
                traversal: None,
                pg_mapping: PgMapping {
                    dbname: db_name.to_owned(),
                    schema_name: mapping_schema_name.clone(),
                    table_name: root_table.name.clone(),
                    columns: root_columns,
                    ddl: Some(ddl_table_mapping_from_table(root_table)),
                    ddl_editing: default_ddl_editing_guidance(),
                },
            },
        )];

        collect_table_mappings(
            db_name,
            coll_name,
            &mapping_schema_name,
            &root_table_name,
            &root_file_stem,
            &Vec::new(),
            &group.child_fields,
            false,
            false,
            &tables_by_name,
            &resolved_child_table_names,
            &mut mappings,
        );
        return mappings;
    }

    if let Some((_, array_field)) = flatten_root_array_object_field(schema) {
        let Some(root_table) = tables_by_name.get(&root_table_name) else {
            return Vec::new();
        };
        let item_fields = array_field
            .types
            .get("Array")
            .and_then(|type_schema| type_schema.array.as_ref())
            .and_then(|items_field| items_field.types.get("Object"))
            .and_then(|type_schema| type_schema.object.as_ref());
        let Some(item_fields) = item_fields else {
            return Vec::new();
        };

        let parent_id_col = flattened_root_parent_id_column(coll_name);
        let root_columns = root_table
            .columns
            .iter()
            .filter_map(|column| {
                let target_field = normalize_pg_identifier(&column.name);
                if target_field == "id" {
                    return None;
                }
                let source_field = if target_field == parent_id_col {
                    Some("_id".to_owned())
                } else {
                    find_source_field_for_column(item_fields, &target_field, false)
                }?;
                Some(MappingColumn {
                    source_field,
                    target_field,
                    data_type: column.col_type.to_lowercase(),
                    nullable: !column.not_null,
                    literal_value: None,
                })
            })
            .collect::<Vec<_>>();

        let root_file_stem = sanitize_pg_name(coll_name);
        let mut mappings = vec![(
            root_file_stem.clone(),
            CollectionMapping {
                collection_name: coll_name.to_owned(),
                mongo_dbname: db_name.to_owned(),
                mongo_path: Some(".".to_owned()),
                traversal: None,
                pg_mapping: PgMapping {
                    dbname: db_name.to_owned(),
                    schema_name: mapping_schema_name.clone(),
                    table_name: root_table.name.clone(),
                    columns: root_columns,
                    ddl: Some(ddl_table_mapping_from_table(root_table)),
                    ddl_editing: default_ddl_editing_guidance(),
                },
            },
        )];

        collect_table_mappings(
            db_name,
            coll_name,
            &mapping_schema_name,
            &root_table_name,
            &root_file_stem,
            &Vec::new(),
            item_fields,
            false,
            false,
            &tables_by_name,
            &resolved_child_table_names,
            &mut mappings,
        );
        return mappings;
    }

    let root_file_stem = sanitize_pg_name(coll_name);
    let mut mappings = Vec::new();
    collect_table_mappings(
        db_name,
        coll_name,
        &mapping_schema_name,
        &root_table_name,
        &root_file_stem,
        &Vec::new(),
        &schema.object,
        true,
        true,
        &tables_by_name,
        &resolved_child_table_names,
        &mut mappings,
    );
    mappings
}

pub fn load_reserved_mapping_table_names(
    base: &Path,
    current_collection_dir: &Path,
) -> Result<std::collections::HashSet<String>> {
    let mut reserved = std::collections::HashSet::new();

    for entry in std::fs::read_dir(base)
        .with_context(|| format!("Failed to read directory {}", base.display()))?
    {
        let path = entry?.path();
        if !path.is_dir() || path == current_collection_dir {
            continue;
        }

        for mapping_entry in std::fs::read_dir(&path)
            .with_context(|| format!("Failed to read directory {}", path.display()))?
        {
            let mapping_path = mapping_entry?.path();
            let Some(file_name) = mapping_path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if !file_name.starts_with("mapping_") || !file_name.ends_with(".yaml") {
                continue;
            }

            let content = std::fs::read_to_string(&mapping_path)
                .with_context(|| format!("Failed to read {}", mapping_path.display()))?;
            let mapping: CollectionMapping = serde_yaml::from_str(&content)
                .with_context(|| format!("Failed to parse {}", mapping_path.display()))?;
            reserved.insert(mapping.pg_mapping.table_name);
        }
    }

    Ok(reserved)
}

pub fn infer_traversal_plan(mapping: &CollectionMapping) -> TraversalPlan {
    let fk = mapping
        .pg_mapping
        .ddl
        .as_ref()
        .and_then(|ddl| ddl.foreign_keys.first());

    let parent_table = fk.map(|foreign_key| foreign_key.to_table.clone());
    let fk_column = fk.map(|foreign_key| normalize_pg_identifier(&foreign_key.from_col));
    let key_column = mapping
        .pg_mapping
        .columns
        .iter()
        .find(|column| column.target_field == "key")
        .map(|column| column.target_field.clone());

    let non_structural_targets = mapping
        .pg_mapping
        .columns
        .iter()
        .filter(|column| column.target_field != "id")
        .filter(|column| {
            fk_column
                .as_ref()
                .is_none_or(|fk_name| column.target_field != *fk_name)
        })
        .filter(|column| column.target_field != "key")
        .map(|column| column.target_field.as_str())
        .collect::<Vec<_>>();

    let source_field_from_path = mapping.mongo_path.as_deref().and_then(|path| {
        if path == "." || path.trim().is_empty() {
            None
        } else {
            path.rsplit('.').next().map(str::to_owned)
        }
    });

    let mode = if mapping.mongo_path.as_deref() == Some(".") {
        TraversalMode::Root
    } else if non_structural_targets.len() == 1 && non_structural_targets[0] == "value" {
        TraversalMode::ArrayScalar
    } else if key_column.is_some() {
        TraversalMode::MapObject
    } else {
        TraversalMode::Object
    };

    TraversalPlan {
        mode,
        parent_table,
        source_field: source_field_from_path,
        fk_column,
        key_column,
    }
}

pub fn mapping_fk_parent_table(mapping: &CollectionMapping) -> Option<String> {
    mapping
        .pg_mapping
        .ddl
        .as_ref()
        .and_then(|ddl| ddl.foreign_keys.first())
        .map(|fk| fk.to_table.clone())
}

pub fn mongo_path_last_segment(path: &str) -> Option<String> {
    let trimmed = path.trim();
    if trimmed.is_empty() || trimmed == "." {
        return None;
    }
    trimmed
        .trim_start_matches('.')
        .split('.')
        .filter(|segment| !segment.is_empty())
        .next_back()
        .map(str::to_owned)
}

pub fn append_mongo_path_segment(parent_path: &str, segment: &str) -> String {
    if parent_path.trim() == "." {
        format!(".{segment}")
    } else {
        format!("{}.{}", parent_path.trim_end_matches('.'), segment)
    }
}

pub fn reconcile_mongo_paths_with_fk_lineage(mappings: &mut [(String, CollectionMapping)]) {
    fn mongo_path_depth(path: &str) -> usize {
        path.trim()
            .trim_start_matches('.')
            .split('.')
            .filter(|segment| !segment.is_empty())
            .count()
    }

    let table_to_index = mappings
        .iter()
        .enumerate()
        .map(|(index, (_, mapping))| (mapping.pg_mapping.table_name.clone(), index))
        .collect::<HashMap<_, _>>();

    // Resolve in passes so deep descendants can follow newly-updated parent paths.
    let mut changed = true;
    while changed {
        changed = false;

        for idx in 0..mappings.len() {
            let Some(current_path) = mappings[idx].1.mongo_path.clone() else {
                continue;
            };
            if current_path.trim() == "." {
                continue;
            }

            let Some(parent_table) = mapping_fk_parent_table(&mappings[idx].1) else {
                continue;
            };
            let Some(parent_index) = table_to_index.get(&parent_table).copied() else {
                continue;
            };
            let Some(parent_path) = mappings[parent_index].1.mongo_path.clone() else {
                continue;
            };

            // Keep wrapper ancestry segments (e.g. .competitions.competitor) when
            // a table is FK-linked directly to the root table.
            if parent_path.trim() == "." && mongo_path_depth(&current_path) > 1 {
                continue;
            }

            let Some(source_tail) = mongo_path_last_segment(&current_path) else {
                continue;
            };

            let reconciled = append_mongo_path_segment(&parent_path, &source_tail);
            if reconciled != current_path {
                mappings[idx].1.mongo_path = Some(reconciled);
                changed = true;
            }
        }
    }
}

pub fn enrich_mappings_with_traversal(mappings: &mut [(String, CollectionMapping)]) {
    for (_, mapping) in mappings.iter_mut() {
        if mapping.traversal.is_none() {
            mapping.traversal = Some(infer_traversal_plan(mapping));
        }
    }
}

/// Write `<dir>/<name>/<name>.json`, `<dir>/<name>/<name>.stats.txt`, `<dir>/<name>/<name>.stats.yaml`, and one `mapping_<table>.yaml` per generated table.
pub fn write_collection_files(
    base: &Path,
    db_name: &str,
    coll_name: &str,
    target_schema: Option<&str>,
    timestamp_fields: &[String],
    known_root_table_names: Option<&HashSet<String>>,
    schema: &CollectionSchema,
    stats_lines: &[String],
    infer_warnings: &[InferWarningYaml],
    read_ops: Option<CollectionReadOpsYaml>,
    has_search_node: bool,
) -> Result<()> {
    // Sanitize collection name for use as a filesystem path component:
    // MongoDB allows '/' in collection names; replace with '_' to avoid
    // path traversal issues when constructing output directories/files.
    let safe_name = coll_name.replace('/', "_");
    let dir = base.join(&safe_name);
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("Failed to create directory {}", dir.display()))?;

    let json_path = dir.join(format!("{safe_name}.json"));
    std::fs::write(&json_path, serde_json::to_string_pretty(schema)?)
        .with_context(|| format!("Failed to write {}", json_path.display()))?;

    let stats_path = dir.join(format!("{safe_name}.stats.txt"));
    std::fs::write(&stats_path, stats_lines.join("\n") + "\n")
        .with_context(|| format!("Failed to write {}", stats_path.display()))?;

    let yaml_stats = stats_to_yaml(
        schema,
        Some(schema.count),
        infer_warnings,
        read_ops,
        has_search_node,
    );
    let yaml_path = dir.join(format!("{safe_name}.stats.yaml"));
    std::fs::write(&yaml_path, serde_yaml::to_string(&yaml_stats)?)
        .with_context(|| format!("Failed to write {}", yaml_path.display()))?;

    let mut reserved_table_names = load_reserved_mapping_table_names(base, &dir)?;
    if let Some(root_table_names) = known_root_table_names {
        let current_root_table_name = sanitize(coll_name);
        reserved_table_names.extend(
            root_table_names
                .iter()
                .filter(|name| name.as_str() != current_root_table_name.as_str())
                .cloned(),
        );
    }

    let mut mappings = build_collection_mappings_with_timestamp_fields(
        db_name,
        coll_name,
        target_schema,
        schema,
        timestamp_fields,
        &reserved_table_names,
    );
    reconcile_mongo_paths_with_fk_lineage(&mut mappings);
    enrich_mappings_with_traversal(&mut mappings);
    let expected_mapping_files = mappings
        .iter()
        .map(|(file_stem, _)| format!("mapping_{}.yaml", file_stem))
        .collect::<std::collections::HashSet<_>>();
    for entry in std::fs::read_dir(&dir)
        .with_context(|| format!("Failed to read directory {}", dir.display()))?
    {
        let path = entry?.path();
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if file_name.starts_with("mapping_")
            && file_name.ends_with(".yaml")
            && !expected_mapping_files.contains(file_name)
        {
            std::fs::remove_file(&path)
                .with_context(|| format!("Failed to remove {}", path.display()))?;
        }
    }

    for (file_stem, mapping) in mappings {
        let mapping_path = dir.join(format!("mapping_{}.yaml", file_stem));
        std::fs::write(&mapping_path, serde_yaml::to_string(&mapping)?)
            .with_context(|| format!("Failed to write {}", mapping_path.display()))?;
    }

    Ok(())
}
