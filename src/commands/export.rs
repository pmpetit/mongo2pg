//! `export` subcommand: exports MongoDB collection data to CSV/SQL-ready
//! artifacts alongside the generated schema tables.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use log::{info, warn};
use strsim::jaro_winkler;

use crate::cli::ExportArgs;
use crate::commands::shared::{
    apply_config_overrides, ensure_output_prefix_segments, resolve_collections_dir,
    resolve_export_chunk_size, split_namespace_scope, stage_export_metadata_from_gcs,
    ConfigOverrides,
};
use crate::export::{
    export_collections_to_sql, resolve_export_write_backend, resolve_grouped_sql_lookup_name,
    ExportWriteBackend,
};
use crate::util::{
    configured_project_root, connection_failed_context, read_conf, should_infer_collection,
};

pub fn sanitize_export_lookup_name(name: &str) -> String {
    let mut s = name.replace(|c: char| !c.is_ascii_alphanumeric() && c != '_', "_");
    if s.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        s = format!("_{s}");
    }
    s.to_lowercase()
}

pub fn resolve_export_sql_lookup_for_collection(
    coll: &str,
    tables_dir: &Path,
    collections_dir: &Path,
    sql_set: &HashSet<String>,
) -> Option<String> {
    let sanitized = sanitize_export_lookup_name(coll);
    let direct_sql = tables_dir.join(format!("{sanitized}.sql"));
    if sql_set.contains(&sanitized) && direct_sql.exists() {
        return Some(sanitized);
    }

    let grouped_sql = resolve_grouped_sql_lookup_name(collections_dir, coll)?;
    let grouped_sql_path = tables_dir.join(format!("{grouped_sql}.sql"));
    if sql_set.contains(&grouped_sql) && grouped_sql_path.exists() {
        Some(grouped_sql)
    } else {
        let shared_name = grouped_sql
            .rsplit_once('_')
            .filter(|(_, suffix)| suffix.chars().all(|ch| ch.is_ascii_digit()))
            .map(|(base, _)| base.to_owned())?;
        let shared_path = tables_dir.join(format!("{shared_name}.sql"));
        sql_set
            .contains(&shared_name)
            .then_some(shared_path)
            .filter(|path| path.exists())
            .map(|_| shared_name)
    }
}

pub fn plan_export_jobs_for_collections(
    collections: impl IntoIterator<Item = String>,
    tables_dir: &Path,
    collections_dir: &Path,
    sql_set: &HashSet<String>,
) -> HashMap<String, Vec<String>> {
    let mut export_jobs = HashMap::<String, Vec<String>>::new();
    for coll in collections {
        if let Some(sql_lookup_name) =
            resolve_export_sql_lookup_for_collection(&coll, tables_dir, collections_dir, sql_set)
        {
            export_jobs.entry(sql_lookup_name).or_default().push(coll);
        }
    }
    export_jobs
}

pub async fn run_export(args: ExportArgs) -> Result<()> {
    let conf = args
        .config
        .as_ref()
        .ok_or_else(|| anyhow!("Provide -c <config>"))?;

    apply_config_overrides(
        conf,
        &ConfigOverrides {
            project_dir: args.project_dir.clone(),
            source_uri: args.mongo.source_uri.clone(),
            namespace: args.namespace.clone(),
            chunk_size: args.chunk_size,
            target_database_name: args.database_name.clone(),
            target_schema_name: args.schema_name.clone(),
            ..ConfigOverrides::default()
        },
    )?;

    let c = read_conf(conf)?;
    let conf_include = c.include.clone();
    let conf_exclude = c.exclude.clone();
    let source_uri = args
        .mongo
        .source_uri
        .clone()
        .or(c.source_uri.clone())
        .ok_or_else(|| {
            anyhow!(
                "No SOURCE_URI provided: pass --source-uri or add SOURCE_URI to the config file"
            )
        })?;

    // Use args.namespace if provided, else fall back to config file.
    // Namespace controls source collections; target database controls SQL tables.
    let namespace = args
        .namespace
        .clone()
        .or(c.namespace.clone())
        .ok_or_else(|| {
            anyhow!("No NAMESPACE provided: pass --namespace or add NAMESPACE to the config file")
        })?;
    let (namespace_db_name, _) = split_namespace_scope(&namespace);
    let tables_db_name = c
        .target_database_name
        .as_deref()
        .unwrap_or(namespace_db_name);
    let export_chunk_size = resolve_export_chunk_size(args.chunk_size.or(c.chunk_size))?;
    let storage_backend = match resolve_export_write_backend(&c.base_dir)? {
        ExportWriteBackend::LocalFs => ExportWriteBackend::LocalFs,
        ExportWriteBackend::Gcs { bucket, prefix } => ExportWriteBackend::Gcs {
            bucket,
            prefix: ensure_output_prefix_segments(
                &prefix,
                c.cluster_name.as_deref(),
                &c.project_dir,
            ),
        },
    };
    match &storage_backend {
        ExportWriteBackend::LocalFs => info!("export backend: local filesystem"),
        ExportWriteBackend::Gcs { bucket, prefix } => {
            info!(
                "export backend: gcs bucket='{}' prefix='{}'",
                bucket, prefix
            );
        }
    }

    let mut export_metadata_stage: Option<tempfile::TempDir> = None;
    let project_root: PathBuf;
    // Use <project_root>/schema/tables/<db_name> for SQL files
    let tables_dir: PathBuf;
    let collections_dir: PathBuf;

    match &storage_backend {
        ExportWriteBackend::LocalFs => {
            project_root = configured_project_root(&c);
            tables_dir = project_root
                .join("schema")
                .join("tables")
                .join(tables_db_name);
            collections_dir = resolve_collections_dir(&project_root, namespace_db_name);
        }
        ExportWriteBackend::Gcs { bucket, prefix } => {
            let Some(stage) = stage_export_metadata_from_gcs(
                bucket,
                prefix,
                c.cluster_name.as_deref(),
                &c.project_dir,
                tables_db_name,
            )
            .await?
            else {
                return Err(anyhow!(
                    "Cannot stage export metadata from gs://{}/{}/schema/tables/{}",
                    bucket,
                    prefix.trim_matches('/'),
                    tables_db_name
                ));
            };

            project_root = stage.path().to_path_buf();
            tables_dir = project_root
                .join("schema")
                .join("tables")
                .join(tables_db_name);
            collections_dir = resolve_collections_dir(&project_root, namespace_db_name);
            info!(
                "export metadata staged from GCS into temporary directory {}",
                project_root.display()
            );
            export_metadata_stage = Some(stage);
        }
    }

    if !tables_dir.is_dir() {
        return Err(anyhow!(
            "Cannot read SQL tables directory {}",
            tables_dir.display()
        ));
    }

    if !collections_dir.is_dir() {
        return Err(anyhow!(
            "Cannot read collections directory {}",
            collections_dir.display()
        ));
    }

    if let Some(stage) = &export_metadata_stage {
        info!(
            "export metadata staging dir (temporary): {}",
            stage.path().display()
        );
    }

    let (data_dir, cleanup_staging_after_export) = match (&storage_backend, args.output_dir.clone())
    {
        (_, Some(dir)) => (dir, false),
        (ExportWriteBackend::LocalFs, None) => (project_root.join("data"), false),
        (ExportWriteBackend::Gcs { .. }, None) => {
            let staging_dir = std::env::temp_dir().join(format!(
                "mongo2pg-gcs-stage-{}-{}",
                std::process::id(),
                Utc::now().timestamp_millis()
            ));
            info!("export staging dir (temporary): {}", staging_dir.display());
            (staging_dir, true)
        }
    };

    let client_options = crate::db::mongo::parse_client_options(&source_uri)
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

    fn warn_missing_sql_schema(
        coll_name: &str,
        sanitized: &str,
        tables_dir: &Path,
        sql_files: &[(String, String)],
    ) {
        let expected_path = tables_dir.join(format!("{sanitized}.sql"));
        let closest_existing = sql_files
            .iter()
            .map(|(stem, sanitized_stem)| (stem, jaro_winkler(sanitized, sanitized_stem)))
            .max_by(|(_, left), (_, right)| left.total_cmp(right))
            .filter(|(_, score)| *score >= 0.88)
            .map(|(stem, _)| tables_dir.join(format!("{stem}.sql")));

        if let Some(closest_path) = closest_existing {
            warn!(
                "SQL schema not found for collection '{coll_name}': expected {}, closest existing file is {}",
                expected_path.display(),
                closest_path.display()
            );
        } else {
            warn!(
                "SQL schema not found for collection '{coll_name}': expected {} – run `to-pg` first",
                expected_path.display()
            );
        }
    }

    // Get all .sql files and their sanitized names
    let mut sql_files: Vec<(String, String)> = std::fs::read_dir(&tables_dir)
        .with_context(|| format!("Cannot read {}", tables_dir.display()))?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("sql"))
        .filter_map(|e| {
            e.path()
                .file_stem()
                .and_then(|s| s.to_str())
                .map(|s| (s.to_owned(), sanitize_export_lookup_name(s)))
        })
        .collect();
    sql_files.sort_by(|a, b| a.1.cmp(&b.1));

    // Build a set of sanitized .sql names for fast lookup
    let sql_set: HashSet<String> = sql_files.iter().map(|(_, s)| s.clone()).collect();

    // sql_lookup_name -> source MongoDB collections
    let mut export_jobs: HashMap<String, Vec<String>> = HashMap::new();

    if let Some(name) = args.collection.clone() {
        let sanitized = sanitize_export_lookup_name(&name);
        if let Some(sql_lookup_name) =
            resolve_export_sql_lookup_for_collection(&name, &tables_dir, &collections_dir, &sql_set)
        {
            export_jobs
                .entry(sql_lookup_name)
                .or_default()
                .push(name.clone());
        } else {
            let sql_path = tables_dir.join(format!("{sanitized}.sql"));
            warn!(
                "SQL schema not found: {} – run `to-pg` first",
                sql_path.display()
            );
        }
    } else {
        // Get all collection names from MongoDB
        let mongo_colls = client
            .database(namespace_db_name)
            .list_collection_names()
            .await
            .with_context(|| {
                format!(
                    "{}: failed to list collections for database {namespace_db_name}",
                    connection_failed_context("mongo", "query")
                )
            })?
            .into_iter()
            .filter(|coll| !coll.starts_with("system."))
            .filter(|coll| should_infer_collection(coll, &conf_include, &conf_exclude));

        let mongo_colls_vec = mongo_colls.collect::<Vec<_>>();
        export_jobs = plan_export_jobs_for_collections(
            mongo_colls_vec.clone(),
            &tables_dir,
            &collections_dir,
            &sql_set,
        );

        for coll in mongo_colls_vec {
            if !export_jobs
                .values()
                .any(|members| members.iter().any(|member| member == &coll))
            {
                let sanitized = sanitize_export_lookup_name(&coll);
                warn_missing_sql_schema(&coll, &sanitized, &tables_dir, &sql_files);
            }
        }
    }

    if export_jobs.is_empty() {
        warn!("No SQL schema files found in {}", tables_dir.display());
        return Ok(());
    }

    let mut jobs: Vec<(String, Vec<String>)> = export_jobs.into_iter().collect();
    jobs.sort_by(|left, right| left.0.cmp(&right.0));
    for (_, collections) in &mut jobs {
        collections.sort();
    }
    let mut failed_jobs: Vec<String> = Vec::new();

    let total_jobs = jobs.len();

    for (index, (sql_lookup_name, coll_names)) in jobs.iter().enumerate() {
        if coll_names.len() == 1 {
            info!(
                "[{}/{}] Exporting {namespace_db_name}.{} via {}.sql",
                index + 1,
                total_jobs,
                coll_names[0],
                sql_lookup_name
            );
        } else {
            info!(
                "[{}/{}] Exporting grouped {} collections into {}.sql ({})",
                index + 1,
                total_jobs,
                coll_names.len(),
                sql_lookup_name,
                coll_names.join(", ")
            );
            for (member_index, coll_name) in coll_names.iter().enumerate() {
                info!(
                    "-> member [{}/{}]: {namespace_db_name}.{} -> {}.sql",
                    member_index + 1,
                    coll_names.len(),
                    coll_name,
                    sql_lookup_name
                );
            }
        }

        match export_collections_to_sql(
            &client,
            namespace_db_name,
            coll_names,
            sql_lookup_name,
            &tables_dir,
            &collections_dir,
            &data_dir,
            export_chunk_size,
            &storage_backend,
        )
        .await
        {
            Ok(()) => {}
            Err(e) => {
                let job_label = format!("{}.{}", namespace_db_name, sql_lookup_name);
                warn!("  warning: export failed for {}: {:#}", job_label, e);
                failed_jobs.push(format!("{}: {}", job_label, e));
            }
        }
    }

    if !failed_jobs.is_empty() {
        return Err(anyhow!(
            "Export failed for {} job(s): {}",
            failed_jobs.len(),
            failed_jobs.join(" | ")
        ));
    }

    if cleanup_staging_after_export && data_dir.exists() {
        if let Err(err) = std::fs::remove_dir_all(&data_dir) {
            warn!(
                "Failed to clean temporary export staging directory {}: {}",
                data_dir.display(),
                err
            );
        } else {
            info!(
                "Cleaned temporary export staging directory {}",
                data_dir.display()
            );
        }
    }

    Ok(())
}
