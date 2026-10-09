//! `export` subcommand: exports MongoDB collection data to CSV/SQL-ready
//! artifacts alongside the generated schema tables.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use futures::StreamExt;
use log::{info, warn};
use strsim::jaro_winkler;

use crate::cli::ExportArgs;
use crate::commands::shared::{
    apply_config_overrides, ensure_output_prefix_segments, multi_db_data_dir,
    multi_db_schema_tables_dir, multi_db_source_collections_dir, resolve_collections_dir,
    resolve_export_chunk_size, split_namespace_scope, stage_export_metadata_from_gcs,
    ConfigOverrides,
};
use crate::export::{
    export_collections_to_sql, resolve_export_write_backend, resolve_grouped_sql_lookup_name,
    ExportWriteBackend,
};
use crate::util::{
    configured_project_root, connection_failed_context, read_conf,
    resolve_target_mapping_for_namespace_index, should_infer_collection_for_database,
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
    _tables_dir: &Path,
    collections_dir: &Path,
    sql_set: &HashSet<String>,
) -> Option<String> {
    let sanitized = sanitize_export_lookup_name(coll);
    if sql_set.contains(&sanitized) {
        return Some(sanitized);
    }

    if let Some(grouped_sql) = resolve_grouped_sql_lookup_name(collections_dir, coll) {
        if sql_set.contains(&grouped_sql) {
            return Some(grouped_sql);
        }

        if let Some(shared_name) = grouped_sql
            .rsplit_once('_')
            .filter(|(_, suffix)| suffix.chars().all(|ch| ch.is_ascii_digit()))
            .map(|(base, _)| base.to_owned())
        {
            if sql_set.contains(&shared_name) {
                return Some(shared_name);
            }
        }
    }

    sanitized
        .split_once('_')
        .map(|(group_prefix, _)| group_prefix)
        .filter(|group_prefix| sql_set.contains(*group_prefix))
        .map(ToOwned::to_owned)
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

fn export_worker_count() -> usize {
    let host_cpus = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1);
    let cgroup_cpu_max = fs::read_to_string("/sys/fs/cgroup/cpu.max").ok();
    let configured_workers = std::env::var("M2PG_EXPORT_WORKERS").ok();

    resolve_export_worker_count(
        configured_workers.as_deref(),
        host_cpus,
        cgroup_cpu_max.as_deref(),
    )
}

fn resolve_export_worker_count(
    configured_workers: Option<&str>,
    host_cpus: usize,
    cgroup_cpu_max: Option<&str>,
) -> usize {
    if let Some(workers) = configured_workers
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|workers| *workers > 0)
    {
        return workers;
    }

    let host_cpus = host_cpus.max(1);
    let cgroup_workers = cgroup_cpu_max.and_then(|content| {
        let mut parts = content.split_whitespace();
        let quota_raw = parts.next()?;
        let period_raw = parts.next()?;
        if quota_raw == "max" {
            return None;
        }
        let quota = quota_raw.parse::<u64>().ok()?;
        let period = period_raw.parse::<u64>().ok()?;
        if period == 0 {
            return None;
        }
        Some((quota / period).max(1) as usize)
    });

    cgroup_workers.unwrap_or(host_cpus).clamp(1, host_cpus)
}

fn effective_export_worker_count(configured_workers: usize, job_count: usize) -> usize {
    configured_workers.max(1).min(job_count)
}

fn order_export_jobs(export_jobs: HashMap<String, Vec<String>>) -> Vec<(String, Vec<String>)> {
    let mut jobs: Vec<(String, Vec<String>)> = export_jobs.into_iter().collect();
    jobs.sort_by(|left, right| left.0.cmp(&right.0));
    for (_, collections) in &mut jobs {
        collections.sort();
    }
    jobs
}

fn export_worker_label(worker_slot: usize, worker_count: usize) -> String {
    format!("[worker {worker_slot}/{worker_count}]")
}

fn should_export_collection(
    db_name: &str,
    coll_name: &str,
    include: &[String],
    exclude: &[String],
) -> bool {
    should_infer_collection_for_database(db_name, coll_name, include, exclude)
}

fn has_collection_json_files(root: &Path) -> bool {
    let mut pending_dirs = vec![root.to_path_buf()];
    while let Some(dir) = pending_dirs.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.filter_map(|entry| entry.ok()) {
            let path = entry.path();
            if path.is_dir() {
                pending_dirs.push(path);
                continue;
            }
            if path
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
            {
                return true;
            }
        }
    }
    false
}

async fn run_multi_db_exports_if_needed(
    args: &ExportArgs,
    conf: &Path,
    initial_conf: &crate::util::ConfData,
) -> Result<bool> {
    if args.namespace.is_some() || initial_conf.namespace_databases.is_empty() {
        return Ok(false);
    }

    let mut failures: Vec<String> = Vec::new();
    let local_project_root = configured_project_root(initial_conf);
    let is_local_backend = matches!(
        resolve_export_write_backend(&initial_conf.base_dir)?,
        ExportWriteBackend::LocalFs
    );

    for (idx, db_name) in initial_conf.namespace_databases.iter().enumerate() {
        if is_local_backend {
            let collections_dir = multi_db_source_collections_dir(&local_project_root, db_name);
            if !collections_dir.is_dir() || !has_collection_json_files(&collections_dir) {
                warn!(
                    "Skipping export for database '{}': no inferred collection JSON files found under {}",
                    db_name,
                    collections_dir.display()
                );
                continue;
            }
        }

        let (mapped_db, mapped_schema) =
            resolve_target_mapping_for_namespace_index(initial_conf, idx, db_name);
        let effective_db = args
            .database_name
            .clone()
            .unwrap_or_else(|| mapped_db.clone());
        let effective_schema = args
            .schema_name
            .clone()
            .unwrap_or_else(|| mapped_schema.clone());

        let stage_dir = tempfile::Builder::new()
            .prefix("mongo2pg-export-conf-")
            .tempdir()
            .context("Failed to create temporary config dir for multi-db export")?;
        let staged_conf = stage_dir.path().join("db.toml");
        std::fs::copy(conf, &staged_conf).with_context(|| {
            format!(
                "Failed to stage config {} to {}",
                conf.display(),
                staged_conf.display()
            )
        })?;

        apply_config_overrides(
            &staged_conf,
            &ConfigOverrides {
                project_dir: args.project_dir.clone(),
                source_uri: args.mongo.source_uri.clone(),
                namespace: Some(db_name.clone()),
                chunk_size: args.chunk_size,
                target_database_name: Some(effective_db),
                target_schema_name: Some(effective_schema),
                ..ConfigOverrides::default()
            },
        )?;

        let mut child_args = args.clone();
        child_args.config = Some(staged_conf.clone());
        child_args.namespace = Some(db_name.clone());
        child_args.database_name = None;
        child_args.schema_name = None;

        if let Err(err) = Box::pin(run_export(child_args)).await {
            failures.push(format!("{}: {:#}", db_name, err));
        }
    }

    if !failures.is_empty() {
        return Err(anyhow!(
            "Export failed for {} configured database(s): {}",
            failures.len(),
            failures.join(" | ")
        ));
    }

    Ok(true)
}

fn resolve_storage_backend(c: &crate::util::ConfData) -> Result<ExportWriteBackend> {
    Ok(match resolve_export_write_backend(&c.base_dir)? {
        ExportWriteBackend::LocalFs => ExportWriteBackend::LocalFs,
        ExportWriteBackend::Gcs { bucket, prefix } => ExportWriteBackend::Gcs {
            bucket,
            prefix: ensure_output_prefix_segments(&prefix, c.cluster_name.as_deref(), &c.project_dir),
        },
    })
}

struct ExportPaths {
    project_root: PathBuf,
    tables_dir: PathBuf,
    collections_dir: PathBuf,
    use_multi_db_layout: bool,
    export_metadata_stage: Option<tempfile::TempDir>,
}

async fn resolve_export_paths(
    c: &crate::util::ConfData,
    storage_backend: &ExportWriteBackend,
    namespace_db_name: &str,
    tables_db_name: &str,
) -> Result<ExportPaths> {
    let is_multi_db = !c.namespace_databases.is_empty();
    let mut use_multi_db_layout = false;

    match storage_backend {
        ExportWriteBackend::LocalFs => {
            let project_root = configured_project_root(c);
            let multi_db_tables_candidate = multi_db_schema_tables_dir(&project_root, namespace_db_name);
            let multi_db_collections_candidate =
                multi_db_source_collections_dir(&project_root, namespace_db_name);

            let (tables_dir, collections_dir) = if multi_db_tables_candidate.is_dir()
                && multi_db_collections_candidate.is_dir()
            {
                use_multi_db_layout = true;
                (multi_db_tables_candidate, multi_db_collections_candidate)
            } else if is_multi_db {
                use_multi_db_layout = true;
                (
                    multi_db_schema_tables_dir(&project_root, namespace_db_name),
                    multi_db_source_collections_dir(&project_root, namespace_db_name),
                )
            } else {
                (
                    project_root.join("schema").join("tables").join(tables_db_name),
                    resolve_collections_dir(&project_root, namespace_db_name),
                )
            };

            Ok(ExportPaths {
                project_root,
                tables_dir,
                collections_dir,
                use_multi_db_layout,
                export_metadata_stage: None,
            })
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

            let project_root = stage.path().to_path_buf();
            let tables_dir = project_root.join("schema").join("tables").join(tables_db_name);
            let collections_dir = resolve_collections_dir(&project_root, namespace_db_name);
            info!(
                "export metadata staged from GCS into temporary directory {}",
                project_root.display()
            );
            Ok(ExportPaths {
                project_root,
                tables_dir,
                collections_dir,
                use_multi_db_layout,
                export_metadata_stage: Some(stage),
            })
        }
    }
}

fn adjust_tables_dir_for_compat(mut tables_dir: PathBuf, tables_db_name: &str) -> PathBuf {
    let has_sql_in_tables_root = fs::read_dir(&tables_dir)
        .ok()
        .into_iter()
        .flat_map(|entries| entries.filter_map(|entry| entry.ok()))
        .any(|entry| {
            entry
                .path()
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("sql"))
        });
    if !has_sql_in_tables_root {
        let nested_tables_dir = tables_dir.join(tables_db_name);
        if nested_tables_dir.is_dir() {
            tables_dir = nested_tables_dir;
        }
    }
    tables_dir
}

fn resolve_data_dir(
    storage_backend: &ExportWriteBackend,
    output_dir: Option<PathBuf>,
    use_multi_db_layout: bool,
    project_root: &Path,
    namespace_db_name: &str,
) -> (PathBuf, bool) {
    match (storage_backend, output_dir) {
        (_, Some(dir)) => (dir, false),
        (ExportWriteBackend::LocalFs, None) if use_multi_db_layout => {
            (multi_db_data_dir(project_root, namespace_db_name), false)
        }
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
    }
}

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

fn load_sql_files(tables_dir: &Path) -> Result<Vec<(String, String)>> {
    let mut sql_files: Vec<(String, String)> = std::fs::read_dir(tables_dir)
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
    Ok(sql_files)
}

async fn build_export_jobs(
    ctx: BuildExportJobsContext<'_>,
) -> Result<HashMap<String, Vec<String>>> {
    let sql_set: HashSet<String> = ctx.sql_files.iter().map(|(_, s)| s.clone()).collect();
    let mut export_jobs: HashMap<String, Vec<String>> = HashMap::new();

    if let Some(name) = ctx.args.collection.clone() {
        if !should_export_collection(
            ctx.namespace_db_name,
            &name,
            ctx.conf_include,
            ctx.conf_exclude,
        ) {
            return Ok(export_jobs);
        }

        let sanitized = sanitize_export_lookup_name(&name);
        if let Some(sql_lookup_name) = resolve_export_sql_lookup_for_collection(
            &name,
            ctx.tables_dir,
            ctx.collections_dir,
            &sql_set,
        ) {
            export_jobs.entry(sql_lookup_name).or_default().push(name);
        } else {
            let sql_path = ctx.tables_dir.join(format!("{sanitized}.sql"));
            warn!("SQL schema not found: {} – run `to-pg` first", sql_path.display());
        }
        return Ok(export_jobs);
    }

    let mongo_colls = ctx
        .client
        .database(ctx.namespace_db_name)
        .list_collection_names()
        .await
        .with_context(|| {
            format!(
                "{}: failed to list collections for database {}",
                connection_failed_context("mongo", "query")
                ,ctx.namespace_db_name
            )
        })?
        .into_iter()
        .filter(|coll| !coll.starts_with("system."))
        .filter(|coll| {
            should_export_collection(
                ctx.namespace_db_name,
                coll,
                ctx.conf_include,
                ctx.conf_exclude,
            )
        });

    let mongo_colls_vec = mongo_colls.collect::<Vec<_>>();
    export_jobs = plan_export_jobs_for_collections(
        mongo_colls_vec.clone(),
        ctx.tables_dir,
        ctx.collections_dir,
        &sql_set,
    );

    for coll in mongo_colls_vec {
        if !export_jobs
            .values()
            .any(|members| members.iter().any(|member| member == &coll))
        {
            let sanitized = sanitize_export_lookup_name(&coll);
            warn_missing_sql_schema(&coll, &sanitized, ctx.tables_dir, ctx.sql_files);
        }
    }

    Ok(export_jobs)
}

struct BuildExportJobsContext<'a> {
    args: &'a ExportArgs,
    client: &'a mongodb::Client,
    namespace_db_name: &'a str,
    tables_dir: &'a Path,
    collections_dir: &'a Path,
    sql_files: &'a [(String, String)],
    conf_include: &'a [String],
    conf_exclude: &'a [String],
}

async fn run_export_jobs(
    client: &mongodb::Client,
    namespace_db_name: &str,
    tables_dir: &Path,
    collections_dir: &Path,
    data_dir: &Path,
    export_chunk_size: u64,
    storage_backend: &ExportWriteBackend,
    export_jobs: HashMap<String, Vec<String>>,
) -> Result<()> {
    let jobs = order_export_jobs(export_jobs);
    let total_jobs = jobs.len();
    if total_jobs == 0 {
        return Ok(());
    }

    let worker_count = effective_export_worker_count(export_worker_count(), total_jobs);
    let next_job = AtomicUsize::new(0);
    let failures_by_worker = futures::stream::iter(0..worker_count)
        .map(|worker_index| {
            let jobs = &jobs;
            let next_job = &next_job;
            async move {
            let worker_slot = worker_index + 1;
            let worker_label = export_worker_label(worker_slot, worker_count);
            let mut failed_jobs = Vec::new();

            loop {
                let job_index = next_job.fetch_add(1, Ordering::Relaxed);
                let Some((sql_lookup_name, coll_names)) = jobs.get(job_index) else {
                    break;
                };

                if coll_names.len() == 1 {
                    info!(
                        "{} [{}/{}] Exporting {namespace_db_name}.{} via {}.sql",
                        worker_label,
                        job_index + 1,
                        total_jobs,
                        coll_names[0],
                        sql_lookup_name
                    );
                } else {
                    info!(
                        "{} [{}/{}] Exporting grouped {} collections into {}.sql ({})",
                        worker_label,
                        job_index + 1,
                        total_jobs,
                        coll_names.len(),
                        sql_lookup_name,
                        coll_names.join(", ")
                    );
                    for (member_index, coll_name) in coll_names.iter().enumerate() {
                        info!(
                            "{} -> member [{}/{}]: {namespace_db_name}.{} -> {}.sql",
                            worker_label,
                            member_index + 1,
                            coll_names.len(),
                            coll_name,
                            sql_lookup_name
                        );
                    }
                }

                let job_label = format!("{}.{}", namespace_db_name, sql_lookup_name);
                let result = export_collections_to_sql(
                    client,
                    namespace_db_name,
                    coll_names,
                    sql_lookup_name,
                    tables_dir,
                    collections_dir,
                    data_dir,
                    export_chunk_size,
                    storage_backend,
                )
                .await;

                match result {
                    Ok(()) => info!("{} completed export {} status=success", worker_label, job_label),
                    Err(err) => {
                        warn!("{} export failed for {}: {:#}", worker_label, job_label, err);
                        info!("{} completed export {} status=failed", worker_label, job_label);
                        failed_jobs.push(format!("{}: {}", job_label, err));
                    }
                }
            }

                failed_jobs
            }
        })
        .buffer_unordered(worker_count)
        .collect::<Vec<_>>()
        .await;

    let mut failed_jobs = failures_by_worker.into_iter().flatten().collect::<Vec<_>>();
    failed_jobs.sort();

    if !failed_jobs.is_empty() {
        return Err(anyhow!(
            "Export failed for {} job(s): {}",
            failed_jobs.len(),
            failed_jobs.join(" | ")
        ));
    }

    Ok(())
}

fn cleanup_staging_dir_if_needed(cleanup_staging_after_export: bool, data_dir: &Path) {
    if !cleanup_staging_after_export || !data_dir.exists() {
        return;
    }
    if let Err(err) = std::fs::remove_dir_all(data_dir) {
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

pub async fn run_export(args: ExportArgs) -> Result<()> {
    run_export_impl(args).await
}

async fn run_export_impl(args: ExportArgs) -> Result<()> {
    let conf = args
        .config
        .as_ref()
        .ok_or_else(|| anyhow!("Provide -c <config>"))?;

    let initial_conf = read_conf(conf)?;
    if run_multi_db_exports_if_needed(&args, conf, &initial_conf).await? {
        return Ok(());
    }

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
    let storage_backend = resolve_storage_backend(&c)?;
    match &storage_backend {
        ExportWriteBackend::LocalFs => info!("export backend: local filesystem"),
        ExportWriteBackend::Gcs { bucket, prefix } => {
            info!(
                "export backend: gcs bucket='{}' prefix='{}'",
                bucket, prefix
            );
        }
    }

    let ExportPaths {
        project_root,
        tables_dir,
        collections_dir,
        use_multi_db_layout,
        export_metadata_stage,
    } = resolve_export_paths(&c, &storage_backend, namespace_db_name, tables_db_name).await?;
    let tables_dir = adjust_tables_dir_for_compat(tables_dir, tables_db_name);

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

    let (data_dir, cleanup_staging_after_export) = resolve_data_dir(
        &storage_backend,
        args.output_dir.clone(),
        use_multi_db_layout,
        &project_root,
        namespace_db_name,
    );

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

    let sql_files = load_sql_files(&tables_dir)?;
    let export_jobs = build_export_jobs(BuildExportJobsContext {
        args: &args,
        client: &client,
        namespace_db_name,
        tables_dir: &tables_dir,
        collections_dir: &collections_dir,
        sql_files: &sql_files,
        conf_include: &conf_include,
        conf_exclude: &conf_exclude,
    })
    .await?;

    if export_jobs.is_empty() {
        warn!("No SQL schema files found in {}", tables_dir.display());
        return Ok(());
    }

    run_export_jobs(
        &client,
        namespace_db_name,
        &tables_dir,
        &collections_dir,
        &data_dir,
        export_chunk_size,
        &storage_backend,
        export_jobs,
    )
    .await?;

    cleanup_staging_dir_if_needed(cleanup_staging_after_export, &data_dir);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        effective_export_worker_count, export_worker_label, order_export_jobs,
        resolve_export_sql_lookup_for_collection, resolve_export_worker_count,
        should_export_collection,
    };
    use std::collections::{HashMap, HashSet};
    use std::fs;

    #[test]
    fn effective_export_worker_count_is_bounded_by_job_count() {
        assert_eq!(effective_export_worker_count(4, 2), 2);
        assert_eq!(effective_export_worker_count(1, 4), 1);
        assert_eq!(effective_export_worker_count(4, 0), 0);
    }

    #[test]
    fn export_worker_label_matches_infer_format() {
        assert_eq!(export_worker_label(2, 4), "[worker 2/4]");
    }

    #[test]
    fn export_jobs_and_grouped_collections_are_sorted() {
        let jobs = HashMap::from([
            ("zebra".to_owned(), vec!["zebra".to_owned()]),
            (
                "events".to_owned(),
                vec!["events_z".to_owned(), "events_a".to_owned()],
            ),
        ]);

        assert_eq!(
            order_export_jobs(jobs),
            vec![
                (
                    "events".to_owned(),
                    vec!["events_a".to_owned(), "events_z".to_owned()]
                ),
                ("zebra".to_owned(), vec!["zebra".to_owned()]),
            ]
        );
    }

    #[test]
    fn export_worker_count_uses_override_and_cgroup_default() {
        assert_eq!(resolve_export_worker_count(Some("3"), 8, Some("2 1")), 3);
        assert_eq!(resolve_export_worker_count(Some("0"), 8, Some("250000 100000")), 2);
        assert_eq!(resolve_export_worker_count(None, 8, Some("max 100000")), 8);
    }

    #[test]
    fn resolve_export_sql_lookup_falls_back_to_group_prefix() {
        let stage = tempfile::tempdir().expect("tempdir should be created");
        let tables_dir = stage.path().join("schema/tables/ciam_qualif");
        let collections_dir = stage.path().join("source/collections");
        fs::create_dir_all(&tables_dir).expect("tables dir should be created");
        fs::create_dir_all(&collections_dir).expect("collections dir should be created");
        fs::write(tables_dir.join("events.sql"), "-- ddl").expect("sql file should be created");

        let sql_set = HashSet::from(["events".to_owned()]);

        let resolved = resolve_export_sql_lookup_for_collection(
            "events_lmza",
            &tables_dir,
            &collections_dir,
            &sql_set,
        );

        assert_eq!(resolved.as_deref(), Some("events"));
    }

    #[test]
    fn resolve_export_sql_lookup_matches_direct_sql_via_sanitized_set() {
        let stage = tempfile::tempdir().expect("tempdir should be created");
        let tables_dir = stage.path().join("schema/tables/db");
        let collections_dir = stage.path().join("source/collections");
        fs::create_dir_all(&tables_dir).expect("tables dir should be created");
        fs::create_dir_all(&collections_dir).expect("collections dir should be created");
        fs::write(
            tables_dir.join("camelCaseCollection.sql"),
            "-- ddl",
        )
        .expect("sql file should be created");

        let sql_set = HashSet::from(["camelcasecollection".to_owned()]);

        let resolved = resolve_export_sql_lookup_for_collection(
            "camelCaseCollection",
            &tables_dir,
            &collections_dir,
            &sql_set,
        );

        assert_eq!(resolved.as_deref(), Some("camelcasecollection"));
    }

    #[test]
    fn export_collection_filter_matches_database_qualified_include() {
        let include = vec!["sample_analytics.events".to_owned()];

        assert!(should_export_collection(
            "sample_analytics",
            "events",
            &include,
            &[]
        ));
        assert!(!should_export_collection(
            "sample_airbnb",
            "events",
            &include,
            &[]
        ));
    }

    #[test]
    fn export_collection_filter_gives_exclude_precedence() {
        let include = vec!["sample_analytics.*".to_owned()];
        let exclude = vec!["sample_analytics.events".to_owned()];

        assert!(!should_export_collection(
            "sample_analytics",
            "events",
            &include,
            &exclude
        ));
        assert!(should_export_collection(
            "sample_analytics",
            "users",
            &include,
            &exclude
        ));
    }
}
