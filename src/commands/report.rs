//! `report` subcommand: renders HTML schema/inference reports from the
//! `source/collections` metadata tree, plus optional `--post-import` mode.

use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use log::{info, warn};

use crate::cli::{PingArgs, ReportArgs};
use crate::commands::infer::move_infer_artifacts_to_gcs_if_needed_with_project_root;
use crate::commands::ping::run_ping;
use crate::commands::shared::{
    apply_config_overrides, download_gcs_prefix_to_local_dir,
    gcs_prefix_candidates_for_project_subdir, resolve_local_project_root_from_config,
    split_namespace_scope, stage_config_path_if_gcs, stage_report_metadata_from_gcs,
    stage_source_collections_from_gcs, write_post_import_report, ConfigOverrides,
};
use crate::export::{resolve_export_write_backend, ExportWriteBackend};
use crate::report::{collect_rows, render_html};
use crate::schema_diagram::{load_tables_by_db, render_schema_html};
use crate::util::{read_conf, resolve_target_mapping_for_namespace_index};

type DbRows = Vec<(String, Vec<crate::report::CollectionRow>)>;

struct ReportRuntimeContext {
    collections_dir: PathBuf,
    namespace: String,
    cluster: String,
    reports_dir: Option<PathBuf>,
    project_name: Option<String>,
    title: Option<String>,
    target_database_name: Option<String>,
}

fn dir_has_sql_files(root: &Path) -> bool {
    let mut pending_dirs = vec![root.to_path_buf()];
    while let Some(dir) = pending_dirs.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
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
                .is_some_and(|ext| ext.eq_ignore_ascii_case("sql"))
            {
                return true;
            }
        }
    }
    false
}

async fn stage_config_if_needed(args: &mut ReportArgs) -> Result<Option<tempfile::TempDir>> {
    if let Some(config) = args.config.take() {
        let (local, stage) = stage_config_path_if_gcs(config).await?;
        args.config = Some(local);
        Ok(stage)
    } else {
        Ok(None)
    }
}

fn require_post_import_namespace(namespace: &str) -> Result<()> {
    if namespace.is_empty() {
        return Err(anyhow!("--post-import requires --namespace"));
    }
    Ok(())
}

fn apply_config_overrides_if_needed(
    args: &mut ReportArgs,
) -> Result<Option<tempfile::NamedTempFile>> {
    let Some(conf) = args.config.as_deref() else {
        return Ok(None);
    };

    let mut staged_config = None;
    let mut target_database_name = None;
    let mut target_schema_name = None;

    if !args.namespace.is_empty() {
        let config = read_conf(conf)?;
        if config.namespace_databases.len() > 1 {
            let requested_database = split_namespace_scope(&args.namespace).0;
            let namespace_index = config
                .namespace_databases
                .iter()
                .position(|database| database == requested_database)
                .ok_or_else(|| {
                    anyhow!(
                        "Namespace '{}' is not present in the configured source namespace list",
                        args.namespace
                    )
                })?;
            let (database, schema) = resolve_target_mapping_for_namespace_index(
                &config,
                namespace_index,
                requested_database,
            );

            let staged = tempfile::Builder::new()
                .prefix("mongo2pg-report-config-")
                .suffix(".toml")
                .tempfile_in(conf.parent().unwrap_or_else(|| Path::new(".")))
                .context("Failed to create temporary report config")?;
            std::fs::copy(conf, staged.path()).with_context(|| {
                format!(
                    "Failed to stage report config {} to {}",
                    conf.display(),
                    staged.path().display()
                )
            })?;

            target_database_name = Some(database);
            target_schema_name = Some(schema);
            staged_config = Some(staged);
        }
    }

    let config_path = staged_config
        .as_ref()
        .map(|staged| staged.path())
        .unwrap_or(conf);
    apply_config_overrides(
        config_path,
        &ConfigOverrides {
            project_dir: args.project_dir.clone(),
            source_uri: args.mongo.source_uri.clone(),
            namespace: (!args.namespace.is_empty()).then(|| args.namespace.clone()),
            target_database_name,
            target_schema_name,
            ..ConfigOverrides::default()
        },
    )?;

    if let Some(staged) = &staged_config {
        args.config = Some(staged.path().to_path_buf());
    }

    Ok(staged_config)
}

async fn run_post_import_if_requested(args: &ReportArgs) -> Result<bool> {
    if !args.post_import {
        return Ok(false);
    }

    require_post_import_namespace(&args.namespace)?;

    let conf = args
        .config
        .as_ref()
        .ok_or_else(|| anyhow!("--post-import requires -c <config>"))?;

    // Post-import validation requires both source and target connectivity.
    // Keep this preflight ping scoped to --post-import so standard report
    // rendering remains file-based and does not require target DB access.
    run_ping(PingArgs {
        config: conf.clone(),
        source: true,
        target: true,
        kafka: false,
        runner: false,
    })
    .await?;

    write_post_import_report(
        conf,
        &args.namespace,
        args.mongo.source_uri.as_deref().unwrap_or(""),
        args.check_md5,
    )
    .await?;
    Ok(true)
}

async fn resolve_report_runtime_context(
    args: &ReportArgs,
) -> Result<(ReportRuntimeContext, Option<tempfile::TempDir>)> {
    if let Some(conf) = args.config.as_deref() {
        let c = read_conf(conf)?;
        let mut staged_metadata = None;
        let namespace = if args.namespace.is_empty() {
            c.namespace.clone().unwrap_or_else(|| c.project_dir.clone())
        } else {
            args.namespace.clone()
        };
        let namespace_db = split_namespace_scope(&namespace).0.to_owned();
        let target_db = c
            .target_database_name
            .clone()
            .unwrap_or_else(|| namespace_db.clone());

        let local_project_root = match resolve_export_write_backend(&c.base_dir)? {
            ExportWriteBackend::LocalFs => resolve_local_project_root_from_config(conf, &c),
            ExportWriteBackend::Gcs { bucket, prefix } => {
                let mut metadata_db_candidates = vec![target_db.clone()];
                if target_db != namespace_db {
                    metadata_db_candidates.push(namespace_db.clone());
                }

                let mut chosen_stage = None;
                let mut chosen_root = None;
                for metadata_db in metadata_db_candidates {
                    let stage = stage_report_metadata_from_gcs(
                        &bucket,
                        &prefix,
                        c.cluster_name.as_deref(),
                        &c.project_dir,
                        &metadata_db,
                    )
                    .await?;
                    let root = stage.path().to_path_buf();
                    let tables_dir = root.join("schema").join("tables").join(&metadata_db);
                    if dir_has_sql_files(&tables_dir) {
                        chosen_root = Some(root);
                        chosen_stage = Some(stage);
                        break;
                    }
                    if chosen_root.is_none() {
                        chosen_root = Some(root);
                        chosen_stage = Some(stage);
                    }
                }

                let root = chosen_root.unwrap_or_else(|| {
                    resolve_local_project_root_from_config(conf, &c)
                });
                staged_metadata = chosen_stage;
                root
            }
        };

        let context = ReportRuntimeContext {
            collections_dir: local_project_root.join("source").join("collections"),
            namespace,
            cluster: c
                .source_uri
                .as_deref()
                .map(crate::report::cluster_from_uri)
                .unwrap_or_default(),
            reports_dir: Some(local_project_root.join("reports")),
            project_name: Some(c.project_dir.clone()),
            title: Some(c.title.clone()),
            target_database_name: c.target_database_name.clone(),
        };
        Ok((context, staged_metadata))
    } else {
        let dir = args
            .collections_dir
            .clone()
            .ok_or_else(|| anyhow!("Provide --collections-dir or -c <config>"))?;
        let context = ReportRuntimeContext {
            collections_dir: dir,
            namespace: args.namespace.clone(),
            cluster: String::new(),
            reports_dir: None,
            project_name: None,
            title: None,
            target_database_name: None,
        };
        Ok((context, None))
    }
}

fn detect_multi_db_layout(collections_dir: &Path) -> bool {
    std::fs::read_dir(collections_dir)
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
        .unwrap_or(false)
}

fn resolve_output_path(
    output: Option<&PathBuf>,
    reports_dir: Option<&PathBuf>,
    project_name: Option<&String>,
) -> Result<PathBuf> {
    if let Some(o) = output {
        return Ok(o.clone());
    }
    if let (Some(dir), Some(_proj)) = (reports_dir, project_name) {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("Can't create reports dir {}", dir.display()))?;
        return Ok(dir.join("main.html"));
    }
    Ok(PathBuf::from("main.html"))
}

fn collect_rows_per_db(
    collections_dir: &Path,
    reports_dir: Option<&PathBuf>,
) -> Result<DbRows> {
    let mut db_names: Vec<String> = std::fs::read_dir(collections_dir)
        .with_context(|| format!("Cannot read {}", collections_dir.display()))?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    db_names.sort();

    let tables_root: Option<PathBuf> = reports_dir
        .map(|r| r.parent().unwrap_or(r).join("schema").join("tables"));

    let db_rows = db_names
        .iter()
        .map(|db_name| {
            let db_dir = collections_dir.join(db_name);
            let tables_dir_opt: Option<PathBuf> = tables_root
                .as_deref()
                .map(|t| t.join(db_name))
                .filter(|p| p.is_dir());
            let rows = match collect_rows(&db_dir, tables_dir_opt.as_deref()) {
                Ok(rows) => rows,
                Err(err) => {
                    warn!(
                        "Failed to collect report rows for database '{}' from {}: {err:#}",
                        db_name,
                        db_dir.display()
                    );
                    Vec::new()
                }
            };
            (db_name.clone(), rows)
        })
        .collect::<Vec<_>>();

    Ok(db_rows)
}

fn append_warnings_from_db_rows(db_rows: &DbRows, warnings: &mut Vec<String>) {
    for (db_name, rows) in db_rows {
        for row in rows.iter().filter(|row| row.has_infer_warnings()) {
            warnings.push(format!("{db_name}.{}", row.name));
        }
    }
}

fn write_multi_db_report(
    db_rows: &DbRows,
    output_path: &Path,
    cluster: &str,
    project_name: Option<&str>,
    title: Option<&str>,
    quiet: bool,
) -> Result<()> {
    let entries: Vec<(&str, &[crate::report::CollectionRow])> = db_rows
        .iter()
        .map(|(name, rows)| (name.as_str(), rows.as_slice()))
        .collect();
    let proj = project_name.unwrap_or("project");
    let title = title.unwrap_or("mongo2pg Report");
    let html = crate::report::render_multi_db_html(&entries, cluster, proj, title);
    crate::commands::shared::write_file_atomically(output_path, &html)
        .with_context(|| format!("Failed to write {}", output_path.display()))?;
    if !quiet {
        info!("Report written to {}", output_path.display());
    }
    Ok(())
}

fn resolve_multi_db_collections_dir(project_root: &Path, db_name: &str) -> PathBuf {
    let canonical = crate::commands::shared::multi_db_source_collections_dir(project_root, db_name);
    if canonical.is_dir() {
        return canonical;
    }

    crate::commands::shared::multi_db_database_root(project_root, db_name)
        .join("source")
        .join("collections")
}

fn resolve_multi_db_tables_dir(project_root: &Path, db_name: &str) -> PathBuf {
    let canonical = crate::commands::shared::multi_db_schema_tables_dir(project_root, db_name);
    if canonical.is_dir() {
        return canonical;
    }

    let legacy_root = crate::commands::shared::multi_db_database_root(project_root, db_name)
        .join("schema")
        .join("tables");
    let legacy_db_scoped = legacy_root.join(db_name);
    if legacy_db_scoped.is_dir() {
        return legacy_db_scoped;
    }

    legacy_root
}

fn tables_dir_for_single_db_report(
    reports_dir: Option<&PathBuf>,
    namespace: &str,
    target_database_name: Option<&str>,
) -> Option<PathBuf> {
    reports_dir.map(|r| {
        let tables_root = r.parent().unwrap_or(r).join("schema").join("tables");
        let namespace_db = split_namespace_scope(namespace).0;

        let mut candidates: Vec<&str> = Vec::new();
        if let Some(target_db) = target_database_name
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            candidates.push(target_db);
        }
        if !candidates.contains(&namespace_db) {
            candidates.push(namespace_db);
        }

        for db_name in candidates {
            let db_tables = tables_root.join(db_name);
            if db_tables.is_dir() && dir_has_sql_files(&db_tables) {
                return db_tables;
            }
        }

        if tables_root.is_dir() && dir_has_sql_files(&tables_root) {
            return tables_root;
        }

        if let Ok(entries) = std::fs::read_dir(&tables_root) {
            for entry in entries.filter_map(|entry| entry.ok()) {
                let path = entry.path();
                if path.is_dir() && dir_has_sql_files(&path) {
                    return path;
                }
            }
        }

        tables_root
    })
}

fn render_and_write_single_db_report(
    context: &ReportRuntimeContext,
    output_path: &Path,
    warnings: &mut Vec<String>,
    quiet: bool,
) -> Result<()> {
    let tables_dir_for_report = tables_dir_for_single_db_report(
        context.reports_dir.as_ref(),
        &context.namespace,
        context.target_database_name.as_deref(),
    );
    let tables_dir_opt = tables_dir_for_report.as_deref().filter(|p| p.is_dir());
    let rows = collect_rows(&context.collections_dir, tables_dir_opt)?;

    warnings.extend(
        rows.iter()
            .filter(|row| row.has_infer_warnings())
            .map(|row| row.name.clone()),
    );
    let title = context.title.as_deref().unwrap_or("mongo2pg Report");
    let html = render_html(&rows, &context.namespace, &context.cluster, title);
    std::fs::write(output_path, &html)
        .with_context(|| format!("Failed to write {}", output_path.display()))?;
    if !quiet {
        info!("Report written to {}", output_path.display());
    }
    Ok(())
}

/// Renders one combined `reports/main.html` (with one tab per database) and
/// one combined schema diagram report, for configurations where
/// `[source].namespace` resolves to more than one database. Reads each
/// database's isolated `<database_name>/source` and `<database_name>/schema`
/// directories under the project root; `config/` and `reports/` stay shared.
async fn run_report_for_configured_databases(
    conf: &Path,
    c: &crate::util::ConfData,
    output_override: Option<&PathBuf>,
    quiet: bool,
) -> Result<()> {
    let output_project_root = resolve_local_project_root_from_config(conf, c);
    let mut read_project_root = output_project_root.clone();
    let mut report_project_root = output_project_root.clone();
    let mut staged_project_root: Option<tempfile::TempDir> = None;

    if let ExportWriteBackend::Gcs { bucket, prefix } = resolve_export_write_backend(&c.base_dir)? {
        // When infer artifacts are uploaded to GCS and local copies are purged,
        // report must stage source/schema metadata locally before reading rows.
        let stage = stage_source_collections_from_gcs(
            &bucket,
            &prefix,
            c.cluster_name.as_deref(),
            &c.project_dir,
        )
        .await?;

        for (idx, db_name) in c.namespace_databases.iter().enumerate() {
            let (target_db_name, _) =
                resolve_target_mapping_for_namespace_index(c, idx, db_name);
            let tables_dir =
                crate::commands::shared::multi_db_schema_tables_dir(stage.path(), &target_db_name);
            std::fs::create_dir_all(&tables_dir).with_context(|| {
                format!(
                    "Cannot create staged schema directory {}",
                    tables_dir.display()
                )
            })?;

            let mut staged_tables = false;
            let mut table_db_candidates = vec![target_db_name.as_str()];
            if db_name != &target_db_name {
                table_db_candidates.push(db_name);
            }
            for table_db_name in table_db_candidates {
                let table_db_name = crate::commands::shared::sanitize_name(table_db_name);
                for candidate in gcs_prefix_candidates_for_project_subdir(
                    &prefix,
                    c.cluster_name.as_deref(),
                    &c.project_dir,
                    &format!("schema/tables/{table_db_name}"),
                ) {
                    let downloaded =
                        download_gcs_prefix_to_local_dir(&bucket, &candidate, &tables_dir).await?;
                    if downloaded > 0 {
                        info!(
                            "staged {} schema files from gs://{}/{} into {}",
                            downloaded,
                            bucket,
                            candidate,
                            tables_dir.display()
                        );
                        staged_tables = true;
                        break;
                    }
                }
                if staged_tables {
                    break;
                }
            }

            if !staged_tables {
                warn!(
                    "No staged schema tables found in GCS for database '{}' under base prefix '{}'",
                    db_name,
                    prefix.trim_matches('/'),
                );
            }
        }

        read_project_root = stage.path().to_path_buf();
        report_project_root = stage.path().to_path_buf();
        staged_project_root = Some(stage);
    }

    let reports_dir = report_project_root.join("reports");
    std::fs::create_dir_all(&reports_dir)
        .with_context(|| format!("Can't create reports dir {}", reports_dir.display()))?;
    let output_path = output_override
        .cloned()
        .unwrap_or_else(|| reports_dir.join("main.html"));

    let mut db_rows: DbRows = Vec::new();
    for (idx, db_name) in c.namespace_databases.iter().enumerate() {
        let collections_dir = resolve_multi_db_collections_dir(&read_project_root, db_name);
        if !collections_dir.is_dir() {
            warn!(
                "Skipping report for database '{db_name}': no source collections found at {}",
                collections_dir.display()
            );
            continue;
        }
        let (target_db_name, _) =
            resolve_target_mapping_for_namespace_index(c, idx, db_name);
        let tables_dir = resolve_multi_db_tables_dir(&read_project_root, &target_db_name);
        let tables_dir_opt = tables_dir.is_dir().then_some(tables_dir.as_path());
        let rows = match collect_rows(&collections_dir, tables_dir_opt) {
            Ok(rows) => rows,
            Err(err) => {
                warn!(
                    "Failed to collect report rows for database '{}' from {}: {err:#}",
                    db_name,
                    collections_dir.display()
                );
                Vec::new()
            }
        };
        db_rows.push((db_name.clone(), rows));
    }

    let mut warning_collections: Vec<String> = Vec::new();
    append_warnings_from_db_rows(&db_rows, &mut warning_collections);

    let cluster = c
        .source_uri
        .as_deref()
        .map(crate::report::cluster_from_uri)
        .unwrap_or_default();
    write_multi_db_report(
        &db_rows,
        &output_path,
        &cluster,
        Some(&c.project_dir),
        Some(&c.title),
        quiet,
    )?;

    let target_databases = c
        .namespace_databases
        .iter()
        .enumerate()
        .map(|(idx, db_name)| {
            resolve_target_mapping_for_namespace_index(c, idx, db_name).0
        })
        .collect::<Vec<_>>();
    generate_configured_schema_diagrams(
        &read_project_root,
        &target_databases,
        &reports_dir,
        quiet,
    )?;

    if staged_project_root.is_some() {
        move_infer_artifacts_to_gcs_if_needed_with_project_root(
            conf,
            Some(report_project_root.as_path()),
        )
        .await?;
    }

    fail_if_warning_collections(quiet, &warning_collections)?;

    // Keep staged tempdir alive until report generation finishes.
    let _keep_stage_alive = staged_project_root;
    Ok(())
}

fn generate_configured_schema_diagrams(
    project_root: &Path,
    databases: &[String],
    reports_dir: &Path,
    quiet: bool,
) -> Result<()> {
    let mut entries: Vec<(String, Vec<crate::schema_diagram::Table>)> = Vec::new();
    for db_name in databases {
        let tables_dir = resolve_multi_db_tables_dir(project_root, db_name);
        if !tables_dir.is_dir() {
            continue;
        }
        let tables = crate::schema_diagram::load_tables(&tables_dir).unwrap_or_default();
        if !tables.is_empty() {
            entries.push((db_name.clone(), tables));
        }
    }

    if entries.is_empty() {
        return Ok(());
    }

    let entries_ref: Vec<(&str, &[crate::schema_diagram::Table])> = entries
        .iter()
        .map(|(name, tables)| (name.as_str(), tables.as_slice()))
        .collect();
    let html = crate::schema_diagram::render_schema_html_multi_db(&entries_ref);
    let schema_path = reports_dir.join("schema.html");
    crate::commands::shared::write_file_atomically(&schema_path, &html)
        .with_context(|| format!("Failed to write {}", schema_path.display()))?;
    if !quiet {
        info!("Schema diagram written to {}", schema_path.display());
    }
    Ok(())
}

fn generate_schema_diagrams_if_available(
    reports_dir: Option<&PathBuf>,
    project_name: Option<&String>,
    quiet: bool,
) -> Result<()> {
    let (Some(rep_dir), Some(proj)) = (reports_dir, project_name) else {
        return Ok(());
    };

    let Some(tables_dir) = schema_tables_dir(rep_dir) else {
        return Ok(());
    };

    match load_tables_by_db(&tables_dir) {
        Ok(db_tables) => write_schema_diagrams(rep_dir, proj, &db_tables, quiet)?,
        Err(e) => warn!("could not generate schema diagram: {e}"),
    }

    Ok(())
}

fn schema_tables_dir(reports_dir: &Path) -> Option<PathBuf> {
    let tables_dir = reports_dir
        .parent()
        .unwrap_or(reports_dir)
        .join("schema")
        .join("tables");
    tables_dir.is_dir().then_some(tables_dir)
}

fn write_schema_diagrams(
    reports_dir: &Path,
    project_name: &str,
    db_tables: &[(String, Vec<crate::schema_diagram::Table>)],
    quiet: bool,
) -> Result<()> {
    for (db_name, tables) in db_tables {
        write_schema_diagram(reports_dir, project_name, db_name, tables, quiet)?;
    }
    Ok(())
}

fn write_schema_diagram(
    reports_dir: &Path,
    project_name: &str,
    db_name: &str,
    tables: &[crate::schema_diagram::Table],
    quiet: bool,
) -> Result<()> {
    if tables.is_empty() {
        return Ok(());
    }

    let (label, filename) = schema_diagram_identity(project_name, db_name);
    let schema_path = reports_dir.join(filename);
    let schema_html = render_schema_html(tables, label);
    std::fs::write(&schema_path, schema_html)
        .with_context(|| format!("Failed to write {}", schema_path.display()))?;
    if !quiet {
        info!("Schema diagram written to {}", schema_path.display());
    }
    Ok(())
}

fn schema_diagram_identity<'a>(project_name: &'a str, db_name: &'a str) -> (&'a str, String) {
    if db_name.is_empty() {
        (project_name, format!("{project_name}.schema.html"))
    } else {
        (db_name, format!("{db_name}.schema.html"))
    }
}

async fn sync_infer_artifacts_if_metadata_staged(
    config: Option<&PathBuf>,
    staged_metadata: Option<&tempfile::TempDir>,
) -> Result<()> {
    if let (Some(conf), Some(stage)) = (config, staged_metadata) {
        move_infer_artifacts_to_gcs_if_needed_with_project_root(conf, Some(stage.path())).await?;
    }
    Ok(())
}

fn fail_if_warning_collections(quiet: bool, warning_collections: &[String]) -> Result<()> {
    if !should_fail_report_on_warnings(quiet, warning_collections.len()) {
        return Ok(());
    }

    let preview = warning_collections
        .iter()
        .take(10)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    let suffix = if warning_collections.len() > 10 {
        format!(", ... (+{} more)", warning_collections.len() - 10)
    } else {
        String::new()
    };
    Err(anyhow!(
        "report contains infer warnings in {} collection(s): {}{}",
        warning_collections.len(),
        preview,
        suffix
    ))
}

pub async fn run_report(mut args: ReportArgs, quiet: bool) -> Result<()> {
    let _staged_config = stage_config_if_needed(&mut args).await?;
    let _overridden_config = apply_config_overrides_if_needed(&mut args)?;
    if run_post_import_if_requested(&args).await? {
        return Ok(());
    }

    if let Some(conf) = args.config.as_deref() {
        if args.namespace.is_empty() {
            let c = read_conf(conf)?;
            if !c.namespace_databases.is_empty() {
                return run_report_for_configured_databases(conf, &c, args.output.as_ref(), quiet)
                    .await;
            }
        }
    }

    let (context, staged_metadata) = resolve_report_runtime_context(&args).await?;
    let is_multi_db = detect_multi_db_layout(&context.collections_dir);
    let output_path =
        resolve_output_path(args.output.as_ref(), context.reports_dir.as_ref(), context.project_name.as_ref())?;

    let mut warning_collections: Vec<String> = Vec::new();
    if is_multi_db {
        let db_rows = collect_rows_per_db(&context.collections_dir, context.reports_dir.as_ref())?;
        append_warnings_from_db_rows(&db_rows, &mut warning_collections);
        write_multi_db_report(
            &db_rows,
            &output_path,
            &context.cluster,
            context.project_name.as_deref(),
            context.title.as_deref(),
            quiet,
        )?;
    } else {
        render_and_write_single_db_report(&context, &output_path, &mut warning_collections, quiet)?;
    }

    generate_schema_diagrams_if_available(context.reports_dir.as_ref(), context.project_name.as_ref(), quiet)?;
    sync_infer_artifacts_if_metadata_staged(args.config.as_ref(), staged_metadata.as_ref()).await?;
    fail_if_warning_collections(quiet, &warning_collections)?;
    Ok(())
}

pub fn should_fail_report_on_warnings(quiet: bool, warning_count: usize) -> bool {
    !quiet && warning_count > 0
}

#[cfg(test)]
mod tests {
    use super::{
        apply_config_overrides_if_needed, require_post_import_namespace, schema_diagram_identity,
        schema_tables_dir, should_fail_report_on_warnings,
    };
    use crate::cli::{ReportArgs, UriArg};
    use crate::util::read_conf;
    use std::path::Path;

    #[test]
    fn should_fail_report_on_warnings_only_when_not_quiet_and_has_warnings() {
        assert!(!should_fail_report_on_warnings(true, 10));
        assert!(!should_fail_report_on_warnings(false, 0));
        assert!(should_fail_report_on_warnings(false, 1));
    }

    #[test]
    fn post_import_requires_explicit_namespace() {
        assert!(require_post_import_namespace("").is_err());
        assert!(require_post_import_namespace("analytics").is_ok());
    }

    #[test]
    fn schema_diagram_identity_uses_project_for_default_database() {
        assert_eq!(
            schema_diagram_identity("project", ""),
            ("project", "project.schema.html".to_owned())
        );
    }

    #[test]
    fn schema_diagram_identity_uses_database_name_when_present() {
        assert_eq!(
            schema_diagram_identity("project", "analytics"),
            ("analytics", "analytics.schema.html".to_owned())
        );
    }

    #[test]
    fn schema_tables_dir_returns_none_when_schema_tables_are_missing() {
        assert!(schema_tables_dir(Path::new("/path/that/does/not/exist")).is_none());
    }

    #[test]
    fn namespace_override_selects_matching_mapping_without_mutating_grouped_config() {
        let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
        let config_path = temp_dir.path().join("grouped.toml");
        std::fs::write(
            &config_path,
            r#"
[project]
title = "Test project"
project_name = "test"
base_dir = "results"

[source]
uri = "mongodb://localhost"
namespace = ["source_a", "source_b"]

[target]
uri = "postgres://localhost"
database_name = ["target_a", "target_b"]
schema_name = ["schema_a", "schema_b"]
"#,
        )
        .expect("config should be written");

        let mut args = ReportArgs {
            mongo: UriArg { source_uri: None },
            config: Some(config_path.clone()),
            collections_dir: None,
            output: None,
            namespace: "source_b".to_owned(),
            project_dir: None,
            post_import: true,
            check_md5: true,
            noaggregate: false,
        };

        let _staged = apply_config_overrides_if_needed(&mut args)
            .expect("grouped config should be staged for selected namespace");
        let selected_config = read_conf(args.config.as_deref().expect("config should be set"))
            .expect("selected config should remain valid");
        let original_config = read_conf(&config_path).expect("original config should remain valid");

        assert_eq!(selected_config.namespace_databases, ["source_b"]);
        assert_eq!(selected_config.target_database_names, ["target_b"]);
        assert_eq!(selected_config.target_schemas, ["schema_b"]);
        assert_eq!(
            original_config.namespace_databases,
            ["source_a", "source_b"]
        );
        assert_eq!(
            original_config.target_database_names,
            ["target_a", "target_b"]
        );
    }

}
