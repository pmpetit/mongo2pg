//! `report` subcommand: renders HTML schema/inference reports from the
//! `source/collections` metadata tree, plus optional `--post-import` mode.

use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use log::{info, warn};

use crate::cli::ReportArgs;
use crate::commands::infer::move_infer_artifacts_to_gcs_if_needed_with_project_root;
use crate::commands::shared::{
    apply_config_overrides, resolve_local_project_root_from_config, split_namespace_scope,
    stage_config_path_if_gcs, stage_report_metadata_from_gcs, write_post_import_report,
    ConfigOverrides,
};
use crate::export::{resolve_export_write_backend, ExportWriteBackend};
use crate::report::{collect_rows, render_html};
use crate::schema_diagram::{load_tables_by_db, render_schema_html};
use crate::util::read_conf;

pub async fn run_report(mut args: ReportArgs, quiet: bool) -> Result<()> {
    let mut _staged_config: Option<tempfile::TempDir> = None;
    let mut _staged_metadata: Option<tempfile::TempDir> = None;
    if let Some(config) = args.config.take() {
        let (local, stage) = stage_config_path_if_gcs(config).await?;
        args.config = Some(local);
        _staged_config = stage;
    }

    if let Some(conf) = args.config.as_deref() {
        apply_config_overrides(
            conf,
            &ConfigOverrides {
                project_dir: args.project_dir.clone(),
                source_uri: args.mongo.source_uri.clone(),
                namespace: (!args.namespace.is_empty()).then(|| args.namespace.clone()),
                ..ConfigOverrides::default()
            },
        )?;
    }

    if args.post_import {
        let conf = args
            .config
            .as_ref()
            .ok_or_else(|| anyhow!("--post-import requires -c <config>"))?;
        let namespace = if args.namespace.is_empty() {
            read_conf(conf)?.namespace.ok_or_else(|| {
                anyhow!(
                    "No NAMESPACE provided: pass --namespace or add NAMESPACE to the config file"
                )
            })?
        } else {
            args.namespace.clone()
        };
        write_post_import_report(
            conf,
            &namespace,
            args.mongo.source_uri.as_deref().unwrap_or(""),
            args.check_md5,
        )
        .await?;

        return Ok(());
    }

    // Resolve collections dir, cluster label, reports dir and project name
    let (collections_dir, namespace, cluster, reports_dir, project_name, ftitle) =
        if let Some(ref conf) = args.config {
            let c = read_conf(conf)?;
            let local_project_root = match resolve_export_write_backend(&c.base_dir)? {
                ExportWriteBackend::LocalFs => resolve_local_project_root_from_config(conf, &c),
                ExportWriteBackend::Gcs { bucket, prefix } => {
                    let db_name = c.namespace.as_deref().unwrap_or(&c.project_dir);
                    let stage = stage_report_metadata_from_gcs(
                        &bucket,
                        &prefix,
                        c.cluster_name.as_deref(),
                        &c.project_dir,
                        db_name,
                    )
                    .await?;
                    let root = stage.path().to_path_buf();
                    _staged_metadata = Some(stage);
                    root
                }
            };
            let ns = args.namespace.clone();
            let ns = if ns.is_empty() {
                c.namespace.unwrap_or_else(|| c.project_dir.clone())
            } else {
                ns
            };
            let cluster = c
                .source_uri
                .as_deref()
                .map(crate::report::cluster_from_uri)
                .unwrap_or_default();
            let cols_dir = local_project_root.join("source").join("collections");
            let rep_dir = local_project_root.join("reports");
            let proj = c.project_dir.clone();
            let ftitle = c.title.clone();
            (
                cols_dir,
                ns,
                cluster,
                Some(rep_dir),
                Some(proj),
                Some(ftitle),
            )
        } else {
            let dir = args
                .collections_dir
                .clone()
                .ok_or_else(|| anyhow!("Provide --collections-dir or -c <config>"))?;
            (dir, args.namespace.clone(), String::new(), None, None, None)
        };

    // Detect whether source/collections has the per-db layout:
    // a per-db layout has subdirs that contain further subdirs (not direct .stats.yaml files).
    let is_multi_db = std::fs::read_dir(&collections_dir)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| e.path().is_dir())
                .any(|e| {
                    // It's a db folder if it contains at least one subdir
                    std::fs::read_dir(e.path())
                        .map(|sub| sub.filter_map(|s| s.ok()).any(|s| s.path().is_dir()))
                        .unwrap_or(false)
                })
        })
        .unwrap_or(false);

    let output_path = if let Some(ref o) = args.output {
        o.clone()
    } else if let (Some(ref dir), Some(ref _proj)) = (&reports_dir, &project_name) {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("Can't create reports dir {}", dir.display()))?;
        dir.join("main.html")
    } else {
        PathBuf::from("main.html")
    };

    let mut warning_collections: Vec<String> = Vec::new();

    if is_multi_db {
        // ── Per-db layout ──────────────────────────────────────────────────────
        // Enumerate database subfolders and collect rows per db.
        let mut db_names: Vec<String> = std::fs::read_dir(&collections_dir)
            .with_context(|| format!("Cannot read {}", collections_dir.display()))?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        db_names.sort();

        // Resolve SQL tables dir for PG tables column (per-db: schema/tables/<db>/)
        let tables_root: Option<PathBuf> = reports_dir
            .as_ref()
            .map(|r| r.parent().unwrap_or(r).join("schema").join("tables"));

        let db_rows: Vec<(String, Vec<crate::report::CollectionRow>)> = db_names
            .iter()
            .map(|db_name| {
                let db_dir = collections_dir.join(db_name);
                let tables_dir_opt: Option<PathBuf> = tables_root
                    .as_deref()
                    .map(|t| t.join(db_name))
                    .filter(|p| p.is_dir());
                let rows = collect_rows(&db_dir, tables_dir_opt.as_deref()).unwrap_or_default();
                (db_name.clone(), rows)
            })
            .collect();

        for (db_name, rows) in &db_rows {
            for row in rows.iter().filter(|row| row.has_infer_warnings()) {
                warning_collections.push(format!("{db_name}.{}", row.name));
            }
        }

        let entries: Vec<(&str, &[crate::report::CollectionRow])> = db_rows
            .iter()
            .map(|(name, rows)| (name.as_str(), rows.as_slice()))
            .collect();

        let proj = project_name.as_deref().unwrap_or("project");
        let title = ftitle.as_deref().unwrap_or("mongo2pg Report");
        let html = crate::report::render_multi_db_html(&entries, &cluster, proj, title);
        std::fs::write(&output_path, &html)
            .with_context(|| format!("Failed to write {}", output_path.display()))?;
        if !quiet {
            info!("Report written to {}", output_path.display());
        }
    } else {
        // ── Flat / single-db layout ────────────────────────────────────────────
        let tables_dir_for_report: Option<PathBuf> = reports_dir.as_ref().map(|r| {
            let tables_root = r.parent().unwrap_or(r).join("schema").join("tables");
            let db_name = split_namespace_scope(&namespace).0;
            let db_tables = tables_root.join(db_name);
            if db_tables.is_dir() {
                db_tables
            } else {
                tables_root
            }
        });
        let tables_dir_opt = tables_dir_for_report.as_deref().filter(|p| p.is_dir());

        let rows = collect_rows(&collections_dir, tables_dir_opt)?;
        warning_collections.extend(
            rows.iter()
                .filter(|row| row.has_infer_warnings())
                .map(|row| row.name.clone()),
        );
        let title = ftitle.as_deref().unwrap_or("mongo2pg Report");
        let html = render_html(&rows, &namespace, &cluster, &title);
        std::fs::write(&output_path, &html)
            .with_context(|| format!("Failed to write {}", output_path.display()))?;
        if !quiet {
            info!("Report written to {}", output_path.display());
        }
    }

    // Generate per-database schema ERD diagrams if SQL tables exist
    if let (Some(ref rep_dir), Some(ref proj)) = (&reports_dir, &project_name) {
        let tables_dir = rep_dir
            .parent()
            .unwrap_or(rep_dir)
            .join("schema")
            .join("tables");
        if tables_dir.is_dir() {
            match load_tables_by_db(&tables_dir) {
                Ok(db_tables) => {
                    for (db_name, tables) in &db_tables {
                        if tables.is_empty() {
                            continue;
                        }
                        // flat layout: use project name; per-db: use db name
                        let label = if db_name.is_empty() {
                            proj.as_str()
                        } else {
                            db_name.as_str()
                        };
                        let filename = if db_name.is_empty() {
                            format!("{proj}.schema.html")
                        } else {
                            format!("{db_name}.schema.html")
                        };
                        let schema_html = render_schema_html(tables, label);
                        let schema_path = rep_dir.join(&filename);
                        std::fs::write(&schema_path, &schema_html).with_context(|| {
                            format!("Failed to write {}", schema_path.display())
                        })?;
                        if !quiet {
                            info!("Schema diagram written to {}", schema_path.display());
                        }
                    }
                }
                Err(e) => warn!("could not generate schema diagram: {e}"),
            }
        }
    }

    if let (Some(conf), Some(stage)) = (args.config.as_deref(), _staged_metadata.as_ref()) {
        move_infer_artifacts_to_gcs_if_needed_with_project_root(conf, Some(stage.path())).await?;
    }

    if should_fail_report_on_warnings(quiet, warning_collections.len()) {
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
        return Err(anyhow!(
            "report contains infer warnings in {} collection(s): {}{}",
            warning_collections.len(),
            preview,
            suffix
        ));
    }

    Ok(())
}

pub fn should_fail_report_on_warnings(quiet: bool, warning_count: usize) -> bool {
    !quiet && warning_count > 0
}
