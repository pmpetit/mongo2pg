//! `import` subcommand: loads exported CSV data into PostgreSQL, executing the
//! generated DDL first and reporting progress/post-import summaries.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::process::Stdio;

use anyhow::{anyhow, Context, Result};
use log::{debug, info, warn};

use crate::cli::ImportArgs;
use crate::commands::shared::{
    apply_config_overrides, build_missing_import_csv_error, download_gcs_prefix_to_local_dir,
    ensure_output_prefix_segments, extract_search_path, format_postgres_error,
    import_table_name_from_csv_path, is_missing_postgis_control_file, is_supported_import_csv_path,
    pg_uri_with_database, preflight_existing_tables_error, quote_ident,
    resolve_local_project_root_from_config, sanitize_name, split_namespace_scope,
    stage_export_metadata_from_gcs, stream_reader_to_copy, strip_postgis_extension_statement,
    strip_psql_preamble, write_post_import_report, ConfigOverrides,
};
use crate::db::pg::connect_client as connect_pg_client;
use crate::export::{ensure_gcs_authentication, resolve_export_write_backend, ExportWriteBackend};
use crate::schema_diagram::parse_sql;
use crate::util::{configured_project_root, read_conf};

pub fn gcs_prefix_candidates_for_import_data(
    prefix: &str,
    cluster_name: Option<&str>,
    project_dir: &str,
    db_name: &str,
) -> Vec<String> {
    let trimmed_prefix = prefix.trim_matches('/');
    let cluster_segment = cluster_name
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let project_segment = project_dir.trim_matches('/');
    let mut candidates = Vec::new();
    let effective_prefix = ensure_output_prefix_segments(prefix, cluster_name, project_dir);

    if !effective_prefix.is_empty() {
        candidates.push(format!("{effective_prefix}/data/{db_name}/"));
    }

    if trimmed_prefix.is_empty() {
        candidates.push(format!("data/{db_name}/"));
        if let Some(cluster_name) = cluster_segment {
            candidates.push(format!("{cluster_name}/data/{db_name}/"));
        }
        if !project_segment.is_empty() {
            candidates.push(format!("{project_segment}/data/{db_name}/"));
        }
    } else {
        candidates.push(format!("{trimmed_prefix}/data/{db_name}/"));
        if !project_segment.is_empty() {
            let ends_with_project = trimmed_prefix
                .split('/')
                .next_back()
                .is_some_and(|last| last == project_segment);
            if !ends_with_project {
                candidates.push(format!(
                    "{trimmed_prefix}/{project_segment}/data/{db_name}/"
                ));
            }
        }
    }

    candidates.sort();
    candidates.dedup();
    candidates
}

pub async fn run_import(args: ImportArgs) -> Result<()> {
    fn process_rss_mb() -> Option<u64> {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        let vm_rss_line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
        let kb = vm_rss_line
            .split_whitespace()
            .nth(1)
            .and_then(|value| value.parse::<u64>().ok())?;
        Some(kb / 1024)
    }

    fn log_import_stage(stage: &str) {
        debug!("[import] stage={stage}");
        if let Some(rss_mb) = process_rss_mb() {
            debug!("[debug][import] stage={stage} rss_mb={rss_mb}");
        }
    }

    fn count_supported_csv_files_recursive(root: &Path) -> usize {
        let mut pending_dirs = vec![root.to_path_buf()];
        let mut count = 0usize;
        while let Some(dir) = pending_dirs.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.filter_map(|entry| entry.ok()) {
                let path = entry.path();
                if path.is_dir() {
                    pending_dirs.push(path);
                } else if is_supported_import_csv_path(&path) {
                    count += 1;
                }
            }
        }
        count
    }

    log_import_stage("start");

    apply_config_overrides(
        &args.config,
        &ConfigOverrides {
            project_dir: args.project_dir.clone(),
            namespace: args.namespace.clone(),
            target_database_name: args.database_name.clone(),
            target_schema_name: args.schema_name.clone(),
            ..ConfigOverrides::default()
        },
    )?;

    let c = read_conf(&args.config)?;
    let target_schema = c.target_schema.clone();
    let target_uri = c
        .target_uri
        .clone()
        .ok_or_else(|| anyhow!("No TARGET_URI provided: add TARGET_URI to the config file"))?;
    let namespace = args
        .namespace
        .clone()
        .or(c.namespace.clone())
        .ok_or_else(|| {
            anyhow!("No NAMESPACE provided: pass --namespace or add NAMESPACE to the config file")
        })?;
    let (db_name, namespace_collection) = split_namespace_scope(&namespace);
    let target_database_name = c.target_database_name.as_deref().unwrap_or(db_name);
    let requested_collection = args.collection.as_deref().or(namespace_collection);
    let requested_collection_dir = requested_collection.map(sanitize_name);

    let storage_backend =
        resolve_export_write_backend(&c.base_dir).unwrap_or(ExportWriteBackend::LocalFs);
    let mut project_root = match &storage_backend {
        ExportWriteBackend::LocalFs => configured_project_root(&c),
        ExportWriteBackend::Gcs { .. } => {
            let local_root = resolve_local_project_root_from_config(&args.config, &c);
            debug!(
                "import metadata root (local, read-only): {}",
                local_root.display()
            );
            local_root
        }
    };
    let mut import_metadata_stage: Option<tempfile::TempDir> = None;

    let mut tables_root = project_root.join("schema").join("tables");
    let mut tables_dir = if tables_root.join(db_name).is_dir() {
        tables_root.join(db_name)
    } else {
        tables_root.clone()
    };

    if !tables_dir.is_dir() {
        if let ExportWriteBackend::Gcs { bucket, prefix } = &storage_backend {
            log_import_stage("gcs_stage_metadata_begin");
            if let Some(stage) = stage_export_metadata_from_gcs(
                bucket,
                prefix,
                c.cluster_name.as_deref(),
                &c.project_dir,
                db_name,
            )
            .await?
            {
                project_root = stage.path().to_path_buf();
                tables_root = project_root.join("schema").join("tables");
                tables_dir = if tables_root.join(db_name).is_dir() {
                    tables_root.join(db_name)
                } else {
                    tables_root.clone()
                };
                info!(
                    "import metadata staged from GCS into temporary directory {}",
                    project_root.display()
                );
                import_metadata_stage = Some(stage);
            } else {
                let expected_prefix = ensure_output_prefix_segments(
                    prefix,
                    c.cluster_name.as_deref(),
                    &c.project_dir,
                );
                return Err(anyhow!(
                    "Cannot stage SQL tables metadata from gs://{}/{}/schema/tables/{}",
                    bucket,
                    expected_prefix,
                    db_name
                ));
            }
            log_import_stage("gcs_stage_metadata_done");
        }
    }

    let data_root = project_root.join("data");
    let mut data_db_dir = if data_root.join(db_name).is_dir() {
        data_root.join(db_name)
    } else {
        data_root.clone()
    };
    let mut import_data_stage: Option<tempfile::TempDir> = None;
    log_import_stage("config_resolved");

    let local_csv_count = if data_db_dir.is_dir() {
        count_supported_csv_files_recursive(&data_db_dir)
    } else {
        0
    };
    let should_stage_from_gcs =
        matches!(&storage_backend, ExportWriteBackend::Gcs { .. }) && local_csv_count == 0;

    if should_stage_from_gcs {
        if let ExportWriteBackend::Gcs { bucket, prefix } = &storage_backend {
            if data_db_dir.is_dir() {
                info!(
                    "local data directory {} has no .csv/.csv.gz files; attempting GCS data staging",
                    data_db_dir.display()
                );
            }
            log_import_stage("gcs_stage_begin");
            ensure_gcs_authentication().await?;

            let stage = tempfile::Builder::new()
                .prefix("mongo2pg-gcs-import-stage-")
                .tempdir()
                .context("Cannot create temporary import staging directory")?;
            let staged_data_root = stage.path().join("data").join(db_name);
            std::fs::create_dir_all(&staged_data_root).with_context(|| {
                format!(
                    "Cannot create staged import data directory {}",
                    staged_data_root.display()
                )
            })?;

            let requested_suffix = requested_collection_dir
                .as_deref()
                .map(|name| format!("{name}/"));
            let mut downloaded = 0usize;
            let candidates = gcs_prefix_candidates_for_import_data(
                prefix,
                c.cluster_name.as_deref(),
                &c.project_dir,
                db_name,
            );
            for candidate in candidates {
                let effective_prefix = if let Some(suffix) = &requested_suffix {
                    format!("{candidate}{suffix}")
                } else {
                    candidate.clone()
                };
                let count =
                    download_gcs_prefix_to_local_dir(bucket, &effective_prefix, &staged_data_root)
                        .await
                        .with_context(|| {
                            format!(
                                "Failed to stage import data from gs://{}/{}",
                                bucket, effective_prefix
                            )
                        })?;
                if count > 0 {
                    let csv_count = count_supported_csv_files_recursive(&staged_data_root);
                    if csv_count > 0 {
                        downloaded += count;
                        info!(
                            "staged {} import data files from gs://{}/{} into {}",
                            count,
                            bucket,
                            effective_prefix,
                            staged_data_root.display()
                        );
                        break;
                    }

                    warn!(
                        "staged {} files from gs://{}/{} but found no .csv/.csv.gz yet under {}; trying next candidate prefix",
                        count,
                        bucket,
                        effective_prefix,
                        staged_data_root.display()
                    );
                }
            }

            if downloaded > 0 {
                data_db_dir = staged_data_root;
                import_data_stage = Some(stage);
            }
            log_import_stage("gcs_stage_done");
        }
    }

    if !tables_dir.is_dir() {
        return Err(anyhow!(
            "Cannot read SQL tables directory {}",
            tables_dir.display()
        ));
    }

    if let Some(stage) = &import_metadata_stage {
        info!(
            "import metadata staging dir (temporary): {}",
            stage.path().display()
        );
    }

    if let Some(stage) = &import_data_stage {
        info!(
            "import data staging dir (temporary): {}",
            stage.path().display()
        );
    }
    if !data_db_dir.is_dir() {
        return Err(anyhow!(
            "Cannot read data directory {}",
            data_db_dir.display()
        ));
    }

    log_import_stage("filesystem_validated");

    log_import_stage("pg_connect_admin_begin");
    // let admin_client = connect_pg_admin_client(&target_uri, target_database_name).await?;
    //ensure_pg_database(&admin_client, target_database_name).await?;

    let db_target_uri = pg_uri_with_database(&target_uri, target_database_name);
    log_import_stage("pg_connect_target_begin");
    let mut pg_client = connect_pg_client(&db_target_uri).await?;

    let mut pending_dirs = vec![tables_dir.clone()];
    let mut sql_files: Vec<PathBuf> = Vec::new();
    while let Some(dir) = pending_dirs.pop() {
        for entry in std::fs::read_dir(&dir)
            .with_context(|| format!("Cannot read {}", dir.display()))?
            .filter_map(|entry| entry.ok())
        {
            let path = entry.path();
            if path.is_dir() {
                pending_dirs.push(path);
                continue;
            }
            sql_files.push(path);
        }
    }

    sql_files.retain(|path| path.extension().and_then(|ext| ext.to_str()) == Some("sql"));
    sql_files.retain(|path| {
        requested_collection_dir
            .as_deref()
            .map_or(true, |collection_dir| {
                path.file_stem().and_then(|stem| stem.to_str()) == Some(collection_dir)
            })
    });
    sql_files.retain(|path| {
        path.file_stem()
            .and_then(|stem| stem.to_str())
            .is_some_and(|name| !name.trim().is_empty())
    });
    sql_files.sort();

    if sql_files.is_empty() {
        return Err(anyhow!("No SQL files found in {}", tables_dir.display()));
    }
    debug!("[debug][import] ddl_files_count={}", sql_files.len());
    log_import_stage("ddl_execute_begin");

    let mut allowed_table_names: HashSet<String> = HashSet::new();
    let mut table_columns_by_name: HashMap<String, Vec<String>> = HashMap::new();
    let mut preflight_existing_tables: Vec<(String, String)> = Vec::new();

    // if let Some(schema_name) = target_schema.as_deref() {
    //     ensure_pg_schema(&pg_client, schema_name).await?;
    // }

    for sql_path in &sql_files {
        let sql = std::fs::read_to_string(sql_path)
            .with_context(|| format!("Failed to read {}", sql_path.display()))?;
        let executable_sql = strip_psql_preamble(&sql);
        if executable_sql.trim().is_empty() {
            continue;
        }

        let file_schema = extract_search_path(&executable_sql)
            .or_else(|| target_schema.clone())
            .unwrap_or_else(|| db_name.to_owned());
        //ensure_pg_schema(&pg_client, &file_schema).await?;

        for table in parse_sql(&executable_sql) {
            let qualified = format!("{}.{}", file_schema, table.name);
            let row = pg_client
                .query_one("SELECT to_regclass($1)::text", &[&qualified])
                .await
                .with_context(|| format!("Failed to check existing table {}", qualified))?;
            let exists: Option<String> = row.try_get(0).with_context(|| {
                format!(
                    "Failed to read to_regclass result while checking existing table {}",
                    qualified
                )
            })?;
            if exists.is_some() {
                preflight_existing_tables.push((file_schema.clone(), table.name.clone()));
            }
            allowed_table_names.insert(table.name.clone());
            table_columns_by_name.insert(
                table.name,
                table
                    .columns
                    .into_iter()
                    .map(|column| column.name)
                    .collect(),
            );
        }
    }

    if !preflight_existing_tables.is_empty() {
        return Err(preflight_existing_tables_error(
            target_database_name,
            &preflight_existing_tables,
        ));
    }

    for sql_path in &sql_files {
        let sql = std::fs::read_to_string(sql_path)
            .with_context(|| format!("Failed to read {}", sql_path.display()))?;
        let executable_sql = strip_psql_preamble(&sql);
        if executable_sql.trim().is_empty() {
            continue;
        }
        match pg_client.batch_execute(&executable_sql).await {
            Ok(()) => {}
            Err(err) if is_missing_postgis_control_file(&err) => {
                let fallback_sql = strip_postgis_extension_statement(&executable_sql);
                pg_client
                    .batch_execute(&fallback_sql)
                    .await
                    .with_context(|| {
                        format!(
                            "Failed to execute {} after removing PostGIS extension statement",
                            sql_path.display()
                        )
                    })?;
            }
            Err(err) => {
                return Err(anyhow!(
                    "Failed to execute {}\n{}",
                    sql_path.display(),
                    format_postgres_error(&err)
                ));
            }
        }
        info!("Created PostgreSQL objects from {}", sql_path.display());
    }

    log_import_stage("ddl_execute_done");
    if import_metadata_stage.take().is_some() {
        log_import_stage("gcs_stage_metadata_released");
    }

    let mut pending_dirs = vec![data_db_dir.clone()];
    let mut csv_candidates: Vec<PathBuf> = Vec::new();
    while let Some(dir) = pending_dirs.pop() {
        for entry in std::fs::read_dir(&dir)
            .with_context(|| format!("Cannot read {}", dir.display()))?
            .filter_map(|entry| entry.ok())
        {
            let path = entry.path();
            if path.is_dir() {
                pending_dirs.push(path);
                continue;
            }
            if !is_supported_import_csv_path(&path) {
                continue;
            }

            let table_name = match import_table_name_from_csv_path(&path) {
                Some(name) => name,
                None => continue,
            };

            let import_collection = path.strip_prefix(&data_db_dir).ok().and_then(|relative| {
                let mut parts = relative.components();
                let first = parts.next()?.as_os_str().to_str()?;
                if parts.next().is_some() {
                    Some(first)
                } else {
                    None
                }
            });

            if !requested_collection_dir
                .as_deref()
                .map_or(true, |collection_dir| {
                    import_collection == Some(collection_dir) || table_name == collection_dir
                })
            {
                continue;
            }

            if allowed_table_names.contains(&table_name) {
                csv_candidates.push(path);
            }
        }
    }

    csv_candidates.sort();
    let mut csv_files_by_table: HashMap<String, PathBuf> = HashMap::new();
    for path in csv_candidates {
        let Some(table_name) = import_table_name_from_csv_path(&path) else {
            continue;
        };
        let is_gz = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".csv.gz"));
        match csv_files_by_table.get(&table_name) {
            Some(existing) => {
                let existing_is_gz = existing
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.ends_with(".csv.gz"));
                if is_gz && !existing_is_gz {
                    csv_files_by_table.insert(table_name, path);
                }
            }
            None => {
                csv_files_by_table.insert(table_name, path);
            }
        }
    }

    let mut csv_files: Vec<PathBuf> = csv_files_by_table.into_values().collect();
    csv_files.sort();

    if csv_files.is_empty() {
        return Err(build_missing_import_csv_error(
            &data_db_dir,
            requested_collection_dir.as_deref(),
            allowed_table_names.len(),
        ));
    }
    debug!("[debug][import] csv_files_count={}", csv_files.len());
    log_import_stage("csv_discovery_done");

    log_import_stage("transaction_begin");
    let transaction = pg_client.transaction().await?;
    transaction
        .batch_execute("SET CONSTRAINTS ALL DEFERRED;")
        .await?;

    let mut imported_relations: Vec<(String, String)> = Vec::new();

    log_import_stage("copy_begin");
    for csv_path in &csv_files {
        let schema = target_schema
            .as_deref()
            .or_else(|| {
                csv_path
                    .parent()
                    .and_then(|path| path.file_name())
                    .and_then(|name| name.to_str())
            })
            .ok_or_else(|| anyhow!("Cannot derive schema name from {}", csv_path.display()))?;
        let table = import_table_name_from_csv_path(csv_path.as_path())
            .ok_or_else(|| anyhow!("Cannot derive table name from {}", csv_path.display()))?;
        info!(
            "Unzip and /COPY file {} into {}.{}",
            csv_path.display(),
            schema,
            table
        );
        let table_columns = table_columns_by_name
            .get(&table)
            .ok_or_else(|| anyhow!("No DDL column metadata found for table {table}"))?;
        let copy_sql = if table_columns.is_empty() {
            warn!(
                "No parsed DDL columns for {}.{}; using COPY without explicit column list",
                schema, table
            );
            format!(
                "COPY {}.{} FROM STDIN WITH (FORMAT csv, HEADER true)",
                quote_ident(schema),
                quote_ident(&table),
            )
        } else {
            let copy_columns = table_columns
                .iter()
                .map(|column| quote_ident(&column.trim_matches('"').replace("\"\"", "\"")))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "COPY {}.{} ({}) FROM STDIN WITH (FORMAT csv, HEADER true)",
                quote_ident(schema),
                quote_ident(&table),
                copy_columns,
            )
        };
        let is_gz = csv_path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".csv.gz"));
        if let Ok(meta) = std::fs::metadata(csv_path) {
            debug!(
                "[import] copy_prepare table={}.{} file={} compressed_bytes={} gz={}",
                schema,
                table,
                csv_path.display(),
                meta.len(),
                is_gz
            );
        }
        let sink = match transaction.copy_in(&copy_sql).await {
            Ok(sink) => sink,
            Err(err) => {
                return Err(anyhow!(
                    "Failed to start COPY for {}.{}\n{}",
                    schema,
                    table,
                    format_postgres_error(&err)
                ));
            }
        };
        let mut sink = pin!(sink);
        let streamed_bytes = if is_gz {
            info!(
                "[import] copy_decompress_begin table={}.{} file={}",
                schema,
                table,
                csv_path.display()
            );
            let mut child = tokio::process::Command::new("gunzip")
                .arg("-c")
                .arg(csv_path)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .with_context(|| {
                    format!(
                        "Failed to execute gunzip for {}. Ensure gunzip is installed",
                        csv_path.display()
                    )
                })?;
            let stdout = child.stdout.take().ok_or_else(|| {
                anyhow!("Failed to capture gunzip stdout for {}", csv_path.display())
            })?;

            let streamed = stream_reader_to_copy(stdout, &mut sink, 64 * 1024)
                .await
                .with_context(|| {
                    format!(
                        "Failed to stream decompressed data for {}",
                        csv_path.display()
                    )
                })?;

            let output = child.wait_with_output().await.with_context(|| {
                format!(
                    "Failed waiting for gunzip process for {}",
                    csv_path.display()
                )
            })?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
                return Err(anyhow!(
                    "gunzip failed for {} (status={}): {}",
                    csv_path.display(),
                    output
                        .status
                        .code()
                        .map(|code| code.to_string())
                        .unwrap_or_else(|| "signal".to_owned()),
                    stderr
                ));
            }
            info!(
                "[import] copy_decompress_done table={}.{} file={} streamed_bytes={}",
                schema,
                table,
                csv_path.display(),
                streamed
            );
            streamed
        } else {
            info!(
                "[import] copy_read_begin table={}.{} file={}",
                schema,
                table,
                csv_path.display()
            );
            let file = tokio::fs::File::open(csv_path)
                .await
                .with_context(|| format!("Failed to open {}", csv_path.display()))?;
            let streamed = stream_reader_to_copy(file, &mut sink, 64 * 1024)
                .await
                .with_context(|| format!("Failed to stream CSV data for {}", csv_path.display()))?;
            info!(
                "[import] copy_read_done table={}.{} file={} streamed_bytes={}",
                schema,
                table,
                csv_path.display(),
                streamed
            );
            streamed
        };
        log_import_stage("copy_payload_loaded");
        let rows = match sink.as_mut().finish().await {
            Ok(rows) => rows,
            Err(err) => {
                return Err(anyhow!(
                    "Failed to finish COPY for {}.{} from {}\n{}",
                    schema,
                    table,
                    csv_path.display(),
                    format_postgres_error(&err)
                ));
            }
        };
        info!(
            "Imported {rows} row(s) into {}.{} from {} (streamed_bytes={})",
            schema,
            table,
            csv_path.display(),
            streamed_bytes
        );
        imported_relations.push((schema.to_owned(), table));
        log_import_stage("copy_table_done");
    }

    log_import_stage("transaction_commit_begin");
    transaction.commit().await?;

    imported_relations.sort();
    imported_relations.dedup();
    log_import_stage("analyze_begin");
    for (schema, table) in &imported_relations {
        info!("Analyze imported table {}.{}", schema, table);
        let analyze_sql = format!("ANALYZE {}.{}", quote_ident(schema), quote_ident(table));
        if let Err(err) = pg_client.batch_execute(&analyze_sql).await {
            return Err(anyhow!(
                "Failed to analyze {}.{}\n{}",
                schema,
                table,
                format_postgres_error(&err)
            ));
        }
    }
    log_import_stage("analyze_done");

    if import_data_stage.take().is_some() {
        log_import_stage("gcs_stage_data_released");
    }
    info!("Import completed for database '{target_database_name}'.");

    let post_import_namespace = if namespace_collection.is_none() {
        args.collection
            .as_deref()
            .map(|collection| format!("{db_name}.{collection}"))
            .unwrap_or_else(|| namespace.clone())
    } else {
        namespace.clone()
    };
    log_import_stage("post_import_report_begin");
    write_post_import_report(&args.config, &post_import_namespace, "", true).await?;
    log_import_stage("done");

    Ok(())
}
