//! `mongo2pg` CLI – Infer a MongoDB collection schema and convert it to PostgreSQL DDL.
//!
//! # Subcommands
//!
//! ## `mongo2pg infer` (default when no subcommand given)
//! ```text
//! mongo2pg infer <SOURCE_URI> <DB.COLLECTION> [OPTIONS]
//! ```
//! Samples documents and writes the inferred schema JSON to stdout.
//!
//! ## `mongo2pg to-pg`
//! ```text
//! mongo2pg to-pg <SCHEMA_FILE> [--table <TABLE_NAME>]
//! ```
//! Converts a schema JSON file produced by `infer` into PostgreSQL DDL.

use std::collections::HashMap;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use bson::{doc, Bson};
use bytes::Bytes;
use clap::{Args, Parser, Subcommand};
use flate2::read::GzDecoder;
use futures::{SinkExt, TryStreamExt};
use indexmap::IndexMap;
use mongo2pg::analyzer::{Analyzer, CollectionSchema, FieldSchema, TypeSchema};
use mongo2pg::export::export_collection;
use mongo2pg::report::{
    collect_rows, compute_cluster_score, compute_db_score, render_cluster_html, render_html,
    render_post_import_html, PostImportCollectionRow, PostImportNode, PostImportTableRow,
    SYSTEM_DATABASES,
};
use mongo2pg::schema_diagram::{load_tables_by_db, parse_sql, render_schema_html};
use mongo2pg::stats::{format_stats, stats_to_yaml};
use mongo2pg::to_pg::schema_to_ddl;
use mongodb::{options::ClientOptions, Client};
use postgres_native_tls::MakeTlsConnector;
use serde::{Deserialize, Serialize};
use strsim::jaro_winkler;

// ──────────────────────────────────────────────────────────────────────────────
// Shared args
// ──────────────────────────────────────────────────────────────────────────────

/// MongoDB source URI argument shared across commands that connect to MongoDB.
/// When `-c` is also provided, this overrides the SOURCE_URI stored in the config file.
#[derive(Args, Debug, Clone)]
struct UriArg {
    /// MongoDB source connection URI (e.g. mongodb://localhost:27017) – required unless -c is given;
    /// overrides the SOURCE_URI stored in the config file when -c is also provided
    #[arg(long = "source-uri", required_unless_present = "config")]
    source_uri: Option<String>,
}

// ──────────────────────────────────────────────────────────────────────────────
// CLI definition
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(
    name = "mongo2pg",
    about = "Infer a MongoDB collection schema and convert it to PostgreSQL DDL",
    version,
    // Allow bare `mongo2pg <SOURCE_URI> <NS>` without an explicit subcommand.
    args_conflicts_with_subcommands = true
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    // Flat infer args (used when no subcommand is given)
    #[command(flatten)]
    infer: Option<InferArgs>,
}

#[derive(Subcommand)]
enum Command {
    /// Sample a MongoDB collection and infer its JSON Schema (default)
    Infer(InferArgs),
    /// Convert a schema JSON file to PostgreSQL DDL CREATE TABLE statements
    ToPg(ToPgArgs),
    /// Initialize a new migration project directory structure
    Init(InitArgs),
    /// Generate an HTML migration report from inferred collection stats
    Report(ReportArgs),
    /// Export MongoDB data to gzipped CSV files (one per SQL table)
    Export(ExportArgs),
    /// Create PostgreSQL objects and import exported CSV files into PostgreSQL
    Import(ImportArgs),
    /// Generate a cluster-level HTML report aggregating scores across multiple databases
    ClusterReport(ClusterReportArgs),
}

#[derive(Parser, Debug)]
struct InferArgs {
    #[command(flatten)]
    mongo: UriArg,

    /// Namespace: either <db>.<collection> to infer one collection, or just <db> to infer all
    /// collections in the database. When omitted (and -c is not given) all user databases on the
    /// server are enumerated and inferred (admin, local, and config are skipped). Can also be set
    /// via NAMESPACE in the config file.
    #[arg(long = "namespace")]
    namespace: Option<String>,

    /// Number of documents to sample (mutually exclusive with --percent); default 1000
    #[arg(short = 'n', long = "number", conflicts_with = "percent")]
    number: Option<u64>,

    /// Percentage of the collection to sample, e.g. 10 for 10% (mutually exclusive with --number)
    #[arg(short = 'p', long = "percent", conflicts_with = "number", value_parser = clap::value_parser!(f64))]
    percent: Option<f64>,

    /// Treat all MongoDB Object fields as JSONB columns in the generated DDL
    /// instead of creating 1:1 child tables (arrays of objects are unaffected)
    #[arg(long = "jsonb", action = clap::ArgAction::SetTrue)]
    jsonb: bool,

    /// Print inferred schema JSON to stdout
    #[arg(long = "print-json", action = clap::ArgAction::SetTrue)]
    print_json: bool,

    /// Deprecated compatibility flag. JSON is no longer printed by default.
    #[arg(long = "no-output", hide = true, action = clap::ArgAction::SetTrue)]
    no_output: bool,

    /// Write <name>.json and <name>.stats.txt into <output_dir>/<name>/ for each collection
    #[arg(short = 'o', long = "output-dir", conflicts_with = "config")]
    output_dir: Option<PathBuf>,

    /// Path to a project config file (TOML) created by `mongo2pg init`
    #[arg(short = 'c', long = "config", conflicts_with = "output_dir")]
    config: Option<PathBuf>,
}

#[derive(Parser, Debug)]
struct ToPgArgs {
    /// Optional collection name; if omitted all collections under source/collections/ are processed
    collection: Option<String>,

    /// Root table name (only valid with a single collection name)
    #[arg(short = 't', long = "table")]
    table: Option<String>,

    /// Path to the project config file (TOML) – derives source/collections and schema/tables paths
    #[arg(short = 'c', long = "config", conflicts_with = "output_dir")]
    config: Option<PathBuf>,

    /// Directory to write the SQL file(s) into (overrides -c)
    #[arg(short = 'o', long = "output-dir", conflicts_with = "config")]
    output_dir: Option<PathBuf>,

    /// PostgreSQL schema name: strips `{schema}_` prefix from child table names and
    /// prepends `CREATE SCHEMA IF NOT EXISTS` + `SET search_path` to the output.
    /// When omitted, each collection is deployed into its own PostgreSQL schema.
    #[arg(long = "schema")]
    schema: Option<String>,
}

#[derive(Parser, Debug)]
struct InitArgs {
    /// Base directory under which the project folder will be created
    #[arg(long)]
    project_base: PathBuf,

    /// Name of the project (becomes a sub-folder inside project_base)
    #[arg(long)]
    project_name: String,

    /// MongoDB source connection URI to store in the project config
    #[arg(long = "source-uri")]
    source_uri: Option<String>,

    /// PostgreSQL target connection URI to store in the project config
    #[arg(long = "target-uri")]
    target_uri: Option<String>,

    /// Namespace to store in the project config (e.g. mydb or mydb.mycoll); when omitted,
    /// NAMESPACE is not written to the config file so `infer` will enumerate all databases
    #[arg(long)]
    namespace: Option<String>,
}

#[derive(Parser, Debug)]
struct ReportArgs {
    #[command(flatten)]
    mongo: UriArg,

    /// Path to the project config file (TOML) – derives source/collections and output paths
    #[arg(short = 'c', long = "config", conflicts_with_all = ["collections_dir", "output"])]
    config: Option<PathBuf>,

    /// Path to the source/collections directory (overrides -c)
    #[arg(long = "collections-dir", conflicts_with = "config")]
    collections_dir: Option<PathBuf>,

    /// Where to write the HTML report (default: reports/main.html or main.html)
    #[arg(short = 'o', long = "output")]
    output: Option<PathBuf>,

    /// Database / namespace label shown in the report header
    #[arg(short = 'n', long = "namespace", default_value = "")]
    namespace: String,

    /// Connect to MongoDB and PostgreSQL and write a post-import validation report.
    #[arg(long = "post-import", action = clap::ArgAction::SetTrue)]
    post_import: bool,
}

#[derive(Parser, Debug)]
struct SchemaArgs {
    #[command(flatten)]
    mongo: UriArg,

    /// Path to the project config file (TOML) – derives schema/tables and reports paths
    #[arg(short = 'c', long = "config", conflicts_with = "tables_dir")]
    config: Option<PathBuf>,

    /// Directory containing the SQL DDL files (overrides -c)
    #[arg(long = "tables-dir", conflicts_with = "config")]
    tables_dir: Option<PathBuf>,

    /// Where to write the HTML diagram (default: reports/<project_name>.schema.html)
    #[arg(short = 'o', long = "output")]
    output: Option<PathBuf>,
}

#[derive(Parser, Debug)]
struct ExportArgs {
    /// Optional collection name; if omitted all collections in schema/tables/ are exported
    collection: Option<String>,

    /// Path to the project config file (TOML) – derives SOURCE_URI, db, schema/tables and data/ paths
    #[arg(short = 'c', long = "config")]
    config: Option<PathBuf>,

    #[command(flatten)]
    mongo: UriArg,

    /// Override the output directory for CSV files (default: <project>/data/)
    #[arg(short = 'o', long = "output-dir")]
    output_dir: Option<PathBuf>,

    /// Namespace: either <db>.<collection> to export one collection, or just <db> to export all
    /// collections in the database. When omitted (and -c is not given) all user databases on the
    /// server are enumerated and exported (admin, local, and config are skipped). Can also be set
    /// via NAMESPACE in the config file. This overrides the namespace in the config file if provided.
    #[arg(long = "namespace")]
    namespace: Option<String>,
}

#[derive(Parser, Debug)]
struct ImportArgs {
    /// Optional collection name; if omitted all collections for the namespace are imported
    collection: Option<String>,

    /// Path to the project config file (TOML) – derives TARGET_URI, schema/tables and data/ paths
    #[arg(short = 'c', long = "config")]
    config: PathBuf,

    /// Namespace: either <db>.<collection> to import one collection, or just <db> to import all.
    /// This overrides the namespace in the config file if provided.
    #[arg(long = "namespace")]
    namespace: Option<String>,
}

#[derive(Parser, Debug)]
struct ClusterReportArgs {
    /// One or more project config files (TOML); can be repeated or comma-separated
    #[arg(long = "configs", value_delimiter = ',', num_args = 1..)]
    configs: Vec<PathBuf>,

    /// Where to write the HTML cluster report (default: cluster.html)
    #[arg(short = 'o', long = "output")]
    output: Option<PathBuf>,

    /// MongoDB cluster label shown in the report header (derived from the first config URI when omitted)
    #[arg(long = "cluster", default_value = "")]
    cluster_label: String,
}

// ──────────────────────────────────────────────────────────────────────────────
// Entry point
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Some(Command::Init(args)) => run_init(args),
        Some(Command::ToPg(args)) => run_to_pg(args, false),
        Some(Command::Report(args)) => run_report(args, false).await,
        Some(Command::Export(args)) => run_export(args).await,
        Some(Command::Import(args)) => run_import(args).await,
        Some(Command::Infer(args)) => run_infer(args).await,
        Some(Command::ClusterReport(args)) => run_cluster_report(args),
        None => run_infer(cli.infer.expect("clap ensures args are present")).await,
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// `to-pg` subcommand
// ──────────────────────────────────────────────────────────────────────────────

fn run_to_pg(args: ToPgArgs, quiet: bool) -> Result<()> {
    fn prepend_database_preamble(ddl: String, db_name: Option<&str>) -> String {
        match db_name {
            None => ddl,
            Some(name) => {
                let quoted_name = quote_ident(name);
                format!("CREATE DATABASE {quoted_name};\n\\connect {quoted_name}\n\n{ddl}")
            }
        }
    }

    // Resolve collections source dir and SQL output dir
    let config_db_name: Option<String> = if let Some(ref conf) = args.config {
        let c = read_conf(conf)?;
        c.namespace
            .map(|namespace| split_namespace_scope(&namespace).0.to_owned())
    } else {
        None
    };

    let config_target_schema: Option<String> = if let Some(ref conf) = args.config {
        read_conf(conf)?.target_schema
    } else {
        None
    };

    let (collections_dir, output_dir) = if let Some(ref conf) = args.config {
        let c = read_conf(conf)?;
        let (base_dir, project_dir) = (c.base_dir, c.project_dir);
        let cols = base_dir
            .join(&project_dir)
            .join("source")
            .join("collections");
        let sql_out = base_dir.join(&project_dir).join("schema").join("tables");
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
        eprintln!(
            "No JSON schema files found in {}",
            collections_dir.display()
        );
        return Ok(());
    }

    for (rel_sql, json_path) in &json_files {
        let sql_path = output_dir.join(rel_sql);
        if let Some(parent) = sql_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create directory {}", parent.display()))?;
        }
        let table_name = args.table.as_deref().unwrap_or_else(|| {
            rel_sql
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("table")
        });
        let content = std::fs::read_to_string(json_path)
            .with_context(|| format!("Failed to read {}", json_path.display()))?;
        let schema: CollectionSchema = serde_json::from_str(&content)
            .with_context(|| format!("Failed to parse {}", json_path.display()))?;
        let ddl = schema_to_ddl(
            &schema,
            table_name,
            args.schema
                .as_deref()
                .or(config_target_schema.as_deref())
                .or(Some(table_name)),
        );
        let db_name = rel_sql
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|s| s.to_str());
        let ddl = prepend_database_preamble(ddl, db_name);
        std::fs::write(&sql_path, &ddl)
            .with_context(|| format!("Failed to write {}", sql_path.display()))?;
        if !quiet {
            println!("SQL written to {}", sql_path.display());
        }
    }

    if !quiet {
        eprintln!(
            "to-pg completed. Review the generated SQL files to confirm that schema names and table names suit your needs."
        );
        eprintln!(
            "Also check that table and column names do not exceed PostgreSQL's 63-byte identifier limit."
        );
        eprintln!(
            "If you modify those SQL files, the next export and report commands will use them as their source of truth."
        );
    }

    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────────
// `infer` subcommand (also the default)
// ──────────────────────────────────────────────────────────────────────────────

async fn run_infer(args: InferArgs) -> Result<()> {
    let chained_config = args.config.clone();
    let chained_output_dir = args.output_dir.clone();
    let quiet_infer = chained_config.is_some();
    // Resolve SOURCE_URI, namespace, number, and percent – reading conf file if -c was given
    let (
        resolved_source_uri,
        effective_output_dir,
        conf_namespace,
        conf_number,
        conf_percent,
        conf_jsonb,
        conf_include,
        conf_exclude,
    ) = if let Some(ref conf) = args.config {
        let c = read_conf(conf)?;
        let source_uri = args.mongo.source_uri.clone().or(c.source_uri).ok_or_else(|| {
                anyhow!("No SOURCE_URI provided: pass --source-uri or add SOURCE_URI to the config file")
            })?;
        let out_dir = args.output_dir.clone().unwrap_or_else(|| {
            c.base_dir
                .join(&c.project_dir)
                .join("source")
                .join("collections")
        });
        (
            source_uri,
            Some(out_dir),
            c.namespace,
            c.number,
            c.percent,
            c.jsonb,
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
            false,
            Vec::new(),
            Vec::new(),
        )
    };

    let namespace = args.namespace.clone().or(conf_namespace);

    let client_options = ClientOptions::parse(&resolved_source_uri)
        .await
        .context("Failed to parse MongoDB SOURCE_URI")?;
    let client = Client::with_options(client_options).context("Failed to create MongoDB client")?;

    // CLI takes priority over conf for number/percent/jsonb; then fall back to defaults
    let resolved_number = args.number.or(conf_number);
    let resolved_percent = args.percent.or(conf_percent);
    let resolved_jsonb = args.jsonb || conf_jsonb;

    let args = InferArgs {
        mongo: UriArg {
            source_uri: Some(resolved_source_uri),
        },
        namespace: namespace.clone(),
        output_dir: effective_output_dir,
        number: resolved_number,
        percent: resolved_percent,
        jsonb: resolved_jsonb,
        config: None,
        ..args
    };

    match namespace {
        None => {
            // No namespace provided: enumerate all user databases and infer each.
            infer_all_databases(&client, &args, &conf_include, &conf_exclude, !quiet_infer).await?;
        }
        Some(ref ns) if ns.contains('.') => {
            // Single collection: <db>.<collection>
            let (db_name, coll_name) = parse_namespace(ns)?;
            let existing_dbs = client
                .list_database_names()
                .await
                .context("Failed to list databases")?;
            if !existing_dbs.iter().any(|d| d == db_name) {
                eprintln!(
                    "Warning: database '{db_name}' does not exist on the server. Available databases: {}",
                    existing_dbs.join(", ")
                );
            }
            let existing_colls = client
                .database(db_name)
                .list_collection_names()
                .await
                .context("Failed to list collections")?;
            if !existing_colls.iter().any(|c| c == coll_name) {
                eprintln!(
                    "Warning: collection '{coll_name}' does not exist in database '{db_name}'. Available collections: {}",
                    existing_colls.join(", ")
                );
            }
            if !should_infer_collection(coll_name, &conf_include, &conf_exclude) {
                eprintln!(
                    "Skipping {db_name}.{coll_name}: filtered out by source.include/source.exclude"
                );
                return Ok(());
            }
            let schema = infer_collection(
                &client,
                db_name,
                coll_name,
                coll_name,
                &args,
                None,
                Some((1, 1)),
                !quiet_infer,
            )
            .await?;
            if args.print_json && !args.no_output {
                println!("{}", serde_json::to_string_pretty(&schema)?);
            }
        }
        Some(ref ns) => {
            // Whole single database: infer every collection.
            let db_name = ns.as_str();
            let existing_dbs = client
                .list_database_names()
                .await
                .context("Failed to list databases")?;
            if !existing_dbs.iter().any(|d| d == db_name) {
                eprintln!(
                    "Warning: database '{db_name}' does not exist on the server. Available databases: {}",
                    existing_dbs.join(", ")
                );
            }
            let db = client.database(db_name);
            let coll_names = db
                .list_collection_names()
                .await
                .context("Failed to list collections")?;
            let filtered_coll_names: Vec<&String> = coll_names
                .iter()
                .filter(|n| !n.starts_with("system."))
                .filter(|n| should_infer_collection(n, &conf_include, &conf_exclude))
                .collect();
            let total_collections = filtered_coll_names.len();

            let mut all_schemas: IndexMap<String, CollectionSchema> = IndexMap::new();
            for (index, coll_name) in filtered_coll_names.iter().enumerate() {
                match infer_collection(
                    &client,
                    db_name,
                    coll_name,
                    coll_name,
                    &args,
                    None,
                    Some((index + 1, total_collections)),
                    !quiet_infer,
                )
                .await
                {
                    Ok(schema) => {
                        all_schemas.insert((*coll_name).clone(), schema);
                    }
                    Err(e) => eprintln!("  [warn] skipping {db_name}.{coll_name}: {e:#}"),
                }
            }
            if args.print_json && !args.no_output {
                println!("{}", serde_json::to_string_pretty(&all_schemas)?);
            }
        }
    }

    if let Some(ref conf) = chained_config {
        run_to_pg(
            ToPgArgs {
                collection: None,
                table: None,
                config: Some(conf.clone()),
                output_dir: None,
                schema: None,
            },
            true,
        )?;
        run_report(
            ReportArgs {
                mongo: UriArg { source_uri: None },
                config: Some(conf.clone()),
                collections_dir: None,
                output: None,
                namespace: String::new(),
                post_import: false,
            },
            true,
        )
        .await?;
        print_infer_summary(conf)?;
    }

    if chained_config.is_none() {
        if let Some(output_dir) = args.output_dir.as_deref() {
            eprintln!(
                "Inference completed. Collection schemas and statistics were written under {}.",
                output_dir.display()
            );
            if chained_output_dir.is_some() {
                eprintln!(
                "If you need PostgreSQL DDL from this standalone output, run to-pg separately on the generated collection files."
            );
            }
        }
    }

    Ok(())
}

fn print_infer_summary(conf: &Path) -> Result<()> {
    let c = read_conf(conf)?;
    let project_root = c.base_dir.join(&c.project_dir);
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
        let rows = collect_rows(
            &collections_dir,
            tables_root.is_dir().then_some(tables_root.as_path()),
        )?;
        let score = compute_db_score(&c.project_dir, &rows).score_db;
        let table_count = rows
            .iter()
            .map(|row| row.tables_count().unwrap_or(0))
            .sum::<usize>();
        (score, rows.len(), table_count)
    };

    println!("Inference summary");
    println!("  Score: {:.2}", score);
    println!("  Collections: {}", collection_count);
    println!("  PostgreSQL tables: {}", table_count);
    println!("  Detailed HTML report: {}", report_path.display());
    println!(
        "  Next step: review the generated DDL files under {} and then run `mongo2pg export -c {}`",
        tables_root.display(),
        conf.display()
    );

    Ok(())
}

/// Infer schemas for all user databases on the server (skipping system databases).
///
/// Output files are written as `<output_dir>/<dbname>/<collname>/`.
/// Report generation is handled separately by the `report` command.
async fn infer_all_databases(
    client: &Client,
    args: &InferArgs,
    include: &[String],
    exclude: &[String],
    emit_stats: bool,
) -> Result<()> {
    let all_dbs = client
        .list_database_names()
        .await
        .context("Failed to list databases")?;

    let user_dbs: Vec<String> = all_dbs
        .into_iter()
        .filter(|db| !SYSTEM_DATABASES.contains(&db.as_str()))
        .collect();

    if user_dbs.is_empty() {
        eprintln!("No user databases found on the server.");
        return Ok(());
    }

    eprintln!(
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
                eprintln!(
                    "  [warn] skipping database '{db_name}' (cannot list collections): {e:#}"
                );
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
        let mut db_schemas: IndexMap<String, CollectionSchema> = IndexMap::new();

        for coll_name in coll_names {
            current_collection += 1;
            let db_out_dir = args.output_dir.as_deref().map(|d| d.join(db_name));
            match infer_collection(
                client,
                db_name,
                coll_name,
                coll_name,
                args,
                db_out_dir.as_deref(),
                Some((current_collection, total_collections)),
                emit_stats,
            )
            .await
            {
                Ok(schema) => {
                    db_schemas.insert(coll_name.clone(), schema);
                }
                Err(e) => eprintln!("  [warn] skipping {db_name}.{coll_name}: {e:#}"),
            }
        }

        if args.print_json && !args.no_output && args.output_dir.is_none() {
            println!(
                "{}",
                serde_json::to_string_pretty(&IndexMap::from([(db_name.clone(), &db_schemas)]))?
            );
        }
    }

    Ok(())
}

/// Returns `true` when `$sample` failed with error 292 (sort exceeds memory limit).
/// Maximum time we allow a single sampling query to run on the server.
const SAMPLE_MAX_TIME: Duration = Duration::from_secs(120);

/// Infer the schema for a single collection, print stats to stderr, and optionally write output files.
///
/// `output_name` controls the directory and file names under `output_dir`.
/// `output_dir_override`, when provided, is used instead of `args.output_dir`.
async fn infer_collection(
    client: &Client,
    db_name: &str,
    coll_name: &str,
    output_name: &str,
    args: &InferArgs,
    output_dir_override: Option<&Path>,
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
        eprintln!("{prefix}Inferring {collection_label} ({sample_basis})");
    } else {
        eprintln!("Inferring {collection_label} ({sample_basis})");
    };

    let mut analyzer = Analyzer::new(true);

    // Try $sample; on any error fall back to a sequential find().limit().
    // $sample internally sorts documents, which can fail on Atlas shared tiers
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
        .max_time(SAMPLE_MAX_TIME)
        .await;

    /// Run a `find().limit()` into `analyzer`, logging any error without propagating.
    async fn find_fallback(
        collection: &mongodb::Collection<bson::Document>,
        analyzer: &mut Analyzer,
        sample_size: u64,
        db_name: &str,
        coll_name: &str,
    ) {
        match collection
            .find(doc! {})
            .limit(sample_size as i64)
            .max_time(SAMPLE_MAX_TIME)
            .await
        {
            Err(e) => {
                eprintln!("  [warn] find() fallback also failed for {db_name}.{coll_name}: {e:#}")
            }
            Ok(mut cur) => loop {
                match cur.try_next().await {
                    Ok(Some(d)) => analyzer.process_document(&d),
                    Ok(None) => break,
                    Err(e) => {
                        eprintln!("  [warn] find() cursor error for {db_name}.{coll_name}: {e:#}");
                        break;
                    }
                }
            },
        }
    }

    match sample_result {
        Err(e) => {
            eprintln!(
                "  [warn] $sample failed for {db_name}.{coll_name} \
                 ({e}); falling back to sequential find().limit({sample_size})"
            );
            find_fallback(&collection, &mut analyzer, sample_size, db_name, coll_name).await;
        }
        Ok(mut cursor) => loop {
            match cursor.try_next().await {
                Ok(Some(doc)) => analyzer.process_document(&doc),
                Ok(None) => break,
                Err(e) => {
                    analyzer = Analyzer::new(true);
                    eprintln!(
                        "  [warn] $sample cursor error for {db_name}.{coll_name} \
                             ({e}); falling back to sequential find().limit({sample_size})"
                    );
                    find_fallback(&collection, &mut analyzer, sample_size, db_name, coll_name)
                        .await;
                    break;
                }
            }
        },
    }

    let mut schema = analyzer.finish();
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
        write_collection_files(out_dir, db_name, output_name, &schema, &stats_lines)
            .with_context(|| format!("Failed to write output files for {output_name}"))?;
    }

    Ok(schema)
}

#[derive(Serialize)]
struct MappingColumn {
    source_field: String,
    target_field: String,
    data_type: String,
    nullable: bool,
}

#[derive(Serialize)]
struct PgMapping {
    dbname: String,
    schema_name: String,
    table_name: String,
    columns: Vec<MappingColumn>,
}

#[derive(Serialize)]
struct CollectionMapping {
    collection_name: String,
    dbname: String,
    pg_mapping: PgMapping,
}

fn is_pg_reserved(s: &str) -> bool {
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

fn sanitize_pg_name(name: &str) -> String {
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

fn build_collection_mapping(
    db_name: &str,
    coll_name: &str,
    schema: &CollectionSchema,
) -> Option<CollectionMapping> {
    let ddl = schema_to_ddl(schema, coll_name, None);
    let root = parse_sql(&ddl).into_iter().next()?;
    let columns = root
        .columns
        .into_iter()
        .filter_map(|column| {
            let source_field = if column.name == "id" {
                schema.object.contains_key("_id").then(|| "_id".to_owned())
            } else {
                schema
                    .object
                    .keys()
                    .find(|raw_name| sanitize_pg_name(raw_name) == column.name)
                    .cloned()
            }?;

            Some(MappingColumn {
                source_field,
                target_field: column.name,
                data_type: column.col_type.to_lowercase(),
                nullable: !column.not_null,
            })
        })
        .collect();

    Some(CollectionMapping {
        collection_name: coll_name.to_owned(),
        dbname: db_name.to_owned(),
        pg_mapping: PgMapping {
            dbname: db_name.to_owned(),
            schema_name: root.name.clone(),
            table_name: root.name,
            columns,
        },
    })
}

/// Write `<dir>/<name>/<name>.json`, `<dir>/<name>/<name>.stats.txt`, `<dir>/<name>/<name>.stats.yaml`, and `mapping_<sanitize(name)>.yaml`.
fn write_collection_files(
    base: &Path,
    db_name: &str,
    coll_name: &str,
    schema: &CollectionSchema,
    stats_lines: &[String],
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

    let yaml_stats = stats_to_yaml(schema, Some(schema.count));
    let yaml_path = dir.join(format!("{safe_name}.stats.yaml"));
    std::fs::write(&yaml_path, serde_yaml::to_string(&yaml_stats)?)
        .with_context(|| format!("Failed to write {}", yaml_path.display()))?;

    if let Some(mapping) = build_collection_mapping(db_name, coll_name, schema) {
        let mapping_path = dir.join(format!("mapping_{}.yaml", sanitize_pg_name(coll_name)));
        std::fs::write(&mapping_path, serde_yaml::to_string(&mapping)?)
            .with_context(|| format!("Failed to write {}", mapping_path.display()))?;
    }

    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────────
// `init` subcommand
// ──────────────────────────────────────────────────────────────────────────────

fn run_init(args: InitArgs) -> Result<()> {
    let project_root = args.project_base.join(&args.project_name);

    let dirs = [
        project_root.join("schema").join("tables"),
        project_root.join("source").join("collections"),
        project_root.join("data"),
        project_root.join("config"),
        project_root.join("reports"),
    ];

    for dir in &dirs {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("Failed to create directory {}", dir.display()))?;
    }

    let conf_path = project_root
        .join("config")
        .join(format!("{}.toml", args.project_name));
    let target_database_name = args
        .namespace
        .as_deref()
        .map(|ns| ns.split('.').next().unwrap_or(ns))
        .unwrap_or(&args.project_name);
    let conf_content = format!(
        "[project]\nbase_dir = \"{}\"\nproject_dir = \"{}\"\n\n[source]\nuri = {}\nnamespace = {}\nnumber = 1000\n# percent = 10.0\njsonb = false\n# include = [\"collection_a\", \"collection_b\"]\n# exclude = [\"collection_to_skip\"]\n\n[target]\nuri = {}\ndatabase_name = \"{}\"\n",
        args.project_base.display(),
        args.project_name,
        args.source_uri
            .as_deref()
            .map(|u| format!("\"{}\"", u.replace('"', "\\\"")))
            .unwrap_or_else(|| "\"mongodb://localhost:27017\"".to_owned()),
        args.namespace
            .as_deref()
            .map(|ns| format!("\"{}\"", ns.replace('"', "\\\"")))
            .unwrap_or_else(|| "\"mydb\"".to_owned()),
        args.target_uri
            .as_deref()
            .map(|u| format!("\"{}\"", u.replace('"', "\\\"")))
            .unwrap_or_else(|| {
                "\"postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable\""
                    .to_owned()
            }),
        target_database_name.replace('"', "\\\""),
    );
    std::fs::write(&conf_path, conf_content)
        .with_context(|| format!("Failed to write {}", conf_path.display()))?;

    println!(
        "Project '{}' initialised at {}",
        args.project_name,
        project_root.display()
    );
    for dir in &dirs {
        println!("  {}", dir.display());
    }
    println!("  {}", conf_path.display());
    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────────
// `export` subcommand
// ──────────────────────────────────────────────────────────────────────────────

async fn run_export(args: ExportArgs) -> Result<()> {
    let conf = args
        .config
        .as_ref()
        .ok_or_else(|| anyhow!("Provide -c <config>"))?;

    let c = read_conf(conf)?;
    let conf_include = c.include.clone();
    let conf_exclude = c.exclude.clone();
    let source_uri = args
        .mongo
        .source_uri
        .clone()
        .or(c.source_uri)
        .ok_or_else(|| {
            anyhow!(
                "No SOURCE_URI provided: pass --source-uri or add SOURCE_URI to the config file"
            )
        })?;

    // Use args.namespace if provided, else fall back to config file
    let db_name = args.namespace.clone().or(c.namespace).ok_or_else(|| {
        anyhow!("No NAMESPACE provided: pass --namespace or add NAMESPACE to the config file")
    })?;

    let project_root = c.base_dir.join(&c.project_dir);
    // Use <project_root>/schema/tables/<db_name> for SQL files
    let tables_dir = project_root.join("schema").join("tables").join(&db_name);
    let data_dir = args
        .output_dir
        .clone()
        .unwrap_or_else(|| project_root.join("data"));

    let client_options = ClientOptions::parse(&source_uri)
        .await
        .context("Failed to parse MongoDB SOURCE_URI")?;
    let client = Client::with_options(client_options).context("Failed to create MongoDB client")?;

    // Determine which collections to export
    // Helper: sanitize a name like to_pg::sanitize
    fn sanitize(name: &str) -> String {
        let mut s = name.replace(|c: char| !c.is_ascii_alphanumeric() && c != '_', "_");
        if s.chars().next().map_or(false, |c| c.is_ascii_digit()) {
            s = format!("_{}", s);
        }
        s.to_lowercase()
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
            eprintln!(
                "  warning: SQL schema not found for collection '{coll_name}': expected {}, closest existing file is {}",
                expected_path.display(),
                closest_path.display()
            );
        } else {
            eprintln!(
                "  warning: SQL schema not found for collection '{coll_name}': expected {} – run `to-pg` first",
                expected_path.display()
            );
        }
    }

    let collections: Vec<String> = if let Some(name) = args.collection.clone() {
        // If a specific collection is requested, check for its sanitized .sql file
        let sanitized = sanitize(&name);
        let sql_path = tables_dir.join(format!("{sanitized}.sql"));
        if sql_path.exists() {
            vec![name]
        } else {
            eprintln!(
                "  warning: SQL schema not found: {} – run `to-pg` first",
                sql_path.display()
            );
            Vec::new()
        }
    } else {
        // Get all .sql files and their sanitized names
        let mut sql_files: Vec<(String, String)> = std::fs::read_dir(&tables_dir)
            .with_context(|| format!("Cannot read {}", tables_dir.display()))?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("sql"))
            .filter_map(|e| {
                e.path()
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .map(|s| (s.to_owned(), sanitize(s)))
            })
            .collect();
        sql_files.sort_by(|a, b| a.1.cmp(&b.1));

        // Build a set of sanitized .sql names for fast lookup
        use std::collections::HashSet;
        let sql_set: HashSet<String> = sql_files.iter().map(|(_, s)| s.clone()).collect();

        // Get all collection names from MongoDB
        let mongo_colls = client
            .database(&db_name)
            .list_collection_names()
            .await?
            .into_iter()
            .filter(|coll| !coll.starts_with("system."))
            .filter(|coll| should_infer_collection(coll, &conf_include, &conf_exclude));

        // For each collection, if its sanitized name matches a sanitized .sql file, export it
        let mut matched = Vec::new();
        for coll in mongo_colls {
            let sanitized = sanitize(&coll);
            let sql_path = tables_dir.join(format!("{sanitized}.sql"));
            if sql_set.contains(&sanitized) && sql_path.exists() {
                matched.push(coll);
            } else {
                warn_missing_sql_schema(&coll, &sanitized, &tables_dir, &sql_files);
            }
        }
        matched.sort();
        matched
    };

    if collections.is_empty() {
        eprintln!("No SQL schema files found in {}", tables_dir.display());
        return Ok(());
    }

    let total_collections = collections.len();

    for (index, coll_name) in collections.iter().enumerate() {
        eprintln!(
            "[{}/{}] Exporting {db_name}.{coll_name}",
            index + 1,
            total_collections
        );
        match export_collection(&client, &db_name, coll_name, &tables_dir, &data_dir).await {
            Ok(()) => {}
            Err(e) => eprintln!("  warning: {e}"),
        }
    }

    Ok(())
}

async fn run_import(args: ImportArgs) -> Result<()> {
    let c = read_conf(&args.config)?;
    let conf_include: Vec<String> = c.include.iter().map(|name| sanitize_name(name)).collect();
    let conf_exclude: Vec<String> = c.exclude.iter().map(|name| sanitize_name(name)).collect();
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
    let should_import_collection =
        |name: &str| should_infer_collection(name, &conf_include, &conf_exclude);

    let project_root = c.base_dir.join(&c.project_dir);
    let tables_root = project_root.join("schema").join("tables");
    let tables_dir = if tables_root.join(db_name).is_dir() {
        tables_root.join(db_name)
    } else {
        tables_root.clone()
    };
    let data_root = project_root.join("data");
    let data_db_dir = if data_root.join(db_name).is_dir() {
        data_root.join(db_name)
    } else {
        data_root.clone()
    };

    if !tables_dir.is_dir() {
        return Err(anyhow!(
            "Cannot read SQL tables directory {}",
            tables_dir.display()
        ));
    }
    if !data_db_dir.is_dir() {
        return Err(anyhow!(
            "Cannot read data directory {}",
            data_db_dir.display()
        ));
    }

    let admin_client = connect_pg_client(&target_uri).await?;
    ensure_pg_database(&admin_client, target_database_name).await?;

    let db_target_uri = pg_uri_with_database(&target_uri, target_database_name);
    let mut pg_client = connect_pg_client(&db_target_uri).await?;

    let mut sql_files: Vec<PathBuf> = std::fs::read_dir(&tables_dir)
        .with_context(|| format!("Cannot read {}", tables_dir.display()))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("sql"))
        .filter(|path| {
            requested_collection_dir
                .as_deref()
                .map_or(true, |collection_dir| {
                    path.file_stem().and_then(|stem| stem.to_str()) == Some(collection_dir)
                })
        })
        .filter(|path| {
            path.file_stem()
                .and_then(|stem| stem.to_str())
                .map(&should_import_collection)
                .unwrap_or(false)
        })
        .collect();
    sql_files.sort();

    if sql_files.is_empty() {
        return Err(anyhow!("No SQL files found in {}", tables_dir.display()));
    }

    use std::collections::HashSet;
    let mut allowed_table_names: HashSet<String> = HashSet::new();

    for sql_path in &sql_files {
        let sql = std::fs::read_to_string(sql_path)
            .with_context(|| format!("Failed to read {}", sql_path.display()))?;
        let executable_sql = strip_psql_preamble(&sql);
        if executable_sql.trim().is_empty() {
            continue;
        }
        for table in parse_sql(&executable_sql) {
            allowed_table_names.insert(table.name);
        }
        pg_client
            .batch_execute(&executable_sql)
            .await
            .with_context(|| format!("Failed to execute {}", sql_path.display()))?;
        println!("Created PostgreSQL objects from {}", sql_path.display());
    }

    let mut csv_files: Vec<PathBuf> = std::fs::read_dir(&data_db_dir)
        .with_context(|| format!("Cannot read {}", data_db_dir.display()))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .filter(|path| {
            requested_collection_dir
                .as_deref()
                .map_or(true, |collection_dir| {
                    path.file_name().and_then(|name| name.to_str()) == Some(collection_dir)
                })
        })
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(&should_import_collection)
                .unwrap_or(false)
        })
        .flat_map(|collection_dir| {
            std::fs::read_dir(&collection_dir)
                .into_iter()
                .flat_map(|entries| entries.filter_map(|entry| entry.ok()))
                .map(|entry| entry.path())
                .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("gz"))
                .filter(|path| {
                    path.file_stem()
                        .and_then(|stem| stem.to_str())
                        .and_then(|stem| stem.strip_suffix(".csv"))
                        .map(|table_name| allowed_table_names.contains(table_name))
                        .unwrap_or(false)
                })
                .collect::<Vec<_>>()
        })
        .collect();
    csv_files.sort();

    if csv_files.is_empty() {
        return Err(anyhow!(
            "No .csv.gz files found in {}",
            data_db_dir.display()
        ));
    }

    let transaction = pg_client.transaction().await?;
    transaction
        .batch_execute("SET CONSTRAINTS ALL DEFERRED;")
        .await?;

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
        let table = csv_path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|stem| stem.strip_suffix(".csv"))
            .ok_or_else(|| anyhow!("Cannot derive table name from {}", csv_path.display()))?;
        let truncate_sql = format!(
            "TRUNCATE TABLE {}.{} CASCADE",
            quote_ident(schema),
            quote_ident(table)
        );
        match transaction.batch_execute(&truncate_sql).await {
            Ok(()) => {}
            Err(err) => {
                return Err(anyhow!(
                    "Failed to truncate {}.{}\n{}",
                    schema,
                    table,
                    format_postgres_error(&err)
                ));
            }
        }
    }

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
        let table = csv_path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|stem| stem.strip_suffix(".csv"))
            .ok_or_else(|| anyhow!("Cannot derive table name from {}", csv_path.display()))?;
        let copy_sql = format!(
            "COPY {}.{} FROM STDIN WITH (FORMAT csv, HEADER true)",
            quote_ident(schema),
            quote_ident(table)
        );
        let file = std::fs::File::open(csv_path)
            .with_context(|| format!("Failed to open {}", csv_path.display()))?;
        let mut decoder = GzDecoder::new(file);
        let mut contents = Vec::new();
        std::io::Read::read_to_end(&mut decoder, &mut contents)
            .with_context(|| format!("Failed to decompress {}", csv_path.display()))?;
        let content_text = String::from_utf8_lossy(&contents).into_owned();

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
        sink.as_mut()
            .send(Bytes::from(contents))
            .await
            .with_context(|| format!("Failed to stream CSV data for {}", csv_path.display()))?;
        let rows = match sink.as_mut().finish().await {
            Ok(rows) => rows,
            Err(err) => {
                let line_detail = extract_copy_error_line(&err)
                    .and_then(|line_number| {
                        csv_line_at(&content_text, line_number)
                            .map(|line| format!("CSV line {line_number}: {line}"))
                    })
                    .unwrap_or_default();
                return Err(anyhow!(
                    "Failed to finish COPY for {}.{} from {}\n{}{}{}",
                    schema,
                    table,
                    csv_path.display(),
                    format_postgres_error(&err),
                    if line_detail.is_empty() { "" } else { "\n" },
                    line_detail
                ));
            }
        };
        println!(
            "Imported {rows} row(s) into {}.{} from {}",
            schema,
            table,
            csv_path.display()
        );
    }

    transaction.commit().await?;
    println!("Import completed for database '{target_database_name}'.");

    let post_import_namespace = if namespace_collection.is_none() {
        args.collection
            .as_deref()
            .map(|collection| format!("{db_name}.{collection}"))
            .unwrap_or_else(|| namespace.clone())
    } else {
        namespace.clone()
    };
    write_post_import_report(&args.config, &post_import_namespace, "").await?;

    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────────
// `report` subcommand
// ──────────────────────────────────────────────────────────────────────────────

async fn run_report(args: ReportArgs, quiet: bool) -> Result<()> {
    if args.post_import {
        let conf = args
            .config
            .as_ref()
            .ok_or_else(|| anyhow!("--post-import requires -c <config>"))?;
        write_post_import_report(
            conf,
            &args.namespace,
            args.mongo.source_uri.as_deref().unwrap_or(""),
        )
        .await?;
        return Ok(());
    }

    // Resolve collections dir, cluster label, reports dir and project name
    let (collections_dir, namespace, cluster, reports_dir, project_name) =
        if let Some(ref conf) = args.config {
            let c = read_conf(conf)?;
            let ns = args.namespace.clone();
            let ns = if ns.is_empty() {
                c.namespace.unwrap_or_else(|| c.project_dir.clone())
            } else {
                ns
            };
            let cluster = c
                .source_uri
                .as_deref()
                .map(mongo2pg::report::cluster_from_uri)
                .unwrap_or_default();
            let cols_dir = c
                .base_dir
                .join(&c.project_dir)
                .join("source")
                .join("collections");
            let rep_dir = c.base_dir.join(&c.project_dir).join("reports");
            let proj = c.project_dir.clone();
            (cols_dir, ns, cluster, Some(rep_dir), Some(proj))
        } else {
            let dir = args
                .collections_dir
                .clone()
                .ok_or_else(|| anyhow!("Provide --collections-dir or -c <config>"))?;
            (dir, args.namespace.clone(), String::new(), None, None)
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
            .with_context(|| format!("Failed to create reports dir {}", dir.display()))?;
        dir.join("main.html")
    } else {
        PathBuf::from("main.html")
    };

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

        let db_rows: Vec<(String, Vec<mongo2pg::report::CollectionRow>)> = db_names
            .iter()
            .map(|db_name| {
                let db_dir = collections_dir.join(db_name);
                let tables_dir_opt = tables_root
                    .as_deref()
                    .map(|t| t.join(db_name))
                    .filter(|p| p.is_dir());
                let rows = mongo2pg::report::collect_rows(&db_dir, tables_dir_opt.as_deref())
                    .unwrap_or_default();
                (db_name.clone(), rows)
            })
            .collect();

        let entries: Vec<(&str, &[mongo2pg::report::CollectionRow])> = db_rows
            .iter()
            .map(|(name, rows)| (name.as_str(), rows.as_slice()))
            .collect();

        let proj = project_name.as_deref().unwrap_or("project");
        let html = mongo2pg::report::render_multi_db_html(&entries, &cluster, proj);
        std::fs::write(&output_path, &html)
            .with_context(|| format!("Failed to write {}", output_path.display()))?;
        if !quiet {
            println!("Report written to {}", output_path.display());
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
        let html = render_html(&rows, &namespace, &cluster);
        std::fs::write(&output_path, &html)
            .with_context(|| format!("Failed to write {}", output_path.display()))?;
        if !quiet {
            println!("Report written to {}", output_path.display());
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
                            println!("Schema diagram written to {}", schema_path.display());
                        }
                    }
                }
                Err(e) => eprintln!("Warning: could not generate schema diagram: {e}"),
            }
        }
    }

    Ok(())
}

async fn write_post_import_report(
    conf: &Path,
    namespace_override: &str,
    source_uri_override: &str,
) -> Result<()> {
    let c = read_conf(conf)?;
    let conf_include: Vec<String> = c.include.iter().map(|name| sanitize_name(name)).collect();
    let conf_exclude: Vec<String> = c.exclude.iter().map(|name| sanitize_name(name)).collect();
    let reports_dir = c.base_dir.join(&c.project_dir).join("reports");
    std::fs::create_dir_all(&reports_dir)
        .with_context(|| format!("Failed to create reports dir {}", reports_dir.display()))?;

    let namespace = if namespace_override.is_empty() {
        c.namespace.clone().ok_or_else(|| {
            anyhow!("No NAMESPACE provided: pass --namespace or add NAMESPACE to the config file")
        })?
    } else {
        namespace_override.to_owned()
    };
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

    let collections_dir = c
        .base_dir
        .join(&c.project_dir)
        .join("source")
        .join("collections");
    let schema_tables_root = c
        .base_dir
        .join(&c.project_dir)
        .join("schema")
        .join("tables");
    let output_path = reports_dir.join("post_report.html");

    let rows = build_post_import_rows(
        &source_uri,
        &target_database_name
            .map(|db_name| pg_uri_with_database(target_uri, db_name))
            .unwrap_or_else(|| target_uri.to_owned()),
        &namespace,
        &conf_include,
        &conf_exclude,
        &collections_dir,
        &schema_tables_root,
    )
    .await?;
    let html = render_post_import_html(
        &rows,
        &namespace,
        &mongo2pg::report::cluster_from_uri(&source_uri),
        &mongo2pg::report::cluster_from_uri(
            &target_database_name
                .map(|db_name| pg_uri_with_database(target_uri, db_name))
                .unwrap_or_else(|| target_uri.to_owned()),
        ),
    );
    std::fs::write(&output_path, html)
        .with_context(|| format!("Failed to write {}", output_path.display()))?;
    println!("Post-import report written to {}", output_path.display());

    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────────
// `cluster-report` subcommand
// ──────────────────────────────────────────────────────────────────────────────

fn run_cluster_report(args: ClusterReportArgs) -> Result<()> {
    if args.configs.is_empty() {
        return Err(anyhow!("Provide at least one config path via --configs"));
    }

    let mut db_scores = Vec::new();
    let mut cluster_label = args.cluster_label.clone();

    for conf_path in &args.configs {
        let c = read_conf(conf_path)?;

        // Derive the database label: NAMESPACE from config, or fall back to PROJECT_DIR.
        let db_name = c.namespace.clone().unwrap_or_else(|| c.project_dir.clone());

        // Derive the cluster label from the first config that has a URI.
        if cluster_label.is_empty() {
            if let Some(ref source_uri) = c.source_uri {
                cluster_label = mongo2pg::report::cluster_from_uri(source_uri);
            }
        }

        let collections_dir = c
            .base_dir
            .join(&c.project_dir)
            .join("source")
            .join("collections");

        let rows = collect_rows(&collections_dir, None)
            .with_context(|| format!("Failed to read collections for {db_name}"))?;

        db_scores.push(compute_db_score(&db_name, &rows));
    }

    let html = render_cluster_html(&db_scores, &cluster_label);

    let output_path = args.output.unwrap_or_else(|| PathBuf::from("cluster.html"));
    std::fs::write(&output_path, &html)
        .with_context(|| format!("Failed to write {}", output_path.display()))?;
    println!("Cluster report written to {}", output_path.display());

    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

fn parse_namespace(ns: &str) -> Result<(&str, &str)> {
    let dot = ns
        .find('.')
        .ok_or_else(|| anyhow!("Namespace must be in the form <db>.<collection>, got: {ns}"))?;
    Ok((&ns[..dot], &ns[dot + 1..]))
}

/// Values parsed from a project config file produced by `mongo2pg init`.
struct ConfData {
    base_dir: PathBuf,
    project_dir: String,
    source_uri: Option<String>,
    target_uri: Option<String>,
    target_database_name: Option<String>,
    target_schema: Option<String>,
    namespace: Option<String>,
    number: Option<u64>,
    percent: Option<f64>,
    jsonb: bool,
    include: Vec<String>,
    exclude: Vec<String>,
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
    #[serde(default)]
    include: Vec<String>,
    #[serde(default)]
    exclude: Vec<String>,
}

fn should_infer_collection(name: &str, include: &[String], exclude: &[String]) -> bool {
    if !exclude.is_empty() {
        !exclude.iter().any(|candidate| candidate == name)
    } else if !include.is_empty() {
        include.iter().any(|candidate| candidate == name)
    } else {
        true
    }
}

#[derive(Debug, Deserialize, Default)]
struct TomlTargetSection {
    uri: Option<String>,
    database_name: Option<String>,
    schema: Option<String>,
}

/// Parse a project config file produced by `mongo2pg init`.
fn read_conf(path: &Path) -> Result<ConfData> {
    fn parse_toml_conf(path: &Path, content: &str) -> Result<ConfData> {
        let parsed: TomlProjectConfig = toml::from_str(content)
            .with_context(|| format!("Failed to parse TOML config {}", path.display()))?;
        let source = parsed.source.unwrap_or_default();
        let target = parsed.target.unwrap_or_default();

        Ok(ConfData {
            base_dir: parsed.project.base_dir,
            project_dir: parsed.project.project_dir,
            source_uri: source.uri,
            target_uri: target.uri,
            target_database_name: target.database_name,
            target_schema: target.schema,
            namespace: source.namespace,
            number: source.number,
            percent: source.percent,
            jsonb: source.jsonb.unwrap_or(false),
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
            project_dir,
            source_uri,
            target_uri,
            target_database_name,
            target_schema,
            namespace,
            number,
            percent,
            jsonb,
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

fn sanitize_name(name: &str) -> String {
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

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn split_namespace_scope(namespace: &str) -> (&str, Option<&str>) {
    if let Some((db_name, coll_name)) = namespace.split_once('.') {
        (db_name, Some(coll_name))
    } else {
        (namespace, None)
    }
}

fn extract_search_path(sql: &str) -> Option<String> {
    sql.lines().find_map(|line| {
        let trimmed = line.trim();
        trimmed
            .strip_prefix("SET search_path = ")
            .map(|rest| rest.trim().trim_end_matches(';').trim().to_owned())
    })
}

fn strip_psql_preamble(sql: &str) -> String {
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

fn pg_uri_with_database(uri: &str, database: &str) -> String {
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

fn format_postgres_error(err: &tokio_postgres::Error) -> String {
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

fn extract_copy_error_line(err: &tokio_postgres::Error) -> Option<usize> {
    let context = err.as_db_error()?.where_()?;
    let marker = ", line ";
    let start = context.find(marker)? + marker.len();
    let digits: String = context[start..]
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect();
    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

fn csv_line_at(contents: &str, line_number: usize) -> Option<&str> {
    if line_number == 0 {
        None
    } else {
        contents.lines().nth(line_number - 1)
    }
}

async fn connect_pg_client(target_uri: &str) -> Result<tokio_postgres::Client> {
    let mut tls_builder = native_tls::TlsConnector::builder();
    if matches!(pg_sslmode(target_uri), Some(mode) if mode.eq_ignore_ascii_case("require")) {
        tls_builder.danger_accept_invalid_certs(true);
        tls_builder.danger_accept_invalid_hostnames(true);
    }
    let tls = tls_builder
        .build()
        .with_context(|| "Failed to initialize PostgreSQL TLS connector")?;
    let tls = MakeTlsConnector::new(tls);

    let (pg_client, pg_connection) = tokio_postgres::connect(target_uri, tls)
        .await
        .with_context(|| "Failed to connect to PostgreSQL using TARGET_URI")?;
    tokio::spawn(async move {
        if let Err(err) = pg_connection.await {
            eprintln!("warning: PostgreSQL connection error: {err}");
        }
    });

    Ok(pg_client)
}

async fn ensure_pg_database(pg_client: &tokio_postgres::Client, db_name: &str) -> Result<()> {
    let create_db_sql = format!("CREATE DATABASE {}", quote_ident(db_name));
    match pg_client.batch_execute(&create_db_sql).await {
        Ok(()) => {
            println!("Created PostgreSQL database {}", quote_ident(db_name));
            Ok(())
        }
        Err(err) if err.code() == Some(&tokio_postgres::error::SqlState::DUPLICATE_DATABASE) => {
            Ok(())
        }
        Err(err) => Err(err).with_context(|| format!("Failed to create database {db_name}")),
    }
}

fn pg_sslmode(uri: &str) -> Option<&str> {
    let query = uri.split_once('?')?.1;
    query.split('&').find_map(|part| {
        let (key, value) = part.split_once('=')?;
        if key.eq_ignore_ascii_case("sslmode") {
            Some(value)
        } else {
            None
        }
    })
}

async fn build_post_import_rows(
    source_uri: &str,
    target_uri: &str,
    namespace: &str,
    include: &[String],
    exclude: &[String],
    collections_root: &Path,
    schema_tables_root: &Path,
) -> Result<Vec<PostImportCollectionRow>> {
    #[derive(Clone)]
    enum CountNodeKind {
        Root,
        Object { field_name: String },
        ArrayScalar { field_name: String },
        ArrayObject { field_name: String },
    }

    #[derive(Clone)]
    struct CountNode {
        name: String,
        is_array: bool,
        mongo_count: u64,
        pg_table_name: Option<String>,
        pg_row_count: Option<i64>,
        kind: CountNodeKind,
        children: Vec<CountNode>,
    }

    fn is_null_type(type_name: &str) -> bool {
        matches!(type_name, "Null" | "Undefined")
    }

    fn child_table_name(parent_name: &str, field: &str, pg_schema: Option<&str>) -> String {
        let raw = format!("{parent_name}_{field}");
        if let Some(schema) = pg_schema {
            let prefix = format!("{}{}", sanitize_pg_name(schema), "_");
            raw.strip_prefix(&prefix).map(str::to_owned).unwrap_or(raw)
        } else {
            raw
        }
    }

    fn build_field_nodes(
        parent_table_name: &str,
        fields: &IndexMap<String, FieldSchema>,
        pg_schema: Option<&str>,
        table_counts: &HashMap<String, PostImportTableRow>,
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
                    let table_name =
                        child_table_name(parent_table_name, &sanitize_pg_name(raw_name), pg_schema);
                    let table_ref = table_counts.get(&table_name);
                    nodes.push(CountNode {
                        name: raw_name.to_string(),
                        is_array: false,
                        mongo_count: 0,
                        pg_table_name: table_ref.and_then(|t| {
                            Some(match &t.schema_name {
                                Some(schema) => format!("{}.{}", schema, t.table_name),
                                None => t.table_name.clone(),
                            })
                        }),
                        pg_row_count: table_ref.map(|t| t.row_count),
                        kind: CountNodeKind::Object {
                            field_name: raw_name.to_string(),
                        },
                        children: build_field_nodes(
                            &table_name,
                            sub_fields,
                            pg_schema,
                            table_counts,
                        ),
                    });
                }
                continue;
            }

            if non_null.len() == 1 && non_null[0].0 == "Array" {
                let type_schema = non_null[0].1;
                if let Some(items_field) = &type_schema.array {
                    let table_name =
                        child_table_name(parent_table_name, &sanitize_pg_name(raw_name), pg_schema);
                    let table_ref = table_counts.get(&table_name);
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
                                        sub_fields,
                                        pg_schema,
                                        table_counts,
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
                        pg_table_name: table_ref.and_then(|t| {
                            Some(match &t.schema_name {
                                Some(schema) => format!("{}.{}", schema, t.table_name),
                                None => t.table_name.clone(),
                            })
                        }),
                        pg_row_count: table_ref.map(|t| t.row_count),
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
                        node.mongo_count += 1;
                        count_children(&mut node.children, child_doc);
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
                                node.mongo_count += 1;
                                count_children(&mut node.children, child_doc);
                            }
                        }
                    }
                }
            }
        }
    }

    fn into_post_import_node(node: CountNode) -> PostImportNode {
        PostImportNode {
            name: node.name,
            is_array: node.is_array,
            mongo_count: node.mongo_count,
            pg_table_name: node.pg_table_name,
            pg_row_count: node.pg_row_count,
            children: node
                .children
                .into_iter()
                .map(into_post_import_node)
                .collect(),
        }
    }

    let (db_name, only_collection) = split_namespace_scope(namespace);

    let mongo_client = Client::with_uri_str(source_uri)
        .await
        .with_context(|| "Failed to connect to MongoDB using SOURCE_URI")?;
    let mongo_db = mongo_client.database(db_name);
    let mut collection_names = mongo_db
        .list_collection_names()
        .await
        .with_context(|| format!("Failed to list collections for MongoDB database {db_name}"))?;
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

    let mut tls_builder = native_tls::TlsConnector::builder();
    if matches!(pg_sslmode(target_uri), Some(mode) if mode.eq_ignore_ascii_case("require")) {
        tls_builder.danger_accept_invalid_certs(true);
        tls_builder.danger_accept_invalid_hostnames(true);
    }
    let tls = tls_builder
        .build()
        .with_context(|| "Failed to initialize PostgreSQL TLS connector")?;
    let tls = MakeTlsConnector::new(tls);

    let (pg_client, pg_connection) = tokio_postgres::connect(target_uri, tls)
        .await
        .with_context(|| "Failed to connect to PostgreSQL using TARGET_URI")?;
    tokio::spawn(async move {
        if let Err(err) = pg_connection.await {
            eprintln!("warning: PostgreSQL connection error: {err}");
        }
    });

    let mut rows = Vec::new();
    for coll_name in collection_names {
        let document_count = mongo_db
            .collection::<bson::Document>(&coll_name)
            .count_documents(doc! {})
            .await
            .with_context(|| {
                format!("Failed to count MongoDB documents for {db_name}.{coll_name}")
            })?;

        let sql_path = ddl_dir.join(format!("{}.sql", sanitize_name(&coll_name)));
        let coll_dir = if collections_root.join(db_name).is_dir() {
            collections_root
                .join(db_name)
                .join(coll_name.replace('/', "_"))
        } else {
            collections_root.join(coll_name.replace('/', "_"))
        };
        let schema_path = coll_dir.join(format!("{}.json", coll_name.replace('/', "_")));
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
            let root_table_name = parsed_tables
                .first()
                .map(|table| table.name.clone())
                .unwrap_or_else(|| sanitize_pg_name(&coll_name));
            let mut table_rows = HashMap::new();
            for table in parsed_tables {
                let qualified_name = match &schema_name {
                    Some(schema) => format!("{}.{}", quote_ident(schema), quote_ident(&table.name)),
                    None => quote_ident(&table.name),
                };
                let count_sql = format!("SELECT COUNT(*)::BIGINT FROM {qualified_name}");
                let row = pg_client
                    .query_one(&count_sql, &[])
                    .await
                    .with_context(|| {
                        format!("Failed to count PostgreSQL rows in {qualified_name}")
                    })?;
                let row_count: i64 = row.get(0);
                table_rows.insert(
                    table.name.clone(),
                    PostImportTableRow {
                        schema_name: schema_name.clone(),
                        table_name: table.name,
                        row_count,
                    },
                );
            }
            let root_ref = table_rows.get(&root_table_name);
            let mut root = CountNode {
                name: coll_name.clone(),
                is_array: false,
                mongo_count: document_count,
                pg_table_name: root_ref.and_then(|t| {
                    Some(match &t.schema_name {
                        Some(schema) => format!("{}.{}", schema, t.table_name),
                        None => t.table_name.clone(),
                    })
                }),
                pg_row_count: root_ref.map(|t| t.row_count),
                kind: CountNodeKind::Root,
                children: build_field_nodes(
                    &root_table_name,
                    &schema.object,
                    schema_name.as_deref(),
                    &table_rows,
                ),
            };

            let mut cursor = mongo_db
                .collection::<bson::Document>(&coll_name)
                .find(doc! {})
                .await
                .with_context(|| {
                    format!("Failed to scan MongoDB documents for {db_name}.{coll_name}")
                })?;
            while let Some(doc) = cursor.try_next().await.with_context(|| {
                format!("Failed to iterate MongoDB documents for {db_name}.{coll_name}")
            })? {
                count_children(&mut root.children, &doc);
            }

            into_post_import_node(root)
        } else {
            PostImportNode {
                name: coll_name.clone(),
                is_array: false,
                mongo_count: document_count,
                pg_table_name: None,
                pg_row_count: None,
                children: Vec::new(),
            }
        };

        rows.push(PostImportCollectionRow {
            name: coll_name,
            document_count,
            root,
        });
    }

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::{sanitize_name, should_infer_collection, strip_psql_preamble, TomlProjectConfig};

    #[test]
    fn should_infer_collection_honors_exclude_before_include() {
        let include = vec!["users".to_owned()];
        let exclude = vec!["users".to_owned(), "audit".to_owned()];

        assert!(!should_infer_collection("users", &include, &exclude));
        assert!(should_infer_collection("orders", &include, &exclude));
    }

    #[test]
    fn should_infer_collection_honors_include_when_exclude_is_empty() {
        let include = vec!["users".to_owned(), "orders".to_owned()];
        let exclude = Vec::new();

        assert!(should_infer_collection("users", &include, &exclude));
        assert!(!should_infer_collection("audit", &include, &exclude));
    }

    #[test]
    fn post_import_filters_live_collection_names_using_sanitized_include_exclude() {
        let include = vec!["activity_feed".to_owned(), "security_logs".to_owned()];
        let exclude = Vec::new();
        let live = ["activity-feed", "security_logs", "admin"];

        let kept: Vec<&str> = live
            .into_iter()
            .filter(|name| should_infer_collection(&sanitize_name(name), &include, &exclude))
            .collect();

        assert_eq!(kept, vec!["activity-feed", "security_logs"]);
    }

    #[test]
    fn toml_source_include_and_exclude_are_parsed() {
        let config: TomlProjectConfig = toml::from_str(
            r#"
[project]
base_dir = "/tmp"
project_dir = "demo"

[source]
include = ["users", "orders"]
exclude = ["audit"]
"#,
        )
        .expect("config should parse");

        let source = config.source.expect("source section should exist");
        assert_eq!(source.include, vec!["users", "orders"]);
        assert_eq!(source.exclude, vec!["audit"]);
    }

    #[test]
    fn toml_target_schema_is_parsed() {
        let config: TomlProjectConfig = toml::from_str(
            r#"
[project]
base_dir = "/tmp"
project_dir = "demo"

[target]
schema = "shared_schema"
"#,
        )
        .expect("config should parse");

        let target = config.target.expect("target section should exist");
        assert_eq!(target.schema.as_deref(), Some("shared_schema"));
    }

    #[test]
    fn strip_psql_preamble_removes_drop_create_and_connect() {
        let sql = r#"
DROP DATABASE IF EXISTS "demo";
CREATE DATABASE "demo";
\connect "demo"

CREATE TABLE demo (
    id INTEGER PRIMARY KEY
);
"#;

        let stripped = strip_psql_preamble(sql);

        assert!(!stripped.contains("DROP DATABASE"));
        assert!(!stripped.contains("CREATE DATABASE"));
        assert!(!stripped.contains("\\connect"));
        assert!(stripped.contains("CREATE TABLE demo"));
    }
}
