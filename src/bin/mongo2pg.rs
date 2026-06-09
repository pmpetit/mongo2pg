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
use mongo2pg::analyzer::{
    Analyzer, CollectionSchema, FieldSchema, TypeSchema, TYPE_NULL, TYPE_UNDEFINED,
};
use mongo2pg::checkmd5::{compute_md5_summaries_for_collection, run_check_md5};
use mongo2pg::export::export_collection;
use mongo2pg::report::{
    collect_rows, compute_cluster_score, compute_db_score, render_cluster_html, render_html,
    render_post_import_html, PostImportCollectionRow, PostImportMd5Column,
    PostImportMd5MismatchRow, PostImportMd5Summary, PostImportNode, PostImportTableRow,
    SYSTEM_DATABASES,
};
use mongo2pg::schema_diagram::{load_tables_by_db, parse_sql, render_schema_html};
use mongo2pg::stats::{
    format_stats, stats_to_yaml, InferWarningMinorityYaml, InferWarningTypeYaml, InferWarningYaml,
};
use mongo2pg::to_pg::schema_to_ddl_with_timestamp_fields;
use mongo2pg::util::{
    can_inline_object_fields, flatten_grouped_root_array_object_fields,
    flatten_root_array_object_field, flattened_root_parent_id_column,
    grouped_root_array_object_fields, inline_object_column_names_with_prefix,
    inline_object_leaf_fields_with_prefix, is_pg_reserved, property_filter_entries_for_collection,
    read_conf, sanitize, scalar_type_family, should_infer_collection,
};
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

    /// Compute MongoDB/PostgreSQL MD5 checks during post-import reporting.
    #[arg(
        long = "check-md5",
        action = clap::ArgAction::Set,
        default_value_t = true,
        default_missing_value = "true",
        num_args = 0..=1,
        requires = "post_import"
    )]
    check_md5: bool,

    /// Print one MD5 per row instead of one collection-level aggregated MD5.
    #[arg(long = "noaggregate", action = clap::ArgAction::SetTrue, requires = "check_md5")]
    noaggregate: bool,
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

    let config_timestamp_fields: Vec<String> = if let Some(ref conf) = args.config {
        read_conf(conf)?.timestamp_fields
    } else {
        Vec::new()
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
        let target_schema = args
            .schema
            .as_deref()
            .or(config_target_schema.as_deref())
            .or(Some(table_name));
        let ddl = if let Some(mapping_tables) =
            load_mapping_ddl_tables(json_path.parent().unwrap_or(&collections_dir))?
        {
            render_ddl_from_mapping_tables(&mapping_tables, target_schema)
        } else {
            schema_to_ddl_with_timestamp_fields(
                &schema,
                table_name,
                target_schema,
                &config_timestamp_fields,
            )
        };
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
        conf_target_schema,
        conf_timestamp_fields,
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
            false,
            None,
            Vec::new(),
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
                conf_target_schema.as_deref(),
                &conf_include,
                &conf_exclude,
                &conf_timestamp_fields,
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
                    conf_target_schema.as_deref(),
                    &conf_include,
                    &conf_exclude,
                    &conf_timestamp_fields,
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
                check_md5: true,
                noaggregate: false,
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

#[derive(Debug, Clone, PartialEq)]
struct InferTypeWarning {
    field_path: String,
    dominant_family: String,
    dominant_ratio: f64,
    minority_families: Vec<(String, f64)>,
    observed_types: Vec<InferWarningTypeYaml>,
}

fn warning_examples(type_schema: &TypeSchema) -> Vec<String> {
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

fn collect_infer_type_warnings(schema: &CollectionSchema) -> Vec<InferTypeWarning> {
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
fn collect_nullable_scalar_warnings(schema: &CollectionSchema) -> Vec<InferWarningYaml> {
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

fn collect_identifier_warnings(schema: &CollectionSchema) -> Vec<InferWarningYaml> {
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

fn emit_infer_type_warnings(db_name: &str, coll_name: &str, schema: &CollectionSchema) {
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

            eprintln!(
                "  [warn] source {}.{} field {} mixes incompatible scalar types: dominant {} ({:.1}% of non-null values), minority {}. Normalize source values before import.",
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
            eprintln!(
                "  [warn] source {}.{} field {} uses PostgreSQL keyword '{}'; it will be renamed to {}.",
                db_name,
                coll_name,
                warning.field_path,
                warning.keyword.as_deref().unwrap_or(""),
                warning.renamed_to.as_deref().unwrap_or(""),
            );
        } else {
            eprintln!(
                "  [warn] source {}.{} field {} matches type name '{}'; consider renaming it.",
                db_name,
                coll_name,
                warning.field_path,
                warning.keyword.as_deref().unwrap_or(""),
            );
        }
    }
}

fn infer_warnings_to_yaml(schema: &CollectionSchema) -> Vec<InferWarningYaml> {
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
async fn infer_all_databases(
    client: &Client,
    args: &InferArgs,
    include: &[String],
    exclude: &[String],
    timestamp_fields: &[String],
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
                None,
                include,
                exclude,
                timestamp_fields,
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
    target_schema: Option<&str>,
    include: &[String],
    exclude: &[String],
    timestamp_fields: &[String],
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
            &schema,
            &stats_lines,
            &infer_warnings,
        )
        .with_context(|| format!("Failed to write output files for {output_name}"))?;
    }

    Ok(schema)
}

fn apply_collection_property_filters(
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MappingColumn {
    source_field: String,
    target_field: String,
    data_type: String,
    nullable: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DdlColumnMapping {
    name: String,
    #[serde(rename = "sql_type", alias = "pg_type")]
    sql_type: String,
    nullable: bool,
    primary_key: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DdlForeignKeyMapping {
    from_col: String,
    to_table: String,
    to_col: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DdlTableMapping {
    name: String,
    columns: Vec<DdlColumnMapping>,
    #[serde(default)]
    foreign_keys: Vec<DdlForeignKeyMapping>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DdlEditingGuidance {
    safe_to_edit: Vec<String>,
    notes: Vec<String>,
}

fn default_ddl_editing_guidance() -> DdlEditingGuidance {
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
struct PgMapping {
    dbname: String,
    schema_name: String,
    table_name: String,
    columns: Vec<MappingColumn>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ddl: Option<DdlTableMapping>,
    #[serde(default = "default_ddl_editing_guidance")]
    ddl_editing: DdlEditingGuidance,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CollectionMapping {
    collection_name: String,
    dbname: String,
    pg_mapping: PgMapping,
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

fn ddl_table_mapping_from_table(table: &mongo2pg::schema_diagram::Table) -> DdlTableMapping {
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

fn render_ddl_from_mapping_tables(tables: &[DdlTableMapping], schema_name: Option<&str>) -> String {
    fn rendered_sql_type(sql_type: &str) -> &str {
        if sql_type.eq_ignore_ascii_case("VARCHAR(0)") {
            "TEXT"
        } else {
            sql_type
        }
    }

    fn ordered_tables<'a>(tables: &'a [DdlTableMapping]) -> Vec<&'a DdlTableMapping> {
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

    let mut ddl = String::new();

    if let Some(schema) = schema_name {
        ddl.push_str(&format!(
            "CREATE SCHEMA IF NOT EXISTS {};\nSET search_path = {};\n\n",
            quote_ident(schema),
            quote_ident(schema)
        ));
    }

    for table in ordered_tables(tables) {
        ddl.push_str(&format!("CREATE TABLE {} (\n", table.name));

        let primary_keys = table
            .columns
            .iter()
            .filter(|column| column.primary_key)
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();

        let mut lines = table
            .columns
            .iter()
            .map(|column| {
                let mut line = format!(
                    "    {} {}",
                    column.name,
                    rendered_sql_type(&column.sql_type)
                );
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
            lines.push(format!("    PRIMARY KEY ({})", primary_keys.join(", ")));
        }

        lines.extend(table.foreign_keys.iter().map(|fk| {
            format!(
                "    FOREIGN KEY ({}) REFERENCES {} ({}) DEFERRABLE INITIALLY DEFERRED",
                fk.from_col, fk.to_table, fk.to_col
            )
        }));

        ddl.push_str(&lines.join(",\n"));
        ddl.push_str("\n);\n\n");
    }

    ddl.trim_end().to_owned() + "\n"
}

fn load_mapping_ddl_tables(collection_dir: &Path) -> Result<Option<Vec<DdlTableMapping>>> {
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
        let Some(ddl) = mapping.pg_mapping.ddl else {
            return Ok(None);
        };
        tables.push(ddl);
    }

    Ok(Some(tables))
}

#[cfg_attr(not(test), allow(dead_code))]
fn build_collection_mappings(
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

fn build_collection_mappings_with_timestamp_fields(
    db_name: &str,
    coll_name: &str,
    schema_name: Option<&str>,
    schema: &CollectionSchema,
    timestamp_fields: &[String],
    reserved_table_names: &std::collections::HashSet<String>,
) -> Vec<(String, CollectionMapping)> {
    fn preferred_child_mapping_table_name(parent_name: &str, field: &str) -> String {
        let field = sanitize_pg_name(field);
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
        reserved_table_names: &std::collections::HashSet<String>,
        assigned_table_names: &std::collections::HashSet<String>,
    ) -> String {
        let field = sanitize_pg_name(field);
        let parent_segment = parent_name.rsplit('_').next().unwrap_or(parent_name);
        let mut candidates = Vec::new();
        for candidate in [
            preferred_child_mapping_table_name(parent_name, &field),
            format!("{parent_segment}_{field}"),
            format!("{parent_name}_{field}"),
        ] {
            if !candidates.iter().any(|existing| existing == &candidate) {
                candidates.push(candidate);
            }
        }

        for candidate in candidates {
            if !reserved_table_names.contains(&candidate)
                && !assigned_table_names.contains(&candidate)
            {
                return candidate;
            }
        }

        let mut suffix = 2_usize;
        loop {
            let candidate = format!("{parent_name}_{field}_{suffix}");
            if !reserved_table_names.contains(&candidate)
                && !assigned_table_names.contains(&candidate)
            {
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
            .unwrap_or_else(|| preferred_child_mapping_table_name(parent_name, field))
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
                let old_child = preferred_child_mapping_table_name(old_parent_name, raw_name);
                let new_child = unique_child_mapping_table_name(
                    new_parent_name,
                    raw_name,
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
                let old_child = preferred_child_mapping_table_name(old_parent_name, raw_name);
                let new_child = unique_child_mapping_table_name(
                    new_parent_name,
                    raw_name,
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

        if let Some(raw_name) = fields
            .keys()
            .find(|raw_name| sanitize_pg_name(raw_name) == column_name)
        {
            return Some(raw_name.clone());
        }

        if is_root {
            if let Some(id_object) = fields
                .get("_id")
                .and_then(|field| field.types.get("Object"))
                .and_then(|type_schema| type_schema.object.as_ref())
            {
                if let Some(raw_name) = id_object
                    .keys()
                    .find(|raw_name| sanitize_pg_name(raw_name) == column_name)
                {
                    return Some(raw_name.clone());
                }
            }
        }

        find_nested_source_field(fields, column_name, is_root)
    }

    fn build_mapping_columns(
        table: &mongo2pg::schema_diagram::Table,
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
                if !is_root && column.name == "id" {
                    return None;
                }

                let source_field = find_source_field_for_column(fields, &column.name, is_root)?;
                Some(MappingColumn {
                    source_field,
                    target_field: column.name.clone(),
                    data_type: column.col_type.to_lowercase(),
                    nullable: !column.not_null,
                })
            })
            .collect()
    }

    fn collect_table_mappings(
        db_name: &str,
        schema_name: &str,
        table_name: &str,
        file_stem: &str,
        fields: &IndexMap<String, FieldSchema>,
        is_root: bool,
        emit_current: bool,
        tables_by_name: &HashMap<String, mongo2pg::schema_diagram::Table>,
        resolved_child_table_names: &HashMap<String, String>,
        out: &mut Vec<(String, CollectionMapping)>,
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

        if emit_current {
            if let Some(table) = tables_by_name.get(table_name) {
                let columns = build_mapping_columns(table, fields, is_root);
                if !columns.is_empty() {
                    out.push((
                        file_stem.to_owned(),
                        CollectionMapping {
                            collection_name: file_stem.to_owned(),
                            dbname: db_name.to_owned(),
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
                            if column.name == "key" {
                                Some(MappingColumn {
                                    source_field: "key".to_owned(),
                                    target_field: column.name.clone(),
                                    data_type: column.col_type.to_lowercase(),
                                    nullable: !column.not_null,
                                })
                            } else {
                                find_source_field_for_column(
                                    &group.child_fields,
                                    &column.name,
                                    false,
                                )
                                .map(|source_field| {
                                    MappingColumn {
                                        source_field,
                                        target_field: column.name.clone(),
                                        data_type: column.col_type.to_lowercase(),
                                        nullable: !column.not_null,
                                    }
                                })
                            }
                        })
                        .collect::<Vec<_>>();
                    if !columns.is_empty() {
                        out.push((
                            child_table.clone(),
                            CollectionMapping {
                                collection_name: child_table.clone(),
                                dbname: db_name.to_owned(),
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
                    schema_name,
                    &child_table,
                    &child_table,
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
                    collect_table_mappings(
                        db_name,
                        schema_name,
                        &child_table,
                        &child_table,
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
                                schema_name,
                                &child_table,
                                &child_table,
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
                                    target_field: column.name.clone(),
                                    data_type: column.col_type.to_lowercase(),
                                    nullable: !column.not_null,
                                })
                                .collect::<Vec<_>>();
                            if !columns.is_empty() {
                                out.push((
                                    child_table.clone(),
                                    CollectionMapping {
                                        collection_name: child_table.clone(),
                                        dbname: db_name.to_owned(),
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
                if column.name == "id" {
                    return None;
                }
                let source_field = if column.name == parent_id_col {
                    Some("_id".to_owned())
                } else if column.name == "key" {
                    Some("key".to_owned())
                } else {
                    group
                        .child_fields
                        .keys()
                        .find(|raw_name| sanitize(raw_name) == column.name)
                        .cloned()
                }?;
                Some(MappingColumn {
                    source_field,
                    target_field: column.name.clone(),
                    data_type: column.col_type.to_lowercase(),
                    nullable: !column.not_null,
                })
            })
            .collect::<Vec<_>>();

        let root_file_stem = sanitize(coll_name);
        let mut mappings = vec![(
            root_file_stem.clone(),
            CollectionMapping {
                collection_name: root_file_stem.clone(),
                dbname: db_name.to_owned(),
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
            &mapping_schema_name,
            &root_table_name,
            &root_file_stem,
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
                if column.name == "id" {
                    return None;
                }
                let source_field = if column.name == parent_id_col {
                    Some("_id".to_owned())
                } else {
                    find_source_field_for_column(item_fields, &column.name, false)
                }?;
                Some(MappingColumn {
                    source_field,
                    target_field: column.name.clone(),
                    data_type: column.col_type.to_lowercase(),
                    nullable: !column.not_null,
                })
            })
            .collect::<Vec<_>>();

        let root_file_stem = sanitize_pg_name(coll_name);
        let mut mappings = vec![(
            root_file_stem.clone(),
            CollectionMapping {
                collection_name: root_file_stem.clone(),
                dbname: db_name.to_owned(),
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
            &mapping_schema_name,
            &root_table_name,
            &root_file_stem,
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
        &mapping_schema_name,
        &root_table_name,
        &root_file_stem,
        &schema.object,
        true,
        true,
        &tables_by_name,
        &resolved_child_table_names,
        &mut mappings,
    );
    mappings
}

fn load_reserved_mapping_table_names(
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

/// Write `<dir>/<name>/<name>.json`, `<dir>/<name>/<name>.stats.txt`, `<dir>/<name>/<name>.stats.yaml`, and one `mapping_<table>.yaml` per generated table.
fn write_collection_files(
    base: &Path,
    db_name: &str,
    coll_name: &str,
    target_schema: Option<&str>,
    timestamp_fields: &[String],
    schema: &CollectionSchema,
    stats_lines: &[String],
    infer_warnings: &[InferWarningYaml],
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

    let yaml_stats = stats_to_yaml(schema, Some(schema.count), infer_warnings);
    let yaml_path = dir.join(format!("{safe_name}.stats.yaml"));
    std::fs::write(&yaml_path, serde_yaml::to_string(&yaml_stats)?)
        .with_context(|| format!("Failed to write {}", yaml_path.display()))?;

    let reserved_table_names = load_reserved_mapping_table_names(base, &dir)?;

    let mappings = build_collection_mappings_with_timestamp_fields(
        db_name,
        coll_name,
        target_schema,
        schema,
        timestamp_fields,
        &reserved_table_names,
    );
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
        "[project]\n title = \"{}\"\nbase_dir = \"{}\"\nproject_dir = \"{}\"\n\n[source]\nuri = {}\nnamespace = {}\nnumber = 1000\n# percent = 10.0\njsonb = false\n# include = [\"collection_a\", \"collection_b\"]\n# exclude = [\"collection_to_skip\"]\ndatetime_field = [\"created_at\", \"last_update\", \"updated_at\", \"*_date\", \"date\"]\n\n[target]\nuri = {}\ndatabase_name = \"{}\"\n",
        "Mongo2Pg Project migration",
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
    let collections_dir = resolve_collections_dir(&project_root, &db_name);
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
        match export_collection(
            &client,
            &db_name,
            coll_name,
            &tables_dir,
            &collections_dir,
            &data_dir,
        )
        .await
        {
            Ok(()) => {}
            Err(e) => eprintln!("  warning: {e}"),
        }
    }

    Ok(())
}

fn resolve_collections_dir(project_root: &Path, db_name: &str) -> std::path::PathBuf {
    let collections_root = project_root.join("source").join("collections");
    if collections_root.join(db_name).is_dir() {
        collections_root.join(db_name)
    } else {
        collections_root
    }
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
    write_post_import_report(&args.config, &post_import_namespace, "", true).await?;

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

        if args.check_md5 {
            let (_, only_collection) = split_namespace_scope(&namespace);
            let collection = only_collection.ok_or_else(|| {
                anyhow!("--check-md5 with --post-import requires a single collection namespace like <db>.<collection>")
            })?;
            run_check_md5(
                collection.to_owned(),
                Some(conf.to_path_buf()),
                !args.noaggregate,
            )
            .await?;
        }

        return Ok(());
    }

    // Resolve collections dir, cluster label, reports dir and project name
    let (collections_dir, namespace, cluster, reports_dir, project_name, ftitle) =
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
                let tables_dir_opt: Option<PathBuf> = tables_root
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
        let title = ftitle.as_deref().unwrap_or("mongo2pg Report");
        let html = mongo2pg::report::render_multi_db_html(&entries, &cluster, proj, title);
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
        let title = ftitle.as_deref().unwrap_or("mongo2pg Report");
        let html = render_html(&rows, &namespace, &cluster, &title);
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
    include_md5: bool,
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
        conf,
        &source_uri,
        &target_database_name
            .map(|db_name| pg_uri_with_database(target_uri, db_name))
            .unwrap_or_else(|| target_uri.to_owned()),
        &namespace,
        &conf_include,
        &conf_exclude,
        &collections_dir,
        &schema_tables_root,
        include_md5,
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

fn normalize_pg_identifier(name: &str) -> String {
    let trimmed = name.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        trimmed[1..trimmed.len() - 1].replace("\"\"", "\"")
    } else {
        trimmed.to_owned()
    }
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
    config_path: &Path,
    source_uri: &str,
    target_uri: &str,
    namespace: &str,
    include: &[String],
    exclude: &[String],
    collections_root: &Path,
    schema_tables_root: &Path,
    include_md5: bool,
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
        md5_summary: Option<PostImportMd5Summary>,
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

    fn build_field_nodes(
        parent_table_name: &str,
        fields: &IndexMap<String, FieldSchema>,
        pg_schema: Option<&str>,
        table_counts: &HashMap<String, PostImportTableRow>,
        md5_summaries: &HashMap<String, PostImportMd5Summary>,
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
                        md5_summary: md5_summaries.get(&table_name).cloned(),
                        kind: CountNodeKind::Object {
                            field_name: raw_name.to_string(),
                        },
                        children: build_field_nodes(
                            &table_name,
                            sub_fields,
                            pg_schema,
                            table_counts,
                            md5_summaries,
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
                                        md5_summaries,
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
                        md5_summary: md5_summaries.get(&table_name).cloned(),
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
            md5_summary: node.md5_summary,
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

    let total_collections = collection_names.len();
    let mut rows = Vec::new();
    for (index, coll_name) in collection_names.into_iter().enumerate() {
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
            let md5_summaries = if include_md5 {
                println!(
                    "[{}/{}] Computing hash (md5) for {}.{}",
                    index + 1,
                    total_collections,
                    db_name,
                    coll_name
                );
                match compute_md5_summaries_for_collection(&coll_name, config_path).await {
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
                                            target_field: column.target_field,
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
                        eprintln!(
                            "warning: failed to compute md5 summary for {}.{}: {}",
                            db_name, coll_name, err
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
                pg_table_name: root_ref.and_then(|t| {
                    Some(match &t.schema_name {
                        Some(schema) => format!("{}.{}", schema, t.table_name),
                        None => t.table_name.clone(),
                    })
                }),
                pg_row_count: root_ref.map(|t| t.row_count),
                md5_summary: md5_summaries.get(&root_table_name).cloned(),
                kind: CountNodeKind::Root,
                children: build_field_nodes(
                    &root_table_name,
                    &schema.object,
                    schema_name.as_deref(),
                    &table_rows,
                    &md5_summaries,
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
                md5_summary: None,
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
    use super::{
        apply_collection_property_filters, build_collection_mappings,
        build_collection_mappings_with_timestamp_fields, collect_infer_type_warnings,
        collect_nullable_scalar_warnings, render_ddl_from_mapping_tables, resolve_collections_dir,
        sanitize_name, should_infer_collection, strip_psql_preamble,
    };
    use bson::doc;
    use mongo2pg::{analyzer::Analyzer};
    use serde::Deserialize;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(Debug, Deserialize)]
    struct TestTomlProjectConfig {
        source: Option<TestTomlSourceSection>,
        target: Option<TestTomlTargetSection>,
    }

    #[derive(Debug, Deserialize)]
    struct TestTomlSourceSection {
        #[serde(default)]
        include: Vec<String>,
        #[serde(default)]
        exclude: Vec<String>,
    }

    #[derive(Debug, Deserialize)]
    struct TestTomlTargetSection {
        schema: Option<String>,
    }

    #[test]
    fn should_infer_collection_honors_exclude_before_include() {
        let include = vec!["users".to_owned()];
        let exclude = vec!["users".to_owned(), "audit".to_owned()];

        assert!(!should_infer_collection("users", &include, &exclude));
        assert!(!should_infer_collection("orders", &include, &exclude));
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
    fn apply_collection_property_filters_excludes_top_level_property_for_matching_collection() {
        let docs = vec![doc! {
            "_id": 1,
            "name": "project-a",
            "archived_services": [{"name": "svc-a"}],
            "tags": ["critical"]
        }];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let mut schema = analyzer.finish();

        apply_collection_property_filters(
            &mut schema,
            "projects",
            &[],
            &["projects.archived_services".to_owned()],
        );

        assert!(schema.object.contains_key("_id"));
        assert!(schema.object.contains_key("name"));
        assert!(schema.object.contains_key("tags"));
        assert!(!schema.object.contains_key("archived_services"));
    }

    #[test]
    fn apply_collection_property_filters_includes_only_requested_top_level_properties() {
        let docs = vec![doc! {
            "_id": 1,
            "name": "project-a",
            "archived_services": [{"name": "svc-a"}],
            "tags": ["critical"]
        }];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let mut schema = analyzer.finish();

        apply_collection_property_filters(
            &mut schema,
            "projects",
            &["projects.archived_services".to_owned()],
            &[],
        );

        assert_eq!(schema.object.len(), 2);
        assert!(schema.object.contains_key("_id"));
        assert!(schema.object.contains_key("archived_services"));
    }

    #[test]
    fn toml_source_include_and_exclude_are_parsed() {
        let config: TestTomlProjectConfig = toml::from_str(
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
        let config: TestTomlProjectConfig = toml::from_str(
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

    #[test]
    fn build_collection_mappings_includes_nested_child_tables() {
        let docs = vec![doc! {
            "_id": bson::oid::ObjectId::new(),
            "name": "advisor",
            "advices": [{
                "advice": "oversized",
                "object_id": "svc-1",
                "object_type": "SERVICE",
                "earnings": {
                    "monthly_gain": 12.5_f64
                }
            }]
        }];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let mappings = build_collection_mappings("dbapi", "advisors", None, &schema);
        let stems = mappings
            .iter()
            .map(|(stem, _)| stem.clone())
            .collect::<Vec<_>>();

        assert!(stems.contains(&"advisors".to_owned()));
        assert!(stems.contains(&"advices".to_owned()));
        assert!(stems.contains(&"earnings".to_owned()));

        let advices_mapping = mappings
            .iter()
            .find(|(stem, _)| stem == "advices")
            .map(|(_, mapping)| mapping)
            .expect("advices mapping should exist");
        assert!(advices_mapping.pg_mapping.ddl.is_some());
        let advice_columns = advices_mapping
            .pg_mapping
            .columns
            .iter()
            .map(|column| column.target_field.as_str())
            .collect::<Vec<_>>();
        assert!(advice_columns.contains(&"advice"));
        assert!(advice_columns.contains(&"object_id"));
        assert!(!advice_columns.contains(&"advisors_id"));

        let earnings_mapping = mappings
            .iter()
            .find(|(stem, _)| stem == "earnings")
            .map(|(_, mapping)| mapping)
            .expect("earnings mapping should exist");
        let earnings_columns = earnings_mapping
            .pg_mapping
            .columns
            .iter()
            .map(|column| column.target_field.as_str())
            .collect::<Vec<_>>();
        assert!(earnings_columns.contains(&"monthly_gain"));
    }

    #[test]
    fn build_collection_mappings_keeps_prefix_when_short_name_is_reserved_elsewhere() {
        let docs = vec![doc! {
            "_id": "project-1",
            "team": {
                "code": "ops",
                "members": [{
                    "ldap": "alice",
                    "roles": ["admin"]
                }]
            }
        }];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();
        let reserved_table_names = std::collections::HashSet::from(["roles".to_owned()]);

        let mappings = build_collection_mappings_with_timestamp_fields(
            "dbapi",
            "projects",
            None,
            &schema,
            &[],
            &reserved_table_names,
        );
        let stems = mappings
            .iter()
            .map(|(stem, _)| stem.clone())
            .collect::<Vec<_>>();

        assert!(stems.contains(&"team".to_owned()));
        assert!(stems.contains(&"members".to_owned()));
        assert!(!stems.contains(&"roles".to_owned()));

        let roles_mapping = mappings
            .iter()
            .find(|(stem, _)| stem == "team")
            .map(|(_, mapping)| mapping)
            .expect("team mapping should exist");
        assert_eq!(roles_mapping.pg_mapping.table_name, "team");
        let ddl = roles_mapping
            .pg_mapping
            .ddl
            .as_ref()
            .expect("team ddl should exist");
        assert_eq!(ddl.name, "team");
    }

    #[test]
    fn build_collection_mappings_promotes_root_array_objects_into_single_table() {
        let docs = vec![doc! {
            "_id": "engine-1",
            "versions": [
                {
                    "major_version": "1",
                    "eol_date": bson::DateTime::now(),
                    "grace_date": bson::DateTime::now()
                }
            ]
        }];
        let mut analyzer = Analyzer::new(false);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let mappings = build_collection_mappings("dbapi", "engine", None, &schema);
        let stems = mappings
            .iter()
            .map(|(stem, _)| stem.clone())
            .collect::<Vec<_>>();

        assert_eq!(stems, vec!["engine".to_owned()]);

        let engine_mapping = mappings
            .iter()
            .find(|(stem, _)| stem == "engine")
            .map(|(_, mapping)| mapping)
            .expect("engine mapping should exist");
        let engine_columns = engine_mapping
            .pg_mapping
            .columns
            .iter()
            .map(|column| (column.source_field.as_str(), column.target_field.as_str()))
            .collect::<Vec<_>>();

        assert!(engine_columns.contains(&("_id", "engine_id")));
        assert!(engine_columns.contains(&("major_version", "major_version")));
        assert!(engine_columns.contains(&("eol_date", "eol_date")));
        assert!(engine_columns.contains(&("grace_date", "grace_date")));
        assert!(engine_mapping.pg_mapping.ddl.is_some());
    }

    #[test]
    fn build_collection_mappings_includes_scalar_array_child_tables() {
        let docs = vec![doc! {
            "_id": "sizing-1",
            "available_versions": ["1", "2"]
        }];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let mappings = build_collection_mappings("dbapi", "sizings", None, &schema);
        let versions_mapping = mappings
            .iter()
            .find(|(stem, _)| stem == "sizings")
            .map(|(_, mapping)| mapping)
            .expect("scalar-array child mapping should exist");

        assert!(versions_mapping.pg_mapping.columns.len() == 2);
        assert_eq!(versions_mapping.pg_mapping.schema_name, "sizings");
    }

    #[test]
    fn build_collection_mappings_groups_same_shape_root_arrays_into_one_keyed_table() {
        let docs = vec![doc! {
            "_id": "community-1",
            "dev": [{
                "available_localizations": ["eu-west-1"],
                "provider": "aiven",
                "cloud": "gcp",
                "network_exposition": "private_platform"
            }],
            "prod": [{
                "available_localizations": ["eu-west-2"],
                "provider": "atlas",
                "cloud": "azure",
                "network_exposition": "public"
            }]
        }];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let mappings = build_collection_mappings("dbapi", "communities", None, &schema);

        let communities = mappings
            .iter()
            .find(|(stem, _)| stem == "communities")
            .map(|(_, mapping)| mapping)
            .expect("grouped mapping should exist");
        let columns = communities
            .pg_mapping
            .columns
            .iter()
            .map(|column| (column.source_field.as_str(), column.target_field.as_str()))
            .collect::<Vec<_>>();

        assert!(columns.contains(&("_id", "communities_id")));
        assert!(columns.contains(&("key", "key")));
        assert!(columns.contains(&("provider", "provider")));
        assert!(!mappings.iter().any(|(stem, _)| stem == "communities_dev"));
        assert!(!mappings.iter().any(|(stem, _)| stem == "communities_prod"));
    }

    #[test]
    fn build_collection_mappings_flattens_scalar_only_object_with_siblings() {
        let docs = vec![doc! {
            "_id": "project-1",
            "environment": "T",
            "providers": [{
                "namespace": "fras-t-dba-176c358",
                "namespace_id": "fras-t-dba-176c358",
                "provider": "aiven",
                "metadata": {
                    "creation_date": "2025-08-11T00:00:00Z",
                    "status": "created"
                }
            }]
        }];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let mappings = build_collection_mappings("dbapi", "projects", None, &schema);

        let providers = mappings
            .iter()
            .find(|(stem, _)| stem == "providers")
            .map(|(_, mapping)| mapping)
            .expect("providers mapping should exist");
        let columns = providers
            .pg_mapping
            .columns
            .iter()
            .map(|column| (column.source_field.as_str(), column.target_field.as_str()))
            .collect::<Vec<_>>();

        assert!(columns.contains(&("namespace", "namespace")));
        assert!(columns.contains(&("namespace_id", "namespace_id")));
        assert!(columns.contains(&("provider", "provider")));

        let metadata = mappings
            .iter()
            .find(|(stem, _)| stem == "metadata")
            .map(|(_, mapping)| mapping)
            .expect("metadata mapping should exist");
        let metadata_columns = metadata
            .pg_mapping
            .columns
            .iter()
            .map(|column| (column.source_field.as_str(), column.target_field.as_str()))
            .collect::<Vec<_>>();
        assert!(metadata_columns.contains(&("creation_date", "creation_date")));
        assert!(metadata_columns.contains(&("status", "status")));
    }

    #[test]
    fn build_collection_mappings_uses_configured_target_schema() {
        let docs = vec![doc! {
            "_id": "sizing-1",
            "available_versions": ["1", "2"]
        }];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let mappings =
            build_collection_mappings("dbapi", "sizings", Some("shared_schema"), &schema);

        assert!(mappings
            .iter()
            .all(|(_, mapping)| mapping.pg_mapping.schema_name == "shared_schema"));
    }

    #[test]
    fn build_collection_mappings_forces_configured_timestamp_fields() {
        let docs = vec![
            doc! { "_id": 1_i32, "last_update": 1650468505_i64 },
            doc! { "_id": 2_i32, "last_update": "2022-08-17T07:57:18Z" },
        ];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();
        let patterns = vec!["last_update".to_owned(), "*_date".to_owned()];

        let mappings = build_collection_mappings_with_timestamp_fields(
            "dbapi",
            "scheduling_jobs",
            Some("dbapi"),
            &schema,
            &patterns,
            &std::collections::HashSet::new(),
        );

        let root_mapping = mappings
            .iter()
            .find(|(stem, _)| stem == "scheduling_jobs")
            .map(|(_, mapping)| mapping)
            .expect("root mapping should exist");

        assert!(root_mapping
            .pg_mapping
            .columns
            .iter()
            .any(|column| column.source_field == "last_update"
                && column.data_type == "timestamp with time zone"));
        assert!(root_mapping
            .pg_mapping
            .ddl
            .as_ref()
            .expect("ddl mapping should exist")
            .columns
            .iter()
            .any(|column| column.name == "last_update"
                && column.sql_type == "TIMESTAMP WITH TIME ZONE"));
    }

    #[test]
    fn render_ddl_from_mapping_tables_uses_editable_mapping_metadata() {
        let sql = render_ddl_from_mapping_tables(
            &[super::DdlTableMapping {
                name: "demo".to_owned(),
                columns: vec![
                    super::DdlColumnMapping {
                        name: "id".to_owned(),
                        sql_type: "BIGSERIAL".to_owned(),
                        nullable: false,
                        primary_key: true,
                    },
                    super::DdlColumnMapping {
                        name: "ram".to_owned(),
                        sql_type: "NUMERIC".to_owned(),
                        nullable: false,
                        primary_key: false,
                    },
                ],
                foreign_keys: Vec::new(),
            }],
            Some("dbapi"),
        );

        assert!(sql.contains("CREATE SCHEMA IF NOT EXISTS \"dbapi\";"));
        assert!(sql.contains("id BIGSERIAL PRIMARY KEY"));
        assert!(sql.contains("ram NUMERIC NOT NULL"));
    }

    #[test]
    fn render_ddl_from_mapping_tables_replaces_varchar_zero_with_text() {
        let sql = render_ddl_from_mapping_tables(
            &[super::DdlTableMapping {
                name: "demo".to_owned(),
                columns: vec![super::DdlColumnMapping {
                    name: "dbapi_service_id".to_owned(),
                    sql_type: "VARCHAR(0)".to_owned(),
                    nullable: true,
                    primary_key: false,
                }],
                foreign_keys: Vec::new(),
            }],
            None,
        );

        assert!(sql.contains("dbapi_service_id TEXT"));
        assert!(!sql.contains("VARCHAR(0)"));
    }

    #[test]
    fn render_ddl_from_mapping_tables_orders_parents_before_children() {
        let sql = render_ddl_from_mapping_tables(
            &[
                super::DdlTableMapping {
                    name: "child".to_owned(),
                    columns: vec![
                        super::DdlColumnMapping {
                            name: "id".to_owned(),
                            sql_type: "BIGSERIAL".to_owned(),
                            nullable: false,
                            primary_key: true,
                        },
                        super::DdlColumnMapping {
                            name: "parent_id".to_owned(),
                            sql_type: "BIGINT".to_owned(),
                            nullable: false,
                            primary_key: false,
                        },
                    ],
                    foreign_keys: vec![super::DdlForeignKeyMapping {
                        from_col: "parent_id".to_owned(),
                        to_table: "parent".to_owned(),
                        to_col: "id".to_owned(),
                    }],
                },
                super::DdlTableMapping {
                    name: "parent".to_owned(),
                    columns: vec![super::DdlColumnMapping {
                        name: "id".to_owned(),
                        sql_type: "BIGSERIAL".to_owned(),
                        nullable: false,
                        primary_key: true,
                    }],
                    foreign_keys: Vec::new(),
                },
            ],
            None,
        );

        let parent_pos = sql
            .find("CREATE TABLE parent (")
            .expect("parent table missing");
        let child_pos = sql
            .find("CREATE TABLE child (")
            .expect("child table missing");
        assert!(
            parent_pos < child_pos,
            "parent table should be rendered before child table"
        );
    }

    #[test]
    fn extract_search_path_strips_identifier_quotes() {
        let sql = "SET search_path = \"dbapi\";";

        assert_eq!(super::extract_search_path(sql).as_deref(), Some("dbapi"));
    }

    #[test]
    fn resolve_collections_dir_falls_back_to_flat_layout() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        let project_root = std::env::temp_dir().join(format!("mongo2pg-export-test-{unique}"));
        let collections_root = project_root.join("source").join("collections");
        std::fs::create_dir_all(&collections_root)
            .expect("flat collections directory should be created");

        let resolved = resolve_collections_dir(&project_root, "dbapi");

        assert_eq!(resolved, collections_root);

        std::fs::remove_dir_all(&project_root).expect("temp project root should be removed");
    }

    #[test]
    fn run_init_writes_default_datetime_field_patterns() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        let project_base = std::env::temp_dir().join(format!("mongo2pg-init-test-{unique}"));

        super::run_init(super::InitArgs {
            project_base: project_base.clone(),
            project_name: "dbapi".to_owned(),
            source_uri: None,
            target_uri: None,
            namespace: Some("dbapi".to_owned()),
        })
        .expect("init should succeed");

        let conf_path = project_base.join("dbapi").join("config").join("dbapi.toml");
        let content = std::fs::read_to_string(&conf_path).expect("config file should be readable");

        assert!(content.contains(
            "datetime_field = [\"created_at\", \"last_update\", \"updated_at\", \"*_date\", \"date\"]"
        ));

        std::fs::remove_dir_all(&project_base).expect("temp project base should be removed");
    }

    #[test]
    fn collect_infer_type_warnings_flags_minor_incompatible_scalar_types() {
        let docs = vec![
            doc! { "advices": [{ "earnings": { "monthly_gain": 12.5_f64 } }] },
            doc! { "advices": [{ "earnings": { "monthly_gain": 7_i32 } }] },
            doc! { "advices": [{ "earnings": { "monthly_gain": "N/A" } }] },
            doc! { "advices": [{ "earnings": { "monthly_gain": bson::Bson::Null } }] },
        ];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let warnings = collect_infer_type_warnings(&schema);

        assert!(warnings.iter().any(|warning| {
            warning.field_path == "advices[].earnings.monthly_gain"
                && warning.dominant_family == "numeric"
                && warning
                    .minority_families
                    .iter()
                    .any(|(family, _)| family == "string")
                && warning.observed_types.iter().any(|observed_type| {
                    observed_type.type_name == "String"
                        && observed_type
                            .examples
                            .iter()
                            .any(|example| example == "\"N/A\"")
                })
        }));
    }

    #[test]
    fn collect_infer_type_warnings_ignores_compatible_numeric_mix() {
        let docs = vec![
            doc! { "value": 12.5_f64 },
            doc! { "value": 7_i32 },
            doc! { "value": bson::Bson::Null },
        ];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let warnings = collect_infer_type_warnings(&schema);

        assert!(warnings.is_empty());
    }

    #[test]
    fn collect_nullable_scalar_warnings_detects_nullable_boolean() {
        let docs = vec![
            doc! { "enabled": true },
            doc! { "enabled": bson::Bson::Null },
            doc! {},
        ];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let warnings = collect_nullable_scalar_warnings(&schema);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].kind, "nullable_scalar");
        assert_eq!(warnings[0].field_path, "enabled");
        assert_eq!(warnings[0].dominant_family, "Boolean");
    }

    #[test]
    fn collect_nullable_scalar_warnings_detects_nullable_in_nested_objects() {
        let docs = vec![
            doc! { "config": { "enabled": true } },
            doc! { "config": { "enabled": bson::Bson::Null } },
            doc! { "config": {} },
        ];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let warnings = collect_nullable_scalar_warnings(&schema);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].kind, "nullable_scalar");
        assert_eq!(warnings[0].field_path, "config.enabled");
        assert_eq!(warnings[0].dominant_family, "Boolean");
    }

    #[test]
    fn collect_nullable_scalar_warnings_detects_nullable_in_array_items() {
        let docs = vec![
            doc! { "items": [{ "enabled": true }] },
            doc! { "items": [{ "enabled": bson::Bson::Null }] },
            doc! { "items": [] },
        ];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let warnings = collect_nullable_scalar_warnings(&schema);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].kind, "nullable_scalar");
        assert_eq!(warnings[0].field_path, "items[].enabled");
        assert_eq!(warnings[0].dominant_family, "Boolean");
    }

    use crate::{ExportArgs, InferArgs, UriArg, ImportArgs};
    use std::path::PathBuf;
    use tokio_postgres::NoTls;

    fn create_default_init_args(
        project_base: PathBuf,
        project_name: String,
        source_uri: Option<String>,
        target_uri: Option<String>,
        ns: String,
    ) -> super::InitArgs {
        super::InitArgs {
            project_base,
            project_name,
            source_uri,
            target_uri,
            namespace: Some(ns),
        }
    }
    fn create_default_infer_args(config: PathBuf) -> InferArgs {
        InferArgs {
            mongo: UriArg {
                source_uri: None,
            },
            namespace: None,
            number: Some(500),
            percent: None, // Set to None because it conflicts with `number`
            jsonb: false,
            print_json: false,
            no_output: false,
            output_dir: None,
            config: Some(config), // Set to Some because it conflicts with `output_dir`
        }
    }
    fn create_default_export_args(config: PathBuf) -> ExportArgs {
        ExportArgs {
            mongo:
                UriArg {
                    source_uri: None,
                },
            collection: None,
            namespace: None,
            output_dir: None,
            config: Some(config),
        }
    }
    fn create_default_import_args(config: PathBuf) -> super::ImportArgs {
        ImportArgs {
            collection: None,
            namespace: None,
            config: config,
        }
    }


    use testcontainers_modules::{mongo, postgres, testcontainers::runners::AsyncRunner};
    use crate::{run_export, run_infer, run_init, run_import, run_check_md5};
    use tempfile::TempDir; // Import the TempDir type
    use chrono::{DateTime, Utc,TimeZone};
    use std::fs;
    use indoc::indoc;

    // Data Structures
    #[derive(serde::Serialize)]
    struct Employee {
        id: i32,
        name: String,
        hire_date: DateTime<Utc>,
    }

    #[tokio::test]
    async fn test_mongo_to_pg_data_flow() -> Result<(), Box<dyn std::error::Error>> {
        // --- Container Startup (remains the same) ---

        let temp_dir = TempDir::new()?;

        // 2. Build your paths relative to the new temporary directory.
        // The `join` method is the correct and safe way to append path segments.
        let table_dir = temp_dir.path().join("schema/tables/test_db");
        let collections_dir = temp_dir.path().join("source/collections/employees");
        let data_dir = temp_dir.path().join("data/test_db/employees");

        // 3. You can now create these directories and any files you need.
        // For example, using std::fs:
        std::fs::create_dir_all(&table_dir)?;
        std::fs::create_dir_all(&collections_dir)?;
        std::fs::create_dir_all(&data_dir)?;

        let (pg_container, mongo_container) = tokio::join!(
            postgres::Postgres::default().start(),
            mongo::Mongo::default().start()
        );
        let pg_container = pg_container?;
        let mongo_container = mongo_container?;

        // --- Establish connections to both databases ---
        // PostgreSQL Client
        let pg_host_port = pg_container.get_host_port_ipv4(5432).await?;
        let pg_connection_string = format!(
            "postgres://postgres:postgres@localhost:{}/postgres?sslmode=disable",
            pg_host_port
        );

        // MongoDB Client
        let db_mongo = "test_db";
        let mongo_host_port = mongo_container.get_host_port_ipv4(27017).await?;
        let mongo_uri = format!("mongodb://localhost:{}", mongo_host_port);
        let mongo_client = mongodb::Client::with_uri_str(&mongo_uri).await?;
        let mongo_db = mongo_client.database(db_mongo);
        let collection = mongo_db.collection::<bson::Document>("employees");

        let new_employee = Employee {
            id: 1,
            name: "Jane Doe".to_string(),
            hire_date: Utc.from_utc_datetime(&chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap().and_hms_opt(0, 0, 0).unwrap()),
        };
        let employee_doc= bson::to_document(&new_employee)?;
        collection.insert_one(employee_doc).await?;

        let init_args = create_default_init_args(
            temp_dir.path().to_path_buf(),
            "test_project".to_owned(),
            Some(mongo_uri.clone()),
            Some(pg_connection_string.clone()),
            db_mongo.to_owned(),
        );
        run_init(init_args).expect("init should succeed");

        assert!(temp_dir.path().join("test_project").exists(), "Project directory should be created");
        assert!(temp_dir.path().join("test_project").join("schema").join("tables").exists(), "Schema tables directory should be created");
        assert!(temp_dir.path().join("test_project").join("source").join("collections").exists(), "Source collections directory should be created");
        assert!(temp_dir.path().join("test_project").join("data").exists(), "Data directory should be created");
        assert!(temp_dir.path().join("test_project").join("config").join("test_project.toml").exists(), "Config file should be created");
        assert!(temp_dir.path().join("test_project").join("reports").exists(), "Reports folder should be created");

        let conf_toml= std::fs::read_to_string(temp_dir.path().join("test_project").join("config").join("test_project.toml"))?;
        assert!(conf_toml.contains(&format!("uri = \"{}\"", mongo_uri)), "Config should contain the MongoDB URI");
        assert!(conf_toml.contains(&format!("uri = \"{}\"", pg_connection_string)), "Config should contain the PostgreSQL URI");
        assert!(conf_toml.contains(&format!("base_dir = \"{}\"", temp_dir.path().to_path_buf().display())), "Config should contain the project_base path");
        assert!(conf_toml.contains("project_dir = \"test_project\""), "Config should contain the project_dir");
        assert!(conf_toml.contains(&format!("namespace = \"{}\"", db_mongo)), "Config should contain the namespace");
        assert!(conf_toml.contains("datetime_field = [\"created_at\", \"last_update\", \"updated_at\", \"*_date\", \"date\"]"), "Config should contain the default datetime field patterns");
        assert!(conf_toml.contains("jsonb = false"), "Config should contain the default jsonb setting");

        let infer_args = create_default_infer_args(temp_dir.path().join("test_project").join("config").join("test_project.toml"));

        run_infer(infer_args).await?;

        let ddl_file_path = temp_dir.path()
        .join("test_project")
        .join("schema")
        .join("tables")
        .join("test_db")
        .join("employees.sql");

        assert!(ddl_file_path.exists(), "DDL file for employees should be created");
        assert!(temp_dir.path().join("test_project").join("source").join("collections").join("employees").join("employees.json").exists(), "Source collections employees should be created");
        assert!(temp_dir.path().join("test_project").join("source").join("collections").join("employees").join("employees.stats.txt").exists(), "Source collections stats txt format for employees should be created");
        assert!(temp_dir.path().join("test_project").join("source").join("collections").join("employees").join("employees.stats.yaml").exists(), "Source collections stats yaml format for employees should be created");
        assert!(temp_dir.path().join("test_project").join("source").join("collections").join("employees").join("mapping_employees.yaml").exists(), "Source collections mapping yaml format for employees should be created");

        let expected_content = indoc! {r#"
            CREATE DATABASE "test_db";
            \connect "test_db"

            CREATE SCHEMA IF NOT EXISTS "employees";
            SET search_path = "employees";

            CREATE TABLE employees (
                id TEXT PRIMARY KEY,
                hire_date TIMESTAMP WITH TIME ZONE NOT NULL,
                name VARCHAR(20) NOT NULL
            );
        "#};    
        let actual_content = fs::read_to_string(&ddl_file_path)
            .expect("Should have been able to read the DDL file");

        // It will show a helpful diff if the content does not match.
        assert_eq!(actual_content.trim(), expected_content.trim());        

        let config = temp_dir.path().join("test_project").join("config").join("test_project.toml");
        let export_args = create_default_export_args(config.clone());
        run_export(export_args).await?;
        assert!(temp_dir.path().join("test_project").join("data").join("test_db").join("employees").join("employees.csv.gz").exists(), "Exported data employees.csv.gz should be created");

        let import_args = create_default_import_args(config.clone());
        run_import(import_args).await?;

        let host_port = pg_container.get_host_port_ipv4(5432).await?;
        let pg_test_db_connection_string = format!(
            "postgres://postgres:postgres@localhost:{}/{}?sslmode=disable",
            host_port, 
            "test_db"
        );
        let (client, connection) =
            tokio_postgres::connect(&pg_test_db_connection_string, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });
        let employee_name = "Jane Doe";
        let hire_date = Utc.with_ymd_and_hms(2024, 1, 15, 0, 0, 0).unwrap();

        client
            .execute("SET search_path TO employees, public", &[])
            .await?;
        let row = client
            .query_one(
                "SELECT name, hire_date FROM employees WHERE name = $1",
                &[&employee_name],
            )
            .await?;

        let retrieved_name: &str = row.get("name");
        let retrieved_date: chrono::DateTime<chrono::Utc> = row.get("hire_date");

        assert_eq!(retrieved_name, employee_name);
        assert_eq!(retrieved_date, hire_date);

        run_check_md5("employees".to_owned(), Some(config.clone()), false).await.expect("can't run checkmd5");

        Ok(())
    }
}
