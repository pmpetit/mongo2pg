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

use std::collections::{HashMap, HashSet};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use apache_avro::{from_avro_datum, types::Value as AvroValue, Schema};
use bson::{doc, Bson};
use bytes::Bytes;
use chrono::Utc;
use clap::{Args, CommandFactory, Parser, Subcommand};
use env_logger::Builder as EnvLoggerBuilder;
use flate2::read::GzDecoder;
use futures::{SinkExt, StreamExt, TryStreamExt};
use google_cloud_storage::client::{Storage, StorageControl};
use indexmap::IndexMap;
use log::{debug, info, warn, Level, LevelFilter};
use mongo2pg::analyzer::{
    Analyzer, CollectionSchema, FieldSchema, TypeSchema, TYPE_NULL, TYPE_UNDEFINED,
};
use mongo2pg::checkmd5::compute_md5_summaries_for_collection;
// use mongo2pg::checkmd5::run_check_md5;
use mongo2pg::export::{
    ensure_gcs_authentication, export_collections_to_sql, resolve_export_write_backend,
    resolve_grouped_sql_lookup_name, ExportWriteBackend, DEFAULT_EXPORT_CHUNK_ROWS,
};
use mongo2pg::mapping_path::mapping_mongo_path_for_segments;
use mongo2pg::report::{
    collect_rows, compute_cluster_score, compute_db_score, render_cluster_html, render_html,
    render_post_import_html, PostImportCollectionRow, PostImportCountDiffRow, PostImportMd5Column,
    PostImportMd5MismatchRow, PostImportMd5Summary, PostImportNode, PostImportTableRow,
    SYSTEM_DATABASES,
};
use mongo2pg::schema_diagram::{load_tables_by_db, parse_sql, render_schema_html};
use mongo2pg::stats::{
    format_stats, stats_to_yaml, CollectionReadOpsYaml, InferWarningMinorityYaml,
    InferWarningTypeYaml, InferWarningYaml,
};
use mongo2pg::to_pg::schema_to_ddl_with_timestamp_fields;
use mongo2pg::util::{
    can_inline_object_fields, flatten_grouped_root_array_object_fields,
    flatten_root_array_object_field, flattened_root_parent_id_column,
    configured_project_root,
    grouped_root_array_object_fields, inline_object_column_names_with_prefix,
    inline_object_leaf_fields_with_prefix, is_pg_reserved, objectid_hex_to_uuid,
    property_filter_entries_for_collection, read_conf, sanitize, scalar_type_family,
    should_infer_collection,
};
use mongodb::{options::ClientOptions, Client};
use postgres_native_tls::MakeTlsConnector;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use strsim::jaro_winkler;
use toml::{map::Map as TomlMap, Value as TomlValue};

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
    /// Runtime log level (error, warn, info, debug, trace). Overrides config value when set.
    #[arg(long = "log-level", global = true)]
    log_level: Option<String>,

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
    /// Consume Kafka CDC topics and apply mapping-based updates into PostgreSQL
    KafkaImport(KafkaImportArgs),
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

    /// Maximum server-side query time in milliseconds for infer sampling reads.
    #[arg(long = "max-time-ms")]
    max_time_ms: Option<u64>,

    /// Documents per chunk in fallback infer reads for huge collections.
    #[arg(long = "chunk-size")]
    chunk_size: Option<u64>,

    /// Maximum retries for Unauthorized getMore errors during chunked infer fallback.
    #[arg(long = "auth-retry-max")]
    auth_retry_max: Option<u32>,

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

    /// PostgreSQL target database name. When -c is provided, this overwrites
    /// target.database_name in the config file before running.
    #[arg(long = "database-name")]
    database_name: Option<String>,

    /// PostgreSQL target schema name. When -c is provided, this overwrites
    /// target.schema_name in the config file before running.
    #[arg(long = "schema-name")]
    schema_name: Option<String>,

    /// Project directory name. When -c is provided, this overwrites
    /// project.project_dir in the config file before running.
    #[arg(long = "project-dir")]
    project_dir: Option<String>,

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
    #[arg(long = "schema", visible_alias = "schema-name")]
    schema: Option<String>,

    /// Project directory name. When -c is provided, this overwrites
    /// project.project_dir in the config file before running.
    #[arg(long = "project-dir")]
    project_dir: Option<String>,
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
    #[arg(long = "namespace")]
    namespace: Option<String>,

    /// Optional cluster segment inserted between base_dir and project_dir for all outputs
    #[arg(long = "cluster-name", visible_alias = "cluster-naem")]
    cluster_name: Option<String>,
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

    /// Project directory name. When -c is provided, this overwrites
    /// project.project_dir in the config file before running.
    #[arg(long = "project-dir")]
    project_dir: Option<String>,

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

    /// PostgreSQL target database name. When -c is provided, this overwrites
    /// target.database_name in the config file before running.
    #[arg(long = "database-name")]
    database_name: Option<String>,

    /// PostgreSQL target schema name. When -c is provided, this overwrites
    /// target.schema_name in the config file before running.
    #[arg(long = "schema-name")]
    schema_name: Option<String>,

    /// Project directory name. When -c is provided, this overwrites
    /// project.project_dir in the config file before running.
    #[arg(long = "project-dir")]
    project_dir: Option<String>,

    /// Maximum buffered table rows before export flushes chunk data to CSV.gz.
    /// Defaults to SOURCE.CHUNK_SIZE from config, then a safe built-in value.
    #[arg(long = "chunk-size")]
    chunk_size: Option<u64>,
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

    /// PostgreSQL target database name. When -c is provided, this overwrites
    /// target.database_name in the config file before running.
    #[arg(long = "database-name")]
    database_name: Option<String>,

    /// PostgreSQL target schema name. When -c is provided, this overwrites
    /// target.schema_name in the config file before running.
    #[arg(long = "schema-name")]
    schema_name: Option<String>,

    /// Project directory name. When -c is provided, this overwrites
    /// project.project_dir in the config file before running.
    #[arg(long = "project-dir")]
    project_dir: Option<String>,
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

#[derive(Parser, Debug)]
struct KafkaImportArgs {
    /// Path to the project config file (TOML)
    #[arg(short = 'c', long = "config")]
    config: PathBuf,

    /// Optional explicit topics list (comma-separated). Overrides [kafka].topics from config.
    #[arg(long = "topics", value_delimiter = ',')]
    topics: Vec<String>,

    /// Optional max messages to consume in this run. Overrides [kafka].max_messages.
    #[arg(long = "max-messages")]
    max_messages: Option<usize>,

    /// Optional consumer offset policy for missing group offsets.
    /// Supported values: latest, earliest, 0.
    /// `0` enables snapshot-equivalent mode (truncate + fresh group + earliest + idle stop).
    /// Overrides [kafka].offset and [kafka].auto_offset_reset.
    #[arg(long = "offset", value_parser = ["latest", "earliest", "0"])]
    offset: Option<String>,

    /// Project directory name. When -c is provided, this overwrites
    /// project.project_dir in the config file before running.
    #[arg(long = "project-dir")]
    project_dir: Option<String>,

    #[arg(long = "group-id")]
    group_id: Option<String>,

    #[arg(long = "topic-prefix")]
    topic_prefix: Option<String>,

    #[arg(long = "database-name")]
    database_name: Option<String>,

    #[arg(long = "schema-name")]
    schema_name: Option<String>,
}

// ──────────────────────────────────────────────────────────────────────────────
// Entry point
// ──────────────────────────────────────────────────────────────────────────────
#[tokio::main]
async fn main() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cli = Cli::parse();

    let log_level = resolve_effective_log_level(&cli)?;
    init_runtime_logger(log_level)?;

    let Cli { command, infer, .. } = cli;
    validate_command_and_args(&command, infer.as_ref())?;

    match command {
        Some(Command::Init(args)) => run_init(args),
        Some(Command::ToPg(args)) => run_to_pg(args, false),
        Some(Command::Report(args)) => run_report(args, false).await,
        Some(Command::Export(args)) => run_export(args).await,
        Some(Command::Import(args)) => run_import(args).await,
        Some(Command::KafkaImport(args)) => run_kafka_import(args).await,
        Some(Command::Infer(args)) => run_infer(args).await,
        Some(Command::ClusterReport(args)) => run_cluster_report(args),
        None => match infer {
            Some(args) => run_infer(args).await,
            None => {
                Cli::command().print_help()?;
                Ok(())
            }
        },
    }
}

fn parse_log_level(raw: &str) -> Result<LevelFilter> {
    let normalized = raw.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "error" => Ok(LevelFilter::Error),
        "warn" | "warning" => Ok(LevelFilter::Warn),
        "info" => Ok(LevelFilter::Info),
        "debug" => Ok(LevelFilter::Debug),
        "trace" => Ok(LevelFilter::Trace),
        _ => Err(anyhow!(
            "invalid log level '{raw}'. Use one of: error, warn, info, debug, trace"
        )),
    }
}

fn resolve_log_level_precedence(
    cli_level: Option<&str>,
    config_level: Option<&str>,
) -> Result<LevelFilter> {
    if let Some(level) = cli_level {
        return parse_log_level(level);
    }
    if let Some(level) = config_level {
        return parse_log_level(level);
    }
    Ok(LevelFilter::Info)
}

fn config_path_from_cli(cli: &Cli) -> Option<&Path> {
    match &cli.command {
        Some(Command::Infer(args)) => args.config.as_deref(),
        Some(Command::ToPg(args)) => args.config.as_deref(),
        Some(Command::Report(args)) => args.config.as_deref(),
        Some(Command::Export(args)) => args.config.as_deref(),
        Some(Command::Import(args)) => Some(args.config.as_path()),
        Some(Command::ClusterReport(args)) => args.configs.first().map(PathBuf::as_path),
        Some(Command::KafkaImport(args)) => Some(args.config.as_path()),
        Some(Command::Init(_)) => None,
        None => cli.infer.as_ref().and_then(|args| args.config.as_deref()),
    }
}

fn resolve_effective_log_level(cli: &Cli) -> Result<LevelFilter> {
    let cli_level = cli.log_level.as_deref();

    if let Some(conf_path) = config_path_from_cli(cli) {
        let conf = read_conf(conf_path).with_context(|| {
            format!(
                "Failed to load logging configuration from {}",
                conf_path.display()
            )
        })?;
        return resolve_log_level_precedence(cli_level, conf.log_level.as_deref());
    }

    resolve_log_level_precedence(cli_level, None)
}

fn format_runtime_log_line(level: Level, elapsed: Duration, message: &str) -> String {
    let timestamp = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
    format!(
        "{timestamp} +{}s [{}] {}",
        elapsed.as_secs(),
        level,
        message
    )
}

fn connection_failed_context(backend: &str, operation: &str) -> String {
    format!("connection_failed backend={backend} operation={operation}")
}

fn init_runtime_logger(level_filter: LevelFilter) -> Result<()> {
    let start = Instant::now();
    let mut builder = EnvLoggerBuilder::new();
    builder.filter_level(level_filter);
    builder.format(move |buf, record| {
        writeln!(
            buf,
            "{}",
            format_runtime_log_line(record.level(), start.elapsed(), &record.args().to_string())
        )
    });
    builder
        .try_init()
        .map_err(|err| anyhow!("failed to initialize logger: {err}"))
}

fn validate_command_and_args(command: &Option<Command>, infer: Option<&InferArgs>) -> Result<()> {
    match command {
        Some(Command::Infer(args)) => validate_infer_args(args),
        Some(Command::Init(args)) => {
            if let Some(uri) = args.source_uri.as_deref() {
                validate_source_uri(uri)?;
            }
            if let Some(uri) = args.target_uri.as_deref() {
                validate_target_uri(uri)?;
            }
            if let Some(ns) = args.namespace.as_deref() {
                validate_namespace_arg(ns)?;
            }
            Ok(())
        }
        Some(Command::ToPg(args)) => {
            if args.table.is_some() && args.collection.is_none() {
                return Err(anyhow!("--table requires <collection>"));
            }
            Ok(())
        }
        Some(Command::Report(args)) => {
            if let Some(uri) = args.mongo.source_uri.as_deref() {
                validate_source_uri(uri)?;
            }
            Ok(())
        }
        Some(Command::Export(args)) => {
            if args.config.is_none() {
                return Err(anyhow!("export requires -c/--config"));
            }
            if let Some(uri) = args.mongo.source_uri.as_deref() {
                validate_source_uri(uri)?;
            }
            if let Some(ns) = args.namespace.as_deref() {
                validate_namespace_arg(ns)?;
            }
            if let Some(chunk_size) = args.chunk_size {
                if chunk_size == 0 {
                    return Err(anyhow!("--chunk-size must be greater than 0"));
                }
            }
            Ok(())
        }
        Some(Command::Import(args)) => {
            if let Some(ns) = args.namespace.as_deref() {
                validate_namespace_arg(ns)?;
            }
            Ok(())
        }
        Some(Command::ClusterReport(args)) => {
            if args.configs.is_empty() {
                return Err(anyhow!(
                    "cluster-report requires at least one --configs value"
                ));
            }
            Ok(())
        }
        Some(Command::KafkaImport(args)) => {
            if let Some(offset) = args.offset.as_deref() {
                if !matches!(offset, "latest" | "earliest" | "0") {
                    return Err(anyhow!("--offset must be one of: latest, earliest, 0"));
                }
            }
            Ok(())
        }
        None => {
            if let Some(args) = infer {
                validate_infer_args(args)
            } else {
                Ok(())
            }
        }
    }
}

fn validate_infer_args(args: &InferArgs) -> Result<()> {
    if args.config.is_none() && args.mongo.source_uri.is_none() {
        return Err(anyhow!(
            "infer requires --source-uri when -c/--config is not provided"
        ));
    }

    if let Some(uri) = args.mongo.source_uri.as_deref() {
        validate_source_uri(uri)?;
    }

    if let Some(ns) = args.namespace.as_deref() {
        validate_namespace_arg(ns)?;
    }

    if let Some(number) = args.number {
        if number == 0 {
            return Err(anyhow!("--number must be greater than 0"));
        }
    }

    if let Some(percent) = args.percent {
        if !(0.0 < percent && percent <= 100.0) {
            return Err(anyhow!("--percent must be > 0 and <= 100"));
        }
    }

    if let Some(chunk_size) = args.chunk_size {
        if chunk_size == 0 {
            return Err(anyhow!("--chunk-size must be greater than 0"));
        }
    }

    if let Some(auth_retry_max) = args.auth_retry_max {
        if auth_retry_max > 100 {
            return Err(anyhow!("--auth-retry-max must be between 0 and 100"));
        }
    }

    Ok(())
}

fn validate_namespace_arg(ns: &str) -> Result<()> {
    if ns.trim().is_empty() {
        return Err(anyhow!("namespace cannot be empty"));
    }
    if ns.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return Err(anyhow!("namespace contains unsafe characters"));
    }
    if let Some((db, coll)) = ns.split_once('.') {
        if db.is_empty() || coll.is_empty() {
            return Err(anyhow!("namespace must be <db> or <db>.<collection>"));
        }
    }
    Ok(())
}

fn validate_source_uri(uri: &str) -> Result<()> {
    if uri.is_empty() || uri.chars().any(|c| c.is_control()) {
        return Err(anyhow!("source URI contains unsafe characters"));
    }
    if !(uri.starts_with("mongodb://") || uri.starts_with("mongodb+srv://")) {
        return Err(anyhow!(
            "source URI must start with mongodb:// or mongodb+srv://"
        ));
    }
    Ok(())
}

fn validate_target_uri(uri: &str) -> Result<()> {
    if uri.is_empty() || uri.chars().any(|c| c.is_control()) {
        return Err(anyhow!("target URI contains unsafe characters"));
    }
    if !(uri.starts_with("postgres://") || uri.starts_with("postgresql://")) {
        return Err(anyhow!(
            "target URI must start with postgres:// or postgresql://"
        ));
    }
    Ok(())
}

#[derive(Default)]
struct ConfigOverrides {
    project_dir: Option<String>,
    source_uri: Option<String>,
    namespace: Option<String>,
    number: Option<u64>,
    percent: Option<f64>,
    max_time_ms: Option<u64>,
    chunk_size: Option<u64>,
    auth_retry_max: Option<u32>,
    jsonb: Option<bool>,
    target_database_name: Option<String>,
    target_schema_name: Option<String>,
    kafka_topics: Option<Vec<String>>,
    kafka_max_messages: Option<usize>,
    kafka_offset: Option<String>,
    kafka_group_id: Option<String>,
    kafka_topic_prefix: Option<String>,
}

fn ensure_toml_table<'a>(
    doc: &'a mut TomlValue,
    key: &str,
) -> Result<&'a mut TomlMap<String, TomlValue>> {
    if !doc.is_table() {
        *doc = TomlValue::Table(TomlMap::new());
    }

    let table = doc
        .as_table_mut()
        .ok_or_else(|| anyhow!("Config root is not a TOML table"))?;

    if !table.contains_key(key) || !table.get(key).is_some_and(TomlValue::is_table) {
        table.insert(key.to_owned(), TomlValue::Table(TomlMap::new()));
    }

    table
        .get_mut(key)
        .and_then(TomlValue::as_table_mut)
        .ok_or_else(|| anyhow!("Failed to access TOML section [{key}]"))
}

fn apply_config_overrides(conf_path: &Path, overrides: &ConfigOverrides) -> Result<()> {
    let has_overrides = overrides.source_uri.is_some()
        || overrides.project_dir.is_some()
        || overrides.namespace.is_some()
        || overrides.number.is_some()
        || overrides.percent.is_some()
        || overrides.max_time_ms.is_some()
        || overrides.chunk_size.is_some()
        || overrides.auth_retry_max.is_some()
        || overrides.jsonb.is_some()
        || overrides.target_database_name.is_some()
        || overrides.target_schema_name.is_some()
        || overrides.kafka_topics.is_some()
        || overrides.kafka_max_messages.is_some()
        || overrides.kafka_offset.is_some()
        || overrides.kafka_group_id.is_some()
        || overrides.kafka_topic_prefix.is_some();

    if !has_overrides {
        return Ok(());
    }

    let raw = std::fs::read_to_string(conf_path)
        .with_context(|| format!("Failed to read config file {}", conf_path.display()))?;
    let mut doc: TomlValue = toml::from_str(&raw)
        .with_context(|| format!("Failed to parse TOML config {}", conf_path.display()))?;

    {
        let project = ensure_toml_table(&mut doc, "project")?;
        if let Some(v) = &overrides.project_dir {
            project.insert("project_dir".to_owned(), TomlValue::String(v.clone()));
        }
    }

    {
        let source = ensure_toml_table(&mut doc, "source")?;
        if let Some(v) = &overrides.source_uri {
            source.insert("uri".to_owned(), TomlValue::String(v.clone()));
        }
        if let Some(v) = &overrides.namespace {
            source.insert("namespace".to_owned(), TomlValue::String(v.clone()));
        }
        if let Some(v) = overrides.number {
            source.insert("number".to_owned(), TomlValue::Integer(v as i64));
        }
        if let Some(v) = overrides.percent {
            source.insert("percent".to_owned(), TomlValue::Float(v));
        }
        if let Some(v) = overrides.max_time_ms {
            source.insert("max_time_ms".to_owned(), TomlValue::Integer(v as i64));
        }
        if let Some(v) = overrides.chunk_size {
            source.insert("chunk_size".to_owned(), TomlValue::Integer(v as i64));
        }
        if let Some(v) = overrides.auth_retry_max {
            source.insert("auth_retry_max".to_owned(), TomlValue::Integer(v as i64));
        }
        if let Some(v) = overrides.jsonb {
            source.insert("jsonb".to_owned(), TomlValue::Boolean(v));
        }
    }

    {
        let target = ensure_toml_table(&mut doc, "target")?;
        if let Some(v) = &overrides.target_database_name {
            target.insert("database_name".to_owned(), TomlValue::String(v.clone()));
        }
        if let Some(v) = &overrides.target_schema_name {
            target.insert("schema_name".to_owned(), TomlValue::String(v.clone()));
        }
    }

    {
        let kafka = ensure_toml_table(&mut doc, "kafka")?;

        if let Some(v) = &overrides.kafka_group_id {
            kafka.insert("group_id".to_owned(), TomlValue::String(v.clone()));
        }
        if let Some(v) = &overrides.kafka_topic_prefix {
            kafka.insert("topic_prefix".to_owned(), TomlValue::String(v.clone()));
        }

        if let Some(v) = &overrides.kafka_topics {
            kafka.insert(
                "topics".to_owned(),
                TomlValue::Array(v.iter().cloned().map(TomlValue::String).collect()),
            );
        }
        if let Some(v) = overrides.kafka_max_messages {
            kafka.insert("max_messages".to_owned(), TomlValue::Integer(v as i64));
        }
        if let Some(v) = &overrides.kafka_offset {
            kafka.insert("offset".to_owned(), TomlValue::String(v.clone()));
            kafka.insert("auto_offset_reset".to_owned(), TomlValue::String(v.clone()));
        }
    }

    let updated = toml::to_string_pretty(&doc)
        .with_context(|| format!("Failed to render TOML config {}", conf_path.display()))?;
    std::fs::write(conf_path, updated)
        .with_context(|| format!("Failed to write config file {}", conf_path.display()))?;

    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────────
// Collection grouping – post-infer consolidation
// ──────────────────────────────────────────────────────────────────────────────

/// A group of collections that share a common prefix (everything before the last `_`).
#[derive(Debug)]
struct CollectionGroup {
    /// Shared table name = prefix (e.g. "events" for events_lmfr / events_lmza).
    prefix: String,
    /// All collection names in the group.
    members: Vec<String>,
    /// First alphabetical member used as the schema representative.
    representative: String,
}

/// Detect candidate groups from a list of collection names.
/// Groups by prefix (everything before the last `_`); only groups with ≥2 members qualify.
fn detect_candidate_groups(names: &[String]) -> Vec<CollectionGroup> {
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
fn collection_field_signature(
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
fn validate_group_schema_compatibility(collections_dir: &Path, group: &CollectionGroup) -> bool {
    group
        .members
        .iter()
        .all(|member| collection_field_signature(collections_dir, member).is_ok())
}

/// Rewrite each group member's mapping YAML to use the shared `prefix` table name.
/// When `add_grouped_key` is true also inserts a `_key TEXT` column carrying the
/// collection suffix as a `literal_value`.
fn apply_grouping_to_mappings(
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

    if let Some(conf) = args.config.as_deref() {
        apply_config_overrides(
            conf,
            &ConfigOverrides {
                project_dir: args.project_dir.clone(),
                target_schema_name: args.schema.clone(),
                ..ConfigOverrides::default()
            },
        )?;
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
        let local_project_root = resolve_local_project_root_from_config(conf, &c);
        let cols = local_project_root
            .join("source")
            .join("collections");
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
            render_ddl_from_mapping_tables(&mapping_tables, target_schema)
        } else {
            schema_to_ddl_with_timestamp_fields(
                &schema,
                table_name,
                target_schema,
                &config_timestamp_fields,
            )
        };
        let db_name = effective_rel_sql
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|s| s.to_str());
        let ddl = prepend_database_preamble(ddl, db_name);

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

    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────────
// `infer` subcommand (also the default)
// ──────────────────────────────────────────────────────────────────────────────

async fn run_infer(args: InferArgs) -> Result<()> {
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
        let local_project_root = resolve_local_project_root_from_config(conf, &c);
        let out_dir = local_project_root.join("source").join("collections");
        info!("infer output root (local): {}", local_project_root.display());
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

    let client_options = ClientOptions::parse(&resolved_source_uri)
        .await
        .with_context(|| {
            format!(
                "{}: failed to parse MongoDB SOURCE_URI",
                connection_failed_context("mongo", "connect")
            )
        })?;
    let client = Client::with_options(client_options).with_context(|| {
        format!(
            "{}: failed to create MongoDB client",
            connection_failed_context("mongo", "connect")
        )
    })?;

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

    if let Some(ref conf) = chained_config {
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
        )?;
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
        debug!(
            "[gcs-debug] infer upload hook skipped: infer ran without -c config"
        );
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

fn validate_infer_artifacts_use_base_dir(conf: &Path) -> Result<()> {
    let c = read_conf(conf)?;
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

fn infer_artifact_directories(project_root: &Path) -> Vec<PathBuf> {
    vec![
        project_root.join("source"),
        project_root.join("schema"),
        project_root.join("reports"),
    ]
}

fn collect_files_recursive(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if !root.exists() {
        return Ok(files);
    }

    for entry in
        std::fs::read_dir(root).with_context(|| format!("Cannot read directory {}", root.display()))?
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

fn infer_object_key(
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

fn infer_object_mime_type(file_path: &Path) -> &'static str {
    match file_path.extension().and_then(|ext| ext.to_str()) {
        Some("json") => "application/json",
        Some("yaml") | Some("yml") => "application/x-yaml",
        Some("sql") => "application/sql",
        Some("html") => "text/html",
        Some("txt") => "text/plain",
        _ => "application/octet-stream",
    }
}

async fn move_infer_artifacts_to_gcs_if_needed(conf: &Path) -> Result<()> {
    debug!(
        "[gcs-debug] infer upload hook entered: config='{}'",
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
        "[gcs-debug] infer upload enabled: base_dir='{}' bucket='{}' prefix='{}'",
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

    let project_root = resolve_local_project_root_from_config(conf, &c);
    debug!(
        "[gcs-debug] infer upload project root resolved: {}",
        project_root.display()
    );
    let artifact_dirs = infer_artifact_directories(&project_root);
    for dir in &artifact_dirs {
        debug!(
            "[gcs-debug] infer upload scan dir: {} exists={} is_dir={}",
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
        "[gcs-debug] infer upload candidate files: {}",
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
            "[gcs-debug] infer upload object: {} -> gs://{}/{}",
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
        "[gcs-debug] infer upload done: uploaded_files={} bucket='{}' prefix='{}'",
        uploaded_files,
        bucket,
        prefix.trim_matches('/'),
    );

    let purge_local = std::env::var("MONGO2PG_GCS_PURGE_LOCAL")
        .ok()
        .map(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false);

    if purge_local {
        for file_path in &files_to_move {
            std::fs::remove_file(file_path).with_context(|| {
                format!("Failed to remove local infer artifact {}", file_path.display())
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

fn print_infer_summary(conf: &Path) -> Result<()> {
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
const DEFAULT_SAMPLE_MAX_TIME: Duration = Duration::from_secs(120);
const DEFAULT_INFER_CHUNK_SIZE: u64 = 1_000_000;
const DEFAULT_INFER_AUTH_RETRY_MAX: u32 = 3;

fn infer_query_max_time(max_time_ms: Option<u64>) -> Duration {
    max_time_ms
        .filter(|value| *value > 0)
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_SAMPLE_MAX_TIME)
}

fn resolve_infer_chunk_size(chunk_size: Option<u64>) -> Result<u64> {
    let resolved = chunk_size.unwrap_or(DEFAULT_INFER_CHUNK_SIZE);
    if resolved == 0 {
        return Err(anyhow!("chunk_size must be greater than 0"));
    }
    if resolved > i64::MAX as u64 {
        return Err(anyhow!("chunk_size must be <= {}", i64::MAX));
    }
    Ok(resolved)
}

fn resolve_export_chunk_size(chunk_size: Option<u64>) -> Result<u64> {
    let resolved = chunk_size.unwrap_or(DEFAULT_EXPORT_CHUNK_ROWS);
    if resolved == 0 {
        return Err(anyhow!("chunk_size must be greater than 0"));
    }
    if resolved > i64::MAX as u64 {
        return Err(anyhow!("chunk_size must be <= {}", i64::MAX));
    }
    Ok(resolved)
}

fn resolve_infer_auth_retry_max(auth_retry_max: Option<u32>) -> Result<u32> {
    let resolved = auth_retry_max.unwrap_or(DEFAULT_INFER_AUTH_RETRY_MAX);
    if resolved > 100 {
        return Err(anyhow!("auth_retry_max must be between 0 and 100"));
    }
    Ok(resolved)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnauthorizedRetryDecision {
    Retry,
    Exhausted,
}

fn is_unauthorized_cursor_error(error_text: &str) -> bool {
    let lower = error_text.to_ascii_lowercase();
    lower.contains("error code 13")
        || lower.contains("unauthorized")
        || lower.contains("requires authentication")
}

fn classify_unauthorized_retry(
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

fn timeout_fallback_hint(error_text: &str, max_time_ms: Option<u64>) -> String {
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
                            "  [warn] find() chunk failed for {}.{} at chunk {}/{} (start_after_id={}, limit={}): {:#}",
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
                                        "  [warn] auth_retry namespace={}.{} chunk={}/{} processed={}/{} retry_attempt={}/{} reason={}",
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
                                        "  [warn] auth_retry_exhausted namespace={}.{} chunk={}/{} processed={}/{} retries={} reason={}",
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
                                        "  [warn] find() chunk cursor error for {}.{} at chunk {}/{}: {:#}",
                                        db_name,
                                        coll_name,
                                        chunk_index,
                                        total_chunks,
                                        e
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
                "  [warn] $sample failed for {db_name}.{coll_name} \
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
                        "  [warn] $sample cursor error for {db_name}.{coll_name} \
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
        )
        .with_context(|| format!("Failed to write output files for {output_name}"))?;
    }

    Ok(schema)
}

fn bson_as_u64(value: &Bson) -> Option<u64> {
    match value {
        Bson::Int32(v) if *v >= 0 => Some(*v as u64),
        Bson::Int64(v) if *v >= 0 => Some(*v as u64),
        Bson::Double(v) if v.is_finite() && *v >= 0.0 => Some(*v as u64),
        Bson::Decimal128(v) => v.to_string().parse::<u64>().ok(),
        _ => None,
    }
}

fn bson_as_timestamp_string(value: &Bson) -> Option<String> {
    match value {
        Bson::DateTime(dt) => {
            chrono::DateTime::<chrono::Utc>::from_timestamp_millis(dt.timestamp_millis())
                .map(|ts| ts.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        }
        Bson::String(text) if !text.trim().is_empty() => Some(text.trim().to_owned()),
        _ => None,
    }
}

fn bson_as_f64(value: &Bson) -> Option<f64> {
    match value {
        Bson::Int32(v) => Some(*v as f64),
        Bson::Int64(v) => Some(*v as f64),
        Bson::Double(v) if v.is_finite() => Some(*v),
        Bson::Decimal128(v) => v.to_string().parse::<f64>().ok(),
        _ => None,
    }
}

fn since_from_uptime_seconds(uptime_seconds: f64) -> Option<String> {
    if !uptime_seconds.is_finite() || uptime_seconds < 0.0 {
        return None;
    }

    let uptime_millis = (uptime_seconds * 1000.0).round() as i64;
    let since = chrono::Utc::now() - chrono::Duration::milliseconds(uptime_millis);
    Some(since.format("%Y-%m-%d %H:%M:%S UTC").to_string())
}

async fn fetch_server_uptime_since(db: &mongodb::Database) -> Option<String> {
    let server_status = db.run_command(doc! { "serverStatus": 1 }).await.ok()?;
    let uptime_seconds = server_status.get("uptime").and_then(bson_as_f64)?;
    since_from_uptime_seconds(uptime_seconds)
}

async fn fetch_collection_read_ops_stats(
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    literal_value: Option<String>,
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
    #[serde(alias = "dbname")]
    mongo_dbname: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    mongo_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    traversal: Option<TraversalPlan>,
    pg_mapping: PgMapping,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TraversalMode {
    Root,
    Object,
    ArrayObject,
    ArrayScalar,
    MapObject,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TraversalPlan {
    mode: TraversalMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent_table: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_field: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fk_column: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key_column: Option<String>,
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
    if s.starts_with(|c: char| c.is_ascii_digit()) {
        format!("_{s}")
    } else {
        s
    }
}

fn normalize_pg_identifier(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        trimmed[1..trimmed.len() - 1].replace("\"\"", "\"")
    } else {
        trimmed.to_owned()
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
    fn is_standalone_index_pseudo_column(column: &DdlColumnMapping) -> bool {
        column.name.eq_ignore_ascii_case("create")
            && column
                .sql_type
                .trim_start()
                .to_ascii_uppercase()
                .starts_with("INDEX IF NOT EXISTS")
    }

    fn rendered_sql_type(sql_type: &str) -> &str {
        if sql_type.eq_ignore_ascii_case("VARCHAR(0)") {
            "TEXT"
        } else {
            sql_type
        }
    }

    fn maybe_quote_ident(ident: &str) -> String {
        if is_pg_reserved(ident) {
            quote_ident(ident)
        } else {
            ident.to_owned()
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

    fn fk_columns(from_col: &str) -> Vec<String> {
        from_col
            .split(',')
            .map(str::trim)
            .filter(|col| !col.is_empty())
            .map(str::to_owned)
            .collect()
    }

    fn fk_index_statements(table: &DdlTableMapping, schema_name: Option<&str>) -> Vec<String> {
        let mut seen = std::collections::HashSet::new();
        let mut statements = Vec::new();

        for fk in &table.foreign_keys {
            let cols = fk_columns(&fk.from_col);
            if cols.is_empty() {
                continue;
            }
            let dedupe_key = cols.join("\0");
            if !seen.insert(dedupe_key) {
                continue;
            }

            let index_name = format!("idx_{}_{}", table.name, cols.join("_"));
            let qualified_table = match schema_name {
                Some(schema) => format!("{}.{}", quote_ident(schema), quote_ident(&table.name)),
                None => quote_ident(&table.name),
            };
            let quoted_cols = cols
                .iter()
                .map(|col| quote_ident(col))
                .collect::<Vec<_>>()
                .join(", ");

            statements.push(format!(
                "CREATE INDEX IF NOT EXISTS {} ON {} ({});",
                quote_ident(&index_name),
                qualified_table,
                quoted_cols
            ));
        }

        statements
    }

    let mut ddl_body = String::new();

    if let Some(schema) = schema_name {
        ddl_body.push_str(&format!(
            "CREATE SCHEMA IF NOT EXISTS {};\nSET search_path = {}, public;\n\n",
            quote_ident(schema),
            quote_ident(schema)
        ));
    }

    for table in ordered_tables(tables) {
        ddl_body.push_str(&format!(
            "CREATE TABLE {} (\n",
            maybe_quote_ident(&table.name)
        ));

        let rendered_columns = table
            .columns
            .iter()
            .filter(|column| !is_standalone_index_pseudo_column(column))
            .collect::<Vec<_>>();

        let primary_keys = rendered_columns
            .iter()
            .filter(|column| column.primary_key)
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();

        let mut lines = rendered_columns
            .iter()
            .map(|column| {
                let mut line = format!(
                    "    {} {}",
                    maybe_quote_ident(&column.name),
                    rendered_sql_type(&column.sql_type)
                );
                if column.primary_key && column.sql_type.eq_ignore_ascii_case("uuid") {
                    line.push_str(" DEFAULT public.gen_random_uuid()");
                }
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
            lines.push(format!(
                "    PRIMARY KEY ({})",
                primary_keys
                    .iter()
                    .map(|column| maybe_quote_ident(column))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }

        lines.extend(table.foreign_keys.iter().map(|fk| {
            let fk_from_cols = fk
                .from_col
                .split(',')
                .map(str::trim)
                .filter(|col| !col.is_empty())
                .map(maybe_quote_ident)
                .collect::<Vec<_>>()
                .join(", ");
            let fk_to_cols = fk
                .to_col
                .split(',')
                .map(str::trim)
                .filter(|col| !col.is_empty())
                .map(maybe_quote_ident)
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "    FOREIGN KEY ({}) REFERENCES {} ({}) DEFERRABLE INITIALLY DEFERRED",
                fk_from_cols,
                maybe_quote_ident(&fk.to_table),
                fk_to_cols
            )
        }));

        ddl_body.push_str(&lines.join(",\n"));
        ddl_body.push_str("\n);\n\n");

        let fk_indexes = fk_index_statements(table, schema_name);
        if !fk_indexes.is_empty() {
            ddl_body.push_str(&fk_indexes.join("\n"));
            ddl_body.push_str("\n\n");
        }
    }

    let needs_pgcrypto = ddl_body.contains("public.gen_random_uuid()");
    let needs_postgis = ddl_body.to_ascii_lowercase().contains("geometry(");

    let mut ddl = String::new();
    if needs_pgcrypto {
        ddl.push_str("CREATE EXTENSION IF NOT EXISTS \"pgcrypto\";\n");
    }
    if needs_postgis {
        ddl.push_str("CREATE EXTENSION IF NOT EXISTS postgis;\n");
    }
    if needs_pgcrypto || needs_postgis {
        ddl.push('\n');
    }

    ddl.push_str(&ddl_body);

    ddl.trim_end().to_owned() + "\n"
}

fn load_mapping_ddl_tables(collection_dir: &Path) -> Result<Option<Vec<DdlTableMapping>>> {
    fn is_standalone_index_pseudo_column(column: &DdlColumnMapping) -> bool {
        column.name.eq_ignore_ascii_case("create")
            && column
                .sql_type
                .trim_start()
                .to_ascii_uppercase()
                .starts_with("INDEX IF NOT EXISTS")
    }

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
        let Some(mut ddl) = mapping.pg_mapping.ddl else {
            return Ok(None);
        };
        ddl.columns
            .retain(|column| !is_standalone_index_pseudo_column(column));

        if mapping.pg_mapping.columns.is_empty() {
            let fk_columns = ddl
                .foreign_keys
                .iter()
                .flat_map(|fk| fk.from_col.split(','))
                .map(str::trim)
                .filter(|col| !col.is_empty())
                .collect::<std::collections::HashSet<_>>();

            for column in &mut ddl.columns {
                if !column.primary_key && !fk_columns.contains(column.name.as_str()) {
                    column.nullable = true;
                }
            }
        }
        tables.push(ddl);
    }

    Ok(Some(tables))
}

fn schema_root_id_is_objectid(schema: &CollectionSchema) -> bool {
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

fn mapping_has_surrogate_bigserial_primary_id(tables: &[DdlTableMapping]) -> bool {
    tables.iter().any(|table| {
        table.columns.iter().any(|column| {
            let sql_type = column.sql_type.trim().to_ascii_lowercase();
            column.primary_key
                && column.name == "id"
                && (sql_type == "bigserial" || sql_type == "serial8")
        })
    })
}

fn mapping_has_flattened_parent_uuid_column(tables: &[DdlTableMapping], table_name: &str) -> bool {
    let expected_parent_id = flattened_root_parent_id_column(table_name);
    tables.iter().any(|table| {
        table.columns.iter().any(|column| {
            column.name == expected_parent_id && column.sql_type.eq_ignore_ascii_case("uuid")
        })
    })
}

fn should_regenerate_from_schema_when_objectid_pk(
    schema: &CollectionSchema,
    mapping_tables: &[DdlTableMapping],
    table_name: &str,
) -> bool {
    schema_root_id_is_objectid(schema)
        && mapping_has_surrogate_bigserial_primary_id(mapping_tables)
        && !mapping_has_flattened_parent_uuid_column(mapping_tables, table_name)
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
        tables_by_name: &HashMap<String, mongo2pg::schema_diagram::Table>,
        resolved_child_table_names: &HashMap<String, String>,
        out: &mut Vec<(String, CollectionMapping)>,
    ) {
        fn table_has_child_references(
            table_name: &str,
            tables_by_name: &HashMap<String, mongo2pg::schema_diagram::Table>,
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

fn infer_traversal_plan(mapping: &CollectionMapping) -> TraversalPlan {
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

fn enrich_mappings_with_traversal(mappings: &mut [(String, CollectionMapping)]) {
    for (_, mapping) in mappings.iter_mut() {
        if mapping.traversal.is_none() {
            mapping.traversal = Some(infer_traversal_plan(mapping));
        }
    }
}

/// Write `<dir>/<name>/<name>.json`, `<dir>/<name>/<name>.stats.txt`, `<dir>/<name>/<name>.stats.yaml`, and one `mapping_<table>.yaml` per generated table.
fn write_collection_files(
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

    let yaml_stats = stats_to_yaml(schema, Some(schema.count), infer_warnings, read_ops);
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

// ──────────────────────────────────────────────────────────────────────────────
// `init` subcommand
// ──────────────────────────────────────────────────────────────────────────────

fn run_init(args: InitArgs) -> Result<()> {
    let project_root = if let Some(cluster_name) = args
        .cluster_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        args.project_base.join(cluster_name).join(&args.project_name)
    } else {
        args.project_base.join(&args.project_name)
    };

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
    let namespace_line = args
        .namespace
        .as_deref()
        .map(|ns| format!("namespace = \"{}\"", ns.replace('"', "\\\"")))
        .unwrap_or_else(|| "#namespace = my_db".to_owned());
    let cluster_line = args
        .cluster_name
        .as_deref()
        .map(|name| format!("cluster_name = \"{}\"", name.replace('"', "\\\"")))
        .unwrap_or_else(|| "# cluster_name = \"cluster-a\"".to_owned());
    let conf_content = format!(
        "[project]\ntitle = \"{}\"\nbase_dir = \"{}\"\n{}\nproject_dir = \"{}\"\n\n[source]\nuri = {}\n{}\nnumber = 1000\n# percent = 10.0\n# chunk_size = 1000000\n# auth_retry_max = 3\n# log_level = \"info\"\njsonb = false\n# include = [\"collection_a\", \"collection_b\"]\n# exclude = [\"collection_to_skip\"]\ndatetime_field = [\"created_at\", \"last_update\", \"updated_at\", \"*_date\", \"date\"]\n\n[target]\nuri = {}\ndatabase_name = \"{}\"\n# schema_name = \"shared_schema\"\n\n[kafka]\nbootstrap_servers = \"localhost:9092\"\ngroup_id = \"mongo2pg-kafka-import\"\n# topics = [\"mongo2pg_dbapi.dbapi.projects\"]\n# topic_prefix = \"mongo2pg_dbapi\"\nschema_registry_url = \"http://localhost:8081\"\n# schema_registry_username = \"\"\n# schema_registry_password = \"\"\noffset = \"latest\"\n# auto_offset_reset = \"earliest\" # legacy key still supported\n# max_messages = 1000\n# batch_log_messages = 100\n",
        "Mongo2Pg Project migration",
        args.project_base.display(),
        cluster_line,
        args.project_name,
        args.source_uri
            .as_deref()
            .map(|u| format!("\"{}\"", u.replace('"', "\\\"")))
            .unwrap_or_else(|| "\"mongodb://localhost:27017\"".to_owned()),
        namespace_line,
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

    info!(
        "Project '{}' initialised at {}",
        args.project_name,
        project_root.display()
    );
    for dir in &dirs {
        info!("{}", dir.display());
    }
    info!("{}", conf_path.display());
    Ok(())
}

fn append_non_empty_segment(path: &mut String, segment: Option<&str>) {
    if let Some(value) = segment.map(str::trim).filter(|value| !value.is_empty()) {
        if !path.is_empty() {
            path.push('/');
        }
        path.push_str(value);
    }
}

fn ensure_output_prefix_segments(prefix: &str, cluster_name: Option<&str>, project_dir: &str) -> String {
    let mut normalized = prefix.trim_matches('/').to_owned();

    let cluster_segment = cluster_name
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if let Some(cluster) = cluster_segment {
        let already_has_cluster = normalized.rsplit('/').next().is_some_and(|segment| segment == cluster)
            || normalized
                .rsplit('/')
                .nth(1)
                .is_some_and(|segment| segment == cluster);
        if !already_has_cluster {
            append_non_empty_segment(&mut normalized, Some(cluster));
        }
    }

    let project_segment = project_dir.trim_matches('/');
    if !project_segment.is_empty() {
        let already_has_project = normalized
            .rsplit('/')
            .next()
            .is_some_and(|segment| segment == project_segment);
        if !already_has_project {
            append_non_empty_segment(&mut normalized, Some(project_segment));
        }
    }

    normalized
}

fn import_table_name_from_csv_path(path: &Path) -> Option<String> {
    let file_name = path.file_name()?.to_str()?;
    if let Some(table) = file_name.strip_suffix(".csv.gz") {
        return Some(table.to_owned());
    }
    file_name.strip_suffix(".csv").map(str::to_owned)
}

fn is_supported_import_csv_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(".csv.gz") || name.ends_with(".csv"))
}

// ──────────────────────────────────────────────────────────────────────────────
// `export` subcommand
// ──────────────────────────────────────────────────────────────────────────────

fn sanitize_export_lookup_name(name: &str) -> String {
    let mut s = name.replace(|c: char| !c.is_ascii_alphanumeric() && c != '_', "_");
    if s.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        s = format!("_{s}");
    }
    s.to_lowercase()
}

fn resolve_export_sql_lookup_for_collection(
    coll: &str,
    tables_dir: &Path,
    collections_dir: &Path,
    sql_set: &std::collections::HashSet<String>,
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
        None
    }
}

fn plan_export_jobs_for_collections(
    collections: impl IntoIterator<Item = String>,
    tables_dir: &Path,
    collections_dir: &Path,
    sql_set: &std::collections::HashSet<String>,
) -> std::collections::HashMap<String, Vec<String>> {
    let mut export_jobs = std::collections::HashMap::<String, Vec<String>>::new();
    for coll in collections {
        if let Some(sql_lookup_name) =
            resolve_export_sql_lookup_for_collection(&coll, tables_dir, collections_dir, sql_set)
        {
            export_jobs.entry(sql_lookup_name).or_default().push(coll);
        }
    }
    export_jobs
}

async fn run_export(args: ExportArgs) -> Result<()> {
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

    // Use args.namespace if provided, else fall back to config file
    let db_name = args.namespace.clone().or(c.namespace.clone()).ok_or_else(|| {
        anyhow!("No NAMESPACE provided: pass --namespace or add NAMESPACE to the config file")
    })?;
    let export_chunk_size = resolve_export_chunk_size(args.chunk_size.or(c.chunk_size))?;
    let storage_backend = match resolve_export_write_backend(&c.base_dir)? {
        ExportWriteBackend::LocalFs => ExportWriteBackend::LocalFs,
        ExportWriteBackend::Gcs { bucket, prefix } => ExportWriteBackend::Gcs {
            bucket,
            prefix: ensure_output_prefix_segments(&prefix, c.cluster_name.as_deref(), &c.project_dir),
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
            tables_dir = project_root.join("schema").join("tables").join(&db_name);
            collections_dir = resolve_collections_dir(&project_root, &db_name);
        }
        ExportWriteBackend::Gcs { bucket, prefix } => {
            let Some(stage) =
                stage_export_metadata_from_gcs(
                    bucket,
                    prefix,
                    c.cluster_name.as_deref(),
                    &c.project_dir,
                    &db_name,
                )
                .await?
            else {
                return Err(anyhow!(
                    "Cannot stage export metadata from gs://{}/{}/schema/tables/{}",
                    bucket,
                    prefix.trim_matches('/'),
                    db_name
                ));
            };

            project_root = stage.path().to_path_buf();
            tables_dir = project_root.join("schema").join("tables").join(&db_name);
            collections_dir = resolve_collections_dir(&project_root, &db_name);
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

    let (data_dir, cleanup_staging_after_export) = match (&storage_backend, args.output_dir.clone()) {
        (_, Some(dir)) => (dir, false),
        (ExportWriteBackend::LocalFs, None) => (project_root.join("data"), false),
        (ExportWriteBackend::Gcs { .. }, None) => {
            let staging_dir = std::env::temp_dir().join(format!(
                "mongo2pg-gcs-stage-{}-{}",
                std::process::id(),
                Utc::now().timestamp_millis()
            ));
            info!(
                "export staging dir (temporary): {}",
                staging_dir.display()
            );
            (staging_dir, true)
        }
    };

    let client_options = ClientOptions::parse(&source_uri).await.with_context(|| {
        format!(
            "{}: failed to parse MongoDB SOURCE_URI",
            connection_failed_context("mongo", "connect")
        )
    })?;
    let client = Client::with_options(client_options).with_context(|| {
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
    use std::collections::{HashMap, HashSet};
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
            .database(&db_name)
            .list_collection_names()
            .await
            .with_context(|| {
                format!(
                    "{}: failed to list collections for database {db_name}",
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
                "[{}/{}] Exporting {db_name}.{} via {}.sql",
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
                    "-> member [{}/{}]: {db_name}.{} -> {}.sql",
                    member_index + 1,
                    coll_names.len(),
                    coll_name,
                    sql_lookup_name
                );
            }
        }

        match export_collections_to_sql(
            &client,
            &db_name,
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
                let job_label = format!("{}.{}", db_name, sql_lookup_name);
                warn!("  warning: export failed for {}: {}", job_label, e);
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

fn resolve_collections_dir(project_root: &Path, db_name: &str) -> std::path::PathBuf {
    let collections_root = project_root.join("source").join("collections");
    if collections_root.join(db_name).is_dir() {
        collections_root.join(db_name)
    } else {
        collections_root
    }
}

fn resolve_local_project_root_from_config(
    conf_path: &Path,
    conf_data: &mongo2pg::util::ConfData,
) -> PathBuf {
    let storage_backend =
        resolve_export_write_backend(&conf_data.base_dir).unwrap_or(ExportWriteBackend::LocalFs);

    match storage_backend {
        ExportWriteBackend::LocalFs => configured_project_root(conf_data),
        ExportWriteBackend::Gcs { prefix, .. } => {
            let current_dir = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
            let conf_abs = std::fs::canonicalize(conf_path).unwrap_or_else(|_| {
                if conf_path.is_absolute() {
                    conf_path.to_path_buf()
                } else {
                    current_dir.join(conf_path)
                }
            });

            let from_config = conf_abs
                .parent()
                .and_then(|p| p.parent())
                .map(|p| p.to_path_buf());
            let from_cwd_project = if let Some(cluster_name) = conf_data
                .cluster_name
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                current_dir.join(cluster_name).join(&conf_data.project_dir)
            } else {
                current_dir.join(&conf_data.project_dir)
            };
            let from_config_parent_with_prefix = from_config.as_ref().and_then(|root| {
                root.parent().map(|p| {
                    let effective_prefix = ensure_output_prefix_segments(
                        &prefix,
                        conf_data.cluster_name.as_deref(),
                        &conf_data.project_dir,
                    );
                    p.join(effective_prefix)
                })
            });

            let has_project_layout = |root: &PathBuf| {
                root.join("source").is_dir()
                    || root.join("schema").is_dir()
                    || root.join("reports").is_dir()
            };

            from_config_parent_with_prefix
                .iter()
                .chain(from_config.iter())
                .chain(std::iter::once(&from_cwd_project))
                .find(|root| has_project_layout(root))
                .cloned()
                .or(from_config_parent_with_prefix)
                .or(from_config)
                .unwrap_or(from_cwd_project)
        }
    }
}

fn gcs_prefix_candidates_for_import_data(
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
                candidates.push(format!("{trimmed_prefix}/{project_segment}/data/{db_name}/"));
            }
        }
    }

    candidates.sort();
    candidates.dedup();
    candidates
}

async fn download_gcs_prefix_to_local_dir(
    bucket: &str,
    object_prefix: &str,
    local_dir: &Path,
) -> Result<usize> {
    debug!(
        "[gcs-debug] metadata stage start: gs://{}/{} -> {}",
        bucket,
        object_prefix,
        local_dir.display()
    );
    let storage = Storage::builder()
        .build()
        .await
        .with_context(|| format!("Failed to initialize GCS data client for gs://{bucket}"))?;
    let storage_control = StorageControl::builder().build().await.with_context(|| {
        format!("Failed to initialize GCS control client for gs://{bucket}")
    })?;
    let bucket_resource = format!("projects/_/buckets/{bucket}");

    let mut downloaded = 0usize;
    let mut page_token = String::new();
    loop {
        let mut request = storage_control
            .list_objects()
            .set_parent(bucket_resource.clone())
            .set_prefix(object_prefix.to_owned());
        if !page_token.is_empty() {
            request = request.set_page_token(page_token.clone());
        }

        let page = request
            .send()
            .await
            .with_context(|| format!("Failed while listing gs://{}/{}", bucket, object_prefix))?;

        for object in page.objects {
            let Some(relative) = object
                .name
                .strip_prefix(object_prefix)
                .map(|path| path.trim_start_matches('/'))
            else {
                continue;
            };
            if relative.is_empty() {
                continue;
            }

            let destination = local_dir.join(relative);
            if let Some(parent) = destination.parent() {
                tokio::fs::create_dir_all(parent).await.with_context(|| {
                    format!("Cannot create local staging directory {}", parent.display())
                })?;
            }

            let mut reader = storage
                .read_object(bucket_resource.clone(), object.name.clone())
                .send()
                .await
                .with_context(|| format!("Failed to open gs://{}/{}", bucket, object.name))?;
            let mut bytes = Vec::new();
            while let Some(chunk) = reader.next().await {
                let chunk = chunk.with_context(|| {
                    format!("Failed reading gs://{}/{}", bucket, object.name)
                })?;
                bytes.extend_from_slice(&chunk);
            }

            tokio::fs::write(&destination, bytes).await.with_context(|| {
                format!("Cannot write staged import file {}", destination.display())
            })?;
            downloaded += 1;
        }

        if page.next_page_token.is_empty() {
            break;
        }
        page_token = page.next_page_token;
    }

    debug!(
        "[gcs-debug] metadata stage done: downloaded_files={} from gs://{}/{}",
        downloaded,
        bucket,
        object_prefix
    );

    Ok(downloaded)
}

fn gcs_prefix_candidates_for_project_subdir(prefix: &str, project_dir: &str, subdir: &str) -> Vec<String> {
    let trimmed_prefix = prefix.trim_matches('/');
    let project_segment = project_dir.trim_matches('/');
    let subdir_segment = subdir.trim_matches('/');
    let mut candidates = Vec::new();

    let effective_prefix = ensure_output_prefix_segments(prefix, None, project_dir);
    if !effective_prefix.is_empty() {
        candidates.push(format!("{effective_prefix}/{subdir_segment}/"));
    }

    if trimmed_prefix.is_empty() {
        candidates.push(format!("{subdir_segment}/"));
        if !project_segment.is_empty() {
            candidates.push(format!("{project_segment}/{subdir_segment}/"));
        }
    } else {
        candidates.push(format!("{trimmed_prefix}/{subdir_segment}/"));
        if !project_segment.is_empty() {
            let ends_with_project = trimmed_prefix
                .split('/')
                .next_back()
                .is_some_and(|last| last == project_segment);
            if !ends_with_project {
                candidates.push(format!("{trimmed_prefix}/{project_segment}/{subdir_segment}/"));
            }
        }
    }

    candidates.sort();
    candidates.dedup();
    candidates
}

async fn stage_export_metadata_from_gcs(
    bucket: &str,
    prefix: &str,
    cluster_name: Option<&str>,
    project_dir: &str,
    db_name: &str,
) -> Result<Option<tempfile::TempDir>> {
    ensure_gcs_authentication().await?;

    let stage = tempfile::Builder::new()
        .prefix("mongo2pg-gcs-export-meta-")
        .tempdir()
        .context("Cannot create temporary export metadata staging directory")?;

    let staged_tables_dir = stage
        .path()
        .join("schema")
        .join("tables")
        .join(db_name);
    std::fs::create_dir_all(&staged_tables_dir).with_context(|| {
        format!(
            "Cannot create staged schema directory {}",
            staged_tables_dir.display()
        )
    })?;

    let mut staged_tables = 0usize;
    for candidate in gcs_prefix_candidates_for_project_subdir(
        &ensure_output_prefix_segments(prefix, cluster_name, project_dir),
        project_dir,
        &format!("schema/tables/{db_name}"),
    ) {
        let count = download_gcs_prefix_to_local_dir(bucket, &candidate, &staged_tables_dir)
            .await
            .with_context(|| {
                format!(
                    "Failed to stage schema metadata from gs://{}/{}",
                    bucket, candidate
                )
            })?;
        if count > 0 {
            staged_tables = count;
            info!(
                "staged {} schema files from gs://{}/{} into {}",
                count,
                bucket,
                candidate,
                staged_tables_dir.display()
            );
            break;
        }
    }

    if staged_tables == 0 {
        return Ok(None);
    }

    let staged_collections_root = stage.path().join("source").join("collections");
    std::fs::create_dir_all(&staged_collections_root).with_context(|| {
        format!(
            "Cannot create staged collections directory {}",
            staged_collections_root.display()
        )
    })?;

    let collection_candidates = [
        format!("source/collections/{db_name}"),
        "source/collections".to_owned(),
    ];
    for subdir in collection_candidates {
        for candidate in gcs_prefix_candidates_for_project_subdir(
            &ensure_output_prefix_segments(prefix, cluster_name, project_dir),
            project_dir,
            &subdir,
        ) {
            let target_dir = if subdir.ends_with(&format!("/{db_name}")) {
                staged_collections_root.join(db_name)
            } else {
                staged_collections_root.clone()
            };
            let count = download_gcs_prefix_to_local_dir(bucket, &candidate, &target_dir)
                .await
                .with_context(|| {
                    format!(
                        "Failed to stage collection mappings from gs://{}/{}",
                        bucket, candidate
                    )
                })?;
            if count > 0 {
                info!(
                    "staged {} collection mapping files from gs://{}/{} into {}",
                    count,
                    bucket,
                    candidate,
                    target_dir.display()
                );
                break;
            }
        }
    }

    Ok(Some(stage))
}

async fn run_import(args: ImportArgs) -> Result<()> {
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

    let storage_backend =
        resolve_export_write_backend(&c.base_dir).unwrap_or(ExportWriteBackend::LocalFs);
    let project_root = match &storage_backend {
        ExportWriteBackend::LocalFs => configured_project_root(&c),
        ExportWriteBackend::Gcs { .. } => {
            let local_root = resolve_local_project_root_from_config(&args.config, &c);
            info!(
                "import metadata root (local, read-only): {}",
                local_root.display()
            );
            local_root
        }
    };
    let tables_root = project_root.join("schema").join("tables");
    let tables_dir = if tables_root.join(db_name).is_dir() {
        tables_root.join(db_name)
    } else {
        tables_root.clone()
    };
    let data_root = project_root.join("data");
    let mut data_db_dir = if data_root.join(db_name).is_dir() {
        data_root.join(db_name)
    } else {
        data_root.clone()
    };
    let mut import_data_stage: Option<tempfile::TempDir> = None;

    if !data_db_dir.is_dir() {
        if let ExportWriteBackend::Gcs { bucket, prefix } = &storage_backend {
            ensure_gcs_authentication().await?;

            let stage = tempfile::Builder::new()
                .prefix("mongo2pg-gcs-import-stage-")
                .tempdir()
                .context("Cannot create temporary import staging directory")?;
            let staged_data_root = stage.path().join("data").join(db_name);
            std::fs::create_dir_all(&staged_data_root).with_context(|| {
                format!("Cannot create staged import data directory {}", staged_data_root.display())
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
                let count = download_gcs_prefix_to_local_dir(bucket, &effective_prefix, &staged_data_root)
                    .await
                    .with_context(|| {
                        format!(
                            "Failed to stage import data from gs://{}/{}",
                            bucket, effective_prefix
                        )
                    })?;
                if count > 0 {
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
            }

            if downloaded > 0 {
                data_db_dir = staged_data_root;
                import_data_stage = Some(stage);
            }
        }
    }

    if !tables_dir.is_dir() {
        return Err(anyhow!(
            "Cannot read SQL tables directory {}",
            tables_dir.display()
        ));
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

    use std::collections::{HashMap, HashSet};
    let mut allowed_table_names: HashSet<String> = HashSet::new();
    let mut table_columns_by_name: HashMap<String, Vec<String>> = HashMap::new();

    for sql_path in &sql_files {
        let sql = std::fs::read_to_string(sql_path)
            .with_context(|| format!("Failed to read {}", sql_path.display()))?;
        let executable_sql = strip_psql_preamble(&sql);
        if executable_sql.trim().is_empty() {
            continue;
        }
        for table in parse_sql(&executable_sql) {
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

    let mut csv_candidates: Vec<PathBuf> = std::fs::read_dir(&data_db_dir)
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
                .filter(|path| is_supported_import_csv_path(path))
                .filter(|path| {
                    import_table_name_from_csv_path(path)
                        .map(|table_name| allowed_table_names.contains(&table_name))
                        .unwrap_or(false)
                })
                .collect::<Vec<_>>()
        })
        .collect();

    csv_candidates.sort();
    let mut csv_files_by_table: std::collections::HashMap<String, PathBuf> =
        std::collections::HashMap::new();
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
        return Err(anyhow!(
            "No .csv or .csv.gz files found in {}",
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
        let table = import_table_name_from_csv_path(csv_path.as_path())
            .ok_or_else(|| anyhow!("Cannot derive table name from {}", csv_path.display()))?;
        let truncate_sql = format!(
            "TRUNCATE TABLE {}.{} CASCADE",
            quote_ident(schema),
            quote_ident(&table)
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
        let table = import_table_name_from_csv_path(csv_path.as_path())
            .ok_or_else(|| anyhow!("Cannot derive table name from {}", csv_path.display()))?;
        let table_columns = table_columns_by_name
            .get(&table)
            .ok_or_else(|| anyhow!("No DDL column metadata found for table {table}"))?;
        let copy_columns = table_columns
            .iter()
            .map(|column| quote_ident(&column.trim_matches('"').replace("\"\"", "\"")))
            .collect::<Vec<_>>()
            .join(", ");
        let copy_sql = format!(
            "COPY {}.{} ({}) FROM STDIN WITH (FORMAT csv, HEADER true)",
            quote_ident(schema),
            quote_ident(&table),
            copy_columns,
        );
        let mut contents = Vec::new();
        let is_gz = csv_path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".csv.gz"));
        if is_gz {
            let file = std::fs::File::open(csv_path)
                .with_context(|| format!("Failed to open {}", csv_path.display()))?;
            let mut decoder = GzDecoder::new(file);
            std::io::Read::read_to_end(&mut decoder, &mut contents)
                .with_context(|| format!("Failed to decompress {}", csv_path.display()))?;
        } else {
            contents = std::fs::read(csv_path)
                .with_context(|| format!("Failed to read {}", csv_path.display()))?;
        }
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
        info!(
            "Imported {rows} row(s) into {}.{} from {}",
            schema,
            table,
            csv_path.display()
        );
    }

    transaction.commit().await?;
    info!("Import completed for database '{target_database_name}'.");

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

async fn run_kafka_import(args: KafkaImportArgs) -> Result<()> {
    async fn publish_to_dlq(
        producer: &FutureProducer,
        source_topic: &str,
        key: Option<&[u8]>,
        payload: &[u8],
    ) -> Result<()> {
        let dlq_topic = format!("dlq_{source_topic}");
        let mut record = FutureRecord::to(&dlq_topic).payload(payload);
        if let Some(key) = key {
            record = record.key(key);
        }

        producer
            .send(record, Duration::from_secs(5))
            .await
            .map_err(|(err, _)| anyhow!("DLQ publish failed for topic {dlq_topic}: {err}"))?;

        Ok(())
    }

    #[derive(Deserialize)]
    struct SchemaRegistrySchemaResponse {
        schema: String,
    }

    fn avro_to_json(value: AvroValue) -> Value {
        match value {
            AvroValue::Null => Value::Null,
            AvroValue::Boolean(v) => Value::Bool(v),
            AvroValue::Int(v) => Value::Number(v.into()),
            AvroValue::Long(v) => Value::Number(v.into()),
            AvroValue::Float(v) => serde_json::Number::from_f64(v as f64)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            AvroValue::Double(v) => serde_json::Number::from_f64(v)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            AvroValue::String(v) => Value::String(v),
            AvroValue::Array(values) => {
                Value::Array(values.into_iter().map(avro_to_json).collect())
            }
            AvroValue::Map(map) => Value::Object(
                map.into_iter()
                    .map(|(k, v)| (k, avro_to_json(v)))
                    .collect::<serde_json::Map<String, Value>>(),
            ),
            AvroValue::Union(_, boxed) => avro_to_json(*boxed),
            AvroValue::Record(fields) => Value::Object(
                fields
                    .into_iter()
                    .map(|(k, v)| (k, avro_to_json(v)))
                    .collect::<serde_json::Map<String, Value>>(),
            ),
            AvroValue::Enum(_, symbol) => Value::String(symbol),
            AvroValue::Uuid(v) => Value::String(v.to_string()),
            AvroValue::Decimal(v) => Value::String(format!("{v:?}")),
            AvroValue::BigDecimal(v) => Value::String(v.to_string()),
            _ => Value::Null,
        }
    }

    async fn fetch_schema_by_id(
        client: &reqwest::Client,
        schema_registry_url: &str,
        schema_registry_username: Option<&str>,
        schema_registry_password: Option<&str>,
        schema_id: u32,
    ) -> Result<Schema> {
        let mut request = client.get(format!(
            "{}/schemas/ids/{}",
            schema_registry_url.trim_end_matches('/'),
            schema_id
        ));
        if let Some(username) = schema_registry_username {
            request = request.basic_auth(username, schema_registry_password.map(|s| s.to_owned()));
        }
        let response = request
            .send()
            .await
            .with_context(|| format!("Failed to fetch schema id {schema_id} from Schema Registry"))?
            .error_for_status()
            .with_context(|| format!("Schema Registry returned error for schema id {schema_id}"))?;
        let payload: SchemaRegistrySchemaResponse = response
            .json()
            .await
            .with_context(|| "Failed to parse Schema Registry schema response")?;
        Schema::parse_str(&payload.schema)
            .with_context(|| format!("Failed to parse Avro schema id {schema_id}"))
    }

    async fn decode_message_value(
        bytes: &[u8],
        schema_registry_url: Option<&str>,
        schema_registry_username: Option<&str>,
        schema_registry_password: Option<&str>,
        http_client: &reqwest::Client,
        schema_cache: &mut HashMap<u32, Schema>,
    ) -> Result<Value> {
        if bytes.len() > 5 && bytes[0] == 0 {
            let schema_id = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
            let schema = if let Some(schema) = schema_cache.get(&schema_id) {
                schema.clone()
            } else {
                let url = schema_registry_url.ok_or_else(|| {
                    anyhow!(
                        "Confluent framing detected but kafka.schema_registry_url is not configured"
                    )
                })?;
                let parsed = fetch_schema_by_id(
                    http_client,
                    url,
                    schema_registry_username,
                    schema_registry_password,
                    schema_id,
                )
                .await?;
                schema_cache.insert(schema_id, parsed.clone());
                parsed
            };

            let mut slice: &[u8] = &bytes[5..];
            let avro = from_avro_datum(&schema, &mut slice, None)
                .with_context(|| "Failed to decode Avro payload")?;
            return Ok(avro_to_json(avro));
        }

        serde_json::from_slice::<Value>(bytes)
            .with_context(|| "Failed to decode message as JSON payload")
    }

    fn parse_topic_db_collection(
        topic: &str,
        topic_prefix: Option<&str>,
        default_db_name: Option<&str>,
    ) -> Option<(String, String)> {
        let mut effective = topic;
        if let Some(prefix) = topic_prefix {
            let prefix_with_dot = format!("{prefix}.");
            if !topic.starts_with(&prefix_with_dot) {
                return None;
            }
            effective = &topic[prefix_with_dot.len()..];
        }
        let segments = effective.split('.').collect::<Vec<_>>();
        if segments.len() >= 2 {
            return Some((
                segments[segments.len() - 2].to_owned(),
                segments[segments.len() - 1].to_owned(),
            ));
        }

        // Some deployments set topic_prefix to include db name already
        // (for example mongo2pg.sample_analytics), so only <collection>
        // remains after prefix trimming. In that case fall back to configured
        // namespace database for the db segment.
        if segments.len() == 1 {
            if let Some(db_name) = default_db_name {
                return Some((db_name.to_owned(), segments[0].to_owned()));
            }
        }

        None
    }

    fn mongo_path_segments(mongo_path: Option<&str>) -> Vec<&str> {
        mongo_path
            .unwrap_or(".")
            .trim()
            .trim_start_matches('.')
            .split('.')
            .filter(|segment| !segment.is_empty())
            .collect()
    }

    fn values_at_path<'a>(value: &'a Value, segments: &[&str]) -> Vec<&'a Value> {
        let mut current = vec![value];
        for segment in segments {
            let mut next = Vec::new();
            for item in current {
                match item {
                    Value::Object(map) => {
                        if let Some(child) = map.get(*segment) {
                            match child {
                                Value::Array(arr) => {
                                    for val in arr {
                                        next.push(val);
                                    }
                                }
                                _ => next.push(child),
                            }
                        }
                    }
                    Value::Array(arr) => {
                        for entry in arr {
                            if let Value::Object(obj) = entry {
                                if let Some(child) = obj.get(*segment) {
                                    match child {
                                        Value::Array(inner) => {
                                            for val in inner {
                                                next.push(val);
                                            }
                                        }
                                        _ => next.push(child),
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            current = next;
            if current.is_empty() {
                break;
            }
        }
        current
    }

    fn find_value_in_nested_json_object<'a>(
        obj: &'a serde_json::Map<String, Value>,
        source_field: &str,
    ) -> Option<&'a Value> {
        if let Some(value) = obj.get(source_field) {
            return Some(value);
        }

        fn visit<'a>(value: &'a Value, source_field: &str, out: &mut Vec<&'a Value>) {
            match value {
                Value::Object(map) => {
                    if let Some(found) = map.get(source_field) {
                        out.push(found);
                    }
                    for child in map.values() {
                        visit(child, source_field, out);
                    }
                }
                Value::Array(items) => {
                    for item in items {
                        visit(item, source_field, out);
                    }
                }
                _ => {}
            }
        }

        let mut matches = Vec::new();
        for value in obj.values() {
            visit(value, source_field, &mut matches);
            if matches.len() > 1 {
                break;
            }
        }

        if matches.len() == 1 {
            Some(matches[0])
        } else {
            None
        }
    }

    fn format_table_insert_exec_summary(table_insert_execs: &HashMap<String, u64>) -> String {
        if table_insert_execs.is_empty() {
            return "none".to_owned();
        }

        let mut entries = table_insert_execs
            .iter()
            .map(|(table, count)| (table.clone(), *count))
            .collect::<Vec<_>>();
        entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

        entries
            .into_iter()
            .map(|(table, count)| format!("{table}:{count}"))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn escape_sql_string(raw: &str) -> String {
        raw.replace('\'', "''")
    }

    fn unwrap_union_tagged_value(value: &Value) -> &Value {
        const UNION_TAGS: [&str; 12] = [
            "null", "string", "boolean", "int", "long", "float", "double", "bytes", "array", "map",
            "record", "enum",
        ];

        let mut current = value;
        loop {
            let Value::Object(obj) = current else {
                break;
            };
            if obj.len() != 1 {
                break;
            }
            let Some((tag, inner)) = obj.iter().next() else {
                break;
            };
            if UNION_TAGS.contains(&tag.as_str()) {
                current = inner;
            } else {
                break;
            }
        }
        current
    }

    fn normalized_sql_type(sql_type: Option<&str>) -> Option<String> {
        let raw = sql_type?.trim();
        if raw.is_empty() {
            return None;
        }

        let lowered = raw.to_ascii_lowercase();
        let mut cut = raw.len();
        for marker in [
            " default ",
            " primary key",
            " not null",
            " null",
            " references ",
            " check ",
            " unique",
        ] {
            if let Some(idx) = lowered.find(marker) {
                cut = cut.min(idx);
            }
        }

        let base = raw[..cut].trim();
        if base.is_empty() {
            None
        } else {
            Some(base.to_owned())
        }
    }

    fn cast_string_literal(raw: &str, sql_type: Option<&str>) -> String {
        let escaped = escape_sql_string(raw);
        if let Some(sql_type) = normalized_sql_type(sql_type) {
            let normalized = match sql_type.trim().to_ascii_lowercase().as_str() {
                "smallserial" | "serial2" => "SMALLINT",
                "serial" | "serial4" => "INTEGER",
                "bigserial" | "serial8" => "BIGINT",
                _ => &sql_type,
            };
            format!("CAST('{escaped}' AS {normalized})")
        } else {
            format!("'{escaped}'")
        }
    }

    fn sql_type_expects_temporal(sql_type: Option<&str>) -> bool {
        let Some(normalized) = normalized_sql_type(sql_type) else {
            return false;
        };
        let lower = normalized.trim().to_ascii_lowercase();
        lower.starts_with("timestamp") || lower == "date" || lower.starts_with("time")
    }

    fn cast_epoch_seconds_literal(seconds_expr: &str, sql_type: Option<&str>) -> String {
        if let Some(target_type) = normalized_sql_type(sql_type) {
            format!("CAST(to_timestamp({seconds_expr}) AS {target_type})")
        } else {
            format!("to_timestamp({seconds_expr})")
        }
    }

    fn temporal_literal_from_epoch_i64(value: i64, sql_type: Option<&str>) -> String {
        let seconds_expr = if value.unsigned_abs() >= 100_000_000_000 {
            format!("{value}::double precision / 1000.0")
        } else {
            format!("{value}::double precision")
        };
        cast_epoch_seconds_literal(&seconds_expr, sql_type)
    }

    fn temporal_literal_from_epoch_u64(value: u64, sql_type: Option<&str>) -> String {
        let seconds_expr = if value >= 100_000_000_000 {
            format!("{value}::double precision / 1000.0")
        } else {
            format!("{value}::double precision")
        };
        cast_epoch_seconds_literal(&seconds_expr, sql_type)
    }

    fn temporal_literal_from_epoch_f64(value: f64, sql_type: Option<&str>) -> String {
        let seconds_expr = if value.abs() >= 100_000_000_000.0 {
            format!("{value} / 1000.0")
        } else {
            value.to_string()
        };
        cast_epoch_seconds_literal(&seconds_expr, sql_type)
    }

    fn temporal_literal_from_epoch_millis_i64(value_ms: i64, sql_type: Option<&str>) -> String {
        // Value is milliseconds since epoch; convert to seconds with fractional part.
        let seconds_expr = format!("{value_ms}::double precision / 1000.0");
        cast_epoch_seconds_literal(&seconds_expr, sql_type)
    }

    fn temporal_literal_from_number(
        raw: &serde_json::Number,
        sql_type: Option<&str>,
    ) -> Option<String> {
        raw.as_i64()
            .map(|v| temporal_literal_from_epoch_i64(v, sql_type))
            .or_else(|| {
                raw.as_u64()
                    .map(|v| temporal_literal_from_epoch_u64(v, sql_type))
            })
            .or_else(|| {
                raw.as_f64()
                    .map(|v| temporal_literal_from_epoch_f64(v, sql_type))
            })
    }

    fn geojson_point_coordinates(value: &Value) -> Option<(f64, f64)> {
        match value {
            Value::String(raw) => serde_json::from_str::<Value>(raw)
                .ok()
                .and_then(|parsed| geojson_point_coordinates(&parsed)),
            Value::Object(obj) => {
                let point_type = obj.get("type")?.as_str()?;
                if !point_type.eq_ignore_ascii_case("point") {
                    return None;
                }
                let coords = obj.get("coordinates")?.as_array()?;
                if coords.len() != 2 {
                    return None;
                }
                let lon = coords[0].as_f64()?;
                let lat = coords[1].as_f64()?;
                Some((lon, lat))
            }
            _ => None,
        }
    }

    // Map Debezium/Mongo Extended JSON wrappers to PostgreSQL SQL literals.
    // Keep this centralized so adding new wrappers is easy.
    fn map_extended_json_literal(value: &Value, sql_type: Option<&str>) -> Option<String> {
        if sql_type_expects_temporal(sql_type) {
            if let Value::Number(raw) = value {
                return temporal_literal_from_number(raw, sql_type);
            }
        }

        let obj = value.as_object()?;

        if sql_type_expects_temporal(sql_type) {
            if let Some(Value::Number(raw)) = obj
                .get("long")
                .or_else(|| obj.get("int"))
                .or_else(|| obj.get("double"))
            {
                return temporal_literal_from_number(raw, sql_type);
            }
            if let Some(Value::String(raw)) = obj.get("string") {
                if let Ok(number) = raw.parse::<f64>() {
                    return Some(temporal_literal_from_epoch_f64(number, sql_type));
                }
            }
        }

        let normalized = normalized_sql_type(sql_type);

        if let Some(Value::String(raw)) = obj.get("$oid") {
            if normalized
                .as_deref()
                .map(|t| t.eq_ignore_ascii_case("uuid"))
                .unwrap_or(false)
            {
                return objectid_hex_to_uuid(raw)
                    .map(|uuid| cast_string_literal(&uuid, sql_type))
                    .or_else(|| Some(cast_string_literal(raw, sql_type)));
            }
            return Some(cast_string_literal(raw, sql_type));
        }

        if let Some(Value::String(raw)) = obj
            .get("$numberDecimal")
            .or_else(|| obj.get("$numberDouble"))
            .or_else(|| obj.get("$numberLong"))
            .or_else(|| obj.get("$numberInt"))
        {
            return Some(cast_string_literal(raw, sql_type));
        }

        if let Some(date_value) = obj.get("$date") {
            return match date_value {
                Value::String(raw) => Some(cast_string_literal(raw, sql_type)),
                // Debezium / Mongo typically represent $date as milliseconds since epoch.
                // Treat numeric $date values as milliseconds to avoid misclassification
                // by heuristics and preserve historical (pre-1970) dates correctly.
                Value::Number(raw) => {
                    if let Some(ms) = raw.as_i64() {
                        Some(temporal_literal_from_epoch_millis_i64(ms, sql_type))
                    } else if let Some(msu) = raw.as_u64() {
                        // safe cast for typical schema values
                        Some(temporal_literal_from_epoch_millis_i64(msu as i64, sql_type))
                    } else if let Some(msf) = raw.as_f64() {
                        // fallback: treat float as milliseconds
                        Some(temporal_literal_from_epoch_f64(msf, sql_type))
                    } else {
                        None
                    }
                }
                Value::Object(inner) => {
                    if let Some(Value::String(ms_raw)) = inner.get("$numberLong") {
                        ms_raw
                            .parse::<i64>()
                            .ok()
                            .map(|ms| temporal_literal_from_epoch_millis_i64(ms, sql_type))
                    } else {
                        None
                    }
                }
                _ => None,
            };
        }

        None
    }

    fn sql_type_expects_numeric(sql_type: Option<&str>) -> bool {
        let Some(normalized) = normalized_sql_type(sql_type) else {
            return false;
        };
        let lower = normalized.trim().to_ascii_lowercase();
        lower.starts_with("smallint")
            || lower.starts_with("integer")
            || lower.starts_with("bigint")
            || lower.starts_with("serial")
            || lower.starts_with("smallserial")
            || lower.starts_with("bigserial")
            || lower.starts_with("numeric")
            || lower.starts_with("decimal")
            || lower.starts_with("real")
            || lower.starts_with("double precision")
    }

    fn is_object_id_wrapper(value: &Value) -> bool {
        let value = unwrap_union_tagged_value(value);
        value
            .as_object()
            .and_then(|obj| obj.get("$oid"))
            .and_then(Value::as_str)
            .is_some()
    }

    fn validate_extended_json_compatibility(
        value: Option<&Value>,
        sql_type: Option<&str>,
        source_field: &str,
        target_field: &str,
        table_name: &str,
    ) -> Result<()> {
        let Some(value) = value.map(unwrap_union_tagged_value) else {
            return Ok(());
        };
        if is_object_id_wrapper(value) && sql_type_expects_numeric(sql_type) {
            let sql_type_name = sql_type.unwrap_or("<unknown>");
            let sample = serde_json::to_string(value)
                .unwrap_or_else(|_| "{\"$oid\":\"<invalid>\"}".to_owned());
            return Err(anyhow!(
                "incompatible ObjectId mapping: source_field={} target_field={} table={} sql_type={} value={} (ObjectId cannot cast to numeric). Update mapping to use numeric source field (example: theaterId) or change target column type to uuid/text",
                source_field,
                target_field,
                table_name,
                sql_type_name,
                sample,
            ));
        }
        Ok(())
    }

    fn singular_collection_name(collection_name: &str) -> String {
        let trimmed = collection_name.trim();
        if trimmed.ends_with("ies") && trimmed.len() > 3 {
            return format!("{}y", &trimmed[..trimmed.len() - 3]);
        }
        if trimmed.ends_with('s') && trimmed.len() > 1 {
            return trimmed[..trimmed.len() - 1].to_owned();
        }
        trimmed.to_owned()
    }

    fn candidate_numeric_root_id_source_fields(collection_name: &str) -> Vec<String> {
        let mut fields = Vec::new();
        let singular = singular_collection_name(collection_name);
        if !singular.is_empty() {
            fields.push(format!("{singular}Id"));
        }
        fields.push("id".to_owned());
        fields
    }

    fn value_is_numeric_compatible(value: &Value) -> bool {
        match value {
            Value::Number(_) => true,
            Value::String(raw) => raw.parse::<i128>().is_ok() || raw.parse::<f64>().is_ok(),
            Value::Object(obj) => {
                obj.get("$numberLong")
                    .and_then(Value::as_str)
                    .map(|raw| raw.parse::<i128>().is_ok())
                    .unwrap_or(false)
                    || obj
                        .get("$numberInt")
                        .and_then(Value::as_str)
                        .map(|raw| raw.parse::<i128>().is_ok())
                        .unwrap_or(false)
                    || obj
                        .get("$numberDouble")
                        .and_then(Value::as_str)
                        .map(|raw| raw.parse::<f64>().is_ok())
                        .unwrap_or(false)
                    || obj
                        .get("$numberDecimal")
                        .and_then(Value::as_str)
                        .map(|raw| raw.parse::<f64>().is_ok())
                        .unwrap_or(false)
            }
            _ => false,
        }
    }

    fn resolve_value_with_numeric_id_fallback<'a>(
        payload_obj: &'a serde_json::Map<String, Value>,
        mapping: &CollectionMapping,
        source_field: &str,
        target_field: &str,
        sql_type: Option<&str>,
        raw_value: Option<&'a Value>,
    ) -> (Option<&'a Value>, String) {
        let needs_fallback = target_field == "id"
            && source_field == "_id"
            && sql_type_expects_numeric(sql_type)
            && raw_value.map(is_object_id_wrapper).unwrap_or(false);

        if !needs_fallback {
            return (raw_value, source_field.to_owned());
        }

        for candidate in candidate_numeric_root_id_source_fields(&mapping.collection_name) {
            if let Some(value) = payload_obj.get(&candidate) {
                if value_is_numeric_compatible(value) {
                    return (Some(value), candidate);
                }
            }
        }

        (raw_value, source_field.to_owned())
    }

    fn sql_literal(value: Option<&Value>, sql_type: Option<&str>) -> String {
        let Some(value) = value.map(unwrap_union_tagged_value) else {
            return "NULL".to_owned();
        };

        match value {
            Value::Null => "NULL".to_owned(),
            Value::Bool(v) => {
                if *v {
                    "TRUE".to_owned()
                } else {
                    "FALSE".to_owned()
                }
            }
            Value::Number(v) => {
                map_extended_json_literal(value, sql_type).unwrap_or_else(|| v.to_string())
            }
            Value::String(v) => {
                let normalized = normalized_sql_type(sql_type);
                if normalized
                    .as_deref()
                    .map(|t| t.to_ascii_lowercase().starts_with("geometry"))
                    .unwrap_or(false)
                {
                    if let Some((lon, lat)) = geojson_point_coordinates(value) {
                        return format!("ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)");
                    }
                }
                if normalized
                    .as_deref()
                    .map(|t| t.eq_ignore_ascii_case("uuid"))
                    .unwrap_or(false)
                {
                    if let Some(uuid) = objectid_hex_to_uuid(v) {
                        return cast_string_literal(&uuid, sql_type);
                    }
                }
                format!("'{}'", escape_sql_string(v))
            }
            Value::Array(items) => {
                let elements = items
                    .iter()
                    .map(|item| match item {
                        Value::Null => "NULL".to_owned(),
                        Value::Bool(v) => {
                            if *v {
                                "TRUE".to_owned()
                            } else {
                                "FALSE".to_owned()
                            }
                        }
                        Value::Number(v) => v.to_string(),
                        Value::String(v) => format!("'{}'", escape_sql_string(v)),
                        other => format!("'{}'", escape_sql_string(&other.to_string())),
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                if let Some(array_type) = normalized_sql_type(sql_type).filter(|t| t.contains("[]"))
                {
                    format!("ARRAY[{elements}]::{array_type}")
                } else {
                    format!("'{}'", escape_sql_string(&value.to_string()))
                }
            }
            Value::Object(_) => {
                if normalized_sql_type(sql_type)
                    .as_deref()
                    .map(|t| t.to_ascii_lowercase().starts_with("geometry"))
                    .unwrap_or(false)
                {
                    if let Some((lon, lat)) = geojson_point_coordinates(value) {
                        return format!("ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)");
                    }
                }
                if let Some(mapped) = map_extended_json_literal(value, sql_type) {
                    return mapped;
                }
                let as_json = value.to_string();
                match normalized_sql_type(sql_type) {
                    Some(t) if t.to_ascii_lowercase().contains("json") => {
                        format!("'{}'::{t}", escape_sql_string(&as_json))
                    }
                    _ => format!("'{}'", escape_sql_string(&as_json)),
                }
            }
        }
    }

    fn varchar_limit(sql_type: Option<&str>) -> Option<usize> {
        let sql = normalized_sql_type(sql_type)?.to_ascii_lowercase();
        let prefix = if sql.starts_with("varchar(") {
            "varchar("
        } else if sql.starts_with("character varying(") {
            "character varying("
        } else {
            return None;
        };
        let rest = &sql[prefix.len()..];
        let end = rest.find(')')?;
        rest[..end].trim().parse::<usize>().ok()
    }

    fn validate_varchar_value(
        value: Option<&Value>,
        sql_type: Option<&str>,
        source_field: &str,
        target_field: &str,
        table_name: &str,
    ) -> Result<()> {
        let value = value.map(unwrap_union_tagged_value);
        let Some(limit) = varchar_limit(sql_type) else {
            return Ok(());
        };
        let Some(Value::String(text)) = value else {
            return Ok(());
        };
        let value_len = text.chars().count();
        if value_len > limit {
            return Err(anyhow!(
                "value exceeds varchar limit: source_field={} target_field={} table={} sql_type={} value_len={} limit={} value_sample={}",
                source_field,
                target_field,
                table_name,
                sql_type.unwrap_or("varchar"),
                value_len,
                limit,
                text.chars().take(80).collect::<String>()
            ));
        }
        Ok(())
    }

    fn validate_required_mapped_value(
        value: Option<&Value>,
        nullable: bool,
        source_field: &str,
        target_field: &str,
        table_name: &str,
    ) -> Result<()> {
        let value = value.map(unwrap_union_tagged_value);
        if nullable {
            return Ok(());
        }
        if value.is_none() || value.is_some_and(Value::is_null) {
            return Err(anyhow!(
                "missing required mapped field: source_field={} target_field={} table={} (value is null or missing)",
                source_field,
                target_field,
                table_name,
            ));
        }
        Ok(())
    }

    fn resolve_source_field_value<'a>(
        payload_doc: &'a Value,
        payload_obj: &'a serde_json::Map<String, Value>,
        source_field: &str,
    ) -> Option<&'a Value> {
        if let Some(value) = payload_obj.get(source_field) {
            return Some(value);
        }

        if source_field.contains('.') {
            let segments = source_field.split('.').collect::<Vec<_>>();
            if let Some(value) = values_at_path(payload_doc, &segments).into_iter().next() {
                return Some(value);
            }

            // For dotted paths we still allow a nested fallback to support
            // flattened payload variants from some connectors.
            return find_value_in_nested_json_object(payload_obj, source_field);
        }

        // For scalar root fields (for example `active`) do not recurse into nested
        // objects: nested siblings can contain the same field name and cause wrong
        // values to be applied to the root row.
        None
    }

    fn resolve_source_field_value_from_map<'a>(
        payload_obj: &'a serde_json::Map<String, Value>,
        source_field: &str,
    ) -> Option<&'a Value> {
        fn value_at_segments<'a>(
            current: &'a serde_json::Map<String, Value>,
            segments: &[&str],
        ) -> Option<&'a Value> {
            let (head, tail) = segments.split_first()?;
            let value = current.get(*head)?;
            if tail.is_empty() {
                return Some(value);
            }
            match value {
                Value::Object(child) => value_at_segments(child, tail),
                _ => None,
            }
        }

        if let Some(value) = payload_obj.get(source_field) {
            return Some(value);
        }

        if source_field.contains('.') {
            let segments = source_field.split('.').collect::<Vec<_>>();
            if let Some(value) = value_at_segments(payload_obj, &segments) {
                return Some(value);
            }

            // Child rows can be flattened from nested objects (for example
            // atmosphericCondition.quality -> { value, quality }).
            // If full dotted path is absent, try progressively shorter suffixes.
            for idx in 1..segments.len() {
                let suffix = &segments[idx..];
                if let Some(value) = value_at_segments(payload_obj, suffix) {
                    return Some(value);
                }
                if suffix.len() == 1 {
                    if let Some(value) = payload_obj.get(suffix[0]) {
                        return Some(value);
                    }
                }
            }
        }

        find_value_in_nested_json_object(payload_obj, source_field)
    }

    fn debezium_document(value: Option<&Value>) -> Result<Option<Value>> {
        let Some(value) = value else {
            return Ok(None);
        };

        let value = unwrap_union_tagged_value(value);
        match value {
            Value::Object(_) => Ok(Some(value.clone())),
            Value::String(raw) => {
                let parsed: Value = serde_json::from_str(raw)
                    .with_context(|| "Failed to parse Debezium document JSON string")?;
                if parsed.is_object() {
                    Ok(Some(parsed))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    fn column_sql_type<'a>(mapping: &'a CollectionMapping, target_field: &str) -> Option<&'a str> {
        let normalized_target = normalize_pg_identifier(target_field);
        mapping
            .pg_mapping
            .ddl
            .as_ref()?
            .columns
            .iter()
            .find(|column| normalize_pg_identifier(&column.name) == normalized_target)
            .map(|column| column.sql_type.as_str())
    }

    fn qualified_table_name(mapping: &CollectionMapping, fallback_schema: Option<&str>) -> String {
        let schema = mapping.pg_mapping.schema_name.as_str().trim();
        let schema = if schema.is_empty() {
            fallback_schema
        } else {
            Some(schema)
        };
        match schema {
            Some(s) => format!(
                "{}.{}",
                quote_ident(s),
                quote_ident(&mapping.pg_mapping.table_name)
            ),
            None => quote_ident(&mapping.pg_mapping.table_name),
        }
    }

    fn root_primary_key(mapping: &CollectionMapping) -> Option<String> {
        mapping
            .pg_mapping
            .ddl
            .as_ref()?
            .columns
            .iter()
            .find(|column| column.primary_key)
            .map(|column| normalize_pg_identifier(&column.name))
    }

    fn root_source_field_for_pk(mapping: &CollectionMapping, pk: &str) -> Option<String> {
        mapping
            .pg_mapping
            .columns
            .iter()
            .find(|column| {
                normalize_pg_identifier(&column.target_field) == normalize_pg_identifier(pk)
            })
            .map(|column| column.source_field.clone())
    }

    fn is_root_mapping(mapping: &CollectionMapping) -> bool {
        mapping.mongo_path.as_deref().unwrap_or(".").trim() == "."
    }

    fn infer_error_target_field(err: &tokio_postgres::Error) -> Option<String> {
        if let Some(column) = err.as_db_error().and_then(|db_err| db_err.column()) {
            return Some(normalize_pg_identifier(column));
        }

        let message = err
            .as_db_error()
            .map(|db_err| db_err.message())
            .unwrap_or_default();
        let marker = "column \"";
        let start = message.find(marker)? + marker.len();
        let end = message[start..].find('"')?;
        Some(normalize_pg_identifier(&message[start..start + end]))
    }

    fn source_field_for_target(mapping: &CollectionMapping, target_field: &str) -> String {
        mapping
            .pg_mapping
            .columns
            .iter()
            .find(|column| {
                normalize_pg_identifier(&column.target_field)
                    == normalize_pg_identifier(target_field)
            })
            .map(|column| column.source_field.clone())
            .unwrap_or_else(|| target_field.to_owned())
    }

    fn annotate_apply_db_error(
        err: tokio_postgres::Error,
        mapping: &CollectionMapping,
        table_name: &str,
    ) -> anyhow::Error {
        let formatted = format_postgres_error(&err);
        if let Some(target_field) = infer_error_target_field(&err) {
            let source_field = source_field_for_target(mapping, &target_field);
            anyhow!(
                "db error: {}\nDETAIL: source_field={} target_field={} table={}",
                formatted,
                source_field,
                target_field,
                table_name
            )
        } else {
            anyhow!("db error: {}", formatted)
        }
    }

    fn direct_children_for_parent<'a>(
        mappings: &'a [CollectionMapping],
        parent_table: &str,
    ) -> Vec<(&'a CollectionMapping, &'a DdlForeignKeyMapping)> {
        mappings
            .iter()
            .filter(|mapping| !is_root_mapping(mapping))
            .filter_map(|mapping| {
                let ddl = mapping.pg_mapping.ddl.as_ref()?;
                let fk = ddl
                    .foreign_keys
                    .iter()
                    .find(|fk| fk.to_table == parent_table && fk.to_col == "id")?;
                Some((mapping, fk))
            })
            .collect()
    }

    fn mapping_depth(mapping: &CollectionMapping) -> usize {
        mongo_path_segments(mapping.mongo_path.as_deref()).len()
    }

    fn mapping_pk_column(mapping: &CollectionMapping) -> Option<String> {
        mapping
            .pg_mapping
            .ddl
            .as_ref()?
            .columns
            .iter()
            .find(|column| column.primary_key)
            .map(|column| normalize_pg_identifier(&column.name))
    }

    fn child_segments_relative_to_parent(
        child_mapping: &CollectionMapping,
        parent_mapping: &CollectionMapping,
    ) -> Vec<String> {
        let child_segments = mongo_path_segments(child_mapping.mongo_path.as_deref())
            .into_iter()
            .map(|segment| segment.to_owned())
            .collect::<Vec<_>>();
        let parent_segments = mongo_path_segments(parent_mapping.mongo_path.as_deref())
            .into_iter()
            .map(|segment| segment.to_owned())
            .collect::<Vec<_>>();

        if child_segments.len() >= parent_segments.len()
            && child_segments
                .iter()
                .take(parent_segments.len())
                .eq(parent_segments.iter())
        {
            child_segments[parent_segments.len()..].to_vec()
        } else {
            child_segments
        }
    }

    fn build_delete_sql_for_mapping(
        mapping: &CollectionMapping,
        mappings: &[CollectionMapping],
        root_table_name: &str,
        root_pk_literal: &str,
        fallback_schema: Option<&str>,
    ) -> Option<String> {
        let mut chain: Vec<(&CollectionMapping, &DdlForeignKeyMapping)> = Vec::new();
        let mut current = mapping;

        while current.pg_mapping.table_name != root_table_name {
            let ddl = current.pg_mapping.ddl.as_ref()?;
            let fk = ddl.foreign_keys.iter().find(|fk| fk.to_col == "id")?;
            chain.push((current, fk));
            current = mappings
                .iter()
                .find(|candidate| candidate.pg_mapping.table_name == fk.to_table)?;
        }

        if chain.is_empty() {
            return None;
        }

        let from = format!(
            "{} AS t0",
            qualified_table_name(chain[0].0, fallback_schema)
        );
        let using_clause = if chain.len() > 1 {
            let using_tables = chain
                .iter()
                .enumerate()
                .skip(1)
                .map(|(idx, (table_mapping, _))| {
                    format!(
                        "{} AS t{}",
                        qualified_table_name(table_mapping, fallback_schema),
                        idx
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!(" USING {using_tables}")
        } else {
            String::new()
        };

        let mut predicates = Vec::new();
        for idx in 0..chain.len().saturating_sub(1) {
            let fk_col = &chain[idx].1.from_col;
            predicates.push(format!("t{idx}.{} = t{}.id", quote_ident(fk_col), idx + 1));
        }
        let last_idx = chain.len() - 1;
        let last_fk = &chain[last_idx].1.from_col;
        predicates.push(format!(
            "t{last_idx}.{} = {root_pk_literal}",
            quote_ident(last_fk)
        ));

        Some(format!(
            "DELETE FROM {from}{using_clause} WHERE {}",
            predicates.join(" AND ")
        ))
    }

    fn load_collection_mapping_folders(
        conf: &mongo2pg::util::ConfData,
        db_name: &str,
    ) -> Result<HashMap<String, Vec<CollectionMapping>>> {
        let project_root = conf.base_dir.join(&conf.project_dir);
        let collections_dir = resolve_collections_dir(&project_root, db_name);
        let mut by_collection = HashMap::new();
        for entry in std::fs::read_dir(&collections_dir)
            .with_context(|| format!("Cannot read {}", collections_dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let folder_name = entry.file_name().to_string_lossy().to_string();
            let mut mappings = Vec::new();
            for file in std::fs::read_dir(&path)
                .with_context(|| format!("Cannot read {}", path.display()))?
            {
                let file = file?;
                let file_path = file.path();
                let Some(name) = file_path.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                if !name.starts_with("mapping_") || !name.ends_with(".yaml") {
                    continue;
                }
                let content = std::fs::read_to_string(&file_path)
                    .with_context(|| format!("Failed to read {}", file_path.display()))?;
                let mapping: CollectionMapping = serde_yaml::from_str(&content)
                    .with_context(|| format!("Failed to parse {}", file_path.display()))?;
                mappings.push(mapping);
            }
            if !mappings.is_empty() {
                by_collection.insert(sanitize_name(&folder_name), mappings);
            }
        }
        Ok(by_collection)
    }

    async fn apply_upsert_event(
        pg_client: &tokio_postgres::Client,
        payload_doc: &Value,
        mappings: &[CollectionMapping],
        fallback_schema: Option<&str>,
        table_insert_execs: &mut HashMap<String, u64>,
    ) -> Result<u64> {
        let root_mapping = mappings
            .iter()
            .find(|mapping| is_root_mapping(mapping))
            .ok_or_else(|| anyhow!("No root mapping (mongo_path: .) found"))?;
        let payload_obj = payload_doc
            .as_object()
            .ok_or_else(|| anyhow!("Kafka upsert payload must be a JSON object"))?;
        let root_table = qualified_table_name(root_mapping, fallback_schema);
        let mut affected_rows = 0_u64;

        let mut mapped_target_fields = std::collections::HashSet::new();
        let mut columns = Vec::new();
        let mut values = Vec::new();
        for column in &root_mapping.pg_mapping.columns {
            let normalized_target_field = normalize_pg_identifier(&column.target_field);
            let raw_value =
                resolve_source_field_value(payload_doc, payload_obj, &column.source_field);
            let sql_type = column_sql_type(root_mapping, &normalized_target_field);
            let (resolved_value, effective_source_field) = resolve_value_with_numeric_id_fallback(
                payload_obj,
                root_mapping,
                &column.source_field,
                &normalized_target_field,
                sql_type,
                raw_value,
            );
            validate_required_mapped_value(
                resolved_value,
                column.nullable,
                &effective_source_field,
                &normalized_target_field,
                &root_table,
            )?;
            validate_extended_json_compatibility(
                resolved_value,
                sql_type,
                &effective_source_field,
                &normalized_target_field,
                &root_table,
            )?;
            validate_varchar_value(
                resolved_value,
                sql_type,
                &effective_source_field,
                &normalized_target_field,
                &root_table,
            )?;
            mapped_target_fields.insert(normalized_target_field.clone());
            columns.push(quote_ident(&normalized_target_field));
            values.push(sql_literal(resolved_value, sql_type));
        }

        if let Some(ddl) = root_mapping.pg_mapping.ddl.as_ref() {
            for ddl_column in &ddl.columns {
                let target_field = normalize_pg_identifier(&ddl_column.name);
                if mapped_target_fields.contains(&target_field) || target_field == "id" {
                    continue;
                }

                let resolved_value =
                    resolve_source_field_value(payload_doc, payload_obj, &target_field);
                if resolved_value.is_none() {
                    continue;
                }

                validate_required_mapped_value(
                    resolved_value,
                    ddl_column.nullable,
                    &target_field,
                    &target_field,
                    &root_table,
                )?;
                validate_extended_json_compatibility(
                    resolved_value,
                    Some(&ddl_column.sql_type),
                    &target_field,
                    &target_field,
                    &root_table,
                )?;
                validate_varchar_value(
                    resolved_value,
                    Some(&ddl_column.sql_type),
                    &target_field,
                    &target_field,
                    &root_table,
                )?;

                mapped_target_fields.insert(target_field.clone());
                columns.push(quote_ident(&target_field));
                values.push(sql_literal(resolved_value, Some(&ddl_column.sql_type)));
            }
        }

        let root_pk = root_primary_key(root_mapping)
            .ok_or_else(|| anyhow!("Root mapping has no primary key in ddl"))?;
        let mut update_targets = columns
            .iter()
            .map(|column| normalize_pg_identifier(column))
            .filter(|target_field| target_field != &root_pk)
            .collect::<Vec<_>>();
        update_targets.sort();
        update_targets.dedup();

        let updates = update_targets
            .into_iter()
            .map(|target_field| {
                format!(
                    "{} = EXCLUDED.{}",
                    quote_ident(&target_field),
                    quote_ident(&target_field)
                )
            })
            .collect::<Vec<_>>();

        let upsert_sql = if updates.is_empty() {
            format!(
                "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING",
                root_table,
                columns.join(", "),
                values.join(", "),
                quote_ident(&root_pk),
            )
        } else {
            format!(
                "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}",
                root_table,
                columns.join(", "),
                values.join(", "),
                quote_ident(&root_pk),
                updates.join(", "),
            )
        };
        affected_rows += pg_client
            .execute(upsert_sql.as_str(), &[])
            .await
            .map_err(|err| annotate_apply_db_error(err, root_mapping, &root_table))?;
        *table_insert_execs.entry(root_table.clone()).or_insert(0) += 1;

        let root_pk_source = root_source_field_for_pk(root_mapping, &root_pk)
            .ok_or_else(|| anyhow!("Root mapping primary key not found in mapped columns"))?;
        let root_pk_sql_type = column_sql_type(root_mapping, &root_pk);
        let (root_pk_value, effective_root_pk_source) = resolve_value_with_numeric_id_fallback(
            payload_obj,
            root_mapping,
            &root_pk_source,
            &root_pk,
            root_pk_sql_type,
            resolve_source_field_value(payload_doc, payload_obj, &root_pk_source),
        );
        let root_pk_value = root_pk_value.ok_or_else(|| {
            anyhow!(
                "Root payload missing {} (resolved source field for target {} is {})",
                root_pk_source,
                root_pk,
                effective_root_pk_source
            )
        })?;

        let root_pk_literal = sql_literal(Some(root_pk_value), root_pk_sql_type);

        let mut non_root_mappings = mappings
            .iter()
            .filter(|mapping| !is_root_mapping(mapping))
            .collect::<Vec<_>>();
        non_root_mappings.sort_by_key(|mapping| std::cmp::Reverse(mapping_depth(mapping)));
        for mapping in non_root_mappings {
            if let Some(delete_sql) = build_delete_sql_for_mapping(
                mapping,
                mappings,
                &root_mapping.pg_mapping.table_name,
                &root_pk_literal,
                fallback_schema,
            ) {
                pg_client.execute(delete_sql.as_str(), &[]).await?;
            }
        }

        let mut pending = vec![(root_mapping, payload_doc.clone(), root_pk_literal)];

        while let Some((parent_mapping, parent_node, parent_pk_literal)) = pending.pop() {
            let children =
                direct_children_for_parent(mappings, &parent_mapping.pg_mapping.table_name);
            for (child_mapping, fk) in children {
                let child_table = qualified_table_name(child_mapping, fallback_schema);
                let relative_segments =
                    child_segments_relative_to_parent(child_mapping, parent_mapping);
                let relative_refs = relative_segments
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                let child_nodes = values_at_path(&parent_node, &relative_refs);
                let child_pk = mapping_pk_column(child_mapping)
                    .ok_or_else(|| anyhow!("Mapping {} has no primary key", child_table))?;
                let child_pk_sql_type = column_sql_type(child_mapping, &child_pk);

                for node in child_nodes {
                    for obj in child_row_objects_for_mapping(node, child_mapping) {
                        let mapped_values = child_mapping
                            .pg_mapping
                            .columns
                            .iter()
                            .map(|mapped| {
                                let val =
                                    resolve_source_field_value_from_map(&obj, &mapped.source_field);
                                (mapped, val)
                            })
                            .collect::<Vec<_>>();

                        let mut child_columns = vec![quote_ident(&fk.from_col)];
                        let mut child_values = vec![parent_pk_literal.clone()];
                        for (mapped, val) in mapped_values {
                            child_columns.push(quote_ident(&mapped.target_field));
                            if val.is_none() && !mapped.nullable {
                                let available_keys =
                                    obj.keys().take(32).cloned().collect::<Vec<_>>().join(", ");
                                let node_preview =
                                    serde_json::to_string(&Value::Object(obj.clone()))
                                        .unwrap_or_else(|_| "<unserializable-node>".to_owned());
                                return Err(anyhow!(
                                    "missing required child mapped field: table={} source_field={} target_field={} mongo_path={} available_keys=[{}] node={}",
                                    child_table,
                                    mapped.source_field,
                                    mapped.target_field,
                                    child_mapping.mongo_path.as_deref().unwrap_or("."),
                                    available_keys,
                                    node_preview,
                                ));
                            }
                            let sql_type = column_sql_type(child_mapping, &mapped.target_field);
                            validate_extended_json_compatibility(
                                val,
                                sql_type,
                                &mapped.source_field,
                                &mapped.target_field,
                                &child_table,
                            )?;
                            validate_varchar_value(
                                val,
                                sql_type,
                                &mapped.source_field,
                                &mapped.target_field,
                                &child_table,
                            )?;
                            child_values.push(sql_literal(val, sql_type));
                        }

                        let insert_sql = format!(
                            "INSERT INTO {} ({}) VALUES ({}) RETURNING {}::text",
                            child_table,
                            child_columns.join(", "),
                            child_values.join(", "),
                            quote_ident(&child_pk)
                        );
                        let row = pg_client
                            .query_one(insert_sql.as_str(), &[])
                            .await
                            .map_err(|err| {
                                annotate_apply_db_error(err, child_mapping, &child_table)
                            })?;
                        let inserted_pk_text: String = row.try_get(0)?;

                        affected_rows += 1;
                        *table_insert_execs.entry(child_table.clone()).or_insert(0) += 1;

                        let inserted_pk_literal =
                            cast_string_literal(&inserted_pk_text, child_pk_sql_type);
                        pending.push((child_mapping, Value::Object(obj), inserted_pk_literal));
                    }
                }
            }
        }

        Ok(affected_rows)
    }

    async fn apply_delete_event(
        pg_client: &tokio_postgres::Client,
        before_doc: &Value,
        mappings: &[CollectionMapping],
        fallback_schema: Option<&str>,
    ) -> Result<()> {
        let root_mapping = mappings
            .iter()
            .find(|mapping| is_root_mapping(mapping))
            .ok_or_else(|| anyhow!("No root mapping (mongo_path: .) found"))?;
        let root_pk = root_primary_key(root_mapping)
            .ok_or_else(|| anyhow!("Root mapping has no primary key in ddl"))?;
        let root_pk_source = root_source_field_for_pk(root_mapping, &root_pk)
            .ok_or_else(|| anyhow!("Root mapping primary key not found in mapped columns"))?;
        let root_pk_value = before_doc
            .as_object()
            .and_then(|obj| obj.get(&root_pk_source))
            .ok_or_else(|| anyhow!("Delete payload missing {}", root_pk_source))?;

        let root_pk_sql_type = column_sql_type(root_mapping, &root_pk);
        let root_pk_literal = sql_literal(Some(root_pk_value), root_pk_sql_type);

        let mut non_root_mappings = mappings
            .iter()
            .filter(|mapping| !is_root_mapping(mapping))
            .collect::<Vec<_>>();
        non_root_mappings.sort_by_key(|mapping| std::cmp::Reverse(mapping_depth(mapping)));
        for mapping in non_root_mappings {
            if let Some(delete_sql) = build_delete_sql_for_mapping(
                mapping,
                mappings,
                &root_mapping.pg_mapping.table_name,
                &root_pk_literal,
                fallback_schema,
            ) {
                pg_client.execute(delete_sql.as_str(), &[]).await?;
            }
        }

        let root_table = qualified_table_name(root_mapping, fallback_schema);
        let delete_root_sql = format!(
            "DELETE FROM {} WHERE {} = {}",
            root_table,
            quote_ident(&root_pk),
            root_pk_literal
        );
        pg_client.execute(delete_root_sql.as_str(), &[]).await?;
        Ok(())
    }

    async fn bootstrap_pg_objects_for_kafka_import(
        admin_client: &tokio_postgres::Client,
        pg_client: &tokio_postgres::Client,
        conf: &mongo2pg::util::ConfData,
        db_name: &str,
    ) -> Result<()> {
        let project_root = conf.base_dir.join(&conf.project_dir);
        let tables_root = project_root.join("schema").join("tables");
        let tables_dir = if tables_root.join(db_name).is_dir() {
            tables_root.join(db_name)
        } else {
            tables_root
        };

        if !tables_dir.is_dir() {
            return Err(anyhow!(
                "Cannot read SQL tables directory {}",
                tables_dir.display()
            ));
        }

        let mut sql_files: Vec<PathBuf> = std::fs::read_dir(&tables_dir)
            .with_context(|| format!("Cannot read {}", tables_dir.display()))?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("sql"))
            .collect();
        sql_files.sort();

        if sql_files.is_empty() {
            return Err(anyhow!(
                "No SQL files found in {}. Run to-pg first.",
                tables_dir.display()
            ));
        }

        let mut ensured_databases = std::collections::HashSet::new();

        for sql_path in &sql_files {
            let sql = std::fs::read_to_string(sql_path)
                .with_context(|| format!("Failed to read {}", sql_path.display()))?;

            if let Some(ddl_db_name) = extract_psql_database_name(&sql) {
                if ddl_db_name != db_name {
                    warn!(
                        "DDL file {} targets database '{}' while kafka-import target database is '{}'",
                        sql_path.display(),
                        ddl_db_name,
                        db_name
                    );
                }
                if ensured_databases.insert(ddl_db_name.clone()) {
                    ensure_pg_database(admin_client, &ddl_db_name).await?;
                }
            }

            let executable_sql = strip_psql_preamble(&sql);
            if executable_sql.trim().is_empty() {
                continue;
            }

            let parsed_tables = parse_sql(&executable_sql);
            let file_schema = extract_search_path(&executable_sql).or(conf.target_schema.clone());

            let mut all_tables_exist = !parsed_tables.is_empty();
            for table in &parsed_tables {
                let qualified = match file_schema.as_deref() {
                    Some(schema) => format!("{schema}.{}", table.name),
                    None => table.name.clone(),
                };
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
                if exists.is_none() {
                    all_tables_exist = false;
                    break;
                }
            }

            if all_tables_exist {
                info!(
                    "Skipping DDL from {} (objects already exist)",
                    sql_path.display()
                );
                continue;
            }

            match pg_client.batch_execute(&executable_sql).await {
                Ok(()) => {
                    info!("Created PostgreSQL objects from {}", sql_path.display());
                }
                Err(err) if is_missing_postgis_control_file(&err) => {
                    let fallback_sql = strip_postgis_extension_statement(&executable_sql);
                    match pg_client.batch_execute(&fallback_sql).await {
                        Ok(()) => {
                            info!(
                                "Created PostgreSQL objects from {} (without PostGIS extension statement)",
                                sql_path.display()
                            );
                        }
                        Err(fallback_err) => {
                            return Err(anyhow!(
                                "Failed to execute {} after removing PostGIS extension statement\n{}",
                                sql_path.display(),
                                format_postgres_error(&fallback_err)
                            ));
                        }
                    }
                }
                Err(err)
                    if err.code() == Some(&tokio_postgres::error::SqlState::DUPLICATE_TABLE)
                        || err.code()
                            == Some(&tokio_postgres::error::SqlState::DUPLICATE_OBJECT)
                        || err.code()
                            == Some(&tokio_postgres::error::SqlState::DUPLICATE_SCHEMA) =>
                {
                    info!(
                        "Skipping existing PostgreSQL objects from {} ({})",
                        sql_path.display(),
                        format_postgres_error(&err)
                    );
                }
                Err(err) => {
                    return Err(anyhow!(
                        "Failed to execute {}\n{}",
                        sql_path.display(),
                        format_postgres_error(&err)
                    ));
                }
            }
        }

        Ok(())
    }

    async fn truncate_tables_for_snapshot(
        pg_client: &tokio_postgres::Client,
        mappings_by_collection: &HashMap<String, Vec<CollectionMapping>>,
        fallback_schema: Option<&str>,
    ) -> Result<usize> {
        let mut qualified_tables = std::collections::HashSet::new();

        for mappings in mappings_by_collection.values() {
            for mapping in mappings {
                let schema = mapping.pg_mapping.schema_name.trim();
                let schema = if schema.is_empty() {
                    fallback_schema
                } else {
                    Some(schema)
                };
                let qualified = match schema {
                    Some(schema) => {
                        format!(
                            "{}.{}",
                            quote_ident(schema),
                            quote_ident(&mapping.pg_mapping.table_name)
                        )
                    }
                    None => quote_ident(&mapping.pg_mapping.table_name),
                };
                qualified_tables.insert(qualified);
            }
        }

        let mut tables = qualified_tables.into_iter().collect::<Vec<_>>();
        tables.sort();

        for qualified in &tables {
            let sql = format!("TRUNCATE TABLE {qualified} CASCADE");
            pg_client
                .batch_execute(&sql)
                .await
                .with_context(|| format!("Failed to truncate table {qualified} for snapshot"))?;
        }

        Ok(tables.len())
    }

    apply_config_overrides(
        &args.config,
        &ConfigOverrides {
            project_dir: args.project_dir.clone(),
            kafka_topics: (!args.topics.is_empty()).then(|| args.topics.clone()),
            kafka_max_messages: args.max_messages,
            kafka_offset: args.offset.clone(),
            kafka_group_id: args.group_id.clone(),
            kafka_topic_prefix: args.topic_prefix.clone(),
            target_database_name: args.database_name.clone(),
            target_schema_name: args.schema_name.clone(),
            ..ConfigOverrides::default()
        },
    )?;

    let conf = read_conf(&args.config)?;
    let kafka_conf = conf
        .kafka
        .clone()
        .ok_or_else(|| anyhow!("Missing [kafka] section in config file"))?;
    let namespace = conf
        .namespace
        .clone()
        .ok_or_else(|| anyhow!("No NAMESPACE provided in config"))?;
    let (namespace_db_name, _) = split_namespace_scope(&namespace);

    let bootstrap_servers = kafka_conf
        .bootstrap_servers
        .clone()
        .ok_or_else(|| anyhow!("kafka.bootstrap_servers is required"))?;
    let group_id = kafka_conf
        .group_id
        .clone()
        .unwrap_or_else(|| "mongo2pg-kafka-import".to_owned());
    let configured_offset = kafka_conf
        .offset
        .clone()
        .or_else(|| kafka_conf.auto_offset_reset.clone())
        .unwrap_or_else(|| "earliest".to_owned());
    let effective_offset = args
        .offset
        .clone()
        .unwrap_or_else(|| configured_offset.clone());
    let snapshot_mode = effective_offset == "0";

    let effective_group_id = if snapshot_mode {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_secs();
        format!("{group_id}-snapshot-{ts}")
    } else {
        group_id.clone()
    };
    let mut topics = if args.topics.is_empty() {
        kafka_conf.topics.clone()
    } else {
        args.topics.clone()
    };

    let target_uri = conf
        .target_uri
        .clone()
        .ok_or_else(|| anyhow!("No TARGET_URI provided in config"))?;
    let target_database_name = conf
        .target_database_name
        .clone()
        .unwrap_or_else(|| namespace_db_name.to_owned());

    let admin_client = connect_pg_client(&target_uri).await?;
    ensure_pg_database(&admin_client, &target_database_name).await?;

    let db_target_uri = pg_uri_with_database(&target_uri, &target_database_name);
    let pg_client = connect_pg_client(&db_target_uri).await?;

    bootstrap_pg_objects_for_kafka_import(&admin_client, &pg_client, &conf, namespace_db_name)
        .await?;

    let mappings_by_collection = load_collection_mapping_folders(&conf, namespace_db_name)?;

    let configured_auto_offset_reset = configured_offset;
    let auto_offset_reset = if snapshot_mode {
        "earliest".to_owned()
    } else {
        effective_offset
    };
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", &effective_group_id)
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", &auto_offset_reset)
        .create()
        .with_context(|| {
            format!(
                "{}: failed to create Kafka consumer",
                connection_failed_context("kafka", "connect")
            )
        })?;
    let dlq_producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .create()
        .with_context(|| {
            format!(
                "{}: failed to create Kafka DLQ producer",
                connection_failed_context("kafka", "connect")
            )
        })?;

    if topics.is_empty() {
        if let Some(prefix) = kafka_conf.topic_prefix.as_deref() {
            let metadata = consumer
                .fetch_metadata(None, Duration::from_secs(10))
                .with_context(|| {
                    format!(
                        "{}: failed to fetch Kafka metadata while resolving topic prefix '{}'",
                        connection_failed_context("kafka", "query"),
                        prefix
                    )
                })?;
            let prefix_with_dot = format!("{prefix}.");
            topics = metadata
                .topics()
                .iter()
                .map(|topic| topic.name().to_owned())
                .filter(|topic| topic.starts_with(&prefix_with_dot))
                .collect::<Vec<_>>();
            topics.sort();
            topics.dedup();

            if topics.is_empty() {
                return Err(anyhow!(
                    "No Kafka topics matched prefix '{}'. Set [kafka].topics, pass --topics, or create topics with this prefix.",
                    prefix
                ));
            }
        } else {
            return Err(anyhow!(
                "No Kafka topics configured. Set [kafka].topics, set [kafka].topic_prefix, or pass --topics"
            ));
        }
    }

    let topic_refs = topics.iter().map(String::as_str).collect::<Vec<_>>();
    consumer.subscribe(&topic_refs).with_context(|| {
        format!(
            "{}: failed to subscribe topics: {}",
            connection_failed_context("kafka", "consume"),
            topics.join(", ")
        )
    })?;

    info!(
        "Kafka import started. group_id={}, topics={}, target_db={}, snapshot_mode={}, offset={}",
        effective_group_id,
        topics.join(","),
        target_database_name,
        snapshot_mode,
        auto_offset_reset
    );

    if snapshot_mode {
        info!(
            "Snapshot mode enabled (offset=0): truncating mapped PostgreSQL tables before consuming Kafka messages"
        );
        let truncated = truncate_tables_for_snapshot(
            &pg_client,
            &mappings_by_collection,
            conf.target_schema.as_deref(),
        )
        .await?;
        info!(
            "Snapshot mode: truncated {} mapped PostgreSQL table(s) before consuming",
            truncated
        );
    } else {
        info!(
            "Snapshot mode disabled: PostgreSQL tables are not truncated (offset={})",
            auto_offset_reset
        );
    }

    let http_client = reqwest::Client::builder().build()?;
    let mut schema_cache: HashMap<u32, Schema> = HashMap::new();
    let max_messages = args.max_messages.or(kafka_conf.max_messages);
    let batch_log_messages = kafka_conf.batch_log_messages.unwrap_or(100).max(1);
    let mut processed = 0_usize;
    let mut polled = 0_usize;
    let mut skipped_topic = 0_usize;
    let mut skipped_db = 0_usize;
    let mut skipped_mapping = 0_usize;
    let mut skipped_no_payload = 0_usize;
    let mut decode_failed = 0_usize;
    let mut apply_failed = 0_usize;
    let mut dlq_published = 0_usize;
    let mut dlq_failed = 0_usize;
    let mut snapshot_inserted_rows = 0_u64;
    let mut table_insert_execs: HashMap<String, u64> = HashMap::new();

    info!(
        "Kafka consumer configuration: auto_offset_reset={}, configured_auto_offset_reset={}, max_messages={}",
        auto_offset_reset,
        configured_auto_offset_reset,
        max_messages
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_owned())
    );
    info!(
        "Loaded mapping folders for {} collection(s)",
        mappings_by_collection.len()
    );

    let mut stream = consumer.stream();
    loop {
        let next_item = if snapshot_mode {
            match tokio::time::timeout(Duration::from_secs(10), stream.next()).await {
                Ok(item) => item,
                Err(_) => {
                    warn!(
                        "Snapshot mode: idle timeout reached (10s without new messages), stopping."
                    );
                    break;
                }
            }
        } else {
            stream.next().await
        };

        let Some(message_result) = next_item else {
            info!("Kafka stream ended by broker/consumer");
            break;
        };

        polled += 1;
        let message = match message_result {
            Ok(msg) => msg,
            Err(err) => {
                warn!(
                    "{} warning: Kafka consume error: {err}",
                    connection_failed_context("kafka", "consume")
                );
                continue;
            }
        };

        let topic = message.topic();
        let Some((db_name, collection_name)) = parse_topic_db_collection(
            topic,
            kafka_conf.topic_prefix.as_deref(),
            Some(namespace_db_name),
        ) else {
            skipped_topic += 1;
            continue;
        };
        if db_name != namespace_db_name {
            skipped_db += 1;
            continue;
        }

        let folder_name = sanitize_name(&collection_name);
        let Some(mappings) = mappings_by_collection.get(&folder_name) else {
            skipped_mapping += 1;
            warn!(
                "no mapping folder for collection '{}' (expected '{}')",
                collection_name, folder_name
            );
            continue;
        };

        let Some(bytes) = message.payload() else {
            skipped_no_payload += 1;
            continue;
        };
        let decoded = match decode_message_value(
            bytes,
            kafka_conf.schema_registry_url.as_deref(),
            kafka_conf.schema_registry_username.as_deref(),
            kafka_conf.schema_registry_password.as_deref(),
            &http_client,
            &mut schema_cache,
        )
        .await
        {
            Ok(value) => value,
            Err(err) => {
                decode_failed += 1;
                warn!("failed to decode message on topic {topic}: {err}");
                continue;
            }
        };

        let payload = decoded
            .get("payload")
            .and_then(|value| value.as_object())
            .map(|obj| Value::Object(obj.clone()))
            .unwrap_or(decoded);

        let op = payload
            .get("op")
            .map(unwrap_union_tagged_value)
            .and_then(|value| value.as_str())
            .unwrap_or("u");
        let after = match debezium_document(payload.get("after").map(unwrap_union_tagged_value)) {
            Ok(value) => value,
            Err(err) => {
                decode_failed += 1;
                warn!("failed to parse Debezium 'after' for topic {topic}: {err:#}");
                continue;
            }
        };
        let before = match debezium_document(payload.get("before").map(unwrap_union_tagged_value)) {
            Ok(value) => value,
            Err(err) => {
                decode_failed += 1;
                warn!("failed to parse Debezium 'before' for topic {topic}: {err:#}");
                continue;
            }
        };

        let result = match op {
            "d" => {
                if let Some(before_doc) = before.as_ref() {
                    apply_delete_event(
                        &pg_client,
                        before_doc,
                        mappings,
                        conf.target_schema.as_deref(),
                    )
                    .await
                    .map(|_| 0_u64)
                } else {
                    Ok(0_u64)
                }
            }
            _ => {
                if let Some(after_doc) = after.as_ref() {
                    apply_upsert_event(
                        &pg_client,
                        after_doc,
                        mappings,
                        conf.target_schema.as_deref(),
                        &mut table_insert_execs,
                    )
                    .await
                } else {
                    Ok(0_u64)
                }
            }
        };

        let applied_rows = match result {
            Ok(rows) => rows,
            Err(err) => {
                apply_failed += 1;
                warn!(
                    "apply failed topic={} collection={} op={}: {:#}\n  hint: extend map_extended_json_literal() in src/bin/mongo2pg.rs to support this payload shape\n  hint: constraint errors include source_field and target_field in details",
                    topic, collection_name, op, err
                );

                let key_bytes = message.key().map(|key| key.to_vec());
                let payload_bytes = message.payload().map(|payload| payload.to_vec());
                if let Some(payload) = payload_bytes.as_deref() {
                    match publish_to_dlq(&dlq_producer, topic, key_bytes.as_deref(), payload).await
                    {
                        Ok(()) => {
                            dlq_published += 1;
                            warn!("message copied to DLQ topic=dlq_{}", topic);
                        }
                        Err(dlq_err) => {
                            dlq_failed += 1;
                            warn!("failed to copy message to DLQ: {dlq_err:#}");
                        }
                    }
                } else {
                    dlq_failed += 1;
                    warn!("failed to copy message to DLQ: message payload missing");
                }

                continue;
            }
        };

        processed += 1;
        if snapshot_mode {
            snapshot_inserted_rows += applied_rows;
        }
        if processed <= 5 || processed % batch_log_messages == 0 {
            info!(
                "Kafka apply ok: processed={} topic={} collection={} op={} affected_rows={}",
                processed, topic, collection_name, op, applied_rows
            );
        }

        if polled % batch_log_messages == 0 {
            info!(
                "Kafka progress: polled={}, processed={}, skipped_topic={}, skipped_db={}, skipped_mapping={}, skipped_no_payload={}, decode_failed={}, apply_failed={}",
                polled,
                processed,
                skipped_topic,
                skipped_db,
                skipped_mapping,
                skipped_no_payload,
                decode_failed,
                apply_failed
            );
            info!(
                "Kafka progress DLQ: dlq_published={}, dlq_failed={}",
                dlq_published, dlq_failed
            );
            info!(
                "Kafka progress tables: impacted_tables={}, insert_execs={}",
                table_insert_execs.len(),
                format_table_insert_exec_summary(&table_insert_execs)
            );
        }

        if let Some(limit) = max_messages {
            if processed >= limit {
                info!("Reached --max-messages limit ({limit}), stopping.");
                break;
            }
        }
    }

    info!(
        "Kafka import finished. polled={}, processed={}, skipped_topic={}, skipped_db={}, skipped_mapping={}, skipped_no_payload={}, decode_failed={}, apply_failed={}",
        polled,
        processed,
        skipped_topic,
        skipped_db,
        skipped_mapping,
        skipped_no_payload,
        decode_failed,
        apply_failed
    );
    info!(
        "Kafka import DLQ summary: dlq_published={}, dlq_failed={}",
        dlq_published, dlq_failed
    );
    info!(
        "Kafka import table summary: impacted_tables={}, insert_execs={}",
        table_insert_execs.len(),
        format_table_insert_exec_summary(&table_insert_execs)
    );
    if snapshot_mode {
        info!(
            "Snapshot summary: total affected rows applied to PostgreSQL={} (upserts + child inserts)",
            snapshot_inserted_rows
        );
    }
    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────────
// `report` subcommand
// ──────────────────────────────────────────────────────────────────────────────

async fn run_report(args: ReportArgs, quiet: bool) -> Result<()> {
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
            let local_project_root = resolve_local_project_root_from_config(conf, &c);
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
    let project_root = configured_project_root(&c);
    let reports_dir = project_root.join("reports");
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

    let collections_dir = project_root.join("source").join("collections");
    let schema_tables_root = project_root.join("schema").join("tables");
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
    info!("Post-import report written to {}", output_path.display());

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

        let collections_dir = configured_project_root(&c).join("source").join("collections");

        let rows = collect_rows(&collections_dir, None)
            .with_context(|| format!("Failed to read collections for {db_name}"))?;

        db_scores.push(compute_db_score(&db_name, &rows));
    }

    let html = render_cluster_html(&db_scores, &cluster_label);

    let output_path = args.output.unwrap_or_else(|| PathBuf::from("cluster.html"));
    std::fs::write(&output_path, &html)
        .with_context(|| format!("Failed to write {}", output_path.display()))?;
    info!("Cluster report written to {}", output_path.display());

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

fn extract_psql_database_name(sql: &str) -> Option<String> {
    for line in sql.lines() {
        let trimmed = line.trim();

        if let Some(rest) = trimmed.strip_prefix("CREATE DATABASE ") {
            let ident = rest.trim().trim_end_matches(';').trim();
            if !ident.is_empty() {
                return Some(normalize_pg_identifier(ident));
            }
        }

        if let Some(rest) = trimmed.strip_prefix("\\connect ") {
            let ident = rest
                .trim()
                .trim_end_matches(';')
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim();
            if !ident.is_empty() {
                return Some(normalize_pg_identifier(ident));
            }
        }
    }

    None
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

fn strip_postgis_extension_statement(sql: &str) -> String {
    sql.lines()
        .filter(|line| {
            let normalized = line.trim().to_ascii_lowercase();
            normalized != "create extension if not exists postgis;"
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn is_missing_postgis_control_file(err: &tokio_postgres::Error) -> bool {
    format_postgres_error(err)
        .to_ascii_lowercase()
        .contains("postgis.control")
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

fn resolve_root_table_name(
    parsed_tables: &[mongo2pg::schema_diagram::Table],
    coll_name: &str,
) -> String {
    let expected = sanitize_name(coll_name);

    if let Some(table) = parsed_tables.iter().find(|table| {
        let name = table.name.trim();
        name.eq_ignore_ascii_case(&expected)
            || name
                .split('.')
                .last()
                .map(|leaf| leaf.eq_ignore_ascii_case(&expected))
                .unwrap_or(false)
    }) {
        return table.name.clone();
    }

    parsed_tables
        .first()
        .map(|table| table.name.clone())
        .unwrap_or_else(|| sanitize_name(coll_name))
}

fn resolve_post_import_table_row<'a>(
    table_name: &str,
    local_rows: &'a HashMap<String, PostImportTableRow>,
    global_rows: &'a HashMap<String, PostImportTableRow>,
) -> Option<&'a PostImportTableRow> {
    local_rows
        .get(table_name)
        .or_else(|| global_rows.get(table_name))
}

fn is_hex_keyed_name(name: &str) -> bool {
    name.len() >= 8 && name.chars().all(|ch| ch.is_ascii_hexdigit())
}

fn is_uuid_keyed_name(name: &str) -> bool {
    let parts = name.split('-').collect::<Vec<_>>();
    parts.len() == 5
        && [8_usize, 4, 4, 4, 12]
            .iter()
            .zip(parts.iter())
            .all(|(expected_len, part)| {
                part.len() == *expected_len && part.chars().all(|ch| ch.is_ascii_hexdigit())
            })
}

fn dynamic_map_value_fields(
    fields: &IndexMap<String, FieldSchema>,
) -> Option<&IndexMap<String, FieldSchema>> {
    if fields.is_empty() {
        return None;
    }
    if !fields
        .keys()
        .all(|name| is_hex_keyed_name(name) || is_uuid_keyed_name(name))
    {
        return None;
    }

    for field in fields.values() {
        let non_null = field
            .types
            .iter()
            .filter(|(type_name, _)| !matches!(type_name.as_str(), "Null" | "Undefined"))
            .collect::<Vec<_>>();
        if non_null.len() == 1 && non_null[0].0.as_str() == "Object" {
            if let Some(value_fields) = non_null[0].1.object.as_ref() {
                if !value_fields.is_empty() {
                    return Some(value_fields);
                }
            }
        }
    }

    None
}

fn count_dynamic_map_entries(doc: &bson::Document) -> u64 {
    doc.values()
        .filter(|value| matches!(value, Bson::Document(entry) if !entry.is_empty()))
        .count() as u64
}

fn child_row_objects_for_mapping(
    node: &Value,
    child_mapping: &CollectionMapping,
) -> Vec<serde_json::Map<String, Value>> {
    match node {
        Value::Array(items) => {
            let mut out = Vec::new();
            for item in items {
                out.extend(child_row_objects_for_mapping(item, child_mapping));
            }
            return out;
        }
        Value::String(raw) => {
            if let Ok(parsed) = serde_json::from_str::<Value>(raw) {
                return child_row_objects_for_mapping(&parsed, child_mapping);
            }
            return Vec::new();
        }
        _ => {}
    }

    let Value::Object(obj) = node else {
        return Vec::new();
    };

    // Empty embedded object means no child rows.
    if obj.is_empty() {
        return Vec::new();
    }

    let mapped_source_fields = child_mapping
        .pg_mapping
        .columns
        .iter()
        .map(|column| column.source_field.as_str())
        .collect::<Vec<_>>();

    let has_mapped_fields = |candidate: &serde_json::Map<String, Value>| {
        mapped_source_fields
            .iter()
            .any(|field| candidate.contains_key(*field))
    };

    let row_from_entry_value = |entry_value: &Value| -> Option<serde_json::Map<String, Value>> {
        match entry_value {
            Value::Object(entry_obj) => Some(entry_obj.clone()),
            Value::String(raw) => serde_json::from_str::<Value>(raw)
                .ok()
                .and_then(|parsed| parsed.as_object().cloned()),
            _ => None,
        }
    };

    let expects_key_column = child_mapping
        .pg_mapping
        .columns
        .iter()
        .any(|mapped| mapped.source_field == "key");

    // Some connectors wrap arrays/objects before the actual row payload.
    // If current object has none of mapped fields, recursively unwrap known
    // wrapper keys first, then any nested aggregate container.
    // For map-style objects that rely on dynamic keys, defer to dedicated
    // map expansion branch below.
    let likely_map_style_object = expects_key_column
        && !obj.contains_key("key")
        && obj
            .values()
            .all(|value| matches!(value, Value::Object(_) | Value::String(_)));

    // Empty object means there is no child row to write.
    if obj.is_empty() && !has_mapped_fields(obj) {
        return Vec::new();
    }

    if !has_mapped_fields(obj) && !likely_map_style_object {
        // Map-like object without dedicated key column: expand all entry payloads
        // only when each entry clearly resolves to mapped fields.
        let mut map_rows = Vec::new();
        let mut all_entries_mappable = true;
        if !obj.is_empty() {
            for entry_value in obj.values() {
                if let Some(row) = row_from_entry_value(entry_value) {
                    if has_mapped_fields(&row) {
                        map_rows.push(row);
                    } else {
                        all_entries_mappable = false;
                        break;
                    }
                } else {
                    all_entries_mappable = false;
                    break;
                }
            }
            if all_entries_mappable && !map_rows.is_empty() {
                return map_rows;
            }
        }

        for wrapper_key in [
            "items",
            "array",
            "values",
            "records",
            "value",
            "transactions",
        ] {
            if let Some(wrapped) = obj.get(wrapper_key) {
                let extracted = child_row_objects_for_mapping(wrapped, child_mapping);
                if !extracted.is_empty() {
                    return extracted;
                }
            }
        }

        for wrapped in obj.values() {
            match wrapped {
                Value::Array(_) | Value::Object(_) | Value::String(_) => {
                    let extracted = child_row_objects_for_mapping(wrapped, child_mapping);
                    if !extracted.is_empty() {
                        return extracted;
                    }
                }
                _ => {}
            }
        }
    }

    let key_from_row_or_entry = |row: &serde_json::Map<String, Value>, entry_key: &str| {
        row.values()
            .find_map(|value| match value {
                Value::String(text) if text == entry_key => Some(text.clone()),
                _ => None,
            })
            .unwrap_or_else(|| entry_key.to_owned())
    };

    // Some connectors encode map entries as {"key": "...", "value": {...}}.
    // Normalize that shape to plain row object.
    if let Some(value_node) = obj.get("value") {
        if let Some(mut row) = row_from_entry_value(value_node) {
            if expects_key_column {
                if let Some(Value::String(existing_key)) = obj.get("key") {
                    let key_value = key_from_row_or_entry(&row, existing_key);
                    row.insert("key".to_owned(), Value::String(key_value));
                }
            }
            return vec![row];
        }
    }

    // Map-style object branch: { "dynamic_key": { ...row... }, ... }
    // Expand into one row per entry; inject dynamic key only when mapping requires it.
    if expects_key_column && !obj.contains_key("key") {
        let mut expanded = Vec::new();
        for (entry_key, entry_value) in obj {
            if let Some(mut row) = row_from_entry_value(entry_value) {
                let key_value = key_from_row_or_entry(&row, entry_key);
                row.insert("key".to_owned(), Value::String(key_value));
                expanded.push(row);
            }
        }
        if !expanded.is_empty() {
            return expanded;
        }
    }

    vec![obj.clone()]
}

async fn connect_pg_client(target_uri: &str) -> Result<tokio_postgres::Client> {
    let mut tls_builder = native_tls::TlsConnector::builder();
    if matches!(pg_sslmode(target_uri), Some(mode) if mode.eq_ignore_ascii_case("require")) {
        tls_builder.danger_accept_invalid_certs(true);
        tls_builder.danger_accept_invalid_hostnames(true);
    }
    let tls = tls_builder.build().with_context(|| {
        format!(
            "{}: failed to initialize PostgreSQL TLS connector",
            connection_failed_context("pg", "connect")
        )
    })?;
    let tls = MakeTlsConnector::new(tls);

    let (pg_client, pg_connection) = tokio_postgres::connect(target_uri, tls)
        .await
        .with_context(|| {
            format!(
                "{}: failed to connect to PostgreSQL using TARGET_URI",
                connection_failed_context("pg", "connect")
            )
        })?;
    tokio::spawn(async move {
        if let Err(err) = pg_connection.await {
            warn!(
                "{} PostgreSQL connection error: {err}",
                connection_failed_context("pg", "connect")
            );
        }
    });

    Ok(pg_client)
}

async fn ensure_pg_database(pg_client: &tokio_postgres::Client, db_name: &str) -> Result<()> {
    let create_db_sql = format!("CREATE DATABASE {}", quote_ident(db_name));
    match pg_client.batch_execute(&create_db_sql).await {
        Ok(()) => {
            info!("Created PostgreSQL database {}", quote_ident(db_name));
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
        MapObject { field_name: String },
        ArrayScalar { field_name: String },
        ArrayObject { field_name: String },
    }

    #[derive(Clone)]
    struct CountNode {
        name: String,
        is_array: bool,
        mongo_count: u64,
        pg_table_key: Option<String>,
        pg_table_name: Option<String>,
        pg_row_count: Option<i64>,
        md5_summary: Option<PostImportMd5Summary>,
        count_diff_rows: Vec<PostImportCountDiffRow>,
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
        global_table_counts: &HashMap<String, PostImportTableRow>,
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
                    let child_fields = dynamic_map_value_fields(sub_fields).unwrap_or(sub_fields);
                    let table_name =
                        child_table_name(parent_table_name, &sanitize_pg_name(raw_name), pg_schema);
                    let table_ref = resolve_post_import_table_row(
                        &table_name,
                        table_counts,
                        global_table_counts,
                    );
                    nodes.push(CountNode {
                        name: raw_name.to_string(),
                        is_array: false,
                        mongo_count: 0,
                        pg_table_key: table_ref.map(|_| table_name.clone()),
                        pg_table_name: table_ref.and_then(|t| {
                            Some(match &t.schema_name {
                                Some(schema) => format!("{}.{}", schema, t.table_name),
                                None => t.table_name.clone(),
                            })
                        }),
                        pg_row_count: table_ref.map(|t| t.row_count),
                        md5_summary: md5_summaries.get(&table_name).cloned(),
                        count_diff_rows: Vec::new(),
                        kind: if std::ptr::eq(child_fields, sub_fields) {
                            CountNodeKind::Object {
                                field_name: raw_name.to_string(),
                            }
                        } else {
                            CountNodeKind::MapObject {
                                field_name: raw_name.to_string(),
                            }
                        },
                        children: build_field_nodes(
                            &table_name,
                            child_fields,
                            pg_schema,
                            table_counts,
                            global_table_counts,
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
                    let table_ref = resolve_post_import_table_row(
                        &table_name,
                        table_counts,
                        global_table_counts,
                    );
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
                                        global_table_counts,
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
                        pg_table_key: table_ref.map(|_| table_name.clone()),
                        pg_table_name: table_ref.and_then(|t| {
                            Some(match &t.schema_name {
                                Some(schema) => format!("{}.{}", schema, t.table_name),
                                None => t.table_name.clone(),
                            })
                        }),
                        pg_row_count: table_ref.map(|t| t.row_count),
                        md5_summary: md5_summaries.get(&table_name).cloned(),
                        count_diff_rows: Vec::new(),
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
                        if !child_doc.is_empty() {
                            node.mongo_count += 1;
                            count_children(&mut node.children, child_doc);
                        }
                    }
                }
                CountNodeKind::MapObject { field_name } => {
                    if let Some(Bson::Document(child_doc)) = doc.get(field_name) {
                        node.mongo_count += count_dynamic_map_entries(child_doc);
                        for value in child_doc.values() {
                            if let Bson::Document(entry_doc) = value {
                                if !entry_doc.is_empty() {
                                    count_children(&mut node.children, entry_doc);
                                }
                            }
                        }
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
                                if !child_doc.is_empty() {
                                    node.mongo_count += 1;
                                    count_children(&mut node.children, child_doc);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn into_post_import_node(node: CountNode) -> PostImportNode {
        let md5_confirms_match = node.md5_summary.as_ref().is_some_and(|summary| {
            summary.mongo_md5 == summary.pg_md5 && summary.mismatches.is_empty()
        });
        let display_mongo_count = if md5_confirms_match {
            node.pg_row_count
                .and_then(|count| u64::try_from(count).ok())
                .unwrap_or(node.mongo_count)
        } else {
            node.mongo_count
        };

        PostImportNode {
            name: node.name,
            is_array: node.is_array,
            mongo_count: display_mongo_count,
            pg_table_name: node.pg_table_name,
            pg_row_count: node.pg_row_count,
            md5_summary: node.md5_summary,
            count_diff_rows: node.count_diff_rows,
            children: node
                .children
                .into_iter()
                .map(into_post_import_node)
                .collect(),
        }
    }

    fn collect_rowcount_mismatch_tables(node: &CountNode, out: &mut Vec<String>) {
        if let (Some(table_name), Some(pg_rows)) = (&node.pg_table_key, node.pg_row_count) {
            if pg_rows != node.mongo_count as i64 {
                let md5_confirms_match = node.md5_summary.as_ref().is_some_and(|summary| {
                    summary.mongo_md5 == summary.pg_md5 && summary.mismatches.is_empty()
                });
                if !md5_confirms_match {
                    out.push(table_name.clone());
                }
            }
        }
        for child in &node.children {
            collect_rowcount_mismatch_tables(child, out);
        }
    }

    fn apply_count_diff_rows(
        node: &mut CountNode,
        rows_by_table: &HashMap<String, Vec<PostImportCountDiffRow>>,
    ) {
        if let Some(table_name) = &node.pg_table_key {
            if let Some(rows) = rows_by_table.get(table_name) {
                node.count_diff_rows = rows.clone();
            }
        }
        for child in &mut node.children {
            apply_count_diff_rows(child, rows_by_table);
        }
    }

    let (db_name, only_collection) = split_namespace_scope(namespace);

    let mongo_client = Client::with_uri_str(source_uri).await.with_context(|| {
        format!(
            "{}: failed to connect to MongoDB using SOURCE_URI",
            connection_failed_context("mongo", "connect")
        )
    })?;
    let mongo_db = mongo_client.database(db_name);
    let mut collection_names = mongo_db.list_collection_names().await.with_context(|| {
        format!(
            "{}: failed to list collections for MongoDB database {db_name}",
            connection_failed_context("mongo", "query")
        )
    })?;
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

    let pg_client = connect_pg_client(target_uri).await.with_context(|| {
        format!(
            "{}: failed during post-import PostgreSQL connection setup",
            connection_failed_context("pg", "connect")
        )
    })?;

    let total_collections = collection_names.len();
    let mut global_table_rows: HashMap<String, PostImportTableRow> = HashMap::new();
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
            let root_table_name = resolve_root_table_name(&parsed_tables, &coll_name);
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
            for (name, row) in &table_rows {
                global_table_rows
                    .entry(name.clone())
                    .or_insert_with(|| PostImportTableRow {
                        schema_name: row.schema_name.clone(),
                        table_name: row.table_name.clone(),
                        row_count: row.row_count,
                    });
            }
            let root_ref = table_rows.get(&root_table_name);
            let md5_summaries = if include_md5 {
                info!(
                    "[{}/{}] ⚙️  compute hash (md5) for {}.{}",
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
                                            source_type: column.source_type,
                                            target_field: column.target_field,
                                            target_type: column.target_type,
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
                        warn!(
                            "failed to compute md5 summary for {}.{}: {}",
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
                pg_table_key: root_ref.map(|_| root_table_name.clone()),
                pg_table_name: root_ref.and_then(|t| {
                    Some(match &t.schema_name {
                        Some(schema) => format!("{}.{}", schema, t.table_name),
                        None => t.table_name.clone(),
                    })
                }),
                pg_row_count: root_ref.map(|t| t.row_count),
                md5_summary: md5_summaries.get(&root_table_name).cloned(),
                count_diff_rows: Vec::new(),
                kind: CountNodeKind::Root,
                children: build_field_nodes(
                    &root_table_name,
                    &schema.object,
                    schema_name.as_deref(),
                    &table_rows,
                    &global_table_rows,
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

            let mut mismatch_tables = Vec::new();
            collect_rowcount_mismatch_tables(&root, &mut mismatch_tables);
            mismatch_tables.sort();
            mismatch_tables.dedup();

            if !mismatch_tables.is_empty() {
                for table_name in &mismatch_tables {
                    info!(
                        "[{}/{}] Rowcount diff detected for {}.{} table {}: searching first 5 differences",
                        index + 1,
                        total_collections,
                        db_name,
                        coll_name,
                        table_name,
                    );
                }

                let mismatch_set = mismatch_tables.iter().cloned().collect::<HashSet<_>>();
                let mut rows_by_table = HashMap::new();

                if !md5_summaries.is_empty() {
                    for (table_name, summary) in &md5_summaries {
                        if !mismatch_set.contains(table_name) {
                            continue;
                        }
                        rows_by_table.insert(
                            table_name.clone(),
                            summary
                                .mismatches
                                .iter()
                                .map(|mismatch| PostImportCountDiffRow {
                                    row_index: mismatch.row_index,
                                    mongo_values: mismatch.mongo_values.clone(),
                                    pg_values: mismatch.pg_values.clone(),
                                })
                                .collect::<Vec<_>>(),
                        );
                    }
                } else {
                    match compute_md5_summaries_for_collection(&coll_name, config_path).await {
                        Ok(summaries) => {
                            for summary in summaries {
                                if !mismatch_set.contains(&summary.table_name) {
                                    continue;
                                }
                                rows_by_table.insert(
                                    summary.table_name,
                                    summary
                                        .summary
                                        .mismatches
                                        .into_iter()
                                        .map(|mismatch| PostImportCountDiffRow {
                                            row_index: mismatch.row_index,
                                            mongo_values: mismatch.mongo_values,
                                            pg_values: mismatch.pg_values,
                                        })
                                        .collect::<Vec<_>>(),
                                );
                            }
                        }
                        Err(err) => {
                            warn!(
                                "failed to collect count differences for {}.{}: {}",
                                db_name, coll_name, err
                            );
                        }
                    }
                }

                apply_count_diff_rows(&mut root, &rows_by_table);
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
                count_diff_rows: Vec::new(),
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
        apply_collection_property_filters, apply_config_overrides, build_collection_mappings,
        build_collection_mappings_with_timestamp_fields, child_row_objects_for_mapping,
        classify_unauthorized_retry, collect_infer_type_warnings, collect_nullable_scalar_warnings,
        connection_failed_context, count_dynamic_map_entries, detect_candidate_groups,
        dynamic_map_value_fields, format_runtime_log_line, infer_query_max_time,
        is_unauthorized_cursor_error, plan_export_jobs_for_collections,
        render_ddl_from_mapping_tables, resolve_collections_dir, resolve_export_chunk_size,
        resolve_export_sql_lookup_for_collection, resolve_infer_auth_retry_max,
        resolve_infer_chunk_size, resolve_log_level_precedence, resolve_post_import_table_row,
        resolve_root_table_name, sanitize_name, should_infer_collection, strip_psql_preamble,
        timeout_fallback_hint, validate_group_schema_compatibility, ConfigOverrides,
        PostImportTableRow, UnauthorizedRetryDecision, DEFAULT_EXPORT_CHUNK_ROWS,
        DEFAULT_INFER_AUTH_RETRY_MAX, DEFAULT_INFER_CHUNK_SIZE, DEFAULT_SAMPLE_MAX_TIME,
    };
    use anyhow::{anyhow, Context as _};
    use bson::doc;
    use log::{Level, LevelFilter};
    use mongo2pg::analyzer::Analyzer;
    use mongo2pg::export::{resolve_export_write_backend, ExportWriteBackend};
    use mongo2pg::schema_diagram::Table;
    use serde::Deserialize;
    use std::collections::{HashMap, HashSet};
    use std::io::Write;
    use std::path::Path;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tempfile::NamedTempFile;

    #[derive(Debug, Deserialize)]
    struct TestTomlProjectConfig {
        project: Option<TestTomlProjectSection>,
        source: Option<TestTomlSourceSection>,
        target: Option<TestTomlTargetSection>,
        kafka: Option<TestTomlKafkaSection>,
    }

    #[derive(Debug, Deserialize)]
    struct TestTomlProjectSection {
        project_dir: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    struct TestTomlSourceSection {
        uri: Option<String>,
        namespace: Option<String>,
        number: Option<u64>,
        percent: Option<f64>,
        max_time_ms: Option<u64>,
        chunk_size: Option<u64>,
        auth_retry_max: Option<u32>,
        jsonb: Option<bool>,
        #[serde(default)]
        include: Vec<String>,
        #[serde(default)]
        exclude: Vec<String>,
    }

    #[derive(Debug, Deserialize)]
    struct TestTomlTargetSection {
        schema_name: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    struct TestTomlKafkaSection {
        topics: Option<Vec<String>>,
        max_messages: Option<usize>,
        offset: Option<String>,
        auto_offset_reset: Option<String>,
    }

    #[test]
    fn should_infer_collection_honors_exclude_before_include() {
        let include = vec!["users".to_owned()];
        let exclude = vec!["users".to_owned(), "audit".to_owned()];

        assert!(!should_infer_collection("users", &include, &exclude));
        assert!(!should_infer_collection("orders", &include, &exclude));
    }

    #[test]
    fn apply_config_overrides_updates_source_and_target_values() {
        let mut file = NamedTempFile::new().expect("temp file should be created");
        writeln!(
            file,
            "[project]\ntitle = \"t\"\nbase_dir = \"/tmp\"\nproject_dir = \"p\"\n\n[source]\nuri = \"mongodb://old\"\nnamespace = \"old.ns\"\nnumber = 10\npercent = 1.0\njsonb = false\n\n[target]\nschema_name = \"old_schema\"\n"
        )
        .expect("seed toml should be written");

        apply_config_overrides(
            file.path(),
            &ConfigOverrides {
                project_dir: Some("sample_airbnb".to_owned()),
                source_uri: Some("mongodb://new".to_owned()),
                namespace: Some("new.ns".to_owned()),
                number: Some(100),
                percent: Some(20.0),
                max_time_ms: Some(60_000),
                chunk_size: Some(1_000_000),
                auth_retry_max: Some(3),
                jsonb: Some(true),
                target_schema_name: Some("sample_training".to_owned()),
                ..ConfigOverrides::default()
            },
        )
        .expect("overrides should be applied");

        let updated =
            std::fs::read_to_string(file.path()).expect("updated file should be readable");
        let parsed: TestTomlProjectConfig =
            toml::from_str(&updated).expect("updated toml should parse");

        let project = parsed.project.expect("project section should exist");
        assert_eq!(project.project_dir.as_deref(), Some("sample_airbnb"));

        let source = parsed.source.expect("source section should exist");
        assert_eq!(source.uri.as_deref(), Some("mongodb://new"));
        assert_eq!(source.namespace.as_deref(), Some("new.ns"));
        assert_eq!(source.number, Some(100));
        assert_eq!(source.percent, Some(20.0));
        assert_eq!(source.max_time_ms, Some(60_000));
        assert_eq!(source.chunk_size, Some(1_000_000));
        assert_eq!(source.auth_retry_max, Some(3));
        assert_eq!(source.jsonb, Some(true));

        let target = parsed.target.expect("target section should exist");
        assert_eq!(target.schema_name.as_deref(), Some("sample_training"));
    }

    #[test]
    fn apply_config_overrides_updates_kafka_values() {
        let mut file = NamedTempFile::new().expect("temp file should be created");
        writeln!(
            file,
            "[project]\ntitle = \"t\"\nbase_dir = \"/tmp\"\nproject_dir = \"p\"\n\n[kafka]\ntopics = [\"old\"]\nmax_messages = 10\noffset = \"latest\"\nauto_offset_reset = \"latest\"\n"
        )
        .expect("seed toml should be written");

        apply_config_overrides(
            file.path(),
            &ConfigOverrides {
                kafka_topics: Some(vec!["a".to_owned(), "b".to_owned()]),
                kafka_max_messages: Some(999),
                kafka_offset: Some("earliest".to_owned()),
                ..ConfigOverrides::default()
            },
        )
        .expect("kafka overrides should be applied");

        let updated =
            std::fs::read_to_string(file.path()).expect("updated file should be readable");
        let parsed: TestTomlProjectConfig =
            toml::from_str(&updated).expect("updated toml should parse");
        let kafka = parsed.kafka.expect("kafka section should exist");

        assert_eq!(kafka.topics, Some(vec!["a".to_owned(), "b".to_owned()]));
        assert_eq!(kafka.max_messages, Some(999));
        assert_eq!(kafka.offset.as_deref(), Some("earliest"));
        assert_eq!(kafka.auto_offset_reset.as_deref(), Some("earliest"));
    }

    #[test]
    fn infer_query_max_time_uses_configured_ms_when_present() {
        assert_eq!(infer_query_max_time(Some(60_000)), Duration::from_secs(60));
        assert_eq!(infer_query_max_time(Some(0)), DEFAULT_SAMPLE_MAX_TIME);
        assert_eq!(infer_query_max_time(None), DEFAULT_SAMPLE_MAX_TIME);
    }

    #[test]
    fn resolve_infer_chunk_size_uses_default_or_configured_value() {
        assert_eq!(
            resolve_infer_chunk_size(None).expect("default chunk size should resolve"),
            DEFAULT_INFER_CHUNK_SIZE
        );
        assert_eq!(
            resolve_infer_chunk_size(Some(2_000_000))
                .expect("configured chunk size should resolve"),
            2_000_000
        );
    }

    #[test]
    fn resolve_infer_chunk_size_rejects_invalid_values() {
        assert!(resolve_infer_chunk_size(Some(0)).is_err());
        assert!(resolve_infer_chunk_size(Some(i64::MAX as u64 + 1)).is_err());
    }

    #[test]
    fn resolve_export_chunk_size_uses_default_or_configured_value() {
        assert_eq!(
            resolve_export_chunk_size(None).expect("default export chunk size should resolve"),
            DEFAULT_EXPORT_CHUNK_ROWS
        );
        assert_eq!(
            resolve_export_chunk_size(Some(100_000))
                .expect("configured export chunk size should resolve"),
            100_000
        );
    }

    #[test]
    fn resolve_export_chunk_size_rejects_invalid_values() {
        assert!(resolve_export_chunk_size(Some(0)).is_err());
        assert!(resolve_export_chunk_size(Some(i64::MAX as u64 + 1)).is_err());
    }

    #[test]
    fn resolve_export_write_backend_uses_local_for_non_gs_base_dir() {
        let backend = resolve_export_write_backend(Path::new("/tmp/work"))
            .expect("local path should resolve to filesystem backend");
        assert_eq!(backend, ExportWriteBackend::LocalFs);
    }

    #[test]
    fn resolve_export_write_backend_uses_gcs_for_gs_prefix() {
        let backend = resolve_export_write_backend(Path::new("gs://my-bucket/path/to/base"))
            .expect("gs URI should resolve to gcs backend");
        assert_eq!(
            backend,
            ExportWriteBackend::Gcs {
                bucket: "my-bucket".to_owned(),
                prefix: "path/to/base".to_owned(),
            }
        );
    }

    #[test]
    fn resolve_export_write_backend_rejects_empty_gcs_bucket() {
        let err =
            resolve_export_write_backend(Path::new("gs://")).expect_err("empty bucket must fail");
        assert!(
            err.to_string().contains("missing bucket name"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    fn resolve_infer_auth_retry_max_uses_default_or_configured_value() {
        assert_eq!(
            resolve_infer_auth_retry_max(None).expect("default auth retry should resolve"),
            DEFAULT_INFER_AUTH_RETRY_MAX
        );
        assert_eq!(
            resolve_infer_auth_retry_max(Some(5)).expect("configured auth retry should resolve"),
            5
        );
    }

    #[test]
    fn resolve_infer_auth_retry_max_rejects_invalid_values() {
        assert!(resolve_infer_auth_retry_max(Some(101)).is_err());
    }

    #[test]
    fn unauthorized_error_classifier_matches_code_and_text_markers() {
        assert!(is_unauthorized_cursor_error(
            "Command failed: Error code 13 (Unauthorized): Command getMore requires authentication"
        ));
        assert!(is_unauthorized_cursor_error("error: unauthorized"));
        assert!(!is_unauthorized_cursor_error(
            "Error code 50 (MaxTimeMSExpired)"
        ));
    }

    #[test]
    fn classify_unauthorized_retry_decides_retry_then_exhausted() {
        assert_eq!(
            classify_unauthorized_retry(
                "Error code 13 (Unauthorized): Command getMore requires authentication",
                0,
                2
            ),
            Some(UnauthorizedRetryDecision::Retry)
        );
        assert_eq!(
            classify_unauthorized_retry(
                "Error code 13 (Unauthorized): Command getMore requires authentication",
                2,
                2
            ),
            Some(UnauthorizedRetryDecision::Exhausted)
        );
        assert_eq!(
            classify_unauthorized_retry("Error code 50 (MaxTimeMSExpired)", 0, 2),
            None
        );
    }

    #[test]
    fn timeout_fallback_hint_mentions_configured_max_time() {
        let hinted = timeout_fallback_hint(
            "Command failed: Error code 50 (MaxTimeMSExpired)",
            Some(60_000),
        );
        assert!(hinted.contains("source.max_time_ms=60000ms"));
        assert!(timeout_fallback_hint("some other error", Some(60_000)).is_empty());
    }

    #[test]
    fn resolve_log_level_precedence_prefers_cli_over_config() {
        let level = resolve_log_level_precedence(Some("debug"), Some("error"))
            .expect("log level precedence should parse");
        assert_eq!(level, LevelFilter::Debug);
    }

    #[test]
    fn resolve_log_level_precedence_uses_config_then_default() {
        let config_level =
            resolve_log_level_precedence(None, Some("warn")).expect("config level should parse");
        assert_eq!(config_level, LevelFilter::Warn);

        let default_level =
            resolve_log_level_precedence(None, None).expect("default level should resolve to info");
        assert_eq!(default_level, LevelFilter::Info);
    }

    #[test]
    fn detect_candidate_groups_groups_by_last_underscore_prefix() {
        let names = vec![
            "events_lmfr".to_owned(),
            "events_lmza".to_owned(),
            "events_bcit".to_owned(),
            "users".to_owned(),
            "ciam_prod".to_owned(),
        ];
        let groups = detect_candidate_groups(&names);
        assert_eq!(groups.len(), 1, "only 'events' prefix should form a group");
        let g = &groups[0];
        assert_eq!(g.prefix, "events");
        assert_eq!(g.members.len(), 3);
        assert!(g.members.contains(&"events_bcit".to_owned()));
        assert!(g.members.contains(&"events_lmfr".to_owned()));
        assert!(g.members.contains(&"events_lmza".to_owned()));
        assert_eq!(g.representative, "events_bcit"); // first alphabetically
    }

    #[test]
    fn detect_candidate_groups_skips_singletons() {
        let names = vec![
            "events_lmfr".to_owned(),
            "users".to_owned(),
            "orders".to_owned(),
        ];
        let groups = detect_candidate_groups(&names);
        assert!(
            groups.is_empty(),
            "no group should form when prefix has only one member"
        );
    }

    #[test]
    fn detect_candidate_groups_handles_multiple_prefixes() {
        let names = vec![
            "events_lmfr".to_owned(),
            "events_lmza".to_owned(),
            "orders_eu".to_owned(),
            "orders_us".to_owned(),
        ];
        let groups = detect_candidate_groups(&names);
        let prefixes: Vec<&str> = groups.iter().map(|g| g.prefix.as_str()).collect();
        assert!(prefixes.contains(&"events"), "events group expected");
        assert!(prefixes.contains(&"orders"), "orders group expected");
    }

    #[test]
    fn validate_group_schema_compatibility_returns_true_for_identical_fields() {
        use std::time::{SystemTime, UNIX_EPOCH};
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("mongo2pg-group-compat-test-{unique}"));
        std::fs::create_dir_all(&dir).expect("create temp dir");

        let schema_a = r#"{"count":10,"sampled":10,"object":{"_id":{"probability":1.0,"types":{"ObjectId":{"probability":1.0,"sampled":10}}},"name":{"probability":1.0,"types":{"String":{"probability":1.0,"sampled":10}}}}}"#;
        for coll in ["events_lmfr", "events_lmza"] {
            let coll_dir = dir.join(coll);
            std::fs::create_dir_all(&coll_dir).expect("create coll dir");
            std::fs::write(coll_dir.join(format!("{coll}.json")), schema_a).expect("write schema");
        }

        let group = super::CollectionGroup {
            prefix: "events".to_owned(),
            members: vec!["events_lmfr".to_owned(), "events_lmza".to_owned()],
            representative: "events_lmfr".to_owned(),
        };
        assert!(validate_group_schema_compatibility(&dir, &group));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn validate_group_schema_compatibility_allows_different_fields_when_artifacts_parse() {
        use std::time::{SystemTime, UNIX_EPOCH};
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("mongo2pg-group-compat-mismatch-{unique}"));
        std::fs::create_dir_all(&dir).expect("create temp dir");

        let schema_a = r#"{"count":10,"sampled":10,"object":{"_id":{"probability":1.0,"types":{"ObjectId":{"probability":1.0,"sampled":10}}},"name":{"probability":1.0,"types":{"String":{"probability":1.0,"sampled":10}}}}}"#;
        let schema_b = r#"{"count":10,"sampled":10,"object":{"_id":{"probability":1.0,"types":{"ObjectId":{"probability":1.0,"sampled":10}}},"title":{"probability":1.0,"types":{"String":{"probability":1.0,"sampled":10}}}}}"#;

        for (coll, schema) in [("events_lmfr", schema_a), ("events_lmza", schema_b)] {
            let coll_dir = dir.join(coll);
            std::fs::create_dir_all(&coll_dir).expect("create coll dir");
            std::fs::write(coll_dir.join(format!("{coll}.json")), schema).expect("write schema");
        }

        let group = super::CollectionGroup {
            prefix: "events".to_owned(),
            members: vec!["events_lmfr".to_owned(), "events_lmza".to_owned()],
            representative: "events_lmfr".to_owned(),
        };
        assert!(validate_group_schema_compatibility(&dir, &group));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn format_runtime_log_line_includes_timestamp_elapsed_and_level() {
        let line = format_runtime_log_line(Level::Info, Duration::from_millis(1250), "hello");
        assert!(line.contains("+1s"));
        assert!(!line.contains(".250"));
        assert!(line.contains("[INFO]"));
        assert!(line.ends_with("hello"));
        let timestamp = line
            .split(' ')
            .next()
            .expect("line should include timestamp");
        assert_eq!(timestamp.len(), 19);
        assert!(!timestamp.contains('.'));
        assert!(!timestamp.contains('+'));
        assert!(!timestamp.contains('Z'));
    }

    #[test]
    fn connection_failed_context_has_stable_backend_token() {
        let ctx = connection_failed_context("mongo", "connect");
        assert_eq!(ctx, "connection_failed backend=mongo operation=connect");
    }

    #[test]
    fn attributed_connection_context_preserves_root_cause_chain() {
        let err = Err::<(), _>(anyhow!("driver refused connection"))
            .with_context(|| connection_failed_context("pg", "connect"))
            .expect_err("context wrap should return error");

        let rendered = format!("{err:#}");
        assert!(rendered.contains("connection_failed backend=pg operation=connect"));
        assert!(rendered.contains("driver refused connection"));
    }

    #[test]
    fn runtime_log_line_keeps_connection_failed_token_in_message() {
        let line = format_runtime_log_line(
            Level::Error,
            Duration::from_secs(2),
            "connection_failed backend=kafka operation=consume: timeout",
        );
        assert!(line.contains("connection_failed backend=kafka operation=consume"));
    }

    #[test]
    fn child_row_objects_for_mapping_expands_dynamic_map_entries() {
        let mapping_yaml = r#"
collection_name: tier_and_details
mongo_dbname: sample_analytics
mongo_path: .tier_and_details
pg_mapping:
  dbname: sample_analytics
  schema_name: sample_analytics
  table_name: tier_and_details
  columns:
    - source_field: key
      target_field: key
      data_type: text
      nullable: false
    - source_field: active
      target_field: active
      data_type: boolean
      nullable: false
    - source_field: tier
      target_field: tier
      data_type: text
      nullable: false
  ddl:
    name: tier_and_details
    columns:
      - name: id
        sql_type: BIGSERIAL
        nullable: false
        primary_key: true
      - name: customers_id
        sql_type: UUID
        nullable: false
        primary_key: false
      - name: key
        sql_type: TEXT
        nullable: false
        primary_key: false
      - name: active
        sql_type: BOOLEAN
        nullable: false
        primary_key: false
      - name: tier
        sql_type: TEXT
        nullable: false
        primary_key: false
    foreign_keys:
      - from_col: customers_id
        to_table: customers
        to_col: id
"#;

        let mapping: super::CollectionMapping = serde_yaml::from_str(mapping_yaml).unwrap();
        let node = serde_json::json!({
            "0df078f33aa74a2e9696e0520c1a828a": { "tier": "Bronze", "active": true },
            "699456451cc24f028d2aa99d7534c219": { "tier": "Silver", "active": false }
        });

        let rows = child_row_objects_for_mapping(&node, &mapping);
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().any(|row| {
            row.get("key")
                == Some(&serde_json::Value::String(
                    "0df078f33aa74a2e9696e0520c1a828a".to_owned(),
                ))
                && row.get("tier") == Some(&serde_json::Value::String("Bronze".to_owned()))
                && row.get("active") == Some(&serde_json::Value::Bool(true))
        }));
        assert!(rows.iter().any(|row| {
            row.get("key")
                == Some(&serde_json::Value::String(
                    "699456451cc24f028d2aa99d7534c219".to_owned(),
                ))
                && row.get("tier") == Some(&serde_json::Value::String("Silver".to_owned()))
                && row.get("active") == Some(&serde_json::Value::Bool(false))
        }));
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
    fn resolve_root_table_name_prefers_collection_match_when_not_first() {
        let parsed_tables = vec![
            Table {
                name: "tier_and_details".to_owned(),
                columns: Vec::new(),
                foreign_keys: Vec::new(),
            },
            Table {
                name: "accounts".to_owned(),
                columns: Vec::new(),
                foreign_keys: Vec::new(),
            },
        ];

        let root = resolve_root_table_name(&parsed_tables, "accounts");
        assert_eq!(root, "accounts");
    }

    #[test]
    fn resolve_root_table_name_matches_schema_qualified_collection_table() {
        let parsed_tables = vec![
            Table {
                name: "sample_analytics.accounts".to_owned(),
                columns: Vec::new(),
                foreign_keys: Vec::new(),
            },
            Table {
                name: "sample_analytics.accounts_addresses".to_owned(),
                columns: Vec::new(),
                foreign_keys: Vec::new(),
            },
        ];

        let root = resolve_root_table_name(&parsed_tables, "accounts");
        assert_eq!(root, "sample_analytics.accounts");
    }

    #[test]
    fn resolve_post_import_table_row_falls_back_to_global_rows() {
        let local_rows = HashMap::new();
        let mut global_rows = HashMap::new();
        global_rows.insert(
            "accounts".to_owned(),
            PostImportTableRow {
                schema_name: Some("sample_analytics".to_owned()),
                table_name: "accounts".to_owned(),
                row_count: 1746,
            },
        );

        let row = resolve_post_import_table_row("accounts", &local_rows, &global_rows)
            .expect("global table row should be found");
        assert_eq!(row.table_name, "accounts");
        assert_eq!(row.row_count, 1746);
    }

    #[test]
    fn dynamic_map_value_fields_detects_uuid_keyed_map_shape() {
        let docs = vec![doc! {
            "_id": "customer-1",
            "tier_and_details": {
                "0df078f33aa74a2e9696e0520c1a828a": {
                    "active": true,
                    "tier": "bronze"
                }
            }
        }];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let tier_and_details = schema
            .object
            .get("tier_and_details")
            .expect("tier_and_details field should exist");
        let obj = tier_and_details
            .types
            .get("Object")
            .and_then(|ts| ts.object.as_ref())
            .expect("tier_and_details object schema should exist");

        let map_values = dynamic_map_value_fields(obj).expect("map value fields should exist");
        assert!(map_values.contains_key("active"));
        assert!(map_values.contains_key("tier"));
    }

    #[test]
    fn count_dynamic_map_entries_ignores_empty_map_objects() {
        let map_doc = doc! {
            "0df078f33aa74a2e9696e0520c1a828a": { "tier": "bronze", "active": true },
            "699456451cc24f028d2aa99d7534c219": { "tier": "silver", "active": false },
            "empty": {},
            "non_doc": "skip"
        };

        assert_eq!(count_dynamic_map_entries(&map_doc), 2);
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
    fn toml_target_schema_name_is_parsed() {
        let config: TestTomlProjectConfig = toml::from_str(
            r#"
[project]
base_dir = "/tmp"
project_dir = "demo"

[target]
schema_name = "shared_schema"
"#,
        )
        .expect("config should parse");

        let target = config.target.expect("target section should exist");
        assert_eq!(target.schema_name.as_deref(), Some("shared_schema"));
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
        assert_eq!(advices_mapping.mongo_path.as_deref(), Some(".advices"));
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
        assert_eq!(
            earnings_mapping.mongo_path.as_deref(),
            Some(".advices.earnings")
        );
        let earnings_columns = earnings_mapping
            .pg_mapping
            .columns
            .iter()
            .map(|column| column.target_field.as_str())
            .collect::<Vec<_>>();
        assert!(earnings_columns.contains(&"monthly_gain"));
    }

    #[test]
    fn build_collection_mappings_keeps_container_parent_table_for_nested_entities_children() {
        let docs = vec![doc! {
            "_id": bson::oid::ObjectId::new(),
            "text": "tweet",
            "entities": {
                "hashtags": [{ "text": "rust", "indices": [0_i32, 4_i32] }],
                "urls": [{ "url": "https://example.com", "indices": [5_i32, 10_i32] }],
                "user_mentions": [{
                    "name": "Ada",
                    "screen_name": "ada",
                    "indices": [11_i32, 14_i32]
                }]
            }
        }];

        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let mappings = build_collection_mappings("sample_training", "tweets", None, &schema);

        let entities_mapping = mappings
            .iter()
            .find(|(_, mapping)| mapping.mongo_path.as_deref() == Some(".entities"))
            .map(|(_, mapping)| mapping)
            .expect("entities mapping should exist");

        assert_eq!(entities_mapping.pg_mapping.table_name, "entities");
        assert!(entities_mapping.pg_mapping.ddl.is_some());

        let ddl = entities_mapping
            .pg_mapping
            .ddl
            .as_ref()
            .expect("entities ddl should exist");
        assert!(ddl.columns.iter().any(|column| column.name == "id"));
        assert!(ddl
            .foreign_keys
            .iter()
            .any(|fk| fk.to_table == "tweets" && fk.to_col == "id"));
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

    // #[test]
    // fn build_collection_mappings_keeps_map_table_name_with_reserved_names() {
    //     let docs = vec![doc! {
    //         "_id": "customer-1",
    //         "accounts": {
    //             "0df078f33aa74a2e9696e0520c1a828a": {
    //                 "active": true,
    //                 "tier": "gold"
    //             }
    //         }
    //     }];
    //     let mut analyzer = Analyzer::new(true);
    //     for doc in &docs {
    //         analyzer.process_document(doc);
    //     }
    //     let schema = analyzer.finish();
    //     let reserved_table_names = std::collections::HashSet::from(["accounts".to_owned()]);

    //     let mappings = build_collection_mappings_with_timestamp_fields(
    //         "sample_analytics",
    //         "customers",
    //         Some("sample_analytics"),
    //         &schema,
    //         &[],
    //         &reserved_table_names,
    //     );

    //     let accounts_mapping = mappings
    //         .iter()
    //         .find(|(_, mapping)| mapping.mongo_path.as_deref() == Some(".accounts"))
    //         .map(|(_, mapping)| mapping)
    //         .expect("accounts child mapping should exist");

    //     assert_eq!(accounts_mapping.pg_mapping.table_name, "accounts");
    // }

    #[test]
    fn build_collection_mappings_keeps_map_table_name_when_no_conflict() {
        let docs = vec![doc! {
            "_id": "customer-1",
            "accounts": {
                "0df078f33aa74a2e9696e0520c1a828a": {
                    "active": true,
                    "tier": "gold"
                }
            }
        }];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let mappings = build_collection_mappings_with_timestamp_fields(
            "sample_analytics",
            "customers",
            Some("sample_analytics"),
            &schema,
            &[],
            &std::collections::HashSet::new(),
        );

        let accounts_mapping = mappings
            .iter()
            .find(|(_, mapping)| mapping.mongo_path.as_deref() == Some(".accounts"))
            .map(|(_, mapping)| mapping)
            .expect("accounts child mapping should exist");

        assert_eq!(accounts_mapping.pg_mapping.table_name, "accounts");
    }

    #[test]
    fn build_collection_mappings_maps_geojson_and_sibling_object_without_field_name_dependency() {
        let docs = vec![doc! {
            "_id": bson::oid::ObjectId::new(),
            "theaterId": 1000_i32,
            "venue": {
                "details": {
                    "street1": "340 W Market",
                    "city": "Bloomington",
                    "state": "MN",
                    "zipcode": "55425"
                },
                "point": {
                    "type": "Point",
                    "coordinates": [-93.24565_f64, 44.85466_f64]
                }
            }
        }];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let mappings = build_collection_mappings_with_timestamp_fields(
            "sample_mflix",
            "theaters",
            None,
            &schema,
            &[],
            &std::collections::HashSet::new(),
        );

        let venue_mapping = mappings
            .iter()
            .find(|(_, mapping)| mapping.pg_mapping.table_name == "theaters_venue")
            .map(|(_, mapping)| mapping)
            .expect("theaters_venue mapping should exist");

        assert_eq!(venue_mapping.mongo_path.as_deref(), Some("."));
        let pairs = venue_mapping
            .pg_mapping
            .columns
            .iter()
            .map(|column| {
                (
                    column.source_field.as_str(),
                    column.target_field.as_str(),
                    column.data_type.as_str(),
                )
            })
            .collect::<Vec<_>>();
        let source_target = pairs
            .iter()
            .map(|(source, target, _)| (*source, *target))
            .collect::<Vec<_>>();

        assert!(source_target.contains(&("venue.details.street1", "street1")));
        assert!(source_target.contains(&("venue.details.city", "city")));
        assert!(source_target.contains(&("venue.details.state", "state")));
        assert!(source_target.contains(&("venue.details.zipcode", "zipcode")));
        assert!(pairs.contains(&("venue.point", "point", "geometry(point,4326)")));
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
        assert!(!columns.contains(&("key", "key")));
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
        // assert!(columns.contains(&("metadata.creation_date", "creation_date")));
        // assert!(columns.contains(&("metadata.status", "status")));
        assert!(!mappings
            .iter()
            .any(|(stem, _)| stem == "providers_metadata"));
    }

    #[test]
    fn build_collection_mappings_flattens_nested_array_object_with_source_paths() {
        let docs = vec![doc! {
            "_id": "project-1",
            "metadata": {
                "project_type": "demo"
            },
            "services": [{
                "metadata": {
                    "created_from": "auto",
                    "first_detection_time": "2024-01-01 00:00:00",
                    "last_update_time": "2024-01-01 00:00:00",
                    "managed": true,
                    "recognized": true
                }
            }]
        }];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let mappings = build_collection_mappings("dbapi", "projects", None, &schema);
        let services = mappings
            .iter()
            .find(|(_, mapping)| mapping.pg_mapping.table_name == "services")
            .map(|(_, mapping)| mapping)
            .expect("services mapping should exist");

        let metadata = mappings
            .iter()
            .find(|(_, mapping)| mapping.pg_mapping.table_name == "services_metadata")
            .map(|(_, mapping)| mapping)
            .expect("metadata mapping should exist");
        let columns = metadata
            .pg_mapping
            .columns
            .iter()
            .map(|column| (column.source_field.as_str(), column.target_field.as_str()))
            .collect::<Vec<_>>();

        assert_eq!(services.mongo_path.as_deref(), Some(".services"));
        assert!(columns.contains(&("created_from", "created_from")));
        assert!(columns.contains(&("first_detection_time", "first_detection_time")));
        assert!(columns.contains(&("last_update_time", "last_update_time")));
        assert!(columns.contains(&("managed", "managed")));
        assert!(columns.contains(&("recognized", "recognized")));
        assert!(mappings
            .iter()
            .any(|(_, mapping)| mapping.pg_mapping.table_name == "services_metadata"));
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
    fn build_collection_mappings_keeps_reserved_root_field_names() {
        let docs = vec![doc! {
            "_id": "account-1",
            "account_id": 7_i32,
            "limit": 10000_i32,
            "products": ["brokerage", "savings"]
        }];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let mappings = build_collection_mappings(
            "sample_analytics",
            "accounts",
            Some("sample_analytics"),
            &schema,
        );
        let root_mapping = mappings
            .iter()
            .find(|(stem, _)| stem == "accounts")
            .map(|(_, mapping)| mapping)
            .expect("accounts root mapping should exist");

        let columns = root_mapping
            .pg_mapping
            .columns
            .iter()
            .map(|column| (column.source_field.as_str(), column.target_field.as_str()))
            .collect::<Vec<_>>();

        assert!(columns.contains(&("limit", "limit")));
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
    fn should_regenerate_from_schema_when_objectid_pk_for_flattened_array_root() {
        let docs = vec![doc! {
            "_id": bson::oid::ObjectId::new(),
            "transactions": [{ "amount": 1_i32 }]
        }];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let mapping_tables = vec![super::DdlTableMapping {
            name: "transactions_transactions_transactions".to_owned(),
            columns: vec![
                super::DdlColumnMapping {
                    name: "id".to_owned(),
                    sql_type: "BIGSERIAL".to_owned(),
                    nullable: false,
                    primary_key: true,
                },
                super::DdlColumnMapping {
                    name: "amount".to_owned(),
                    sql_type: "INTEGER".to_owned(),
                    nullable: false,
                    primary_key: false,
                },
            ],
            foreign_keys: Vec::new(),
        }];

        assert!(super::should_regenerate_from_schema_when_objectid_pk(
            &schema,
            &mapping_tables,
            "transactions",
        ));
    }

    #[test]
    fn should_not_regenerate_when_mapping_already_contains_parent_uuid_column() {
        let docs = vec![doc! {
            "_id": bson::oid::ObjectId::new(),
            "transactions": [{ "amount": 1_i32 }]
        }];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let mapping_tables = vec![super::DdlTableMapping {
            name: "transactions_transactions_transactions".to_owned(),
            columns: vec![
                super::DdlColumnMapping {
                    name: "id".to_owned(),
                    sql_type: "BIGSERIAL".to_owned(),
                    nullable: false,
                    primary_key: true,
                },
                super::DdlColumnMapping {
                    name: "transactions_id".to_owned(),
                    sql_type: "UUID".to_owned(),
                    nullable: false,
                    primary_key: false,
                },
            ],
            foreign_keys: Vec::new(),
        }];

        assert!(!super::should_regenerate_from_schema_when_objectid_pk(
            &schema,
            &mapping_tables,
            "transactions",
        ));
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
    fn render_ddl_from_mapping_tables_skips_legacy_index_pseudo_columns() {
        let sql = render_ddl_from_mapping_tables(
            &[super::DdlTableMapping {
                name: "competitor".to_owned(),
                columns: vec![
                    super::DdlColumnMapping {
                        name: "id".to_owned(),
                        sql_type: "BIGSERIAL".to_owned(),
                        nullable: false,
                        primary_key: true,
                    },
                    super::DdlColumnMapping {
                        name: "companies_id".to_owned(),
                        sql_type: "UUID".to_owned(),
                        nullable: false,
                        primary_key: false,
                    },
                    super::DdlColumnMapping {
                        name: "CREATE".to_owned(),
                        sql_type:
                            "INDEX IF NOT EXISTS idx_competitor_companies_id ON competitor (companies_id"
                                .to_owned(),
                        nullable: false,
                        primary_key: false,
                    },
                ],
                foreign_keys: vec![super::DdlForeignKeyMapping {
                    from_col: "companies_id".to_owned(),
                    to_table: "companies".to_owned(),
                    to_col: "id".to_owned(),
                }],
            }],
            None,
        );

        assert!(!sql.contains(
            "CREATE INDEX IF NOT EXISTS idx_competitor_companies_id ON competitor (companies_id,"
        ));
        assert!(sql.contains("FOREIGN KEY (companies_id) REFERENCES companies (id)"));
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
    fn render_ddl_from_mapping_tables_adds_index_for_foreign_key_columns() {
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
            Some("dbapi"),
        );

        assert!(sql.contains(
            "CREATE INDEX IF NOT EXISTS \"idx_child_parent_id\" ON \"dbapi\".\"child\" (\"parent_id\");"
        ));
    }

    #[test]
    fn render_ddl_from_mapping_tables_adds_index_for_composite_foreign_keys() {
        let sql = render_ddl_from_mapping_tables(
            &[super::DdlTableMapping {
                name: "child".to_owned(),
                columns: vec![
                    super::DdlColumnMapping {
                        name: "id".to_owned(),
                        sql_type: "BIGSERIAL".to_owned(),
                        nullable: false,
                        primary_key: true,
                    },
                    super::DdlColumnMapping {
                        name: "parent_a".to_owned(),
                        sql_type: "BIGINT".to_owned(),
                        nullable: false,
                        primary_key: false,
                    },
                    super::DdlColumnMapping {
                        name: "parent_b".to_owned(),
                        sql_type: "BIGINT".to_owned(),
                        nullable: false,
                        primary_key: false,
                    },
                ],
                foreign_keys: vec![super::DdlForeignKeyMapping {
                    from_col: "parent_a, parent_b".to_owned(),
                    to_table: "parent".to_owned(),
                    to_col: "id_a, id_b".to_owned(),
                }],
            }],
            None,
        );

        assert!(sql.contains(
            "CREATE INDEX IF NOT EXISTS \"idx_child_parent_a_parent_b\" ON \"child\" (\"parent_a\", \"parent_b\");"
        ));
    }

    #[test]
    fn render_ddl_from_mapping_tables_emits_only_pgcrypto_for_uuid_without_geometry() {
        let sql = render_ddl_from_mapping_tables(
            &[
                super::DdlTableMapping {
                    name: "transactions".to_owned(),
                    columns: vec![
                        super::DdlColumnMapping {
                            name: "id".to_owned(),
                            sql_type: "UUID DEFAULT public.gen_random_uuid()".to_owned(),
                            nullable: false,
                            primary_key: true,
                        },
                        super::DdlColumnMapping {
                            name: "account_id".to_owned(),
                            sql_type: "INTEGER".to_owned(),
                            nullable: false,
                            primary_key: false,
                        },
                    ],
                    foreign_keys: Vec::new(),
                },
                super::DdlTableMapping {
                    name: "transactions_transactions".to_owned(),
                    columns: vec![
                        super::DdlColumnMapping {
                            name: "id".to_owned(),
                            sql_type: "BIGSERIAL".to_owned(),
                            nullable: false,
                            primary_key: true,
                        },
                        super::DdlColumnMapping {
                            name: "transactions_id".to_owned(),
                            sql_type: "UUID".to_owned(),
                            nullable: false,
                            primary_key: false,
                        },
                    ],
                    foreign_keys: vec![super::DdlForeignKeyMapping {
                        from_col: "transactions_id".to_owned(),
                        to_table: "transactions".to_owned(),
                        to_col: "id".to_owned(),
                    }],
                },
            ],
            Some("sample_analytics"),
        );

        assert!(sql.contains("CREATE EXTENSION IF NOT EXISTS \"pgcrypto\";"));
        assert!(!sql.contains("CREATE EXTENSION IF NOT EXISTS postgis;"));
    }

    #[test]
    fn render_ddl_from_mapping_tables_emits_postgis_when_geometry_present() {
        let sql = render_ddl_from_mapping_tables(
            &[super::DdlTableMapping {
                name: "venues".to_owned(),
                columns: vec![
                    super::DdlColumnMapping {
                        name: "id".to_owned(),
                        sql_type: "UUID DEFAULT public.gen_random_uuid()".to_owned(),
                        nullable: false,
                        primary_key: true,
                    },
                    super::DdlColumnMapping {
                        name: "point".to_owned(),
                        sql_type: "geometry(Point,4326)".to_owned(),
                        nullable: false,
                        primary_key: false,
                    },
                ],
                foreign_keys: Vec::new(),
            }],
            Some("sample_analytics"),
        );

        assert!(sql.contains("CREATE EXTENSION IF NOT EXISTS \"pgcrypto\";"));
        assert!(sql.contains("CREATE EXTENSION IF NOT EXISTS postgis;"));
    }

    #[test]
    fn load_mapping_ddl_tables_relaxes_non_key_not_null_when_mapping_columns_empty() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        let mapping_dir = std::env::temp_dir().join(format!("mongo2pg-mapping-ddl-test-{unique}"));
        std::fs::create_dir_all(&mapping_dir).expect("mapping dir should be created");

        let mapping_yaml = r#"
collection_name: investments
mongo_dbname: sample_training
mongo_path: .investments
pg_mapping:
  dbname: sample_training
  schema_name: sample_training
  table_name: investments
  columns: []
  ddl:
    name: investments
    columns:
      - name: id
        sql_type: BIGSERIAL
        nullable: false
        primary_key: true
      - name: funding_rounds_id
        sql_type: BIGINT
        nullable: false
        primary_key: false
      - name: company_name
        sql_type: TEXT
        nullable: false
        primary_key: false
    foreign_keys:
      - from_col: funding_rounds_id
        to_table: funding_rounds
        to_col: id
"#;

        let mapping_path = mapping_dir.join("mapping_investments.yaml");
        std::fs::write(&mapping_path, mapping_yaml).expect("mapping file should be written");

        let tables = super::load_mapping_ddl_tables(&mapping_dir)
            .expect("mapping load should succeed")
            .expect("ddl tables should be present");

        let investments = tables
            .iter()
            .find(|table| table.name == "investments")
            .expect("investments ddl should be loaded");

        let company_name = investments
            .columns
            .iter()
            .find(|column| column.name == "company_name")
            .expect("company_name should be present");
        assert!(company_name.nullable);

        let funding_rounds_id = investments
            .columns
            .iter()
            .find(|column| column.name == "funding_rounds_id")
            .expect("funding_rounds_id should be present");
        assert!(!funding_rounds_id.nullable);

        std::fs::remove_dir_all(&mapping_dir).expect("temp mapping dir should be removed");
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
    fn resolve_export_sql_lookup_for_collection_uses_grouped_mapping_when_direct_missing() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("mongo2pg-export-lookup-test-{unique}"));
        let tables_dir = root.join("schema").join("tables");
        let collections_dir = root.join("source").join("collections");
        std::fs::create_dir_all(&tables_dir).expect("tables dir should be created");
        std::fs::create_dir_all(collections_dir.join("events_a"))
            .expect("collections dir should be created");

        std::fs::write(
            tables_dir.join("events.sql"),
            "CREATE TABLE events (id BIGINT);",
        )
        .expect("grouped sql should be written");
        std::fs::write(
            collections_dir.join("events_a").join("mapping_events.yaml"),
            "mongo_path: .\npg_mapping:\n  table_name: events\n",
        )
        .expect("mapping file should be written");

        let mut sql_set = HashSet::new();
        sql_set.insert("events".to_owned());

        let sql_lookup = resolve_export_sql_lookup_for_collection(
            "events_a",
            &tables_dir,
            &collections_dir,
            &sql_set,
        );

        assert_eq!(sql_lookup.as_deref(), Some("events"));

        std::fs::remove_dir_all(&root).expect("temp root should be removed");
    }

    #[test]
    fn resolve_export_sql_lookup_for_collection_prefers_direct_sql() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("mongo2pg-export-direct-test-{unique}"));
        let tables_dir = root.join("schema").join("tables");
        let collections_dir = root.join("source").join("collections");
        std::fs::create_dir_all(&tables_dir).expect("tables dir should be created");
        std::fs::create_dir_all(collections_dir.join("events_a"))
            .expect("collections dir should be created");

        std::fs::write(
            tables_dir.join("events_a.sql"),
            "CREATE TABLE events_a (id BIGINT);",
        )
        .expect("direct sql should be written");
        std::fs::write(
            tables_dir.join("events.sql"),
            "CREATE TABLE events (id BIGINT);",
        )
        .expect("grouped sql should be written");
        std::fs::write(
            collections_dir.join("events_a").join("mapping_events.yaml"),
            "mongo_path: .\npg_mapping:\n  table_name: events\n",
        )
        .expect("mapping file should be written");

        let mut sql_set = HashSet::new();
        sql_set.insert("events_a".to_owned());
        sql_set.insert("events".to_owned());

        let sql_lookup = resolve_export_sql_lookup_for_collection(
            "events_a",
            &tables_dir,
            &collections_dir,
            &sql_set,
        );

        assert_eq!(sql_lookup.as_deref(), Some("events_a"));

        std::fs::remove_dir_all(&root).expect("temp root should be removed");
    }

    #[test]
    fn plan_export_jobs_groups_collections_by_sql_lookup() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("mongo2pg-export-jobs-test-{unique}"));
        let tables_dir = root.join("schema").join("tables");
        let collections_dir = root.join("source").join("collections");
        std::fs::create_dir_all(&tables_dir).expect("tables dir should be created");
        std::fs::create_dir_all(collections_dir.join("events_a"))
            .expect("events_a dir should be created");
        std::fs::create_dir_all(collections_dir.join("events_b"))
            .expect("events_b dir should be created");

        std::fs::write(
            tables_dir.join("events.sql"),
            "CREATE TABLE events (id BIGINT);",
        )
        .expect("events sql should be written");
        std::fs::write(
            tables_dir.join("users.sql"),
            "CREATE TABLE users (id BIGINT);",
        )
        .expect("users sql should be written");
        std::fs::write(
            collections_dir.join("events_a").join("mapping_events.yaml"),
            "mongo_path: .\npg_mapping:\n  table_name: events\n",
        )
        .expect("events_a mapping should be written");
        std::fs::write(
            collections_dir.join("events_b").join("mapping_events.yaml"),
            "mongo_path: .\npg_mapping:\n  table_name: events\n",
        )
        .expect("events_b mapping should be written");

        let sql_set = ["events", "users"]
            .into_iter()
            .map(str::to_owned)
            .collect::<HashSet<_>>();
        let jobs = plan_export_jobs_for_collections(
            vec![
                "events_a".to_owned(),
                "events_b".to_owned(),
                "users".to_owned(),
            ],
            &tables_dir,
            &collections_dir,
            &sql_set,
        );

        let mut events_members = jobs
            .get("events")
            .cloned()
            .expect("events group should be present");
        events_members.sort();
        assert_eq!(
            events_members,
            vec!["events_a".to_owned(), "events_b".to_owned()]
        );

        let users_members = jobs
            .get("users")
            .cloned()
            .expect("users group should be present");
        assert_eq!(users_members, vec!["users".to_owned()]);

        std::fs::remove_dir_all(&root).expect("temp root should be removed");
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
        assert!(content.contains("namespace = \"dbapi\""));
        assert!(!content.contains("#namespace = \"dbapi\""));

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

    use crate::{ExportArgs, ImportArgs, InferArgs, InitArgs, UriArg};
    use std::path::PathBuf;
    use tokio_postgres::NoTls;

    fn create_default_init_args(
        project_base: PathBuf,
        project_name: String,
        source_uri: Option<String>,
        target_uri: Option<String>,
        namespace: Option<String>,
    ) -> InitArgs {
        InitArgs {
            project_base,
            project_name,
            source_uri,
            target_uri,
            namespace,
        }
    }
    fn create_default_infer_args(config: PathBuf) -> InferArgs {
        InferArgs {
            mongo: UriArg { source_uri: None },
            namespace: None,
            number: Some(500),
            percent: None, // Set to None because it conflicts with `number`
            max_time_ms: None,
            chunk_size: None,
            auth_retry_max: None,
            jsonb: false,
            print_json: false,
            no_output: false,
            database_name: None,
            schema_name: None,
            project_dir: None,
            output_dir: None,
            config: Some(config), // Set to Some because it conflicts with `output_dir`
        }
    }
    fn create_default_export_args(config: PathBuf) -> ExportArgs {
        ExportArgs {
            mongo: UriArg { source_uri: None },
            collection: None,
            namespace: None,
            database_name: None,
            schema_name: None,
            project_dir: None,
            chunk_size: None,
            output_dir: None,
            config: Some(config),
        }
    }
    fn create_default_import_args(config: PathBuf) -> super::ImportArgs {
        ImportArgs {
            collection: None,
            namespace: None,
            database_name: None,
            schema_name: None,
            project_dir: None,
            config: config,
        }
    }

    use crate::{run_export, run_import, run_infer, run_init};
    use chrono::{DateTime, TimeZone, Utc};
    use indoc::indoc;
    use std::fs;
    use tempfile::TempDir; // Import the TempDir type
    use testcontainers_modules::{mongo, postgres, testcontainers::runners::AsyncRunner};

    // Data Structures
    #[derive(serde::Serialize)]
    struct Employee {
        id: i32,
        name: String,
        hire_date: DateTime<Utc>,
        created_at: String,
        last_update: String,
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
            hire_date: Utc.from_utc_datetime(
                &chrono::NaiveDate::from_ymd_opt(2024, 1, 15)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            ),
            created_at: "2024-01-15T00:00:00Z".to_string(),
            last_update: "2024-01-15T00:00:00Z".to_string(),
        };
        let employee_doc = bson::to_document(&new_employee)?;
        collection.insert_one(employee_doc).await?;

        let init_args = create_default_init_args(
            temp_dir.path().to_path_buf(),
            "test_project".to_owned(),
            Some(mongo_uri.clone()),
            Some(pg_connection_string.clone()),
            Some(db_mongo.to_owned()),
        );
        run_init(init_args).expect("init should succeed");

        assert!(
            temp_dir.path().join("test_project").exists(),
            "Project directory should be created"
        );
        assert!(
            temp_dir
                .path()
                .join("test_project")
                .join("schema")
                .join("tables")
                .exists(),
            "Schema tables directory should be created"
        );
        assert!(
            temp_dir
                .path()
                .join("test_project")
                .join("source")
                .join("collections")
                .exists(),
            "Source collections directory should be created"
        );
        assert!(
            temp_dir.path().join("test_project").join("data").exists(),
            "Data directory should be created"
        );
        assert!(
            temp_dir
                .path()
                .join("test_project")
                .join("config")
                .join("test_project.toml")
                .exists(),
            "Config file should be created"
        );
        assert!(
            temp_dir
                .path()
                .join("test_project")
                .join("reports")
                .exists(),
            "Reports folder should be created"
        );

        let conf_toml = std::fs::read_to_string(
            temp_dir
                .path()
                .join("test_project")
                .join("config")
                .join("test_project.toml"),
        )?;
        assert!(
            conf_toml.contains(&format!("uri = {mongo_uri:?}")),
            "Config should contain the MongoDB URI"
        );
        assert!(
            conf_toml.contains(&format!("uri = {pg_connection_string:?}")),
            "Config should contain the PostgreSQL URI"
        );
        assert!(
            conf_toml.contains(&format!(
                "base_dir = \"{}\"",
                temp_dir.path().to_path_buf().display()
            )),
            "Config should contain the project_base path"
        );
        assert!(
            conf_toml.contains("project_dir = \"test_project\""),
            "Config should contain the project_dir"
        );
        assert!(
            conf_toml.contains(&format!("namespace = \"{}\"", db_mongo)),
            "Config should contain the namespace"
        );
        assert!(conf_toml.contains("datetime_field = [\"created_at\", \"last_update\", \"updated_at\", \"*_date\", \"date\"]"), "Config should contain the default datetime field patterns");
        assert!(
            conf_toml.contains("jsonb = false"),
            "Config should contain the default jsonb setting"
        );

        let infer_args = create_default_infer_args(
            temp_dir
                .path()
                .join("test_project")
                .join("config")
                .join("test_project.toml"),
        );

        run_infer(infer_args).await?;

        log::info!("Inserted employee into MongoDB: {:?}", new_employee.name);

        let ddl_file_path = temp_dir
            .path()
            .join("test_project")
            .join("schema")
            .join("tables")
            .join("test_db")
            .join("employees.sql");

        assert!(
            ddl_file_path.exists(),
            "DDL file for employees should be created"
        );
        assert!(
            temp_dir
                .path()
                .join("test_project")
                .join("source")
                .join("collections")
                .join("employees")
                .join("employees.json")
                .exists(),
            "Source collections employees should be created"
        );
        assert!(
            temp_dir
                .path()
                .join("test_project")
                .join("source")
                .join("collections")
                .join("employees")
                .join("employees.stats.txt")
                .exists(),
            "Source collections stats txt format for employees should be created"
        );
        assert!(
            temp_dir
                .path()
                .join("test_project")
                .join("source")
                .join("collections")
                .join("employees")
                .join("employees.stats.yaml")
                .exists(),
            "Source collections stats yaml format for employees should be created"
        );
        assert!(
            temp_dir
                .path()
                .join("test_project")
                .join("source")
                .join("collections")
                .join("employees")
                .join("mapping_employees.yaml")
                .exists(),
            "Source collections mapping yaml format for employees should be created"
        );

        let expected_content = indoc! {r#"
            CREATE DATABASE "test_db";
            \connect "test_db"

            CREATE EXTENSION IF NOT EXISTS "pgcrypto";

            CREATE SCHEMA IF NOT EXISTS "employees";
            SET search_path = "employees", public;

            CREATE TABLE employees (
                id UUID DEFAULT public.gen_random_uuid() PRIMARY KEY,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                hire_date TIMESTAMP WITH TIME ZONE NOT NULL,
                last_update TIMESTAMP WITH TIME ZONE NOT NULL,
                name VARCHAR(20) NOT NULL
            );
        "#};
        let actual_content =
            fs::read_to_string(&ddl_file_path).expect("Should have been able to read the DDL file");

        // It will show a helpful diff if the content does not match.
        assert_eq!(actual_content.trim(), expected_content.trim());

        let config = temp_dir
            .path()
            .join("test_project")
            .join("config")
            .join("test_project.toml");
        let export_args = create_default_export_args(config.clone());
        run_export(export_args).await?;
        assert!(
            temp_dir
                .path()
                .join("test_project")
                .join("data")
                .join("test_db")
                .join("employees")
                .join("employees.csv.gz")
                .exists(),
            "Exported data employees.csv.gz should be created"
        );

        let import_args = create_default_import_args(config.clone());
        run_import(import_args).await?;

        let host_port = pg_container.get_host_port_ipv4(5432).await?;
        let pg_test_db_connection_string = format!(
            "postgres://postgres:postgres@localhost:{}/{}?sslmode=disable",
            host_port, "test_db"
        );
        let (client, connection) =
            tokio_postgres::connect(&pg_test_db_connection_string, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                log::error!("PostgreSQL connection error: {}", e);
            }
        });
        let employee_name = "Jane Doe";
        let hire_date = Utc.with_ymd_and_hms(2024, 1, 15, 0, 0, 0).unwrap();
        let created_at = Utc.with_ymd_and_hms(2024, 1, 15, 0, 0, 0).unwrap();
        let last_update = Utc.with_ymd_and_hms(2024, 1, 15, 0, 0, 0).unwrap();

        client
            .execute("SET search_path TO employees, public", &[])
            .await?;
        let row = client
            .query_one(
                "SELECT name, hire_date, created_at, last_update FROM employees WHERE name = $1",
                &[&employee_name],
            )
            .await?;

        let retrieved_name: &str = row.get("name");
        let retrieved_date: chrono::DateTime<chrono::Utc> = row.get("hire_date");
        let retrieved_created_at: chrono::DateTime<chrono::Utc> = row.get("created_at");
        let retrieved_last_update: chrono::DateTime<chrono::Utc> = row.get("last_update");

        assert_eq!(retrieved_name, employee_name);
        assert_eq!(retrieved_date, hire_date);
        assert_eq!(retrieved_created_at, created_at);
        assert_eq!(retrieved_last_update, last_update);

        Ok(())
    }
}
