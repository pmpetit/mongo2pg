//! Global CLI option structs: the top-level parser (`Cli`) plus per-subcommand
//! argument structs. Subcommand execution logic lives in `commands::*`; this module
//! only defines what Clap parses.

use std::path::PathBuf;

use clap::{Args, Parser};

use crate::cli::commands::Command;

/// MongoDB source URI argument shared across commands that connect to MongoDB.
/// When `-c` is also provided, this overrides the SOURCE_URI stored in the config file.
#[derive(Args, Debug, Clone)]
pub struct UriArg {
    /// MongoDB source connection URI (e.g. mongodb://localhost:27017) – required unless -c is given;
    /// overrides the SOURCE_URI stored in the config file when -c is also provided
    #[arg(long = "source-uri", required_unless_present = "config")]
    pub source_uri: Option<String>,
}

#[derive(Parser)]
#[command(
    name = "mongo2pg",
    about = "Infer a MongoDB collection schema and convert it to PostgreSQL DDL",
    version,
    // Allow bare `mongo2pg <SOURCE_URI> <NS>` without an explicit subcommand.
    args_conflicts_with_subcommands = true
)]
pub struct Cli {
    /// Runtime log level (error, warn, info, debug, trace). Overrides config value when set.
    #[arg(long = "log-level", global = true)]
    pub log_level: Option<String>,

    /// Runtime log format (text, json). Overrides config value when set.
    #[arg(long = "log-format", global = true)]
    pub log_format: Option<String>,

    /// Unified runtime service name used for runtime logs.
    /// When set, this overrides env/fallback service-name resolution.
    #[arg(long = "dd-service", global = true)]
    pub service_name: Option<String>,

    #[command(subcommand)]
    pub command: Option<Command>,

    // Flat infer args (used when no subcommand is given)
    #[command(flatten)]
    pub infer: Option<InferArgs>,
}

#[derive(Parser, Debug)]
pub struct InferArgs {
    #[command(flatten)]
    pub mongo: UriArg,

    /// Namespace: either <db>.<collection> to infer one collection, or just <db> to infer all
    /// collections in the database. When omitted (and -c is not given) all user databases on the
    /// server are enumerated and inferred (admin, local, and config are skipped). Can also be set
    /// via NAMESPACE in the config file.
    #[arg(long = "namespace")]
    pub namespace: Option<String>,

    /// Number of documents to sample (mutually exclusive with --percent); default 1000
    #[arg(short = 'n', long = "number", conflicts_with = "percent")]
    pub number: Option<u64>,

    /// Percentage of the collection to sample, e.g. 10 for 10% (mutually exclusive with --number)
    #[arg(short = 'p', long = "percent", conflicts_with = "number", value_parser = clap::value_parser!(f64))]
    pub percent: Option<f64>,

    /// Maximum server-side query time in milliseconds for infer sampling reads.
    #[arg(long = "max-time-ms")]
    pub max_time_ms: Option<u64>,

    /// Documents per chunk in fallback infer reads for huge collections.
    #[arg(long = "chunk-size")]
    pub chunk_size: Option<u64>,

    /// Maximum retries for Unauthorized getMore errors during chunked infer fallback.
    #[arg(long = "auth-retry-max")]
    pub auth_retry_max: Option<u32>,

    /// Treat all MongoDB Object fields as JSONB columns in the generated DDL
    /// instead of creating 1:1 child tables (arrays of objects are unaffected)
    #[arg(long = "jsonb", action = clap::ArgAction::SetTrue)]
    pub jsonb: bool,

    /// Print inferred schema JSON to stdout
    #[arg(long = "print-json", action = clap::ArgAction::SetTrue)]
    pub print_json: bool,

    /// Deprecated compatibility flag. JSON is no longer printed by default.
    #[arg(long = "no-output", hide = true, action = clap::ArgAction::SetTrue)]
    pub no_output: bool,

    /// PostgreSQL target database name. When -c is provided, this overwrites
    /// target.database_name in the config file before running.
    #[arg(long = "database-name")]
    pub database_name: Option<String>,

    /// PostgreSQL target schema name. When -c is provided, this overwrites
    /// target.schema_name in the config file before running.
    #[arg(long = "schema-name")]
    pub schema_name: Option<String>,

    /// Project directory name. When -c is provided, this overwrites
    /// project.project_dir in the config file before running.
    #[arg(long = "project-dir")]
    pub project_dir: Option<String>,

    /// Write <name>.json and <name>.stats.txt into <output_dir>/<name>/ for each collection
    #[arg(short = 'o', long = "output-dir", conflicts_with = "config")]
    pub output_dir: Option<PathBuf>,

    /// Path to a project config file (TOML) created by `mongo2pg init`
    #[arg(short = 'c', long = "config", conflicts_with = "output_dir")]
    pub config: Option<PathBuf>,
}

#[derive(Parser, Debug)]
pub struct ToPgArgs {
    /// Optional collection name; if omitted all collections under source/collections/ are processed
    pub collection: Option<String>,

    /// Root table name (only valid with a single collection name)
    #[arg(short = 't', long = "table")]
    pub table: Option<String>,

    /// Path to the project config file (TOML) – derives source/collections and schema/tables paths
    #[arg(short = 'c', long = "config", conflicts_with = "output_dir")]
    pub config: Option<PathBuf>,

    /// Directory to write the SQL file(s) into (overrides -c)
    #[arg(short = 'o', long = "output-dir", conflicts_with = "config")]
    pub output_dir: Option<PathBuf>,

    /// PostgreSQL schema name: strips `{schema}_` prefix from child table names and
    /// prepends `CREATE SCHEMA IF NOT EXISTS` + `SET search_path` to the output.
    /// When omitted, each collection is deployed into its own PostgreSQL schema.
    #[arg(long = "schema", visible_alias = "schema-name")]
    pub schema: Option<String>,

    /// Project directory name. When -c is provided, this overwrites
    /// project.project_dir in the config file before running.
    #[arg(long = "project-dir")]
    pub project_dir: Option<String>,
}

#[derive(Parser, Debug)]
pub struct InitArgs {
    /// Base directory under which the project folder will be created
    #[arg(long)]
    pub project_base: PathBuf,

    /// Name of the project (becomes a sub-folder inside project_base)
    #[arg(long)]
    pub project_name: String,

    /// MongoDB source connection URI to store in the project config
    #[arg(long = "source-uri")]
    pub source_uri: Option<String>,

    /// PostgreSQL target connection URI to store in the project config
    #[arg(long = "target-uri")]
    pub target_uri: Option<String>,

    /// Namespace to store in the project config (e.g. mydb or mydb.mycoll); when omitted,
    /// NAMESPACE is not written to the config file so `infer` will enumerate all databases
    #[arg(long = "namespace")]
    pub namespace: Option<String>,

    /// Optional cluster segment appended under project_dir for all outputs
    #[arg(long = "cluster-name", visible_alias = "cluster-naem")]
    pub cluster_name: Option<String>,
}

#[derive(Parser, Debug)]
pub struct ReportArgs {
    #[command(flatten)]
    pub mongo: UriArg,

    /// Path to the project config file (TOML) – derives source/collections and output paths
    #[arg(short = 'c', long = "config", conflicts_with_all = ["collections_dir", "output"])]
    pub config: Option<PathBuf>,

    /// Path to the source/collections directory (overrides -c)
    #[arg(long = "collections-dir", conflicts_with = "config")]
    pub collections_dir: Option<PathBuf>,

    /// Where to write the HTML report (default: reports/main.html or main.html)
    #[arg(short = 'o', long = "output")]
    pub output: Option<PathBuf>,

    /// Database / namespace label shown in the report header
    #[arg(short = 'n', long = "namespace", default_value = "")]
    pub namespace: String,

    /// Project directory name. When -c is provided, this overwrites
    /// project.project_dir in the config file before running.
    #[arg(long = "project-dir")]
    pub project_dir: Option<String>,

    /// Connect to MongoDB and PostgreSQL and write a post-import validation report.
    #[arg(long = "post-import", action = clap::ArgAction::SetTrue)]
    pub post_import: bool,

    /// Compute MongoDB/PostgreSQL MD5 checks during post-import reporting.
    #[arg(
        long = "check-md5",
        action = clap::ArgAction::Set,
        default_value_t = true,
        default_missing_value = "true",
        num_args = 0..=1,
        requires = "post_import"
    )]
    pub check_md5: bool,

    /// Print one MD5 per row instead of one collection-level aggregated MD5.
    #[arg(long = "noaggregate", action = clap::ArgAction::SetTrue, requires = "check_md5")]
    pub noaggregate: bool,
}

#[derive(Parser, Debug)]
pub struct SchemaArgs {
    #[command(flatten)]
    pub mongo: UriArg,

    /// Path to the project config file (TOML) – derives schema/tables and reports paths
    #[arg(short = 'c', long = "config", conflicts_with = "tables_dir")]
    pub config: Option<PathBuf>,

    /// Directory containing the SQL DDL files (overrides -c)
    #[arg(long = "tables-dir", conflicts_with = "config")]
    pub tables_dir: Option<PathBuf>,

    /// Where to write the HTML diagram (default: reports/<project_name>.schema.html)
    #[arg(short = 'o', long = "output")]
    pub output: Option<PathBuf>,
}

#[derive(Parser, Debug)]
pub struct ExportArgs {
    /// Optional collection name; if omitted all collections in schema/tables/ are exported
    pub collection: Option<String>,

    /// Path to the project config file (TOML) – derives SOURCE_URI, db, schema/tables and data/ paths
    #[arg(short = 'c', long = "config")]
    pub config: Option<PathBuf>,

    #[command(flatten)]
    pub mongo: UriArg,

    /// Override the output directory for CSV files (default: <project>/data/)
    #[arg(short = 'o', long = "output-dir")]
    pub output_dir: Option<PathBuf>,

    /// Namespace: either <db>.<collection> to export one collection, or just <db> to export all
    /// collections in the database. When omitted (and -c is not given) all user databases on the
    /// server are enumerated and exported (admin, local, and config are skipped). Can also be set
    /// via NAMESPACE in the config file. This overrides the namespace in the config file if provided.
    #[arg(long = "namespace")]
    pub namespace: Option<String>,

    /// PostgreSQL target database name. When -c is provided, this overwrites
    /// target.database_name in the config file before running.
    #[arg(long = "database-name")]
    pub database_name: Option<String>,

    /// PostgreSQL target schema name. When -c is provided, this overwrites
    /// target.schema_name in the config file before running.
    #[arg(long = "schema-name")]
    pub schema_name: Option<String>,

    /// Project directory name. When -c is provided, this overwrites
    /// project.project_dir in the config file before running.
    #[arg(long = "project-dir")]
    pub project_dir: Option<String>,

    /// Maximum buffered table rows before export flushes chunk data to CSV.gz.
    /// Defaults to SOURCE.CHUNK_SIZE from config, then a safe built-in value.
    #[arg(long = "chunk-size")]
    pub chunk_size: Option<u64>,
}

#[derive(Parser, Debug)]
pub struct ImportArgs {
    /// Optional collection name; if omitted all collections for the namespace are imported
    pub collection: Option<String>,

    /// Path to the project config file (TOML) – derives TARGET_URI, schema/tables and data/ paths
    #[arg(short = 'c', long = "config")]
    pub config: PathBuf,

    /// Namespace: either <db>.<collection> to import one collection, or just <db> to import all.
    /// This overrides the namespace in the config file if provided.
    #[arg(long = "namespace")]
    pub namespace: Option<String>,

    /// PostgreSQL target database name. When -c is provided, this overwrites
    /// target.database_name in the config file before running.
    #[arg(long = "database-name")]
    pub database_name: Option<String>,

    /// PostgreSQL target schema name. When -c is provided, this overwrites
    /// target.schema_name in the config file before running.
    #[arg(long = "schema-name")]
    pub schema_name: Option<String>,

    /// Project directory name. When -c is provided, this overwrites
    /// project.project_dir in the config file before running.
    #[arg(long = "project-dir")]
    pub project_dir: Option<String>,
}

#[derive(Parser, Debug)]
pub struct ClusterReportArgs {
    /// One or more project config files (TOML); can be repeated or comma-separated
    #[arg(long = "configs", value_delimiter = ',', num_args = 1..)]
    pub configs: Vec<PathBuf>,

    /// Where to write the HTML cluster report (default: cluster.html)
    #[arg(short = 'o', long = "output")]
    pub output: Option<PathBuf>,

    /// MongoDB cluster label shown in the report header (derived from the first config URI when omitted)
    #[arg(long = "cluster", default_value = "")]
    pub cluster_label: String,
}

#[derive(Parser, Debug)]
pub struct KafkaImportArgs {
    /// Path to the project config file (TOML)
    #[arg(short = 'c', long = "config")]
    pub config: PathBuf,

    /// Optional explicit topics list (comma-separated). Overrides [kafka].topics from config.
    #[arg(long = "topics", value_delimiter = ',')]
    pub topics: Vec<String>,

    /// Optional max messages to consume in this run. Overrides [kafka].max_messages.
    #[arg(long = "max-messages")]
    pub max_messages: Option<usize>,

    /// Optional consumer offset policy for missing group offsets.
    /// Supported values: latest, earliest, 0.
    /// `0` enables snapshot-equivalent mode (truncate + fresh group + earliest + idle stop).
    /// Overrides [kafka].offset and [kafka].auto_offset_reset.
    #[arg(long = "offset", value_parser = ["latest", "earliest", "0"])]
    pub offset: Option<String>,

    /// Project directory name. When -c is provided, this overwrites
    /// project.project_dir in the config file before running.
    #[arg(long = "project-dir")]
    pub project_dir: Option<String>,

    #[arg(long = "group-id")]
    pub group_id: Option<String>,

    #[arg(long = "topic-prefix")]
    pub topic_prefix: Option<String>,

    #[arg(long = "database-name")]
    pub database_name: Option<String>,

    #[arg(long = "schema-name")]
    pub schema_name: Option<String>,

    /// Reuse existing destination tables during bootstrap instead of failing preflight.
    /// When enabled, DDL files that target already-existing tables are skipped.
    #[arg(long = "force", action = clap::ArgAction::SetTrue)]
    pub force: bool,
}

#[derive(Parser, Debug)]
pub struct PingArgs {
    /// Path to the project config file (TOML)
    #[arg(short = 'c', long = "config")]
    pub config: PathBuf,

    /// Check MongoDB source connectivity
    #[arg(long = "source", action = clap::ArgAction::SetTrue)]
    pub source: bool,

    /// Check PostgreSQL target connectivity
    #[arg(long = "target", action = clap::ArgAction::SetTrue)]
    pub target: bool,

    /// Check Kafka connectivity/reachability
    #[arg(long = "kafka", action = clap::ArgAction::SetTrue)]
    pub kafka: bool,

    /// Check Kafka worker consumer-group health (members assigned to configured topics)
    #[arg(long = "runner", action = clap::ArgAction::SetTrue)]
    pub runner: bool,
}
