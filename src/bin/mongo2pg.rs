//! `mongo2pg` CLI – Infer a MongoDB collection schema and convert it to PostgreSQL DDL.
//!
//! # Subcommands
//!
//! ## `mongo2pg infer` (default when no subcommand given)
//! ```text
//! mongo2pg infer <URI> <DB.COLLECTION> [OPTIONS]
//! ```
//! Samples documents and writes the inferred schema JSON to stdout.
//!
//! ## `mongo2pg to-pg`
//! ```text
//! mongo2pg to-pg <SCHEMA_FILE> [--table <TABLE_NAME>]
//! ```
//! Converts a schema JSON file produced by `infer` into PostgreSQL DDL.

use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use bson::doc;
use clap::{Args, Parser, Subcommand};
use futures::TryStreamExt;
use indexmap::IndexMap;
use mongo2pg::analyzer::{Analyzer, CollectionSchema};
use mongo2pg::export::export_collection;
use mongo2pg::report::{
    collect_rows, compute_db_score, render_cluster_html, render_html, SYSTEM_DATABASES,
};
use mongo2pg::schema_diagram::{load_tables_by_db, render_schema_html};
use mongo2pg::stats::{format_stats, stats_to_yaml};
use mongo2pg::to_pg::schema_to_ddl;
use mongodb::{options::ClientOptions, Client};

// ──────────────────────────────────────────────────────────────────────────────
// Shared args
// ──────────────────────────────────────────────────────────────────────────────

/// MongoDB URI argument shared across commands that connect to MongoDB.
/// When `-c` is also provided, this overrides the URI stored in the config file.
#[derive(Args, Debug, Clone)]
struct UriArg {
    /// MongoDB connection URI (e.g. mongodb://localhost:27017) – required unless -c is given;
    /// overrides the URI stored in the config file when -c is also provided
    #[arg(long = "uri", required_unless_present = "config")]
    uri: Option<String>,
}

// ──────────────────────────────────────────────────────────────────────────────
// CLI definition
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(
    name = "mongo2pg",
    about = "Infer a MongoDB collection schema and convert it to PostgreSQL DDL",
    version,
    // Allow bare `mongo2pg <URI> <NS>` without an explicit subcommand.
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

    /// Suppress schema output to stdout
    #[arg(long = "no-output", action = clap::ArgAction::SetTrue)]
    no_output: bool,

    /// Write <name>.json and <name>.stats.txt into <output_dir>/<name>/ for each collection
    #[arg(short = 'o', long = "output-dir", conflicts_with = "config")]
    output_dir: Option<PathBuf>,

    /// Path to a .conf file (created by `mongo2pg init`) to derive the output directory
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

    /// Path to the project config file (.conf) – derives source/collections and schema/tables paths
    #[arg(short = 'c', long = "config", conflicts_with = "output_dir")]
    config: Option<PathBuf>,

    /// Directory to write the SQL file(s) into (overrides -c)
    #[arg(short = 'o', long = "output-dir", conflicts_with = "config")]
    output_dir: Option<PathBuf>,

    /// PostgreSQL schema name: strips `{schema}_` prefix from child table names and
    /// prepends `CREATE SCHEMA IF NOT EXISTS` + `SET search_path` to the output.
    #[arg(long = "schema", conflicts_with = "schema_per_collection")]
    schema: Option<String>,

    /// Use each collection name as its own PostgreSQL schema: equivalent to `--schema <collection>`
    /// applied per collection. Mutually exclusive with `--schema`.
    #[arg(long = "schema-per-collection", action = clap::ArgAction::SetTrue)]
    schema_per_collection: bool,
}

#[derive(Parser, Debug)]
struct InitArgs {
    /// Base directory under which the project folder will be created
    #[arg(long)]
    project_base: PathBuf,

    /// Name of the project (becomes a sub-folder inside project_base)
    #[arg(long)]
    project_name: String,

    /// MongoDB connection URI to store in the project config
    #[arg(long)]
    uri: Option<String>,

    /// Namespace to store in the project config (e.g. mydb or mydb.mycoll); when omitted,
    /// NAMESPACE is not written to the config file so `infer` will enumerate all databases
    #[arg(long)]
    namespace: Option<String>,
}

#[derive(Parser, Debug)]
struct ReportArgs {
    #[command(flatten)]
    mongo: UriArg,

    /// Path to the project config file (.conf) – derives source/collections and output paths
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
}

#[derive(Parser, Debug)]
struct SchemaArgs {
    #[command(flatten)]
    mongo: UriArg,

    /// Path to the project config file (.conf) – derives schema/tables and reports paths
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

    /// Path to the project config file (.conf) – derives URI, db, schema/tables and data/ paths
    #[arg(short = 'c', long = "config")]
    config: Option<PathBuf>,

    #[command(flatten)]
    mongo: UriArg,

    /// Override the output directory for CSV files (default: <project>/data/)
    #[arg(short = 'o', long = "output-dir")]
    output_dir: Option<PathBuf>,
}

#[derive(Parser, Debug)]
struct ClusterReportArgs {
    /// One or more project config (.conf) files; can be repeated or comma-separated
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
        Some(Command::ToPg(args)) => run_to_pg(args),
        Some(Command::Report(args)) => run_report(args),
        Some(Command::Export(args)) => run_export(args).await,
        Some(Command::Infer(args)) => run_infer(args).await,
        Some(Command::ClusterReport(args)) => run_cluster_report(args),
        None => run_infer(cli.infer.expect("clap ensures args are present")).await,
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// `to-pg` subcommand
// ──────────────────────────────────────────────────────────────────────────────

fn run_to_pg(args: ToPgArgs) -> Result<()> {
    // Resolve collections source dir and SQL output dir
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
            vec![(PathBuf::from(format!("{}.sql", name.to_lowercase())), flat)]
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
                entries.push((
                    PathBuf::from(format!("{}.sql", top_name.to_lowercase())),
                    direct_json,
                ));
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
            if args.schema_per_collection {
                Some(table_name)
            } else {
                args.schema.as_deref()
            },
        );
        std::fs::write(&sql_path, &ddl)
            .with_context(|| format!("Failed to write {}", sql_path.display()))?;
        println!("SQL written to {}", sql_path.display());
    }

    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────────
// `infer` subcommand (also the default)
// ──────────────────────────────────────────────────────────────────────────────

async fn run_infer(args: InferArgs) -> Result<()> {
    // Resolve URI, namespace, number, and percent – reading conf file if -c was given
    let (resolved_uri, effective_output_dir, conf_namespace, conf_number, conf_percent, conf_jsonb) =
        if let Some(ref conf) = args.config {
            let c = read_conf(conf)?;
            let uri = args.mongo.uri.clone().or(c.uri).ok_or_else(|| {
                anyhow!("No URI provided: pass it as an argument or add URI to the config file")
            })?;
            let out_dir = args.output_dir.clone().unwrap_or_else(|| {
                c.base_dir
                    .join(&c.project_dir)
                    .join("source")
                    .join("collections")
            });
            (
                uri,
                Some(out_dir),
                c.namespace,
                c.number,
                c.percent,
                c.jsonb,
            )
        } else {
            let uri = args
                .mongo
                .uri
                .clone()
                .ok_or_else(|| anyhow!("No URI provided: pass --uri or -c <config>"))?;
            (uri, args.output_dir.clone(), None, None, None, false)
        };

    let namespace = args.namespace.clone().or(conf_namespace);

    let client_options = ClientOptions::parse(&resolved_uri)
        .await
        .context("Failed to parse MongoDB URI")?;
    let client = Client::with_options(client_options).context("Failed to create MongoDB client")?;

    // CLI takes priority over conf for number/percent/jsonb; then fall back to defaults
    let resolved_number = args.number.or(conf_number);
    let resolved_percent = args.percent.or(conf_percent);
    let resolved_jsonb = args.jsonb || conf_jsonb;

    let args = InferArgs {
        mongo: UriArg {
            uri: Some(resolved_uri),
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
            infer_all_databases(&client, &args).await?;
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
            let schema =
                infer_collection(&client, db_name, coll_name, coll_name, &args, None).await?;
            if !args.no_output {
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

            let mut all_schemas: IndexMap<String, CollectionSchema> = IndexMap::new();
            for coll_name in coll_names.iter().filter(|n| !n.starts_with("system.")) {
                match infer_collection(&client, db_name, coll_name, coll_name, &args, None).await {
                    Ok(schema) => {
                        all_schemas.insert(coll_name.clone(), schema);
                    }
                    Err(e) => eprintln!("  [warn] skipping {db_name}.{coll_name}: {e:#}"),
                }
            }
            if !args.no_output {
                println!("{}", serde_json::to_string_pretty(&all_schemas)?);
            }
        }
    }
    Ok(())
}

/// Infer schemas for all user databases on the server (skipping system databases).
///
/// Output files are written as `<output_dir>/<dbname>/<collname>/`.
/// Report generation is handled separately by the `report` command.
async fn infer_all_databases(client: &Client, args: &InferArgs) -> Result<()> {
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

        let mut db_schemas: IndexMap<String, CollectionSchema> = IndexMap::new();

        for coll_name in coll_names.iter().filter(|n| !n.starts_with("system.")) {
            let db_out_dir = args.output_dir.as_deref().map(|d| d.join(db_name));
            match infer_collection(
                client,
                db_name,
                coll_name,
                coll_name,
                args,
                db_out_dir.as_deref(),
            )
            .await
            {
                Ok(schema) => {
                    db_schemas.insert(coll_name.clone(), schema);
                }
                Err(e) => eprintln!("  [warn] skipping {db_name}.{coll_name}: {e:#}"),
            }
        }

        if !args.no_output && args.output_dir.is_none() {
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
) -> Result<CollectionSchema> {
    let output_dir = output_dir_override.or(args.output_dir.as_deref());
    let db = client.database(db_name);
    let collection = db.collection::<bson::Document>(coll_name);

    let (sample_size, known_total) = if let Some(pct) = args.percent {
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
        (n, Some(total))
    } else {
        (args.number.unwrap_or(1000), None)
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

    let stderr = io::stderr();
    let mut handle = stderr.lock();
    writeln!(handle, "[{db_name}.{coll_name}]")?;
    for line in &stats_lines {
        writeln!(handle, "{line}")?;
    }
    drop(handle);

    if let Some(out_dir) = output_dir {
        write_collection_files(out_dir, output_name, &schema, &stats_lines)
            .with_context(|| format!("Failed to write output files for {output_name}"))?;
    }

    Ok(schema)
}

/// Write `<dir>/<name>/<name>.json`, `<dir>/<name>/<name>.stats.txt`, and `<dir>/<name>/<name>.stats.yaml`.
fn write_collection_files(
    base: &Path,
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
        .join(format!("{}.conf", args.project_name));
    let conf_content = format!(
        "BASE_DIR = {}\nPROJECT_DIR = {}\n{}{}\n# NUMBER = 1000\n# PERCENT = 10\n# JSONB = false\n",
        args.project_base.display(),
        args.project_name,
        args.uri
            .as_deref()
            .map(|u| format!("URI = {}\n", u))
            .unwrap_or_else(|| "# URI = mongodb://localhost:27017\n".to_owned()),
        args.namespace
            .as_deref()
            .map(|ns| format!("NAMESPACE = {}\n", ns))
            .unwrap_or_else(|| "# NAMESPACE = mydb\n".to_owned()),
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
    let uri = args
        .mongo
        .uri
        .clone()
        .or(c.uri)
        .ok_or_else(|| anyhow!("No URI provided: pass --uri or add URI to the config file"))?;
    let db_name = c
        .namespace
        .ok_or_else(|| anyhow!("No NAMESPACE in config file"))?;

    let project_root = c.base_dir.join(&c.project_dir);
    let tables_dir = project_root.join("schema").join("tables");
    let data_dir = args
        .output_dir
        .clone()
        .unwrap_or_else(|| project_root.join("data"));

    let client_options = ClientOptions::parse(&uri)
        .await
        .context("Failed to parse MongoDB URI")?;
    let client = Client::with_options(client_options).context("Failed to create MongoDB client")?;

    // Determine which collections to export
    let collections: Vec<String> = if let Some(name) = args.collection.clone() {
        vec![name]
    } else {
        let mut names: Vec<String> = std::fs::read_dir(&tables_dir)
            .with_context(|| format!("Cannot read {}", tables_dir.display()))?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("sql"))
            .filter_map(|e| {
                e.path()
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .map(|s| s.to_owned())
            })
            .collect();
        names.sort();
        names
    };

    if collections.is_empty() {
        eprintln!("No SQL schema files found in {}", tables_dir.display());
        return Ok(());
    }

    for coll_name in &collections {
        eprintln!("[{db_name}.{coll_name}]");
        match export_collection(&client, &db_name, coll_name, &tables_dir, &data_dir).await {
            Ok(files) => {
                for f in files {
                    println!("{f}");
                }
            }
            Err(e) => eprintln!("  warning: {e}"),
        }
    }

    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────────
// `report` subcommand
// ──────────────────────────────────────────────────────────────────────────────

fn run_report(args: ReportArgs) -> Result<()> {
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
                .uri
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
        println!("Report written to {}", output_path.display());
    } else {
        // ── Flat / single-db layout ────────────────────────────────────────────
        let tables_dir_for_report: Option<PathBuf> = reports_dir
            .as_ref()
            .map(|r| r.parent().unwrap_or(r).join("schema").join("tables"));
        let tables_dir_opt = tables_dir_for_report.as_deref().filter(|p| p.is_dir());

        let rows = collect_rows(&collections_dir, tables_dir_opt)?;
        let html = render_html(&rows, &namespace, &cluster);
        std::fs::write(&output_path, &html)
            .with_context(|| format!("Failed to write {}", output_path.display()))?;
        println!("Report written to {}", output_path.display());
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
                        println!("Schema diagram written to {}", schema_path.display());
                    }
                }
                Err(e) => eprintln!("Warning: could not generate schema diagram: {e}"),
            }
        }
    }

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
            if let Some(ref uri) = c.uri {
                cluster_label = mongo2pg::report::cluster_from_uri(uri);
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

/// Values parsed from a `.conf` file produced by `mongo2pg init`.
struct ConfData {
    base_dir: PathBuf,
    project_dir: String,
    uri: Option<String>,
    namespace: Option<String>,
    number: Option<u64>,
    percent: Option<f64>,
    jsonb: bool,
}

/// Parse a `.conf` file produced by `mongo2pg init`.
fn read_conf(path: &Path) -> Result<ConfData> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read config file {}", path.display()))?;

    let mut base_dir: Option<PathBuf> = None;
    let mut project_dir: Option<String> = None;
    let mut uri: Option<String> = None;
    let mut namespace: Option<String> = None;
    let mut number: Option<u64> = None;
    let mut percent: Option<f64> = None;
    let mut jsonb: bool = false;

    for line in content.lines() {
        if let Some((key, val)) = line.split_once('=') {
            match key.trim() {
                "BASE_DIR" => base_dir = Some(PathBuf::from(val.trim())),
                "PROJECT_DIR" => project_dir = Some(val.trim().to_owned()),
                "URI" => uri = Some(val.trim().to_owned()),
                "NAMESPACE" => namespace = Some(val.trim().to_owned()),
                "NUMBER" => number = val.trim().parse().ok(),
                "PERCENT" => percent = val.trim().parse().ok(),
                "JSONB" => {
                    jsonb = matches!(val.trim().to_lowercase().as_str(), "true" | "1" | "yes")
                }
                _ => {}
            }
        }
    }

    let base_dir = base_dir.ok_or_else(|| anyhow!("BASE_DIR not found in {}", path.display()))?;
    let project_dir =
        project_dir.ok_or_else(|| anyhow!("PROJECT_DIR not found in {}", path.display()))?;

    Ok(ConfData {
        base_dir,
        project_dir,
        uri,
        namespace,
        number,
        percent,
        jsonb,
    })
}
