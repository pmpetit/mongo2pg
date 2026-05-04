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

use anyhow::{anyhow, Context, Result};
use bson::doc;
use clap::{Args, Parser, Subcommand};
use futures::TryStreamExt;
use indexmap::IndexMap;
use mongo2pg::analyzer::{Analyzer, CollectionSchema};
use mongo2pg::export::export_collection;
use mongo2pg::report::{collect_rows, compute_db_score, render_cluster_html, render_html};
use mongo2pg::schema_diagram::{load_tables, render_schema_html};
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

    /// Namespace: either <db>.<collection> to infer one collection,
    /// or just <db> to infer all collections in the database – required unless -c is given
    #[arg(long = "namespace", required_unless_present = "config")]
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
    #[command(flatten)]
    mongo: UriArg,

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

    /// Where to write the HTML report (default: reports/<namespace>.html or report.html)
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

    // Collect the JSON files to process
    let json_files: Vec<(String, PathBuf)> = if let Some(ref name) = args.collection {
        let json = collections_dir.join(name).join(format!("{name}.json"));
        vec![(name.clone(), json)]
    } else {
        let mut entries: Vec<(String, PathBuf)> = std::fs::read_dir(&collections_dir)
            .with_context(|| format!("Cannot read {}", collections_dir.display()))?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .filter_map(|e| {
                let name = e.file_name().to_string_lossy().into_owned();
                let json = e.path().join(format!("{name}.json"));
                if json.exists() {
                    Some((name, json))
                } else {
                    None
                }
            })
            .collect();
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

    std::fs::create_dir_all(&output_dir)
        .with_context(|| format!("Failed to create directory {}", output_dir.display()))?;

    for (name, json_path) in &json_files {
        let table_name = args.table.as_deref().unwrap_or(name);
        let content = std::fs::read_to_string(json_path)
            .with_context(|| format!("Failed to read {}", json_path.display()))?;
        let schema: CollectionSchema = serde_json::from_str(&content)
            .with_context(|| format!("Failed to parse {}", json_path.display()))?;
        let ddl = schema_to_ddl(&schema, table_name);
        let sql_path = output_dir.join(format!("{table_name}.sql"));
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
                .expect("clap ensures uri is present when -c is absent");
            (uri, args.output_dir.clone(), None, None, None, false)
        };

    let namespace = args
        .namespace
        .clone()
        .or(conf_namespace)
        .ok_or_else(|| {
            anyhow!("No namespace provided: pass <db> or <db>.<collection> as an argument or add NAMESPACE to the config file")
        })?;

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
        namespace: Some(namespace.clone()),
        output_dir: effective_output_dir,
        number: resolved_number,
        percent: resolved_percent,
        jsonb: resolved_jsonb,
        config: None,
        ..args
    };

    if namespace.contains('.') {
        // Single collection: <db>.<collection>
        let (db_name, coll_name) = parse_namespace(&namespace)?;
        let schema = infer_collection(&client, db_name, coll_name, &args).await?;
        if !args.no_output {
            println!("{}", serde_json::to_string_pretty(&schema)?);
        }
    } else {
        // Whole database: infer every collection
        let db = client.database(&namespace);
        let coll_names = db
            .list_collection_names()
            .await
            .context("Failed to list collections")?;

        let mut all_schemas: IndexMap<String, CollectionSchema> = IndexMap::new();
        for coll_name in &coll_names {
            let schema = infer_collection(&client, &namespace, coll_name, &args).await?;
            all_schemas.insert(coll_name.clone(), schema);
        }
        if !args.no_output {
            println!("{}", serde_json::to_string_pretty(&all_schemas)?);
        }
    }
    Ok(())
}

/// Infer the schema for a single collection, print stats to stderr, and optionally write output files.
async fn infer_collection(
    client: &Client,
    db_name: &str,
    coll_name: &str,
    args: &InferArgs,
) -> Result<CollectionSchema> {
    let output_dir = args.output_dir.as_deref();
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

    let pipeline = vec![doc! { "$sample": { "size": sample_size as i64 } }];
    let mut cursor = collection
        .aggregate(pipeline)
        .await
        .with_context(|| format!("Failed to run $sample aggregation on {db_name}.{coll_name}"))?;
    while let Some(doc) = cursor.try_next().await.context("Cursor error")? {
        analyzer.process_document(&doc);
    }

    let mut schema = analyzer.finish();
    let total_docs = if let Some(t) = known_total {
        t
    } else {
        collection
            .estimated_document_count()
            .await
            .context("Failed to get document count")?
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
        write_collection_files(out_dir, coll_name, &schema, &stats_lines)
            .with_context(|| format!("Failed to write output files for {coll_name}"))?;
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
    let dir = base.join(coll_name);
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("Failed to create directory {}", dir.display()))?;

    let json_path = dir.join(format!("{coll_name}.json"));
    std::fs::write(&json_path, serde_json::to_string_pretty(schema)?)
        .with_context(|| format!("Failed to write {}", json_path.display()))?;

    let stats_path = dir.join(format!("{coll_name}.stats.txt"));
    std::fs::write(&stats_path, stats_lines.join("\n") + "\n")
        .with_context(|| format!("Failed to write {}", stats_path.display()))?;

    let yaml_stats = stats_to_yaml(schema, Some(schema.count));
    let yaml_path = dir.join(format!("{coll_name}.stats.yaml"));
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
        format!("NAMESPACE = {}\n", args.project_name),
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
    // Resolve collections dir and namespace from -c or explicit flags
    let (collections_dir, namespace, cluster, default_output_dir) =
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
            let out_dir = c.base_dir.join(&c.project_dir).join("reports");
            (cols_dir, ns, cluster, Some((out_dir, c.project_dir)))
        } else {
            let dir = args
                .collections_dir
                .clone()
                .ok_or_else(|| anyhow!("Provide --collections-dir or -c <config>"))?;
            (dir, args.namespace.clone(), String::new(), None)
        };

    // Resolve tables dir for the PG tables count column (only when using a conf file)
    let tables_dir_for_report: Option<std::path::PathBuf> =
        default_output_dir.as_ref().map(|(reports_dir, _)| {
            reports_dir
                .parent()
                .unwrap_or(reports_dir)
                .join("schema")
                .join("tables")
        });
    let tables_dir_opt = tables_dir_for_report.as_deref().filter(|p| p.is_dir());

    let rows = collect_rows(&collections_dir, tables_dir_opt)?;

    let html = render_html(&rows, &namespace, &cluster);

    let output_path = if let Some(ref o) = args.output {
        o.clone()
    } else if let Some((ref dir, ref project_name)) = default_output_dir {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("Failed to create reports dir {}", dir.display()))?;
        dir.join(format!("{project_name}.html"))
    } else {
        PathBuf::from("report.html")
    };

    std::fs::write(&output_path, &html)
        .with_context(|| format!("Failed to write {}", output_path.display()))?;
    println!("Report written to {}", output_path.display());

    // Also generate the schema ERD diagram if SQL tables exist
    if let Some((ref reports_dir, ref project_name)) = default_output_dir {
        let tables_dir = reports_dir
            .parent()
            .unwrap_or(reports_dir)
            .join("schema")
            .join("tables");
        if tables_dir.is_dir() {
            match load_tables(&tables_dir) {
                Ok(tables) if !tables.is_empty() => {
                    let schema_html = render_schema_html(&tables, project_name);
                    let schema_path = reports_dir.join(format!("{project_name}.schema.html"));
                    std::fs::write(&schema_path, &schema_html)
                        .with_context(|| format!("Failed to write {}", schema_path.display()))?;
                    println!("Schema diagram written to {}", schema_path.display());
                }
                Ok(_) => {}
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
        return Err(anyhow!(
            "Provide at least one config path via --configs"
        ));
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

    let output_path = args
        .output
        .unwrap_or_else(|| PathBuf::from("cluster.html"));
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
