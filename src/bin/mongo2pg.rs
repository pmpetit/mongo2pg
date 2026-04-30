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
use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use bson::doc;
use clap::{Parser, Subcommand};
use futures::TryStreamExt;
use mongo2pg::analyzer::{Analyzer, CollectionSchema};
use mongo2pg::stats::format_stats;
use mongo2pg::to_pg::schema_to_ddl;
use mongodb::{options::ClientOptions, Client};

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
}

#[derive(Parser, Debug)]
struct InferArgs {
    /// MongoDB connection URI (e.g. mongodb://localhost:27017)
    uri: String,

    /// Namespace in the form <db>.<collection>
    namespace: String,

    /// Number of documents to sample (mutually exclusive with --percent)
    #[arg(
        short = 'n',
        long = "number",
        default_value_t = 1000,
        conflicts_with = "percent"
    )]
    number: u64,

    /// Percentage of the collection to sample, e.g. 10 for 10% (mutually exclusive with --number)
    #[arg(short = 'p', long = "percent", conflicts_with = "number", value_parser = clap::value_parser!(f64))]
    percent: Option<f64>,

    /// Suppress schema output to stdout
    #[arg(long = "no-output", action = clap::ArgAction::SetTrue)]
    no_output: bool,
}

#[derive(Parser, Debug)]
struct ToPgArgs {
    /// Path to a schema JSON file produced by `mongo2pg infer`
    schema_file: PathBuf,

    /// Root table name (defaults to the schema file stem)
    #[arg(short = 't', long = "table")]
    table: Option<String>,
}

// ──────────────────────────────────────────────────────────────────────────────
// Entry point
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Some(Command::ToPg(args)) => run_to_pg(args),
        Some(Command::Infer(args)) => run_infer(args).await,
        None => run_infer(cli.infer.expect("clap ensures args are present")).await,
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// `to-pg` subcommand
// ──────────────────────────────────────────────────────────────────────────────

fn run_to_pg(args: ToPgArgs) -> Result<()> {
    let content = std::fs::read_to_string(&args.schema_file)
        .with_context(|| format!("Failed to read {}", args.schema_file.display()))?;

    let schema: CollectionSchema = serde_json::from_str(&content)
        .context("Failed to parse schema JSON – make sure the file was produced by mongo2pg")?;

    let table_name = args.table.unwrap_or_else(|| {
        args.schema_file
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("collection")
            .to_owned()
    });

    println!("{}", schema_to_ddl(&schema, &table_name));
    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────────
// `infer` subcommand (also the default)
// ──────────────────────────────────────────────────────────────────────────────

async fn run_infer(args: InferArgs) -> Result<()> {
    let (db_name, coll_name) = parse_namespace(&args.namespace)?;

    let client_options = ClientOptions::parse(&args.uri)
        .await
        .context("Failed to parse MongoDB URI")?;
    let client = Client::with_options(client_options).context("Failed to create MongoDB client")?;
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
        (args.number, None)
    };

    let mut analyzer = Analyzer::new(true);

    let pipeline = vec![doc! { "$sample": { "size": sample_size as i64 } }];
    let mut cursor = collection
        .aggregate(pipeline)
        .await
        .context("Failed to run $sample aggregation")?;
    while let Some(doc) = cursor.try_next().await.context("Cursor error")? {
        analyzer.process_document(&doc);
    }

    let mut schema = analyzer.finish();

    {
        let total_docs = if let Some(t) = known_total {
            t
        } else {
            collection
                .estimated_document_count()
                .await
                .context("Failed to get document count")?
        };
        schema.count = total_docs;
        let stats_lines = format_stats(&schema, Some(total_docs));
        let stderr = io::stderr();
        let mut handle = stderr.lock();
        for line in stats_lines {
            writeln!(handle, "{line}")?;
        }
    }

    if !args.no_output {
        println!("{}", serde_json::to_string_pretty(&schema)?);
    }
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
