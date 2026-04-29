//! `mongo2pg` CLI – Sample a MongoDB collection and infer its schema.
//!
//! # Usage
//! ```text
//! mongo2pg <URI> <DB.COLLECTION> [OPTIONS]
//!
//! Options:
//!   -n, --number <N>       Number of documents to sample [default: 1000]
//!   -p, --percent <PCT>    Percentage of the collection to sample (mutually exclusive with -n)
//!       --values           Collect sample values (default)
//!       --no-values        Disable sample-value collection
//!       --sampling         Use $sample aggregation (default)
//!       --no-sampling      Use sequential find/limit instead of $sample
//!       --no-output        Suppress schema output to stdout (useful with --stats)
//! ```

use std::io::{self, Write};

use anyhow::{anyhow, Context, Result};
use bson::doc;
use clap::Parser;
use futures::TryStreamExt;
use mongo2pg::analyzer::Analyzer;
use mongo2pg::converters::to_expanded_schema;
use mongo2pg::stats::format_stats;
use mongodb::{options::ClientOptions, Client};

// ──────────────────────────────────────────────────────────────────────────────
// CLI argument definition
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "mongo2pg",
    about = "Sample a MongoDB collection and infer its JSON Schema",
    version
)]
struct Args {
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

    /// Collect sample values (default: true)
    #[arg(long = "values", default_value_t = true, action = clap::ArgAction::SetTrue)]
    values: bool,

    /// Disable sample-value collection
    #[arg(long = "no-values", action = clap::ArgAction::SetTrue)]
    no_values: bool,

    /// Use $sample aggregation (default: true)
    #[arg(long = "sampling", default_value_t = true, action = clap::ArgAction::SetTrue)]
    sampling: bool,

    /// Use sequential find/limit instead of $sample
    #[arg(long = "no-sampling", action = clap::ArgAction::SetTrue)]
    no_sampling: bool,

    /// Suppress schema output to stdout (useful with --stats)
    #[arg(long = "no-output", action = clap::ArgAction::SetTrue)]
    no_output: bool,
}

// ──────────────────────────────────────────────────────────────────────────────
// Entry point
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let collect_values = !args.no_values;
    let use_sampling = !args.no_sampling;

    // Parse namespace
    let (db_name, coll_name) = parse_namespace(&args.namespace)?;

    // Connect to MongoDB
    let client_options = ClientOptions::parse(&args.uri)
        .await
        .context("Failed to parse MongoDB URI")?;
    let client = Client::with_options(client_options).context("Failed to create MongoDB client")?;
    let db = client.database(db_name);
    let collection = db.collection::<bson::Document>(coll_name);

    // Resolve the number of documents to sample.
    // When --percent is given we need the collection size first.
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

    // Build analyzer
    let mut analyzer = Analyzer::new(collect_values);

    // Sample documents
    if use_sampling {
        // Use $sample aggregation
        let pipeline = vec![doc! { "$sample": { "size": sample_size as i64 } }];
        let mut cursor = collection
            .aggregate(pipeline)
            .await
            .context("Failed to run $sample aggregation")?;
        while let Some(doc) = cursor.try_next().await.context("Cursor error")? {
            analyzer.process_document(&doc);
        }
    } else {
        // Sequential find/limit
        let find_opts = mongodb::options::FindOptions::builder()
            .limit(sample_size as i64)
            .build();
        let mut cursor = collection
            .find(doc! {})
            .with_options(find_opts)
            .await
            .context("Failed to run find")?;
        while let Some(doc) = cursor.try_next().await.context("Cursor error")? {
            analyzer.process_document(&doc);
        }
    }

    let schema = analyzer.finish();

    // Print stats to stderr
    {
        let total_docs = if let Some(t) = known_total {
            t
        } else {
            collection
                .estimated_document_count()
                .await
                .context("Failed to get document count")?
        };
        let stats_lines = format_stats(&schema, Some(total_docs));
        let stderr = io::stderr();
        let mut handle = stderr.lock();
        for line in stats_lines {
            writeln!(handle, "{line}")?;
        }
    }

    // Convert schema to expanded format and print as JSON
    let value = to_expanded_schema(&schema);

    if !args.no_output {
        println!("{}", serde_json::to_string_pretty(&value)?);
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
