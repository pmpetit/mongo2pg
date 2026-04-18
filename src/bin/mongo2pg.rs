//! `mongo2pg` CLI – Sample a MongoDB collection and infer its schema.
//!
//! # Usage
//! ```text
//! mongo2pg <URI> <DB.COLLECTION> [OPTIONS]
//!
//! Options:
//!   -n, --number <N>       Number of documents to sample [default: 1000]
//!   -f, --format <FMT>     Output format: expanded|mongodb|standard|json|yaml|table [default: expanded]
//!   -s, --stats            Print statistics to stderr
//!   -t, --semantic-types   Enable semantic-type detection (e.g., email)
//!       --values           Collect sample values (default)
//!       --no-values        Disable sample-value collection
//!       --sampling         Use $sample aggregation (default)
//!       --no-sampling      Use sequential find/limit instead of $sample
//! ```

use std::io::{self, Write};

use anyhow::{anyhow, Context, Result};
use bson::doc;
use clap::Parser;
use futures::TryStreamExt;
use mongodb::{Client, options::ClientOptions};
use mongo2pg::analyzer::Analyzer;
use mongo2pg::converters::{to_expanded_schema, to_json_schema, to_mongodb_schema};
use mongo2pg::semantic_types::SemanticDetector;
use mongo2pg::stats::format_stats;

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

    /// Number of documents to sample
    #[arg(short = 'n', long = "number", default_value_t = 1000)]
    number: u64,

    /// Output format: expanded (default), mongodb, standard
    /// Renderer: json, yaml, table
    #[arg(short = 'f', long = "format", default_value = "expanded")]
    format: String,

    /// Print statistics to stderr
    #[arg(short = 's', long = "stats", default_value_t = false)]
    stats: bool,

    /// Enable semantic-type detection
    #[arg(short = 't', long = "semantic-types", default_value_t = false)]
    semantic_types: bool,

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

    // Build analyzer
    let detector = if args.semantic_types {
        Some(SemanticDetector::new())
    } else {
        None
    };
    let mut analyzer = Analyzer::new(collect_values, detector);

    // Sample documents
    if use_sampling {
        // Use $sample aggregation
        let pipeline = vec![doc! { "$sample": { "size": args.number as i64 } }];
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
            .limit(args.number as i64)
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
    if args.stats {
        let stats_lines = format_stats(&schema);
        let stderr = io::stderr();
        let mut handle = stderr.lock();
        for line in stats_lines {
            writeln!(handle, "{line}")?;
        }
    }

    // Convert schema to the requested format
    let (schema_dialect, renderer) = parse_format(&args.format);
    let value = match schema_dialect {
        SchemaDialect::Expanded => to_expanded_schema(&schema),
        SchemaDialect::MongoDB => to_mongodb_schema(&schema),
        SchemaDialect::Standard => to_json_schema(&schema),
    };

    // Render output
    let output = match renderer {
        Renderer::Json => serde_json::to_string_pretty(&value)?,
        Renderer::Yaml => serde_yaml::to_string(&value)
            .map_err(|e| anyhow!("YAML serialization error: {e}"))?,
        Renderer::Table => render_table(&schema),
    };

    println!("{output}");
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

#[derive(Debug)]
enum SchemaDialect {
    Expanded,
    MongoDB,
    Standard,
}

#[derive(Debug)]
enum Renderer {
    Json,
    Yaml,
    Table,
}

fn parse_format(format: &str) -> (SchemaDialect, Renderer) {
    // The format flag doubles as both dialect selector and renderer.
    match format.to_lowercase().as_str() {
        "mongodb" => (SchemaDialect::MongoDB, Renderer::Json),
        "standard" => (SchemaDialect::Standard, Renderer::Json),
        "json" => (SchemaDialect::Expanded, Renderer::Json),
        "yaml" => (SchemaDialect::Expanded, Renderer::Yaml),
        "table" => (SchemaDialect::Expanded, Renderer::Table),
        _ => (SchemaDialect::Expanded, Renderer::Json), // default: expanded + json
    }
}

/// Render a simple ASCII table of the top-level schema fields.
fn render_table(schema: &mongo2pg::analyzer::CollectionSchema) -> String {
    let mut lines = Vec::new();
    let header = format!(
        "{:<30} {:>8} {:>8} {}",
        "Field", "Count", "Prob", "Types"
    );
    let sep = "-".repeat(header.len().max(60));
    lines.push(sep.clone());
    lines.push(header);
    lines.push(sep.clone());
    for (name, field) in &schema.object {
        let type_list: Vec<String> = field
            .types
            .keys()
            .map(|t| t.as_str().to_owned())
            .collect();
        lines.push(format!(
            "{:<30} {:>8} {:>8.3} {}",
            name,
            field.count,
            field.prop_in_object,
            type_list.join(", ")
        ));
    }
    lines.push(sep);
    lines.join("\n")
}
