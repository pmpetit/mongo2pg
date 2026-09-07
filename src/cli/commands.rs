//! Subcommand enum: maps each CLI subcommand name to its argument struct.
//! Command execution logic lives in `commands::*`, not here.

use clap::Subcommand;

use crate::cli::args::{
    ClusterReportArgs, ExportArgs, ImportArgs, InferArgs, InitArgs, KafkaImportArgs, PingArgs,
    ReportArgs, ToPgArgs,
};

#[derive(Subcommand)]
pub enum Command {
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
    /// Check backend connectivity for selected dependencies
    Ping(PingArgs),
}
