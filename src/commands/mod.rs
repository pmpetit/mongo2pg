//! Command dispatcher and per-subcommand handlers.
//!
//! This module will hold `run_command`, the central dispatcher that maps a parsed
//! subcommand to exactly one handler, plus one handler module per subcommand. See
//! `openspec/changes/refactor-cli-commands-db-engine-layout/design.md` for the migration plan.

pub mod cluster_report;
pub mod export;
pub mod import;
pub mod infer;
pub mod init;
pub mod kafka_import;
pub mod ping;
pub mod report;
pub mod shared;
pub mod to_pg;

use anyhow::Result;
use clap::CommandFactory;

use crate::cli::{Cli, Command, InferArgs};

/// Central dispatcher mapping a parsed subcommand to exactly one handler.
pub async fn run_command(command: Option<Command>, infer: Option<InferArgs>) -> Result<()> {
    match command {
        Some(Command::Init(args)) => init::run_init(args).await,
        Some(Command::ToPg(args)) => to_pg::run_to_pg(args, false).await,
        Some(Command::Report(args)) => report::run_report(args, false).await,
        Some(Command::Export(args)) => export::run_export(args).await,
        Some(Command::Import(args)) => import::run_import(args).await,
        Some(Command::KafkaImport(args)) => kafka_import::run_kafka_import(args).await,
        Some(Command::Ping(args)) => ping::run_ping(args).await,
        Some(Command::Infer(args)) => infer::run_infer(args).await,
        Some(Command::ClusterReport(args)) => cluster_report::run_cluster_report(args),
        None => match infer {
            Some(args) => infer::run_infer(args).await,
            None => {
                Cli::command().print_help()?;
                Ok(())
            }
        },
    }
}
