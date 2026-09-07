//! CLI argument and subcommand definitions (Clap types).
//!
//! This module holds the parser-facing types (`Cli`, global option structs, and the
//! `Command` subcommand enum) migrated out of the binary entrypoint. See
//! `openspec/changes/refactor-cli-commands-db-engine-layout/design.md` for the migration plan.

pub mod args;
pub mod commands;

pub use args::{
    Cli, ClusterReportArgs, ExportArgs, ImportArgs, InferArgs, InitArgs, KafkaImportArgs,
    PingArgs, ReportArgs, SchemaArgs, ToPgArgs, UriArg,
};
pub use commands::Command;
