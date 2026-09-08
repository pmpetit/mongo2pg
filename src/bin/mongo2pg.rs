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

use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use clap::Parser;
use env_logger::Builder as EnvLoggerBuilder;
use log::{Level, LevelFilter};
// use mongo2pg::engine::checksum::run_check_md5;
use mongo2pg::util::read_conf;
use tracing::Instrument;

// ──────────────────────────────────────────────────────────────────────────────
// CLI definition (see mongo2pg::cli::{args, commands})
// ──────────────────────────────────────────────────────────────────────────────

use mongo2pg::cli::{Cli, Command, InferArgs};
#[cfg(test)]
use mongo2pg::cli::{ExportArgs, ImportArgs, InitArgs, PingArgs, UriArg};
#[cfg(test)]
use mongo2pg::commands::export::{
    plan_export_jobs_for_collections, resolve_export_sql_lookup_for_collection, run_export,
};
#[cfg(test)]
use mongo2pg::commands::import::run_import;
#[cfg(test)]
use mongo2pg::commands::infer::{
    apply_collection_property_filters, build_collection_mappings,
    build_collection_mappings_with_timestamp_fields, classify_unauthorized_retry,
    collect_infer_type_warnings, collect_nullable_scalar_warnings, infer_query_max_time,
    is_unauthorized_cursor_error, reconcile_mongo_paths_with_fk_lineage,
    resolve_infer_auth_retry_max, resolve_infer_chunk_size, run_infer,
    should_regenerate_from_schema_when_objectid_pk, timeout_fallback_hint,
    UnauthorizedRetryDecision, DEFAULT_INFER_AUTH_RETRY_MAX, DEFAULT_INFER_CHUNK_SIZE,
    DEFAULT_SAMPLE_MAX_TIME,
};
#[cfg(test)]
use mongo2pg::commands::init::run_init;
#[cfg(test)]
use mongo2pg::commands::ping::{ping_failed_exit, ping_requested_backends, PingBackend};
#[cfg(test)]
use mongo2pg::commands::report::should_fail_report_on_warnings;
use mongo2pg::commands::shared::*;
#[cfg(test)]
use mongo2pg::commands::to_pg::{
    detect_candidate_groups, validate_group_schema_compatibility, CollectionGroup,
};
#[cfg(test)]
use mongo2pg::export::DEFAULT_EXPORT_CHUNK_ROWS;
#[cfg(test)]
use mongo2pg::report::PostImportTableRow;
#[cfg(test)]
use mongo2pg::util::should_infer_collection;

// ──────────────────────────────────────────────────────────────────────────────
// Entry point
// ──────────────────────────────────────────────────────────────────────────────
#[tokio::main]
async fn main() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let mut cli = Cli::parse();
    let _config_stages = stage_cli_config_paths(&mut cli).await?;

    let runtime_service_name = resolve_runtime_service_name(&cli);
    let runtime_project_name = resolve_runtime_project_name(&cli);
    let runtime_namespace = resolve_runtime_namespace(&cli);
    let (log_level, log_format) = resolve_effective_runtime_log_settings(&cli)?;
    init_runtime_logger(
        log_level,
        log_format,
        &runtime_service_name,
        &runtime_project_name,
        &runtime_namespace,
    )?;

    let Cli { command, infer, .. } = cli;
    validate_command_and_args(&command, infer.as_ref())?;

    let command_name = command_name_from_command(&command);
    let root_span = tracing::info_span!(
        "mongo2pg.command",
        command = command_name,
        service_name = runtime_service_name.as_str(),
        project_name = runtime_project_name.as_str(),
        namespace = runtime_namespace.as_str()
    );

    let result = async move { mongo2pg::commands::run_command(command, infer).await }
        .instrument(root_span)
        .await;

    result
}

fn normalize_project_name(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn project_name_from_command(command: &Option<Command>) -> Option<String> {
    match command {
        Some(Command::Init(args)) => normalize_project_name(&args.project_name),
        Some(Command::Infer(args)) => args
            .project_dir
            .as_deref()
            .and_then(normalize_project_name),
        Some(Command::ToPg(args)) => args
            .project_dir
            .as_deref()
            .and_then(normalize_project_name),
        Some(Command::Report(args)) => args
            .project_dir
            .as_deref()
            .and_then(normalize_project_name),
        Some(Command::Export(args)) => args
            .project_dir
            .as_deref()
            .and_then(normalize_project_name),
        Some(Command::Import(args)) => args
            .project_dir
            .as_deref()
            .and_then(normalize_project_name),
        Some(Command::KafkaImport(args)) => args
            .project_dir
            .as_deref()
            .and_then(normalize_project_name),
        Some(Command::ClusterReport(args)) => {
            let first = args.configs.first()?;
            let conf = read_conf(first).ok()?;
            normalize_project_name(&conf.project_dir)
        }
        Some(Command::Ping(_)) | None => None,
    }
}

fn resolve_default_service_name(cli: &Cli) -> String {
    if let Some(project_name) = project_name_from_command(&cli.command) {
        return format!("m2pg-{}", project_name);
    }

    if let Some(args) = cli.infer.as_ref() {
        if let Some(project_name) = args
            .project_dir
            .as_deref()
            .and_then(normalize_project_name)
        {
            return format!("m2pg-{}", project_name);
        }
    }

    if let Some(conf_path) = config_path_from_cli(cli) {
        if let Ok(conf) = read_conf(conf_path) {
            if let Some(project_name) = normalize_project_name(&conf.project_dir) {
                return format!("m2pg-{}", project_name);
            }
        }
    }

    "m2pg-unknown".to_owned()
}

fn resolve_runtime_service_name(cli: &Cli) -> String {
    if let Some(value) = cli.service_name.as_deref() {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return trimmed.to_owned();
        }
    }

    for key in [
        "M2PG_SERVICE_NAME",
        "DD_SERVICE",
        "K8S_CRONJOB_NAME",
        "CRONJOB_NAME",
    ] {
        if let Ok(value) = std::env::var(key) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return trimmed.to_owned();
            }
        }
    }

    resolve_default_service_name(cli)
}

fn normalize_runtime_namespace(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn namespace_from_command(command: &Option<Command>) -> Option<String> {
    match command {
        Some(Command::Infer(args)) => args
            .namespace
            .as_deref()
            .and_then(normalize_runtime_namespace),
        Some(Command::Report(args)) => normalize_runtime_namespace(&args.namespace),
        Some(Command::Export(args)) => args
            .namespace
            .as_deref()
            .and_then(normalize_runtime_namespace),
        Some(Command::Import(args)) => args
            .namespace
            .as_deref()
            .and_then(normalize_runtime_namespace),
        Some(Command::ClusterReport(args)) => {
            let first = args.configs.first()?;
            let conf = read_conf(first).ok()?;
            conf.namespace
                .as_deref()
                .and_then(normalize_runtime_namespace)
        }
        Some(Command::Init(args)) => args
            .namespace
            .as_deref()
            .and_then(normalize_runtime_namespace),
        Some(Command::ToPg(_)) | Some(Command::KafkaImport(_)) | Some(Command::Ping(_)) | None => {
            None
        }
    }
}

fn resolve_runtime_project_name(cli: &Cli) -> String {
    if let Some(conf_path) = config_path_from_cli(cli) {
        if let Ok(conf) = read_conf(conf_path) {
            if let Some(project_name) = normalize_project_name(&conf.project_dir) {
                return project_name;
            }
        }
    }

    if let Some(project_name) = project_name_from_command(&cli.command) {
        return project_name;
    }

    if let Some(args) = cli.infer.as_ref() {
        if let Some(project_name) = args
            .project_dir
            .as_deref()
            .and_then(normalize_project_name)
        {
            return project_name;
        }
    }

    "unknown".to_owned()
}

fn resolve_runtime_namespace(cli: &Cli) -> String {
    if let Some(conf_path) = config_path_from_cli(cli) {
        if let Ok(conf) = read_conf(conf_path) {
            if let Some(namespace) = conf
                .namespace
                .as_deref()
                .and_then(normalize_runtime_namespace)
            {
                return namespace;
            }
        }
    }

    if let Some(namespace) = namespace_from_command(&cli.command) {
        return namespace;
    }

    if let Some(args) = cli.infer.as_ref() {
        if let Some(namespace) = args
            .namespace
            .as_deref()
            .and_then(normalize_runtime_namespace)
        {
            return namespace;
        }
    }

    "unknown".to_owned()
}

fn command_name_from_command(command: &Option<Command>) -> &'static str {
    match command {
        Some(Command::Init(_)) => "init",
        Some(Command::Infer(_)) => "infer",
        Some(Command::ToPg(_)) => "to-pg",
        Some(Command::Report(_)) => "report",
        Some(Command::Export(_)) => "export",
        Some(Command::Import(_)) => "import",
        Some(Command::ClusterReport(_)) => "cluster-report",
        Some(Command::KafkaImport(_)) => "kafka-import",
        Some(Command::Ping(_)) => "ping",
        None => "infer",
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeLogFormat {
    Text,
    Json,
}

fn parse_log_format(raw: &str) -> Result<RuntimeLogFormat> {
    let normalized = raw.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "text" => Ok(RuntimeLogFormat::Text),
        "json" => Ok(RuntimeLogFormat::Json),
        _ => Err(anyhow!(
            "invalid log format '{raw}'. Use one of: text, json"
        )),
    }
}

fn resolve_log_format_precedence(
    cli_format: Option<&str>,
    config_format: Option<&str>,
) -> Result<RuntimeLogFormat> {
    if let Some(format) = cli_format {
        return parse_log_format(format);
    }
    if let Some(format) = config_format {
        return parse_log_format(format);
    }
    Ok(RuntimeLogFormat::Text)
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
        Some(Command::Ping(args)) => Some(args.config.as_path()),
        Some(Command::Init(_)) => None,
        None => cli.infer.as_ref().and_then(|args| args.config.as_deref()),
    }
}

fn resolve_effective_runtime_log_settings(cli: &Cli) -> Result<(LevelFilter, RuntimeLogFormat)> {
    let cli_level = cli.log_level.as_deref();
    let cli_format = cli.log_format.as_deref();

    if let Some(conf_path) = config_path_from_cli(cli) {
        let conf = read_conf(conf_path).with_context(|| {
            format!(
                "Failed to load logging configuration from {}",
                conf_path.display()
            )
        })?;
        let level = resolve_log_level_precedence(cli_level, conf.log_level.as_deref())?;
        let format = resolve_log_format_precedence(cli_format, conf.log_format.as_deref())?;
        return Ok((level, format));
    }

    let level = resolve_log_level_precedence(cli_level, None)?;
    let format = resolve_log_format_precedence(cli_format, None)?;
    Ok((level, format))
}

fn format_runtime_log_line(
    level: Level,
    elapsed: Duration,
    service_name: &str,
    message: &str,
    use_level_color: bool,
) -> String {
    let timestamp = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
    let level_label = if use_level_color && level == Level::Warn {
        // 256-color orange (xterm palette index 208)
        "\u{001b}[38;5;208mWARN\u{001b}[0m".to_string()
    } else {
        level.to_string()
    };
    format!(
        "{timestamp} +{}s [{}] service={} {}",
        elapsed.as_secs(),
        level_label,
        service_name,
        message
    )
}

fn format_runtime_log_line_as_json(
    level: Level,
    elapsed: Duration,
    service_name: &str,
    project_name: &str,
    namespace: &str,
    message: &str,
) -> String {
    let payload = serde_json::json!({
        "ts": Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string(),
        "elapsed_s": elapsed.as_secs(),
        "level": level.to_string(),
        "service": service_name,
        "project_name": project_name,
        "namespace": namespace,
        "msg": message,
    });
    payload.to_string()
}

fn init_runtime_logger(
    level_filter: LevelFilter,
    log_format: RuntimeLogFormat,
    service_name: &str,
    project_name: &str,
    namespace: &str,
) -> Result<()> {
    let start = Instant::now();
    let use_level_color = io::stderr().is_terminal() && std::env::var_os("NO_COLOR").is_none();
    let service_name = service_name.to_owned();
    let project_name = project_name.to_owned();
    let namespace = namespace.to_owned();
    let mut builder = EnvLoggerBuilder::new();
    builder.filter_level(level_filter);
    builder.format(move |buf, record| {
        let line = match log_format {
            RuntimeLogFormat::Text => format_runtime_log_line(
                record.level(),
                start.elapsed(),
                &service_name,
                &record.args().to_string(),
                use_level_color,
            ),
            RuntimeLogFormat::Json => format_runtime_log_line_as_json(
                record.level(),
                start.elapsed(),
                &service_name,
                &project_name,
                &namespace,
                &record.args().to_string(),
            ),
        };
        writeln!(buf, "{line}")
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
        Some(Command::Ping(args)) => {
            if !(args.source || args.target || args.kafka || args.runner) {
                return Err(anyhow!(
                    "ping requires at least one backend flag: --source and/or --target and/or --kafka and/or --runner"
                ));
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

async fn stage_cli_config_paths(cli: &mut Cli) -> Result<Vec<tempfile::TempDir>> {
    let mut stages = Vec::new();

    match &mut cli.command {
        Some(Command::Infer(args)) => {
            if let Some(config) = args.config.take() {
                let (local, stage) = stage_config_path_if_gcs(config).await?;
                args.config = Some(local);
                if let Some(stage) = stage {
                    stages.push(stage);
                }
            }
        }
        Some(Command::ToPg(args)) => {
            if let Some(config) = args.config.take() {
                let (local, stage) = stage_config_path_if_gcs(config).await?;
                args.config = Some(local);
                if let Some(stage) = stage {
                    stages.push(stage);
                }
            }
        }
        Some(Command::Report(args)) => {
            if let Some(config) = args.config.take() {
                let (local, stage) = stage_config_path_if_gcs(config).await?;
                args.config = Some(local);
                if let Some(stage) = stage {
                    stages.push(stage);
                }
            }
        }
        Some(Command::Export(args)) => {
            if let Some(config) = args.config.take() {
                let (local, stage) = stage_config_path_if_gcs(config).await?;
                args.config = Some(local);
                if let Some(stage) = stage {
                    stages.push(stage);
                }
            }
        }
        Some(Command::Import(args)) => {
            let config = std::mem::take(&mut args.config);
            let (local, stage) = stage_config_path_if_gcs(config).await?;
            args.config = local;
            if let Some(stage) = stage {
                stages.push(stage);
            }
        }
        Some(Command::ClusterReport(args)) => {
            let mut local_configs = Vec::with_capacity(args.configs.len());
            for config in args.configs.drain(..) {
                let (local, stage) = stage_config_path_if_gcs(config).await?;
                local_configs.push(local);
                if let Some(stage) = stage {
                    stages.push(stage);
                }
            }
            args.configs = local_configs;
        }
        Some(Command::KafkaImport(args)) => {
            let config = std::mem::take(&mut args.config);
            let (local, stage) = stage_config_path_if_gcs(config).await?;
            args.config = local;
            if let Some(stage) = stage {
                stages.push(stage);
            }
        }
        Some(Command::Ping(args)) => {
            let config = std::mem::take(&mut args.config);
            let (local, stage) = stage_config_path_if_gcs(config).await?;
            args.config = local;
            if let Some(stage) = stage {
                stages.push(stage);
            }
        }
        Some(Command::Init(_)) => {}
        None => {
            if let Some(args) = &mut cli.infer {
                if let Some(config) = args.config.take() {
                    let (local, stage) = stage_config_path_if_gcs(config).await?;
                    args.config = Some(local);
                    if let Some(stage) = stage {
                        stages.push(stage);
                    }
                }
            }
        }
    }

    Ok(stages)
}

// ──────────────────────────────────────────────────────────────────────────────
// `infer` subcommand (also the default)
// ──────────────────────────────────────────────────────────────────────────────

// ──────────────────────────────────────────────────────────────────────────────
// `init` subcommand
// ──────────────────────────────────────────────────────────────────────────────

// ──────────────────────────────────────────────────────────────────────────────
// `export` subcommand
// ──────────────────────────────────────────────────────────────────────────────

// ──────────────────────────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::{
        apply_collection_property_filters, apply_config_overrides, build_collection_mappings,
        build_collection_mappings_with_timestamp_fields, child_row_objects_for_mapping,
        classify_unauthorized_retry, collect_infer_type_warnings, collect_nullable_scalar_warnings,
        count_dynamic_map_entries, detect_candidate_groups, dynamic_map_value_fields,
        format_post_import_trace_line, format_runtime_log_line, format_runtime_log_line_as_json,
        infer_query_max_time, is_unauthorized_cursor_error, plan_export_jobs_for_collections,
        render_ddl_from_mapping_tables, resolve_collections_dir, resolve_export_chunk_size,
        resolve_export_sql_lookup_for_collection, resolve_infer_auth_retry_max,
        resolve_infer_chunk_size, resolve_log_format_precedence, resolve_log_level_precedence,
        resolve_post_import_table_row, resolve_root_table_name, sanitize_name,
        should_fail_post_import_on_warnings, should_fail_report_on_warnings,
        should_infer_collection, strip_psql_preamble, timeout_fallback_hint,
        validate_command_and_args, validate_group_schema_compatibility, Cli, Command,
        ConfigOverrides, PostImportTableRow, UnauthorizedRetryDecision, DEFAULT_EXPORT_CHUNK_ROWS,
        DEFAULT_INFER_AUTH_RETRY_MAX, DEFAULT_INFER_CHUNK_SIZE, DEFAULT_SAMPLE_MAX_TIME,
    };
    use anyhow::{anyhow, Context as _};
    use bson::doc;
    use clap::Parser;
    use log::{Level, LevelFilter};
    use mongo2pg::cli::KafkaImportArgs;
    use mongo2pg::commands::ping::kafka_worker_child_extra_args;
    use mongo2pg::engine::analyzer::Analyzer;
    use mongo2pg::export::{resolve_export_write_backend, ExportWriteBackend};
    use mongo2pg::schema_diagram::Table;
    use mongo2pg::util::connection_failed_context;
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
    fn resolve_log_format_precedence_prefers_cli_over_config() {
        let format = resolve_log_format_precedence(Some("json"), Some("text"))
            .expect("log format precedence should parse");
        assert_eq!(format, super::RuntimeLogFormat::Json);
    }

    #[test]
    fn resolve_log_format_precedence_uses_config_then_default() {
        let config_format =
            resolve_log_format_precedence(None, Some("json")).expect("config format should parse");
        assert_eq!(config_format, super::RuntimeLogFormat::Json);

        let default_format = resolve_log_format_precedence(None, None)
            .expect("default format should resolve to text");
        assert_eq!(default_format, super::RuntimeLogFormat::Text);
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
        let line = format_runtime_log_line(
            Level::Info,
            Duration::from_millis(1250),
            "m2pg-local",
            "hello",
            false,
        );
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
    fn format_runtime_log_line_as_json_emits_expected_fields() {
        let line = format_runtime_log_line_as_json(
            Level::Warn,
            Duration::from_millis(2500),
            "m2pg-local",
            "sample_training",
            "sample_training",
            "hello",
        );
        let parsed: serde_json::Value =
            serde_json::from_str(&line).expect("json log line should parse");

        assert_eq!(parsed.get("elapsed_s").and_then(|v| v.as_u64()), Some(2));
        assert_eq!(parsed.get("level").and_then(|v| v.as_str()), Some("WARN"));
        assert_eq!(
            parsed.get("service").and_then(|v| v.as_str()),
            Some("m2pg-local")
        );
        assert_eq!(
            parsed.get("project_name").and_then(|v| v.as_str()),
            Some("sample_training")
        );
        assert_eq!(
            parsed.get("namespace").and_then(|v| v.as_str()),
            Some("sample_training")
        );
        assert_eq!(parsed.get("msg").and_then(|v| v.as_str()), Some("hello"));

        let ts = parsed
            .get("ts")
            .and_then(|v| v.as_str())
            .expect("timestamp should be string");
        assert_eq!(ts.len(), 19);
        assert!(!ts.contains('.'));
        assert!(!ts.contains('Z'));
        assert!(!ts.contains('+'));
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
            "m2pg-local",
            "connection_failed backend=kafka operation=consume: timeout",
            false,
        );
        assert!(line.contains("connection_failed backend=kafka operation=consume"));
    }

    #[test]
    fn post_import_trace_line_start_contains_required_context() {
        let line = format_post_import_trace_line(
            "start",
            "sample_db.orders",
            true,
            "begin write_post_import_report",
        );
        assert!(line.contains("post_import_report stage=start"));
        assert!(line.contains("namespace=sample_db.orders"));
        assert!(line.contains("include_md5=true"));
        assert!(line.contains("begin write_post_import_report"));
    }

    #[test]
    fn post_import_trace_line_failure_includes_error_detail() {
        let line = format_post_import_trace_line(
            "failure",
            "sample_db.orders",
            true,
            "elapsed_ms=12 error=boom",
        );
        assert!(line.contains("post_import_report stage=failure"));
        assert!(line.contains("elapsed_ms=12"));
        assert!(line.contains("error=boom"));
    }

    #[test]
    fn post_import_trace_line_success_includes_summary_context() {
        let line = format_post_import_trace_line(
            "success",
            "sample_db.orders",
            false,
            "elapsed_ms=45 collections=3 output=/tmp/post_report.html",
        );
        assert!(line.contains("post_import_report stage=success"));
        assert!(line.contains("include_md5=false"));
        assert!(line.contains("collections=3"));
        assert!(line.contains("output=/tmp/post_report.html"));
    }

    #[test]
    fn report_warning_exit_triggers_when_not_quiet_and_has_warnings() {
        assert!(should_fail_report_on_warnings(false, 1));
        assert!(should_fail_report_on_warnings(false, 42));
    }

    #[test]
    fn report_warning_exit_skips_when_quiet_or_no_warnings() {
        assert!(!should_fail_report_on_warnings(true, 3));
        assert!(!should_fail_report_on_warnings(false, 0));
        assert!(!should_fail_report_on_warnings(true, 0));
    }

    #[test]
    fn post_import_warning_exit_triggers_when_warnings_exist() {
        assert!(should_fail_post_import_on_warnings(1));
        assert!(should_fail_post_import_on_warnings(9));
    }

    #[test]
    fn post_import_warning_exit_skips_when_no_warning() {
        assert!(!should_fail_post_import_on_warnings(0));
    }

    #[test]
    fn ping_cli_parses_selected_backend_flags() {
        let cli = Cli::try_parse_from([
            "mongo2pg",
            "ping",
            "-c",
            "sample.toml",
            "--source",
            "--kafka",
            "--runner",
        ])
        .expect("ping CLI args should parse");

        match cli.command {
            Some(Command::Ping(args)) => {
                assert!(args.source);
                assert!(!args.target);
                assert!(args.kafka);
                assert!(args.runner);
                assert_eq!(args.config, PathBuf::from("sample.toml"));
            }
            _ => panic!("expected ping command"),
        }
    }

    #[test]
    fn kafka_import_cli_parses_force_flag() {
        let cli = Cli::try_parse_from(["mongo2pg", "kafka-import", "-c", "sample.toml", "--force"])
            .expect("kafka-import CLI args should parse");

        match cli.command {
            Some(Command::KafkaImport(args)) => {
                assert!(args.force);
                assert_eq!(args.config, PathBuf::from("sample.toml"));
            }
            _ => panic!("expected kafka-import command"),
        }
    }

    #[test]
    fn kafka_worker_child_args_include_operational_flags_and_force() {
        let args = KafkaImportArgs {
            config: PathBuf::from("sample.toml"),
            topics: vec!["topic.a".to_owned(), "topic.b".to_owned()],
            max_messages: Some(100),
            offset: Some("earliest".to_owned()),
            project_dir: Some("sample_airbnb".to_owned()),
            group_id: Some("group1".to_owned()),
            topic_prefix: Some("mongo2pg_sample_airbnb".to_owned()),
            database_name: Some("sample_airbnb".to_owned()),
            schema_name: Some("sample_airbnb".to_owned()),
            force: true,
        };

        let child_args = kafka_worker_child_extra_args(&args);
        assert!(child_args.contains(&"--force".to_owned()));
        assert!(child_args
            .windows(2)
            .any(|w| w == ["--topics", "topic.a,topic.b"]));
        assert!(child_args.windows(2).any(|w| w == ["--offset", "earliest"]));
    }

    #[test]
    fn ping_validation_requires_one_backend_flag() {
        let cli = Cli::try_parse_from(["mongo2pg", "ping", "-c", "sample.toml"])
            .expect("ping CLI args should parse before semantic validation");
        let Cli { command, infer, .. } = cli;

        let err = validate_command_and_args(&command, infer.as_ref())
            .expect_err("ping without backend flags should fail validation");
        assert!(
            format!("{err:#}").contains("ping requires at least one backend flag"),
            "validation error should mention required backend flags"
        );
    }

    #[test]
    fn ping_requested_backends_preserves_flag_order() {
        let args = super::PingArgs {
            config: PathBuf::from("sample.toml"),
            source: true,
            target: true,
            kafka: true,
            runner: true,
        };
        let selected = super::ping_requested_backends(&args);
        assert_eq!(
            selected,
            vec![
                super::PingBackend::Source,
                super::PingBackend::Target,
                super::PingBackend::Kafka,
                super::PingBackend::Runner,
            ]
        );
    }

    #[test]
    fn ping_exit_behavior_all_pass_vs_any_fail() {
        assert!(!super::ping_failed_exit(0));
        assert!(super::ping_failed_exit(1));
    }

    #[test]
    fn ping_failure_render_includes_backend_attribution_token() {
        let err = Err::<(), _>(anyhow!("broker timeout"))
            .with_context(|| connection_failed_context("kafka", "query"))
            .expect_err("context wrap should return error");
        let rendered = format!("kafka:\n{err:#}");
        assert!(rendered.contains("connection_failed backend=kafka operation=query"));
        assert!(rendered.contains("broker timeout"));
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
    fn resolve_target_database_name_prefers_target_database_name() {
        let resolved = super::resolve_target_database_name_from_conf(
            Some("ciam_prep2"),
            Some("ciam_prep.events_lmpt"),
        );
        assert_eq!(resolved.as_deref(), Some("ciam_prep2"));
    }

    #[test]
    fn resolve_target_database_name_falls_back_to_namespace_database() {
        let resolved =
            super::resolve_target_database_name_from_conf(None, Some("ciam_prep.events_lmpt"));
        assert_eq!(resolved.as_deref(), Some("ciam_prep"));
    }

    #[test]
    fn resolve_preamble_database_name_prefers_config_db_name() {
        let rel = PathBuf::from("ciam_prep/events_lmpt.sql");
        let resolved = super::resolve_preamble_database_name(Some("ciam_prep2"), &rel);
        assert_eq!(resolved.as_deref(), Some("ciam_prep2"));
    }

    #[test]
    fn resolve_preamble_database_name_falls_back_to_rel_path_parent() {
        let rel = PathBuf::from("ciam_prep/events_lmpt.sql");
        let resolved = super::resolve_preamble_database_name(None, &rel);
        assert_eq!(resolved.as_deref(), Some("ciam_prep"));
    }

    #[test]
    fn extract_postgres_uri_username_returns_none_without_userinfo() {
        let user = super::extract_postgres_uri_username("postgres://pg-host:5432/defaultdb");
        assert_eq!(user, None);
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
    fn reconcile_mongo_paths_with_fk_lineage_reanchors_children_to_fk_parent_paths() {
        let mut mappings = vec![
            (
                "companies".to_owned(),
                super::CollectionMapping {
                    collection_name: "companies".to_owned(),
                    mongo_dbname: "sample_training".to_owned(),
                    mongo_path: Some(".".to_owned()),
                    traversal: None,
                    pg_mapping: super::PgMapping {
                        dbname: "sample_training".to_owned(),
                        schema_name: "sample_training".to_owned(),
                        table_name: "companies".to_owned(),
                        columns: vec![],
                        ddl: None,
                        ddl_editing: super::default_ddl_editing_guidance(),
                    },
                },
            ),
            (
                "funding_rounds".to_owned(),
                super::CollectionMapping {
                    collection_name: "funding_rounds".to_owned(),
                    mongo_dbname: "sample_training".to_owned(),
                    mongo_path: Some(".funding_rounds".to_owned()),
                    traversal: None,
                    pg_mapping: super::PgMapping {
                        dbname: "sample_training".to_owned(),
                        schema_name: "sample_training".to_owned(),
                        table_name: "funding_rounds".to_owned(),
                        columns: vec![],
                        ddl: Some(super::DdlTableMapping {
                            name: "funding_rounds".to_owned(),
                            columns: vec![],
                            foreign_keys: vec![super::DdlForeignKeyMapping {
                                from_col: "companies_id".to_owned(),
                                to_table: "companies".to_owned(),
                                to_col: "id".to_owned(),
                            }],
                        }),
                        ddl_editing: super::default_ddl_editing_guidance(),
                    },
                },
            ),
            (
                "companies_investments".to_owned(),
                super::CollectionMapping {
                    collection_name: "investments".to_owned(),
                    mongo_dbname: "sample_training".to_owned(),
                    mongo_path: Some(".investments".to_owned()),
                    traversal: None,
                    pg_mapping: super::PgMapping {
                        dbname: "sample_training".to_owned(),
                        schema_name: "sample_training".to_owned(),
                        table_name: "companies_investments".to_owned(),
                        columns: vec![],
                        ddl: Some(super::DdlTableMapping {
                            name: "companies_investments".to_owned(),
                            columns: vec![],
                            foreign_keys: vec![super::DdlForeignKeyMapping {
                                from_col: "funding_rounds_id".to_owned(),
                                to_table: "funding_rounds".to_owned(),
                                to_col: "id".to_owned(),
                            }],
                        }),
                        ddl_editing: super::default_ddl_editing_guidance(),
                    },
                },
            ),
            (
                "funding_round".to_owned(),
                super::CollectionMapping {
                    collection_name: "funding_round".to_owned(),
                    mongo_dbname: "sample_training".to_owned(),
                    mongo_path: Some(".investments.funding_round".to_owned()),
                    traversal: None,
                    pg_mapping: super::PgMapping {
                        dbname: "sample_training".to_owned(),
                        schema_name: "sample_training".to_owned(),
                        table_name: "funding_round".to_owned(),
                        columns: vec![],
                        ddl: Some(super::DdlTableMapping {
                            name: "funding_round".to_owned(),
                            columns: vec![],
                            foreign_keys: vec![super::DdlForeignKeyMapping {
                                from_col: "investments_id".to_owned(),
                                to_table: "companies_investments".to_owned(),
                                to_col: "id".to_owned(),
                            }],
                        }),
                        ddl_editing: super::default_ddl_editing_guidance(),
                    },
                },
            ),
        ];

        super::reconcile_mongo_paths_with_fk_lineage(&mut mappings);

        let by_table = mappings
            .iter()
            .map(|(_, mapping)| {
                (
                    mapping.pg_mapping.table_name.clone(),
                    mapping.mongo_path.clone().unwrap_or_default(),
                )
            })
            .collect::<HashMap<_, _>>();

        assert_eq!(
            by_table.get("companies_investments").map(String::as_str),
            Some(".funding_rounds.investments")
        );
        assert_eq!(
            by_table.get("funding_round").map(String::as_str),
            Some(".funding_rounds.investments.funding_round")
        );
    }

    #[test]
    fn reconcile_mongo_paths_with_fk_lineage_preserves_wrapper_segments_under_root_parent() {
        let mut mappings = vec![
            (
                "companies".to_owned(),
                super::CollectionMapping {
                    collection_name: "companies".to_owned(),
                    mongo_dbname: "sample_training".to_owned(),
                    mongo_path: Some(".".to_owned()),
                    traversal: None,
                    pg_mapping: super::PgMapping {
                        dbname: "sample_training".to_owned(),
                        schema_name: "sample_training".to_owned(),
                        table_name: "companies".to_owned(),
                        columns: vec![],
                        ddl: None,
                        ddl_editing: super::default_ddl_editing_guidance(),
                    },
                },
            ),
            (
                "competitor".to_owned(),
                super::CollectionMapping {
                    collection_name: "competitor".to_owned(),
                    mongo_dbname: "sample_training".to_owned(),
                    mongo_path: Some(".competitions.competitor".to_owned()),
                    traversal: None,
                    pg_mapping: super::PgMapping {
                        dbname: "sample_training".to_owned(),
                        schema_name: "sample_training".to_owned(),
                        table_name: "competitor".to_owned(),
                        columns: vec![],
                        ddl: Some(super::DdlTableMapping {
                            name: "competitor".to_owned(),
                            columns: vec![],
                            foreign_keys: vec![super::DdlForeignKeyMapping {
                                from_col: "companies_id".to_owned(),
                                to_table: "companies".to_owned(),
                                to_col: "id".to_owned(),
                            }],
                        }),
                        ddl_editing: super::default_ddl_editing_guidance(),
                    },
                },
            ),
        ];

        super::reconcile_mongo_paths_with_fk_lineage(&mut mappings);

        let competitor_path = mappings
            .iter()
            .find(|(_, mapping)| mapping.pg_mapping.table_name == "competitor")
            .and_then(|(_, mapping)| mapping.mongo_path.clone());
        assert_eq!(competitor_path.as_deref(), Some(".competitions.competitor"));
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

    #[test]
    fn build_collection_mappings_keeps_map_table_name_with_reserved_names() {
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
        let reserved_table_names = std::collections::HashSet::from(["accounts".to_owned()]);

        let mappings = build_collection_mappings_with_timestamp_fields(
            "sample_analytics",
            "customers",
            Some("sample_analytics"),
            &schema,
            &[],
            &reserved_table_names,
        );

        let accounts_mapping = mappings
            .iter()
            .find(|(_, mapping)| mapping.mongo_path.as_deref() == Some(".accounts"))
            .map(|(_, mapping)| mapping)
            .expect("accounts child mapping should exist");

        assert_eq!(accounts_mapping.pg_mapping.table_name, "customers_accounts");
    }

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
    fn child_row_objects_for_mapping_keeps_root_payload_with_dotted_source_fields() {
        let mapping: super::CollectionMapping = serde_yaml::from_str(
            r#"
collection_name: location
mongo_dbname: sample_mflix
mongo_path: .
pg_mapping:
  dbname: sample_mflix
  schema_name: sample_mflix
  table_name: theaters_location
  columns:
    - source_field: location.address.city
      target_field: city
      data_type: varchar(20)
      nullable: false
"#,
        )
        .expect("mapping should parse");
        let node = serde_json::json!({
            "_id": { "$oid": "59a47287cfa9a3a73e51ec78" },
            "location": { "address": { "city": "Bloomington" } }
        });

        let rows = child_row_objects_for_mapping(&node, &mapping);

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0]
                .get("location")
                .and_then(|location| location.get("address"))
                .and_then(|address| address.get("city")),
            Some(&serde_json::Value::String("Bloomington".to_owned()))
        );
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
                "provider": "provider1",
                "cloud": "gcp",
                "network_exposition": "private_platform"
            }],
            "prod": [{
                "available_localizations": ["eu-west-2"],
                "provider": "provider2",
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
                "namespace": "nprd-t-dba-176c358",
                "namespace_id": "nprd-t-dba-176c358",
                "provider": "provider1",
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
    fn render_ddl_from_mapping_tables_with_owner_emits_alter_schema_owner() {
        let sql = super::render_ddl_from_mapping_tables_with_owner(
            &[super::DdlTableMapping {
                name: "demo".to_owned(),
                columns: vec![super::DdlColumnMapping {
                    name: "id".to_owned(),
                    sql_type: "BIGSERIAL".to_owned(),
                    nullable: false,
                    primary_key: true,
                }],
                foreign_keys: Vec::new(),
            }],
            Some("ciam_prep2"),
            Some("user_ciam"),
        );

        assert!(sql.contains("CREATE SCHEMA IF NOT EXISTS \"ciam_prep2\";"));
        assert!(sql.contains("ALTER SCHEMA \"ciam_prep2\" OWNER TO \"user_ciam\";"));
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
    fn render_ddl_from_mapping_tables_adds_index_for_grouped_key_column() {
        let sql = render_ddl_from_mapping_tables(
            &[super::DdlTableMapping {
                name: "events".to_owned(),
                columns: vec![
                    super::DdlColumnMapping {
                        name: "id".to_owned(),
                        sql_type: "BIGSERIAL".to_owned(),
                        nullable: false,
                        primary_key: true,
                    },
                    super::DdlColumnMapping {
                        name: "_key".to_owned(),
                        sql_type: "TEXT".to_owned(),
                        nullable: true,
                        primary_key: false,
                    },
                ],
                foreign_keys: Vec::new(),
            }],
            Some("dbapi"),
        );

        assert!(sql.contains(
            "CREATE INDEX IF NOT EXISTS \"idx_events_key\" ON \"dbapi\".\"events\" (\"_key\");"
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
    fn pg_admin_fallback_uris_includes_expected_candidates_without_duplicates() {
        let target_uri = "postgres://user:pw@localhost:5432/defaultdb?sslmode=require";
        let target_database_name = "defaultdb";
        let mut uris = vec![target_uri.to_owned()];
        for db_name in ["postgres", "template1", target_database_name] {
            let candidate = super::pg_uri_with_database(target_uri, db_name);
            if !uris.iter().any(|existing| existing == &candidate) {
                uris.push(candidate);
            }
        }

        assert_eq!(uris.len(), 3);
        assert_eq!(
            uris[0],
            "postgres://user:pw@localhost:5432/defaultdb?sslmode=require"
        );
        assert_eq!(
            uris[1],
            "postgres://user:pw@localhost:5432/postgres?sslmode=require"
        );
        assert_eq!(
            uris[2],
            "postgres://user:pw@localhost:5432/template1?sslmode=require"
        );
    }

    #[test]
    fn preflight_existing_tables_error_includes_remediation() {
        let err = super::preflight_existing_tables_error(
            "ciam_prep",
            &[("events".to_owned(), "root".to_owned())],
        );
        let rendered = format!("{err:#}");
        assert!(rendered.contains("destination table(s) already exist"));
        assert!(rendered.contains("Drop the existing table(s)"));
        assert!(rendered.contains("\"events\".\"root\""));
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

    #[tokio::test]
    async fn run_init_writes_default_datetime_field_patterns() {
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
            cluster_name: None,
        })
        .await
        .expect("init should succeed");

        let conf_path = project_base.join("dbapi").join("config").join("dbapi.toml");
        let content = std::fs::read_to_string(&conf_path).expect("config file should be readable");

        assert!(content.contains(
            "datetime_field = [\"created_at\", \"last_update\", \"updated_at\", \"*_date\", \"date\"]"
        ));
        assert!(content.contains("namespace = \"dbapi\""));
        assert!(!content.contains("#namespace = \"dbapi\""));
        assert!(content.contains("database_name = \"dbapi\""));
        assert!(content.contains("schema_name = \"dbapi\""));
        assert!(!content.contains("# schema_name = \"shared_schema\""));

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
            cluster_name: None,
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
    use testcontainers_modules::{
        mongo, postgres,
        testcontainers::{runners::AsyncRunner, ImageExt},
    };

    // Data Structures
    #[derive(serde::Serialize)]
    struct Employee {
        id: i32,
        name: String,
        hire_date: DateTime<Utc>,
        created_at: String,
        last_update: String,
    }

    fn docker_available_for_testcontainers() -> bool {
        match std::env::var("DOCKER_HOST") {
            Ok(host) => {
                if let Some(path) = host.strip_prefix("unix://") {
                    return std::path::Path::new(path).exists();
                }
                true
            }
            Err(_) => std::path::Path::new("/var/run/docker.sock").exists(),
        }
    }

    #[tokio::test]
    async fn test_mongo_to_pg_data_flow() -> Result<(), Box<dyn std::error::Error>> {
        if !docker_available_for_testcontainers() {
            eprintln!(
                "Skipping test_mongo_to_pg_data_flow: Docker socket unavailable for testcontainers"
            );
            return Ok(());
        }

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
            postgres::Postgres::default().with_tag("17").start(),
            mongo::Mongo::default().with_tag("8.0").start()
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

        let db_mongo = "test_db";
        let (pg_admin_client, pg_admin_connection) =
            tokio_postgres::connect(&pg_connection_string, NoTls).await?;
        tokio::spawn(async move {
            if let Err(err) = pg_admin_connection.await {
                log::error!("PostgreSQL admin connection error: {}", err);
            }
        });
        let db_exists_row = pg_admin_client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)",
                &[&db_mongo],
            )
            .await?;
        let db_exists: bool = db_exists_row.get(0);
        if !db_exists {
            pg_admin_client
                .execute(
                    &format!("CREATE DATABASE {}", super::quote_ident(db_mongo)),
                    &[],
                )
                .await?;
        }

        // MongoDB Client
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
        run_init(init_args).await.expect("init should succeed");

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
        assert!(
            conf_toml.contains(&format!("database_name = \"{}\"", db_mongo)),
            "Config should contain database_name derived from namespace"
        );
        assert!(
            conf_toml.contains(&format!("schema_name = \"{}\"", db_mongo)),
            "Config should default schema_name to database_name"
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
            --CREATE DATABASE "test_db";
            \connect "test_db"

            CREATE EXTENSION IF NOT EXISTS "pgcrypto";

            CREATE SCHEMA IF NOT EXISTS "test_db";
            ALTER SCHEMA "test_db" OWNER TO "postgres";
            SET search_path = "test_db", public;

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
            .execute("SET search_path TO test_db, public", &[])
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

    #[test]
    fn missing_import_csv_error_mentions_export_and_layout() {
        let err =
            super::build_missing_import_csv_error(Path::new("/tmp/project/data/mydb"), None, 2);
        let message = err.to_string();

        assert!(message.contains("No .csv or .csv.gz files found in /tmp/project/data/mydb"));
        assert!(message.contains("Found 2 SQL table(s), but no matching data files."));
        assert!(
            message.contains("Expected layout: data/<db>/<collection>/<table>.csv.gz (or .csv).")
        );
        assert!(message.contains("mongo2pg export -c <config>"));
    }

    #[test]
    fn missing_import_csv_error_mentions_collection_scope() {
        let err = super::build_missing_import_csv_error(
            Path::new("/tmp/project/data/mydb"),
            Some("users"),
            0,
        );
        let message = err.to_string();

        assert!(message.contains("for requested collection 'users'"));
        assert!(message.contains("No importable SQL tables were discovered from schema files."));
    }
}
