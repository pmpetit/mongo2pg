//! `init` subcommand: scaffold a new project directory (or GCS prefix) with a
//! starter config file and the standard directory layout.

use anyhow::{Context, Result};
use bytes::Bytes;
use google_cloud_storage::client::Storage;
use log::info;

use crate::cli::InitArgs;
use crate::commands::shared::ensure_output_prefix_segments;
use crate::commands::infer::{
    DEFAULT_INFER_AUTH_RETRY_MAX, DEFAULT_INFER_CHUNK_SIZE,
};
use crate::export::{ensure_gcs_authentication, resolve_export_write_backend, ExportWriteBackend};

const INIT_DEFAULT_MAX_TIME_MS: u128 = 120_000;

pub async fn run_init(args: InitArgs) -> Result<()> {
    fn parse_csv_values(raw: Option<&str>) -> Vec<String> {
        raw.map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
    }

    fn quote_toml_string(value: &str) -> String {
        format!("\"{}\"", value.replace('"', "\\\""))
    }

    fn render_toml_string_or_array(values: &[String]) -> String {
        if values.len() <= 1 {
            values
                .first()
                .map(|value| quote_toml_string(value))
                .unwrap_or_else(|| "\"\"".to_owned())
        } else {
            let items = values
                .iter()
                .map(|value| quote_toml_string(value))
                .collect::<Vec<_>>()
                .join(", ");
            format!("[{items}]")
        }
    }

    if let Some(percent) = args.percent {
        if !(0.0 < percent && percent <= 100.0) {
            anyhow::bail!("--percent must be > 0 and <= 100");
        }
    }
    let cluster_name = args
        .cluster_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let config_file_name = format!("{}.toml", cluster_name.unwrap_or(&args.project_name));

    let project_title = if let Some(cluster_name) = args
        .cluster_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        format!("Mongo2Pg Project migration ({cluster_name})")
    } else {
        "Mongo2Pg Project migration".to_owned()
    };
    let namespace_values = parse_csv_values(args.namespace.as_deref());
    let configured_database_values = parse_csv_values(args.database_name.as_deref());
    let configured_schema_values = parse_csv_values(args.schema_name.as_deref());

    if namespace_values.len() > 1
        && !configured_database_values.is_empty()
        && configured_database_values.len() != namespace_values.len()
    {
        anyhow::bail!(
            "--database-name list length ({}) must match --namespace list length ({}) when passing multiple namespaces",
            configured_database_values.len(),
            namespace_values.len()
        );
    }
    if namespace_values.len() > 1
        && !configured_schema_values.is_empty()
        && configured_schema_values.len() != namespace_values.len()
    {
        anyhow::bail!(
            "--schema-name list length ({}) must match --namespace list length ({}) when passing multiple namespaces",
            configured_schema_values.len(),
            namespace_values.len()
        );
    }

    let inferred_database_values: Vec<String> = if !namespace_values.is_empty() {
        namespace_values
            .iter()
            .map(|ns| ns.split('.').next().unwrap_or(ns).to_owned())
            .collect()
    } else {
        vec![args.project_name.clone()]
    };
    let target_database_values = if configured_database_values.is_empty() {
        inferred_database_values
    } else {
        configured_database_values
    };
    let target_schema_values = if configured_schema_values.is_empty() {
        target_database_values.clone()
    } else {
        configured_schema_values
    };

    let namespace_line = if namespace_values.is_empty() {
        "#namespace = \"my_db\"\n# namespace can also be an array to process several \
databases in one run, e.g.:\n# namespace = [\"my_db\", \"other_db\"]"
            .to_owned()
    } else {
        format!(
            "namespace = {}",
            render_toml_string_or_array(&namespace_values)
        )
    };
    let cluster_line = args
        .cluster_name
        .as_deref()
        .map(|name| format!("cluster_name = \"{}\"", name.replace('"', "\\\"")))
        .unwrap_or_else(|| "# cluster_name = \"cluster-a\"".to_owned());
    let sampling_line = args
        .percent
        .map(|percent| format!("percent = {percent}"))
        .unwrap_or_else(|| "number = 1000".to_owned());
    let log_format_line = args
        .log_format
        .as_deref()
        .map(|format| format!("log_format = \"{}\"", format.replace('"', "\\\"")))
        .unwrap_or_else(|| "log_format = \"text\"".to_owned());
    let conf_content = format!(
        "[project]\ntitle = \"{}\"\nbase_dir = \"{}\"\n{}\nproject_dir = \"{}\"\n\n[source]\nuri = {}\n{}\nnumber = 1000\n# percent = 10.0\nmax_time_ms = {}\nchunk_size = {}\nauth_retry_max = {}\ninfer_mode = \"raw\"\nlog_level = \"info\"\n{}\nadd_grouped_key = false\njsonb = false\n# include = [\"collection_a\", \"collection_b\"]\n# exclude = [\"collection_to_skip\"]\ndatetime_field = [\"created_at\", \"last_update\", \"updated_at\", \"*_date\", \"date\"]\n\n[target]\nuri = {}\ndatabase_name = {}\nschema_name = {}\n\n[kafka]\nbootstrap_servers = \"localhost:9092\"\ngroup_id = \"mongo2pg-kafka-import\"\n# topics = [\"mongo2pg_dbapi.dbapi.projects\"]\n# topic_prefix = \"mongo2pg_dbapi\"\n# security_protocol = \"SASL_SSL\"\n# sasl_mechanism = \"PLAIN\"\n# sasl_username = \"<kafka_api_key>\"\n# sasl_password = \"<kafka_api_secret>\"\nschema_registry_url = \"http://localhost:8081\"\n# schema_registry_username = \"\"\n# schema_registry_password = \"\"\noffset = \"latest\"\nauto_offset_reset = \"earliest\"\n# max_messages = 1000\nbatch_log_messages = 100\n# flush_batch_after = \"1000ms\"\n# poll_interval_ms = 500\n# poll_size = 1000\ntransaction_batch_size = 1\nworker_count = 1\ngroup_id_log_suffix = true\nstop_on_no_lag = false\n",
        project_title.replace('"', "\\\""),
        args.project_base.display(),
        cluster_line,
        args.project_name,
        args.source_uri
            .as_deref()
            .map(|u| format!("\"{}\"", u.replace('"', "\\\"")))
            .unwrap_or_else(|| "\"mongodb://localhost:27017\"".to_owned()),
        namespace_line,
        INIT_DEFAULT_MAX_TIME_MS,
        DEFAULT_INFER_CHUNK_SIZE,
        DEFAULT_INFER_AUTH_RETRY_MAX,
        log_format_line,
        args.target_uri
            .as_deref()
            .map(|u| format!("\"{}\"", u.replace('"', "\\\"")))
            .unwrap_or_else(|| {
                "\"postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable\""
                    .to_owned()
            }),
        render_toml_string_or_array(&target_database_values),
        render_toml_string_or_array(&target_schema_values),
    )
    .replace("number = 1000", &sampling_line);
    let storage_backend = resolve_export_write_backend(&args.project_base)?;
    match storage_backend {
        ExportWriteBackend::LocalFs => {
            let project_root = if let Some(cluster_name) = cluster_name {
                args.project_base
                    .join(&args.project_name)
                    .join(cluster_name)
            } else {
                args.project_base.join(&args.project_name)
            };

            let dirs = [
                project_root.join("schema").join("tables"),
                project_root.join("data"),
                project_root.join("config"),
                project_root.join("reports"),
            ];

            for dir in &dirs {
                std::fs::create_dir_all(dir)
                    .with_context(|| format!("Failed to create directory {}", dir.display()))?;
            }

            let conf_path = project_root.join("config").join(config_file_name);
            std::fs::write(&conf_path, conf_content)
                .with_context(|| format!("Failed to write {}", conf_path.display()))?;

            info!(
                "Project '{}' initialised at {}",
                args.project_name,
                project_root.display()
            );
            for dir in &dirs {
                info!("{}", dir.display());
            }
            info!("{}", conf_path.display());
            Ok(())
        }
        ExportWriteBackend::Gcs { bucket, prefix } => {
            ensure_gcs_authentication().await?;
            let storage = Storage::builder()
                .build()
                .await
                .context("Failed to initialize Google Cloud Storage client")?;
            let bucket_resource = format!("projects/_/buckets/{bucket}");

            let effective_prefix =
                ensure_output_prefix_segments(&prefix, cluster_name, &args.project_name);
            let project_root_uri = if effective_prefix.is_empty() {
                format!("gs://{bucket}")
            } else {
                format!("gs://{bucket}/{effective_prefix}")
            };

            let config_object = if effective_prefix.is_empty() {
                format!("config/{config_file_name}")
            } else {
                format!("{effective_prefix}/config/{config_file_name}")
            };

            storage
                .write_object(
                    bucket_resource,
                    config_object.clone(),
                    Bytes::from(conf_content.into_bytes()),
                )
                .send_buffered()
                .await
                .with_context(|| {
                    format!(
                        "Failed to write config to gs://{}/{}",
                        bucket, config_object
                    )
                })?;

            let folder_markers = [
                "schema/tables/",
                "data/",
                "config/",
                "reports/",
            ];
            for folder in folder_markers {
                let marker_object = format!("{effective_prefix}/{folder}");
                storage
                    .write_object(
                        format!("projects/_/buckets/{bucket}"),
                        marker_object,
                        Bytes::new(),
                    )
                    .send_buffered()
                    .await
                    .context("Failed to create GCS project folder marker")?;
            }

            info!(
                "Project '{}' initialised at {}",
                args.project_name, project_root_uri
            );
            info!(
                "https://storage.cloud.google.com/{}/{}",
                bucket, config_object
            );
            Ok(())
        }
    }
}
