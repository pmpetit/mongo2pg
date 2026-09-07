//! `init` subcommand: scaffold a new project directory (or GCS prefix) with a
//! starter config file and the standard directory layout.

use anyhow::{Context, Result};
use bytes::Bytes;
use google_cloud_storage::client::Storage;
use log::info;

use crate::cli::InitArgs;
use crate::commands::shared::ensure_output_prefix_segments;
use crate::export::{ensure_gcs_authentication, resolve_export_write_backend, ExportWriteBackend};

pub async fn run_init(args: InitArgs) -> Result<()> {
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
    let target_database_name = args
        .namespace
        .as_deref()
        .map(|ns| ns.split('.').next().unwrap_or(ns))
        .unwrap_or(&args.project_name);
    let namespace_line = args
        .namespace
        .as_deref()
        .map(|ns| format!("namespace = \"{}\"", ns.replace('"', "\\\"")))
        .unwrap_or_else(|| "#namespace = my_db".to_owned());
    let cluster_line = args
        .cluster_name
        .as_deref()
        .map(|name| format!("cluster_name = \"{}\"", name.replace('"', "\\\"")))
        .unwrap_or_else(|| "# cluster_name = \"cluster-a\"".to_owned());
    let conf_content = format!(
        "[project]\ntitle = \"{}\"\nbase_dir = \"{}\"\n{}\nproject_dir = \"{}\"\n\n[source]\nuri = {}\n{}\nnumber = 1000\n# percent = 10.0\n# chunk_size = 1000000\n# auth_retry_max = 3\n# log_level = \"info\"\n# log-format = \"text\" # or \"json\"\njsonb = false\n# include = [\"collection_a\", \"collection_b\"]\n# exclude = [\"collection_to_skip\"]\ndatetime_field = [\"created_at\", \"last_update\", \"updated_at\", \"*_date\", \"date\"]\n\n[target]\nuri = {}\ndatabase_name = \"{}\"\nschema_name = \"{}\"\n\n[kafka]\nbootstrap_servers = \"localhost:9092\"\ngroup_id = \"mongo2pg-kafka-import\"\n# topics = [\"mongo2pg_dbapi.dbapi.projects\"]\n# topic_prefix = \"mongo2pg_dbapi\"\n# security_protocol = \"SASL_SSL\"\n# sasl_mechanism = \"PLAIN\"\n# sasl_username = \"<kafka_api_key>\"\n# sasl_password = \"<kafka_api_secret>\"\nschema_registry_url = \"http://localhost:8081\"\n# schema_registry_username = \"\"\n# schema_registry_password = \"\"\noffset = \"latest\"\n# auto_offset_reset = \"earliest\" # legacy key still supported\n# max_messages = 1000\n# batch_log_messages = 100\n# flush_batch_after = \"1000ms\"\n# poll_interval_ms = 500\n# poll_size = 1000\n# transaction_batch_size = 200\n# worker_count = 1\n# group_id_log_suffix = true\n# stop_on_no_lag = false\n",
        project_title.replace('"', "\\\""),
        args.project_base.display(),
        cluster_line,
        args.project_name,
        args.source_uri
            .as_deref()
            .map(|u| format!("\"{}\"", u.replace('"', "\\\"")))
            .unwrap_or_else(|| "\"mongodb://localhost:27017\"".to_owned()),
        namespace_line,
        args.target_uri
            .as_deref()
            .map(|u| format!("\"{}\"", u.replace('"', "\\\"")))
            .unwrap_or_else(|| {
                "\"postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable\""
                    .to_owned()
            }),
        target_database_name.replace('"', "\\\""),
        target_database_name.replace('"', "\\\""),
    );
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
                project_root.join("source").join("collections"),
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
                "source/collections/",
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
            info!("{}/schema/tables", project_root_uri);
            info!("{}/source/collections", project_root_uri);
            info!("{}/data", project_root_uri);
            info!("{}/config", project_root_uri);
            info!("{}/reports", project_root_uri);
            info!("gs://{}/{}", bucket, config_object);
            Ok(())
        }
    }
}
