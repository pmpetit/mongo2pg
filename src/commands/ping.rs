//! `ping` subcommand: connectivity checks for the configured Mongo source,
//! Postgres target, and/or Kafka bootstrap servers.

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use bson::doc;
use log::{info, warn};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::ClientConfig;

use crate::cli::{KafkaImportArgs, PingArgs};
use crate::db::kafka::{apply_security_config, detect_default_ssl_ca_location};
use crate::db::mongo::{client_with_options, parse_client_options};
use crate::db::pg::connect_client as connect_pg_client;
use crate::util::{connection_failed_context, read_conf};

fn redact_uri_password(uri: &str) -> String {
    let trimmed = uri.trim();
    let Some(scheme_sep) = trimmed.find("://") else {
        return trimmed.to_owned();
    };

    let scheme = &trimmed[..scheme_sep + 3];
    let rest = &trimmed[scheme_sep + 3..];
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    let suffix = &rest[authority_end..];

    if let Some((userinfo, hostpart)) = authority.rsplit_once('@') {
        let username = userinfo.split(':').next().unwrap_or("");
        if username.is_empty() {
            format!("{scheme}{hostpart}{suffix}")
        } else {
            format!("{scheme}{username}:***@{hostpart}{suffix}")
        }
    } else {
        format!("{scheme}{authority}{suffix}")
    }
}

fn uri_username(uri: &str) -> Option<String> {
    let rest = uri.split_once("://")?.1;
    let authority = rest.split('/').next().unwrap_or(rest);
    let (userinfo, _) = authority.rsplit_once('@')?;
    let username = userinfo.split(':').next().unwrap_or("").trim();
    if username.is_empty() {
        None
    } else {
        Some(username.to_owned())
    }
}

fn uri_host_port(uri: &str) -> (Option<String>, Option<String>) {
    let rest = match uri.split_once("://") {
        Some((_, value)) => value,
        None => uri,
    };
    let authority = rest.split('/').next().unwrap_or(rest);
    let after_creds = authority
        .rsplit_once('@')
        .map(|(_, host)| host)
        .unwrap_or(authority);

    let first_host = after_creds
        .split(',')
        .map(str::trim)
        .find(|value| !value.is_empty());
    let Some(host_entry) = first_host else {
        return (None, None);
    };

    if let Some(stripped) = host_entry.strip_prefix('[') {
        if let Some(end_bracket) = stripped.find(']') {
            let host = stripped[..end_bracket].to_owned();
            let remainder = &stripped[end_bracket + 1..];
            let port = remainder
                .strip_prefix(':')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned);
            return (Some(host), port);
        }
    }

    if let Some((host, port)) = host_entry.rsplit_once(':') {
        if port.chars().all(|ch| ch.is_ascii_digit()) {
            return (Some(host.to_owned()), Some(port.to_owned()));
        }
    }

    (Some(host_entry.to_owned()), None)
}

fn kafka_hosts_ports(bootstrap_servers: &str) -> (String, String) {
    let mut hosts = Vec::new();
    let mut ports = Vec::new();

    for entry in bootstrap_servers.split(',').map(str::trim).filter(|v| !v.is_empty()) {
        let without_scheme = entry
            .strip_prefix("kafka-secure://")
            .or_else(|| entry.strip_prefix("kafka://"))
            .or_else(|| entry.strip_prefix("ssl://"))
            .unwrap_or(entry);
        let host_port = without_scheme
            .split_once('/')
            .map(|(value, _)| value)
            .unwrap_or(without_scheme)
            .trim();
        if host_port.is_empty() {
            continue;
        }

        let (host, port) = if let Some(stripped) = host_port.strip_prefix('[') {
            if let Some(end_bracket) = stripped.find(']') {
                let host = stripped[..end_bracket].to_owned();
                let remainder = &stripped[end_bracket + 1..];
                let port = remainder
                    .strip_prefix(':')
                    .map(str::trim)
                    .unwrap_or("")
                    .to_owned();
                (host, port)
            } else {
                (host_port.to_owned(), String::new())
            }
        } else if let Some((host, port)) = host_port.rsplit_once(':') {
            if port.chars().all(|ch| ch.is_ascii_digit()) {
                (host.to_owned(), port.to_owned())
            } else {
                (host_port.to_owned(), String::new())
            }
        } else {
            (host_port.to_owned(), String::new())
        };

        hosts.push(host);
        if !port.is_empty() {
            ports.push(port);
        }
    }

    let host_display = if hosts.is_empty() {
        "unknown".to_owned()
    } else {
        hosts.join(",")
    };
    let port_display = if ports.is_empty() {
        "unknown".to_owned()
    } else {
        ports.join(",")
    };

    (host_display, port_display)
}

fn detect_process_ip_for_endpoint(host: Option<&str>, port: Option<&str>) -> String {
    let resolved_port = port
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(80);

    if let Some(host_value) = host.map(str::trim).filter(|value| !value.is_empty()) {
        if let Ok(socket) = std::net::UdpSocket::bind("0.0.0.0:0") {
            if socket.connect((host_value, resolved_port)).is_ok() {
                if let Ok(addr) = socket.local_addr() {
                    return addr.ip().to_string();
                }
            }
        }
        if let Ok(socket) = std::net::UdpSocket::bind("[::]:0") {
            if socket.connect((host_value, resolved_port)).is_ok() {
                if let Ok(addr) = socket.local_addr() {
                    return addr.ip().to_string();
                }
            }
        }
    }

    for fallback in ["8.8.8.8:80", "1.1.1.1:80", "9.9.9.9:80"] {
        if let Ok(socket) = std::net::UdpSocket::bind("0.0.0.0:0") {
            if socket.connect(fallback).is_ok() {
                if let Ok(addr) = socket.local_addr() {
                    return addr.ip().to_string();
                }
            }
        }
    }

    "unknown".to_owned()
}

fn ping_failure_context(backend: PingBackend, conf: &crate::util::ConfData) -> String {
    match backend {
        PingBackend::Source => {
            let uri = conf
                .source_uri
                .as_deref()
                .map(redact_uri_password)
                .unwrap_or_else(|| "<missing>".to_owned());
            let (host, port) = conf
                .source_uri
                .as_deref()
                .map(uri_host_port)
                .unwrap_or((None, None));
            let username = conf
                .source_uri
                .as_deref()
                .and_then(uri_username)
                .unwrap_or_else(|| "unknown".to_owned());
            let process_ip = detect_process_ip_for_endpoint(host.as_deref(), port.as_deref());
            format!(
                "type=source host={} port={} username={} process_ip={} url={}",
                host.unwrap_or_else(|| "unknown".to_owned()),
                port.unwrap_or_else(|| "unknown".to_owned()),
                username,
                process_ip,
                uri
            )
        }
        PingBackend::Target => {
            let uri = conf
                .target_uri
                .as_deref()
                .map(redact_uri_password)
                .unwrap_or_else(|| "<missing>".to_owned());
            let (host, port) = conf
                .target_uri
                .as_deref()
                .map(uri_host_port)
                .unwrap_or((None, None));
            let username = conf
                .target_uri
                .as_deref()
                .and_then(uri_username)
                .unwrap_or_else(|| "unknown".to_owned());
            let process_ip = detect_process_ip_for_endpoint(host.as_deref(), port.as_deref());
            format!(
                "type=target host={} port={} username={} process_ip={} url={}",
                host.unwrap_or_else(|| "unknown".to_owned()),
                port.unwrap_or_else(|| "unknown".to_owned()),
                username,
                process_ip,
                uri
            )
        }
        PingBackend::Kafka => {
            let bootstrap_servers = conf
                .kafka
                .as_ref()
                .and_then(|kafka| kafka.bootstrap_servers.as_deref())
                .unwrap_or("");
            let (hosts, ports) = kafka_hosts_ports(bootstrap_servers);
            let username = conf
                .kafka
                .as_ref()
                .and_then(|kafka| kafka.sasl_username.as_deref())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("unknown");
            let url = if bootstrap_servers.trim().is_empty() {
                "<missing>".to_owned()
            } else {
                format!("kafka://{}", bootstrap_servers.trim())
            };
            let first_host = hosts.split(',').next().map(str::trim);
            let first_port = ports.split(',').next().map(str::trim);
            let process_ip = detect_process_ip_for_endpoint(first_host, first_port);
            format!(
                "type=kafka host={} port={} username={} process_ip={} url={}",
                hosts, ports, username, process_ip, url
            )
        }
        PingBackend::Runner => {
            let process_ip = detect_process_ip_for_endpoint(None, None);
            format!("type=runner process_ip={}", process_ip)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PingBackend {
    Source,
    Target,
    Kafka,
    Runner,
}

impl PingBackend {
    fn label(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Target => "target",
            Self::Kafka => "kafka",
            Self::Runner => "runner",
        }
    }
}

pub fn ping_requested_backends(args: &PingArgs) -> Vec<PingBackend> {
    let mut selected = Vec::new();
    if args.source {
        selected.push(PingBackend::Source);
    }
    if args.target {
        selected.push(PingBackend::Target);
    }
    if args.kafka {
        selected.push(PingBackend::Kafka);
    }
    if args.runner {
        selected.push(PingBackend::Runner);
    }
    selected
}

pub fn ping_failed_exit(failures: usize) -> bool {
    failures > 0
}

pub fn kafka_worker_child_extra_args(args: &KafkaImportArgs) -> Vec<String> {
    let mut argv = Vec::new();

    if !args.topics.is_empty() {
        argv.push("--topics".to_owned());
        argv.push(args.topics.join(","));
    }
    if let Some(value) = args.offset.as_deref() {
        argv.push("--offset".to_owned());
        argv.push(value.to_owned());
    }
    if let Some(value) = args.project_dir.as_deref() {
        argv.push("--project-dir".to_owned());
        argv.push(value.to_owned());
    }
    if let Some(value) = args.group_id.as_deref() {
        argv.push("--group-id".to_owned());
        argv.push(value.to_owned());
    }
    if let Some(value) = args.topic_prefix.as_deref() {
        argv.push("--topic-prefix".to_owned());
        argv.push(value.to_owned());
    }
    if let Some(value) = args.database_name.as_deref() {
        argv.push("--database-name".to_owned());
        argv.push(value.to_owned());
    }
    if let Some(value) = args.schema_name.as_deref() {
        argv.push("--schema-name".to_owned());
        argv.push(value.to_owned());
    }
    if args.force {
        argv.push("--force".to_owned());
    }
    argv
}

pub async fn run_ping(args: PingArgs) -> Result<()> {
    fn normalize_kafka_bootstrap_servers(raw: &str) -> Result<String> {
        fn normalize_one(endpoint: &str) -> Option<String> {
            let trimmed = endpoint.trim();
            if trimmed.is_empty() {
                return None;
            }

            let without_scheme = trimmed
                .strip_prefix("kafka-secure://")
                .or_else(|| trimmed.strip_prefix("kafka://"))
                .or_else(|| trimmed.strip_prefix("ssl://"))
                .unwrap_or(trimmed);

            let without_path = without_scheme
                .split_once('/')
                .map(|(host_port, _)| host_port)
                .unwrap_or(without_scheme)
                .trim_end_matches('/');

            if without_path.is_empty() {
                None
            } else {
                Some(without_path.to_owned())
            }
        }

        let normalized = raw.split(',').filter_map(normalize_one).collect::<Vec<_>>();

        if normalized.is_empty() {
            return Err(anyhow!(
                "kafka.bootstrap_servers resolved to empty value after URI normalization"
            ));
        }

        Ok(normalized.join(","))
    }

    async fn ping_source_backend(conf: &crate::util::ConfData) -> Result<()> {
        let source_uri = conf
            .source_uri
            .as_deref()
            .ok_or_else(|| anyhow!("No SOURCE_URI provided: add SOURCE_URI to the config file"))?;
        let client_options = parse_client_options(source_uri).await.with_context(|| {
            format!(
                "{}: failed to parse MongoDB SOURCE_URI",
                connection_failed_context("mongo", "connect")
            )
        })?;
        let client = client_with_options(client_options).with_context(|| {
            format!(
                "{}: failed to connect to MongoDB using SOURCE_URI",
                connection_failed_context("mongo", "connect")
            )
        })?;
        client
            .database("admin")
            .run_command(doc! {"ping": 1_i32})
            .await
            .with_context(|| {
                format!(
                    "{}: failed MongoDB ping command",
                    connection_failed_context("mongo", "query")
                )
            })?;
        Ok(())
    }

    async fn ping_target_backend(conf: &crate::util::ConfData) -> Result<()> {
        let target_uri = conf
            .target_uri
            .as_deref()
            .ok_or_else(|| anyhow!("No TARGET_URI provided: add TARGET_URI to the config file"))?;
        let pg_client = connect_pg_client(target_uri).await?;
        pg_client
            .query_one("SELECT 1", &[])
            .await
            .with_context(|| {
                format!(
                    "{}: failed PostgreSQL ping query",
                    connection_failed_context("pg", "query")
                )
            })?;
        Ok(())
    }

    async fn ping_kafka_backend(conf: &crate::util::ConfData) -> Result<()> {
        let kafka_conf = conf
            .kafka
            .as_ref()
            .ok_or_else(|| anyhow!("Missing [kafka] section in config file"))?;
        let bootstrap_servers_raw = kafka_conf
            .bootstrap_servers
            .as_deref()
            .ok_or_else(|| anyhow!("kafka.bootstrap_servers is required"))?;
        let bootstrap_servers = normalize_kafka_bootstrap_servers(bootstrap_servers_raw)?;

        let default_ssl_ca_location = detect_default_ssl_ca_location(kafka_conf);

        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &bootstrap_servers)
            .set(
                "group.id",
                kafka_conf.group_id.as_deref().unwrap_or("mongo2pg-ping"),
            );
        apply_security_config(
            &mut client_config,
            kafka_conf,
            default_ssl_ca_location.as_deref(),
        );

        let consumer: StreamConsumer = client_config.create().with_context(|| {
            format!(
                "{}: failed to create Kafka consumer for ping",
                connection_failed_context("kafka", "connect")
            )
        })?;
        consumer
            .fetch_metadata(None, Duration::from_secs(10))
            .with_context(|| {
                format!(
                    "{}: failed to fetch Kafka metadata",
                    connection_failed_context("kafka", "query")
                )
            })?;

        Ok(())
    }

    async fn ping_runner_backend(conf: &crate::util::ConfData) -> Result<()> {
        let kafka_conf = conf
            .kafka
            .as_ref()
            .ok_or_else(|| anyhow!("Missing [kafka] section in config file"))?;
        let effective_group_id = kafka_conf
            .group_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("mongo2pg-kafka-import");
        let bootstrap_servers_raw = kafka_conf
            .bootstrap_servers
            .as_deref()
            .ok_or_else(|| anyhow!("kafka.bootstrap_servers is required"))?;
        let bootstrap_servers = normalize_kafka_bootstrap_servers(bootstrap_servers_raw)?;

        let default_ssl_ca_location = detect_default_ssl_ca_location(kafka_conf);

        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &bootstrap_servers)
            .set("group.id", effective_group_id);
        apply_security_config(
            &mut client_config,
            kafka_conf,
            default_ssl_ca_location.as_deref(),
        );

        let consumer: StreamConsumer = client_config.create().with_context(|| {
            format!(
                "{}: failed to create Kafka consumer for runner ping",
                connection_failed_context("kafka", "connect")
            )
        })?;

        let metadata = consumer
            .fetch_metadata(None, Duration::from_secs(10))
            .with_context(|| {
                format!(
                    "{}: failed to fetch Kafka metadata for runner ping",
                    connection_failed_context("kafka", "query")
                )
            })?;

        let mut topics = kafka_conf
            .topics
            .iter()
            .map(|topic| topic.trim())
            .filter(|topic| !topic.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();

        if topics.is_empty() {
            if let Some(prefix) = kafka_conf.topic_prefix.as_deref() {
                topics = metadata
                    .topics()
                    .iter()
                    .map(|topic| topic.name().to_owned())
                    .filter(|topic| topic.starts_with(prefix))
                    .collect::<Vec<_>>();
                topics.sort();
                topics.dedup();

                if topics.is_empty() {
                    return Err(anyhow!(
                        "No Kafka topics matched prefix '{}'. Set [kafka].topics, set [kafka].topic_prefix, or create topics with this prefix.",
                        prefix
                    ));
                }
            } else {
                return Err(anyhow!(
                    "No Kafka topics configured for runner check. Set [kafka].topics or [kafka].topic_prefix."
                ));
            }
        }

        let existing_topics = metadata
            .topics()
            .iter()
            .map(|topic| topic.name().to_owned())
            .collect::<std::collections::HashSet<_>>();
        let missing_topics = topics
            .iter()
            .filter(|topic| !existing_topics.contains(*topic))
            .cloned()
            .collect::<Vec<_>>();

        if !missing_topics.is_empty() {
            return Err(anyhow!(
                "Kafka topic(s) not found on broker metadata: {}",
                missing_topics.join(", ")
            ));
        }

        let group_list = consumer
            .fetch_group_list(Some(effective_group_id), Duration::from_secs(10))
            .with_context(|| {
                format!(
                    "{}: failed to fetch Kafka consumer groups for runner ping",
                    connection_failed_context("kafka", "query")
                )
            })?;

        let Some(group) = group_list
            .groups()
            .iter()
            .find(|group| group.name() == effective_group_id)
        else {
            return Err(anyhow!(
                "Kafka runner group '{}' not found. Start kafka-import worker(s) first.",
                effective_group_id
            ));
        };

        if group.members().is_empty() {
            return Err(anyhow!(
                "Kafka runner group '{}' has no active members. Worker(s) may not be running.",
                effective_group_id
            ));
        }

        let has_assignment = group.members().iter().any(|member| {
            member
                .assignment()
                .map(|assignment| !assignment.is_empty())
                .unwrap_or(false)
        });

        if !has_assignment {
            return Err(anyhow!(
                "Kafka runner group '{}' has members but no partition assignment yet (group_state={}); workers may not be listening to topics yet",
                effective_group_id,
                group.state()
            ));
        }

        let state = group.state().to_ascii_lowercase();
        if matches!(state.as_str(), "dead" | "empty") {
            return Err(anyhow!(
                "Kafka runner group '{}' is in unhealthy state '{}'",
                effective_group_id,
                group.state()
            ));
        }

        info!(
            "Runner check passed for group '{}' (state='{}', members={}, topics={})",
            effective_group_id,
            group.state(),
            group.members().len(),
            topics.join(",")
        );

        Ok(())
    }

    let conf = read_conf(&args.config)?;
    let selected_backends = ping_requested_backends(&args);
    let mut failures = Vec::new();

    for backend in selected_backends {
        let result = match backend {
            PingBackend::Source => ping_source_backend(&conf).await,
            PingBackend::Target => ping_target_backend(&conf).await,
            PingBackend::Kafka => ping_kafka_backend(&conf).await,
            PingBackend::Runner => ping_runner_backend(&conf).await,
        };

        match result {
            Ok(()) => info!("Ping {}: ok", backend.label()),
            Err(err) => {
                let context = ping_failure_context(backend, &conf);
                warn!("Ping {}: failed ({})", backend.label(), context);
                failures.push(format!("{} ({context}):\n{err:#}", backend.label()));
            }
        }
    }

    if ping_failed_exit(failures.len()) {
        return Err(anyhow!(
            "Ping failed for {} backend(s):\n{}",
            failures.len(),
            failures.join("\n\n")
        ));
    }

    info!("Ping completed successfully.");
    Ok(())
}
