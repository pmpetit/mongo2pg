//! PostgreSQL connector: client construction shared across command handlers.

use anyhow::{Context, Result};
use log::warn;
use postgres_native_tls::MakeTlsConnector;

use crate::util::connection_failed_context;

/// Extract the `sslmode` query-string parameter from a PostgreSQL connection URI, if present.
pub fn pg_sslmode(uri: &str) -> Option<&str> {
    let query = uri.split_once('?')?.1;
    query.split('&').find_map(|part| {
        let (key, value) = part.split_once('=')?;
        if key.eq_ignore_ascii_case("sslmode") {
            Some(value)
        } else {
            None
        }
    })
}

/// Connect to PostgreSQL using `target_uri`, spawning a background task that drives
/// the connection and logs any connection errors.
pub async fn connect_client(target_uri: &str) -> Result<tokio_postgres::Client> {
    let mut tls_builder = native_tls::TlsConnector::builder();
    if matches!(pg_sslmode(target_uri), Some(mode) if mode.eq_ignore_ascii_case("require")) {
        tls_builder.danger_accept_invalid_certs(true);
        tls_builder.danger_accept_invalid_hostnames(true);
    }
    let tls = tls_builder.build().with_context(|| {
        format!(
            "{}: failed to initialize PostgreSQL TLS connector",
            connection_failed_context("pg", "connect")
        )
    })?;
    let tls = MakeTlsConnector::new(tls);

    let (pg_client, pg_connection) = tokio_postgres::connect(target_uri, tls)
        .await
        .with_context(|| {
            format!(
                "{}: failed to connect to PostgreSQL using TARGET_URI",
                connection_failed_context("pg", "connect")
            )
        })?;
    tokio::spawn(async move {
        if let Err(err) = pg_connection.await {
            warn!(
                "{} PostgreSQL connection error: {err}",
                connection_failed_context("pg", "connect")
            );
        }
    });

    Ok(pg_client)
}
