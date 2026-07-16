use crate::analyzer::{CollectionSchema, FieldSchema, TypeSchema};
use crate::util::{
    can_inline_object_fields, flatten_grouped_root_array_object_fields,
    flatten_root_array_object_field, flattened_root_parent_id_column,
    grouped_root_array_object_fields, inline_object_column_names_with_prefix,
    inline_object_leaf_fields_with_prefix, read_conf, scalar_type_family,
};
use crate::export::resolve_grouped_sql_lookup_name;
use anyhow::{anyhow, Context, Result};
use bson::{doc, Bson, Document};
use chrono::{DateTime, SecondsFormat, Utc};
use futures::{SinkExt, TryStreamExt};
use postgres_native_tls::MakeTlsConnector;
use serde::{Deserialize,Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Instant;
use tokio_postgres::{Client, Row};
use log::{debug, error, info, warn};

#[derive(Debug, Clone, Deserialize)]
struct MappingYaml {
    #[serde(default)]
    #[serde(alias = "dbname")]
    mongo_dbname: Option<String>,
    #[serde(default)]
    mongo_path: Option<String>,
    #[serde(default)]
    traversal: Option<TraversalYaml>,
    pg_mapping: PgMappingYaml,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TraversalModeYaml {
    Root,
    Object,
    ArrayObject,
    ArrayScalar,
    MapObject,
}

#[derive(Debug, Clone, Deserialize)]
struct TraversalYaml {
    mode: TraversalModeYaml,
    #[serde(default)]
    parent_table: Option<String>,
    #[serde(default)]
    source_field: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct PgMappingYaml {
    #[serde(default)]
    dbname: Option<String>,
    #[serde(default)]
    schema_name: Option<String>,
    table_name: String,
    #[serde(default)]
    columns: Vec<MappingColumnYaml>,
    #[serde(default)]
    ddl: Option<DdlTableYaml>,
}

#[derive(Debug, Clone, Deserialize)]
struct MappingColumnYaml {
    source_field: String,
    target_field: String,
    #[serde(default)]
    data_type: Option<String>,
    #[serde(default)]
    literal_value: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct DdlTableYaml {
    columns: Vec<DdlColumnYaml>,
    #[serde(default)]
    foreign_keys: Vec<DdlForeignKeyYaml>,
}

#[derive(Debug, Clone, Deserialize)]
struct DdlColumnYaml {
    name: String,
    sql_type: String,
}

#[derive(Debug, Clone, Deserialize)]
struct DdlForeignKeyYaml {
    from_col: String,
}

#[derive(Debug, Clone)]
pub struct Md5ColumnMapping {
    pub source_field: String,
    pub source_type: Option<String>,
    pub target_field: String,
    pub target_type: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Md5Summary {
    pub mongo_md5: String,
    pub pg_md5: String,
    pub columns: Vec<Md5ColumnMapping>,
    pub mismatches: Vec<Md5MismatchRow>,
}

#[derive(Debug, Clone)]
pub struct Md5MismatchRow {
    pub row_index: usize,
    pub mongo_values: Option<Vec<String>>,
    pub pg_values: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct Md5TableSummary {
    pub table_name: String,
    pub summary: Md5Summary,
}

#[derive(Debug, Clone)]
struct MappingTarget {
    source_collection: String,
    mapping_path: PathBuf,
    mapping_yaml: MappingYaml,
    source_path: SourcePath,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SourcePath {
    path: Vec<String>,
    scalar_array_field: Option<String>,
    grouped_fields: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HashRecord {
    md5: String,
    values: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CollectionChecksumResult {
    mongo_md5: String,
    pg_md5: String,
    matches: bool,
    /// Populate when `matches` is false
    mismatches: Option<MismatchDelta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MismatchDelta {
    mongo_only: Vec<RowSnapshot>,
    pg_only: Vec<RowSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RowSnapshot {
    md5: String,
    delta: Vec<String>, // The string representations from your values array
}

#[cfg(target_os = "linux")]
fn linux_process_memory_kib() -> (Option<u64>, Option<u64>) {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return (None, None);
    };

    let mut rss_kib: Option<u64> = None;
    let mut hwm_kib: Option<u64> = None;

    for line in status.lines() {
        if let Some(value) = line.strip_prefix("VmRSS:") {
            rss_kib = value
                .split_whitespace()
                .next()
                .and_then(|raw| raw.parse::<u64>().ok());
            continue;
        }
        if let Some(value) = line.strip_prefix("VmHWM:") {
            hwm_kib = value
                .split_whitespace()
                .next()
                .and_then(|raw| raw.parse::<u64>().ok());
        }
    }

    (rss_kib, hwm_kib)
}

fn log_oom_probe(stage: &str, table_name: &str, mongo_rows: usize, pg_rows: usize) {
    #[cfg(target_os = "linux")]
    {
        let (rss_kib, hwm_kib) = linux_process_memory_kib();
        debug!(
            "oom_probe stage={} table={} pid={} mongo_docs={} pg_rows={} vmrss_kib={} vmhwm_kib={}",
            stage,
            table_name,
            std::process::id(),
            mongo_rows,
            pg_rows,
            rss_kib.map(|v| v.to_string()).unwrap_or_else(|| "na".to_owned()),
            hwm_kib.map(|v| v.to_string()).unwrap_or_else(|| "na".to_owned())
        );
    }

    #[cfg(not(target_os = "linux"))]
    {
        log::debug!(
            "oom_probe stage={} table={} pid={} mongo_rows={} pg_rows={} vmrss_kib=na vmhwm_kib=na",
            stage,
            table_name,
            std::process::id(),
            mongo_rows,
            pg_rows,
        );
    }
}

fn copy_text_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '\t' => escaped.push_str("\\t"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn append_copy_text_row(buffer: &mut String, md5: &str, values: &str) {
    buffer.push_str(&copy_text_escape(md5));
    buffer.push('\t');
    buffer.push_str(&copy_text_escape(values));
    buffer.push('\n');
}

fn build_row_deltas(
    mongo_vals: Option<&[String]>,
    pg_vals: Option<&[String]>,
    _target_fields: &[String], // Prepended with '_' since field names aren't needed anymore
) -> (Vec<String>, Vec<String>) {
    match (mongo_vals, pg_vals) {
        // Case 1: Row completely missing in MongoDB
        (None, Some(pv)) => (
            vec![format!("Row missing in MongoDB")],
            vec![format!("Row exists only in PostgreSQL: {:?}", pv)],
        ),
        // Case 2: Row completely missing in PostgreSQL
        (Some(mv), None) => (
            vec![format!("Row exists only in MongoDB: {:?}", mv)],
            vec![format!("Row missing in PostgreSQL")],
        ),
        // Case 3: Both exist, check if the data arrays differ
        (Some(mv), Some(pv)) => {
            if mv == pv {
                // Perfect match, no deltas to return
                (vec![], vec![])
            } else {
                // They are different, just return the complete document/row representations
                (
                    vec![format!("MongoDB Document: {:?}", mv)],
                    vec![format!("PostgreSQL Row: {:?}", pv)],
                )
            }
        }
        // Case 4: Neither exists
        (None, None) => (vec![], vec![]),
    }
}

/// Orchestrates the process using temporary tables to sort and calculate the aggregate checksums.
async fn compute_collection_checksums_via_temp_tables(
    pg_read_client: &Client,
    _pg_read_uri: Option<&str>,
    pg_write_client: &Client,
    mongo_collection: &mongodb::Collection<bson::Document>,
    source_fields: &[(String, Option<String>)], // Typed MongoDB source fields
    source_path: &SourcePath,
    db_name: &str,
    coll_name: &str,
    schema_name: Option<&str>,
    table_name: &str,
    target_fields: &[String],
    pg_key_filter: Option<(&str, &str)>,
) -> Result<CollectionChecksumResult> {
    const COPY_BUFFER_ROWS: usize = 5_000;
    const MONGO_CURSOR_RETRY_MAX: u32 = 4;
    const PG_STREAM_RETRY_MAX: u32 = 4;
    const PG_STREAM_OPEN_TIMEOUT_SECS: u64 = 600;
    const PG_STREAM_IDLE_HEARTBEAT_SECS: u64 = 45;
    const PG_STREAM_IDLE_STALL_HEARTBEATS: u32 = 3;
    const PG_STREAM_IDLE_STALL_RETRY_MAX: u32 = 2;
    const PG_QUERY_HEARTBEAT_SECS: u64 = 30;
    const PG_EXPLAIN_TIMEOUT_SECS: u64 = 120;

    async fn await_pg_with_heartbeat<T, F>(
        future: F,
        table_name: &str,
        stage: &str,
        mongo_rows: usize,
        pg_rows: usize,
        heartbeat_secs: u64,
    ) -> Result<T>
    where
        F: std::future::Future<Output = std::result::Result<T, tokio_postgres::Error>>,
    {
        futures::pin_mut!(future);
        loop {
            match tokio::time::timeout(std::time::Duration::from_secs(heartbeat_secs), future.as_mut())
                .await
            {
                Ok(Ok(value)) => return Ok(value),
                Ok(Err(err)) => return Err(err.into()),
                Err(_) => {
                    info!(
                        "  ↳ ⏳ Waiting for PostgreSQL query stage={} table={} (mongo_rows={} pg_rows={} idle={}s)",
                        stage,
                        table_name,
                        mongo_rows,
                        pg_rows,
                        heartbeat_secs,
                    );
                    log_oom_probe(stage, table_name, mongo_rows, pg_rows);
                }
            }
        }
    }

    async fn explain_select_query(client: &Client, select_sql: &str, timeout_secs: u64) -> String {
        let explain_sql = format!("EXPLAIN (FORMAT TEXT) {select_sql}");
        match tokio::time::timeout(
            std::time::Duration::from_secs(timeout_secs),
            client.query(&explain_sql, &[]),
        )
        .await
        {
            Ok(Ok(rows)) => {
                let plan = rows
                    .iter()
                    .map(|row| row.get::<usize, String>(0))
                    .collect::<Vec<_>>()
                    .join("\n");
                if plan.is_empty() {
                    "<empty explain output>".to_owned()
                } else {
                    plan
                }
            }
            Ok(Err(err)) => format!("<explain failed: {:#}>", err),
            Err(_) => format!("<explain timed out after {}s>", timeout_secs),
        }
    }

    let suffix = sanitize_pg_name(table_name);
    let temp_mongo_table = format!("temp_mongo_hashes_{suffix}");
    let temp_pg_table = format!("temp_pg_hashes_{suffix}");

    let temp_mongo_ident = quote_ident(&temp_mongo_table);
    let temp_pg_ident = quote_ident(&temp_pg_table);

    log_oom_probe("start", table_name, 0, 0);
 
    // --- STEP 1: Initialize temporary tables ---
    // Using ON COMMIT PRESERVE ROWS so they last for the lifetime of our session/connection
    pg_write_client
        .execute(
            &format!(
                "CREATE TEMP TABLE {} (md5 TEXT, values TEXT) ON COMMIT PRESERVE ROWS",
                temp_mongo_ident
            ),
            &[],
        )
        .await
        .context("Failed to create temporary table for MongoDB hashes")?;

    pg_write_client
        .execute(
            &format!(
                "CREATE TEMP TABLE {} (md5 TEXT, values TEXT) ON COMMIT PRESERVE ROWS",
                temp_pg_ident
            ),
            &[],
        )
        .await
        .context("Failed to create temporary table for Postgres hashes")?;

    log_oom_probe("temp_tables_ready", table_name, 0, 0);

    // --- STEP 2: Stream Mongo data, calculate MD5 row-by-row, and save to temp table ---
    info!("🔄 [Collection: {}] Starting MongoDB md5 docs compute...", coll_name);
    let mut mongo_row_count: usize = 0;
    let mongo_log_every: usize = 10000;
    let mongo_total_docs = match mongo_collection.estimated_document_count().await {
        Ok(total) => Some(total as usize),
        Err(err) => {
            warn!(
                "Could not estimate MongoDB total docs for {}.{} table={}: {:#}",
                db_name, coll_name, table_name, err
            );
            None
        }
    };
    let mongo_copy_sql = format!("COPY {} (md5, values) FROM STDIN", temp_mongo_ident);
    let mongo_copy_sink = pg_write_client.copy_in(&mongo_copy_sql).await?;
    let mut mongo_copy_sink = std::pin::pin!(mongo_copy_sink);
    let mut mongo_copy_buffer = String::with_capacity(256 * 1024);
    let mut mongo_copy_buffered_rows = 0usize;
    let mut last_seen_id: Option<Bson> = None;
    let mut cursor_retry_attempt = 0_u32;
    'mongo_stream: loop {
        let find_filter = match &last_seen_id {
            Some(last_id) => doc! { "_id": { "$gt": last_id.clone() } },
            None => doc! {},
        };
        let mut mongo_cursor = mongo_collection
            .find(find_filter)
            .sort(doc! { "_id": 1 })
            .no_cursor_timeout(true)
            .await
            .with_context(|| format!("Failed to query {db_name}.{coll_name} for md5 stream"))?;

        loop {
            let next_doc = match mongo_cursor.try_next().await {
                Ok(value) => value,
                Err(err) => {
                    if cursor_retry_attempt < MONGO_CURSOR_RETRY_MAX {
                        cursor_retry_attempt += 1;
                        warn!(
                            "Mongo md5 cursor retry {}/{} for {}.{} table={} rows={} last_id={} reason={:#}",
                            cursor_retry_attempt,
                            MONGO_CURSOR_RETRY_MAX,
                            db_name,
                            coll_name,
                            table_name,
                            mongo_row_count,
                            last_seen_id
                                .as_ref()
                                .map(|id| id.to_string())
                                .unwrap_or_else(|| "<begin>".to_owned()),
                            err
                        );
                        continue 'mongo_stream;
                    }
                    return Err(anyhow!(
                        "Mongo md5 cursor failed after {} retries for {}.{} table={} rows={} last_id={}: {:#}",
                        MONGO_CURSOR_RETRY_MAX,
                        db_name,
                        coll_name,
                        table_name,
                        mongo_row_count,
                        last_seen_id
                            .as_ref()
                            .map(|id| id.to_string())
                            .unwrap_or_else(|| "<begin>".to_owned()),
                        err
                    ));
                }
            };

            let Some(doc) = next_doc else {
                break 'mongo_stream;
            };

            cursor_retry_attempt = 0;
            if let Some(doc_id) = doc.get("_id") {
                last_seen_id = Some(doc_id.clone());
            }

            if source_path.path.is_empty()
                && source_path.scalar_array_field.is_none()
                && source_path.grouped_fields.is_none()
            {
                let record = mongo_hash_record_for_columns(&doc, source_fields);
                let record_values = mongo_hash_values_pipe_for_columns(&doc, source_fields);

                append_copy_text_row(&mut mongo_copy_buffer, &record.md5, &record_values);
                mongo_copy_buffered_rows += 1;
                if mongo_copy_buffered_rows >= COPY_BUFFER_ROWS {
                    mongo_copy_sink
                        .as_mut()
                        .send(bytes::Bytes::from(std::mem::take(&mut mongo_copy_buffer).into_bytes()))
                        .await?;
                    mongo_copy_buffered_rows = 0;
                }
                mongo_row_count += 1;
                if mongo_row_count % mongo_log_every == 0 {
                    if let Some(total_docs) = mongo_total_docs {
                        info!(
                            "  ↳ 📥 Read {}/{} MongoDB docs, compute md5 and copy result to temp table from {}...",
                            mongo_row_count,
                            total_docs,
                            coll_name
                        );
                    } else {
                        info!(
                            "  ↳ 📥 Read {} MongoDB docs, compute md5 and copy result to temp table from {}...",
                            mongo_row_count,
                            coll_name
                        );
                    }
                    //log_oom_probe("mongo_stream_progress", coll_name, mongo_row_count, 0);
                }
                continue;
            }

            for mut source_doc in extract_source_documents(&doc, source_path) {
                if source_fields.iter().any(|(field, _)| field == "_id")
                    && !source_doc.contains_key("_id")
                {
                    if let Some(root_id) = doc.get("_id") {
                        source_doc.insert("_id", root_id.clone());
                    }
                }

                let record = mongo_hash_record_for_columns(&source_doc, source_fields);
                let record_values = mongo_hash_values_pipe_for_columns(&source_doc, source_fields);
                append_copy_text_row(&mut mongo_copy_buffer, &record.md5, &record_values);
                mongo_copy_buffered_rows += 1;
                if mongo_copy_buffered_rows >= COPY_BUFFER_ROWS {
                    mongo_copy_sink
                        .as_mut()
                        .send(bytes::Bytes::from(std::mem::take(&mut mongo_copy_buffer).into_bytes()))
                        .await?;
                    mongo_copy_buffered_rows = 0;
                }
                mongo_row_count += 1;
                if mongo_row_count % mongo_log_every == 0 {
                    if let Some(total_docs) = mongo_total_docs {
                        info!(
                            "  ↳ 📥 Read nested {}/{} MongoDB docs, compute md5 and copy result to temp table from {}...",
                            mongo_row_count,
                            total_docs,
                            coll_name
                        );
                    } else {
                        info!(
                            "  ↳ 📥 Read nested {} MongoDB docs, compute md5 and copy result to temp table from {}...",
                            mongo_row_count,
                            coll_name
                        );
                    }
                    //log_oom_probe("mongo_stream_progress", table_name, mongo_row_count, 0);
                }
            }
        }
    }
    if !mongo_copy_buffer.is_empty() {
        mongo_copy_sink
            .as_mut()
            .send(bytes::Bytes::from(std::mem::take(&mut mongo_copy_buffer).into_bytes()))
            .await?;
    }
    mongo_copy_sink.as_mut().finish().await?;

    info!(
        "✅ [Collection: {}] MongoDB md5 docs complete. Docs: {}",
        coll_name, mongo_row_count
    );
    log_oom_probe("mongo_stream_done", coll_name, mongo_row_count, 0);

    // --- STEP 3: Stream target Postgres data (unordered), calculate MD5, and save to temp table ---
    info!("🔄 [Table: {}] Starting PostgreSQL md5 rows compute...", table_name);
    let pg_resume_field = source_fields
        .iter()
        .zip(target_fields.iter())
        .find_map(|((source_field, _), target_field)| {
            if source_field == "_id" {
                Some(target_field.as_str())
            } else {
                None
            }
        })
        .or_else(|| {
            target_fields
                .iter()
                .find(|field| field.eq_ignore_ascii_case("id"))
                .map(String::as_str)
        });
    // Keep ordering only when we have a stable resume key (_id/id).
    // Fallback ordering by first projected column can trigger expensive sorts.
    let pg_order_field = pg_resume_field;
    log_oom_probe("pg_stream_start", table_name, mongo_row_count, 0);
    let _pg_stream_started_at = Instant::now();

    let pg_copy_sql = format!("COPY {} (md5, values) FROM STDIN", temp_pg_ident);
    let pg_copy_sink = pg_write_client.copy_in(&pg_copy_sql).await?;
    let mut pg_copy_sink = std::pin::pin!(pg_copy_sink);
    let mut pg_copy_buffer = String::with_capacity(256 * 1024);
    let mut pg_copy_buffered_rows = 0usize;

    let pg_qualified_table = match schema_name {
        Some(schema_name) => format!("{}.{}", quote_ident(schema_name), quote_ident(table_name)),
        None => quote_ident(table_name),
    };
    let mut pg_count_sql = format!("SELECT COUNT(*)::BIGINT FROM {pg_qualified_table}");
    if let Some((field, value)) = pg_key_filter {
        pg_count_sql.push_str(&format!(
            " WHERE {} = '{}'",
            quote_ident(field),
            value.replace('\'', "''")
        ));
    }
    let pg_total_rows = match tokio::time::timeout(
        std::time::Duration::from_secs(PG_EXPLAIN_TIMEOUT_SECS),
        pg_read_client.query_one(&pg_count_sql, &[]),
    )
    .await
    {
        Ok(Ok(row)) => Some(row.get::<usize, i64>(0) as usize),
        Ok(Err(err)) => {
            warn!(
                "Could not count PostgreSQL total rows for table={} query={}: {:#}",
                table_name, pg_count_sql, err
            );
            None
        }
        Err(_) => {
            warn!(
                "Counting PostgreSQL total rows timed out after {}s for table={} query={}",
                PG_EXPLAIN_TIMEOUT_SECS,
                table_name,
                pg_count_sql
            );
            None
        }
    };

    let mut pg_row_count = 0;
    let mut pg_last_order_value: Option<String> = None;
    let mut pg_retry_attempt = 0_u32;
    let mut pg_idle_stall_retry_attempt = 0_u32;
    let pg_resume_field_name = pg_resume_field.map(str::to_owned);
    let pg_resume_field_index = pg_resume_field_name
        .as_ref()
        .and_then(|field| target_fields.iter().position(|candidate| candidate == field));
    let mut pg_idle_heartbeat_count = 0_u32;

    'pg_stream: loop {
        let select_sql = pg_select_query_unordered(
            schema_name,
            table_name,
            target_fields,
            pg_order_field,
            pg_key_filter,
            pg_resume_field_name
                .as_deref()
                .zip(pg_last_order_value.as_deref()),
        );

        let pg_stream_open = tokio::time::timeout(
            std::time::Duration::from_secs(PG_STREAM_OPEN_TIMEOUT_SECS),
            async {
                pg_read_client
                    .query_raw(
                        &select_sql,
                        std::iter::empty::<&(dyn tokio_postgres::types::ToSql + Sync)>(),
                    )
                    .await
                    .map_err(anyhow::Error::from)
            },
        )
        .await;

        let pg_stream = match pg_stream_open {
            Err(_) => {
                let explain_plan =
                    explain_select_query(pg_read_client, &select_sql, PG_EXPLAIN_TIMEOUT_SECS)
                        .await;
                if pg_retry_attempt < PG_STREAM_RETRY_MAX {
                    pg_retry_attempt += 1;
                    warn!(
                        "PostgreSQL md5 stream open timed out after {}s, retry {}/{} for table={} rows={} last_order={} query={} explain_plan={}",
                        PG_STREAM_OPEN_TIMEOUT_SECS,
                        pg_retry_attempt,
                        PG_STREAM_RETRY_MAX,
                        table_name,
                        pg_row_count,
                        pg_last_order_value.as_deref().unwrap_or("<begin>"),
                        select_sql,
                        explain_plan
                    );
                    continue 'pg_stream;
                }
                return Err(anyhow!(
                    "PostgreSQL md5 stream open timed out after {}s and {} retries for table={} rows={} last_order={} query={} explain_plan={}",
                    PG_STREAM_OPEN_TIMEOUT_SECS,
                    PG_STREAM_RETRY_MAX,
                    table_name,
                    pg_row_count,
                    pg_last_order_value.as_deref().unwrap_or("<begin>"),
                    select_sql,
                    explain_plan
                ));
            }
            Ok(Err(err)) => {
                if pg_retry_attempt < PG_STREAM_RETRY_MAX {
                    pg_retry_attempt += 1;
                    warn!(
                        "PostgreSQL md5 stream retry {}/{} for table={} rows={} last_order={} reason={:#} query={}",
                        pg_retry_attempt,
                        PG_STREAM_RETRY_MAX,
                        table_name,
                        pg_row_count,
                        pg_last_order_value.as_deref().unwrap_or("<begin>"),
                        err,
                        select_sql
                    );
                    continue 'pg_stream;
                }
                return Err(anyhow!(
                    "PostgreSQL md5 stream failed after {} retries for table={} rows={} last_order={}: {:#}; query={}",
                    PG_STREAM_RETRY_MAX,
                    table_name,
                    pg_row_count,
                    pg_last_order_value.as_deref().unwrap_or("<begin>"),
                    err,
                    select_sql
                ));
            }
            Ok(Ok(stream)) => stream,
        };

        futures::pin_mut!(pg_stream);
        loop {
            let row = match tokio::time::timeout(
                std::time::Duration::from_secs(PG_STREAM_IDLE_HEARTBEAT_SECS),
                pg_stream.try_next(),
            )
            .await
            {
                Ok(Ok(value)) => value,
                Ok(Err(err)) => {
                    if pg_retry_attempt < PG_STREAM_RETRY_MAX {
                        pg_retry_attempt += 1;
                        warn!(
                            "PostgreSQL md5 stream retry {}/{} for table={} rows={} last_order={} reason={:#} query={}",
                            pg_retry_attempt,
                            PG_STREAM_RETRY_MAX,
                            table_name,
                            pg_row_count,
                            pg_last_order_value
                                .as_deref()
                                .unwrap_or("<begin>"),
                            err,
                            select_sql
                        );
                        continue 'pg_stream;
                    }
                    return Err(anyhow!(
                        "PostgreSQL md5 stream failed after {} retries for table={} rows={} last_order={}: {:#}; query={}",
                        PG_STREAM_RETRY_MAX,
                        table_name,
                        pg_row_count,
                        pg_last_order_value
                            .as_deref()
                            .unwrap_or("<begin>"),
                        err,
                        select_sql
                    ));
                }
                Err(timeout_err) => {
                    pg_idle_heartbeat_count += 1;
                    info!(
                        "  ↳ ⏳ Waiting for PostgreSQL rows from {} (processed={} last_order={} idle={}s timeout_error={} idle_count={}/{})",
                        table_name,
                        pg_row_count,
                        pg_last_order_value
                            .as_deref()
                            .unwrap_or("<begin>"),
                        PG_STREAM_IDLE_HEARTBEAT_SECS,
                        timeout_err,
                        pg_idle_heartbeat_count,
                        PG_STREAM_IDLE_STALL_HEARTBEATS,
                    );
                    log_oom_probe("pg_stream_idle", table_name, mongo_row_count, pg_row_count);

                    if pg_idle_heartbeat_count >= PG_STREAM_IDLE_STALL_HEARTBEATS {
                        if pg_resume_field_name.is_some()
                            && pg_idle_stall_retry_attempt < PG_STREAM_IDLE_STALL_RETRY_MAX
                        {
                            pg_idle_stall_retry_attempt += 1;
                            warn!(
                                "PostgreSQL md5 stream idle-stall retry {}/{} for table={} rows={} last_order={}",
                                pg_idle_stall_retry_attempt,
                                PG_STREAM_IDLE_STALL_RETRY_MAX,
                                table_name,
                                pg_row_count,
                                pg_last_order_value.as_deref().unwrap_or("<begin>")
                            );
                            pg_idle_heartbeat_count = 0;
                            continue 'pg_stream;
                        }

                        return Err(anyhow!(
                            "PostgreSQL md5 stream stalled for {} heartbeat intervals on table={} rows={} last_order={} (resume_field={}): no progress",
                            PG_STREAM_IDLE_STALL_HEARTBEATS,
                            table_name,
                            pg_row_count,
                            pg_last_order_value.as_deref().unwrap_or("<begin>"),
                            pg_resume_field_name.as_deref().unwrap_or("<none>")
                        ));
                    }
                    continue;
                }
            };

            let Some(row) = row else {
                break 'pg_stream;
            };

            pg_idle_heartbeat_count = 0;
            pg_idle_stall_retry_attempt = 0;
            pg_retry_attempt = 0;
            if let Some(order_idx) = pg_resume_field_index {
                let order_value: String = row.get(order_idx);
                pg_last_order_value = Some(order_value);
            }

            // Transform columns into normalized representations and calculate row MD5
            let record = pg_hash_record(&row);
            let record_values = pg_hash_values_pipe(&row, target_fields);
            append_copy_text_row(&mut pg_copy_buffer, &record.md5, &record_values);
            pg_copy_buffered_rows += 1;
            if pg_copy_buffered_rows >= COPY_BUFFER_ROWS {
                pg_copy_sink
                    .as_mut()
                    .send(bytes::Bytes::from(std::mem::take(&mut pg_copy_buffer).into_bytes()))
                    .await?;
                pg_copy_buffered_rows = 0;
            }
            pg_row_count += 1;
            if pg_row_count % mongo_log_every == 0 {
                if let Some(total_rows) = pg_total_rows {
                    info!(
                        "  ↳ 📥 Read {}/{} PostgreSQL rows, compute md5 and /copy to temp table from {}...",
                        pg_row_count,
                        total_rows,
                        table_name
                    );
                } else {
                    info!(
                        "  ↳ 📥 Read {} PostgreSQL rows, compute md5 and /copy to temp table from {}...",
                        pg_row_count,
                        table_name
                    );
                }
                log_oom_probe("pg_stream_progress", table_name, mongo_row_count, pg_row_count);
            }
        }
    }
    if !pg_copy_buffer.is_empty() {
        pg_copy_sink
            .as_mut()
            .send(bytes::Bytes::from(std::mem::take(&mut pg_copy_buffer).into_bytes()))
            .await?;
    }
    pg_copy_sink.as_mut().finish().await?;

    info!(
        "✅ [Collection: {}] PostgreSQL md5 rows completed. Rows: {}",
        table_name, pg_row_count
    );
    log_oom_probe("pg_stream_done", table_name, mongo_row_count, pg_row_count);

    info!(
        "🔄 [Collection: {}] Starting aggregate checksum. Create index & analyze pg & mongo temp tables (this may take a while).",
        table_name
    );

    await_pg_with_heartbeat(
        pg_write_client.execute(
            &format!("CREATE INDEX ON {} (md5) include (values)", temp_mongo_ident),
            &[],
        ),
        table_name,
        "create_mongo_temp_index_wait",
        mongo_row_count,
        pg_row_count,
        PG_QUERY_HEARTBEAT_SECS,
    )
    .await
    .context("Failed to create md5 index for MongoDB temp hashes")?;
    info!("Created index for MongoDB temp hashes. Now creating index for PostgreSQL temp hashes...");
    await_pg_with_heartbeat(
        pg_write_client.execute(
            &format!("CREATE INDEX ON {} (md5) include (values)", temp_pg_ident),
            &[],
        ),
        table_name,
        "create_pg_temp_index_wait",
        mongo_row_count,
        pg_row_count,
        PG_QUERY_HEARTBEAT_SECS,
    )
    .await
    .context("Failed to create md5 index for PostgreSQL temp hashes")?;
    info!("Created index for PostgreSQL temp hashes. Now analyzing both temp tables...");
    await_pg_with_heartbeat(
        pg_write_client.execute(
            &format!("ANALYZE {}", temp_mongo_ident),
            &[],
        ),
        table_name,
        "analyze_mongo_temp_wait",
        mongo_row_count,
        pg_row_count,
        PG_QUERY_HEARTBEAT_SECS,
    )
    .await
    .context("Failed to analyze mongo temp hashes")?;
    info!("Analyzed MongoDB temp hashes. Now analyzing PostgreSQL temp hashes...");
    await_pg_with_heartbeat(
        pg_write_client.execute(
            &format!("ANALYZE {}", temp_pg_ident),
            &[],
        ),
        table_name,
        "analyze_pg_temp_wait",
        mongo_row_count,
        pg_row_count,
        PG_QUERY_HEARTBEAT_SECS,
    )
    .await
    .context("Failed to analyze pg temp hashes")?;
    info!("Analyzed PostgreSQL temp hashes. Now performing aggregate checksum queries...");
    // 3. Complete internal sorted aggregation calculations using string_agg entirely inside PG
    let mongo_agg_row = await_pg_with_heartbeat(
        pg_write_client.query_one(
            &format!(
                "SELECT COALESCE(md5(string_agg(md5, '' ORDER BY md5)), md5('')) FROM {}",
                temp_mongo_ident
            ),
            &[],
        ),
        table_name,
        "aggregate_mongo_wait",
        mongo_row_count,
        pg_row_count,
        PG_QUERY_HEARTBEAT_SECS,
    )
    .await?;
    let final_mongo_md5: String = mongo_agg_row.get(0);
    let pg_agg_row = await_pg_with_heartbeat(
        pg_write_client.query_one(
            &format!(
                "SELECT COALESCE(md5(string_agg(md5, '' ORDER BY md5)), md5('')) FROM {}",
                temp_pg_ident
            ),
            &[],
        ),
        table_name,
        "aggregate_pg_wait",
        mongo_row_count,
        pg_row_count,
        PG_QUERY_HEARTBEAT_SECS,
    )
    .await?;
    let final_pg_md5: String = pg_agg_row.get(0);

    let matches = final_mongo_md5 == final_pg_md5;
    let mut mismatches = None;
    
    if matches {
        info!(
            "✅ [Collection: {} Table: {}] Aggregate checksum completed (matches={})",
            coll_name, table_name, matches
        );
    } else {
        warn!(
            "⚠️ [Collection: {} Table: {}] Checksum mismatch detected! MongoDB md5={} vs PostgreSQL md5={}, retrieving first 5 row differences for inspection...",
            coll_name, table_name, final_mongo_md5, final_pg_md5
        );
    }

    // --- STEP 5: If datasets diverge, find first 5 rows that differ, matched by id ---
    if !matches {
        // Three cases joined by id:
        //   1. Same id, different hash -> data was modified
        //   2. id in Mongo only        -> row missing from PostgreSQL
        //   3. id in PG only           -> row missing from MongoDB
        let diff_query = format!(
            "WITH m_ord AS (
                SELECT md5, values, ROW_NUMBER() OVER (ORDER BY values) AS rn
                FROM {mongo}
             ),
             p_ord AS (
                SELECT md5, values, ROW_NUMBER() OVER (ORDER BY values) AS rn
                FROM {pg}
             )
             SELECT
                m_ord.md5  AS mongo_md5,
                p_ord.md5  AS pg_md5,
                m_ord.values AS mongo_vals,
                p_ord.values AS pg_vals
             FROM m_ord
             FULL OUTER JOIN p_ord ON m_ord.rn = p_ord.rn
             WHERE m_ord.md5 IS DISTINCT FROM p_ord.md5
                OR m_ord.values IS DISTINCT FROM p_ord.values
             ORDER BY COALESCE(m_ord.rn, p_ord.rn)
             LIMIT 5",
            mongo = temp_mongo_ident,
            pg = temp_pg_ident,
        );

        let diff_rows = await_pg_with_heartbeat(
            pg_write_client.query(&diff_query, &[]),
            table_name,
            "mismatch_diff_wait",
            mongo_row_count,
            pg_row_count,
            PG_QUERY_HEARTBEAT_SECS,
        )
        .await?;
        let mut mongo_only = Vec::new();
        let mut pg_only = Vec::new();

        for row in diff_rows {
            let m_md5: Option<String> = row.get("mongo_md5");
            let p_md5: Option<String> = row.get("pg_md5");
            let mongo_vals: Option<String> = row.get("mongo_vals");
            let pg_vals: Option<String> = row.get("pg_vals");

            let mongo_split: Option<Vec<String>> =
                mongo_vals.as_ref().map(|s| s.split('|').map(String::from).collect());
            let pg_split: Option<Vec<String>> =
                pg_vals.as_ref().map(|s| s.split('|').map(String::from).collect());

            let (mongo_deltas, pg_deltas) = build_row_deltas(
                mongo_split.as_deref(),
                pg_split.as_deref(),
                target_fields,
            );

            mongo_only.push(RowSnapshot { md5: m_md5.unwrap_or_default(), delta: mongo_deltas });
            pg_only.push(RowSnapshot { md5: p_md5.unwrap_or_default(), delta: pg_deltas });
        }

        mismatches = Some(MismatchDelta { mongo_only, pg_only });
    }
    //--- STEP 5: Clean up scratchpad tables explicitly ---
    pg_write_client
        .execute(&format!("DROP TABLE {}", temp_mongo_ident), &[])
        .await?;
    pg_write_client
        .execute(&format!("DROP TABLE {}", temp_pg_ident), &[])
        .await?;
    log_oom_probe("cleanup_done", table_name, mongo_row_count, pg_row_count);

    Ok(CollectionChecksumResult {
        mongo_md5: final_mongo_md5,
        pg_md5: final_pg_md5,
        matches,
        mismatches,
    })
}

fn pg_select_query_unordered(
    schema_name: Option<&str>,
    table_name: &str,
    target_fields: &[String],
    order_by_field: Option<&str>,
    equality_filter: Option<(&str, &str)>,
    resume_after: Option<(&str, &str)>,
) -> String {
    fn quote_literal(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }

    let select_list = target_fields
        .iter()
        .map(|field| {
            let quoted = quote_ident(field);
            format!("COALESCE(to_json({quoted})::text, 'null') AS {quoted}")
        })
        .collect::<Vec<_>>()
        .join(", ");

    let qualified_table = match schema_name {
        Some(schema_name) => format!("{}.{}", quote_ident(schema_name), quote_ident(table_name)),
        None => quote_ident(table_name),
    };

    let mut predicates: Vec<String> = Vec::new();
    if let Some((field, value)) = equality_filter {
        predicates.push(format!("{} = {}", quote_ident(field), quote_literal(value)));
    }
    if let Some((field, value)) = resume_after {
        predicates.push(format!(
            "COALESCE(to_json({})::text, 'null') > {}",
            quote_ident(field),
            quote_literal(value)
        ));
    }
    let where_clause = if predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", predicates.join(" AND "))
    };

    match order_by_field {
        Some(field) if resume_after.is_some() => format!(
            "SELECT {select_list} FROM {qualified_table}{where_clause} ORDER BY COALESCE(to_json({})::text, 'null')",
            quote_ident(field)
        ),
        Some(field) => format!(
            "SELECT {select_list} FROM {qualified_table}{where_clause} ORDER BY {}",
            quote_ident(field)
        ),
        None => format!(
            "SELECT {select_list} FROM {qualified_table}{where_clause}"
        ),
    }
}

fn grouped_key_filter_for_target(target: &MappingTarget) -> Option<(String, String)> {
    let key_mapping = target
        .mapping_yaml
        .pg_mapping
        .columns
        .iter()
        .find(|column| column.target_field.eq_ignore_ascii_case("_key"))?;

    if let Some(value) = key_mapping
        .literal_value
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Some(("_key".to_owned(), value.to_owned()));
    }

    let table_name = target.mapping_yaml.pg_mapping.table_name.as_str();
    target
        .source_collection
        .strip_prefix(table_name)
        .and_then(|rest| rest.strip_prefix('_'))
        .filter(|suffix| !suffix.is_empty())
        .map(|suffix| ("_key".to_owned(), suffix.to_owned()))
}

fn comparable_md5_columns(columns: &[MappingColumnYaml]) -> Vec<&MappingColumnYaml> {
    columns
        .iter()
        .filter(|column| {
            !column.source_field.trim().is_empty() && !column.target_field.trim().is_empty()
        })
        .collect()
}

fn md5_hex_from_fragments<I, S>(fragments: I) -> String
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut concat = String::new();
    for fragment in fragments {
        concat.push_str(fragment.as_ref());
    }
    format!("{:x}", md5::compute(concat.as_bytes()))
}

fn bson_to_comparable_json(value: &Bson) -> serde_json::Value {
    match value {
        Bson::Double(v) => serde_json::json!(v),
        Bson::String(v) => serde_json::Value::String(v.clone()),
        Bson::Array(values) => serde_json::Value::Array(
            values
                .iter()
                .map(bson_to_comparable_json)
                .collect::<Vec<_>>(),
        ),
        Bson::Document(doc) => serde_json::Value::Object(
            doc.iter()
                .map(|(key, value)| (key.clone(), bson_to_comparable_json(value)))
                .collect(),
        ),
        Bson::Boolean(v) => serde_json::json!(v),
        Bson::Null | Bson::Undefined => serde_json::Value::Null,
        Bson::Int32(v) => serde_json::json!(v),
        Bson::Int64(v) => serde_json::json!(v),
        Bson::ObjectId(v) => serde_json::Value::String(v.to_hex()),
        Bson::DateTime(v) => {
            serde_json::Value::String(v.try_to_rfc3339_string().unwrap_or_else(|_| v.to_string()))
        }
        //Bson::DateTime(v) => serde_json::Value::String(v.to_string()),
        Bson::Timestamp(v) => serde_json::Value::String(v.to_string()),
        Bson::Binary(v) => serde_json::Value::String(v.to_string()),
        Bson::RegularExpression(v) => serde_json::Value::String(v.to_string()),
        Bson::Decimal128(v) => {
            if let Ok(number) = v.to_string().parse::<f64>() {
                serde_json::json!(number)
            } else {
                serde_json::Value::String(v.to_string())
            }
        }
        other => serde_json::Value::String(other.to_string()),
    }
}

fn canonicalize_json_value(value: &serde_json::Value) -> String {
    fn strip_embedded_nuls(raw: &str) -> String {
        raw.chars().filter(|ch| *ch != '\u{0000}').collect()
    }

    fn canonicalize_json_object(map: &serde_json::Map<String, serde_json::Value>) -> String {
        format!(
            "{{{}}}",
            map.iter()
                .map(|(key, value)| format!(
                    "{}: {}",
                    serde_json::to_string(key)
                        .expect("serializing canonical JSON object key should succeed"),
                    canonicalize_json_value(value)
                ))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn canonical_geojson_geometry(value: &serde_json::Value) -> Option<serde_json::Value> {
        let obj = value.as_object()?;
        let geometry_type = obj.get("type")?.as_str()?;
        let coordinates = obj.get("coordinates")?.clone();

        // PostgreSQL geometry -> to_json may include CRS metadata and MongoDB
        // GeoJSON may include extra properties (for example is_location_exact).
        // For md5 comparison, keep only geometry identity fields.
        if matches!(
            geometry_type,
            "Point"
                | "LineString"
                | "Polygon"
                | "MultiPoint"
                | "MultiLineString"
                | "MultiPolygon"
                | "GeometryCollection"
        ) {
            return Some(serde_json::json!({
                "type": geometry_type,
                "coordinates": coordinates,
            }));
        }

        None
    }

    if let Some(geometry) = canonical_geojson_geometry(value) {
        if let serde_json::Value::Object(map) = geometry {
            return canonicalize_json_object(&map);
        }
    }

    match value {
        serde_json::Value::Null => "null".to_owned(),
        serde_json::Value::Bool(v) => v.to_string(),
        serde_json::Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                value.to_string()
            } else if let Some(value) = number.as_u64() {
                value.to_string()
            } else if let Some(value) = number.as_f64() {
                if value.is_finite() && value.fract() == 0.0 {
                    format!("{value:.0}")
                } else {
                    number.to_string()
                }
            } else {
                number.to_string()
            }
        }
        serde_json::Value::String(v) => {
            let cleaned = strip_embedded_nuls(v);
            {
                // Attempt to parse as RFC3339 / ISO8601 (e.g. "2024-01-15T00:00:00Z").
                if let Ok(dt) = cleaned.parse::<DateTime<Utc>>() {
                    let normalized_ts = dt.to_rfc3339_opts(SecondsFormat::Secs, true);
                    return serde_json::to_string(&normalized_ts)
                        .expect("serializing normalized timestamp should succeed");
                }

                // Fallback: MongoDB may store dates as "Fri Apr 03 11:15:02 UTC 2009".
                if let Ok(naive) =
                    chrono::NaiveDateTime::parse_from_str(&cleaned, "%a %b %d %H:%M:%S UTC %Y")
                {
                    let dt = naive.and_utc();
                    let normalized_ts = dt.to_rfc3339_opts(SecondsFormat::Secs, true);
                    return serde_json::to_string(&normalized_ts)
                        .expect("serializing normalized timestamp should succeed");
                }

                // Not a timestamp — treat as a regular string.
                serde_json::to_string(&cleaned)
                    .expect("serializing canonical JSON string should succeed")
            }
        }
        serde_json::Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(canonicalize_json_value)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        serde_json::Value::Object(map) => canonicalize_json_object(map),
    }
}

fn normalize_json_literal(literal: &str) -> String {
    serde_json::from_str::<serde_json::Value>(literal)
        .map(|value| canonicalize_json_value(&value))
        .unwrap_or_else(|_| literal.to_owned())
}

fn is_textual_pg_type(data_type: Option<&str>) -> bool {
    matches!(
        data_type.map(|value| value.trim().to_ascii_lowercase()),
        Some(data_type)
            if matches!(
                data_type.as_str(),
                "text"
                    | "varchar"
                    | "character varying"
                    | "char"
                    | "character"
                    | "bpchar"
                    | "citext"
            )
    )
}

fn mongo_source_scalar_families(
    fields: &indexmap::IndexMap<String, FieldSchema>,
    source_field: &str,
) -> HashSet<String> {
    fn is_null_type_name(type_name: &str) -> bool {
        matches!(type_name, "Null" | "Undefined")
    }

    let mut current_fields = fields;
    let mut segments = source_field
        .split('.')
        .filter(|segment| !segment.is_empty())
        .peekable();

    while let Some(segment) = segments.next() {
        let Some(field_schema) = current_fields.get(segment) else {
            return HashSet::new();
        };

        if segments.peek().is_none() {
            return field_schema
                .types
                .keys()
                .filter(|type_name| !is_null_type_name(type_name.as_str()))
                .filter_map(|type_name| scalar_type_family(type_name.as_str()))
                .map(|family| family.to_owned())
                .collect::<HashSet<_>>();
        }

        let next_fields = field_schema
            .types
            .iter()
            .filter(|(type_name, _)| !is_null_type_name(type_name.as_str()))
            .find_map(|(type_name, type_schema)| {
                if type_name == "Object" {
                    type_schema.object.as_ref()
                } else if type_name == "Array" {
                    type_schema
                        .array
                        .as_ref()
                        .and_then(|items_field| items_field.types.get("Object"))
                        .and_then(|object_schema| object_schema.object.as_ref())
                } else {
                    None
                }
            });

        let Some(next_fields) = next_fields else {
            return HashSet::new();
        };
        current_fields = next_fields;
    }

    HashSet::new()
}

fn target_type_family(data_type: Option<&str>) -> Option<&'static str> {
    let raw = data_type?.trim().to_ascii_lowercase();
    let mut cut = raw.len();
    for marker in [
        " default ",
        " primary key",
        " not null",
        " null",
        " references ",
        " check ",
        " unique",
    ] {
        if let Some(idx) = raw.find(marker) {
            cut = cut.min(idx);
        }
    }
    let raw = raw[..cut].trim().to_owned();

    if matches!(
        raw.as_str(),
        "text" | "varchar" | "character varying" | "char" | "character" | "bpchar" | "citext"
    ) || raw.starts_with("varchar(")
        || raw.starts_with("character varying(")
    {
        return Some("string");
    }

    if raw == "uuid" {
        return Some("uuid");
    }

    if raw.contains("timestamp") || raw == "date" || raw.starts_with("time") {
        return Some("datetime");
    }

    if matches!(
        raw.as_str(),
        "smallint"
            | "integer"
            | "bigint"
            | "real"
            | "double precision"
            | "numeric"
            | "decimal"
            | "serial"
            | "bigserial"
    ) {
        return Some("numeric");
    }

    if raw == "boolean" {
        return Some("boolean");
    }

    None
}


fn drop_incompatible_columns(
    schema: &CollectionSchema,
    source_path: &SourcePath,
    mapping_yaml: &mut MappingYaml,
) {
    let Some(fields) = fields_for_source_path(schema, source_path) else {
        return;
    };

    let ddl_type_by_target = mapping_yaml
        .pg_mapping
        .ddl
        .as_ref()
        .map(|ddl| {
            ddl.columns
                .iter()
                .map(|column| (column.name.clone(), column.sql_type.clone()))
                .collect::<std::collections::HashMap<_, _>>()
        })
        .unwrap_or_default();

    mapping_yaml.pg_mapping.columns.retain(|column| {
        let target_type = column.data_type.as_deref().or_else(|| {
            ddl_type_by_target
                .get(&column.target_field)
                .map(String::as_str)
        });
        let Some(target_family) = target_type_family(target_type) else {
            return true;
        };

        let source_families = mongo_source_scalar_families(&fields, &column.source_field);
        if source_families.is_empty() {
            return true;
        }

        source_families.contains(target_family)
    });
}


fn mongo_field_literal_for_type(
    doc: &Document,
    field: &str,
    target_data_type: Option<&str>,
) -> String {
    fn find_doc_value<'a>(doc: &'a Document, field: &str) -> Option<&'a Bson> {
        let (head, tail) = match field.split_once('.') {
            Some((head, tail)) => (head, Some(tail)),
            None => (field, None),
        };
        let value = doc.get(head)?;
        match tail {
            Some(rest) => match value {
                Bson::Document(child_doc) => find_doc_value(child_doc, rest),
                _ => None,
            },
            None => Some(value),
        }
    }

    let value = find_doc_value(doc, field).unwrap_or(&Bson::Null);
    let comparable = bson_to_comparable_json(value);
    let normalized = normalize_json_literal(
        &serde_json::to_string(&comparable)
            .expect("serializing MongoDB value to JSON should succeed"),
    );

    if is_textual_pg_type(target_data_type) && !matches!(comparable, serde_json::Value::Null) {
        match comparable {
            serde_json::Value::String(_) => normalized,
            _ => serde_json::to_string(&normalized)
                .expect("serializing PostgreSQL text comparison literal should succeed"),
        }
    } else {
        normalized
    }
}


fn pg_hash_values_pipe(row: &Row, target_fields: &[String]) -> String {
    target_fields
        .iter()
        .enumerate()
        .map(|(index, _)| {
            let val: Option<String> = row.get(index);
            // SQL NULL → "null" matches MongoDB Bson::Null serialization
            normalize_json_literal(val.as_deref().unwrap_or("null"))
        })
        .collect::<Vec<_>>()
        .join("|")
}


fn mongo_hash_values_pipe_for_columns(
    doc: &Document,
    source_fields: &[(String, Option<String>)],
) -> String {
    source_fields
        .iter()
        .map(|(field, target_data_type)| {
            mongo_field_literal_for_type(doc, field, target_data_type.as_deref())
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn mongo_hash_record_for_columns(
    doc: &Document,
    source_fields: &[(String, Option<String>)],
) -> HashRecord {
    let values = source_fields
        .iter()
        .map(|(field, target_data_type)| {
            mongo_field_literal_for_type(doc, field, target_data_type.as_deref())
        })
        .collect::<Vec<_>>();
    HashRecord {
        md5: md5_hex_from_fragments(values.iter()),
        values,
    }
}

fn sanitize_pg_name(name: &str) -> String {
    let s: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect();
    if s.starts_with(|c: char| c.is_ascii_digit()) {
        format!("_{s}")
    } else {
        s
    }
}

fn is_null_type(type_name: &str) -> bool {
    matches!(type_name, "Null" | "Undefined")
}

fn child_mapping_table_name(parent_name: &str, field: &str) -> String {
    let field = sanitize_pg_name(field);
    let ancestor_segments = parent_name.split('_').collect::<Vec<_>>();
    if ancestor_segments.iter().any(|segment| *segment == field) {
        let parent_segment = ancestor_segments.last().copied().unwrap_or(parent_name);
        format!("{parent_segment}_{field}")
    } else {
        field
    }
}

fn build_mapping_source_paths(
    collection: &str,
    schema: &CollectionSchema,
) -> HashMap<String, SourcePath> {
    fn visit_fields(
        parent_table_name: &str,
        path_prefix: &[String],
        fields: &indexmap::IndexMap<String, FieldSchema>,
        active_grouped_fields: Option<&[String]>,
        out: &mut HashMap<String, SourcePath>,
    ) {
        let grouped_root_fields = if path_prefix.is_empty() && active_grouped_fields.is_none() {
            grouped_root_array_object_fields(fields)
        } else {
            Vec::new()
        };
        let grouped_representatives = grouped_root_fields
            .iter()
            .map(|group| (group.representative.clone(), group))
            .collect::<HashMap<_, _>>();
        let grouped_members = grouped_root_fields
            .iter()
            .flat_map(|group| group.members.iter().cloned())
            .collect::<std::collections::HashSet<_>>();

        for (raw_name, field) in fields {
            if let Some(group) = grouped_representatives.get(raw_name) {
                let child_table = child_mapping_table_name(parent_table_name, raw_name);
                out.insert(
                    child_table.clone(),
                    SourcePath {
                        path: path_prefix.to_vec(),
                        scalar_array_field: None,
                        grouped_fields: Some(group.members.clone()),
                    },
                );
                visit_fields(
                    &child_table,
                    &[],
                    &group.child_fields,
                    Some(group.members.as_slice()),
                    out,
                );
                continue;
            }
            if grouped_members.contains(raw_name) {
                continue;
            }

            let non_null: Vec<(&str, &TypeSchema)> = field
                .types
                .iter()
                .filter(|(type_name, _)| !is_null_type(type_name.as_str()))
                .map(|(type_name, type_schema)| (type_name.as_str(), type_schema))
                .collect();

            if non_null.len() == 1 && non_null[0].0 == "Object" {
                let type_schema = non_null[0].1;
                if type_schema.as_jsonb {
                    continue;
                }
                if let Some(sub_fields) = &type_schema.object {
                    let child_table = child_mapping_table_name(parent_table_name, raw_name);
                    let mut child_path = path_prefix.to_vec();
                    child_path.push(raw_name.clone());
                    out.insert(
                        child_table.clone(),
                        SourcePath {
                            path: child_path.clone(),
                            scalar_array_field: None,
                            grouped_fields: active_grouped_fields.map(|fields| fields.to_vec()),
                        },
                    );
                    visit_fields(
                        &child_table,
                        &child_path,
                        sub_fields,
                        active_grouped_fields,
                        out,
                    );
                }
                continue;
            }

            if non_null.len() == 1 && non_null[0].0 == "Array" {
                let type_schema = non_null[0].1;
                if let Some(items_field) = &type_schema.array {
                    if let Some(object_schema) = items_field.types.get("Object") {
                        if let Some(sub_fields) = &object_schema.object {
                            let child_table = child_mapping_table_name(parent_table_name, raw_name);
                            let mut child_path = path_prefix.to_vec();
                            child_path.push(raw_name.clone());
                            out.insert(
                                child_table.clone(),
                                SourcePath {
                                    path: child_path.clone(),
                                    scalar_array_field: None,
                                    grouped_fields: active_grouped_fields
                                        .map(|fields| fields.to_vec()),
                                },
                            );
                            visit_fields(
                                &child_table,
                                &child_path,
                                sub_fields,
                                active_grouped_fields,
                                out,
                            );
                        }
                    } else {
                        let child_table = child_mapping_table_name(parent_table_name, raw_name);
                        let mut child_path = path_prefix.to_vec();
                        child_path.push(raw_name.clone());
                        out.insert(
                            child_table,
                            SourcePath {
                                path: child_path,
                                scalar_array_field: Some(raw_name.clone()),
                                grouped_fields: active_grouped_fields.map(|fields| fields.to_vec()),
                            },
                        );
                    }
                }
            }
        }
    }

    let root_table = sanitize_pg_name(collection);
    let mut out = HashMap::new();
    if let Some(group) = flatten_grouped_root_array_object_fields(schema) {
        out.insert(
            root_table.clone(),
            SourcePath {
                path: Vec::new(),
                scalar_array_field: None,
                grouped_fields: Some(group.members.clone()),
            },
        );
        visit_fields(
            &root_table,
            &[],
            &group.child_fields,
            Some(group.members.as_slice()),
            &mut out,
        );
        return out;
    }

    if let Some((field_name, array_field)) = flatten_root_array_object_field(schema) {
        let item_fields = array_field
            .types
            .get("Array")
            .and_then(|type_schema| type_schema.array.as_ref())
            .and_then(|items_field| items_field.types.get("Object"))
            .and_then(|type_schema| type_schema.object.as_ref());
        if let Some(item_fields) = item_fields {
            let prefix = vec![field_name.to_owned()];
            out.insert(
                root_table.clone(),
                SourcePath {
                    path: prefix.clone(),
                    scalar_array_field: None,
                    grouped_fields: None,
                },
            );
            visit_fields(&root_table, &prefix, item_fields, None, &mut out);
            return out;
        }
    }

    out.insert(
        root_table.clone(),
        SourcePath {
            path: Vec::new(),
            scalar_array_field: None,
            grouped_fields: None,
        },
    );
    visit_fields(&root_table, &[], &schema.object, None, &mut out);
    out
}

fn extract_source_documents(doc: &Document, source_path: &SourcePath) -> Vec<Document> {
    fn is_hex_keyed_name(name: &str) -> bool {
        name.len() >= 8 && name.chars().all(|ch| ch.is_ascii_hexdigit())
    }

    fn is_uuid_keyed_name(name: &str) -> bool {
        let parts = name.split('-').collect::<Vec<_>>();
        parts.len() == 5
            && [8_usize, 4, 4, 4, 12]
                .iter()
                .zip(parts.iter())
                .all(|(expected_len, part)| {
                    part.len() == *expected_len && part.chars().all(|ch| ch.is_ascii_hexdigit())
                })
    }

    fn extract_map_entry_document(value: &Bson) -> Option<Document> {
        match value {
            Bson::Document(child_doc) if !child_doc.is_empty() => Some(child_doc.clone()),
            Bson::String(raw) => serde_json::from_str::<serde_json::Value>(raw)
                .ok()
                .and_then(|parsed| bson::to_document(&parsed).ok())
                .filter(|child_doc| !child_doc.is_empty()),
            _ => None,
        }
    }

    fn expand_dynamic_map_documents(
        doc: &Document,
        root_id: Option<&Bson>,
    ) -> Option<Vec<Document>> {
        if doc.is_empty() {
            return Some(Vec::new());
        }

        let keys = doc.keys().collect::<Vec<_>>();
        if !keys
            .iter()
            .all(|key| is_uuid_keyed_name(key) || is_hex_keyed_name(key))
        {
            return None;
        }

        let mut expanded = Vec::new();
        for value in doc.values() {
            let mut child_doc = extract_map_entry_document(value)?;
            if !child_doc.contains_key("_id") {
                if let Some(root_id) = root_id {
                    child_doc.insert("_id", root_id.clone());
                }
            }
            expanded.push(child_doc);
        }

        Some(expanded)
    }

    fn walk(doc: &Document, source_path: &SourcePath, root_id: Option<&Bson>) -> Vec<Document> {
        if let Some(grouped_fields) = &source_path.grouped_fields {
            let grouped_docs = grouped_fields
                .iter()
                .flat_map(|field_name| match doc.get(field_name) {
                    Some(Bson::Array(items)) => items
                        .iter()
                        .filter_map(|item| match item {
                            Bson::Document(child_doc) => {
                                if child_doc.is_empty() {
                                    return None;
                                }
                                let mut cloned = child_doc.clone();
                                cloned.insert("key", field_name.clone());
                                if let Some(root_id) = root_id {
                                    cloned.insert("_id", root_id.clone());
                                }
                                Some(cloned)
                            }
                            _ => None,
                        })
                        .collect::<Vec<_>>(),
                    Some(Bson::Document(child_doc)) => {
                        if child_doc.is_empty() {
                            return Vec::new();
                        }
                        let mut cloned = child_doc.clone();
                        cloned.insert("key", field_name.clone());
                        if let Some(root_id) = root_id {
                            cloned.insert("_id", root_id.clone());
                        }
                        vec![cloned]
                    }
                    _ => Vec::new(),
                })
                .collect::<Vec<_>>();

            if source_path.path.is_empty() {
                return grouped_docs;
            }

            return grouped_docs
                .iter()
                .flat_map(|grouped_doc| {
                    walk(
                        grouped_doc,
                        &SourcePath {
                            path: source_path.path.clone(),
                            scalar_array_field: source_path.scalar_array_field.clone(),
                            grouped_fields: None,
                        },
                        grouped_doc.get("_id").or(root_id),
                    )
                })
                .collect();
        }

        if source_path.path.is_empty() {
            if let Some(expanded) = expand_dynamic_map_documents(doc, root_id) {
                return expanded;
            }
            if doc.is_empty() {
                return Vec::new();
            }
            let mut cloned = doc.clone();
            if !cloned.contains_key("_id") {
                if let Some(root_id) = root_id {
                    cloned.insert("_id", root_id.clone());
                }
            }
            return vec![cloned];
        }

        let field_name = &source_path.path[0];
        let remaining = source_path.path[1..].to_vec();
        match doc.get(field_name) {
            Some(Bson::Document(child_doc)) => walk(
                child_doc,
                &SourcePath {
                    path: remaining,
                    scalar_array_field: source_path.scalar_array_field.clone(),
                    grouped_fields: None,
                },
                root_id,
            ),
            Some(Bson::Array(items)) => items
                .iter()
                .filter_map(|item| match item {
                    Bson::Document(child_doc) => Some(walk(
                        child_doc,
                        &SourcePath {
                            path: remaining.clone(),
                            scalar_array_field: source_path.scalar_array_field.clone(),
                            grouped_fields: None,
                        },
                        root_id,
                    )),
                    _ if remaining.is_empty() => {
                        source_path.scalar_array_field.as_ref().map(|field| {
                            let mut synthetic = Document::new();
                            synthetic.insert(field.clone(), item.clone());
                            if let Some(root_id) = root_id {
                                synthetic.insert("_id", root_id.clone());
                            }
                            vec![synthetic]
                        })
                    }
                    _ => None,
                })
                .flatten()
                .collect(),
            _ => Vec::new(),
        }
    }

    walk(doc, source_path, doc.get("_id"))
}

// fn sort_hash_records(records: &mut [HashRecord]) {
//     records.sort_by(|left, right| {
//         left.values
//             .cmp(&right.values)
//             .then(left.md5.cmp(&right.md5))
//     });
// }

fn read_mapping_yaml(path: &Path) -> Result<MappingYaml> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("Failed to open mapping YAML: {}", path.display()))?;
    serde_yaml::from_reader(file).with_context(|| format!("Failed to parse {}", path.display()))
}

fn fields_for_source_path(
    schema: &CollectionSchema,
    source_path: &SourcePath,
) -> Option<indexmap::IndexMap<String, FieldSchema>> {
    fn descend(
        fields: &indexmap::IndexMap<String, FieldSchema>,
        path: &[String],
    ) -> Option<indexmap::IndexMap<String, FieldSchema>> {
        if path.is_empty() {
            return Some(fields.clone());
        }

        let field = fields.get(&path[0])?;
        let type_schema = field
            .types
            .iter()
            .find(|(type_name, _)| !is_null_type(type_name.as_str()))?
            .1;

        if let Some(object_fields) = type_schema.object.as_ref() {
            return descend(object_fields, &path[1..]);
        }

        if let Some(array_items) = type_schema.array.as_ref() {
            if let Some(object_schema) = array_items.types.get("Object") {
                if let Some(object_fields) = object_schema.object.as_ref() {
                    return descend(object_fields, &path[1..]);
                }
            }
        }

        None
    }

    if let Some(grouped_fields) = source_path.grouped_fields.as_ref() {
        let group = grouped_root_array_object_fields(&schema.object)
            .into_iter()
            .find(|group| &group.members == grouped_fields)?;
        return descend(&group.child_fields, &source_path.path);
    }

    descend(&schema.object, &source_path.path)
}

fn find_source_field_for_column(
    fields: &indexmap::IndexMap<String, FieldSchema>,
    column_name: &str,
    is_root: bool,
) -> Option<String> {
    fn reserved_inline_sibling_names(
        fields: &indexmap::IndexMap<String, FieldSchema>,
        current_raw_name: &str,
        is_root: bool,
    ) -> std::collections::HashSet<String> {
        let mut reserved = std::collections::HashSet::new();

        for (raw_name, field) in fields {
            if raw_name == current_raw_name {
                continue;
            }

            let non_null = field
                .types
                .iter()
                .filter(|(type_name, _)| !is_null_type(type_name.as_str()))
                .collect::<Vec<_>>();
            if non_null.is_empty() {
                continue;
            }

            if raw_name == "_id"
                && is_root
                && non_null.len() == 1
                && non_null[0].0.as_str() == "Object"
            {
                if let Some(sub_fields) = non_null[0].1.object.as_ref() {
                    for (path, _) in inline_object_leaf_fields_with_prefix(sub_fields, &[]) {
                        reserved.insert(sanitize_pg_name(&path.join("_")));
                    }
                }
                continue;
            }

            if non_null.len() == 1 && non_null[0].0.as_str() == "Object" {
                if let Some(sub_fields) = non_null[0].1.object.as_ref() {
                    if can_inline_object_fields(sub_fields) {
                        for (path, _) in inline_object_leaf_fields_with_prefix(sub_fields, &[]) {
                            if let Some(last) = path.last() {
                                reserved.insert(sanitize_pg_name(last));
                            }
                        }
                        continue;
                    }
                }
            }

            if raw_name == "_id" && is_root {
                reserved.insert("id".to_owned());
            } else {
                reserved.insert(sanitize_pg_name(raw_name));
            }
        }

        reserved
    }

    fn find_nested_source_field(
        fields: &indexmap::IndexMap<String, FieldSchema>,
        column_name: &str,
        is_root: bool,
    ) -> Option<String> {
        for (raw_name, field) in fields {
            let non_null = field
                .types
                .iter()
                .filter(|(type_name, _)| !is_null_type(type_name.as_str()))
                .collect::<Vec<_>>();
            if non_null.len() != 1 || non_null[0].0.as_str() != "Object" {
                continue;
            }
            let Some(sub_fields) = non_null[0].1.object.as_ref() else {
                continue;
            };
            if !can_inline_object_fields(sub_fields) {
                continue;
            }

            let reserved = reserved_inline_sibling_names(fields, raw_name, is_root);
            let prefix = vec![raw_name.clone()];
            let column_names =
                inline_object_column_names_with_prefix(sub_fields, &prefix, &reserved);
            for (source_field, target_field) in column_names {
                if target_field == column_name {
                    return Some(source_field);
                }
            }
        }

        None
    }

    if is_root && column_name == "id" && fields.contains_key("_id") {
        return Some("_id".to_owned());
    }

    if let Some(raw_name) = fields
        .keys()
        .find(|raw_name| sanitize_pg_name(raw_name) == column_name)
    {
        return Some(raw_name.clone());
    }

    if is_root {
        if let Some(id_object) = fields
            .get("_id")
            .and_then(|field| field.types.get("Object"))
            .and_then(|type_schema| type_schema.object.as_ref())
        {
            if let Some(raw_name) = id_object
                .keys()
                .find(|raw_name| sanitize_pg_name(raw_name) == column_name)
            {
                return Some(raw_name.clone());
            }
        }
    }

    find_nested_source_field(fields, column_name, is_root)
}

fn backfill_mapping_columns_from_schema(
    collection: &str,
    schema: &CollectionSchema,
    source_path: &SourcePath,
    mapping_yaml: &mut MappingYaml,
) {
    if !mapping_yaml.pg_mapping.columns.is_empty() {
        return;
    }

    let Some(ddl) = mapping_yaml.pg_mapping.ddl.as_ref() else {
        return;
    };

    let foreign_key_columns = ddl
        .foreign_keys
        .iter()
        .map(|fk| fk.from_col.as_str())
        .collect::<Vec<_>>();

    let mut push_column = |source_field: String, ddl_column: &DdlColumnYaml| {
        mapping_yaml.pg_mapping.columns.push(MappingColumnYaml {
            source_field,
            target_field: ddl_column.name.clone(),
            data_type: Some(ddl_column.sql_type.to_ascii_lowercase()),
            literal_value: None,
        });
    };

    if let Some(scalar_array_field) = source_path.scalar_array_field.as_ref() {
        for ddl_column in &ddl.columns {
            if ddl_column.name == "id"
                || foreign_key_columns.iter().any(|fk| *fk == ddl_column.name)
            {
                continue;
            }
            push_column(scalar_array_field.clone(), ddl_column);
        }
        return;
    }

    if let Some((array_field_name, array_field)) = flatten_root_array_object_field(schema) {
        if source_path.path == vec![array_field_name.to_owned()] {
            let parent_id_col = flattened_root_parent_id_column(collection);
            let item_fields = array_field
                .types
                .get("Array")
                .and_then(|type_schema| type_schema.array.as_ref())
                .and_then(|items_field| items_field.types.get("Object"))
                .and_then(|type_schema| type_schema.object.as_ref());
            if let Some(item_fields) = item_fields {
                for ddl_column in &ddl.columns {
                    if ddl_column.name == "id" {
                        continue;
                    }
                    if ddl_column.name == parent_id_col {
                        push_column("_id".to_owned(), ddl_column);
                        continue;
                    }
                    if let Some(source_field) =
                        find_source_field_for_column(item_fields, &ddl_column.name, false)
                    {
                        push_column(source_field, ddl_column);
                    }
                }
            }
            return;
        }
    }

    if let Some(group) = flatten_grouped_root_array_object_fields(schema) {
        if source_path.path.is_empty()
            && source_path.grouped_fields.as_ref() == Some(&group.members)
        {
            let parent_id_col = flattened_root_parent_id_column(collection);
            for ddl_column in &ddl.columns {
                if ddl_column.name == "id" {
                    continue;
                }
                if ddl_column.name == parent_id_col {
                    push_column("_id".to_owned(), ddl_column);
                    continue;
                }
                if ddl_column.name == "key" {
                    push_column("key".to_owned(), ddl_column);
                    continue;
                }
                if let Some(source_field) = group
                    .child_fields
                    .keys()
                    .find(|raw_name| sanitize_pg_name(raw_name) == ddl_column.name)
                {
                    push_column(source_field.clone(), ddl_column);
                }
            }
            return;
        }
    }

    let Some(fields) = fields_for_source_path(schema, source_path) else {
        return;
    };

    for ddl_column in &ddl.columns {
        if foreign_key_columns.iter().any(|fk| *fk == ddl_column.name) {
            continue;
        }

        if source_path.path.is_empty() && ddl_column.name == "id" && fields.contains_key("_id") {
            push_column("_id".to_owned(), ddl_column);
            continue;
        }

        if !source_path.path.is_empty() && ddl_column.name == "id" {
            continue;
        }

        if let Some(source_field) =
            find_source_field_for_column(&fields, &ddl_column.name, source_path.path.is_empty())
        {
            push_column(source_field.clone(), ddl_column);
        }
    }
}

fn collection_paths_from_conf(
    conf: &crate::util::ConfData,
    collections_root_override: Option<&Path>,
) -> Result<(String, PathBuf)> {
    let namespace = conf
        .namespace
        .as_ref()
        .ok_or_else(|| anyhow!("NAMESPACE not found in config"))?;
    let db_name = namespace
        .split_once('.')
        .map(|(db_name, _)| db_name)
        .unwrap_or(namespace)
        .to_owned();
    let collections_root = collections_root_override
        .map(|path| path.to_path_buf())
        .unwrap_or_else(|| {
            crate::util::configured_project_root(conf)
                .join("source")
                .join("collections")
        });
    let collections_dir = if collections_root.join(&db_name).is_dir() {
        collections_root.join(&db_name)
    } else {
        collections_root
    };
    Ok((db_name, collections_dir))
}

fn discover_mapping_targets_for_collection(
    collection: &str,
    conf: &crate::util::ConfData,
    collections_root_override: Option<&Path>,
) -> Result<Vec<MappingTarget>> {
    fn list_mapping_files(dir: &Path) -> Result<Vec<PathBuf>> {
        if !dir.is_dir() {
            return Ok(Vec::new());
        }

        let mut files = std::fs::read_dir(dir)
            .with_context(|| format!("Cannot read mapping directory {}", dir.display()))?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| path.is_file())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .map(|name| name.starts_with("mapping_") && name.ends_with(".yaml"))
                    .unwrap_or(false)
            })
            .collect::<Vec<_>>();
        files.sort();
        Ok(files)
    }

    fn parse_mongo_path(path: &str) -> SourcePath {
        let trimmed = path.trim();
        let segments = if trimmed == "." || trimmed.is_empty() {
            Vec::new()
        } else {
            trimmed
                .trim_start_matches('.')
                .split('.')
                .filter(|segment| !segment.is_empty())
                .map(|segment| segment.to_owned())
                .collect::<Vec<_>>()
        };

        SourcePath {
            path: segments,
            scalar_array_field: None,
            grouped_fields: None,
        }
    }

    fn source_path_from_traversal_chain(
        table_name: &str,
        mapping_by_table: &HashMap<String, MappingYaml>,
        fallback_source_paths: &HashMap<String, SourcePath>,
        visiting: &mut HashSet<String>,
    ) -> Option<SourcePath> {
        if !visiting.insert(table_name.to_owned()) {
            return None;
        }

        let derived = mapping_by_table.get(table_name).and_then(|mapping| {
            let traversal = mapping.traversal.as_ref()?;
            match traversal.mode {
                TraversalModeYaml::Root => Some(SourcePath {
                    path: Vec::new(),
                    scalar_array_field: None,
                    grouped_fields: None,
                }),
                _ => {
                    let mut source_segments = traversal
                        .source_field
                        .as_deref()
                        .unwrap_or_default()
                        .split('.')
                        .filter(|segment| !segment.trim().is_empty())
                        .map(|segment| segment.to_owned())
                        .collect::<Vec<_>>();
                    if source_segments.is_empty() {
                        return None;
                    }

                    let mut parent_path = traversal.parent_table.as_deref().and_then(|parent| {
                        source_path_from_traversal_chain(
                            parent,
                            mapping_by_table,
                            fallback_source_paths,
                            visiting,
                        )
                        .or_else(|| fallback_source_paths.get(parent).cloned())
                    });

                    if parent_path.is_none() {
                        parent_path = Some(SourcePath {
                            path: Vec::new(),
                            scalar_array_field: None,
                            grouped_fields: None,
                        });
                    }

                    parent_path.map(|mut parent| {
                        parent.path.append(&mut source_segments);
                        parent.scalar_array_field = None;
                        parent.grouped_fields = None;
                        parent
                    })
                }
            }
        });

        visiting.remove(table_name);
        derived
    }

    let (_, collections_dir) = collection_paths_from_conf(conf, collections_root_override)?;
    let safe_collection_name = collection.replace('/', "_");
    let member_dir = collections_dir.join(&safe_collection_name);
    let mut coll_dir = member_dir.clone();
    let mut schema_path = member_dir.join(format!("{safe_collection_name}.json"));
    let mut grouped_lookup: Option<String> = None;

    if !schema_path.is_file() {
        if let Some(lookup) = resolve_grouped_sql_lookup_name(&collections_dir, collection) {
            grouped_lookup = Some(lookup.clone());
            let grouped_dir = collections_dir.join(&lookup);
            let grouped_schema = grouped_dir.join(format!("{lookup}.json"));
            if grouped_schema.is_file() {
                schema_path = grouped_schema;
            }
        }

        if !schema_path.is_file() {
            if let Some((group_prefix, _)) = collection.split_once('_') {
                let safe_group_prefix = group_prefix.replace('/', "_");
                let grouped_dir = collections_dir.join(&safe_group_prefix);
                let grouped_schema = grouped_dir.join(format!("{safe_group_prefix}.json"));
                if grouped_schema.is_file() {
                    schema_path = grouped_schema;
                }
            }
        }
    }

    let mut mapping_files = list_mapping_files(&member_dir)?;
    if mapping_files.is_empty() {
        if let Some(lookup) = grouped_lookup.as_deref() {
            let grouped_dir = collections_dir.join(lookup);
            let grouped_files = list_mapping_files(&grouped_dir)?;
            if !grouped_files.is_empty() {
                coll_dir = grouped_dir;
                mapping_files = grouped_files;
            }
        }
    }

    if mapping_files.is_empty() {
        if let Some((group_prefix, _)) = collection.split_once('_') {
            let safe_group_prefix = group_prefix.replace('/', "_");
            let grouped_dir = collections_dir.join(&safe_group_prefix);
            let grouped_files = list_mapping_files(&grouped_dir)?;
            if !grouped_files.is_empty() {
                coll_dir = grouped_dir;
                mapping_files = grouped_files;
            }
        }
    }

    if mapping_files.is_empty() {
        return Err(anyhow!(
            "No mapping files found for collection {} (checked {})",
            collection,
            coll_dir.display()
        ));
    }

    let schema: CollectionSchema = serde_json::from_str(
        &std::fs::read_to_string(&schema_path)
            .with_context(|| format!("Failed to read {}", schema_path.display()))?,
    )
    .with_context(|| format!("Failed to parse {}", schema_path.display()))?;
    let source_paths = build_mapping_source_paths(collection, &schema);

    let mut parsed_mappings = mapping_files
        .into_iter()
        .map(|mapping_path| {
            let mapping_yaml = read_mapping_yaml(&mapping_path)?;
            Ok((mapping_path, mapping_yaml))
        })
        .collect::<Result<Vec<_>>>()?;

    let mapping_by_table = parsed_mappings
        .iter()
        .map(|(_, mapping_yaml)| {
            (
                mapping_yaml.pg_mapping.table_name.clone(),
                mapping_yaml.clone(),
            )
        })
        .collect::<HashMap<_, _>>();

    let mut targets = parsed_mappings
        .drain(..)
        .map(|(mapping_path, mut mapping_yaml)| {
            let table_name = mapping_yaml.pg_mapping.table_name.clone();
            let source_path = source_path_from_traversal_chain(
                &table_name,
                &mapping_by_table,
                &source_paths,
                &mut HashSet::new(),
            )
                .or_else(|| source_paths.get(&table_name).cloned())
                .or_else(|| {
                    mapping_yaml.mongo_path.as_deref().map(|mongo_path| {
                        let parsed = parse_mongo_path(mongo_path);
                        source_paths
                            .values()
                            .find(|candidate| candidate.path == parsed.path)
                            .cloned()
                            .unwrap_or(parsed)
                    })
                })
                .ok_or_else(|| {
                    anyhow!(
                        "No MongoDB source path found for table {} in {}",
                        table_name,
                        mapping_path.display()
                    )
                })?;
            backfill_mapping_columns_from_schema(
                collection,
                &schema,
                &source_path,
                &mut mapping_yaml,
            );
            drop_incompatible_columns(&schema, &source_path, &mut mapping_yaml);
            Ok(MappingTarget {
                source_collection: collection.to_owned(),
                mapping_path,
                mapping_yaml,
                source_path,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    targets.sort_by(|left, right| {
        left.source_path
            .path
            .len()
            .cmp(&right.source_path.path.len())
            .then(
                left.mapping_yaml
                    .pg_mapping
                    .table_name
                    .cmp(&right.mapping_yaml.pg_mapping.table_name),
            )
    });
    Ok(targets)
}

// fn aggregate_md5_hexes(md5_hexes: impl IntoIterator<Item = String>) -> String {
//     md5_hex_from_fragments(md5_hexes)
// }



fn pg_hash_record(row: &Row) -> HashRecord {
    let values = (0..row.len())
        .map(|index| normalize_json_literal(&row.get::<usize, String>(index)))
        .collect::<Vec<_>>();
    HashRecord {
        md5: md5_hex_from_fragments(values.iter()),
        values,
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn pg_uri_with_database(uri: &str, database: &str) -> String {
    let (base, query) = match uri.split_once('?') {
        Some((base, query)) => (base, format!("?{query}")),
        None => (uri, String::new()),
    };
    let authority_start = uri.find("://").map(|pos| pos + 3).unwrap_or(0);
    match base[authority_start..].find('/') {
        Some(offset) => {
            let slash = authority_start + offset;
            format!("{}{database}{}", &base[..=slash], query)
        }
        None => format!("{base}/{database}{query}"),
    }
}

fn pg_sslmode(uri: &str) -> Option<&str> {
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

async fn connect_pg_client(target_uri: &str) -> Result<Client> {
    let mut tls_builder = native_tls::TlsConnector::builder();
    if matches!(pg_sslmode(target_uri), Some(mode) if mode.eq_ignore_ascii_case("require")) {
        tls_builder.danger_accept_invalid_certs(true);
        tls_builder.danger_accept_invalid_hostnames(true);
    }
    let tls = tls_builder
        .build()
        .with_context(|| "Failed to initialize PostgreSQL TLS connector")?;
    let tls = MakeTlsConnector::new(tls);

    let (pg_client, pg_connection) = tokio_postgres::connect(target_uri, tls)
        .await
        .with_context(|| "Failed to connect to PostgreSQL using TARGET_URI")?;
    tokio::spawn(async move {
        if let Err(err) = pg_connection.await {
            let message = err.to_string();
            if message.contains("error communicating with the server")
                || message.contains("connection closed")
            {
                debug!("PostgreSQL connection closed: {err}");
            } else {
                error!("PostgreSQL connection error: {err}");
            }
        }
    });

    Ok(pg_client)
}

pub async fn compute_md5_summaries_for_collection(
    collection: &str,
    config_path: &Path,
) -> Result<Vec<Md5TableSummary>> {
    compute_md5_summaries_for_collection_with_collections_root(collection, config_path, None).await
}

pub async fn compute_md5_summaries_for_collection_with_collections_root(
    collection: &str,
    config_path: &Path,
    collections_root_override: Option<&Path>,
) -> Result<Vec<Md5TableSummary>> {
    fn is_transient_md5_error(err: &anyhow::Error) -> bool {
        let message = err.to_string().to_ascii_lowercase();
        message.contains("connection closed")
            || message.contains("error communicating with the server")
            || message.contains("server monitor timeout")
            || message.contains("connection reset")
            || message.contains("broken pipe")
            || message.contains("eof")
            || message.contains("temporarily unavailable")
            || message.contains("network")
            || message.contains("interrupted")
            || message.contains("timed out")
            || message.contains("timeout")
    }

    let conf = read_conf(config_path)?;
    let targets =
        discover_mapping_targets_for_collection(collection, &conf, collections_root_override)?;
    let mongo_uri = conf
        .source_uri
        .as_ref()
        .ok_or_else(|| anyhow!("SOURCE_URI not found in config"))?;
    let (db_name, _) = collection_paths_from_conf(&conf, collections_root_override)?;
    let client_options = mongodb::options::ClientOptions::parse(mongo_uri).await?;
    let mongo_client = mongodb::Client::with_options(client_options)?;
    let mut summaries = Vec::new();

    const TARGET_MD5_RETRY_MAX: u32 = 4;

    for target in targets {
        let ddl_type_by_target = target
            .mapping_yaml
            .pg_mapping
            .ddl
            .as_ref()
            .map(|ddl| {
                ddl.columns
                    .iter()
                    .map(|column| (column.name.clone(), column.sql_type.clone()))
                    .collect::<std::collections::HashMap<_, _>>()
            })
            .unwrap_or_default();

        let md5_columns = comparable_md5_columns(&target.mapping_yaml.pg_mapping.columns);
        if md5_columns.is_empty() {
            continue;
        }

        let typed_source_fields: Vec<(String, Option<String>)> = md5_columns
            .iter()
            .map(|c| (c.source_field.clone(), c.data_type.clone()))
            .collect();
        let target_fields: Vec<String> = md5_columns.iter().map(|c| c.target_field.clone()).collect();
        let columns = md5_columns
            .iter()
            .map(|column| Md5ColumnMapping {
                source_field: column.source_field.clone(),
                source_type: column.data_type.clone(),
                target_field: column.target_field.clone(),
                target_type: ddl_type_by_target
                    .get(&column.target_field)
                    .cloned()
                    .or_else(|| column.data_type.clone()),
            })
            .collect::<Vec<_>>();

        if target_fields.is_empty() {
            return Err(anyhow!(
                "No target fields found in mapping YAML: {}",
                target.mapping_path.display()
            ));
        }

        let mongo_collection = mongo_client
            .database(&db_name)
            .collection::<bson::Document>(&target.source_collection);

        let target_uri = conf
            .target_uri
            .as_ref()
            .ok_or_else(|| anyhow!("TARGET_URI not found in config"))?;
        let target_database_name = conf
            .target_database_name
            .as_deref()
            .or(target.mapping_yaml.pg_mapping.dbname.as_deref())
            .or(target.mapping_yaml.mongo_dbname.as_deref())
            .ok_or_else(|| anyhow!("TARGET_DATABASE_NAME not found in config or mapping"))?;
        let schema_name = conf
            .target_schema
            .as_deref()
            .or(target.mapping_yaml.pg_mapping.schema_name.as_deref());
        let table_name = target.mapping_yaml.pg_mapping.table_name.clone();
        let grouped_key_filter = grouped_key_filter_for_target(&target);
        let pg_uri = pg_uri_with_database(target_uri, target_database_name);

        let mut attempt = 0_u32;
        let result = loop {
            attempt += 1;
            let compute_result: Result<CollectionChecksumResult> = async {
                let pg_read_client = connect_pg_client(&pg_uri).await?;
                let pg_write_client = connect_pg_client(&pg_uri).await?;
                compute_collection_checksums_via_temp_tables(
                    &pg_read_client,
                    Some(&pg_uri),
                    &pg_write_client,
                    &mongo_collection,
                    &typed_source_fields,
                    &target.source_path,
                    &db_name,
                    &target.source_collection,
                    schema_name,
                    &table_name,
                    &target_fields,
                    grouped_key_filter
                        .as_ref()
                        .map(|(field, value)| (field.as_str(), value.as_str())),
                )
                .await
            }
            .await;

            match compute_result {
                Ok(result) => break result,
                Err(err) => {
                    if attempt <= TARGET_MD5_RETRY_MAX && is_transient_md5_error(&err) {
                        let backoff_secs = (1_u64 << (attempt - 1)).min(15);
                        warn!(
                            "retrying md5 summary for {}.{} table={} attempt={}/{} after {}s due to transient error: {}",
                            db_name,
                            target.source_collection,
                            table_name,
                            attempt,
                            TARGET_MD5_RETRY_MAX + 1,
                            backoff_secs,
                            err
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
                        continue;
                    }
                    return Err(err);
                }
            }
        };

        let mismatches = if let Some(mismatch_delta) = result.mismatches {
            mismatch_delta
                .mongo_only
                .iter()
                .enumerate()
                .map(|(idx, m_row)| Md5MismatchRow {
                    row_index: idx + 1,
                    mongo_values: Some(m_row.delta.clone()),
                    pg_values: mismatch_delta.pg_only.get(idx).map(|row| row.delta.clone()),
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        summaries.push(Md5TableSummary {
            table_name,
            summary: Md5Summary {
                mongo_md5: result.mongo_md5,
                pg_md5: result.pg_md5,
                columns,
                mismatches,
            },
        });
    }

    Ok(summaries)
}


#[cfg(test)]
mod tests {


    fn mongo_field_literal(doc: &Document, field: &str) -> String {
        mongo_field_literal_for_type(doc, field, None)
    }

    fn mongo_hash_record(doc: &Document, source_fields: &[String]) -> HashRecord {
        let typed_source_fields = source_fields
            .iter()
            .map(|field| (field.clone(), None))
            .collect::<Vec<_>>();
        mongo_hash_record_for_columns(doc, &typed_source_fields)
    }
    fn mongo_sort_doc(source_fields: &[String]) -> Document {
        source_fields
            .iter()
            .map(|field| (field.clone(), Bson::Int32(1)))
            .collect()
    }
    use super::{
        backfill_mapping_columns_from_schema, build_mapping_source_paths,
        comparable_md5_columns, drop_incompatible_columns,
        extract_source_documents, md5_hex_from_fragments,
        mongo_field_literal_for_type,
        normalize_json_literal, HashRecord, MappingYaml, SourcePath,
    };
    use crate::analyzer::Analyzer;
    use bson::{doc, Bson};

    #[test]
    fn comparable_md5_columns_skips_blank_fields() {
        let mapping_yaml: MappingYaml = serde_yaml::from_str(
            r#"
pg_mapping:
    table_name: weather
    columns:
        - { source_field: "", target_field: pressure }
        - { source_field: pressure, target_field: "" }
        - { source_field: pressure, target_field: pressure }
"#,
        )
        .expect("mapping yaml should parse");

        let columns = comparable_md5_columns(&mapping_yaml.pg_mapping.columns);
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].source_field, "pressure");
        assert_eq!(columns[0].target_field, "pressure");
    }

    #[test]
    fn comparable_md5_columns_empty_when_no_source_target_pairs() {
        let mapping_yaml: MappingYaml = serde_yaml::from_str(
            r#"
pg_mapping:
    table_name: weather
    columns: []
"#,
        )
        .expect("mapping yaml should parse");

        let columns = comparable_md5_columns(&mapping_yaml.pg_mapping.columns);
        assert!(columns.is_empty());
    }

    #[test]
    fn backfill_mapping_columns_recovers_legacy_root_jsonb_mapping() {
        let docs = vec![doc! {
            "_id": "pg",
            "versions": [
                {"major_version": "16", "eol_date": bson::DateTime::now()}
            ]
        }];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();
        let mut mapping_yaml: MappingYaml = serde_yaml::from_str(
            r#"
pg_mapping:
  table_name: engine
  columns: []
  ddl:
    columns:
            - { name: id, sql_type: "VARCHAR(20)" }
            - { name: versions, sql_type: JSONB }
    foreign_keys: []
"#,
        )
        .expect("mapping yaml should parse");

        backfill_mapping_columns_from_schema(
            "engine",
            &schema,
            &SourcePath {
                path: Vec::new(),
                scalar_array_field: None,
                grouped_fields: None,
            },
            &mut mapping_yaml,
        );

        let columns = mapping_yaml
            .pg_mapping
            .columns
            .iter()
            .map(|column| (column.source_field.as_str(), column.target_field.as_str()))
            .collect::<Vec<_>>();
        assert!(columns.contains(&("_id", "id")));
        assert!(columns.contains(&("versions", "versions")));
    }

    #[test]
    fn backfill_mapping_columns_restores_nested_object_child_table() {
        let docs = vec![doc! {
                "_id": "project-1",
                "providers": [{
                        "namespace": "fras-t-dba-176c358",
                        "namespace_id": "fras-t-dba-176c358",
                        "provider": "aiven",
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
        let mut mapping_yaml: MappingYaml = serde_yaml::from_str(
            r#"
pg_mapping:
    table_name: projects_providers_metadata
    columns: []
    ddl:
        columns:
            - { name: id, sql_type: BIGSERIAL }
            - { name: projects_providers_id, sql_type: BIGINT }
            - { name: creation_date, sql_type: "TIMESTAMP WITH TIME ZONE" }
            - { name: status, sql_type: TEXT }
        foreign_keys:
            - { from_col: projects_providers_id }
"#,
        )
        .expect("mapping yaml should parse");
        backfill_mapping_columns_from_schema(
            "projects",
            &schema,
            &SourcePath {
                path: vec!["providers".to_owned(), "metadata".to_owned()],
                scalar_array_field: None,
                grouped_fields: None,
            },
            &mut mapping_yaml,
        );

        let columns = mapping_yaml
            .pg_mapping
            .columns
            .iter()
            .map(|column| (column.source_field.as_str(), column.target_field.as_str()))
            .collect::<Vec<_>>();
        assert!(columns.contains(&("creation_date", "creation_date")));
        assert!(columns.contains(&("status", "status")));
    }

    #[test]
    fn mongo_hash_record_uses_only_requested_fields() {
        let doc = doc! {
            "name": "alice",
            "age": 42,
            "ignored": "x"
        };
        let fields = vec!["name".to_owned(), "age".to_owned()];
        let expected_concat = format!(
            "{}{}",
            mongo_field_literal(&doc, "name"),
            mongo_field_literal(&doc, "age")
        );

        let record = mongo_hash_record(&doc, &fields);

        assert_eq!(
            record.md5,
            format!("{:x}", md5::compute(expected_concat.as_bytes()))
        );
        assert_eq!(record.values, vec!["\"alice\"", "42"]);
    }

    #[test]
    fn missing_mongo_field_hashes_as_null_literal() {
        let doc = doc! { "name": "alice" };

        assert_eq!(mongo_field_literal(&doc, "missing"), "null");
    }

    #[test]
    fn normalize_json_literal_coalesces_integral_numbers() {
        assert_eq!(normalize_json_literal("647.0"), "647");
        assert_eq!(normalize_json_literal("0.0"), "0");
        assert_eq!(
            normalize_json_literal("[4, 647.0, \"business-16\", 16, 1050, \"3 nodes cluster\"]"),
            "[4, 647, \"business-16\", 16, 1050, \"3 nodes cluster\"]"
        );
    }

    #[test]
    fn normalize_json_literal_preserves_json_strings() {
        assert_eq!(normalize_json_literal("\"647.0\""), "\"647.0\"");
    }

    #[test]
    fn normalize_json_literal_canonicalizes_geojson_geometry_fields() {
        let mongodb_geo = r#"{"type": "Point", "coordinates": [151.12236, -33.88839], "is_location_exact": false}"#;
        let postgres_geo = r#"{"type": "Point", "crs": {"type": "name", "properties": {"name": "EPSG:4326"}}, "coordinates": [151.12236, -33.88839]}"#;

        assert_eq!(
            normalize_json_literal(mongodb_geo),
            normalize_json_literal(postgres_geo)
        );
    }

    #[test]
    fn mongo_field_literal_coerces_numeric_values_for_text_targets() {
        let doc = doc! { "monthly_gain": 0 };

        assert_eq!(
            mongo_field_literal_for_type(&doc, "monthly_gain", Some("text")),
            "\"0\""
        );
    }

    #[test]
    fn mongo_field_literal_does_not_double_encode_string_values_for_text_targets() {
        let doc = doc! { "status": "OK", "object_id": bson::oid::ObjectId::parse_str("638f6313d082904d34928256").unwrap() };

        assert_eq!(
            mongo_field_literal_for_type(&doc, "status", Some("text")),
            "\"OK\""
        );
        assert_eq!(
            mongo_field_literal_for_type(&doc, "object_id", Some("text")),
            "\"638f6313d082904d34928256\""
        );
    }

    #[test]
    fn md5_from_fragments_concatenates_in_order() {
        let digest = md5_hex_from_fragments(["\"alice\"", "42", "null"]);

        assert_eq!(
            digest,
            format!("{:x}", md5::compute("\"alice\"42null".as_bytes()))
        );
    }


    #[test]
    fn mongo_sort_doc_uses_all_source_fields_in_order() {
        let sort = mongo_sort_doc(&[
            "id".to_owned(),
            "last_update".to_owned(),
            "region".to_owned(),
        ]);

        let entries = sort.into_iter().collect::<Vec<_>>();
        assert_eq!(
            entries,
            vec![
                ("id".to_owned(), Bson::Int32(1)),
                ("last_update".to_owned(), Bson::Int32(1)),
                ("region".to_owned(), Bson::Int32(1)),
            ]
        );
    }

    #[test]
    fn build_mapping_source_paths_tracks_nested_tables() {
        let docs = vec![doc! {
            "_id": 1,
            "advices": [{
                "advice": "oversized",
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

        let source_paths = build_mapping_source_paths("advisors", &schema);

        assert_eq!(
            source_paths.get("advisors"),
            Some(&SourcePath {
                path: vec!["advices".to_owned()],
                scalar_array_field: None,
                grouped_fields: None,
            })
        );
        assert_eq!(source_paths.get("advices"), None);
        assert_eq!(
            source_paths.get("earnings"),
            Some(&SourcePath {
                path: vec!["advices".to_owned(), "earnings".to_owned()],
                scalar_array_field: None,
                grouped_fields: None,
            })
        );
    }

    #[test]
    fn build_mapping_source_paths_tracks_scalar_array_tables() {
        let docs = vec![doc! {
            "_id": 1,
            "available_versions": ["2", "3.3"]
        }];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let source_paths = build_mapping_source_paths("sizings", &schema);

        assert_eq!(
            source_paths.get("available_versions"),
            Some(&SourcePath {
                path: vec!["available_versions".to_owned()],
                scalar_array_field: Some("available_versions".to_owned()),
                grouped_fields: None,
            })
        );
    }

    #[test]
    fn build_mapping_source_paths_groups_same_shape_root_arrays() {
        let docs = vec![doc! {
            "_id": 1,
            "dev": [{
                "provider": "aiven",
                "available_localizations": ["eu-west-1"]
            }],
            "prod": [{
                "provider": "atlas",
                "available_localizations": ["eu-west-2"]
            }]
        }];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let source_paths = build_mapping_source_paths("communities", &schema);

        assert_eq!(
            source_paths.get("communities"),
            Some(&SourcePath {
                path: Vec::new(),
                scalar_array_field: None,
                grouped_fields: Some(vec!["dev".to_owned(), "prod".to_owned()]),
            })
        );
        assert_eq!(
            source_paths.get("available_localizations"),
            Some(&SourcePath {
                path: vec!["available_localizations".to_owned()],
                scalar_array_field: Some("available_localizations".to_owned()),
                grouped_fields: Some(vec!["dev".to_owned(), "prod".to_owned()]),
            })
        );
        assert_eq!(source_paths.get("communities_dev"), None);
        assert_eq!(source_paths.get("communities_prod"), None);
    }

    #[test]
    fn extract_source_documents_flattens_nested_array_objects() {
        let doc = doc! {
            "advices": [
                {
                    "earnings": { "monthly_gain": 12.5_f64 }
                },
                {
                    "earnings": { "monthly_gain": 7.0_f64 }
                }
            ]
        };

        let nested = extract_source_documents(
            &doc,
            &SourcePath {
                path: vec!["advices".to_owned(), "earnings".to_owned()],
                scalar_array_field: None,
                grouped_fields: None,
            },
        );

        assert_eq!(nested.len(), 2);
        assert_eq!(nested[0].get_f64("monthly_gain"), Ok(12.5));
        assert_eq!(nested[1].get_f64("monthly_gain"), Ok(7.0));
    }

    #[test]
    fn extract_source_documents_expands_scalar_arrays() {
        let doc = doc! {
            "_id": 42,
            "available_versions": ["2", "3.3"]
        };

        let nested = extract_source_documents(
            &doc,
            &SourcePath {
                path: vec!["available_versions".to_owned()],
                scalar_array_field: Some("available_versions".to_owned()),
                grouped_fields: None,
            },
        );

        assert_eq!(nested.len(), 2);
        assert_eq!(nested[0].get_str("available_versions"), Ok("2"));
        assert_eq!(nested[1].get_str("available_versions"), Ok("3.3"));
        assert_eq!(nested[0].get_i32("_id"), Ok(42));
    }

    #[test]
    fn extract_source_documents_skips_empty_nested_objects() {
        let doc = doc! {
            "_id": 1,
            "tier_and_details": {}
        };

        let nested = extract_source_documents(
            &doc,
            &SourcePath {
                path: vec!["tier_and_details".to_owned()],
                scalar_array_field: None,
                grouped_fields: None,
            },
        );

        assert!(
            nested.is_empty(),
            "empty nested object must not be extracted for md5"
        );
    }

    #[test]
    fn extract_source_documents_groups_root_array_siblings_with_key() {
        let doc = doc! {
            "_id": 42,
            "dev": [{"provider": "aiven"}],
            "prod": [{"provider": "atlas"}]
        };

        let nested = extract_source_documents(
            &doc,
            &SourcePath {
                path: Vec::new(),
                scalar_array_field: None,
                grouped_fields: Some(vec!["dev".to_owned(), "prod".to_owned()]),
            },
        );

        assert_eq!(nested.len(), 2);
        assert_eq!(nested[0].get_str("key"), Ok("dev"));
        assert_eq!(nested[1].get_str("key"), Ok("prod"));
    }

    #[test]
    fn extract_source_documents_grouped_root_skips_empty_objects() {
        let doc = doc! {
            "_id": 42,
            "dev": [{}],
            "prod": [{"provider": "atlas"}]
        };

        let nested = extract_source_documents(
            &doc,
            &SourcePath {
                path: Vec::new(),
                scalar_array_field: None,
                grouped_fields: Some(vec!["dev".to_owned(), "prod".to_owned()]),
            },
        );

        assert_eq!(nested.len(), 1);
        assert_eq!(nested[0].get_str("key"), Ok("prod"));
    }

    #[test]
    fn extract_source_documents_expands_uuid_keyed_map_documents() {
        let doc = doc! {
            "_id": "customer-1",
            "tier_and_details": {
                "0df078f33aa74a2e9696e0520c1a828a": {
                    "active": true,
                    "tier": "bronze"
                },
                "699456451cc24f028d2aa99d7534c219": {
                    "active": false,
                    "tier": "silver"
                }
            }
        };

        let nested = extract_source_documents(
            &doc,
            &SourcePath {
                path: vec!["tier_and_details".to_owned()],
                scalar_array_field: None,
                grouped_fields: None,
            },
        );

        assert_eq!(nested.len(), 2);
        assert!(nested
            .iter()
            .any(|row| row.get_bool("active") == Ok(true) && row.get_str("tier") == Ok("bronze")));
        assert!(nested
            .iter()
            .any(|row| row.get_bool("active") == Ok(false) && row.get_str("tier") == Ok("silver")));
    }

    #[test]
    fn mongo_field_literal_supports_dotted_paths_for_inlined_objects() {
        let doc = doc! {
            "metadata": {
                "creation_date": "2025-08-11T00:00:00Z"
            }
        };

        assert_eq!(
            mongo_field_literal(&doc, "metadata.creation_date"),
            "\"2025-08-11T00:00:00Z\""
        );
    }

    #[test]
    fn drop_incompatible_columns_skips_objectid_to_uuid() {
        let docs = vec![doc! {
            "_id": bson::oid::ObjectId::new(),
            "status": "ok"
        }];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let mut mapping_yaml: MappingYaml = serde_yaml::from_str(
            r#"
pg_mapping:
  table_name: accounts
  columns:
    - { source_field: _id, target_field: id, data_type: UUID }
    - { source_field: status, target_field: status, data_type: TEXT }
  ddl:
    columns:
      - { name: id, sql_type: UUID }
      - { name: status, sql_type: TEXT }
    foreign_keys: []
"#,
        )
        .expect("mapping yaml should parse");

        drop_incompatible_columns(
            &schema,
            &SourcePath {
                path: Vec::new(),
                scalar_array_field: None,
                grouped_fields: None,
            },
            &mut mapping_yaml,
        );

        let remaining_targets = mapping_yaml
            .pg_mapping
            .columns
            .iter()
            .map(|column| column.target_field.as_str())
            .collect::<Vec<_>>();
        assert!(!remaining_targets.contains(&"id"));
        assert!(remaining_targets.contains(&"status"));
    }

    #[test]
    fn drop_incompatible_columns_skips_string_to_datetime() {
        let docs = vec![doc! {
            "status": "ok",
            "monthly_gain": "0"
        }];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let mut mapping_yaml: MappingYaml = serde_yaml::from_str(
            r#"
pg_mapping:
  table_name: advisors
  columns:
    - { source_field: status, target_field: status, data_type: TEXT }
    - { source_field: monthly_gain, target_field: monthly_gain, data_type: "TIMESTAMP WITH TIME ZONE" }
  ddl:
    columns:
      - { name: status, sql_type: TEXT }
      - { name: monthly_gain, sql_type: "TIMESTAMP WITH TIME ZONE" }
    foreign_keys: []
"#,
        )
        .expect("mapping yaml should parse");

        drop_incompatible_columns(
            &schema,
            &SourcePath {
                path: Vec::new(),
                scalar_array_field: None,
                grouped_fields: None,
            },
            &mut mapping_yaml,
        );

        let remaining_targets = mapping_yaml
            .pg_mapping
            .columns
            .iter()
            .map(|column| column.target_field.as_str())
            .collect::<Vec<_>>();
        assert!(remaining_targets.contains(&"status"));
        assert!(!remaining_targets.contains(&"monthly_gain"));
    }

    #[test]
    fn drop_incompatible_columns_skips_objectid_to_uuid_when_uuid_from_ddl_with_default() {
        let docs = vec![doc! {
            "_id": bson::oid::ObjectId::new(),
            "status": "ok"
        }];
        let mut analyzer = Analyzer::new(true);
        for doc in &docs {
            analyzer.process_document(doc);
        }
        let schema = analyzer.finish();

        let mut mapping_yaml: MappingYaml = serde_yaml::from_str(
            r#"
pg_mapping:
  table_name: accounts
  columns:
    - { source_field: _id, target_field: id }
    - { source_field: status, target_field: status }
  ddl:
    columns:
      - { name: id, sql_type: "UUID DEFAULT public.gen_random_uuid() PRIMARY KEY" }
      - { name: status, sql_type: TEXT }
    foreign_keys: []
"#,
        )
        .expect("mapping yaml should parse");

        drop_incompatible_columns(
            &schema,
            &SourcePath {
                path: Vec::new(),
                scalar_array_field: None,
                grouped_fields: None,
            },
            &mut mapping_yaml,
        );

        let remaining_targets = mapping_yaml
            .pg_mapping
            .columns
            .iter()
            .map(|column| column.target_field.as_str())
            .collect::<Vec<_>>();
        assert!(!remaining_targets.contains(&"id"));
        assert!(remaining_targets.contains(&"status"));
    }


    use crate::checkmd5::bson_to_comparable_json;
    use chrono::{TimeZone, Utc};
    use mongodb::bson::DateTime;
    use serde_json::json;
    #[test]
    fn test_datetime_conversion_to_rfc3339() {
        // --- 1. Arrange: Create the input data ---

        // Create a specific UTC date.
        // This is the same date from your original example.
        let test_date = Utc.with_ymd_and_hms(2023, 9, 6, 9, 11, 36).unwrap();

        // Convert the chrono DateTime into a Bson::DateTime.
        let bson_datetime = Bson::DateTime(DateTime::from(test_date));

        // --- 2. Act: Call the function we want to test ---
        let result = bson_to_comparable_json(&bson_datetime);

        // --- 3. Assert: Check if the output is correct ---

        // Define the expected output: a JSON string in RFC 3339 format.
        // The "+00:00" signifies the UTC timezone.
        let expected_json = json!("2023-09-06T09:11:36Z");

        // Assert that the result matches our expectation.
        // The test will pass if they are equal, and fail otherwise.
        assert_eq!(result, expected_json);
    }

    use crate::checkmd5::{
        grouped_key_filter_for_target, pg_select_query_unordered, read_mapping_yaml,
    };
    use std::path::PathBuf;
    #[test]
    fn test_pg_select_query_unordered_for_mapping_address() {
        // 1. Arrange: Locate and parse the mapping_address.yaml fixture
        let fixture_path = PathBuf::from("tests/fixtures/mapping_address.yaml");

        let mapping_yaml = read_mapping_yaml(&fixture_path)
            .expect("Failed to read mapping_address.yaml fixture file");

        let schema_name = mapping_yaml.pg_mapping.schema_name.as_deref(); // Expecting Some("sample_airbnb")
        let table_name = &mapping_yaml.pg_mapping.table_name; // Expecting "address"

        // Collect target fields from the parsed columns in the exact order specified
        let target_fields: Vec<String> = mapping_yaml
            .pg_mapping
            .columns
            .iter()
            .map(|col| col.target_field.clone())
            .collect();

        // Ensure we loaded the expected columns from mapping_address.yaml
        assert_eq!(
            target_fields,
            vec![
                "country".to_string(),
                "country_code".to_string(),
                "government_area".to_string(),
                "location".to_string(),
                "market".to_string(),
                "street".to_string(),
                "suburb".to_string()
            ]
        );

        // 2. Act: Generate the SQL select query using your function
        let generated_sql =
            pg_select_query_unordered(schema_name, table_name, &target_fields, None, None, None);

        //println!("Generated SQL: {}", generated_sql);

        // 3. Assert: Verify the generated SQL matches the exact expected string format
        let expected_sql = concat!(
            "SELECT ",
            "COALESCE(to_json(\"country\")::text, 'null') AS \"country\", ",
            "COALESCE(to_json(\"country_code\")::text, 'null') AS \"country_code\", ",
            "COALESCE(to_json(\"government_area\")::text, 'null') AS \"government_area\", ",
            "COALESCE(to_json(\"location\")::text, 'null') AS \"location\", ",
            "COALESCE(to_json(\"market\")::text, 'null') AS \"market\", ",
            "COALESCE(to_json(\"street\")::text, 'null') AS \"street\", ",
            "COALESCE(to_json(\"suburb\")::text, 'null') AS \"suburb\" ",
            "FROM \"sample_airbnb\".\"address\""
        );
        //println!("Expected SQL: {}", expected_sql);
        assert_eq!(generated_sql, expected_sql);
    }

    #[test]
    fn pg_select_query_unordered_adds_grouped_key_where_clause() {
        let sql = pg_select_query_unordered(
            Some("sample_airbnb"),
            "address",
            &["country".to_owned()],
            None,
            Some(("_key", "dev")),
            None,
        );

        assert_eq!(
            sql,
            "SELECT COALESCE(to_json(\"country\")::text, 'null') AS \"country\" FROM \"sample_airbnb\".\"address\" WHERE \"_key\" = 'dev'"
        );
    }

    #[test]
    fn grouped_key_filter_for_target_prefers_mapping_literal_value() {
        let mapping_yaml: MappingYaml = serde_yaml::from_str(
            r#"
pg_mapping:
  table_name: events
  columns:
    - { source_field: "", target_field: _key, literal_value: prod }
"#,
        )
        .expect("mapping yaml should parse");

        let target = super::MappingTarget {
            source_collection: "events_dev".to_owned(),
            mapping_path: PathBuf::from("mapping_events.yaml"),
            mapping_yaml,
            source_path: super::SourcePath {
                path: Vec::new(),
                scalar_array_field: None,
                grouped_fields: None,
            },
        };

        assert_eq!(
            grouped_key_filter_for_target(&target),
            Some(("_key".to_owned(), "prod".to_owned()))
        );
    }

    #[test]
    fn grouped_key_filter_for_target_falls_back_to_collection_suffix() {
        let mapping_yaml: MappingYaml = serde_yaml::from_str(
            r#"
pg_mapping:
  table_name: events
  columns:
    - { source_field: "", target_field: _key }
"#,
        )
        .expect("mapping yaml should parse");

        let target = super::MappingTarget {
            source_collection: "events_dev".to_owned(),
            mapping_path: PathBuf::from("mapping_events.yaml"),
            mapping_yaml,
            source_path: super::SourcePath {
                path: Vec::new(),
                scalar_array_field: None,
                grouped_fields: None,
            },
        };

        assert_eq!(
            grouped_key_filter_for_target(&target),
            Some(("_key".to_owned(), "dev".to_owned()))
        );
    }

    use super::*;
    use crate::checkmd5::compute_collection_checksums_via_temp_tables;
    mod common {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/common/mod.rs"));
    }
    use common::TestHarness;
    #[tokio::test]
    async fn test_compute_collection_checksums_via_temp_tables_with_containers(
    ) -> Result<(), Box<dyn std::error::Error>> {
        // 1. Setup isolated test environment/containers
        let harness = TestHarness::new(
            "tests/fixtures/listingsandreviews.json",
            "tests/fixtures/listingsandreviews.sql",
            "tests/fixtures/mapping_listingsandreviews.yaml",
        )
        .await?;

        // 2. Specify fields to test deterministically
        let source_fields = vec![
            ("access".to_string(), Some("text".to_string())),
            ("accommodates".to_string(), Some("integer".to_string())),
            ("amenities".to_string(), Some("text[]".to_string())),
            ("property_type".to_string(), Some("text".to_string())),
            ("room_type".to_string(), Some("varchar(20)".to_string())),
            ("summary".to_string(), Some("text".to_string())),
        ];
        let target_fields = vec![
            "access".to_string(),
            "accommodates".to_string(),
            "amenities".to_string(),
            "property_type".to_string(),
            "room_type".to_string(),
            "summary".to_string(),
        ];

        // 3. Act: Run the verification engine
        let result = compute_collection_checksums_via_temp_tables(
            &harness.pg_read_client,
            None,
            &harness.pg_client,
            &harness.mongo_collection,
            &source_fields,
            &SourcePath {
                path: Vec::new(),
                scalar_array_field: None,
                grouped_fields: None,
            },
            "test_db",
            "employees",
            harness.schema_name.as_deref(),
            &harness.table_name,
            &target_fields,
            None,
        )
        .await?;

        if !result.matches {
            if let Some(mismatches) = result.mismatches {
                    eprintln!("\n=== DATA MISMATCH DETECTED ===");
                    eprintln!("Global Mongo MD5 Checksum: {}", result.mongo_md5);
                    eprintln!("Global Postgres MD5 Checksum: {}", result.pg_md5);
                
                    eprintln!("\nFirst 5 Rows Only in MongoDB:");
                    for row in mismatches.mongo_only {
                        eprintln!("  - Row MD5: {}", row.md5);
                        eprintln!("    Values Snapshot: {:?}", row.delta);
                    }

                    eprintln!("\nFirst 5 Rows Only in PostgreSQL:");
                    for row in mismatches.pg_only {
                        eprintln!("  - Row MD5: {}", row.md5);
                        eprintln!("    Values Snapshot: {:?}", row.delta);
                    }
                    eprintln!("===============================\n");
                }
        }

        assert!(
            result.matches, 
            "The computed aggregate dataset signatures from MongoDB and PostgreSQL tables must match exactly!"
        );
        assert!(!result.mongo_md5.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_compute_collection_checksums_via_temp_tables_with_containers_nok(
    ) -> Result<(), Box<dyn std::error::Error>> {
        // 1. Setup isolated test environment/containers
        let harness = TestHarness::new(
            "tests/fixtures/listingsandreviews.json",
            "tests/fixtures/listingsandreviews.sql",
            "tests/fixtures/mapping_listingsandreviews.yaml",
        )
        .await?;

        // --- INTENTIONAL FAILURE INJECTION ---
        // Alter a single field in Postgres to cause a hash mismatch with Mongo.
        // We change the 'room_type' to simulate a validation or padding bug.
        let qualified_table = match &harness.schema_name {
            Some(schema) => format!("\"{}\".\"{}\"", schema, harness.table_name),
            None => format!("\"{}\"", harness.table_name),
        };


        let room_types = [
            "Entire home/apt-1",
            "Entire home/apt-2",
            "Entire home/apt-3",
            "Private room-1",
            "Private room-2",
            "Shared room-1",
            "Hotel room-1",
            "Entire home/apt-2bis",
            "Private room-3",
            "Shared room-2",
        ];

        // Use qualified_table from your harness setup context if applicable
        for i in 0..10 {
            // Explicitly define target_id as i64 to match Postgres Int8
            let target_id: i64 = 10009999 + i as i64;
            let current_room_type = room_types[i];

            harness.pg_client
                .execute(
                    &format!("UPDATE {} SET room_type = $1 WHERE id = $2;", qualified_table),
                    &[&current_room_type, &target_id]
                )
                .await?;
        }

        // 2. Specify fields to test deterministically
        let source_fields = vec![
            ("access".to_string(), Some("text".to_string())),
            ("accommodates".to_string(), Some("integer".to_string())),
            ("amenities".to_string(), Some("text[]".to_string())),
            ("property_type".to_string(), Some("text".to_string())),
            ("room_type".to_string(), Some("varchar(20)".to_string())),
            ("summary".to_string(), Some("text".to_string())),
        ];
        let target_fields = vec![
            "access".to_string(),
            "accommodates".to_string(),
            "amenities".to_string(),
            "property_type".to_string(),
            "room_type".to_string(),
            "summary".to_string(),
        ];

        // 3. Act: Run the verification engine
        let result = compute_collection_checksums_via_temp_tables(
            &harness.pg_read_client,
            None,
            &harness.pg_client,
            &harness.mongo_collection,
            &source_fields,
            &SourcePath {
                path: Vec::new(),
                scalar_array_field: None,
                grouped_fields: None,
            },
            "test_db",
            "employees",
            harness.schema_name.as_deref(),
            &harness.table_name,
            &target_fields,
            None,
        )
        .await?;

        if !result.matches {
            if let Some(mismatches) = result.mismatches {
                eprintln!("\n=== DATA MISMATCH DETECTED ===");
                eprintln!("Global Mongo MD5 Checksum: {}", result.mongo_md5);
                eprintln!("Global Postgres MD5 Checksum: {}", result.pg_md5);
                
                eprintln!("\nFirst 5 Rows Only in MongoDB:");
                for row in mismatches.mongo_only {
                    eprintln!("  - Row MD5: {}", row.md5);
                    eprintln!("    Values Snapshot: {:?}", row.delta);
                }

                eprintln!("\nFirst 5 Rows Only in PostgreSQL:");
                for row in mismatches.pg_only {
                    eprintln!("  - Row MD5: {}", row.md5);
                    eprintln!("    Values Snapshot: {:?}", row.delta);
                }
                eprintln!("===============================\n");
            }
        }

        assert!(
            !result.matches, 
            "The computed aggregate dataset signatures from MongoDB and PostgreSQL tables must not match !"
        );
        assert!(!result.mongo_md5.is_empty());

        Ok(())
    }    
}
