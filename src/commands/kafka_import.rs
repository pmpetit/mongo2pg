use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use apache_avro::{from_avro_datum, types::Value as AvroValue, Schema};
use bson::doc;
use bytes::Bytes;
use chrono::TimeZone;
use futures::{SinkExt, Stream, StreamExt};
use log::{debug, error, info, warn};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::pin::pin;
use tracing::Instrument;

use crate::cli::KafkaImportArgs;
use crate::commands::ping::kafka_worker_child_extra_args;
use crate::commands::shared::{
    child_row_objects_for_mapping, extract_psql_database_name, extract_search_path,
    format_postgres_error, is_missing_postgis_control_file, normalize_pg_identifier,
    pg_uri_with_database, preflight_existing_tables_error, quote_ident, resolve_collections_dir,
    resolve_local_project_root_from_config, sanitize_name, split_namespace_scope,
    stage_export_metadata_from_gcs, strip_postgis_extension_statement, strip_psql_preamble,
    CollectionMapping, DdlForeignKeyMapping,
};
#[cfg(test)]
use crate::commands::shared::{default_ddl_editing_guidance, DdlTableMapping, PgMapping};
use crate::db::mongo::{client_with_options, parse_client_options};
use crate::db::pg::connect_client as connect_pg_client;
use crate::export::{resolve_export_write_backend, ExportWriteBackend};
use crate::schema_diagram::parse_sql;
use crate::util::{
    configured_project_root, connection_failed_context, objectid_hex_to_uuid, read_conf,
};

fn is_root_mapping(mapping: &CollectionMapping) -> bool {
    mapping.mongo_path.as_deref().unwrap_or(".").trim() == "."
        && mapping
            .pg_mapping
            .ddl
            .as_ref()
            .is_none_or(|ddl| ddl.foreign_keys.iter().all(|fk| fk.to_col != "id"))
}

fn ensure_parent_inserted_before_child(
    parent_mapping: &CollectionMapping,
    child_mapping: &CollectionMapping,
    fk: &DdlForeignKeyMapping,
    inserted_tables: &std::collections::HashSet<String>,
) -> Result<()> {
    let expected_parent = parent_mapping.pg_mapping.table_name.as_str();
    if normalize_pg_identifier(&fk.to_table) != normalize_pg_identifier(expected_parent) {
        return Err(anyhow!(
            "invalid fk traversal order: child_table={} fk_to_table={} current_parent_table={}",
            child_mapping.pg_mapping.table_name,
            fk.to_table,
            parent_mapping.pg_mapping.table_name,
        ));
    }

    if !inserted_tables.contains(expected_parent) {
        return Err(anyhow!(
            "fk parent not inserted before child: parent_table={} child_table={} fk={}=>{}.{}",
            parent_mapping.pg_mapping.table_name,
            child_mapping.pg_mapping.table_name,
            fk.from_col,
            fk.to_table,
            fk.to_col,
        ));
    }

    Ok(())
}

fn parse_topic_db_collection(
    topic: &str,
    topic_prefix: Option<&str>,
    default_db_name: Option<&str>,
) -> Option<(String, String)> {
    let mut effective = topic;
    if let Some(prefix) = topic_prefix {
        if !topic.starts_with(prefix) {
            return None;
        }
        effective = topic[prefix.len()..].trim_start_matches('.');
    }

    let segments = effective.split('.').collect::<Vec<_>>();
    if segments.len() >= 2 {
        return Some((
            segments[segments.len() - 2].to_owned(),
            segments[segments.len() - 1].to_owned(),
        ));
    }

    // Some deployments set topic_prefix to include db name already
    // (for example mongo2pg.sample_analytics), so only <collection>
    // remains after prefix trimming. In that case fall back to configured
    // namespace database for the db segment.
    if segments.len() == 1 {
        if let Some(db_name) = default_db_name {
            return Some((db_name.to_owned(), segments[0].to_owned()));
        }
    }

    None
}

pub async fn run_kafka_import(args: KafkaImportArgs) -> Result<()> {
    enum ReadStageEvent<T> {
        Message(T),
        StreamEnded,
        IdleTimeout,
    }

    enum WriteStageOutcome {
        Applied(u64),
        Skipped,
    }

    struct SpanRateLimiter {
        rate_per_sec: f64,
        burst: f64,
        tokens: f64,
        last_refill: Instant,
    }

    impl SpanRateLimiter {
        fn new(rate_per_sec: f64, burst: f64) -> Self {
            let normalized_rate = if rate_per_sec.is_finite() && rate_per_sec > 0.0 {
                rate_per_sec
            } else {
                0.0
            };
            let normalized_burst = if burst.is_finite() && burst > 0.0 {
                burst
            } else {
                1.0
            };

            Self {
                rate_per_sec: normalized_rate,
                burst: normalized_burst,
                tokens: normalized_burst,
                last_refill: Instant::now(),
            }
        }

        fn allow(&mut self) -> bool {
            let now = Instant::now();
            let elapsed = now.duration_since(self.last_refill).as_secs_f64();
            self.last_refill = now;
            self.tokens = (self.tokens + elapsed * self.rate_per_sec).min(self.burst);

            if self.tokens >= 1.0 {
                self.tokens -= 1.0;
                true
            } else {
                false
            }
        }
    }

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

    async fn publish_to_dlq(
        producer: &FutureProducer,
        source_topic: &str,
        key: Option<&[u8]>,
        payload: &[u8],
    ) -> Result<()> {
        let dlq_topic = format!("dlq_{source_topic}");
        let mut record = FutureRecord::to(&dlq_topic).payload(payload);
        if let Some(key) = key {
            record = record.key(key);
        }

        producer
            .send(record, Duration::from_secs(5))
            .await
            .map_err(|(err, _)| anyhow!("DLQ publish failed for topic {dlq_topic}: {err}"))?;

        Ok(())
    }

    async fn publish_to_dlq_topic(
        producer: &FutureProducer,
        dlq_topic: &str,
        key: Option<&[u8]>,
        payload: &[u8],
    ) -> Result<()> {
        let mut record = FutureRecord::to(dlq_topic).payload(payload);
        if let Some(key) = key {
            record = record.key(key);
        }

        producer
            .send(record, Duration::from_secs(5))
            .await
            .map_err(|(err, _)| anyhow!("DLQ publish failed for topic {dlq_topic}: {err}"))?;

        Ok(())
    }

    fn sanitize_dlq_collection_component(collection_name: &str) -> String {
        let cleaned = collection_name
            .chars()
            .map(|ch| {
                if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                    ch
                } else {
                    '_'
                }
            })
            .collect::<String>();
        cleaned.trim_matches('_').to_owned()
    }

    fn adhoc_dlq_topic_for_collection(collection_name: &str) -> String {
        let component = sanitize_dlq_collection_component(collection_name);
        if component.is_empty() {
            "dlq_unknown_collection".to_owned()
        } else {
            format!("dlq_{component}")
        }
    }

    fn csv_escape_copy_field(value: &str) -> String {
        let needs_quotes = value
            .chars()
            .any(|ch| matches!(ch, ',' | '\n' | '\r' | '"'));
        if !needs_quotes {
            return value.to_owned();
        }
        let escaped = value.replace('"', "\"\"");
        format!("\"{escaped}\"")
    }

    fn parse_flush_batch_after(raw: &str) -> Result<Duration> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(anyhow!(
                "Invalid kafka.flush_batch_after: empty value. Use values like '1000ms', '2s', or '1m'"
            ));
        }

        let normalized = trimmed.to_ascii_lowercase();
        let (value_part, multiplier_ms) = if let Some(value) = normalized.strip_suffix("ms") {
            (value.trim(), 1_u64)
        } else if let Some(value) = normalized.strip_suffix('s') {
            (value.trim(), 1_000_u64)
        } else if let Some(value) = normalized.strip_suffix('m') {
            (value.trim(), 60_000_u64)
        } else if normalized.chars().all(|ch| ch.is_ascii_digit()) {
            (normalized.as_str(), 1_u64)
        } else {
            return Err(anyhow!(
                "Invalid kafka.flush_batch_after='{}'. Use values like '1000ms', '2s', or '1m'",
                raw
            ));
        };

        let value = value_part.parse::<u64>().with_context(|| {
            format!(
                "Invalid kafka.flush_batch_after='{}': duration value must be a positive integer",
                raw
            )
        })?;
        if value == 0 {
            return Err(anyhow!(
                "Invalid kafka.flush_batch_after='{}': duration must be > 0",
                raw
            ));
        }

        let millis = value.checked_mul(multiplier_ms).ok_or_else(|| {
            anyhow!(
                "Invalid kafka.flush_batch_after='{}': duration is too large",
                raw
            )
        })?;
        Ok(Duration::from_millis(millis))
    }

    fn parse_quoted_sql_literal(raw: &str) -> Option<(String, &str)> {
        if !raw.starts_with('\'') {
            return None;
        }

        let mut out = String::new();
        let mut idx = 1usize;

        while idx < raw.len() {
            let tail = &raw[idx..];
            let mut iter = tail.char_indices();
            let (_, ch) = iter.next()?;

            if ch == '\'' {
                let next_idx = idx + ch.len_utf8();
                if let Some(next_ch) = raw[next_idx..].chars().next() {
                    if next_ch == '\'' {
                        out.push('\'');
                        idx = next_idx + next_ch.len_utf8();
                        continue;
                    }
                }

                idx = next_idx;
                return Some((out, &raw[idx..]));
            }

            out.push(ch);
            idx += ch.len_utf8();
        }

        None
    }

    fn extract_cast_inner_expr(raw: &str) -> Option<&str> {
        let trimmed = raw.trim();
        if !trimmed.ends_with(')') {
            return None;
        }

        let upper = trimmed.to_ascii_uppercase();
        if !upper.starts_with("CAST(") {
            return None;
        }

        let inner = &trimmed[5..trimmed.len() - 1];
        let upper_inner = inner.to_ascii_uppercase();
        let as_pos = upper_inner.rfind(" AS ")?;
        Some(inner[..as_pos].trim())
    }

    fn parse_array_elements_sql(inner: &str) -> Option<Vec<Option<String>>> {
        let mut idx = 0usize;
        let bytes = inner.as_bytes();
        let mut out: Vec<Option<String>> = Vec::new();

        while idx < bytes.len() {
            while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
                idx += 1;
            }
            if idx >= bytes.len() {
                break;
            }

            let tail = &inner[idx..];
            if tail.starts_with('\'') {
                let (content, suffix) = parse_quoted_sql_literal(tail)?;
                out.push(Some(content));
                idx = inner.len() - suffix.len();
            } else {
                let mut end = idx;
                while end < bytes.len() && bytes[end] != b',' {
                    end += 1;
                }
                let token = inner[idx..end].trim();
                if token.is_empty() {
                    return None;
                }
                if token.eq_ignore_ascii_case("NULL") {
                    out.push(None);
                } else if token.eq_ignore_ascii_case("TRUE")
                    || token.eq_ignore_ascii_case("FALSE")
                    || token.parse::<i128>().is_ok()
                    || token.parse::<f64>().is_ok()
                {
                    out.push(Some(token.to_owned()));
                } else {
                    return None;
                }
                idx = end;
            }

            while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
                idx += 1;
            }
            if idx < bytes.len() {
                if bytes[idx] != b',' {
                    return None;
                }
                idx += 1;
            }
        }

        Some(out)
    }

    fn to_pg_array_text(elements: &[Option<String>]) -> String {
        let rendered = elements
            .iter()
            .map(|elem| match elem {
                None => "NULL".to_owned(),
                Some(raw)
                    if raw.eq_ignore_ascii_case("TRUE")
                        || raw.eq_ignore_ascii_case("FALSE")
                        || raw.parse::<i128>().is_ok()
                        || raw.parse::<f64>().is_ok() =>
                {
                    raw.clone()
                }
                Some(raw) => {
                    let escaped = raw.replace('\\', "\\\\").replace('"', "\\\"");
                    format!("\"{escaped}\"")
                }
            })
            .collect::<Vec<_>>()
            .join(",");
        format!("{{{rendered}}}")
    }

    fn parse_sql_array_literal_to_copy_text(raw: &str) -> Option<String> {
        let trimmed = raw.trim();
        let upper = trimmed.to_ascii_uppercase();
        if !upper.starts_with("ARRAY[") {
            return None;
        }

        let mut idx = 6usize; // after "ARRAY["
        let bytes = trimmed.as_bytes();
        let mut in_string = false;
        while idx < bytes.len() {
            if in_string {
                if bytes[idx] == b'\'' {
                    if idx + 1 < bytes.len() && bytes[idx + 1] == b'\'' {
                        idx += 2;
                        continue;
                    }
                    in_string = false;
                }
                idx += 1;
                continue;
            }

            if bytes[idx] == b'\'' {
                in_string = true;
                idx += 1;
                continue;
            }

            if bytes[idx] == b']' {
                let inner = &trimmed[6..idx];
                let suffix = trimmed[idx + 1..].trim();
                if !(suffix.is_empty() || suffix.starts_with("::")) {
                    return None;
                }
                let elements = parse_array_elements_sql(inner)?;
                return Some(to_pg_array_text(&elements));
            }
            idx += 1;
        }

        None
    }

    fn parse_numeric_sql_expr(raw: &str) -> Option<f64> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return None;
        }

        let normalized = trimmed
            .strip_suffix("::double precision")
            .unwrap_or(trimmed)
            .trim();

        if let Some((left, right)) = normalized.split_once('/') {
            let lhs = parse_numeric_sql_expr(left)?;
            let rhs = parse_numeric_sql_expr(right)?;
            if rhs == 0.0 {
                return None;
            }
            return Some(lhs / rhs);
        }

        normalized.parse::<f64>().ok()
    }

    fn parse_to_timestamp_literal_to_copy_text(raw: &str) -> Option<String> {
        let trimmed = raw.trim();
        if !trimmed.starts_with("to_timestamp(") || !trimmed.ends_with(')') {
            return None;
        }

        let inner = &trimmed[13..trimmed.len() - 1];
        let seconds = parse_numeric_sql_expr(inner)?;
        let millis = (seconds * 1000.0).round() as i64;
        let dt = chrono::Utc.timestamp_millis_opt(millis).single()?;
        Some(dt.to_rfc3339())
    }

    fn parse_postgis_point_literal_to_copy_text(raw: &str) -> Option<String> {
        let trimmed = raw.trim();
        let upper = trimmed.to_ascii_uppercase();
        let prefix = "ST_SETSRID(ST_MAKEPOINT(";
        if !upper.starts_with(prefix) {
            return None;
        }

        let mut idx = prefix.len();
        let bytes = trimmed.as_bytes();

        let lon_start = idx;
        while idx < bytes.len() && bytes[idx] != b',' {
            idx += 1;
        }
        if idx >= bytes.len() {
            return None;
        }
        let lon_expr = trimmed[lon_start..idx].trim();
        idx += 1; // skip comma between lon and lat

        while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }

        let lat_start = idx;
        while idx < bytes.len() && bytes[idx] != b')' {
            idx += 1;
        }
        if idx >= bytes.len() {
            return None;
        }
        let lat_expr = trimmed[lat_start..idx].trim();
        idx += 1; // skip ')' closing ST_MakePoint

        while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }
        if idx >= bytes.len() || bytes[idx] != b',' {
            return None;
        }
        idx += 1; // skip comma before srid

        while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }

        let srid_start = idx;
        while idx < bytes.len() && bytes[idx] != b')' {
            idx += 1;
        }
        if idx >= bytes.len() {
            return None;
        }
        let srid_expr = trimmed[srid_start..idx].trim();
        idx += 1; // skip ')' closing ST_SetSRID

        let suffix = trimmed[idx..].trim();
        if !(suffix.is_empty() || suffix.starts_with("::")) {
            return None;
        }

        let lon = parse_numeric_sql_expr(lon_expr)?;
        let lat = parse_numeric_sql_expr(lat_expr)?;
        let srid = parse_numeric_sql_expr(srid_expr)?;
        let srid_int = srid.round() as i64;
        if (srid - srid_int as f64).abs() > f64::EPSILON {
            return None;
        }

        Some(format!("SRID={srid_int};POINT({lon} {lat})"))
    }

    fn sql_literal_to_copy_text(sql_literal_text: &str) -> Option<Option<String>> {
        let trimmed = sql_literal_text.trim();
        if trimmed.eq_ignore_ascii_case("NULL") {
            return Some(None);
        }

        if let Some(inner_expr) = extract_cast_inner_expr(trimmed) {
            return sql_literal_to_copy_text(inner_expr);
        }

        if let Some(array_text) = parse_sql_array_literal_to_copy_text(trimmed) {
            return Some(Some(array_text));
        }

        if let Some(ts_text) = parse_to_timestamp_literal_to_copy_text(trimmed) {
            return Some(Some(ts_text));
        }

        if let Some(point_text) = parse_postgis_point_literal_to_copy_text(trimmed) {
            return Some(Some(point_text));
        }

        if let Some((content, suffix)) = parse_quoted_sql_literal(trimmed) {
            let suffix = suffix.trim();
            if suffix.is_empty() || suffix.starts_with("::") {
                return Some(Some(content));
            }
            return None;
        }

        let upper = trimmed.to_ascii_uppercase();
        if upper == "TRUE" || upper == "FALSE" {
            return Some(Some(trimmed.to_owned()));
        }

        if trimmed.parse::<i128>().is_ok() || trimmed.parse::<f64>().is_ok() {
            return Some(Some(trimmed.to_owned()));
        }

        None
    }

    struct SnapshotBufferedMessage {
        topic: String,
        collection_name: String,
        folder_name: String,
        op: String,
        payload: Value,
        before: Option<Value>,
        after: Option<Value>,
        key_bytes: Option<Vec<u8>>,
        payload_bytes: Option<Vec<u8>>,
    }

    struct SnapshotCopyBatch {
        table_name: String,
        columns: Vec<String>,
        csv_rows: Vec<String>,
        sql_rows: Vec<String>,
        on_conflict_clause: String,
        messages: Vec<SnapshotBufferedMessage>,
    }

    enum SnapshotCopyRowBuildOutcome {
        Built((String, Vec<String>, String, String, String)),
        SkippedNonRootMapping { table_name: String },
        SkippedUnconvertibleLiteral { table_name: String, detail: String },
        SkippedEmptyColumns { table_name: String },
    }

    #[derive(Deserialize)]
    struct SchemaRegistrySchemaResponse {
        schema: String,
    }

    fn avro_to_json(value: AvroValue) -> Value {
        match value {
            AvroValue::Null => Value::Null,
            AvroValue::Boolean(v) => Value::Bool(v),
            AvroValue::Int(v) => Value::Number(v.into()),
            AvroValue::Long(v) => Value::Number(v.into()),
            AvroValue::Float(v) => serde_json::Number::from_f64(v as f64)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            AvroValue::Double(v) => serde_json::Number::from_f64(v)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            AvroValue::String(v) => Value::String(v),
            AvroValue::Array(values) => {
                Value::Array(values.into_iter().map(avro_to_json).collect())
            }
            AvroValue::Map(map) => Value::Object(
                map.into_iter()
                    .map(|(k, v)| (k, avro_to_json(v)))
                    .collect::<serde_json::Map<String, Value>>(),
            ),
            AvroValue::Union(_, boxed) => avro_to_json(*boxed),
            AvroValue::Record(fields) => Value::Object(
                fields
                    .into_iter()
                    .map(|(k, v)| (k, avro_to_json(v)))
                    .collect::<serde_json::Map<String, Value>>(),
            ),
            AvroValue::Enum(_, symbol) => Value::String(symbol),
            AvroValue::Uuid(v) => Value::String(v.to_string()),
            AvroValue::Decimal(v) => Value::String(format!("{v:?}")),
            AvroValue::BigDecimal(v) => Value::String(v.to_string()),
            _ => Value::Null,
        }
    }

    async fn fetch_schema_by_id(
        client: &reqwest::Client,
        schema_registry_url: &str,
        schema_registry_username: Option<&str>,
        schema_registry_password: Option<&str>,
        schema_id: u32,
    ) -> Result<Schema> {
        let mut request = client.get(format!(
            "{}/schemas/ids/{}",
            schema_registry_url.trim_end_matches('/'),
            schema_id
        ));
        if let Some(username) = schema_registry_username {
            request = request.basic_auth(username, schema_registry_password.map(|s| s.to_owned()));
        }
        let response = request
            .send()
            .await
            .with_context(|| format!("Failed to fetch schema id {schema_id} from Schema Registry"))?
            .error_for_status()
            .with_context(|| format!("Schema Registry returned error for schema id {schema_id}"))?;
        let payload: SchemaRegistrySchemaResponse = response
            .json()
            .await
            .with_context(|| "Failed to parse Schema Registry schema response")?;
        Schema::parse_str(&payload.schema)
            .with_context(|| format!("Failed to parse Avro schema id {schema_id}"))
    }

    async fn decode_message_value(
        bytes: &[u8],
        schema_registry_url: Option<&str>,
        schema_registry_username: Option<&str>,
        schema_registry_password: Option<&str>,
        http_client: &reqwest::Client,
        schema_cache: &mut HashMap<u32, Schema>,
    ) -> Result<Value> {
        // If Schema Registry is not configured, treat payloads as JsonConverter
        // output and decode directly as JSON.
        if schema_registry_url.is_none() {
            return serde_json::from_slice::<Value>(bytes).with_context(|| {
                "Failed to decode message as JSON payload (no schema registry configured)"
            });
        }

        if bytes.len() > 5 && bytes[0] == 0 {
            let schema_id = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
            let schema = if let Some(schema) = schema_cache.get(&schema_id) {
                schema.clone()
            } else {
                let url = schema_registry_url.ok_or_else(|| {
                    anyhow!(
                        "Confluent framing detected but kafka.schema_registry_url is not configured"
                    )
                })?;
                let parsed = fetch_schema_by_id(
                    http_client,
                    url,
                    schema_registry_username,
                    schema_registry_password,
                    schema_id,
                )
                .await?;
                schema_cache.insert(schema_id, parsed.clone());
                parsed
            };

            let mut slice: &[u8] = &bytes[5..];
            let avro = from_avro_datum(&schema, &mut slice, None)
                .with_context(|| "Failed to decode Avro payload")?;
            return Ok(avro_to_json(avro));
        }

        serde_json::from_slice::<Value>(bytes)
            .with_context(|| "Failed to decode message as JSON payload")
    }

    fn mongo_path_segments(mongo_path: Option<&str>) -> Vec<&str> {
        mongo_path
            .unwrap_or(".")
            .trim()
            .trim_start_matches('.')
            .split('.')
            .filter(|segment| !segment.is_empty())
            .collect()
    }

    fn values_at_path<'a>(value: &'a Value, segments: &[&str]) -> Vec<&'a Value> {
        let mut current = vec![value];
        for segment in segments {
            let mut next = Vec::new();
            for item in current {
                match item {
                    Value::Object(map) => {
                        if let Some(child) = map.get(*segment) {
                            match child {
                                Value::Array(arr) => {
                                    for val in arr {
                                        next.push(val);
                                    }
                                }
                                _ => next.push(child),
                            }
                        }
                    }
                    Value::Array(arr) => {
                        for entry in arr {
                            if let Value::Object(obj) = entry {
                                if let Some(child) = obj.get(*segment) {
                                    match child {
                                        Value::Array(inner) => {
                                            for val in inner {
                                                next.push(val);
                                            }
                                        }
                                        _ => next.push(child),
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            current = next;
            if current.is_empty() {
                break;
            }
        }
        current
    }

    fn find_value_in_nested_json_object<'a>(
        obj: &'a serde_json::Map<String, Value>,
        source_field: &str,
    ) -> Option<&'a Value> {
        if let Some(value) = obj.get(source_field) {
            return Some(value);
        }

        fn visit<'a>(value: &'a Value, source_field: &str, out: &mut Vec<&'a Value>) {
            match value {
                Value::Object(map) => {
                    if let Some(found) = map.get(source_field) {
                        out.push(found);
                    }
                    for child in map.values() {
                        visit(child, source_field, out);
                    }
                }
                Value::Array(items) => {
                    for item in items {
                        visit(item, source_field, out);
                    }
                }
                _ => {}
            }
        }

        let mut matches = Vec::new();
        for value in obj.values() {
            visit(value, source_field, &mut matches);
            if matches.len() > 1 {
                break;
            }
        }

        if matches.len() == 1 {
            Some(matches[0])
        } else {
            None
        }
    }

    fn format_table_insert_exec_summary(table_insert_execs: &HashMap<String, u64>) -> String {
        if table_insert_execs.is_empty() {
            return "none".to_owned();
        }

        let mut entries = table_insert_execs
            .iter()
            .map(|(table, count)| (table.clone(), *count))
            .collect::<Vec<_>>();
        entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

        entries
            .into_iter()
            .map(|(table, count)| format!("{table}:{count}"))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn escape_sql_string(raw: &str) -> String {
        raw.replace('\'', "''")
    }

    fn unwrap_union_tagged_value(value: &Value) -> &Value {
        const UNION_TAGS: [&str; 12] = [
            "null", "string", "boolean", "int", "long", "float", "double", "bytes", "array", "map",
            "record", "enum",
        ];

        let mut current = value;
        loop {
            let Value::Object(obj) = current else {
                break;
            };
            if obj.len() != 1 {
                break;
            }
            let Some((tag, inner)) = obj.iter().next() else {
                break;
            };
            if UNION_TAGS.contains(&tag.as_str()) {
                current = inner;
            } else {
                break;
            }
        }
        current
    }

    fn normalized_sql_type(sql_type: Option<&str>) -> Option<String> {
        let raw = sql_type?.trim();
        if raw.is_empty() {
            return None;
        }

        let lowered = raw.to_ascii_lowercase();
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
            if let Some(idx) = lowered.find(marker) {
                cut = cut.min(idx);
            }
        }

        let base = raw[..cut].trim();
        if base.is_empty() {
            None
        } else {
            Some(base.to_owned())
        }
    }

    fn cast_string_literal(raw: &str, sql_type: Option<&str>) -> String {
        let escaped = escape_sql_string(raw);
        if let Some(sql_type) = normalized_sql_type(sql_type) {
            let normalized = match sql_type.trim().to_ascii_lowercase().as_str() {
                "smallserial" | "serial2" => "SMALLINT",
                "serial" | "serial4" => "INTEGER",
                "bigserial" | "serial8" => "BIGINT",
                _ => &sql_type,
            };
            format!("CAST('{escaped}' AS {normalized})")
        } else {
            format!("'{escaped}'")
        }
    }

    fn sql_type_expects_temporal(sql_type: Option<&str>) -> bool {
        let Some(normalized) = normalized_sql_type(sql_type) else {
            return false;
        };
        let lower = normalized.trim().to_ascii_lowercase();
        lower.starts_with("timestamp") || lower == "date" || lower.starts_with("time")
    }

    fn cast_epoch_seconds_literal(seconds_expr: &str, sql_type: Option<&str>) -> String {
        if let Some(target_type) = normalized_sql_type(sql_type) {
            format!("CAST(to_timestamp({seconds_expr}) AS {target_type})")
        } else {
            format!("to_timestamp({seconds_expr})")
        }
    }

    fn temporal_literal_from_epoch_i64(value: i64, sql_type: Option<&str>) -> String {
        let seconds_expr = if value.unsigned_abs() >= 100_000_000_000 {
            format!("{value}::double precision / 1000.0")
        } else {
            format!("{value}::double precision")
        };
        cast_epoch_seconds_literal(&seconds_expr, sql_type)
    }

    fn temporal_literal_from_epoch_u64(value: u64, sql_type: Option<&str>) -> String {
        let seconds_expr = if value >= 100_000_000_000 {
            format!("{value}::double precision / 1000.0")
        } else {
            format!("{value}::double precision")
        };
        cast_epoch_seconds_literal(&seconds_expr, sql_type)
    }

    fn temporal_literal_from_epoch_f64(value: f64, sql_type: Option<&str>) -> String {
        let seconds_expr = if value.abs() >= 100_000_000_000.0 {
            format!("{value} / 1000.0")
        } else {
            value.to_string()
        };
        cast_epoch_seconds_literal(&seconds_expr, sql_type)
    }

    fn temporal_literal_from_epoch_millis_i64(value_ms: i64, sql_type: Option<&str>) -> String {
        // Value is milliseconds since epoch; convert to seconds with fractional part.
        let seconds_expr = format!("{value_ms}::double precision / 1000.0");
        cast_epoch_seconds_literal(&seconds_expr, sql_type)
    }

    fn temporal_literal_from_number(
        raw: &serde_json::Number,
        sql_type: Option<&str>,
    ) -> Option<String> {
        raw.as_i64()
            .map(|v| temporal_literal_from_epoch_i64(v, sql_type))
            .or_else(|| {
                raw.as_u64()
                    .map(|v| temporal_literal_from_epoch_u64(v, sql_type))
            })
            .or_else(|| {
                raw.as_f64()
                    .map(|v| temporal_literal_from_epoch_f64(v, sql_type))
            })
    }

    fn geojson_point_coordinates(value: &Value) -> Option<(f64, f64)> {
        match value {
            Value::String(raw) => serde_json::from_str::<Value>(raw)
                .ok()
                .and_then(|parsed| geojson_point_coordinates(&parsed)),
            Value::Object(obj) => {
                let point_type = obj.get("type")?.as_str()?;
                if !point_type.eq_ignore_ascii_case("point") {
                    return None;
                }
                let coords = obj.get("coordinates")?.as_array()?;
                if coords.len() != 2 {
                    return None;
                }
                let lon = coords[0].as_f64()?;
                let lat = coords[1].as_f64()?;
                Some((lon, lat))
            }
            _ => None,
        }
    }

    // Map Debezium/Mongo Extended JSON wrappers to PostgreSQL SQL literals.
    // Keep this centralized so adding new wrappers is easy.
    fn map_extended_json_literal(value: &Value, sql_type: Option<&str>) -> Option<String> {
        if sql_type_expects_temporal(sql_type) {
            if let Value::Number(raw) = value {
                return temporal_literal_from_number(raw, sql_type);
            }
        }

        let obj = value.as_object()?;

        if sql_type_expects_temporal(sql_type) {
            if let Some(Value::Number(raw)) = obj
                .get("long")
                .or_else(|| obj.get("int"))
                .or_else(|| obj.get("double"))
            {
                return temporal_literal_from_number(raw, sql_type);
            }
            if let Some(Value::String(raw)) = obj.get("string") {
                if let Ok(number) = raw.parse::<f64>() {
                    return Some(temporal_literal_from_epoch_f64(number, sql_type));
                }
            }
        }

        let normalized = normalized_sql_type(sql_type);

        if let Some(Value::String(raw)) = obj.get("$oid") {
            if normalized
                .as_deref()
                .map(|t| t.eq_ignore_ascii_case("uuid"))
                .unwrap_or(false)
            {
                return objectid_hex_to_uuid(raw)
                    .map(|uuid| cast_string_literal(&uuid, sql_type))
                    .or_else(|| Some(cast_string_literal(raw, sql_type)));
            }
            return Some(cast_string_literal(raw, sql_type));
        }

        if let Some(Value::String(raw)) = obj
            .get("$numberDecimal")
            .or_else(|| obj.get("$numberDouble"))
            .or_else(|| obj.get("$numberLong"))
            .or_else(|| obj.get("$numberInt"))
        {
            return Some(cast_string_literal(raw, sql_type));
        }

        if let Some(date_value) = obj.get("$date") {
            return match date_value {
                Value::String(raw) => Some(cast_string_literal(raw, sql_type)),
                // Debezium / Mongo typically represent $date as milliseconds since epoch.
                // Treat numeric $date values as milliseconds to avoid misclassification
                // by heuristics and preserve historical (pre-1970) dates correctly.
                Value::Number(raw) => {
                    if let Some(ms) = raw.as_i64() {
                        Some(temporal_literal_from_epoch_millis_i64(ms, sql_type))
                    } else if let Some(msu) = raw.as_u64() {
                        // safe cast for typical schema values
                        Some(temporal_literal_from_epoch_millis_i64(msu as i64, sql_type))
                    } else if let Some(msf) = raw.as_f64() {
                        // fallback: treat float as milliseconds
                        Some(temporal_literal_from_epoch_f64(msf, sql_type))
                    } else {
                        None
                    }
                }
                Value::Object(inner) => {
                    if let Some(Value::String(ms_raw)) = inner.get("$numberLong") {
                        ms_raw
                            .parse::<i64>()
                            .ok()
                            .map(|ms| temporal_literal_from_epoch_millis_i64(ms, sql_type))
                    } else {
                        None
                    }
                }
                _ => None,
            };
        }

        None
    }

    fn sql_type_expects_numeric(sql_type: Option<&str>) -> bool {
        let Some(normalized) = normalized_sql_type(sql_type) else {
            return false;
        };
        let lower = normalized.trim().to_ascii_lowercase();
        lower.starts_with("smallint")
            || lower.starts_with("integer")
            || lower.starts_with("bigint")
            || lower.starts_with("serial")
            || lower.starts_with("smallserial")
            || lower.starts_with("bigserial")
            || lower.starts_with("numeric")
            || lower.starts_with("decimal")
            || lower.starts_with("real")
            || lower.starts_with("double precision")
    }

    fn is_object_id_wrapper(value: &Value) -> bool {
        let value = unwrap_union_tagged_value(value);
        value
            .as_object()
            .and_then(|obj| obj.get("$oid"))
            .and_then(Value::as_str)
            .is_some()
    }

    fn validate_extended_json_compatibility(
        value: Option<&Value>,
        sql_type: Option<&str>,
        source_field: &str,
        target_field: &str,
        table_name: &str,
    ) -> Result<()> {
        let Some(value) = value.map(unwrap_union_tagged_value) else {
            return Ok(());
        };
        if is_object_id_wrapper(value) && sql_type_expects_numeric(sql_type) {
            let sql_type_name = sql_type.unwrap_or("<unknown>");
            let sample = serde_json::to_string(value)
                .unwrap_or_else(|_| "{\"$oid\":\"<invalid>\"}".to_owned());
            return Err(anyhow!(
                "incompatible ObjectId mapping: source_field={} target_field={} table={} sql_type={} value={} (ObjectId cannot cast to numeric). Update mapping to use numeric source field (example: theaterId) or change target column type to uuid/text",
                source_field,
                target_field,
                table_name,
                sql_type_name,
                sample,
            ));
        }
        Ok(())
    }

    fn singular_collection_name(collection_name: &str) -> String {
        let trimmed = collection_name.trim();
        if trimmed.ends_with("ies") && trimmed.len() > 3 {
            return format!("{}y", &trimmed[..trimmed.len() - 3]);
        }
        if trimmed.ends_with('s') && trimmed.len() > 1 {
            return trimmed[..trimmed.len() - 1].to_owned();
        }
        trimmed.to_owned()
    }

    fn candidate_numeric_root_id_source_fields(collection_name: &str) -> Vec<String> {
        let mut fields = Vec::new();
        let singular = singular_collection_name(collection_name);
        if !singular.is_empty() {
            fields.push(format!("{singular}Id"));
        }
        fields.push("id".to_owned());
        fields
    }

    fn value_is_numeric_compatible(value: &Value) -> bool {
        match value {
            Value::Number(_) => true,
            Value::String(raw) => raw.parse::<i128>().is_ok() || raw.parse::<f64>().is_ok(),
            Value::Object(obj) => {
                obj.get("$numberLong")
                    .and_then(Value::as_str)
                    .map(|raw| raw.parse::<i128>().is_ok())
                    .unwrap_or(false)
                    || obj
                        .get("$numberInt")
                        .and_then(Value::as_str)
                        .map(|raw| raw.parse::<i128>().is_ok())
                        .unwrap_or(false)
                    || obj
                        .get("$numberDouble")
                        .and_then(Value::as_str)
                        .map(|raw| raw.parse::<f64>().is_ok())
                        .unwrap_or(false)
                    || obj
                        .get("$numberDecimal")
                        .and_then(Value::as_str)
                        .map(|raw| raw.parse::<f64>().is_ok())
                        .unwrap_or(false)
            }
            _ => false,
        }
    }

    fn resolve_value_with_numeric_id_fallback<'a>(
        payload_obj: &'a serde_json::Map<String, Value>,
        mapping: &CollectionMapping,
        source_field: &str,
        target_field: &str,
        sql_type: Option<&str>,
        raw_value: Option<&'a Value>,
    ) -> (Option<&'a Value>, String) {
        let needs_fallback = target_field == "id"
            && source_field == "_id"
            && sql_type_expects_numeric(sql_type)
            && raw_value.map(is_object_id_wrapper).unwrap_or(false);

        if !needs_fallback {
            return (raw_value, source_field.to_owned());
        }

        for candidate in candidate_numeric_root_id_source_fields(&mapping.collection_name) {
            if let Some(value) = payload_obj.get(&candidate) {
                if value_is_numeric_compatible(value) {
                    return (Some(value), candidate);
                }
            }
        }

        (raw_value, source_field.to_owned())
    }

    fn sql_literal(value: Option<&Value>, sql_type: Option<&str>) -> String {
        let Some(value) = value.map(unwrap_union_tagged_value) else {
            return "NULL".to_owned();
        };

        match value {
            Value::Null => "NULL".to_owned(),
            Value::Bool(v) => {
                if *v {
                    "TRUE".to_owned()
                } else {
                    "FALSE".to_owned()
                }
            }
            Value::Number(v) => {
                map_extended_json_literal(value, sql_type).unwrap_or_else(|| v.to_string())
            }
            Value::String(v) => {
                let normalized = normalized_sql_type(sql_type);
                if normalized
                    .as_deref()
                    .map(|t| t.to_ascii_lowercase().starts_with("geometry"))
                    .unwrap_or(false)
                {
                    if let Some((lon, lat)) = geojson_point_coordinates(value) {
                        return format!("'SRID=4326;POINT({lon} {lat})'");
                    }
                }
                if normalized
                    .as_deref()
                    .map(|t| t.eq_ignore_ascii_case("uuid"))
                    .unwrap_or(false)
                {
                    if let Some(uuid) = objectid_hex_to_uuid(v) {
                        return cast_string_literal(&uuid, sql_type);
                    }
                }
                format!("'{}'", escape_sql_string(v))
            }
            Value::Array(items) => {
                let elements = items
                    .iter()
                    .map(|item| match item {
                        Value::Null => "NULL".to_owned(),
                        Value::Bool(v) => {
                            if *v {
                                "TRUE".to_owned()
                            } else {
                                "FALSE".to_owned()
                            }
                        }
                        Value::Number(v) => v.to_string(),
                        Value::String(v) => format!("'{}'", escape_sql_string(v)),
                        other => format!("'{}'", escape_sql_string(&other.to_string())),
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                if let Some(array_type) = normalized_sql_type(sql_type).filter(|t| t.contains("[]"))
                {
                    format!("ARRAY[{elements}]::{array_type}")
                } else {
                    format!("'{}'", escape_sql_string(&value.to_string()))
                }
            }
            Value::Object(_) => {
                if normalized_sql_type(sql_type)
                    .as_deref()
                    .map(|t| t.to_ascii_lowercase().starts_with("geometry"))
                    .unwrap_or(false)
                {
                    if let Some((lon, lat)) = geojson_point_coordinates(value) {
                        return format!("'SRID=4326;POINT({lon} {lat})'");
                    }
                }
                if let Some(mapped) = map_extended_json_literal(value, sql_type) {
                    return mapped;
                }
                let as_json = value.to_string();
                match normalized_sql_type(sql_type) {
                    Some(t) if t.to_ascii_lowercase().contains("json") => {
                        format!("'{}'::{t}", escape_sql_string(&as_json))
                    }
                    _ => format!("'{}'", escape_sql_string(&as_json)),
                }
            }
        }
    }

    fn varchar_limit(sql_type: Option<&str>) -> Option<usize> {
        let sql = normalized_sql_type(sql_type)?.to_ascii_lowercase();
        let prefix = if sql.starts_with("varchar(") {
            "varchar("
        } else if sql.starts_with("character varying(") {
            "character varying("
        } else {
            return None;
        };
        let rest = &sql[prefix.len()..];
        let end = rest.find(')')?;
        rest[..end].trim().parse::<usize>().ok()
    }

    fn validate_varchar_value(
        value: Option<&Value>,
        sql_type: Option<&str>,
        source_field: &str,
        target_field: &str,
        table_name: &str,
    ) -> Result<()> {
        let value = value.map(unwrap_union_tagged_value);
        let Some(limit) = varchar_limit(sql_type) else {
            return Ok(());
        };
        let Some(Value::String(text)) = value else {
            return Ok(());
        };
        let value_len = text.chars().count();
        if value_len > limit {
            return Err(anyhow!(
                "value exceeds varchar limit: source_field={} target_field={} table={} sql_type={} value_len={} limit={} value_sample={}",
                source_field,
                target_field,
                table_name,
                sql_type.unwrap_or("varchar"),
                value_len,
                limit,
                text.chars().take(80).collect::<String>()
            ));
        }
        Ok(())
    }

    fn validate_required_mapped_value(
        value: Option<&Value>,
        nullable: bool,
        source_field: &str,
        target_field: &str,
        table_name: &str,
    ) -> Result<()> {
        let value = value.map(unwrap_union_tagged_value);
        if nullable {
            return Ok(());
        }
        if value.is_none() || value.is_some_and(Value::is_null) {
            return Err(anyhow!(
                "missing required mapped field: source_field={} target_field={} table={} (value is null or missing)",
                source_field,
                target_field,
                table_name,
            ));
        }
        Ok(())
    }

    fn is_missing_required_mapped_field_error(err: &anyhow::Error) -> bool {
        err.chain()
            .any(|cause| cause.to_string().contains("missing required mapped field:"))
    }

    fn parse_missing_required_mapped_field_error(
        err: &anyhow::Error,
    ) -> Option<(String, String, String)> {
        let marker = "missing required mapped field:";
        let msg = err
            .chain()
            .map(std::string::ToString::to_string)
            .find(|line| line.contains(marker))?;
        let detail = msg
            .split_once(marker)
            .map(|(_, rhs)| rhs.trim())
            .unwrap_or("");

        let extract = |key: &str| -> Option<String> {
            let token = format!("{key}=");
            let start = detail.find(&token)? + token.len();
            let tail = &detail[start..];
            let end = tail.find(' ').unwrap_or(tail.len());
            Some(tail[..end].trim().to_owned())
        };

        let source_field = extract("source_field")?;
        let target_field = extract("target_field")?;
        let table_name = extract("table")?;
        Some((source_field, target_field, table_name))
    }

    fn is_duplicate_key_error(err: &anyhow::Error) -> bool {
        err.chain().any(|cause| {
            let msg = cause.to_string();
            msg.contains("duplicate key value violates unique constraint")
                || msg.contains("SQLSTATE 23505")
        })
    }

    fn resolve_source_field_value<'a>(
        payload_doc: &'a Value,
        payload_obj: &'a serde_json::Map<String, Value>,
        source_field: &str,
    ) -> Option<&'a Value> {
        if let Some(value) = payload_obj.get(source_field) {
            return Some(value);
        }

        if source_field.contains('.') {
            let segments = source_field.split('.').collect::<Vec<_>>();
            if let Some(value) = values_at_path(payload_doc, &segments).into_iter().next() {
                return Some(value);
            }

            // For dotted paths we still allow a nested fallback to support
            // flattened payload variants from some connectors.
            return find_value_in_nested_json_object(payload_obj, source_field);
        }

        // For scalar root fields (for example `active`) do not recurse into nested
        // objects: nested siblings can contain the same field name and cause wrong
        // values to be applied to the root row.
        None
    }

    fn resolve_source_field_value_from_map<'a>(
        payload_obj: &'a serde_json::Map<String, Value>,
        source_field: &str,
    ) -> Option<&'a Value> {
        fn value_at_segments<'a>(
            current: &'a serde_json::Map<String, Value>,
            segments: &[&str],
        ) -> Option<&'a Value> {
            let (head, tail) = segments.split_first()?;
            let value = current.get(*head)?;
            if tail.is_empty() {
                return Some(value);
            }
            match value {
                Value::Object(child) => value_at_segments(child, tail),
                _ => None,
            }
        }

        if let Some(value) = payload_obj.get(source_field) {
            return Some(value);
        }

        if source_field.contains('.') {
            let segments = source_field.split('.').collect::<Vec<_>>();
            if let Some(value) = value_at_segments(payload_obj, &segments) {
                return Some(value);
            }

            // Child rows can be flattened from nested objects (for example
            // atmosphericCondition.quality -> { value, quality }).
            // If full dotted path is absent, try progressively shorter suffixes.
            for idx in 1..segments.len() {
                let suffix = &segments[idx..];
                if let Some(value) = value_at_segments(payload_obj, suffix) {
                    return Some(value);
                }
                if suffix.len() == 1 {
                    if let Some(value) = payload_obj.get(suffix[0]) {
                        return Some(value);
                    }
                }
            }
        }

        find_value_in_nested_json_object(payload_obj, source_field)
    }

    fn infer_grouped_key_value(
        mapping: &CollectionMapping,
        collection_name: &str,
    ) -> Option<String> {
        let table_name = sanitize_name(&mapping.pg_mapping.table_name);
        let collection_name = sanitize_name(collection_name);
        collection_name
            .strip_prefix(&format!("{table_name}_"))
            .filter(|suffix| !suffix.is_empty())
            .map(|suffix| suffix.to_owned())
    }

    fn debezium_document(value: Option<&Value>) -> Result<Option<Value>> {
        let Some(value) = value else {
            return Ok(None);
        };

        let value = unwrap_union_tagged_value(value);
        match value {
            Value::Object(_) => Ok(Some(value.clone())),
            Value::String(raw) => {
                let parsed: Value = match serde_json::from_str(raw) {
                    Ok(parsed) => parsed,
                    Err(primary_err) => json5::from_str(raw)
                        .with_context(|| {
                            format!(
                                "Failed to parse Debezium document JSON string (strict JSON error: {primary_err})"
                            )
                        })?,
                };
                if parsed.is_object() {
                    Ok(Some(parsed))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    fn column_sql_type<'a>(mapping: &'a CollectionMapping, target_field: &str) -> Option<&'a str> {
        let normalized_target = normalize_pg_identifier(target_field);
        mapping
            .pg_mapping
            .ddl
            .as_ref()?
            .columns
            .iter()
            .find(|column| normalize_pg_identifier(&column.name) == normalized_target)
            .map(|column| column.sql_type.as_str())
    }

    fn qualified_table_name(mapping: &CollectionMapping, fallback_schema: Option<&str>) -> String {
        let schema = fallback_schema
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .or_else(|| {
                let mapped = mapping.pg_mapping.schema_name.as_str().trim();
                if mapped.is_empty() {
                    None
                } else {
                    Some(mapped)
                }
            });
        match schema {
            Some(s) => format!(
                "{}.{}",
                quote_ident(s),
                quote_ident(&mapping.pg_mapping.table_name)
            ),
            None => quote_ident(&mapping.pg_mapping.table_name),
        }
    }

    fn root_primary_key(mapping: &CollectionMapping) -> Option<String> {
        mapping
            .pg_mapping
            .ddl
            .as_ref()?
            .columns
            .iter()
            .find(|column| column.primary_key)
            .map(|column| normalize_pg_identifier(&column.name))
    }

    fn root_source_field_for_pk(mapping: &CollectionMapping, pk: &str) -> Option<String> {
        mapping
            .pg_mapping
            .columns
            .iter()
            .find(|column| {
                normalize_pg_identifier(&column.target_field) == normalize_pg_identifier(pk)
            })
            .map(|column| column.source_field.clone())
    }

    fn infer_error_target_field(err: &tokio_postgres::Error) -> Option<String> {
        if let Some(column) = err.as_db_error().and_then(|db_err| db_err.column()) {
            return Some(normalize_pg_identifier(column));
        }

        let message = err
            .as_db_error()
            .map(|db_err| db_err.message())
            .unwrap_or_default();
        let marker = "column \"";
        let start = message.find(marker)? + marker.len();
        let end = message[start..].find('"')?;
        Some(normalize_pg_identifier(&message[start..start + end]))
    }

    fn source_field_for_target(mapping: &CollectionMapping, target_field: &str) -> String {
        mapping
            .pg_mapping
            .columns
            .iter()
            .find(|column| {
                normalize_pg_identifier(&column.target_field)
                    == normalize_pg_identifier(target_field)
            })
            .map(|column| column.source_field.clone())
            .unwrap_or_else(|| target_field.to_owned())
    }

    fn annotate_apply_db_error(
        err: tokio_postgres::Error,
        mapping: &CollectionMapping,
        table_name: &str,
    ) -> anyhow::Error {
        let formatted = format_postgres_error(&err);
        if let Some(target_field) = infer_error_target_field(&err) {
            let source_field = source_field_for_target(mapping, &target_field);
            anyhow!(
                "db error: {}\nDETAIL: source_field={} target_field={} table={}",
                formatted,
                source_field,
                target_field,
                table_name
            )
        } else {
            anyhow!("db error: {}", formatted)
        }
    }

    fn direct_children_for_parent<'a>(
        mappings: &'a [CollectionMapping],
        parent_table: &str,
    ) -> Vec<(&'a CollectionMapping, &'a DdlForeignKeyMapping)> {
        mappings
            .iter()
            .filter(|mapping| !is_root_mapping(mapping))
            .filter_map(|mapping| {
                let ddl = mapping.pg_mapping.ddl.as_ref()?;
                let fk = ddl
                    .foreign_keys
                    .iter()
                    .find(|fk| fk.to_table == parent_table && fk.to_col == "id")?;
                Some((mapping, fk))
            })
            .collect()
    }

    fn mapping_fk_depth_to_root(
        mapping: &CollectionMapping,
        mappings: &[CollectionMapping],
        root_table_name: &str,
    ) -> usize {
        let mut depth = 0usize;
        let mut current = mapping;

        while current.pg_mapping.table_name != root_table_name {
            let Some(ddl) = current.pg_mapping.ddl.as_ref() else {
                break;
            };
            let Some(fk) = ddl.foreign_keys.iter().find(|fk| fk.to_col == "id") else {
                break;
            };
            let Some(parent) = mappings
                .iter()
                .find(|candidate| candidate.pg_mapping.table_name == fk.to_table)
            else {
                break;
            };
            depth += 1;
            current = parent;
        }

        depth
    }

    fn mapping_pk_column(mapping: &CollectionMapping) -> Option<String> {
        mapping
            .pg_mapping
            .ddl
            .as_ref()?
            .columns
            .iter()
            .find(|column| column.primary_key)
            .map(|column| normalize_pg_identifier(&column.name))
    }

    fn child_segments_relative_to_parent(
        child_mapping: &CollectionMapping,
        parent_mapping: &CollectionMapping,
    ) -> Vec<String> {
        let child_segments = mongo_path_segments(child_mapping.mongo_path.as_deref())
            .into_iter()
            .map(|segment| segment.to_owned())
            .collect::<Vec<_>>();
        let parent_segments = mongo_path_segments(parent_mapping.mongo_path.as_deref())
            .into_iter()
            .map(|segment| segment.to_owned())
            .collect::<Vec<_>>();

        if child_segments.len() >= parent_segments.len()
            && child_segments
                .iter()
                .take(parent_segments.len())
                .eq(parent_segments.iter())
        {
            child_segments[parent_segments.len()..].to_vec()
        } else {
            child_segments
        }
    }
    fn build_delete_sql_for_mapping(
        mapping: &CollectionMapping,
        mappings: &[CollectionMapping],
        root_table_name: &str,
        root_pk_literal: &str,
        fallback_schema: Option<&str>,
    ) -> Option<String> {
        let mut chain: Vec<(&CollectionMapping, &DdlForeignKeyMapping)> = Vec::new();
        let mut current = mapping;

        while current.pg_mapping.table_name != root_table_name {
            let ddl = current.pg_mapping.ddl.as_ref()?;
            let fk = ddl.foreign_keys.iter().find(|fk| fk.to_col == "id")?;
            chain.push((current, fk));
            current = mappings
                .iter()
                .find(|candidate| candidate.pg_mapping.table_name == fk.to_table)?;
        }

        if chain.is_empty() {
            return None;
        }

        let from = format!(
            "{} AS t0",
            qualified_table_name(chain[0].0, fallback_schema)
        );
        let using_clause = if chain.len() > 1 {
            let using_tables = chain
                .iter()
                .enumerate()
                .skip(1)
                .map(|(idx, (table_mapping, _))| {
                    format!(
                        "{} AS t{}",
                        qualified_table_name(table_mapping, fallback_schema),
                        idx
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!(" USING {using_tables}")
        } else {
            String::new()
        };

        let mut predicates = Vec::new();
        for idx in 0..chain.len().saturating_sub(1) {
            let fk_col = &chain[idx].1.from_col;
            predicates.push(format!("t{idx}.{} = t{}.id", quote_ident(fk_col), idx + 1));
        }
        let last_idx = chain.len() - 1;
        let last_fk = &chain[last_idx].1.from_col;
        predicates.push(format!(
            "t{last_idx}.{} = {root_pk_literal}",
            quote_ident(last_fk)
        ));

        Some(format!(
            "DELETE FROM {from}{using_clause} WHERE {}",
            predicates.join(" AND ")
        ))
    }

    fn load_collection_mapping_folders(
        collections_dir: &Path,
    ) -> Result<HashMap<String, Vec<CollectionMapping>>> {
        fn load_mapping_files_in_dir(dir: &Path) -> Result<Vec<CollectionMapping>> {
            let mut mappings = Vec::new();
            for file in
                std::fs::read_dir(dir).with_context(|| format!("Cannot read {}", dir.display()))?
            {
                let file = file?;
                let file_path = file.path();
                if !file_path.is_file() {
                    continue;
                }
                let Some(name) = file_path.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                if !name.starts_with("mapping_") || !name.ends_with(".yaml") {
                    continue;
                }
                let content = std::fs::read_to_string(&file_path)
                    .with_context(|| format!("Failed to read {}", file_path.display()))?;
                let mapping: CollectionMapping = serde_yaml::from_str(&content)
                    .with_context(|| format!("Failed to parse {}", file_path.display()))?;
                mappings.push(mapping);
            }
            Ok(mappings)
        }

        let mut by_collection: HashMap<String, Vec<CollectionMapping>> = HashMap::new();

        // Support layouts where `collections_dir` is already a concrete collection
        // folder containing mapping_*.yaml files directly.
        let direct_mappings = load_mapping_files_in_dir(collections_dir)?;
        if !direct_mappings.is_empty() {
            if let Some(dir_name) = collections_dir.file_name().and_then(|name| name.to_str()) {
                // When mappings are loaded from a concrete collection folder
                // (e.g. source/collections/data), all mapping_*.yaml files in
                // that folder belong to the same Kafka collection/topic.
                let key = sanitize_name(dir_name);
                by_collection
                    .entry(key)
                    .or_default()
                    .extend(direct_mappings);
            } else {
                // Fallback for unexpected paths without a terminal folder name.
                for mapping in direct_mappings {
                    let key = sanitize_name(&mapping.collection_name);
                    by_collection.entry(key).or_default().push(mapping);
                }
            }
        }

        for entry in std::fs::read_dir(collections_dir)
            .with_context(|| format!("Cannot read {}", collections_dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let folder_name = entry.file_name().to_string_lossy().to_string();
            let mappings = load_mapping_files_in_dir(&path)?;
            if !mappings.is_empty() {
                by_collection
                    .entry(sanitize_name(&folder_name))
                    .or_insert(mappings);
            }
        }
        Ok(by_collection)
    }

    async fn resolve_kafka_import_metadata_dirs(
        conf_path: &Path,
        conf: &crate::util::ConfData,
        db_name: &str,
    ) -> Result<(PathBuf, PathBuf, Option<tempfile::TempDir>)> {
        let storage_backend =
            resolve_export_write_backend(&conf.base_dir).unwrap_or(ExportWriteBackend::LocalFs);

        let mut project_root = match &storage_backend {
            ExportWriteBackend::LocalFs => configured_project_root(conf),
            ExportWriteBackend::Gcs { .. } => {
                let local_root = resolve_local_project_root_from_config(conf_path, conf);
                debug!(
                    "kafka-import metadata root (local, read-only): {}",
                    local_root.display()
                );
                local_root
            }
        };

        let mut tables_root = project_root.join("schema").join("tables");
        let mut tables_dir = if tables_root.join(db_name).is_dir() {
            tables_root.join(db_name)
        } else {
            tables_root.clone()
        };
        let mut collections_dir = resolve_collections_dir(&project_root, db_name);
        let mut metadata_stage = None;

        if matches!(&storage_backend, ExportWriteBackend::Gcs { .. }) {
            if let ExportWriteBackend::Gcs { bucket, prefix } = &storage_backend {
                if let Some(stage) = stage_export_metadata_from_gcs(
                    bucket,
                    prefix,
                    conf.cluster_name.as_deref(),
                    &conf.project_dir,
                    db_name,
                )
                .await?
                {
                    project_root = stage.path().to_path_buf();
                    tables_root = project_root.join("schema").join("tables");
                    tables_dir = if tables_root.join(db_name).is_dir() {
                        tables_root.join(db_name)
                    } else {
                        tables_root.clone()
                    };
                    collections_dir = resolve_collections_dir(&project_root, db_name);

                    info!(
                        "kafka-import metadata staged from GCS into temporary directory {}",
                        project_root.display()
                    );

                    metadata_stage = Some(stage);
                }
            }
        }

        Ok((tables_dir, collections_dir, metadata_stage))
    }

    async fn traced_pg_execute<C>(
        pg_client: &C,
        sql: &str,
        _sql_kind: &str,
        _table: &str,
    ) -> std::result::Result<u64, tokio_postgres::Error>
    where
        C: tokio_postgres::GenericClient + Sync,
    {
        pg_client.execute(sql, &[]).await
    }

    async fn traced_pg_query_one<C>(
        pg_client: &C,
        sql: &str,
        _sql_kind: &str,
        _table: &str,
    ) -> std::result::Result<tokio_postgres::Row, tokio_postgres::Error>
    where
        C: tokio_postgres::GenericClient + Sync,
    {
        pg_client.query_one(sql, &[]).await
    }

    async fn traced_pg_batch_execute(
        pg_client: &tokio_postgres::Client,
        sql: &str,
        _sql_kind: &str,
        _source: &str,
    ) -> std::result::Result<(), tokio_postgres::Error> {
        pg_client.batch_execute(sql).await
    }

    async fn apply_upsert_event<C>(
        pg_client: &C,
        payload_doc: &Value,
        mappings: &[CollectionMapping],
        fallback_schema: Option<&str>,
        collection_name: &str,
        table_insert_execs: &mut HashMap<String, u64>,
    ) -> Result<u64>
    where
        C: tokio_postgres::GenericClient + Sync,
    {
        let root_mapping = mappings
            .iter()
            .find(|mapping| is_root_mapping(mapping))
            .ok_or_else(|| anyhow!("No root mapping (mongo_path: .) found"))?;
        let payload_obj = payload_doc
            .as_object()
            .ok_or_else(|| anyhow!("Kafka upsert payload must be a JSON object"))?;
        let root_table = qualified_table_name(root_mapping, fallback_schema);
        let mut affected_rows = 0_u64;

        let mut mapped_target_fields = std::collections::HashSet::new();
        let mut columns = Vec::new();
        let mut values = Vec::new();
        for column in &root_mapping.pg_mapping.columns {
            let normalized_target_field = normalize_pg_identifier(&column.target_field);
            let mut literal_fallback: Option<Value> = None;
            if let Some(literal) = column.literal_value.as_deref() {
                literal_fallback = Some(Value::String(literal.to_owned()));
            } else if normalized_target_field == "_key" {
                literal_fallback =
                    infer_grouped_key_value(root_mapping, collection_name).map(Value::String);
            }

            let raw_value =
                resolve_source_field_value(payload_doc, payload_obj, &column.source_field)
                    .or(literal_fallback.as_ref());
            let sql_type = column_sql_type(root_mapping, &normalized_target_field);
            let (resolved_value, effective_source_field) = resolve_value_with_numeric_id_fallback(
                payload_obj,
                root_mapping,
                &column.source_field,
                &normalized_target_field,
                sql_type,
                raw_value,
            );
            validate_required_mapped_value(
                resolved_value,
                column.nullable,
                &effective_source_field,
                &normalized_target_field,
                &root_table,
            )?;
            validate_extended_json_compatibility(
                resolved_value,
                sql_type,
                &effective_source_field,
                &normalized_target_field,
                &root_table,
            )?;
            validate_varchar_value(
                resolved_value,
                sql_type,
                &effective_source_field,
                &normalized_target_field,
                &root_table,
            )?;
            mapped_target_fields.insert(normalized_target_field.clone());
            columns.push(quote_ident(&normalized_target_field));
            values.push(sql_literal(resolved_value, sql_type));
        }

        if let Some(ddl) = root_mapping.pg_mapping.ddl.as_ref() {
            for ddl_column in &ddl.columns {
                let target_field = normalize_pg_identifier(&ddl_column.name);
                if mapped_target_fields.contains(&target_field) || target_field == "id" {
                    continue;
                }

                let resolved_value =
                    resolve_source_field_value(payload_doc, payload_obj, &target_field);
                if resolved_value.is_none() {
                    continue;
                }

                validate_required_mapped_value(
                    resolved_value,
                    ddl_column.nullable,
                    &target_field,
                    &target_field,
                    &root_table,
                )?;
                validate_extended_json_compatibility(
                    resolved_value,
                    Some(&ddl_column.sql_type),
                    &target_field,
                    &target_field,
                    &root_table,
                )?;
                validate_varchar_value(
                    resolved_value,
                    Some(&ddl_column.sql_type),
                    &target_field,
                    &target_field,
                    &root_table,
                )?;

                mapped_target_fields.insert(target_field.clone());
                columns.push(quote_ident(&target_field));
                values.push(sql_literal(resolved_value, Some(&ddl_column.sql_type)));
            }
        }

        let root_pk = root_primary_key(root_mapping)
            .ok_or_else(|| anyhow!("Root mapping has no primary key in ddl"))?;
        let mut update_targets = columns
            .iter()
            .map(|column| normalize_pg_identifier(column))
            .filter(|target_field| target_field != &root_pk)
            .collect::<Vec<_>>();
        update_targets.sort();
        update_targets.dedup();

        let updates = update_targets
            .into_iter()
            .map(|target_field| {
                format!(
                    "{} = EXCLUDED.{}",
                    quote_ident(&target_field),
                    quote_ident(&target_field)
                )
            })
            .collect::<Vec<_>>();

        let upsert_sql = if updates.is_empty() {
            format!(
                "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING",
                root_table,
                columns.join(", "),
                values.join(", "),
                quote_ident(&root_pk),
            )
        } else {
            format!(
                "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}",
                root_table,
                columns.join(", "),
                values.join(", "),
                quote_ident(&root_pk),
                updates.join(", "),
            )
        };
        affected_rows += traced_pg_execute(
            pg_client,
            upsert_sql.as_str(),
            "upsert",
            root_table.as_str(),
        )
        .await
        .map_err(|err| annotate_apply_db_error(err, root_mapping, &root_table))?;
        *table_insert_execs.entry(root_table.clone()).or_insert(0) += 1;

        let root_pk_source = root_source_field_for_pk(root_mapping, &root_pk)
            .ok_or_else(|| anyhow!("Root mapping primary key not found in mapped columns"))?;
        let root_pk_sql_type = column_sql_type(root_mapping, &root_pk);
        let (root_pk_value, effective_root_pk_source) = resolve_value_with_numeric_id_fallback(
            payload_obj,
            root_mapping,
            &root_pk_source,
            &root_pk,
            root_pk_sql_type,
            resolve_source_field_value(payload_doc, payload_obj, &root_pk_source),
        );
        let root_pk_value = root_pk_value.ok_or_else(|| {
            anyhow!(
                "Root payload missing {} (resolved source field for target {} is {})",
                root_pk_source,
                root_pk,
                effective_root_pk_source
            )
        })?;

        let root_pk_literal = sql_literal(Some(root_pk_value), root_pk_sql_type);
        let mut inserted_tables = std::collections::HashSet::new();
        inserted_tables.insert(root_mapping.pg_mapping.table_name.clone());

        let mut non_root_mappings = mappings
            .iter()
            .filter(|mapping| !is_root_mapping(mapping))
            .collect::<Vec<_>>();
        non_root_mappings.sort_by_key(|mapping| {
            std::cmp::Reverse(mapping_fk_depth_to_root(
                mapping,
                mappings,
                &root_mapping.pg_mapping.table_name,
            ))
        });
        for mapping in non_root_mappings {
            let mapping_table = qualified_table_name(mapping, fallback_schema);
            if let Some(delete_sql) = build_delete_sql_for_mapping(
                mapping,
                mappings,
                &root_mapping.pg_mapping.table_name,
                &root_pk_literal,
                fallback_schema,
            ) {
                traced_pg_execute(
                    pg_client,
                    delete_sql.as_str(),
                    "delete_children",
                    mapping_table.as_str(),
                )
                .await?;
            }
        }

        let mut pending = vec![(root_mapping, payload_doc.clone(), root_pk_literal)];

        while let Some((parent_mapping, parent_node, parent_pk_literal)) = pending.pop() {
            let children =
                direct_children_for_parent(mappings, &parent_mapping.pg_mapping.table_name);
            for (child_mapping, fk) in children {
                ensure_parent_inserted_before_child(
                    parent_mapping,
                    child_mapping,
                    fk,
                    &inserted_tables,
                )?;
                let child_table = qualified_table_name(child_mapping, fallback_schema);
                let relative_segments =
                    child_segments_relative_to_parent(child_mapping, parent_mapping);
                let relative_refs = relative_segments
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                let child_nodes = values_at_path(&parent_node, &relative_refs);
                let child_pk = mapping_pk_column(child_mapping)
                    .ok_or_else(|| anyhow!("Mapping {} has no primary key", child_table))?;
                let child_pk_sql_type = column_sql_type(child_mapping, &child_pk);

                for node in child_nodes {
                    for obj in child_row_objects_for_mapping(node, child_mapping) {
                        let mapped_values = child_mapping
                            .pg_mapping
                            .columns
                            .iter()
                            .map(|mapped| {
                                let normalized_target =
                                    normalize_pg_identifier(&mapped.target_field);
                                let mut literal_fallback: Option<Value> = None;
                                if let Some(literal) = mapped.literal_value.as_deref() {
                                    literal_fallback = Some(Value::String(literal.to_owned()));
                                } else if normalized_target == "_key" {
                                    literal_fallback =
                                        infer_grouped_key_value(child_mapping, collection_name)
                                            .map(Value::String);
                                }

                                let val =
                                    resolve_source_field_value_from_map(&obj, &mapped.source_field)
                                        .or(literal_fallback.as_ref())
                                        .cloned();
                                (mapped, val)
                            })
                            .collect::<Vec<_>>();

                        let mut child_columns = vec![quote_ident(&fk.from_col)];
                        let mut child_values = vec![parent_pk_literal.clone()];
                        for (mapped, val) in mapped_values {
                            child_columns.push(quote_ident(&mapped.target_field));
                            if val.is_none() && !mapped.nullable {
                                let available_keys =
                                    obj.keys().take(32).cloned().collect::<Vec<_>>().join(", ");
                                let node_preview =
                                    serde_json::to_string(&Value::Object(obj.clone()))
                                        .unwrap_or_else(|_| "<unserializable-node>".to_owned());
                                return Err(anyhow!(
                                    "missing required child mapped field: table={} source_field={} target_field={} mongo_path={} available_keys=[{}] node={}",
                                    child_table,
                                    mapped.source_field,
                                    mapped.target_field,
                                    child_mapping.mongo_path.as_deref().unwrap_or("."),
                                    available_keys,
                                    node_preview,
                                ));
                            }
                            let sql_type = column_sql_type(child_mapping, &mapped.target_field);
                            validate_extended_json_compatibility(
                                val.as_ref(),
                                sql_type,
                                &mapped.source_field,
                                &mapped.target_field,
                                &child_table,
                            )?;
                            validate_varchar_value(
                                val.as_ref(),
                                sql_type,
                                &mapped.source_field,
                                &mapped.target_field,
                                &child_table,
                            )?;
                            child_values.push(sql_literal(val.as_ref(), sql_type));
                        }

                        let insert_sql = format!(
                            "INSERT INTO {} ({}) VALUES ({}) RETURNING {}::text",
                            child_table,
                            child_columns.join(", "),
                            child_values.join(", "),
                            quote_ident(&child_pk)
                        );
                        let row = traced_pg_query_one(
                            pg_client,
                            insert_sql.as_str(),
                            "insert_returning",
                            child_table.as_str(),
                        )
                        .await
                        .map_err(|err| annotate_apply_db_error(err, child_mapping, &child_table))?;
                        let inserted_pk_text: String = row.try_get(0)?;

                        affected_rows += 1;
                        *table_insert_execs.entry(child_table.clone()).or_insert(0) += 1;
                        inserted_tables.insert(child_mapping.pg_mapping.table_name.clone());

                        let inserted_pk_literal =
                            cast_string_literal(&inserted_pk_text, child_pk_sql_type);
                        pending.push((child_mapping, Value::Object(obj), inserted_pk_literal));
                    }
                }
            }
        }

        Ok(affected_rows)
    }

    async fn apply_delete_event<C>(
        pg_client: &C,
        before_doc: &Value,
        mappings: &[CollectionMapping],
        fallback_schema: Option<&str>,
    ) -> Result<()>
    where
        C: tokio_postgres::GenericClient + Sync,
    {
        let root_mapping = mappings
            .iter()
            .find(|mapping| is_root_mapping(mapping))
            .ok_or_else(|| anyhow!("No root mapping (mongo_path: .) found"))?;
        let root_pk = root_primary_key(root_mapping)
            .ok_or_else(|| anyhow!("Root mapping has no primary key in ddl"))?;
        let root_pk_source = root_source_field_for_pk(root_mapping, &root_pk)
            .ok_or_else(|| anyhow!("Root mapping primary key not found in mapped columns"))?;
        let root_pk_value = before_doc
            .as_object()
            .and_then(|obj| obj.get(&root_pk_source))
            .ok_or_else(|| anyhow!("Delete payload missing {}", root_pk_source))?;

        let root_pk_sql_type = column_sql_type(root_mapping, &root_pk);
        let root_pk_literal = sql_literal(Some(root_pk_value), root_pk_sql_type);

        let mut non_root_mappings = mappings
            .iter()
            .filter(|mapping| !is_root_mapping(mapping))
            .collect::<Vec<_>>();
        non_root_mappings.sort_by_key(|mapping| {
            std::cmp::Reverse(mapping_fk_depth_to_root(
                mapping,
                mappings,
                &root_mapping.pg_mapping.table_name,
            ))
        });
        for mapping in non_root_mappings {
            let mapping_table = qualified_table_name(mapping, fallback_schema);
            if let Some(delete_sql) = build_delete_sql_for_mapping(
                mapping,
                mappings,
                &root_mapping.pg_mapping.table_name,
                &root_pk_literal,
                fallback_schema,
            ) {
                traced_pg_execute(
                    pg_client,
                    delete_sql.as_str(),
                    "delete_children",
                    mapping_table.as_str(),
                )
                .await?;
            }
        }

        let root_table = qualified_table_name(root_mapping, fallback_schema);
        let delete_root_sql = format!(
            "DELETE FROM {} WHERE {} = {}",
            root_table,
            quote_ident(&root_pk),
            root_pk_literal
        );
        traced_pg_execute(
            pg_client,
            delete_root_sql.as_str(),
            "delete_root",
            root_table.as_str(),
        )
        .await?;
        Ok(())
    }

    async fn bootstrap_pg_objects_for_kafka_import(
        // admin_client: &tokio_postgres::Client,
        pg_client: &tokio_postgres::Client,
        tables_dir: &Path,
        fallback_schema: Option<&str>,
        db_name: &str,
        reuse_existing_tables: bool,
    ) -> Result<()> {
        if !tables_dir.is_dir() {
            return Err(anyhow!(
                "Cannot read SQL tables directory {}",
                tables_dir.display()
            ));
        }

        let mut sql_files: Vec<PathBuf> = std::fs::read_dir(tables_dir)
            .with_context(|| format!("Cannot read {}", tables_dir.display()))?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("sql"))
            .collect();
        sql_files.sort();

        if sql_files.is_empty() {
            return Err(anyhow!(
                "No SQL files found in {}. Run to-pg first.",
                tables_dir.display()
            ));
        }

        // let mut ensured_databases = std::collections::HashSet::new();
        let mut existing_tables: Vec<(String, String)> = Vec::new();
        let mut ddl_files_with_existing_tables = std::collections::HashSet::new();

        for sql_path in &sql_files {
            let sql = std::fs::read_to_string(sql_path)
                .with_context(|| format!("Failed to read {}", sql_path.display()))?;

            if let Some(ddl_db_name) = extract_psql_database_name(&sql) {
                if ddl_db_name != db_name {
                    warn!(
                        "DDL file {} targets database '{}' while kafka-import target database is '{}'",
                        sql_path.display(),
                        ddl_db_name,
                        db_name
                    );
                }
                // if ensured_databases.insert(ddl_db_name.clone()) {
                //     ensure_pg_database(admin_client, &ddl_db_name).await?;
                // }
            }

            let executable_sql = strip_psql_preamble(&sql);
            if executable_sql.trim().is_empty() {
                continue;
            }

            let parsed_tables = parse_sql(&executable_sql);
            let file_schema =
                extract_search_path(&executable_sql).or_else(|| fallback_schema.map(str::to_owned));

            // if let Some(schema_name) = file_schema.as_deref() {
            //     ensure_pg_schema(pg_client, schema_name).await?;
            // }

            for table in &parsed_tables {
                let qualified = match file_schema.as_deref() {
                    Some(schema) => format!("{schema}.{}", table.name),
                    None => table.name.clone(),
                };
                let row = pg_client
                    .query_one("SELECT to_regclass($1)::text", &[&qualified])
                    .await
                    .with_context(|| format!("Failed to check existing table {}", qualified))?;
                let exists: Option<String> = row.try_get(0).with_context(|| {
                    format!(
                        "Failed to read to_regclass result while checking existing table {}",
                        qualified
                    )
                })?;
                if exists.is_some() {
                    let schema_name = file_schema.clone().unwrap_or_else(|| "public".to_owned());
                    if reuse_existing_tables {
                        ddl_files_with_existing_tables.insert(sql_path.clone());
                    } else {
                        existing_tables.push((schema_name, table.name.clone()));
                    }
                }
            }
        }

        if !existing_tables.is_empty() {
            return Err(preflight_existing_tables_error(db_name, &existing_tables));
        }

        if reuse_existing_tables && !ddl_files_with_existing_tables.is_empty() {
            warn!(
                "--force enabled: reusing existing destination tables and skipping DDL execution for {} file(s)",
                ddl_files_with_existing_tables.len()
            );
        }

        for sql_path in &sql_files {
            if reuse_existing_tables && ddl_files_with_existing_tables.contains(sql_path) {
                info!(
                    "Reusing existing PostgreSQL objects from {} (--force): skipped DDL execution",
                    sql_path.display()
                );
                continue;
            }

            let sql = std::fs::read_to_string(sql_path)
                .with_context(|| format!("Failed to read {}", sql_path.display()))?;
            let executable_sql = strip_psql_preamble(&sql);
            if executable_sql.trim().is_empty() {
                continue;
            }

            let sql_path_text = sql_path.display().to_string();
            match traced_pg_batch_execute(
                pg_client,
                &executable_sql,
                "ddl_apply",
                sql_path_text.as_str(),
            )
            .await
            {
                Ok(()) => {
                    info!("Created PostgreSQL objects from {}", sql_path.display());
                }
                Err(err) if is_missing_postgis_control_file(&err) => {
                    let fallback_sql = strip_postgis_extension_statement(&executable_sql);
                    match traced_pg_batch_execute(
                        pg_client,
                        &fallback_sql,
                        "ddl_apply_without_postgis",
                        sql_path_text.as_str(),
                    )
                    .await
                    {
                        Ok(()) => {
                            info!(
                                "Created PostgreSQL objects from {} (without PostGIS extension statement)",
                                sql_path.display()
                            );
                        }
                        Err(fallback_err) => {
                            return Err(anyhow!(
                                "Failed to execute {} after removing PostGIS extension statement\n{}",
                                sql_path.display(),
                                format_postgres_error(&fallback_err)
                            ));
                        }
                    }
                }
                Err(err) => {
                    return Err(anyhow!(
                        "Failed to execute {}\n{}",
                        sql_path.display(),
                        format_postgres_error(&err)
                    ));
                }
            }
        }

        Ok(())
    }

    async fn read_msg_from_topics<S>(
        stream: &mut S,
        idle_timeout: Duration,
    ) -> ReadStageEvent<S::Item>
    where
        S: Stream + Unpin,
    {
        match tokio::time::timeout(idle_timeout, stream.next()).await {
            Ok(Some(item)) => ReadStageEvent::Message(item),
            Ok(None) => ReadStageEvent::StreamEnded,
            Err(_) => ReadStageEvent::IdleTimeout,
        }
    }

    async fn write_to_pg(
        pg_client: &tokio_postgres::Client,
        payload: &Value,
        op: &str,
        before: Option<&Value>,
        after: Option<&Value>,
        mappings: &[CollectionMapping],
        collection_name: &str,
        topic: &str,
        effective_target_schema: Option<&str>,
        transaction_batching_enabled: bool,
        tx_pending_table_insert_execs: &mut HashMap<String, u64>,
        table_insert_execs: &mut HashMap<String, u64>,
        fallback_payload_as_after: &mut usize,
        skipped_missing_after: &mut usize,
        skipped_missing_before: &mut usize,
        skipped_missing_required_snapshot: &mut usize,
        skipped_missing_required_snapshot_by_table: &mut HashMap<String, usize>,
        skipped_missing_required_snapshot_samples: &mut Vec<String>,
        skipped_non_data_op: &mut usize,
    ) -> Result<WriteStageOutcome> {
        let result: Result<u64> = match op {
            "d" => {
                if let Some(before_doc) = before {
                    apply_delete_event(pg_client, before_doc, mappings, effective_target_schema)
                        .await
                        .map(|_| 0_u64)
                } else {
                    *skipped_missing_before += 1;
                    if *skipped_missing_before <= 5 {
                        warn!(
                            "kafka-import skipped message: reason=missing_before_for_delete topic={} collection={} op={} payload_keys={}",
                            topic,
                            collection_name,
                            op,
                            payload
                                .as_object()
                                .map(|obj| {
                                    obj.keys()
                                        .map(|k| k.as_str())
                                        .collect::<Vec<_>>()
                                        .join(",")
                                })
                                .unwrap_or_else(|| "<non-object>".to_owned())
                        );
                    } else if *skipped_missing_before == 6 {
                        warn!(
                            "missing-before skip log limit reached (5); suppressing additional per-row details"
                        );
                    }
                    return Ok(WriteStageOutcome::Skipped);
                }
            }
            "c" | "u" | "r" => {
                let fallback_after = payload
                    .as_object()
                    .filter(|obj| {
                        !obj.contains_key("after")
                            && !obj.contains_key("before")
                            && !obj.contains_key("op")
                            && !obj.contains_key("source")
                            && !obj.contains_key("transaction")
                    })
                    .map(|obj| Value::Object(obj.clone()));

                let after_doc_ref = if let Some(after_doc) = after {
                    Some(after_doc)
                } else {
                    fallback_after.as_ref()
                };

                if let Some(after_doc) = after_doc_ref {
                    if after.is_none() {
                        *fallback_payload_as_after += 1;
                    }

                    let upsert_result = if transaction_batching_enabled {
                        apply_upsert_event(
                            pg_client,
                            after_doc,
                            mappings,
                            effective_target_schema,
                            collection_name,
                            tx_pending_table_insert_execs,
                        )
                        .await
                    } else {
                        apply_upsert_event(
                            pg_client,
                            after_doc,
                            mappings,
                            effective_target_schema,
                            collection_name,
                            table_insert_execs,
                        )
                        .await
                    };

                    match upsert_result {
                        Ok(rows) => Ok(rows),
                        Err(err) if op == "r" && is_missing_required_mapped_field_error(&err) => {
                            *skipped_missing_required_snapshot += 1;
                            if let Some((source_field, target_field, table_name)) =
                                parse_missing_required_mapped_field_error(&err)
                            {
                                *skipped_missing_required_snapshot_by_table
                                    .entry(table_name.clone())
                                    .or_insert(0) += 1;
                                if skipped_missing_required_snapshot_samples.len() < 20 {
                                    skipped_missing_required_snapshot_samples.push(format!(
                                        "table={table_name} reason=missing_required_mapped_field source_field={source_field} target_field={target_field} topic={topic} collection={collection_name}"
                                    ));
                                }
                            }
                            if *skipped_missing_required_snapshot <= 5 {
                                warn!(
                                    "snapshot row skipped due to missing required mapped field: topic={} collection={} details={:#}",
                                    topic,
                                    collection_name,
                                    err
                                );
                            } else if *skipped_missing_required_snapshot == 6 {
                                warn!(
                                    "snapshot missing-required skip log limit reached (5); suppressing additional per-row details"
                                );
                            }
                            return Ok(WriteStageOutcome::Skipped);
                        }
                        Err(err) => Err(err),
                    }
                } else {
                    *skipped_missing_after += 1;
                    if *skipped_missing_after <= 5 {
                        warn!(
                            "kafka-import skipped message: reason=missing_after_for_data_op topic={} collection={} op={} payload_keys={} fallback_payload_as_after={}",
                            topic,
                            collection_name,
                            op,
                            payload
                                .as_object()
                                .map(|obj| {
                                    obj.keys()
                                        .map(|k| k.as_str())
                                        .collect::<Vec<_>>()
                                        .join(",")
                                })
                                .unwrap_or_else(|| "<non-object>".to_owned()),
                            fallback_after.is_some()
                        );
                    } else if *skipped_missing_after == 6 {
                        warn!(
                            "missing-after skip log limit reached (5); suppressing additional per-row details"
                        );
                    }
                    return Ok(WriteStageOutcome::Skipped);
                }
            }
            _ => {
                *skipped_non_data_op += 1;
                if *skipped_non_data_op <= 5 {
                    warn!(
                        "kafka-import skipped message: reason=non_data_op topic={} collection={} op={}",
                        topic,
                        collection_name,
                        op
                    );
                } else if *skipped_non_data_op == 6 {
                    warn!(
                        "non-data-op skip log limit reached (5); suppressing additional per-row details"
                    );
                }
                return Ok(WriteStageOutcome::Skipped);
            }
        };

        Ok(WriteStageOutcome::Applied(result?))
    }

    fn build_snapshot_copy_row(
        payload: &Value,
        after_doc: Option<&Value>,
        mappings: &[CollectionMapping],
        fallback_schema: Option<&str>,
        collection_name: &str,
    ) -> Result<SnapshotCopyRowBuildOutcome> {
        let root_mapping = mappings
            .iter()
            .find(|mapping| is_root_mapping(mapping))
            .ok_or_else(|| anyhow!("No root mapping (mongo_path: .) found"))?;
        let root_table = qualified_table_name(root_mapping, fallback_schema);

        if mappings.iter().any(|mapping| !is_root_mapping(mapping)) {
            return Ok(SnapshotCopyRowBuildOutcome::SkippedNonRootMapping {
                table_name: root_table,
            });
        }

        let root_doc = if let Some(after) = after_doc {
            after
        } else {
            payload
        };

        let payload_obj = root_doc
            .as_object()
            .ok_or_else(|| anyhow!("Kafka upsert payload must be a JSON object"))?;

        let mut columns = Vec::new();
        let mut cells = Vec::new();
        let mut sql_values = Vec::new();
        let mut normalized_targets = Vec::new();

        for column in &root_mapping.pg_mapping.columns {
            let normalized_target_field = normalize_pg_identifier(&column.target_field);
            let mut literal_fallback: Option<Value> = None;
            if let Some(literal) = column.literal_value.as_deref() {
                literal_fallback = Some(Value::String(literal.to_owned()));
            } else if normalized_target_field == "_key" {
                literal_fallback =
                    infer_grouped_key_value(root_mapping, collection_name).map(Value::String);
            }

            let raw_value = resolve_source_field_value(root_doc, payload_obj, &column.source_field)
                .or(literal_fallback.as_ref());
            let sql_type = column_sql_type(root_mapping, &normalized_target_field);
            let (resolved_value, effective_source_field) = resolve_value_with_numeric_id_fallback(
                payload_obj,
                root_mapping,
                &column.source_field,
                &normalized_target_field,
                sql_type,
                raw_value,
            );
            validate_required_mapped_value(
                resolved_value,
                column.nullable,
                &effective_source_field,
                &normalized_target_field,
                &root_table,
            )?;
            validate_extended_json_compatibility(
                resolved_value,
                sql_type,
                &effective_source_field,
                &normalized_target_field,
                &root_table,
            )?;
            validate_varchar_value(
                resolved_value,
                sql_type,
                &effective_source_field,
                &normalized_target_field,
                &root_table,
            )?;

            let literal_sql = sql_literal(resolved_value, sql_type);
            let Some(copy_cell) = sql_literal_to_copy_text(&literal_sql) else {
                return Ok(SnapshotCopyRowBuildOutcome::SkippedUnconvertibleLiteral {
                    table_name: root_table.clone(),
                    detail: format!(
                        "target_field={} source_field={} sql_type={} literal={}",
                        normalized_target_field,
                        effective_source_field,
                        sql_type.unwrap_or("unknown"),
                        literal_sql
                    ),
                });
            };

            columns.push(quote_ident(&normalized_target_field));
            cells.push(copy_cell);
            sql_values.push(literal_sql);
            normalized_targets.push(normalized_target_field);
        }

        if columns.is_empty() {
            return Ok(SnapshotCopyRowBuildOutcome::SkippedEmptyColumns {
                table_name: root_table,
            });
        }

        let root_pk = root_primary_key(root_mapping)
            .ok_or_else(|| anyhow!("Root mapping has no primary key in ddl"))?;
        let mut update_targets = normalized_targets
            .into_iter()
            .filter(|target_field| target_field != &root_pk)
            .collect::<Vec<_>>();
        update_targets.sort();
        update_targets.dedup();
        let updates = update_targets
            .iter()
            .map(|target_field| {
                format!(
                    "{} = EXCLUDED.{}",
                    quote_ident(target_field),
                    quote_ident(target_field)
                )
            })
            .collect::<Vec<_>>();
        let on_conflict_clause = if updates.is_empty() {
            format!("ON CONFLICT ({}) DO NOTHING", quote_ident(&root_pk))
        } else {
            format!(
                "ON CONFLICT ({}) DO UPDATE SET {}",
                quote_ident(&root_pk),
                updates.join(", ")
            )
        };

        let row = cells
            .iter()
            .map(|cell| match cell {
                Some(value) => csv_escape_copy_field(value),
                None => "\\N".to_owned(),
            })
            .collect::<Vec<_>>()
            .join(",");

        let sql_row = format!("({})", sql_values.join(", "));

        Ok(SnapshotCopyRowBuildOutcome::Built((
            root_table,
            columns,
            row,
            sql_row,
            on_conflict_clause,
        )))
    }

    #[allow(clippy::too_many_arguments)]
    async fn flush_snapshot_copy_buffer(
        pg_client: &tokio_postgres::Client,
        dlq_producer: &FutureProducer,
        messages: &mut Vec<SnapshotBufferedMessage>,
        flush_reason: &str,
        mappings_by_collection: &HashMap<String, Vec<CollectionMapping>>,
        fallback_schema: Option<&str>,
        table_insert_execs: &mut HashMap<String, u64>,
        copy_allowed_cache: &mut HashMap<String, bool>,
        processed: &mut usize,
        total_affected_rows: &mut u64,
        snapshot_inserted_rows: &mut u64,
        apply_failed: &mut usize,
        dlq_published: &mut usize,
        dlq_failed: &mut usize,
        fallback_payload_as_after: &mut usize,
        skipped_missing_after: &mut usize,
        skipped_missing_before: &mut usize,
        skipped_missing_required_snapshot: &mut usize,
        skipped_missing_required_snapshot_by_table: &mut HashMap<String, usize>,
        skipped_missing_required_snapshot_samples: &mut Vec<String>,
        copy_skipped_by_table: &mut HashMap<String, usize>,
        copy_skipped_samples: &mut Vec<String>,
        skipped_non_data_op: &mut usize,
        copy_attempts: &mut usize,
        copy_failed: &mut usize,
        fallback_replay_attempts: &mut usize,
        fallback_replay_failed: &mut usize,
        fallback_replay_dlq_published: &mut usize,
        fallback_replay_dlq_failed: &mut usize,
        copy_eligible_rows: &mut usize,
        copy_skipped_copy_disabled: &mut usize,
        copy_skipped_missing_mapping: &mut usize,
        copy_skipped_non_root_mapping: &mut usize,
        copy_skipped_unconvertible_literal: &mut usize,
        copy_skipped_empty_columns: &mut usize,
        copy_unconvertible_literal_samples: &mut Vec<String>,
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let buffered_messages = messages.len();
        let processed_before = *processed;
        let affected_rows_before = *total_affected_rows;
        let copy_attempts_before = *copy_attempts;
        let copy_failed_before = *copy_failed;
        let fallback_replay_attempts_before = *fallback_replay_attempts;
        let fallback_replay_failed_before = *fallback_replay_failed;
        let copy_eligible_rows_before = *copy_eligible_rows;
        let copy_skipped_copy_disabled_before = *copy_skipped_copy_disabled;
        let copy_skipped_missing_mapping_before = *copy_skipped_missing_mapping;
        let copy_skipped_non_root_mapping_before = *copy_skipped_non_root_mapping;
        let copy_skipped_unconvertible_literal_before = *copy_skipped_unconvertible_literal;
        let copy_skipped_empty_columns_before = *copy_skipped_empty_columns;

        let drained = std::mem::take(messages);
        let mut batches: Vec<SnapshotCopyBatch> = Vec::new();
        let mut pass_through: Vec<SnapshotBufferedMessage> = Vec::new();

        for message in drained {
            if !matches!(message.op.as_str(), "c" | "u" | "r") {
                pass_through.push(message);
                continue;
            }

            let Some(mappings) = mappings_by_collection.get(&message.folder_name) else {
                *copy_skipped_missing_mapping += 1;
                pass_through.push(message);
                continue;
            };

            let row = build_snapshot_copy_row(
                &message.payload,
                message.after.as_ref(),
                mappings,
                fallback_schema,
                &message.collection_name,
            )?;

            let (table_name, columns, csv_row, sql_row, on_conflict_clause) = match row {
                SnapshotCopyRowBuildOutcome::Built(row) => {
                    *copy_eligible_rows += 1;
                    row
                }
                SnapshotCopyRowBuildOutcome::SkippedNonRootMapping { table_name } => {
                    *copy_skipped_non_root_mapping += 1;
                    *copy_skipped_by_table.entry(table_name.clone()).or_insert(0) += 1;
                    if copy_skipped_samples.len() < 20 {
                        copy_skipped_samples.push(format!(
                            "table={} reason=copy_non_root_mapping collection={} topic={}",
                            table_name, message.collection_name, message.topic
                        ));
                    }
                    pass_through.push(message);
                    continue;
                }
                SnapshotCopyRowBuildOutcome::SkippedUnconvertibleLiteral { table_name, detail } => {
                    *copy_skipped_unconvertible_literal += 1;
                    *copy_skipped_by_table.entry(table_name.clone()).or_insert(0) += 1;
                    if copy_skipped_samples.len() < 20 {
                        copy_skipped_samples.push(format!(
                            "table={} reason=copy_unconvertible_literal collection={} topic={} detail={}",
                            table_name, message.collection_name, message.topic, detail
                        ));
                    }
                    if copy_unconvertible_literal_samples.len() < 5 {
                        warn!(
                            "snapshot COPY skipped row due to unconvertible literal; sample: {}",
                            detail
                        );
                        copy_unconvertible_literal_samples.push(detail);
                        if copy_unconvertible_literal_samples.len() == 5 {
                            warn!(
                                "snapshot COPY unconvertible literal sample limit reached (5); suppressing additional samples"
                            );
                        }
                    }
                    pass_through.push(message);
                    continue;
                }
                SnapshotCopyRowBuildOutcome::SkippedEmptyColumns { table_name } => {
                    *copy_skipped_empty_columns += 1;
                    *copy_skipped_by_table.entry(table_name.clone()).or_insert(0) += 1;
                    if copy_skipped_samples.len() < 20 {
                        copy_skipped_samples.push(format!(
                            "table={} reason=copy_empty_columns collection={} topic={}",
                            table_name, message.collection_name, message.topic
                        ));
                    }
                    pass_through.push(message);
                    continue;
                }
            };

            if let Some(batch) = batches.iter_mut().find(|batch| {
                batch.table_name == table_name
                    && batch.columns == columns
                    && batch.on_conflict_clause == on_conflict_clause
            }) {
                batch.csv_rows.push(csv_row);
                batch.sql_rows.push(sql_row);
                batch.messages.push(message);
            } else {
                batches.push(SnapshotCopyBatch {
                    table_name,
                    columns,
                    csv_rows: vec![csv_row],
                    sql_rows: vec![sql_row],
                    on_conflict_clause,
                    messages: vec![message],
                });
            }
        }

        for batch in batches {
            let copy_allowed = copy_allowed_cache
                .get(&batch.table_name)
                .copied()
                .unwrap_or(true);

            if !copy_allowed {
                *copy_skipped_copy_disabled += batch.messages.len();
                *copy_skipped_by_table
                    .entry(batch.table_name.clone())
                    .or_insert(0) += batch.messages.len();
                if copy_skipped_samples.len() < 20 {
                    copy_skipped_samples.push(format!(
                        "table={} reason=copy_disabled buffered_rows={}",
                        batch.table_name,
                        batch.messages.len()
                    ));
                }
                *fallback_replay_attempts += batch.messages.len();
                let fallback_sql = format!(
                    "INSERT INTO {} ({}) VALUES {} {}",
                    batch.table_name,
                    batch.columns.join(", "),
                    batch.sql_rows.join(", "),
                    batch.on_conflict_clause
                );
                match traced_pg_execute(
                    pg_client,
                    fallback_sql.as_str(),
                    "upsert_batch_fallback",
                    batch.table_name.as_str(),
                )
                .await
                {
                    Ok(rows) => {
                        *processed += batch.messages.len();
                        *total_affected_rows += rows;
                        *snapshot_inserted_rows += rows;
                        *table_insert_execs
                            .entry(batch.table_name.clone())
                            .or_insert(0) += batch.messages.len() as u64;
                    }
                    Err(err) => {
                        *copy_failed += 1;
                        warn!(
                            "batch fallback upsert failed for table {} after COPY disabled; replaying per-message: {}",
                            batch.table_name,
                            format_postgres_error(&err)
                        );
                        pass_through.extend(batch.messages);
                    }
                }
                continue;
            }

            *copy_attempts += 1;

            if let Err(begin_err) = pg_client.batch_execute("BEGIN").await {
                *copy_failed += 1;
                warn!(
                    "failed to BEGIN snapshot COPY batch for table {}: {:#}",
                    batch.table_name, begin_err
                );
            } else {
                let copy_sql = format!(
                    "COPY {} ({}) FROM STDIN WITH (FORMAT csv, NULL '\\N')",
                    batch.table_name,
                    batch.columns.join(", ")
                );

                let copy_result: Result<u64> = async {
                    let sink = pg_client.copy_in(&copy_sql).await.map_err(|err| {
                        anyhow!(
                            "failed to start snapshot COPY for {}: {}",
                            batch.table_name,
                            format_postgres_error(&err)
                        )
                    })?;
                    let mut sink = pin!(sink);
                    let payload = format!("{}\n", batch.csv_rows.join("\n"));
                    sink.as_mut()
                        .send(Bytes::copy_from_slice(payload.as_bytes()))
                        .await
                        .map_err(|err| {
                            anyhow!(
                                "failed to stream snapshot COPY payload: {}",
                                format_postgres_error(&err)
                            )
                        })?;
                    let rows = sink.as_mut().finish().await.map_err(|err| {
                        anyhow!(
                            "failed to finish snapshot COPY payload: {}",
                            format_postgres_error(&err)
                        )
                    })?;
                    Ok(rows)
                }
                .await;

                match copy_result {
                    Ok(rows) => {
                        if let Err(commit_err) = pg_client.batch_execute("COMMIT").await {
                            *copy_failed += 1;
                            error!(
                                "failed to COMMIT snapshot COPY batch for table {}: {:#}",
                                batch.table_name, commit_err
                            );
                            let _ = pg_client.batch_execute("ROLLBACK").await;
                        } else {
                            *processed += batch.messages.len();
                            *total_affected_rows += rows;
                            *snapshot_inserted_rows += rows;
                            *table_insert_execs
                                .entry(batch.table_name.clone())
                                .or_insert(0) += batch.messages.len() as u64;
                            continue;
                        }
                    }
                    Err(copy_err) => {
                        *copy_failed += 1;
                        error!(
                            "snapshot COPY failed for table {}: {:#}",
                            batch.table_name, copy_err
                        );
                        if is_duplicate_key_error(&copy_err) {
                            copy_allowed_cache.insert(batch.table_name.clone(), false);
                            info!(
                                "snapshot COPY disabled for table {} after duplicate-key failure; remaining rows will use insert-upsert fallback",
                                batch.table_name
                            );
                        }
                        let _ = pg_client.batch_execute("ROLLBACK").await;
                    }
                }
            }

            *fallback_replay_attempts += batch.messages.len();
            let fallback_sql = format!(
                "INSERT INTO {} ({}) VALUES {} {}",
                batch.table_name,
                batch.columns.join(", "),
                batch.sql_rows.join(", "),
                batch.on_conflict_clause
            );
            match traced_pg_execute(
                pg_client,
                fallback_sql.as_str(),
                "upsert_batch_fallback",
                batch.table_name.as_str(),
            )
            .await
            {
                Ok(rows) => {
                    *processed += batch.messages.len();
                    *total_affected_rows += rows;
                    *snapshot_inserted_rows += rows;
                    *table_insert_execs
                        .entry(batch.table_name.clone())
                        .or_insert(0) += batch.messages.len() as u64;
                    continue;
                }
                Err(err) => {
                    warn!(
                        "batch fallback upsert failed for table {}; replaying per-message with DLQ protection: {}",
                        batch.table_name,
                        format_postgres_error(&err)
                    );
                }
            }

            for message in batch.messages {
                let Some(mappings) = mappings_by_collection.get(&message.folder_name) else {
                    *fallback_replay_failed += 1;
                    *apply_failed += 1;
                    continue;
                };

                let mut tx_pending_unused: HashMap<String, u64> = HashMap::new();

                let write_outcome = write_to_pg(
                    pg_client,
                    &message.payload,
                    &message.op,
                    message.before.as_ref(),
                    message.after.as_ref(),
                    mappings,
                    &message.collection_name,
                    &message.topic,
                    fallback_schema,
                    false,
                    &mut tx_pending_unused,
                    table_insert_execs,
                    fallback_payload_as_after,
                    skipped_missing_after,
                    skipped_missing_before,
                    skipped_missing_required_snapshot,
                    skipped_missing_required_snapshot_by_table,
                    skipped_missing_required_snapshot_samples,
                    skipped_non_data_op,
                )
                .await;

                match write_outcome {
                    Ok(WriteStageOutcome::Applied(rows)) => {
                        *processed += 1;
                        *total_affected_rows += rows;
                        *snapshot_inserted_rows += rows;
                    }
                    Ok(WriteStageOutcome::Skipped) => {}
                    Err(err) => {
                        *fallback_replay_failed += 1;
                        *apply_failed += 1;
                        warn!(
                            "fallback replay apply failed topic={} collection={} op={}: {:#}",
                            message.topic, message.collection_name, message.op, err
                        );

                        if let Some(payload) = message.payload_bytes.as_deref() {
                            let dlq_topic =
                                adhoc_dlq_topic_for_collection(&message.collection_name);
                            match publish_to_dlq_topic(
                                dlq_producer,
                                &dlq_topic,
                                message.key_bytes.as_deref(),
                                payload,
                            )
                            .await
                            {
                                Ok(()) => {
                                    *dlq_published += 1;
                                    *fallback_replay_dlq_published += 1;
                                }
                                Err(dlq_err) => {
                                    *dlq_failed += 1;
                                    *fallback_replay_dlq_failed += 1;
                                    warn!(
                                        "failed to publish fallback replay message to ad hoc DLQ {}: {:#}",
                                        dlq_topic, dlq_err
                                    );
                                }
                            }
                        } else {
                            *dlq_failed += 1;
                            *fallback_replay_dlq_failed += 1;
                        }
                    }
                }
            }
        }

        for message in pass_through {
            let Some(mappings) = mappings_by_collection.get(&message.folder_name) else {
                *apply_failed += 1;
                continue;
            };
            let mut tx_pending_unused: HashMap<String, u64> = HashMap::new();
            let write_outcome = write_to_pg(
                pg_client,
                &message.payload,
                &message.op,
                message.before.as_ref(),
                message.after.as_ref(),
                mappings,
                &message.collection_name,
                &message.topic,
                fallback_schema,
                false,
                &mut tx_pending_unused,
                table_insert_execs,
                fallback_payload_as_after,
                skipped_missing_after,
                skipped_missing_before,
                skipped_missing_required_snapshot,
                skipped_missing_required_snapshot_by_table,
                skipped_missing_required_snapshot_samples,
                skipped_non_data_op,
            )
            .await;

            match write_outcome {
                Ok(WriteStageOutcome::Applied(rows)) => {
                    *processed += 1;
                    *total_affected_rows += rows;
                    *snapshot_inserted_rows += rows;
                }
                Ok(WriteStageOutcome::Skipped) => {}
                Err(err) => {
                    *apply_failed += 1;
                    warn!(
                        "snapshot pass-through apply failed topic={} collection={} op={}: {:#}",
                        message.topic, message.collection_name, message.op, err
                    );
                }
            }
        }

        let flushed_processed_delta = processed.saturating_sub(processed_before);
        let affected_rows_delta = total_affected_rows.saturating_sub(affected_rows_before);
        let copy_attempts_delta = copy_attempts.saturating_sub(copy_attempts_before);
        let copy_failed_delta = copy_failed.saturating_sub(copy_failed_before);
        let fallback_replay_attempts_delta =
            fallback_replay_attempts.saturating_sub(fallback_replay_attempts_before);
        let fallback_replay_failed_delta =
            fallback_replay_failed.saturating_sub(fallback_replay_failed_before);
        let copy_eligible_rows_delta = copy_eligible_rows.saturating_sub(copy_eligible_rows_before);
        let copy_skipped_copy_disabled_delta =
            copy_skipped_copy_disabled.saturating_sub(copy_skipped_copy_disabled_before);
        let copy_skipped_missing_mapping_delta =
            copy_skipped_missing_mapping.saturating_sub(copy_skipped_missing_mapping_before);
        let copy_skipped_non_root_mapping_delta =
            copy_skipped_non_root_mapping.saturating_sub(copy_skipped_non_root_mapping_before);
        let copy_skipped_unconvertible_literal_delta = copy_skipped_unconvertible_literal
            .saturating_sub(copy_skipped_unconvertible_literal_before);
        let copy_skipped_empty_columns_delta =
            copy_skipped_empty_columns.saturating_sub(copy_skipped_empty_columns_before);

        info!(
            "Kafka copy flush: reason={}, buffered_messages={}, flushed_processed_delta={}, affected_rows_delta={}, copy_attempts_delta={}, copy_failed_delta={}, fallback_replay_attempts_delta={}, fallback_replay_failed_delta={}, copy_eligible_rows_delta={}, copy_skipped_copy_disabled_delta={}, copy_skipped_missing_mapping_delta={}, copy_skipped_non_root_mapping_delta={}, copy_skipped_unconvertible_literal_delta={}, copy_skipped_empty_columns_delta={}",
            flush_reason,
            buffered_messages,
            flushed_processed_delta,
            affected_rows_delta,
            copy_attempts_delta,
            copy_failed_delta,
            fallback_replay_attempts_delta,
            fallback_replay_failed_delta,
            copy_eligible_rows_delta,
            copy_skipped_copy_disabled_delta,
            copy_skipped_missing_mapping_delta,
            copy_skipped_non_root_mapping_delta,
            copy_skipped_unconvertible_literal_delta,
            copy_skipped_empty_columns_delta
        );

        Ok(())
    }

    fn consumer_lag_snapshot(consumer: &StreamConsumer) -> Result<Option<(usize, i64, i64)>> {
        let assignment = consumer
            .assignment()
            .with_context(|| connection_failed_context("kafka", "query"))?;
        let assigned = assignment.elements();
        if assigned.is_empty() {
            return Ok(None);
        }

        let positions = consumer
            .position()
            .with_context(|| connection_failed_context("kafka", "query"))?;
        let mut position_by_partition: HashMap<(String, i32), i64> = HashMap::new();
        for item in positions.elements() {
            let pos = match item.offset() {
                rdkafka::Offset::Offset(value) => value,
                _ => -1,
            };
            position_by_partition.insert((item.topic().to_owned(), item.partition()), pos);
        }

        let committed = consumer
            .committed_offsets(assignment.clone(), Duration::from_secs(1))
            .with_context(|| connection_failed_context("kafka", "query"))?;
        let mut committed_by_partition: HashMap<(String, i32), i64> = HashMap::new();
        for item in committed.elements() {
            let committed_offset = match item.offset() {
                rdkafka::Offset::Offset(value) => value,
                _ => -1,
            };
            committed_by_partition.insert(
                (item.topic().to_owned(), item.partition()),
                committed_offset,
            );
        }

        let assigned_count = assigned.len();
        let mut total_lag = 0_i64;
        let mut total_end_offsets = 0_i64;
        for item in assigned {
            let (low, high) = consumer
                .fetch_watermarks(item.topic(), item.partition(), Duration::from_secs(1))
                .with_context(|| connection_failed_context("kafka", "query"))?;
            let end = high.max(low).max(0);
            total_end_offsets += end;

            let pos = position_by_partition
                .get(&(item.topic().to_owned(), item.partition()))
                .copied()
                .unwrap_or(-1);
            let committed_pos = committed_by_partition
                .get(&(item.topic().to_owned(), item.partition()))
                .copied()
                .unwrap_or(-1);
            let effective_pos = if pos >= 0 {
                pos
            } else if committed_pos >= 0 {
                committed_pos
            } else {
                low.max(0)
            };
            let lag = (end - effective_pos).max(0);
            total_lag += lag;
        }

        Ok(Some((assigned_count, total_lag, total_end_offsets)))
    }

    fn commit_offset(
        consumer: &StreamConsumer,
        message: &BorrowedMessage<'_>,
        enable_auto_commit: bool,
    ) -> Result<()> {
        if enable_auto_commit {
            return Ok(());
        }

        consumer
            .commit_message(message, CommitMode::Async)
            .with_context(|| connection_failed_context("kafka", "commit"))
    }

    let mut conf = read_conf(&args.config)?;
    if let Some(project_dir) = args.project_dir.clone() {
        conf.project_dir = project_dir;
    }

    let mut kafka_conf = conf
        .kafka
        .clone()
        .ok_or_else(|| anyhow!("Missing [kafka] section in config file"))?;
    let cli_topics_supplied = !args.topics.is_empty();
    if !args.topics.is_empty() {
        kafka_conf.topics = args.topics.clone();
    }
    if let Some(group_id) = args.group_id.clone() {
        kafka_conf.group_id = Some(group_id);
    }
    if let Some(topic_prefix) = args.topic_prefix.clone() {
        kafka_conf.topic_prefix = Some(topic_prefix);
    }
    if !args.topics.is_empty()
        && kafka_conf
            .topic_prefix
            .as_deref()
            .map(str::trim)
            .is_some_and(|value| !value.is_empty())
    {
        warn!(
            "Both --topics and kafka.topic_prefix are set; these options are mutually exclusive. Ignoring topic_prefix and consuming explicit topics only."
        );
        kafka_conf.topic_prefix = None;
    }
    if let Some(offset) = args.offset.clone() {
        kafka_conf.offset = Some(offset.clone());
        kafka_conf.auto_offset_reset = Some(offset);
    }
    if let Some(max_messages) = args.max_messages {
        kafka_conf.max_messages = Some(max_messages);
    }
    conf.kafka = Some(kafka_conf.clone());
    let namespace = conf
        .namespace
        .clone()
        .ok_or_else(|| anyhow!("No NAMESPACE provided in config"))?;
    let (namespace_db_name, _) = split_namespace_scope(&namespace);

    let source_uri = conf
        .source_uri
        .as_deref()
        .ok_or_else(|| anyhow!("No SOURCE_URI provided: add SOURCE_URI to the config file"))?;
    info!("preflight ping source: begin");
    let source_client_options = parse_client_options(source_uri).await.with_context(|| {
        format!(
            "{}: failed to parse MongoDB SOURCE_URI",
            connection_failed_context("mongo", "connect")
        )
    })?;
    let source_client = client_with_options(source_client_options).with_context(|| {
        format!(
            "{}: failed to connect to MongoDB using SOURCE_URI",
            connection_failed_context("mongo", "connect")
        )
    })?;
    source_client
        .database("admin")
        .run_command(doc! { "ping": 1_i32 })
        .await
        .with_context(|| {
            format!(
                "{}: failed MongoDB ping command",
                connection_failed_context("mongo", "query")
            )
        })?;
    info!("preflight ping source: ok");

    let bootstrap_servers_raw = kafka_conf
        .bootstrap_servers
        .clone()
        .ok_or_else(|| anyhow!("kafka.bootstrap_servers is required"))?;
    let bootstrap_servers = normalize_kafka_bootstrap_servers(&bootstrap_servers_raw)?;
    if bootstrap_servers != bootstrap_servers_raw {
        info!(
            "Normalized kafka.bootstrap_servers from '{}' to '{}'",
            bootstrap_servers_raw, bootstrap_servers
        );
    }
    let group_id = kafka_conf
        .group_id
        .clone()
        .unwrap_or_else(|| "mongo2pg-kafka-import".to_owned());
    let configured_worker_count = kafka_conf.worker_count.unwrap_or(1).max(1);
    let is_worker_child = std::env::var("MONGO2PG_KAFKA_WORKER_CHILD")
        .map(|value| value == "1")
        .unwrap_or(false);
    let worker_total_from_parent = std::env::var("MONGO2PG_KAFKA_WORKERS_TOTAL")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0);
    let worker_count_for_logs = worker_total_from_parent.unwrap_or(configured_worker_count);
    let worker_count = if is_worker_child {
        1
    } else {
        configured_worker_count
    };
    let group_id_log_suffix = kafka_conf.group_id_log_suffix.unwrap_or(true);
    let configured_offset = kafka_conf
        .offset
        .clone()
        .or_else(|| kafka_conf.auto_offset_reset.clone())
        .unwrap_or_else(|| "earliest".to_owned());
    let effective_offset = args
        .offset
        .clone()
        .unwrap_or_else(|| configured_offset.clone());
    let snapshot_mode = effective_offset == "0";

    info!(
        "Kafka worker resolution: configured_worker_count={}, effective_worker_count={}, worker_count_for_logs={}, is_worker_child={}, snapshot_mode={}, cli_max_messages={}, config_max_messages={}",
        configured_worker_count,
        worker_count,
        worker_count_for_logs,
        is_worker_child,
        snapshot_mode,
        args.max_messages
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_owned()),
        kafka_conf
            .max_messages
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_owned())
    );

    if worker_count > 1 && (args.max_messages.is_some() || kafka_conf.max_messages.is_some()) {
        warn!(
            "Kafka worker mode disabled: configured_worker_count={} but max_messages is set (cli_max_messages={}, config_max_messages={})",
            configured_worker_count,
            args.max_messages
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned()),
            kafka_conf
                .max_messages
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned())
        );
        return Err(anyhow!(
            "kafka.worker_count > 1 is not supported with max_messages or --max-messages"
        ));
    }

    let effective_group_id = if snapshot_mode {
        if let Ok(group_id_from_parent) = std::env::var("MONGO2PG_KAFKA_EFFECTIVE_GROUP_ID") {
            group_id_from_parent
        } else {
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_else(|_| Duration::from_secs(0))
                .as_secs();
            format!("{group_id}-snapshot-{ts}")
        }
    } else {
        group_id.clone()
    };
    let mut topics = if args.topics.is_empty() {
        kafka_conf.topics.clone()
    } else {
        args.topics.clone()
    };
    let normalized_topics = topics
        .iter()
        .map(|topic| topic.trim())
        .filter(|topic| !topic.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if topics != normalized_topics {
        warn!(
            "Kafka topics were normalized (trimmed whitespace / dropped empty entries) before subscription"
        );
    }
    topics = normalized_topics;
    let mut spawned_worker_children = Vec::new();

    let target_uri = conf
        .target_uri
        .clone()
        .ok_or_else(|| anyhow!("No TARGET_URI provided in config"))?;
    let target_database_name = args
        .database_name
        .clone()
        .or_else(|| conf.target_database_name.clone())
        .unwrap_or_else(|| namespace_db_name.to_owned());
    let effective_target_schema = args
        .schema_name
        .clone()
        .or_else(|| conf.target_schema.clone());

    let (tables_dir, collections_dir, kafka_import_metadata_stage) =
        resolve_kafka_import_metadata_dirs(&args.config, &conf, namespace_db_name).await?;

    if !tables_dir.is_dir() {
        return Err(anyhow!(
            "Cannot read SQL tables directory {}",
            tables_dir.display()
        ));
    }

    if !collections_dir.is_dir() {
        return Err(anyhow!(
            "Cannot read collections directory {}",
            collections_dir.display()
        ));
    }

    if let Some(stage) = &kafka_import_metadata_stage {
        info!(
            "kafka-import metadata staging dir (temporary): {}",
            stage.path().display()
        );
    }

    //let admin_client = connect_pg_admin_client(&target_uri, &target_database_name).await?;
    //ensure_pg_database(&admin_client, &target_database_name).await?;

    let db_target_uri = pg_uri_with_database(&target_uri, &target_database_name);
    info!("preflight ping target: begin");
    let pg_client = connect_pg_client(&db_target_uri).await?;
    pg_client
        .query_one("SELECT 1", &[])
        .await
        .with_context(|| {
            format!(
                "{}: failed PostgreSQL ping query",
                connection_failed_context("pg", "query")
            )
        })?;
    info!("preflight ping target: ok");

    if is_worker_child {
        info!("Skipping PostgreSQL bootstrap/preflight in worker child process");
    } else {
        bootstrap_pg_objects_for_kafka_import(
            // &admin_client,
            &pg_client,
            &tables_dir,
            effective_target_schema.as_deref(),
            namespace_db_name,
            args.force,
        )
        .await?;
    }

    let mut mappings_by_collection = load_collection_mapping_folders(&collections_dir)?;
    if let Some(collections_root) = collections_dir.parent().filter(|parent| {
        parent
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name == "collections")
    }) {
        let fallback_mappings = load_collection_mapping_folders(collections_root)?;
        let mut added_from_root = 0usize;
        for (collection, mappings) in fallback_mappings {
            if let std::collections::hash_map::Entry::Vacant(entry) =
                mappings_by_collection.entry(collection)
            {
                entry.insert(mappings);
                added_from_root += 1;
            }
        }
        if added_from_root > 0 {
            info!(
                "Loaded {} additional collection mapping folder(s) from fallback root {}",
                added_from_root,
                collections_root.display()
            );
        }
    }

    let configured_auto_offset_reset = configured_offset;
    let auto_offset_reset = if snapshot_mode {
        "earliest".to_owned()
    } else {
        effective_offset
    };

    let default_ssl_ca_location = crate::db::kafka::detect_default_ssl_ca_location(&kafka_conf);
    if let Some(ca_path) = default_ssl_ca_location.as_deref() {
        info!(
            "kafka ssl_ca_location not configured; using detected system CA bundle {}",
            ca_path
        );
    }

    let apply_kafka_security = |client_config: &mut ClientConfig| {
        crate::db::kafka::apply_security_config(
            client_config,
            &kafka_conf,
            default_ssl_ca_location.as_deref(),
        );
    };

    let has_sasl_user = kafka_conf
        .sasl_username
        .as_deref()
        .map(str::trim)
        .is_some_and(|value| !value.is_empty());
    let has_sasl_password = kafka_conf
        .sasl_password
        .as_deref()
        .map(str::trim)
        .is_some_and(|value| !value.is_empty());
    if has_sasl_user != has_sasl_password {
        warn!("kafka.sasl_username/kafka.sasl_password should both be set for SASL authentication");
    }

    let mut consumer_config = ClientConfig::new();
    consumer_config
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", &effective_group_id)
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", &auto_offset_reset);

    if let Some(val) = kafka_conf.fetch_min_bytes {
        consumer_config.set("fetch.min.bytes", val.to_string());
    }
    if let Some(val) = kafka_conf.fetch_wait_max_ms {
        consumer_config.set("fetch.wait.max.ms", val.to_string());
    }
    if let Some(val) = kafka_conf.max_partition_fetch_bytes {
        consumer_config.set("max.partition.fetch.bytes", val.to_string());
    }
    if let Some(val) = kafka_conf.fetch_max_bytes {
        consumer_config.set("fetch.max.bytes", val.to_string());
    }

    if let Some(val) = kafka_conf.queued_max_messages_kbytes {
        consumer_config.set("queued.max.messages.kbytes", val.to_string());
    }

    let enable_auto_commit = kafka_conf.enable_auto_commit.unwrap_or(true);
    consumer_config.set("enable.auto.commit", enable_auto_commit.to_string());

    if let Some(poll_size) = kafka_conf.poll_size.filter(|value| *value > 0) {
        consumer_config.set("queued.min.messages", &poll_size.to_string());
    }
    if let Some(debug_value) = kafka_conf
        .debug
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        consumer_config.set("debug", debug_value);
    }
    apply_kafka_security(&mut consumer_config);
    let consumer: StreamConsumer = consumer_config.create().with_context(|| {
        format!(
            "{}: failed to create Kafka consumer",
            connection_failed_context("kafka", "connect")
        )
    })?;
    info!("preflight ping kafka: begin");
    consumer
        .fetch_metadata(None, Duration::from_secs(10))
        .with_context(|| {
            format!(
                "{}: failed to fetch Kafka metadata",
                connection_failed_context("kafka", "query")
            )
        })?;
    info!("preflight ping kafka: ok");

    let mut dlq_producer_config = ClientConfig::new();
    dlq_producer_config
        .set("bootstrap.servers", &bootstrap_servers)
        .set("message.timeout.ms", "5000");
    if let Some(debug_value) = kafka_conf
        .debug
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        dlq_producer_config.set("debug", debug_value);
    }
    apply_kafka_security(&mut dlq_producer_config);
    let dlq_producer: FutureProducer = dlq_producer_config.create().with_context(|| {
        format!(
            "{}: failed to create Kafka DLQ producer",
            connection_failed_context("kafka", "connect")
        )
    })?;

    if topics.is_empty() {
        if let Some(prefix) = kafka_conf.topic_prefix.as_deref() {
            let metadata = consumer
                .fetch_metadata(None, Duration::from_secs(10))
                .with_context(|| {
                    format!(
                        "{}: failed to fetch Kafka metadata while resolving topic prefix '{}'",
                        connection_failed_context("kafka", "query"),
                        prefix
                    )
                })?;
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
                    "No Kafka topics matched prefix '{}'. Set [kafka].topics, pass --topics, or create topics with this prefix.",
                    prefix
                ));
            }
        } else {
            return Err(anyhow!(
                "No Kafka topics configured. Set [kafka].topics, set [kafka].topic_prefix, or pass --topics"
            ));
        }
    }

    if !topics.is_empty() {
        let metadata = consumer
            .fetch_metadata(None, Duration::from_secs(10))
            .with_context(|| {
                format!(
                    "{}: failed to fetch Kafka metadata while validating topic list",
                    connection_failed_context("kafka", "query")
                )
            })?;
        let existing_topics = metadata
            .topics()
            .iter()
            .map(|topic| topic.name().to_owned())
            .collect::<HashSet<_>>();
        let missing_topics = topics
            .iter()
            .filter(|topic| !existing_topics.contains(*topic))
            .cloned()
            .collect::<Vec<_>>();

        if !missing_topics.is_empty() {
            return Err(anyhow!(
                "Kafka topic(s) not found on broker metadata: {}. Check --topics / [kafka].topics / topic_prefix.",
                missing_topics.join(", ")
            ));
        }
    }

    if worker_count > 1 {
        let current_exe = std::env::current_exe()
            .context("failed to resolve current executable for kafka worker spawning")?;
        info!(
            "kafka worker mode enabled: spawning {} extra worker process(es) (total workers={})",
            worker_count - 1,
            worker_count
        );

        for worker_index in 1..worker_count {
            let mut command = tokio::process::Command::new(&current_exe);
            command
                .arg("kafka-import")
                .arg("--config")
                .arg(&args.config)
                .env("MONGO2PG_KAFKA_WORKER_CHILD", "1")
                .env("MONGO2PG_KAFKA_WORKER_INDEX", worker_index.to_string())
                .env(
                    "MONGO2PG_KAFKA_WORKERS_TOTAL",
                    configured_worker_count.to_string(),
                )
                .env("MONGO2PG_KAFKA_EFFECTIVE_GROUP_ID", &effective_group_id)
                .stdin(Stdio::null())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit());
            for arg in kafka_worker_child_extra_args(&args) {
                command.arg(arg);
            }

            let child = command.spawn().with_context(|| {
                format!("failed to spawn kafka worker process {}", worker_index)
            })?;
            info!("spawned kafka worker process {}", worker_index);
            spawned_worker_children.push(child);
        }
    }

    let topic_refs = topics.iter().map(String::as_str).collect::<Vec<_>>();
    consumer.subscribe(&topic_refs).with_context(|| {
        format!(
            "{}: failed to subscribe topics: {}",
            connection_failed_context("kafka", "consume"),
            topics.join(", ")
        )
    })?;

    let worker_identity = if is_worker_child {
        let worker_index = std::env::var("MONGO2PG_KAFKA_WORKER_INDEX")
            .ok()
            .unwrap_or_else(|| "unknown".to_owned());
        format!("child-{worker_index}")
    } else if worker_count > 1 {
        "parent-0".to_owned()
    } else {
        "single".to_owned()
    };
    let effective_group_id_log = if worker_count_for_logs > 1 && group_id_log_suffix {
        format!("{}@{}", effective_group_id, worker_identity)
    } else {
        effective_group_id.clone()
    };

    let _kafka_import_span = tracing::info_span!(
        "kafka_import.run",
        worker = worker_identity,
        workers_total = worker_count_for_logs as u64,
        snapshot_mode = snapshot_mode,
        project_name = conf.project_dir.as_str(),
        namespace = namespace.as_str()
    )
    .entered();

    info!(
        "Kafka import started. worker={}, workers_total={}, group_id={}, topics={}, target_db={}, snapshot_mode={}, offset={}",
        worker_identity,
        worker_count_for_logs,
        effective_group_id_log,
        topics.join(","),
        target_database_name,
        snapshot_mode,
        auto_offset_reset
    );
    let http_client = reqwest::Client::builder().build()?;
    let mut schema_cache: HashMap<u32, Schema> = HashMap::new();
    let max_messages = args.max_messages.or(kafka_conf.max_messages);
    let batch_log_messages = kafka_conf.batch_log_messages.unwrap_or(100).max(1);
    let transaction_batch_size = kafka_conf.transaction_batch_size.unwrap_or(1).max(1);
    let flush_batch_after = kafka_conf
        .flush_batch_after
        .as_deref()
        .map(parse_flush_batch_after)
        .transpose()?;
    let stop_on_no_lag = kafka_conf.stop_on_no_lag.unwrap_or(false);
    let snapshot_copy_enabled = kafka_conf
        .copy_mode
        .unwrap_or(enable_auto_commit && transaction_batch_size > 1);
    let transaction_batching_enabled = !snapshot_copy_enabled && transaction_batch_size > 1;
    let configured_write_mode = if snapshot_copy_enabled {
        "copy_batch_insert_fallback"
    } else {
        "insert_upsert"
    };
    let processed_mode = if snapshot_copy_enabled {
        "copied"
    } else {
        "inserted"
    };
    let mut processed = 0_usize;
    let mut polled = 0_usize;
    let mut skipped_topic = 0_usize;
    let mut skipped_db = 0_usize;
    let mut skipped_mapping = 0_usize;
    let mut skipped_no_payload = 0_usize;
    let mut skipped_non_data_op = 0_usize;
    let mut skipped_missing_after = 0_usize;
    let mut skipped_missing_before = 0_usize;
    let mut skipped_missing_required_snapshot = 0_usize;
    let mut skipped_missing_required_snapshot_by_table: HashMap<String, usize> = HashMap::new();
    let mut skipped_missing_required_snapshot_samples: Vec<String> = Vec::new();
    let mut copy_skipped_by_table: HashMap<String, usize> = HashMap::new();
    let mut copy_skipped_samples: Vec<String> = Vec::new();
    let mut fallback_payload_as_after = 0_usize;
    let mut decode_failed = 0_usize;
    let mut apply_failed = 0_usize;
    let mut commit_failed = 0_usize;
    let mut dlq_published = 0_usize;
    let mut dlq_failed = 0_usize;
    let mut snapshot_inserted_rows = 0_u64;
    let mut total_affected_rows = 0_u64;
    let mut op_c = 0_usize;
    let mut op_u = 0_usize;
    let mut op_r = 0_usize;
    let mut op_d = 0_usize;
    let mut op_other = 0_usize;
    let mut table_insert_execs: HashMap<String, u64> = HashMap::new();
    // let mut tx_pending_processed = 0_usize;
    // let mut tx_pending_rows = 0_u64;
    // let mut tx_pending_snapshot_rows = 0_u64;
    // let mut tx_pending_table_insert_execs: HashMap<String, u64> = HashMap::new();
    let mut tx_rolled_back_batches = 0_usize;
    let mut tx_rolled_back_messages = 0_usize;
    let mut snapshot_buffer: Vec<SnapshotBufferedMessage> = Vec::new();
    let mut copy_attempts = 0_usize;
    let mut copy_failed = 0_usize;
    let mut fallback_replay_attempts = 0_usize;
    let mut fallback_replay_failed = 0_usize;
    let mut fallback_replay_dlq_published = 0_usize;
    let mut fallback_replay_dlq_failed = 0_usize;
    let mut copy_eligible_rows = 0_usize;
    let mut copy_skipped_copy_disabled = 0_usize;
    let mut copy_skipped_missing_mapping = 0_usize;
    let mut copy_skipped_non_root_mapping = 0_usize;
    let mut copy_skipped_unconvertible_literal = 0_usize;
    let mut copy_skipped_empty_columns = 0_usize;
    let mut copy_unconvertible_literal_samples: Vec<String> = Vec::new();
    let mut copy_allowed_cache: HashMap<String, bool> = HashMap::new();
    let global_span_rate_per_sec = std::env::var("M2PG_SPAN_RATE_GLOBAL_PER_SEC")
        .ok()
        .and_then(|raw| raw.trim().parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or(15.0);
    let workers_for_rate = worker_count_for_logs.max(1) as f64;
    let db_write_span_rate_per_worker = global_span_rate_per_sec / workers_for_rate;
    let db_write_span_burst_per_worker = std::env::var("M2PG_SPAN_BURST_PER_WORKER")
        .ok()
        .and_then(|raw| raw.trim().parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or_else(|| db_write_span_rate_per_worker.max(1.0).ceil());
    let read_decode_rate_global_per_sec = std::env::var("M2PG_SPAN_RATE_READ_DECODE_PER_SEC")
        .ok()
        .and_then(|raw| raw.trim().parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or(2.0);
    let read_decode_span_rate_per_worker = read_decode_rate_global_per_sec / workers_for_rate;
    let read_decode_span_burst_per_worker =
        std::env::var("M2PG_SPAN_BURST_READ_DECODE_PER_WORKER")
            .ok()
            .and_then(|raw| raw.trim().parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value > 0.0)
            .unwrap_or_else(|| read_decode_span_rate_per_worker.max(1.0).ceil());
    let mut db_write_span_rate_limiter = SpanRateLimiter::new(
        db_write_span_rate_per_worker,
        db_write_span_burst_per_worker,
    );
    let mut read_decode_span_rate_limiter = SpanRateLimiter::new(
        read_decode_span_rate_per_worker,
        read_decode_span_burst_per_worker,
    );
    let mut span_emitted = 0_u64;
    let mut span_suppressed = 0_u64;

    info!(
        "Kafka consumer configuration: worker={}, workers_total={}, group_id_log={}, auto_offset_reset={}, configured_auto_offset_reset={}, max_messages={}, fetch_wait_max_ms={}, poll_size={}, queued_max_messages_kbytes={}, transaction_batch_size={}, flush_batch_after={}",
        worker_identity,
        worker_count_for_logs,
        effective_group_id_log,
        auto_offset_reset,
        configured_auto_offset_reset,
        max_messages
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_owned()),
        kafka_conf
            .fetch_wait_max_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "default".to_owned()),
        kafka_conf
            .poll_size
            .map(|value| value.to_string())
            .unwrap_or_else(|| "default".to_owned()),
        kafka_conf
            .queued_max_messages_kbytes
            .map(|value| value.to_string())
            .unwrap_or_else(|| "default".to_owned()),
        transaction_batch_size,
        flush_batch_after
            .map(|value| format!("{}ms", value.as_millis()))
            .unwrap_or_else(|| "disabled".to_owned())
    );
    info!(
        "Kafka write path configured: mode={}, snapshot_copy_enabled={}",
        configured_write_mode, snapshot_copy_enabled
    );
    info!(
        "Kafka trace span rate limiter: db_write_global_rate_per_sec={}, read_decode_global_rate_per_sec={}, workers_total={}, db_write_per_worker_rate_per_sec={:.3}, db_write_per_worker_burst={}, read_decode_per_worker_rate_per_sec={:.3}, read_decode_per_worker_burst={}",
        global_span_rate_per_sec,
        read_decode_rate_global_per_sec,
        worker_count_for_logs,
        db_write_span_rate_per_worker,
        db_write_span_burst_per_worker,
        read_decode_span_rate_per_worker,
        read_decode_span_burst_per_worker
    );
    info!(
        "Loaded mapping folders for {} collection(s)",
        mappings_by_collection.len()
    );

    // 1. Consume messages in adaptive batches
    let mut stream = consumer.stream();
    let mut tx_open = false;
    let mut tx_pending_processed = 0_usize;
    let mut tx_pending_rows = 0_u64;
    let mut tx_pending_snapshot_rows = 0_u64;
    let mut tx_pending_table_insert_execs: HashMap<String, u64> = HashMap::new();
    let mut tx_batch_started_at: Option<Instant> = None;
    let mut snapshot_buffer_started_at: Option<Instant> = None;
    let mut stop_on_no_lag_streak = 0usize;
    let stop_on_no_lag_required_streak = if worker_count_for_logs > 1 { 3 } else { 10 };
    let mut stop_on_no_lag_seen_end_offsets = false;
    let mut stop_on_no_lag_no_assignment_streak = 0usize;
    let mut stop_on_no_lag_had_assignment = false;
    let stop_on_no_lag_min_runtime = if worker_count_for_logs > 1 {
        Duration::from_secs(60)
    } else {
        Duration::from_secs(30)
    };
    let stop_on_no_lag_no_assignment_grace = stop_on_no_lag_min_runtime;
    let stop_on_no_lag_heartbeat_interval = Duration::from_secs(5);
    let loop_started_at = Instant::now();
    let mut stop_on_no_lag_last_heartbeat_at = loop_started_at;
    let mut stop_on_no_lag_waited_for_runtime_logged = false;

    // In snapshot mode, use a long idle timeout to detect completion.
    // In streaming mode, use a slower default on multi-worker/CI runners to
    // avoid busy poll loops; configurable via kafka.idle_timeout_ms.
    let idle_timeout = if snapshot_mode {
        Duration::from_secs(10)
    } else {
        let default_idle_timeout_ms = if worker_count_for_logs > 1 { 500 } else { 200 };
        let configured_idle_timeout_ms = kafka_conf
            .idle_timeout_ms
            .unwrap_or(default_idle_timeout_ms)
            .max(10);
        Duration::from_millis(configured_idle_timeout_ms)
    };

    if stop_on_no_lag {
        info!(
            "stop_on_no_lag enabled: worker={} workers_total={} required_streak={} idle_timeout_ms={} min_runtime_ms={}",
            worker_identity,
            worker_count_for_logs,
            stop_on_no_lag_required_streak,
            idle_timeout.as_millis(),
            stop_on_no_lag_min_runtime.as_millis()
        );
    }

    let explicit_topics_without_prefix = cli_topics_supplied
        && kafka_conf
            .topic_prefix
            .as_deref()
            .map(str::trim)
            .is_none_or(|value| value.is_empty());
    let mut logged_explicit_topic_fallback = false;

    loop {
        // 1. Wait for the next message with an idle timeout
        let with_kafka_read_span = read_decode_span_rate_limiter.allow();
        if with_kafka_read_span {
            span_emitted += 1;
        } else {
            span_suppressed += 1;
        }
        let read_future = read_msg_from_topics(&mut stream, idle_timeout);
        let next_item = match if with_kafka_read_span {
            read_future
                .instrument(tracing::info_span!(
                    "kafka_import.stage.kafka_read.poll",
                    worker = worker_identity.as_str(),
                    project_name = conf.project_dir.as_str(),
                    namespace = namespace.as_str()
                ))
                .await
        } else {
            read_future.await
        } {
            ReadStageEvent::Message(msg_res) => msg_res,
            ReadStageEvent::StreamEnded => {
                if snapshot_copy_enabled {
                    flush_snapshot_copy_buffer(
                        &pg_client,
                        &dlq_producer,
                        &mut snapshot_buffer,
                        "stream_end",
                        &mappings_by_collection,
                        effective_target_schema.as_deref(),
                        &mut table_insert_execs,
                        &mut copy_allowed_cache,
                        &mut processed,
                        &mut total_affected_rows,
                        &mut snapshot_inserted_rows,
                        &mut apply_failed,
                        &mut dlq_published,
                        &mut dlq_failed,
                        &mut fallback_payload_as_after,
                        &mut skipped_missing_after,
                        &mut skipped_missing_before,
                        &mut skipped_missing_required_snapshot,
                        &mut skipped_missing_required_snapshot_by_table,
                        &mut skipped_missing_required_snapshot_samples,
                        &mut copy_skipped_by_table,
                        &mut copy_skipped_samples,
                        &mut skipped_non_data_op,
                        &mut copy_attempts,
                        &mut copy_failed,
                        &mut fallback_replay_attempts,
                        &mut fallback_replay_failed,
                        &mut fallback_replay_dlq_published,
                        &mut fallback_replay_dlq_failed,
                        &mut copy_eligible_rows,
                        &mut copy_skipped_copy_disabled,
                        &mut copy_skipped_missing_mapping,
                        &mut copy_skipped_non_root_mapping,
                        &mut copy_skipped_unconvertible_literal,
                        &mut copy_skipped_empty_columns,
                        &mut copy_unconvertible_literal_samples,
                    )
                    .await?;
                }
                info!("Kafka stream ended by broker/consumer");
                break;
            }
            ReadStageEvent::IdleTimeout => {
                // Timeout hit!
                if snapshot_mode {
                    if snapshot_copy_enabled {
                        flush_snapshot_copy_buffer(
                            &pg_client,
                            &dlq_producer,
                            &mut snapshot_buffer,
                            "snapshot_idle_timeout",
                            &mappings_by_collection,
                            effective_target_schema.as_deref(),
                            &mut table_insert_execs,
                            &mut copy_allowed_cache,
                            &mut processed,
                            &mut total_affected_rows,
                            &mut snapshot_inserted_rows,
                            &mut apply_failed,
                            &mut dlq_published,
                            &mut dlq_failed,
                            &mut fallback_payload_as_after,
                            &mut skipped_missing_after,
                            &mut skipped_missing_before,
                            &mut skipped_missing_required_snapshot,
                            &mut skipped_missing_required_snapshot_by_table,
                            &mut skipped_missing_required_snapshot_samples,
                            &mut copy_skipped_by_table,
                            &mut copy_skipped_samples,
                            &mut skipped_non_data_op,
                            &mut copy_attempts,
                            &mut copy_failed,
                            &mut fallback_replay_attempts,
                            &mut fallback_replay_failed,
                            &mut fallback_replay_dlq_published,
                            &mut fallback_replay_dlq_failed,
                            &mut copy_eligible_rows,
                            &mut copy_skipped_copy_disabled,
                            &mut copy_skipped_missing_mapping,
                            &mut copy_skipped_non_root_mapping,
                            &mut copy_skipped_unconvertible_literal,
                            &mut copy_skipped_empty_columns,
                            &mut copy_unconvertible_literal_samples,
                        )
                        .await?;
                    }
                    if polled == 0 {
                        warn!(
                            "Snapshot mode: idle timeout reached (10s without new messages), no Kafka messages were consumed; check topic selection/prefix and connector status."
                        );
                    } else {
                        info!(
                            "Snapshot mode complete: idle timeout reached (10s without new messages), stopping."
                        );
                    }
                    break;
                }

                // In streaming COPY mode, idle gaps should flush trailing partial batches.
                // Without this, row persistence can appear quantized to transaction_batch_size
                // when workers are stopped shortly after lag reaches zero.
                if snapshot_copy_enabled && !snapshot_buffer.is_empty() {
                    flush_snapshot_copy_buffer(
                        &pg_client,
                        &dlq_producer,
                        &mut snapshot_buffer,
                        "stream_idle_timeout",
                        &mappings_by_collection,
                        effective_target_schema.as_deref(),
                        &mut table_insert_execs,
                        &mut copy_allowed_cache,
                        &mut processed,
                        &mut total_affected_rows,
                        &mut snapshot_inserted_rows,
                        &mut apply_failed,
                        &mut dlq_published,
                        &mut dlq_failed,
                        &mut fallback_payload_as_after,
                        &mut skipped_missing_after,
                        &mut skipped_missing_before,
                        &mut skipped_missing_required_snapshot,
                        &mut skipped_missing_required_snapshot_by_table,
                        &mut skipped_missing_required_snapshot_samples,
                        &mut copy_skipped_by_table,
                        &mut copy_skipped_samples,
                        &mut skipped_non_data_op,
                        &mut copy_attempts,
                        &mut copy_failed,
                        &mut fallback_replay_attempts,
                        &mut fallback_replay_failed,
                        &mut fallback_replay_dlq_published,
                        &mut fallback_replay_dlq_failed,
                        &mut copy_eligible_rows,
                        &mut copy_skipped_copy_disabled,
                        &mut copy_skipped_missing_mapping,
                        &mut copy_skipped_non_root_mapping,
                        &mut copy_skipped_unconvertible_literal,
                        &mut copy_skipped_empty_columns,
                        &mut copy_unconvertible_literal_samples,
                    )
                    .await?;
                    snapshot_buffer_started_at = None;
                }

                // Under normal streaming, flush any pending transaction when idle to keep latency low
                if transaction_batching_enabled && tx_open && tx_pending_processed > 0 {
                    if let Err(commit_err) = pg_client.batch_execute("COMMIT").await {
                        error!("Failed to commit idle transaction batch: {:?}", commit_err);
                        let _ = pg_client.batch_execute("ROLLBACK").await;
                        tx_rolled_back_batches += 1;
                        tx_rolled_back_messages += tx_pending_processed;
                    } else {
                        processed += tx_pending_processed;
                        total_affected_rows += tx_pending_rows;
                        for (table, count) in tx_pending_table_insert_execs.drain() {
                            *table_insert_execs.entry(table).or_insert(0) += count;
                        }
                        info!(
                            "Flushed idle transaction batch: processed={} messages, affected_rows={}",
                            tx_pending_processed, tx_pending_rows
                        );
                    }
                    tx_open = false;
                    tx_pending_processed = 0;
                    tx_pending_rows = 0;
                    tx_batch_started_at = None;
                }

                if stop_on_no_lag {
                    match consumer_lag_snapshot(&consumer) {
                        Ok(Some((assigned_partitions, total_lag, total_end_offsets))) => {
                            stop_on_no_lag_had_assignment = true;
                            stop_on_no_lag_no_assignment_streak = 0;
                            let elapsed = Instant::now().duration_since(loop_started_at);
                            let min_runtime_reached = elapsed >= stop_on_no_lag_min_runtime;
                            let buffers_empty =
                                snapshot_buffer.is_empty() && tx_pending_processed == 0;
                            if total_end_offsets > 0 {
                                stop_on_no_lag_seen_end_offsets = true;
                            }
                            let ready_for_stable = stop_on_no_lag_seen_end_offsets
                                && total_lag == 0
                                && buffers_empty
                                && min_runtime_reached;

                            if ready_for_stable {
                                stop_on_no_lag_streak += 1;
                                info!(
                                    "stop_on_no_lag check: worker={} assigned_partitions={} total_lag={} total_end_offsets={} stable={}/{}",
                                    worker_identity,
                                    assigned_partitions,
                                    total_lag,
                                    total_end_offsets,
                                    stop_on_no_lag_streak,
                                    stop_on_no_lag_required_streak
                                );
                                if stop_on_no_lag_streak >= stop_on_no_lag_required_streak {
                                    info!(
                                        "stop_on_no_lag satisfied: worker={} lag remained zero for {} idle checks; stopping kafka-import",
                                        worker_identity,
                                        stop_on_no_lag_required_streak
                                    );
                                    break;
                                }
                            } else {
                                if stop_on_no_lag_streak > 0 {
                                    info!(
                                        "stop_on_no_lag reset: worker={} assigned_partitions={} previous_stable={} reason=conditions_changed seen_end_offsets={} total_lag={} buffers_empty={} min_runtime_reached={}",
                                        worker_identity,
                                        assigned_partitions,
                                        stop_on_no_lag_streak,
                                        stop_on_no_lag_seen_end_offsets,
                                        total_lag,
                                        buffers_empty,
                                        min_runtime_reached
                                    );
                                }
                                if stop_on_no_lag_seen_end_offsets
                                    && total_lag == 0
                                    && buffers_empty
                                    && !min_runtime_reached
                                    && !stop_on_no_lag_waited_for_runtime_logged
                                {
                                    info!(
                                        "stop_on_no_lag wait: worker={} lag is zero but min runtime not reached yet (elapsed_ms={}, required_ms={})",
                                        worker_identity,
                                        elapsed.as_millis(),
                                        stop_on_no_lag_min_runtime.as_millis()
                                    );
                                    stop_on_no_lag_waited_for_runtime_logged = true;
                                }

                                if Instant::now().duration_since(stop_on_no_lag_last_heartbeat_at)
                                    >= stop_on_no_lag_heartbeat_interval
                                {
                                    info!(
                                        "stop_on_no_lag heartbeat: worker={} state=waiting assigned_partitions={} total_lag={} total_end_offsets={} seen_end_offsets={} buffers_empty={} min_runtime_reached={} stable={}/{} elapsed_ms={}",
                                        worker_identity,
                                        assigned_partitions,
                                        total_lag,
                                        total_end_offsets,
                                        stop_on_no_lag_seen_end_offsets,
                                        buffers_empty,
                                        min_runtime_reached,
                                        stop_on_no_lag_streak,
                                        stop_on_no_lag_required_streak,
                                        elapsed.as_millis()
                                    );
                                    stop_on_no_lag_last_heartbeat_at = Instant::now();
                                }

                                stop_on_no_lag_streak = 0;
                            }
                        }
                        Ok(None) => {
                            if stop_on_no_lag_streak > 0 {
                                info!(
                                    "stop_on_no_lag reset: worker={} previous_stable={} reason=no_assignment_snapshot",
                                    worker_identity,
                                    stop_on_no_lag_streak
                                );
                            }
                            stop_on_no_lag_streak = 0;
                            let no_assignment_allowed = stop_on_no_lag_had_assignment
                                || Instant::now().duration_since(loop_started_at)
                                    >= stop_on_no_lag_no_assignment_grace;
                            if no_assignment_allowed
                                && snapshot_buffer.is_empty()
                                && tx_pending_processed == 0
                            {
                                stop_on_no_lag_no_assignment_streak += 1;
                                info!(
                                    "stop_on_no_lag check: worker={} assigned_partitions=0 stable_no_assignment={}/{}",
                                    worker_identity,
                                    stop_on_no_lag_no_assignment_streak,
                                    stop_on_no_lag_required_streak
                                );
                                if stop_on_no_lag_no_assignment_streak
                                    >= stop_on_no_lag_required_streak
                                {
                                    info!(
                                        "stop_on_no_lag satisfied: worker={} has no assigned partitions for {} idle checks; stopping kafka-import",
                                        worker_identity,
                                        stop_on_no_lag_required_streak
                                    );
                                    break;
                                }
                            } else if Instant::now()
                                .duration_since(stop_on_no_lag_last_heartbeat_at)
                                >= stop_on_no_lag_heartbeat_interval
                            {
                                info!(
                                    "stop_on_no_lag heartbeat: worker={} state=waiting_no_assignment no_assignment_allowed={} had_assignment={} buffers_empty={} pending_rows={} stable_no_assignment={}/{} elapsed_ms={}",
                                    worker_identity,
                                    no_assignment_allowed,
                                    stop_on_no_lag_had_assignment,
                                    snapshot_buffer.is_empty() && tx_pending_processed == 0,
                                    tx_pending_processed,
                                    stop_on_no_lag_no_assignment_streak,
                                    stop_on_no_lag_required_streak,
                                    Instant::now().duration_since(loop_started_at).as_millis()
                                );
                                stop_on_no_lag_last_heartbeat_at = Instant::now();
                            }
                        }
                        Err(err) => {
                            warn!("stop_on_no_lag lag snapshot failed: {:#}", err);
                            stop_on_no_lag_streak = 0;
                            stop_on_no_lag_no_assignment_streak = 0;
                        }
                    }
                }

                continue;
            }
        };

        polled += 1;
        macro_rules! maybe_log_progress {
            () => {
                if polled % batch_log_messages == 0 {
                    let buffered_inflight = if snapshot_copy_enabled {
                        snapshot_buffer.len()
                    } else if transaction_batching_enabled {
                        tx_pending_processed
                    } else {
                        0
                    };
                    let effective_processed = processed + buffered_inflight;
                    info!(
                        "Kafka progress: polled={}, processed={}({}), flushed_processed={}, buffered_inflight={}, skipped_topic={}, skipped_db={}, skipped_mapping={}, skipped_no_payload={}, skipped_non_data_op={}, skipped_missing_after={}, skipped_missing_before={}, skipped_missing_required_snapshot={}, decode_failed={}, apply_failed={}, commit_failed={}, trace_spans_emitted={}, trace_spans_suppressed={}",
                        polled, effective_processed, processed_mode, processed, buffered_inflight, skipped_topic, skipped_db, skipped_mapping, skipped_no_payload, skipped_non_data_op, skipped_missing_after, skipped_missing_before, skipped_missing_required_snapshot, decode_failed, apply_failed, commit_failed, span_emitted, span_suppressed
                    );
                    debug!(
                        "Kafka progress ops: c={}, u={}, r={}, d={}, other={}, total_affected_rows={}, fallback_payload_as_after={}",
                        op_c, op_u, op_r, op_d, op_other, total_affected_rows, fallback_payload_as_after
                    );
                    debug!("Kafka progress DLQ: dlq_published={}, dlq_failed={}", dlq_published, dlq_failed);
                    debug!(
                        "Kafka progress tables: impacted_tables={}, insert_execs={}",
                        table_insert_execs.len(),
                        format_table_insert_exec_summary(&table_insert_execs)
                    );
                    if snapshot_copy_enabled {
                        debug!(
                            "Kafka progress snapshot-copy: batch_size={}, buffered_messages={}, copy_attempts={}, copy_failed={}, fallback_replay_attempts={}, fallback_replay_failed={}, fallback_replay_dlq_published={}, fallback_replay_dlq_failed={}, copy_eligible_rows={}, copy_skipped_copy_disabled={}, copy_skipped_missing_mapping={}, copy_skipped_non_root_mapping={}, copy_skipped_unconvertible_literal={}, copy_skipped_empty_columns={}, copy_unconvertible_literal_sample={}",
                            transaction_batch_size,
                            snapshot_buffer.len(),
                            copy_attempts,
                            copy_failed,
                            fallback_replay_attempts,
                            fallback_replay_failed,
                            fallback_replay_dlq_published,
                                fallback_replay_dlq_failed,
                                copy_eligible_rows,
                                copy_skipped_copy_disabled,
                                copy_skipped_missing_mapping,
                                copy_skipped_non_root_mapping,
                                copy_skipped_unconvertible_literal,
                                copy_skipped_empty_columns,
                                if copy_unconvertible_literal_samples.is_empty() {
                                    "none".to_owned()
                                } else {
                                    copy_unconvertible_literal_samples.join(" || ")
                                }
                        );
                    }
                    if transaction_batching_enabled {
                        debug!(
                            "Kafka progress tx-batch: trx_batch_size={}, staged_messages={}, rolled_back_batches={}, rolled_back_messages={}",
                            transaction_batch_size,
                            tx_pending_processed,
                            tx_rolled_back_batches,
                            tx_rolled_back_messages
                        );
                    }
                }
            };
        }

        let message = match next_item {
            Ok(msg) => msg,
            Err(err) => {
                warn!(
                    "{} warning: Kafka consume error: {err}",
                    connection_failed_context("kafka", "consume")
                );
                maybe_log_progress!();
                continue;
            }
        };

        let topic = message.topic();
        let Some((db_name, mut collection_name)) = parse_topic_db_collection(
            topic,
            kafka_conf.topic_prefix.as_deref(),
            Some(namespace_db_name),
        ) else {
            skipped_topic += 1;
            if skipped_topic <= 5 {
                warn!(
                    "kafka-import skipped message: reason=topic_not_matching_expected_format topic={} expected_db={} topic_prefix={:?}",
                    topic,
                    namespace_db_name,
                    kafka_conf.topic_prefix
                );
            } else if skipped_topic == 6 {
                warn!("topic-format skip log limit reached (5); suppressing additional per-row details");
            }
            maybe_log_progress!();
            continue;
        };
        if db_name != namespace_db_name {
            if explicit_topics_without_prefix {
                if let Some((_, tail_collection)) = topic.rsplit_once('.') {
                    if !logged_explicit_topic_fallback {
                        warn!(
                            "--topics provided without topic_prefix: interpreting topic tail as collection and using source namespace '{}' as db",
                            namespace_db_name
                        );
                        logged_explicit_topic_fallback = true;
                    }
                    collection_name = tail_collection.to_owned();
                } else {
                    skipped_db += 1;
                    if skipped_db <= 5 {
                        warn!(
                            "kafka-import skipped message: reason=db_name_mismatch topic={} parsed_db={} expected_db={}",
                            topic,
                            db_name,
                            namespace_db_name
                        );
                    } else if skipped_db == 6 {
                        warn!("db-mismatch skip log limit reached (5); suppressing additional per-row details");
                    }
                    maybe_log_progress!();
                    continue;
                }
            } else {
                skipped_db += 1;
                if skipped_db <= 5 {
                    warn!(
                        "kafka-import skipped message: reason=db_name_mismatch topic={} parsed_db={} expected_db={}",
                        topic,
                        db_name,
                        namespace_db_name
                    );
                } else if skipped_db == 6 {
                    warn!("db-mismatch skip log limit reached (5); suppressing additional per-row details");
                }
                maybe_log_progress!();
                continue;
            }
        }

        let folder_name = sanitize_name(&collection_name);
        let Some(mappings) = mappings_by_collection.get(&folder_name) else {
            skipped_mapping += 1;
            warn!(
                "kafka-import skipped message: reason=mapping_not_found topic={} collection={} folder_name={} expected_mapping_folder={}",
                topic,
                collection_name,
                folder_name,
                folder_name
            );
            maybe_log_progress!();
            continue;
        };

        let Some(bytes) = message.payload() else {
            skipped_no_payload += 1;
            if skipped_no_payload <= 5 {
                warn!(
                    "kafka-import skipped message: reason=empty_payload topic={} collection={}",
                    topic, collection_name
                );
            } else if skipped_no_payload == 6 {
                warn!(
                    "empty-payload skip log limit reached (5); suppressing additional per-row details"
                );
            }
            maybe_log_progress!();
            continue;
        };
        let with_decode_span = read_decode_span_rate_limiter.allow();
        if with_decode_span {
            span_emitted += 1;
        } else {
            span_suppressed += 1;
        }
        let decode_future = decode_message_value(
            bytes,
            kafka_conf.schema_registry_url.as_deref(),
            kafka_conf.schema_registry_username.as_deref(),
            kafka_conf.schema_registry_password.as_deref(),
            &http_client,
            &mut schema_cache,
        );
        let decoded = match if with_decode_span {
            decode_future
                .instrument(tracing::info_span!(
                    "kafka_import.stage.decode.message",
                    worker = worker_identity.as_str(),
                    topic = topic,
                    collection = collection_name.as_str(),
                    project_name = conf.project_dir.as_str(),
                    namespace = namespace.as_str()
                ))
                .await
        } else {
            decode_future.await
        } {
            Ok(value) => value,
            Err(err) => {
                decode_failed += 1;
                warn!("failed to decode message on topic {topic}: {err}");
                maybe_log_progress!();
                continue;
            }
        };

        let payload = decoded
            .get("payload")
            .and_then(|value| value.as_object())
            .map(|obj| Value::Object(obj.clone()))
            .unwrap_or(decoded);

        let op = payload
            .get("op")
            .map(unwrap_union_tagged_value)
            .and_then(|value| value.as_str())
            .unwrap_or("u");
        match op {
            "c" => op_c += 1,
            "u" => op_u += 1,
            "r" => op_r += 1,
            "d" => op_d += 1,
            _ => op_other += 1,
        }
        let after = match debezium_document(payload.get("after").map(unwrap_union_tagged_value)) {
            Ok(value) => value,
            Err(err) => {
                decode_failed += 1;
                warn!("failed to parse Debezium 'after' for topic {topic}: {err:#}");
                maybe_log_progress!();
                continue;
            }
        };
        let before = match debezium_document(payload.get("before").map(unwrap_union_tagged_value)) {
            Ok(value) => value,
            Err(err) => {
                decode_failed += 1;
                warn!("failed to parse Debezium 'before' for topic {topic}: {err:#}");
                maybe_log_progress!();
                continue;
            }
        };

        if snapshot_copy_enabled {
            snapshot_buffer.push(SnapshotBufferedMessage {
                topic: topic.to_owned(),
                collection_name: collection_name.clone(),
                folder_name: folder_name.clone(),
                op: op.to_owned(),
                payload: payload.clone(),
                before,
                after,
                key_bytes: message.key().map(|key| key.to_vec()),
                payload_bytes: message.payload().map(|payload| payload.to_vec()),
            });
            if snapshot_buffer.len() == 1 {
                snapshot_buffer_started_at = Some(Instant::now());
            }

            let flush_due_to_size = snapshot_buffer.len() >= transaction_batch_size;
            let flush_due_to_elapsed = flush_batch_after.is_some_and(|threshold| {
                snapshot_buffer_started_at
                    .is_some_and(|started_at| started_at.elapsed() >= threshold)
            });
            if flush_due_to_size || flush_due_to_elapsed {
                let flush_reason = match (flush_due_to_size, flush_due_to_elapsed) {
                    (true, true) => "full",
                    (true, false) => "full",
                    (false, true) => "time_elapsed",
                    (false, false) => "unknown",
                };
                let with_span = db_write_span_rate_limiter.allow();
                if with_span {
                    span_emitted += 1;
                } else {
                    span_suppressed += 1;
                }
                let buffered_messages = snapshot_buffer.len() as u64;
                flush_snapshot_copy_buffer(
                    &pg_client,
                    &dlq_producer,
                    &mut snapshot_buffer,
                    flush_reason,
                    &mappings_by_collection,
                    effective_target_schema.as_deref(),
                    &mut table_insert_execs,
                    &mut copy_allowed_cache,
                    &mut processed,
                    &mut total_affected_rows,
                    &mut snapshot_inserted_rows,
                    &mut apply_failed,
                    &mut dlq_published,
                    &mut dlq_failed,
                    &mut fallback_payload_as_after,
                    &mut skipped_missing_after,
                    &mut skipped_missing_before,
                    &mut skipped_missing_required_snapshot,
                    &mut skipped_missing_required_snapshot_by_table,
                    &mut skipped_missing_required_snapshot_samples,
                    &mut copy_skipped_by_table,
                    &mut copy_skipped_samples,
                    &mut skipped_non_data_op,
                    &mut copy_attempts,
                    &mut copy_failed,
                    &mut fallback_replay_attempts,
                    &mut fallback_replay_failed,
                    &mut fallback_replay_dlq_published,
                    &mut fallback_replay_dlq_failed,
                    &mut copy_eligible_rows,
                    &mut copy_skipped_copy_disabled,
                    &mut copy_skipped_missing_mapping,
                    &mut copy_skipped_non_root_mapping,
                    &mut copy_skipped_unconvertible_literal,
                    &mut copy_skipped_empty_columns,
                    &mut copy_unconvertible_literal_samples,
                )
                .instrument(if with_span {
                    tracing::info_span!(
                        "kafka_import.stage.db_write.copy_mode",
                        worker = worker_identity.as_str(),
                        flush_reason = flush_reason,
                        buffered_messages = buffered_messages,
                        project_name = conf.project_dir.as_str(),
                        namespace = namespace.as_str()
                    )
                } else {
                    tracing::Span::none()
                })
                .await?;
                snapshot_buffer_started_at = None;
            }

            maybe_log_progress!();

            if let Some(limit) = max_messages {
                if processed >= limit {
                    info!("Reached --max-messages limit ({limit}), stopping.");
                    break;
                }
            }

            continue;
        }

        // 2. Open PostgreSQL transaction if not already open
        if transaction_batching_enabled && !tx_open {
            if let Err(err) = pg_client.batch_execute("BEGIN").await {
                error!("Failed to BEGIN transaction batch: {:?}", err);
                return Err(anyhow!(err));
            }
            tx_open = true;
        }

        let with_span = db_write_span_rate_limiter.allow();
        if with_span {
            span_emitted += 1;
        } else {
            span_suppressed += 1;
        }
        let write_outcome = write_to_pg(
            &pg_client,
            &payload,
            op,
            before.as_ref(),
            after.as_ref(),
            mappings,
            &collection_name,
            topic,
            effective_target_schema.as_deref(),
            transaction_batching_enabled,
            &mut tx_pending_table_insert_execs,
            &mut table_insert_execs,
            &mut fallback_payload_as_after,
            &mut skipped_missing_after,
            &mut skipped_missing_before,
            &mut skipped_missing_required_snapshot,
            &mut skipped_missing_required_snapshot_by_table,
            &mut skipped_missing_required_snapshot_samples,
            &mut skipped_non_data_op,
        )
        .instrument(if with_span {
            tracing::info_span!(
                "kafka_import.stage.db_write.sql_apply",
                worker = worker_identity.as_str(),
                topic = topic,
                collection = collection_name.as_str(),
                op = op,
                project_name = conf.project_dir.as_str(),
                namespace = namespace.as_str(),
            )
        } else {
            tracing::Span::none()
        })
        .await;

        let applied_rows = match write_outcome {
            Ok(WriteStageOutcome::Applied(rows)) => rows,
            Ok(WriteStageOutcome::Skipped) => {
                maybe_log_progress!();
                continue;
            }
            Err(err) => {
                apply_failed += 1;
                warn!(
                    "apply failed topic={} collection={} op={}: {:#}\n  hint: extend map_extended_json_literal() in src/bin/mongo2pg.rs to support this payload shape",
                    topic, collection_name, op, err
                );

                let key_bytes = message.key().map(|key| key.to_vec());
                let payload_bytes = message.payload().map(|payload| payload.to_vec());
                if let Some(payload) = payload_bytes.as_deref() {
                    match publish_to_dlq(&dlq_producer, topic, key_bytes.as_deref(), payload).await
                    {
                        Ok(()) => {
                            dlq_published += 1;
                            warn!("message copied to DLQ topic=dlq_{}", topic);
                        }
                        Err(dlq_err) => {
                            dlq_failed += 1;
                            warn!("failed to copy message to DLQ: {dlq_err:#}");
                        }
                    }
                } else {
                    dlq_failed += 1;
                    warn!("failed to copy message to DLQ: message payload missing");
                }

                if transaction_batching_enabled && tx_open {
                    if let Err(rollback_err) = pg_client.batch_execute("ROLLBACK").await {
                        warn!(
                            "failed to rollback kafka-import transaction batch: {:#}",
                            rollback_err
                        );
                    }
                    tx_open = false;
                    tx_rolled_back_batches += 1;
                    tx_rolled_back_messages += tx_pending_processed;
                    tx_pending_processed = 0;
                    tx_pending_rows = 0;
                    tx_pending_snapshot_rows = 0;
                    tx_pending_table_insert_execs.clear();
                    tx_batch_started_at = None;
                }

                maybe_log_progress!();
                continue;
            }
        };

        if let Err(err) = commit_offset(&consumer, &message, enable_auto_commit) {
            commit_failed += 1;
            warn!("failed to commit offset for topic {}: {:#}", topic, err);
            maybe_log_progress!();
            continue;
        }

        // 3. Increment counters and commit if we hit the batch size limit
        if transaction_batching_enabled {
            tx_pending_processed += 1;
            tx_pending_rows += applied_rows;
            if tx_pending_processed == 1 {
                tx_batch_started_at = Some(Instant::now());
            }
            if snapshot_mode {
                tx_pending_snapshot_rows += applied_rows;
            }

            let flush_due_to_size = tx_pending_processed >= transaction_batch_size;
            let flush_due_to_elapsed = flush_batch_after.is_some_and(|threshold| {
                tx_batch_started_at.is_some_and(|started_at| started_at.elapsed() >= threshold)
            });
            if flush_due_to_size || flush_due_to_elapsed {
                pg_client
                    .batch_execute("COMMIT")
                    .await
                    .context("failed to commit kafka-import transaction batch")?;
                tx_open = false;

                processed += tx_pending_processed;
                total_affected_rows += tx_pending_rows;
                if snapshot_mode {
                    snapshot_inserted_rows += tx_pending_snapshot_rows;
                }
                for (table, count) in tx_pending_table_insert_execs.drain() {
                    *table_insert_execs.entry(table).or_insert(0) += count;
                }

                tx_pending_processed = 0;
                tx_pending_rows = 0;
                tx_pending_snapshot_rows = 0;
                tx_batch_started_at = None;
            }
        } else {
            processed += 1;
            total_affected_rows += applied_rows;
            if snapshot_mode {
                snapshot_inserted_rows += applied_rows;
            }
        }

        let effective_processed = if transaction_batching_enabled {
            processed + tx_pending_processed
        } else {
            processed
        };

        if effective_processed <= 5 || effective_processed % batch_log_messages == 0 {
            debug!(
                "Kafka apply ok: processed={} topic={} collection={} op={} affected_rows={}",
                effective_processed, topic, collection_name, op, applied_rows
            );
        }

        maybe_log_progress!();

        if let Some(limit) = max_messages {
            if effective_processed >= limit {
                info!("Reached --max-messages limit ({limit}), stopping.");
                break;
            }
        }
    }

    if snapshot_copy_enabled {
        flush_snapshot_copy_buffer(
            &pg_client,
            &dlq_producer,
            &mut snapshot_buffer,
            "final_flush",
            &mappings_by_collection,
            effective_target_schema.as_deref(),
            &mut table_insert_execs,
            &mut copy_allowed_cache,
            &mut processed,
            &mut total_affected_rows,
            &mut snapshot_inserted_rows,
            &mut apply_failed,
            &mut dlq_published,
            &mut dlq_failed,
            &mut fallback_payload_as_after,
            &mut skipped_missing_after,
            &mut skipped_missing_before,
            &mut skipped_missing_required_snapshot,
            &mut skipped_missing_required_snapshot_by_table,
            &mut skipped_missing_required_snapshot_samples,
            &mut copy_skipped_by_table,
            &mut copy_skipped_samples,
            &mut skipped_non_data_op,
            &mut copy_attempts,
            &mut copy_failed,
            &mut fallback_replay_attempts,
            &mut fallback_replay_failed,
            &mut fallback_replay_dlq_published,
            &mut fallback_replay_dlq_failed,
            &mut copy_eligible_rows,
            &mut copy_skipped_copy_disabled,
            &mut copy_skipped_missing_mapping,
            &mut copy_skipped_non_root_mapping,
            &mut copy_skipped_unconvertible_literal,
            &mut copy_skipped_empty_columns,
            &mut copy_unconvertible_literal_samples,
        )
        .await?;
    }

    // 4. Final flush of any trailing open transaction before exiting
    if transaction_batching_enabled && tx_pending_processed > 0 {
        pg_client
            .batch_execute("COMMIT")
            .await
            .context("failed to commit final kafka-import transaction batch")?;
        tx_open = false;

        processed += tx_pending_processed;
        total_affected_rows += tx_pending_rows;
        if snapshot_mode {
            snapshot_inserted_rows += tx_pending_snapshot_rows;
        }
        for (table, count) in tx_pending_table_insert_execs.drain() {
            *table_insert_execs.entry(table).or_insert(0) += count;
        }
    }

    if transaction_batching_enabled && tx_open {
        if let Err(rollback_err) = pg_client.batch_execute("ROLLBACK").await {
            warn!(
                "failed to rollback trailing open kafka-import transaction batch: {:#}",
                rollback_err
            );
        }
    }

    info!(
        "Kafka import finished. polled={}, processed={}({}), skipped_topic={}, skipped_db={}, skipped_mapping={}, skipped_no_payload={}, skipped_non_data_op={}, skipped_missing_after={}, skipped_missing_before={}, skipped_missing_required_snapshot={}, decode_failed={}, apply_failed={}, commit_failed={}, trace_spans_emitted={}, trace_spans_suppressed={}",
        polled,
        processed,
        processed_mode,
        skipped_topic,
        skipped_db,
        skipped_mapping,
        skipped_no_payload,
        skipped_non_data_op,
        skipped_missing_after,
        skipped_missing_before,
        skipped_missing_required_snapshot,
        decode_failed,
        apply_failed,
        commit_failed,
        span_emitted,
        span_suppressed
    );
    // info!(
    //     "Kafka import op summary: c={}, u={}, r={}, d={}, other={}, total_affected_rows={}, fallback_payload_as_after={}",
    //     op_c,
    //     op_u,
    //     op_r,
    //     op_d,
    //     op_other,
    //     total_affected_rows,
    //     fallback_payload_as_after
    // );
    // info!(
    //     "Kafka import DLQ summary: dlq_published={}, dlq_failed={}",
    //     dlq_published, dlq_failed
    // );
    // if snapshot_copy_enabled {
    //     info!(
    //         "Kafka import snapshot-copy summary: batch_size={}, copy_attempts={}, copy_failed={}, fallback_replay_attempts={}, fallback_replay_failed={}, fallback_replay_dlq_published={}, fallback_replay_dlq_failed={}, copy_eligible_rows={}, copy_skipped_missing_mapping={}, copy_skipped_non_root_mapping={}, copy_skipped_unconvertible_literal={}, copy_skipped_empty_columns={}, copy_unconvertible_literal_sample={}",
    //         transaction_batch_size,
    //         copy_attempts,
    //         copy_failed,
    //         fallback_replay_attempts,
    //         fallback_replay_failed,
    //         fallback_replay_dlq_published,
    //         fallback_replay_dlq_failed,
    //         copy_eligible_rows,
    //         copy_skipped_missing_mapping,
    //         copy_skipped_non_root_mapping,
    //         copy_skipped_unconvertible_literal,
    //         copy_skipped_empty_columns,
    //         if copy_unconvertible_literal_samples.is_empty() {
    //             "none".to_owned()
    //         } else {
    //             copy_unconvertible_literal_samples.join(" || ")
    //         }
    //     );
    // }
    // info!(
    //     "Kafka import table summary: impacted_tables={}, insert_execs={}",
    //     table_insert_execs.len(),
    //     format_table_insert_exec_summary(&table_insert_execs)
    // );
    // if transaction_batching_enabled {
    //     info!(
    //         "Kafka import tx-batch summary: batch_size={}, rolled_back_batches={}, rolled_back_messages={}",
    //         transaction_batch_size,
    //         tx_rolled_back_batches,
    //         tx_rolled_back_messages
    //     );
    // }
    // if snapshot_mode {
    //     info!(
    //         "Snapshot summary: total affected rows applied to PostgreSQL={} (upserts + child inserts)",
    //         snapshot_inserted_rows
    //     );
    // }

    // let write_path_used = if snapshot_copy_enabled {
    //     if copy_attempts == 0 {
    //         "insert_only_passthrough"
    //     } else if copy_failed == 0 {
    //         "copy"
    //     } else if copy_failed < copy_attempts {
    //         "copy_with_insert_fallback"
    //     } else {
    //         "insert_fallback_after_copy_failures"
    //     }
    // } else {
    //     "insert_upsert"
    // };
    // info!(
    //     "Kafka write path used: mode={}, copy_attempts={}, copy_failed={}, fallback_replay_attempts={}, table_insert_execs={}",
    //     write_path_used,
    //     copy_attempts,
    //     copy_failed,
    //     fallback_replay_attempts,
    //     format_table_insert_exec_summary(&table_insert_execs)
    // );

    #[derive(Serialize)]
    struct KafkaImportWriteModeStatsYaml {
        snapshot_copy_enabled: bool,
        transaction_batch_size: usize,
        trace_spans_emitted: u64,
        trace_spans_suppressed: u64,
        copy_attempts: usize,
        copy_failed: usize,
        fallback_replay_attempts: usize,
        fallback_replay_failed: usize,
        fallback_replay_dlq_published: usize,
        fallback_replay_dlq_failed: usize,
        copy_eligible_rows: usize,
        copy_skipped_missing_mapping: usize,
        copy_skipped_non_root_mapping: usize,
        copy_skipped_unconvertible_literal: usize,
        copy_skipped_empty_columns: usize,
        copy_unconvertible_literal_samples: Vec<String>,
        dlq_published: usize,
        dlq_failed: usize,
        apply_failed: usize,
        processed: usize,
        total_affected_rows: u64,
        snapshot_inserted_rows: u64,
        skipped_missing_required_snapshot: usize,
        skipped_missing_required_snapshot_by_table: HashMap<String, usize>,
        skipped_missing_required_snapshot_samples: Vec<String>,
        copy_skipped_by_table: HashMap<String, usize>,
        copy_skipped_samples: Vec<String>,
    }

    let kafka_import_write_mode_stats = KafkaImportWriteModeStatsYaml {
        snapshot_copy_enabled,
        transaction_batch_size,
        trace_spans_emitted: span_emitted,
        trace_spans_suppressed: span_suppressed,
        copy_attempts,
        copy_failed,
        fallback_replay_attempts,
        fallback_replay_failed,
        fallback_replay_dlq_published,
        fallback_replay_dlq_failed,
        copy_eligible_rows,
        copy_skipped_missing_mapping,
        copy_skipped_non_root_mapping,
        copy_skipped_unconvertible_literal,
        copy_skipped_empty_columns,
        copy_unconvertible_literal_samples,
        dlq_published,
        dlq_failed,
        apply_failed,
        processed,
        total_affected_rows,
        snapshot_inserted_rows,
        skipped_missing_required_snapshot,
        skipped_missing_required_snapshot_by_table,
        skipped_missing_required_snapshot_samples,
        copy_skipped_by_table,
        copy_skipped_samples,
    };

    if !is_worker_child {
        let reports_root =
            resolve_local_project_root_from_config(&args.config, &conf).join("reports");
        if let Err(err) = std::fs::create_dir_all(&reports_root) {
            warn!(
                "failed to create kafka-import reports directory {}: {:#}",
                reports_root.display(),
                err
            );
        } else {
            let write_mode_stats_path = reports_root.join("kafka_import_write_mode.stats.yaml");
            match serde_yaml::to_string(&kafka_import_write_mode_stats) {
                Ok(yaml) => {
                    if let Err(err) = std::fs::write(&write_mode_stats_path, yaml) {
                        warn!(
                            "failed to write kafka-import write-mode stats to {}: {:#}",
                            write_mode_stats_path.display(),
                            err
                        );
                    } else {
                        info!(
                            "Kafka import write-mode stats written to {}",
                            write_mode_stats_path.display()
                        );
                    }
                }
                Err(err) => {
                    warn!(
                        "failed to serialize kafka-import write-mode stats: {:#}",
                        err
                    );
                }
            }
        }
    } else {
        info!("Skipping kafka-import write-mode stats file write in child worker process");
    }

    if !spawned_worker_children.is_empty() {
        for mut child in spawned_worker_children {
            let status = child
                .wait()
                .await
                .context("failed while waiting for kafka worker child process")?;
            if !status.success() {
                return Err(anyhow!("kafka worker child exited with status {status}"));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mapping(table_name: &str, foreign_keys: Vec<DdlForeignKeyMapping>) -> CollectionMapping {
        CollectionMapping {
            collection_name: table_name.to_owned(),
            mongo_dbname: "sample_mflix".to_owned(),
            mongo_path: Some(".".to_owned()),
            traversal: None,
            pg_mapping: PgMapping {
                dbname: "sample_mflix".to_owned(),
                schema_name: "sample_mflix".to_owned(),
                table_name: table_name.to_owned(),
                columns: Vec::new(),
                ddl: Some(DdlTableMapping {
                    name: table_name.to_owned(),
                    columns: Vec::new(),
                    foreign_keys,
                }),
                ddl_editing: default_ddl_editing_guidance(),
            },
        }
    }

    #[test]
    fn flattened_child_mapping_with_parent_fk_is_not_root() {
        let root = mapping("theaters", Vec::new());
        let child = mapping(
            "theaters_location",
            vec![DdlForeignKeyMapping {
                from_col: "theaters_id".to_owned(),
                to_table: "theaters".to_owned(),
                to_col: "id".to_owned(),
            }],
        );

        assert!(is_root_mapping(&root));
        assert!(!is_root_mapping(&child));
    }

    #[test]
    fn parent_must_be_inserted_before_child_fk_insert() {
        let parent = mapping("theaters", Vec::new());
        let child = mapping(
            "theaters_location",
            vec![DdlForeignKeyMapping {
                from_col: "theaters_id".to_owned(),
                to_table: "theaters".to_owned(),
                to_col: "id".to_owned(),
            }],
        );
        let fk = child
            .pg_mapping
            .ddl
            .as_ref()
            .and_then(|ddl| ddl.foreign_keys.first())
            .cloned()
            .expect("child fk should exist");

        let mut inserted = std::collections::HashSet::new();
        assert!(ensure_parent_inserted_before_child(&parent, &child, &fk, &inserted).is_err());

        inserted.insert("theaters".to_owned());
        assert!(ensure_parent_inserted_before_child(&parent, &child, &fk, &inserted).is_ok());
    }

    #[test]
    fn parse_topic_db_collection_accepts_prefix_with_trailing_dot() {
        let parsed = parse_topic_db_collection(
            "dev-prep.events_azer",
            Some("dev-prep."),
            Some("dev-prep"),
        );

        assert_eq!(
            parsed,
            Some(("dev-prep".to_owned(), "events_azer".to_owned()))
        );
    }

    #[test]
    fn parse_topic_db_collection_accepts_prefix_without_trailing_dot() {
        let parsed = parse_topic_db_collection(
            "dev-events.events_azer",
            Some("dev-events"),
            Some("dev-events"),
        );

        assert_eq!(
            parsed,
            Some(("dev-events".to_owned(), "events_azer".to_owned()))
        );
    }
}
