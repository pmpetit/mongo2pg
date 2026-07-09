//! Export MongoDB collection data to gzipped CSV files.
//!
//! For each collection, reads the generated SQL DDL from `schema/tables/<name>.sql`
//! to determine the table structure, then streams all documents from MongoDB and
//! writes one gzipped CSV file per SQL table into `data/<name>/`.
//!
//! Nested arrays and objects are expanded across child tables exactly as
//! `to_pg` generated them, so the CSV files can be loaded directly into
//! PostgreSQL with `\COPY`.

use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;

use anyhow::{anyhow, Context, Result};
use bson::Bson;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use flate2::write::GzEncoder;
use flate2::Compression;
use futures::TryStreamExt;
use google_cloud_auth::credentials::Builder as AuthBuilder;
use google_cloud_storage::client::{Storage, StorageControl};
use log::{debug, info, warn};
use mongodb::Client;
use serde::Deserialize;

use crate::schema_diagram::{parse_sql, Table as SqlTable};

use crate::util::{objectid_hex_to_uuid, sanitize};

pub const DEFAULT_EXPORT_CHUNK_ROWS: u64 = 50_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExportWriteBackend {
    LocalFs,
    Gcs { bucket: String, prefix: String },
}

pub fn resolve_export_write_backend(base_dir: &Path) -> Result<ExportWriteBackend> {
    let raw = base_dir.to_string_lossy();
    if let Some(remainder) = raw
        .strip_prefix("gs://")
        .or_else(|| raw.strip_prefix("gs:/"))
    {
        let trimmed = remainder.trim_matches('/');
        let (bucket, prefix) = trimmed
            .split_once('/')
            .map_or((trimmed, ""), |(bucket, prefix)| (bucket, prefix));
        if bucket.is_empty() {
            return Err(anyhow!(
                "Invalid GCS base_dir '{}': missing bucket name after gs://",
                raw
            ));
        }
        return Ok(ExportWriteBackend::Gcs {
            bucket: bucket.to_owned(),
            prefix: prefix.to_owned(),
        });
    }
    Ok(ExportWriteBackend::LocalFs)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CloudWriteErrorCategory {
    Authentication,
    Authorization,
    NotFound,
    Transient,
    Other,
}

fn categorize_cloud_error_message(message: &str) -> CloudWriteErrorCategory {
    let lower = message.to_ascii_lowercase();
    if lower.contains("unauthenticated")
        || lower.contains("invalid authentication")
        || lower.contains("no credentials")
        || lower.contains("credential")
        || lower.contains("token")
    {
        return CloudWriteErrorCategory::Authentication;
    }
    if lower.contains("permission denied")
        || lower.contains("forbidden")
        || lower.contains("access denied")
        || lower.contains("status: 403")
    {
        return CloudWriteErrorCategory::Authorization;
    }
    if lower.contains("not found") || lower.contains("status: 404") {
        return CloudWriteErrorCategory::NotFound;
    }
    if lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("connection reset")
        || lower.contains("temporar")
        || lower.contains("unavailable")
        || lower.contains("status: 429")
        || lower.contains("status: 500")
        || lower.contains("status: 502")
        || lower.contains("status: 503")
        || lower.contains("status: 504")
    {
        return CloudWriteErrorCategory::Transient;
    }
    CloudWriteErrorCategory::Other
}

fn format_categorized_cloud_error(
    action: &str,
    context: &str,
    err: &anyhow::Error,
) -> anyhow::Error {
    let msg = err.to_string();
    let category = categorize_cloud_error_message(&msg);
    match category {
        CloudWriteErrorCategory::Authentication => anyhow!(
            "cloud write error [authentication] during {} for {}: {}. Check ADC/service account credentials",
            action,
            context,
            msg
        ),
        CloudWriteErrorCategory::Authorization => anyhow!(
            "cloud write error [authorization] during {} for {}: {}. Verify storage.objects permissions",
            action,
            context,
            msg
        ),
        CloudWriteErrorCategory::NotFound => anyhow!(
            "cloud write error [not_found] during {} for {}: {}. Verify bucket and path",
            action,
            context,
            msg
        ),
        CloudWriteErrorCategory::Transient => anyhow!(
            "cloud write error [transient] during {} for {}: {}. Retry may succeed",
            action,
            context,
            msg
        ),
        CloudWriteErrorCategory::Other => anyhow!(
            "cloud write error [other] during {} for {}: {}",
            action,
            context,
            msg
        ),
    }
}

pub async fn ensure_gcs_authentication() -> Result<()> {
    const STORAGE_RW_SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_write";

    match std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
        Ok(path) => debug!(
            "[gcs-debug] auth preflight: GOOGLE_APPLICATION_CREDENTIALS is set (adc_source=key_file path='{}')",
            path
        ),
        Err(_) => debug!(
            "[gcs-debug] auth preflight: GOOGLE_APPLICATION_CREDENTIALS not set (adc_source=metadata_or_default)"
        ),
    }

    let credentials = AuthBuilder::default()
        .with_scopes([STORAGE_RW_SCOPE])
        .build_access_token_credentials()
        .map_err(|err| {
            format_categorized_cloud_error(
                "auth_preflight",
                "google application default credentials",
                &anyhow!(err),
            )
        })?;

    credentials.access_token().await.map_err(|err| {
        format_categorized_cloud_error(
            "auth_preflight",
            "google application default credentials",
            &anyhow!(err),
        )
    })?;

    Ok(())
}

async fn preflight_gcs_destination(bucket: &str) -> Result<()> {
    ensure_gcs_authentication().await?;
    let storage_control = StorageControl::builder().build().await.map_err(|err| {
        format_categorized_cloud_error(
            "preflight",
            &format!("bucket {bucket}"),
            &anyhow!(err),
        )
    })?;
    let bucket_resource = format!("projects/_/buckets/{bucket}");
    storage_control
        .get_bucket()
        .set_name(bucket_resource)
        .send()
        .await
        .map_err(|err| {
            format_categorized_cloud_error(
                "preflight",
                &format!("bucket {bucket}"),
                &anyhow!(err),
            )
        })?;
    Ok(())
}

fn gcs_preflight_enabled() -> bool {
    std::env::var("MONGO2PG_GCS_PREFLIGHT")
        .ok()
        .map(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

fn gcs_object_key(prefix: &str, db_name: &str, sql_lookup_name: &str, file_name: &str) -> String {
    let mut parts: Vec<&str> = Vec::new();
    let trimmed_prefix = prefix.trim_matches('/');
    if !trimmed_prefix.is_empty() {
        parts.push(trimmed_prefix);
    }
    parts.push("data");
    parts.push(db_name);
    parts.push(sql_lookup_name);
    parts.push(file_name);
    parts.join("/")
}

async fn upload_export_files_to_gcs(
    out_dir: &Path,
    db_name: &str,
    sql_lookup_name: &str,
    bucket: &str,
    prefix: &str,
) -> Result<()> {
    debug!(
        "[gcs-debug] export upload start: bucket='{}' prefix='{}' db='{}' sql='{}' local_dir='{}'",
        bucket,
        prefix.trim_matches('/'),
        db_name,
        sql_lookup_name,
        out_dir.display()
    );
    let storage = Storage::builder().build().await.map_err(|err| {
        format_categorized_cloud_error(
            "upload_init",
            &format!("bucket {bucket}"),
            &anyhow!(err),
        )
    })?;
    let bucket_resource = format!("projects/_/buckets/{bucket}");
    let mut uploaded_files = 0usize;

    for entry in std::fs::read_dir(out_dir)
        .with_context(|| format!("Cannot read {} for GCS upload", out_dir.display()))?
    {
        let path = entry?.path();
        if !path.is_file() {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !file_name.ends_with(".csv.gz") {
            continue;
        }

        let object_name = gcs_object_key(prefix, db_name, sql_lookup_name, file_name);
        let bytes = tokio::fs::read(&path)
            .await
            .with_context(|| format!("Cannot read staged export file {}", path.display()))?;

        debug!(
            "[gcs-debug] export upload object: {} -> gs://{}/{}",
            path.display(),
            bucket,
            object_name
        );
        storage
            .write_object(
                bucket_resource.clone(),
                object_name.clone(),
                bytes::Bytes::from(bytes),
            )
            .send_buffered()
            .await
            .map_err(|err| {
                format_categorized_cloud_error(
                    "upload",
                    &format!("gs://{bucket}/{object_name}"),
                    &anyhow!(err),
                )
            })?;
        uploaded_files += 1;
    }
    debug!(
        "[gcs-debug] export upload done: uploaded_files={} bucket='{}' prefix='{}'",
        uploaded_files,
        bucket,
        prefix.trim_matches('/'),
    );
    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────────
// BSON → CSV string
// ──────────────────────────────────────────────────────────────────────────────

fn bson_to_string(val: &Bson) -> Option<String> {
    match val {
        Bson::ObjectId(oid) => Some(oid.to_hex()),
        Bson::String(s) => Some(s.clone()),
        Bson::Int32(n) => Some(n.to_string()),
        Bson::Int64(n) => Some(n.to_string()),
        Bson::Double(d) => Some(d.to_string()),
        Bson::Boolean(b) => Some(b.to_string()),
        Bson::DateTime(dt) => Some(format_millis(dt.timestamp_millis())),
        Bson::Decimal128(d) => Some(d.to_string()),
        Bson::Timestamp(ts) => Some(ts.time.to_string()),
        Bson::Array(arr) => {
            let elements: Vec<String> = arr
                .iter()
                // Convert each BSON element in the array to a string
                .filter_map(bson_to_string)
                // Format each element for the PostgreSQL array
                .map(|elem_str| {
                    let needs_quoting = elem_str.is_empty()
                        || elem_str.contains(',')
                        || elem_str.contains('{')
                        || elem_str.contains('}')
                        || elem_str.contains('"')
                        || elem_str.contains('\\')
                        || elem_str.chars().any(char::is_whitespace);

                    if needs_quoting {
                        // If the string contains special characters, wrap it in double quotes
                        // and escape any internal quotes or backslashes.
                        format!(
                            "\"{}\"",
                            elem_str.replace('\\', "\\\\").replace('"', "\\\"")
                        )
                    } else {
                        elem_str
                    }
                })
                .collect();

            // Join all elements with a comma and wrap in curly braces
            Some(format!("{{{}}}", elements.join(",")))
        }
        // Bson::Array(arr) => {
        //     let json_string = serde_json::to_string(
        //         &arr.iter().map(bson_to_json_value).collect::<Vec<_>>()
        //     )
        //     .unwrap_or_default();
        //     Some(format!("ARRAY{}", json_string))
        // }
        // Bson::Array(arr) => Some(
        //     serde_json::to_string(&arr.iter().map(bson_to_json_value).collect::<Vec<_>>())
        //         .unwrap_or_default(),
        // ),
        Bson::Null | Bson::Undefined => None,

        // For complex / uncommon types fall back to BSON extended-JSON representation.
        other => Some(serde_json::to_string(other).unwrap_or_default()),
    }
}

fn bson_to_uuid_string(val: &Bson) -> Option<String> {
    match val {
        Bson::ObjectId(oid) => objectid_hex_to_uuid(&oid.to_hex()),
        Bson::String(raw) => objectid_hex_to_uuid(raw),
        _ => None,
    }
}

fn numeric_timestamp_to_millis(value: f64) -> Option<i64> {
    if !value.is_finite() {
        return None;
    }

    let abs = value.abs();
    let millis = if abs >= 1_000_000_000_000_000.0 {
        value / 1000.0
    } else if abs >= 1_000_000_000_000.0 {
        value
    } else {
        value * 1000.0
    };

    Some(millis.round() as i64)
}

fn format_datetime(dt: DateTime<Utc>) -> String {
    dt.format("%Y-%m-%d %H:%M:%S%.f%:z").to_string()
}

fn normalize_datetime_string(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Ok(number) = trimmed.parse::<f64>() {
        return numeric_timestamp_to_millis(number)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .map(format_datetime);
    }

    if let Ok(parsed) = DateTime::parse_from_rfc3339(trimmed) {
        return Some(format_datetime(parsed.with_timezone(&Utc)));
    }

    const DATETIME_FORMATS: [&str; 4] = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ];

    for format in DATETIME_FORMATS {
        if let Ok(parsed) = NaiveDateTime::parse_from_str(trimmed, format) {
            return Some(format_datetime(parsed.and_utc()));
        }
    }

    if let Ok(parsed) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        return parsed
            .and_hms_opt(0, 0, 0)
            .map(|dt| format_datetime(dt.and_utc()));
    }

    Some(trimmed.to_owned())
}

fn bson_to_timestamp_string(val: &Bson) -> Option<String> {
    match val {
        Bson::DateTime(dt) => {
            DateTime::<Utc>::from_timestamp_millis(dt.timestamp_millis()).map(format_datetime)
        }
        Bson::Timestamp(ts) => {
            DateTime::<Utc>::from_timestamp(ts.time as i64, 0).map(format_datetime)
        }
        Bson::String(text) => normalize_datetime_string(text),
        Bson::Int32(value) => numeric_timestamp_to_millis(*value as f64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .map(format_datetime),
        Bson::Int64(value) => numeric_timestamp_to_millis(*value as f64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .map(format_datetime),
        Bson::Double(value) => numeric_timestamp_to_millis(*value)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .map(format_datetime),
        Bson::Decimal128(value) => value
            .to_string()
            .parse::<f64>()
            .ok()
            .and_then(numeric_timestamp_to_millis)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .map(format_datetime),
        Bson::Array(arr) => {
            let elements: Vec<String> = arr
                .iter()
                .filter_map(bson_to_timestamp_string)
                .map(|elem_str| {
                    let needs_quoting = elem_str.is_empty()
                        || elem_str.contains(',')
                        || elem_str.contains('{')
                        || elem_str.contains('}')
                        || elem_str.contains('"')
                        || elem_str.contains('\\')
                        || elem_str.chars().any(char::is_whitespace);

                    if needs_quoting {
                        format!(
                            "\"{}\"",
                            elem_str.replace('\\', "\\\\").replace('"', "\\\"")
                        )
                    } else {
                        elem_str
                    }
                })
                .collect();

            Some(format!("{{{}}}", elements.join(",")))
        }
        Bson::Null | Bson::Undefined => None,
        other => bson_to_string(other),
    }
}

fn is_timestamp_col_type(col_type: &str) -> bool {
    col_type
        .trim()
        .to_ascii_uppercase()
        .starts_with("TIMESTAMP")
}

fn is_uuid_col_type(col_type: &str) -> bool {
    col_type.trim().to_ascii_uppercase().starts_with("UUID")
}

fn is_geometry_col_type(col_type: &str) -> bool {
    col_type.trim().to_ascii_lowercase().starts_with("geometry")
}

fn bson_number_to_f64(value: &Bson) -> Option<f64> {
    match value {
        Bson::Double(v) => Some(*v),
        Bson::Int32(v) => Some(*v as f64),
        Bson::Int64(v) => Some(*v as f64),
        Bson::Decimal128(v) => v.to_string().parse::<f64>().ok(),
        _ => None,
    }
}

fn geojson_point_coordinates_from_bson(value: &Bson) -> Option<(f64, f64)> {
    match value {
        Bson::String(raw) => {
            let parsed: serde_json::Value = serde_json::from_str(raw).ok()?;
            let obj = parsed.as_object()?;
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
        Bson::Document(doc) => {
            let point_type = doc.get_str("type").ok()?;
            if !point_type.eq_ignore_ascii_case("point") {
                return None;
            }
            let coords = doc.get_array("coordinates").ok()?;
            if coords.len() != 2 {
                return None;
            }
            let lon = bson_number_to_f64(&coords[0])?;
            let lat = bson_number_to_f64(&coords[1])?;
            Some((lon, lat))
        }
        _ => None,
    }
}

fn bson_to_geometry_ewkt(val: &Bson) -> Option<String> {
    match val {
        Bson::Null | Bson::Undefined => None,
        Bson::String(text) => {
            if let Some((lon, lat)) = geojson_point_coordinates_from_bson(val) {
                return Some(format!("SRID=4326;POINT({lon} {lat})"));
            }

            let trimmed = text.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_owned())
            }
        }
        Bson::Document(_) => geojson_point_coordinates_from_bson(val)
            .map(|(lon, lat)| format!("SRID=4326;POINT({lon} {lat})")),
        other => bson_to_string(other),
    }
}

fn serialize_column_value(
    jsonb_cols: &HashSet<String>,
    timestamp_cols: &HashSet<String>,
    uuid_cols: &HashSet<String>,
    geometry_cols: &HashSet<String>,
    col: &str,
    value: &Bson,
) -> Option<String> {
    if jsonb_cols.contains(col) {
        Some(serde_json::to_string(&bson_to_json_value(value)).unwrap_or_default())
    } else if timestamp_cols.contains(col) {
        bson_to_timestamp_string(value)
    } else if uuid_cols.contains(col) {
        bson_to_uuid_string(value)
    } else if geometry_cols.contains(col) {
        bson_to_geometry_ewkt(value)
    } else {
        bson_to_string(value)
    }
}

/// Format a Unix-millisecond timestamp as a PostgreSQL-ingestible UTC datetime.
fn format_millis(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .map(format_datetime)
        .unwrap_or_default()
}

// ──────────────────────────────────────────────────────────────────────────────
// CSV escaping
// ──────────────────────────────────────────────────────────────────────────────

fn csv_escape(s: &str) -> String {
    if s.is_empty() || s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_owned()
    }
}

fn csv_cell_text(cell: Option<&str>) -> String {
    match cell {
        Some(text) => csv_escape(text),
        None => String::new(),
    }
}

fn csv_header_for_table(sql_t: &SqlTable) -> Vec<String> {
    sql_t
        .columns
        .iter()
        .map(|c| {
            if c.name.starts_with('"') && c.name.ends_with('"') {
                format!("\"{}\"", unquote_sql_ident(&c.name).replace('"', "\"\""))
            } else {
                csv_escape(&unquote_sql_ident(&c.name))
            }
        })
        .collect()
}

#[cfg(test)]
fn flush_chunk_buffers(
    sql_tables: &[SqlTable],
    out_dir: &Path,
    all_rows: &mut HashMap<String, Vec<Vec<Option<String>>>>,
    header_written: &mut HashSet<String>,
) -> Result<()> {
    for sql_t in sql_tables {
        let Some(rows) = all_rows.get_mut(&sql_t.name) else {
            continue;
        };
        if rows.is_empty() {
            continue;
        }

        let csv_path = out_dir.join(format!("{}.csv.gz", sql_t.name));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&csv_path)
            .with_context(|| format!("Cannot open {} for append", csv_path.display()))?;
        let mut gz = GzEncoder::new(file, Compression::default());

        if !header_written.contains(&sql_t.name) {
            let header = csv_header_for_table(sql_t);
            writeln!(gz, "{}", header.join(","))
                .with_context(|| format!("Write error for {}", csv_path.display()))?;
            header_written.insert(sql_t.name.clone());
        }

        for row in rows.drain(..) {
            let line: Vec<String> = row.iter().map(|v| csv_cell_text(v.as_deref())).collect();
            writeln!(gz, "{}", line.join(","))
                .with_context(|| format!("Write error for {}", csv_path.display()))?;
        }

        gz.finish()
            .with_context(|| format!("GZ flush error for {}", csv_path.display()))?;
    }

    all_rows.retain(|_, rows| !rows.is_empty());
    Ok(())
}

fn unquote_sql_ident(ident: &str) -> String {
    if ident.len() >= 2 && ident.starts_with('"') && ident.ends_with('"') {
        ident[1..ident.len() - 1].replace("\"\"", "\"")
    } else {
        ident.to_owned()
    }
}

/// Convert a BSON value to a clean `serde_json::Value`, mapping BSON-specific
/// types (ObjectId, DateTime, …) to their natural JSON equivalents.
/// Used to serialise JSONB columns so PostgreSQL COPY can ingest them.
fn bson_to_json_value(val: &Bson) -> serde_json::Value {
    match val {
        Bson::Double(d) => serde_json::Number::from_f64(*d)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Bson::String(s) => serde_json::Value::String(s.clone()),
        Bson::Array(arr) => serde_json::Value::Array(arr.iter().map(bson_to_json_value).collect()),
        Bson::Document(doc) => {
            let map = doc
                .iter()
                .map(|(k, v)| (k.clone(), bson_to_json_value(v)))
                .collect();
            serde_json::Value::Object(map)
        }
        Bson::Boolean(b) => serde_json::Value::Bool(*b),
        Bson::Null | Bson::Undefined => serde_json::Value::Null,
        Bson::Int32(n) => serde_json::Value::Number((*n).into()),
        Bson::Int64(n) => serde_json::Value::Number((*n).into()),
        Bson::ObjectId(oid) => serde_json::Value::String(oid.to_hex()),
        Bson::DateTime(dt) => serde_json::Value::String(format_millis(dt.timestamp_millis())),
        Bson::Decimal128(d) => serde_json::Value::String(d.to_string()),
        Bson::Timestamp(ts) => serde_json::Value::Number(ts.time.into()),
        other => serde_json::to_value(other).unwrap_or(serde_json::Value::Null),
    }
}

struct TableNode {
    /// SQL table name
    sql_name: String,
    /// Column names in declaration order (from SQL)
    columns: Vec<String>,
    /// Primary-key column names in declaration order.
    pk_cols: Vec<String>,
    /// FK column pointing to the parent table (`None` for root tables)
    fk_col: Option<String>,
    /// MongoDB field name at the current document level that yields this table's data.
    /// Empty for the root table.
    mongo_field: String,
    /// Root-level sibling MongoDB fields that are exported through this same child table.
    grouped_root_fields: Option<Vec<String>>,
    /// `true` when this child table represents a scalar array
    /// (has exactly 3 columns: pk, fk, `value`).
    is_scalar_array: bool,
    /// Columns whose SQL type is JSONB – serialised as clean JSON rather than
    /// plain text so PostgreSQL COPY can ingest them.
    jsonb_cols: HashSet<String>,
    /// Columns whose SQL type is timestamp-like and should be exported in a
    /// PostgreSQL-ingestible timestamp text format.
    timestamp_cols: HashSet<String>,
    /// Columns whose SQL type is UUID and should receive ObjectId->UUID conversion.
    uuid_cols: HashSet<String>,
    /// Columns whose SQL type is geometry and should be exported as EWKT text.
    geometry_cols: HashSet<String>,
    /// For a promoted root array-of-objects table, the MongoDB array field to iterate.
    root_array_field: Option<String>,
    /// For a promoted root array-of-objects table, the root `_id` column stored on each row.
    root_parent_id_col: Option<String>,
    children: Vec<TableNode>,
}

fn build_tree(
    sql_tables: &[SqlTable],
    flattened_root: Option<(String, String)>,
    grouped_root_sources: &HashMap<String, Vec<String>>,
) -> Vec<TableNode> {
    build_tree_with_grouped_root(sql_tables, flattened_root, None, grouped_root_sources)
}

fn build_tree_with_grouped_root(
    sql_tables: &[SqlTable],
    flattened_root: Option<(String, String)>,
    flattened_grouped_root: Option<(Vec<String>, String)>,
    grouped_root_sources: &HashMap<String, Vec<String>>,
) -> Vec<TableNode> {
    let names: std::collections::HashSet<&str> =
        sql_tables.iter().map(|t| t.name.as_str()).collect();

    let mut parent_of: HashMap<&str, &str> = HashMap::new();
    for t in sql_tables {
        for fk in &t.foreign_keys {
            if names.contains(fk.to_table.as_str()) {
                parent_of.insert(&t.name, &fk.to_table);
                break;
            }
        }
    }

    let mut children_of: HashMap<&str, Vec<&SqlTable>> = HashMap::new();
    for t in sql_tables {
        if let Some(&par) = parent_of.get(t.name.as_str()) {
            children_of.entry(par).or_default().push(t);
        }
    }

    fn build_node_legacy(
        sql_t: &SqlTable,
        parent_name: Option<&str>,
        children_of: &HashMap<&str, Vec<&SqlTable>>,
        flattened_root: Option<(String, String)>,
        flattened_grouped_root: Option<(Vec<String>, String)>,
        grouped_root_sources: &HashMap<String, Vec<String>>,
    ) -> TableNode {
        let mongo_field = match parent_name {
            Some(p) => sql_t
                .name
                .strip_prefix(&format!("{p}_"))
                .unwrap_or(&sql_t.name)
                .to_owned(),
            None => String::new(),
        };

        let pk_cols: Vec<String> = sql_t
            .columns
            .iter()
            .filter(|c| c.primary_key)
            .map(|c| unquote_sql_ident(&c.name))
            .collect();

        let fk_col = sql_t
            .foreign_keys
            .first()
            .map(|fk| unquote_sql_ident(&fk.from_col));

        let columns: Vec<String> = sql_t
            .columns
            .iter()
            .map(|c| unquote_sql_ident(&c.name))
            .collect();

        let is_scalar_array =
            fk_col.is_some() && columns.len() == 3 && columns.iter().any(|c| c == "value");

        let jsonb_cols: HashSet<String> = sql_t
            .columns
            .iter()
            .filter(|c| c.col_type.eq_ignore_ascii_case("JSONB"))
            .map(|c| unquote_sql_ident(&c.name))
            .collect();
        let timestamp_cols: HashSet<String> = sql_t
            .columns
            .iter()
            .filter(|c| is_timestamp_col_type(&c.col_type))
            .map(|c| unquote_sql_ident(&c.name))
            .collect();
        let uuid_cols: HashSet<String> = sql_t
            .columns
            .iter()
            .filter(|c| is_uuid_col_type(&c.col_type))
            .map(|c| unquote_sql_ident(&c.name))
            .collect();
        let geometry_cols: HashSet<String> = sql_t
            .columns
            .iter()
            .filter(|c| is_geometry_col_type(&c.col_type))
            .map(|c| unquote_sql_ident(&c.name))
            .collect();

        let children: Vec<TableNode> = children_of
            .get(sql_t.name.as_str())
            .map(|cs| {
                cs.iter()
                    .map(|child_sql| {
                        build_node_legacy(
                            child_sql,
                            Some(&sql_t.name),
                            children_of,
                            None,
                            None,
                            grouped_root_sources,
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();

        let (root_array_field, root_parent_id_col, root_grouped_fields) = if parent_name.is_none() {
            if let Some((field_name, parent_id_col)) = flattened_root {
                (Some(field_name), Some(parent_id_col), None)
            } else if let Some((grouped_fields, parent_id_col)) = flattened_grouped_root {
                (None, Some(parent_id_col), Some(grouped_fields))
            } else {
                (None, None, None)
            }
        } else {
            (None, None, None)
        };
        let grouped_root_fields = if parent_name.is_some() {
            grouped_root_sources.get(&sql_t.name).cloned()
        } else {
            root_grouped_fields
        };

        TableNode {
            sql_name: sql_t.name.clone(),
            columns,
            pk_cols,
            fk_col,
            mongo_field,
            grouped_root_fields,
            is_scalar_array,
            jsonb_cols,
            timestamp_cols,
            uuid_cols,
            geometry_cols,
            root_array_field,
            root_parent_id_col,
            children,
        }
    }

    sql_tables
        .iter()
        .filter(|t| !parent_of.contains_key(t.name.as_str()))
        .map(|r| {
            build_node_legacy(
                r,
                None,
                &children_of,
                flattened_root.clone(),
                flattened_grouped_root.clone(),
                grouped_root_sources,
            )
        })
        .collect()
}

#[cfg(test)]
fn grouped_root_table_sources(
    schema: &crate::analyzer::CollectionSchema,
    coll_name: &str,
) -> HashMap<String, Vec<String>> {
    crate::util::grouped_root_array_object_fields(&schema.object)
        .into_iter()
        .map(|group| {
            (
                format!(
                    "{}_{}",
                    sanitize(coll_name),
                    sanitize(&group.representative)
                ),
                group.members,
            )
        })
        .collect()
}

#[cfg(test)]
fn flattened_grouped_root_for_export(
    sql_tables: &[SqlTable],
    schema: &crate::analyzer::CollectionSchema,
    coll_name: &str,
) -> Option<(Vec<String>, String)> {
    let group = crate::util::flatten_grouped_root_array_object_fields(schema)?;
    let parent_id_col = crate::util::flattened_root_parent_id_column(coll_name);
    let root_table = sql_tables
        .iter()
        .find(|table| table.foreign_keys.is_empty())?;

    if root_table
        .columns
        .iter()
        .any(|column| column.name == parent_id_col)
        && root_table.columns.iter().any(|column| column.name == "key")
    {
        Some((group.members, parent_id_col))
    } else {
        None
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// BSON field lookup
// ──────────────────────────────────────────────────────────────────────────────

/// Find `sql_col` in a BSON document.
///
/// Tries an exact-name match first, then falls back to comparing sanitized names
/// so that e.g. `"invoiceId"` in MongoDB matches the `"invoiceid"` SQL column.
fn find_mongo_field<'a>(doc: &'a bson::Document, sql_col: &str) -> Option<&'a Bson> {
    fn collect_nested_matches<'a>(
        doc: &'a bson::Document,
        sql_col: &str,
        matches: &mut Vec<&'a Bson>,
    ) {
        for (key, val) in doc.iter() {
            if let Bson::Document(child_doc) = val {
                if sanitize(key) == sql_col {
                    matches.push(val);
                }
                if let Some(value) = find_mongo_field(child_doc, sql_col) {
                    matches.push(value);
                }
            }
            if let Bson::Array(arr) = val {
                // Search array items that are documents for matches as well.
                for item in arr {
                    if let Bson::Document(item_doc) = item {
                        if sanitize(key) == sql_col {
                            // array value itself is candidate
                            matches.push(val);
                        }
                        if let Some(value) = find_mongo_field(item_doc, sql_col) {
                            matches.push(value);
                        }
                    }
                }
            }
        }
    }

    if let Some(v) = doc.get(sql_col) {
        return Some(v);
    }
    for (key, val) in doc.iter() {
        if sanitize(key) == sql_col {
            return Some(val);
        }
        if let Bson::Document(child_doc) = val {
            let key_prefix = format!("{}_", sanitize(key));
            if let Some(remainder) = sql_col.strip_prefix(&key_prefix) {
                if let Some(value) = find_mongo_field(child_doc, remainder) {
                    return Some(value);
                }
            }
        }
    }

    let mut matches = Vec::new();
    collect_nested_matches(doc, sql_col, &mut matches);
    if matches.len() == 1 {
        return matches.into_iter().next();
    }

    None
}

#[derive(Debug, Clone, Deserialize)]
struct ExportMappingYaml {
    #[serde(default)]
    traversal: Option<ExportTraversalPlan>,
    #[serde(default)]
    mongo_path: Option<String>,
    pg_mapping: ExportPgMappingYaml,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ExportTraversalMode {
    Root,
    Object,
    ArrayObject,
    ArrayScalar,
    MapObject,
}

#[derive(Debug, Clone, Deserialize)]
struct ExportTraversalPlan {
    mode: ExportTraversalMode,
    #[serde(default)]
    parent_table: Option<String>,
    #[serde(default)]
    source_field: Option<String>,
    #[serde(default)]
    fk_column: Option<String>,
    // #[serde(default)]
    // key_column: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ExportPgMappingYaml {
    table_name: String,
    #[serde(default)]
    columns: Vec<ExportMappingColumnYaml>,
}

#[derive(Debug, Clone, Deserialize)]
struct ExportMappingColumnYaml {
    source_field: String,
    target_field: String,
    #[serde(default)]
    literal_value: Option<String>,
}

#[derive(Debug, Clone)]
struct ExportTablePlan {
    traversal: ExportTraversalPlan,
    mongo_path: Option<String>,
    target_to_source: HashMap<String, String>,
    target_to_literal: HashMap<String, String>,
}

fn load_export_table_plans(
    collections_dir: &Path,
    safe_name: &str,
) -> HashMap<String, ExportTablePlan> {
    let mappings_dir = collections_dir.join(safe_name);
    let mut plans = HashMap::new();

    let Ok(entries) = std::fs::read_dir(&mappings_dir) else {
        return plans;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !file_name.starts_with("mapping_") || !file_name.ends_with(".yaml") {
            continue;
        }

        let Ok(content) = std::fs::read_to_string(&path) else {
            continue;
        };
        let Ok(mapping) = serde_yaml::from_str::<ExportMappingYaml>(&content) else {
            continue;
        };
        let Some(traversal) = mapping.traversal else {
            continue;
        };

        let target_to_literal = mapping
            .pg_mapping
            .columns
            .iter()
            .filter_map(|column| {
                column
                    .literal_value
                    .as_ref()
                    .map(|lit| (sanitize(&column.target_field), lit.clone()))
            })
            .collect::<HashMap<_, _>>();
        let target_to_source = mapping
            .pg_mapping
            .columns
            .into_iter()
            .filter(|column| {
                !column.target_field.trim().is_empty() && !column.source_field.trim().is_empty()
            })
            .map(|column| (sanitize(&column.target_field), column.source_field))
            .collect::<HashMap<_, _>>();

        plans.insert(
            sanitize(&mapping.pg_mapping.table_name),
            ExportTablePlan {
                traversal,
                mongo_path: mapping.mongo_path,
                target_to_source,
                target_to_literal,
            },
        );
    }

    plans
}

pub fn resolve_grouped_sql_lookup_name(collections_dir: &Path, coll_name: &str) -> Option<String> {
    let safe_name = coll_name.replace('/', "_");
    let mappings_dir = collections_dir.join(&safe_name);
    let entries = std::fs::read_dir(&mappings_dir).ok()?;

    for entry in entries.flatten() {
        let path = entry.path();
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !file_name.starts_with("mapping_") || !file_name.ends_with(".yaml") {
            continue;
        }
        let Ok(content) = std::fs::read_to_string(&path) else {
            continue;
        };
        let Ok(mapping) = serde_yaml::from_str::<ExportMappingYaml>(&content) else {
            continue;
        };

        let is_root = mapping.mongo_path.as_deref() == Some(".")
            || mapping
                .traversal
                .as_ref()
                .is_some_and(|plan| matches!(plan.mode, ExportTraversalMode::Root));
        if is_root {
            return Some(sanitize(&mapping.pg_mapping.table_name));
        }
    }

    None
}

fn build_node_from_plan(
    sql_t: &SqlTable,
    depth: usize,
    parent_key: Option<&str>,
    children_of: &HashMap<String, Vec<String>>,
    sql_by_key: &HashMap<String, &SqlTable>,
    plans: &HashMap<String, ExportTablePlan>,
) -> Option<TableNode> {
    let table_key = sanitize(&sql_t.name);
    let plan = plans.get(&table_key)?;

    let pk_cols: Vec<String> = sql_t
        .columns
        .iter()
        .filter(|c| c.primary_key)
        .map(|c| unquote_sql_ident(&c.name))
        .collect();

    let fk_col = sql_t
        .foreign_keys
        .first()
        .map(|fk| unquote_sql_ident(&fk.from_col));

    let columns: Vec<String> = sql_t
        .columns
        .iter()
        .map(|c| unquote_sql_ident(&c.name))
        .collect();

    let is_scalar_array = matches!(plan.traversal.mode, ExportTraversalMode::ArrayScalar);

    let jsonb_cols: HashSet<String> = sql_t
        .columns
        .iter()
        .filter(|c| c.col_type.eq_ignore_ascii_case("JSONB"))
        .map(|c| unquote_sql_ident(&c.name))
        .collect();
    let timestamp_cols: HashSet<String> = sql_t
        .columns
        .iter()
        .filter(|c| is_timestamp_col_type(&c.col_type))
        .map(|c| unquote_sql_ident(&c.name))
        .collect();
    let uuid_cols: HashSet<String> = sql_t
        .columns
        .iter()
        .filter(|c| is_uuid_col_type(&c.col_type))
        .map(|c| unquote_sql_ident(&c.name))
        .collect();
    let geometry_cols: HashSet<String> = sql_t
        .columns
        .iter()
        .filter(|c| is_geometry_col_type(&c.col_type))
        .map(|c| unquote_sql_ident(&c.name))
        .collect();

    let children = children_of
        .get(&table_key)
        .map(|keys| {
            keys.iter()
                .filter_map(|key| {
                    sql_by_key.get(key).and_then(|child_sql| {
                        build_node_from_plan(
                            child_sql,
                            depth + 1,
                            Some(&table_key),
                            children_of,
                            sql_by_key,
                            plans,
                        )
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let mongo_field = if parent_key.is_none() {
        String::new()
    } else {
        let fallback = plan.traversal.source_field.clone().unwrap_or_else(|| {
            parent_key
                .and_then(|parent| {
                    sql_t
                        .name
                        .strip_prefix(&format!("{parent}_"))
                        .map(str::to_owned)
                })
                .unwrap_or_else(|| sql_t.name.clone())
        });

        if depth == 1 {
            plan.mongo_path
                .as_deref()
                .map(str::trim)
                .map(|path| path.trim_start_matches('.'))
                .filter(|path| !path.is_empty())
                .map(str::to_owned)
                .unwrap_or(fallback)
        } else {
            fallback
        }
    };

    let (root_array_field, root_parent_id_col) = if parent_key.is_none()
        && matches!(plan.traversal.mode, ExportTraversalMode::ArrayObject)
    {
        (
            plan.traversal.source_field.clone(),
            plan.traversal.fk_column.clone().map(|name| sanitize(&name)),
        )
    } else {
        (None, None)
    };

    Some(TableNode {
        sql_name: sql_t.name.clone(),
        columns,
        pk_cols,
        fk_col,
        mongo_field,
        grouped_root_fields: None,
        is_scalar_array,
        jsonb_cols,
        timestamp_cols,
        uuid_cols,
        geometry_cols,
        root_array_field,
        root_parent_id_col,
        children,
    })
}

fn build_tree_from_mapping_plan(
    sql_tables: &[SqlTable],
    plans: &HashMap<String, ExportTablePlan>,
) -> Option<(Vec<TableNode>, HashMap<String, String>)> {
    if sql_tables.is_empty() || plans.is_empty() {
        return None;
    }

    if !sql_tables
        .iter()
        .all(|table| plans.contains_key(&sanitize(&table.name)))
    {
        return None;
    }

    let mut children_of: HashMap<String, Vec<String>> = HashMap::new();
    let mut roots = Vec::new();

    for table in sql_tables {
        let table_key = sanitize(&table.name);
        let Some(plan) = plans.get(&table_key) else {
            return None;
        };
        let parent = plan
            .traversal
            .parent_table
            .as_ref()
            .map(|name| sanitize(name));

        if let Some(parent_key) = parent {
            children_of
                .entry(parent_key)
                .or_default()
                .push(table_key.clone());
        } else {
            roots.push(table_key);
        }
    }

    let sql_by_key = sql_tables
        .iter()
        .map(|table| (sanitize(&table.name), table))
        .collect::<HashMap<_, _>>();

    let root_nodes = roots
        .iter()
        .filter_map(|root_key| {
            sql_by_key.get(root_key).and_then(|sql_t| {
                build_node_from_plan(sql_t, 0, None, &children_of, &sql_by_key, plans)
            })
        })
        .collect::<Vec<_>>();

    if root_nodes.is_empty() {
        return None;
    }

    let root_target_to_source = roots
        .first()
        .and_then(|root_key| plans.get(root_key))
        .map(|plan| plan.target_to_source.clone())
        .unwrap_or_default();

    Some((root_nodes, root_target_to_source))
}

fn find_field_by_segment<'a>(doc: &'a bson::Document, segment: &str) -> Option<&'a Bson> {
    if let Some(value) = doc.get(segment) {
        return Some(value);
    }

    let wanted = sanitize(segment);
    doc.iter().find_map(|(key, value)| {
        if sanitize(key) == wanted {
            Some(value)
        } else {
            None
        }
    })
}

fn find_mongo_field_by_source_path<'a>(
    doc: &'a bson::Document,
    source_field: &str,
) -> Option<&'a Bson> {
    if source_field.trim().is_empty() {
        return None;
    }

    let mut segments = source_field.split('.');
    let first = segments.next()?;
    let mut current = find_field_by_segment(doc, first)?;

    for segment in segments {
        let Bson::Document(child_doc) = current else {
            return None;
        };
        current = find_field_by_segment(child_doc, segment)?;
    }

    Some(current)
}

fn collect_mongo_values_by_path(current: &Bson, segments: &[&str], out: &mut Vec<Bson>) {
    if segments.is_empty() {
        out.push(current.clone());
        return;
    }

    match current {
        Bson::Document(doc) => {
            if let Some(next) = find_field_by_segment(doc, segments[0]) {
                collect_mongo_values_by_path(next, &segments[1..], out);
            }
        }
        Bson::Array(items) => {
            for item in items {
                collect_mongo_values_by_path(item, segments, out);
            }
        }
        _ => {}
    }
}

fn find_mongo_field_for_traversal(doc: &bson::Document, source_field: &str) -> Option<Bson> {
    let trimmed = source_field.trim();
    if trimmed.is_empty() {
        return None;
    }

    let segments = trimmed
        .split('.')
        .filter(|segment| !segment.trim().is_empty())
        .collect::<Vec<_>>();

    if segments.is_empty() {
        return None;
    }

    let mut values = Vec::new();
    if let Some(first) = find_field_by_segment(doc, segments[0]) {
        collect_mongo_values_by_path(first, &segments[1..], &mut values);
    }

    if values.is_empty() {
        return find_mongo_field(doc, trimmed).cloned();
    }

    if values.len() == 1 {
        values.into_iter().next()
    } else {
        Some(Bson::Array(values))
    }
}

fn find_root_mongo_field_mapped<'a>(
    doc: &'a bson::Document,
    target_col: &str,
    root_target_to_source: &HashMap<String, String>,
) -> Option<&'a Bson> {
    let source_field = root_target_to_source
        .get(target_col)
        .map(String::as_str)
        .unwrap_or(target_col);

    find_mongo_field_by_source_path(doc, source_field).or_else(|| {
        doc.get_document("_id")
            .ok()
            .and_then(|id_doc| find_mongo_field_by_source_path(id_doc, source_field))
    })
}

fn extract_child_document_rows(
    child_doc: &bson::Document,
    child: &TableNode,
    parent_id: &str,
    all_rows: &mut HashMap<String, Vec<Vec<Option<String>>>>,
    counters: &mut HashMap<String, u64>,
) {
    let mut non_system_columns = child
        .columns
        .iter()
        .filter(|col| {
            if child.pk_cols.iter().any(|pk| pk == *col) {
                return false;
            }
            if Some(*col) == child.fk_col.as_ref() {
                return false;
            }
            *col != "key"
        })
        .peekable();

    let has_map_key_column = child.columns.iter().any(|col| col == "key");

    let looks_like_map_document = child.fk_col.is_some()
        && has_map_key_column
        && non_system_columns.peek().is_some()
        && !child_doc.is_empty()
        && child_doc
            .values()
            .all(|value| matches!(value, Bson::Document(_)));

    if !looks_like_map_document {
        extract_rows(
            &Bson::Document(child_doc.clone()),
            child,
            Some(parent_id),
            false,
            all_rows,
            counters,
        );
        return;
    }

    for (map_key, map_value) in child_doc {
        let Bson::Document(item_doc) = map_value else {
            continue;
        };

        let child_id = {
            let c = counters.entry(child.sql_name.clone()).or_insert(0);
            *c += 1;
            c.to_string()
        };

        let child_row: Vec<Option<String>> = child
            .columns
            .iter()
            .map(|col| {
                if child.pk_cols.iter().any(|pk| pk == col) {
                    Some(child_id.clone())
                } else if Some(col) == child.fk_col.as_ref() {
                    Some(parent_id.to_owned())
                } else if col == "key" {
                    Some(map_key.clone())
                } else {
                    find_mongo_field(item_doc, col).and_then(|v| {
                        serialize_column_value(
                            &child.jsonb_cols,
                            &child.timestamp_cols,
                            &child.uuid_cols,
                            &child.geometry_cols,
                            col,
                            v,
                        )
                    })
                }
            })
            .collect();

        if should_skip_empty_row(child, &child_row, false) {
            continue;
        }

        all_rows
            .entry(child.sql_name.clone())
            .or_default()
            .push(child_row);

        for grandchild in &child.children {
            match find_mongo_field_for_traversal(item_doc, &grandchild.mongo_field).as_ref() {
                Some(Bson::Array(arr)) => {
                    if grandchild.is_scalar_array {
                        for item in arr {
                            let grandchild_id = {
                                let c = counters.entry(grandchild.sql_name.clone()).or_insert(0);
                                *c += 1;
                                c.to_string()
                            };
                            let grandchild_row: Vec<Option<String>> = grandchild
                                .columns
                                .iter()
                                .map(|col| {
                                    if grandchild.pk_cols.iter().any(|pk| pk == col) {
                                        Some(grandchild_id.clone())
                                    } else if Some(col) == grandchild.fk_col.as_ref() {
                                        Some(child_id.clone())
                                    } else if col == "value" {
                                        serialize_column_value(
                                            &grandchild.jsonb_cols,
                                            &grandchild.timestamp_cols,
                                            &grandchild.uuid_cols,
                                            &grandchild.geometry_cols,
                                            col,
                                            item,
                                        )
                                    } else {
                                        None
                                    }
                                })
                                .collect();
                            all_rows
                                .entry(grandchild.sql_name.clone())
                                .or_default()
                                .push(grandchild_row);
                        }
                    } else {
                        for item in arr {
                            extract_rows(
                                item,
                                grandchild,
                                Some(&child_id),
                                false,
                                all_rows,
                                counters,
                            );
                        }
                    }
                }
                Some(Bson::Document(doc_val)) => {
                    extract_rows(
                        &Bson::Document(doc_val.clone()),
                        grandchild,
                        Some(&child_id),
                        false,
                        all_rows,
                        counters,
                    );
                }
                _ => {}
            }
        }
    }
}

fn map_document_entries<'a>(
    node: &TableNode,
    doc: &'a bson::Document,
) -> Vec<(&'a str, &'a bson::Document)> {
    let non_structural_cols: Vec<&String> = node
        .columns
        .iter()
        .filter(|col| {
            !node.pk_cols.iter().any(|pk| pk == *col)
                && node.fk_col.as_ref().is_none_or(|fk| fk != *col)
                && col.as_str() != "key"
        })
        .collect();

    // Container-only tables (PK/FK only) should not be treated as map objects.
    // They exist to anchor nested children and must keep the full current document context.
    if non_structural_cols.is_empty() {
        return Vec::new();
    }

    // Regular embedded object tables already expose value fields at current level.
    // Dynamic map objects do not, and keep one nested document per map key.
    if non_structural_cols
        .iter()
        .any(|col| find_mongo_field(doc, col.as_str()).is_some())
    {
        return Vec::new();
    }

    doc.iter()
        .filter_map(|(entry_key, entry_val)| match entry_val {
            Bson::Document(entry_doc) => Some((entry_key.as_str(), entry_doc)),
            _ => None,
        })
        .collect()
}

fn row_has_payload(node: &TableNode, row: &[Option<String>]) -> bool {
    node.columns.iter().enumerate().any(|(index, col)| {
        if node.pk_cols.iter().any(|pk| pk == col)
            || node.fk_col.as_ref().is_some_and(|fk| fk == col)
            || col == "key"
        {
            return false;
        }

        row.get(index).and_then(|value| value.as_ref()).is_some()
    })
}

fn should_skip_empty_row(node: &TableNode, row: &[Option<String>], is_root: bool) -> bool {
    !is_root && node.children.is_empty() && !row_has_payload(node, row)
}

// ──────────────────────────────────────────────────────────────────────────────
// Row extraction  (recursive, depth-first)
// ──────────────────────────────────────────────────────────────────────────────

fn extract_rows(
    val: &Bson,
    node: &TableNode,
    parent_id: Option<&str>,
    is_root: bool,
    all_rows: &mut HashMap<String, Vec<Vec<Option<String>>>>,
    counters: &mut HashMap<String, u64>,
) {
    let empty = HashMap::new();
    extract_rows_with_mapping(
        val, node, parent_id, is_root, all_rows, counters, &empty, &empty,
    );
}

fn extract_rows_with_mapping(
    val: &Bson,
    node: &TableNode,
    parent_id: Option<&str>,
    is_root: bool,
    all_rows: &mut HashMap<String, Vec<Vec<Option<String>>>>,
    counters: &mut HashMap<String, u64>,
    root_target_to_source: &HashMap<String, String>,
    root_target_to_literal: &HashMap<String, String>,
) {
    let doc = match val {
        Bson::Document(d) => d,
        _ => return,
    };
    if is_root {
        if let (Some(root_parent_id_col), Some(grouped_fields)) =
            (&node.root_parent_id_col, &node.grouped_root_fields)
        {
            let parent_source_id = doc
                .get("_id")
                .and_then(bson_to_uuid_string)
                .or_else(|| doc.get("_id").and_then(bson_to_string))
                .unwrap_or_default();
            for grouped_field in grouped_fields {
                if let Some(Bson::Array(items)) = find_mongo_field(doc, grouped_field) {
                    for item in items {
                        let item_doc = match item {
                            Bson::Document(item_doc) => item_doc,
                            _ => continue,
                        };
                        let c = counters.entry(node.sql_name.clone()).or_insert(0);
                        *c += 1;
                        let my_id = c.to_string();

                        let row: Vec<Option<String>> = node
                            .columns
                            .iter()
                            .map(|col| {
                                if node.pk_cols.iter().any(|pk| pk == col) {
                                    Some(my_id.clone())
                                } else if col == root_parent_id_col {
                                    Some(parent_source_id.clone())
                                } else if col == "key" {
                                    Some(grouped_field.clone())
                                } else {
                                    find_mongo_field(item_doc, col).and_then(|v| {
                                        serialize_column_value(
                                            &node.jsonb_cols,
                                            &node.timestamp_cols,
                                            &node.uuid_cols,
                                            &node.geometry_cols,
                                            col,
                                            v,
                                        )
                                    })
                                }
                            })
                            .collect();

                        all_rows.entry(node.sql_name.clone()).or_default().push(row);

                        for child in &node.children {
                            match find_mongo_field_for_traversal(item_doc, &child.mongo_field)
                                .as_ref()
                            {
                                Some(Bson::Array(arr)) => {
                                    if child.is_scalar_array {
                                        for child_item in arr {
                                            let child_id = {
                                                let c = counters
                                                    .entry(child.sql_name.clone())
                                                    .or_insert(0);
                                                *c += 1;
                                                c.to_string()
                                            };
                                            let child_row: Vec<Option<String>> = child
                                                .columns
                                                .iter()
                                                .map(|col| {
                                                    if child.pk_cols.iter().any(|pk| pk == col) {
                                                        Some(child_id.clone())
                                                    } else if Some(col) == child.fk_col.as_ref() {
                                                        Some(my_id.clone())
                                                    } else if col == "value" {
                                                        serialize_column_value(
                                                            &child.jsonb_cols,
                                                            &child.timestamp_cols,
                                                            &child.uuid_cols,
                                                            &child.geometry_cols,
                                                            col,
                                                            child_item,
                                                        )
                                                    } else {
                                                        None
                                                    }
                                                })
                                                .collect();
                                            all_rows
                                                .entry(child.sql_name.clone())
                                                .or_default()
                                                .push(child_row);
                                        }
                                    } else {
                                        for child_item in arr {
                                            extract_rows(
                                                child_item,
                                                child,
                                                Some(&my_id),
                                                false,
                                                all_rows,
                                                counters,
                                            );
                                        }
                                    }
                                }
                                Some(doc_val @ Bson::Document(_)) => {
                                    extract_rows(
                                        doc_val,
                                        child,
                                        Some(&my_id),
                                        false,
                                        all_rows,
                                        counters,
                                    );
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            return;
        }
        if let (Some(root_array_field), Some(root_parent_id_col)) =
            (&node.root_array_field, &node.root_parent_id_col)
        {
            let parent_source_id = doc
                .get("_id")
                .and_then(bson_to_uuid_string)
                .or_else(|| doc.get("_id").and_then(bson_to_string))
                .unwrap_or_default();
            if let Some(Bson::Array(items)) = find_mongo_field(doc, root_array_field) {
                for item in items {
                    let item_doc = match item {
                        Bson::Document(item_doc) => item_doc,
                        _ => continue,
                    };
                    let c = counters.entry(node.sql_name.clone()).or_insert(0);
                    *c += 1;
                    let my_id = c.to_string();

                    let row: Vec<Option<String>> = node
                        .columns
                        .iter()
                        .map(|col| {
                            if node.pk_cols.iter().any(|pk| pk == col) {
                                Some(my_id.clone())
                            } else if col == root_parent_id_col {
                                Some(parent_source_id.clone())
                            } else {
                                find_mongo_field(item_doc, col).and_then(|v| {
                                    serialize_column_value(
                                        &node.jsonb_cols,
                                        &node.timestamp_cols,
                                        &node.uuid_cols,
                                        &node.geometry_cols,
                                        col,
                                        v,
                                    )
                                })
                            }
                        })
                        .collect();

                    all_rows.entry(node.sql_name.clone()).or_default().push(row);

                    for child in &node.children {
                        match find_mongo_field_for_traversal(item_doc, &child.mongo_field).as_ref()
                        {
                            Some(Bson::Array(arr)) => {
                                if child.is_scalar_array {
                                    for child_item in arr {
                                        let child_id = {
                                            let c =
                                                counters.entry(child.sql_name.clone()).or_insert(0);
                                            *c += 1;
                                            c.to_string()
                                        };
                                        let child_row: Vec<Option<String>> = child
                                            .columns
                                            .iter()
                                            .map(|col| {
                                                if child.pk_cols.iter().any(|pk| pk == col) {
                                                    Some(child_id.clone())
                                                } else if Some(col) == child.fk_col.as_ref() {
                                                    Some(my_id.clone())
                                                } else if col == "value" {
                                                    serialize_column_value(
                                                        &child.jsonb_cols,
                                                        &child.timestamp_cols,
                                                        &child.uuid_cols,
                                                        &child.geometry_cols,
                                                        col,
                                                        child_item,
                                                    )
                                                } else {
                                                    None
                                                }
                                            })
                                            .collect();
                                        all_rows
                                            .entry(child.sql_name.clone())
                                            .or_default()
                                            .push(child_row);
                                    }
                                } else {
                                    for child_item in arr {
                                        extract_rows(
                                            child_item,
                                            child,
                                            Some(&my_id),
                                            false,
                                            all_rows,
                                            counters,
                                        );
                                    }
                                }
                            }
                            Some(doc_val @ Bson::Document(_)) => {
                                extract_rows(
                                    doc_val,
                                    child,
                                    Some(&my_id),
                                    false,
                                    all_rows,
                                    counters,
                                );
                            }
                            _ => {}
                        }
                    }
                }
            }
            return;
        }
    }

    if !is_root {
        let map_entries = map_document_entries(node, doc);
        if !map_entries.is_empty() {
            for (entry_key, entry_doc) in map_entries {
                let c = counters.entry(node.sql_name.clone()).or_insert(0);
                *c += 1;
                let child_id = c.to_string();

                let row: Vec<Option<String>> = node
                    .columns
                    .iter()
                    .map(|col| {
                        if node.pk_cols.iter().any(|pk| pk == col) {
                            Some(child_id.clone())
                        } else if Some(col) == node.fk_col.as_ref() {
                            Some(parent_id.unwrap_or("").to_owned())
                        } else if col == "key" {
                            Some(entry_key.to_owned())
                        } else {
                            find_mongo_field(entry_doc, col).and_then(|v| {
                                serialize_column_value(
                                    &node.jsonb_cols,
                                    &node.timestamp_cols,
                                    &node.uuid_cols,
                                    &node.geometry_cols,
                                    col,
                                    v,
                                )
                            })
                        }
                    })
                    .collect();

                if should_skip_empty_row(node, &row, false) {
                    continue;
                }

                all_rows.entry(node.sql_name.clone()).or_default().push(row);

                for child in &node.children {
                    match find_mongo_field_for_traversal(entry_doc, &child.mongo_field).as_ref() {
                        Some(Bson::Array(arr)) => {
                            if child.is_scalar_array {
                                for item in arr {
                                    let grandchild_id = {
                                        let c = counters.entry(child.sql_name.clone()).or_insert(0);
                                        *c += 1;
                                        c.to_string()
                                    };
                                    let child_row: Vec<Option<String>> = child
                                        .columns
                                        .iter()
                                        .map(|col| {
                                            if child.pk_cols.iter().any(|pk| pk == col) {
                                                Some(grandchild_id.clone())
                                            } else if Some(col) == child.fk_col.as_ref() {
                                                Some(child_id.clone())
                                            } else if col == "value" {
                                                serialize_column_value(
                                                    &child.jsonb_cols,
                                                    &child.timestamp_cols,
                                                    &child.uuid_cols,
                                                    &child.geometry_cols,
                                                    col,
                                                    item,
                                                )
                                            } else {
                                                None
                                            }
                                        })
                                        .collect();
                                    all_rows
                                        .entry(child.sql_name.clone())
                                        .or_default()
                                        .push(child_row);
                                }
                            } else {
                                for item in arr {
                                    extract_rows(
                                        item,
                                        child,
                                        Some(&child_id),
                                        false,
                                        all_rows,
                                        counters,
                                    );
                                }
                            }
                        }
                        Some(doc_val @ Bson::Document(_)) => {
                            extract_rows(
                                doc_val,
                                child,
                                Some(&child_id),
                                false,
                                all_rows,
                                counters,
                            );
                        }
                        _ => {}
                    }
                }
            }
            return;
        }
    }

    // Assign an ID for this row.
    let my_id: String = if is_root {
        // Root table: _id → id
        doc.get("_id")
            .and_then(bson_to_uuid_string)
            .or_else(|| doc.get("_id").and_then(bson_to_string))
            .unwrap_or_default()
    } else {
        let c = counters.entry(node.sql_name.clone()).or_insert(0);
        *c += 1;
        c.to_string()
    };

    // Build the row by iterating SQL columns in order.
    let row: Vec<Option<String>> = node
        .columns
        .iter()
        .map(|col| {
            if (!is_root && node.pk_cols.iter().any(|pk| pk == col))
                || (is_root && node.pk_cols.len() == 1 && node.pk_cols[0] == "id" && col == "id")
            {
                Some(my_id.clone())
            } else if Some(col) == node.fk_col.as_ref() {
                Some(parent_id.unwrap_or("").to_owned())
            } else {
                if is_root {
                    if let Some(lit) = root_target_to_literal.get(col.as_str()) {
                        return Some(lit.clone());
                    }
                }
                let lookup = if is_root {
                    find_root_mongo_field_mapped(doc, col, root_target_to_source)
                } else {
                    find_mongo_field(doc, col)
                };
                lookup.and_then(|v| {
                    serialize_column_value(
                        &node.jsonb_cols,
                        &node.timestamp_cols,
                        &node.uuid_cols,
                        &node.geometry_cols,
                        col,
                        v,
                    )
                })
            }
        })
        .collect();

    if should_skip_empty_row(node, &row, is_root) {
        return;
    }

    all_rows.entry(node.sql_name.clone()).or_default().push(row);

    // Recurse into child tables.
    for child in &node.children {
        if let Some(grouped_fields) = &child.grouped_root_fields {
            for grouped_field in grouped_fields {
                match find_mongo_field(doc, grouped_field) {
                    Some(Bson::Array(arr)) => {
                        for item in arr {
                            let item_doc = match item {
                                Bson::Document(item_doc) => item_doc,
                                _ => continue,
                            };
                            let child_id = {
                                let c = counters.entry(child.sql_name.clone()).or_insert(0);
                                *c += 1;
                                c.to_string()
                            };
                            let child_row: Vec<Option<String>> = child
                                .columns
                                .iter()
                                .map(|col| {
                                    if child.pk_cols.iter().any(|pk| pk == col) {
                                        Some(child_id.clone())
                                    } else if Some(col) == child.fk_col.as_ref() {
                                        Some(my_id.clone())
                                    } else if col == "key" {
                                        Some(grouped_field.clone())
                                    } else {
                                        find_mongo_field(item_doc, col).and_then(|v| {
                                            serialize_column_value(
                                                &child.jsonb_cols,
                                                &child.timestamp_cols,
                                                &child.uuid_cols,
                                                &child.geometry_cols,
                                                col,
                                                v,
                                            )
                                        })
                                    }
                                })
                                .collect();
                            all_rows
                                .entry(child.sql_name.clone())
                                .or_default()
                                .push(child_row);

                            for grandchild in &child.children {
                                match find_mongo_field_for_traversal(
                                    item_doc,
                                    &grandchild.mongo_field,
                                )
                                .as_ref()
                                {
                                    Some(Bson::Array(arr)) => {
                                        if grandchild.is_scalar_array {
                                            for item in arr {
                                                let grandchild_id = {
                                                    let c = counters
                                                        .entry(grandchild.sql_name.clone())
                                                        .or_insert(0);
                                                    *c += 1;
                                                    c.to_string()
                                                };
                                                let grandchild_row: Vec<Option<String>> =
                                                    grandchild
                                                        .columns
                                                        .iter()
                                                        .map(|col| {
                                                            if grandchild
                                                                .pk_cols
                                                                .iter()
                                                                .any(|pk| pk == col)
                                                            {
                                                                Some(grandchild_id.clone())
                                                            } else if Some(col)
                                                                == grandchild.fk_col.as_ref()
                                                            {
                                                                Some(child_id.clone())
                                                            } else if col == "value" {
                                                                serialize_column_value(
                                                                    &grandchild.jsonb_cols,
                                                                    &grandchild.timestamp_cols,
                                                                    &grandchild.uuid_cols,
                                                                    &grandchild.geometry_cols,
                                                                    col,
                                                                    item,
                                                                )
                                                            } else {
                                                                None
                                                            }
                                                        })
                                                        .collect();
                                                all_rows
                                                    .entry(grandchild.sql_name.clone())
                                                    .or_default()
                                                    .push(grandchild_row);
                                            }
                                        } else {
                                            for item in arr {
                                                extract_rows(
                                                    item,
                                                    grandchild,
                                                    Some(&child_id),
                                                    false,
                                                    all_rows,
                                                    counters,
                                                );
                                            }
                                        }
                                    }
                                    Some(doc_val @ Bson::Document(_)) => {
                                        extract_rows(
                                            doc_val,
                                            grandchild,
                                            Some(&child_id),
                                            false,
                                            all_rows,
                                            counters,
                                        );
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                    Some(doc_val @ Bson::Document(_)) => {
                        extract_rows(doc_val, child, Some(&my_id), false, all_rows, counters);
                    }
                    _ => {}
                }
            }
            continue;
        }

        match find_mongo_field_for_traversal(doc, &child.mongo_field).as_ref() {
            Some(Bson::Array(arr)) => {
                if child.is_scalar_array {
                    // One row per scalar element.
                    for item in arr {
                        let child_id = {
                            let c = counters.entry(child.sql_name.clone()).or_insert(0);
                            *c += 1;
                            c.to_string()
                        };
                        let child_row: Vec<Option<String>> = child
                            .columns
                            .iter()
                            .map(|col| {
                                if child.pk_cols.iter().any(|pk| pk == col) {
                                    Some(child_id.clone())
                                } else if Some(col) == child.fk_col.as_ref() {
                                    Some(my_id.clone())
                                } else if col == "value" {
                                    serialize_column_value(
                                        &child.jsonb_cols,
                                        &child.timestamp_cols,
                                        &child.uuid_cols,
                                        &child.geometry_cols,
                                        col,
                                        item,
                                    )
                                } else {
                                    None
                                }
                            })
                            .collect();
                        all_rows
                            .entry(child.sql_name.clone())
                            .or_default()
                            .push(child_row);
                    }
                } else {
                    // One row per document element.
                    for item in arr {
                        extract_rows(item, child, Some(&my_id), false, all_rows, counters);
                    }
                }
            }
            Some(doc_val @ Bson::Document(_)) => {
                // Embedded 1:1 object.
                if let Bson::Document(child_doc) = doc_val {
                    extract_child_document_rows(child_doc, child, &my_id, all_rows, counters);
                }
            }
            _ => {} // field absent or unexpected type – skip
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Public API
// ──────────────────────────────────────────────────────────────────────────────

/// Export a single MongoDB collection to gzipped CSV files.
///
/// One `.csv.gz` file is written per SQL table (root + all child tables) into
/// `<data_dir>/<db_name>/<sanitize(coll_name)>/`.  The SQL schema is read from
/// `<tables_dir>/<sanitize(coll_name)>.sql`.
pub async fn export_collections_to_sql(
    client: &Client,
    db_name: &str,
    coll_names: &[String],
    sql_lookup_name: &str,
    tables_dir: &Path,
    collections_dir: &Path,
    data_dir: &Path,
    chunk_size: u64,
    backend: &ExportWriteBackend,
) -> Result<()> {
    const PROGRESS_LOG_EVERY_DOCS: u64 = 10_000;

    if coll_names.is_empty() {
        return Ok(());
    }
    if chunk_size == 0 {
        return Err(anyhow::anyhow!("export chunk_size must be greater than 0"));
    }

    if let ExportWriteBackend::Gcs { bucket, .. } = backend {
        if gcs_preflight_enabled() {
            info!(
                "-> export preflight: validating GCS destination bucket '{}'",
                bucket
            );
            if let Err(err) = preflight_gcs_destination(bucket).await {
                warn!(
                    "GCS preflight failed for bucket '{}': {}. Continuing and attempting direct upload",
                    bucket,
                    err
                );
            }
        } else {
            info!(
                "-> export preflight: skipped (set MONGO2PG_GCS_PREFLIGHT=1 to enable)"
            );
        }
    }

    let sql_lookup_name = sanitize(sql_lookup_name);
    let sql_path = tables_dir.join(format!("{sql_lookup_name}.sql"));
    if !sql_path.exists() {
        return Err(anyhow::anyhow!(
            "SQL schema not found: {} – run `to-pg` first",
            sql_path.display()
        ));
    }

    let sql = std::fs::read_to_string(&sql_path)
        .with_context(|| format!("Cannot read {}", sql_path.display()))?;

    let sql_tables = parse_sql(&sql);
    if sql_tables.is_empty() {
        return Err(anyhow::anyhow!(
            "No CREATE TABLE statements found in {}",
            sql_path.display()
        ));
    }

    let mut coll_names = coll_names.to_vec();
    coll_names.sort();

    let out_dir = data_dir.join(db_name).join(&sql_lookup_name);
    std::fs::create_dir_all(&out_dir)
        .with_context(|| format!("Cannot create {}", out_dir.display()))?;

    fn split_numeric_suffix(name: &str) -> Option<(&str, usize)> {
        let (base, suffix) = name.rsplit_once('_')?;
        let parsed = suffix.parse::<usize>().ok()?;
        Some((base, parsed))
    }

    let table_names = sql_tables
        .iter()
        .map(|table| table.name.clone())
        .collect::<HashSet<_>>();
    let mut alias_candidates: HashMap<String, Vec<(usize, String)>> = HashMap::new();
    for table_name in &table_names {
        if let Some((base, suffix)) = split_numeric_suffix(table_name) {
            if !table_names.contains(base) {
                alias_candidates
                    .entry(base.to_owned())
                    .or_default()
                    .push((suffix, table_name.clone()));
            }
        }
    }
    let mut alias_for_table: HashMap<String, String> = HashMap::new();
    for (base, mut candidates) in alias_candidates {
        candidates.sort_by_key(|(suffix, _)| *suffix);
        if let Some((_, table_name)) = candidates.into_iter().next() {
            alias_for_table.insert(table_name, base);
        }
    }

    let mut expected_files = sql_tables
        .iter()
        .flat_map(|table| {
            [
                format!("{}.csv", table.name),
                format!("{}.csv.gz", table.name),
            ]
        })
        .collect::<HashSet<_>>();
    for alias in alias_for_table.values() {
        expected_files.insert(format!("{alias}.csv"));
        expected_files.insert(format!("{alias}.csv.gz"));
    }

    for entry in
        std::fs::read_dir(&out_dir).with_context(|| format!("Cannot read {}", out_dir.display()))?
    {
        let path = entry?.path();
        if !path.is_file() {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let is_export_file = file_name.ends_with(".csv") || file_name.ends_with(".csv.gz");
        if is_export_file && !expected_files.contains(file_name) {
            std::fs::remove_file(&path)
                .with_context(|| format!("Cannot remove stale export file {}", path.display()))?;
        }
    }

    for expected in &expected_files {
        let expected_path = out_dir.join(expected);
        if expected_path.is_file() {
            std::fs::remove_file(&expected_path)
                .with_context(|| format!("Cannot reset export file {}", expected_path.display()))?;
        }
    }

    let mut writers: HashMap<String, GzEncoder<BufWriter<std::fs::File>>> = HashMap::new();
    for sql_t in &sql_tables {
        let csv_path = out_dir.join(format!("{}.csv.gz", sql_t.name));
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&csv_path)
            .with_context(|| format!("Cannot open {} for write", csv_path.display()))?;
        let mut gz = GzEncoder::new(BufWriter::new(file), Compression::default());
        let header = csv_header_for_table(sql_t).join(",");
        writeln!(gz, "{header}")
            .with_context(|| format!("Cannot write CSV header to {}", csv_path.display()))?;
        writers.insert(sql_t.name.clone(), gz);
    }

    let mut all_rows: HashMap<String, Vec<Vec<Option<String>>>> = HashMap::new();
    let mut counters: HashMap<String, u64> = HashMap::new();

    let total_sources = coll_names.len();
    for (source_index, coll_name) in coll_names.iter().enumerate() {
        info!(
            "-> export source [{}/{}]: {}.{} -> {}.sql",
            source_index + 1,
            total_sources,
            db_name,
            coll_name,
            sql_lookup_name
        );

        let safe_name = coll_name.replace('/', "_");
        let plans = load_export_table_plans(collections_dir, &safe_name);

        let (roots, root_target_to_source) = if let Some((roots, root_target_to_source)) =
            build_tree_from_mapping_plan(&sql_tables, &plans)
        {
            (roots, root_target_to_source)
        } else {
            warn!(
                "traversal metadata incomplete for '{}'; falling back to SQL-structure traversal",
                coll_name
            );

            let fallback_roots = build_tree(&sql_tables, None, &HashMap::new());
            let fallback_root_target_to_source = sql_tables
                .iter()
                .find(|table| table.foreign_keys.is_empty())
                .and_then(|root_table| plans.get(&sanitize(&root_table.name)))
                .map(|plan| plan.target_to_source.clone())
                .unwrap_or_default();

            (fallback_roots, fallback_root_target_to_source)
        };

        let root_target_to_literal = roots
            .first()
            .and_then(|root_node| plans.get(&sanitize(&root_node.sql_name)))
            .map(|plan| plan.target_to_literal.clone())
            .unwrap_or_default();

        let db = client.database(db_name);
        let collection = db.collection::<bson::Document>(coll_name);
        let mut cursor = collection
            .find(bson::doc! {})
            .await
            .with_context(|| format!("Failed to query {db_name}.{coll_name}"))?;
        let mut source_docs_exported = 0_u64;

        while let Some(doc) = cursor.try_next().await.context("Cursor error")? {
            source_docs_exported += 1;
            if source_docs_exported % PROGRESS_LOG_EVERY_DOCS == 0 {
                info!(
                    "progress {}.{}: {} docs exported",
                    db_name, coll_name, source_docs_exported
                );
            }

            let bson_val = Bson::Document(doc);
            for root in &roots {
                extract_rows_with_mapping(
                    &bson_val,
                    root,
                    None,
                    true,
                    &mut all_rows,
                    &mut counters,
                    &root_target_to_source,
                    &root_target_to_literal,
                );
            }

            for (table_name, rows) in all_rows.drain() {
                let Some(writer) = writers.get_mut(&table_name) else {
                    warn!(
                        "Skipping {} rows for unknown table '{}'",
                        rows.len(),
                        table_name
                    );
                    continue;
                };
                for row in rows {
                    let line: Vec<String> = row
                        .iter()
                        .map(|value| csv_cell_text(value.as_deref()))
                        .collect();
                    writeln!(writer, "{}", line.join(",")).with_context(|| {
                        format!(
                            "Cannot append CSV row for table '{}' in {}",
                            table_name,
                            out_dir.display()
                        )
                    })?;
                }
            }
        }

        info!(
            "-> completed {}.{}: {} docs exported",
            db_name, coll_name, source_docs_exported
        );
    }

    for (table_name, rows) in all_rows.drain() {
        let Some(writer) = writers.get_mut(&table_name) else {
            warn!(
                "Skipping {} rows for unknown table '{}'",
                rows.len(),
                table_name
            );
            continue;
        };
        for row in rows {
            let line: Vec<String> = row
                .iter()
                .map(|value| csv_cell_text(value.as_deref()))
                .collect();
            writeln!(writer, "{}", line.join(",")).with_context(|| {
                format!(
                    "Cannot append CSV row for table '{}' in {}",
                    table_name,
                    out_dir.display()
                )
            })?;
        }
    }

    for (table_name, writer) in writers {
        writer
            .finish()
            .with_context(|| format!("Cannot finish gzip stream for table '{}'", table_name))?;
    }

    for sql_t in &sql_tables {
        if let Some(alias) = alias_for_table.get(&sql_t.name) {
            let csv_path = out_dir.join(format!("{}.csv.gz", sql_t.name));
            let alias_path = out_dir.join(format!("{alias}.csv.gz"));
            std::fs::copy(&csv_path, &alias_path).with_context(|| {
                format!(
                    "Cannot create alias export file {} from {}",
                    alias_path.display(),
                    csv_path.display()
                )
            })?;
        }
    }

    if let ExportWriteBackend::Gcs { bucket, prefix } = backend {
        info!(
            "-> export finalize: uploading grouped artifacts to gs://{}/{}",
            bucket,
            prefix.trim_matches('/')
        );
        upload_export_files_to_gcs(&out_dir, db_name, &sql_lookup_name, bucket, prefix).await?;
    }

    Ok(())
}

pub async fn export_collection(
    client: &Client,
    db_name: &str,
    coll_name: &str,
    tables_dir: &Path,
    collections_dir: &Path,
    data_dir: &Path,
) -> Result<()> {
    let sql_lookup_name = sanitize(coll_name);
    export_collections_to_sql(
        client,
        db_name,
        &[coll_name.to_owned()],
        &sql_lookup_name,
        tables_dir,
        collections_dir,
        data_dir,
        DEFAULT_EXPORT_CHUNK_ROWS,
        &ExportWriteBackend::LocalFs,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::{
        build_tree, build_tree_with_grouped_root, categorize_cloud_error_message, extract_rows,
        flattened_grouped_root_for_export, flush_chunk_buffers, gcs_object_key,
        grouped_root_table_sources, unquote_sql_ident, CloudWriteErrorCategory,
    };
    use crate::analyzer::Analyzer;
    use crate::schema_diagram::parse_sql;
    use bson::{doc, Bson};
    use flate2::read::MultiGzDecoder;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::io::Read;

    #[test]
    fn gcs_error_categorization_maps_auth_and_permission() {
        assert_eq!(
            categorize_cloud_error_message("Unauthenticated request, missing token"),
            CloudWriteErrorCategory::Authentication
        );
        assert_eq!(
            categorize_cloud_error_message("forbidden: status: 403"),
            CloudWriteErrorCategory::Authorization
        );
    }

    #[test]
    fn gcs_error_categorization_maps_not_found_and_transient() {
        assert_eq!(
            categorize_cloud_error_message("bucket not found status: 404"),
            CloudWriteErrorCategory::NotFound
        );
        assert_eq!(
            categorize_cloud_error_message("service unavailable status: 503"),
            CloudWriteErrorCategory::Transient
        );
    }

    #[test]
    fn gcs_object_key_keeps_grouped_export_layout() {
        let key = gcs_object_key("team/prefix", "ciam_prep", "events", "events.csv.gz");
        assert_eq!(key, "team/prefix/data/ciam_prep/events/events.csv.gz");
    }

    #[test]
    fn export_root_rows_use_flattened_object_id_fields_and_skip_fake_primary_column() {
        let sql = r#"
CREATE TABLE security_logs (
    log_type TEXT NOT NULL,
    projectid TEXT NOT NULL,
    provider TEXT NOT NULL,
    last_execution TEXT NOT NULL,
    PRIMARY KEY (log_type, projectid, provider)
);
"#;

        let tables = parse_sql(sql);
        let roots = build_tree(&tables, None, &HashMap::new());
        let mut all_rows = HashMap::new();
        let mut counters = HashMap::new();
        let doc = doc! {
            "_id": {
                "projectid": "FRAS-P-SAM-FRTERR2",
                "provider": "atlas",
                "log_type": "dbAccessHistory"
            },
            "last_execution": "2023-07-13T09:02:15.833170"
        };

        extract_rows(
            &Bson::Document(doc),
            &roots[0],
            None,
            true,
            &mut all_rows,
            &mut counters,
        );

        let rows = all_rows.get("security_logs").expect("root rows missing");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 4);
        assert_eq!(rows[0][0].as_deref(), Some("dbAccessHistory"));
        assert_eq!(rows[0][1].as_deref(), Some("FRAS-P-SAM-FRTERR2"));
        assert_eq!(rows[0][2].as_deref(), Some("atlas"));
        assert_eq!(rows[0][3].as_deref(), Some("2023-07-13T09:02:15.833170"));
    }

    #[test]
    fn chunk_flush_writes_header_once_and_appends_rows() {
        let sql = r#"
CREATE TABLE customers (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL
);
"#;
        let sql_tables = parse_sql(sql);
        let temp = tempfile::tempdir().expect("temp dir should be created");

        let mut all_rows: HashMap<String, Vec<Vec<Option<String>>>> = HashMap::new();
        let mut header_written = HashSet::new();

        all_rows.insert(
            "customers".to_owned(),
            vec![
                vec![Some("1".to_owned()), Some("Alice".to_owned())],
                vec![Some("2".to_owned()), Some("Bob".to_owned())],
            ],
        );
        flush_chunk_buffers(&sql_tables, temp.path(), &mut all_rows, &mut header_written)
            .expect("first flush should succeed");

        all_rows.insert(
            "customers".to_owned(),
            vec![vec![Some("3".to_owned()), Some("Carol".to_owned())]],
        );
        flush_chunk_buffers(&sql_tables, temp.path(), &mut all_rows, &mut header_written)
            .expect("second flush should succeed");

        let mut content = String::new();
        let file = std::fs::File::open(temp.path().join("customers.csv.gz"))
            .expect("chunked output should exist");
        let mut decoder = MultiGzDecoder::new(file);
        decoder
            .read_to_string(&mut content)
            .expect("gzip content should decode");

        let lines = content.lines().collect::<Vec<_>>();
        assert_eq!(lines[0], "id,name");
        assert_eq!(lines.len(), 4);
        assert_eq!(lines[1], "1,Alice");
        assert_eq!(lines[2], "2,Bob");
        assert_eq!(lines[3], "3,Carol");
    }

    #[test]
    fn chunk_flush_grouped_rows_append_without_truncation() {
        let sql = r#"
CREATE TABLE events (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL
);
"#;
        let sql_tables = parse_sql(sql);
        let temp = tempfile::tempdir().expect("temp dir should be created");

        let mut all_rows: HashMap<String, Vec<Vec<Option<String>>>> = HashMap::new();
        let mut header_written = HashSet::new();

        // First grouped source chunk.
        all_rows.insert(
            "events".to_owned(),
            vec![vec![Some("e1".to_owned()), Some("events_bcit".to_owned())]],
        );
        flush_chunk_buffers(&sql_tables, temp.path(), &mut all_rows, &mut header_written)
            .expect("first grouped chunk should flush");

        // Second grouped source chunk into same target table file.
        all_rows.insert(
            "events".to_owned(),
            vec![vec![Some("e2".to_owned()), Some("events_lmza".to_owned())]],
        );
        flush_chunk_buffers(&sql_tables, temp.path(), &mut all_rows, &mut header_written)
            .expect("second grouped chunk should flush");

        let mut content = String::new();
        let file = std::fs::File::open(temp.path().join("events.csv.gz"))
            .expect("grouped output should exist");
        let mut decoder = MultiGzDecoder::new(file);
        decoder
            .read_to_string(&mut content)
            .expect("gzip content should decode");

        assert!(content.contains("id,source"));
        assert!(content.contains("e1,events_bcit"));
        assert!(content.contains("e2,events_lmza"));
    }

    #[test]
    fn export_root_rows_write_scalar_id_column_from_mongo_id() {
        let sql = r#"
CREATE TABLE activity_feed (
    id TEXT PRIMARY KEY,
    activity TEXT NOT NULL,
    targetid TEXT NOT NULL,
    targettype TEXT NOT NULL,
    timestamp DOUBLE PRECISION NOT NULL,
    who TEXT NOT NULL
);
"#;

        let tables = parse_sql(sql);
        let roots = build_tree(&tables, None, &HashMap::new());
        let mut all_rows = HashMap::new();
        let mut counters = HashMap::new();
        let doc = doc! {
            "_id": "6267ddea9270b5e839a81ac4",
            "activity": "Project created",
            "targetid": "FRAS-D-NLX-FEATURE1",
            "targettype": "project",
            "timestamp": 1650468505.273496,
            "who": "me"
        };

        extract_rows(
            &Bson::Document(doc),
            &roots[0],
            None,
            true,
            &mut all_rows,
            &mut counters,
        );

        let rows = all_rows.get("activity_feed").expect("root rows missing");
        assert_eq!(rows.len(), 1);
        assert!(rows[0][0].as_deref().is_some_and(|value| !value.is_empty()));
        assert_eq!(rows[0][1].as_deref(), Some("Project created"));
        assert_eq!(rows[0][2].as_deref(), Some("FRAS-D-NLX-FEATURE1"));
    }

    #[test]
    fn export_map_object_rows_emit_one_child_row_per_key() {
        let sql = r#"
CREATE TABLE customers (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE tier_and_details (
    id BIGSERIAL PRIMARY KEY,
    customers_id UUID NOT NULL,
    key TEXT NOT NULL,
    active BOOLEAN NOT NULL,
    benefits TEXT[] NOT NULL,
    tier VARCHAR(20) NOT NULL,
    FOREIGN KEY (customers_id) REFERENCES customers (id) DEFERRABLE INITIALLY DEFERRED
);
"#;

        let tables = parse_sql(sql);
        let roots = build_tree(&tables, None, &HashMap::new());
        let mut all_rows = HashMap::new();
        let mut counters = HashMap::new();
        let doc = doc! {
            "_id": "5ca4bbcea2dd94ee58162a68",
            "name": "Elizabeth Ray",
            "tier_and_details": {
                "0df078f33aa74a2e9696e0520c1a828a": {
                    "tier": "Bronze",
                    "active": true,
                    "benefits": ["sports tickets"]
                },
                "699456451cc24f028d2aa99d7534c219": {
                    "tier": "Bronze",
                    "active": true,
                    "benefits": ["24 hour dedicated line", "concierge services"]
                }
            }
        };

        extract_rows(
            &Bson::Document(doc),
            &roots[0],
            None,
            true,
            &mut all_rows,
            &mut counters,
        );

        let rows = all_rows
            .get("tier_and_details")
            .expect("tier_and_details rows missing");
        assert_eq!(rows.len(), 2);
        assert!(rows
            .iter()
            .all(|row| { row[1].as_deref() == Some("00000000-5ca4-bbce-a2dd-94ee58162a68") }));
        assert!(rows
            .iter()
            .any(|row| row[2].as_deref() == Some("0df078f33aa74a2e9696e0520c1a828a")));
        assert!(rows
            .iter()
            .any(|row| row[2].as_deref() == Some("699456451cc24f028d2aa99d7534c219")));
    }

    #[test]
    fn export_promoted_root_array_objects_write_one_row_per_item() {
        let sql = r#"
CREATE TABLE engine (
    id BIGSERIAL PRIMARY KEY,
    engine_id TEXT NOT NULL,
    eol_date TIMESTAMP WITH TIME ZONE NOT NULL,
    grace_date TIMESTAMP WITH TIME ZONE NOT NULL,
    major_version TEXT NOT NULL
);
"#;

        let tables = parse_sql(sql);
        let roots = build_tree(
            &tables,
            Some(("versions".to_owned(), "engine_id".to_owned())),
            &HashMap::new(),
        );
        let mut all_rows = HashMap::new();
        let mut counters = HashMap::new();
        let doc = doc! {
            "_id": "pg",
            "versions": [
                {
                    "major_version": "13",
                    "grace_date": "2025-05-13T00:00:00Z",
                    "eol_date": "2025-11-13T00:00:00Z"
                },
                {
                    "major_version": "14",
                    "grace_date": "2026-05-12T00:00:00Z",
                    "eol_date": "2026-11-12T00:00:00Z"
                }
            ]
        };

        extract_rows(
            &Bson::Document(doc),
            &roots[0],
            None,
            true,
            &mut all_rows,
            &mut counters,
        );

        let rows = all_rows.get("engine").expect("root rows missing");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1].as_deref(), Some("pg"));
        assert_eq!(rows[0][4].as_deref(), Some("13"));
        assert_eq!(rows[1][1].as_deref(), Some("pg"));
        assert_eq!(rows[1][4].as_deref(), Some("14"));
    }

    #[test]
    fn export_timestamp_columns_coerce_numeric_values_to_iso_strings() {
        let sql = r#"
CREATE TABLE scheduling_jobs (
    id TEXT PRIMARY KEY,
    last_update TIMESTAMP WITH TIME ZONE NOT NULL
);
"#;

        let tables = parse_sql(sql);
        let roots = build_tree(&tables, None, &HashMap::new());
        let mut all_rows = HashMap::new();
        let mut counters = HashMap::new();
        let doc = doc! {
            "_id": "job-1",
            "last_update": 1650468505.273496_f64
        };

        extract_rows(
            &Bson::Document(doc),
            &roots[0],
            None,
            true,
            &mut all_rows,
            &mut counters,
        );

        let rows = all_rows.get("scheduling_jobs").expect("root rows missing");
        assert_eq!(rows[0][0].as_deref(), Some("job-1"));
        assert_eq!(rows[0][1].as_deref(), Some("2022-04-20 15:28:25.273+00:00"));
    }

    #[test]
    fn export_geometry_columns_write_ewkt_from_geojson_point() {
        let sql = r#"
CREATE TABLE places (
    id TEXT PRIMARY KEY,
    geo geometry(Point,4326) NOT NULL
);
"#;

        let tables = parse_sql(sql);
        let roots = build_tree(&tables, None, &HashMap::new());
        let mut all_rows = HashMap::new();
        let mut counters = HashMap::new();
        let doc = doc! {
            "_id": "place-1",
            "geo": {
                "type": "Point",
                "coordinates": [2.3522_f64, 48.8566_f64]
            }
        };

        extract_rows(
            &Bson::Document(doc),
            &roots[0],
            None,
            true,
            &mut all_rows,
            &mut counters,
        );

        let rows = all_rows.get("places").expect("root rows missing");
        assert_eq!(rows[0][0].as_deref(), Some("place-1"));
        assert_eq!(
            rows[0][1].as_deref(),
            Some("SRID=4326;POINT(2.3522 48.8566)")
        );
    }

    #[test]
    fn export_embedded_location_with_address_and_geo_is_not_treated_as_map_document() {
        let sql = r#"
CREATE TABLE theaters (
    id UUID PRIMARY KEY,
    theaterid INTEGER NOT NULL
);

CREATE TABLE theaters_location (
    id BIGSERIAL PRIMARY KEY,
    theaters_id UUID NOT NULL,
    city VARCHAR(20) NOT NULL,
    state VARCHAR(2) NOT NULL,
    street1 TEXT NOT NULL,
    street2 VARCHAR(20),
    zipcode VARCHAR(20) NOT NULL,
    geo geometry(Point,4326) NOT NULL,
    FOREIGN KEY (theaters_id) REFERENCES theaters (id) DEFERRABLE INITIALLY DEFERRED
);
"#;

        let tables = parse_sql(sql);
        let roots = build_tree(&tables, None, &HashMap::new());
        let mut all_rows = HashMap::new();
        let mut counters = HashMap::new();
        let doc = doc! {
            "_id": bson::oid::ObjectId::parse_str("59a47286cfa9a3a73e51e72d").unwrap(),
            "theaterId": 1001,
            "location": {
                "address": {
                    "city": "California",
                    "state": "MD",
                    "street1": "45235 Worth Ave.",
                    "zipcode": "20619"
                },
                "geo": {
                    "type": "Point",
                    "coordinates": [-76.512345_f64, 38.123456_f64]
                }
            }
        };

        extract_rows(
            &Bson::Document(doc),
            &roots[0],
            None,
            true,
            &mut all_rows,
            &mut counters,
        );

        let rows = all_rows
            .get("theaters_location")
            .expect("theaters_location rows missing");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][2].as_deref(), Some("California"));
        assert_eq!(rows[0][3].as_deref(), Some("MD"));
        assert_eq!(rows[0][4].as_deref(), Some("45235 Worth Ave."));
        assert_eq!(rows[0][6].as_deref(), Some("20619"));
        assert_eq!(
            rows[0][7].as_deref(),
            Some("SRID=4326;POINT(-76.512345 38.123456)")
        );
    }

    #[test]
    fn export_groups_same_shape_root_arrays_into_one_keyed_child_table() {
        let sql = r#"
CREATE TABLE communities (
    id BIGSERIAL PRIMARY KEY,
    communities_id TEXT NOT NULL,
    key TEXT NOT NULL,
    cloud TEXT NOT NULL,
    network_exposition TEXT NOT NULL,
    provider TEXT NOT NULL
);

CREATE TABLE communities_available_localizations (
    id BIGSERIAL PRIMARY KEY,
    communities_id BIGINT NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY (communities_id) REFERENCES communities (id) DEFERRABLE INITIALLY DEFERRED
);
"#;

        let mut analyzer = Analyzer::new(true);
        analyzer.process_document(&doc! {
            "_id": "community-1",
            "dev": [{
                "provider": "aiven",
                "cloud": "gcp",
                "network_exposition": "private_platform"
            }],
            "prod": [{
                "provider": "atlas",
                "cloud": "azure",
                "network_exposition": "public"
            }]
        });
        let schema = analyzer.finish();

        let tables = parse_sql(sql);
        let flattened_grouped_root =
            flattened_grouped_root_for_export(&tables, &schema, "communities");
        let grouped_root_sources = grouped_root_table_sources(&schema, "communities");
        let roots = build_tree_with_grouped_root(
            &tables,
            None,
            flattened_grouped_root,
            &grouped_root_sources,
        );
        let mut all_rows = HashMap::new();
        let mut counters = HashMap::new();
        let doc = doc! {
            "_id": "community-1",
            "dev": [{
                "available_localizations": ["eu-west-1"],
                "provider": "aiven",
                "cloud": "gcp",
                "network_exposition": "private_platform"
            }],
            "prod": [{
                "available_localizations": ["eu-west-2"],
                "provider": "atlas",
                "cloud": "azure",
                "network_exposition": "public"
            }]
        };

        extract_rows(
            &Bson::Document(doc),
            &roots[0],
            None,
            true,
            &mut all_rows,
            &mut counters,
        );

        let rows = all_rows
            .get("communities")
            .expect("grouped root rows missing");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1].as_deref(), Some("community-1"));
        assert_eq!(rows[0][2].as_deref(), Some("dev"));
        assert_eq!(rows[1][2].as_deref(), Some("prod"));

        let child_rows = all_rows
            .get("communities_available_localizations")
            .expect("grouped nested rows missing");
        assert_eq!(child_rows.len(), 2);
    }

    #[test]
    fn export_map_object_child_table_emits_rows_per_entry() {
        let sql = r#"
CREATE TABLE customers (
    id UUID DEFAULT public.gen_random_uuid() PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE tier_and_details (
    id BIGSERIAL PRIMARY KEY,
    customers_id UUID NOT NULL,
    key TEXT NOT NULL,
    active BOOLEAN NOT NULL,
    benefits TEXT[] NOT NULL,
    tier VARCHAR(20) NOT NULL,
    FOREIGN KEY (customers_id) REFERENCES customers (id) DEFERRABLE INITIALLY DEFERRED
);
"#;

        let tables = parse_sql(sql);
        let roots = build_tree(&tables, None, &HashMap::new());
        let mut all_rows = HashMap::new();
        let mut counters = HashMap::new();
        let doc = doc! {
            "_id": bson::oid::ObjectId::parse_str("5ca4bbcea2dd94ee58162a84").unwrap(),
            "name": "Alice",
            "tier_and_details": {
                "0134c72f17e3419cbdc857171cbb5651": {
                    "active": true,
                    "benefits": ["cashback", "support"],
                    "tier": "gold"
                },
                "01c680e72a154c3abb7e3c71a8848553": {
                    "active": false,
                    "benefits": ["support"],
                    "tier": "silver"
                }
            }
        };

        extract_rows(
            &Bson::Document(doc),
            &roots[0],
            None,
            true,
            &mut all_rows,
            &mut counters,
        );

        let customer_rows = all_rows.get("customers").expect("customers rows missing");
        assert_eq!(customer_rows.len(), 1);

        let detail_rows = all_rows
            .get("tier_and_details")
            .expect("tier_and_details rows missing");
        assert_eq!(detail_rows.len(), 2);

        let keys: std::collections::HashSet<String> = detail_rows
            .iter()
            .filter_map(|row| row[2].clone())
            .collect();
        assert!(keys.contains("0134c72f17e3419cbdc857171cbb5651"));
        assert!(keys.contains("01c680e72a154c3abb7e3c71a8848553"));

        assert_eq!(detail_rows[0][3].is_some(), true);
        assert_eq!(detail_rows[0][4].is_some(), true);
        assert_eq!(detail_rows[0][5].is_some(), true);
    }

    #[test]
    fn export_map_like_object_without_key_column_emits_rows_per_entry() {
        let sql = r#"
CREATE TABLE customers (
    id UUID DEFAULT public.gen_random_uuid() PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE tier_and_details (
    id BIGSERIAL PRIMARY KEY,
    customers_id UUID NOT NULL,
    active BOOLEAN NOT NULL,
    benefits TEXT[] NOT NULL,
    tier VARCHAR(20) NOT NULL,
    FOREIGN KEY (customers_id) REFERENCES customers (id) DEFERRABLE INITIALLY DEFERRED
);
"#;

        let tables = parse_sql(sql);
        let roots = build_tree(&tables, None, &HashMap::new());
        let mut all_rows = HashMap::new();
        let mut counters = HashMap::new();
        let doc = doc! {
            "_id": bson::oid::ObjectId::parse_str("5ca4bbcea2dd94ee58162a84").unwrap(),
            "name": "Alice",
            "tier_and_details": {
                "0134c72f17e3419cbdc857171cbb5651": {
                    "active": true,
                    "benefits": ["cashback", "support"],
                    "tier": "gold"
                },
                "01c680e72a154c3abb7e3c71a8848553": {
                    "active": false,
                    "benefits": ["support"],
                    "tier": "silver"
                }
            }
        };

        extract_rows(
            &Bson::Document(doc),
            &roots[0],
            None,
            true,
            &mut all_rows,
            &mut counters,
        );

        let detail_rows = all_rows
            .get("tier_and_details")
            .expect("tier_and_details rows missing");
        assert_eq!(detail_rows.len(), 2);

        assert!(detail_rows
            .iter()
            .any(|row| row[2].as_deref() == Some("true") && row[4].as_deref() == Some("gold")));
        assert!(detail_rows
            .iter()
            .any(|row| row[2].as_deref() == Some("false") && row[4].as_deref() == Some("silver")));
    }

    //     #[test]
    //     fn export_container_object_without_payload_keeps_child_document_context() {
    //         let sql = r#"
    // CREATE TABLE companies (
    //     id UUID DEFAULT public.gen_random_uuid() PRIMARY KEY,
    //     name TEXT NOT NULL
    // );

    // CREATE TABLE companies_investments (
    //     id BIGSERIAL PRIMARY KEY,
    //     companies_id UUID NOT NULL,
    //     FOREIGN KEY (companies_id) REFERENCES companies (id) DEFERRABLE INITIALLY DEFERRED
    // );

    // CREATE TABLE financial_org (
    //     id BIGSERIAL PRIMARY KEY,
    //     companies_investments_id BIGINT NOT NULL,
    //     name TEXT NOT NULL,
    //     permalink TEXT NOT NULL,
    //     FOREIGN KEY (companies_investments_id) REFERENCES companies_investments (id) DEFERRABLE INITIALLY DEFERRED
    // );
    // "#;

    //         let tables = parse_sql(sql);
    //         let roots = build_tree(&tables, None, &HashMap::new());
    //         let mut all_rows = HashMap::new();
    //         let mut counters = HashMap::new();
    //         let doc = doc! {
    //             "_id": bson::oid::ObjectId::parse_str("5ca4bbcea2dd94ee58162a84").unwrap(),
    //             "name": "Wetpaint",
    //             "companies_investments": {
    //                 "company": Bson::Null,
    //                 "financial_org": {
    //                     "name": "Frazier Technology Ventures",
    //                     "permalink": "frazier-technology-ventures"
    //                 },
    //                 "person": Bson::Null
    //             }
    //         };

    //         extract_rows(
    //             &Bson::Document(doc),
    //             &roots[0],
    //             None,
    //             true,
    //             &mut all_rows,
    //             &mut counters,
    //         );

    //         let investment_rows = all_rows
    //             .get("companies_investments")
    //             .expect("companies_investments rows missing");
    //         assert_eq!(investment_rows.len(), 1);

    //         let financial_rows = all_rows
    //             .get("financial_org")
    //             .expect("financial_org rows missing");
    //         assert_eq!(financial_rows.len(), 1);
    //         assert_eq!(
    //             financial_rows[0][2].as_deref(),
    //             Some("Frazier Technology Ventures")
    //         );
    //         assert_eq!(
    //             financial_rows[0][3].as_deref(),
    //             Some("frazier-technology-ventures")
    //         );
    //     }

    #[test]
    fn export_skips_empty_embedded_review_scores_object() {
        let sql = r#"
CREATE TABLE listingsandreviews (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE review_scores (
    id BIGSERIAL PRIMARY KEY,
    listingsandreviews_id UUID NOT NULL,
    review_scores_accuracy INTEGER,
    review_scores_checkin INTEGER,
    review_scores_cleanliness INTEGER,
    review_scores_communication INTEGER,
    review_scores_location INTEGER,
    review_scores_rating INTEGER,
    review_scores_value INTEGER,
    FOREIGN KEY (listingsandreviews_id) REFERENCES listingsandreviews (id) DEFERRABLE INITIALLY DEFERRED
);
"#;

        let tables = parse_sql(sql);
        let roots = build_tree(&tables, None, &HashMap::new());
        let mut all_rows = HashMap::new();
        let mut counters = HashMap::new();
        let doc = doc! {
            "_id": bson::oid::ObjectId::parse_str("5ca4bbcea2dd94ee58162a84").unwrap(),
            "name": "Alice",
            "review_scores": {}
        };

        extract_rows(
            &Bson::Document(doc),
            &roots[0],
            None,
            true,
            &mut all_rows,
            &mut counters,
        );

        let root_rows = all_rows
            .get("listingsandreviews")
            .expect("root rows missing");
        assert_eq!(root_rows.len(), 1);

        assert!(
            all_rows
                .get("review_scores")
                .map(|rows| rows.is_empty())
                .unwrap_or(true),
            "empty embedded review_scores object should not emit a null-only child row"
        );
    }

    #[test]
    fn export_restores_scalar_only_object_sibling_child_rows() {
        let sql = r#"
CREATE TABLE projects (
    id TEXT PRIMARY KEY
);

CREATE TABLE projects_providers (
    id BIGSERIAL PRIMARY KEY,
    projects_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    namespace_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    FOREIGN KEY (projects_id) REFERENCES projects (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE projects_providers_metadata (
    id BIGSERIAL PRIMARY KEY,
    projects_providers_id BIGINT NOT NULL,
    creation_date TIMESTAMP WITH TIME ZONE NOT NULL,
    status TEXT NOT NULL,
    FOREIGN KEY (projects_providers_id) REFERENCES projects_providers (id) DEFERRABLE INITIALLY DEFERRED
);
"#;

        let tables = parse_sql(sql);
        let roots = build_tree(&tables, None, &HashMap::new());
        let mut all_rows = HashMap::new();
        let mut counters = HashMap::new();
        let doc = doc! {
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
        };

        extract_rows(
            &Bson::Document(doc),
            &roots[0],
            None,
            true,
            &mut all_rows,
            &mut counters,
        );

        let rows = all_rows
            .get("projects_providers")
            .expect("providers rows missing");
        assert_eq!(rows.len(), 1);

        let metadata_rows = all_rows
            .get("projects_providers_metadata")
            .expect("providers metadata rows missing");
        assert_eq!(metadata_rows.len(), 1);
        assert_eq!(
            metadata_rows[0][2].as_deref(),
            Some("2025-08-11 00:00:00+00:00")
        );
        assert_eq!(metadata_rows[0][3].as_deref(), Some("created"));
    }

    #[test]
    fn export_timestamp_columns_normalize_rfc3339_strings() {
        let sql = r#"
CREATE TABLE scheduling_jobs (
    id TEXT PRIMARY KEY,
    last_update TIMESTAMP WITH TIME ZONE NOT NULL
);
"#;

        let tables = parse_sql(sql);
        let roots = build_tree(&tables, None, &HashMap::new());
        let mut all_rows = HashMap::new();
        let mut counters = HashMap::new();
        let doc = doc! {
            "_id": "job-1",
            "last_update": "2022-04-20T15:28:25.273Z"
        };

        extract_rows(
            &Bson::Document(doc),
            &roots[0],
            None,
            true,
            &mut all_rows,
            &mut counters,
        );

        let rows = all_rows.get("scheduling_jobs").expect("root rows missing");
        assert_eq!(rows[0][1].as_deref(), Some("2022-04-20 15:28:25.273+00:00"));
    }

    #[test]
    fn export_timestamp_array_columns_normalize_string_items() {
        let sql = r#"
CREATE TABLE engine (
    id TEXT PRIMARY KEY,
    release_date TIMESTAMP WITH TIME ZONE[] NOT NULL
);
"#;

        let tables = parse_sql(sql);
        let roots = build_tree(&tables, None, &HashMap::new());
        let mut all_rows = HashMap::new();
        let mut counters = HashMap::new();
        let doc = doc! {
            "_id": "engine-1",
            "release_date": [
                "2025-01-01T00:00:00Z",
                "2025-01-02T00:00:00Z"
            ]
        };

        extract_rows(
            &Bson::Document(doc),
            &roots[0],
            None,
            true,
            &mut all_rows,
            &mut counters,
        );

        let rows = all_rows.get("engine").expect("root rows missing");
        assert_eq!(rows[0][0].as_deref(), Some("engine-1"));
        assert_eq!(
            rows[0][1].as_deref(),
            Some("{\"2025-01-01 00:00:00+00:00\",\"2025-01-02 00:00:00+00:00\"}")
        );
    }

    #[test]
    fn export_jsonb_root_array_objects_stay_on_root_row() {
        let sql = r#"
CREATE TABLE engine (
    id TEXT PRIMARY KEY,
    versions JSONB NOT NULL
);
"#;

        let tables = parse_sql(sql);
        let mut analyzer = Analyzer::new(true);
        analyzer.process_document(&doc! {
            "_id": "pg",
            "versions": [
                {
                    "major_version": "13",
                    "grace_date": "2025-05-13T00:00:00Z",
                    "eol_date": "2025-11-13T00:00:00Z"
                },
                {
                    "major_version": "14",
                    "grace_date": "2026-05-12T00:00:00Z",
                    "eol_date": "2026-11-12T00:00:00Z"
                }
            ]
        });
        let mut schema = analyzer.finish();
        schema.mark_objects_as_jsonb();
        let roots = build_tree(&tables, None, &HashMap::new());

        let mut all_rows = HashMap::new();
        let mut counters = HashMap::new();
        let doc = doc! {
            "_id": "pg",
            "versions": [
                {
                    "major_version": "13",
                    "grace_date": "2025-05-13T00:00:00Z",
                    "eol_date": "2025-11-13T00:00:00Z"
                },
                {
                    "major_version": "14",
                    "grace_date": "2026-05-12T00:00:00Z",
                    "eol_date": "2026-11-12T00:00:00Z"
                }
            ]
        };

        extract_rows(
            &Bson::Document(doc),
            &roots[0],
            None,
            true,
            &mut all_rows,
            &mut counters,
        );

        let rows = all_rows.get("engine").expect("root rows missing");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].as_deref(), Some("pg"));
        assert_eq!(
            rows[0][1].as_deref(),
            Some(
                "[{\"major_version\":\"13\",\"grace_date\":\"2025-05-13T00:00:00Z\",\"eol_date\":\"2025-11-13T00:00:00Z\"},{\"major_version\":\"14\",\"grace_date\":\"2026-05-12T00:00:00Z\",\"eol_date\":\"2026-11-12T00:00:00Z\"}]"
            )
        );
    }

    #[test]
    fn export_array_text_values_are_csv_escaped() {
        let sql = r#"
CREATE TABLE host (
    id BIGSERIAL PRIMARY KEY,
    host_verifications TEXT[] NOT NULL
);
"#;
        let tables = parse_sql(sql);
        let mut analyzer = Analyzer::new(true);
        let doc = doc! {
            "_id": "10021707",
            "host_verifications": [
                "email",
                "phone",
                "reviews",
                "kba"
            ]
        };
        analyzer.process_document(&doc);
        let roots = build_tree(&tables, None, &HashMap::new());
        let mut all_rows = HashMap::new();
        let mut counters = HashMap::new();
        extract_rows(
            &Bson::Document(doc),
            &roots[0],
            None,
            true,
            &mut all_rows,
            &mut counters,
        );
        let rows = all_rows.get("host").expect("root rows missing");
        eprintln!("rows={rows:#?}");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].as_deref(), Some("10021707"));
        assert_eq!(rows[0][1].as_deref(), Some("{email,phone,reviews,kba}"));
    }

    #[test]
    fn export_unquotes_reserved_identifier_columns() {
        let sql = r#"
CREATE TABLE accounts (
    id UUID PRIMARY KEY,
    "limit" INTEGER,
    products TEXT[] NOT NULL
);
"#;

        let tables = parse_sql(sql);
        let roots = build_tree(&tables, None, &HashMap::new());
        let mut all_rows = HashMap::new();
        let mut counters = HashMap::new();
        let doc = doc! {
            "_id": bson::oid::ObjectId::parse_str("5ca4bbc7a2dd94ee5816238c").unwrap(),
            "limit": 371138,
            "products": ["Derivatives", "InvestmentStock"]
        };

        extract_rows(
            &Bson::Document(doc),
            &roots[0],
            None,
            true,
            &mut all_rows,
            &mut counters,
        );

        assert_eq!(unquote_sql_ident("\"limit\""), "limit");
        assert!(roots[0].columns.iter().any(|column| column == "limit"));

        let rows = all_rows.get("accounts").expect("accounts rows missing");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1].as_deref(), Some("371138"));
    }
}
