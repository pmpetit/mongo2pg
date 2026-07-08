//! HTML report generation from per-collection `.stats.yaml` files.

use crate::stats::InferWarningYaml;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

// ──────────────────────────────────────────────────────────────────────────────
// Constants
// ──────────────────────────────────────────────────────────────────────────────

/// MongoDB system databases that are skipped when enumerating user databases.
pub const SYSTEM_DATABASES: &[&str] = &["admin", "local", "config"];

// ──────────────────────────────────────────────────────────────────────────────
// Cluster-level data structures
// ──────────────────────────────────────────────────────────────────────────────

/// Aggregated migrability scores for a single database, derived from its
/// per-collection [`CollectionRow`] data.
pub struct DatabaseScore {
    /// Database / project name.
    pub name: String,
    /// Total complexity score: `1.5 × N + Σ C_i` (where N = collection count).
    pub score_db: f64,
    /// Document-count-weighted average of per-collection scores.
    pub score_avg: f64,
    /// Maximum per-collection score across all collections.
    pub score_max: f64,
    /// Total number of documents across all collections.
    pub total_docs: u64,
    /// Number of collections.
    pub collection_count: usize,
}

/// Aggregated migrability scores for a whole cluster, derived from
/// per-database [`DatabaseScore`] values.
pub struct ClusterScore {
    /// Total cluster complexity: `1.5 × D + Σ score_db_j` (D = database count).
    pub score_total: f64,
    /// Total-document-weighted average of per-database `score_avg` values.
    pub score_avg: f64,
    /// Maximum `score_db` across all databases.
    pub score_max: f64,
    /// Number of databases.
    pub db_count: usize,
}

/// Compute a [`DatabaseScore`] from a slice of [`CollectionRow`]s for one database.
pub fn compute_db_score(name: &str, rows: &[CollectionRow]) -> DatabaseScore {
    let doc_count = |r: &CollectionRow| match &r.stats.documents_in_collection {
        serde_yaml::Value::Number(n) => n.as_u64().unwrap_or(0),
        _ => 0,
    };

    let total_docs: u64 = rows.iter().map(doc_count).sum();

    // Only include collections with at least one document in the score formula,
    // matching the Python compute_cluster_scores behaviour (count == 0 → skip).
    let non_empty: Vec<&CollectionRow> = rows.iter().filter(|r| doc_count(r) > 0).collect();
    let n = non_empty.len() as f64;
    let score_sum: f64 = non_empty.iter().map(|r| r.stats.migrability_score).sum();
    let score_db = ((1.5 * n + score_sum) * 100.0).round() / 100.0;

    let score_max: f64 = rows
        .iter()
        .map(|r| r.stats.migrability_score)
        .fold(0.0_f64, f64::max);

    let total_weighted: f64 = rows
        .iter()
        .map(|r| {
            let docs = doc_count(r) as f64;
            r.stats.migrability_score * docs
        })
        .sum();
    let score_avg = if total_docs > 0 {
        (total_weighted / total_docs as f64 * 100.0).round() / 100.0
    } else {
        0.0
    };

    DatabaseScore {
        name: name.to_owned(),
        score_db,
        score_avg,
        score_max: (score_max * 100.0).round() / 100.0,
        total_docs,
        collection_count: non_empty.len(),
    }
}

/// Compute a [`ClusterScore`] from a slice of [`DatabaseScore`]s.
pub fn compute_cluster_score(dbs: &[DatabaseScore]) -> ClusterScore {
    let d = dbs.len() as f64;
    let score_db_sum: f64 = dbs.iter().map(|db| db.score_db).sum();
    let score_total = ((1.5 * d + score_db_sum) * 100.0).round() / 100.0;

    let total_docs: u64 = dbs.iter().map(|db| db.total_docs).sum();
    let total_weighted: f64 = dbs
        .iter()
        .map(|db| db.score_avg * db.total_docs as f64)
        .sum();
    let score_avg = if total_docs > 0 {
        (total_weighted / total_docs as f64 * 100.0).round() / 100.0
    } else {
        0.0
    };

    let score_max: f64 = dbs.iter().map(|db| db.score_db).fold(0.0_f64, f64::max);

    ClusterScore {
        score_total,
        score_avg,
        score_max: (score_max * 100.0).round() / 100.0,
        db_count: dbs.len(),
    }
}

/// Subset of [`crate::stats::StatsYaml`] we need for the report.
#[derive(Debug, Deserialize)]
pub struct CollectionStatsYaml {
    pub documents_in_collection: serde_yaml::Value,
    pub documents_sampled: u64,
    pub width_top_level: usize,
    pub width_max: f64,
    pub width_max_level: usize,
    pub depth_max: usize,
    pub branch_total: f64,
    #[serde(default)]
    pub branch_per_level: indexmap::IndexMap<String, f64>,
    #[serde(default)]
    pub array_field_count: usize,
    #[serde(default)]
    pub avg_fields_per_doc: f64,
    #[serde(default)]
    pub migrability_score: f64,
    #[serde(default)]
    pub infer_warnings: Vec<InferWarningYaml>,
    #[serde(default)]
    pub read_ops: Option<CollectionReadOpsYaml>,
  }

  #[derive(Debug, Deserialize)]
  pub struct CollectionReadOpsYaml {
    pub read_ops: u64,
    #[serde(default)]
    pub since: Option<String>,
}

pub struct CollectionRow {
    pub name: String,
    pub stats: CollectionStatsYaml,
  /// Resolved PostgreSQL target table for this collection, when known.
  pub pg_target_table: Option<String>,
    /// PostgreSQL tables generated for this collection: `(table_name, ddl)`.
    /// Empty when no `schema/tables/` directory was provided.
    pub table_names: Vec<(String, String)>,
}

impl CollectionRow {
    pub fn tables_count(&self) -> Option<usize> {
        if self.table_names.is_empty() {
            None
        } else {
            Some(self.table_names.len())
        }
    }

    pub fn has_infer_warnings(&self) -> bool {
        !self.stats.infer_warnings.is_empty()
    }
}

pub struct PostImportTableRow {
    pub schema_name: Option<String>,
    pub table_name: String,
    pub row_count: i64,
}

#[derive(Clone)]
pub struct PostImportMd5Column {
    pub source_field: String,
    pub source_type: Option<String>,
    pub target_field: String,
    pub target_type: Option<String>,
}

#[derive(Clone)]
pub struct PostImportMd5MismatchRow {
    pub row_index: usize,
    pub mongo_values: Option<Vec<String>>,
    pub pg_values: Option<Vec<String>>,
}

#[derive(Clone)]
pub struct PostImportCountDiffRow {
    pub row_index: usize,
    pub mongo_values: Option<Vec<String>>,
    pub pg_values: Option<Vec<String>>,
}

#[derive(Clone)]
pub struct PostImportMd5Summary {
    pub mongo_md5: String,
    pub pg_md5: String,
    pub columns: Vec<PostImportMd5Column>,
    pub mismatches: Vec<PostImportMd5MismatchRow>,
}

pub struct PostImportNode {
    pub name: String,
    pub is_array: bool,
    pub mongo_count: u64,
    pub pg_table_name: Option<String>,
    pub pg_row_count: Option<i64>,
    pub md5_summary: Option<PostImportMd5Summary>,
    pub count_diff_rows: Vec<PostImportCountDiffRow>,
    pub children: Vec<PostImportNode>,
}

pub struct PostImportCollectionRow {
    pub name: String,
    pub document_count: u64,
    pub root: PostImportNode,
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn render_documents_cell(stats: &CollectionStatsYaml) -> String {
  let doc_count = match &stats.documents_in_collection {
    serde_yaml::Value::Number(n) => n.as_u64().unwrap_or(0).to_string(),
    _ => "startup".to_owned(),
  };

  let read_ops_hint = stats
    .read_ops
    .as_ref()
    .map(|read_ops| {
      let since = read_ops.since.as_deref().unwrap_or("startup");
      format!(
        r#"<div class="docs-extra" title="MongoDB collection read operations from $collStats latencyStats.reads">reads: {} since {}</div>"#,
        read_ops.read_ops,
        escape_html(since),
      )
    })
    .unwrap_or_default();

  format!(
    r#"<td class="num">{}{}</td>"#,
    doc_count,
    read_ops_hint,
  )
}

/// Read every `<base>/<collection>/<collection>.stats.yaml` and return sorted rows.
///
/// When `tables_dir` is `Some`, each collection's `.sql` file is parsed to fill
/// `CollectionRow::table_names`.
pub fn collect_rows(base: &Path, tables_dir: Option<&Path>) -> Result<Vec<CollectionRow>> {
    let mut rows: Vec<CollectionRow> = Vec::new();

    let entries = std::fs::read_dir(base)
        .with_context(|| format!("Cannot read directory {}", base.display()))?;

    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_owned();
        let yaml_path = path.join(format!("{name}.stats.yaml"));
        if !yaml_path.exists() {
            continue;
        }
        let content = std::fs::read_to_string(&yaml_path)
            .with_context(|| format!("Cannot read {}", yaml_path.display()))?;
        let stats: CollectionStatsYaml = serde_yaml::from_str(&content)
            .with_context(|| format!("Cannot parse {}", yaml_path.display()))?;

        let table_names: Vec<(String, String)> = tables_dir
            .and_then(|dir| {
                let sql_path = dir.join(format!("{}.sql", name.to_lowercase()));
                std::fs::read_to_string(&sql_path).ok()
            })
            .map(|sql| {
                sql.split("CREATE TABLE ")
                    .skip(1)
                    .filter_map(|chunk| {
                        let paren = chunk.find('(')?;
                        let close = chunk.find(");")?;
                        let tname = chunk[..paren].trim().to_owned();
                        let ddl = format!("CREATE TABLE {}", &chunk[..close + 2]);
                        Some((tname, ddl))
                    })
                    .collect()
            })
            .unwrap_or_default();

          let pg_target_table = crate::export::resolve_grouped_sql_lookup_name(base, &name)
            .or_else(|| table_names.first().map(|(table_name, _)| table_name.clone()));

        rows.push(CollectionRow {
            name,
            stats,
            pg_target_table,
            table_names,
        });
    }

    rows.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(rows)
}

/// Extract the host (and port) portion from a MongoDB URI for display purposes.
/// `mongodb://user:pass@host:port/db?opts` → `host:port`
pub fn cluster_from_uri(uri: &str) -> String {
    let without_scheme = uri.split_once("://").map(|(_, r)| r).unwrap_or(uri);
    let after_creds = without_scheme
        .split_once('@')
        .map(|(_, r)| r)
        .unwrap_or(without_scheme);
    let host = after_creds.split('/').next().unwrap_or(after_creds);
    let host = host.split('?').next().unwrap_or(host);
    host.to_owned()
}

/// Render the per-database HTML report string.
/// `cluster` is the MongoDB host shown in the header (pass an empty string to omit it).
pub fn render_html(rows: &[CollectionRow], namespace: &str, cluster: &str, title: &str) -> String {
    let now = chrono::Utc::now()
        .format("%Y-%m-%d %H:%M:%S UTC")
        .to_string();

    let total_docs: u64 = rows
        .iter()
        .map(|r| match &r.stats.documents_in_collection {
            serde_yaml::Value::Number(n) => n.as_u64().unwrap_or(0),
            _ => 0,
        })
        .sum();

    let has_tables = rows.iter().any(|r| !r.table_names.is_empty());
    let total_pg_tables: usize = rows.iter().map(|r| r.table_names.len()).sum();

    // DB-level migrability aggregates
    // C_db = 1.5 * N + sum(C_i)
    let n = rows.len() as f64;
    let score_sum: f64 = rows.iter().map(|r| r.stats.migrability_score).sum();
    let score_db = (1.5 * n + score_sum) * 100.0 / 100.0;
    let score_max: f64 = rows
        .iter()
        .map(|r| r.stats.migrability_score)
        .fold(0.0_f64, f64::max);
    let total_weighted: f64 = rows
        .iter()
        .map(|r| {
            let docs = match &r.stats.documents_in_collection {
                serde_yaml::Value::Number(n) => n.as_u64().unwrap_or(0) as f64,
                _ => 0.0,
            };
            r.stats.migrability_score * docs
        })
        .sum();
    let score_avg = if total_docs > 0 {
        (total_weighted / total_docs as f64 * 100.0).round() / 100.0
    } else {
        0.0
    };
    let complexity_label = if score_db < 30.0 {
        ("Easy", "#27ae60")
    } else if score_db < 80.0 {
        ("Medium", "#e67e22")
    } else {
        ("Hard", "#c0392b")
    };

    // Number of columns (used for colspan on detail rows)
    let col_count = 9 + if has_tables { 1 } else { 0 };

    let table_rows: String = rows
        .iter()
        .map(|r| {
            let docs_cell = render_documents_cell(&r.stats);

            let score_color = if r.stats.migrability_score < 3.0 {
                "#27ae60"
            } else if r.stats.migrability_score < 8.0 {
                "#e67e22"
            } else {
                "#c0392b"
            };

            let (name_cell, detail_row) = if r.table_names.is_empty() {
              let warning_detail = if r.has_infer_warnings() {
                render_infer_warning_detail(&r.stats.infer_warnings)
              } else {
                String::new()
              };
                // No SQL schema available – plain name, no expand control
                (
                format!(
                  r#"<td class="name"><span class="collection-name {warning_class}">{name}</span>{warning_detail}</td>"#,
                  warning_class = if r.has_infer_warnings() { "has-warning" } else { "" },
                  name = escape_html(&r.name),
                  warning_detail = warning_detail,
                ),
                    String::new(),
                )
            } else {
                let pills: String = r
                    .table_names
                    .iter()
                    .map(|(tname, ddl)| {
                        let safe_id = format!(
                            "ddl-{}-{}",
                            r.name.chars().map(|c| if c.is_alphanumeric() { c } else { '_' }).collect::<String>(),
                            tname.chars().map(|c| if c.is_alphanumeric() { c } else { '_' }).collect::<String>(),
                        );
                        let ddl_esc = ddl
                            .replace('&', "&amp;")
                            .replace('<', "&lt;")
                            .replace('>', "&gt;");
                        format!(
                            r#"<div class="pill-block"><span class="pill clickable-pill" onclick="toggleDDL('{sid}')" title="Click to show DDL"><span class="pill-arrow" id="arr-{sid}">&#9658;</span> {tname}</span><pre class="ddl-block" id="{sid}" style="display:none">{ddl_esc}</pre></div>"#,
                            sid = safe_id,
                            tname = tname,
                            ddl_esc = ddl_esc,
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("");
                let detail_id = format!("detail-{}", r.name);
                let icon_id = format!("icon-{}", r.name);
                let warning_detail = if r.has_infer_warnings() {
                  render_infer_warning_detail(&r.stats.infer_warnings)
                } else {
                  String::new()
                };
                let name_cell = format!(
                    r#"<td class="name expandable" onclick="toggleDetail('{name}')" title="Click to expand PG tables">
                <span class="expand-icon" id="{icon_id}">▶</span> <span class="collection-name {warning_class}">{name}</span>{warning_detail}
            </td>"#,
                    name = r.name,
                    icon_id = icon_id,
                  warning_class = if r.has_infer_warnings() { "has-warning" } else { "" },
                  warning_detail = warning_detail,
                );
                let detail_row = format!(
                    r#"<tr class="detail-row" id="{detail_id}" style="display:none">
          <td colspan="{col_count}" class="detail-cell">
            <div class="table-list">{pills}</div>
          </td>
        </tr>"#,
                    detail_id = detail_id,
                    col_count = col_count,
                    pills = pills,
                );
                (name_cell, detail_row)
            };

            let tables_cell = if has_tables {
                format!(
                    r#"<td class="num">{}</td>"#,
                    r.tables_count()
                        .map_or("-".to_owned(), |n| n.to_string())
                )
            } else {
                String::new()
            };
            let pg_table_cell = format!(
              r#"<td class="pg-table">{}</td>"#,
              r.pg_target_table
                .as_deref()
                .map(escape_html)
                .unwrap_or_else(|| "-".to_owned())
            );

            format!(
                r#"<tr class="collection-row">
          {name_cell}
            {pg_table_cell}
          {docs_cell}
          <td class="num">{sampled}</td>
          <td class="num">{width_top}</td>
          <td class="num">{width_max:.1} <span class="level">(L{width_max_level})</span></td>
          <td class="num">{depth}</td>
          <td class="num" title="{branch_tooltip}">{branch:.1}</td>
          <td class="num"><span class="score-badge" style="color:{score_color};font-weight:700">{score:.2}</span></td>
          {tables_cell}
        </tr>
        {detail_row}"#,
                name_cell = name_cell,
                pg_table_cell = pg_table_cell,
                docs_cell = docs_cell,
                sampled = r.stats.documents_sampled,
                width_top = r.stats.width_top_level,
                width_max = r.stats.width_max,
                width_max_level = r.stats.width_max_level,
                depth = r.stats.depth_max,
                branch = r.stats.branch_total,
                branch_tooltip = {
                    let levels = r.stats.branch_per_level.iter()
                        .map(|(k, v)| format!("{}: {:.2}", k, v))
                        .collect::<Vec<_>>()
                        .join("  ");
                    format!("Expected fields/doc by level — {}", levels)
                },
                score = r.stats.migrability_score,
                score_color = score_color,
                tables_cell = tables_cell,
                detail_row = detail_row,
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>mongo2pg – {namespace} @ {cluster}</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #f5f7fa;
      color: #333;
      margin: 0;
      padding: 2rem;
    }}
    h1 {{ color: #2c3e50; margin-bottom: 0.25rem; }}
    .subtitle {{ color: #7f8c8d; font-size: 0.9rem; margin-bottom: 2rem; }}
    .summary-grid {{
      display: flex;
      gap: 1rem;
      margin-bottom: 2rem;
      flex-wrap: wrap;
    }}
    .card {{
      background: white;
      border-radius: 8px;
      padding: 1rem 1.5rem;
      box-shadow: 0 1px 4px rgba(0,0,0,.08);
      min-width: 160px;
    }}
    .card .label {{ font-size: 0.75rem; text-transform: uppercase; color: #7f8c8d; }}
    .card .value {{ font-size: 1.6rem; font-weight: 700; color: #2c3e50; }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: white;
      border-radius: 8px;
      overflow: hidden;
      box-shadow: 0 1px 4px rgba(0,0,0,.08);
    }}
    thead {{ background: #2c3e50; color: white; }}
    th[title] {{ cursor: help; }}
    th {{
      padding: 0.75rem 1rem;
      text-align: left;
      font-size: 0.8rem;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }}
    th.num, td.num {{ text-align: right; }}
    td {{
      padding: 0.6rem 1rem;
      border-bottom: 1px solid #ecf0f1;
      font-size: 0.9rem;
    }}
    tr:last-child td {{ border-bottom: none; }}
    tr.collection-row:hover td {{ background: #f0f4f8; }}
    td.name {{ font-weight: 600; color: #2c3e50; }}
    .collection-name.has-warning {{ color: #b58900; }}
    td.expandable {{
      cursor: pointer;
      user-select: none;
    }}
    td.expandable:hover {{ color: #2980b9; }}
    .expand-icon {{
      display: inline-block;
      font-size: 0.65rem;
      color: #95a5a6;
      margin-right: 0.3rem;
      transition: transform 0.15s;
    }}
    .expand-icon.open {{ transform: rotate(90deg); }}
    .collection-warning {{ display: inline-block; margin-left: 0.45rem; vertical-align: middle; }}
    .collection-warning-summary {{
      display: inline-block;
      list-style: none;
      cursor: pointer;
      border: 1px solid #f4d03f;
      background: #fcf3cf;
      color: #9a7d0a;
      border-radius: 999px;
      padding: 0.08rem 0.5rem;
      font-size: 0.72rem;
      font-weight: 700;
    }}
    .collection-warning-summary::-webkit-details-marker {{ display: none; }}
    .collection-warning-popover {{
      margin-top: 0.35rem;
      min-width: 320px;
      max-width: 520px;
      background: #fffdf3;
      border: 1px solid #f7dc6f;
      border-radius: 6px;
      box-shadow: 0 2px 6px rgba(0,0,0,.08);
      padding: 0.7rem 0.85rem;
      color: #5d4b00;
    }}
    .collection-warning-title {{ font-size: 0.78rem; font-weight: 700; margin-bottom: 0.45rem; color: #7d6608; }}
    .collection-warning-list {{ margin: 0; padding-left: 1rem; }}
    .collection-warning-list > li {{ margin-bottom: 0.5rem; font-size: 0.78rem; line-height: 1.35; }}
    .collection-warning-list > li:last-child {{ margin-bottom: 0; }}
    .collection-warning-types {{ margin-top: 0.3rem; padding-left: 1rem; }}
    .collection-warning-types li {{ margin-bottom: 0.22rem; font-size: 0.75rem; line-height: 1.35; }}
    .collection-warning-types li:last-child {{ margin-bottom: 0; }}
    .collection-warning-examples {{ color: #7d6608; }}
    .detail-row td.detail-cell {{
      background: #f8fafc;
      padding: 0.5rem 1rem 0.75rem 2.5rem;
      border-top: none;
    }}
    .table-list {{ display: flex; flex-wrap: wrap; gap: 0.4rem; }}
    .pill {{
      display: inline-block;
      background: #eaf4fb;
      color: #2471a3;
      border: 1px solid #aed6f1;
      border-radius: 4px;
      padding: 0.15rem 0.55rem;
      font-size: 0.78rem;
      font-family: monospace;
    }}
    .pill-block {{ display: inline-flex; flex-direction: column; }}
    .clickable-pill {{ cursor: pointer; user-select: none; }}
    .clickable-pill:hover {{ background: #d6eaf8; }}
    .pill-arrow {{
      display: inline-block;
      font-size: 0.6rem;
      color: #5d8aa8;
      margin-right: 0.25rem;
      transition: transform 0.15s;
    }}
    .pill-arrow.open {{ transform: rotate(90deg); }}
    .ddl-block {{
      background: #1e1e2e;
      color: #cdd6f4;
      border: 1px solid #45475a;
      border-radius: 4px;
      padding: 0.6rem 0.8rem;
      font-size: 0.78rem;
      font-family: monospace;
      white-space: pre;
      margin-top: 0.25rem;
      text-align: left;
      max-width: 100%;
      overflow-x: auto;
    }}
    .level {{ color: #95a5a6; font-size: 0.8rem; }}
    .score-badge {{ font-size: 0.95rem; }}
    .complexity-badge {{
      display: inline-block;
      padding: 0.15rem 0.6rem;
      border-radius: 4px;
      font-size: 0.85rem;
      font-weight: 700;
      color: white;
    }}
    footer {{ margin-top: 2rem; font-size: 0.75rem; color: #aaa; }}
    .score-explainer {{ margin-top: 1rem; margin-bottom: 2rem; font-size: 0.82rem; color: #7f8c8d; }}
    .score-explainer code {{ background: #eee; padding: 0.1rem 0.3rem; border-radius: 3px; }}
    .docs-extra {{
      font-size: 0.72rem;
      color: #7f8c8d;
      font-weight: 500;
      margin-top: 0.15rem;
      white-space: nowrap;
    }}
  </style>
</head>
<body>
  <h1>mongo2pg – Migration Report {title}</h1>
  <p class="subtitle">Cluster: <strong>{cluster}</strong> &nbsp;|&nbsp; Database: <strong>{namespace}</strong> &nbsp;|&nbsp; Generated: {now}</p>

  <div class="summary-grid">
    <div class="card"><div class="label">Collections</div><div class="value">{count}</div></div>
    <div class="card"><div class="label">Total Documents</div><div class="value">{total_docs}</div></div>
    {pg_tables_card}
    <div class="card">
      <div class="label">Complexity Score</div>
      <div class="value" style="color:{complexity_color}">{score_db:.1}</div>
    </div>
    <div class="card">
      <div class="label">Complexity</div>
      <div class="value" style="font-size:1.2rem;margin-top:0.3rem">
        <span class="complexity-badge" style="background:{complexity_color}">{complexity_label}</span>
      </div>
    </div>
    <div class="card"><div class="label">Score (avg weighted)</div><div class="value" style="font-size:1.3rem">{score_avg:.2}</div></div>
    <div class="card"><div class="label">Score (max collection)</div><div class="value" style="font-size:1.3rem">{score_max:.2}</div></div>
  </div>

  <p class="score-explainer">
    <strong>Complexity score</strong> per collection:
    <code>C = depth/2 + array_fields + distinct_fields/avg_fields_per_doc</code>.
    DB total: <code>1.5 × collections + Σ C<sub>i</sub></code>.
    Thresholds: &lt;30 Easy · 30–80 Medium · &gt;80 Hard.
    Effective: {count} collections, Σ C<sub>i</sub> = {score_sum:.2} &nbsp;→&nbsp;
    1.5 × {count} + {score_sum:.2} = <strong>{score_db:.2}</strong>.<br>
    <strong>Width (top)</strong>: number of top-level fields in the collection schema.<br>
    <strong>Width (max)</strong>: highest field count found at any single nesting level, with the level shown in parentheses (probability-weighted).<br>
    <strong>Depth (max)</strong>: maximum nesting depth — top-level fields are depth 1, their sub-fields depth 2, etc.<br>
    <strong>Fields (total)</strong>: expected number of fields a typical document in the collection
    actually has, summed across all nesting levels (probability-weighted). Hover the value for the per-level breakdown.
  </p>

  <table>
    <thead>
      <tr>
        <th>Collection</th>
        <th>PG Table</th>
        <th class="num">Documents</th>
        <th class="num">Sampled</th>
        <th class="num" title="Number of top-level fields in the collection schema">Width (top)</th>
        <th class="num" title="Highest field count found at any single nesting level, with the level shown in parentheses (probability-weighted)">Width (max)</th>
        <th class="num" title="Maximum nesting depth: top-level fields are depth 1, their sub-fields depth 2, etc.">Depth (max)</th>
        <th class="num" title="Expected number of fields a typical document has, summed across all levels (probability-weighted). Hover a value for the per-level breakdown.">Fields (total)</th>
        <th class="num">Score</th>
        {tables_header}
      </tr>
    </thead>
    <tbody>
      {table_rows}
    </tbody>
  </table>

  <footer>Generated by <a href="https://github.com/pmpetit/mongo2pg">mongo2pg</a></footer>

  <script>
    function toggleDetail(name) {{
      var row  = document.getElementById('detail-' + name);
      var icon = document.getElementById('icon-'   + name);
      if (!row) return;
      var open = row.style.display !== 'none';
      row.style.display = open ? 'none' : '';
      if (open) {{ icon.classList.remove('open'); }}
      else      {{ icon.classList.add('open');    }}
    }}
    function toggleDDL(id) {{
      var block = document.getElementById(id);
      var arr   = document.getElementById('arr-' + id);
      if (!block) return;
      var open = block.style.display !== 'none';
      block.style.display = open ? 'none' : '';
      if (open) {{ arr.classList.remove('open'); }}
      else      {{ arr.classList.add('open');    }}
    }}
  </script>
</body>
</html>
"#,
        namespace = namespace,
        title = title,
        cluster = cluster,
        now = now,
        count = rows.len(),
        total_docs = total_docs,
        score_db = score_db,
        score_sum = score_sum,
        score_avg = score_avg,
        score_max = score_max,
        complexity_label = complexity_label.0,
        complexity_color = complexity_label.1,
        pg_tables_card = if has_tables {
            format!(
                r#"<div class="card"><div class="label">PG Tables</div><div class="value">{total_pg_tables}</div></div>"#,
                total_pg_tables = total_pg_tables,
            )
        } else {
            String::new()
        },
        table_rows = table_rows,
        tables_header = if has_tables {
            r#"<th class="num">PG Tables</th>"#
        } else {
            ""
        },
    )
}

fn render_infer_warning_detail(warnings: &[InferWarningYaml]) -> String {
    let items = warnings
    .iter()
    .map(|warning| {
      let type_items = if warning.observed_types.is_empty() {
        String::new()
      } else {
        let rendered_types = warning
          .observed_types
          .iter()
          .map(|observed_type| {
            let examples = if observed_type.examples.is_empty() {
              "no sampled values".to_owned()
            } else {
              observed_type
                .examples
                .iter()
                .map(|example| escape_html(example))
                .collect::<Vec<_>>()
                .join(", ")
            };
            format!(
              r#"<li><strong>{type_name}</strong> ({ratio:.1}%): <span class="collection-warning-examples">{examples}</span></li>"#,
              type_name = escape_html(&observed_type.type_name),
              ratio = observed_type.ratio * 100.0,
              examples = examples,
            )
          })
          .collect::<Vec<_>>()
          .join("");
        format!(r#"<ul class="collection-warning-types">{}</ul>"#, rendered_types)
      };

      if warning.kind == "pg_keyword" {
        return format!(
          r#"<li><strong>{field}</strong>: PostgreSQL keyword <span class="collection-warning-examples">{keyword}</span>; it will be renamed to <span class="collection-warning-examples">{renamed_to}</span>.{type_items}</li>"#,
          field = escape_html(&warning.field_path),
          keyword = escape_html(warning.keyword.as_deref().unwrap_or("")),
          renamed_to = escape_html(warning.renamed_to.as_deref().unwrap_or("")),
          type_items = type_items,
        );
      }
      if warning.kind == "type_name" {
        return format!(
          r#"<li><strong>{field}</strong>: field name matches type name <span class="collection-warning-examples">{keyword}</span>; consider renaming it.{type_items}</li>"#,
          field = escape_html(&warning.field_path),
          keyword = escape_html(warning.keyword.as_deref().unwrap_or("")),
          type_items = type_items,
        );
      }
      if warning.kind == "nullable_scalar" {
        return format!(
          r#"<li><strong>{field}</strong>: {type_name} field can be null/undefined; consider normalizing to a default value.</li>"#,
          field = escape_html(&warning.field_path),
          type_name = escape_html(&warning.dominant_family),
        );
      }
      let minority = warning
        .minority_families
        .iter()
        .map(|minority| {
          format!(
            "{} ({:.1}%)",
            escape_html(&minority.family),
            minority.ratio * 100.0
          )
        })
        .collect::<Vec<_>>()
        .join(", ");
      format!(
        r#"<li><strong>{field}</strong>: dominant {dominant} ({dominant_ratio:.1}%), minority {minority}{type_items}</li>"#,
        field = escape_html(&warning.field_path),
        dominant = escape_html(&warning.dominant_family),
        dominant_ratio = warning.dominant_ratio * 100.0,
        minority = minority,
        type_items = type_items,
      )
    })
    .collect::<Vec<_>>()
    .join("");

    format!(
        r#"<details class="collection-warning" onclick="event.stopPropagation()"><summary class="collection-warning-summary">warning</summary><div class="collection-warning-popover"><div class="collection-warning-title">Infer warnings</div><ul class="collection-warning-list">{items}</ul></div></details>"#,
        items = items,
    )
}

pub fn render_post_import_html(
    rows: &[PostImportCollectionRow],
    namespace: &str,
    mongo_cluster: &str,
    pg_target: &str,
) -> String {
    fn render_count_diff_detail(rows: &[PostImportCountDiffRow], delta: i64) -> String {
        if rows.is_empty() {
            return String::new();
        }

        let mismatch_rows = rows
            .iter()
            .map(|mismatch| {
                let mongo_values = mismatch
                    .mongo_values
                    .as_ref()
                    .map(|values| escape_html(&format!("[{}]", values.join(", "))))
                    .unwrap_or_else(|| "missing row".to_owned());
                let pg_values = mismatch
                    .pg_values
                    .as_ref()
                    .map(|values| escape_html(&format!("[{}]", values.join(", "))))
                    .unwrap_or_else(|| "missing row".to_owned());
                format!(
                    r#"<tr><td>{}</td><td><code>{}</code></td><td><code>{}</code></td></tr>"#,
                    mismatch.row_index, mongo_values, pg_values,
                )
            })
            .collect::<Vec<_>>()
            .join("");

        format!(
            r#"<details class="count-detail"><summary class="count-summary match-badge is-mismatch">delta {:+}</summary><div class="count-popover"><div class="count-mismatch-label">First 5 differences by mapped row values</div><table class="count-mismatch-table"><thead><tr><th>Row</th><th>MongoDB</th><th>PostgreSQL</th></tr></thead><tbody>{}</tbody></table></div></details>"#,
            delta, mismatch_rows,
        )
    }

    fn render_md5_detail(md5_summary: &PostImportMd5Summary, detail_id: &str) -> String {
        let summary_label = if md5_summary.mongo_md5 == md5_summary.pg_md5 {
            escape_html(&md5_summary.mongo_md5)
        } else {
            format!(
                "{} != {}",
                escape_html(&md5_summary.mongo_md5),
                escape_html(&md5_summary.pg_md5)
            )
        };
        let columns = md5_summary
      .columns
      .iter()
      .map(|column| {
        let source_label = column
          .source_type
          .as_deref()
          .filter(|value| !value.trim().is_empty())
          .map(|value| format!("mongodb ({})", escape_html(value)))
          .unwrap_or_else(|| "mongodb".to_owned());
        let target_label = column
          .target_type
          .as_deref()
          .filter(|value| !value.trim().is_empty())
          .map(|value| format!("pg ({})", escape_html(value)))
          .unwrap_or_else(|| "pg".to_owned());
        format!(
          r#"<li><span class="md5-source-label">{}</span>: <span class="md5-source">{}</span><span class="md5-arrow"> -> </span><span class="md5-target-label">{}</span>: <span class="md5-target">{}</span></li>"#,
          source_label,
          escape_html(&column.source_field),
          target_label,
          escape_html(&column.target_field),
        )
      })
      .collect::<Vec<_>>()
      .join("");
        let mismatch_rows = md5_summary
            .mismatches
            .iter()
            .map(|mismatch| {
                let mongo_values = mismatch
                    .mongo_values
                    .as_ref()
                    .map(|values| escape_html(&format!("[{}]", values.join(", "))))
                    .unwrap_or_else(|| "missing row".to_owned());
                let pg_values = mismatch
                    .pg_values
                    .as_ref()
                    .map(|values| escape_html(&format!("[{}]", values.join(", "))))
                    .unwrap_or_else(|| "missing row".to_owned());
                format!(
                    r#"<tr><td>{}</td><td><code>{}</code></td><td><code>{}</code></td></tr>"#,
                    mismatch.row_index, mongo_values, pg_values,
                )
            })
            .collect::<Vec<_>>()
            .join("");
        let mismatch_detail = if md5_summary.mismatches.is_empty() {
          String::new()
        } else {
          format!(
            r#"<button type="button" class="md5-open-window" onclick="openMd5DiffWindow('{detail_id}')">Open diff in new page</button><template id="{detail_id}"><!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>mongo2pg md5 mismatch</title><style>body{{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;margin:0;padding:1.25rem;background:#f8fafc;color:#1f2937}}h1{{margin:.2rem 0 .5rem;font-size:1.1rem}}.meta{{margin:0 0 1rem;font-size:.9rem;color:#475569}}table{{width:100%;border-collapse:collapse;background:white}}th,td{{border:1px solid #cbd5e1;padding:.45rem .5rem;vertical-align:top;text-align:left;font-size:.85rem;line-height:1.35}}th{{background:#e2e8f0;color:#0f172a}}code{{white-space:pre-wrap;word-break:break-word}}</style></head><body><h1>First 5 non-corresponding rows</h1><p class="meta"><strong>MongoDB:</strong> {mongo_md5}<br><strong>PostgreSQL:</strong> {pg_md5}</p><table><thead><tr><th>Row</th><th>MongoDB</th><th>PostgreSQL</th></tr></thead><tbody>{mismatch_rows}</tbody></table></body></html></template>"#,
            detail_id = detail_id,
            mongo_md5 = escape_html(&md5_summary.mongo_md5),
            pg_md5 = escape_html(&md5_summary.pg_md5),
            mismatch_rows = mismatch_rows,
          )
        };
        let state_class = if md5_summary.mongo_md5 == md5_summary.pg_md5 {
            "is-match"
        } else {
            "is-mismatch"
        };

        format!(
            r#"<details class="md5-detail {state_class}"><summary class="md5-summary">{summary_label}</summary><div class="md5-popover"><div><strong>MongoDB</strong>: <span class="md5-full">{mongo_md5}</span></div><div><strong>PostgreSQL</strong>: <span class="md5-full">{pg_md5}</span></div><div class="md5-columns-label">Columns involved</div><ul class="md5-columns">{columns}</ul>{mismatch_detail}</div></details>"#,
            state_class = state_class,
            summary_label = summary_label,
            mongo_md5 = escape_html(&md5_summary.mongo_md5),
            pg_md5 = escape_html(&md5_summary.pg_md5),
            columns = columns,
            mismatch_detail = mismatch_detail,
        )
    }

    fn sum_mongo_rows(node: &PostImportNode) -> u64 {
        node.mongo_count + node.children.iter().map(sum_mongo_rows).sum::<u64>()
    }

    fn count_pg_tables(node: &PostImportNode) -> usize {
        usize::from(node.pg_table_name.is_some())
            + node.children.iter().map(count_pg_tables).sum::<usize>()
    }

    fn sum_pg_rows(node: &PostImportNode) -> i64 {
        node.pg_row_count.unwrap_or(0) + node.children.iter().map(sum_pg_rows).sum::<i64>()
    }

    fn render_node(node: &PostImportNode, depth: usize, prefix: &str, index: &mut usize) -> String {
        let node_id = format!("{prefix}-{}", *index);
        *index += 1;
        let has_children = !node.children.is_empty();
        let toggle = if has_children {
            format!(
                r#"<button class="tree-toggle" type="button" onclick="toggleNode('{node_id}')"><span class="tree-arrow open" id="arrow-{node_id}">▶</span></button>"#,
                node_id = node_id,
            )
        } else {
            "<span class=\"tree-spacer\"></span>".to_owned()
        };
        let mongo_shape = if node.is_array { "[ ]" } else { "{ }" };
        let mismatch = node
            .pg_row_count
            .map(|pg_rows| pg_rows - node.mongo_count as i64);
        let md5_detail_id = format!("md5-diff-{node_id}");
        let md5_detail = node
            .md5_summary
            .as_ref()
          .map(|summary| render_md5_detail(summary, &md5_detail_id))
            .unwrap_or_default();
        let pg_cell = match (&node.pg_table_name, node.pg_row_count) {
            (Some(table_name), Some(row_count)) => format!(
                r#"<div class="pg-ref"><span class="pg-table">{}</span><span class="pg-count">{}</span>{}{}{}</div>"#,
                escape_html(table_name),
                row_count,
                match mismatch {
                    Some(0) => "<span class=\"match-badge is-match\">match</span>".to_owned(),
                    Some(delta) if !node.count_diff_rows.is_empty() => {
                        render_count_diff_detail(&node.count_diff_rows, delta)
                    }
                    Some(delta) => {
                        format!("<span class=\"match-badge is-mismatch\">delta {delta:+}</span>")
                    }
                    None => String::new(),
                },
                if md5_detail.is_empty() {
                    ""
                } else {
                    "<span class=\"md5-label\">md5</span>"
                },
                md5_detail,
            ),
            (Some(table_name), None) => format!(
                r#"<div class="pg-ref"><span class="pg-table">{}</span>{}</div>"#,
                escape_html(table_name),
                md5_detail,
            ),
            _ => "<div class=\"pg-empty\">-</div>".to_owned(),
        };
        let children = if has_children {
            let rendered = node
                .children
                .iter()
                .map(|child| render_node(child, depth + 1, prefix, index))
                .collect::<Vec<_>>()
                .join("");
            format!(
                r#"<div class="tree-children" id="children-{node_id}">{rendered}</div>"#,
                node_id = node_id,
                rendered = rendered,
            )
        } else {
            String::new()
        };

        format!(
            r#"<div class="tree-node" style="--depth:{depth}">
  <div class="tree-row">
  <div class="mongo-cell">
    {toggle}
    <span class="json-key">{label}</span>
    <span class="json-shape">{mongo_shape}</span>
    <span class="mongo-count">{mongo_count}</span>
  </div>
  <div class="pg-cell">{pg_cell}</div>
  </div>
  {children}
</div>"#,
            depth = depth,
            toggle = toggle,
            label = escape_html(&node.name),
            mongo_shape = mongo_shape,
            mongo_count = node.mongo_count,
            pg_cell = pg_cell,
            children = children,
        )
    }

    let now = chrono::Utc::now()
        .format("%Y-%m-%d %H:%M:%S UTC")
        .to_string();

    let total_docs: u64 = rows.iter().map(|r| r.document_count).sum();
    let total_mongo_rows: u64 = rows.iter().map(|r| sum_mongo_rows(&r.root)).sum();
    let total_tables: usize = rows.iter().map(|r| count_pg_tables(&r.root)).sum();
    let total_pg_rows: i64 = rows.iter().map(|r| sum_pg_rows(&r.root)).sum();

    let collection_sections = rows
        .iter()
        .map(|row| {
      let safe_name = row
        .name
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect::<String>();
      let mut index = 0;
      let tree = render_node(&row.root, 0, &safe_name, &mut index);
      let mongo_total = sum_mongo_rows(&row.root);
      let table_total = sum_pg_rows(&row.root);
      let table_count = count_pg_tables(&row.root);
      format!(
        r#"<section class="collection-card">
  <div class="collection-summary">
  <div>
    <h2>{label}</h2>
    <p class="collection-meta">MongoDB documents: <strong>{document_count}</strong> &nbsp;|&nbsp; MongoDB expanded rows: <strong>{mongo_total}</strong> &nbsp;|&nbsp; PostgreSQL tables: <strong>{table_count}</strong> &nbsp;|&nbsp; PostgreSQL rows: <strong>{table_total}</strong></p>
  </div>
  </div>
  <div class="compare-head">
  <div>MongoDB JSON</div>
  <div>PostgreSQL</div>
  </div>
  <div class="tree-panel">{tree}</div>
</section>"#,
        label = escape_html(&row.name),
        document_count = row.document_count,
        mongo_total = mongo_total,
        table_count = table_count,
        table_total = table_total,
        tree = tree,
      )
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>mongo2pg – Post-Import Report</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #f5f7fa;
      color: #333;
      margin: 0;
      padding: 2rem;
    }}
    h1 {{ color: #2c3e50; margin-bottom: 0.25rem; }}
    .subtitle {{ color: #7f8c8d; font-size: 0.9rem; margin-bottom: 2rem; }}
    .summary-grid {{ display: flex; gap: 1rem; margin-bottom: 2rem; flex-wrap: wrap; }}
    .card {{
      background: white;
      border-radius: 8px;
      padding: 1rem 1.5rem;
      box-shadow: 0 1px 4px rgba(0,0,0,.08);
      min-width: 180px;
    }}
    .card .label {{ font-size: 0.75rem; text-transform: uppercase; color: #7f8c8d; }}
    .card .value {{ font-size: 1.6rem; font-weight: 700; color: #2c3e50; }}
    .collection-card {{
      background: white;
      border-radius: 12px;
      box-shadow: 0 1px 4px rgba(0,0,0,.08);
      margin-bottom: 1.5rem;
      overflow: hidden;
    }}
    .collection-summary {{ padding: 1rem 1.25rem 0.6rem; }}
    .collection-summary h2 {{ margin: 0 0 0.25rem; color: #2c3e50; font-size: 1.15rem; }}
    .collection-meta {{ margin: 0; color: #6b7c93; font-size: 0.92rem; }}
    .compare-head {{
      display: grid;
      grid-template-columns: 1.4fr 0.8fr;
      gap: 1rem;
      padding: 0.8rem 1.25rem;
      background: #2c3e50;
      color: white;
      font-size: 0.78rem;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      font-weight: 700;
    }}
    .tree-panel {{ padding: 0.35rem 0 0.7rem; }}
    .tree-node {{ --indent-step: 1.3rem; }}
    .tree-row {{
      display: grid;
      grid-template-columns: 1.4fr 0.8fr;
      gap: 1rem;
      align-items: center;
      padding: 0.15rem 1.25rem;
    }}
    .tree-row:hover {{ background: #f8fafc; }}
    .mongo-cell {{
      display: flex;
      align-items: center;
      gap: 0.45rem;
      padding-left: calc(var(--depth) * var(--indent-step));
      min-height: 2rem;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    }}
    .tree-toggle {{
      border: none;
      background: transparent;
      padding: 0;
      width: 1rem;
      cursor: pointer;
      color: #5d6d7e;
    }}
    .tree-spacer {{ display: inline-block; width: 1rem; }}
    .tree-arrow {{ display: inline-block; font-size: 0.7rem; transition: transform 0.15s; }}
    .tree-arrow.open {{ transform: rotate(90deg); }}
    .json-key {{ color: #1f3a5f; font-weight: 600; }}
    .json-shape {{ color: #7f8c8d; }}
    .mongo-count, .pg-count {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 3rem;
      padding: 0.15rem 0.45rem;
      border-radius: 999px;
      background: #edf2f7;
      color: #334e68;
      font-size: 0.78rem;
      font-weight: 700;
    }}
    .pg-cell {{ display: flex; align-items: center; min-height: 2rem; }}
    .pg-ref {{ display: flex; align-items: center; gap: 0.6rem; flex-wrap: wrap; }}
    .pg-table {{
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      color: #2471a3;
      background: #eaf4fb;
      border: 1px solid #aed6f1;
      border-radius: 6px;
      padding: 0.15rem 0.45rem;
    }}
    .match-badge {{
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 0.12rem 0.45rem;
      font-size: 0.74rem;
      font-weight: 700;
    }}
    .match-badge.is-match {{ background: #e8f7ef; color: #1e8449; }}
    .match-badge.is-mismatch {{ background: #fdecea; color: #c0392b; }}
    .count-detail {{ display: inline-block; }}
    .count-summary {{
      cursor: pointer;
      list-style: none;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      word-break: break-all;
    }}
    .count-summary::-webkit-details-marker {{ display: none; }}
    .count-popover {{
      margin-top: 0.35rem;
      padding: 0.6rem 0.75rem;
      border-radius: 8px;
      border: 1px solid #a7f3d0;
      background: white;
      box-shadow: 0 8px 24px rgba(15, 23, 42, 0.12);
      min-width: 22rem;
      max-width: 36rem;
      font-size: 0.8rem;
      color: #134e4a;
    }}
    .count-mismatch-label {{ margin-top: 0.45rem; font-weight: 700; color: #115e59; }}
    .count-mismatch-table {{ width: 100%; border-collapse: collapse; margin-top: 0.5rem; }}
    .count-mismatch-table th,
    .count-mismatch-table td {{
      border: 1px solid #ccfbf1;
      padding: 0.3rem 0.4rem;
      text-align: left;
      vertical-align: top;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 0.72rem;
    }}
    .count-mismatch-table th {{ background: #f0fdfa; color: #134e4a; font-weight: 700; }}
    .md5-label {{ color: #6b7c93; font-size: 0.74rem; font-weight: 700; text-transform: uppercase; }}
    .md5-detail {{ display: inline-block; }}
    .md5-summary {{
      cursor: pointer;
      list-style: none;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 0.74rem;
      border-radius: 6px;
      padding: 0.15rem 0.45rem;
      background: #f4f7fb;
      border: 1px solid #d6e2ee;
      color: #334e68;
      word-break: break-all;
    }}
    .md5-detail.is-match .md5-summary {{ border-color: #b7e4c7; background: #ecfdf3; color: #1e8449; }}
    .md5-detail.is-mismatch .md5-summary {{ border-color: #f5c2c0; background: #fff3f2; color: #c0392b; }}
    .md5-summary::-webkit-details-marker {{ display: none; }}
    .md5-popover {{
      margin-top: 0.35rem;
      padding: 0.6rem 0.75rem;
      border-radius: 8px;
      border: 1px solid #d6e2ee;
      background: white;
      box-shadow: 0 8px 24px rgba(15, 23, 42, 0.12);
      min-width: 22rem;
      max-width: 36rem;
      font-size: 0.8rem;
      color: #334e68;
    }}
    .md5-full {{ font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; word-break: break-all; }}
    .md5-columns-label {{ margin-top: 0.45rem; font-weight: 700; color: #1f3a5f; }}
    .md5-columns {{ margin: 0.35rem 0 0; padding-left: 1.2rem; }}
    .md5-columns li {{ margin: 0.15rem 0; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }}
    .md5-source-label {{ color: #5b21b6; font-weight: 700; }}
    .md5-target-label {{ color: #1d4ed8; font-weight: 700; }}
    .md5-source {{ color: #7c3aed; }}
    .md5-target {{ color: #2471a3; }}
    .md5-arrow {{ color: #7f8c8d; }}
    .md5-open-window {{
      margin-top: 0.5rem;
      border: 1px solid #93c5fd;
      border-radius: 6px;
      background: #eff6ff;
      color: #1d4ed8;
      font-size: 0.75rem;
      font-weight: 700;
      padding: 0.22rem 0.55rem;
      cursor: pointer;
    }}
    .pg-empty {{ color: #95a5a6; font-style: italic; }}
    .tree-children {{ display: block; }}
    footer {{ margin-top: 2rem; font-size: 0.75rem; color: #aaa; }}
  </style>
</head>
<body>
  <h1>mongo2pg – Post-Import Report</h1>
  <p class="subtitle">MongoDB: <strong>{mongo_cluster}</strong> &nbsp;|&nbsp; PostgreSQL: <strong>{pg_target}</strong> &nbsp;|&nbsp; Namespace: <strong>{namespace}</strong> &nbsp;|&nbsp; Generated: {now}</p>

  <div class="summary-grid">
    <div class="card"><div class="label">Collections</div><div class="value">{collection_count}</div></div>
    <div class="card"><div class="label">MongoDB Documents</div><div class="value">{total_docs}</div></div>
    <div class="card"><div class="label">MongoDB Expanded Rows</div><div class="value">{total_mongo_rows}</div></div>
    <div class="card"><div class="label">PostgreSQL Tables</div><div class="value">{total_tables}</div></div>
    <div class="card"><div class="label">PostgreSQL Rows</div><div class="value">{total_pg_rows}</div></div>
  </div>

  {collection_sections}

  <footer>Generated by <a href="https://github.com/pmpetit/mongo2pg">mongo2pg</a></footer>

  <script>
    function toggleNode(id) {{
      var row = document.getElementById('children-' + id);
      var icon = document.getElementById('arrow-' + id);
      if (!row) return;
      var open = row.style.display !== 'none';
      row.style.display = open ? 'none' : '';
      if (open) {{ icon.classList.remove('open'); }}
      else      {{ icon.classList.add('open'); }}
    }}

    function openMd5DiffWindow(templateId) {{
      var tpl = document.getElementById(templateId);
      if (!tpl) return;
      var popup = window.open('', '_blank');
      if (!popup) return;
      popup.document.open();
      popup.document.write(tpl.innerHTML);
      popup.document.close();
    }}
  </script>
</body>
</html>
"#,
        mongo_cluster = escape_html(mongo_cluster),
        pg_target = escape_html(pg_target),
        namespace = escape_html(namespace),
        now = now,
        collection_count = rows.len(),
        total_docs = total_docs,
        total_mongo_rows = total_mongo_rows,
        total_tables = total_tables,
        total_pg_rows = total_pg_rows,
        collection_sections = collection_sections,
    )
}

/// Render a cluster-level HTML report from a slice of [`DatabaseScore`]s.
///
/// The report shows one row per database and summary cards for the cluster score,
/// doc-weighted average, and the worst-database score.
/// `cluster` is the MongoDB host shown in the header.
pub fn render_cluster_html(dbs: &[DatabaseScore], cluster: &str) -> String {
    let now = chrono::Utc::now()
        .format("%Y-%m-%d %H:%M:%S UTC")
        .to_string();

    let cs = compute_cluster_score(dbs);

    let total_docs: u64 = dbs.iter().map(|db| db.total_docs).sum();
    let total_collections: usize = dbs.iter().map(|db| db.collection_count).sum();
    let score_db_sum: f64 = (dbs.iter().map(|db| db.score_db).sum::<f64>() * 100.0).round() / 100.0;

    // Use score_total / D against the same fixed 30/80 thresholds as the DB level,
    // so cluster and database complexity labels are on the same scale.
    let d = dbs.len() as f64;
    let score_per_db = if d > 0.0 { cs.score_total / d } else { 0.0 };
    let complexity_label = if score_per_db < 30.0 {
        ("Easy", "#27ae60")
    } else if score_per_db < 80.0 {
        ("Medium", "#e67e22")
    } else {
        ("Hard", "#c0392b")
    };

    let table_rows: String = dbs
        .iter()
        .map(|db| {
            let score_color = if db.score_db < 30.0 {
                "#27ae60"
            } else if db.score_db < 80.0 {
                "#e67e22"
            } else {
                "#c0392b"
            };
            format!(
                r#"<tr class="collection-row">
          <td class="name">{name}</td>
          <td class="num">{collections}</td>
          <td class="num">{total_docs}</td>
          <td class="num"><span class="score-badge" style="color:{score_color};font-weight:700">{score_db:.2}</span></td>
          <td class="num">{score_avg:.2}</td>
          <td class="num">{score_max:.2}</td>
        </tr>"#,
                name = db.name,
                collections = db.collection_count,
                total_docs = db.total_docs,
                score_db = db.score_db,
                score_avg = db.score_avg,
                score_max = db.score_max,
                score_color = score_color,
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>mongo2pg – Cluster Report @ {cluster}</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #f5f7fa;
      color: #333;
      margin: 0;
      padding: 2rem;
    }}
    h1 {{ color: #2c3e50; margin-bottom: 0.25rem; }}
    .subtitle {{ color: #7f8c8d; font-size: 0.9rem; margin-bottom: 2rem; }}
    .summary-grid {{
      display: flex;
      gap: 1rem;
      margin-bottom: 2rem;
      flex-wrap: wrap;
    }}
    .card {{
      background: white;
      border-radius: 8px;
      padding: 1rem 1.5rem;
      box-shadow: 0 1px 4px rgba(0,0,0,.08);
      min-width: 160px;
    }}
    .card .label {{ font-size: 0.75rem; text-transform: uppercase; color: #7f8c8d; }}
    .card .value {{ font-size: 1.6rem; font-weight: 700; color: #2c3e50; }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: white;
      border-radius: 8px;
      overflow: hidden;
      box-shadow: 0 1px 4px rgba(0,0,0,.08);
    }}
    thead {{ background: #2c3e50; color: white; }}
    th {{
      padding: 0.75rem 1rem;
      text-align: left;
      font-size: 0.8rem;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }}
    th.num, td.num {{ text-align: right; }}
    td {{
      padding: 0.6rem 1rem;
      border-bottom: 1px solid #ecf0f1;
      font-size: 0.9rem;
    }}
    tr:last-child td {{ border-bottom: none; }}
    tr.collection-row:hover td {{ background: #f0f4f8; }}
    td.name {{ font-weight: 600; color: #2c3e50; }}
    .score-badge {{ font-size: 0.95rem; }}
    .complexity-badge {{
      display: inline-block;
      padding: 0.15rem 0.6rem;
      border-radius: 4px;
      font-size: 0.85rem;
      font-weight: 700;
      color: white;
    }}
    footer {{ margin-top: 2rem; font-size: 0.75rem; color: #aaa; }}
    .score-explainer {{ margin-top: 1rem; margin-bottom: 2rem; font-size: 0.82rem; color: #7f8c8d; }}
    .score-explainer code {{ background: #eee; padding: 0.1rem 0.3rem; border-radius: 3px; }}
  </style>
</head>
<body>
  <h1>mongo2pg – Cluster Report</h1>
  <p class="subtitle">Cluster: <strong>{cluster}</strong> &nbsp;|&nbsp; Generated: {now}</p>

  <div class="summary-grid">
    <div class="card"><div class="label">Databases</div><div class="value">{db_count}</div></div>
    <div class="card"><div class="label">Collections</div><div class="value">{total_collections}</div></div>
    <div class="card"><div class="label">Total Documents</div><div class="value">{total_docs}</div></div>
    <div class="card">
      <div class="label">Cluster Score</div>
      <div class="value" style="color:{complexity_color}">{score_total:.1}</div>
    </div>
    <div class="card">
      <div class="label">Complexity</div>
      <div class="value" style="font-size:1.2rem;margin-top:0.3rem">
        <span class="complexity-badge" style="background:{complexity_color}">{complexity_label}</span>
      </div>
    </div>
    <div class="card"><div class="label">Score (avg weighted)</div><div class="value" style="font-size:1.3rem">{score_avg:.2}</div></div>
    <div class="card"><div class="label">Score (max database)</div><div class="value" style="font-size:1.3rem">{score_max:.2}</div></div>
  </div>

  <p class="score-explainer">
    <strong>DB complexity score</strong>: <code>1.5 × collections + Σ C<sub>i</sub></code>
    where <code>C<sub>i</sub> = depth/2 + array_fields + distinct_fields/avg_fields_per_doc</code>.<br>
    <strong>Cluster score</strong>: <code>1.5 × databases + Σ score_db<sub>j</sub></code><br>
    &nbsp;&nbsp;&nbsp;= <code>1.5 × {db_count} + {score_db_sum:.2}</code> = <strong>{score_total:.2}</strong>.<br>
    Thresholds (per database): &lt;30 Easy · 30–80 Medium · &gt;80 Hard (scaled by database count for the cluster).
  </p>

  <table>
    <thead>
      <tr>
        <th>Database</th>
        <th class="num">Collections</th>
        <th class="num">Documents</th>
        <th class="num" title="1.5 × collections + Σ collection scores">Score (total)</th>
        <th class="num" title="Document-count-weighted average of per-collection scores">Score (avg weighted)</th>
        <th class="num" title="Highest per-collection score in this database">Score (max collection)</th>
      </tr>
    </thead>
    <tbody>
      {table_rows}
    </tbody>
  </table>

  <footer>Generated by <a href="https://github.com/pmpetit/mongo2pg">mongo2pg</a></footer>
</body>
</html>
"#,
        cluster = cluster,
        now = now,
        db_count = cs.db_count,
        total_collections = total_collections,
        total_docs = total_docs,
        score_total = cs.score_total,
        score_avg = cs.score_avg,
        score_max = cs.score_max,
        complexity_label = complexity_label.0,
        complexity_color = complexity_label.1,
        table_rows = table_rows,
        score_db_sum = score_db_sum,
    )
}

#[cfg(test)]
mod tests {
    use super::{
        render_html, render_post_import_html, CollectionRow, CollectionStatsYaml,
        PostImportCollectionRow, PostImportCountDiffRow, PostImportMd5Column,
        PostImportMd5MismatchRow, PostImportMd5Summary, PostImportNode,
    };
    use crate::stats::{InferWarningMinorityYaml, InferWarningTypeYaml, InferWarningYaml};

    #[test]
    fn render_html_highlights_collections_with_infer_warnings() {
        let html = render_html(
            &[CollectionRow {
                name: "advisors".to_owned(),
                stats: CollectionStatsYaml {
                    documents_in_collection: serde_yaml::Value::Number(2937_u64.into()),
                    documents_sampled: 2937,
                    width_top_level: 7,
                    width_max: 12.0,
                    width_max_level: 3,
                    depth_max: 4,
                    branch_total: 18.0,
                    branch_per_level: indexmap::IndexMap::from([
                        ("L1".to_owned(), 3.0),
                        ("L2".to_owned(), 6.0),
                    ]),
                    array_field_count: 1,
                    avg_fields_per_doc: 4.2,
                    migrability_score: 6.5,
                    infer_warnings: vec![InferWarningYaml {
                        kind: "mixed_scalar_types".to_owned(),
                        field_path: "advices[].earnings.monthly_gain".to_owned(),
                        renamed_to: None,
                        keyword: None,
                        dominant_family: "numeric".to_owned(),
                        dominant_ratio: 0.95,
                        minority_families: vec![InferWarningMinorityYaml {
                            family: "string".to_owned(),
                            ratio: 0.05,
                        }],
                        observed_types: vec![
                            InferWarningTypeYaml {
                                type_name: "Double".to_owned(),
                                ratio: 0.80,
                                examples: vec!["12.5".to_owned(), "51.4".to_owned()],
                            },
                            InferWarningTypeYaml {
                                type_name: "String".to_owned(),
                                ratio: 0.05,
                                examples: vec!["\"N/A\"".to_owned()],
                            },
                        ],
                    }],
                      read_ops: None,
                },
                pg_target_table: Some("advisors".to_owned()),
                table_names: Vec::new(),
            }],
            "dbapi",
            "cluster0",
            "Test Infer Warning Highlighting Details",
        );

        assert!(html.contains("collection-name has-warning"));
        assert!(html.contains("Infer warnings"));
        assert!(html.contains("advices[].earnings.monthly_gain"));
        assert!(html.contains("minority string (5.0%)"));
        assert!(html.contains("Double"));
        assert!(html.contains("12.5, 51.4"));
        assert!(html.contains("\"N/A\""));
    }

    #[test]
    fn render_html_shows_pg_keyword_warning_details() {
        let html = render_html(
            &[CollectionRow {
                name: "scheduling_executions".to_owned(),
                stats: CollectionStatsYaml {
                    documents_in_collection: serde_yaml::Value::Number(106_u64.into()),
                    documents_sampled: 106,
                    width_top_level: 7,
                    width_max: 7.0,
                    width_max_level: 1,
                    depth_max: 1,
                    branch_total: 7.0,
                    branch_per_level: indexmap::IndexMap::from([("L1".to_owned(), 7.0)]),
                    array_field_count: 0,
                    avg_fields_per_doc: 7.0,
                    migrability_score: 2.0,
                    infer_warnings: vec![InferWarningYaml {
                        kind: "pg_keyword".to_owned(),
                        field_path: "timestamp".to_owned(),
                        renamed_to: Some("_timestamp".to_owned()),
                        keyword: Some("timestamp".to_owned()),
                        dominant_family: String::new(),
                        dominant_ratio: 0.0,
                        minority_families: Vec::new(),
                        observed_types: vec![InferWarningTypeYaml {
                            type_name: "Int32".to_owned(),
                            ratio: 0.98,
                            examples: vec!["1609954220".to_owned()],
                        }],
                    }],
                      read_ops: None,
                },
                pg_target_table: Some("scheduling_executions".to_owned()),
                table_names: Vec::new(),
            }],
            "dbapi",
            "cluster0",
            "Test PG Keyword Warning Details",
        );

        assert!(html.contains("timestamp"));
        assert!(html.contains("PostgreSQL keyword"));
        assert!(html.contains("_timestamp"));
        assert!(html.contains("Int32"));
    }

    #[test]
    fn render_html_shows_type_name_warning_details() {
        let html = render_html(
            &[CollectionRow {
                name: "sample".to_owned(),
                stats: CollectionStatsYaml {
                    documents_in_collection: serde_yaml::Value::Number(10_u64.into()),
                    documents_sampled: 10,
                    width_top_level: 2,
                    width_max: 2.0,
                    width_max_level: 1,
                    depth_max: 1,
                    branch_total: 2.0,
                    branch_per_level: indexmap::IndexMap::from([("L1".to_owned(), 2.0)]),
                    array_field_count: 0,
                    avg_fields_per_doc: 2.0,
                    migrability_score: 1.5,
                    infer_warnings: vec![InferWarningYaml {
                        kind: "type_name".to_owned(),
                        field_path: "date".to_owned(),
                        renamed_to: None,
                        keyword: Some("date".to_owned()),
                        dominant_family: String::new(),
                        dominant_ratio: 0.0,
                        minority_families: Vec::new(),
                        observed_types: vec![InferWarningTypeYaml {
                            type_name: "Date".to_owned(),
                            ratio: 1.0,
                            examples: vec!["\"2026-05-22 18:50:52.681 +00:00:00\"".to_owned()],
                        }],
                    }],
                      read_ops: None,
                },
                pg_target_table: Some("sample".to_owned()),
                table_names: Vec::new(),
            }],
            "dbapi",
            "cluster0",
            "Test Infer Warning Highlighting Details",
        );

        assert!(html.contains("field name matches type name"));
        assert!(html.contains("date"));
        assert!(html.contains("Date"));
    }

      #[test]
      fn render_html_shows_collection_read_ops_hint() {
        let html = render_html(
          &[CollectionRow {
            name: "customers".to_owned(),
            stats: CollectionStatsYaml {
              documents_in_collection: serde_yaml::Value::Number(500_u64.into()),
              documents_sampled: 500,
              width_top_level: 8,
              width_max: 8.0,
              width_max_level: 1,
              depth_max: 2,
              branch_total: 9.4,
              branch_per_level: indexmap::IndexMap::from([
                ("L1".to_owned(), 8.0),
                ("L2".to_owned(), 1.4),
              ]),
              array_field_count: 1,
              avg_fields_per_doc: 6.8,
              migrability_score: 2.3,
              infer_warnings: Vec::new(),
              read_ops: Some(super::CollectionReadOpsYaml {
                read_ops: 1234,
                since: Some("2026-07-03 10:00:00 UTC".to_owned()),
              }),
            },
            pg_target_table: Some("customers".to_owned()),
            table_names: Vec::new(),
          }],
          "sample_analytics",
          "cluster0",
          "Read Ops",
        );

        assert!(html.contains("reads: 1234 since 2026-07-03 10:00:00 UTC"));
      }

  #[test]
  fn render_html_shows_grouped_pg_target_table() {
    let html = render_html(
      &[
        CollectionRow {
          name: "events_bcit".to_owned(),
          stats: CollectionStatsYaml {
            documents_in_collection: serde_yaml::Value::Number(10_u64.into()),
            documents_sampled: 10,
            width_top_level: 2,
            width_max: 2.0,
            width_max_level: 1,
            depth_max: 1,
            branch_total: 2.0,
            branch_per_level: indexmap::IndexMap::from([("L1".to_owned(), 2.0)]),
            array_field_count: 0,
            avg_fields_per_doc: 2.0,
            migrability_score: 1.0,
            infer_warnings: Vec::new(),
            read_ops: None,
          },
          pg_target_table: Some("events".to_owned()),
          table_names: Vec::new(),
        },
        CollectionRow {
          name: "events_lmza".to_owned(),
          stats: CollectionStatsYaml {
            documents_in_collection: serde_yaml::Value::Number(20_u64.into()),
            documents_sampled: 20,
            width_top_level: 2,
            width_max: 2.0,
            width_max_level: 1,
            depth_max: 1,
            branch_total: 2.0,
            branch_per_level: indexmap::IndexMap::from([("L1".to_owned(), 2.0)]),
            array_field_count: 0,
            avg_fields_per_doc: 2.0,
            migrability_score: 1.0,
            infer_warnings: Vec::new(),
            read_ops: None,
          },
          pg_target_table: Some("events".to_owned()),
          table_names: Vec::new(),
        },
      ],
      "sample",
      "cluster0",
      "Grouped PG Table",
    );

    assert!(html.contains("<th>PG Table</th>"));
    assert!(html.contains(r#"class="pg-table">events</td>"#));
  }

  #[test]
  fn render_html_is_backward_compatible_when_pg_target_table_missing() {
    let html = render_html(
      &[CollectionRow {
        name: "legacy_collection".to_owned(),
        stats: CollectionStatsYaml {
          documents_in_collection: serde_yaml::Value::Number(3_u64.into()),
          documents_sampled: 3,
          width_top_level: 1,
          width_max: 1.0,
          width_max_level: 1,
          depth_max: 1,
          branch_total: 1.0,
          branch_per_level: indexmap::IndexMap::from([("L1".to_owned(), 1.0)]),
          array_field_count: 0,
          avg_fields_per_doc: 1.0,
          migrability_score: 0.5,
          infer_warnings: Vec::new(),
          read_ops: None,
        },
        pg_target_table: None,
        table_names: Vec::new(),
      }],
      "legacy",
      "cluster0",
      "Legacy",
    );

    assert!(html.contains("legacy_collection"));
    assert!(html.contains(r#"class="pg-table">-</td>"#));
  }

    #[test]
    fn render_post_import_html_shows_clickable_md5_details() {
        let html = render_post_import_html(
            &[PostImportCollectionRow {
                name: "scheduling_jobs".to_owned(),
                document_count: 3,
                root: PostImportNode {
                    name: "scheduling_jobs".to_owned(),
                    is_array: false,
                    mongo_count: 3,
                    pg_table_name: Some("dbapi.scheduling_jobs".to_owned()),
                    pg_row_count: Some(3),
                    md5_summary: Some(PostImportMd5Summary {
                        mongo_md5: "abc123".to_owned(),
                        pg_md5: "abc123".to_owned(),
                        columns: vec![PostImportMd5Column {
                            source_field: "last_update".to_owned(),
                            source_type: Some("TIMESTAMP WITH TIME ZONE".to_owned()),
                            target_field: "last_update".to_owned(),
                            target_type: Some("TIMESTAMP WITH TIME ZONE".to_owned()),
                        }],
                        mismatches: Vec::new(),
                    }),
                    count_diff_rows: Vec::new(),
                    children: Vec::new(),
                },
            }],
            "dbapi.scheduling_jobs",
            "mongo-host",
            "pg-host",
        );

        assert!(html.contains("<details class=\"md5-detail is-match\">"));
        assert!(html.contains("Columns involved"));
        assert!(html.contains("last_update"));
        assert!(html.contains("mongodb (TIMESTAMP WITH TIME ZONE)"));
        assert!(html.contains("pg (TIMESTAMP WITH TIME ZONE)"));
        assert!(html.contains("abc123"));
    }

    #[test]
    fn render_post_import_html_shows_first_mismatched_rows() {
        let html = render_post_import_html(
            &[PostImportCollectionRow {
                name: "advisors".to_owned(),
                document_count: 2,
                root: PostImportNode {
                    name: "advisors".to_owned(),
                    is_array: false,
                    mongo_count: 2,
                    pg_table_name: Some("dbapi.earnings".to_owned()),
                    pg_row_count: Some(2),
                    md5_summary: Some(PostImportMd5Summary {
                        mongo_md5: "mongo-md5".to_owned(),
                        pg_md5: "pg-md5".to_owned(),
                        columns: vec![PostImportMd5Column {
                            source_field: "monthly_gain".to_owned(),
                            source_type: Some("TIMESTAMP WITH TIME ZONE".to_owned()),
                            target_field: "monthly_gain".to_owned(),
                            target_type: Some("TIMESTAMP WITH TIME ZONE".to_owned()),
                        }],
                        mismatches: vec![PostImportMd5MismatchRow {
                            row_index: 1,
                            mongo_values: Some(vec!["12.5".to_owned()]),
                            pg_values: Some(vec!["\"12.5\"".to_owned()]),
                        }],
                    }),
                    count_diff_rows: Vec::new(),
                    children: Vec::new(),
                },
            }],
            "dbapi.advisors",
            "mongo-host",
            "pg-host",
        );

        assert!(html.contains("First 5 non-corresponding rows"));
        assert!(html.contains("monthly_gain"));
        assert!(html.contains("12.5"));
        assert!(html.contains("\"12.5\""));
    }

    #[test]
    fn render_post_import_html_shows_clickable_count_diff_details() {
        let html = render_post_import_html(
            &[PostImportCollectionRow {
                name: "customers".to_owned(),
                document_count: 3,
                root: PostImportNode {
                    name: "customers_accounts".to_owned(),
                    is_array: false,
                    mongo_count: 3,
                    pg_table_name: Some("sample_analytics.customers_accounts".to_owned()),
                    pg_row_count: Some(1),
                    md5_summary: None,
                    count_diff_rows: vec![PostImportCountDiffRow {
                        row_index: 2,
                        mongo_values: Some(vec!["\"acc-a\"".to_owned(), "true".to_owned()]),
                        pg_values: None,
                    }],
                    children: Vec::new(),
                },
            }],
            "sample_analytics.customers",
            "mongo-host",
            "pg-host",
        );

        assert!(html.contains("delta "));
        assert!(html.contains("First 5 differences by mapped row values"));
        assert!(html.contains("missing row"));
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Multi-database combined report
// ──────────────────────────────────────────────────────────────────────────────

/// Render a single HTML report combining all databases in one page.
///
/// Structure:
/// 1. Cluster-level summary cards (score, db count, collection count, doc count)
/// 2. Per-database summary table (one row per db with its own score/avg/max)
/// 3. Per-database section: mini score cards + full collection details table
pub fn render_multi_db_html(
    entries: &[(&str, &[CollectionRow])],
    cluster: &str,
    project_name: &str,
    title: &str,
) -> String {
    let now = chrono::Utc::now()
        .format("%Y-%m-%d %H:%M:%S UTC")
        .to_string();

    // ── Compute per-db and cluster scores ─────────────────────────────────────
    let db_scores: Vec<DatabaseScore> = entries
        .iter()
        .map(|(name, rows)| compute_db_score(name, rows))
        .collect();
    let cs = compute_cluster_score(&db_scores);

    let total_collections: usize = db_scores.iter().map(|d| d.collection_count).sum();
    let total_docs: u64 = db_scores.iter().map(|d| d.total_docs).sum();
    let score_db_sum: f64 =
        (db_scores.iter().map(|d| d.score_db).sum::<f64>() * 100.0).round() / 100.0;

    let d = db_scores.len() as f64;
    let score_per_db = if d > 0.0 { cs.score_total / d } else { 0.0 };
    let (complexity_label, complexity_color) = if score_per_db < 30.0 {
        ("Easy", "#27ae60")
    } else if score_per_db < 80.0 {
        ("Medium", "#e67e22")
    } else {
        ("Hard", "#c0392b")
    };

    // ── DB summary table rows ─────────────────────────────────────────────────
    let db_summary_rows: String = db_scores
        .iter()
        .map(|db| {
            let score_color = if db.score_db < 30.0 {
                "#27ae60"
            } else if db.score_db < 80.0 {
                "#e67e22"
            } else {
                "#c0392b"
            };
            format!(
                r##"<tr class="collection-row">
          <td class="name"><a href="#{name}">{name}</a></td>
          <td class="num">{collections}</td>
          <td class="num">{docs}</td>
          <td class="num"><span class="score-badge" style="color:{score_color};font-weight:700">{score_db:.2}</span></td>
          <td class="num">{score_avg:.2}</td>
          <td class="num">{score_max:.2}</td>
        </tr>"##,
                name = db.name,
                collections = db.collection_count,
                docs = db.total_docs,
                score_db = db.score_db,
                score_avg = db.score_avg,
                score_max = db.score_max,
                score_color = score_color,
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    // ── Per-db collection sections ────────────────────────────────────────────
    let db_sections: String = entries
        .iter()
        .zip(db_scores.iter())
        .map(|((db_name, rows), db_score)| {
            let (db_complexity_label, db_complexity_color) = if db_score.score_db < 30.0 {
                ("Easy", "#27ae60")
            } else if db_score.score_db < 80.0 {
                ("Medium", "#e67e22")
            } else {
                ("Hard", "#c0392b")
            };
            let score_max: f64 = rows
                .iter()
                .map(|r| r.stats.migrability_score)
                .fold(0.0_f64, f64::max);

            let has_tables = rows.iter().any(|r| !r.table_names.is_empty());
            let col_count = 9 + if has_tables { 1 } else { 0 };

            let collection_rows: String = rows
                .iter()
                .map(|r| {
                    let docs_cell = render_documents_cell(&r.stats);
                    let score_color = if r.stats.migrability_score < 3.0 {
                        "#27ae60"
                    } else if r.stats.migrability_score < 8.0 {
                        "#e67e22"
                    } else {
                        "#c0392b"
                    };
                    let (name_cell, detail_row) = if r.table_names.is_empty() {
                        let warning_detail = if r.has_infer_warnings() {
                            render_infer_warning_detail(&r.stats.infer_warnings)
                        } else {
                            String::new()
                        };
                        (
                            format!(
                                r#"<td class="name"><span class="collection-name {warning_class}">{name}</span>{warning_detail}</td>"#,
                                warning_class = if r.has_infer_warnings() { "has-warning" } else { "" },
                                name = escape_html(&r.name),
                                warning_detail = warning_detail,
                            ),
                            String::new(),
                        )
                    } else {
                        let pills: String = r
                            .table_names
                            .iter()
                            .map(|(tname, ddl)| {
                                let safe_id = format!(
                                    "ddl-{}-{}-{}",
                                    db_name.chars().map(|c| if c.is_alphanumeric() { c } else { '_' }).collect::<String>(),
                                    r.name.chars().map(|c| if c.is_alphanumeric() { c } else { '_' }).collect::<String>(),
                                    tname.chars().map(|c| if c.is_alphanumeric() { c } else { '_' }).collect::<String>(),
                                );
                                let ddl_esc = ddl
                                    .replace('&', "&amp;")
                                    .replace('<', "&lt;")
                                    .replace('>', "&gt;");
                                format!(
                                    r#"<div class="pill-block"><span class="pill clickable-pill" onclick="toggleDDL('{sid}')" title="Click to show DDL"><span class="pill-arrow" id="arr-{sid}">&#9658;</span> {tname}</span><pre class="ddl-block" id="{sid}" style="display:none">{ddl_esc}</pre></div>"#,
                                    sid = safe_id,
                                    tname = tname,
                                    ddl_esc = ddl_esc,
                                )
                            })
                            .collect::<Vec<_>>()
                            .join("");
                        let detail_id = format!("detail-{db_name}-{}", r.name);
                        let icon_id = format!("icon-{db_name}-{}", r.name);
                        let key = format!("{db_name}-{}", r.name);
                        let warning_detail = if r.has_infer_warnings() {
                            render_infer_warning_detail(&r.stats.infer_warnings)
                        } else {
                            String::new()
                        };
                        let name_cell = format!(
                            r#"<td class="name expandable" onclick="toggleDetail('{key}')" title="Click to expand PG tables">
              <span class="expand-icon" id="{icon_id}">▶</span> <span class="collection-name {warning_class}">{coll}</span>{warning_detail}
            </td>"#,
                            key = key,
                            icon_id = icon_id,
                            coll = escape_html(&r.name),
                            warning_class = if r.has_infer_warnings() { "has-warning" } else { "" },
                            warning_detail = warning_detail,
                        );
                        let detail_row = format!(
                            r#"<tr class="detail-row" id="{detail_id}" style="display:none">
          <td colspan="{col_count}" class="detail-cell">
            <div class="table-list">{pills}</div>
          </td>
        </tr>"#,
                        );
                        (name_cell, detail_row)
                    };
                    let tables_cell = if has_tables {
                        format!(
                            r#"<td class="num">{}</td>"#,
                            r.tables_count().map_or("-".to_owned(), |n| n.to_string())
                        )
                    } else {
                        String::new()
                    };
                    let pg_table_cell = format!(
                      r#"<td class="pg-table">{}</td>"#,
                      r.pg_target_table
                        .as_deref()
                        .map(escape_html)
                        .unwrap_or_else(|| "-".to_owned())
                    );
                    format!(
                        r#"<tr class="collection-row">
          {name_cell}
                {pg_table_cell}
          {docs_cell}
          <td class="num">{sampled}</td>
          <td class="num">{width_top}</td>
          <td class="num">{width_max:.1} <span class="level">(L{width_max_level})</span></td>
          <td class="num">{depth}</td>
          <td class="num" title="{branch_tooltip}">{branch:.1}</td>
          <td class="num"><span class="score-badge" style="color:{score_color};font-weight:700">{score:.2}</span></td>
          {tables_cell}
        </tr>
        {detail_row}"#,
                        docs_cell = docs_cell,
                        pg_table_cell = pg_table_cell,
                        sampled = r.stats.documents_sampled,
                        width_top = r.stats.width_top_level,
                        width_max = r.stats.width_max,
                        width_max_level = r.stats.width_max_level,
                        depth = r.stats.depth_max,
                        branch = r.stats.branch_total,
                        branch_tooltip = {
                            let levels = r
                                .stats
                                .branch_per_level
                                .iter()
                                .map(|(k, v)| format!("{}: {:.2}", k, v))
                                .collect::<Vec<_>>()
                                .join("  ");
                            format!("Expected fields/doc by level — {}", levels)
                        },
                        score = r.stats.migrability_score,
                        score_color = score_color,
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");

            let tables_header = if has_tables {
                r#"<th class="num">PG Tables</th>"#
            } else {
                ""
            };

            let score_sum_i = ((db_score.score_db - 1.5 * db_score.collection_count as f64) * 100.0).round() / 100.0;
            format!(
                r#"  <div class="db-section" id="{db_name}">
    <h2 class="db-heading">{db_name}</h2>
    <div class="summary-grid" style="margin-bottom:1rem">
      <div class="card"><div class="label">Collections</div><div class="value">{coll_count}</div></div>
      <div class="card"><div class="label">Documents</div><div class="value">{docs}</div></div>
      <div class="card"><div class="label">Score (total)</div><div class="value" style="color:{db_complexity_color}">{score_db:.1}</div></div>
      <div class="card"><div class="label">Complexity</div><div class="value" style="font-size:1.2rem;margin-top:0.3rem"><span class="complexity-badge" style="background:{db_complexity_color}">{db_complexity_label}</span></div></div>
      <div class="card"><div class="label">Score (avg)</div><div class="value" style="font-size:1.3rem">{score_avg:.2}</div></div>
      <div class="card"><div class="label">Score (max coll)</div><div class="value" style="font-size:1.3rem">{score_max:.2}</div></div>
    </div>
    <p class="score-explainer">
      <strong>DB score</strong>: <code>1.5 × collections + Σ C<sub>i</sub></code><br>
      &nbsp;&nbsp;&nbsp;= <code>1.5 × {coll_count} + {score_sum_i:.2}</code> = <strong>{score_db:.2}</strong>.
    </p>
    <table>
      <thead>
        <tr>
          <th>Collection</th>
          <th>PG Table</th>
          <th class="num">Documents</th>
          <th class="num">Sampled</th>
          <th class="num" title="Number of top-level fields">Width (top)</th>
          <th class="num" title="Highest field count at any nesting level">Width (max)</th>
          <th class="num" title="Maximum nesting depth">Depth (max)</th>
          <th class="num" title="Expected total fields per doc (probability-weighted)">Fields (total)</th>
          <th class="num">Score</th>
          {tables_header}
        </tr>
      </thead>
      <tbody>
        {collection_rows}
      </tbody>
    </table>
  </div>"#,
                db_name = db_name,
                coll_count = db_score.collection_count,
                docs = db_score.total_docs,
                score_db = db_score.score_db,
                score_avg = db_score.score_avg,
                score_max = score_max,
                score_sum_i = score_sum_i,
                db_complexity_label = db_complexity_label,
                db_complexity_color = db_complexity_color,
                tables_header = tables_header,
                collection_rows = collection_rows,
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>mongo2pg – {project_name} @ {cluster}</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #f5f7fa;
      color: #333;
      margin: 0;
      padding: 2rem;
    }}
    h1 {{ color: #2c3e50; margin-bottom: 0.25rem; }}
    h2 {{ color: #2c3e50; margin-top: 0; margin-bottom: 0.75rem; font-size: 1.25rem; }}
    .subtitle {{ color: #7f8c8d; font-size: 0.9rem; margin-bottom: 2rem; }}
    .summary-grid {{
      display: flex;
      gap: 1rem;
      margin-bottom: 2rem;
      flex-wrap: wrap;
    }}
    .card {{
      background: white;
      border-radius: 8px;
      padding: 1rem 1.5rem;
      box-shadow: 0 1px 4px rgba(0,0,0,.08);
      min-width: 160px;
    }}
    .card .label {{ font-size: 0.75rem; text-transform: uppercase; color: #7f8c8d; }}
    .card .value {{ font-size: 1.6rem; font-weight: 700; color: #2c3e50; }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: white;
      border-radius: 8px;
      overflow: hidden;
      box-shadow: 0 1px 4px rgba(0,0,0,.08);
    }}
    thead {{ background: #2c3e50; color: white; }}
    th[title] {{ cursor: help; }}
    th {{
      padding: 0.75rem 1rem;
      text-align: left;
      font-size: 0.8rem;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }}
    th.num, td.num {{ text-align: right; }}
    td {{
      padding: 0.6rem 1rem;
      border-bottom: 1px solid #ecf0f1;
      font-size: 0.9rem;
    }}
    tr:last-child td {{ border-bottom: none; }}
    tr.collection-row:hover td {{ background: #f0f4f8; }}
    td.name {{ font-weight: 600; color: #2c3e50; }}
    td.name a {{ color: inherit; text-decoration: none; }}
    td.name a:hover {{ text-decoration: underline; color: #2980b9; }}
    td.expandable {{
      cursor: pointer;
      user-select: none;
    }}
    td.expandable:hover {{ color: #2980b9; }}
    .expand-icon {{
      display: inline-block;
      font-size: 0.65rem;
      color: #95a5a6;
      margin-right: 0.3rem;
      transition: transform 0.15s;
    }}
    .expand-icon.open {{ transform: rotate(90deg); }}
    .collection-name.has-warning {{ color: #b58900; }}
    .collection-warning {{ display: inline-block; margin-left: 0.45rem; vertical-align: middle; }}
    .collection-warning-summary {{
      display: inline-block;
      list-style: none;
      cursor: pointer;
      border: 1px solid #f4d03f;
      background: #fcf3cf;
      color: #9a7d0a;
      border-radius: 999px;
      padding: 0.08rem 0.5rem;
      font-size: 0.72rem;
      font-weight: 700;
    }}
    .collection-warning-summary::-webkit-details-marker {{ display: none; }}
    .collection-warning-popover {{
      margin-top: 0.35rem;
      min-width: 320px;
      max-width: 520px;
      background: #fffdf3;
      border: 1px solid #f7dc6f;
      border-radius: 6px;
      box-shadow: 0 2px 6px rgba(0,0,0,.08);
      padding: 0.7rem 0.85rem;
      color: #5d4b00;
    }}
    .collection-warning-title {{ font-size: 0.78rem; font-weight: 700; margin-bottom: 0.45rem; color: #7d6608; }}
    .collection-warning-list {{ margin: 0; padding-left: 1rem; }}
    .collection-warning-list > li {{ margin-bottom: 0.5rem; font-size: 0.78rem; line-height: 1.35; }}
    .collection-warning-list > li:last-child {{ margin-bottom: 0; }}
    .collection-warning-types {{ margin-top: 0.3rem; padding-left: 1rem; }}
    .collection-warning-types li {{ margin-bottom: 0.22rem; font-size: 0.75rem; line-height: 1.35; }}
    .collection-warning-types li:last-child {{ margin-bottom: 0; }}
    .collection-warning-examples {{ color: #7d6608; }}
    .detail-row td.detail-cell {{
      background: #f8fafc;
      padding: 0.5rem 1rem 0.75rem 2.5rem;
      border-top: none;
    }}
    .table-list {{ display: flex; flex-wrap: wrap; gap: 0.4rem; }}
    .pill {{
      display: inline-block;
      background: #eaf4fb;
      color: #2471a3;
      border: 1px solid #aed6f1;
      border-radius: 4px;
      padding: 0.15rem 0.55rem;
      font-size: 0.78rem;
      font-family: monospace;
    }}
    .pill-block {{ display: inline-flex; flex-direction: column; }}
    .clickable-pill {{ cursor: pointer; user-select: none; }}
    .clickable-pill:hover {{ background: #d6eaf8; }}
    .pill-arrow {{
      display: inline-block;
      font-size: 0.6rem;
      color: #5d8aa8;
      margin-right: 0.25rem;
      transition: transform 0.15s;
    }}
    .pill-arrow.open {{ transform: rotate(90deg); }}
    .ddl-block {{
      background: #1e1e2e;
      color: #cdd6f4;
      border: 1px solid #45475a;
      border-radius: 4px;
      padding: 0.6rem 0.8rem;
      font-size: 0.78rem;
      font-family: monospace;
      white-space: pre;
      margin-top: 0.25rem;
      text-align: left;
      max-width: 100%;
      overflow-x: auto;
    }}
    .level {{ color: #95a5a6; font-size: 0.8rem; }}
    .score-badge {{ font-size: 0.95rem; }}
    .complexity-badge {{
      display: inline-block;
      padding: 0.15rem 0.6rem;
      border-radius: 4px;
      font-size: 0.85rem;
      font-weight: 700;
      color: white;
    }}
    .db-section {{ margin-top: 3rem; }}
    .db-heading {{
      font-size: 1.4rem;
      color: #2c3e50;
      padding-bottom: 0.4rem;
      border-bottom: 2px solid #bdc3c7;
      margin-bottom: 1rem;
    }}
    footer {{ margin-top: 2rem; font-size: 0.75rem; color: #aaa; }}
    .score-explainer {{ margin-top: -1rem; margin-bottom: 1.5rem; font-size: 0.82rem; color: #7f8c8d; }}
    .score-explainer code {{ background: #eee; padding: 0.1rem 0.3rem; border-radius: 3px; }}
    .docs-extra {{
      font-size: 0.72rem;
      color: #7f8c8d;
      font-weight: 500;
      margin-top: 0.15rem;
      white-space: nowrap;
    }}
  </style>
</head>
<body>
  <h1>mongo2pg – Migration Report {title}</h1>
  <p class="subtitle">Cluster: <strong>{cluster}</strong> &nbsp;|&nbsp; Project: <strong>{project_name}</strong> &nbsp;|&nbsp; Generated: {now}</p>

  <div class="summary-grid">
    <div class="card"><div class="label">Databases</div><div class="value">{db_count}</div></div>
    <div class="card"><div class="label">Collections</div><div class="value">{total_collections}</div></div>
    <div class="card"><div class="label">Total Documents</div><div class="value">{total_docs}</div></div>
    <div class="card">
      <div class="label">Cluster Score</div>
      <div class="value" style="color:{complexity_color}">{score_total:.1}</div>
    </div>
    <div class="card">
      <div class="label">Complexity</div>
      <div class="value" style="font-size:1.2rem;margin-top:0.3rem">
        <span class="complexity-badge" style="background:{complexity_color}">{complexity_label}</span>
      </div>
    </div>
    <div class="card"><div class="label">Score (avg weighted)</div><div class="value" style="font-size:1.3rem">{score_avg:.2}</div></div>
    <div class="card"><div class="label">Score (max db)</div><div class="value" style="font-size:1.3rem">{score_max:.2}</div></div>
  </div>

  <p class="score-explainer">
    <strong>Cluster score</strong>: <code>1.5 × databases + Σ score_db<sub>j</sub></code><br>
    &nbsp;&nbsp;&nbsp;= <code>1.5 × {db_count} + {score_db_sum:.2}</code> = <strong>{score_total:.2}</strong>.
  </p>

  <h2>Databases</h2>
  <table>
    <thead>
      <tr>
        <th>Database</th>
        <th class="num">Collections</th>
        <th class="num">Documents</th>
        <th class="num" title="1.5 × collections + Σ collection scores">Score (total)</th>
        <th class="num" title="Document-count-weighted average of per-collection scores">Score (avg)</th>
        <th class="num" title="Highest per-collection score">Score (max)</th>
      </tr>
    </thead>
    <tbody>
      {db_summary_rows}
    </tbody>
  </table>

  {db_sections}

  <footer>Generated by <a href="https://github.com/pmpetit/mongo2pg">mongo2pg</a></footer>

  <script>
    function toggleDetail(key) {{
      var row  = document.getElementById('detail-' + key);
      var icon = document.getElementById('icon-'   + key);
      if (!row) return;
      var open = row.style.display !== 'none';
      row.style.display = open ? 'none' : '';
      if (open) {{ icon.classList.remove('open'); }}
      else      {{ icon.classList.add('open');    }}
    }}
    function toggleDDL(id) {{
      var block = document.getElementById(id);
      var arr   = document.getElementById('arr-' + id);
      if (!block) return;
      var open = block.style.display !== 'none';
      block.style.display = open ? 'none' : '';
      if (open) {{ arr.classList.remove('open'); }}
      else      {{ arr.classList.add('open');    }}
    }}
  </script>
</body>
</html>
"#,
        project_name = project_name,
        title = title,
        cluster = cluster,
        now = now,
        db_count = cs.db_count,
        total_collections = total_collections,
        total_docs = total_docs,
        score_total = cs.score_total,
        score_avg = cs.score_avg,
        score_max = cs.score_max,
        score_db_sum = score_db_sum,
        complexity_label = complexity_label,
        complexity_color = complexity_color,
        db_summary_rows = db_summary_rows,
        db_sections = db_sections,
    )
}
