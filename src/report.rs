//! HTML report generation from per-collection `.stats.yaml` files.

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
    let total_docs: u64 = rows
        .iter()
        .map(|r| match &r.stats.documents_in_collection {
            serde_yaml::Value::Number(n) => n.as_u64().unwrap_or(0),
            _ => 0,
        })
        .sum();

    let n = rows.len() as f64;
    let score_sum: f64 = rows.iter().map(|r| r.stats.migrability_score).sum();
    let score_db = ((1.5 * n + score_sum) * 100.0).round() / 100.0;

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

    DatabaseScore {
        name: name.to_owned(),
        score_db,
        score_avg,
        score_max: (score_max * 100.0).round() / 100.0,
        total_docs,
        collection_count: rows.len(),
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
}

pub struct CollectionRow {
    pub name: String,
    pub stats: CollectionStatsYaml,
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
                let sql_path = dir.join(format!("{name}.sql"));
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

        rows.push(CollectionRow {
            name,
            stats,
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
pub fn render_html(rows: &[CollectionRow], namespace: &str, cluster: &str) -> String {
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
    let col_count = 8 + if has_tables { 1 } else { 0 };

    let table_rows: String = rows
        .iter()
        .map(|r| {
            let doc_count = match &r.stats.documents_in_collection {
                serde_yaml::Value::Number(n) => n.as_u64().unwrap_or(0).to_string(),
                _ => "unknown".to_owned(),
            };

            let score_color = if r.stats.migrability_score < 3.0 {
                "#27ae60"
            } else if r.stats.migrability_score < 8.0 {
                "#e67e22"
            } else {
                "#c0392b"
            };

            let (name_cell, detail_row) = if r.table_names.is_empty() {
                // No SQL schema available – plain name, no expand control
                (
                    format!(r#"<td class="name">{}</td>"#, r.name),
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
                let name_cell = format!(
                    r#"<td class="name expandable" onclick="toggleDetail('{name}')" title="Click to expand PG tables">
              <span class="expand-icon" id="{icon_id}">▶</span> {name}
            </td>"#,
                    name = r.name,
                    icon_id = icon_id,
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

            format!(
                r#"<tr class="collection-row">
          {name_cell}
          <td class="num">{doc_count}</td>
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
                doc_count = doc_count,
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
  </style>
</head>
<body>
  <h1>mongo2pg – Migration Report</h1>
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
    Thresholds: &lt;30 Easy · 30–80 Medium · &gt;80 Hard.<br>
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
        cluster = cluster,
        now = now,
        count = rows.len(),
        total_docs = total_docs,
        score_db = score_db,
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

    // Thresholds scale with D so that a cluster of D databases is graded on the
    // same relative scale as a single database.
    let d = dbs.len() as f64;
    let threshold_easy = 30.0 * d;
    let threshold_hard = 80.0 * d;
    let complexity_label = if cs.score_total < threshold_easy {
        ("Easy", "#27ae60")
    } else if cs.score_total < threshold_hard {
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
    <strong>Cluster score</strong>: <code>1.5 × databases + Σ score_db<sub>j</sub></code>.
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
    )
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

    let d = db_scores.len() as f64;
    let threshold_easy = 30.0 * d;
    let threshold_hard = 80.0 * d;
    let (complexity_label, complexity_color) = if cs.score_total < threshold_easy {
        ("Easy", "#27ae60")
    } else if cs.score_total < threshold_hard {
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
            let col_count = 8 + if has_tables { 1 } else { 0 };

            let collection_rows: String = rows
                .iter()
                .map(|r| {
                    let doc_count = match &r.stats.documents_in_collection {
                        serde_yaml::Value::Number(n) => n.as_u64().unwrap_or(0).to_string(),
                        _ => "unknown".to_owned(),
                    };
                    let score_color = if r.stats.migrability_score < 3.0 {
                        "#27ae60"
                    } else if r.stats.migrability_score < 8.0 {
                        "#e67e22"
                    } else {
                        "#c0392b"
                    };
                    let (name_cell, detail_row) = if r.table_names.is_empty() {
                        (
                            format!(r#"<td class="name">{}</td>"#, r.name),
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
                        let name_cell = format!(
                            r#"<td class="name expandable" onclick="toggleDetail('{key}')" title="Click to expand PG tables">
              <span class="expand-icon" id="{icon_id}">▶</span> {coll}
            </td>"#,
                            key = key,
                            icon_id = icon_id,
                            coll = r.name,
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
                    format!(
                        r#"<tr class="collection-row">
          {name_cell}
          <td class="num">{doc_count}</td>
          <td class="num">{sampled}</td>
          <td class="num">{width_top}</td>
          <td class="num">{width_max:.1} <span class="level">(L{width_max_level})</span></td>
          <td class="num">{depth}</td>
          <td class="num" title="{branch_tooltip}">{branch:.1}</td>
          <td class="num"><span class="score-badge" style="color:{score_color};font-weight:700">{score:.2}</span></td>
          {tables_cell}
        </tr>
        {detail_row}"#,
                        doc_count = doc_count,
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
    <table>
      <thead>
        <tr>
          <th>Collection</th>
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
  </style>
</head>
<body>
  <h1>mongo2pg – Migration Report</h1>
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
        cluster = cluster,
        now = now,
        db_count = cs.db_count,
        total_collections = total_collections,
        total_docs = total_docs,
        score_total = cs.score_total,
        score_avg = cs.score_avg,
        score_max = cs.score_max,
        complexity_label = complexity_label,
        complexity_color = complexity_color,
        db_summary_rows = db_summary_rows,
        db_sections = db_sections,
    )
}
