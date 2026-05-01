//! HTML report generation from per-collection `.stats.yaml` files.

use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::schema_diagram::parse_sql;

/// Subset of [`crate::stats::StatsYaml`] we need for the report.
#[derive(Debug, Deserialize)]
pub struct CollectionStatsYaml {
    pub documents_in_collection: serde_yaml::Value,
    pub documents_sampled: u64,
    pub width_top_level: usize,
    pub width_max: usize,
    pub width_max_level: usize,
    pub depth_max: usize,
    pub branch_total: usize,
}

pub struct CollectionRow {
    pub name: String,
    pub stats: CollectionStatsYaml,
    /// PostgreSQL table names generated for this collection (from the `.sql` file).
    /// Empty when no `schema/tables/` directory was provided.
    pub table_names: Vec<String>,
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

        let table_names = tables_dir
            .and_then(|dir| {
                let sql_path = dir.join(format!("{name}.sql"));
                std::fs::read_to_string(&sql_path).ok()
            })
            .map(|sql| parse_sql(&sql).into_iter().map(|t| t.name).collect())
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

/// Render the HTML report string.
pub fn render_html(rows: &[CollectionRow], namespace: &str) -> String {
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
    // Number of columns in the table (used for colspan on detail rows)
    let col_count = 7 + if has_tables { 1 } else { 0 };

    let table_rows: String = rows
        .iter()
        .map(|r| {
            let doc_count = match &r.stats.documents_in_collection {
                serde_yaml::Value::Number(n) => n.as_u64().unwrap_or(0).to_string(),
                _ => "unknown".to_owned(),
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
                    .map(|t| format!(r#"<span class="pill">{t}</span>"#))
                    .collect::<Vec<_>>()
                    .join(" ");
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
          <td class="num">{width_max} <span class="level">(L{width_max_level})</span></td>
          <td class="num">{depth}</td>
          <td class="num">{branch}</td>
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
  <title>mongo2pg – Migration Report</title>
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
    .level {{ color: #95a5a6; font-size: 0.8rem; }}
    footer {{ margin-top: 2rem; font-size: 0.75rem; color: #aaa; }}
  </style>
</head>
<body>
  <h1>mongo2pg – Migration Report</h1>
  <p class="subtitle">Database: <strong>{namespace}</strong> &nbsp;|&nbsp; Generated: {now}</p>

  <div class="summary-grid">
    <div class="card"><div class="label">Collections</div><div class="value">{count}</div></div>
    <div class="card"><div class="label">Total Documents</div><div class="value">{total_docs}</div></div>
    {pg_tables_card}
  </div>

  <table>
    <thead>
      <tr>
        <th>Collection</th>
        <th class="num">Documents</th>
        <th class="num">Sampled</th>
        <th class="num">Width (top)</th>
        <th class="num">Width (max)</th>
        <th class="num">Depth (max)</th>
        <th class="num">Fields (total)</th>
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
  </script>
</body>
</html>
"#,
        namespace = namespace,
        now = now,
        count = rows.len(),
        total_docs = total_docs,
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
