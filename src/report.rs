//! HTML report generation from per-collection `.stats.yaml` files.

use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

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
}

/// Read every `<base>/<collection>/<collection>.stats.yaml` and return sorted rows.
pub fn collect_rows(base: &Path) -> Result<Vec<CollectionRow>> {
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
        rows.push(CollectionRow { name, stats });
    }

    rows.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(rows)
}

/// Render the HTML report string.
pub fn render_html(rows: &[CollectionRow], namespace: &str) -> String {
    let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string();

    let total_docs: u64 = rows
        .iter()
        .map(|r| match &r.stats.documents_in_collection {
            serde_yaml::Value::Number(n) => n.as_u64().unwrap_or(0),
            _ => 0,
        })
        .sum();

    let table_rows: String = rows
        .iter()
        .map(|r| {
            let doc_count = match &r.stats.documents_in_collection {
                serde_yaml::Value::Number(n) => n.as_u64().unwrap_or(0).to_string(),
                _ => "unknown".to_owned(),
            };
            format!(
                r#"<tr>
          <td class="name">{name}</td>
          <td class="num">{doc_count}</td>
          <td class="num">{sampled}</td>
          <td class="num">{width_top}</td>
          <td class="num">{width_max} <span class="level">(L{width_max_level})</span></td>
          <td class="num">{depth}</td>
          <td class="num">{branch}</td>
        </tr>"#,
                name = r.name,
                doc_count = doc_count,
                sampled = r.stats.documents_sampled,
                width_top = r.stats.width_top_level,
                width_max = r.stats.width_max,
                width_max_level = r.stats.width_max_level,
                depth = r.stats.depth_max,
                branch = r.stats.branch_total,
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
    tr:hover td {{ background: #f0f4f8; }}
    td.name {{ font-weight: 600; color: #2c3e50; }}
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
        namespace = namespace,
        now = now,
        count = rows.len(),
        total_docs = total_docs,
        table_rows = table_rows,
    )
}
