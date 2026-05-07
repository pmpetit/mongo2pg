//! Parse generated SQL DDL files and produce a Mermaid ERD HTML diagram.

use std::path::Path;

use anyhow::{Context, Result};

use crate::analyzer::CollectionSchema;

// ──────────────────────────────────────────────────────────────────────────────
// Data model
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub col_type: String,
    pub not_null: bool,
    pub primary_key: bool,
}

#[derive(Debug, Clone)]
pub struct ForeignKey {
    pub from_col: String,
    pub to_table: String,
    pub to_col: String,
}

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub foreign_keys: Vec<ForeignKey>,
}

// ──────────────────────────────────────────────────────────────────────────────
// SQL parser
// ──────────────────────────────────────────────────────────────────────────────

/// Parse all `CREATE TABLE` statements in `sql`.
pub fn parse_sql(sql: &str) -> Vec<Table> {
    let mut tables = Vec::new();

    // Split on CREATE TABLE boundaries
    for chunk in sql.split("CREATE TABLE ").skip(1) {
        // chunk starts with: "table_name (\n    col ...\n);"
        let paren = match chunk.find('(') {
            Some(p) => p,
            None => continue,
        };
        let close = match chunk.rfind(");") {
            Some(c) => c,
            None => continue,
        };

        let name = chunk[..paren].trim().to_owned();
        let body = &chunk[paren + 1..close];

        let mut columns = Vec::new();
        let mut foreign_keys = Vec::new();

        for raw_line in body.lines() {
            let line = raw_line.trim().trim_end_matches(',').trim();
            if line.is_empty() {
                continue;
            }

            if let Some(fk_def) = line.strip_prefix("FOREIGN KEY (") {
                // FOREIGN KEY (from_col) REFERENCES to_table (to_col)
                if let Some((from_part, rest)) = fk_def.split_once(") REFERENCES ") {
                    let from_col = from_part.trim().to_owned();
                    let rest = rest.trim();
                    if let Some(sp) = rest.find(" (") {
                        let to_table = rest[..sp].trim().to_owned();
                        let to_col = rest[sp + 2..].trim_end_matches(')').trim().to_owned();
                        foreign_keys.push(ForeignKey {
                            from_col,
                            to_table,
                            to_col,
                        });
                    }
                }
                continue;
            }

            // Regular column: name TYPE [NOT NULL] [PRIMARY KEY]
            let mut tokens = line.splitn(3, ' ');
            let col_name = match tokens.next() {
                Some(n) if !n.is_empty() => n.to_owned(),
                _ => continue,
            };
            let col_type = match tokens.next() {
                Some(t) => t.to_owned(),
                None => continue,
            };
            let rest = tokens.next().unwrap_or("").to_uppercase();
            let not_null = rest.contains("NOT NULL") || rest.contains("PRIMARY KEY");
            let primary_key = rest.contains("PRIMARY KEY") || col_name.to_lowercase() == "id";

            columns.push(Column {
                name: col_name,
                col_type,
                not_null,
                primary_key,
            });
        }

        tables.push(Table {
            name,
            columns,
            foreign_keys,
        });
    }

    tables
}

/// Read and parse all `.sql` files in `dir`.
pub fn load_tables(dir: &Path) -> Result<Vec<Table>> {
    let mut all: Vec<Table> = Vec::new();

    let entries =
        std::fs::read_dir(dir).with_context(|| format!("Cannot read {}", dir.display()))?;

    let mut paths: Vec<_> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("sql"))
        .collect();
    paths.sort();

    for path in paths {
        let sql = std::fs::read_to_string(&path)
            .with_context(|| format!("Cannot read {}", path.display()))?;
        all.extend(parse_sql(&sql));
    }

    Ok(all)
}

// ──────────────────────────────────────────────────────────────────────────────
// Mermaid ERD renderer
// ──────────────────────────────────────────────────────────────────────────────

fn mermaid_type(col_type: &str) -> &str {
    // Mermaid ERD doesn't support spaces in types – use the first word
    match col_type.split_whitespace().next().unwrap_or("TEXT") {
        "BIGSERIAL" => "BIGINT",
        "DOUBLE" => "FLOAT",
        "TIMESTAMP" => "TIMESTAMP",
        "NUMERIC" => "NUMERIC",
        other => other,
    }
}

/// Build the Mermaid `erDiagram` block.
fn build_mermaid(tables: &[Table]) -> String {
    let mut out = String::from("erDiagram\n");

    // Table definitions
    for table in tables {
        out.push_str(&format!("  {} {{\n", table.name));
        for col in &table.columns {
            let mut attrs = Vec::new();
            if col.primary_key {
                attrs.push("PK");
            }
            for fk in &table.foreign_keys {
                if fk.from_col == col.name {
                    attrs.push("FK");
                    break;
                }
            }
            if attrs.is_empty() && col.not_null {
                attrs.push("\"NOT NULL\"");
            }
            let attr_str = if attrs.is_empty() {
                String::new()
            } else {
                format!(" {}", attrs.join(" "))
            };
            out.push_str(&format!(
                "    {} {}{}\n",
                mermaid_type(&col.col_type),
                col.name,
                attr_str
            ));
        }
        out.push_str("  }\n");
    }

    out.push('\n');

    // Relationships
    for table in tables {
        for fk in &table.foreign_keys {
            out.push_str(&format!(
                "  {} ||--o{{ {} : \"{}\"\n",
                fk.to_table, table.name, fk.from_col
            ));
        }
    }

    out
}

/// Render the full HTML page.
pub fn render_schema_html(tables: &[Table], project_name: &str) -> String {
    let now = chrono::Utc::now().format("%Y-%m-%d %H:%M UTC").to_string();
    let mermaid = build_mermaid(tables);

    // Table list sidebar
    let sidebar: String = tables
        .iter()
        .map(|t| {
            let fk_count = t.foreign_keys.len();
            let badge = if fk_count > 0 {
                format!(" <span class=\"badge\">{fk_count} FK</span>")
            } else {
                String::new()
            };
            format!(
                "<li><a href=\"#\" onclick=\"return false\">{}{}</a> <small>({} cols)</small></li>",
                t.name,
                badge,
                t.columns.len()
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
  <title>mongo2pg – Schema Diagram – {project_name}</title>
  <script src="https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"></script>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #f5f7fa;
      color: #333;
      margin: 0;
      display: flex;
      flex-direction: column;
      min-height: 100vh;
    }}
    header {{
      background: #2c3e50;
      color: white;
      padding: 1rem 2rem;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }}
    header h1 {{ margin: 0; font-size: 1.2rem; }}
    header small {{ opacity: 0.6; font-size: 0.75rem; }}
    .layout {{
      display: flex;
      flex: 1;
    }}
    aside {{
      width: 220px;
      background: white;
      border-right: 1px solid #ddd;
      padding: 1rem;
      overflow-y: auto;
      flex-shrink: 0;
    }}
    aside h2 {{ font-size: 0.8rem; text-transform: uppercase; color: #7f8c8d; margin: 0 0 0.75rem; }}
    aside ul {{ list-style: none; margin: 0; padding: 0; }}
    aside li {{ margin-bottom: 0.4rem; font-size: 0.85rem; }}
    aside a {{ color: #2c3e50; text-decoration: none; font-weight: 600; }}
    .badge {{
      background: #e74c3c;
      color: white;
      border-radius: 3px;
      padding: 0 4px;
      font-size: 0.7rem;
    }}
    main {{
      flex: 1;
      padding: 2rem;
      overflow: auto;
    }}
    .mermaid {{
      background: white;
      border-radius: 8px;
      padding: 2rem;
      box-shadow: 0 1px 4px rgba(0,0,0,.08);
    }}
    footer {{ padding: 0.75rem 2rem; font-size: 0.75rem; color: #aaa; text-align: right; }}
  </style>
</head>
<body>
  <header>
    <h1>mongo2pg – Schema Diagram – {project_name}</h1>
    <small>Generated: {now}</small>
  </header>
  <div class="layout">
    <aside>
      <h2>Tables ({count})</h2>
      <ul>
        {sidebar}
      </ul>
    </aside>
    <main>
      <div class="mermaid">
{mermaid}
      </div>
    </main>
  </div>
  <footer>Generated by <a href="https://github.com/pmpetit/mongo2pg">mongo2pg</a></footer>
  <script>
    mermaid.initialize({{ startOnLoad: true, theme: 'default', er: {{ diagramPadding: 40 }} }});
  </script>
</body>
</html>
"#,
        project_name = project_name,
        now = now,
        count = tables.len(),
        sidebar = sidebar,
        mermaid = mermaid,
    )
}

// ──────────────────────────────────────────────────────────────────────────────
// MongoDB collection schema Mermaid renderer
// ──────────────────────────────────────────────────────────────────────────────

/// Build a Mermaid `erDiagram` block from inferred MongoDB collection schemas.
///
/// Each collection is rendered as an entity whose attributes are its top-level
/// fields together with their dominant BSON type.
pub fn build_mongo_mermaid(collections: &[(&str, &CollectionSchema)]) -> String {
    let mut out = String::from("erDiagram\n");
    for (name, schema) in collections {
        // Mermaid entity names must be alphanumeric / underscored.
        let safe_name = sanitize_mermaid_id(name);
        out.push_str(&format!("  {safe_name} {{\n"));
        for (field_name, field) in &schema.object {
            // Pick the dominant (highest-probability non-Undefined) type.
            let type_str = field
                .types
                .iter()
                .filter(|(t, _)| t.as_str() != "Undefined")
                .max_by(|a, b| {
                    a.1.probability
                        .partial_cmp(&b.1.probability)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .map(|(t, _)| t.as_str())
                .unwrap_or("Mixed");
            let safe_field = sanitize_mermaid_id(field_name);
            out.push_str(&format!("    {type_str} {safe_field}\n"));
        }
        out.push_str("  }\n");
    }
    out
}

/// Replace characters that are not valid in Mermaid entity/attribute identifiers
/// with underscores.
fn sanitize_mermaid_id(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Render an HTML page with a Mermaid ER diagram built from inferred MongoDB
/// collection schemas.
///
/// This is the MongoDB-schema counterpart of [`render_schema_html`], produced
/// during `infer` (before `to-pg` generates PostgreSQL DDL).
pub fn render_mongo_schema_html(collections: &[(&str, &CollectionSchema)], db_name: &str) -> String {
    let now = chrono::Utc::now().format("%Y-%m-%d %H:%M UTC").to_string();
    let mermaid = build_mongo_mermaid(collections);

    let sidebar: String = collections
        .iter()
        .map(|(name, schema)| {
            format!(
                "<li><a href=\"#\" onclick=\"return false\">{name}</a> <small>({} fields)</small></li>",
                schema.object.len()
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
  <title>mongo2pg – Schema Diagram – {db_name}</title>
  <script src="https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"></script>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #f5f7fa;
      color: #333;
      margin: 0;
      display: flex;
      flex-direction: column;
      min-height: 100vh;
    }}
    header {{
      background: #2c3e50;
      color: white;
      padding: 1rem 2rem;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }}
    header h1 {{ margin: 0; font-size: 1.2rem; }}
    header small {{ opacity: 0.6; font-size: 0.75rem; }}
    .layout {{
      display: flex;
      flex: 1;
    }}
    aside {{
      width: 220px;
      background: white;
      border-right: 1px solid #ddd;
      padding: 1rem;
      overflow-y: auto;
      flex-shrink: 0;
    }}
    aside h2 {{ font-size: 0.8rem; text-transform: uppercase; color: #7f8c8d; margin: 0 0 0.75rem; }}
    aside ul {{ list-style: none; margin: 0; padding: 0; }}
    aside li {{ margin-bottom: 0.4rem; font-size: 0.85rem; }}
    aside a {{ color: #2c3e50; text-decoration: none; font-weight: 600; }}
    main {{
      flex: 1;
      padding: 2rem;
      overflow: auto;
    }}
    .mermaid {{
      background: white;
      border-radius: 8px;
      padding: 2rem;
      box-shadow: 0 1px 4px rgba(0,0,0,.08);
    }}
    footer {{ padding: 0.75rem 2rem; font-size: 0.75rem; color: #aaa; text-align: right; }}
  </style>
</head>
<body>
  <header>
    <h1>mongo2pg – MongoDB Schema – {db_name}</h1>
    <small>Generated: {now}</small>
  </header>
  <div class="layout">
    <aside>
      <h2>Collections ({count})</h2>
      <ul>
        {sidebar}
      </ul>
    </aside>
    <main>
      <div class="mermaid">
{mermaid}
      </div>
    </main>
  </div>
  <footer>Generated by <a href="https://github.com/pmpetit/mongo2pg">mongo2pg</a></footer>
  <script>
    mermaid.initialize({{ startOnLoad: true, theme: 'default', er: {{ diagramPadding: 40 }} }});
  </script>
</body>
</html>
"#,
        db_name = db_name,
        now = now,
        count = collections.len(),
        sidebar = sidebar,
        mermaid = mermaid,
    )
}
