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

fn parse_primary_key_columns(line: &str) -> Option<Vec<String>> {
    let upper = line.to_uppercase();
    let start = if upper.starts_with("PRIMARY KEY (") {
        line.find('(')?
    } else if upper.starts_with("CONSTRAINT ") && upper.contains(" PRIMARY KEY (") {
        line.find("PRIMARY KEY (")? + "PRIMARY KEY ".len()
    } else {
        return None;
    };
    let end = line.rfind(')')?;
    let cols = line[start + 1..end]
        .split(',')
        .map(|col| col.trim().to_owned())
        .filter(|col| !col.is_empty())
        .collect::<Vec<_>>();
    if cols.is_empty() {
        None
    } else {
        Some(cols)
    }
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
        // End at first CREATE TABLE terminator; later `);` may belong to
        // standalone statements like CREATE INDEX.
        let close = match chunk.find(");") {
            Some(c) => c,
            None => continue,
        };

        let name = chunk[..paren].trim().to_owned();
        let body = &chunk[paren + 1..close];

        let mut columns: Vec<Column> = Vec::new();
        let mut foreign_keys: Vec<ForeignKey> = Vec::new();

        for raw_line in body.lines() {
            let line = raw_line.trim().trim_end_matches(',').trim();
            if line.is_empty() {
                continue;
            }

            if line.to_uppercase().starts_with("CREATE INDEX ") {
                continue;
            }

            if let Some(pk_cols) = parse_primary_key_columns(line) {
                for col in &mut columns {
                    if pk_cols.iter().any(|pk| pk.eq_ignore_ascii_case(&col.name)) {
                        col.primary_key = true;
                        col.not_null = true;
                    }
                }
                continue;
            }

            if let Some(fk_def) = line.strip_prefix("FOREIGN KEY (") {
                // FOREIGN KEY (from_col) REFERENCES to_table (to_col)
                if let Some((from_part, rest)) = fk_def.split_once(") REFERENCES ") {
                    let from_col = from_part.trim().to_owned();
                    let rest = rest.trim();
                    if let Some(sp) = rest.find(" (") {
                        let to_table = rest[..sp].trim().to_owned();
                        let to_col = rest[sp + 2..]
                            .split(')')
                            .next()
                            .unwrap_or("")
                            .trim()
                            .to_owned();
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
            let mut tokens = line.split_whitespace();
            let col_name = match tokens.next() {
                Some(n) if !n.is_empty() => n.to_owned(),
                _ => continue,
            };
            let remainder = line[col_name.len()..].trim();
            if remainder.is_empty() {
                continue;
            }
            let upper_remainder = remainder.to_uppercase();
            let keyword_index = [" NOT NULL", " PRIMARY KEY"]
                .iter()
                .filter_map(|keyword| upper_remainder.find(keyword))
                .min();
            let col_type = keyword_index
                .map(|index| remainder[..index].trim())
                .unwrap_or(remainder)
                .to_owned();
            let rest = keyword_index
                .map(|index| upper_remainder[index..].to_owned())
                .unwrap_or_default();
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

#[cfg(test)]
mod tests {
    use super::parse_sql;

    #[test]
    fn parse_sql_marks_table_level_primary_key_columns_without_fake_primary_column() {
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
        let table = &tables[0];
        let cols: Vec<&str> = table.columns.iter().map(|c| c.name.as_str()).collect();

        assert_eq!(
            cols,
            vec!["log_type", "projectid", "provider", "last_execution"]
        );
        assert!(table.columns[0].primary_key);
        assert!(table.columns[1].primary_key);
        assert!(table.columns[2].primary_key);
        assert!(!table.columns[3].primary_key);
    }

    #[test]
    fn parse_sql_keeps_multi_word_column_types_and_clean_foreign_keys() {
        let sql = r#"
CREATE TABLE parent (
    id TEXT PRIMARY KEY
);

CREATE TABLE child (
    id BIGSERIAL PRIMARY KEY,
    parent_id TEXT NOT NULL,
    amount DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    FOREIGN KEY (parent_id) REFERENCES parent (id) DEFERRABLE INITIALLY DEFERRED
);
"#;

        let tables = parse_sql(sql);
        let child = tables
            .iter()
            .find(|table| table.name == "child")
            .expect("child table should exist");

        let amount = child
            .columns
            .iter()
            .find(|column| column.name == "amount")
            .expect("amount column should exist");
        assert_eq!(amount.col_type, "DOUBLE PRECISION");

        let created_at = child
            .columns
            .iter()
            .find(|column| column.name == "created_at")
            .expect("created_at column should exist");
        assert_eq!(created_at.col_type, "TIMESTAMP WITH TIME ZONE");

        let fk = child
            .foreign_keys
            .iter()
            .find(|fk| fk.from_col == "parent_id")
            .expect("parent_id foreign key should exist");
        assert_eq!(fk.to_table, "parent");
        assert_eq!(fk.to_col, "id");
    }

    #[test]
    fn parse_sql_ignores_create_index_after_table() {
        let sql = r#"
CREATE TABLE parent (
    id TEXT PRIMARY KEY
);

CREATE TABLE child (
    id BIGSERIAL PRIMARY KEY,
    parent_id TEXT NOT NULL,
    FOREIGN KEY (parent_id) REFERENCES parent (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX IF NOT EXISTS idx_child_parent_id ON child (parent_id);
"#;

        let tables = parse_sql(sql);
        let child = tables
            .iter()
            .find(|table| table.name == "child")
            .expect("child table should exist");

        assert!(
            child
                .columns
                .iter()
                .all(|column| !column.name.eq_ignore_ascii_case("create")),
            "CREATE INDEX line must not be parsed as table column"
        );
        assert_eq!(child.foreign_keys.len(), 1);
        assert_eq!(child.foreign_keys[0].from_col, "parent_id");
    }
}

/// Read and parse all `.sql` files in `dir`.
pub fn load_tables(dir: &Path) -> Result<Vec<Table>> {
    let mut all: Vec<Table> = Vec::new();

    let entries =
        std::fs::read_dir(dir).with_context(|| format!("Cannot read {}", dir.display()))?;

    let mut paths: Vec<_> = entries
        .filter_map(|e| e.ok())
        .flat_map(|e| {
            let p = e.path();
            if p.is_dir() {
                // Per-db layout: recurse one level into db subfolders
                std::fs::read_dir(&p)
                    .map(|sub| {
                        sub.filter_map(|s| s.ok())
                            .map(|s| s.path())
                            .filter(|sp| sp.extension().and_then(|x| x.to_str()) == Some("sql"))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            } else if p.extension().and_then(|x| x.to_str()) == Some("sql") {
                vec![p]
            } else {
                vec![]
            }
        })
        .collect();
    paths.sort();

    for path in paths {
        let sql = std::fs::read_to_string(&path)
            .with_context(|| format!("Cannot read {}", path.display()))?;
        all.extend(parse_sql(&sql));
    }

    Ok(all)
}

/// Read SQL files grouped by database subfolder.
///
/// Returns `(db_name, tables)` pairs sorted by `db_name`.
/// For a flat layout (no subdirs) the single entry uses `""` as the db name.
pub fn load_tables_by_db(dir: &Path) -> Result<Vec<(String, Vec<Table>)>> {
    let entries =
        std::fs::read_dir(dir).with_context(|| format!("Cannot read {}", dir.display()))?;

    let mut by_db: Vec<(String, Vec<Table>)> = Vec::new();
    let mut flat: Vec<Table> = Vec::new();

    let mut top_entries: Vec<_> = entries.filter_map(|e| e.ok()).collect();
    top_entries.sort_by_key(|e| e.path());

    for entry in top_entries {
        let p = entry.path();
        if p.is_dir() {
            let db_name = p
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_owned();
            let mut sub_paths: Vec<_> = std::fs::read_dir(&p)
                .with_context(|| format!("Cannot read {}", p.display()))?
                .filter_map(|s| s.ok())
                .map(|s| s.path())
                .filter(|sp| sp.extension().and_then(|x| x.to_str()) == Some("sql"))
                .collect();
            sub_paths.sort();
            let mut tables: Vec<Table> = Vec::new();
            for path in sub_paths {
                let sql = std::fs::read_to_string(&path)
                    .with_context(|| format!("Cannot read {}", path.display()))?;
                tables.extend(parse_sql(&sql));
            }
            if !tables.is_empty() {
                by_db.push((db_name, tables));
            }
        } else if p.extension().and_then(|x| x.to_str()) == Some("sql") {
            let sql = std::fs::read_to_string(&p)
                .with_context(|| format!("Cannot read {}", p.display()))?;
            flat.extend(parse_sql(&sql));
        }
    }

    if !flat.is_empty() {
        // Flat layout: single entry with empty db name
        by_db.push((String::new(), flat));
    }

    Ok(by_db)
}

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
///
/// When the number of tables exceeds `MAX_ERD_TABLES` the Mermaid diagram is
/// replaced with an informational message; the sidebar table list is always shown.
pub fn render_schema_html(tables: &[Table], project_name: &str) -> String {
    /// Mermaid ERD starts timing-out / producing blank output above ~50 entities.
    const MAX_ERD_TABLES: usize = 50;

    let now = chrono::Utc::now().format("%Y-%m-%d %H:%M UTC").to_string();
    let mermaid_block = if tables.len() <= MAX_ERD_TABLES {
        let mermaid = build_mermaid(tables);
        format!(
            r#"<div class="mermaid">
{mermaid}
      </div>"#
        )
    } else {
        format!(
            r#"<div class="too-many">
        <p>⚠ Too many tables ({count}) to render as an ERD diagram (limit: {MAX_ERD_TABLES}).</p>
        <p>Run <code>mongo2pg to-pg</code> per database and use the per-database schema diagram instead.</p>
      </div>"#,
            count = tables.len(),
        )
    };

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
    .too-many {{
      background: #fff8e1;
      border: 1px solid #f9a825;
      border-radius: 8px;
      padding: 1.5rem 2rem;
      color: #5d4037;
      font-size: 0.95rem;
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
      {mermaid_block}
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
        mermaid_block = mermaid_block,
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
pub fn render_mongo_schema_html(
    collections: &[(&str, &CollectionSchema)],
    db_name: &str,
) -> String {
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
