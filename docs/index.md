<p align="center">
  <img src="logo.png" alt="mongo2pg logo" width="500">
</p>

# mongo2pg

`mongo2pg` is a command-line tool that inspects MongoDB collections, infers
their structure, generates PostgreSQL DDL, exports relationally-shaped CSV
data, and produces HTML reports to support MongoDB-to-PostgreSQL migrations.

---

## Why mongo2pg?

Moving from MongoDB to PostgreSQL is usually not blocked by raw data volume. It
is blocked by shape:

- embedded documents become dependent tables
- arrays become one-to-many expansions
- mixed field types become type-mapping decisions
- very long nested names become PostgreSQL identifier problems

`mongo2pg` helps you answer those questions before you load data into
PostgreSQL.

---

## Key Features

| Feature | Description |
|---|---|
| Schema inference | Samples MongoDB collections and writes JSON schema-like structure plus stats |
| Project workflow | `init` creates a repeatable migration project with config, source, schema, data, and report folders |
| PostgreSQL DDL generation | `to-pg` converts inferred collection schemas into PostgreSQL `CREATE TABLE` statements |
| Per-collection PostgreSQL schemas | Each collection is deployed into its own PostgreSQL schema by default |
| CSV export | `export` expands MongoDB documents into `.csv.gz` files matching the generated SQL tables |
| HTML reporting | `report` generates collection-level, schema-level, cluster-level, and post-import validation reports |
| Post-import validation | `report --post-import` compares MongoDB occurrence counts with PostgreSQL row counts |

---

## Typical Workflow

```bash
# 1. Create a migration project
mongo2pg init \
  --project-base ./projects \
  --project-name sample_airbnb \
  --source-uri 'mongodb://user:pass@localhost:27017/?authSource=admin' \
  --namespace sample_airbnb

# 2. Infer schemas and statistics
mongo2pg infer -c ./projects/sample_airbnb/config/sample_airbnb.conf

# 3. Generate PostgreSQL DDL
mongo2pg to-pg -c ./projects/sample_airbnb/config/sample_airbnb.conf

# 4. Export relational CSV files
mongo2pg export -c ./projects/sample_airbnb/config/sample_airbnb.conf

# 5. Generate reports
mongo2pg report -c ./projects/sample_airbnb/config/sample_airbnb.conf

# 6. Validate a loaded PostgreSQL database
mongo2pg report -c ./projects/sample_airbnb/config/sample_airbnb.conf \
  --post-import \
  --namespace sample_airbnb
```

---

## Outputs

`mongo2pg` works from a project directory that separates concerns clearly:

```text
<project>/
  config/                project configuration (`SOURCE_URI`, `TARGET_URI`, `NAMESPACE`)
  source/collections/    inferred collection schemas and stats
  schema/tables/         generated PostgreSQL DDL
  data/                  exported `.csv.gz` files
  reports/               HTML reports and schema diagrams
```

---

## Screenshots

### Migration report overview

<p align="center">
  <img src="retail.png" alt="mongo2pg migration report overview" width="1000">
</p>

### Schema diagram

<p align="center">
  <img src="schema_diagram.png" alt="mongo2pg schema diagram" width="1000">
</p>

### Post-import validation

<p align="center">
  <img src="post_report.png" alt="mongo2pg post-import validation report" width="1000">
</p>

---

## Documentation Map

- [Installation](install.md)
- [How-To Guides](how-to/README.md)
- [CLI Reference](reference/README.md)
- [Tutorial](tutorial/README.md)
