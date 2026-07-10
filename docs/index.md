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
| PostgreSQL DDL generation | `infer` refreshes PostgreSQL `CREATE TABLE` statements under `schema/tables/` when used with a project config |
| Collection filtering | Configure `[source].include` / `[source].exclude` in TOML to scope `infer`, `to-pg`, `export`, and `import` runs (`exclude` wins) |
| Per-collection PostgreSQL schemas | Each collection is deployed into its own PostgreSQL schema by default |
| CSV export | `export` expands MongoDB documents into `.csv.gz` files matching the generated SQL tables |
| PostgreSQL import | `import` creates PostgreSQL objects and loads exported `.csv.gz` files using `TARGET_URI` |
| HTML reporting | `infer` writes collection-level and schema-level reports, flags collections with infer warnings, and `report` can regenerate them from existing files |
| Post-import validation | `import` writes `reports/post_report.html` automatically, and `report --post-import` can regenerate it later |

---

## Typical Workflow

```bash
# 1. Create a migration project
mongo2pg init \
  --project-base ./projects \
  --project-name sample_airbnb \
  --cluster-name dev-cluster \
  --source-uri 'mongodb://user:pass@localhost:27017/?authSource=admin' \
  --target-uri 'postgres://postgres:x@localhost:5432/postgres?sslmode=disable' \
  --namespace sample_airbnb

# 2. Infer schemas, generate PostgreSQL DDL, and write the main reports
mongo2pg infer -c ./projects/sample_airbnb/dev-cluster/config/dev-cluster.toml

# 3. Export relational CSV files
mongo2pg export -c ./projects/sample_airbnb/dev-cluster/config/dev-cluster.toml

# 4. Create PostgreSQL objects and import the exported CSV files
mongo2pg import -c ./projects/sample_airbnb/dev-cluster/config/dev-cluster.toml

# 5. `import` already generated reports/post_report.html.
#    Run this only if you want to regenerate it.
mongo2pg report -c ./projects/sample_airbnb/dev-cluster/config/dev-cluster.toml --post-import
```

---

## Outputs

`mongo2pg` works from a project directory that separates concerns clearly:

```text
<project>/
  config/                project configuration (`[source]`, `[target]`, include/exclude filters)
  source/collections/    inferred collection schemas and stats
  schema/tables/         generated PostgreSQL DDL
  data/                  exported `.csv.gz` files
  reports/               HTML reports and schema diagrams
```

With `--cluster-name`, the resolved root is `<project-base>/<project-name>/<cluster-name>/`
and the config file name is `<cluster-name>.toml`.

---

## Screenshots

### Cluster overview

<p align="center">
  <img src="main2.png" alt="mongo2pg cluster overview report" width="1000">
</p>

### Database drill-down

<p align="center">
  <img src="main1.png" alt="mongo2pg database detail report" width="1000">
</p>

### Schema diagram

<p align="center">
  <img src="main3.png" alt="mongo2pg schema diagram view" width="1000">
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
