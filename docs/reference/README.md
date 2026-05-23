# CLI Reference

`mongo2pg` exposes six subcommands.

---

## `mongo2pg init`

Creates a project directory structure and a `.conf` file for repeatable runs.

```text
mongo2pg init --project-base <dir>
              --project-name <name>
              [--source-uri <mongodb-uri>]
              [--namespace <db-or-db.collection>]
```

| Flag | Description |
|---|---|
| `--project-base` | Base directory where the project folder will be created |
| `--project-name` | Project name |
| `--source-uri` | MongoDB source connection URI stored in the config file |
| `--namespace` | Default namespace stored in the config file |

---

## `mongo2pg infer`

Samples MongoDB data and writes inferred collection schemas and statistics.

```text
mongo2pg infer --source-uri <mongodb-uri>
               [--namespace <db-or-db.collection>]
               [--number <n> | --percent <pct>]
               [--output-dir <dir>]
               [--jsonb]
               [--no-output]

mongo2pg infer -c <config>
```

| Flag | Description |
|---|---|
| `--source-uri` | MongoDB source connection URI |
| `--namespace` | One collection, one database, or omitted to enumerate all user databases |
| `--number` | Number of documents to sample |
| `--percent` | Percentage of the collection to sample |
| `--output-dir` | Directory where inferred files are written |
| `--jsonb` | Emit MongoDB objects as JSONB columns instead of child tables where applicable |
| `--no-output` | Suppress JSON schema output to stdout |
| `-c, --config` | Project config file |

---

## `mongo2pg to-pg`

Converts inferred collection schemas to PostgreSQL DDL.

```text
mongo2pg to-pg [collection] [--table <name>] [--schema <name>] [-c <config> | -o <dir>]
```

| Flag | Description |
|---|---|
| `[collection]` | Optional collection name |
| `--table` | Root table name override for a single collection |
| `--schema` | PostgreSQL schema name override |
| `-c, --config` | Project config file |
| `-o, --output-dir` | SQL output directory override |

---

## `mongo2pg export`

Exports MongoDB data into gzipped CSV files matching generated PostgreSQL
tables.

```text
mongo2pg export [collection] -c <config> [--output-dir <dir>] [--namespace <db-or-db.collection>]
```

| Flag | Description |
|---|---|
| `[collection]` | Optional collection name |
| `-c, --config` | Project config file |
| `--output-dir` | CSV output directory override |
| `--namespace` | Database or fully qualified collection namespace |

---

## `mongo2pg report`

Generates HTML migration reports.

```text
mongo2pg report [--collections-dir <dir> | -c <config>] [--output <file>] [--namespace <ns>] [--post-import]
```

| Flag | Description |
|---|---|
| `-c, --config` | Project config file |
| `--collections-dir` | Path to `source/collections/` |
| `--output` | HTML output file |
| `--namespace` | Namespace label or selection |
| `--post-import` | Compare MongoDB expanded counts with PostgreSQL row counts |

---
