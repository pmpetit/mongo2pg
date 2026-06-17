# CLI Reference

`mongo2pg` exposes seven subcommands.

---

## `mongo2pg init`

Creates a project directory structure and a TOML config file for repeatable runs.

```text
mongo2pg init --project-base <dir>
              --project-name <name>
              [--source-uri <mongodb-uri>]
              [--target-uri <postgres-uri>]
              [--namespace <db-or-db.collection>]
```

| Flag | Description |
|---|---|
| `--project-base` | Base directory where the project folder will be created |
| `--project-name` | Project name |
| `--source-uri` | MongoDB source connection URI stored in the config file |
| `--target-uri` | PostgreSQL target connection URI stored in the config file |
| `--namespace` | Default namespace stored in the config file |

---

## `mongo2pg infer`

Samples MongoDB data and writes inferred collection schemas and statistics.
With `-c <config>`, it also refreshes PostgreSQL DDL and the main HTML reports.

```text
mongo2pg infer --source-uri <mongodb-uri>
               [--namespace <db-or-db.collection>]
               [--number <n> | --percent <pct>]
               [--output-dir <dir>]
               [--jsonb]
               [--print-json]

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
| `--print-json` | Print the inferred schema JSON to stdout |
| `-c, --config` | Project config file |

When using `-c <config>`, collection filters from `[source].include` and
`[source].exclude` are applied (`exclude` takes precedence).
Collections with infer warnings are highlighted in `reports/main.html`, and
warning details are also written to each `<collection>.stats.yaml`.

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

With `-c <config>`, `[source].include` / `[source].exclude` filters are also
applied before export.

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

## `mongo2pg import`

Creates PostgreSQL objects from generated SQL files and loads exported
`.csv.gz` files into PostgreSQL using `COPY FROM STDIN`.

```text
mongo2pg import [collection] -c <config> [--namespace <db-or-db.collection>]
```

| Flag | Description |
|---|---|
| `[collection]` | Optional collection name |
| `-c, --config` | Project config file |
| `--namespace` | Database or fully qualified collection namespace |

With `-c <config>`, `[source].include` / `[source].exclude` filters are also
applied before import.

---

## `[kafka]` config section properties

`mongo2pg kafka-import -c <config>` reads Kafka settings from the project TOML
`[kafka]` section.

Example:

```toml
[kafka]
bootstrap_servers = "localhost:9092"
group_id = "mongo2pg-kafka-import"
topics = ["mongo2pg_sample_airbnb.sample_airbnb.listingsAndReviews"]
topic_prefix = "mongo2pg_sample_airbnb"
schema_registry_url = "http://localhost:8081"
# schema_registry_username = ""
# schema_registry_password = ""
# offset = "latest"
# auto_offset_reset = "earliest"  # legacy key still supported
# max_messages = 1000
# batch_log_messages = 100
```

| Property | Required | Description |
|---|---|---|
| `bootstrap_servers` | Yes | Kafka bootstrap servers (for example `localhost:9092`) |
| `group_id` | No | Consumer group id. Default: `mongo2pg-kafka-import` |
| `topics` | Yes* | Explicit topic list consumed by `kafka-import` |
| `topic_prefix` | No | Prefix expected before `<db>.<collection>` in topic names (for example `mongo2pg_sample_airbnb`) |
| `schema_registry_url` | No | Schema Registry base URL used for Confluent-framed Avro payloads |
| `schema_registry_username` | No | Optional basic-auth username for Schema Registry |
| `schema_registry_password` | No | Optional basic-auth password for Schema Registry |
| `offset` | No | Offset policy override (`latest`, `earliest`, `0`) |
| `auto_offset_reset` | No | Legacy alias for offset policy when `offset` is absent |
| `max_messages` | No | Stop after this many successfully applied messages |
| `batch_log_messages` | No | Progress log interval for `kafka-import`. Default: `100` |

`*` `topics` can be omitted only when you pass `--topics` on the CLI.

### Topic parsing behavior

- With `topic_prefix` set, topic names must start with `<topic_prefix>.`.
- The prefix is removed, then the last two segments are interpreted as `<db>.<collection>`.
- Messages whose topic does not match the prefix are skipped.

### Offset behavior

- `offset = "latest"` starts from latest offset when no committed group offset exists.
- `offset = "earliest"` starts from earliest offset when no committed group offset exists.
- `offset = "0"` enables snapshot-equivalent mode: fresh group id, earliest consumption, mapped-table truncate before apply, idle timeout stop.
