# CLI Reference

`mongo2pg` exposes seven subcommands.

---

## `mongo2pg init`

Creates a project directory structure and a TOML config file for repeatable runs.

```text
mongo2pg init --project-base <dir>
              --project-name <name>
              [--cluster-name <cluster-name>]
              [--source-uri <mongodb-uri>]
              [--target-uri <postgres-uri>]
              [--namespace <db-or-db.collection>]
```

| Flag | Description |
| --- | --- |
| `--project-base` | Base directory where the project folder will be created |
| `--project-name` | Project name |
| `--cluster-name` | Optional cluster segment appended after project name in generated paths |
| `--source-uri` | MongoDB source connection URI stored in the config file |
| `--target-uri` | PostgreSQL target connection URI stored in the config file |
| `--namespace` | Default namespace stored in the config file |

When `--cluster-name` is provided, `init` creates:

```text
<project-base>/<project-name>/<cluster-name>/
    config/<cluster-name>.toml
    source/collections/
    schema/tables/
    data/
    reports/
```

Without `--cluster-name`, layout remains:

```text
<project-base>/<project-name>/
    config/<project-name>.toml
    source/collections/
    schema/tables/
    data/
    reports/
```

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
| --- | --- |
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
| --- | --- |
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
| --- | --- |
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
| --- | --- |
| `[collection]` | Optional collection name |
| `-c, --config` | Project config file |
| `--namespace` | Database or fully qualified collection namespace |

With `-c <config>`, `[source].include` / `[source].exclude` filters are also
applied before import.

Import preflight behavior:

- Ensures target database exists before connecting to the destination database session.
- Ensures target schema exists before executing destination table DDL.
- Fails fast with actionable errors when database/schema creation is denied by PostgreSQL privileges.
- Stops early if any destination tables already exist; drop or clean destination tables before retrying.

---

## `mongo2pg ping`

Checks backend connectivity for selected dependencies without running infer/export/import flows.

```text
mongo2pg ping -c <config> [--source] [--target] [--kafka]
```

| Flag | Description |
| --- | --- |
| `-c, --config` | Project config file |
| `--source` | Validate MongoDB SOURCE_URI connectivity |
| `--target` | Validate PostgreSQL TARGET_URI connectivity |
| `--kafka` | Validate Kafka bootstrap/auth reachability |

At least one backend flag is required. The command prints one pass/fail line per selected backend and exits non-zero if any selected backend fails.

Examples:

```bash
mongo2pg ping -c ./projects/airbnb/config/airbnb.toml --source
mongo2pg ping -c ./projects/airbnb/config/airbnb.toml --target
mongo2pg ping -c ./projects/airbnb/config/airbnb.toml --kafka
```

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
# transaction_batch_size = 200
# flush_batch_after = "1000ms"
# copy_mode = true
# worker_count = 4
# stop_on_no_lag = true
# group_id_log_suffix = true
```

| Property | Required | Description |
| --- | --- | --- |
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
| `transaction_batch_size` | No | Flush/commit threshold by message count. Default: `1` |
| `flush_batch_after` | No | Time-based flush threshold for partial batches. Accepts `ms`, `s`, `m` (example: `1000ms`). Disabled by default |
| `copy_mode` | No | Enables COPY-based apply path for Kafka import batches. Default: auto (`true` when `enable_auto_commit=true` and `transaction_batch_size>1`, otherwise `false`) |
| `worker_count` | No | Number of Kafka-import worker processes. Default: `1` |
| `stop_on_no_lag` | No | Stops kafka-import once lag remains stable at zero for enough idle checks. Default: `false` |
| `group_id_log_suffix` | No | Adds worker suffix to group id in logs for multi-worker runs. Default: `true` |

`*` `topics` can be omitted when either:

- `--topics` is passed on the CLI, or
- `topic_prefix` is set and matching broker topics are auto-discovered.

### Topic parsing behavior

- With `topic_prefix` set, topic names must start with `<topic_prefix>.`.
- If `topics` is empty, `kafka-import` auto-discovers broker topics starting with `<topic_prefix>.` and subscribes to them.
- If `--topics` is passed while `topic_prefix` is also set, `kafka-import` logs a warning and ignores `topic_prefix` (explicit topics take precedence).
- The prefix is removed, then the last two segments are interpreted as `<db>.<collection>`.
- Messages whose topic does not match the prefix are skipped.

### Offset behavior

- `offset = "latest"` starts from latest offset when no committed group offset exists.
- `offset = "earliest"` starts from earliest offset when no committed group offset exists.
- `offset = "0"` enables snapshot-equivalent mode: fresh group id, earliest consumption, mapped-table truncate before apply, idle timeout stop.

### Copy mode and stop behavior

- `copy_mode = true` enables COPY-based flushing for buffered Kafka messages.
- `worker_count > 1` starts child worker processes and shares one effective consumer group id.
- `stop_on_no_lag = true` waits for lag to stay stable at zero before stopping.
- For multi-worker runs, enable `group_id_log_suffix = true` to simplify worker-specific log triage.

### Kafka-import module ownership

- CLI parsing remains in `src/cli`, and command dispatch is centralized in `src/commands/mod.rs`.
- Kafka-import runtime implementation is isolated in `src/commands/kafka_import.rs` alongside the other command handlers.
- Keep behavior changes in dedicated feature changesets; extraction-oriented edits should preserve existing stage/counter semantics.

### Kafka-import refactor validation checklist

- Build binary: `cargo build`.
- Run kafka-focused unit tests in binary: `cargo test --bin mongo2pg kafka_`.
- Run COPY/fallback parity tests: `cargo test --test kafka_snapshot_copy_test`.
- Run container-backed parity tests when Docker is available: `cargo test --test kafka_snapshot_copy_test -- --ignored --nocapture`.
